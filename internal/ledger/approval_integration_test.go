package ledger

import (
	"crypto/ed25519"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"example.com/llmgateway/internal/core"
)

func requester(client, workload string) core.Principal {
	return core.Principal{Client: client, Workload: workload, App: "support"}
}

func approver(client, workload string) core.Principal {
	return core.Principal{Client: client, Workload: workload, Role: "model-approver"}
}

func TestPGApprovalLifecycleAndScopes(t *testing.T) {
	ctx, s := fixture(t, 100)
	p := requester("client", "spiffe://lab/requester")
	reviewer := approver("client", "spiffe://lab/reviewer")
	first, err := s.RequestApproval(ctx, p, "grant-1", "model-a", time.Minute)
	if err != nil || first.State != "pending" {
		t.Fatalf("request state: %+v %v", first, err)
	}
	replay, err := s.RequestApproval(ctx, p, "grant-1", "model-a", time.Minute)
	if err != nil || !replay.ExpiresAt.Equal(first.ExpiresAt) {
		t.Fatalf("replayed request changed expiry: %+v %v", replay, err)
	}
	_, err = s.RequestApproval(ctx, p, "grant-1", "model-b", time.Minute)
	if !errors.Is(err, ErrApprovalConflict) {
		t.Fatalf("request changed model on replay: %v", err)
	}
	if _, err := s.MatchingApproval(ctx, p, "model-a"); !errors.Is(err, ErrApprovalNotFound) {
		t.Fatalf("pending granted access: %v", err)
	}
	self := p
	self.Role = reviewer.Role
	if _, err := s.DecideApproval(ctx, self, "grant-1", true, "self", reviewer.Role); !errors.Is(err, ErrApprovalForbidden) {
		t.Fatalf("self approved: %v", err)
	}
	if _, err := s.DecideApproval(ctx, requester("client", "other"), "grant-1", true, "no role", reviewer.Role); !errors.Is(err, ErrApprovalForbidden) {
		t.Fatalf("untrusted role decided: %v", err)
	}
	if _, err := s.DecideApproval(ctx, approver("foreign", "reviewer"), "grant-1", true, "foreign", reviewer.Role); !errors.Is(err, ErrApprovalNotFound) {
		t.Fatalf("foreign client decided: %v", err)
	}
	granted, err := s.DecideApproval(ctx, reviewer, "grant-1", true, "temporary incident access", reviewer.Role)
	if err != nil || granted.State != "approved" || granted.DecidedBy != reviewer.Workload {
		t.Fatalf("approve: %+v %v", granted, err)
	}
	if _, err := s.MatchingApproval(ctx, p, "model-a"); err != nil {
		t.Fatal(err)
	}
	for _, scope := range []struct {
		p     core.Principal
		model string
	}{
		{requester("client", "different"), "model-a"},
		{requester("foreign", p.Workload), "model-a"},
		{p, "model-b"},
	} {
		if _, err := s.MatchingApproval(ctx, scope.p, scope.model); !errors.Is(err, ErrApprovalNotFound) {
			t.Fatalf("scope escaped: %+v %v", scope, err)
		}
	}
	replay, err = s.DecideApproval(ctx, reviewer, "grant-1", true, "temporary incident access", reviewer.Role)
	if err != nil || !replay.UpdatedAt.Equal(granted.UpdatedAt) {
		t.Fatalf("decision replay mutated record: %+v %v", replay, err)
	}
	if _, err := s.DecideApproval(ctx, reviewer, "grant-1", false, "changed mind", reviewer.Role); !errors.Is(err, ErrApprovalConflict) {
		t.Fatalf("decision overwritten: %v", err)
	}
	revoked, err := s.RevokeApproval(ctx, reviewer, "grant-1", "incident resolved", reviewer.Role)
	if err != nil || revoked.State != "revoked" {
		t.Fatalf("revoke: %+v %v", revoked, err)
	}
	if _, err := s.MatchingApproval(ctx, p, "model-a"); !errors.Is(err, ErrApprovalNotFound) {
		t.Fatalf("revoked grant active: %v", err)
	}
	balances(t, ctx, s, 0, 0)
	if err := s.VerifyAudit(ctx, s.key.Public().(ed25519.PublicKey)); err != nil {
		t.Fatal(err)
	}
	var events int
	if err := s.DB.QueryRowContext(ctx, `SELECT count(*) FROM gateway_audit WHERE kind LIKE 'approval_%'`).Scan(&events); err != nil || events != 3 {
		t.Fatalf("want request/approve/revoke once each: %d %v", events, err)
	}
}

func TestPGApprovalConcurrentDecisionAndExpiry(t *testing.T) {
	ctx, s := fixture(t, 100)
	p := requester("client", "requester")
	reviewer := approver("client", "reviewer")
	if _, err := s.RequestApproval(ctx, p, "grant", "model", time.Minute); err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	results := make(chan error, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	for _, allow := range []bool{true, false} {
		go func() {
			ready.Done()
			<-gate
			_, err := s.DecideApproval(ctx, reviewer, "grant", allow, "reviewed", reviewer.Role)
			results <- err
		}()
	}
	ready.Wait()
	close(gate)
	successes, conflicts := 0, 0
	for range 2 {
		switch err := <-results; {
		case err == nil:
			successes++
		case errors.Is(err, ErrApprovalConflict):
			conflicts++
		default:
			t.Error(err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("competing decisions successes=%d conflicts=%d", successes, conflicts)
	}
	if _, err := s.RequestApproval(ctx, p, "expires", "model", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DecideApproval(ctx, reviewer, "expires", true, "reviewed", reviewer.Role); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.ExecContext(ctx, `UPDATE gateway_approvals SET expires_at = clock_timestamp()-interval '1 second' WHERE id='expires'`); err != nil {
		t.Fatal(err)
	}
	expired, err := s.GetApproval(ctx, "client", "expires")
	if err != nil || expired.State != "expired" {
		t.Fatalf("expired read: %+v %v", expired, err)
	}
	if _, err := s.MatchingApproval(ctx, p, "model"); err == nil {
		// Another grant named "grant" may have won approval. Isolate this expiry check.
		if _, err := s.DB.ExecContext(ctx, `UPDATE gateway_approvals SET expires_at = clock_timestamp()-interval '1 second' WHERE id='grant'`); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.MatchingApproval(ctx, p, "model"); !errors.Is(err, ErrApprovalNotFound) {
		t.Fatalf("expired grant active: %v", err)
	}
	replayed, err := s.RequestApproval(ctx, p, "expires", "model", time.Minute)
	if err != nil || replayed.State != "expired" || !replayed.ExpiresAt.Equal(expired.ExpiresAt) {
		t.Fatalf("replay renewed expiry: %+v %v", replayed, err)
	}
	if _, err := s.DecideApproval(ctx, reviewer, "expires", true, "reviewed", reviewer.Role); !errors.Is(err, ErrApprovalConflict) {
		t.Fatalf("expired grant was approved: %v", err)
	}
}

func TestPGApprovalAuditFailureRollsBackDecision(t *testing.T) {
	ctx, s := fixture(t, 100)
	p := requester("client", "requester")
	reviewer := approver("client", "reviewer")
	if _, err := s.RequestApproval(ctx, p, "grant", "model", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.ExecContext(ctx, `
		ALTER TABLE gateway_audit ADD CONSTRAINT reject_approval
		CHECK (kind <> 'approval_approved') NOT VALID`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DecideApproval(ctx, reviewer, "grant", true, "reviewed", reviewer.Role); err == nil {
		t.Fatal("approval succeeded without its audit event")
	}
	got, err := s.GetApproval(ctx, "client", "grant")
	if err != nil || got.State != "pending" {
		t.Fatalf("decision partially committed: %+v %v", got, err)
	}
	if _, err := s.MatchingApproval(ctx, p, "model"); !errors.Is(err, ErrApprovalNotFound) {
		t.Fatalf("unaudited approval active: %v", err)
	}
}

func TestPGDispatchRechecksExactApprovalAfterReservation(t *testing.T) {
	for _, scenario := range []string{"valid", "expired", "revoked", "missing", "wrong_model", "wrong_workload", "wrong_client"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, s := fixture(t, 100)
			p := requester("client", "spiffe://lab/requester")
			reviewer := approver("client", "spiffe://lab/reviewer")
			a := attempt("attempt", 10)
			a.Principal = p
			a.Route.RequiresApproval = true
			a.Route.Model = "model-a"
			grantClient, grantWorkload, grantModel := p.Client, p.Workload, a.Route.Model
			switch scenario {
			case "wrong_client":
				grantClient = "foreign"
				if err := s.SeedBudget(ctx, grantClient, 100); err != nil {
					t.Fatal(err)
				}
				reviewer.Client = grantClient
			case "wrong_workload":
				grantWorkload = "spiffe://lab/other"
			case "wrong_model":
				grantModel = "model-b"
			}
			grantP := requester(grantClient, grantWorkload)
			if _, err := s.RequestApproval(ctx, grantP, "grant", grantModel, time.Minute); err != nil {
				t.Fatal(err)
			}
			if _, err := s.DecideApproval(ctx, reviewer, "grant", true, "incident reviewed", reviewer.Role); err != nil {
				t.Fatal(err)
			}
			a.ApprovalID = "grant"
			if scenario == "missing" {
				a.ApprovalID = ""
			}
			reserve(t, ctx, s, a)
			switch scenario {
			case "expired":
				if _, err := s.DB.ExecContext(ctx, `UPDATE gateway_approvals SET expires_at=clock_timestamp()-interval '1 second'`); err != nil {
					t.Fatal(err)
				}
			case "revoked":
				if _, err := s.RevokeApproval(ctx, reviewer, "grant", "incident ended", reviewer.Role); err != nil {
					t.Fatal(err)
				}
			}
			err := s.MarkDispatch(ctx, a.ID)
			wantState, wantEvents := "reserved", 0
			if scenario == "valid" {
				if err != nil {
					t.Fatal(err)
				}
				wantState, wantEvents = "dispatching", 1
			} else if err == nil {
				t.Fatal("invalid approval permitted dispatch")
			}
			var state string
			var dispatches int
			if err := s.DB.QueryRowContext(ctx, `
				SELECT state, (SELECT count(*) FROM gateway_audit WHERE kind='dispatching' AND attempt_id=$1)
				FROM gateway_attempts WHERE id=$1`, a.ID).Scan(&state, &dispatches); err != nil {
				t.Fatal(err)
			}
			if state != wantState || dispatches != wantEvents {
				t.Fatalf("state=%s dispatch events=%d, want %s/%d", state, dispatches, wantState, wantEvents)
			}
			balances(t, ctx, s, 10, 0)
		})
	}
}

// Hold the audit head to pause MarkDispatch after it acquires the approval row.
// Revocation must then wait for dispatch to commit rather than pass its check.
func TestPGDispatchAndRevocationSerialize(t *testing.T) {
	ctx, s := fixture(t, 100)
	p := requester("client", "spiffe://lab/requester")
	reviewer := approver("client", "spiffe://lab/reviewer")
	if _, err := s.RequestApproval(ctx, p, "grant", "model-a", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DecideApproval(ctx, reviewer, "grant", true, "reviewed", reviewer.Role); err != nil {
		t.Fatal(err)
	}
	a := attempt("attempt", 10)
	a.Principal, a.Route.Model, a.Route.RequiresApproval, a.ApprovalID = p, "model-a", true, "grant"
	reserve(t, ctx, s, a)

	head, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer head.Rollback()
	var headPID int
	if err := head.QueryRowContext(ctx, `SELECT pg_backend_pid() FROM gateway_audit_head WHERE singleton FOR UPDATE`).Scan(&headPID); err != nil {
		t.Fatal(err)
	}
	dispatchDone := make(chan error, 1)
	go func() { dispatchDone <- s.MarkDispatch(ctx, a.ID) }()
	waitBlocked := func(blocker int) int {
		t.Helper()
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			var pid int
			err := s.DB.QueryRowContext(ctx, `
				SELECT pid FROM pg_stat_activity WHERE datname=current_database()
				AND $1::integer = ANY(pg_blocking_pids(pid)) LIMIT 1`, blocker).Scan(&pid)
			if err == nil {
				return pid
			}
			if !errors.Is(err, sql.ErrNoRows) {
				t.Fatal(err)
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				t.Fatal("transaction never reached expected lock barrier")
			}
		}
	}
	dispatchPID := waitBlocked(headPID)
	revokeDone := make(chan error, 1)
	go func() {
		_, err := s.RevokeApproval(ctx, reviewer, "grant", "incident ended", reviewer.Role)
		revokeDone <- err
	}()
	waitBlocked(dispatchPID)
	if err := head.Commit(); err != nil {
		t.Fatal(err)
	}
	for _, done := range []<-chan error{dispatchDone, revokeDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("transaction failed to finish")
		}
	}
	var dispatchSeq, revokeSeq int64
	if err := s.DB.QueryRowContext(ctx, `
		SELECT (SELECT seq FROM gateway_audit WHERE kind='dispatching' AND attempt_id='attempt'),
			(SELECT seq FROM gateway_audit WHERE kind='approval_revoked')`).Scan(&dispatchSeq, &revokeSeq); err != nil {
		t.Fatal(err)
	}
	if dispatchSeq >= revokeSeq {
		t.Fatalf("dispatch/revocation audit ordering %d/%d", dispatchSeq, revokeSeq)
	}
}
