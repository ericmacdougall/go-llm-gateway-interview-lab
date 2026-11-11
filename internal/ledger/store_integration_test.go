package ledger

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"example.com/llmgateway/internal/core"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func fixture(t *testing.T, limit int64) (context.Context, *Store) {
	t.Helper()
	dsn := os.Getenv("GATEWAY_TEST_DSN")
	if dsn == "" {
		t.Skip("set GATEWAY_TEST_DSN to a disposable PostgreSQL database")
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal("invalid test database configuration")
	}
	if !strings.HasPrefix(cfg.Host, "/") && cfg.Host != "localhost" && cfg.Host != "127.0.0.1" && cfg.Host != "::1" {
		t.Fatal("test fixture requires local PostgreSQL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	admin := stdlib.OpenDB(*cfg)
	t.Cleanup(func() { _ = admin.Close() })
	var tag [8]byte
	if _, err := rand.Read(tag[:]); err != nil {
		t.Fatal(err)
	}
	name := "ledger_test_" + hex.EncodeToString(tag[:])
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := admin.ExecContext(dropCtx, "DROP SCHEMA "+pgx.Identifier{name}.Sanitize()+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cfg.RuntimeParams["search_path"] = name
	db := stdlib.OpenDB(*cfg)
	db.SetMaxOpenConns(10)
	s := &Store{DB: db, key: key}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.SeedBudget(ctx, "client", limit); err != nil {
		t.Fatal(err)
	}
	return ctx, s
}

func attempt(id string, estimate int64) core.Attempt {
	return core.Attempt{
		ID: id, RequestID: "req-" + id, Estimate: estimate,
		Principal:     core.Principal{Client: "client", Workload: "spiffe://lab/client"},
		Route:         core.Route{ID: "lab", Provider: "local", Account: "a", Model: "m", Region: "lab-region"},
		PolicyVersion: "v1", Rule: "client-cloud",
	}
}

func reserve(t *testing.T, ctx context.Context, s *Store, a core.Attempt) {
	t.Helper()
	ok, err := s.Reserve(ctx, a)
	if err != nil || !ok {
		t.Fatalf("reserve %s: ok=%v err=%v", a.ID, ok, err)
	}
}

func balances(t *testing.T, ctx context.Context, s *Store, wantHeld, wantSpent int64) {
	t.Helper()
	var held, spent int64
	err := s.DB.QueryRowContext(ctx, `SELECT held, spent FROM gateway_budgets WHERE client_id='client'`).Scan(&held, &spent)
	if err != nil || held != wantHeld || spent != wantSpent {
		t.Fatalf("balances held=%d spent=%d, want %d/%d: %v", held, spent, wantHeld, wantSpent, err)
	}
}

func race(t *testing.T, count int, fn func(int) (bool, error)) int {
	t.Helper()
	type result struct {
		ok  bool
		err error
	}
	gate := make(chan struct{})
	results := make(chan result, count)
	var ready sync.WaitGroup
	ready.Add(count)
	for i := 0; i < count; i++ {
		go func() {
			ready.Done()
			<-gate
			ok, err := fn(i)
			results <- result{ok, err}
		}()
	}
	ready.Wait()
	close(gate)
	applied := 0
	for i := 0; i < count; i++ {
		r := <-results
		if r.err != nil {
			t.Error(r.err)
		}
		if r.ok {
			applied++
		}
	}
	return applied
}

func TestPGAdmissionAndSettlementRace(t *testing.T) {
	ctx, s := fixture(t, 10)
	admitted := race(t, 9, func(i int) (bool, error) {
		return s.Reserve(ctx, attempt(fmt.Sprint(i), 10))
	})
	if admitted != 1 {
		t.Fatalf("admitted=%d, want 1", admitted)
	}
	balances(t, ctx, s, 10, 0)
	var id string
	if err := s.DB.QueryRowContext(ctx, `SELECT id FROM gateway_attempts`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkDispatch(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkDispatch(ctx, id); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("repeat dispatch: %v", err)
	}
	final := core.Evidence{Final: true, Actual: 7, ProviderRequestID: "provider-r1", Ref: "usage-r1"}
	applied := race(t, 8, func(int) (bool, error) { return s.Settle(ctx, id, final) })
	if applied != 1 {
		t.Fatalf("settlements=%d, want 1", applied)
	}
	balances(t, ctx, s, 0, 7)
	var events int
	if err := s.DB.QueryRowContext(ctx, `SELECT count(*) FROM gateway_audit WHERE kind='settled' AND attempt_id=$1`, id).Scan(&events); err != nil || events != 1 {
		t.Fatalf("initial settlement events=%d: %v", events, err)
	}
	final.Actual = 8
	if ok, err := s.Settle(ctx, id, final); ok || !errors.Is(err, ErrConflictingUsage) {
		t.Fatalf("conflicting replay: ok=%v err=%v", ok, err)
	}
	balances(t, ctx, s, 0, 7)
	if err := s.VerifyAudit(ctx, s.key.Public().(ed25519.PublicKey)); err != nil {
		t.Fatal(err)
	}
}

func TestPGRecoveryKeepsOnlyPossiblyDispatchedHold(t *testing.T) {
	ctx, s := fixture(t, 20)
	reserve(t, ctx, s, attempt("unused", 10))
	reserve(t, ctx, s, attempt("crashed", 10))
	if err := s.MarkDispatch(ctx, "crashed"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.ExecContext(ctx, `UPDATE gateway_attempts SET updated_at = clock_timestamp() - interval '1 hour'`); err != nil {
		t.Fatal(err)
	}
	if err := s.RecoverStale(ctx, time.Minute, 10); err != nil {
		t.Fatal(err)
	}
	balances(t, ctx, s, 10, 0)
	if err := s.MarkDispatch(ctx, "unused"); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("released reservation could dispatch: %v", err)
	}
	if err := s.MarkDispatch(ctx, "crashed"); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("crashed attempt could dispatch again: %v", err)
	}
	pending, err := s.ListDue(ctx, 10)
	if err != nil || len(pending) != 1 || pending[0].Attempt.ID != "crashed" {
		t.Fatalf("pending=%v err=%v", pending, err)
	}
	if ok, err := s.Settle(ctx, "crashed", core.Evidence{}); ok || err == nil {
		t.Fatalf("unconfirmed usage was settled: ok=%v err=%v", ok, err)
	}
	balances(t, ctx, s, 10, 0)
	ok, err := s.ResolvePending(ctx, "crashed", 6, "lab-operator", "provider statement verified")
	if err != nil || !ok {
		t.Fatalf("manual resolution: ok=%v err=%v", ok, err)
	}
	balances(t, ctx, s, 0, 6)
	var kind string
	var actual sql.NullInt64
	err = s.DB.QueryRowContext(ctx, `
		SELECT g.kind, a.actual FROM gateway_audit g JOIN gateway_attempts a ON a.id=g.attempt_id
		WHERE g.attempt_id='crashed' AND g.kind='manual_resolution'`).Scan(&kind, &actual)
	if err != nil || kind != "manual_resolution" || !actual.Valid || actual.Int64 != 6 {
		t.Fatalf("missing manual event: %s %v %v", kind, actual, err)
	}
}

func TestPGAuditDetectsTamperingAndWrongKey(t *testing.T) {
	ctx, s := fixture(t, 10)
	reserve(t, ctx, s, attempt("a", 10))
	pub := s.key.Public().(ed25519.PublicKey)
	if err := s.VerifyAudit(ctx, pub); err != nil {
		t.Fatal(err)
	}
	wrong, wrongKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.VerifyAudit(ctx, wrong); err == nil {
		t.Fatal("wrong trusted key verified")
	}
	restart := &Store{DB: s.DB, key: wrongKey}
	if err := restart.Migrate(ctx); err == nil {
		t.Fatal("wrong signing key admitted on restart")
	}
	if _, err := s.DB.ExecContext(ctx, `UPDATE gateway_audit SET payload = convert_to('{}', 'utf8') WHERE seq=1`); err != nil {
		t.Fatal(err)
	}
	if err := s.VerifyAudit(ctx, pub); err == nil {
		t.Fatal("tampered audit chain verified")
	}
}

func TestPGDuplicateReservationCannotAddSecondHold(t *testing.T) {
	ctx, s := fixture(t, 30)
	a := attempt("same", 10)
	reserve(t, ctx, s, a)
	if ok, err := s.Reserve(ctx, a); ok || !errors.Is(err, ErrDuplicateAttempt) {
		t.Fatalf("duplicate reserve: ok=%v err=%v", ok, err)
	}
	balances(t, ctx, s, 10, 0)
	if err := s.SeedBudget(ctx, "client", 999); err != nil {
		t.Fatal(err)
	}
	var limit int64
	if err := s.DB.QueryRowContext(ctx, `SELECT limit_amount FROM gateway_budgets WHERE client_id='client'`).Scan(&limit); err != nil || limit != 30 {
		t.Fatalf("seed reset active budget: %d %v", limit, err)
	}
}
