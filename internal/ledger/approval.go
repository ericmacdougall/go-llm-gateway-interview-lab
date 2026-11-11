package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"example.com/llmgateway/internal/core"
)

var (
	ErrApprovalForbidden = errors.New("approval action forbidden")
	ErrApprovalNotFound  = errors.New("approval not found")
	ErrApprovalInvalid   = errors.New("invalid approval")
	ErrApprovalConflict  = errors.New("approval already resolved or scope differs")
)

// Approval is an explicit, expiring permit for one workload and configured model.
// Expired is an effective read state. The original decision remains in storage.
type Approval struct {
	ID          string    `json:"id"`
	Client      string    `json:"client"`
	Workload    string    `json:"workload"`
	Model       string    `json:"model"`
	State       string    `json:"state"`
	ExpiresAt   time.Time `json:"expires_at"`
	RequestedAt time.Time `json:"requested_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	DecidedBy   string    `json:"decided_by,omitempty"`
	Reason      string    `json:"reason,omitempty"`
}

type approvalScanner interface {
	Scan(...any) error
}

const approvalColumns = `id, client_id, workload, model,
	CASE WHEN state IN ('pending', 'approved') AND expires_at <= clock_timestamp()
	THEN 'expired' ELSE state END,
	expires_at, requested_at, updated_at, decided_by, reason`

func scanApproval(row approvalScanner) (Approval, error) {
	var a Approval
	err := row.Scan(&a.ID, &a.Client, &a.Workload, &a.Model, &a.State, &a.ExpiresAt,
		&a.RequestedAt, &a.UpdatedAt, &a.DecidedBy, &a.Reason)
	if errors.Is(err, sql.ErrNoRows) {
		return Approval{}, ErrApprovalNotFound
	}
	return a, err
}

// RequestApproval preserves the expiry of an idempotent repeated request.
func (s *Store) RequestApproval(ctx context.Context, p core.Principal, id, model string, ttl time.Duration) (Approval, error) {
	if p.Client == "" || p.Workload == "" || len(id) < 1 || len(id) > 128 ||
		len(model) < 1 || len(model) > 256 || ttl < time.Second || ttl%time.Second != 0 {
		return Approval{}, ErrApprovalInvalid
	}
	principalJSON, err := json.Marshal(p)
	if err != nil {
		return Approval{}, err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return Approval{}, err
	}
	defer tx.Rollback()
	a, err := scanApproval(tx.QueryRowContext(ctx, `
		INSERT INTO gateway_approvals (client_id, id, workload, model, requester, state, ttl_seconds, expires_at)
		VALUES ($1, $2, $3, $4, $5, 'pending', $6, clock_timestamp() + $6::bigint * interval '1 second')
		ON CONFLICT (client_id, id) DO NOTHING RETURNING `+approvalColumns,
		p.Client, id, p.Workload, model, principalJSON, int64(ttl/time.Second)))
	if errors.Is(err, ErrApprovalNotFound) {
		a, err = scanApproval(tx.QueryRowContext(ctx, `
			SELECT `+approvalColumns+` FROM gateway_approvals WHERE client_id = $1 AND id = $2 FOR UPDATE`, p.Client, id))
		if err != nil {
			return Approval{}, err
		}
		var seconds int64
		if err := tx.QueryRowContext(ctx, `SELECT ttl_seconds FROM gateway_approvals WHERE client_id = $1 AND id = $2`,
			p.Client, id).Scan(&seconds); err != nil {
			return Approval{}, err
		}
		if a.Workload != p.Workload || a.Model != model || seconds != int64(ttl/time.Second) {
			return Approval{}, ErrApprovalConflict
		}
		return a, tx.Commit()
	}
	if err != nil {
		return Approval{}, err
	}
	if err := s.appendAudit(ctx, tx, approvalEvent("approval_requested", p, a)); err != nil {
		return Approval{}, err
	}
	return a, tx.Commit()
}

func (s *Store) GetApproval(ctx context.Context, client, id string) (Approval, error) {
	return scanApproval(s.DB.QueryRowContext(ctx, `
		SELECT `+approvalColumns+` FROM gateway_approvals WHERE client_id = $1 AND id = $2`, client, id))
}

func (s *Store) MatchingApproval(ctx context.Context, p core.Principal, model string) (Approval, error) {
	return scanApproval(s.DB.QueryRowContext(ctx, `
		SELECT `+approvalColumns+` FROM gateway_approvals
		WHERE client_id = $1 AND workload = $2 AND model = $3
			AND state = 'approved' AND expires_at > clock_timestamp()
		ORDER BY expires_at DESC, id LIMIT 1`, p.Client, p.Workload, model))
}

func (s *Store) DecideApproval(ctx context.Context, p core.Principal, id string, allow bool, reason, requiredRole string) (Approval, error) {
	state := "denied"
	if allow {
		state = "approved"
	}
	return s.changeApproval(ctx, p, id, "pending", state, reason, requiredRole)
}

func (s *Store) RevokeApproval(ctx context.Context, p core.Principal, id, reason, requiredRole string) (Approval, error) {
	return s.changeApproval(ctx, p, id, "approved", "revoked", reason, requiredRole)
}

func (s *Store) changeApproval(ctx context.Context, p core.Principal, id, from, to, reason, requiredRole string) (Approval, error) {
	if requiredRole == "" || p.Role != requiredRole || p.Client == "" || p.Workload == "" {
		return Approval{}, ErrApprovalForbidden
	}
	if strings.TrimSpace(reason) == "" || len(reason) > 2048 {
		return Approval{}, ErrApprovalInvalid
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return Approval{}, err
	}
	defer tx.Rollback()
	a, err := scanApproval(tx.QueryRowContext(ctx, `
		SELECT `+approvalColumns+` FROM gateway_approvals WHERE client_id = $1 AND id = $2 FOR UPDATE`, p.Client, id))
	if err != nil {
		return Approval{}, err
	}
	if a.Workload == p.Workload {
		return Approval{}, ErrApprovalForbidden
	}
	// Identical decision retries preserve original timestamps and audit event.
	if a.State == to && a.DecidedBy == p.Workload && a.Reason == reason {
		return a, tx.Commit()
	}
	if a.State != from {
		return Approval{}, ErrApprovalConflict
	}
	a, err = scanApproval(tx.QueryRowContext(ctx, `
		UPDATE gateway_approvals SET state = $3, decided_by = $4, reason = $5, updated_at = clock_timestamp()
		WHERE client_id = $1 AND id = $2 AND state = $6 AND expires_at > clock_timestamp()
		RETURNING `+approvalColumns, p.Client, id, to, p.Workload, reason, from))
	if errors.Is(err, ErrApprovalNotFound) {
		return Approval{}, ErrApprovalConflict
	}
	if err != nil {
		return Approval{}, err
	}
	if err := s.appendAudit(ctx, tx, approvalEvent("approval_"+to, p, a)); err != nil {
		return Approval{}, err
	}
	return a, tx.Commit()
}

func approvalEvent(kind string, actor core.Principal, a Approval) core.Event {
	return core.Event{
		Kind: kind, Principal: actor,
		Detail: map[string]any{
			"approval_id": a.ID, "client": a.Client, "workload": a.Workload,
			"model": a.Model, "state": a.State, "expires_at": a.ExpiresAt,
			"reason": a.Reason, "decided_by": a.DecidedBy,
		},
	}
}
