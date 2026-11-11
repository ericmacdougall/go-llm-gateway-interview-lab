// Package ledger persists admission holds, attempt state and signed audit records.
package ledger

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"example.com/llmgateway/internal/core"
	_ "github.com/jackc/pgx/v5/stdlib"
)

//go:embed schema.sql
var schema string

var (
	ErrDuplicateAttempt = errors.New("attempt already exists")
	ErrInvalidState     = errors.New("attempt is not eligible for transition")
	ErrConflictingUsage = errors.New("usage conflicts with existing evidence")
	ErrUnknownBudget    = errors.New("budget is not configured")
)

type Store struct {
	DB  *sql.DB
	key ed25519.PrivateKey
}

var _ core.Store = (*Store)(nil)

func Open(ctx context.Context, dsn string, key ed25519.PrivateKey) (*Store, error) {
	if len(key) != ed25519.PrivateKeySize {
		return nil, errors.New("valid Ed25519 signing key required")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{DB: db, key: append(ed25519.PrivateKey(nil), key...)}, nil
}

func (s *Store) Close() error { return s.DB.Close() }

func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Serialize CREATE IF NOT EXISTS for concurrent startups in this database.
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(719184153124)`); err != nil {
		return err
	}
	for _, stmt := range strings.Split(schema, ";") {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO gateway_audit_head (singleton, key_public, seq, digest)
		VALUES (true, $1, 0, $2) ON CONFLICT (singleton) DO NOTHING`,
		[]byte(s.key.Public().(ed25519.PublicKey)), make([]byte, 32)); err != nil {
		return err
	}
	var public []byte
	if err := tx.QueryRowContext(ctx, `SELECT key_public FROM gateway_audit_head WHERE singleton`).Scan(&public); err != nil {
		return err
	}
	if !s.key.Public().(ed25519.PublicKey).Equal(ed25519.PublicKey(public)) {
		return errors.New("audit signing key does not match the configured database chain")
	}
	return tx.Commit()
}

// SeedBudget only inserts a missing client. Existing limits and spend are preserved.
func (s *Store) SeedBudget(ctx context.Context, client string, limit int64) error {
	if client == "" || limit < 0 {
		return errors.New("invalid budget")
	}
	_, err := s.DB.ExecContext(ctx, `
		INSERT INTO gateway_budgets (client_id, limit_amount) VALUES ($1, $2)
		ON CONFLICT (client_id) DO NOTHING`, client, limit)
	return err
}

func (s *Store) Reserve(ctx context.Context, a core.Attempt) (bool, error) {
	if a.ID == "" || a.RequestID == "" || a.Principal.Client == "" || a.Estimate < 0 {
		return false, errors.New("invalid reservation")
	}
	payload, err := json.Marshal(a)
	if err != nil {
		return false, err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	// Numeric comparison avoids overflow if a provider overruns its estimate.
	var client string
	err = tx.QueryRowContext(ctx, `
		UPDATE gateway_budgets SET held = held + $2::bigint
		WHERE client_id = $1 AND limit_amount::numeric - spent - held >= $2::bigint
		RETURNING client_id`, a.Principal.Client, a.Estimate).Scan(&client)
	if errors.Is(err, sql.ErrNoRows) {
		if err := s.appendAudit(ctx, tx, attemptEvent("budget_denied", a, nil)); err != nil {
			return false, err
		}
		return false, tx.Commit()
	}
	if err != nil {
		return false, err
	}
	var id string
	err = tx.QueryRowContext(ctx, `
		INSERT INTO gateway_attempts (id, client_id, request_id, attempt, estimate, state)
		VALUES ($1, $2, $3, $4, $5, 'reserved')
		ON CONFLICT (id) DO NOTHING RETURNING id`,
		a.ID, client, a.RequestID, payload, a.Estimate).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		// Roll back the balance change too. A reused attempt ID cannot dispatch again.
		return false, ErrDuplicateAttempt
	}
	if err != nil {
		return false, err
	}
	if err := s.appendAudit(ctx, tx, attemptEvent("reserved", a, nil)); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) MarkDispatch(ctx context.Context, id string) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var raw []byte
	err = tx.QueryRowContext(ctx, `
		SELECT attempt FROM gateway_attempts
		WHERE id = $1 AND state = 'reserved' FOR UPDATE`, id).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrInvalidState
	}
	if err != nil {
		return err
	}
	a, err := parseAttempt(raw)
	if err != nil {
		return err
	}
	if a.Route.RequiresApproval {
		if a.ApprovalID == "" {
			return ErrApprovalForbidden
		}
		var grantID string
		err := tx.QueryRowContext(ctx, `
			SELECT id FROM gateway_approvals
			WHERE client_id = $1 AND workload = $2 AND model = $3 AND id = $4
				AND state = 'approved' AND expires_at > clock_timestamp()
			FOR SHARE`, a.Principal.Client, a.Principal.Workload, a.Route.Model, a.ApprovalID).Scan(&grantID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrApprovalForbidden
		}
		if err != nil {
			return err
		}
	}
	if err := s.appendAudit(ctx, tx, attemptEvent("dispatching", a, nil)); err != nil {
		return err
	}
	// Recheck time after potentially waiting for the chain head. Holding the
	// approval row through commit serializes revocation with dispatch.
	result, err := tx.ExecContext(ctx, `
		UPDATE gateway_attempts SET state = 'dispatching', updated_at = clock_timestamp()
		WHERE id = $1 AND state = 'reserved' AND (
			NOT $2::boolean OR EXISTS (
				SELECT 1 FROM gateway_approvals WHERE client_id = $3 AND workload = $4
					AND model = $5 AND id = $6 AND state = 'approved'
					AND expires_at > clock_timestamp()
			)
		)`, id, a.Route.RequiresApproval, a.Principal.Client, a.Principal.Workload, a.Route.Model, a.ApprovalID)
	if err := requireOne(result, err); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) MarkUncertain(ctx context.Context, id string, e core.Evidence) error {
	if e.Final {
		return errors.New("final evidence must use settlement")
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var raw []byte
	var requestID string
	err = tx.QueryRowContext(ctx, `
		UPDATE gateway_attempts SET state = 'uncertain', updated_at = clock_timestamp(),
			next_check_at = clock_timestamp(),
			provider_request_id = COALESCE(NULLIF($2, ''), provider_request_id)
		WHERE id = $1 AND state IN ('dispatching', 'uncertain')
			AND ($2 = '' OR provider_request_id IN ('', $2))
		RETURNING attempt, provider_request_id`, id, e.ProviderRequestID).Scan(&raw, &requestID)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrInvalidState
	}
	if err != nil {
		return err
	}
	a, err := parseAttempt(raw)
	if err != nil {
		return err
	}
	e.ProviderRequestID = requestID
	if err := s.appendAudit(ctx, tx, attemptEvent("uncertain", a, evidenceDetail(e))); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) Settle(ctx context.Context, id string, e core.Evidence) (bool, error) {
	return s.settle(ctx, id, e, "", "")
}

// ResolvePending applies a deliberate, audited manual disposition for uncertain usage.
func (s *Store) ResolvePending(ctx context.Context, id string, amount int64, actor, reason string) (bool, error) {
	if strings.TrimSpace(actor) == "" || strings.TrimSpace(reason) == "" {
		return false, errors.New("manual resolution requires an actor and reason")
	}
	return s.settle(ctx, id, core.Evidence{Final: true, Actual: amount, Ref: "manual", Outcome: "manual_resolution"}, actor, reason)
}

func (s *Store) settle(ctx context.Context, id string, e core.Evidence, actor, reason string) (bool, error) {
	if !e.Final || e.Actual < 0 || e.Ref == "" {
		return false, errors.New("settlement requires final usage evidence")
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	client, err := lockAccount(ctx, tx, id)
	if err != nil {
		return false, err
	}
	var raw []byte
	var hold int64
	var requestID string
	err = tx.QueryRowContext(ctx, `
		UPDATE gateway_attempts SET state = 'settled', actual = $2, evidence_ref = $3,
			provider_request_id = COALESCE(NULLIF($4, ''), provider_request_id), updated_at = clock_timestamp()
		WHERE id = $1 AND (state = 'uncertain' OR (state = 'dispatching' AND NOT $5::boolean))
			AND ($4 = '' OR provider_request_id IN ('', $4))
		RETURNING attempt, estimate, provider_request_id`,
		id, e.Actual, e.Ref, e.ProviderRequestID, actor != "").Scan(&raw, &hold, &requestID)
	if errors.Is(err, sql.ErrNoRows) {
		var actual sql.NullInt64
		if err := tx.QueryRowContext(ctx, `SELECT actual, provider_request_id FROM gateway_attempts WHERE id = $1`, id).Scan(&actual, &requestID); err != nil {
			return false, err
		}
		if e.ProviderRequestID != "" && requestID != "" && e.ProviderRequestID != requestID {
			return false, ErrConflictingUsage
		}
		if actual.Valid {
			if actual.Int64 != e.Actual {
				return false, ErrConflictingUsage
			}
			return false, nil
		}
		return false, ErrInvalidState
	}
	if err != nil {
		return false, err
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE gateway_budgets SET held = held - $2, spent = spent + $3
		WHERE client_id = $1 AND held >= $2`, client, hold, e.Actual)
	if err := requireOne(res, err); err != nil {
		return false, err
	}
	a, err := parseAttempt(raw)
	if err != nil {
		return false, err
	}
	e.ProviderRequestID = requestID
	detail := evidenceDetail(e)
	kind := "settled"
	if actor != "" {
		kind = "manual_resolution"
		detail["actor"], detail["reason"] = actor, reason
	}
	if err := s.appendAudit(ctx, tx, attemptEvent(kind, a, detail)); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// Lock order for all budget mutations is budget -> attempt -> audit head.
func lockAccount(ctx context.Context, tx *sql.Tx, id string) (string, error) {
	var client string
	err := tx.QueryRowContext(ctx, `
		SELECT b.client_id FROM gateway_budgets b
		JOIN gateway_attempts a USING (client_id) WHERE a.id = $1 FOR UPDATE OF b`, id).Scan(&client)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrUnknownBudget
	}
	return client, err
}

func requireOne(r sql.Result, err error) error {
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("ledger invariant: expected one row, got %d", n)
	}
	return nil
}

func parseAttempt(raw []byte) (core.Attempt, error) {
	var a core.Attempt
	err := json.Unmarshal(raw, &a)
	return a, err
}

func evidenceDetail(e core.Evidence) map[string]any {
	detail := map[string]any{"provider_request_id": e.ProviderRequestID, "evidence_ref": e.Ref, "outcome": e.Outcome, "final": e.Final}
	if e.Final {
		detail["actual"] = e.Actual
	}
	return detail
}

func attemptEvent(kind string, a core.Attempt, detail map[string]any) core.Event {
	if detail == nil {
		detail = make(map[string]any)
	}
	detail["provider"] = a.Route.Provider
	detail["provider_account"] = a.Route.Account
	detail["model"] = a.Route.Model
	detail["route"] = a.Route.ID
	detail["region"] = a.Route.Region
	detail["policy_version"] = a.PolicyVersion
	detail["policy_rule"] = a.Rule
	detail["estimate"] = a.Estimate
	if a.ApprovalID != "" {
		detail["approval_id"] = a.ApprovalID
	}
	return core.Event{Kind: kind, Principal: a.Principal, RequestID: a.RequestID, AttemptID: a.ID, Detail: detail}
}
