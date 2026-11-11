package ledger

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"example.com/llmgateway/internal/core"
)

func (s *Store) ListDue(ctx context.Context, limit int) ([]core.Pending, error) {
	if limit < 1 {
		return nil, errors.New("positive scan batch size required")
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT attempt, provider_request_id FROM gateway_attempts
		WHERE state = 'uncertain' AND next_check_at <= clock_timestamp()
		ORDER BY next_check_at, id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pending []core.Pending
	for rows.Next() {
		var raw []byte
		var p core.Pending
		if err := rows.Scan(&raw, &p.ProviderRequestID); err != nil {
			return nil, err
		}
		p.Attempt, err = parseAttempt(raw)
		if err != nil {
			return nil, err
		}
		pending = append(pending, p)
	}
	return pending, rows.Err()
}

func (s *Store) DeferCheck(ctx context.Context, id string, delay time.Duration) error {
	if delay < time.Microsecond {
		return errors.New("positive reconcile delay required")
	}
	_, err := s.DB.ExecContext(ctx, `
		UPDATE gateway_attempts SET next_check_at = GREATEST(next_check_at,
			clock_timestamp() + ($2::bigint * interval '1 microsecond'))
		WHERE id = $1 AND state = 'uncertain'`, id, delay.Microseconds())
	return err
}

// RecoverStale moves potentially dispatched calls to uncertain. A reserved row
// still has no dispatch marker, so a successful conditional release rules out
// subsequent dispatch and can safely restore the hold to the budget.
func (s *Store) RecoverStale(ctx context.Context, age time.Duration, limit int) error {
	if age < time.Microsecond || limit < 1 {
		return errors.New("positive stale age and batch size required")
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT id FROM gateway_attempts WHERE state IN ('reserved', 'dispatching')
			AND updated_at <= clock_timestamp() - ($1::bigint * interval '1 microsecond')
		ORDER BY updated_at, id LIMIT $2`, age.Microseconds(), limit)
	if err != nil {
		return err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, id := range ids {
		if err := s.recoverOne(ctx, id, age); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) recoverOne(ctx context.Context, id string, age time.Duration) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	client, err := lockAccount(ctx, tx, id)
	if err != nil {
		return err
	}
	var raw []byte
	var state string
	var estimate int64
	err = tx.QueryRowContext(ctx, `
		UPDATE gateway_attempts SET state = CASE WHEN state = 'reserved' THEN 'released' ELSE 'uncertain' END,
			updated_at = clock_timestamp(), next_check_at = clock_timestamp()
		WHERE id = $1 AND state IN ('reserved', 'dispatching')
			AND updated_at <= clock_timestamp() - ($2::bigint * interval '1 microsecond')
		RETURNING attempt, state, estimate`, id, age.Microseconds()).Scan(&raw, &state, &estimate)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	a, err := parseAttempt(raw)
	if err != nil {
		return err
	}
	kind, reason := "uncertain", "stale_dispatch"
	if state == "released" {
		res, err := tx.ExecContext(ctx, `
			UPDATE gateway_budgets SET held = held - $2 WHERE client_id = $1 AND held >= $2`, client, estimate)
		if err := requireOne(res, err); err != nil {
			return err
		}
		kind, reason = "released", "never_dispatched"
	}
	if err := s.appendAudit(ctx, tx, attemptEvent(kind, a, map[string]any{"reason": reason})); err != nil {
		return err
	}
	return tx.Commit()
}
