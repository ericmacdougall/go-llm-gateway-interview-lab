package reconcile

import (
	"context"
	"errors"
	"time"

	"example.com/llmgateway/internal/core"
	"example.com/llmgateway/internal/ledger"
)

// Worker polls PostgreSQL for usage evidence. It never invokes inference.
type Worker struct {
	Store         *ledger.Store
	Lookup        core.UsageLookup
	Interval      time.Duration
	StaleAfter    time.Duration
	LookupTimeout time.Duration
	DBTimeout     time.Duration
	BatchSize     int
	LockKey       int64
	OnError       func(error)
}

func (w Worker) validate() error {
	if w.Store == nil || w.Lookup == nil || w.Interval < time.Microsecond ||
		w.StaleAfter < time.Microsecond || w.LookupTimeout <= 0 ||
		w.DBTimeout <= 0 || w.BatchSize < 1 {
		return errors.New("incomplete reconciliation worker configuration")
	}
	return nil
}

// Run starts with an immediate pass and then waits a fixed delay after each pass.
func (w Worker) Run(ctx context.Context) error {
	if err := w.validate(); err != nil {
		return err
	}
	for ctx.Err() == nil {
		_, err := w.RunOnce(ctx)
		if err != nil && ctx.Err() == nil && w.OnError != nil {
			w.OnError(err)
		}
		timer := time.NewTimer(w.Interval)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
	return ctx.Err()
}

func (w Worker) RunOnce(ctx context.Context) (bool, error) {
	if err := w.validate(); err != nil {
		return false, err
	}
	run := Locked{
		DB: w.Store.DB, Key: w.LockKey, Scan: w.scan,
		AcquireTimeout: w.DBTimeout, CleanupTimeout: w.DBTimeout,
		ProbeInterval: time.Second, ProbeTimeout: w.DBTimeout,
	}
	return run.RunOnce(ctx)
}

func (w Worker) scan(ctx context.Context) error {
	dbCtx, cancel := context.WithTimeout(ctx, w.DBTimeout)
	err := w.Store.RecoverStale(dbCtx, w.StaleAfter, w.BatchSize)
	cancel()
	if err != nil {
		return err
	}
	dbCtx, cancel = context.WithTimeout(ctx, w.DBTimeout)
	pending, err := w.Store.ListDue(dbCtx, w.BatchSize)
	cancel()
	if err != nil {
		return err
	}
	var failures error
	for _, p := range pending {
		if err := ctx.Err(); err != nil {
			return errors.Join(failures, err)
		}
		failures = errors.Join(failures, w.check(ctx, p))
	}
	return failures
}

func (w Worker) check(ctx context.Context, p core.Pending) error {
	lookupCtx, cancel := context.WithTimeout(ctx, w.LookupTimeout)
	e, err := w.Lookup.Lookup(lookupCtx, p)
	cancel()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	dbCtx, cancel := context.WithTimeout(ctx, w.DBTimeout)
	defer cancel()
	if err != nil || !e.Final {
		return errors.Join(err, w.Store.DeferCheck(dbCtx, p.Attempt.ID, w.Interval))
	}
	_, err = w.Store.Settle(dbCtx, p.Attempt.ID, e)
	if err != nil && dbCtx.Err() == nil {
		err = errors.Join(err, w.Store.DeferCheck(dbCtx, p.Attempt.ID, w.Interval))
	}
	return err
}
