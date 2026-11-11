// Package reconcile runs bounded usage recovery passes and coordinates replicas.
package reconcile

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"time"
)

var (
	ErrUnlock      = errors.New("reconciliation advisory unlock failed")
	ErrLockSession = errors.New("reconciliation advisory session probe failed")
)

// Locked reserves a PostgreSQL session for a scan. Accounting runs separately;
// conditional settlement still guards writes when scans overlap.
type Locked struct {
	DB             *sql.DB
	Key            int64
	AcquireTimeout time.Duration
	CleanupTimeout time.Duration
	ProbeInterval  time.Duration
	ProbeTimeout   time.Duration
	Scan           func(context.Context) error
	// BeforeUnlock is an observation/fault hook. It runs on the caller after the
	// monitor joins, under the bounded cleanup context.
	BeforeUnlock func(context.Context, int) error
}

func (w Locked) RunOnce(ctx context.Context) (ran bool, err error) {
	if w.DB == nil || w.Scan == nil || w.AcquireTimeout <= 0 || w.CleanupTimeout <= 0 ||
		w.ProbeInterval <= 0 || w.ProbeTimeout <= 0 {
		return false, errors.New("incomplete advisory worker configuration")
	}
	acquireCtx, cancel := context.WithTimeout(ctx, w.AcquireTimeout)
	conn, err := w.DB.Conn(acquireCtx)
	if err != nil {
		cancel()
		return false, err
	}
	var acquired bool
	var pid int
	err = conn.QueryRowContext(acquireCtx, `
		SELECT pg_backend_pid(), pg_try_advisory_lock($1::bigint)`, w.Key).Scan(&pid, &acquired)
	cancel()
	if err != nil {
		discard(conn)
		return false, err
	}
	if !acquired {
		return false, conn.Close()
	}
	defer func() {
		err = errors.Join(err, w.release(conn, pid))
	}()
	scanCtx, cancelScan := context.WithCancelCause(ctx)
	defer cancelScan(nil)
	stop := make(chan struct{})
	watchDone := make(chan error, 1)
	go func() {
		watchDone <- w.watch(scanCtx, stop, conn, cancelScan)
	}()
	defer func() {
		close(stop)
		err = errors.Join(err, <-watchDone)
	}()
	return true, w.Scan(scanCtx)
}

func (w Locked) watch(ctx context.Context, stop <-chan struct{}, conn *sql.Conn, cancelScan context.CancelCauseFunc) error {
	ticker := time.NewTicker(w.ProbeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-stop:
			return nil
		case <-ticker.C:
			probeCtx, cancel := context.WithTimeout(ctx, w.ProbeTimeout)
			var alive int
			err := conn.QueryRowContext(probeCtx, `SELECT 1`).Scan(&alive)
			cancel()
			if err == nil {
				continue
			}
			if ctx.Err() != nil {
				return nil
			}
			err = fmt.Errorf("%w: %w", ErrLockSession, err)
			cancelScan(err)
			return err
		}
	}
}

func (w Locked) release(conn *sql.Conn, pid int) error {
	ctx, cancel := context.WithTimeout(context.Background(), w.CleanupTimeout)
	defer cancel()
	if w.BeforeUnlock != nil {
		if err := w.BeforeUnlock(ctx, pid); err != nil {
			discard(conn)
			return fmt.Errorf("%w: %w", ErrUnlock, err)
		}
	}
	var unlocked bool
	err := conn.QueryRowContext(ctx, `SELECT pg_advisory_unlock($1::bigint)`, w.Key).Scan(&unlocked)
	if err != nil {
		discard(conn)
		return fmt.Errorf("%w: %w", ErrUnlock, err)
	}
	if !unlocked {
		discard(conn)
		return fmt.Errorf("%w: session did not own the lock", ErrUnlock)
	}
	return conn.Close()
}

func discard(conn *sql.Conn) {
	_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	_ = conn.Close()
}
