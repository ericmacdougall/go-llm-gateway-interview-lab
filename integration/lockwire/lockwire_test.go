package lockwire

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"example.com/llmgateway/internal/reconcile"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

type fixture struct {
	cfg      *pgx.ConnConfig
	observer *sql.DB
	key      int64
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	dsn := os.Getenv("GATEWAY_TEST_DSN")
	if dsn == "" {
		t.Skip("GATEWAY_TEST_DSN is required for local PostgreSQL integration")
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil || !filepath.IsAbs(cfg.Host) {
		t.Fatal("configure GATEWAY_TEST_DSN with an isolated local PostgreSQL socket")
	}
	cfg.Fallbacks = nil
	var key [8]byte
	if _, err := rand.Read(key[:]); err != nil {
		t.Fatal(err)
	}
	return fixture{cfg: cfg, observer: openPool(t, cfg), key: int64(binary.BigEndian.Uint64(key[:]))}
}

func openPool(t *testing.T, cfg *pgx.ConnConfig) *sql.DB {
	t.Helper()
	db := stdlib.OpenDB(*cfg)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxIdleTime(0)
	db.SetConnMaxLifetime(0)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

type sessionState struct {
	alive bool
	held  bool
}

func (f fixture) observe(ctx context.Context, pid int) (sessionState, error) {
	var state sessionState
	err := f.observer.QueryRowContext(ctx, `
		SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid=$1),
		       EXISTS (SELECT 1 FROM pg_locks
		               WHERE pid=$1 AND locktype='advisory' AND granted)
	`, pid).Scan(&state.alive, &state.held)
	return state, err
}

func (f fixture) awaitSession(t *testing.T, ctx context.Context, pid int, want sessionState) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		got, err := f.observe(ctx, pid)
		if err != nil {
			t.Fatalf("observe PostgreSQL session: %v", err)
		}
		if got == want {
			return
		}
		if !got.alive && want.alive {
			t.Fatalf("backend %d closed before server-side unlock could be observed", pid)
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("backend %d state = %+v, want %+v: %v", pid, got, want, ctx.Err())
		}
	}
}

func (f fixture) worker(db *sql.DB) reconcile.Locked {
	return reconcile.Locked{
		DB: db, Key: f.key,
		AcquireTimeout: 3 * time.Second,
		CleanupTimeout: 3 * time.Second,
		ProbeInterval:  time.Hour, // The short test scan has no concurrent probe.
		ProbeTimeout:   time.Second,
		Scan:           func(context.Context) error { return nil },
	}
}

func (f fixture) assertRetiredAndTakeover(t *testing.T, ctx context.Context, poolA *sql.DB, oldPID int) {
	t.Helper()

	// A pool probe may force replacement of a broken connection. Observe
	// physical disposal before issuing any further requests through A's pool.
	f.awaitSession(t, ctx, oldPID, sessionState{})

	var newPID int
	if err := poolA.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&newPID); err != nil {
		t.Fatalf("query replacement lock session: %v", err)
	}
	if newPID == oldPID {
		t.Fatalf("lock pool reused backend %d after unlock error", oldPID)
	}

	poolB := openPool(t, f.cfg)
	b := f.worker(poolB)
	scans := 0
	b.Scan = func(context.Context) error { scans++; return nil }
	b.BeforeUnlock = func(ctx context.Context, pid int) error {
		if pid == oldPID || pid == newPID {
			return fmt.Errorf("replica B reused a session from A")
		}
		state, err := f.observe(ctx, pid)
		if err != nil {
			return err
		}
		if state != (sessionState{alive: true, held: true}) {
			return fmt.Errorf("replica B did not hold its advisory lock: %+v", state)
		}
		return nil
	}
	ran, err := b.RunOnce(ctx)
	if err != nil || !ran || scans != 1 {
		t.Fatalf("replica B takeover: ran=%v scans=%d err=%v", ran, scans, err)
	}
}

func receive[T any](t *testing.T, ctx context.Context, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-ctx.Done():
		t.Fatalf("waiting for worker barrier: %v", ctx.Err())
		var zero T
		return zero
	}
}

func TestLostUnlockReplyRetiresSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	f := newFixture(t)
	proxy := startFaultProxy(t, filepath.Join(f.cfg.Host, ".s.PGSQL."+strconv.Itoa(int(f.cfg.Port))))
	proxyConfig := f.cfg.Copy()
	proxyConfig.Host = proxy.address.IP.String()
	proxyConfig.Port = uint16(proxy.address.Port)
	proxyConfig.TLSConfig = nil
	// Execute in one extended-protocol round trip. With the default
	// statement cache, the gate could hide the separate prepare reply
	// before the driver has sent the actual unlock.
	proxyConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	poolA := openPool(t, proxyConfig)

	atUnlock := make(chan int, 1)
	proceed := make(chan struct{})
	a := f.worker(poolA)
	a.BeforeUnlock = func(cleanupCtx context.Context, pid int) error {
		atUnlock <- pid
		select {
		case <-proceed:
			return nil
		case <-cleanupCtx.Done():
			return cleanupCtx.Err()
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	done := make(chan struct{})
	var ran bool
	var runErr error
	go func() {
		defer close(done)
		ran, runErr = a.RunOnce(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("worker did not exit after test cancellation")
		}
	})

	oldPID := receive(t, ctx, atUnlock)
	state, err := f.observe(ctx, oldPID)
	if err != nil || state != (sessionState{alive: true, held: true}) {
		t.Fatalf("before unlock: state=%+v err=%v", state, err)
	}
	close(proxy.armed)
	close(proceed)
	receive(t, ctx, proxy.withheld)
	f.awaitSession(t, ctx, oldPID, sessionState{alive: true})

	receive(t, ctx, done)
	if !ran || !errors.Is(runErr, reconcile.ErrUnlock) {
		t.Fatalf("expected completed scan with unlock error: ran=%v err=%v", ran, runErr)
	}
	f.assertRetiredAndTakeover(t, ctx, poolA, oldPID)
}

func TestUnlockHookErrorDiscardsLiveLockSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	f := newFixture(t)
	poolA := openPool(t, f.cfg)
	a := f.worker(poolA)

	injected := errors.New("fixture: unlock not sent")
	var oldPID int
	a.BeforeUnlock = func(ctx context.Context, pid int) error {
		oldPID = pid
		state, err := f.observe(ctx, pid)
		if err != nil {
			return err
		}
		if state != (sessionState{alive: true, held: true}) {
			return fmt.Errorf("synthetic fault precondition: %+v", state)
		}
		// The driver never sees a failed query. Only the worker's cleanup
		// path can decide to retire this otherwise healthy session.
		return injected
	}
	ran, err := a.RunOnce(ctx)
	if !ran || !errors.Is(err, reconcile.ErrUnlock) || !errors.Is(err, injected) {
		t.Fatalf("expected explicit cleanup failure: ran=%v err=%v", ran, err)
	}
	f.assertRetiredAndTakeover(t, ctx, poolA, oldPID)
}

func TestLockBackendLossCancelsActiveScan(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	f := newFixture(t)
	poolA := openPool(t, f.cfg)
	a := f.worker(poolA)
	a.ProbeInterval = 25 * time.Millisecond

	enteredScan := make(chan struct{})
	a.Scan = func(ctx context.Context) error {
		close(enteredScan)
		<-ctx.Done()
		return context.Cause(ctx)
	}
	done := make(chan struct{})
	var ran bool
	var runErr error
	go func() {
		defer close(done)
		ran, runErr = a.RunOnce(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("worker did not exit after test cancellation")
		}
	})

	receive(t, ctx, enteredScan)
	// PostgreSQL represents a bigint advisory key in the two OID
	// halves below. Restrict the signal to our fresh fixture key and DB.
	var pid int
	err := f.observer.QueryRowContext(ctx, `
		SELECT pid
		FROM pg_locks
		WHERE locktype='advisory' AND granted AND objsubid=1
		  AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
		  AND ((classid::bigint << 32) | objid::bigint)=$1::bigint
	`, f.key).Scan(&pid)
	if err != nil {
		t.Fatalf("find fixture lock backend: %v", err)
	}

	// Another replica is excluded before A's session is ended.
	b := f.worker(openPool(t, f.cfg))
	b.Scan = func(context.Context) error { return errors.New("overlapping scan ran") }
	if ran, err := b.RunOnce(ctx); ran || err != nil {
		t.Fatalf("expected overlapping replica to skip: ran=%v err=%v", ran, err)
	}

	var terminated bool
	err = f.observer.QueryRowContext(ctx,
		`SELECT pg_terminate_backend($1, 1500)`, pid).Scan(&terminated)
	if err != nil || !terminated {
		t.Fatalf("terminate fixture backend: terminated=%v err=%v", terminated, err)
	}
	receive(t, ctx, done)
	if !ran || !errors.Is(runErr, reconcile.ErrLockSession) || ctx.Err() != nil {
		t.Fatalf("session loss should cancel scan before fixture deadline: ran=%v err=%v", ran, runErr)
	}
	f.assertRetiredAndTakeover(t, ctx, poolA, pid)
}
