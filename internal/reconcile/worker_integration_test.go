package reconcile

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"os"
	"strings"
	"testing"
	"time"

	"example.com/llmgateway/internal/core"
	"example.com/llmgateway/internal/ledger"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

type usageFunc func(context.Context, core.Pending) (core.Evidence, error)

func (f usageFunc) Lookup(ctx context.Context, p core.Pending) (core.Evidence, error) {
	return f(ctx, p)
}

func TestPGWorkerRetainsUnknownThenSettles(t *testing.T) {
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
	defer cancel()
	admin := stdlib.OpenDB(*cfg)
	defer admin.Close()
	var tag [8]byte
	if _, err := rand.Read(tag[:]); err != nil {
		t.Fatal(err)
	}
	name := "reconcile_test_" + hex.EncodeToString(tag[:])
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := admin.ExecContext(cleanupCtx, "DROP SCHEMA "+pgx.Identifier{name}.Sanitize()+" CASCADE"); err != nil {
			t.Error(err)
		}
	}()
	cfg.RuntimeParams["search_path"] = name
	registered := stdlib.RegisterConnConfig(cfg)
	defer stdlib.UnregisterConnConfig(registered)
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	store, err := ledger.Open(ctx, registered, key)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.SeedBudget(ctx, "client", 10); err != nil {
		t.Fatal(err)
	}
	a := core.Attempt{ID: "attempt", RequestID: "request", Estimate: 10, Principal: core.Principal{Client: "client"}}
	ok, err := store.Reserve(ctx, a)
	if err != nil || !ok {
		t.Fatalf("reserve: %v %v", ok, err)
	}
	if err := store.MarkDispatch(ctx, a.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkUncertain(ctx, a.ID, core.Evidence{ProviderRequestID: "provider-1", Outcome: "timeout"}); err != nil {
		t.Fatal(err)
	}
	queries := 0
	worker := Worker{
		Store: store,
		Lookup: usageFunc(func(ctx context.Context, p core.Pending) (core.Evidence, error) {
			queries++
			if p.Attempt.ID != a.ID || p.ProviderRequestID != "provider-1" {
				t.Errorf("lookup not scoped to recorded attempt: %+v", p)
			}
			if queries == 1 {
				return core.Evidence{}, nil
			}
			return core.Evidence{Final: true, Actual: 7, Ref: "usage", ProviderRequestID: "provider-1"}, nil
		}),
		Interval: time.Minute, StaleAfter: time.Hour, LookupTimeout: time.Second, DBTimeout: 3 * time.Second,
		BatchSize: 10, LockKey: 73218165,
	}
	assertState := func(state string, held, spent int64, settlements int) {
		t.Helper()
		var gotState string
		var gotHeld, gotSpent int64
		var count int
		err := store.DB.QueryRowContext(ctx, `
			SELECT state, held, spent,
				(SELECT count(*) FROM gateway_audit WHERE kind = 'settled' AND attempt_id = $1)
			FROM gateway_attempts JOIN gateway_budgets USING (client_id) WHERE id = $1`, a.ID).
			Scan(&gotState, &gotHeld, &gotSpent, &count)
		if err != nil || gotState != state || gotHeld != held || gotSpent != spent || count != settlements {
			t.Fatalf("state=%s held=%d spent=%d settlements=%d err=%v", gotState, gotHeld, gotSpent, count, err)
		}
	}
	run := func() {
		t.Helper()
		ran, err := worker.RunOnce(ctx)
		if !ran || err != nil {
			t.Fatalf("reconcile pass: ran=%v err=%v", ran, err)
		}
	}
	run()
	assertState("uncertain", 10, 0, 0)
	run()
	if queries != 1 {
		t.Fatalf("nonfinal result did not defer next check: %d lookups", queries)
	}
	if _, err := store.DB.ExecContext(ctx, `UPDATE gateway_attempts SET next_check_at = clock_timestamp() WHERE id = $1`, a.ID); err != nil {
		t.Fatal(err)
	}
	run()
	assertState("settled", 0, 7, 1)
	run()
	if queries != 2 {
		t.Fatalf("settled row was looked up again: %d lookups", queries)
	}
}
