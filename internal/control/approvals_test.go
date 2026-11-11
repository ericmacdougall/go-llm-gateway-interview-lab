package control

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"example.com/llmgateway/internal/approval"
	"example.com/llmgateway/internal/core"
	"example.com/llmgateway/internal/ledger"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

type fixedAuth struct{ p core.Principal }

func (a fixedAuth) Principal(*http.Request) (core.Principal, bool) { return a.p, a.p.Workload != "" }

func TestApprovalHTTPFlow(t *testing.T) {
	dsn := os.Getenv("GATEWAY_TEST_DSN")
	if dsn == "" {
		t.Skip("GATEWAY_TEST_DSN required")
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal("invalid disposable database configuration")
	}
	if !strings.HasPrefix(cfg.Host, "/") && cfg.Host != "127.0.0.1" && cfg.Host != "localhost" {
		t.Fatal("local database required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	db := stdlib.OpenDB(*cfg)
	defer db.Close()
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	schema := "control_" + hex.EncodeToString(random[:])
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := db.ExecContext(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(context.Background(), "DROP SCHEMA "+quoted+" CASCADE")
	cfg.RuntimeParams["search_path"] = schema
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	store, err := ledger.Open(ctx, cfg.ConnString(), key)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.SeedBudget(ctx, "client", 100); err != nil {
		t.Fatal(err)
	}
	service, err := approval.New(store, approval.Config{ApproverRole: "approver", Models: map[string][]string{"client": {"model"}}, MaxTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	requester := core.Principal{Client: "client", Workload: "requester", Role: "user"}
	approver := core.Principal{Client: "client", Workload: "approver", Role: "approver"}
	perform := func(p core.Principal, method, path, body string) int {
		t.Helper()
		h := Handler{Service: service, Auth: fixedAuth{p}, Next: http.NotFoundHandler()}
		request := httptest.NewRequest(method, path, strings.NewReader(body)).WithContext(ctx)
		response := httptest.NewRecorder()
		h.ServeHTTP(response, request)
		return response.Code
	}
	check := func(got, want int) {
		t.Helper()
		if got != want {
			t.Fatalf("HTTP status=%d want %d", got, want)
		}
	}
	check(perform(core.Principal{}, http.MethodPost, "/v1/approvals", "{}"), 401)
	check(perform(requester, http.MethodPost, "/v1/approvals", `{"id":"grant","model":"model","ttl_seconds":60,"role":"approver"}`), 400)
	check(perform(requester, http.MethodPost, "/v1/approvals", `{"id":"grant","model":"model","ttl_seconds":60}`), 200)
	check(perform(requester, http.MethodPost, "/v1/approvals/grant/decision", `{"allow":true,"reason":"self"}`), 403)
	foreign := approver
	foreign.Client = "foreign"
	check(perform(foreign, http.MethodPost, "/v1/approvals/grant/decision", `{"allow":true,"reason":"cross client"}`), 404)
	check(perform(approver, http.MethodPost, "/v1/approvals/grant/decision", `{"allow":true,"reason":"test access"}`), 200)
	allowed, err := service.Allows(ctx, requester, "model")
	if err != nil || !allowed {
		t.Fatalf("approved permit=%v err=%v", allowed, err)
	}
	check(perform(approver, http.MethodPost, "/v1/approvals/grant/revoke", `{"reason":"test complete"}`), 200)
	allowed, err = service.Allows(ctx, requester, "model")
	if err != nil || allowed {
		t.Fatalf("revoked permit=%v err=%v", allowed, err)
	}
	if err := store.VerifyAudit(ctx, pub); err != nil {
		t.Fatal(err)
	}
}
