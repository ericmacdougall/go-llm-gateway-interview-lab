package device

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"example.com/llmgateway/internal/core"
)

func TestDeviceContinuity(t *testing.T) {
	var managed, direct, forbidden atomic.Int64
	outside := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { forbidden.Add(1) }))
	defer outside.Close()
	local := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		direct.Add(1)
		http.Redirect(w, r, outside.URL, http.StatusTemporaryRedirect)
	}))
	defer local.Close()
	gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		managed.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer gateway.Close()
	cfg := Config{Client: "client", Role: "incident", GatewayURL: gateway.URL, AllowedRegions: []string{"inside"}, Approved: []Endpoint{{"model", local.URL, "inside"}}, MaxGrant: time.Minute}
	unsafe := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	if _, err := New(cfg, unsafe, nil, func(context.Context, core.Event) error { return nil }); err == nil {
		t.Fatal("accepted transport that disables TLS verification")
	}
	var events []core.Event
	denyTrace := false
	d, err := New(cfg, gateway.Client().Transport.(*http.Transport), local.Client().Transport.(*http.Transport), func(_ context.Context, e core.Event) error {
		if denyTrace {
			return errors.New("trace offline")
		}
		events = append(events, e)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	admin := core.Principal{Client: "client", Workload: "device", Role: "incident"}
	viewer := core.Principal{Client: "client", Workload: "other", Role: "viewer"}
	if r, err := d.Send(ctx, admin, true, "", nil); err != nil || r.Status != 503 || managed.Load() != 1 || direct.Load() != 0 {
		t.Fatalf("normal route must not auto-bypass: %#v %v", r, err)
	}
	if _, err := d.Send(ctx, admin, false, "", nil); !errors.Is(err, ErrBlocked) {
		t.Fatal("outage should fail closed", err)
	}
	if _, err := d.Activate(ctx, viewer, "model", time.Second); !errors.Is(err, ErrBlocked) {
		t.Fatal("wrong role", err)
	}
	if _, err := d.Activate(ctx, admin, "unapproved", time.Second); !errors.Is(err, ErrBlocked) {
		t.Fatal("unapproved endpoint", err)
	}
	if _, err := d.Activate(ctx, admin, "model", time.Hour); !errors.Is(err, ErrBlocked) {
		t.Fatal("unbounded duration", err)
	}
	denyTrace = true
	if _, err := d.Activate(ctx, admin, "model", time.Minute); err == nil {
		t.Fatal("grant issued without trace")
	}
	denyTrace = false
	grant, err := d.Activate(ctx, admin, "model", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	other := admin
	other.Workload = "other"
	if _, err := d.Send(ctx, other, false, grant.ID, nil); !errors.Is(err, ErrBlocked) {
		t.Fatal("grant escaped workload", err)
	}
	denyTrace = true
	if _, err := d.Send(ctx, admin, false, grant.ID, nil); err == nil || direct.Load() != 0 {
		t.Fatal("untraced outbound attempt", err)
	}
	denyTrace = false
	if r, err := d.Send(ctx, admin, false, grant.ID, nil); err != nil || r.Status != 307 || direct.Load() != 1 || forbidden.Load() != 0 {
		t.Fatalf("direct result=%#v err=%v outside=%d", r, err, forbidden.Load())
	}
	if len(events) != 2 || events[0].Kind != "direct_grant_activated" || events[1].Kind != "direct_grant_used" || events[1].Detail["gateway_accounting"] != false {
		t.Fatalf("wrong trace: %+v", events)
	}
	d.now = func() time.Time { return grant.ExpiresAt }
	if _, err := d.Send(ctx, admin, false, grant.ID, nil); !errors.Is(err, ErrBlocked) || direct.Load() != 1 {
		t.Fatal("expired grant", err)
	}
	cfg.Approved[0].Region = "outside"
	if _, err := New(cfg, nil, nil, func(context.Context, core.Event) error { return nil }); err == nil {
		t.Fatal("unapproved region")
	}
}
