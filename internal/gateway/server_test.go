package gateway

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"log"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"example.com/llmgateway/internal/core"
)

const testWorkload = "spiffe://test.example/service/reporting"
const validBody = `{"messages":[{"role":"user","content":"hi"}],"max_tokens":1}`

type memAttempt struct {
	a     core.Attempt
	state string
	e     core.Evidence
}

type memStore struct {
	mu                     sync.Mutex
	limit, reserved, spent int64
	attempts               []memAttempt
	events                 []core.Event
	dispatchErr            bool
}

func (m *memStore) Reserve(_ context.Context, a core.Attempt) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if a.Estimate > m.limit-m.reserved-m.spent {
		return false, nil
	}
	m.reserved += a.Estimate
	m.attempts = append(m.attempts, memAttempt{a: a, state: "reserved"})
	return true, nil
}

func (m *memStore) MarkDispatch(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.dispatchErr {
		return errors.New("controlled dispatch failure")
	}
	for i := range m.attempts {
		if a := &m.attempts[i]; a.a.ID == id && a.state == "reserved" {
			a.state = "dispatching"
			return nil
		}
	}
	return errors.New("cannot dispatch")
}

func (m *memStore) MarkUncertain(_ context.Context, id string, e core.Evidence) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.attempts {
		if a := &m.attempts[i]; a.a.ID == id && a.state == "dispatching" {
			a.state, a.e = "uncertain", e
			return nil
		}
	}
	return errors.New("cannot retain attempt")
}

func (m *memStore) Settle(_ context.Context, id string, e core.Evidence) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.attempts {
		if a := &m.attempts[i]; a.a.ID == id && a.state == "dispatching" && e.Final {
			a.state, a.e = "settled", e
			m.reserved -= a.a.Estimate
			m.spent += e.Actual
			return true, nil
		}
	}
	return false, errors.New("cannot settle")
}

func (m *memStore) Audit(_ context.Context, e core.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, e)
	return nil
}

func (m *memStore) snapshot() ([]memAttempt, []core.Event, int64, int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]memAttempt(nil), m.attempts...), append([]core.Event(nil), m.events...), m.reserved, m.spent
}

func provider(t *testing.T, mode string) (*Fixture, string) {
	t.Helper()
	f, err := NewFixture(mode)
	if err != nil {
		t.Fatal(err)
	}
	u := httptest.NewServer(f)
	t.Cleanup(u.Close)
	return f, u.URL
}

func route(id, endpoint, environment, region string) core.Route {
	return core.Route{
		ID: id, Provider: id, Account: "test-account", Model: "test-model",
		Region: region, Environment: environment, Endpoint: endpoint,
		InputUnit: 1, OutputUnit: 1, Capabilities: []string{"chat"},
	}
}

func config(routes ...core.Route) Config {
	return Config{
		Identities: map[string]core.Principal{testWorkload: {
			Client: "client-a", Department: "reporting", App: "portal", Role: "operator",
		}},
		Clients: map[string]ClientPolicy{"client-a": {
			Version: "policy-3", Rule: "client-cloud", Expression: `region == "us" && role == "operator"`,
			Environment: "client-cloud", Routes: routes,
		}},
		MaxAttempts: 3, RequestTimeoutMS: 1000, AttemptTimeoutMS: 100, AllowLoopbackHTTP: true,
	}
}

func newServer(t *testing.T, cfg Config, m *memStore) *Server {
	t.Helper()
	s, err := New(cfg, m)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.CloseIdleConnections)
	return s
}

type testCA struct {
	cert *x509.Certificate
	key  ed25519.PrivateKey
}

func newCA(t *testing.T) testCA {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "gateway test CA"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return testCA{cert, key}
}

func (c testCA) workload(t *testing.T, rawURI string) tls.Certificate {
	t.Helper()
	uri, err := url.Parse(rawURI)
	if err != nil {
		t.Fatal(err)
	}
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	leaf := &x509.Certificate{
		SerialNumber: big.NewInt(2), URIs: []*url.URL{uri},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, leaf, c.cert, pub, c.key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func mtlsServer(t *testing.T, s *Server, ca testCA) *httptest.Server {
	t.Helper()
	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)
	server := httptest.NewUnstartedServer(s)
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool, MinVersion: tls.VersionTLS13}
	server.StartTLS()
	t.Cleanup(server.Close)
	return server
}

func mtlsClient(t *testing.T, server *httptest.Server, certs ...tls.Certificate) *http.Client {
	t.Helper()
	transport := server.Client().Transport.(*http.Transport).Clone()
	transport.TLSClientConfig.Certificates = certs
	// Force an untrusted test certificate onto the wire instead of letting
	// the default client omit it after inspecting the server CA list.
	if len(certs) == 1 {
		transport.TLSClientConfig.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return &certs[0], nil
		}
	}
	t.Cleanup(transport.CloseIdleConnections)
	return &http.Client{Transport: transport, Timeout: 3 * time.Second}
}

func call(t *testing.T, c *http.Client, server, body string, headers map[string]string) (int, string, error) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, server+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	return resp.StatusCode, string(data), err
}

func localCall(t *testing.T, s *Server, body string) (int, string) {
	t.Helper()
	ca := newCA(t)
	server := mtlsServer(t, s, ca)
	status, reply, err := call(t, mtlsClient(t, server, ca.workload(t, testWorkload)), server.URL, body, nil)
	if err != nil {
		t.Fatal(err)
	}
	return status, reply
}

func TestMTLSBindsOnlyVerifiedConfiguredSPIFFEIdentity(t *testing.T) {
	f, endpoint := provider(t, outcomeOK)
	db := &memStore{limit: 100}
	s := newServer(t, config(route("primary", endpoint, "client-cloud", "us")), db)
	ca := newCA(t)
	server := mtlsServer(t, s, ca)
	for _, tc := range []struct {
		name    string
		certs   []tls.Certificate
		headers map[string]string
		want    int
	}{
		{name: "no cert", want: 0},
		{name: "untrusted", certs: []tls.Certificate{newCA(t).workload(t, testWorkload)}, want: 0},
		{name: "unknown workload", certs: []tls.Certificate{ca.workload(t, "spiffe://test.example/foreign")}, want: 401},
		{name: "spoofed client", certs: []tls.Certificate{ca.workload(t, testWorkload)}, headers: map[string]string{"X-Tenant-ID": "foreign"}, want: 400},
		{name: "trusted", certs: []tls.Certificate{ca.workload(t, testWorkload)}, want: 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, reply, err := call(t, mtlsClient(t, server, tc.certs...), server.URL, validBody, tc.headers)
			if tc.want == 0 {
				if err == nil {
					t.Fatalf("expected handshake rejection, got %d", got)
				}
			} else if err != nil || got != tc.want {
				t.Fatalf("got %d %s %v, want %d", got, reply, err, tc.want)
			}
		})
	}
	ats, _, held, spent := db.snapshot()
	if len(ats) != 1 || f.Calls() != 1 || ats[0].a.Principal.Client != "client-a" ||
		ats[0].a.Principal.Workload != testWorkload || held != 0 || spent != 3 {
		t.Fatalf("identity/accounting invariant failed: attempts=%d calls=%d held=%d spent=%d", len(ats), f.Calls(), held, spent)
	}
}

func TestPolicyRestrictsFallbackAndAuditsDeniedRequest(t *testing.T) {
	primary, primaryURL := provider(t, outcomeUnavailable)
	external, externalURL := provider(t, outcomeOK)
	foreignRegion, foreignURL := provider(t, outcomeOK)
	routes := []core.Route{
		route("primary", primaryURL, "client-cloud", "us"),
		route("external", externalURL, "external", "us"),
		route("foreign-region", foreignURL, "client-cloud", "eu"),
	}
	db := &memStore{limit: 100}
	s := newServer(t, config(routes...), db)
	status, reply := localCall(t, s, validBody)
	if status != 503 || !strings.Contains(reply, "routes_exhausted") || primary.Calls() != 1 ||
		external.Calls() != 0 || foreignRegion.Calls() != 0 {
		t.Fatalf("no eligible fallback expected: %d %s calls=%d/%d/%d",
			status, reply, primary.Calls(), external.Calls(), foreignRegion.Calls())
	}
	deny := config(routes...)
	p := deny.Clients["client-a"]
	p.Expression = "false"
	deny.Clients["client-a"] = p
	denied := newServer(t, deny, db)
	status, reply = localCall(t, denied, validBody)
	ats, audit, _, _ := db.snapshot()
	if status != 403 || len(ats) != 1 || len(audit) != 1 ||
		audit[0].Detail["policy_version"] != "policy-3" || audit[0].Detail["rule"] != "client-cloud" ||
		primary.Calls() != 1 {
		t.Fatalf("expected audited denial without more inference: %d %s", status, reply)
	}
}

func TestFallbackClassificationAndLimits(t *testing.T) {
	for _, tc := range []struct {
		firstMode            string
		wantStatus, attempts int
		maxAttempts          int
		wantCode             string
	}{
		{outcomeCapability, 200, 2, 2, "chat.completion"},
		{outcomeSafety, 422, 1, 2, "provider_safety_refusal"},
		{outcomeUnknown, 502, 1, 2, "unknown_provider_outcome"},
		{outcomeUnavailable, 503, 1, 1, "routes_exhausted"},
	} {
		t.Run(tc.firstMode, func(t *testing.T) {
			first, firstURL := provider(t, tc.firstMode)
			second, secondURL := provider(t, outcomeOK)
			cfg := config(route("first", firstURL, "client-cloud", "us"), route("second", secondURL, "client-cloud", "us"))
			cfg.MaxAttempts = tc.maxAttempts
			db := &memStore{limit: 100}
			status, reply := localCall(t, newServer(t, cfg, db), validBody)
			ats, _, _, _ := db.snapshot()
			if status != tc.wantStatus || !strings.Contains(reply, tc.wantCode) ||
				first.Calls() != 1 || second.Calls() != int64(tc.attempts-1) || len(ats) != tc.attempts {
				t.Fatalf("unexpected outcome %d %s, attempts=%d calls=%d/%d",
					status, reply, len(ats), first.Calls(), second.Calls())
			}
			if len(ats) == 2 && (ats[0].a.ID == ats[1].a.ID || ats[0].a.RequestID != ats[1].a.RequestID) {
				t.Fatal("fallback must use a separate attempt linked to the same request")
			}
		})
	}
}

func TestUnknownUsageKeepsHoldAndFreshFallbackReservation(t *testing.T) {
	for _, tc := range []struct {
		limit  int64
		status int
	}{
		{10, 429}, // Each attempt holds 7. The second cannot spend the first hold.
		{100, 200},
	} {
		first, firstURL := provider(t, "timeout_then_usage")
		second, secondURL := provider(t, outcomeOK)
		db := &memStore{limit: tc.limit}
		s := newServer(t, config(route("first", firstURL, "client-cloud", "us"), route("second", secondURL, "client-cloud", "us")), db)
		status, reply := localCall(t, s, validBody)
		ats, _, held, spent := db.snapshot()
		if status != tc.status || len(ats) == 0 || ats[0].state != "uncertain" || held != 7 || first.Calls() != 1 {
			t.Fatalf("expected uncertain hold across fallback: status=%d reply=%s attempts=%d held=%d", status, reply, len(ats), held)
		}
		if (tc.status == 429 && (second.Calls() != 0 || spent != 0)) ||
			(tc.status == 200 && (second.Calls() != 1 || spent != 3)) {
			t.Fatalf("fallback did not use fresh budget: second=%d spent=%d", second.Calls(), spent)
		}
		e, err := s.Lookup(context.Background(), core.Pending{Attempt: ats[0].a})
		if err != nil || !e.Final || e.Actual != 3 || first.Calls() != 1 {
			t.Fatalf("lookup should reconcile through evidence, never infer: %v %v calls=%d", e, err, first.Calls())
		}
	}
}

func TestDispatchMustCommitBeforeProviderAndDeadlineStopsFallback(t *testing.T) {
	f, firstURL := provider(t, "timeout")
	second, secondURL := provider(t, outcomeOK)
	cfg := config(route("first", firstURL, "client-cloud", "us"), route("second", secondURL, "client-cloud", "us"))
	db := &memStore{limit: 100, dispatchErr: true}
	status, _ := localCall(t, newServer(t, cfg, db), validBody)
	if status != 503 || f.Calls() != 0 || second.Calls() != 0 {
		t.Fatal("provider was contacted without successful dispatch state")
	}
	cfg.RequestTimeoutMS, cfg.AttemptTimeoutMS = 70, 70
	db = &memStore{limit: 100}
	status, reply := localCall(t, newServer(t, cfg, db), validBody)
	ats, _, held, _ := db.snapshot()
	if status != 504 || !strings.Contains(reply, "request_deadline") || second.Calls() != 0 ||
		len(ats) != 1 || ats[0].state != "uncertain" || held != 7 {
		t.Fatalf("deadline did not retain pending usage and block fallback: %d %s", status, reply)
	}
}

func TestRedirectNeverForwardsProviderRequest(t *testing.T) {
	var redirected atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(target.Close)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(upstream.Close)
	db := &memStore{limit: 100}
	s := newServer(t, config(route("redirect", upstream.URL, "client-cloud", "us")), db)
	status, reply := localCall(t, s, validBody)
	if status != 502 || redirected.Load() != 0 {
		t.Fatalf("redirect left the approved origin: %d %s redirected=%d", status, reply, redirected.Load())
	}
}

func TestBadPolicyRejectedBeforeServing(t *testing.T) {
	for _, expr := range []string{`missing == true`, `"not a bool"`} {
		c := config(route("test", "http://127.0.0.1:1", "client-cloud", "us"))
		p := c.Clients["client-a"]
		p.Expression = expr
		c.Clients["client-a"] = p
		if _, err := New(c, &memStore{}); err == nil {
			t.Fatalf("accepted bad CEL policy %s", expr)
		}
	}
}

func TestBufferedSSECompletesOnlyAfterFinalAccounting(t *testing.T) {
	_, endpoint := provider(t, outcomeOK)
	db := &memStore{limit: 100}
	s := newServer(t, config(route("stream", endpoint, "client-cloud", "us")), db)
	status, reply := localCall(t, s, strings.TrimSuffix(validBody, "}")+`,"stream":true}`)
	ats, _, held, spent := db.snapshot()
	if status != 200 || !strings.Contains(reply, "chat.completion.chunk") ||
		!strings.HasSuffix(reply, "data: [DONE]\n\n") || len(ats) != 1 ||
		ats[0].state != "settled" || held != 0 || spent != 3 {
		t.Fatalf("unexpected stream or accounting: status=%d held=%d spent=%d", status, held, spent)
	}
}

func TestIncompleteUsageDoesNotBecomeZeroCharge(t *testing.T) {
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req ProviderRequest
		if err := decodeLimited(r.Body, maxRequestBody, &req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"attempt_id": req.AttemptID, "request_id": "incomplete-usage", "code": outcomeOK,
			"text": "result", "usage": map[string]int64{"input_tokens": 0},
		})
	}))
	t.Cleanup(first.Close)
	second, secondURL := provider(t, outcomeOK)
	db := &memStore{limit: 100}
	s := newServer(t, config(route("first", first.URL, "client-cloud", "us"),
		route("second", secondURL, "client-cloud", "us")), db)
	status, reply := localCall(t, s, validBody)
	ats, _, held, spent := db.snapshot()
	if status != 502 || len(ats) != 1 || ats[0].state != "uncertain" ||
		held != 7 || spent != 0 || second.Calls() != 0 {
		t.Fatalf("incomplete evidence was treated as final or retryable: status=%d reply=%s held=%d spent=%d",
			status, reply, held, spent)
	}
}

func TestDuplicateCanonicalProviderAndModelRejected(t *testing.T) {
	base := route("first", "https://Provider.example", "client-cloud", "us")
	alias := route("alias", "https://provider.example.:0443/", "client-cloud", "us")
	if s, err := New(config(base, alias), &memStore{}); err == nil {
		s.CloseIdleConnections()
		t.Fatal("fallback can repeat the same provider origin and model through an alias")
	}
	alias.Endpoint = "https://another.example"
	s, err := New(config(base, alias), &memStore{})
	if err != nil {
		t.Fatal(err)
	}
	s.CloseIdleConnections()
}
