package gateway

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"example.com/llmgateway/internal/core"
)

func protocolConfig(t *testing.T, protocol, origin string) Config {
	t.Helper()
	t.Setenv("TEST_PROVIDER_AUTH", "local-fixture-credential")
	r := route("vendor", origin, "client-cloud", "us")
	r.Protocol, r.AuthEnv = protocol, "TEST_PROVIDER_AUTH"
	return config(r)
}

func TestVendorTextProtocols(t *testing.T) {
	for _, protocol := range []string{"openai", "openrouter", "anthropic", "gemini"} {
		t.Run(protocol, func(t *testing.T) {
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				var payload map[string]json.RawMessage
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					t.Error(err)
				}
				switch protocol {
				case "openai":
					if r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer local-fixture-credential" ||
						string(payload["max_completion_tokens"]) != "1" || string(payload["store"]) != "false" {
						t.Errorf("bad OpenAI request path/headers/shape")
					}
					io.WriteString(w, `{"id":"chatcmpl-test","choices":[{"message":{"content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":1},"extra_field":true}`)
				case "openrouter":
					if r.URL.Path != "/api/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer local-fixture-credential" ||
						string(payload["max_tokens"]) != "1" || len(payload["max_completion_tokens"]) != 0 ||
						r.Header.Get("X-OpenRouter-Title") != "Go gateway reference" {
						t.Errorf("bad OpenRouter request path/headers/shape")
					}
					io.WriteString(w, `{"id":"gen-test","choices":[{"message":{"content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":1,"cost":0.00001},"provider":"OpenAI"}`)
				case "anthropic":
					if r.URL.Path != "/v1/messages" || r.Header.Get("x-api-key") != "local-fixture-credential" ||
						r.Header.Get("anthropic-version") != "2023-06-01" || string(payload["system"]) != `"system text"` {
						t.Errorf("bad Anthropic request path/headers/shape")
					}
					io.WriteString(w, `{"id":"msg-test","type":"message","content":[{"type":"text","text":"hello"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"cache_read_input_tokens":1,"output_tokens":1}}`)
				case "gemini":
					if r.URL.Path != "/v1beta/models/test-model:generateContent" || r.Header.Get("x-goog-api-key") != "local-fixture-credential" ||
						!strings.Contains(string(payload["systemInstruction"]), "system text") {
						t.Errorf("bad Gemini request path/headers/shape")
					}
					io.WriteString(w, `{"responseId":"gem-test","candidates":[{"content":{"role":"model","parts":[{"text":"hello"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"thoughtsTokenCount":1}}`)
				}
			}))
			t.Cleanup(upstream.Close)
			db := &memStore{limit: 100}
			s := newServer(t, protocolConfig(t, protocol, upstream.URL), db)
			status, reply := localCall(t, s,
				`{"messages":[{"role":"system","content":"system text"},{"role":"user","content":"hi"}],"max_tokens":1}`)
			ats, _, held, spent := db.snapshot()
			if status != 200 || !strings.Contains(reply, "hello") || calls.Load() != 1 ||
				len(ats) != 1 || ats[0].state != "settled" || held != 0 || spent != 3 {
				t.Fatalf("bad normalized result: status=%d reply=%s held=%d spent=%d", status, reply, held, spent)
			}
			// Real provider protocols have no generic per-request usage lookup.
			e, err := s.Lookup(context.Background(), core.Pending{Attempt: ats[0].a})
			if err != nil || e.Final || calls.Load() != 1 {
				t.Fatalf("vendor reconciliation must remain pending without matching evidence: %v %v", e, err)
			}
		})
	}
}

func TestVendorSafetyTerminalAndMissingUsagePending(t *testing.T) {
	for _, tc := range []struct {
		protocol, response string
		wantStatus         int
	}{
		{"openai", `{"id":"o","choices":[{"message":{"refusal":"no"},"finish_reason":"stop"}],"usage":{"prompt_tokens":0,"completion_tokens":0}}`, 422},
		{"anthropic", `{"id":"a","type":"message","content":[],"stop_reason":"refusal","usage":{"input_tokens":0,"output_tokens":0}}`, 422},
		{"gemini", `{"responseId":"g","promptFeedback":{"blockReason":"SAFETY"}}`, 422},
		{"openai", `{"id":"o","choices":[{"message":{"content":"hello"},"finish_reason":"stop"}]}`, 502},
	} {
		t.Run(tc.protocol+fmt.Sprint(tc.wantStatus), func(t *testing.T) {
			vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.WriteString(w, tc.response)
			}))
			t.Cleanup(vendor.Close)
			cfg := protocolConfig(t, tc.protocol, vendor.URL)
			second, origin := provider(t, outcomeOK)
			policy := cfg.Clients["client-a"]
			policy.Routes = append(policy.Routes, route("second", origin, "client-cloud", "us"))
			cfg.Clients["client-a"] = policy
			db := &memStore{limit: 100}
			status, reply := localCall(t, newServer(t, cfg, db), validBody)
			ats, _, held, _ := db.snapshot()
			if status != tc.wantStatus || second.Calls() != 0 || len(ats) != 1 {
				t.Fatalf("safety or unknown outcome fell back: %d %s", status, reply)
			}
			if tc.wantStatus == 502 && (ats[0].state != "uncertain" || held != 7) {
				t.Fatalf("missing usage was released: %+v held=%d", ats, held)
			}
		})
	}
}

func TestVendorStreamingProtocols(t *testing.T) {
	for _, protocol := range []string{"openai", "openrouter", "anthropic", "gemini"} {
		t.Run(protocol, func(t *testing.T) {
			clientSawFirst := make(chan struct{})
			providerDone := make(chan struct{})
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				var first, last string
				switch protocol {
				case "openai":
					first = `data: {"id":"stream-o","choices":[{"delta":{"content":"hello"},"finish_reason":null}]}` + "\n\n"
					last = `data: {"id":"stream-o","choices":[{"delta":{},"finish_reason":"stop"}]}` + "\n\n" +
						`data: {"id":"stream-o","choices":[],"usage":{"prompt_tokens":2,"completion_tokens":1}}` + "\n\n" + "data: [DONE]\n\n"
				case "openrouter":
					if r.URL.Path != "/api/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer local-fixture-credential" {
						t.Error("bad OpenRouter stream endpoint/auth")
					}
					first = `data: {"id":"gen-stream","choices":[{"delta":{"content":"hello"},"finish_reason":null}]}` + "\n\n"
					last = `data: {"id":"gen-stream","choices":[{"delta":{},"finish_reason":"stop"}]}` + "\n\n" +
						`data: {"id":"gen-stream","choices":[{"delta":{"content":"","role":"assistant"},"finish_reason":"stop","native_finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":1}}` + "\n\n" + "data: [DONE]\n\n"
				case "anthropic":
					first = `event: message_start` + "\n" +
						`data: {"type":"message_start","message":{"id":"stream-a","usage":{"input_tokens":2,"output_tokens":0}}}` + "\n\n" +
						`data: {"type":"content_block_start","content_block":{"type":"text","text":""}}` + "\n\n" +
						`data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"hello"}}` + "\n\n"
					last = `data: {"type":"content_block_stop"}` + "\n\n" +
						`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}` + "\n\n" +
						`data: {"type":"message_stop"}` + "\n\n"
				case "gemini":
					if r.URL.Query().Get("alt") != "sse" || !strings.HasSuffix(r.URL.Path, ":streamGenerateContent") {
						t.Error("bad Gemini stream route")
					}
					first = `data: {"responseId":"stream-g","candidates":[{"content":{"role":"model","parts":[{"text":"hello"}]}}]}` + "\n\n"
					last = `data: {"responseId":"stream-g","candidates":[{"content":{"role":"model","parts":[]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":2,"candidatesTokenCount":1}}` + "\n\n"
				}
				io.WriteString(w, first)
				w.(http.Flusher).Flush()
				select {
				case <-clientSawFirst:
				case <-r.Context().Done():
					return
				}
				io.WriteString(w, last)
				close(providerDone)
			}))
			t.Cleanup(upstream.Close)
			db := &memStore{limit: 100}
			s := newServer(t, protocolConfig(t, protocol, upstream.URL), db)
			ca := newCA(t)
			server := mtlsServer(t, s, ca)
			client := mtlsClient(t, server, ca.workload(t, testWorkload))
			resp, err := client.Post(server.URL+"/v1/chat/completions", "application/json",
				strings.NewReader(`{"messages":[{"role":"user","content":"hi"}],"max_tokens":1,"stream":true}`))
			if err != nil {
				t.Fatal(err)
			}
			reader := bufio.NewReader(resp.Body)
			firstLine, err := reader.ReadString('\n')
			if err != nil || !strings.Contains(firstLine, "hello") {
				t.Fatalf("first provider delta not delivered before provider completion: %q %v", firstLine, err)
			}
			close(clientSawFirst) // Only now may the provider send final usage.
			rest, err := io.ReadAll(reader)
			resp.Body.Close()
			if err != nil || !strings.Contains(string(rest), "[DONE]") {
				t.Fatalf("stream did not finish: %s %v", rest, err)
			}
			<-providerDone
			ats, _, held, spent := db.snapshot()
			if len(ats) != 1 || ats[0].state != "settled" || held != 0 || spent != 3 {
				t.Fatalf("stream settlement differs: %+v held=%d spent=%d", ats, held, spent)
			}
		})
	}
}

func TestPartialStreamRetainsHoldWithoutFallback(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, `data: {"id":"partial","choices":[{"delta":{"content":"partial"},"finish_reason":null}]}`+"\n\n")
		w.(http.Flusher).Flush()
		// EOF with no final finish reason, usage or DONE.
	}))
	t.Cleanup(upstream.Close)
	cfg := protocolConfig(t, "openai", upstream.URL)
	next, nextURL := provider(t, outcomeOK)
	pc := cfg.Clients["client-a"]
	pc.Routes = append(pc.Routes, route("second", nextURL, "client-cloud", "us"))
	cfg.Clients["client-a"] = pc
	db := &memStore{limit: 100}
	status, reply := localCall(t, newServer(t, cfg, db),
		`{"messages":[{"role":"user","content":"hi"}],"max_tokens":1,"stream":true}`)
	ats, _, held, spent := db.snapshot()
	if status != 200 || !strings.Contains(reply, "partial") || !strings.Contains(reply, `"code":"unknown"`) ||
		len(ats) != 1 || ats[0].state != "uncertain" || held != 7 || spent != 0 || next.Calls() != 0 {
		t.Fatalf("partial stream must keep hold without fallback: status=%d reply=%s held=%d spent=%d", status, reply, held, spent)
	}
}

type approvalFunc func(context.Context, core.Principal, string) (bool, error)

func (f approvalFunc) Allows(ctx context.Context, p core.Principal, model string) (bool, error) {
	return f(ctx, p, model)
}

func (f approvalFunc) GrantID(ctx context.Context, p core.Principal, model string) (string, error) {
	allowed, err := f(ctx, p, model)
	if !allowed || err != nil {
		return "", err
	}
	return "grant-test", nil
}

func TestApprovalCheckPrecedesEveryProtectedAttempt(t *testing.T) {
	f, origin := provider(t, outcomeOK)
	cfg := config(route("approved", origin, "client-cloud", "us"))
	policy := cfg.Clients["client-a"]
	policy.Routes[0].RequiresApproval = true
	cfg.Clients["client-a"] = policy
	db := &memStore{limit: 100}
	s := newServer(t, cfg, db)
	status, _ := localCall(t, s, validBody)
	if status != 403 || f.Calls() != 0 {
		t.Fatal("missing approval checker must deny protected routes")
	}
	var checks int
	s.SetApprovals(approvalFunc(func(ctx context.Context, p core.Principal, model string) (bool, error) {
		checks++
		if p.Client != "client-a" || p.Workload != testWorkload || model != "test-model" {
			t.Error("approval check widened authenticated identity or resource")
		}
		return true, nil
	}))
	status, reply := localCall(t, s, validBody)
	if status != 200 || f.Calls() != 1 || checks != 1 {
		t.Fatalf("approved route did not run: %d %s checks=%d", status, reply, checks)
	}
}

func TestStreamSafetyRefusalCannotFallBack(t *testing.T) {
	for _, prefix := range []string{"", `data: {"id":"safety","choices":[{"delta":{"content":"partial"},"finish_reason":null}]}` + "\n\n"} {
		origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, prefix)
			io.WriteString(w, `data: {"id":"safety","choices":[{"delta":{"refusal":"blocked"},"finish_reason":null}]}`+"\n\n")
		}))
		t.Cleanup(origin.Close)
		cfg := protocolConfig(t, "openai", origin.URL)
		next, nextURL := provider(t, outcomeOK)
		policy := cfg.Clients["client-a"]
		policy.Routes = append(policy.Routes, route("second", nextURL, "client-cloud", "us"))
		cfg.Clients["client-a"] = policy
		db := &memStore{limit: 100}
		status, reply := localCall(t, newServer(t, cfg, db),
			`{"messages":[{"role":"user","content":"hi"}],"max_tokens":1,"stream":true}`)
		if prefix == "" && (status != 422 || !strings.Contains(reply, "provider_safety_refusal")) ||
			prefix != "" && (status != 200 || !strings.Contains(reply, `"code":"safety"`)) || next.Calls() != 0 {
			t.Fatalf("safety refusal retried or hidden: %d %s next=%d", status, reply, next.Calls())
		}
		ats, _, held, spent := db.snapshot()
		if len(ats) != 1 || ats[0].state != "uncertain" || held != 7 || spent != 0 {
			t.Fatalf("missing final usage was fabricated: %+v held=%d spent=%d", ats, held, spent)
		}
	}
}

type notifyingStore struct {
	*memStore
	uncertain chan struct{}
}

func (s *notifyingStore) MarkUncertain(ctx context.Context, id string, e core.Evidence) error {
	err := s.memStore.MarkUncertain(ctx, id, e)
	close(s.uncertain)
	return err
}

func TestConsumerDisconnectCancelsStreamAndRetainsHold(t *testing.T) {
	providerCanceled := make(chan struct{})
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, `data: {"id":"disconnected","choices":[{"delta":{"content":"partial"},"finish_reason":null}]}`+"\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(providerCanceled)
	}))
	t.Cleanup(origin.Close)
	cfg := protocolConfig(t, "openai", origin.URL)
	cfg.RequestTimeoutMS, cfg.AttemptTimeoutMS = 10000, 10000
	memory := &memStore{limit: 100}
	store := &notifyingStore{memStore: memory, uncertain: make(chan struct{})}
	s, err := New(cfg, store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.CloseIdleConnections)
	ca := newCA(t)
	server := mtlsServer(t, s, ca)
	client := mtlsClient(t, server, ca.workload(t, testWorkload))
	resp, err := client.Post(server.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"messages":[{"role":"user","content":"hi"}],"max_tokens":1,"stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bufio.NewReader(resp.Body).ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	// Both barriers must arrive before the 10-second inference deadline.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, ch := range []<-chan struct{}{providerCanceled, store.uncertain} {
		select {
		case <-ch:
		case <-ctx.Done():
			t.Fatal("caller disconnect did not cancel provider and retain accounting")
		}
	}
	ats, _, held, spent := memory.snapshot()
	if len(ats) != 1 || ats[0].state != "uncertain" || held != 7 || spent != 0 {
		t.Fatalf("disconnected stream lost uncertain accounting: %+v held=%d spent=%d", ats, held, spent)
	}
}

type revokingStore struct {
	*memStore
	grantValid atomic.Bool
}

func (s *revokingStore) Reserve(ctx context.Context, a core.Attempt) (bool, error) {
	ok, err := s.memStore.Reserve(ctx, a)
	// Deterministic race boundary. A concurrent approver revokes the grant
	// after admission starts, before the durable dispatch transaction.
	s.grantValid.Store(false)
	return ok, err
}

func (s *revokingStore) MarkDispatch(ctx context.Context, id string) error {
	if !s.grantValid.Load() {
		return errors.New("approval revoked before dispatch")
	}
	return s.memStore.MarkDispatch(ctx, id)
}

func TestGrantIdentitySurvivesReservationAndRevocationBlocksDispatch(t *testing.T) {
	fixture, origin := provider(t, outcomeOK)
	cfg := config(route("protected", origin, "client-cloud", "us"))
	policy := cfg.Clients["client-a"]
	policy.Routes[0].RequiresApproval = true
	cfg.Clients["client-a"] = policy
	store := &revokingStore{memStore: &memStore{limit: 100}}
	store.grantValid.Store(true)
	s, err := New(cfg, store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.CloseIdleConnections)
	s.SetApprovals(approvalFunc(func(context.Context, core.Principal, string) (bool, error) {
		return true, nil
	}))
	status, reply := localCall(t, s, validBody)
	ats, _, held, spent := store.snapshot()
	if status != 503 || !strings.Contains(reply, "dispatch_unavailable") || len(ats) != 1 ||
		ats[0].a.ApprovalID != "grant-test" || ats[0].state != "reserved" ||
		held != 7 || spent != 0 || fixture.Calls() != 0 {
		t.Fatalf("revoked grant was dispatched or grant identity was lost: %d %s attempts=%+v", status, reply, ats)
	}
}

type boolOnlyApprovals struct{}

func (boolOnlyApprovals) Allows(context.Context, core.Principal, string) (bool, error) {
	return true, nil
}

func TestProtectedRouteRequiresDurableGrantIdentity(t *testing.T) {
	fixture, origin := provider(t, outcomeOK)
	cfg := config(route("protected", origin, "client-cloud", "us"))
	policy := cfg.Clients["client-a"]
	policy.Routes[0].RequiresApproval = true
	cfg.Clients["client-a"] = policy
	store := &memStore{limit: 100}
	s := newServer(t, cfg, store)
	s.SetApprovals(boolOnlyApprovals{})
	status, _ := localCall(t, s, validBody)
	ats, _, _, _ := store.snapshot()
	if status != 403 || len(ats) != 0 || fixture.Calls() != 0 {
		t.Fatal("boolean approval without bindable grant must not admit protected route")
	}
}
