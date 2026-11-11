package gateway

import (
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
)

// Fixture is a deterministic, in-memory lab provider. It stores usage evidence,
// never prompts, and makes no external requests.
type Fixture struct {
	mode    string
	calls   atomic.Int64
	lookups atomic.Int64
	mu      sync.Mutex
	records map[string]ProviderReply
}

func NewFixture(mode string) (*Fixture, error) {
	switch mode {
	case outcomeOK, outcomeCapability, outcomeSafety, outcomeUnavailable, outcomeUnknown,
		"timeout", "timeout_then_usage", "redirect":
	default:
		return nil, errors.New("invalid fixture mode")
	}
	return &Fixture{mode: mode, records: make(map[string]ProviderReply)}, nil
}

func (f *Fixture) Calls() int64 { return f.calls.Load() }

func (f *Fixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/infer":
		f.infer(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/usage":
		f.lookup(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/calls":
		writeJSON(w, http.StatusOK, map[string]int64{"calls": f.Calls(), "usage_lookups": f.lookups.Load()})
	default:
		writeError(w, http.StatusNotFound, "not_found", "fixture endpoint not found")
	}
}

func (f *Fixture) infer(w http.ResponseWriter, r *http.Request) {
	f.calls.Add(1)
	var req ProviderRequest
	if err := decodeLimited(r.Body, maxUpstreamBody, &req); err != nil ||
		!validLabel(req.AttemptID) || len(req.AttemptID) > 80 || !validLabel(req.Model) {
		writeError(w, http.StatusBadRequest, "bad_fixture_request", "invalid fixture request")
		return
	}
	if f.mode == "timeout" {
		<-r.Context().Done()
		return
	}
	if f.mode == "redirect" {
		http.Redirect(w, r, "/redirected-provider", http.StatusTemporaryRedirect)
		return
	}
	reply := ProviderReply{
		AttemptID: req.AttemptID, RequestID: "fixture-" + req.AttemptID, Code: f.mode,
	}
	status := http.StatusOK
	switch f.mode {
	case outcomeOK, "timeout_then_usage":
		reply.Code = outcomeOK
		reply.Text = "fixture completion"
		var input int64
		for _, msg := range req.Messages {
			input += int64(len(msg.Role) + len(msg.Content))
		}
		reply.Usage = &Usage{InputTokens: (input + 3) / 4, OutputTokens: 1}
	case outcomeCapability:
		status, reply.Usage = http.StatusUnprocessableEntity, &Usage{}
	case outcomeSafety:
		status, reply.Usage = http.StatusForbidden, &Usage{}
	case outcomeUnavailable:
		status, reply.Usage = http.StatusServiceUnavailable, &Usage{}
	default:
		status = http.StatusBadGateway
	}
	if reply.Usage != nil {
		f.mu.Lock()
		if len(f.records) < 4096 {
			evidence := reply
			evidence.Text = ""
			f.records[req.AttemptID] = evidence
		}
		f.mu.Unlock()
	}
	if f.mode == "timeout_then_usage" {
		<-r.Context().Done()
		return
	}
	writeJSON(w, status, reply)
}

func (f *Fixture) lookup(w http.ResponseWriter, r *http.Request) {
	f.lookups.Add(1)
	q := r.URL.Query()
	id, providerID := q.Get("attempt_id"), q.Get("request_id")
	f.mu.Lock()
	reply, ok := f.records[id]
	f.mu.Unlock()
	if !ok || (providerID != "" && providerID != reply.RequestID) {
		writeError(w, http.StatusNotFound, "usage_pending", "no final usage record found")
		return
	}
	writeJSON(w, http.StatusOK, reply)
}
