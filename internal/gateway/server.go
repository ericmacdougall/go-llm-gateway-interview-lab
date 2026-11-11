package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"example.com/llmgateway/internal/core"
)

const (
	maxRequestBody    = 128 << 10
	maxOutputTokens   = 4096
	accountingTimeout = 2 * time.Second
)

type completionRequest struct {
	Model                string    `json:"model,omitempty"`
	Messages             []Message `json:"messages"`
	MaxTokens            int       `json:"max_tokens,omitempty"`
	Stream               bool      `json:"stream,omitempty"`
	RequiredCapabilities []string  `json:"required_capabilities,omitempty"`
}

type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/healthz":
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	case r.Method == http.MethodPost && r.URL.Path == "/v1/chat/completions":
		s.complete(w, r)
	default:
		writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
	}
}

// principal requires a verified TLS leaf and reads only its URI SAN.
func (s *Server) principal(r *http.Request) (core.Principal, bool) {
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return core.Principal{}, false
	}
	leaf := r.TLS.PeerCertificates[0]
	verified := false
	for _, chain := range r.TLS.VerifiedChains {
		if len(chain) != 0 && chain[0].Equal(leaf) {
			verified = true
			break
		}
	}
	if !verified || len(leaf.URIs) != 1 {
		return core.Principal{}, false
	}
	principal, ok := s.identities[leaf.URIs[0].String()]
	return principal, ok
}

func (s *Server) complete(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.principal(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "invalid_identity", "verified workload identity required")
		return
	}
	for _, header := range []string{"X-Client-ID", "X-Tenant-ID", "X-Workload-ID", "X-User-ID", "X-Department", "X-App", "X-Role"} {
		if len(r.Header.Values(header)) != 0 {
			writeError(w, http.StatusBadRequest, "identity_override", "identity fields come from the verified workload")
			return
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()
	var q completionRequest
	if err := decodeLimited(r.Body, maxRequestBody, &q); err != nil || !q.validate() {
		writeError(w, http.StatusBadRequest, "invalid_request", "invalid completion request")
		return
	}
	requestID, err := newID("req-")
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "request_id_unavailable", "could not accept request")
		return
	}
	w.Header().Set("X-Request-ID", requestID)

	pc := s.clients[principal.Client]
	routes, err := pc.eligible(ctx, principal, q)
	if err != nil {
		s.reject(w, ctx, principal, pc, requestID, http.StatusServiceUnavailable, "policy_error")
		return
	}
	if len(routes) == 0 {
		s.reject(w, ctx, principal, pc, requestID, http.StatusForbidden, "no_eligible_route")
		return
	}

	attemptCount, approvalDenied := 0, false
	for _, route := range routes {
		if ctx.Err() != nil || attemptCount >= s.maxAttempts {
			break
		}
		approvalID := ""
		if route.RequiresApproval {
			grants, ok := s.approvals.(interface {
				GrantID(context.Context, core.Principal, string) (string, error)
			})
			if !ok {
				approvalDenied = true
				continue
			}
			grant, err := grants.GrantID(ctx, principal, route.Model)
			if err != nil {
				writeError(w, http.StatusServiceUnavailable, "approval_unavailable", "could not establish current approval")
				return
			}
			if grant == "" {
				approvalDenied = true
				continue
			}
			approvalID = grant
		}
		attemptCount++
		attemptID, err := newID("att-")
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "attempt_id_unavailable", "could not start provider attempt")
			return
		}
		estimate, _ := cost(route, q.inputUnits(), int64(q.MaxTokens))
		// A configured zero input rate and a zero result are possible for
		// metering schemes. Reserve at least one unit to use a common ledger.
		a := core.Attempt{
			ID: attemptID, RequestID: requestID, Principal: principal, Route: route, ApprovalID: approvalID,
			PolicyVersion: pc.version, Rule: pc.rule, Estimate: max(estimate, 1),
		}
		accepted, err := s.store.Reserve(ctx, a)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "accounting_unavailable", "could not reserve budget")
			return
		}
		if !accepted {
			s.reject(w, ctx, principal, pc, requestID, http.StatusTooManyRequests, "budget_exhausted")
			return
		}
		// The provider side effect is legal only after this write commits.
		if err := s.store.MarkDispatch(ctx, a.ID); err != nil {
			writeError(w, http.StatusServiceUnavailable, "dispatch_unavailable", "could not record dispatch")
			return
		}
		attemptTimeout := s.attemptTimeout
		if route.TimeoutMS > 0 {
			attemptTimeout = min(attemptTimeout, time.Duration(route.TimeoutMS)*time.Millisecond)
		}
		attemptCtx, stopAttempt := context.WithTimeout(ctx, attemptTimeout)
		stream := completionStream{w: w, id: requestID, model: route.Model}
		var outcome providerOutcome
		if q.Stream && route.Protocol != "" && route.Protocol != "fixture" {
			if deadline, ok := ctx.Deadline(); ok {
				_ = http.NewResponseController(w).SetWriteDeadline(deadline)
			}
			outcome = s.invokeStream(attemptCtx, a, q, stream.emit)
		} else {
			outcome = s.invoke(attemptCtx, a, q)
		}
		stopAttempt()

		if err := s.recordOutcome(ctx, a.ID, outcome.evidence); err != nil {
			if stream.committed {
				stream.finish("accounting_unavailable")
				return
			}
			writeError(w, http.StatusServiceUnavailable, "accounting_unavailable", "could not record outcome")
			return
		}
		if stream.committed {
			stream.finish(outcome.code)
			return // Never fall back after any content reached the caller.
		}
		switch outcome.code {
		case outcomeOK:
			if ctx.Err() == nil {
				writeCompletion(w, requestID, route.Model, outcome, q.Stream)
				return
			}
		case outcomeSafety:
			writeError(w, http.StatusUnprocessableEntity, "provider_safety_refusal", "provider refused the request")
			return
		case outcomeCapability, outcomeUnavailable:
			// Each possible next attempt repeats admission and has a separate hold.
		default:
			writeError(w, http.StatusBadGateway, "unknown_provider_outcome", "could not establish provider outcome")
			return
		}
	}

	if attemptCount == 0 && approvalDenied {
		s.reject(w, ctx, principal, pc, requestID, http.StatusForbidden, "approval_required")
		return
	}
	if ctx.Err() != nil {
		writeError(w, http.StatusGatewayTimeout, "request_deadline", "gateway request deadline exceeded")
		return
	}
	writeError(w, http.StatusServiceUnavailable, "routes_exhausted", "no permitted route completed the request")
}

func (s *Server) recordOutcome(ctx context.Context, id string, e core.Evidence) error {
	// A canceled inference request does not cancel this bounded write. The
	// handler remains active, so graceful shutdown still tracks the operation.
	accountingCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), accountingTimeout)
	defer cancel()
	if e.Final {
		_, err := s.store.Settle(accountingCtx, id, e)
		return err
	}
	return s.store.MarkUncertain(accountingCtx, id, e)
}

func (s *Server) reject(
	w http.ResponseWriter, ctx context.Context, p core.Principal, pc clientPolicy,
	requestID string, status int, reason string,
) {
	auditCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), accountingTimeout)
	defer cancel()
	if err := s.store.Audit(auditCtx, core.Event{
		Kind: "request_denied", Principal: p, RequestID: requestID,
		Detail: map[string]any{"reason": reason, "policy_version": pc.version, "rule": pc.rule},
	}); err != nil {
		writeError(w, http.StatusServiceUnavailable, "audit_unavailable", "could not record policy decision")
		return
	}
	writeError(w, status, reason, strings.ReplaceAll(reason, "_", " "))
}

func (p clientPolicy) eligible(ctx context.Context, identity core.Principal, q completionRequest) ([]core.Route, error) {
	var routes []core.Route
	capabilities := q.RequiredCapabilities
	if capabilities == nil {
		capabilities = []string{}
	}
	for _, route := range p.routes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if route.Environment != p.environment || (q.Model != "" && q.Model != route.Model) ||
			!hasCapabilities(route, capabilities) {
			continue
		}
		allowed, _, err := p.program.ContextEval(ctx, map[string]any{
			"client": identity.Client, "workload": identity.Workload, "department": identity.Department,
			"app": identity.App, "role": identity.Role, "route_id": route.ID, "provider": route.Provider,
			"model": route.Model, "region": route.Region, "environment": route.Environment,
			"requested_model": q.Model, "required_capabilities": capabilities,
		})
		if err != nil {
			return nil, err
		}
		if yes, ok := allowed.Value().(bool); ok && yes {
			routes = append(routes, route)
		}
	}
	return routes, nil
}

func hasCapabilities(r core.Route, required []string) bool {
	for _, c := range required {
		if !slices.Contains(r.Capabilities, c) {
			return false
		}
	}
	return true
}

func (q *completionRequest) validate() bool {
	if q.MaxTokens == 0 {
		q.MaxTokens = 64
	}
	if q.MaxTokens < 1 || q.MaxTokens > maxOutputTokens ||
		len(q.Messages) < 1 || len(q.Messages) > 64 || len(q.RequiredCapabilities) > 16 ||
		(q.Model != "" && !validLabel(q.Model)) {
		return false
	}
	for _, m := range q.Messages {
		if m.Content == "" || (m.Role != "system" && m.Role != "user" && m.Role != "assistant") {
			return false
		}
	}
	for _, c := range q.RequiredCapabilities {
		if !validLabel(c) {
			return false
		}
	}
	return true
}

func (q completionRequest) inputUnits() int64 {
	var n int64
	for _, m := range q.Messages {
		n += int64(len(m.Role) + len(m.Content))
	}
	return n
}

func newID(prefix string) (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(b[:]), nil
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	var b errorBody
	b.Error.Code, b.Error.Message = code, message
	writeJSON(w, status, b)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeCompletion(w http.ResponseWriter, id, model string, outcome providerOutcome, stream bool) {
	if !stream {
		writeJSON(w, http.StatusOK, map[string]any{
			"id": id, "model": model, "object": "chat.completion",
			"choices": []any{map[string]any{
				"index": 0, "message": Message{Role: "assistant", Content: outcome.text}, "finish_reason": "stop",
			}},
		})
		return
	}
	// The lab adapter buffers one bounded completion before sending SSE.
	// Nothing is streamed from the provider or retried after the first byte.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	for _, choice := range []map[string]any{
		{"index": 0, "delta": Message{Role: "assistant", Content: outcome.text}, "finish_reason": nil},
		{"index": 0, "delta": map[string]any{}, "finish_reason": "stop"},
	} {
		b, _ := json.Marshal(map[string]any{
			"id": id, "model": model, "object": "chat.completion.chunk", "choices": []any{choice},
		})
		if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
			return
		}
	}
	_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
}
