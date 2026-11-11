package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"syscall"

	"example.com/llmgateway/internal/core"
)

const (
	maxUpstreamBody    = 256 << 10
	maxUsageTokens     = 1_000_000_000
	outcomeOK          = "ok"
	outcomeCapability  = "capability"
	outcomeUnavailable = "unavailable"
	outcomeSafety      = "safety"
	outcomeUnknown     = "unknown"
)

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ProviderRequest struct {
	AttemptID string    `json:"attempt_id"`
	Model     string    `json:"model"`
	Messages  []Message `json:"messages"`
	MaxTokens int       `json:"max_tokens"`
}

type Usage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
}

// UnmarshalJSON distinguishes explicit zero usage from incomplete evidence.
func (u *Usage) UnmarshalJSON(data []byte) error {
	var counters struct {
		Input  *int64 `json:"input_tokens"`
		Output *int64 `json:"output_tokens"`
	}
	if err := decodeLimited(bytes.NewReader(data), maxUpstreamBody, &counters); err != nil {
		return err
	}
	if counters.Input == nil || counters.Output == nil {
		return errors.New("incomplete usage counters")
	}
	u.InputTokens, u.OutputTokens = *counters.Input, *counters.Output
	return nil
}

// ProviderReply is the normalized lab adapter contract, not a vendor API.
type ProviderReply struct {
	AttemptID string `json:"attempt_id"`
	RequestID string `json:"request_id"`
	Code      string `json:"code"`
	Text      string `json:"text,omitempty"`
	Usage     *Usage `json:"usage,omitempty"`
}

type UsageReply = ProviderReply

type providerOutcome struct {
	code     string
	text     string
	evidence core.Evidence
}

func (s *Server) invoke(ctx context.Context, a core.Attempt, q completionRequest) providerOutcome {
	if a.Route.Protocol != "" && a.Route.Protocol != "fixture" {
		return s.invokeProvider(ctx, a, q)
	}
	body, err := json.Marshal(ProviderRequest{
		AttemptID: a.ID, Model: a.Route.Model, Messages: q.Messages, MaxTokens: q.MaxTokens,
	})
	if err != nil {
		return unknownOutcome()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint(a.Route, "/infer"), bytes.NewReader(body))
	if err != nil {
		return unknownOutcome()
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := s.upstream.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, syscall.ECONNREFUSED) ||
			errors.Is(err, syscall.ECONNRESET) || errors.Is(err, io.EOF) ||
			errors.Is(err, io.ErrUnexpectedEOF) {
			return providerOutcome{code: outcomeUnavailable, evidence: core.Evidence{Outcome: outcomeUnavailable}}
		}
		return unknownOutcome()
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		return unknownOutcome()
	}
	var reply ProviderReply
	if err := decodeLimited(resp.Body, maxUpstreamBody, &reply); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return providerOutcome{code: outcomeUnavailable, evidence: core.Evidence{Outcome: outcomeUnavailable}}
		}
		return unknownOutcome()
	}
	if reply.AttemptID != a.ID || !validLabel(reply.RequestID) {
		return unknownOutcome()
	}

	code := classifyReply(resp.StatusCode, reply.Code)
	evidence := core.Evidence{ProviderRequestID: reply.RequestID, Outcome: code}
	if code != outcomeUnknown && reply.Usage != nil {
		actual, ok := cost(a.Route, reply.Usage.InputTokens, reply.Usage.OutputTokens)
		if !ok {
			evidence.Outcome = outcomeUnknown
			return providerOutcome{code: outcomeUnknown, evidence: evidence}
		}
		evidence.Final, evidence.Actual = true, actual
		evidence.Ref = "response:" + reply.RequestID
	}
	if code == outcomeOK && (reply.Text == "" || !evidence.Final) {
		evidence.Outcome = outcomeUnknown
		return providerOutcome{code: outcomeUnknown, evidence: evidence}
	}
	return providerOutcome{code: code, text: reply.Text, evidence: evidence}
}

func classifyReply(status int, code string) string {
	// Safety wins regardless of the provider's HTTP status.
	switch code {
	case outcomeSafety:
		return outcomeSafety
	case outcomeOK:
		if status == http.StatusOK {
			return outcomeOK
		}
	case outcomeCapability:
		if status == http.StatusBadRequest || status == http.StatusUnprocessableEntity {
			return outcomeCapability
		}
	case outcomeUnavailable:
		switch status {
		case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			return outcomeUnavailable
		}
	}
	return outcomeUnknown
}

func unknownOutcome() providerOutcome {
	return providerOutcome{code: outcomeUnknown, evidence: core.Evidence{Outcome: outcomeUnknown}}
}

func endpoint(route core.Route, suffix string) string {
	return strings.TrimSuffix(route.Endpoint, "/") + suffix
}

// Lookup performs a read-only usage lookup. An attempt ID is the lab provider's
// stable correlation key when the provider request ID was lost with the reply.
func (s *Server) Lookup(ctx context.Context, p core.Pending) (core.Evidence, error) {
	if !s.knownUsageRoute(p.Attempt) || !validLabel(p.Attempt.ID) ||
		(p.ProviderRequestID != "" && !validLabel(p.ProviderRequestID)) {
		return core.Evidence{}, errors.New("usage route or correlation key is no longer configured")
	}
	// Provider aggregate usage endpoints do not establish an individual
	// timed-out request's exact cost. Preserve the hold for manual evidence.
	if p.Attempt.Route.Protocol != "" && p.Attempt.Route.Protocol != "fixture" {
		return core.Evidence{}, nil
	}
	ctx, cancel := context.WithTimeout(ctx, s.attemptTimeout)
	defer cancel()
	args := url.Values{"attempt_id": {p.Attempt.ID}}
	if p.ProviderRequestID != "" {
		args.Set("request_id", p.ProviderRequestID)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		endpoint(p.Attempt.Route, "/usage")+"?"+args.Encode(), nil)
	if err != nil {
		return core.Evidence{}, errors.New("construct usage lookup")
	}
	req.Header.Set("Accept", "application/json")
	resp, err := s.upstream.Do(req)
	if err != nil {
		return core.Evidence{}, errors.New("usage lookup transport failure")
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusAccepted {
		return core.Evidence{}, nil
	}
	if resp.StatusCode != http.StatusOK {
		return core.Evidence{}, fmt.Errorf("usage lookup returned HTTP %d", resp.StatusCode)
	}
	var reply UsageReply
	if err := decodeLimited(resp.Body, maxUpstreamBody, &reply); err != nil {
		return core.Evidence{}, errors.New("malformed usage response")
	}
	if reply.AttemptID != p.Attempt.ID || !validLabel(reply.RequestID) ||
		(p.ProviderRequestID != "" && reply.RequestID != p.ProviderRequestID) || reply.Usage == nil {
		return core.Evidence{}, errors.New("usage response does not establish usage for this attempt")
	}
	actual, ok := cost(p.Attempt.Route, reply.Usage.InputTokens, reply.Usage.OutputTokens)
	if !ok {
		return core.Evidence{}, errors.New("invalid usage counters")
	}
	switch reply.Code {
	case outcomeOK, outcomeSafety, outcomeCapability, outcomeUnavailable:
	default:
		return core.Evidence{}, errors.New("unrecognized usage outcome")
	}
	return core.Evidence{
		Final: true, Actual: actual, ProviderRequestID: reply.RequestID,
		Ref: "usage:" + reply.RequestID, Outcome: reply.Code,
	}, nil
}

func (s *Server) knownUsageRoute(a core.Attempt) bool {
	pc, ok := s.clients[a.Principal.Client]
	if !ok || pc.environment != a.Route.Environment {
		return false
	}
	for _, r := range pc.routes {
		h := a.Route
		if r.ID == h.ID && r.Provider == h.Provider && r.Account == h.Account && r.Model == h.Model &&
			r.Endpoint == h.Endpoint && r.Protocol == h.Protocol && r.AuthEnv == h.AuthEnv && r.Environment == h.Environment && r.Region == h.Region &&
			h.InputUnit >= 0 && h.InputUnit <= maxUnitRate && h.OutputUnit >= 0 && h.OutputUnit <= maxUnitRate {
			return true
		}
	}
	return false
}

func cost(r core.Route, input, output int64) (int64, bool) {
	if input < 0 || input > maxUsageTokens || output < 0 || output > maxUsageTokens {
		return 0, false
	}
	// Both counts and configured unit prices are bounded at 1e9.
	return input*r.InputUnit + output*r.OutputUnit, true
}

func decodeLimited(r io.Reader, limit int64, dst any) error {
	b, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return err
	}
	if int64(len(b)) > limit {
		return errors.New("JSON body exceeds limit")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return errors.New("unexpected trailing JSON")
	}
	return nil
}

func transportOutcome(ctx context.Context, err error) providerOutcome {
	if ctx.Err() == context.Canceled {
		return unknownOutcome()
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return providerOutcome{code: outcomeUnavailable, evidence: core.Evidence{Outcome: outcomeUnavailable}}
	}
	return unknownOutcome()
}
