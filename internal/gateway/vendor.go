package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	"example.com/llmgateway/internal/core"
)

type openAIReply struct {
	ID      string `json:"id"`
	Choices []struct {
		FinishReason string `json:"finish_reason"`
		Message      struct {
			Content   *string           `json:"content"`
			Refusal   *string           `json:"refusal"`
			ToolCalls []json.RawMessage `json:"tool_calls"`
		} `json:"message"`
	} `json:"choices"`
	Usage *struct {
		Input  *int64 `json:"prompt_tokens"`
		Output *int64 `json:"completion_tokens"`
	} `json:"usage"`
}

type anthropicReply struct {
	ID         string `json:"id"`
	Type       string `json:"type"`
	StopReason string `json:"stop_reason"`
	Content    []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	Usage *struct {
		Input         *int64 `json:"input_tokens"`
		Output        *int64 `json:"output_tokens"`
		CacheCreation int64  `json:"cache_creation_input_tokens"`
		CacheRead     int64  `json:"cache_read_input_tokens"`
	} `json:"usage"`
}

type geminiPart struct {
	Text         string          `json:"text,omitempty"`
	Thought      bool            `json:"thought,omitempty"`
	FunctionCall json.RawMessage `json:"functionCall,omitempty"`
	InlineData   json.RawMessage `json:"inlineData,omitempty"`
}

type geminiContent struct {
	Role  string       `json:"role,omitempty"`
	Parts []geminiPart `json:"parts"`
}

type geminiReply struct {
	ResponseID string `json:"responseId"`
	Candidates []struct {
		Content      geminiContent `json:"content"`
		FinishReason string        `json:"finishReason"`
	} `json:"candidates"`
	PromptFeedback struct {
		BlockReason string `json:"blockReason"`
	} `json:"promptFeedback"`
	Usage *struct {
		Input     *int64 `json:"promptTokenCount"`
		Output    *int64 `json:"candidatesTokenCount"`
		Thoughts  int64  `json:"thoughtsTokenCount"`
		ToolInput int64  `json:"toolUsePromptTokenCount"`
	} `json:"usageMetadata"`
}

func (s *Server) providerRequest(ctx context.Context, a core.Attempt, q completionRequest, stream bool) (*http.Request, error) {
	var payload any
	var suffix string
	switch a.Route.Protocol {
	case "openai":
		payload = map[string]any{
			"model": a.Route.Model, "messages": q.Messages, "max_completion_tokens": q.MaxTokens,
			"stream": stream, "n": 1, "store": false,
		}
		if stream {
			payload.(map[string]any)["stream_options"] = map[string]bool{"include_usage": true}
		}
		suffix = "/v1/chat/completions"
	case "openrouter":
		payload = map[string]any{
			"model": a.Route.Model, "messages": q.Messages,
			"max_tokens": q.MaxTokens, "stream": stream,
		}
		suffix = "/api/v1/chat/completions"
	case "anthropic":
		var system []string
		var messages []Message
		for _, m := range q.Messages {
			if m.Role == "system" {
				system = append(system, m.Content)
			} else {
				messages = append(messages, m)
			}
		}
		if len(messages) == 0 {
			return nil, errors.New("anthropic requires a conversation message")
		}
		payload = map[string]any{
			"model": a.Route.Model, "messages": messages, "max_tokens": q.MaxTokens, "stream": stream,
		}
		if len(system) != 0 {
			payload.(map[string]any)["system"] = strings.Join(system, "\n\n")
		}
		suffix = "/v1/messages"
	case "gemini":
		var system []geminiPart
		var contents []geminiContent
		for _, m := range q.Messages {
			if m.Role == "system" {
				system = append(system, geminiPart{Text: m.Content})
			} else {
				role := m.Role
				if role == "assistant" {
					role = "model"
				}
				contents = append(contents, geminiContent{role, []geminiPart{{Text: m.Content}}})
			}
		}
		if len(contents) == 0 {
			return nil, errors.New("gemini requires conversation content")
		}
		payload = map[string]any{
			"contents": contents, "generationConfig": map[string]any{"maxOutputTokens": q.MaxTokens, "candidateCount": 1},
		}
		if len(system) != 0 {
			payload.(map[string]any)["systemInstruction"] = geminiContent{Parts: system}
		}
		model := strings.TrimPrefix(a.Route.Model, "models/")
		suffix = "/v1beta/models/" + url.PathEscape(model) + ":generateContent"
		if stream {
			suffix = "/v1beta/models/" + url.PathEscape(model) + ":streamGenerateContent?alt=sse"
		}
	default:
		return nil, errors.New("unknown provider protocol")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint(a.Route, suffix), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	credential, ok := s.credentials[a.Route.AuthEnv]
	if !ok || credential == "" {
		return nil, errors.New("provider credential unavailable")
	}
	switch a.Route.Protocol {
	case "openai", "openrouter":
		req.Header.Set("Authorization", "Bearer "+credential)
		if a.Route.Protocol == "openrouter" {
			req.Header.Set("X-OpenRouter-Title", "Go gateway reference")
		}
	case "anthropic":
		req.Header.Set("x-api-key", credential)
		req.Header.Set("anthropic-version", "2023-06-01")
	case "gemini":
		req.Header.Set("x-goog-api-key", credential)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if stream {
		req.Header.Set("Accept", "text/event-stream")
	}
	req.Header.Set("X-Client-Request-ID", a.ID)
	return req, nil
}

func (s *Server) invokeProvider(ctx context.Context, a core.Attempt, q completionRequest) providerOutcome {
	req, err := s.providerRequest(ctx, a, q, false)
	if err != nil {
		return unknownOutcome()
	}
	resp, err := s.upstream.Do(req)
	if err != nil {
		return transportOutcome(ctx, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return providerFailure(resp)
	}
	return decodeProviderReply(a, resp)
}

func decodeProviderReply(a core.Attempt, resp *http.Response) providerOutcome {
	ref := resp.Header.Get("X-Request-ID")
	switch a.Route.Protocol {
	case "openai", "openrouter":
		var reply openAIReply
		if decodeVendorJSON(resp.Body, &reply) != nil || len(reply.Choices) != 1 {
			return unknownOutcome()
		}
		if !safeRequestID(ref) {
			ref = reply.ID
		}
		e := providerEvidence(a.Route, ref, nil, nil)
		if reply.Usage != nil {
			e = providerEvidence(a.Route, ref, reply.Usage.Input, reply.Usage.Output)
		}
		c := reply.Choices[0]
		if c.FinishReason == "content_filter" || c.Message.Refusal != nil && *c.Message.Refusal != "" {
			return normalizedOutcome(outcomeSafety, "", e)
		}
		if c.FinishReason != "stop" && c.FinishReason != "length" || len(c.Message.ToolCalls) != 0 ||
			c.Message.Content == nil || *c.Message.Content == "" {
			return normalizedOutcome(outcomeUnknown, "", e)
		}
		return normalizedOutcome(outcomeOK, *c.Message.Content, e)
	case "anthropic":
		var reply anthropicReply
		if decodeVendorJSON(resp.Body, &reply) != nil || reply.Type != "message" {
			return unknownOutcome()
		}
		if !safeRequestID(ref) {
			ref = reply.ID
		}
		e := providerEvidence(a.Route, ref, nil, nil)
		if reply.Usage != nil {
			input, ok := sumUsage(reply.Usage.Input, reply.Usage.CacheCreation, reply.Usage.CacheRead)
			if ok {
				e = providerEvidence(a.Route, ref, &input, reply.Usage.Output)
			}
		}
		if reply.StopReason == "refusal" {
			return normalizedOutcome(outcomeSafety, "", e)
		}
		if reply.StopReason != "end_turn" && reply.StopReason != "max_tokens" && reply.StopReason != "stop_sequence" {
			return normalizedOutcome(outcomeUnknown, "", e)
		}
		var text strings.Builder
		for _, b := range reply.Content {
			if b.Type != "text" {
				return normalizedOutcome(outcomeUnknown, "", e)
			}
			text.WriteString(b.Text)
		}
		return normalizedOutcome(outcomeOK, text.String(), e)
	case "gemini":
		var reply geminiReply
		if decodeVendorJSON(resp.Body, &reply) != nil {
			return unknownOutcome()
		}
		if !safeRequestID(ref) {
			ref = reply.ResponseID
		}
		e := geminiEvidence(a.Route, ref, reply)
		if reply.PromptFeedback.BlockReason != "" && reply.PromptFeedback.BlockReason != "BLOCK_REASON_UNSPECIFIED" {
			return normalizedOutcome(outcomeSafety, "", e)
		}
		if len(reply.Candidates) != 1 {
			return normalizedOutcome(outcomeUnknown, "", e)
		}
		c := reply.Candidates[0]
		if geminiSafety(c.FinishReason) {
			return normalizedOutcome(outcomeSafety, "", e)
		}
		if c.FinishReason != "STOP" && c.FinishReason != "MAX_TOKENS" {
			return normalizedOutcome(outcomeUnknown, "", e)
		}
		var text strings.Builder
		for _, p := range c.Content.Parts {
			if len(p.FunctionCall) != 0 || len(p.InlineData) != 0 {
				return normalizedOutcome(outcomeUnknown, "", e)
			}
			if !p.Thought {
				text.WriteString(p.Text)
			}
		}
		return normalizedOutcome(outcomeOK, text.String(), e)
	}
	return unknownOutcome()
}

func providerEvidence(route core.Route, ref string, input, output *int64) core.Evidence {
	e := core.Evidence{Outcome: outcomeUnknown}
	if safeRequestID(ref) {
		e.ProviderRequestID = ref
	}
	if input == nil || output == nil {
		return e
	}
	actual, ok := cost(route, *input, *output)
	if ok {
		e.Final, e.Actual = true, actual
		if e.ProviderRequestID != "" {
			e.Ref = "response:" + e.ProviderRequestID
		} else {
			// Matched synchronous response establishes usage without a provider ID.
			e.Ref = "response:matched-attempt"
		}
	}
	return e
}

func safeRequestID(id string) bool {
	if len(id) == 0 || len(id) > 200 {
		return false
	}
	for _, r := range id {
		if r < 33 || r > 126 {
			return false
		}
	}
	return true
}

func normalizedOutcome(code, text string, e core.Evidence) providerOutcome {
	e.Outcome = code
	if code == outcomeOK && (text == "" || !e.Final) {
		e.Outcome = outcomeUnknown
		return providerOutcome{code: outcomeUnknown, evidence: e}
	}
	return providerOutcome{code: code, text: text, evidence: e}
}

func geminiSafety(reason string) bool {
	switch reason {
	case "SAFETY", "RECITATION", "BLOCKLIST", "PROHIBITED_CONTENT", "SPII", "IMAGE_SAFETY", "IMAGE_PROHIBITED_CONTENT", "MODEL_ARMOR":
		return true
	}
	return false
}

func sumUsage(base *int64, other ...int64) (int64, bool) {
	if base == nil || *base < 0 || *base > maxUsageTokens {
		return 0, false
	}
	total := *base
	for _, n := range other {
		if n < 0 || n > maxUsageTokens || total > maxUsageTokens-n {
			return 0, false
		}
		total += n
	}
	return total, true
}

func geminiEvidence(route core.Route, ref string, reply geminiReply) core.Evidence {
	if reply.Usage == nil {
		return providerEvidence(route, ref, nil, nil)
	}
	in, okIn := sumUsage(reply.Usage.Input, reply.Usage.ToolInput)
	out, okOut := sumUsage(reply.Usage.Output, reply.Usage.Thoughts)
	if !okIn || !okOut {
		return providerEvidence(route, ref, nil, nil)
	}
	return providerEvidence(route, ref, &in, &out)
}

func decodeVendorJSON(r io.Reader, out any) error {
	b, err := io.ReadAll(io.LimitReader(r, maxUpstreamBody+1))
	if err != nil || len(b) > maxUpstreamBody {
		return errors.New("invalid provider response")
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	if dec.Decode(out) != nil || dec.Decode(new(any)) != io.EOF {
		return errors.New("invalid provider JSON")
	}
	return nil
}

func providerFailure(resp *http.Response) providerOutcome {
	var envelope struct {
		Error struct {
			Type   string `json:"type"`
			Code   string `json:"code"`
			Status string `json:"status"`
			Param  string `json:"param"`
		} `json:"error"`
	}
	// Failure bodies are never returned or logged because they can echo input.
	_ = decodeVendorJSON(resp.Body, &envelope)
	ref := resp.Header.Get("X-Request-ID")
	e := providerEvidence(core.Route{}, ref, nil, nil)
	code := outcomeUnknown
	switch {
	case envelope.Error.Code == "content_policy_violation" || envelope.Error.Type == "refusal":
		code = outcomeSafety
	case resp.StatusCode == 429 || resp.StatusCode == 502 || resp.StatusCode == 503 ||
		resp.StatusCode == 504 || resp.StatusCode == 529:
		code = outcomeUnavailable
	case resp.StatusCode == 400 && envelope.Error.Code == "unsupported_parameter":
		code = outcomeCapability
	case resp.StatusCode == 400 && envelope.Error.Status == "UNIMPLEMENTED":
		code = outcomeCapability
	}
	return normalizedOutcome(code, "", e)
}
