package gateway

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"example.com/llmgateway/internal/core"
)

type streamState struct {
	code          string
	ref           string
	input, output *int64
	total         int
	sawEnd        bool
	text          strings.Builder
}

func (s *Server) invokeStream(ctx context.Context, a core.Attempt, q completionRequest, emit func(string) error) providerOutcome {
	req, err := s.providerRequest(ctx, a, q, true)
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
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		return unknownOutcome()
	}
	state := streamState{code: outcomeUnknown, ref: resp.Header.Get("X-Request-ID")}
	err = readSSE(resp.Body, func(data string) error {
		return state.accept(a.Route.Protocol, data, emit)
	})
	e := providerEvidence(a.Route, state.ref, nil, nil)
	if err != nil {
		if state.code == outcomeSafety {
			return normalizedOutcome(outcomeSafety, "", e)
		}
		return normalizedOutcome(outcomeUnknown, "", e)
	}
	if state.sawEnd {
		e = providerEvidence(a.Route, state.ref, state.input, state.output)
	}
	return normalizedOutcome(state.code, state.text.String(), e)
}

func readSSE(reader io.Reader, accept func(string) error) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), 64<<10)
	var lines []string
	eventSize, total := 0, 0
	dispatch := func() error {
		if len(lines) == 0 {
			return nil
		}
		data := strings.Join(lines, "\n")
		lines, eventSize = nil, 0
		return accept(data)
	}
	for scanner.Scan() {
		line := scanner.Text()
		total += len(line) + 1
		if total > maxUpstreamBody {
			return errors.New("provider stream exceeds limit")
		}
		if line == "" {
			if err := dispatch(); err != nil {
				return err
			}
			continue
		}
		if strings.HasPrefix(line, "data:") {
			value := strings.TrimPrefix(line, "data:")
			value = strings.TrimPrefix(value, " ")
			eventSize += len(value)
			if eventSize > 64<<10 {
				return errors.New("provider event exceeds limit")
			}
			lines = append(lines, value)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return dispatch()
}

func (s *streamState) add(text string, emit func(string) error) error {
	if text == "" {
		return nil
	}
	s.total += len(text)
	if s.total > maxUpstreamBody {
		return errors.New("provider output exceeds limit")
	}
	if err := emit(text); err != nil {
		return err
	}
	s.text.WriteString(text)
	return nil
}

func (s *streamState) accept(protocol, data string, emit func(string) error) error {
	if (protocol == "openai" || protocol == "openrouter") && data == "[DONE]" {
		s.sawEnd = true
		return nil
	}
	if s.sawEnd {
		return errors.New("provider data after terminal event")
	}
	switch protocol {
	case "openai", "openrouter":
		return s.acceptOpenAI(data, emit)
	case "anthropic":
		return s.acceptAnthropic(data, emit)
	case "gemini":
		return s.acceptGemini(data, emit)
	}
	return errors.New("unknown stream protocol")
}

func (s *streamState) setID(id string) error {
	if !safeRequestID(id) {
		return nil
	}
	// HTTP request IDs and body completion IDs may differ. Prefer a stable body ID.
	s.ref = id
	return nil
}

func (s *streamState) acceptOpenAI(data string, emit func(string) error) error {
	var chunk struct {
		ID      string `json:"id"`
		Choices []struct {
			Delta struct {
				Content   string            `json:"content"`
				Refusal   string            `json:"refusal"`
				ToolCalls []json.RawMessage `json:"tool_calls"`
			} `json:"delta"`
			FinishReason *string `json:"finish_reason"`
		} `json:"choices"`
		Usage *struct {
			Input  *int64 `json:"prompt_tokens"`
			Output *int64 `json:"completion_tokens"`
		} `json:"usage"`
		Error json.RawMessage `json:"error"`
	}
	if json.Unmarshal([]byte(data), &chunk) != nil || len(chunk.Error) != 0 || len(chunk.Choices) > 1 {
		return errors.New("invalid OpenAI stream event")
	}
	_ = s.setID(chunk.ID)
	if chunk.Usage != nil {
		s.input, s.output = chunk.Usage.Input, chunk.Usage.Output
	}
	for _, choice := range chunk.Choices {
		if choice.Delta.Refusal != "" || choice.FinishReason != nil && *choice.FinishReason == "content_filter" {
			s.code = outcomeSafety
			return errors.New("provider refusal")
		}
		if len(choice.Delta.ToolCalls) != 0 {
			return errors.New("unsupported tool stream")
		}
		if err := s.add(choice.Delta.Content, emit); err != nil {
			return err
		}
		if choice.FinishReason != nil {
			switch *choice.FinishReason {
			case "stop", "length":
				s.code = outcomeOK
			default:
				return errors.New("unknown provider stop reason")
			}
		}
	}
	return nil
}

func (s *streamState) acceptAnthropic(data string, emit func(string) error) error {
	var chunk struct {
		Type         string         `json:"type"`
		Message      anthropicReply `json:"message"`
		ContentBlock struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content_block"`
		Delta struct {
			Type       string  `json:"type"`
			Text       string  `json:"text"`
			StopReason *string `json:"stop_reason"`
		} `json:"delta"`
		Usage *struct {
			Output *int64 `json:"output_tokens"`
		} `json:"usage"`
	}
	if json.Unmarshal([]byte(data), &chunk) != nil {
		return errors.New("invalid Anthropic stream event")
	}
	switch chunk.Type {
	case "ping", "content_block_stop":
		return nil
	case "message_start":
		_ = s.setID(chunk.Message.ID)
		if chunk.Message.Usage != nil {
			input, ok := sumUsage(chunk.Message.Usage.Input,
				chunk.Message.Usage.CacheCreation, chunk.Message.Usage.CacheRead)
			if ok {
				s.input = &input
			}
		}
	case "content_block_start":
		if chunk.ContentBlock.Type != "text" {
			return errors.New("unsupported content block")
		}
		return s.add(chunk.ContentBlock.Text, emit)
	case "content_block_delta":
		if chunk.Delta.Type != "text_delta" {
			return errors.New("unsupported content delta")
		}
		return s.add(chunk.Delta.Text, emit)
	case "message_delta":
		if chunk.Usage != nil {
			s.output = chunk.Usage.Output
		}
		if chunk.Delta.StopReason != nil {
			switch *chunk.Delta.StopReason {
			case "end_turn", "max_tokens", "stop_sequence":
				s.code = outcomeOK
			case "refusal":
				s.code = outcomeSafety
				return errors.New("provider refusal")
			default:
				return errors.New("unknown provider stop reason")
			}
		}
	case "message_stop":
		s.sawEnd = true
	default:
		return errors.New("unrecognized Anthropic stream event")
	}
	return nil
}

func (s *streamState) acceptGemini(data string, emit func(string) error) error {
	var reply geminiReply
	if json.Unmarshal([]byte(data), &reply) != nil {
		return errors.New("invalid Gemini stream event")
	}
	_ = s.setID(reply.ResponseID)
	if reply.PromptFeedback.BlockReason != "" && reply.PromptFeedback.BlockReason != "BLOCK_REASON_UNSPECIFIED" {
		s.code = outcomeSafety
		return errors.New("provider refusal")
	}
	if reply.Usage != nil {
		input, okInput := sumUsage(reply.Usage.Input, reply.Usage.ToolInput)
		output, okOutput := sumUsage(reply.Usage.Output, reply.Usage.Thoughts)
		if okInput && okOutput {
			s.input, s.output = &input, &output
		}
	}
	if len(reply.Candidates) > 1 {
		return errors.New("multiple provider candidates")
	}
	for _, c := range reply.Candidates {
		if geminiSafety(c.FinishReason) {
			s.code = outcomeSafety
			return errors.New("provider refusal")
		}
		for _, part := range c.Content.Parts {
			if len(part.FunctionCall) != 0 || len(part.InlineData) != 0 {
				return errors.New("unsupported provider part")
			}
			if !part.Thought {
				if err := s.add(part.Text, emit); err != nil {
					return err
				}
			}
		}
		if c.FinishReason != "" {
			if c.FinishReason != "STOP" && c.FinishReason != "MAX_TOKENS" {
				return errors.New("unknown provider stop reason")
			}
			s.code, s.sawEnd = outcomeOK, true
		}
	}
	return nil
}

type completionStream struct {
	w         http.ResponseWriter
	id, model string
	committed bool
}

func (s *completionStream) emit(text string) error {
	if !s.committed {
		s.w.Header().Set("Content-Type", "text/event-stream")
		s.w.Header().Set("Cache-Control", "no-store")
		s.w.Header().Set("X-Accel-Buffering", "no")
		s.committed = true
	}
	if err := s.write(map[string]any{
		"id": s.id, "model": s.model, "object": "chat.completion.chunk",
		"choices": []any{map[string]any{"index": 0, "delta": map[string]string{"content": text}}},
	}); err != nil {
		return err
	}
	return http.NewResponseController(s.w).Flush()
}

func (s *completionStream) write(value any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(s.w, "data: %s\n\n", body)
	return err
}

func (s *completionStream) finish(code string) {
	if code == outcomeOK {
		_ = s.write(map[string]any{
			"id": s.id, "model": s.model, "object": "chat.completion.chunk",
			"choices": []any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": "stop"}},
		})
	} else {
		_ = s.write(map[string]any{"error": map[string]string{"code": code, "message": "stream ended without a complete result"}})
	}
	_, _ = fmt.Fprint(s.w, "data: [DONE]\n\n")
	_ = http.NewResponseController(s.w).Flush()
}
