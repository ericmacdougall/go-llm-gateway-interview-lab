package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"example.com/llmgateway/internal/approval"
	"example.com/llmgateway/internal/core"
	"example.com/llmgateway/internal/ledger"
)

type Authenticator interface {
	Principal(*http.Request) (core.Principal, bool)
}

type Handler struct {
	Service *approval.Service
	Auth    Authenticator
	Next    http.Handler
}

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/approvals" && !strings.HasPrefix(r.URL.Path, "/v1/approvals/") {
		h.Next.ServeHTTP(w, r)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	r = r.WithContext(ctx)
	p, ok := h.Auth.Principal(r)
	if !ok {
		reply(w, 401, map[string]string{"error": "invalid_identity"})
		return
	}
	for _, header := range []string{"X-Client-ID", "X-Tenant-ID", "X-Workload-ID", "X-User-ID", "X-Department", "X-App", "X-Role"} {
		if r.Header.Get(header) != "" {
			reply(w, 400, map[string]string{"error": "identity_override"})
			return
		}
	}
	var result ledger.Approval
	var err error
	id := strings.TrimPrefix(r.URL.Path, "/v1/approvals/")
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/v1/approvals":
		var q struct {
			ID    string `json:"id"`
			Model string `json:"model"`
			TTL   int64  `json:"ttl_seconds"`
		}
		if decode(r, &q) != nil || q.TTL < 1 || q.TTL > 86400 {
			reply(w, 400, map[string]string{"error": "invalid_request"})
			return
		}
		result, err = h.Service.Request(r.Context(), p, q.ID, q.Model, time.Duration(q.TTL)*time.Second)
	case r.Method == http.MethodGet && id != "" && !strings.Contains(id, "/"):
		result, err = h.Service.Get(r.Context(), p, id)
	case r.Method == http.MethodPost && strings.HasSuffix(id, "/decision"):
		var q struct {
			Allow  *bool  `json:"allow"`
			Reason string `json:"reason"`
		}
		id = strings.TrimSuffix(id, "/decision")
		if id == "" || strings.Contains(id, "/") || decode(r, &q) != nil || q.Allow == nil {
			reply(w, 400, map[string]string{"error": "invalid_request"})
			return
		}
		result, err = h.Service.Decide(r.Context(), p, id, *q.Allow, q.Reason)
	case r.Method == http.MethodPost && strings.HasSuffix(id, "/revoke"):
		var q struct {
			Reason string `json:"reason"`
		}
		id = strings.TrimSuffix(id, "/revoke")
		if id == "" || strings.Contains(id, "/") || decode(r, &q) != nil {
			reply(w, 400, map[string]string{"error": "invalid_request"})
			return
		}
		result, err = h.Service.Revoke(r.Context(), p, id, q.Reason)
	default:
		reply(w, 404, map[string]string{"error": "not_found"})
		return
	}
	if err != nil {
		status, code := 503, "approval_unavailable"
		switch {
		case errors.Is(err, approval.ErrInvalid):
			status, code = 400, "invalid_request"
		case errors.Is(err, approval.ErrForbidden):
			status, code = 403, "forbidden"
		case errors.Is(err, approval.ErrNotFound):
			status, code = 404, "not_found"
		case errors.Is(err, approval.ErrConflict):
			status, code = 409, "conflict"
		}
		reply(w, status, map[string]string{"error": code})
		return
	}
	reply(w, 200, result)
}

func decode(r *http.Request, dst any) error {
	defer r.Body.Close()
	data, err := io.ReadAll(io.LimitReader(r.Body, 16385))
	if err != nil {
		return err
	}
	if len(data) > 16384 {
		return errors.New("body too large")
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return errors.New("trailing content")
	}
	return nil
}
func reply(w http.ResponseWriter, status int, v any) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
