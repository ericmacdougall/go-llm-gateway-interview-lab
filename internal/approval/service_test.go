package approval

import (
	"context"
	"errors"
	"testing"
	"time"

	"example.com/llmgateway/internal/core"
	"example.com/llmgateway/internal/ledger"
)

func TestServiceRejectsUnconfiguredScopesBeforeStorage(t *testing.T) {
	models := map[string][]string{"client": {"model-a"}}
	s, err := New(&ledger.Store{}, Config{ApproverRole: "reviewer", Models: models, MaxTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	models["client"][0] = "model-b"
	delete(models, "client")
	if !s.models["client"]["model-a"] || s.models["client"]["model-b"] {
		t.Fatal("configuration mutation changed service scope")
	}
	ctx := context.Background()
	p := core.Principal{Client: "client", Workload: "workload"}
	if _, err := s.Request(ctx, p, "a", "model-b", time.Minute); !errors.Is(err, ErrForbidden) {
		t.Fatalf("unconfigured model accepted: %v", err)
	}
	if _, err := s.Request(ctx, core.Principal{Client: "foreign", Workload: "workload"}, "a", "model-a", time.Minute); !errors.Is(err, ErrForbidden) {
		t.Fatalf("foreign client accepted: %v", err)
	}
	if _, err := s.Request(ctx, p, "a", "model-a", 2*time.Hour); !errors.Is(err, ErrInvalid) {
		t.Fatalf("TTL above configured maximum accepted: %v", err)
	}
	if _, err := s.Decide(ctx, p, "a", true, "approve"); !errors.Is(err, ErrForbidden) {
		t.Fatalf("unprivileged decision reached storage: %v", err)
	}
	if allowed, err := s.Allows(ctx, p, "model-b"); allowed || !errors.Is(err, ErrForbidden) {
		t.Fatalf("unconfigured model allowed: %v %v", allowed, err)
	}
}

func TestServiceRequiresCompleteConfiguration(t *testing.T) {
	if _, err := New(nil, Config{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := New(&ledger.Store{}, Config{ApproverRole: "reviewer", Models: map[string][]string{"": {"model"}}, MaxTTL: time.Hour}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}
