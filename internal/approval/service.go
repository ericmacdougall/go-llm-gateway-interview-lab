// Package approval provides expiring model-access grants within configured scope.
// A grant supplements model access. Gateway sovereignty and budget checks still
// decide whether an attempt can dispatch.
package approval

import (
	"context"
	"errors"
	"time"

	"example.com/llmgateway/internal/core"
	"example.com/llmgateway/internal/ledger"
)

var (
	ErrForbidden = ledger.ErrApprovalForbidden
	ErrNotFound  = ledger.ErrApprovalNotFound
	ErrInvalid   = ledger.ErrApprovalInvalid
	ErrConflict  = ledger.ErrApprovalConflict
)

type Config struct {
	ApproverRole string
	Models       map[string][]string
	MaxTTL       time.Duration
}

type Service struct {
	store  *ledger.Store
	role   string
	models map[string]map[string]bool
	maxTTL time.Duration
}

func New(store *ledger.Store, cfg Config) (*Service, error) {
	if store == nil || cfg.ApproverRole == "" || cfg.MaxTTL < time.Second ||
		cfg.MaxTTL > 24*time.Hour || len(cfg.Models) == 0 {
		return nil, ErrInvalid
	}
	s := &Service{store: store, role: cfg.ApproverRole, maxTTL: cfg.MaxTTL, models: make(map[string]map[string]bool)}
	for client, models := range cfg.Models {
		if client == "" || len(models) == 0 {
			return nil, ErrInvalid
		}
		s.models[client] = make(map[string]bool)
		for _, model := range models {
			if model == "" || len(model) > 256 {
				return nil, ErrInvalid
			}
			s.models[client][model] = true
		}
	}
	return s, nil
}

func (s *Service) Request(ctx context.Context, p core.Principal, id, model string, ttl time.Duration) (ledger.Approval, error) {
	if !s.models[p.Client][model] {
		return ledger.Approval{}, ErrForbidden
	}
	if ttl > s.maxTTL {
		return ledger.Approval{}, ErrInvalid
	}
	return s.store.RequestApproval(ctx, p, id, model, ttl)
}

func (s *Service) Get(ctx context.Context, p core.Principal, id string) (ledger.Approval, error) {
	if p.Client == "" || p.Workload == "" {
		return ledger.Approval{}, ErrForbidden
	}
	a, err := s.store.GetApproval(ctx, p.Client, id)
	if err != nil {
		return ledger.Approval{}, err
	}
	if a.Workload != p.Workload && p.Role != s.role {
		// Hide other workloads' requests from non-approvers.
		return ledger.Approval{}, ErrNotFound
	}
	return a, nil
}

func (s *Service) Decide(ctx context.Context, p core.Principal, id string, allow bool, reason string) (ledger.Approval, error) {
	if p.Role != s.role || p.Client == "" || p.Workload == "" {
		return ledger.Approval{}, ErrForbidden
	}
	// Recheck configured model scope so stale grants cannot approve a removed model.
	a, err := s.store.GetApproval(ctx, p.Client, id)
	if err != nil {
		return ledger.Approval{}, err
	}
	if !s.models[p.Client][a.Model] {
		return ledger.Approval{}, ErrForbidden
	}
	return s.store.DecideApproval(ctx, p, id, allow, reason, s.role)
}

func (s *Service) Revoke(ctx context.Context, p core.Principal, id, reason string) (ledger.Approval, error) {
	return s.store.RevokeApproval(ctx, p, id, reason, s.role)
}

func (s *Service) Matching(ctx context.Context, p core.Principal, model string) (ledger.Approval, error) {
	if !s.models[p.Client][model] || p.Workload == "" {
		return ledger.Approval{}, ErrForbidden
	}
	return s.store.MatchingApproval(ctx, p, model)
}

func (s *Service) Allows(ctx context.Context, p core.Principal, model string) (bool, error) {
	_, err := s.Matching(ctx, p, model)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

// GrantID returns the exact active grant to persist with the provider attempt.
// Dispatch rechecks this grant transactionally after reservation.
func (s *Service) GrantID(ctx context.Context, p core.Principal, model string) (string, error) {
	a, err := s.Matching(ctx, p, model)
	if errors.Is(err, ErrNotFound) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return a.ID, nil
}
