package core

import "context"

// Principal binds an authenticated workload to its configured client and scope.
type Principal struct {
	Client     string `json:"client"`
	Workload   string `json:"workload"`
	Department string `json:"department,omitempty"`
	App        string `json:"app,omitempty"`
	Role       string `json:"role,omitempty"`
}

type Route struct {
	TimeoutMS        int      `json:"timeout_ms,omitempty"`
	RequiresApproval bool     `json:"requires_approval,omitempty"`
	Protocol         string   `json:"protocol,omitempty"`
	AuthEnv          string   `json:"auth_env,omitempty"`
	ID               string   `json:"id"`
	Provider         string   `json:"provider"`
	Account          string   `json:"account"`
	Model            string   `json:"model"`
	Region           string   `json:"region"`
	Environment      string   `json:"environment"`
	Endpoint         string   `json:"endpoint"`
	Capabilities     []string `json:"capabilities"`
	InputUnit        int64    `json:"input_unit"`
	OutputUnit       int64    `json:"output_unit"`
}

type Attempt struct {
	ApprovalID    string    `json:"approval_id,omitempty"`
	ID            string    `json:"id"`
	RequestID     string    `json:"request_id"`
	Principal     Principal `json:"principal"`
	Route         Route     `json:"route"`
	PolicyVersion string    `json:"policy_version"`
	Rule          string    `json:"rule"`
	Estimate      int64     `json:"estimate"`
}

type Evidence struct {
	Final             bool
	Actual            int64
	Ref               string
	ProviderRequestID string
	Outcome           string
}

type Pending struct {
	Attempt           Attempt
	ProviderRequestID string
}

type Event struct {
	Kind      string
	Principal Principal
	RequestID string
	AttemptID string
	Detail    map[string]any
}

type Store interface {
	Reserve(context.Context, Attempt) (bool, error)
	// MarkDispatch is durably recorded before any upstream bytes may be sent.
	MarkDispatch(context.Context, string) error
	MarkUncertain(context.Context, string, Evidence) error
	Settle(context.Context, string, Evidence) (bool, error)
	Audit(context.Context, Event) error
}

type UsageLookup interface {
	Lookup(context.Context, Pending) (Evidence, error)
}
