// Package gateway implements the Go gateway reference request path.
package gateway

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"cel.dev/cel-go/cel"
	"example.com/llmgateway/internal/core"
)

type Config struct {
	Identities        map[string]core.Principal `json:"identities"`
	Clients           map[string]ClientPolicy   `json:"clients"`
	MaxAttempts       int                       `json:"max_attempts"`
	RequestTimeoutMS  int                       `json:"request_timeout_ms"`
	AttemptTimeoutMS  int                       `json:"attempt_timeout_ms"`
	AllowLoopbackHTTP bool                      `json:"allow_loopback_http"`
}

type ClientPolicy struct {
	Version     string       `json:"version"`
	Rule        string       `json:"rule"`
	Expression  string       `json:"expression"`
	Environment string       `json:"environment"`
	Routes      []core.Route `json:"routes"`
}

type clientPolicy struct {
	version, rule, environment string
	routes                     []core.Route
	program                    cel.Program
}

type ApprovalChecker interface {
	Allows(context.Context, core.Principal, string) (bool, error)
}

type Server struct {
	approvals      ApprovalChecker
	store          core.Store
	clients        map[string]clientPolicy
	identities     map[string]core.Principal
	upstream       *http.Client
	maxAttempts    int
	requestTimeout time.Duration
	attemptTimeout time.Duration
	credentials    map[string]string
}

const maxUnitRate = 1_000_000_000

var trustDomainPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,252}$`)
var labelPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.:/-]{0,127}$`)

func New(cfg Config, store core.Store) (*Server, error) {
	if store == nil {
		return nil, errors.New("gateway store is required")
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 2
	}
	if cfg.RequestTimeoutMS == 0 {
		cfg.RequestTimeoutMS = 5000
	}
	if cfg.AttemptTimeoutMS == 0 {
		cfg.AttemptTimeoutMS = min(cfg.RequestTimeoutMS, 2000)
	}
	if cfg.MaxAttempts < 1 || cfg.MaxAttempts > 8 ||
		cfg.RequestTimeoutMS < 1 || cfg.RequestTimeoutMS > 120_000 ||
		cfg.AttemptTimeoutMS < 1 || cfg.AttemptTimeoutMS > cfg.RequestTimeoutMS {
		return nil, errors.New("invalid attempt limit or request deadlines")
	}

	env, err := policyEnvironment()
	if err != nil {
		return nil, err
	}
	clients := make(map[string]clientPolicy, len(cfg.Clients))
	credentials := make(map[string]string)
	for client, pc := range cfg.Clients {
		if !validLabel(client) || !validLabel(pc.Version) || !validLabel(pc.Rule) || !validLabel(pc.Environment) ||
			strings.TrimSpace(pc.Expression) == "" || len(pc.Expression) > 8192 ||
			len(pc.Routes) < 1 || len(pc.Routes) > 64 {
			return nil, errors.New("invalid client policy metadata, expression, or routes")
		}
		ast, issues := env.Compile(pc.Expression)
		if issues.Err() != nil {
			return nil, fmt.Errorf("compile policy for %s: %w", client, issues.Err())
		}
		if ast.OutputType() != cel.BoolType {
			return nil, fmt.Errorf("policy for %s must return a boolean", client)
		}
		program, err := env.Program(ast, cel.CostLimit(10_000), cel.InterruptCheckFrequency(64))
		if err != nil {
			return nil, fmt.Errorf("construct policy for %s: %w", client, err)
		}

		routes := make([]core.Route, 0, len(pc.Routes))
		seen := make(map[string]bool)
		seenOriginModel := make(map[[2]string]bool)
		for _, route := range pc.Routes {
			if err := validateRoute(route, cfg.AllowLoopbackHTTP); err != nil {
				return nil, fmt.Errorf("invalid route for %s: %w", client, err)
			}
			if seen[route.ID] {
				return nil, fmt.Errorf("duplicate route ID for %s", client)
			}
			seen[route.ID] = true
			target := [2]string{canonicalOrigin(route.Endpoint), route.Model}
			if seenOriginModel[target] {
				return nil, fmt.Errorf("duplicate provider origin and model for %s", client)
			}
			seenOriginModel[target] = true
			if route.AuthEnv != "" {
				key, ok := os.LookupEnv(route.AuthEnv)
				if !ok || key == "" || len(key) > 8192 || strings.ContainsAny(key, "\r\n") {
					return nil, errors.New("provider credential environment variable is absent or invalid")
				}
				credentials[route.AuthEnv] = key
			}
			route.Capabilities = append([]string(nil), route.Capabilities...)
			routes = append(routes, route)
		}
		clients[client] = clientPolicy{pc.Version, pc.Rule, pc.Environment, routes, program}
	}

	identities := make(map[string]core.Principal, len(cfg.Identities))
	for uri, principal := range cfg.Identities {
		if !validWorkloadURI(uri) || (principal.Workload != "" && principal.Workload != uri) {
			return nil, errors.New("invalid SPIFFE identity mapping")
		}
		if _, ok := clients[principal.Client]; !ok {
			return nil, errors.New("identity references an unknown client")
		}
		for _, value := range []string{principal.Department, principal.App, principal.Role} {
			if value != "" && !validLabel(value) {
				return nil, errors.New("invalid principal scope")
			}
		}
		principal.Workload = uri
		identities[uri] = principal
	}
	if len(clients) == 0 || len(identities) == 0 {
		return nil, errors.New("client policies and workload identities are required")
	}

	transport := &http.Transport{
		DialContext:            (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:      true,
		TLSHandshakeTimeout:    3 * time.Second,
		IdleConnTimeout:        30 * time.Second,
		MaxIdleConns:           64,
		MaxConnsPerHost:        32,
		MaxIdleConnsPerHost:    8,
		MaxResponseHeaderBytes: 32 << 10,
		DisableCompression:     true,
	}
	upstream := &http.Client{
		Transport:     transport,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}

	return &Server{
		store: store, clients: clients, identities: identities, upstream: upstream, credentials: credentials,
		maxAttempts:    cfg.MaxAttempts,
		requestTimeout: time.Duration(cfg.RequestTimeoutMS) * time.Millisecond,
		attemptTimeout: time.Duration(cfg.AttemptTimeoutMS) * time.Millisecond,
	}, nil
}

// CloseIdleConnections is called after the HTTP server and reconciler have stopped.
func (s *Server) CloseIdleConnections() {
	s.upstream.CloseIdleConnections()
}

func policyEnvironment() (*cel.Env, error) {
	var opts []cel.EnvOption
	for _, name := range []string{
		"client", "workload", "department", "app", "role", "route_id",
		"provider", "model", "region", "environment", "requested_model",
	} {
		opts = append(opts, cel.Variable(name, cel.StringType))
	}
	opts = append(opts, cel.Variable("required_capabilities", cel.ListType(cel.StringType)))
	return cel.NewEnv(opts...)
}

func validLabel(value string) bool { return labelPattern.MatchString(value) }

func validWorkloadURI(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "spiffe" && u.Opaque == "" &&
		trustDomainPattern.MatchString(u.Host) && u.User == nil &&
		u.Path != "" && u.Path != "/" && u.RawPath == "" &&
		path.Clean(u.Path) == u.Path && !strings.ContainsAny(u.Path, "%\\") &&
		u.RawQuery == "" && !u.ForceQuery && u.Fragment == "" && u.String() == raw
}

func validateRoute(r core.Route, allowLoopbackHTTP bool) error {
	if r.TimeoutMS < 0 || r.TimeoutMS > 120000 {
		return errors.New("invalid route timeout")
	}
	switch r.Protocol {
	case "", "fixture":
		if r.AuthEnv != "" {
			return errors.New("fixture routes do not accept provider credentials")
		}
	case "openai", "openrouter", "anthropic", "gemini":
		if !regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,127}$`).MatchString(r.AuthEnv) {
			return errors.New("provider route requires a credential environment variable name")
		}
	default:
		return errors.New("unknown provider protocol")
	}
	for _, label := range []string{r.ID, r.Provider, r.Account, r.Model, r.Region, r.Environment} {
		if !validLabel(label) {
			return errors.New("invalid route metadata")
		}
	}
	if r.InputUnit < 0 || r.InputUnit > maxUnitRate || r.OutputUnit < 0 ||
		r.OutputUnit > maxUnitRate || (r.InputUnit == 0 && r.OutputUnit == 0) {
		return errors.New("invalid unit pricing")
	}
	if len(r.Capabilities) > 32 {
		return errors.New("too many route capabilities")
	}
	for _, label := range r.Capabilities {
		if !validLabel(label) {
			return errors.New("invalid route capability")
		}
	}
	u, err := url.Parse(r.Endpoint)
	if err != nil || u.Opaque != "" || u.Hostname() == "" || u.User != nil ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" ||
		(u.Path != "" && u.Path != "/") || u.RawPath != "" {
		return errors.New("endpoint must be an origin URL")
	}
	if p := u.Port(); p != "" {
		port, err := strconv.Atoi(p)
		if err != nil || port < 1 || port > 65535 {
			return errors.New("invalid endpoint port")
		}
	}
	if u.Scheme == "https" {
		return nil
	}
	ip, ipErr := netip.ParseAddr(u.Hostname())
	if u.Scheme == "http" && allowLoopbackHTTP && ipErr == nil && ip.IsLoopback() {
		return nil
	}
	return errors.New("endpoint requires HTTPS unless explicitly configured for a loopback fixture")
}

// canonicalOrigin is used after validateRoute has accepted an origin URL.
func canonicalOrigin(endpoint string) string {
	u, _ := url.Parse(endpoint)
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	if ip, err := netip.ParseAddr(host); err == nil {
		host = ip.Unmap().String()
	}
	port := u.Port()
	if n, err := strconv.Atoi(port); err == nil {
		port = strconv.Itoa(n)
	}
	if port == "" {
		if u.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	return u.Scheme + "://" + net.JoinHostPort(host, port)
}

// SetApprovals configures approval checking before serving requests.
func (s *Server) SetApprovals(checker ApprovalChecker) {
	s.approvals = checker
}

// Principal resolves only the verified TLS identity and its configured scope.
// Request headers cannot supply or widen the resulting identity.
func (s *Server) Principal(r *http.Request) (core.Principal, bool) {
	return s.principal(r)
}
