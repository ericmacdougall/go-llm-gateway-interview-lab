// Package device models a managed device's AI egress policy at the HTTP boundary.
package device

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"example.com/llmgateway/internal/core"
)

var ErrBlocked = errors.New("device AI egress blocked")

type Endpoint struct {
	ID, URL, Region string
}

type Config struct {
	Client, Role, GatewayURL string
	AllowedRegions           []string
	Approved                 []Endpoint
	MaxGrant                 time.Duration
}

type Grant struct {
	ID, Workload, Endpoint string
	ExpiresAt              time.Time
}

type Device struct {
	cfg             Config
	approved        map[string]Endpoint
	trace           func(context.Context, core.Event) error
	managed, direct *http.Client
	now             func() time.Time
	mu              sync.RWMutex
	grants          map[string]Grant
}

func New(cfg Config, managed, direct *http.Transport, trace func(context.Context, core.Event) error) (*Device, error) {
	if cfg.Client == "" || cfg.Role == "" || cfg.MaxGrant <= 0 || trace == nil || !secureEndpoint(cfg.GatewayURL) || !validTransport(managed) || !validTransport(direct) {
		return nil, errors.New("invalid device configuration")
	}
	allowed := make(map[string]bool)
	for _, region := range cfg.AllowedRegions {
		if region != "" {
			allowed[region] = true
		}
	}
	d := &Device{cfg: cfg, approved: make(map[string]Endpoint), trace: trace, managed: httpClient(managed), direct: httpClient(direct), now: time.Now, grants: make(map[string]Grant)}
	for _, endpoint := range cfg.Approved {
		if endpoint.ID == "" || d.approved[endpoint.ID].ID != "" || !allowed[endpoint.Region] || !secureEndpoint(endpoint.URL) {
			return nil, errors.New("invalid approved provider endpoint")
		}
		d.approved[endpoint.ID] = endpoint
	}
	return d, nil
}

func secureEndpoint(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.Hostname() != "" && u.User == nil &&
		u.Opaque == "" && u.RawQuery == "" && !u.ForceQuery && u.Fragment == ""
}

func validTransport(transport *http.Transport) bool {
	if transport == nil {
		return true
	}
	tls := transport.TLSClientConfig
	return transport.DialTLS == nil && transport.DialTLSContext == nil &&
		(tls == nil || (!tls.InsecureSkipVerify && tls.ServerName == ""))
}

func httpClient(source *http.Transport) *http.Client {
	if source == nil {
		source = http.DefaultTransport.(*http.Transport)
	}
	transport := source.Clone()
	transport.Proxy = nil
	return &http.Client{Transport: transport, Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
}

// Activate accepts a principal provided by the managed device's trusted identity boundary.
func (d *Device) Activate(ctx context.Context, principal core.Principal, endpointID string, duration time.Duration) (Grant, error) {
	if principal.Client != d.cfg.Client || principal.Workload == "" || principal.Role != d.cfg.Role ||
		d.approved[endpointID].ID == "" || duration <= 0 || duration > d.cfg.MaxGrant {
		return Grant{}, ErrBlocked
	}
	var key [16]byte
	if _, err := rand.Read(key[:]); err != nil {
		return Grant{}, err
	}
	grant := Grant{ID: hex.EncodeToString(key[:]), Workload: principal.Workload, Endpoint: endpointID, ExpiresAt: d.now().Add(duration)}
	if err := d.trace(ctx, core.Event{Kind: "direct_grant_activated", Principal: principal, Detail: map[string]any{
		"grant": grant.ID, "endpoint": endpointID, "region": d.approved[endpointID].Region, "expires_at": grant.ExpiresAt,
	}}); err != nil {
		return Grant{}, fmt.Errorf("trace grant: %w", err)
	}
	d.mu.Lock()
	for id, g := range d.grants {
		if !d.now().Before(g.ExpiresAt) {
			delete(d.grants, id)
		}
	}
	d.grants[grant.ID] = grant
	d.mu.Unlock()
	return grant, nil
}

type Response struct {
	Status int
	Body   []byte
}

// Send routes to the managed gateway or an explicitly granted endpoint.
// Health and principal are trusted local inputs. A provider error never switches paths.
func (d *Device) Send(ctx context.Context, principal core.Principal, managedAvailable bool, grantID string, payload []byte) (Response, error) {
	if principal.Client != d.cfg.Client || principal.Workload == "" || len(payload) > 1<<20 {
		return Response{}, ErrBlocked
	}
	if managedAvailable {
		if grantID != "" {
			return Response{}, ErrBlocked
		}
		return post(ctx, d.managed, d.cfg.GatewayURL, payload)
	}
	d.mu.RLock()
	grant, ok := d.grants[grantID]
	d.mu.RUnlock()
	if !ok || grant.Workload != principal.Workload || principal.Role != d.cfg.Role || !d.now().Before(grant.ExpiresAt) {
		return Response{}, ErrBlocked
	}
	endpoint := d.approved[grant.Endpoint]
	if err := d.trace(ctx, core.Event{Kind: "direct_grant_used", Principal: principal, Detail: map[string]any{
		"grant": grant.ID, "endpoint": endpoint.ID, "region": endpoint.Region, "gateway_accounting": false,
	}}); err != nil {
		return Response{}, fmt.Errorf("trace direct access: %w", err)
	}
	if !d.now().Before(grant.ExpiresAt) {
		return Response{}, ErrBlocked
	}
	callCtx, cancel := context.WithDeadline(ctx, grant.ExpiresAt)
	defer cancel()
	return post(callCtx, d.direct, endpoint.URL, payload)
}

func post(ctx context.Context, client *http.Client, endpoint string, payload []byte) (Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return Response{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return Response{}, err
	}
	defer resp.Body.Close()
	const max = 2 << 20
	body, err := io.ReadAll(io.LimitReader(resp.Body, max+1))
	if err != nil {
		return Response{}, err
	}
	if len(body) > max {
		return Response{}, errors.New("device reply exceeds limit")
	}
	return Response{Status: resp.StatusCode, Body: body}, nil
}
