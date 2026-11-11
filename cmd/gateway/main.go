package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"example.com/llmgateway/internal/approval"
	"example.com/llmgateway/internal/control"
	"example.com/llmgateway/internal/gateway"
	"example.com/llmgateway/internal/ledger"
	"example.com/llmgateway/internal/reconcile"
)

type settings struct {
	Listen             string           `json:"listen"`
	TLSCert            string           `json:"tls_cert"`
	TLSKey             string           `json:"tls_key"`
	ClientCA           string           `json:"client_ca"`
	SigningKey         string           `json:"signing_key"`
	Budgets            map[string]int64 `json:"budgets"`
	ReconcileSeconds   int              `json:"reconcile_seconds"`
	ReconcileLockKey   int64            `json:"reconcile_lock_key"`
	ApproverRole       string           `json:"approver_role"`
	ApprovalMaxSeconds int              `json:"approval_max_seconds"`
	Gateway            gateway.Config   `json:"gateway"`
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:]); err != nil {
		// Errors crossing this boundary are controlled messages. Never emit
		// connection configuration, private keys, or provider payloads.
		log.Print(err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: gateway serve|fixture|keygen|verify|resolve|report")
	}
	switch args[0] {
	case "keygen":
		if len(args) != 2 {
			return errors.New("usage: gateway keygen PRIVATE_KEY_PATH")
		}
		return generateKey(args[1])
	case "fixture":
		flags := flag.NewFlagSet("fixture", flag.ContinueOnError)
		mode := flags.String("mode", "ok", "fixture behavior")
		addr := flags.String("listen", "127.0.0.1:0", "loopback listener")
		if err := flags.Parse(args[1:]); err != nil {
			return errors.New("invalid fixture flags")
		}
		host, _, err := net.SplitHostPort(*addr)
		ip := net.ParseIP(host)
		if err != nil || ip == nil || !ip.IsLoopback() {
			return errors.New("fixture must listen on a loopback IP")
		}
		f, err := gateway.NewFixture(*mode)
		if err != nil {
			return errors.New("invalid fixture mode")
		}
		fixture := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet && r.URL.Path == "/calls" {
				_ = json.NewEncoder(w).Encode(map[string]int64{"calls": f.Calls()})
				return
			}
			f.ServeHTTP(w, r)
		})
		return serve(ctx, *addr, fixture, nil, nil)
	case "serve", "verify", "resolve", "report":
		flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
		path := flags.String("config", "gateway.local.json", "configuration path")
		client := flags.String("client", "", "client for an operator usage report")
		attempt, actor, reason := new(string), new(string), new(string)
		amount := new(int64)
		if args[0] == "resolve" {
			attempt = flags.String("attempt", "", "uncertain attempt ID")
			actor = flags.String("actor", "", "operator identity")
			reason = flags.String("reason", "", "non-sensitive justification")
			amount = flags.Int64("amount", -1, "confirmed accounting units")
		}
		if err := flags.Parse(args[1:]); err != nil {
			return errors.New("invalid gateway flags")
		}
		cfg, err := readConfig(*path)
		if err != nil {
			return err
		}
		dsn := os.Getenv("GATEWAY_DATABASE_URL")
		if dsn == "" {
			return errors.New("GATEWAY_DATABASE_URL is required")
		}
		key, err := readKey(cfg.SigningKey)
		if err != nil {
			return err
		}
		startup, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		store, err := ledger.Open(startup, dsn, key)
		if err != nil {
			return errors.New("database initialization failed")
		}
		safeToClose := true
		defer func() {
			if safeToClose {
				_ = store.Close()
			}
		}()
		if args[0] == "report" {
			groups, err := store.Report(startup, *client)
			if err != nil {
				return errors.New("usage reporting failed")
			}
			return json.NewEncoder(os.Stdout).Encode(groups)
		}
		if args[0] == "resolve" {
			if *attempt == "" || *actor == "" || *reason == "" || *amount < 0 {
				return errors.New("resolve requires attempt, actor, reason and nonnegative amount")
			}
			applied, err := store.ResolvePending(startup, *attempt, *amount, *actor, *reason)
			if err != nil {
				return errors.New("manual resolution failed")
			}
			if !applied {
				return errors.New("attempt already resolved")
			}
			fmt.Println("manual resolution applied and audited")
			return nil
		}
		if args[0] == "verify" {
			if err := store.VerifyAudit(startup, key.Public().(ed25519.PublicKey)); err != nil {
				return errors.New("audit verification failed")
			}
			fmt.Println("audit chain and signatures verified")
			return nil
		}
		if err := store.Migrate(startup); err != nil {
			return errors.New("database migration failed")
		}
		if err := store.VerifyAudit(startup, key.Public().(ed25519.PublicKey)); err != nil {
			return errors.New("audit history or key failed verification")
		}
		for client, limit := range cfg.Budgets {
			if err := store.SeedBudget(startup, client, limit); err != nil {
				return errors.New("budget provisioning failed")
			}
		}
		server, err := gateway.New(cfg.Gateway, store)
		if err != nil {
			return errors.New("invalid gateway routing configuration")
		}
		defer server.CloseIdleConnections()
		var handler http.Handler = server
		if cfg.ApproverRole != "" {
			if cfg.ApprovalMaxSeconds < 1 || cfg.ApprovalMaxSeconds > 86400 {
				return errors.New("invalid approval TTL")
			}
			models := make(map[string][]string)
			for client, pc := range cfg.Gateway.Clients {
				for _, route := range pc.Routes {
					models[client] = append(models[client], route.Model)
				}
			}
			approvals, err := approval.New(store, approval.Config{ApproverRole: cfg.ApproverRole, Models: models, MaxTTL: time.Duration(cfg.ApprovalMaxSeconds) * time.Second})
			if err != nil {
				return errors.New("invalid approval configuration")
			}
			server.SetApprovals(approvals)
			handler = control.Handler{Service: approvals, Auth: server, Next: server}
		}
		tlsCfg, err := loadTLS(cfg)
		if err != nil {
			return err
		}
		var worker func(context.Context) error
		if cfg.ReconcileSeconds > 0 {
			if cfg.ReconcileSeconds > 86400 || cfg.ReconcileLockKey == 0 {
				return errors.New("invalid reconciliation interval or lock key")
			}
			w := reconcile.Worker{Store: store, Lookup: server, Interval: time.Duration(cfg.ReconcileSeconds) * time.Second,
				StaleAfter: 3 * time.Minute, LookupTimeout: 3 * time.Second, DBTimeout: 3 * time.Second, BatchSize: 10, LockKey: cfg.ReconcileLockKey, OnError: func(error) {
					log.Print("reconciliation pass reported an error; unresolved attempts remain eligible for retry")
				}}
			worker = w.Run
		}
		handler = readinessHandler{next: handler, db: store.DB}
		err = serve(ctx, cfg.Listen, handler, tlsCfg, worker)
		safeToClose = !errors.Is(err, errIncompleteShutdown)
		return err
	default:
		return errors.New("usage: gateway serve|fixture|keygen|verify|resolve|report")
	}
}

func readConfig(path string) (settings, error) {
	file, err := os.Open(path)
	if err != nil {
		return settings{}, errors.New("cannot open gateway configuration")
	}
	defer file.Close()
	var cfg settings
	dec := json.NewDecoder(io.LimitReader(file, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return settings{}, errors.New("cannot decode gateway configuration")
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return settings{}, errors.New("unexpected trailing gateway configuration")
	}
	if cfg.Listen == "" || cfg.ReconcileSeconds < 0 {
		return settings{}, errors.New("invalid listener or interval")
	}
	return cfg, nil
}

func loadTLS(cfg settings) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(cfg.TLSCert, cfg.TLSKey)
	if err != nil {
		return nil, errors.New("cannot load server TLS key pair")
	}
	ca, err := os.ReadFile(cfg.ClientCA)
	if err != nil {
		return nil, errors.New("cannot load client trust roots")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("no client trust roots")
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}, nil
}

func generateKey(path string) error {
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return errors.New("cannot generate signing key")
	}
	raw, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return errors.New("cannot encode signing key")
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return errors.New("cannot create exclusive signing key file")
	}
	defer file.Close()
	if err := pem.Encode(file, &pem.Block{Type: "PRIVATE KEY", Bytes: raw}); err != nil {
		return errors.New("cannot write signing key")
	}
	return file.Sync()
}

func readKey(path string) (ed25519.PrivateKey, error) {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("signing key must be a private regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.New("cannot read signing key")
	}
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, errors.New("invalid signing key encoding")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	key, ok := parsed.(ed25519.PrivateKey)
	if err != nil || !ok {
		return nil, errors.New("signing key must be Ed25519")
	}
	return key, nil
}

type readinessHandler struct {
	next http.Handler
	db   *sql.DB
}

func (h readinessHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet || r.URL.Path != "/readyz" {
		h.next.ServeHTTP(w, r)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	w.Header().Set("Cache-Control", "no-store")
	if h.db.PingContext(ctx) != nil {
		http.Error(w, "database unavailable", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready\n"))
}

var errIncompleteShutdown = errors.New("shutdown left work active")

type trackedHandler struct {
	http.Handler
	mu       sync.Mutex
	draining bool
	active   sync.WaitGroup
}

func (h *trackedHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	if h.draining {
		h.mu.Unlock()
		http.Error(w, "gateway draining", http.StatusServiceUnavailable)
		return
	}
	h.active.Add(1)
	h.mu.Unlock()
	defer h.active.Done()
	h.Handler.ServeHTTP(w, r)
}

func (h *trackedHandler) drain() <-chan struct{} {
	h.mu.Lock()
	h.draining = true
	h.mu.Unlock()
	done := make(chan struct{})
	go func() { h.active.Wait(); close(done) }()
	return done
}

func serve(ctx context.Context, addr string, h http.Handler, tlsCfg *tls.Config, worker func(context.Context) error) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return errors.New("cannot bind listener")
	}
	if tlsCfg != nil {
		listener = tls.NewListener(listener, tlsCfg)
	}
	reqCtx, cancelReq := context.WithCancel(context.Background())
	defer cancelReq()
	tracked := &trackedHandler{Handler: h}
	server := &http.Server{Handler: tracked, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 2 * time.Minute, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 32 << 10,
		BaseContext: func(net.Listener) context.Context { return reqCtx }}
	workerCtx, stopWorker := context.WithCancel(context.Background())
	defer stopWorker()
	var workerDone chan error
	if worker != nil {
		workerDone = make(chan error, 1)
		go func() { workerDone <- worker(workerCtx) }()
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	fmt.Println(listener.Addr().String())
	var result error
	select {
	case <-ctx.Done():
	case <-serveDone:
		result = errors.New("HTTP server exited unexpectedly")
	case <-workerDone:
		workerDone = nil
		result = errors.New("reconciliation worker exited unexpectedly")
	}
	handlersDone := tracked.drain()
	stopWorker()
	drain, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(drain); err != nil {
		cancelReq()
		_ = server.Close()
		result = errors.Join(result, errors.New("HTTP drain deadline exceeded"))
	}
	join, cancelJoin := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancelJoin()
	select {
	case <-handlersDone:
	case <-join.Done():
		result = errors.Join(result, errIncompleteShutdown, errors.New("HTTP handler shutdown deadline exceeded"))
	}
	if workerDone != nil {
		select {
		case <-workerDone:
		case <-join.Done():
			result = errors.Join(result, errIncompleteShutdown, errors.New("worker shutdown deadline exceeded"))
		}
	}
	return result
}
