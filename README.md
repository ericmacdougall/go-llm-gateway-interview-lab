# Go LLM gateway interview lab

A bounded educational reconstruction of a policy-aware Go gateway: mutual TLS workload identity, CEL route authorization, PostgreSQL budget transactions, bounded provider fallback, usage reconciliation and an Ed25519-signed audit chain. It is reference code for exploring engineering decisions, not an original production repository or a ready-made production service. No employer, customer, private deployment or interview transcript is included.

## Run locally

Use Go 1.25 or newer, a C compiler, Git, Bash and PostgreSQL 16 **server tools** on PATH. Smoke/serve also need OpenSSL, curl and Python 3. Run as a normal user from a short writable path so the private PostgreSQL Unix socket fits the host's filename limit.

```sh
export PATH=/usr/lib/postgresql/16/bin:$PATH # typical Ubuntu location
./scripts/lab.sh test
./scripts/lab.sh smoke
./scripts/lab.sh serve
```

Each command creates its own disposable socket-only PostgreSQL cluster. The tests exercise real transactions and PostgreSQL connection faults. The smoke starts the actual binary, synthetic loopback providers and mTLS listener, then checks policy and budget denial, terminal safety refusal, bounded fallback, unknown usage reconciliation, forbidden-provider counters and signed audit verification. Serve stays up until interrupted. The script cleans up its own processes but retains private development keys and logs in ignored `.lab/` directories. Keep those directories private.

For serve mode, use the printed CA/client certificate paths and listener port:

```sh
curl --cacert /path/to/ca.pem --cert /path/to/client.pem --key /path/to/client.key \
  -H 'Content-Type: application/json' \
  -d '{"model":"demo-model","max_tokens":32,"messages":[{"role":"user","content":"hello"}]}' \
  https://127.0.0.1:PORT/v1/chat/completions
```

## CI and artifact gate

```sh
./scripts/ci.sh check # formatting, modules, vet and helper syntax
./scripts/ci.sh test  # race detector, real PG integration and mTLS smoke
./scripts/ci.sh all  # native Linux amd64: checks, tests and exact-byte artifact gate
```

The workflow in `.github/workflows/ci.yml` runs the same entry point. A workflow file does not itself establish a successful hosted run, branch protection or deployment approval; consult this repository's Actions runs for current results. CI uses synthetic providers and needs no cloud credentials. `build` alone creates an artifact without establishing test success. See [CI and release gates](docs/ci-and-release.md).

## Architecture and source

- [Technical deep dive and physical ERD](docs/deep-dive.md): request/identity, transaction, recovery and audit paths, decisions and tradeoffs.
- [Provider protocol contract](internal/gateway/README.md): the supported text/SSE subset and accounting limitations.
- [Staging operator template](docs/deployment.md): explicit target guard, local defaults, persistent state and rollback boundaries.
- [Verification guide](docs/verification.md): assertions, negative controls and what each check can establish.
- [Exact PostgreSQL DDL](internal/ledger/schema.sql): five tables, 38 columns, two declared foreign keys and four partial indexes.

The gateway supports a narrow OpenAI-like text endpoint, outbound OpenAI Chat Completions, Anthropic Messages, Gemini GenerateContent and OpenRouter adapters, and deterministic fixtures. Configure provider endpoints, model names, rates and credentials explicitly. The public request format is not a complete vendor SDK replacement. `gateway report`, `verify` and `resolve` are operator commands; OS/database access is their authority boundary.

The managed-device package models application-level routing and temporary approved access. It does not install MDM, VPN, DNS interception or an OS enforcement agent. The reference does not implement Cedar, cloud KMS key custody, generic exact per-request vendor usage recovery, external audit anchoring, production invoice equivalence, automatic certificate renewal or disaster recovery. Signing detects changes relative to a trusted key/head; it does not make a mutable database immutable. No production performance or cost benefit is asserted.

No license file is included; no license grant is inferred from this reconstruction.
