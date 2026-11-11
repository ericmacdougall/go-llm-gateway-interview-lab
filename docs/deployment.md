# Disposable staging template

These operator tools demonstrate a single Linux/systemd staging installation. They are not a cloud/VM provisioner or an automatic production rollout. They contain no private host, project, account or deployment receipt. Start with `scripts/lab.sh` for a self-contained local demonstration; the scripts below require an explicitly prepared disposable OS instance.

## Target and prerequisites

Provision Go's Linux amd64 binary, PostgreSQL 16, Python 3, OpenSSL and systemd. Create the dedicated `gatewaylab` OS/database role, database `llmgateway`, application working directory and versioned release layout through your own reviewed provisioning method. The database role should have only the privileges required for this reference. It currently owns its tables and performs startup DDL; a production installation should separate migration and runtime privileges.

| Path or unit | Purpose |
|---|---|
| `/opt/llm-gateway/releases/<release-id>/` | Root-owned executable plus `deploy/` assets; retain prior compatible release. |
| `/opt/llm-gateway/current` | Operator-controlled symlink to one versioned release. |
| `/etc/llm-gateway/gateway.json` | Identity map, route inventory, policy, listener and worker settings. |
| `/etc/llm-gateway/server.key` | Service-readable private server key, mode 0600. |
| `/etc/llm-gateway/audit.pem` | Persistent Ed25519 signing key, mode 0600; preserve across restart. |
| `/etc/llm-gateway/operator/` | Root-only CA signing key and client credentials, mode 0700. |
| `/etc/llm-gateway/provider.env` | Optional root-owned mode-0600 provider environment file; never commit it. |
| `/var/lib/llm-gateway/` | Explicit persistent application writable directory. `PrivateTmp` also provides private temporary directories. |
| `llm-gateway.service` | Dedicated gateway identity, embedded reconciliation and bounded shutdown. |
| `llm-gateway-fixture@NAME.service` | Synthetic providers under a separate `gatewayfixture` OS account. |

The systemd unit uses a local PostgreSQL Unix socket with peer authentication as `gatewaylab`. Verify that your PostgreSQL configuration actually permits that identity. It does not automatically make a remote database secure or available.

Select the **exact intended hostname** before invoking the installer. The default listener is loopback; changing the address requires an explicitly selected unicast IPv4 address assigned to the target. The same value controls configuration, certificate SAN and acceptance probes. Port 8443 and the example trust domain `gateway-staging.example.test` are fixed; this is not an arbitrary certificate-extension interface.

```sh
export GATEWAY_STAGING_HOSTNAME=gateway-lab # must equal this target's actual hostname
export GATEWAY_STAGING_ADDRESS=127.0.0.1   # safe local default
# On the prepared disposable target, as its authorized operator:
sh /opt/llm-gateway/current/deploy/install-staging.sh
```

The installer checks hostname and validates the address before writes. It rejects unspecified, multicast and limited-broadcast addresses, newline/extension injection, and an existing listener or trust domain that differs from the requested configuration. An existing server certificate must verify under the retained CA for the requested IP. It will not silently rotate existing private keys or rewrite an incompatible configuration. To change listener or trust domain, prepare an explicit reviewed migration with matching trust material and config.

The installer expects the prepared release to exist. It creates fixture identity when missing, issues absent CA/server/requester/approver credentials, preserves existing audit key/configuration, installs the units, then restarts fixtures and gateway. It creates a 90-day CA and 30-day leaf certificates. It is not an automatic certificate renewer. Run only inside the disposable target, never on a hypervisor or an unrelated workstation.

## Identity and request path

Requester and approver client certificates use these example URI SANs:

```text
spiffe://gateway-staging.example.test/workload/requester
spiffe://gateway-staging.example.test/workload/approver
```

The mapping binds each verified URI to client `staging`, department `engineering`, app `lab` and its role. A request cannot override that scope through headers. The listener requires a verified client certificate and TLS 1.3. `/healthz` reports application liveness; `/readyz` checks database reachability with a one-second bound. Both still cross the client-certificate TLS boundary, but are not provider availability checks or application-grant endpoints.

Fixtures listen only on loopback ports 18441–18446. The staging CEL policy allows `client-cloud` routes and excludes `external`. The literal route labels are trusted configuration assertions, not proof of physical residency. Real provider geography, account retention rules and network egress controls require independent evidence.

## Acceptance and state continuity

On the selected target, using the same address as installation:

```sh
GATEWAY_STAGING_ADDRESS=127.0.0.1 python3 /opt/llm-gateway/current/deploy/verify-staging.py
systemctl status llm-gateway --no-pager
```

The acceptance script reads operator credentials locally, sends synthetic requests and **mutates** attempts, approvals, budgets and audit history. It checks mTLS, policy/safety/outage behavior, scoped approval and revocation, timeout reconciliation and audit verification. Its large-token call intentionally fits the example staging budget. The isolated CI smoke separately asserts insufficient-budget rejection. Do not use this script as a read-only readiness probe.

Inspect aggregate usage using `gateway report -config CONFIG -client CLIENT` as the authorized OS/database operator. `gateway verify` checks the signed history. `gateway resolve` requires an attempt, confirmed amount, operator identity and non-sensitive reason; it records an audited disposition. Never release an unknown hold merely because it is old.

Before restart, record both budgets, attempt count, audit head/key fingerprint and unresolved holds; after restart verify continuity and old/new signatures. This is separate from backup restoration or VM-loss recovery.

## Optional external provider probes

External calls require your own explicitly approved provider accounts, endpoints, regions and models. They can incur charges and send the supplied content to third parties. They are not required by CI. Securely provision a root-owned mode-0600 `/etc/llm-gateway/provider.env` containing only the credentials your operator approved; do not display values or commit this file. The recognized generic variable names are `GATEWAY_OPENAI_KEY`, `GATEWAY_ANTHROPIC_KEY` and `GATEWAY_OPENROUTER_KEY`.

Set the corresponding approved **model names**, then run `deploy/enable-providers.py`. For each credential present, its model must be explicit and valid; there is no hardcoded account-specific model selection.

```sh
export GATEWAY_OPENAI_MODEL=your-approved-model
export GATEWAY_ANTHROPIC_MODEL=your-approved-model
export GATEWAY_OPENROUTER_MODEL=your-approved-provider/model
python3 /opt/llm-gateway/current/deploy/enable-providers.py
```

This adds a distinct `livecheck` client that permits `external-approved` routes. It does not widen the synthetic `staging` client's policy. The script configures routes for the official public provider origins; the account name is an operator-supplied classification, not a cloud-account attestation. Restart the service to load the config/environment. Provision an operator-only `live.pem`/`live.key` certificate under the same CA with URI `spiffe://gateway-staging.example.test/workload/live` through your controlled issuance process. The installer does not issue that optional identity automatically.

`deploy/verify-providers.py` sends six bounded synthetic calls (text and SSE for each provider), checks request trace IDs and settled usage, and requires all three configured protocols unless `--allow-partial` is explicit. Use the same staging address. It reads no API key itself; the gateway uses its provisioned environment. Do not loop billable probes as a health check. OpenRouter's public origin alone cannot guarantee its downstream model's serving region. Configured integer accounting rates are not an exact provider invoice model.

## Release and rollback

Promote only a reviewed artifact whose hash and source match successful CI. Rehearse the exact binary against a separate listener/database. Take a consistent database backup and preserve configuration, CA and signing keys through your secure backup mechanism; test restoration separately. Switch the active symlink only after rehearsal, restart, then run the positive/negative checks and state-continuity assertions.

On failure, stop dispatch and retain durable state. Restore a previous binary/config only if schema/key expectations remain compatible. Preserve unknown holds, completed settlements, provider IDs, approvals, audit records and keys. Restoring an older database to undo code can erase real provider charges and recreate work, so it needs a separately reviewed recovery plan.

Startup DDL is additive `CREATE TABLE/INDEX IF NOT EXISTS` under a migration lock. There is no ordered migration history, down-migration engine, automated canary, failover controller or tested disaster-recovery system here. Separate those proposals from implemented behavior. The runtime owns its audit relations; use independent retained checkpoints to detect a consistent rollback of both chain and head. Audit key rotation needs explicit verification history, not simply replacing `audit.pem`.
