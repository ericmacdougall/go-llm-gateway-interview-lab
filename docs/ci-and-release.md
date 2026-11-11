# Go LLM gateway: executable CI and release gates

This is the CI/release design for the public **educational reconstruction**. It describes the supplied implementation and proposed promotion gates; it contains no private deployment receipt or original production pipeline. Implementation: `scripts/ci.sh`, `.github/workflows/ci.yml`, `scripts/lab.sh`, `deploy/` and the Go tests named below.

The GitHub workflow runs the supplied entry point after publication. Adding YAML does not establish a successful run, branch protection, environment approvals or a deployment. Consult the candidate commit's Actions run for actual hosted status. A local execution is evidence about that execution, not a hosted receipt. Separate staging checks are required before promotion.

## What executes, in what order

Run as an ordinary user, with Go 1.25 or newer, a C compiler, PostgreSQL **16 server tools** (`initdb`, `pg_ctl`, `createdb`, `psql`), Git, Bash, Python 3, OpenSSL and curl. On Ubuntu, prepend `/usr/lib/postgresql/16/bin` to PATH. Use a short writable checkout path; PostgreSQL's Unix-socket filename has a length limit. Do not run a lab as root.

```sh
export PATH=/usr/lib/postgresql/16/bin:$PATH
./scripts/ci.sh check   # formatting, modules, vet and deployment-helper syntax
./scripts/ci.sh test    # all Go packages once with race detector + real PG + mTLS smoke
./scripts/ci.sh build   # Linux amd64 binary and provenance; DOES NOT establish test success
./scripts/ci.sh all     # check -> test -> build, stopping on first failure
```

`all` requires a native Linux amd64 host, removes only its three prior output files before checking, and builds distributables after successful tests. It fingerprints the source before checks and rejects changes during the run. The CGO-disabled, trimmed-path smoke executable must have the same SHA-256 as the final distribution binary, or the gate fails. `check` and `test` can run on other native hosts; `build` can cross-compile but alone establishes no runtime-test evidence. `test` delegates database lifecycle to `scripts/lab.sh`, which always creates a private socket-only PostgreSQL cluster, overrides both database DSNs, and stops its own server on exit. It does not use an operator's staging database even if a DSN was present in the environment. It also uses synthetic loopback providers; it does not read provider credentials or spend API credits. `GOFLAGS` is reset so an ambient `-run` filter cannot silently reduce coverage.

| Gate | Real mechanism | Failure stops | Why and tradeoff |
|---|---|---|---|
| Source style | `gofmt -l cmd internal integration`; any output fails | Before tests/build | A check, never an automatic source edit. Small, deterministic gate. |
| Dependencies | `go mod verify` | Before tests/build | Verifies cached dependency contents against module checksums. Does not establish that every upstream dependency is safe. |
| Static analysis | `go vet ./...` | Before tests/build | Finds supported Go diagnostics; it is not a security audit. |
| Helpers | Bash/POSIX-shell parse; Python `compile`; `python3 scripts/test_staging_helpers.py` | Before tests/build | Checks invalid targets before mutation, listener/SAN/identity consistency, existing config preservation and explicit models using virtual files and mutation sentinels. No installer side effects, cloud operations or provider calls. |
| Workflow lint | `actionlint` when installed | Before tests/build if present and failing | Explicitly reports absence. Optional tool lint is not represented as a passed required gate. Hosted workflow execution remains necessary. |
| Real transactions | `GOFLAGS='-race -json' ./scripts/lab.sh test`; all packages, `-p 1 -count=1` | Before smoke/build | Real PG semantics, no test-cache reuse, lower fleet resource pressure; package serialization trades elapsed time for bounded load. |
| Nonvacuity | Parse JSON and require exact package/test PASS pairs, reject skips | Before smoke/build | A green `go test` with missing DSN is insufficient: integration tests otherwise skip. A renamed required test intentionally needs CI's evidence list updated. |
| End to end | `./scripts/lab.sh smoke` | Before build | Real binary, TLS handshake, local provider counters, database assertions and signed audit verification. Synthetic providers test boundaries without third-party flakiness or cost. |
| Distribution | `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -buildvcs=false` | No accepted artifact if build fails | One portable Linux target; race tests use CGO separately. Other architectures require their own build/run evidence. |
| Integrity | SHA-256 binary + manifest of actual allowed source-file hashes | Artifact promotion | Digest identifies bytes; it is not a trusted publisher signature or hosted attestation. |

## Tests that must actually run

The JSON gate checks the full test names and module-qualified packages, not a generic `PASS` substring. Required package `example.com/llmgateway/internal/ledger`:

- `TestPGAdmissionAndSettlementRace`: concurrent budget admission and competing settlement; only the winning conditional transition affects money.
- `TestPGRecoveryKeepsOnlyPossiblyDispatchedHold`: recover definitely unsent work while retaining possibly billed usage.
- `TestPGAuditDetectsTamperingAndWrongKey`: signature/chain verification rejects modifications and an incorrect public key.
- `TestPGDuplicateReservationCannotAddSecondHold`: duplicate reservation identity cannot create another hold.
- `TestPGApprovalLifecycleAndScopes`, `TestPGApprovalConcurrentDecisionAndExpiry`, `TestPGApprovalAuditFailureRollsBackDecision`: scope, expiry, decision races and atomic approval/audit behavior.
- `TestPGDispatchRechecksExactApprovalAfterReservation`, `TestPGDispatchAndRevocationSerialize`: reservation cannot make a stale or revoked grant sufficient for dispatch.
- `TestPGReportPreservesScopeAndPendingUsage`: reporting retains attribution and distinguishes unknown usage.

Required package `example.com/llmgateway/internal/reconcile`: `TestPGWorkerRetainsUnknownThenSettles` checks provider usage lookup without replaying inference, retained holds for uncertainty, conditional settlement and repeat scans.

Required package `example.com/llmgateway/integration/lockwire`:

- `TestLostUnlockReplyRetiresSession`: PostgreSQL executes unlock, a wire proxy withholds the response, and the ambiguous pinned connection is retired so another worker can acquire.
- `TestUnlockHookErrorDiscardsLiveLockSession`: unsuccessful cleanup cannot return a session with uncertain lock state to the pool.
- `TestLockBackendLossCancelsActiveScan`: backend loss cancels active work and permits takeover; duplicate candidate processing still cannot double settle.

The remaining packages also execute once, including direct OpenAI/Anthropic/OpenRouter protocol adapters, buffered streaming behavior, policy, mTLS identity binding, terminal safety refusal, protected routes and device continuity. Requiring a selected set of database tests is an additional guard, not a replacement for all-package execution. A race-detector pass means the exercised paths had no detected race; it proves no throughput or latency number.

The smoke proves mTLS success and rejection without a client certificate; allowed capability fallback; policy denial with **zero forbidden-provider calls**; terminal safety refusal; unavailable permitted routes fail closed; insufficient-budget rejection; timeout fallback with original hold later reconciled; no inference replay during reconciliation; expected per-provider request counts; held budget returning to zero; and audit signature verification. It creates fresh ephemeral keys for its private database. The wrapper hashes only the lab's executable and compares it with the distribution artifact; private lab material never enters the artifact. Staging key continuity and deployment-environment behavior remain separate gates.

## Artifact and source identity

Only these files are eligible for the upload step:

```text
dist/ci/gateway-linux-amd64
dist/ci/gateway-linux-amd64.sha256
dist/ci/manifest.json
```

Never upload `.lab`, database directories, certificates/private keys, `provider.env`, arbitrary configuration JSON, API responses, logs, a recursive checkout archive or the entire `dist` directory. Lab paths are private local diagnostics. The GitHub upload uses an explicit three-file allowlist, not a wildcard. No API payload is needed for provenance.

The manifest records the actual binary digest, target, Go toolchain, `-trimpath`/`-buildvcs=false` settings, Git HEAD, a `source_dirty` flag, and an ordered hash list of the working source allowlist: Go/SQL in source roots, deployment scripts/services, shell scripts, repository Markdown and the workflow. It includes new CI files even if they are untracked. It compares those bytes and paths with the named HEAD; an old scaffold HEAD must therefore not be reported as the commit containing the reconstructed implementation. It excludes `.lab`, secrets, local configuration and the output directory. It contains hashes and source paths, not source contents or host credentials.

Validate the binary in its artifact directory:

```sh
cd dist/ci
sha256sum -c gateway-linux-amd64.sha256
```

For a release, require `source_dirty=false` on a clean published commit, a matching required CI receipt for that commit, and a reviewer-approved source manifest. A dirty reconstruction build can be a useful local reference artifact, but its HEAD alone is inadequate release provenance. Do not relabel it as a signed supply-chain attestation. Byte-for-byte reproducibility across toolchain versions is not promised; pin and rerun the same Go patch version when comparing output bytes.

## Hosted PR and build configuration

`Gateway CI / PostgreSQL, mTLS and build` is the proposed required status context. The workflow runs on pull requests, pushes to `main` and explicit dispatch. It runs on an ephemeral Ubuntu 24.04 runner, installs PG16 tools, then calls exactly `./scripts/ci.sh all` as the normal runner user. It does not call the installer or a production control plane. A 25-minute timeout bounds a stuck run. New work on the same ref cancels the old CI run; cleanup is best-effort on forced termination and the ephemeral runner is discarded.

The token has `contents: read`; checkout does not persist credentials. There is no `pull_request_target`, deployment SSH key, cloud secret, OIDC write permission or provider API key in this workflow. It avoids privileged fork execution. Successful jobs retain only the allowlisted artifact for 14 days. Repository administrators must actually configure required status checks, approval requirements and protected deployment environments after publication; YAML cannot establish those settings by itself.

Verified official action tag references (read-only `git ls-remote` on 2026-11-10):

| Official action tag | Pinned commit |
|---|---|
| `actions/checkout` v4.3.1 | `34e114876b0b11c390a56381ad16ebd13914f8d5` |
| `actions/setup-go` v5.5.0 | `d35c59abb061a4a6fb18e82ac0862c26744d6ab5` |
| `actions/upload-artifact` v4.6.2 | `ea165f8d65b6e75b540449e92b4886f43607fa02` |

These are the chosen reviewed version pins, not a claim that they are the newest major versions. Upgrade through a reviewed dependency change. Primary references: [GitHub secure use of Actions](https://docs.github.com/en/actions/reference/security/secure-use), [official checkout action](https://github.com/actions/checkout), [setup-go](https://github.com/actions/setup-go), [upload-artifact](https://github.com/actions/upload-artifact), and [Go test flag implementation](https://go.dev/src/cmd/go/internal/test/testflag.go) for `GOFLAGS` parsing. SHA pinning prevents a moving tag from silently changing a future run; it does not remove the need to review action code and runner trust.

## Promotion and deployment: concrete gates

CI is not continuous deployment. The existing scripts are staging-specific operator tools, not a universal multi-cloud rollout system. `deploy/install-staging.sh` requires an explicit matching `GATEWAY_STAGING_HOSTNAME`, validates its selected IPv4 listener, expects a prepared versioned release and role/directories, installs systemd units, preserves existing CA/client/audit keys, and restarts services. Its address defaults to loopback; its example trust domain and port are fixed. It must never run on a hypervisor or unrelated host. It does not provision an entire fleet or prove high availability.

| Phase | Required evidence before advancing | Failure/stop criterion | Reason/tradeoff |
|---|---|---|---|
| Design | Policy/fallback/budget invariants, approved schema, threat/negative cases, ownership and rollout scope | Data boundary or billing semantics unresolved | Prevent polished diagrams from concealing undefined behavior. |
| Pull request | Independent source review; exact candidate commit; `ci.sh all`; migration compatibility review | Missing PG tests, unverified identity mapping, ambiguous accounting, destructive SQL, secret artifacts | Review is about actual changed code, not only a demo. |
| Build | Clean published source, CI receipt, binary hash, matching manifest, retained previous binary | Dirty/mismatched artifact or wrong target | Promote the tested binary; do not rebuild ad hoc on the guest. |
| Pre-deploy | Target hostname/IP verified; candidate installed in versioned path; disk/memory headroom; DB backup and restore rehearsal; prior release available | Wrong host, missing backup/restore evidence, key continuity uncertain | Operational checks are independent from unit tests. |
| Staging rehearsal | Real service/DB, synthetic mTLS flow and negative flows, DB/audit checks, rollback rehearsal | Auth bypass, forbidden endpoint call, double settlement, unknown hold released, unverifiable old audit records | Staging is the mandatory place to exercise real network/process boundaries. |
| Provider enablement | Separate operator authorization; only named provider accounts; scoped secret references; allowed region/endpoints; minimal synthetic billable check | Required account/region not established, usage opaque, secret exposure | CI adapters are not evidence that a real cloud account enforces the client's restrictions. |
| Promotion | Exact artifact identity; staging evidence; independent reviewer/main signoff; explicit target/rollout and stop conditions | Any material auth/data/money/availability issue | The owner cannot self-certify deployment safety. |
| After deployment | Live negative and positive flow checks; signed audit verifies old+new records; durable DB/holds survive restart; service errors inspected without payload logging | Any forbidden route, accounting drift, signature discontinuity or sustained failure | A restart and HTTP 200 alone are inadequate. |

Specific staging assertions, with their evidence source, are:

1. **Traffic/auth path:** no certificate and untrusted/wrong SPIFFE certificate cannot reach inference; a configured verified workload is attributed to its tenant/department/app. No caller-supplied tenant field grants identity.
2. **Policy/data sovereignty:** a disallowed model fails before provider dispatch; an unavailable permitted model never causes an external-region call. Fixture counter stays zero. Real network egress controls require separate host/network evidence.
3. **Budget/data path:** insufficient reservation rejects before inference. Concurrent reservations cannot spend the same balance. Duplicate settlement cannot debit twice. Pending unknown usage remains held across restart.
4. **Recovery/event path:** provider timeout records uncertainty; recovery queries provider usage rather than retrying inference. Shared request ID links attempts. Duplicate scans remain safe through conditional DB state updates even when lock ownership changes.
5. **Approval/auth path:** authorized scope and expiry are enforced; revoked/expired grants cannot dispatch, including between reservation and dispatch. A temporary device escape hatch must retain allowed endpoints/regions and automatic expiry. Central reporting of bypass usage remains the explicitly accepted gap.
6. **Audit/key path:** chain/signature verification rejects tampering, verifies older records after deployment, and uses the preserved signing key. This reference uses a local Ed25519 key; cloud KMS custody is not implemented.
7. **Operations:** verify systemd user, mode bits, process start/stop, database reachability, signal-driven drain, bounded shutdown, automatic restart, observed release hash and restore of previous version.

`deploy/verify-staging.py` and `deploy/verify-providers.py` are current target-specific tools. Read their CLI and effects before running; some provider checks make real calls and charge the configured account. They are intentionally absent from CI. Record actual results and the binary hash rather than assuming the tools cover every proposed production requirement.

## Migration, rollback and retained state

Current `internal/ledger/schema.sql` is startup DDL using additive `CREATE TABLE IF NOT EXISTS` and indexes. This is a convenient reconstructed baseline, not a complete versioned migration service. It cannot safely express arbitrary column/data transformations, downgrade compatibility or distributed migration ordering. The promotion gate must reject a schema-breaking release until versioned migration tracking, explicit locking/order, expand/backfill/contract steps and rollback/restore evidence are added.

A controlled future sequence is: review additive expansion; apply it once under a migration lock using a least-privileged migration role; deploy binaries that read both formats; backfill in bounded restartable batches; verify invariants; only then remove old fields after the rollback window. App credentials should not indefinitely retain DDL privilege. This is a proposal, not current functionality.

For the current staging shape, install candidate binary/assets under `/opt/llm-gateway/releases/<release-id>`, record its SHA, preserve the old `current` target, validate config/schema compatibility, then switch `/opt/llm-gateway/current` to that version and restart via the operator procedure. This describes the required controlled method; the CI workflow does not perform the switch. A successful artifact build does not authorize changes to production IAM or provider policies.

Rollback means restore the previous compatible binary/config and restart, then retest auth, policy, pending reservations, reconciliation and old/new audit verification. **Keep** PostgreSQL data, unresolved holds, provider request IDs, completed settlement states, approvals, audit chain, CA/trust material, provider secrets and the signing key. Never restore an old database snapshot merely to undo code while newer financial/provider activity exists; that can erase charges and recreate already processed work. In that situation stop dispatch, reconcile/provider-compare, and perform an explicitly reviewed recovery plan.

Immediate rollback or stop-dispatch triggers include forbidden egress; identity/policy bypass; double debit; dropped unknown reservation; broken approval revocation; signed audit discontinuity; startup migration incompatibility; crash loops; or exhausted operational headroom. Rollback alone cannot undo already transmitted data or provider costs. A key compromise requires revocation/rotation and verification-history design, not simply rerunning `keygen`.

## Likely interview questions at each phase

| Interview question | Answer anchored to this reference | Decision and tradeoff |
|---|---|---|
| Why not just unit tests? | Real PG tests exercise conditional updates, transaction rollback, backend loss and session-scoped advisory locks; mTLS smoke uses the actual binary and real handshakes. | More setup and runtime, but mocks cannot prove lock/session behavior. |
| How do you stop an accidentally skipped DB suite? | Private lab provisions DSN and JSON parser requires exact database test PASS events, including lockwire takeover. | Explicit names add maintenance when tests change. |
| What prevents double charging if two workers scan the same row? | Conditional terminal transition and balance/audit update in one transaction; advisory lock reduces work but is not the financial correctness boundary. | A lost session may overlap work; database idempotence remains essential. |
| Can fallback bypass a denied tenant/region? | Every candidate is policy-eligible and every attempt gets fresh budget admission; forbidden-provider counter must stay zero. | Reliability is bounded by permission; no eligible route returns an error. |
| Do CI runs use production API keys? | No. Protocol tests and loopback fixtures cover routing/accounting; authorized real-provider verification is separate. | Reliable low-cost PR gates; real integrations still need staging evidence. |
| What do you publish? | Linux binary, binary digest and source manifest only. Private lab trees and payload logs stay out. | Less diagnostic convenience in artifacts, lower exposure risk. |
| Is the provenance cryptographically trusted? | It identifies exact local source/binary bytes, HEAD and dirty state; it is not a signed hosted attestation. | Clear boundary until a trusted builder/attestation chain is implemented. |
| How do you know it was the tested code? | Require clean candidate commit, matching CI receipt and manifest; use same artifact in staging/promotion. A HEAD with different working source is insufficient. | No convenient ad hoc rebuild on the target. |
| How does shutdown affect holds? | Stop admission and worker, drain/cancel bounded in-flight work, retain uncertain dispatch for reconciliation; do not free a hold just because a process exits. | Some budget remains unavailable until usage is known or reviewed. |
| How do releases preserve audit validity? | Preserve the signing key and chain state, verify old and newly written records after restart. | Key rotation requires explicit historical-verification design. |
| Can you roll back a migration? | Only if old binary/schema remain compatible; keep financial/audit state. Current installer is additive, not a general migration rollback engine. | Destructive changes need expand/contract and restore rehearsal. |
| Is CI deployed and enforced already? | The script can be run now; hosted workflow and branch-protection enforcement require publication/configuration and an actual run receipt. | State verified execution separately from proposed automation. |
| What would change for multiple clouds? | Per-environment trusted identity, egress/region policy, provider account, DB/key custody and release gates; same portable Go artifact. | A single binary simplifies distribution but does not make sovereignty a code-only guarantee. |

## Verification record discipline

Record the exact source manifest digest, command, runner/toolchain/PG versions, test outcome, smoke outcome, binary digest and independent review state in the delivery packet. A controlled broken-format fixture must fail before the clean gate succeeds. Do not edit source after the full run without rerunning affected checks and invalidating stale release evidence. Keep the actual run receipt separate from this design document to avoid changing the source manifest simply to announce its own test result.
