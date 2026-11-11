# Verification guide

This guide describes reproducible checks for the public reconstruction. It contains no private deployment receipts, account identifiers, hosted-CI success claim or performance measurement. Read the actual workflow run for the candidate commit before treating it as green.

## Reproduce the synthetic gate

On a native Linux amd64 host with Go 1.25+, PostgreSQL 16 server tools, a C compiler, OpenSSL, Python 3, Git and curl:

```sh
./scripts/ci.sh all
```

The script fails on formatting output, dependency verification failure, vet diagnostics, helper syntax errors, test failure, a missing/skipped mandatory PostgreSQL test, mTLS smoke failure, changed source during the run, or a distribution binary differing from the smoke executable. It runs all packages once with the race detector. The manifest identifies actual allowlisted source bytes and dirty state, rather than assuming an old HEAD contains local changes.

| Property | Test or observation | Evidence limit |
|---|---|---|
| Atomic budget admission | `TestPGAdmissionAndSettlementRace` | Real concurrent PostgreSQL transactions; not a capacity benchmark. |
| No double charge | Same race test; `TestPGDuplicateReservationCannotAddSecondHold`; repeated reconciliation | State-guarded settlement and transaction assertions. |
| Unknown usage stays held | `TestPGRecoveryKeepsOnlyPossiblyDispatchedHold`, `TestPGWorkerRetainsUnknownThenSettles` | Fixture evidence lookup; real-provider recovery remains account-specific. |
| Approval scope and revocation | `internal/ledger/approval_integration_test.go` and gateway/control tests | Dispatch/revocation ordering and zero provider calls when denied. |
| Session loss and takeover | `TestLockBackendLossCancelsActiveScan` | Explicit barriers and actual backend termination; financial idempotence is tested separately. |
| Ambiguous unlock response | `TestLostUnlockReplyRetiresSession` | Real proxy forwards unlock and withholds replies; observes backend retirement before a pool probe. |
| Healthy session cleanup error | `TestUnlockHookErrorDiscardsLiveLockSession` | Forces explicit retirement rather than relying on a failed query to break the connection. |
| Signed audit | `TestPGAuditDetectsTamperingAndWrongKey`, smoke `gateway verify` | Detects alteration relative to expected key and retained head, not undetectable consistent rollback without an external anchor. |
| Identity boundary | mTLS tests and smoke | No certificate rejected; workload mapping and caller override tests. |
| Routing boundary | Policy tests plus zero forbidden-provider counters in smoke | Does not prove cloud execution geography from a metadata label. |
| Streaming boundary | `internal/gateway/vendor_test.go` | Local protocol fixtures, explicit barriers and bounded SSE parsing; not complete vendor API parity. |

The smoke proves authenticated success, policy denial, insufficient-budget denial, terminal safety refusal, capability fallback, unavailable permitted routes failing closed, timeout fallback, later reconciliation without inference replay, expected counters, zero remaining held units and audit verification.

## Meaningful negative controls

A disposable unformatted Go file must make `scripts/ci.sh check` fail before tests/build. Remove only that control, then check the clean source. For changes to transaction predicates, authorization or physical session cleanup, introduce a controlled broken copy in an isolated checkout and confirm the corresponding focused assertion fails. Never weaken the required gate, run tests against a real client's database, or publish lab keys/logs.

`python3 scripts/test_staging_helpers.py` is a portable standard-library check run by the CI `check` mode. It rejects missing/wrong hostname, malformed or non-unicast addresses and injected certificate-extension text before mutation utilities or credentials; validates loopback/explicit address, generated SAN text and example identities; preserves existing configuration; rejects mismatched probe listeners and missing model names. Filesystem/credential access is virtual and privileged utilities are sentinels. It is not an actual OS installation or certificate-issuance test.

A missing `GATEWAY_TEST_DSN` can make direct integration test invocations skip. The CI entry point prevents vacuous success by creating its own database and checking exact JSON package/test PASS pairs. Generic `PASS` text is insufficient.

## Staging verification is separate

`deploy/verify-staging.py` is a mutating synthetic acceptance script for an explicitly provisioned disposable staging instance. It exercises its mTLS API and database, creates approvals and usage rows, and verifies the chain. It requires OS/operator access. It does not provision the environment, test full VM-loss restore or establish high availability.

`deploy/verify-providers.py` makes billable provider calls if configured. It requests text and upstream SSE from all three configured OpenAI/Anthropic/OpenRouter routes, capped at 32 output tokens, and checks settled accounting. It must be run deliberately with approved accounts/models and synthetic data. It is not part of CI and must not become a periodic health probe. Provider usage or delivery uncertainty may leave holds requiring manual review. Gemini is covered by local protocol fixtures; this script does not probe it.

Before promotion, execute the exact candidate binary in a separate staging database/listener, verify identity and route denials, preserve signing-key/history continuity across restart, rehearse rollback and backup restoration, and record the candidate hash. Do not infer a throughput, latency, availability or cost claim from correctness tests.
