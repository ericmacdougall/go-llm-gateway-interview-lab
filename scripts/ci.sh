#!/usr/bin/env bash
# Synthetic-only CI. Never source provider credentials or target an existing DB.
set -euo pipefail
cd "$(dirname "$0")/.."
umask 077
mode=${1:-all}
case "$mode" in check|test|build|all) ;; *) echo 'usage: scripts/ci.sh check|test|build|all' >&2; exit 2;; esac
for tool in go git python3; do
  command -v "$tool" >/dev/null || { echo "Missing $tool" >&2; exit 2; }
done
# Make all invocations independent of ambient GOFLAGS (especially test filters).
unset GOFLAGS CI_EXPECTED_SOURCE_SHA256 CI_EXPECTED_BINARY_SHA256
source_fingerprint() {
  python3 - <<'PYHASH'
from pathlib import Path
import hashlib
files = [Path(p) for p in ('go.mod','go.sum','README.md','.gitignore','.github/workflows/ci.yml')]
for directory, suffixes in {'cmd':{'.go','.sql'}, 'internal':{'.go','.sql'}, 'integration':{'.go','.sql'}, 'scripts':{'.sh','.py'}, 'deploy':{'.sh','.py','.service'}, 'docs':{'.md'}}.items():
    files.extend(p for p in Path(directory).rglob('*') if p.is_file() and p.suffix in suffixes)
if any(p.is_symlink() for p in files): raise SystemExit('Source symlink rejected')
body = ''.join(p.as_posix()+'\0'+hashlib.sha256(p.read_bytes()).hexdigest()+'\n' for p in sorted(files))
print(hashlib.sha256(body.encode()).hexdigest())
PYHASH
}
check() {
  local unformatted
  unformatted=$(gofmt -l cmd internal integration)
  if [[ -n "$unformatted" ]]; then
    printf 'Unformatted Go files:\n%s\n' "$unformatted" >&2
    return 1
  fi
  go mod verify
  go vet ./...
  bash -n scripts/ci.sh scripts/lab.sh
  sh -n deploy/install-staging.sh
  python3 - <<'PY'
from pathlib import Path
for p in sorted(Path('deploy').glob('*.py')):
    compile(p.read_bytes(), str(p), 'exec')  # No __pycache__ written into source.
print('Deployment Python syntax passed')
PY
  python3 scripts/test_staging_helpers.py
  if command -v actionlint >/dev/null; then
    actionlint .github/workflows/ci.yml
  else
    echo 'actionlint unavailable; optional workflow lint not run'
  fi
}
test_all() {
  [[ $(id -u) != 0 ]] || { echo 'Run PostgreSQL lab as a nonroot user' >&2; return 2; }
  for tool in initdb pg_ctl createdb psql openssl curl gcc; do
    command -v "$tool" >/dev/null || { echo "Missing $tool (PostgreSQL server tools must be on PATH)" >&2; return 2; }
  done
  [[ $(initdb --version) == *' 16.'* ]] || { echo 'CI requires PostgreSQL 16 server tools' >&2; return 2; }
  mkdir -p .lab
  local evidence
  evidence=$(mktemp "$PWD/.lab/ci-test.XXXXXX")
  # lab.sh creates its OWN private, socket-only database and overrides both DSNs.
  # JSON permits exact test/package evidence rather than grepping generic PASS.
  GOFLAGS='-race -json' ./scripts/lab.sh test | tee "$evidence"
  python3 - "$evidence" <<'PY'
import json, sys
required = {
 'example.com/llmgateway/internal/ledger': {
  'TestPGAdmissionAndSettlementRace', 'TestPGRecoveryKeepsOnlyPossiblyDispatchedHold',
  'TestPGAuditDetectsTamperingAndWrongKey', 'TestPGDuplicateReservationCannotAddSecondHold',
  'TestPGApprovalLifecycleAndScopes', 'TestPGApprovalConcurrentDecisionAndExpiry',
  'TestPGApprovalAuditFailureRollsBackDecision', 'TestPGDispatchRechecksExactApprovalAfterReservation',
  'TestPGDispatchAndRevocationSerialize', 'TestPGReportPreservesScopeAndPendingUsage'},
 'example.com/llmgateway/internal/reconcile': {'TestPGWorkerRetainsUnknownThenSettles'},
 'example.com/llmgateway/integration/lockwire': {
  'TestLostUnlockReplyRetiresSession', 'TestUnlockHookErrorDiscardsLiveLockSession',
  'TestLockBackendLossCancelsActiveScan'},
}
passed = set()
for line in open(sys.argv[1]):
    if not line.startswith('{'):  # lab's private-directory announcement is not JSON.
        continue
    e = json.loads(line)
    key = (e.get('Package'), e.get('Test'))
    if e.get('Action') == 'pass':
        passed.add(key)
    if e.get('Action') in ('skip', 'fail') and key[1] in required.get(key[0], set()):
        raise SystemExit(f'Required PostgreSQL test {key} {e["Action"]}ped/failed')
missing = {(p, t) for p, tests in required.items() for t in tests} - passed
if missing:
    raise SystemExit(f'Missing real PostgreSQL test passes: {sorted(missing)}')
print(f'Required real PostgreSQL tests passed: {sum(map(len, required.values()))}')
PY
  local smoke_evidence
  smoke_evidence=$(mktemp "$PWD/.lab/ci-smoke.XXXXXX")
  CGO_ENABLED=0 GOFLAGS='-trimpath -buildvcs=false' ./scripts/lab.sh smoke | tee "$smoke_evidence"
  CI_EXPECTED_BINARY_SHA256=$(python3 - "$smoke_evidence" <<'PY'
from pathlib import Path
import hashlib, sys
labs = [Path(line.removeprefix('Private lab files: ').strip()) for line in open(sys.argv[1]) if line.startswith('Private lab files: ')]
if len(labs) != 1 or labs[0].resolve().parent != Path('.lab').resolve():
    raise SystemExit('Missing or invalid smoke lab directory')
print(hashlib.sha256((labs[0] / 'gateway').read_bytes()).hexdigest())
PY
  )
  export CI_EXPECTED_BINARY_SHA256
}
build() {
  mkdir -p dist/ci
  rm -f dist/ci/gateway-linux-amd64 dist/ci/gateway-linux-amd64.sha256 dist/ci/manifest.json
  # No platform-dependent CGO in the distributable. Race tests above use CGO.
  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -buildvcs=false -o dist/ci/gateway-linux-amd64 ./cmd/gateway
  python3 - "$mode" <<'PY'
from pathlib import Path
import hashlib, json, os, subprocess, sys
root = Path('.')
def sha(data): return hashlib.sha256(data).hexdigest()
def allowed(name):
    p = Path(name)
    return (name in {'go.mod', 'go.sum', 'README.md', '.gitignore'} or
        (p.parts[0] in {'cmd', 'internal', 'integration'} and p.suffix in {'.go', '.sql'}) or
        (p.parts[0] == 'scripts' and p.suffix in {'.sh', '.py'}) or
        (p.parts[0] == 'deploy' and p.suffix in {'.py', '.sh', '.service'}) or
        (p.parts[0] == 'docs' and p.suffix == '.md') or
        name == '.github/workflows/ci.yml')
paths = [Path(p) for p in ('go.mod','go.sum','README.md','.gitignore','.github/workflows/ci.yml')]
for directory in ('cmd','internal','integration','scripts','deploy','docs'):
    paths.extend(p for p in Path(directory).rglob('*') if p.is_file() and allowed(p.as_posix()))
paths = sorted(paths)
if any(p.is_symlink() for p in paths): raise SystemExit('Source symlink rejected')
entries = [{'path':p.as_posix(), 'sha256':sha(p.read_bytes())} for p in paths]
head = subprocess.check_output(['git','rev-parse','HEAD'], text=True).strip()
tracked = subprocess.check_output(['git','ls-tree','-r','--name-only','HEAD'], text=True).splitlines()
old = {}
for name in tracked:
    if allowed(name):
        old[name] = sha(subprocess.check_output(['git','show',f'HEAD:{name}']))
actual = {e['path']:e['sha256'] for e in entries}
binary = Path('dist/ci/gateway-linux-amd64')
digest = sha(binary.read_bytes())
if os.environ.get('CI_EXPECTED_BINARY_SHA256', digest) != digest:
    binary.unlink()
    raise SystemExit('Distribution binary differs from the smoke-tested executable')
Path(str(binary)+'.sha256').write_text(f'{digest}  {binary.name}\n')
source_digest = sha(''.join(e['path']+'\0'+e['sha256']+'\n' for e in entries).encode())
if os.environ.get('CI_EXPECTED_SOURCE_SHA256', source_digest) != source_digest:
    binary.unlink()
    Path(str(binary)+'.sha256').unlink()
    raise SystemExit('Source changed during CI; refusing artifact')
manifest = {'schema':1, 'git_head':head, 'source_dirty':actual != old,
 'source_sha256':source_digest, 'source_files':entries,
 'go_version':subprocess.check_output(['go','version'],text=True).strip(),
 'target':'linux/amd64', 'cgo_enabled':False, 'buildvcs':False,
 'binary':binary.name, 'binary_sha256':digest,
 'gate':sys.argv[1], 'test_evidence':'separate execution receipt required; build mode alone does not test'}
Path('dist/ci/manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
print(f'Built {binary.name}: SHA256 {digest}; source SHA256 {source_digest}; source_dirty={manifest["source_dirty"]}')
PY
}
case "$mode" in
  check) check;;
  test) test_all;;
  build) build;;
  all)
    # A failed current run must not leave a previous releasable artifact behind.
    rm -f dist/ci/gateway-linux-amd64 dist/ci/gateway-linux-amd64.sha256 dist/ci/manifest.json
    [[ $(go env GOHOSTOS)/$(go env GOHOSTARCH) == linux/amd64 ]] || { echo 'Full distribution gate requires a native linux/amd64 host; check/test support native hosts' >&2; exit 2; }
    CI_EXPECTED_SOURCE_SHA256=$(source_fingerprint)
    export CI_EXPECTED_SOURCE_SHA256
    check
    test_all
    [[ $(source_fingerprint) == "$CI_EXPECTED_SOURCE_SHA256" ]] || { echo 'Source changed during CI' >&2; exit 1; }
    build
    ;;
esac
