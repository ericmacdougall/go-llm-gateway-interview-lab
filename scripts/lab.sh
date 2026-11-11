#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mode=${1:-test}
case "$mode" in test|smoke|serve) ;; *) echo "usage: scripts/lab.sh test|smoke|serve" >&2; exit 2;; esac
umask 077
for tool in initdb pg_ctl createdb psql go; do
  command -v "$tool" >/dev/null || { echo "Missing $tool" >&2; exit 2; }
done
mkdir -p .lab
lab=$(mktemp -d "$PWD/.lab/run.XXXXXX")
mkdir "$lab/socket"
pg_running=false
jobs=()
cleanup() {
  local pid
  for pid in "${jobs[@]+"${jobs[@]}"}"; do
    [[ "$pid" =~ ^[0-9]+$ ]] && kill "$pid" 2>/dev/null || true
  done
  for pid in "${jobs[@]+"${jobs[@]}"}"; do
    [[ "$pid" =~ ^[0-9]+$ ]] && wait "$pid" 2>/dev/null || true
  done
  if "$pg_running"; then pg_ctl -D "$lab/db" -m fast -w stop >/dev/null || true; fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
initdb -D "$lab/db" --auth=trust --encoding=UTF8 --no-locale >"$lab/init.log"
pg_ctl -D "$lab/db" -l "$lab/postgres.log" -o "-c listen_addresses='' -k '$lab/socket' -p 55483" -w start >/dev/null
pg_running=true
createdb -h "$lab/socket" -p 55483 gateway
export GATEWAY_TEST_DSN="host=$lab/socket port=55483 dbname=gateway sslmode=disable"
export GATEWAY_DATABASE_URL="$GATEWAY_TEST_DSN"
printf 'Private lab files: %s\n' "$lab"
if [[ "$mode" == test ]]; then
  go test -p 1 -count=1 ./...
  exit
fi

for tool in openssl python3 curl; do
  command -v "$tool" >/dev/null || { echo "Missing $tool" >&2; exit 2; }
done
go build -o "$lab/gateway" ./cmd/gateway
"$lab/gateway" keygen "$lab/audit.pem"
openssl req -x509 -newkey rsa:2048 -sha256 -nodes -keyout "$lab/ca.key" -out "$lab/ca.pem" -subj /CN=GatewayLab -days 1 >/dev/null 2>&1
for role in server client; do
  openssl req -new -newkey rsa:2048 -sha256 -nodes -keyout "$lab/$role.key" -out "$lab/$role.csr" -subj "/CN=GatewayLab-$role" >/dev/null 2>&1
  if [[ "$role" == server ]]; then
    printf 'subjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth\n' >"$lab/$role.ext"
  else
    printf 'subjectAltName=URI:spiffe://lab.example/workload/demo\nextendedKeyUsage=clientAuth\n' >"$lab/$role.ext"
  fi
  openssl x509 -req -in "$lab/$role.csr" -CA "$lab/ca.pem" -CAkey "$lab/ca.key" -CAserial "$lab/ca.srl" -CAcreateserial -sha256 -out "$lab/$role.pem" -days 1 -extfile "$lab/$role.ext" >/dev/null 2>&1
done

wait_address() {
  local pid=$1 file=$2 i
  for ((i=0;i<200;i++)); do
    if [[ -s "$file" ]]; then head -n 1 "$file"; return; fi
    kill -0 "$pid" 2>/dev/null || break
    sleep .05
  done
  echo "Listener did not start. Check lab logs." >&2
  return 1
}

for name in capability ok outside safety timed unavailable; do
  behavior="$name"
  [[ "$name" != outside ]] || behavior=ok
  [[ "$name" != timed ]] || behavior=timeout_then_usage
  "$lab/gateway" fixture -mode "$behavior" >"$lab/$name.addr" 2>"$lab/$name.log" &
  jobs+=("$!")
  wait_address "$!" "$lab/$name.addr" >/dev/null
done
python3 - "$lab" <<'PY'
from pathlib import Path
import json, sys
p = Path(sys.argv[1])
def route(name, model, env, route_id=None):
    addr = (p / f"{name}.addr").read_text().strip()
    return {"id": route_id or name, "provider": name, "account": "fixture", "model": model,
            "region": env, "environment": env, "endpoint": f"http://{addr}",
            "capabilities": ["chat"], "input_unit": 1, "output_unit": 1}
cfg = {
    "listen": "127.0.0.1:0", "tls_cert": str(p / "server.pem"),
    "tls_key": str(p / "server.key"), "client_ca": str(p / "ca.pem"),
    "signing_key": str(p / "audit.pem"), "budgets": {"demo": 500},
    "reconcile_seconds": 2, "reconcile_lock_key": 8190045,
    "gateway": {
        "identities": {"spiffe://lab.example/workload/demo":
                       {"client": "demo", "workload": "spiffe://lab.example/workload/demo",
                        "department": "demo", "app": "local"}},
        "clients": {"demo": {
            "version": "v1", "rule": "client-environment",
            "expression": 'client == "demo" && environment == "client-cloud"',
            "environment": "client-cloud",
            "routes": [route("capability", "demo-model", "client-cloud"),
                       route("ok", "demo-model", "client-cloud"),
                       route("safety", "safety-model", "client-cloud"),
                       route("ok", "safety-model", "client-cloud", "ok-safety"),
                       route("timed", "timed-model", "client-cloud"),
                       route("ok", "timed-model", "client-cloud", "ok-timed"),
                       route("unavailable", "down-model", "client-cloud"),
                       route("outside", "down-model", "external", "outside-down"),
                       route("outside", "outside-model", "external")]
        }},
        "max_attempts": 3, "request_timeout_ms": 6000,
        "attempt_timeout_ms": 800, "allow_loopback_http": True
    }
}
(p / "gateway.local.json").write_text(json.dumps(cfg, indent=2))
PY
"$lab/gateway" serve -config "$lab/gateway.local.json" >"$lab/gateway.addr" 2>"$lab/gateway.log" &
jobs+=("$!")
addr=$(wait_address "$!" "$lab/gateway.addr")
url="https://$addr/v1/chat/completions"
printf 'Gateway URL: %s\n' "$url"
if [[ "$mode" == serve ]]; then
  printf 'Client CA: %s\nClient cert: %s\nClient key path: %s\n' "$lab/ca.pem" "$lab/client.pem" "$lab/client.key"
  wait "${jobs[${#jobs[@]}-1]}"
  exit
fi

request() {
  curl --silent --show-error --max-time 10 --cacert "$lab/ca.pem" --cert "$lab/client.pem" \
    --key "$lab/client.key" --header 'Content-Type: application/json' \
    --output "$lab/last-response.json" --write-out '%{http_code}' --data "$1" "$url"
}
code=$(request '{"model":"demo-model","max_tokens":32,"messages":[{"role":"user","content":"synthetic smoke check"}]}')
[[ "$code" == 200 ]] || { echo "Fallback request failed with HTTP $code" >&2; exit 1; }
code=$(request '{"model":"outside-model","max_tokens":32,"messages":[{"role":"user","content":"synthetic denial check"}]}')
check_code() {
  local status=$1 want=$2 reason=$3
  [[ "$status" == "$want" ]] || { echo "Expected HTTP $want ($reason), got $status" >&2; exit 1; }
  python3 -c 'import json,sys; sys.exit(0 if json.load(open(sys.argv[1]))["error"]["code"] == sys.argv[2] else "wrong response error code")' "$lab/last-response.json" "$reason"
}
check_code "$code" 403 no_eligible_route
code=$(request '{"model":"safety-model","max_tokens":32,"messages":[{"role":"user","content":"synthetic refusal check"}]}')
check_code "$code" 422 provider_safety_refusal
code=$(request '{"model":"down-model","max_tokens":32,"messages":[{"role":"user","content":"synthetic outage check"}]}')
check_code "$code" 503 routes_exhausted
code=$(request '{"model":"demo-model","max_tokens":4096,"messages":[{"role":"user","content":"synthetic budget check"}]}')
check_code "$code" 429 budget_exhausted
code=$(request '{"model":"timed-model","max_tokens":32,"messages":[{"role":"user","content":"synthetic reconciliation check"}]}')
[[ "$code" == 200 ]] || { echo "Timeout fallback returned HTTP $code" >&2; exit 1; }

sql() { psql -XAt -v ON_ERROR_STOP=1 -h "$lab/socket" -p 55483 -d gateway -c "$1"; }
recovered=0
for ((i=0;i<60;i++)); do
  recovered=$(sql "SELECT count(*) FROM gateway_attempts WHERE attempt->'route'->>'id'='timed' AND state='settled' AND actual > 0 AND provider_request_id <> ''")
  [[ "$recovered" == 1 ]] && break
  sleep .15
done
[[ "$recovered" == 1 ]] || { echo "Timed-out attempt did not reconcile" >&2; exit 1; }
[[ "$(sql "SELECT count(*) FROM gateway_audit WHERE kind='uncertain'")" == 1 ]]
[[ "$(sql "SELECT held FROM gateway_budgets WHERE client_id='demo'")" == 0 ]]
if curl --silent --max-time 5 --cacert "$lab/ca.pem" "$url" >/dev/null 2>&1; then
  echo "Unauthenticated connection unexpectedly completed" >&2
  exit 1
fi
for spec in capability:1 ok:2 outside:0 safety:1 timed:1 unavailable:1; do
  name=${spec%:*}
  expected=${spec#*:}
  addr=$(cat "$lab/$name.addr")
  count=$(curl --silent --show-error --max-time 5 "http://$addr/calls" | python3 -c 'import json,sys; print(json.load(sys.stdin)["calls"])')
  [[ "$count" == "$expected" ]] || { echo "$name calls=$count expected=$expected" >&2; exit 1; }
done
"$lab/gateway" verify -config "$lab/gateway.local.json"
echo 'Smoke passed: mTLS, policy and budget denial, terminal safety, outage boundary, capability and timeout fallback, reconciliation without inference replay, signed audit verification.'
