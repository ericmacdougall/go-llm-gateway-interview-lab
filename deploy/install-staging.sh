#!/bin/sh
set -eu
# Run only on an explicitly selected disposable staging OS instance after its
# packages, role, directories and versioned binary are provisioned.
: "${GATEWAY_STAGING_HOSTNAME:?Set the exact staging hostname before installing}"
export GATEWAY_STAGING_HOSTNAME
GATEWAY_STAGING_ADDRESS=${GATEWAY_STAGING_ADDRESS:-127.0.0.1}
export GATEWAY_STAGING_ADDRESS
# Validate before any write or privilege-bearing operation. The SAN domain and
# port are fixed examples; address input must never become extension syntax.
python3 - <<'PY'
import ipaddress, json, os, re
from pathlib import Path
name=os.environ['GATEWAY_STAGING_HOSTNAME']
if not re.fullmatch(r'[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?',name):
    raise SystemExit('Invalid expected hostname')
address=ipaddress.IPv4Address(os.environ['GATEWAY_STAGING_ADDRESS'])
if address.is_unspecified or address.is_multicast or str(address)=='255.255.255.255':
    raise SystemExit('Select a concrete unicast staging IPv4 address')
p=Path('/etc/llm-gateway/gateway.json')
if p.exists():
    current=json.loads(p.read_text())
    identities=current.get('gateway',{}).get('identities',{})
    expected={f'spiffe://gateway-staging.example.test/workload/{role}' for role in ('requester','approver')}
    if current.get('listen')!=f'{address}:8443' or not expected.issubset(identities):
        raise SystemExit('Existing listener or trust-domain differs; migrate configuration explicitly')
PY
test "$(hostname)" = "$GATEWAY_STAGING_HOSTNAME" || { echo 'Target hostname mismatch' >&2; exit 1; }
test -x /opt/llm-gateway/current/gateway
if test -f /etc/llm-gateway/server.pem; then
  openssl verify -CAfile /etc/llm-gateway/ca.pem -verify_ip "$GATEWAY_STAGING_ADDRESS" -purpose sslserver /etc/llm-gateway/server.pem >/dev/null
fi
umask 077
if ! id gatewayfixture >/dev/null 2>&1; then useradd --system --no-create-home --shell /usr/sbin/nologin gatewayfixture; fi
chown root:root /opt/llm-gateway /opt/llm-gateway/releases
chmod 755 /opt/llm-gateway /opt/llm-gateway/releases
release=$(readlink -f /opt/llm-gateway/current)
case "$release" in /opt/llm-gateway/releases/*) ;; *) echo "invalid release target" >&2; exit 1;; esac
chown -R root:root "$release"
chmod 755 "$release" "$release/gateway"
cfg=/etc/llm-gateway
mkdir -p "$cfg/operator"
chmod 700 "$cfg/operator"
chown root:root "$cfg/operator"
if ! test -f "$cfg/ca.pem"; then
  openssl req -x509 -newkey rsa:2048 -sha256 -nodes -keyout "$cfg/operator/ca.key" -out "$cfg/ca.pem" -subj /CN=GatewayStagingCA -days 90 >/dev/null 2>&1
fi
for role in server requester approver; do
  dest="$cfg/operator"
  if test "$role" = server; then dest="$cfg"; fi
  if ! test -f "$dest/$role.pem"; then
    openssl req -new -newkey rsa:2048 -sha256 -nodes -keyout "$dest/$role.key" -out "$cfg/$role.csr" -subj "/CN=GatewayStaging-$role" >/dev/null 2>&1
    if test "$role" = server; then
      printf 'subjectAltName=IP:%s,IP:127.0.0.1,DNS:gateway-staging.example.test\nextendedKeyUsage=serverAuth\n' "$GATEWAY_STAGING_ADDRESS" >"$cfg/$role.ext"
    else
      printf 'subjectAltName=URI:spiffe://gateway-staging.example.test/workload/%s\nextendedKeyUsage=clientAuth\n' "$role" >"$cfg/$role.ext"
    fi
    openssl x509 -req -in "$cfg/$role.csr" -CA "$cfg/ca.pem" -CAkey "$cfg/operator/ca.key" -CAserial "$cfg/ca.srl" -CAcreateserial -sha256 -out "$dest/$role.pem" -days 30 -extfile "$cfg/$role.ext" >/dev/null 2>&1
  fi
done
if ! test -f "$cfg/audit.pem"; then /opt/llm-gateway/current/gateway keygen "$cfg/audit.pem"; fi
python3 /opt/llm-gateway/current/deploy/configure-staging.py
# Certificate issuance and client identities are operator material. The service
# can read only its server key, trust roots, policy and audit signing key.
chown root:gatewaylab "$cfg"
chmod 750 "$cfg"
chown gatewaylab:gatewaylab "$cfg/server.key" "$cfg/server.pem" "$cfg/audit.pem"
chmod 600 "$cfg/server.key" "$cfg/audit.pem"
chown root:gatewaylab "$cfg/gateway.json" "$cfg/ca.pem"
chmod 640 "$cfg/gateway.json" "$cfg/ca.pem"
# Repeated rollout preserves operator credentials, CA, signing key and DB state.
install -m 644 /opt/llm-gateway/current/deploy/gateway.service /etc/systemd/system/llm-gateway.service
install -m 644 /opt/llm-gateway/current/deploy/fixture@.service /etc/systemd/system/llm-gateway-fixture@.service
systemctl daemon-reload
for name in capability ok outside safety timed down; do
  systemctl enable "llm-gateway-fixture@$name.service"
  systemctl restart "llm-gateway-fixture@$name.service"
done
systemctl enable llm-gateway.service
systemctl restart llm-gateway.service
