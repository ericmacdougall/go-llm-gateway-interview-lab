"""Configure an explicitly selected, isolated synthetic staging instance."""
import ipaddress
import json
import os
from pathlib import Path
p=Path('/etc/llm-gateway')
address=ipaddress.IPv4Address(os.environ.get('GATEWAY_STAGING_ADDRESS','127.0.0.1'))
if address.is_unspecified or address.is_multicast or str(address)=='255.255.255.255':
    raise SystemExit('Select a concrete unicast staging IPv4 address')
listen=f'{address}:8443'
if (p/'gateway.json').exists() and json.loads((p/'gateway.json').read_text()).get('listen')!=listen:
    raise SystemExit('Existing configuration listener differs; explicitly migrate it before reconfiguration')
def route(name,model,env,approval=False):
    ports={'capability':18441,'ok':18442,'outside':18443,'safety':18444,'timed':18445,'down':18446}
    return dict(id=f'{name}-{model}',provider=name,account='staging-fixture',model=model,
                region=env,environment=env,endpoint=f'http://127.0.0.1:{ports[name]}',
                capabilities=['chat'],input_unit=1,output_unit=1,requires_approval=approval,timeout_ms=800)
routes=[route('capability','demo-model','client-cloud'),route('ok','demo-model','client-cloud'),
        route('outside','outside-model','external'),route('safety','safety-model','client-cloud'),
        route('ok','safety-model','client-cloud'),route('timed','timed-model','client-cloud'),
        route('ok','timed-model','client-cloud'),route('down','down-model','client-cloud'),
        route('outside','down-model','external'),route('ok','approval-model','client-cloud',True)]
identities={}
for who,role in [('requester','user'),('approver','approver')]:
    uri=f'spiffe://gateway-staging.example.test/workload/{who}'
    identities[uri]=dict(client='staging',workload=uri,role=role,department='engineering',app='lab')
cfg=dict(listen=listen,tls_cert=str(p/'server.pem'),tls_key=str(p/'server.key'),
         client_ca=str(p/'ca.pem'),signing_key=str(p/'audit.pem'),budgets={'staging':100000000},
         reconcile_seconds=3,reconcile_lock_key=8190045,approver_role='approver',approval_max_seconds=3600,
         gateway=dict(identities=identities,clients={'staging':dict(version='staging-v1',rule='cloud-boundary',
            expression='client == "staging" && environment == "client-cloud"',environment='client-cloud',routes=routes)},
            max_attempts=3,request_timeout_ms=6000,attempt_timeout_ms=800,allow_loopback_http=True))
if not (p/'gateway.json').exists():
    (p/'gateway.json').write_text(json.dumps(cfg,indent=2)+'\n')
for i,(name,mode) in enumerate([('capability','capability'),('ok','ok'),('outside','ok'),('safety','safety'),('timed','timeout_then_usage'),('down','unavailable')],18441):
    (p/f'fixture-{name}.env').write_text(f'LISTEN=127.0.0.1:{i}\nMODE={mode}\n')
