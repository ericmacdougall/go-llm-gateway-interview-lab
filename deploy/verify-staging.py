"""Exercise an operator-selected disposable staging gateway; this mutates state."""
import ipaddress, json, os, ssl, urllib.request, urllib.error, time, subprocess, uuid
from pathlib import Path

cfg=Path('/etc/llm-gateway')
address=ipaddress.IPv4Address(os.environ.get('GATEWAY_STAGING_ADDRESS','127.0.0.1'))
if address.is_unspecified or address.is_multicast or str(address)=='255.255.255.255':
    raise SystemExit('Select a concrete unicast staging IPv4 address')
if json.loads((cfg/'gateway.json').read_text()).get('listen')!=f'{address}:8443':
    raise SystemExit('Probe address differs from configured listener')
base=f'https://{address}:8443'
def call(path,body=None,role='requester'):
    ctx=ssl.create_default_context(cafile=str(cfg/'ca.pem'))
    if role:
        ctx.load_cert_chain(str(cfg/f'operator/{role}.pem'),str(cfg/f'operator/{role}.key'))
    data=None if body is None else json.dumps(body).encode()
    request=urllib.request.Request(base+path,data=data,headers={'Content-Type':'application/json'})
    try:
        with urllib.request.urlopen(request,context=ctx,timeout=10) as response:
            return response.status,json.load(response)
    except urllib.error.HTTPError as exc:
        return exc.code,json.load(exc)
def infer(model,max_tokens=32,stream=False):
    return call('/v1/chat/completions',dict(model=model,max_tokens=max_tokens,stream=stream,
                messages=[{'role':'user','content':'synthetic deployment check'}]))
def expect(actual,want):
    assert actual==want,(actual,want)
def sql(query):
    return subprocess.check_output(['runuser','-u','gatewaylab','--','psql','-XAt','-v','ON_ERROR_STOP=1',
                                   '-d','llmgateway','-c',query],text=True).strip()
expect(call('/healthz')[0],200)
# Readiness has a plain response, checked separately through curl.
try:
    call('/healthz',role=None)
    raise AssertionError('certless request completed')
except (urllib.error.URLError,ssl.SSLError):
    pass
expect(infer('demo-model')[0],200)
status,body=infer('outside-model')
expect((status,body['error']['code']),(403,'no_eligible_route'))
expect(infer('safety-model')[0],422)
expect(infer('down-model')[0],503)
# A staging budget is intentionally large enough for repeated rehearsal runs.
expect(infer('demo-model',4096)[0],200)
# First prove the approval route is denied, then request a scoped grant.
expect(infer('approval-model')[0],403)
approval_id='grant-'+uuid.uuid4().hex
status,body=call('/v1/approvals',dict(id=approval_id,model='approval-model',ttl_seconds=120))
expect((status,body['state']),(200,'pending'))
expect(call(f'/v1/approvals/{approval_id}/decision',dict(allow=True,reason='self-check'))[0],403)
status,body=call(f'/v1/approvals/{approval_id}/decision',dict(allow=True,reason='staging acceptance'),role='approver')
expect((status,body['state']),(200,'approved'))
expect(infer('approval-model')[0],200)
status,body=call(f'/v1/approvals/{approval_id}/revoke',dict(reason='staging check complete'),role='approver')
expect((status,body['state']),(200,'revoked'))
expect(infer('approval-model')[0],403)
before=int(sql("SELECT count(*) FROM gateway_attempts WHERE attempt->'route'->>'provider'='timed' AND state='settled' AND actual>0"))
expect(infer('timed-model')[0],200)
for _ in range(60):
    unsettled=int(sql("SELECT count(*) FROM gateway_attempts WHERE state IN ('dispatching','uncertain')"))
    if unsettled==0:break
    time.sleep(.2)
expect(unsettled,0)
expect(sql("SELECT held FROM gateway_budgets WHERE client_id='staging'"),'0')
expect(sql("SELECT count(*) FROM gateway_attempts WHERE attempt->'route'->>'environment'='external'"),'0')
expect(int(sql("SELECT count(*) FROM gateway_attempts WHERE attempt->'route'->>'provider'='timed' AND state='settled' AND actual>0")),before+1)
subprocess.run(['runuser','-u','gatewaylab','--','env',
    'GATEWAY_DATABASE_URL=host=/var/run/postgresql dbname=llmgateway user=gatewaylab sslmode=disable',
    '/opt/llm-gateway/current/gateway','verify','-config','/etc/llm-gateway/gateway.json'],check=True)
print('DEPLOYED_ACCEPTANCE_PASS mTLS policy safety fallback approvals revoke reconciliation audit')
