"""Enable explicitly external-approved synthetic probes using provisioned credentials."""
import json
import os
from pathlib import Path
p=Path('/etc/llm-gateway/gateway.json')
cfg=json.loads(p.read_text())
identity='spiffe://gateway-staging.example.test/workload/live'
cfg['gateway']['identities'][identity]=dict(client='livecheck',workload=identity,department='engineering',app='provider-acceptance',role='user')
available={line.split("=",1)[0] for line in Path('/etc/llm-gateway/provider.env').read_text().splitlines()}
routes=[]
for protocol,provider,endpoint,model,auth in [
    ('openai','openai-direct','https://api.openai.com',os.environ.get('GATEWAY_OPENAI_MODEL'),'GATEWAY_OPENAI_KEY'),
    ('anthropic','anthropic-direct','https://api.anthropic.com',os.environ.get('GATEWAY_ANTHROPIC_MODEL'),'GATEWAY_ANTHROPIC_KEY'),
    ('openrouter','openrouter','https://openrouter.ai',os.environ.get('GATEWAY_OPENROUTER_MODEL'),'GATEWAY_OPENROUTER_KEY')]:
    if auth not in available: continue
    if not model or len(model)>256 or any(ord(c)<32 for c in model):
        raise SystemExit('Set a valid approved model name for each configured provider')
    routes.append(dict(id=protocol+'-live',protocol=protocol,auth_env=auth,provider=provider,account='operator-configured-account',
                       model=model,region='external-approved',environment='external-approved',endpoint=endpoint,
                       capabilities=['chat'],input_unit=1,output_unit=1))
cfg['gateway']['clients']['livecheck']=dict(version='external-v1',rule='synthetic-live-probes',
    expression='client == "livecheck" && environment == "external-approved"',environment='external-approved',routes=routes)
cfg['budgets']['livecheck']=10000
cfg['gateway']['request_timeout_ms']=30000
cfg['gateway']['attempt_timeout_ms']=20000
for route in cfg['gateway']['clients']['staging']['routes']:
    route['timeout_ms']=800
if not routes:
    raise SystemExit('No configured provider credentials')
p.write_text(json.dumps(cfg,indent=2)+'\n')
