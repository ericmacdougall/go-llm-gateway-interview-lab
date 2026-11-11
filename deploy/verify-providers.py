"""Six bounded synthetic probes through the deployed gateway. No account keys are read."""
import ipaddress,json,os,ssl,urllib.request,urllib.error,subprocess,sys
from pathlib import Path

p=Path('/etc/llm-gateway')
address=ipaddress.IPv4Address(os.environ.get('GATEWAY_STAGING_ADDRESS','127.0.0.1'))
if address.is_unspecified or address.is_multicast or str(address)=='255.255.255.255':
    raise SystemExit('Select a concrete unicast staging IPv4 address')
cfg=json.loads((p/'gateway.json').read_text())
if cfg.get('listen')!=f'{address}:8443':
    raise SystemExit('Probe address differs from configured listener')
ctx=ssl.create_default_context(cafile=str(p/'ca.pem'))
ctx.load_cert_chain(str(p/'operator/live.pem'),str(p/'operator/live.key'))
routes=cfg['gateway']['clients']['livecheck']['routes']
assert routes,'no configured provider routes'
models=[route['model'] for route in routes]
protocols={route['protocol'] for route in routes}
missing={'openai','anthropic','openrouter'}-protocols
failures=[]
for model in models:
    for stream in [False,True]:
        data=json.dumps(dict(model=model,max_tokens=32,stream=stream,messages=[dict(role='user',content='Reply with OK only.')])).encode()
        request=urllib.request.Request(f'https://{address}:8443/v1/chat/completions',data=data,headers={'Content-Type':'application/json'})
        try:
            with urllib.request.urlopen(request,context=ctx,timeout=45) as response:
                raw=response.read(262144)
                request_id=response.headers.get('X-Request-ID')
                if stream:
                    events=[line[6:] for line in raw.decode().splitlines() if line.startswith('data: ')]
                    assert events and events[-1]=='[DONE]','missing SSE terminator'
                    decoded=[json.loads(event) for event in events[:-1]]
                    assert all('error' not in event for event in decoded),'SSE failure'
                    assert any(event.get('choices',[{}])[0].get('delta',{}).get('content') for event in decoded),'no text delta'
                else:
                    result=json.loads(raw)
                    assert result['choices'][0]['message']['content'],'no completion text'
                assert request_id,'missing request trace'
                # IDs are generated locally, pass to psql using a variable-free
                # restricted hex form after checking the exact expected syntax.
                assert request_id.startswith('req-') and all(c in '0123456789abcdef' for c in request_id[4:]),'invalid trace ID'
                query="SELECT state||':'||COALESCE(actual::text,'pending') FROM gateway_attempts WHERE client_id='livecheck' AND request_id='"+request_id+"'"
                state=subprocess.check_output(['runuser','-u','gatewaylab','--','psql','-XAt','-d','llmgateway','-c',query],text=True).strip()
                assert state.startswith('settled:'),'usage not settled'
                print(json.dumps(dict(model=model,stream=stream,status=200,trace=request_id,accounting=state)))
        except urllib.error.HTTPError as exc:
            # Gateway errors are normalized and contain no provider credentials.
            body=json.load(exc)
            code=body.get('error',{}).get('code','unknown')
            failures.append(dict(model=model,stream=stream,status=exc.code,code=code))
        except Exception as exc:
            failures.append(dict(model=model,stream=stream,error=type(exc).__name__))
if failures:
    print(json.dumps({'failures':failures}))
    raise SystemExit(1)
if missing:
    print('LIVE_PROVIDERS_PARTIAL missing '+', '.join(sorted(missing)))
    if '--allow-partial' not in sys.argv: raise SystemExit(2)
else:
    print('LIVE_PROVIDERS_PASS OpenAI direct, Anthropic direct, OpenRouter; text and SSE, traced and settled')
