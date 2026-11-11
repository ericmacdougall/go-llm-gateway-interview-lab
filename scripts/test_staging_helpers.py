#!/usr/bin/env python3
"""Hermetic tests for staging templates: no keys, provider calls or OS changes."""
from pathlib import Path
from unittest.mock import patch
import json
import os
import runpy
import socket
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
CONFIG = '/etc/llm-gateway/gateway.json'
INVALID = ('0.0.0.0', '224.0.0.1', '255.255.255.255',
           '127.0.0.1\nextendedKeyUsage=clientAuth', 'not-an-address')


class StagingHelpers(unittest.TestCase):
    def configure(self, address=None, existing=None):
        files = {} if existing is None else {CONFIG: json.dumps(existing)}
        writes = []

        def write(path, text, *args, **kwargs):
            files[str(path)] = text
            writes.append(str(path))
            return len(text)

        env = {} if address is None else {'GATEWAY_STAGING_ADDRESS': address}
        with patch.dict(os.environ, env, clear=True), \
                patch.object(Path, 'exists', lambda p: str(p) in files), \
                patch.object(Path, 'read_text', lambda p, *a, **k: files[str(p)]), \
                patch.object(Path, 'write_text', write):
            runpy.run_path(str(ROOT / 'deploy/configure-staging.py'))
        return json.loads(files[CONFIG]), writes

    def test_listener_identity_and_certificate_extension_agree(self):
        script = (ROOT / 'deploy/install-staging.sh').read_text()
        san_line = next(line.strip().split(' >')[0] for line in script.splitlines()
                        if "printf 'subjectAltName=IP:" in line)
        for address in (None, '127.0.0.1', '192.0.2.10'):
            with self.subTest(address=address):
                cfg, writes = self.configure(address)
                selected = address or '127.0.0.1'
                self.assertEqual(cfg['listen'], selected + ':8443')
                self.assertEqual(set(cfg['gateway']['identities']), {
                    'spiffe://gateway-staging.example.test/workload/' + role
                    for role in ('requester', 'approver')})
                self.assertEqual(len(writes), 7)
                result = subprocess.run(['sh'], input=san_line,
                    env={'PATH': os.environ['PATH'], 'GATEWAY_STAGING_ADDRESS': selected},
                    capture_output=True, text=True, check=True)
                self.assertEqual(result.stdout,
                    f'subjectAltName=IP:{selected},IP:127.0.0.1,DNS:gateway-staging.example.test\n'
                    'extendedKeyUsage=serverAuth\n')

    def test_existing_configuration_is_preserved_and_mismatch_rejected(self):
        cfg, _ = self.configure()
        cfg['budgets']['staging'] = 12345
        preserved, writes = self.configure(existing=cfg)
        self.assertEqual(preserved, cfg)
        self.assertNotIn(CONFIG, writes)
        with self.assertRaises(SystemExit):
            self.configure('127.0.0.1', {'listen': '192.0.2.10:8443'})

    def test_invalid_addresses_rejected_before_writes_or_credentials(self):
        for name in ('configure-staging.py', 'verify-staging.py', 'verify-providers.py'):
            for address in INVALID:
                with self.subTest(helper=name, address=address):
                    writes = []
                    with patch.dict(os.environ, {'GATEWAY_STAGING_ADDRESS': address}, clear=True), \
                            patch.object(Path, 'exists', lambda p: False), \
                            patch.object(Path, 'write_text', lambda *a, **k: writes.append(a)), \
                            patch('ssl.create_default_context', side_effect=AssertionError(
                                'credentials accessed before address validation')):
                        with self.assertRaises((ValueError, SystemExit)):
                            runpy.run_path(str(ROOT / 'deploy' / name))
                    self.assertEqual(writes, [])

    def test_probes_reject_mismatched_listener_before_credentials(self):
        for name in ('verify-staging.py', 'verify-providers.py'):
            with self.subTest(helper=name), \
                    patch.dict(os.environ, {}, clear=True), \
                    patch.object(Path, 'read_text', lambda *a, **k:
                        json.dumps({'listen': '192.0.2.10:8443'})), \
                    patch('ssl.create_default_context', side_effect=AssertionError(
                        'credentials accessed before listener check')):
                with self.assertRaises(SystemExit):
                    runpy.run_path(str(ROOT / 'deploy' / name))

    def test_provider_models_are_explicit_before_configuration_write(self):
        cfg = {'gateway': {'identities': {}, 'clients': {'staging': {'routes': []}}},
               'budgets': {}}
        files = {CONFIG: json.dumps(cfg), '/etc/llm-gateway/provider.env':
                 'GATEWAY_OPENAI_KEY=\nGATEWAY_ANTHROPIC_KEY=\nGATEWAY_OPENROUTER_KEY=\n'}
        for good in (False, True):
            env = {f'GATEWAY_{p}_MODEL': 'example-approved-model'
                   for p in ('OPENAI', 'ANTHROPIC', 'OPENROUTER')} if good else {}
            writes = []
            with self.subTest(valid=good), patch.dict(os.environ, env, clear=True), \
                    patch.object(Path, 'read_text', lambda p, *a, **k: files[str(p)]), \
                    patch.object(Path, 'write_text', lambda p, text, *a, **k: writes.append(text)):
                if not good:
                    with self.assertRaises(SystemExit):
                        runpy.run_path(str(ROOT / 'deploy/enable-providers.py'))
                    self.assertEqual(writes, [])
                else:
                    runpy.run_path(str(ROOT / 'deploy/enable-providers.py'))
                    self.assertEqual(len(writes), 1)
                    current = json.loads(writes[0])
                    routes = current['gateway']['clients']['livecheck']['routes']
                    self.assertEqual(len(routes), 3)
                    self.assertTrue(all(route['model'] == 'example-approved-model' and
                        route['account'] == 'operator-configured-account' for route in routes))
                    self.assertIn('spiffe://gateway-staging.example.test/workload/live',
                                  current['gateway']['identities'])

    def test_installer_rejects_bad_target_before_mutation_utilities(self):
        with tempfile.TemporaryDirectory(prefix='gateway-lab-guard-test-') as directory:
            directory = Path(directory)
            effects = directory / 'effects'
            # Execute the installer's actual Python guard with an empty virtual
            # target filesystem; never inspect this machine's /etc configuration.
            python = directory / 'python3'
            python.write_text('#!' + sys.executable + '\nimport pathlib,sys\n'
                'from unittest.mock import patch\nsource=sys.stdin.read()\n'
                'with patch.object(pathlib.Path,"exists",lambda p:False):'
                'exec(compile(source,"installer-preflight","exec"))\n')
            python.chmod(0o755)
            for name in ('useradd', 'chown', 'chmod', 'mkdir', 'openssl', 'install', 'systemctl'):
                stub = directory / name
                stub.write_text('#!/bin/sh\nprintf mutation >> "' + str(effects) + '"\nexit 98\n')
                stub.chmod(0o755)
            base = {'PATH': str(directory) + ':' + os.environ['PATH']}
            cases = [({}, 'Set the exact staging hostname'),
                ({'GATEWAY_STAGING_HOSTNAME': 'other-target.example.test'}, 'Target hostname mismatch'),
                ({'GATEWAY_STAGING_HOSTNAME': 'bad\nhost'}, 'Invalid expected hostname')]
            for address in INVALID:
                cases.append(({'GATEWAY_STAGING_HOSTNAME': socket.gethostname(),
                               'GATEWAY_STAGING_ADDRESS': address},
                              'Select a concrete' if address in INVALID[:3] else 'AddressValueError'))
            for env, message in cases:
                with self.subTest(target=env):
                    result = subprocess.run(['sh', str(ROOT / 'deploy/install-staging.sh')],
                        env=dict(base, **env), capture_output=True, text=True)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn(message, result.stderr)
                    self.assertFalse(effects.exists())
            # A positive control runs precisely the preflight prefix; it does
            # not attempt privileged provisioning or generate certificates.
            prefix = (ROOT / 'deploy/install-staging.sh').read_text().split(
                'test -x /opt/llm-gateway/current/gateway')[0]
            result = subprocess.run(['sh'], input=prefix, env=dict(base,
                GATEWAY_STAGING_HOSTNAME=socket.gethostname(),
                GATEWAY_STAGING_ADDRESS='192.0.2.10'), capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse(effects.exists())


if __name__ == '__main__':
    unittest.main(verbosity=2)
