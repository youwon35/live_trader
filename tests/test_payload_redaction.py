"""Exercise production redaction without importing broker services or user data."""
import ast
from functools import lru_cache
from pathlib import Path
from types import SimpleNamespace
import re
import unittest
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

SOURCE = Path(__file__).resolve().parents[1] / 'live_trader' / 'state.py'


class Environment(dict):
    scans = 0

    def items(self):
        self.scans += 1
        return super().items()


def redactors(environment):
    tree = ast.parse(SOURCE.read_text(encoding='utf-8-sig'))
    first = next(i for i, node in enumerate(tree.body)
                 if isinstance(node, ast.Assign)
                 and any(isinstance(t, ast.Name) and t.id == 'SENSITIVE_SNAPSHOT_KEYS' for t in node.targets))
    last = next(i for i, node in enumerate(tree.body)
                if isinstance(node, ast.FunctionDef) and node.name == 'future_text')
    namespace = {'os': SimpleNamespace(environ=environment), 're': re,
                 'lru_cache': lru_cache, 'parse_qsl': parse_qsl, 'urlencode': urlencode,
                 'urlsplit': urlsplit, 'urlunsplit': urlunsplit,
                 'LIVE_TRADER_SECRET_KEYS': {'CUSTOM_CONFIGURED_SECRET'}}
    exec(compile(ast.Module(body=tree.body[first:last], type_ignores=[]), str(SOURCE), 'exec'), namespace)
    return namespace


class PayloadRedactionTests(unittest.TestCase):
    def test_large_nested_payload_scans_environment_once_and_preserves_order_identity(self):
        env = Environment(BINANCE_API_KEY='example-key-1234', CUSTOM_CONFIGURED_SECRET='sensitive-token-5678')
        redact = redactors(env)['redact_sensitive_payload']
        value = {'rows': [{'text': 'example-key-1234 sensitive-token-5678', 'orderId': f'order-{i}',
                           'account_id': '12345678', 'count': i} for i in range(1000)]}
        result = redact(value)
        self.assertEqual(env.scans, 1)
        self.assertEqual(result['rows'][-1], {'text': '*** ***', 'orderId': 'order-999', 'account_id': '***', 'count': 999})
        self.assertEqual(value['rows'][0]['account_id'], '12345678')

    def test_new_operation_reads_rotated_credentials_and_does_not_cache_them(self):
        env = Environment(BINANCE_API_KEY='first-key-1234')
        functions = redactors(env)
        self.assertEqual(functions['redact_sensitive_payload']('first-key-1234'), '***')
        env['BINANCE_API_KEY'] = 'second-key-5678'
        self.assertEqual(functions['redact_sensitive_payload']('second-key-5678'), '***')
        self.assertEqual(functions['mask_secrets']('second-key-5678'), '***')
        self.assertEqual(env.scans, 3)

    def test_structured_and_freeform_credentials_keep_existing_masking_contract(self):
        redact = redactors(Environment())['redact_sensitive_payload']
        result = redact({'values': (
            'Authorization: Bearer sensitive-123',
            'api-key=hidden-key',
            'https://api.telegram.org/bot12345:secret/sendMessage',
            'https://host/path?token=hidden&orderId=456',
            'eyJabcdefghijk.abcdefghijkl.abcdefghijkl',
        ), 'token': '', 'api_secret': None, 'signed': {'signature': 'abc'}, 'quantity': 0, 'ok': False})
        self.assertEqual(result['values'], [
            'Authorization: Bearer ***', 'api-key=***',
            'https://api.telegram.org/bot***/sendMessage',
            'https://host/path?token=%2A%2A%2A&orderId=456', '***'])
        self.assertEqual(result['token'], '')
        self.assertIsNone(result['api_secret'])
        self.assertEqual(result['signed'], {'signature': '***'})
        self.assertEqual(result['quantity'], 0)
        self.assertIs(result['ok'], False)


if __name__ == '__main__':
    unittest.main()
