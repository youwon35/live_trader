"""Only synthetic C/temp SQLite books are created; no real capital is touched."""
from contextlib import closing
import importlib
import json
from pathlib import Path
import socket
import sqlite3
import sys
from tempfile import TemporaryDirectory
from types import ModuleType
import unittest
from unittest.mock import patch

NAME = '_existing_kis_owned_read_test'
package = ModuleType(NAME)
package.__path__ = [str(Path(__file__).resolve().parents[1] / 'live_trader')]
sys.modules[NAME] = package
local = importlib.import_module(NAME + '.kis_shared_krw_local')
shared = importlib.import_module(NAME + '.kis_shared_krw_readiness')
BINDING = {'profileId': 'a' * 64, 'accountFingerprint': 'b' * 64,
    'credentialConfigurationHash': 'c' * 64}


def principal(binding=BINDING, *, deployment='existing', currency='KRW'):
    scope = {'version': 'kis-owned-capital-v2', 'deploymentId': deployment,
        'accountFingerprint': binding['accountFingerprint'],
        'credentialConfigurationHash': binding['credentialConfigurationHash'],
        'accountProfileId': binding['profileId'], 'reviewBindingHash': 'd' * 64,
        'capitalPercent': '5', 'quoteCurrency': currency,
        'members': [{'strategyInstanceId': deployment + '-member',
            'sourceArtifactHash': 'e' * 64, 'portfolioHash': 'f' * 64,
            'symbol': '005930' if currency == 'KRW' else 'AAPL', 'quoteCurrency': currency}]}
    scope['scopeHash'] = shared._hash(scope)
    return {'scope': scope, 'principal': '1000', 'occurredAt': '2026-10-19T01:00:00+00:00'}


def create_existing_database(path, bodies=None):
    """Synthetic fixture only, deliberately outside the helper under test."""
    db = sqlite3.connect(path)
    db.execute('CREATE TABLE principals (deployment_id TEXT PRIMARY KEY, account_currency TEXT NOT NULL, payload TEXT NOT NULL, content_hash TEXT NOT NULL)')
    db.execute('CREATE TABLE receipts (event_key TEXT PRIMARY KEY, deployment_id TEXT NOT NULL, payload TEXT NOT NULL, content_hash TEXT NOT NULL)')
    for body in ([principal()] if bodies is None else bodies):
        insert_principal(db, body)
    db.commit()
    db.close()


def insert_principal(db, body):
    scope = body['scope']
    db.execute('INSERT INTO principals VALUES (?,?,?,?)', (scope['deploymentId'],
        scope['accountFingerprint'] + ':' + scope['quoteCurrency'], json.dumps(body), shared._hash(body)))


class ExistingOwnedReadTests(unittest.TestCase):
    def setUp(self):
        self.work = TemporaryDirectory()
        self.addCleanup(self.work.cleanup)
        self.root = Path(self.work.name)
        self.path = self.root / 'existing.sqlite3'
        self.no_network = patch.object(socket.socket, 'connect', side_effect=AssertionError('network forbidden'))
        self.no_network.start()
        self.addCleanup(self.no_network.stop)

    def inspect(self, binding=BINDING):
        result = local.read_existing_owned_books(self.path, binding)
        for key in local._FLAGS:
            self.assertIs(result[key], False, key)
        for key in ('path', 'databaseHash', 'sidecarHash', 'accountBinding'):
            self.assertNotIn(key, result)
        return result

    def test_missing_does_not_open_sqlite_or_create_parent_or_return_zero(self):
        self.path = self.root / 'missing-parent' / 'missing.sqlite3'
        with patch.object(local.sqlite3, 'connect', side_effect=AssertionError('missing must not open')):
            result = self.inspect()
        self.assertEqual(result['status'], 'UNKNOWN')
        self.assertIsNone(result['ownedBooks'])
        self.assertIsNone(result['dependencyHash'])
        self.assertFalse(self.path.parent.exists())

    def test_reads_only_existing_normal_store_and_no_initializer_or_db_context(self):
        create_existing_database(self.path, [principal(), principal(deployment='overseas', currency='USD')])
        before = self.path.read_bytes()
        filenames = {p.name for p in self.root.iterdir()}
        with patch.object(local.KisOwnedCapitalStore, 'initialize', side_effect=AssertionError('no init')), \
             patch.object(local.KisOwnedCapitalStore, '_db', side_effect=AssertionError('no writable context')):
            result = self.inspect()
        self.assertEqual(result['status'], 'OBSERVED_LOCAL_INVENTORY_ONLY')
        self.assertEqual(len(result['ownedBooks']), 2)
        self.assertEqual({book['scope']['quoteCurrency'] for book in result['ownedBooks']}, {'KRW', 'USD'})
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual({p.name for p in self.root.iterdir()}, filenames)
        self.assertEqual(result['dependencyHash'], shared._hash(result['ownedBooks']))
        with patch.object(local, 'MAX_BOOKS', 1):
            bounded = self.inspect()
        self.assertEqual(bounded['status'], 'UNKNOWN')
        self.assertIn('KIS_LOCAL_OWNED_BOOK_LIMIT_EXCEEDED', bounded['reasons'])
        self.assertEqual(self.path.read_bytes(), before)

    def test_corrupt_seal_schema_or_receipt_discards_whole_inventory(self):
        for kind in ('seal', 'schema', 'receipt'):
            with self.subTest(kind=kind):
                self.path = self.root / (kind + '.sqlite3')
                create_existing_database(self.path)
                with closing(sqlite3.connect(self.path)) as db:
                    if kind == 'seal':
                        db.execute('UPDATE principals SET content_hash=?', ('0' * 64,))
                    elif kind == 'schema':
                        db.execute('ALTER TABLE principals ADD COLUMN unexpected TEXT')
                    else:
                        db.execute('INSERT INTO receipts VALUES (?,?,?,?)',
                            ('event', 'existing', '{"bad":"record"}', '0' * 64))
                    db.commit()
                before = self.path.read_bytes()
                result = self.inspect()
                self.assertEqual(result['status'], 'UNKNOWN')
                self.assertIsNone(result['ownedBooks'])
                self.assertEqual(self.path.read_bytes(), before)

    def test_same_account_old_mode_configuration_is_kept_for_binding_rejection(self):
        old_binding = {**BINDING, 'profileId': '1' * 64, 'credentialConfigurationHash': '2' * 64}
        other_account = {**BINDING, 'accountFingerprint': '3' * 64}
        create_existing_database(self.path, [principal(old_binding),
            principal(other_account, deployment='unrelated')])
        result = self.inspect()
        self.assertEqual(result['status'], 'OBSERVED_LOCAL_INVENTORY_ONLY')
        self.assertEqual(len(result['ownedBooks']), 1)
        scope = result['ownedBooks'][0]['scope']
        self.assertEqual(scope['credentialConfigurationHash'], old_binding['credentialConfigurationHash'])
        self.assertNotEqual(scope['accountProfileId'], BINDING['profileId'])
        self.assertFalse(result['accountCashReconciled'])

    def test_external_commit_during_separate_project_read_discards_all(self):
        create_existing_database(self.path)
        real_project = local.KisOwnedCapitalStore.project
        def interleaved(store, scope, marks):
            value = real_project(store, scope, marks)
            with closing(sqlite3.connect(self.path)) as writer:
                insert_principal(writer, principal(deployment='concurrently-added'))
                writer.commit()
            return value
        with patch.object(local.KisOwnedCapitalStore, 'project', interleaved):
            result = self.inspect()
        self.assertEqual(result['status'], 'UNKNOWN')
        self.assertIsNone(result['ownedBooks'])
        self.assertIsNone(result['dependencyHash'])
        self.assertIn('KIS_LOCAL_OWNED_CHANGED_DURING_READ', result['reasons'])

    def test_empty_existing_inventory_is_local_empty_not_zero_account_funds(self):
        create_existing_database(self.path, [])
        result = self.inspect()
        self.assertEqual(result['ownedBooks'], [])
        self.assertEqual(result['status'], 'OBSERVED_LOCAL_INVENTORY_ONLY')
        self.assertFalse(result['accountCashReconciled'])
        self.assertNotIn('availableCash', result)

    def test_wal_or_hot_sidecar_is_unknown_without_opening_or_creating_sqlite_files(self):
        create_existing_database(self.path)
        sidecar = Path(str(self.path) + '-journal')
        sidecar.write_bytes(b'synthetic-existing-journal')
        before = {p.name: p.read_bytes() for p in self.root.iterdir()}
        with patch.object(local.sqlite3, 'connect', side_effect=AssertionError('sidecar must not open')):
            result = self.inspect()
        self.assertEqual(result['status'], 'UNKNOWN')
        self.assertEqual({p.name: p.read_bytes() for p in self.root.iterdir()}, before)
        sidecar.unlink()
        header = bytearray(self.path.read_bytes())
        header[18:20] = b'\x02\x02'
        self.path.write_bytes(header)
        with patch.object(local.sqlite3, 'connect', side_effect=AssertionError('WAL header must not open')):
            result = self.inspect()
        self.assertIn('KIS_LOCAL_OWNED_WAL_MODE_UNSUPPORTED', result['reasons'])


if __name__ == '__main__':
    unittest.main()
