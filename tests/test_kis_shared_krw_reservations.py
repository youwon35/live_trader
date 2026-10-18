"""Synthetic local budgets only: no accounts, orders, FX or capital producers."""
from contextlib import closing
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
from tempfile import TemporaryDirectory
import time
import unittest
from unittest.mock import patch

SOURCE = Path(__file__).resolve().parents[1] / 'live_trader/kis_shared_krw_reservations.py'
spec = importlib.util.spec_from_file_location('_pure_kis_shared_krw_reservations_test', SOURCE)
budget = importlib.util.module_from_spec(spec)
spec.loader.exec_module(budget)


def policy(**changes):
    value = {'schemaVersion': budget.POLICY_SCHEMA, 'accountFingerprint': 'a' * 64,
        'accountProfileId': 'b' * 64, 'credentialConfigurationHash': 'c' * 64,
        'capKrw': '100', 'reviewBindingHash': 'd' * 64}
    value.update(changes)
    value.pop('policyHash', None)
    return {**value, 'policyHash': budget.canonical_hash(value)}


def request(current=None, **changes):
    current = current or policy()
    value = {'schemaVersion': budget.REQUEST_SCHEMA, 'reservationId': 'reservation-one',
        'deploymentId': 'deployment-one', 'scopeHash': 'e' * 64,
        'accountFingerprint': current['accountFingerprint'],
        'accountProfileId': current['accountProfileId'],
        'credentialConfigurationHash': current['credentialConfigurationHash'],
        'requestedKrw': '60', 'basisHash': 'f' * 64, 'nativeCurrency': 'KRW',
        'policyHash': current['policyHash']}
    value.update(changes)
    value.pop('requestHash', None)
    return {**value, 'requestHash': budget.canonical_hash(value)}


def race_worker():
    path, ready, current, allocation = sys.argv[2:6]
    current, allocation = json.loads(current), json.loads(allocation)
    deadline = time.monotonic() + 10
    while not Path(ready).exists():
        if time.monotonic() >= deadline:
            raise RuntimeError('synthetic race start deadline')
        time.sleep(0.01)
    with closing(sqlite3.connect(path, timeout=5)) as db:
        try:
            db.execute('BEGIN IMMEDIATE')
            budget.ensure_schema(db)
            result = budget.reserve_in_transaction(db, policy=current, request=allocation)
            db.commit()
            print(json.dumps({'status': result['status'], 'idempotent': result['idempotent'],
                'reservationCount': result['pool']['reservationCount'],
                'reservedKrw': result['pool']['reservedKrw']}), flush=True)
        except budget.KisSharedKrwReservationError as error:
            db.rollback()
            print(json.dumps({'status': 'BLOCKED', 'reason': str(error)}), flush=True)


class SharedKrwReservationTests(unittest.TestCase):
    def setUp(self):
        self.work = TemporaryDirectory()
        self.addCleanup(self.work.cleanup)
        self.root = Path(self.work.name)
        self.path = self.root / 'synthetic-owned.sqlite3'
        self.no_network = patch.object(socket.socket, 'connect', side_effect=AssertionError('network forbidden'))
        self.no_network.start()
        self.addCleanup(self.no_network.stop)

    def reserve(self, allocation=None, current=None):
        current = current or policy()
        allocation = allocation or request(current)
        with closing(sqlite3.connect(self.path)) as db:
            try:
                db.execute('BEGIN IMMEDIATE')
                budget.ensure_schema(db)
                result = budget.reserve_in_transaction(db, policy=current, request=allocation)
                db.commit()
                return result
            except BaseException:
                db.rollback()
                raise

    def inspect(self):
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('BEGIN')
            return budget.inspect_in_transaction(db, policy()['accountFingerprint'])

    def assert_not_authority(self, value):
        self.assertTrue(value['localBudgetOnly'])
        for key in budget._FALSE:
            self.assertIs(value[key], False, key)

    def test_unknown_inspection_creates_no_schema_and_is_not_zero_cap(self):
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('BEGIN')
            result = budget.inspect_in_transaction(db, policy()['accountFingerprint'])
            self.assertEqual(db.execute('SELECT name FROM sqlite_master').fetchall(), [])
            self.assertEqual(result['status'], 'UNKNOWN_NO_POOL')
            for key in ('policy', 'capKrw', 'reservedKrw', 'remainingKrw', 'reservationCount', 'receipts'):
                self.assertIsNone(result[key], key)
            self.assert_not_authority(result)

    def test_domestic_and_foreign_use_same_pool_and_retry_is_restart_idempotent(self):
        first = self.reserve()
        self.assertFalse(first['idempotent'])
        second = self.reserve(request(reservationId='foreign', deploymentId='foreign-deployment',
            nativeCurrency='USD', requestedKrw='40'))
        self.assertEqual(second['pool']['reservedKrw'], '100')
        self.assertEqual(second['pool']['remainingKrw'], '0')
        retry = self.reserve()
        self.assertTrue(retry['idempotent'])
        self.assertEqual(retry['receipt'], first['receipt'])
        self.assertEqual(self.inspect()['reservationCount'], 2)
        with self.assertRaisesRegex(budget.KisSharedKrwReservationError, 'CAP_EXCEEDED'):
            self.reserve(request(reservationId='extra', deploymentId='extra', requestedKrw='1'))
        self.assertEqual(self.inspect()['reservedKrw'], '100')
        for value in (first, first['receipt'], second['pool']):
            self.assert_not_authority(value)

    def test_same_id_changed_content_or_duplicate_deployment_never_reallocates(self):
        self.reserve()
        for changes in ({'requestedKrw': '59'}, {'scopeHash': '1' * 64},
                        {'basisHash': '2' * 64}, {'nativeCurrency': 'USD'}):
            with self.subTest(changes=changes), self.assertRaisesRegex(
                    budget.KisSharedKrwReservationError, 'IDEMPOTENCY_CONFLICT'):
                self.reserve(request(**changes))
        with self.assertRaisesRegex(budget.KisSharedKrwReservationError, 'DEPLOYMENT_ALREADY_RESERVED'):
            self.reserve(request(reservationId='different-id'))
        self.assertEqual(self.inspect()['reservationCount'], 1)

    def test_changed_cap_profile_or_configuration_cannot_split_account_pool(self):
        self.reserve()
        for current in (policy(capKrw='200'), policy(accountProfileId='1' * 64),
                        policy(credentialConfigurationHash='2' * 64), policy(reviewBindingHash='3' * 64)):
            with self.subTest(current=current['policyHash']), self.assertRaisesRegex(
                    budget.KisSharedKrwReservationError, 'EXISTING_POOL_POLICY_OR_BINDING_CHANGED'):
                self.reserve(request(current, reservationId='new', deploymentId='new'), current)
        self.assertEqual(self.inspect()['capKrw'], '100')
        self.assertEqual(self.inspect()['reservedKrw'], '60')

    def test_two_independent_processes_cannot_both_exceed_the_same_cap(self):
        ready = self.root / 'start-synthetic-race'
        allocations = [request(reservationId='race-' + str(index), deploymentId='race-' + str(index),
            nativeCurrency=currency) for index, currency in enumerate(('KRW', 'USD'))]
        children = []
        try:
            for allocation in allocations:
                children.append(subprocess.Popen([sys.executable, '-I', '-B', str(Path(__file__).resolve()),
                    '--race-worker', str(self.path), str(ready), json.dumps(policy()), json.dumps(allocation)],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                    creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0)))
            ready.write_text('synthetic start', encoding='ascii')
            results = []
            for child in children:
                stdout, stderr = child.communicate(timeout=15)
                self.assertEqual(child.returncode, 0, stderr)
                results.append(json.loads(stdout))
            self.assertEqual(sum(row['status'] == 'LOCAL_BUDGET_RESERVED' for row in results), 1)
            self.assertEqual(sum(row.get('reason') == 'KIS_SHARED_KRW_CAP_EXCEEDED' for row in results), 1)
            self.assertEqual(self.inspect()['reservedKrw'], '60')
            self.assertEqual(self.inspect()['reservationCount'], 1)
            winner = next(index for index, row in enumerate(results) if row['status'] == 'LOCAL_BUDGET_RESERVED')
            restarted = subprocess.Popen([sys.executable, '-I', '-B', str(Path(__file__).resolve()),
                '--race-worker', str(self.path), str(ready), json.dumps(policy()), json.dumps(allocations[winner])],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
            children.append(restarted)
            stdout, stderr = restarted.communicate(timeout=15)
            self.assertEqual(restarted.returncode, 0, stderr)
            retried = json.loads(stdout)
            self.assertTrue(retried['idempotent'])
            self.assertEqual((retried['reservationCount'], retried['reservedKrw']), (1, '60'))
        finally:
            for child in children:
                if child.poll() is None:
                    child.terminate()
                    child.wait(timeout=5)

    def test_principal_and_reservation_roll_back_in_the_callers_same_transaction(self):
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('CREATE TABLE synthetic_principals (deployment TEXT PRIMARY KEY)')
            db.commit()
            db.execute('BEGIN IMMEDIATE')
            budget.ensure_schema(db)
            budget.reserve_in_transaction(db, policy=policy(), request=request())
            db.execute('INSERT INTO synthetic_principals VALUES (?)', ('deployment-one',))
            db.rollback()  # Simulated caller cancellation after both local writes.
            db.execute('BEGIN')
            self.assertEqual(db.execute('SELECT * FROM synthetic_principals').fetchall(), [])
            self.assertEqual(budget.inspect_in_transaction(db, policy()['accountFingerprint'])['status'], 'UNKNOWN_NO_POOL')

    def test_persisted_head_cas_detects_an_interleaved_change_and_rolls_back(self):
        callback = None
        class InterleavingConnection(sqlite3.Connection):
            def execute(self, sql, parameters=()):
                nonlocal callback
                if callback is not None and sql.startswith('INSERT INTO ' + budget.RESERVATION_TABLE + ' '):
                    action, callback = callback, None
                    action(self)
                return super().execute(sql, parameters)
        with closing(sqlite3.connect(self.path, factory=InterleavingConnection)) as db:
            db.execute('BEGIN IMMEDIATE')
            budget.ensure_schema(db)
            callback = lambda connection: budget.reserve_in_transaction(connection, policy=policy(),
                request=request(reservationId='interleaved', deploymentId='interleaved', requestedKrw='20'))
            with self.assertRaisesRegex(budget.KisSharedKrwReservationError, 'RESERVATION_CAS_CHANGED'):
                budget.reserve_in_transaction(db, policy=policy(), request=request())
            db.rollback()
            db.execute('BEGIN')
            self.assertEqual(budget.inspect_in_transaction(db, policy()['accountFingerprint'])['status'], 'UNKNOWN_NO_POOL')

    def test_append_only_guards_and_corrupt_seal_fail_closed(self):
        self.reserve()
        with closing(sqlite3.connect(self.path)) as db:
            for table in (budget.POOL_TABLE, budget.RESERVATION_TABLE):
                with self.assertRaises(sqlite3.DatabaseError):
                    db.execute('DELETE FROM ' + table)
                db.rollback()
                with self.assertRaises(sqlite3.DatabaseError):
                    db.execute('INSERT OR REPLACE INTO ' + table + ' SELECT * FROM ' + table)
                db.rollback()
            db.execute('DROP TRIGGER ' + budget.RESERVATION_TABLE + '_no_update')
            db.execute('UPDATE ' + budget.RESERVATION_TABLE + ' SET content_hash=?', ('0' * 64,))
            db.execute(budget._TRIGGERS[budget.RESERVATION_TABLE + '_no_update'])
            db.commit()
        with self.assertRaisesRegex(budget.KisSharedKrwReservationError, 'CHAIN_OR_SEAL_INVALID'):
            self.inspect()
        with self.assertRaises(budget.KisSharedKrwReservationError):
            self.reserve(request(reservationId='next', deploymentId='next', requestedKrw='1'))

    def test_database_busy_is_closed_and_transaction_is_never_implicitly_committed(self):
        self.reserve(request(requestedKrw='10'))
        with closing(sqlite3.connect(self.path)) as lock, closing(sqlite3.connect(self.path, timeout=0)) as db:
            lock.execute('BEGIN IMMEDIATE')
            db.execute('BEGIN')
            with self.assertRaisesRegex(budget.KisSharedKrwReservationError, 'DATABASE_BUSY_OR_UNAVAILABLE'):
                budget.reserve_in_transaction(db, policy=policy(),
                    request=request(reservationId='busy', deploymentId='busy', requestedKrw='1'))
            self.assertTrue(db.in_transaction)
            db.rollback()
            lock.rollback()
        self.assertEqual(self.inspect()['reservedKrw'], '10')

    def test_validation_is_whole_won_exact_bound_and_does_not_accept_authority_flags(self):
        for amount in (True, 1, '-1', '0', '1.00', '1e3', '01', '９', '9' * 25):
            with self.subTest(amount=amount), self.assertRaises(budget.KisSharedKrwReservationError):
                budget.validate_request(request(requestedKrw=amount))
        with self.assertRaises(budget.KisSharedKrwReservationError):
            budget.validate_policy({**policy(), 'financialReady': True})
        with self.assertRaises(budget.KisSharedKrwReservationError):
            budget.validate_request({**request(), 'verified': True})
        with closing(sqlite3.connect(self.path)) as db:
            with self.assertRaisesRegex(budget.KisSharedKrwReservationError, 'CALLER_TRANSACTION_REQUIRED'):
                budget.ensure_schema(db)
        for currency in ('KRW', 'USD', 'HKD', 'JPY', 'CNY', 'GBP'):
            self.assertEqual(budget.validate_request(request(nativeCurrency=currency))['nativeCurrency'], currency)

    def test_stop_expiry_fill_and_boolean_assertions_can_never_release_money(self):
        self.reserve()
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('BEGIN IMMEDIATE')
            for proof in ({'flat': True, 'settlementVerified': True}, {'status': 'filled'},
                          {'status': 'stopped'}, {'status': 'expired'}, None):
                with self.assertRaisesRegex(budget.KisSharedKrwReservationError, 'RELEASE_PRODUCER_UNAVAILABLE'):
                    budget.release_in_transaction(db, request=proof)
            self.assertEqual(budget.inspect_in_transaction(db, policy()['accountFingerprint'])['reservedKrw'], '60')


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--race-worker':
        race_worker()
    else:
        unittest.main()
