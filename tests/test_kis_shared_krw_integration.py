"""Synthetic shared budget + real owned-store/manual-start integration only."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
import importlib
import sqlite3
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

import test_kis_equity_operational as fixture
from trading_runtime.artifact_governance import stable_sha256

bridge = importlib.import_module(fixture.PACKAGE + '.kis_shared_krw_bridge')
reservation = importlib.import_module(fixture.PACKAGE + '.kis_shared_krw_reservations')
Store = fixture.ledger.KisOwnedCapitalStore


def scope(deployment='domestic', currency='KRW'):
    account = fixture.ACCOUNT
    result = {'version': 'kis-owned-capital-v2', 'deploymentId': deployment,
        'accountFingerprint': account['accountFingerprint'], 'accountProfileId': account['profileId'],
        'credentialConfigurationHash': account['credentialConfigurationHash'],
        'reviewBindingHash': 'd'*64, 'capitalPercent': '10', 'quoteCurrency': currency,
        'members': [{'strategyInstanceId': 'member', 'sourceArtifactHash': 'c'*64,
            'portfolioHash': 'e'*64, 'symbol': '005930' if currency == 'KRW' else 'AAPL',
            'quoteCurrency': currency}]}
    result['scopeHash'] = stable_sha256(result)
    return result


def plan(current_scope, amount='10000', cap='100000'):
    return bridge.prepare_local_shared_plan(current_scope, cap_krw=cap,
        allocation_krw=amount, budget_review_hash='9'*64, basis_hash='8'*64)


def observation(current_scope, cash='100000'):
    return {'currency': current_scope['quoteCurrency'], 'profileId': current_scope['accountProfileId'],
        'accountFingerprint': current_scope['accountFingerprint'],
        'credentialConfigurationHash': current_scope['credentialConfigurationHash'],
        'openOrders': 'NONE_OBSERVED_COMPLETE_ACCOUNT_CURRENCY_SCOPE',
        'asOf': datetime.now(timezone.utc).isoformat(), 'availableCash': cash}


def reviewed_input(account, amount='10000', cap='100000'):
    policy = {'schemaVersion': reservation.POLICY_SCHEMA,
        'accountFingerprint': account['accountFingerprint'], 'accountProfileId': account['profileId'],
        'credentialConfigurationHash': account['credentialConfigurationHash'],
        'capKrw': cap, 'reviewBindingHash': '9'*64}
    policy['policyHash'] = reservation.canonical_hash(policy)
    value = {'schemaVersion': bridge.INPUT_SCHEMA, 'policy': policy, 'allocationKrw': amount}
    value['inputHash'] = reservation.canonical_hash(value)
    return value


class SharedKrwIntegrationTests(unittest.TestCase):
    def test_domestic_principal_and_foreign_budget_compete_in_one_account(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(Path(tmp)/'owned.sqlite3'); kr = scope(); kr_plan = plan(kr, '60000')
            self.assertEqual('10000', store.initialize(kr, observation(kr), shared_reservation=kr_plan)['ownedCash'])
            us = scope('overseas', 'USD'); us_plan = plan(us, '40000')
            store.reserve_shared_krw_budget(us_plan['policy'], us_plan['request'])
            state = store.shared_krw_budget(kr['accountFingerprint'])
            self.assertEqual(('100000','0',2), (state['reservedKrw'],state['remainingKrw'],state['reservationCount']))
            third = plan(scope('third'), '1')
            with self.assertRaisesRegex(ValueError, 'CAP_EXCEEDED'):
                store.reserve_shared_krw_budget(third['policy'], third['request'])
            with closing(sqlite3.connect(store.path)) as db:
                self.assertEqual(1, db.execute('SELECT COUNT(*) FROM principals').fetchone()[0])
            self.assertFalse(state['financialReady']); self.assertFalse(state['orderAuthorityGranted'])

    def test_same_transaction_cancellation_rolls_back_pool_and_principal(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(Path(tmp)/'owned.sqlite3'); current = scope(); bundle = plan(current)
            calls = []
            def cancel():
                calls.append(None)
                if len(calls) == 3: raise RuntimeError('SYNTHETIC_CANCEL_BEFORE_COMMIT')
            with self.assertRaisesRegex(RuntimeError, 'SYNTHETIC_CANCEL'):
                store.initialize(current, observation(current), shared_reservation=bundle, commit_guard=cancel)
            with closing(sqlite3.connect(store.path)) as db:
                self.assertEqual(0, db.execute('SELECT COUNT(*) FROM principals').fetchone()[0])
                self.assertIsNone(db.execute('SELECT 1 FROM sqlite_master WHERE name=?', (reservation.POOL_TABLE,)).fetchone())

    def test_under_reserved_or_foreign_native_principal_is_rejected_before_db_creation(self):
        with tempfile.TemporaryDirectory() as tmp:
            for current, amount, code in ((scope(), '9999', 'BELOW_NATIVE_PRINCIPAL'),
                    (scope('us', 'USD'), '10000', 'NATIVE_FUNDING_PRODUCER_UNAVAILABLE')):
                store = Store(Path(tmp)/(current['deploymentId']+'.sqlite3'))
                with self.assertRaisesRegex(ValueError, code):
                    store.initialize(current, observation(current), shared_reservation=plan(current, amount))
                self.assertFalse(store.path.exists())
        # Fractional principal is always covered by rounding the budget upward.
        with self.assertRaisesRegex(ValueError, 'BELOW_NATIVE_PRINCIPAL'):
            bridge.validate_shared_allocation(scope(), plan(scope(), '10000'), principal=Decimal('10000.0001'))

    def test_existing_legacy_book_is_not_silently_zero_or_auto_migrated(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(Path(tmp)/'owned.sqlite3'); current = scope()
            store.initialize(current, observation(current)); before = store.path.read_bytes()
            bundle = plan(scope('new'))
            with self.assertRaisesRegex(ValueError, 'MIGRATION_REQUIRED'):
                store.reserve_shared_krw_budget(bundle['policy'], bundle['request'])
            self.assertEqual(before, store.path.read_bytes())

    def test_foreign_cash_legacy_cannot_bypass_an_existing_shared_pool(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(Path(tmp)/'owned.sqlite3'); current = scope()
            store.initialize(current, observation(current), shared_reservation=plan(current))
            other = scope('legacy-bypass')
            with self.assertRaisesRegex(ValueError, 'EXISTING_POOL_REQUIRES_EXACT_RESERVATION'):
                store.initialize(other, observation(other))
            self.assertEqual(1, store.shared_krw_budget(current['accountFingerprint'])['reservationCount'])

    def test_real_store_two_threads_do_not_over_allocate_different_currencies(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'owned.sqlite3'
            bundles = [plan(scope('kr', 'KRW'), '60000'), plan(scope('us', 'USD'), '60000')]
            def reserve(bundle):
                try:
                    Store(path).reserve_shared_krw_budget(bundle['policy'], bundle['request'])
                    return 'OK'
                except ValueError: return 'BLOCKED'
            with ThreadPoolExecutor(max_workers=2) as threads:
                self.assertEqual(['BLOCKED','OK'], sorted(threads.map(reserve, bundles)))
            self.assertEqual('60000', Store(path).shared_krw_budget(scope()['accountFingerprint'])['reservedKrw'])

    def test_retry_and_new_store_instance_keep_one_principal_and_one_reservation(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'owned.sqlite3'; current = scope(); bundle = plan(current)
            first = Store(path).initialize(current, observation(current), shared_reservation=bundle)
            again = Store(path).initialize(current, observation(current), shared_reservation=bundle)
            self.assertEqual(first, again)
            self.assertEqual(1, Store(path).shared_krw_budget(current['accountFingerprint'])['reservationCount'])

    def runtime(self, tmp):
        spec, _, _ = fixture.fixture('KRX'); reader = fixture.FakeReader('KRX')
        reader.settings['KIS_OVERSEAS_FUNDING_MODE'] = 'INTEGRATED_MARGIN'
        account = fixture.profiles.saved_account_profiles(reader.settings)[0]
        policies = fixture.service.prepare_reviewed_policies(account, spec,
            authorization_text='SYNTHETIC LOCAL PREPARATION ONLY', now=datetime.now(timezone.utc))
        permissions = {'liveReviewBindingHash':'a'*64, 'liveAccountProfileId':account['profileId'],
            'liveAccountFingerprint':account['accountFingerprint'],
            'liveAccountCredentialConfigurationHash':account['credentialConfigurationHash'],
            'liveCapitalPolicy':{'basis':'ACCOUNT_AVAILABLE_CASH','percent':10},
            'kisEquityExecutionPolicies':{spec.artifact_hash:policies['executionPolicy']},
            'kisEquityFeeReservePolicy':policies['feeReservePolicy'], 'kisSavedSlippageBps':50,
            'kisSharedKrwReservationPlan':reviewed_input(account)}
        deployment = {'environment':'LIVE','lifecycle':'before-live-small', 'accountId':account['accountBindingId'],
            'route':'kis','permissions':permissions}
        runtime = fixture.runtime_module.NativeEquityRuntime(tmp,'deployment',[spec],reader=reader,
            deployment_reader=lambda *a: deepcopy(deployment),dispatch_reader=lambda:[],
            owned_store=Store(Path(tmp)/'logs'/'kis_owned_capital_v2.sqlite3'))
        return runtime, deployment, reader

    def test_real_runtime_manual_start_commits_local_reservation_and_principal(self):
        with tempfile.TemporaryDirectory() as tmp:
            runtime, deployment, reader = self.runtime(tmp)
            fact = runtime.observe_manual_start_initialization()
            self.assertFalse(runtime.store.path.exists())
            self.assertEqual('INTEGRATED_MARGIN', fact['fundingMode'])
            result = runtime.commit_manual_start_initialization(fact)
            self.assertEqual('9000', result['ownedCash'])
            stored = runtime.store.shared_krw_reservation(fact['scope'])
            self.assertEqual(fact['sharedReservation']['request'], stored['request'])
            self.assertFalse(stored['financialReady']); self.assertFalse(stored['orderAuthorityGranted'])

    def test_missing_policy_blocks_real_runtime_before_any_get_or_db(self):
        with tempfile.TemporaryDirectory() as tmp:
            runtime, deployment, reader = self.runtime(tmp)
            deployment['permissions'].pop('kisSharedKrwReservationPlan')
            with self.assertRaisesRegex(ValueError, 'REVIEWED_ALLOCATION_REQUIRED'):
                runtime.observe_manual_start_initialization()
            self.assertEqual([], reader.events); self.assertFalse(runtime.store.path.exists())

    def test_changed_plan_after_observe_and_after_commit_invalidates_fact_and_position_context(self):
        with tempfile.TemporaryDirectory() as tmp:
            runtime, deployment, reader = self.runtime(tmp)
            fact = runtime.observe_manual_start_initialization()
            original = deepcopy(deployment['permissions']['kisSharedKrwReservationPlan'])
            changed = deepcopy(original); changed['allocationKrw'] = '20000'
            changed['inputHash'] = reservation.canonical_hash({k:v for k,v in changed.items() if k != 'inputHash'})
            deployment['permissions']['kisSharedKrwReservationPlan'] = changed
            with self.assertRaisesRegex(ValueError, 'SCOPE_OR_FRESHNESS_CHANGED'):
                runtime.commit_manual_start_initialization(fact)
            self.assertFalse(runtime.store.path.exists())
            deployment['permissions']['kisSharedKrwReservationPlan'] = original
            runtime.commit_manual_start_initialization(fact)
            deployment['permissions']['kisSharedKrwReservationPlan'] = changed
            output = runtime.context(runtime.specs[0],'POSITION',100,runtime.specs,{})
            self.assertEqual('BLOCKED', output['status']); self.assertFalse(output['orderAuthorityGranted'])

    def test_review_input_has_no_scope_hash_cycle_and_rejects_cash_claims(self):
        current = scope(); value = reviewed_input(fixture.ACCOUNT)
        self.assertNotIn('scopeHash', value); self.assertNotIn('scopeHash', value['policy'])
        computed = bridge.shared_plan_from_reviewed_input(current, value)
        self.assertEqual(current['scopeHash'], computed['request']['scopeHash'])
        changed = deepcopy(value); changed['actualFeesVerified'] = True
        changed['inputHash'] = reservation.canonical_hash({k:v for k,v in changed.items() if k != 'inputHash'})
        with self.assertRaises(ValueError): bridge.validate_reviewed_allocation_input(changed)

    def test_source_and_packaged_roots_reuse_one_durable_budget_after_restart(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict('os.environ',{'LOCALAPPDATA':tmp}):
            root = Path(tmp); spec, _, _ = fixture.fixture('KRX')
            reader = fixture.FakeReader('KRX'); reader.settings['KIS_OVERSEAS_FUNDING_MODE']='INTEGRATED_MARGIN'
            first = fixture.runtime_module.NativeEquityRuntime(root/'source', 'domestic', [spec], reader=reader)
            one = plan(scope('domestic'), '60000')
            first.store.reserve_shared_krw_budget(one['policy'], one['request'])
            second = fixture.runtime_module.NativeEquityRuntime(root/'packaged', 'overseas', [spec], reader=reader)
            self.assertEqual(first.store.path, second.store.path)
            two = plan(scope('overseas','USD'), '60000')
            with self.assertRaisesRegex(ValueError, 'CAP_EXCEEDED'):
                second.store.reserve_shared_krw_budget(two['policy'], two['request'])
            self.assertEqual('60000', second.store.shared_krw_budget(one['policy']['accountFingerprint'])['reservedKrw'])
            self.assertFalse((root/'source'/'logs').exists()); self.assertFalse((root/'packaged'/'logs').exists())

    def test_source_legacy_file_requires_reviewed_migration_without_copying_it(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict('os.environ',{'LOCALAPPDATA':str(Path(tmp)/'common')}):
            root = Path(tmp)/'source'; old = Store(root/'logs'/'kis_owned_capital_v2.sqlite3')
            current = scope(); old.initialize(current, observation(current)); before=old.path.read_bytes()
            spec, _, _ = fixture.fixture('KRX'); reader=fixture.FakeReader('KRX')
            reader.settings['KIS_OVERSEAS_FUNDING_MODE']='INTEGRATED_MARGIN'
            with self.assertRaisesRegex(ValueError,'SEPARATE_LEGACY_DATABASE_REVIEWED_MIGRATION_REQUIRED'):
                fixture.runtime_module.NativeEquityRuntime(root,'new',[spec],reader=reader)
            self.assertEqual(before,old.path.read_bytes());self.assertFalse((Path(tmp)/'common').exists())

    def test_relative_common_root_is_rejected_and_account_pools_stay_separate(self):
        paths=importlib.import_module(fixture.PACKAGE+'.kis_shared_krw_paths')
        with patch.dict('os.environ',{'LOCALAPPDATA':'relative/path'}):
            with self.assertRaisesRegex(ValueError,'ABSOLUTE_COMMON_DATA_ROOT_REQUIRED'):
                paths.shared_capital_path()
        first=scope();second=deepcopy(first);second['accountFingerprint']='f'*64
        second['scopeHash']=stable_sha256({k:v for k,v in second.items() if k!='scopeHash'})
        self.assertNotEqual(plan(first)['request']['reservationId'],plan(second)['request']['reservationId'])

    def test_position_context_discards_plan_changed_during_owned_sync(self):
        with tempfile.TemporaryDirectory() as tmp:
            runtime,deployment,reader=self.runtime(tmp);fact=runtime.observe_manual_start_initialization()
            runtime.commit_manual_start_initialization(fact)
            def change(scope):
                value=deployment['permissions']['kisSharedKrwReservationPlan'];value['allocationKrw']='20000'
                value['inputHash']=reservation.canonical_hash({k:v for k,v in value.items() if k!='inputHash'})
            runtime.sync_owned_fills=change
            result=runtime.context(runtime.specs[0],'POSITION',100,runtime.specs,{})
            self.assertEqual('BLOCKED',result['status'])
            self.assertIn('KIS_SHARED_KRW_CONTEXT_POLICY_OR_SCOPE_CHANGED',result['reasons'])

    def test_review_policy_builder_binds_account_budget_input_without_financial_authority(self):
        review=importlib.import_module(fixture.PACKAGE+'.paper_candidate_review')
        spec,_,_=fixture.fixture('KRX');value=reviewed_input(fixture.ACCOUNT)
        result=review._review_execution_policies(Path('.'),spec.artifact,None,fixture.ACCOUNT,
            {'savedSlippageBps':50,'kisSharedKrwReservationPlan':value},specs=[spec])
        self.assertEqual(value,result['kisSharedKrwReservationPlan'])
        modified=deepcopy(value);modified['policy']['accountFingerprint']='f'*64
        modified['policy']['policyHash']=reservation.canonical_hash({k:v for k,v in modified['policy'].items() if k!='policyHash'})
        modified['inputHash']=reservation.canonical_hash({k:v for k,v in modified.items() if k!='inputHash'})
        with self.assertRaisesRegex(ValueError,'REVIEWED_ACCOUNT_CHANGED'):
            review._review_execution_policies(Path('.'),spec.artifact,None,fixture.ACCOUNT,
                {'kisSharedKrwReservationPlan':modified},specs=[spec])


if __name__ == '__main__': unittest.main()
