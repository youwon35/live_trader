from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import json
import sqlite3
import tempfile
import unittest

from live_trader.kis_owned_capital import KisOwnedCapitalStore
from live_trader.kis_scoped_cleanup_scope import cleanup_read_runtime, original_owned_cleanup_scope, cached_cleanup_terminal_is_complete, cleanup_reconciliation_strategy
from test_kis_scoped_cleanup_truth import fixture, Observer, NOW


class OriginalCleanupScopeTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.scope,self.owned,working,history=fixture('KRX')
        self.store=KisOwnedCapitalStore(Path(self.temp.name)/'original.sqlite3')
        self.store.initialize(self.scope,{'currency':'KRW','profileId':self.scope['accountProfileId'],
            'accountFingerprint':self.scope['accountFingerprint'],'credentialConfigurationHash':self.scope['credentialConfigurationHash'],
            'openOrders':'NONE_OBSERVED_COMPLETE_ACCOUNT_CURRENCY_SCOPE','asOf':NOW.isoformat(),'availableCash':'100000'},now=NOW)
        member=self.scope['members'][0]
        from test_kis_equity_operational import fixture as source_fixture
        source_spec, _contract, _context = source_fixture('KRX')
        self.spec=SimpleNamespace(strategy_instance_id=member['strategyInstanceId'],artifact_hash=member['sourceArtifactHash'],
            portfolio_hash=member['portfolioHash'],symbol=member['symbol'],artifact=source_spec.artifact)
        def expired():raise ValueError('ENTRY_OR_FEE_POLICY_EXPIRED')
        self.native=SimpleNamespace(store=self.store,specs=[self.spec],observer=Observer(working,history),scope=expired)
        self.context={'deploymentId':self.scope['deploymentId'],'accountFingerprint':self.scope['accountFingerprint'],
            'credentialConfigurationHash':self.scope['credentialConfigurationHash'],'capitalCurrency':'KRW'}

    def test_expired_entry_policy_only_cleanup_can_read_original_append_only_scope(self):
        observer=cleanup_read_runtime(self.native,self.context)
        self.assertEqual(observer.scope(),self.scope)
        with self.assertRaisesRegex(ValueError,'POLICY_EXPIRED'):self.native.scope()
        for method in ('place_order','cancel_order','initialize_at_reviewed_manual_start'):
            self.assertFalse(hasattr(observer,method))
        returned=observer.scope();returned['members'][0]['symbol']='tampered'
        self.assertEqual(observer.scope(),self.scope)

    def test_cleanup_reconciliation_carries_original_pinned_source_venue_without_entry_policy_or_new_authority(self):
        from live_trader.status_observation import strategy_execution_observation_scope
        strategy = cleanup_reconciliation_strategy(self.native, self.context)
        self.assertEqual('KRX', strategy['exchange'])
        self.assertEqual(self.scope['scopeHash'], strategy['cleanup_source_scope_hash'])
        self.assertEqual(self.spec.artifact_hash, strategy['cleanup_source_venue_contracts'][0]['sourceArtifactHash'])
        self.assertEqual('KIS_DOMESTIC_ONLY', strategy_execution_observation_scope(strategy))
        self.assertNotIn('authority_granted', strategy)
        with self.assertRaisesRegex(ValueError, 'POLICY_EXPIRED'):
            self.native.scope()

    def test_cleanup_never_infers_venue_from_krw_or_symbol_when_original_source_contract_is_missing_or_changed(self):
        original = deepcopy(self.spec.artifact)
        self.spec.artifact = {}
        with self.assertRaisesRegex(ValueError, 'SOURCE_VENUE_CONTRACT_REQUIRED'):
            cleanup_reconciliation_strategy(self.native, self.context)
        self.spec.artifact = original
        self.spec.artifact['executionSizing']['quantityContract']['exchange'] = 'NYSE'
        with self.assertRaisesRegex(ValueError, 'hash mismatch'):
            cleanup_reconciliation_strategy(self.native, self.context)

    def test_usd_cleanup_preserves_actual_overseas_source_and_cannot_borrow_domestic_coverage(self):
        from test_kis_equity_operational import fixture as source_fixture
        from live_trader.status_observation import strategy_execution_observation_scope
        scope, _owned, working, history = fixture('NASD')
        store = KisOwnedCapitalStore(Path(self.temp.name) / 'overseas-original.sqlite3')
        store.initialize(scope, {'currency':'USD','profileId':scope['accountProfileId'],
            'accountFingerprint':scope['accountFingerprint'],'credentialConfigurationHash':scope['credentialConfigurationHash'],
            'openOrders':'NONE_OBSERVED_COMPLETE_ACCOUNT_CURRENCY_SCOPE','asOf':NOW.isoformat(),'availableCash':'10000'}, now=NOW)
        spec, _contract, _context = source_fixture('NASD')
        native = SimpleNamespace(store=store, specs=[spec])
        context = {**self.context, 'capitalCurrency':'USD'}
        strategy = cleanup_reconciliation_strategy(native, context)
        self.assertEqual('NASD', strategy['exchange'])
        self.assertEqual('USD', strategy['currency'])
        self.assertEqual('UNVERIFIED', strategy_execution_observation_scope(strategy))

    def test_changed_account_credentials_currency_deployment_or_source_never_reuses_principal(self):
        for key,value in [('accountFingerprint','f'*64),('credentialConfigurationHash','e'*64),('capitalCurrency','USD'),('deploymentId','different')]:
            with self.subTest(key=key),self.assertRaises(ValueError):original_owned_cleanup_scope(self.native,{**self.context,key:value})
        self.spec.artifact_hash='d'*64
        with self.assertRaisesRegex(ValueError,'CANONICAL_MEMBERS_CHANGED'):original_owned_cleanup_scope(self.native,self.context)

    def test_missing_storage_is_not_initialized_and_invalid_seal_is_not_trusted(self):
        absent=Path(self.temp.name)/'absent'/'principal.sqlite3'
        native=SimpleNamespace(store=SimpleNamespace(path=absent),specs=[self.spec])
        with self.assertRaises(ValueError):original_owned_cleanup_scope(native,self.context)
        self.assertFalse(absent.parent.exists())
        # A separate malformed fixture, never an edit of real append-only data.
        fake=Path(self.temp.name)/'malformed.sqlite3'
        with sqlite3.connect(fake) as db:
            db.execute('CREATE TABLE principals(deployment_id TEXT,payload TEXT,content_hash TEXT)')
            db.execute('INSERT INTO principals VALUES(?,?,?)',(self.scope['deploymentId'],json.dumps({'scope':self.scope}), '0'*64))
        db.close();native.store.path=fake
        with self.assertRaisesRegex(ValueError,'SCOPE_SEAL_CHANGED'):original_owned_cleanup_scope(native,self.context)

    def test_legacy_zero_fill_and_fractional_cancel_cache_cannot_skip_current_terminal_truth(self):
        entry={'identity':{'exchange':'KRX'},'proof':{'executedQuantity':'0','terminalFacts':{
            'orderedQuantity':'3','canceledQuantity':'3','rejectedQuantity':'0'}},'receipts':[]}
        self.assertFalse(cached_cleanup_terminal_is_complete(entry))
        entry['proof']['executedQuantity']='1';entry['receipts']=[{'alreadyVerifiedPositiveNotional':True}]
        entry['proof']['terminalFacts'].update(canceledQuantity='1.5',rejectedQuantity='0.5')
        self.assertFalse(cached_cleanup_terminal_is_complete(entry))
        entry['proof']['terminalFacts'].update(canceledQuantity='2',rejectedQuantity='0')
        self.assertTrue(cached_cleanup_terminal_is_complete(entry))
