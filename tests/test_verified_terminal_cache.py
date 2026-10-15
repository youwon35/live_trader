from __future__ import annotations
from copy import deepcopy
from contextlib import closing
from datetime import datetime,timezone
from pathlib import Path
import importlib,json,socket,sqlite3,tempfile,unittest
from unittest.mock import Mock,patch
import test_native_spot_preflight as spot
import test_kis_equity_operational as equity

cache_module=importlib.import_module(spot.P+'.verified_terminal_cache')


class VerifiedTerminalCacheTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)
        self.net=patch.object(socket,'socket',side_effect=AssertionError('All actual broker networking forbidden'))
        self.net.start();self.addCleanup(self.net.stop)

    def _spot(self,provider='binance'):
        spec=spot.spec(provider);deployment,account,scope=spot.deployment(spec);reader=spot.Reader(spec,cash='1000000')
        runtime=spot.runtime_module.NativeSpotRuntime(self.root,'deployment',[spec],reader=reader,roots=[],deployment_reader=lambda *args:deployment,dispatch_reader=lambda:[])
        runtime.initialize_at_reviewed_manual_start()
        row={'order_id':'local-42','idempotency_key':'key-42','broker_id':provider,'symbol':spec.symbol,'quantity':'1','side':'BUY','broker_order_id':'42',
            'metadata':{'deployment_id':'deployment','strategy_instance_id':'member','source_artifact_hash':spec.artifact_hash,
                'portfolio_hash':spec.portfolio_hash,'native_account_fingerprint':account['accountFingerprint']}}
        runtime.dispatch_reader=lambda:[row]
        if provider=='binance':
            reader.order={'symbol':spec.symbol,'orderId':42,'side':'BUY','status':'FILLED','executedQty':'1'}
            reader.trades=[{'symbol':spec.symbol,'orderId':42,'id':1,'isBuyer':True,'qty':'1','price':'100',
                'commission':'0.01','commissionAsset':'ETH','time':int(datetime.now(timezone.utc).timestamp()*1000)}]
        else:
            reader.order={'uuid':'42','market':spec.symbol,'side':'bid','state':'done','executed_volume':'1','paid_fee':'100','trades_count':1,
                'trades':[{'volume':'1','funds':'100000','created_at':datetime.now(timezone.utc).isoformat()}]}
        return runtime,spec,scope,reader,row,deployment

    def test_binance_and_upbit_completed_receipts_survive_restart_without_any_history_get(self):
        for provider in ('binance','upbit'):
            with self.subTest(provider=provider):
                self.root=Path(self.temp.name)/provider
                runtime,spec,scope,reader,row,deployment=self._spot(provider)
                runtime.sync_owned_fills(scope);before=runtime.store.project(scope,{'member':'100'})
                reader._get=Mock(side_effect=AssertionError('Completed order history must not be fetched'))
                runtime.sync_owned_fills(scope);reader._get.assert_not_called()
                restarted=spot.runtime_module.NativeSpotRuntime(self.root,'deployment',[spec],reader=reader,roots=[],
                    deployment_reader=lambda *args:deployment,dispatch_reader=lambda:[row])
                restarted.sync_owned_fills(scope);reader._get.assert_not_called()
                self.assertEqual(restarted.store.project(scope,{'member':'100'}),before)

    def test_cached_terminal_history_never_replaces_current_per_trade_rules_and_account_preflight(self):
        runtime,spec,scope,reader,row,deployment=self._spot();runtime.sync_owned_fills(scope);reader.calls.clear()
        current=runtime.context(spec,'BUY',100,[spec],{})
        self.assertEqual(current['equityScope'],'OWNED_DEPLOYMENT_ONLY',current)
        paths=[path for _,path in reader.calls]
        self.assertIn('/api/v3/exchangeInfo',paths);self.assertIn('/api/v3/account/commission',paths)
        self.assertNotIn('/api/v3/order',paths);self.assertNotIn('/api/v3/myTrades',paths)

    def test_nonterminal_unknown_incomplete_and_unpriced_fee_orders_are_never_cached(self):
        for fault in ('nonterminal','unknown','incomplete-trades','third-asset-fee'):
            self.root=Path(self.temp.name)/fault
            runtime,spec,scope,reader,row,deployment=self._spot()
            if fault=='nonterminal':reader.order['status']='PARTIALLY_FILLED'
            elif fault=='unknown':reader.order['status']='UNKNOWN'
            elif fault=='incomplete-trades':reader.trades=[]
            else:reader.trades[0]['commissionAsset']='BNB'
            with self.subTest(fault=fault),self.assertRaises(ValueError):runtime.sync_owned_fills(scope)
            self.assertFalse(runtime.terminal_cache.path.exists())
            self.assertEqual(runtime.store.project(scope,{})['fillCount'],0)

    def test_changed_immutable_dispatch_scope_fails_before_any_network(self):
        runtime,spec,scope,reader,row,deployment=self._spot();runtime.sync_owned_fills(scope)
        reader._get=Mock(side_effect=AssertionError('No GET after cached scope changed'));row['quantity']='2'
        with self.assertRaisesRegex(ValueError,'IMMUTABLE_DISPATCH_SCOPE_CHANGED'):runtime.sync_owned_fills(scope)
        reader._get.assert_not_called()

    def test_current_credential_and_member_source_rebinding_cannot_reuse_a_cache(self):
        runtime,spec,scope,reader,row,deployment=self._spot();runtime.sync_owned_fills(scope)
        identity=cache_module.terminal_identity(scope,spec,row)
        for key in ('credentialConfigurationHash','accountFingerprint','deploymentScopeHash','sourceArtifactHash','portfolioHash'):
            with self.subTest(key=key),self.assertRaises(ValueError):runtime.terminal_cache.get({**identity,key:'f'*64})

    def test_one_broker_order_cannot_be_attributed_to_another_member_or_deployment(self):
        runtime,spec,scope,reader,row,deployment=self._spot();runtime.sync_owned_fills(scope)
        identity=cache_module.terminal_identity(scope,spec,row)
        for change in ({'strategyInstanceId':'other'},{'deploymentId':'other','idempotencyKey':'other'}):
            with self.assertRaises(ValueError):runtime.terminal_cache.get({**identity,**change})

    def test_exact_duplicate_is_idempotent_and_changed_completed_receipt_is_rejected(self):
        runtime,spec,scope,reader,row,deployment=self._spot();runtime.sync_owned_fills(scope)
        identity=cache_module.terminal_identity(scope,spec,row);entry=runtime.terminal_cache.get(identity)
        self.assertFalse(runtime.terminal_cache.put(identity,entry['receipts'],entry['proof']))
        changed=deepcopy(entry['receipts']);changed[0]['fee']='0.02'
        proof={**entry['proof'],'receiptsHash':cache_module.stable_sha256(changed)}
        with self.assertRaisesRegex(ValueError,'TERMINAL_RECEIPT_CHANGED'):runtime.terminal_cache.put(identity,changed,proof)

    def test_cache_requires_positive_completion_and_actual_spot_fees(self):
        runtime,spec,scope,reader,row,deployment=self._spot();runtime.sync_owned_fills(scope)
        identity=cache_module.terminal_identity(scope,spec,row);entry=runtime.terminal_cache.get(identity)
        for field,value in (('complete',False),('terminalVerified',False),('actualFeesObserved',False),('terminalStatus','UNKNOWN'),('allTradesHash','')):
            with self.subTest(field=field),self.assertRaises(ValueError):runtime.terminal_cache.put(identity,entry['receipts'],{**entry['proof'],field:value})

    def test_cache_is_append_only_and_corrupt_seal_is_never_refetched_or_silently_replaced(self):
        runtime,spec,scope,reader,row,deployment=self._spot();runtime.sync_owned_fills(scope)
        with closing(sqlite3.connect(runtime.terminal_cache.path)) as db:
            with self.assertRaises(sqlite3.IntegrityError):db.execute('DELETE FROM terminal_receipts')
            db.execute('DROP TRIGGER terminal_cache_no_update');db.execute("UPDATE terminal_receipts SET payload='{}'")
            db.commit()
        reader._get=Mock(side_effect=AssertionError('Corrupt cache must block, not refetch'))
        with self.assertRaisesRegex(ValueError,'SEAL_INVALID'):runtime.sync_owned_fills(scope)
        reader._get.assert_not_called()

    def test_invalid_sqlite_cache_is_a_safe_blocker(self):
        runtime,spec,scope,reader,row,deployment=self._spot()
        runtime.terminal_cache.path.write_bytes(b'not sqlite')
        with self.assertRaisesRegex(ValueError,'DATABASE_INVALID'):runtime.sync_owned_fills(scope)

    def test_zero_fill_terminal_cancel_is_cached_without_inventing_positions(self):
        runtime,spec,scope,reader,row,deployment=self._spot();reader.order.update({'status':'CANCELED','executedQty':'0'});reader.trades=[]
        self.assertEqual(runtime.sync_owned_fills(scope),0)
        reader._get=Mock(side_effect=AssertionError('Zero-fill terminal order must be cached'))
        self.assertEqual(runtime.sync_owned_fills(scope),0);reader._get.assert_not_called()
        self.assertEqual(runtime.store.project(scope,{})['fillCount'],0)

    def test_kis_exact_terminal_receipt_restart_has_no_additional_query_and_no_fee_relabel(self):
        helper=equity.KisEquityOperationalTests();spec,context,dispatch,official=helper._identity_fixture()
        dispatch.update({'order_date':'20261015','broker_order_key':'20261015:1:2','broker_order_identity_verified':True,'order_date_source':'BROKER_ACK'})
        official.update({'ft_ccld_qty':'3','nccs_qty':'0','ft_ccld_amt3':'300','prcs_stat_name':'체결'})
        runtime=equity.runtime_module.NativeEquityRuntime(self.root,'deployment',[spec],reader=equity.FakeReader(),dispatch_reader=lambda:[dispatch])
        scope=helper._scope();runtime.store.initialize(scope,helper._observation(),now=equity.NOW)
        runtime.observer.terminal_orders=Mock(return_value=[official]);runtime.sync_owned_fills(scope)
        runtime.observer.terminal_orders=Mock(side_effect=AssertionError('KIS completed history must not be queried'))
        runtime.sync_owned_fills(scope);runtime.observer.terminal_orders.assert_not_called()
        restarted=equity.runtime_module.NativeEquityRuntime(self.root,'deployment',[spec],reader=equity.FakeReader(),dispatch_reader=lambda:[dispatch])
        restarted.observer.terminal_orders=Mock(side_effect=AssertionError('KIS restart must reuse exact terminal history'))
        restarted.sync_owned_fills(scope);restarted.observer.terminal_orders.assert_not_called()
        projection=restarted.store.project(scope,{'member':'100'})
        self.assertEqual(projection['ownedPositions']['member'],'3');self.assertFalse(projection['actualFeesReconciled'])

    def test_kis_nonterminal_and_partial_unknown_receipt_cannot_enter_cache(self):
        helper=equity.KisEquityOperationalTests();spec,context,dispatch,official=helper._identity_fixture()
        dispatch.update({'order_date':'20261015','broker_order_key':'20261015:1:2','broker_order_identity_verified':True,'order_date_source':'BROKER_ACK'})
        runtime=equity.runtime_module.NativeEquityRuntime(self.root,'deployment',[spec],reader=equity.FakeReader(),dispatch_reader=lambda:[dispatch])
        runtime.observer.terminal_orders=lambda *args:[official]
        with self.assertRaises(ValueError):runtime.sync_owned_fills(helper._scope())
        self.assertFalse(runtime.terminal_cache.path.exists())


if __name__=='__main__':unittest.main()
