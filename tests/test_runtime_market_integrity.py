import ast
import copy
import os
from dataclasses import replace
from pathlib import Path
import tempfile
from types import SimpleNamespace
import threading
import unittest
from unittest.mock import Mock, patch

from live_trader import state
from live_trader.continuous_live import LiveContinuousController
from live_trader.order_management import OrderIntent
from live_trader.runtime_scope import RuntimeScope
from trading_runtime.market_clock_integrity import StickyMarketClockIntegrity
from trading_runtime.market_source_integrity import StickyMarketSourceIntegrity, IMMUTABLE_OPEN_CONFLICT


class RuntimeMarketIntegrityTests(unittest.TestCase):
    def fixture(self, broker='binance', mode='SMALL_LIVE'):
        profile = 'stock' if broker == 'kis' else 'crypto'
        currency = {'binance':'USDT','binance-futures':'USDT','upbit':'KRW','kis':'KRW'}[broker]
        scope = RuntimeScope(broker, 'a'*64, 'deployment-'+broker, profile, 'binding-'+broker, 'b'*64, currency)
        source = StickyMarketSourceIntegrity()
        clock = StickyMarketClockIntegrity()
        supervisor = SimpleNamespace(running=True,phase='RUNNING',_stop=threading.Event(),market_source_integrity_snapshot=source.snapshot,
            market_clock_integrity_snapshot=clock.snapshot, snapshot=Mock(side_effect=AssertionError('full snapshot forbidden')))
        controller = SimpleNamespace(runtime_scope=scope, profile_id=profile, mode=mode,
            deployment_id=scope.deployment_id, supervisor=supervisor)
        session = SimpleNamespace(session_id='session-'+broker, runtime_instance_id='owner-'+broker,
            deployment_id=scope.deployment_id, deployment_manifest_hash='c'*64, profile=profile,
            mode=mode, lifecycle='RUNNING', metadata=scope.to_dict(), config_revision=1, config_hash='d'*64)
        intent = OrderIntent('source', 'equity' if broker=='kis' else 'crypto', '005930' if broker=='kis' else 'BTCUSDT',
            'BUY', 1, 100, mode, 'fixture', {'broker_id':broker, 'deployment_id':scope.deployment_id,
            'runtime_scope_id':scope.runtime_id, 'runtimeScopeId':scope.runtime_id})
        manager = SimpleNamespace(runtimes={scope.runtime_id:controller}, snapshot=Mock(side_effect=AssertionError('manager snapshot forbidden')))
        return SimpleNamespace(scope=scope, source=source, clock=clock, controller=controller,
            session=session, intent=intent, manager=manager)

    def evaluate(self, item, resolver=None):
        with patch.object(state, 'LIVE_CONTINUOUS_CONTROLLER', item.manager), patch.object(state, 'current_mode', return_value='FULL_LIVE'), patch.object(state, '_active_session_for_intent', side_effect=resolver or (lambda intent:(item.scope.runtime_id,item.session))):
            return state.runtime_market_integrity_dispatch_allowed(item.intent)

    def test_all_brokers_and_both_live_modes_use_only_short_readers(self):
        for broker in ('binance','upbit','binance-futures','kis'):
            for mode in ('SMALL_LIVE','FULL_LIVE'):
                with self.subTest(broker=broker,mode=mode):
                    item=self.fixture(broker,mode)
                    self.assertEqual(self.evaluate(item),(True,''))
                    item.controller.supervisor.snapshot.assert_not_called();item.manager.snapshot.assert_not_called()

    def test_source_failure_is_sticky_and_does_not_borrow_other_clean_account(self):
        item=self.fixture();other=self.fixture('upbit')
        item.manager.runtimes.update(other.manager.runtimes)
        item.source.observe({'sourceIntegrity':{'schemaVersion':'paper-market-data-source-integrity-v1','failureCodes':[IMMUTABLE_OPEN_CONFLICT]}})
        for _ in range(2):
            allowed,reason=self.evaluate(item)
            self.assertFalse(allowed);self.assertIn(IMMUTABLE_OPEN_CONFLICT,reason)
        other.manager=item.manager
        self.assertTrue(self.evaluate(other)[0])

    def test_source_and_clock_failures_are_both_returned(self):
        item=self.fixture('kis');item.source.observe({'sourceIntegrity':None});item.clock.invalidate()
        allowed,reason=self.evaluate(item)
        self.assertFalse(allowed);self.assertIn('SOURCE_INTEGRITY_STATUS_INVALID',reason);self.assertIn('CLOCK_INTEGRITY_STATUS_INVALID',reason)

    def test_unknown_missing_or_unreadable_evidence_fails_closed(self):
        for field,reader in (('market_source_integrity_snapshot',None),('market_clock_integrity_snapshot',None),
                ('market_source_integrity_snapshot',lambda:None),('market_clock_integrity_snapshot',lambda:{}),
                ('market_source_integrity_snapshot',Mock(side_effect=OSError('unreadable')))):
            with self.subTest(field=field,reader=reader):
                item=self.fixture();setattr(item.controller.supervisor,field,reader)
                self.assertFalse(self.evaluate(item)[0])

    def test_session_account_credentials_mode_deployment_profile_and_controller_must_match(self):
        alterations=[lambda x:setattr(x.session,'lifecycle','STOPPED'),lambda x:setattr(x.session,'mode','FULL_LIVE'),
            lambda x:setattr(x.session,'deployment_id','other'),lambda x:setattr(x.session,'profile','stock'),
            lambda x:x.session.metadata.update(accountFingerprint='e'*64),
            lambda x:x.session.metadata.update(credentialConfigurationHash='e'*64),
            lambda x:x.session.metadata.update(accountBindingId='other'),
            lambda x:x.session.metadata.update(capitalCurrency='USD'),
            lambda x:x.session.metadata.update(runtimeScopeId='other'),
            lambda x:setattr(x.controller,'runtime_scope',replace(x.scope,account_fingerprint='e'*64)),
            lambda x:setattr(x.controller,'deployment_id','other'),lambda x:setattr(x.controller,'profile_id','stock'),
            lambda x:setattr(x.controller,'mode','FULL_LIVE'),lambda x:setattr(x.controller,'supervisor',None),
            lambda x:x.manager.runtimes.clear()]
        for mutate in alterations:
            item=self.fixture();mutate(item)
            self.assertFalse(self.evaluate(item)[0])

    def test_unbound_ambiguous_and_conflicting_intent_identity_is_denied(self):
        for change in ({'runtimeScopeId':'other'}, {'deploymentId':'other'}, {'native_account_fingerprint':'e'*64},
                {'runtime_session_id':'other'}, {'broker_id':'upbit'}, {'runtime_scope_id':'' ,'runtimeScopeId':''}):
            item=self.fixture();item.intent.metadata.update(change)
            self.assertFalse(self.evaluate(item)[0])
        item=self.fixture()
        self.assertFalse(self.evaluate(item,lambda intent:('',None))[0])
        item.intent=replace(item.intent,mode='MONITOR');self.assertFalse(self.evaluate(item)[0])
        item=self.fixture()
        with patch.object(state,'current_mode',return_value='MONITOR'):
            self.assertFalse(state.runtime_market_integrity_dispatch_allowed(item.intent)[0])

    def test_reader_cannot_rebind_session_controller_scope_or_global_mode_mid_check(self):
        for change in ('session','controller','scope','mode'):
            item=self.fixture();calls=[0]
            def read():
                if change=='session':item.session=copy.copy(item.session);item.session.session_id='superseded'
                elif change=='controller':item.manager.runtimes[item.scope.runtime_id]=copy.copy(item.controller)
                elif change=='scope':item.controller.runtime_scope=replace(item.scope,credential_configuration_hash='e'*64)
                else:calls[0]=1
                return item.source.snapshot()
            item.controller.supervisor.market_source_integrity_snapshot=read
            with self.subTest(change=change),patch.object(state,'LIVE_CONTINUOUS_CONTROLLER',item.manager),patch.object(state,'_active_session_for_intent',side_effect=lambda intent:(item.scope.runtime_id,item.session)),patch.object(state,'current_mode',side_effect=lambda:'MONITOR' if calls[0] else 'FULL_LIVE'):
                self.assertFalse(state.runtime_market_integrity_dispatch_allowed(item.intent)[0])

    def test_kis_final_wire_clock_reader_also_requires_source_proof(self):
        item=self.fixture('kis')
        with patch.object(state,'LIVE_CONTINUOUS_CONTROLLER',item.manager):
            state._require_kis_scoped_runtime_clock_integrity(item.scope.runtime_id)
            item.source.observe({'sourceIntegrity':None})
            with self.assertRaisesRegex(ValueError,'market-source-integrity'):
                state._require_kis_scoped_runtime_clock_integrity(item.scope.runtime_id)

    def test_source_latching_during_clock_reader_is_denied_before_return(self):
        item=self.fixture()
        def clock_read():
            item.source.observe({'sourceIntegrity':None})
            return item.clock.snapshot()
        item.controller.supervisor.market_clock_integrity_snapshot=clock_read
        allowed,reason=self.evaluate(item)
        self.assertFalse(allowed);self.assertIn('SOURCE_INTEGRITY_STATUS_INVALID',reason)

    def test_real_supervisor_thread_and_stop_event_are_required_without_snapshot_locks(self):
        for change in ('dead','stopping','missing-event','failed-phase','stopped-phase'):
            item=self.fixture()
            if change=='dead':item.controller.supervisor.running=False
            elif change=='stopping':item.controller.supervisor._stop.set()
            elif change=='missing-event':del item.controller.supervisor._stop
            elif change=='failed-phase':item.controller.supervisor.phase='FAILED'
            else:item.controller.supervisor.phase='STOPPED'
            with self.subTest(change=change):self.assertFalse(self.evaluate(item)[0])
        item=self.fixture()
        def clock_read():
            item.controller.supervisor._stop.set()
            return item.clock.snapshot()
        item.controller.supervisor.market_clock_integrity_snapshot=clock_read
        self.assertFalse(self.evaluate(item)[0])

    def test_actual_controller_and_supervisor_properties_keep_short_reader_and_shutdown_contract(self):
        from trading_runtime.continuous_runtime import ContinuousRuntimeSupervisor
        item=self.fixture()
        release=threading.Event()
        owner=threading.Thread(target=release.wait,name='offline-integrity-fixture')
        supervisor=object.__new__(ContinuousRuntimeSupervisor)
        supervisor._thread=owner;supervisor._stop=threading.Event();supervisor.phase='RUNNING'
        supervisor._market_source_integrity=item.source;supervisor._market_clock_integrity=item.clock
        supervisor.feeds=[];supervisor.engine=Mock()
        supervisor.engine.snapshot.side_effect=AssertionError('engine snapshot forbidden')
        try:
            owner.start()
            with tempfile.TemporaryDirectory() as directory:
                controller=LiveContinuousController(Path(directory));controller.bind_runtime_scope(item.scope)
                controller.mode=item.intent.mode;controller.supervisor=supervisor
                item.controller=controller;item.manager.runtimes[item.scope.runtime_id]=controller
                self.assertTrue(self.evaluate(item)[0])
                with patch.object(state,'LIVE_CONTINUOUS_CONTROLLER',item.manager),patch.object(state,'current_mode',return_value='FULL_LIVE'),patch.object(state,'_active_session_for_intent',return_value=(item.scope.runtime_id,item.session)),patch.object(state,'snapshot',return_value={}),patch.object(state,'submit_order_intent',return_value={'ok':True}) as submit:
                    self.assertTrue(LiveContinuousController._submit_deferred_intent(item.intent)['ok'])
                    submit.assert_called_once()
                    item.source.observe({'sourceIntegrity':None})
                    self.assertFalse(LiveContinuousController._submit_deferred_intent(item.intent)['ok'])
                    self.assertEqual(submit.call_count,1)
                supervisor.engine.snapshot.assert_not_called()
                supervisor._stop.set()
                self.assertTrue(supervisor.running)
                self.assertFalse(self.evaluate(item)[0])
        finally:
            release.set();owner.join(1)
        self.assertFalse(supervisor.running)

    def test_deferred_integrity_veto_precedes_refresh_ticket_and_submission(self):
        for broker in ('binance','upbit','kis'):
            item=self.fixture(broker)
            with self.subTest(broker=broker),patch.object(state,'runtime_market_integrity_dispatch_allowed',return_value=(False,'source-fault')),patch.object(state,'refresh_kis_scoped_runtime_preflight') as refresh,patch('live_trader.native_order_ticket.validate_native_ticket') as ticket,patch.object(state,'submit_order_intent') as submit:
                result=LiveContinuousController._submit_deferred_intent(item.intent)
                self.assertFalse(result['ok']);self.assertFalse(result['orderSubmitted'])
                refresh.assert_not_called();ticket.assert_not_called();submit.assert_not_called()

    def test_final_dispatch_rechecks_after_native_ticket_and_existing_gates(self):
        source=(Path(__file__).resolve().parents[1]/'live_trader/state.py').read_text(encoding='utf-8')
        function=next(n for n in ast.parse(source).body if isinstance(n,ast.FunctionDef) and n.name=='live_broker_dispatch_allowed')
        calls=[]
        namespace={'__package__':'live_trader','OrderIntent':OrderIntent,'continuous_dispatch_allowed':lambda intent:(True,''),
            'broker_id_from_symbol':lambda *a:'binance','_KIS_CONTROL_RESERVATION':{},
            'STATE':{'dry_run':False,'operator_confirmed':True},'LIVE_BROKER_DISPATCH_MODES':{'SMALL_LIVE','FULL_LIVE'},
            '_intent_runtime_mode':lambda intent:'SMALL_LIVE','functional_test_execution_requested':lambda intent:False,
            'real_orders_enabled':lambda:True,'durable_control_halt_active':lambda intent:False,
            'intent_market_data_age_seconds':lambda *a,**kw:0,'absolute_currency_limits_for_intent':lambda intent:{'status':'READY'},
            'runtime_market_integrity_dispatch_allowed':lambda intent:calls.append('integrity') or (False,'source-fault')}
        exec(compile(ast.Module(body=[function],type_ignores=[]),'<final-dispatch>','exec'),namespace)
        intent=self.fixture().intent;intent.metadata['native_quantity_model']={}
        with patch('live_trader.native_order_ticket.validate_native_ticket',side_effect=lambda intent:calls.append('ticket')):
            self.assertEqual(namespace['live_broker_dispatch_allowed'](intent,dry_run=False),(False,'source-fault'))
        self.assertEqual(calls,['ticket','integrity'])
        calls.clear();namespace['STATE']['kill_switch']=True
        self.assertFalse(namespace['live_broker_dispatch_allowed'](intent,dry_run=False)[0]);self.assertEqual(calls,[])

    def test_new_place_guard_is_not_inserted_in_cancel_or_cleanup_functions(self):
        source=(Path(__file__).resolve().parents[1]/'live_trader/state.py').read_text(encoding='utf-8')
        tree=ast.parse(source)
        protected=[node for node in tree.body if isinstance(node,ast.FunctionDef) and
            ('cleanup' in node.name or node.name in {'stop_continuous_runtime','cancel_order','_continuous_stop_control'})]
        self.assertTrue(protected)
        for node in protected:
            self.assertNotIn('runtime_market_integrity_dispatch_allowed',ast.unparse(node),node.name)


class SavedSettingsRuntimeScopeTests(unittest.TestCase):
    def resolve(self, broker='binance', *, mutate=None, missing=False):
        from live_trader.account_profiles import saved_account_profiles
        from contextlib import ExitStack
        settings={'BINANCE_API_KEY':'fixture-key','BINANCE_API_SECRET':'fixture-secret',
            'UPBIT_ACCESS_KEY':'fixture-key','UPBIT_SECRET_KEY':'fixture-secret',
            'KIS_APP_KEY':'fixture-key','KIS_APP_SECRET':'fixture-secret','KIS_ACCOUNT_NO':'12345678',
            'KIS_ACCOUNT_PRODUCT_CODE':'01'}
        profile='stock' if broker=='kis' else 'crypto'
        currency={'binance':'USDT','upbit':'KRW','kis':'KRW'}[broker]
        with patch.dict(os.environ,settings,clear=True),ExitStack() as stack:
            # Use the actual saved-settings reader and account hash derivation.
            account=next(row for row in saved_account_profiles(state.runtime_settings()) if row['brokerId']==broker)
            deployment={'route':broker,'accountId':account['accountBindingId'],'permissions':{
                'liveAccountProfileId':account['profileId'],'liveAccountFingerprint':account['accountFingerprint'],
                'liveAccountCredentialConfigurationHash':account['credentialConfigurationHash']}}
            if mutate:mutate(deployment)
            if missing:
                os.environ['BINANCE_API_SECRET']='';os.environ['UPBIT_SECRET_KEY']='';os.environ['KIS_APP_SECRET']=''
            strategy={'broker_id':broker,'deployment_id':'dep-fixture','strategy_id':'source-fixture',
                'artifact_source_path':str(Path(__file__).parent/'fixture-source.json')}
            stack.enter_context(patch.object(state,'portfolio_rows',return_value=[]))
            stack.enter_context(patch.object(state,'strategy_rows',return_value=[strategy]))
            stack.enter_context(patch.object(state,'_operational_strategy',return_value=strategy))
            stack.enter_context(patch.object(state,'_current_live_deployment_binding',return_value={'deploymentId':'dep-fixture'}))
            store=Mock();store.get.return_value=deployment
            stack.enter_context(patch.object(state,'DeploymentStore',return_value=store))
            stack.enter_context(patch('live_trader.native_spot_quantity.sizing_from_artifact',return_value={'quantityContract':{'quoteCurrency':currency}}))
            return state.resolve_continuous_runtime_scope(profile,'SMALL_LIVE','', 'source-fixture','dep-fixture'),account

    def test_existing_read_only_settings_reader_is_imported_and_scopes_all_saved_accounts(self):
        from live_trader.read_only_account import runtime_settings
        self.assertIs(state.runtime_settings,runtime_settings)
        for broker in ('binance','upbit','kis'):
            with self.subTest(broker=broker):
                scope,account=self.resolve(broker)
                self.assertEqual(scope['accountFingerprint'],account['accountFingerprint'])
                self.assertEqual(scope['credentialConfigurationHash'],account['credentialConfigurationHash'])
                self.assertEqual(scope['accountBindingId'],account['accountBindingId'])
                self.assertNotIn('fixture-secret',repr(scope));self.assertNotIn('fixture-key',repr(scope))

    def test_missing_settings_unknown_registry_account_or_changed_credentials_are_denied(self):
        for kwargs in ({'missing':True},{'mutate':lambda row:row.update(accountId='foreign-account')},
                {'mutate':lambda row:row['permissions'].update(liveAccountCredentialConfigurationHash='e'*64)},
                {'mutate':lambda row:row.update(route='upbit')},
                {'mutate':lambda row:row.update(permissions={})}):
            with self.subTest(kwargs=kwargs),self.assertRaises(ValueError):self.resolve(**kwargs)


if __name__=='__main__':unittest.main()
