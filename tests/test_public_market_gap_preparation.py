from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch
from trading_runtime.promotion_policy import build_paper_promotion_policy
from trading_runtime.market_gap_preparation import MarketGapError
from live_trader import market_gap_preparation as public
from live_trader import state
from live_trader.server import LiveTraderHandler

class SourceBindingTests(unittest.TestCase):
    def setUp(self):
        self.temp=TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        source=Path(self.temp.name)/'source.json';source.write_text('{}',encoding='utf-8')
        self.policy=build_paper_promotion_policy()
        self.row={'strategy_id':'s','deployment_id':'d','artifact_source_path':str(source),'portfolio_gate':{'portfolioId':'p'}}
        inputs={'metadata':{'portfolioId':'p','strategyId':'s'},'portfolio_artifact_hash':'a'*64,
                'deployment_id':'d','config_revision':1,'config_hash':'b'*64,'risk_policy_hash':'c'*64}
        self.fake=SimpleNamespace(STATE={'config_revision':1,'kill_switch':False},strategy_rows=lambda rows:[self.row],portfolio_rows=lambda:[],
            _operational_strategy=lambda *a,**k:self.row,strategy_broker_id=lambda row:'upbit',
            resolve_continuous_runtime_scope=lambda *a,**k:{'brokerId':'upbit','runtimeScopeId':'account-1','accountFingerprint':'d'*64,'credentialConfigurationHash':'e'*64},
            _operational_manifest_inputs=lambda row:deepcopy(inputs),load_strategy_artifacts=lambda:[self.row],
            verified_strategy_artifact_reference=lambda row:{'artifactHash':'f'*64},
            paper_live_qualification_gate_for_strategy=lambda row:{'promotionPolicy':self.policy})
        self.selection={'profile_id':'crypto','mode':'SMALL_LIVE','portfolio_id':'p','deployment_id':'d','strategy_id':'s'}
        self.loaded={'portfolioId':'p','portfolioHash':'a'*64,'sourceGraphHash':'b'*64,'members':[{
            'strategyInstanceId':'i','sourceArtifactHash':'f'*64,'provider':'upbit','brokerId':'upbit',
            'symbol':'KRW-SOL','instrumentId':'KRW-SOL','timeframe':'5m','marketKind':'SPOT','baseCurrency':'SOL','quoteCurrency':'KRW'}]}

    def test_current_loaded_Source_and_policy_bind_account_settings_and_original_selection(self):
        with patch.object(public,'load_source_target',return_value=self.loaded) as read:
            plan=public.capture_plan(self.fake,self.selection,observed_at_utc='2026-10-16T12:01:00Z')
            self.assertTrue(public.binding_is_current(self.fake,plan))
            self.assertEqual(read.call_args.args[0],Path(self.temp.name))
        self.assertEqual(plan['binding']['selection'],self.selection)
        self.assertEqual(plan['binding']['policyHash'],self.policy['policyHash'])
        self.assertFalse(plan['authorityGranted']);self.assertFalse(plan['runtimeStarted'])

    def test_deployment_fallback_cannot_mislabel_foreign_strategy_or_portfolio(self):
        for field in ('deployment_id','portfolio_id','strategy_id'):
            with self.subTest(field=field),patch.object(public,'load_source_target') as read:
                with self.assertRaises(MarketGapError):public.capture_plan(self.fake,{**self.selection,field:'foreign'})
                read.assert_not_called()

    def test_changed_revision_account_credentials_source_or_policy_invalidates_result(self):
        with patch.object(public,'load_source_target',return_value=self.loaded):
            plan=public.capture_plan(self.fake,self.selection,observed_at_utc='2026-10-16T12:01:00Z')
            self.fake.STATE['config_revision']=2;self.assertFalse(public.binding_is_current(self.fake,plan));self.fake.STATE['config_revision']=1
            self.loaded['sourceGraphHash']='0'*64;self.assertFalse(public.binding_is_current(self.fake,plan));self.loaded['sourceGraphHash']='b'*64
            self.fake.resolve_continuous_runtime_scope=lambda *a,**k:{'brokerId':'upbit','runtimeScopeId':'account-1','accountFingerprint':'different','credentialConfigurationHash':'e'*64}
            self.assertFalse(public.binding_is_current(self.fake,plan))
    def test_hot_guard_never_reloads_history_but_final_full_account_Source_policy_rechecks(self):
        with patch.object(public,'load_source_target',return_value=self.loaded) as read:
            plan,current,final=public.capture_preparation(self.fake,self.selection)
            self.assertTrue(all(current(plan) for _ in range(100)));self.assertEqual(read.call_count,1)
            self.loaded['sourceGraphHash']='0'*64
            self.assertFalse(final(plan));self.assertEqual(read.call_count,2)
            self.fake.STATE['config_revision']=2;self.assertFalse(current(plan))

    def test_caller_URL_policy_members_unknown_fields_and_stock_are_rejected_before_read(self):
        for selection in ({**self.selection,'members':[]},{**self.selection,'endpoint':'https://evil.invalid'},{**self.selection,'profile_id':'stock'}):
            with patch.object(public,'load_source_target') as read:
                with self.assertRaises(MarketGapError):public.capture_plan(self.fake,selection)
                read.assert_not_called()

class PublicGapHttpTests(unittest.TestCase):
    def handler(self,path,body=None,allowed=True):
        handler=object.__new__(LiveTraderHandler);handler.path=path
        handler._authorize_functional_http=Mock(return_value=allowed)
        handler._send_functional_http_denial=Mock();handler.read_json=Mock(return_value=body);handler.send_json=Mock()
        return handler

    def test_mutations_require_normal_cookie_origin_csrf_and_reject_absolute_form_before_body(self):
        for path in ('/api/preparation/market-gaps/begin','/api/preparation/market-gaps/cancel'):
            for request in (path,'http://localhost'+path):
                handler=self.handler(request,{},False)
                with patch.object(state,'begin_public_market_gap_preparation') as begin,patch.object(state,'cancel_public_market_gap_preparation') as cancel:
                    handler.do_POST()
                handler.read_json.assert_not_called();begin.assert_not_called();cancel.assert_not_called()

    def test_authenticated_begin_calls_only_public_diagnostic_never_start_or_native(self):
        handler=self.handler('/api/preparation/market-gaps/begin',{'profile_id':'crypto'})
        with patch.object(state,'begin_public_market_gap_preparation',return_value={'ok':True}) as begin,patch.object(state,'start_continuous_runtime') as start:
            handler.do_POST()
        handler._authorize_functional_http.assert_called_once_with(require_origin=True);begin.assert_called_once();start.assert_not_called()

    def test_status_is_cookie_guarded_and_requires_exact_single_id(self):
        for query in ('','?preparationId=a&preparationId=b','?preparationId=a&extra=1'):
            handler=self.handler('/api/preparation/market-gaps/status'+query)
            with patch.object(state,'public_market_gap_preparation_status') as status:handler.do_GET()
            status.assert_not_called()
        handler=self.handler('/api/preparation/market-gaps/status?preparationId=a')
        with patch.object(state,'public_market_gap_preparation_status',return_value={'ok':True}) as status:handler.do_GET()
        status.assert_called_once_with('a');handler._authorize_functional_http.assert_called_once_with(require_origin=False)

    def test_cancel_exact_body_and_stale_optional_Start_note_never_reach_original_start(self):
        handler=self.handler('/api/preparation/market-gaps/cancel',{'preparationId':'a','source':{}})
        with patch.object(state,'cancel_public_market_gap_preparation') as cancel:handler.do_POST()
        cancel.assert_not_called()
        handler=self.handler('/api/runtime/start',{'profile_id':'crypto','mode':'SMALL_LIVE','marketGapPreparationId':'old'})
        with patch.object(state,'public_market_gap_start_note_current',return_value=False),patch.object(state,'start_continuous_runtime') as start:
            handler.do_POST()
        start.assert_not_called();self.assertFalse(handler.send_json.call_args.args[0]['runtimeStarted'])

    def test_STOP_wrapper_cancels_matching_diagnostic_before_existing_STOP_locks(self):
        order=[]
        with patch.object(state,'invalidate_public_market_gap_preparations',side_effect=lambda **k:order.append(('cancel',k))):
            @state._public_gap_stop_invalidator
            def original(profile_id='',**kwargs):order.append(('original',profile_id,kwargs));return 'original-result'
            self.assertEqual(original('crypto',runtime_id='r',deployment_id='d'),'original-result')
        self.assertEqual(order[0],('cancel',{'profile_id':'crypto','runtime_id':'r','deployment_id':'d'}));self.assertEqual(order[1][0],'original')
