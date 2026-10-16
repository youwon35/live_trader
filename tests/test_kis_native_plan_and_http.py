import copy
from contextlib import ExitStack
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from live_trader import state, contracts
from live_trader.server import LiveTraderHandler
from trading_runtime.artifact_governance import artifact_reference, verify_strategy_artifact
from trading_runtime.portfolio_runtime import load_portfolio_runtime_path
from test_live_kis_native_bootstrap import rest_fixture
from test_kis_scoped_operational_authority import context


class NativePlanTests(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory();self.addCleanup(self.directory.cleanup)
        self.root=Path(self.directory.name);(self.root/'portfolios').mkdir();(self.root/'strategy-instances').mkdir()
        self.expected=rest_fixture()
        for key,value in self.expected.sources.items():
            (self.root/(key+'.json')).write_text(json.dumps(value),encoding='utf-8')
        for key,value in self.expected.instances.items():
            (self.root/'strategy-instances'/(key+'.json')).write_text(json.dumps(value),encoding='utf-8')
        self.path=self.root/'portfolios/portfolio.json';self.path.write_text(json.dumps(self.expected.portfolio),encoding='utf-8')
        self.loaded=load_portfolio_runtime_path(self.path,artifact_root=self.root)
        self.context={**context(),'portfolioId':self.loaded.portfolio_id}
        self.rows=[]
        for spec in self.loaded.specs:
            source=self.expected.sources[spec.strategy_id]
            verification=verify_strategy_artifact(source)
            self.rows.append({'symbol':spec.symbol,'artifact_reference':artifact_reference(source),
                'artifact_integrity':{'valid':verification.valid,'declaredHash':verification.declared_hash,
                                      'computedHash':verification.computed_hash},
                'paper_live_qualification':{'required':True,'ready':True,'portfolioRequired':True,
                    'strategyInstanceId':spec.strategy_instance_id,'portfolioArtifactId':self.loaded.portfolio_id,
                    'portfolioArtifactHash':self.loaded.portfolio_hash,'promotionPolicy':self.expected.promotion_policy,
                    'promotionPolicyHash':self.expected.promotion_policy['policyHash']}})
        self.stack=ExitStack();self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.object(state,'_kis_scoped_start_material',return_value=(self.context,None)))
        self.stack.enter_context(patch.object(state,'_upbit_functional_durable_authority_open',return_value=False))
        self.stack.enter_context(patch.dict(state.STATE,{'kill_switch':False}))
        self.stack.enter_context(patch.object(state,'portfolio_rows',return_value=[{'id':self.loaded.portfolio_id,'source_path':str(self.path)}]))
        self.stack.enter_context(patch.object(contracts,'strategy_artifact_dirs',return_value=[self.root]))
        self.governance=self.stack.enter_context(patch.object(state,'OPERATIONAL_GOVERNANCE'))
        self.governance.get_deployment_manifest.return_value=SimpleNamespace(portfolio_artifact_hash=self.loaded.portfolio_hash,manifest_hash='1'*64)
        self.stack.enter_context(patch.object(state,'resolve_continuous_runtime_scope',return_value={'runtimeScopeId':'runtime-alpha'}))
        self.stack.enter_context(patch.object(state,'load_strategy_artifacts',side_effect=lambda:copy.deepcopy(self.rows)))

    def capture(self):
        return state._capture_kis_native_start_plan('stock','SMALL_LIVE',self.loaded.portfolio_id,'deployment-alpha','strategy-alpha')

    def test_actual_source_instance_portfolio_loader_and_verified_policy_produce_bound_plan(self):
        plan=self.capture()
        self.assertEqual(plan['loaded'].portfolio_hash,self.loaded.portfolio_hash)
        self.assertEqual(plan['artifactRoot'],self.root.resolve())
        self.assertEqual(plan['promotionPolicy'],self.expected.promotion_policy)
        self.assertEqual(plan['deploymentManifestHash'],'1'*64)
        self.assertIsNone(plan['preflight'])  # old expiry is not extended by plan capture

    def test_operational_member_digest_cannot_replace_canonical_source_hash(self):
        self.rows[0]['artifact_reference']['artifactHash']=state._operational_artifact_hash(self.rows[0])
        with self.assertRaises(ValueError):self.capture()

    def test_foreign_parent_policy_or_missing_full_policy_is_not_borrowed(self):
        for key,value in [('portfolioArtifactId','foreign-parent'),('portfolioArtifactHash','0'*64),
                          ('portfolioRequired',False),('promotionPolicy',None),('promotionPolicyHash','0'*64)]:
            original=copy.deepcopy(self.rows)
            self.rows[0]['paper_live_qualification'][key]=value
            with self.subTest(key=key),self.assertRaises(ValueError):self.capture()
            self.rows=original

    def test_missing_portfolio_or_unconfigured_canonical_root_is_hard_hold(self):
        self.context['portfolioId']=''
        with self.assertRaisesRegex(ValueError,'sealed-portfolio-required'):self.capture()
        self.context['portfolioId']=self.loaded.portfolio_id
        with patch.object(contracts,'strategy_artifact_dirs',return_value=[self.root/'foreign']):
            with self.assertRaisesRegex(ValueError,'artifact-root-unproven'):self.capture()

    def test_raw_graph_tamper_equal_path_is_not_hidden_by_cached_context(self):
        original=self.path.read_bytes();value=json.loads(original);value['name']='changed'
        self.path.write_text(json.dumps(value),encoding='utf-8')
        with self.assertRaises((ValueError,RuntimeError)):self.capture()


class NativePreparationHttpTests(unittest.TestCase):
    def handler(self,path,authorized=True,payload=None):
        handler=object.__new__(LiveTraderHandler);handler.path=path
        handler._authorize_functional_http=Mock(return_value=authorized)
        handler._send_functional_http_denial=Mock();handler.send_json=Mock();handler.read_json=Mock(return_value=payload)
        return handler

    def test_cancel_requires_session_origin_csrf_and_relative_target_before_body_or_state(self):
        for target in ('/api/runtime/kis-native-prepare-cancel','http://localhost/api/runtime/kis-native-prepare-cancel'):
            handler=self.handler(target,False,{'preparationId':'a'*43})
            with patch.object(state,'cancel_kis_native_preparation') as cancel:handler.do_POST()
            handler.read_json.assert_not_called();cancel.assert_not_called()

    def test_status_unknown_extra_duplicate_query_or_unauthenticated_never_calls_state(self):
        for path,authorized in [('/api/runtime/kis-native-prepare-status?preparationId='+('a'*43),False),
                ('http://localhost/api/runtime/kis-native-prepare-status?preparationId='+('a'*43),True),
                ('/api/runtime/kis-native-prepare-status?preparationId=a&preparationId=b',True),
                ('/api/runtime/kis-native-prepare-status?preparationId=a&grant=true',True)]:
            handler=self.handler(path,authorized)
            with patch.object(state,'kis_native_preparation_status') as status:handler.do_GET()
            status.assert_not_called()

    def test_exact_status_is_observation_only_and_cancel_accepts_no_authority_fields(self):
        handler=self.handler('/api/runtime/kis-native-prepare-status?preparationId='+('a'*43))
        with (patch.object(state,'kis_native_preparation_status',return_value={'ok':True,'status':'READY'}) as status,
                patch.object(state,'start_continuous_runtime') as start):
            handler.do_GET();status.assert_called_once_with('a'*43);start.assert_not_called()
        for payload in ({'preparationId':'a'*43,'runtimeId':'other'}, {'preparationId':'a'*43,'cleanup_requested':True},None):
            handler=self.handler('/api/runtime/kis-native-prepare-cancel',True,payload)
            with patch.object(state,'cancel_kis_native_preparation') as cancel:handler.do_POST()
            cancel.assert_not_called()
        handler=self.handler('/api/runtime/kis-native-prepare-cancel',True,{'preparationId':'a'*43})
        with patch.object(state,'cancel_kis_native_preparation',return_value={'ok':True}) as cancel:handler.do_POST()
        cancel.assert_called_once_with('a'*43)
        handler._authorize_functional_http.assert_called_once_with(require_origin=True)
