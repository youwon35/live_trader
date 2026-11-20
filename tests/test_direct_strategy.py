"""Offline canonical catalog -> account binding -> normal runtime contract tests."""
from __future__ import annotations
import ast
from copy import deepcopy
import importlib
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

STAGE = Path(__file__).resolve().parents[1]
SYSTEM = Path('D:/github/PROGRAM/trading-system')
runtime_package = STAGE.parents[1] / 'packages/trading_runtime' if STAGE.parent.name == 'apps' else SYSTEM / 'packages/trading_runtime'
sys.path.insert(0, str(runtime_package if runtime_package.exists() else SYSTEM / 'packages/trading_runtime'))
import trading_runtime
trading_runtime.__path__.insert(0, str(STAGE.parent / 'shared/trading_runtime'))
# Package __init__ can eagerly import the old loader before the staging overlay.
if (STAGE.parent/'shared/trading_runtime/portfolio_runtime.py').is_file():
    sys.modules.pop('trading_runtime.portfolio_runtime',None)
PACKAGE = '_direct_live_offline'
package = types.ModuleType(PACKAGE)
package.__path__ = [str(STAGE / 'live_trader')]
sys.modules[PACKAGE] = package
service = importlib.import_module(PACKAGE + '.direct_strategy')
contracts = importlib.import_module(PACKAGE + '.contracts')
from trading_runtime.artifact_governance import (DeploymentStore, artifact_reference, stable_sha256,
    seal_strategy_artifact, seal_strategy_instance, seal_portfolio_artifact, assert_verified_strategy_instance)
from trading_runtime.strategy_runtime_eligibility import runtime_instance_payload
from trading_runtime.portfolio_runtime import load_portfolio_runtime

SETTINGS = {'BINANCE_API_KEY':'offline-public-id', 'BINANCE_API_SECRET':'offline-secret'}

def write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding='utf-8')

def fixture(root, *, portfolio=False, mode='real', plugin='moving_average_cross', stage='backtested'):
    source = seal_strategy_artifact({'id':'source-one','schemaVersion':'strategy-artifact-v1',
        'artifactType':'strategy','name':'Direct offline fixture','plugin':plugin,'symbol':'BTCUSDT',
        'asset':'CRYPTO','timeframe':'1m','parameters':{'shortMa':3,'longMa':5},
        'lifecycle_status':stage, 'data_mode':mode, 'finalTest':{'status':'fail'},
        'permissions':{'backtester_verified':False, 'paper_trader_verified':False, 'fail_reasons':['research-return-low']}})
    reference = artifact_reference(source)
    write(root / 'source.json', source)
    result = None
    if portfolio:
        instance = seal_strategy_instance({**runtime_instance_payload(source, reference['artifactHash'], 'template-one'),
            'symbol':'BTCUSDT','timeframe':'1m','marketDataProvider':'binance','brokerId':'binance'})
        embedded = {**instance, 'instanceId':'member-one', 'templateInstanceId':'template-one',
            'sourceInstanceHash':assert_verified_strategy_instance(instance)}
        result = seal_portfolio_artifact({'id':'portfolio-one','schemaVersion':'portfolio-artifact-v1',
            'artifactType':'portfolio','lifecycle_status':'backtested','strategyInstances':[embedded]})
        write(root / 'strategy-instances/one.json', instance)
        write(root / 'portfolios/one.json', result)
    return source, result

class DirectStrategyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=STAGE)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source, _ = fixture(self.root)
        self.network = patch.object(socket, 'socket', side_effect=AssertionError('Network forbidden'))
        self.network.start()
        self.addCleanup(self.network.stop)

    def catalog(self): return service.list_candidates(roots=[self.root],settings=SETTINGS)
    def request(self, kind='strategy:'):
        catalog = self.catalog()
        row = next(row for row in catalog['candidates'] if row['candidateId'].startswith(kind))
        return {key:row[key] for key in ('rootKey','candidateId','identityHash','registryHash')} | {
            'accountProfileId':catalog['accounts'][0]['profileId'], 'capitalPercent':10}
    def register(self, kind='strategy:'):
        return service.register(self.request(kind),roots=[self.root],settings=SETTINGS)

    def test_research_failure_can_register_without_paper_source_mutation_or_execution(self):
        original = (self.root/'source.json').read_bytes()
        result = self.register()
        self.assertEqual(result['mode'],'MONITOR')
        self.assertIs(result['authorizationGranted'],False)
        self.assertEqual(result['ordersSubmitted'],0)
        deployment = DeploymentStore(self.root).get(result['deploymentId'])
        self.assertEqual(deployment['lifecycle'],'backtested')
        self.assertIs(deployment['permissions']['paper_trader_verified'],False)
        self.assertNotIn('paperEvidenceId', deployment['permissions'])
        self.assertEqual((self.root/'source.json').read_bytes(),original)
        self.assertTrue(service.assess(self.root,self.source,deployment,settings=SETTINGS)['ready'])

    def test_same_bar_breakout_is_absent_from_live_catalog_and_rejects_old_standalone(self):
        source, _ = fixture(self.root, plugin="breakout")
        with self.assertRaisesRegex(ValueError, "같은 봉 진입·청산"):
            service.source_eligibility(source)
        self.assertEqual(self.catalog()["candidates"], [])

        tree = ast.parse((STAGE / "live_trader/continuous_live.py").read_text(encoding="utf-8-sig"))
        cls = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "LiveContinuousController")
        method = next(node for node in cls.body if isinstance(node, ast.FunctionDef) and node.name == "_standalone_spec")
        method.decorator_list = []
        namespace = {"Any": object, "RuntimeStrategySpec": object}
        exec(compile(ast.Module(body=[method], type_ignores=[]), "<actual standalone spec>", "exec"), namespace)
        with self.assertRaisesRegex(ValueError, "같은 봉 진입·청산"):
            namespace["_standalone_spec"](source)

    def test_source_mode_lifecycle_tamper_and_unimplemented_plugin_rejected(self):
        for options in ({'mode':'sample'},{'mode':'unknown'},{'stage':'retired'},{'plugin':'missing_plugin'}):
            with self.subTest(options=options):
                fixture(self.root,**options)
                self.assertEqual(self.catalog()['candidates'],[])
        fixture(self.root)
        changed=json.loads((self.root/'source.json').read_text());changed['parameters']['longMa']=100
        write(self.root/'source.json',changed)
        self.assertEqual(self.catalog()['candidates'],[])

    def test_request_cas_and_changed_account_and_source_fail_closed(self):
        request=self.request()
        result=self.register()
        with self.assertRaises(ValueError):service.register(request,roots=[self.root],settings=SETTINGS)
        deployment=DeploymentStore(self.root).get(result['deploymentId'])
        self.assertFalse(service.assess(self.root,self.source,deployment,settings={**SETTINGS,'BINANCE_API_SECRET':'changed'})['ready'])
        other=deepcopy(self.source);other['parameters']['longMa']=7;other=seal_strategy_artifact(other)
        write(self.root/'source.json',other)
        self.assertFalse(service.assess(self.root,self.source,deployment,settings=SETTINGS)['ready'])

    def test_invalid_capital_and_unknown_request_fields_do_not_register(self):
        request=self.request()
        for value in (True,0,-1,101,float('nan'),float('inf'),'10'):
            with self.subTest(value=value),self.assertRaises(ValueError):
                service.register({**request,'capitalPercent':value},roots=[self.root],settings=SETTINGS)
        with self.assertRaises(ValueError): service.register({**request,'start':True},roots=[self.root],settings=SETTINGS)
        self.assertEqual(DeploymentStore(self.root).list(),[])

    def test_portfolio_runtime_spec_exactly_matches_direct_binding(self):
        self.source, portfolio=fixture(self.root,portfolio=True)
        result=self.register('portfolio:')
        deployment=DeploymentStore(self.root).get(result['deploymentId'])
        direct=service.assess(self.root,self.source,deployment,settings=SETTINGS)
        self.assertTrue(direct['ready'],direct)
        row={'direct_execution':direct,'lifecycle_status':'backtested'}
        spec=load_portfolio_runtime(self.root,'portfolio-one').specs[0]
        self.assertTrue(service.binding_covers_spec(row,spec))
        from dataclasses import replace
        self.assertFalse(service.binding_covers_spec(row,replace(spec,symbol='ETHUSDT')))
        pid,pref,members=service.manifest_members(row)
        metadata={'directBindings':[direct['binding']],'directBindingsHash':stable_sha256([direct['binding']]),
            'strategyMembers':members,'deploymentBinding':{**deployment,'directExecutionBindingHash':direct['binding']['bindingHash']}}
        self.assertTrue(service.validate_manifest(metadata))
        metadata['strategyMembers'][0]['timeframe']='5m'
        self.assertFalse(service.validate_manifest(metadata))

    def test_projection_does_not_require_paper_or_claim_research_pass(self):
        result=self.register()
        deployment=DeploymentStore(self.root).get(result['deploymentId'])
        with patch.dict(os.environ,SETTINGS,clear=False):
            enriched=contracts.enrich_strategy_artifact_runtime(self.root,self.root/'source.json',self.source,deployment_rows=[deployment])
        self.assertTrue(enriched['_direct_execution']['ready'],enriched['_direct_execution'])
        self.assertFalse(enriched['_paper_live_qualification_required'])
        projected=contracts.normalize_strategy_artifact(enriched)
        self.assertFalse(projected['paper_trader_verified'])
        self.assertFalse(projected['backtester_verified'])
        self.assertTrue(projected['permissions']['live_allowed'],projected['permissions'])

    def test_settings_only_futures_route_is_not_relabelled_as_spot(self):
        source=deepcopy(self.source)
        source['settings']=source.pop('parameters')
        source.update(symbol='SOLUSDT',timeframe='1h',marketDataProvider='binance-futures',
            brokerId='binance-futures',marketType='futures',dataArtifact={'provider':'binance'})
        source=seal_strategy_artifact(source)
        self.assertEqual(service._broker(source,'SOLUSDT'),'binance-futures')
        instance=seal_strategy_instance(runtime_instance_payload(source,artifact_reference(source)['artifactHash'],'futures-template'))
        embedded={**instance,'instanceId':'futures-member','templateInstanceId':'futures-template',
            'sourceInstanceHash':assert_verified_strategy_instance(instance)}
        portfolio=seal_portfolio_artifact({'id':'futures-portfolio','lifecycle_status':'backtested',
            'strategyInstances':[embedded]})
        write(self.root/'source.json',source);write(self.root/'strategy-instances/future.json',instance)
        write(self.root/'portfolios/future.json',portfolio)
        spec=load_portfolio_runtime(self.root,'futures-portfolio').specs[0]
        self.assertEqual((spec.provider,spec.broker_id),('binance-futures','binance-futures'))
        candidates=service._catalog(self.root)
        self.assertEqual(len(candidates),2)
        self.assertTrue(all(row['members'][0]['brokerId']=='binance-futures' for row in candidates))
        self.assertEqual(spec.parameters,source['settings'])

    def test_actual_normal_deployment_manifest_and_standalone_spec_bind(self):
        self.source=seal_strategy_artifact({**self.source,"settings":{**self.source["parameters"],"fee":0.1,"interval":"1h"}})
        write(self.root/"source.json",self.source)
        result=self.register();deployment=DeploymentStore(self.root).get(result['deploymentId'])
        with patch.dict(os.environ,SETTINGS,clear=False):
            row=contracts.normalize_strategy_artifact(contracts.enrich_strategy_artifact_runtime(self.root,self.root/'source.json',self.source,deployment_rows=[deployment]))
        tree=ast.parse((STAGE/'live_trader/state.py').read_text(encoding='utf-8-sig'))
        names={'_current_live_deployment_binding','_live_deployment_binding_is_executable','_operational_artifact_hash'}
        selected=[node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name in names]
        from trading_runtime.lifecycle import normalize_lifecycle_status
        namespace={'Any':object,'Path':Path,'DeploymentStore':DeploymentStore,'governance_sha256':stable_sha256,
            'normalize_lifecycle_status':normalize_lifecycle_status,'safe_float':lambda value,default=0:float(value or default),
            '_normalized_execution_permission_digest':lambda value:stable_sha256(value)}
        exec(compile(ast.Module(body=selected,type_ignores=[]),'<pure actual normal binding>','exec'),namespace)
        binding=namespace['_current_live_deployment_binding'](row,require_store=True)
        self.assertTrue(namespace['_live_deployment_binding_is_executable'](binding,mode='SMALL_LIVE'))
        pid,pref,members=service.manifest_members(row)
        metadata={'deploymentBinding':binding,'directBindings':[row['direct_execution']['binding']],
            'directBindingsHash':stable_sha256([row['direct_execution']['binding']]),'strategyMembers':members}
        self.assertTrue(service.validate_manifest(metadata),metadata)
        self.assertEqual(namespace['_operational_artifact_hash'](row),members[0]['artifactHash'])
        controller=ast.parse((STAGE/'live_trader/continuous_live.py').read_text(encoding='utf-8-sig'))
        cls=next(n for n in controller.body if isinstance(n,ast.ClassDef) and n.name=='LiveContinuousController')
        method=next(n for n in cls.body if isinstance(n,ast.FunctionDef) and n.name=='_standalone_spec')
        method.decorator_list=[]
        from trading_runtime.continuous_runtime import RuntimeStrategySpec
        from trading_runtime.portfolio_runtime import infer_market_route
        namespace.update(RuntimeStrategySpec=RuntimeStrategySpec,infer_market_route=infer_market_route,artifact_content_hash=lambda _: (_ for _ in ()).throw(AssertionError('DTO hash cannot replace canonical source')))
        exec(compile(ast.Module(body=[method],type_ignores=[]),'<actual standalone spec>','exec'),namespace)
        spec=namespace['_standalone_spec'](row)
        self.assertTrue(service.binding_covers_spec(row,spec))

    def test_actual_controller_direct_gate_keeps_identity_and_futures_guards(self):
        self.source, portfolio=fixture(self.root,portfolio=True)
        result=self.register('portfolio:'); deployment=DeploymentStore(self.root).get(result['deploymentId'])
        direct=service.assess(self.root,self.source,deployment,settings=SETTINGS)
        row={'direct_execution':direct,'deployment_id':result['deploymentId'],
             'lifecycle_status':'backtested','live_eligible':True,'live_small_eligible':True}
        spec=load_portfolio_runtime(self.root,'portfolio-one').specs[0]
        tree=ast.parse((STAGE/'live_trader/continuous_live.py').read_text(encoding='utf-8-sig'))
        cls=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='LiveContinuousController')
        cls.body=[n for n in cls.body if isinstance(n,ast.FunctionDef) and n.name in {
            '_direct_binding_for_spec','_spec_mode_allowed','_paper_final_binding_blocker'}]
        namespace={'__package__':PACKAGE,'Any':object,
            'futures_execution_policy_from_artifact':lambda value:{'valid':False}}
        state=types.ModuleType(PACKAGE+'.state')
        state.portfolio_rows=lambda:[]
        state.strategy_rows=lambda _: [row]
        state.paper_live_qualification_gate_for_strategy=lambda _: (_ for _ in ()).throw(AssertionError('Direct must not load Paper'))
        with patch.dict(sys.modules,{PACKAGE+'.state':state}):
            exec(compile(ast.Module(body=[cls],type_ignores=[]),'<actual direct controller gates>','exec'),namespace)
            controller=namespace['LiveContinuousController']
            self.assertTrue(controller._spec_mode_allowed(spec,'SMALL_LIVE',result['deploymentId']))
            self.assertEqual(controller._paper_final_binding_blocker((spec,),result['deploymentId']),'')
            self.assertFalse(controller._spec_mode_allowed(spec,'SMALL_LIVE','wrong-deployment'))
            from dataclasses import replace
            self.assertFalse(controller._spec_mode_allowed(replace(spec,artifact={**spec.artifact,'market_type':'futures'}),'SMALL_LIVE',result['deploymentId']))
            state.strategy_rows=lambda _: [row,{**row,'deployment_id':'another'}]
            self.assertEqual(controller._direct_binding_for_spec(spec),{})
            self.assertTrue(controller._paper_final_binding_blocker((spec,)).startswith('direct-binding-ambiguous'))
            self.assertTrue(controller._spec_mode_allowed(spec,'SMALL_LIVE',result['deploymentId']))

if __name__=='__main__':unittest.main()
