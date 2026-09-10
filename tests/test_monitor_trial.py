from __future__ import annotations
from copy import deepcopy
import importlib.util
import ast
import types
from urllib.parse import urlparse
from unittest.mock import Mock
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import test_paper_candidate_inbox as base
from trading_runtime.artifact_governance import seal_strategy_artifact, seal_strategy_instance, seal_portfolio_artifact, artifact_reference, assert_verified_strategy_instance, DeploymentStore, EvidenceStore, stable_sha256
from trading_runtime.paper_live_contract import validate_paper_live_evidence

name=base.PACKAGE+".monitor_trial"
if name in sys.modules:service=sys.modules[name]
else:
 spec=importlib.util.spec_from_file_location(name,base.APP_ROOT/"live_trader/monitor_trial.py")
 service=importlib.util.module_from_spec(spec);sys.modules[name]=service;spec.loader.exec_module(service)


def fixture(root, *, plugin="moving_average_cross", prices=None, change=None):
 flags={"evidenceClass":"FUNCTIONAL_TEST_NON_PROMOTION","promotionEligible":False,"useAsPromotionEvidence":False,"backtester_verified":True,"permissions":{"live_allowed":False,"live_small_eligible":False,"live_eligible":False}}
 raw={**deepcopy(flags),"id":"trial-strategy","artifact_schema_version":"strategy-artifact-v1","symbol":"BTCUSDT","timeframe":"5m","plugin":plugin,"parameters":{"shortMa":3,"longMa":5},
  "traderContract":{"canPlaceOrders":False},"sample_prices":prices if prices is not None else [100+(i%20 if i%40<20 else 20-i%20) for i in range(260)],
  "dataArtifact":{"closedBarProvenance":{"confirmed":True,"interval":"5m","finalBarEnd":"2026-08-03T18:20:00Z","datasetUpdatedAt":"2026-08-03T18:30:00Z"}}}
 if change:change(raw)
 strategy=seal_strategy_artifact(raw);ref=artifact_reference(strategy)
 instance=seal_strategy_instance({"instanceId":"trial-instance","sourceStrategyId":ref["artifactId"],"sourceArtifactHash":ref["artifactHash"],"symbol":"BTCUSDT","timeframe":"5m","pluginId":plugin,"parameters":raw["parameters"],"marketDataProvider":"binance","brokerId":"binance"})
 instance_hash=assert_verified_strategy_instance(instance)
 portfolio=seal_portfolio_artifact({**deepcopy(flags),"id":"trial-portfolio","name":"관찰 시험 구성","strategyInstances":[{**instance,"sourceInstanceHash":instance_hash}]})
 base.write(root/"strategy.json",strategy);base.write(root/"strategy-instances/instance.json",instance);base.write(root/"portfolios/portfolio.json",portfolio)
 return strategy,instance,portfolio


class MonitorTrialTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
  self.root=Path(self.temp.name)/"artifacts";self.output=Path(self.temp.name)/"results";fixture(self.root)
  self.network=patch.object(base.socket,"socket",side_effect=AssertionError("network forbidden"));self.network.start();self.addCleanup(self.network.stop)
 def row(self):return service.list_monitor_trials([self.root])[0]
 def run_trial(self,request=None):return service.run_monitor_trial(request or self.row()["request"],roots=[self.root],report_root=self.output)
 def test_production_loader_engine_calculate_signals_without_orders_or_authority(self):
  before=base.file_bytes(self.root)
  with patch.object(DeploymentStore,"transition",side_effect=AssertionError("authority forbidden")),patch.object(DeploymentStore,"create_definition",side_effect=AssertionError("deployment forbidden")),patch.object(EvidenceStore,"save_paper",side_effect=AssertionError("promotion forbidden")):
   report=self.run_trial()
  self.assertEqual(before,base.file_bytes(self.root));self.assertEqual("MONITOR",report["mode"])
  self.assertEqual(260,report["summary"]["decisionCount"]);self.assertGreater(report["summary"]["signals"]["BUY"],0);self.assertGreater(report["summary"]["signals"]["SELL"],0)
  self.assertEqual(0,report["ordersSubmitted"]);self.assertEqual(0,report["accountCalls"]);self.assertFalse(report["currentDeploymentChanged"])
  self.assertFalse(report["promotionEligible"]);self.assertFalse(report["authorizationGranted"])
  self.assertIsNone(report["actualPeriodStart"]);self.assertEqual("SYNTHETIC_CONTIGUOUS_REPLAY_CADENCE",report["timestampMode"])
  saved=json.loads(Path(report["reportPath"]).read_text(encoding="utf-8"));digest=saved.pop("reportHash");self.assertEqual(digest,stable_sha256(saved))
  self.assertFalse(validate_paper_live_evidence(saved).valid)
  self.assertFalse(any(key.startswith(base.PACKAGE+".state") or key.startswith(base.PACKAGE+".continuous_live") for key in sys.modules))
 def test_list_is_read_only_and_keeps_normal_publication_candidates_empty(self):
  before=base.file_bytes(self.root);inbox=base.service.list_paper_candidates(roots=[self.root])
  self.assertEqual([],inbox["candidates"]);self.assertFalse(inbox["canImport"]);self.assertTrue(inbox["monitorTrials"][0]["canRun"]);self.assertEqual(before,base.file_bytes(self.root))
 def test_tampered_or_stale_source_identity_is_rejected(self):
  request=self.row()["request"];payload=json.loads((self.root/"strategy.json").read_text());payload["sample_prices"][0]+=1;base.write(self.root/"strategy.json",payload)
  with self.assertRaises(ValueError):self.run_trial(request)
  self.assertFalse(self.output.exists())
 def test_request_cannot_choose_an_arbitrary_path_or_authority_mode(self):
  request=self.row()["request"]
  for invalid in ({**request,"path":"C:/"},{**request,"mode":"SMALL_LIVE"},{**request,"rootKey":"unknown"},{**request,"identityHash":"stale"}):
   with self.subTest(invalid=invalid),self.assertRaises(ValueError):self.run_trial(invalid)
  self.assertFalse(self.output.exists())
 def test_normal_or_order_enabled_artifacts_cannot_enter_trial(self):
  for change in (lambda raw:raw.update(promotionEligible=True),lambda raw:raw["permissions"].update(live_allowed=True),lambda raw:raw["traderContract"].update(canPlaceOrders=True)):
   fixture(self.root,change=change);self.assertFalse(self.row()["canRun"])
 def test_missing_or_invalid_close_samples_are_blocked(self):
  for prices in ([1,2], [100]*259+[False], [100]*259+[0], [100]*259+[float("inf")]):
   fixture(self.root,prices=prices);self.assertFalse(self.row()["canRun"])
 def test_ohlcv_dependent_plugins_cannot_use_synthetic_close_adapter(self):
  fixture(self.root,plugin="breakout");self.assertFalse(self.row()["canRun"])
 def test_missing_or_future_source_end_is_not_guessed(self):
  for change in (lambda raw:raw.update(dataArtifact={}),lambda raw:raw["dataArtifact"]["closedBarProvenance"].update(finalBarEnd="2026-09-01T00:00:00Z")):
   fixture(self.root,change=change);self.assertFalse(self.row()["canRun"])
 def test_input_change_during_evaluation_prevents_report_commit(self):
  request=self.row()["request"];original=service._load;calls=0
  def changing(*args):
   nonlocal calls
   loaded,bindings,identity=original(*args);calls+=1
   if calls>1:identity={**identity,"changed":True}
   return loaded,bindings,identity
  with patch.object(service,"_load",side_effect=changing),self.assertRaisesRegex(ValueError,"시험 중"):
   self.run_trial(request)
  self.assertFalse(self.output.exists())
 def test_report_cannot_be_read_as_normal_paper_evidence(self):
  report=self.run_trial();base.write(self.root/"evidence/paper/not-a-promotion.json",report)
  inbox=base.service.list_paper_candidates(roots=[self.root]);self.assertEqual("BLOCKED",inbox["candidates"][0]["status"]);self.assertFalse(inbox["canImport"])

 def test_trial_post_requires_native_authorization_before_body_and_no_snapshot_hook(self):
  tree=ast.parse((base.APP_ROOT/"live_trader/server.py").read_text(encoding="utf-8-sig"))
  method=next(node for cls in tree.body if isinstance(cls,ast.ClassDef) for node in cls.body if isinstance(node,ast.FunctionDef) and node.name=="do_POST")
  paths_node=next(node.value for node in tree.body if isinstance(node,ast.Assign) and any(isinstance(target,ast.Name) and target.id=="_FUNCTIONAL_MUTATION_PATHS" for target in node.targets))
  paths=eval(compile(ast.Expression(paths_node),"<paths>","eval"),{"frozenset":frozenset})
  run=Mock(return_value={"ok":True,"authorizationGranted":False});namespace={"urlparse":urlparse,"_FUNCTIONAL_STATUS_PATHS":set(),"_FUNCTIONAL_MUTATION_PATHS":paths,"state":types.SimpleNamespace(run_local_monitor_trial=run)}
  cls=ast.ClassDef(name="Handler",bases=[],keywords=[],body=[method],decorator_list=[]);ast.fix_missing_locations(cls)
  exec(compile(ast.Module(body=[cls],type_ignores=[]),"<isolated-http>","exec"),namespace)
  handler=namespace["Handler"]();handler.path="/api/monitor-trial/run";handler.read_json=Mock(return_value={"fixture":True});handler.send_json=Mock();handler._authorize_functional_http=Mock(return_value=False)
  handler.do_POST();handler._authorize_functional_http.assert_called_once_with(require_origin=True);handler.read_json.assert_not_called();run.assert_not_called()
  handler._authorize_functional_http.return_value=True;handler.do_POST();run.assert_called_once_with({"fixture":True});handler.send_json.assert_called_once_with({"ok":True,"authorizationGranted":False})
 def test_duplicate_streams_are_not_silently_replaced_by_first_samples(self):
  path=self.root/"portfolios/portfolio.json";payload=json.loads(path.read_text());payload["strategyInstances"].append(deepcopy(payload["strategyInstances"][0]));base.write(path,seal_portfolio_artifact(payload))
  self.assertFalse(self.row()["canRun"])


if __name__=="__main__":unittest.main()
