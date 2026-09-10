from __future__ import annotations
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import test_paper_candidate_inbox as fixture
from trading_runtime.artifact_governance import DeploymentStore, seal_strategy_artifact, stable_sha256

spec=importlib.util.spec_from_file_location(fixture.PACKAGE+".paper_candidate_import",fixture.APP_ROOT/"live_trader/paper_candidate_import.py")
service=importlib.util.module_from_spec(spec);sys.modules[spec.name]=service;spec.loader.exec_module(service)


class PaperCandidateImportTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.strategy,self.instance,self.portfolio,self.evidence=fixture.fixture(self.root)
        self.addCleanup(patch.stopall)
        patch.object(fixture.socket,"socket",side_effect=AssertionError("network forbidden")).start()

    def row(self):
        result=fixture.service.list_paper_candidates(roots=[self.root]);self.assertTrue(result["ok"],result)
        return result["candidates"][0]

    def run_import(self,request=None):
        return service.import_paper_candidate(request or self.row()["importRequest"],roots=[self.root])

    def test_exact_candidate_registers_draft_monitor_with_pins_and_no_order_authority(self):
        original={name:raw for name,raw in fixture.file_bytes(self.root).items() if not name.startswith("deployments")}
        request=self.row()["importRequest"]
        result=self.run_import(request);entry=DeploymentStore(self.root).get(result["deploymentId"])
        self.assertTrue(result["ok"]);self.assertFalse(result["authorizationGranted"])
        self.assertEqual(("draft","MONITOR","live-account-unresolved"),(entry["lifecycle"],entry["mode"],entry["accountId"]))
        self.assertEqual(1,entry["revision"])
        for key in ("live_allowed","live_small_eligible","live_eligible"):self.assertIs(entry["permissions"][key],False)
        self.assertEqual(self.evidence["evidenceId"],entry["permissions"]["paperEvidenceId"])
        self.assertEqual(original,{name:raw for name,raw in fixture.file_bytes(self.root).items() if not name.startswith("deployments")})
        before=fixture.file_bytes(self.root)
        again=self.run_import(request);self.assertTrue(again["alreadyRegistered"])
        self.assertEqual(before,fixture.file_bytes(self.root));self.assertTrue(self.row()["registered"])

    def test_portfolio_import_preserves_exact_portfolio_and_child_instance(self):
        self.root=self.root/"portfolio";self.root.mkdir()
        self.strategy,self.instance,self.portfolio,self.evidence=fixture.fixture(self.root,portfolio=True)
        result=self.run_import();entry=DeploymentStore(self.root).get(result["deploymentId"])
        self.assertEqual("portfolio-golden",entry["portfolioArtifact"]["artifactId"])
        self.assertEqual("strategy-instance-golden",entry["permissions"]["paperStrategyInstanceId"])
        self.assertEqual("portfolio-golden",entry["permissions"]["paperPortfolioInstanceId"])

    def test_stale_registry_snapshot_cannot_create_or_overwrite(self):
        request=self.row()["importRequest"]
        DeploymentStore(self.root).create_definition(deployment_id="other",strategy_artifact=self.strategy,portfolio_artifact=None,account_id="test",environment="PAPER",symbol="BTCUSDT")
        before=fixture.file_bytes(self.root)
        with self.assertRaisesRegex(ValueError,"바뀌었거나"):self.run_import(request)
        self.assertEqual(before,fixture.file_bytes(self.root))

    def test_resealed_source_and_changed_request_are_rejected_before_registration(self):
        request=self.row()["importRequest"]
        changed=deepcopy(self.strategy);changed["parameters"]={"shortMa":2,"longMa":6}
        fixture.write(self.root/"strategy.json",seal_strategy_artifact(changed))
        with self.assertRaises(ValueError):self.run_import(request)
        self.assertEqual([],DeploymentStore(self.root).list())

    def test_active_deployment_is_never_replaced_or_rebound(self):
        request=self.row()["importRequest"];store=DeploymentStore(self.root)
        store.create_definition(deployment_id="active",strategy_artifact=self.strategy,portfolio_artifact=None,account_id="configured",environment="SMALL_LIVE",symbol="BTCUSDT")
        store.transition("active",lifecycle="before-live-small",mode="SMALL_LIVE",actor="fixture",reason="existing",permissions={"live_small_eligible":True})
        before=fixture.file_bytes(self.root)
        self.assertFalse(self.row()["canImport"])
        with self.assertRaises(ValueError):self.run_import(request)
        self.assertEqual(before,fixture.file_bytes(self.root))

    def test_existing_unresolved_draft_gets_pins_with_revision_cas(self):
        store=DeploymentStore(self.root)
        store.create_definition(deployment_id="draft",strategy_artifact=self.strategy,portfolio_artifact=None,account_id="live-account-unresolved",environment="SMALL_LIVE",symbol="BTCUSDT")
        definition=list(store.definition_root.glob("*.json"))[0].read_bytes()
        result=self.run_import();self.assertEqual("draft",result["deploymentId"])
        self.assertEqual(2,store.get("draft")["revision"])
        self.assertEqual(definition,list(store.definition_root.glob("*.json"))[0].read_bytes())

    def test_duplicate_published_evidence_is_not_silently_selected(self):
        request=self.row()["importRequest"]
        fixture.write(self.root/"evidence/paper/duplicate.json",self.evidence)
        with self.assertRaisesRegex(ValueError,"중복"):self.run_import(request)
        self.assertEqual([],DeploymentStore(self.root).list())

    def test_arbitrary_paths_missing_identity_and_boolean_revision_fail_closed(self):
        good=self.row()["importRequest"]
        for bad in ({**good,"path":"C:/"},{**good,"rootKey":"unknown"},{**good,"expectedRevision":False},{**good,"identityHash":"unknown"}):
            with self.subTest(bad=list(bad)),self.assertRaises(ValueError):self.run_import(bad)
        self.assertEqual([],DeploymentStore(self.root).list())

    def test_live_catalog_reads_registered_pins_but_does_not_grant_live_permissions(self):
        result=self.run_import()
        contracts=sys.modules[fixture.PACKAGE+".contracts"]
        value=contracts.enrich_strategy_artifact_runtime(self.root,self.root/"strategy.json",self.strategy)
        self.assertEqual(result["deploymentId"],value["_deployment"]["deploymentId"])
        self.assertTrue(value["_paper_live_qualification"]["ready"],value["_paper_live_qualification"].get("issues"))
        self.assertFalse(value["_deployment"]["permissions"]["live_allowed"])


if __name__=="__main__":unittest.main()
