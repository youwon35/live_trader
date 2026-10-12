from __future__ import annotations
import importlib.util, socket, sys, tempfile, types, unittest
from pathlib import Path
from unittest.mock import patch
import test_paper_candidate_inbox as fixture
from trading_runtime.artifact_governance import DeploymentStore

spec=importlib.util.spec_from_file_location("_inactive_import_tool_test",fixture.APP_ROOT/"scripts/import_inactive_paper_candidate.py")
tool=importlib.util.module_from_spec(spec);spec.loader.exec_module(tool)

class InactiveImportToolTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)
        self.strategy,self.instance,self.portfolio,self.evidence=fixture.fixture(self.root)
        self.net=patch.object(socket,"socket",side_effect=AssertionError("network forbidden"));self.net.start();self.addCleanup(self.net.stop)
        self.roots=patch.object(tool.inbox,"configured_artifact_roots",return_value=[self.root]);self.roots.start();self.addCleanup(self.roots.stop)
        identity=fixture.service.list_paper_candidates(roots=[self.root])["candidates"][0]["identity"]
        self.args=types.SimpleNamespace(evidence_id=identity["evidenceId"],strategy_id=identity["strategyArtifactId"],strategy_hash=identity["strategyArtifactHash"],instance_id=identity["strategyInstanceId"],portfolio_id="",portfolio_hash="",root=self.root,apply=False)

    def test_plan_has_no_file_write_and_apply_only_registers_inactive_exact_candidate(self):
        before=fixture.file_bytes(self.root);reply=tool.run(self.args);self.assertEqual("READ_ONLY_IMPORT_PLAN",reply["operation"]);self.assertEqual(before,fixture.file_bytes(self.root))
        self.args.apply=True;reply=tool.run(self.args);self.assertEqual(("draft","MONITOR"),(reply["lifecycle"],reply["mode"]));self.assertFalse(reply["authorizationGranted"])
        for key in ("armingCalls","runtimeStarts","brokerCalls","ordersSubmitted"):self.assertEqual(0,reply[key])
        before=fixture.file_bytes(self.root);reply=tool.run(self.args);self.assertTrue(reply["alreadyRegistered"]);self.assertEqual(before,fixture.file_bytes(self.root))
        entry=DeploymentStore(self.root).get(reply["deploymentId"]);self.assertEqual("live-account-unresolved",entry["accountId"])
        for key in ("live_allowed","live_small_eligible","live_eligible"):self.assertIs(entry["permissions"][key],False)

    def test_wrong_expected_graph_missing_evidence_and_unconfigured_root_never_mutate(self):
        before=fixture.file_bytes(self.root)
        for field,value in (("strategy_id","other"),("strategy_hash","changed"),("instance_id","other"),("portfolio_id","other"),("evidence_id","missing"),("root",self.root/"not-configured")):
            changed=types.SimpleNamespace(**{**vars(self.args),field:value,"apply":True})
            with self.subTest(field=field),self.assertRaises(ValueError):tool.run(changed)
        self.assertEqual(before,fixture.file_bytes(self.root))

    def test_existing_account_bound_or_active_candidate_is_never_replaced(self):
        self.args.apply=True;reply=tool.run(self.args);store=DeploymentStore(self.root);store.transition(reply["deploymentId"],lifecycle="before-live-small",mode="SMALL_LIVE",actor="isolated fixture",reason="existing activation",permissions={"live_allowed":True,"live_small_eligible":True},expected_revision=reply["revision"],expected_registry_hash=fixture.stable_sha256(store.registry_snapshot()))
        before=fixture.file_bytes(self.root)
        with self.assertRaises(ValueError):tool.run(self.args)
        self.assertEqual(before,fixture.file_bytes(self.root))

if __name__=="__main__":unittest.main()
