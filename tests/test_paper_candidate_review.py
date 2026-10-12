from __future__ import annotations
from copy import deepcopy
import importlib.util
import json
import os
import threading
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import test_paper_candidate_inbox as fixture
from trading_runtime.artifact_governance import DeploymentStore, safe_file_token, verify_hash_chain
from trading_runtime import deployment_atomic


def module(name):
    spec = importlib.util.spec_from_file_location(fixture.PACKAGE + "." + name, fixture.APP_ROOT / "live_trader" / (name + ".py"))
    value = importlib.util.module_from_spec(spec); sys.modules[spec.name] = value; spec.loader.exec_module(value)
    return value

importer = module("paper_candidate_import")
reviewer = module("paper_candidate_review")
accounts = module("account_profiles")
confirmation = module("safety_confirmation")

SETTINGS = {"BINANCE_API_KEY": "fixture-never-real-key", "BINANCE_API_SECRET": "fixture-never-real-secret"}
CONTROLS = {"configRevision": 1, "riskPolicyRevision": 1, "mode": "MONITOR", "killSwitch": True, "dryRun": True, "newEntriesBlocked": True}


class PaperCandidateReviewTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.strategy, self.instance, self.portfolio, self.evidence = fixture.fixture(self.root)
        row = self.row()
        importer.import_paper_candidate(row["importRequest"], roots=[self.root])
        self.safety = confirmation.SafetyConfirmationStore()
        self.addCleanup(patch.stopall)
        patch.object(fixture.socket, "socket", side_effect=AssertionError("network forbidden")).start()

    def row(self):
        reply = fixture.service.list_paper_candidates(roots=[self.root])
        self.assertTrue(reply["ok"], reply)
        return reply["candidates"][0]

    def request(self):
        row = self.row(); self.assertTrue(row["canReview"], row)
        return {**row["reviewRequest"], "accountProfileId": accounts.saved_account_profiles(SETTINGS)[0]["profileId"],
                "capitalBasis": "ACCOUNT_AVAILABLE_CASH", "capitalPercent": 100}

    def challenge(self, request):
        result = reviewer.issue_review_confirmation(request, safety_store=self.safety, roots=[self.root], settings=SETTINGS, controls=CONTROLS)
        self.assertTrue(result["ok"], result)
        return {"challengeId": result["challengeId"], "token": result["token"], "typedPhrase": result["expectedPhrase"]}

    def approve(self, request, token, **kwargs):
        return reviewer.approve_paper_candidate(request, token, safety_store=self.safety, roots=[self.root],
            settings=kwargs.get("settings", SETTINGS), controls=kwargs.get("controls", CONTROLS))

    def test_saved_profiles_return_only_masked_identity_and_no_credentials(self):
        settings = {**SETTINGS, "KIS_APP_KEY": "kis-fixture-key", "KIS_APP_SECRET": "kis-fixture-secret", "KIS_ACCOUNT_NO": "12345678", "KIS_ACCOUNT_PRODUCT_CODE": "01", "UPBIT_ACCESS_KEY": "upbit-fixture-key", "UPBIT_SECRET_KEY": "upbit-fixture-secret"}
        reply = accounts.public_saved_accounts(settings)
        text = json.dumps(reply)
        for raw in ("12345678", *[value for key, value in settings.items() if "KEY" in key or "SECRET" in key]):
            self.assertNotIn(raw, text)
        self.assertEqual(3, len(reply["accounts"]))
        self.assertEqual((0, 0), (reply["brokerCalls"], reply["ordersSubmitted"]))
        self.assertEqual("••••5678", reply["accounts"][0]["maskedAccount"])

    def test_explicit_review_creates_immutable_bound_successor_and_keeps_execution_closed(self):
        request = self.request()
        store = DeploymentStore(self.root)
        prior_path = store.definition_root / (safe_file_token(request["deploymentId"], "deployment") + ".json")
        prior_bytes = prior_path.read_bytes()
        result = self.approve(request, self.challenge(request))
        self.assertTrue(result["ok"], result)
        self.assertEqual(("before-live-small", "MONITOR"), (result["lifecycle"], result["mode"]))
        self.assertFalse(result["authorizationGranted"]); self.assertFalse(result["realOrdersArmed"])
        self.assertEqual((0, 0), (result["brokerCalls"], result["ordersSubmitted"]))
        entry = store.get(result["deploymentId"])
        self.assertTrue(entry["permissions"]["live_small_eligible"])
        self.assertFalse(entry["permissions"]["live_allowed"])
        self.assertFalse(entry["permissions"]["live_eligible"])
        self.assertEqual(100, entry["permissions"]["liveCapitalPolicy"]["percent"])
        self.assertFalse(entry["permissions"]["liveCapitalPolicy"]["balanceQueriedAtReview"])
        self.assertTrue(entry["accountId"].startswith("live-account:binance:"))
        self.assertEqual(prior_bytes, prior_path.read_bytes())
        old = store.get(request["deploymentId"])
        self.assertEqual(("retired", "OFF"), (old["lifecycle"], old["mode"]))
        self.assertEqual(result["deploymentId"], old["permissions"]["supersededByDeploymentId"])
        self.assertEqual(result["deploymentId"], self.row()["deployment"]["deploymentId"])
        self.assertFalse(self.row()["canReview"])
        events = [json.loads(line) for line in store.event_path.read_text(encoding="utf-8").splitlines()]
        self.assertTrue(verify_hash_chain(events)[0])

    def test_wrong_phrase_is_one_use_and_has_no_deployment_mutation(self):
        request = self.request(); token = self.challenge(request); token["typedPhrase"] = "WRONG"
        before = fixture.file_bytes(self.root)
        self.assertFalse(self.approve(request, token)["ok"])
        self.assertEqual(before, fixture.file_bytes(self.root))
        token["typedPhrase"] = "LIVE " + accounts.saved_account_profiles(SETTINGS)[0]["maskedAccount"][-4:].upper()
        self.assertFalse(self.approve(request, token)["ok"])

    def test_confirmation_cannot_change_capital_or_control_revision(self):
        request = self.request(); token = self.challenge(request)
        changed = {**request, "capitalPercent": 50}
        before = fixture.file_bytes(self.root)
        self.assertFalse(self.approve(changed, token)["ok"])
        self.assertEqual(before, fixture.file_bytes(self.root))
        token = self.challenge(request)
        self.assertFalse(self.approve(request, token, controls={**CONTROLS, "configRevision": 2})["ok"])
        self.assertEqual(before, fixture.file_bytes(self.root))

    def test_changed_saved_credentials_or_foreign_origin_never_approves(self):
        request = self.request(); token = self.challenge(request)
        for overrides in ({"BINANCE_API_SECRET": "rotated"}, {"BINANCE_BASE_URL": "https://foreign.example"}):
            before = fixture.file_bytes(self.root)
            with self.assertRaisesRegex(ValueError, "저장 계좌"):
                self.approve(request, token, settings={**SETTINGS, **overrides})
            self.assertEqual(before, fixture.file_bytes(self.root))

    def test_stale_registry_revision_blocks_challenge_and_no_rebinding(self):
        request = self.request()
        store = DeploymentStore(self.root); store.get(request["deploymentId"])
        store.transition(request["deploymentId"], lifecycle="draft", mode="MONITOR", actor="other", reason="concurrent update")
        with self.assertRaises(ValueError): self.challenge(request)
        fresh = self.request(); result = self.approve(fresh, self.challenge(fresh))
        before = fixture.file_bytes(self.root)
        with self.assertRaises(ValueError): self.approve(fresh, {})
        self.assertEqual(before, fixture.file_bytes(self.root))
        self.assertEqual(result["deploymentId"], self.row()["deployment"]["deploymentId"])

    def test_instance_or_publication_change_after_challenge_blocks(self):
        request = self.request(); token = self.challenge(request)
        path = self.root / "strategy-instances/instance.json"
        changed = deepcopy(self.instance); changed["parameters"] = {"shortMa": 2, "longMa": 4}
        from trading_runtime.artifact_governance import seal_strategy_instance
        fixture.write(path, seal_strategy_instance(changed))
        with self.assertRaises(ValueError): self.approve(request, token)
        self.assertEqual("draft", DeploymentStore(self.root).get(request["deploymentId"])["lifecycle"])

    def test_crash_after_durable_transaction_recovers_both_sides_together(self):
        request = self.request(); token = self.challenge(request)
        original_recover = deployment_atomic._recover
        def fail_after_journal(path):
            if (Path(path) / ".deployment-transaction.json").exists():
                raise RuntimeError("injected crash after journal")
            return original_recover(path)
        with patch.object(deployment_atomic, "_recover", side_effect=fail_after_journal):
            with self.assertRaises(RuntimeError): self.approve(request, token)
        entries = DeploymentStore(self.root).list()
        self.assertEqual(2, len(entries))
        self.assertEqual({"retired", "before-live-small"}, {row["lifecycle"] for row in entries})
        self.assertFalse((self.root / "deployments/.deployment-transaction.json").exists())
        self.assertFalse(self.row()["canReview"])

    def test_parallel_duplicate_confirmation_has_one_atomic_successor(self):
        request = self.request(); token = self.challenge(request)
        results, errors = [], []
        def submit():
            try: results.append(self.approve(request, token))
            except ValueError as exc: errors.append(type(exc).__name__)
        workers = [threading.Thread(target=submit) for _ in range(2)]
        for worker in workers: worker.start()
        for worker in workers: worker.join(5); self.assertFalse(worker.is_alive())
        self.assertEqual(1, sum(result.get("ok") is True for result in results))
        self.assertEqual(2, len(DeploymentStore(self.root).list()))

    def test_bound_live_consumer_rejects_credential_rotation_after_review(self):
        request = self.request(); result = self.approve(request, self.challenge(request))
        entry = DeploymentStore(self.root).get(result["deploymentId"])
        consumer = sys.modules[fixture.PACKAGE + ".contracts"]
        with patch.dict(os.environ, SETTINGS, clear=True):
            valid = consumer.load_pinned_paper_live_qualification(self.root, self.strategy, entry)
            self.assertTrue(valid["ready"], valid.get("issues"))
            os.environ["BINANCE_API_SECRET"] = "rotated-after-review"
            blocked = consumer.load_pinned_paper_live_qualification(self.root, self.strategy, entry)
            self.assertFalse(blocked["ready"])
            self.assertIn("paper-live-reviewed-account-binding-changed", blocked["issues"])

    def test_portfolio_candidate_uses_the_exact_child_and_portfolio_binding(self):
        self.root = self.root / "portfolio"; self.root.mkdir()
        self.strategy, self.instance, self.portfolio, self.evidence = fixture.fixture(self.root, portfolio=True)
        importer.import_paper_candidate(self.row()["importRequest"], roots=[self.root])
        request = self.request(); result = self.approve(request, self.challenge(request))
        entry = DeploymentStore(self.root).get(result["deploymentId"])
        self.assertEqual("portfolio-golden", entry["portfolioArtifact"]["artifactId"])
        self.assertEqual("strategy-instance-golden", entry["permissions"]["paperStrategyInstanceId"])

    def test_malformed_request_and_missing_confirmation_cannot_prepare(self):
        request = self.request(); before = fixture.file_bytes(self.root)
        for changes in ({"capitalPercent": True}, {"capitalPercent": 0}, {"capitalPercent": 101}, {"capitalBasis": "ALL_EQUITY"}, {"expectedRevision": True}):
            with self.subTest(changes=changes), self.assertRaises(ValueError): self.challenge({**request, **changes})
        self.assertFalse(self.approve(request, None)["ok"])
        self.assertEqual(before, fixture.file_bytes(self.root))


if __name__ == "__main__": unittest.main()
