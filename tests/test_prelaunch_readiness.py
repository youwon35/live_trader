from __future__ import annotations

from contextlib import ExitStack
import importlib.util
import json
import os
from pathlib import Path
import socket
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

APP = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP / "tests"))
spec = importlib.util.spec_from_file_location("_prelaunch_readiness_test", APP / "tools" / "prelaunch_readiness.py")
service = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = service
spec.loader.exec_module(service)
inbox, profiles, _ = service.dependencies()
from test_paper_candidate_inbox import fixture, file_bytes
from trading_runtime.artifact_governance import DeploymentStore

SETTINGS = {"BINANCE_API_KEY": "SYNTHETIC_PRIVATE_KEY_SENTINEL", "BINANCE_API_SECRET": "SYNTHETIC_PRIVATE_SECRET_SENTINEL"}


class PrelaunchReadinessTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.root = self.base / "artifacts"
        self.root.mkdir()
        self.runtime = self.base / "runtime"
        strategy, _, portfolio, _ = fixture(self.root)
        DeploymentStore(self.root).create_definition(deployment_id="prelaunch-test-live",
            strategy_artifact=strategy, portfolio_artifact=portfolio,
            account_id="live-account-unresolved", environment="SMALL_LIVE", symbol="BTCUSDT", route="paper-candidate")

    def report(self, settings=None):
        return service.build_report(self.root, self.runtime, settings or {}, inbox=inbox, profiles=profiles)

    def test_real_sealed_candidate_checks_have_no_network_or_file_mutations(self):
        before = file_bytes(self.base)
        modules_before = set(sys.modules)
        with ExitStack() as stack:
            for owner, name in ((socket, "socket"), (Path, "write_text"), (Path, "write_bytes"), (Path, "mkdir"), (os, "replace")):
                stack.enter_context(patch.object(owner, name, side_effect=AssertionError("side effect forbidden")))
            report = self.report(SETTINGS)
        self.assertFalse(report["errors"], report)
        self.assertEqual(report["candidates"][0]["status"], "SEALED_CURRENT_SOURCE_VERIFIED")
        self.assertTrue(report["deployments"][0]["definitionAndArtifactValid"])
        self.assertEqual(file_bytes(self.base), before)
        self.assertEqual(set(report["sideEffects"].values()), {0})
        new_modules = set(sys.modules) - modules_before
        self.assertNotIn("live_trader.state", new_modules)
        self.assertNotIn("live_trader.server", new_modules)
        self.assertNotIn("live_trader.env_loader", new_modules)

    def test_credentials_presence_never_claims_authentication_or_market_readiness(self):
        report = self.report(SETTINGS)
        markets = {row["market"]: row for row in report["markets"]}
        self.assertTrue(markets["BINANCE_SPOT"]["savedAccountProfilePresent"])
        self.assertFalse(markets["UPBIT_SPOT"]["savedAccountProfilePresent"])
        self.assertTrue(all(row["status"] == "NOT_READY" for row in markets.values()))
        self.assertTrue(all(row["currentBrokerEvidence"] == "NOT_QUERIED" for row in markets.values()))
        self.assertIn("NATIVE_EQUITY_PRODUCTION_CONTRACT_INCOMPLETE", markets["KIS_KR_STOCK_ETF"]["blockers"])
        self.assertIn("UNSUPPORTED", markets["KIS_US_STOCK_ETF"]["staticCodeCapability"])
        self.assertFalse(report["liveReady"])
        self.assertFalse(report["authorizationGranted"])
        rendered = json.dumps(report)
        for secret in SETTINGS.values():
            self.assertNotIn(secret, rendered)

    def test_missing_credentials_require_saved_profile(self):
        report = self.report()
        for row in report["markets"]:
            self.assertIn("SAVED_CREDENTIAL_PROFILE_MISSING_OR_UNREADABLE", row["blockers"])
        deployment = report["deployments"][0]
        self.assertFalse(deployment["accountReviewedAgainstSavedConfiguration"])
        self.assertFalse(deployment["capitalPolicyPresent"])
        self.assertEqual(deployment["ownedCapital"], "NOT_INITIALIZED")

    def test_corrupt_registry_fails_closed_and_discards_partial_inventory(self):
        (self.root / "deployments" / "deployment-registry.json").write_text('{"entries":', encoding="utf-8")
        report = self.report(SETTINGS)
        self.assertIn("CANONICAL_REGISTRY_OR_CATALOG_INVALID", report["errors"])
        self.assertFalse(report["deployments"])
        self.assertFalse(report["candidates"])
        self.assertIsNone(report["registryHash"])

    def test_missing_registry_is_not_an_empty_success(self):
        report = service.build_report(self.base / "missing", self.runtime, SETTINGS, inbox=inbox, profiles=profiles)
        self.assertIn("CANONICAL_REGISTRY_MISSING", report["errors"])
        self.assertFalse(report["deployments"])

    def test_changed_artifact_does_not_retain_valid_status(self):
        path = self.root / "strategy.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["symbol"] = "ETHUSDT"
        path.write_text(json.dumps(payload), encoding="utf-8")
        report = self.report(SETTINGS)
        self.assertFalse(report["deployments"][0]["definitionAndArtifactValid"])
        self.assertEqual(report["candidates"][0]["status"], "BLOCKED_INVALID_OR_STALE_EVIDENCE")

    def test_settings_use_read_only_secret_reader_and_do_not_mutate_environment(self):
        env = self.base / ".env"
        env.write_text('BINANCE_API_KEY="from-file#literal" # comment\nBINANCE_API_SECRET=\n', encoding="utf-8")
        class Store:
            def get(self, key):
                return "from-protected-store" if key == "live_trader.BINANCE_API_SECRET" else ""
            def set(self, *args):
                raise AssertionError("secret writes forbidden")
        before = file_bytes(self.base)
        with patch.dict(os.environ, {}, clear=True):
            loaded, status = service.read_settings(env, environ={"BINANCE_API_KEY": "os-priority"}, secret_store=Store())
            self.assertEqual(dict(os.environ), {})
        self.assertEqual(status, "READ")
        self.assertEqual(loaded["BINANCE_API_KEY"], "os-priority")
        self.assertEqual(loaded["BINANCE_API_SECRET"], "from-protected-store")
        self.assertEqual(file_bytes(self.base), before)

    def test_unreadable_secret_is_missing_not_a_valid_profile(self):
        class Store:
            def get(self, key): return ""
        settings, _ = service.read_settings(self.base / "missing.env", environ={}, secret_store=Store())
        self.assertFalse(profiles.saved_account_profiles(settings))

    def _principal(self):
        account = profiles.saved_account_profiles(SETTINGS)[0]
        deployment = {"deploymentId": "principal-test", "permissions": {"liveReviewBindingHash": "review-test"}}
        scope = {"schemaVersion": "live-owned-capital-v1", "deploymentId": "principal-test",
            "accountProfileId": account["profileId"], "accountFingerprint": account["accountFingerprint"],
            "credentialConfigurationHash": account["credentialConfigurationHash"], "reviewBindingHash": "review-test"}
        scope["scopeHash"] = inbox.stable_sha256(scope)
        body = {"scope": scope, "principal": "10", "occurredAt": "2026-10-15T00:00:00+00:00"}
        path = self.base / "principal.sqlite3"
        db = sqlite3.connect(path)
        try:
            db.execute("CREATE TABLE principals (deployment_id TEXT PRIMARY KEY, scope_json TEXT, principal TEXT, occurred_at TEXT, content_hash TEXT)")
            db.execute("INSERT INTO principals VALUES (?,?,?,?,?)", ("principal-test", json.dumps(scope), "10", body["occurredAt"], inbox.stable_sha256(body)))
            db.commit()
        finally:
            db.close()
        return path, deployment, account

    def test_positive_principal_metadata_is_bound_but_never_claims_current_equity(self):
        path, deployment, account = self._principal()
        before = file_bytes(self.base)
        self.assertEqual(service._capital_status(path, deployment, account, inbox.stable_sha256),
                         "STORED_ACCOUNT_BINDING_VERIFIED_CURRENT_EQUITY_UNOBSERVED")
        self.assertEqual(file_bytes(self.base), before)
        # Successful deletion on Windows also proves no SQLite handle remains.
        path.unlink()

    def test_corrupt_principal_and_changed_saved_account_fail_closed(self):
        path, deployment, account = self._principal()
        changed = {**account, "credentialConfigurationHash": "changed"}
        self.assertEqual(service._capital_status(path, deployment, changed, inbox.stable_sha256), "INVALID_OR_CHANGED_SCOPE")
        db = sqlite3.connect(path)
        try:
            db.execute("UPDATE principals SET content_hash='corrupt'")
            db.commit()
        finally:
            db.close()
        self.assertEqual(service._capital_status(path, deployment, account, inbox.stable_sha256), "INVALID_OR_CHANGED_SCOPE")

    def test_active_wal_is_unverified_without_journal_or_shm_creation(self):
        path, deployment, account = self._principal()
        path.with_name(path.name + "-wal").write_bytes(b"synthetic active WAL")
        before = file_bytes(self.base)
        self.assertEqual(service._capital_status(path, deployment, account, inbox.stable_sha256), "UNVERIFIED_ACTIVE_WAL")
        self.assertEqual(file_bytes(self.base), before)


if __name__ == "__main__":
    unittest.main()
