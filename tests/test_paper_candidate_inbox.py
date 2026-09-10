from __future__ import annotations

import ast
from contextlib import ExitStack
from copy import deepcopy
import importlib.util
import json
import os
from pathlib import Path
import socket
import sqlite3
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch
from urllib.parse import urlparse

APP_ROOT = Path(__file__).resolve().parents[1]
SYSTEM_ROOT = APP_ROOT.parents[1]
sys.path.insert(0, str(SYSTEM_ROOT / "packages" / "trading_runtime"))
# Import the pure service without real app __init__ / user .env migration.
PACKAGE = "_paper_candidate_inbox_isolated"
package = types.ModuleType(PACKAGE)
package.__path__ = [str(APP_ROOT / "live_trader")]
sys.modules[PACKAGE] = package
spec = importlib.util.spec_from_file_location(PACKAGE + ".paper_candidate_inbox", APP_ROOT / "live_trader" / "paper_candidate_inbox.py")
service = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = service
spec.loader.exec_module(service)
gold_spec = importlib.util.spec_from_file_location("_paper_inbox_gold", SYSTEM_ROOT / "packages" / "trading_runtime" / "tests" / "test_paper_live_contract.py")
gold = importlib.util.module_from_spec(gold_spec)
gold_spec.loader.exec_module(gold)
# The production portfolio runtime uses the artifact id as portfolioInstanceId.
fixture_ast = ast.parse(Path(gold_spec.origin).read_text(encoding="utf-8"))
fixture_fn = next(node for node in fixture_ast.body if isinstance(node, ast.FunctionDef) and node.name == "golden_paper_live_evidence")
for node in ast.walk(fixture_fn):
    if isinstance(node, ast.Constant) and node.value == "portfolio-instance-golden":
        node.value = "portfolio-golden"
exec(compile(ast.Module(body=[fixture_fn], type_ignores=[]), "<production-portfolio-fixture>", "exec"), gold.__dict__)
from trading_runtime.artifact_governance import (
    DeploymentStore, EvidenceStore, artifact_reference, assert_verified_strategy_instance,
    seal_strategy_artifact, seal_strategy_instance, seal_portfolio_artifact,
    stable_sha256, safe_file_token,
)
from trading_runtime.paper_live_contract import validate_paper_live_evidence


def write(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def fixture(root, *, portfolio=False):
    strategy = seal_strategy_artifact({
        "id": "strategy-golden", "schemaVersion": "strategy-artifact-v1", "artifactType": "strategy",
        "name": "Golden current strategy", "plugin": "moving_average_cross", "symbol": "BTCUSDT",
        "asset": "CRYPTO", "timeframe": "1m", "parameters": {"shortMa": 3, "longMa": 5},
    })
    ref = artifact_reference(strategy)
    instance = seal_strategy_instance({
        "instanceId": "template-instance-golden" if portfolio else "strategy-instance-golden",
        "schemaVersion": "strategy-instance-v1", "sourceStrategyId": "strategy-golden",
        "sourceArtifactHash": ref["artifactHash"], "symbol": "BTCUSDT", "timeframe": "1m",
        "pluginId": "moving_average_cross", "parameters": strategy["parameters"],
        "marketDataProvider": "binance", "brokerId": "binance",
    })
    instance_hash = assert_verified_strategy_instance(instance)
    embedded = {**instance, "instanceId": "strategy-instance-golden", "templateInstanceId": instance["instanceId"], "sourceInstanceHash": instance_hash}
    portfolio_payload = seal_portfolio_artifact({
        "id": "portfolio-golden", "schemaVersion": "portfolio-artifact-v1", "artifactType": "portfolio", "strategyInstances": [embedded],
    }) if portfolio else None
    replacements = {"strategy": ref["artifactHash"], "strategy-content": ref["contentHash"]}
    if portfolio:
        portfolio_ref = artifact_reference(portfolio_payload)
        replacements.update({"portfolio": portfolio_ref["artifactHash"], "portfolio-content": portfolio_ref["contentHash"], "forward-evidence-portfolio": portfolio_ref["artifactHash"]})
    else:
        replacements["forward-evidence-portfolio"] = service._standalone_scope_hash(strategy, instance, instance_hash)
    real_sha = gold._sha
    with patch.object(gold, "_sha", side_effect=lambda label: replacements.get(label) or real_sha(label)):
        evidence = gold.golden_paper_live_evidence(portfolio=portfolio)
    assert validate_paper_live_evidence(evidence).valid
    write(root / "strategy.json", strategy)
    write(root / "strategy-instances" / "instance.json", instance)
    if portfolio:
        write(root / "portfolios" / "portfolio.json", portfolio_payload)
    EvidenceStore(root).save_paper(evidence)
    return strategy, instance, portfolio_payload, evidence


def file_bytes(root):
    return {str(path.relative_to(root)): path.read_bytes() for path in root.rglob("*") if path.is_file()}


class PaperCandidateInboxTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "artifacts"
        self.root.mkdir()
        self.strategy, self.instance, self.portfolio, self.evidence = fixture(self.root)
        self.network = patch.object(socket, "socket", side_effect=AssertionError("network forbidden"))
        self.network.start()
        self.addCleanup(self.network.stop)

    def inbox(self):
        return service.list_paper_candidates(roots=[self.root])

    def row(self):
        result = self.inbox()
        self.assertTrue(result["ok"], result)
        return result["candidates"][0]

    def test_exact_standalone_evidence_is_verified_without_any_write_or_runtime_call(self):
        before = file_bytes(self.root)
        with ExitStack() as stack:
            for owner, name in [(Path, "write_text"), (Path, "write_bytes"), (Path, "mkdir"), (os, "open"), (os, "replace"), (sqlite3, "connect"), (DeploymentStore, "transition"), (DeploymentStore, "create_definition"), (EvidenceStore, "save_paper")]:
                stack.enter_context(patch.object(owner, name, side_effect=AssertionError("mutation/runtime forbidden")))
            result = self.inbox()
        self.assertTrue(result["ok"], result)
        row = result["candidates"][0]
        self.assertEqual(row["status"], "VERIFIED_READ_ONLY", row)
        self.assertEqual(row["instanceHash"], assert_verified_strategy_instance(self.instance))
        self.assertEqual(row["deployment"]["mode"], "UNREGISTERED")
        self.assertIs(result["readOnly"], True)
        self.assertIs(result["canImport"], True)
        self.assertIs(result["authorizationGranted"], False)
        self.assertIs(row["canImport"], True)
        self.assertNotIn("request", row)
        self.assertIn("주문 실행을 승인하지 않습니다", result["requiredNextStep"])
        self.assertEqual(row["importRequest"]["expectedRevision"], 0)
        self.assertEqual(row["importRequest"]["registryHash"], stable_sha256(service._registry(self.root)))
        self.assertEqual(file_bytes(self.root), before)

    def test_standalone_wire_hash_matches_paper_producer_without_runtime_dependency(self):
        source = ast.parse((SYSTEM_ROOT / "apps" / "paper_trader" / "desktop" / "paper_governance.py").read_text(encoding="utf-8-sig"))
        fn = next(node for node in source.body if isinstance(node, ast.FunctionDef) and node.name == "standalone_runtime_scope_identity")
        namespace = {"stable_sha256": stable_sha256}
        exec(compile(ast.Module(body=[fn], type_ignores=[]), "<paper-v1-producer-parity>", "exec"), namespace)
        _, expected = namespace[fn.name](strategy_id="strategy-golden", strategy_instance_id="strategy-instance-golden", symbol="BTCUSDT", provider="binance", inferred_provider="binance", broker_id="binance", timeframe="1m", artifact_hash=artifact_reference(self.strategy)["artifactHash"], instance_content_hash=assert_verified_strategy_instance(self.instance))
        self.assertEqual(service._standalone_scope_hash(self.strategy, self.instance, assert_verified_strategy_instance(self.instance)), expected)

    def test_same_id_resealed_instance_parameters_provider_symbol_and_schedule_are_blocked(self):
        for field, value in [("parameters", {"shortMa": 2}), ("marketDataProvider", "other"), ("symbol", "ETHUSDT"), ("timeframe", "5m"), ("schedule", {"hour": 10})]:
            with self.subTest(field=field):
                changed = deepcopy(self.instance)
                changed[field] = value
                write(self.root / "strategy-instances" / "instance.json", seal_strategy_instance(changed))
                self.assertEqual(self.row()["status"], "BLOCKED")
        write(self.root / "strategy-instances" / "instance.json", self.instance)
        self.assertEqual(self.row()["status"], "VERIFIED_READ_ONLY")

    def test_portfolio_template_link_is_exact_and_resealed_template_is_blocked(self):
        self.root = self.root / "portfolio"
        self.root.mkdir()
        self.strategy, self.instance, self.portfolio, self.evidence = fixture(self.root, portfolio=True)
        self.assertEqual(self.row()["status"], "VERIFIED_READ_ONLY", self.row())
        changed = deepcopy(self.instance)
        changed["schedule"] = {"hour": 10}
        write(self.root / "strategy-instances" / "instance.json", seal_strategy_instance(changed))
        row = self.row()
        self.assertEqual(row["status"], "BLOCKED")
        self.assertIn("sourceInstanceHash", row["detail"])

    def test_artifact_reseal_and_duplicate_instance_never_claim_exact_match(self):
        write(self.root / "strategy-instances" / "duplicate.json", self.instance)
        self.assertEqual(self.row()["status"], "BLOCKED")
        (self.root / "strategy-instances" / "duplicate.json").unlink()
        changed = deepcopy(self.strategy)
        changed["parameters"] = {"shortMa": 2}
        write(self.root / "strategy.json", seal_strategy_artifact(changed))
        self.assertEqual(self.row()["status"], "BLOCKED")

    def test_modified_manifest_binding_envelope_or_identity_is_blocked(self):
        path = self.root / "evidence" / "paper" / f"{safe_file_token(self.evidence['evidenceId'])}.json"
        for field in ("evidenceId", "deploymentId", "strategyArtifact", "result", "metrics", "evidenceBundle"):
            with self.subTest(field=field):
                changed = deepcopy(self.evidence)
                changed[field] = "modified"
                write(path, changed)
                self.assertEqual(self.row()["status"], "BLOCKED")
        write(path, self.evidence)

    def test_malformed_duplicate_keys_and_oversized_json_are_bounded(self):
        path = self.root / "evidence" / "paper" / "broken.json"
        for raw in ("{", '{"evidenceId":"x","evidenceId":"y"}', "x" * (service.MAX_BYTES + 1)):
            path.write_text(raw, encoding="utf-8")
            result = self.inbox()
            self.assertTrue(any(row["status"] == "BLOCKED" for row in result["candidates"]))
        with patch.object(service, "MAX_FILES", 0):
            self.assertFalse(self.inbox()["ok"])

    def test_stale_registry_is_reported_without_changing_any_file(self):
        before = file_bytes(self.root)
        original = service._registry(self.root)
        changed = deepcopy(original)
        changed["updatedAt"] = "new revision"
        with patch.object(service, "_registry", side_effect=[original, changed]):
            result = self.inbox()
        self.assertFalse(result["ok"])
        self.assertEqual(result["candidates"], [])
        self.assertEqual(file_bytes(self.root), before)

    def test_current_deployment_revision_is_read_without_permission_or_mode_change(self):
        store = DeploymentStore(self.root)
        store.create_definition(deployment_id="existing", strategy_artifact=self.strategy, portfolio_artifact=None, environment="SMALL_LIVE", account_id="test", symbol="BTCUSDT")
        before = file_bytes(self.root)
        row = self.row()
        self.assertEqual(row["deployment"]["revision"], 1)
        self.assertEqual(row["deployment"]["mode"], "MONITOR")
        self.assertEqual(file_bytes(self.root), before)

    def test_bound_account_blocks_import_with_reason_without_exposing_account_or_mutating(self):
        private_account = "fixture-private-account-do-not-expose"
        DeploymentStore(self.root).create_definition(deployment_id="bound", strategy_artifact=self.strategy,
            portfolio_artifact=None, environment="SMALL_LIVE", account_id=private_account, symbol="BTCUSDT")
        before = file_bytes(self.root)
        result = self.inbox()
        row = result["candidates"][0]
        self.assertEqual(row["status"], "VERIFIED_READ_ONLY")
        self.assertFalse(row["canImport"])
        self.assertFalse(row["registered"])
        self.assertEqual([reason["code"] for reason in row["blockedReasons"]], ["DEPLOYMENT_ACCOUNT_ALREADY_BOUND"])
        self.assertNotIn(private_account, json.dumps(result))
        self.assertNotIn("importRequest", row)
        self.assertIs(result["authorizationGranted"], False)
        self.assertEqual(file_bytes(self.root), before)

    def test_registration_reasons_preserve_existing_verdict_and_registered_precedence(self):
        DeploymentStore(self.root).create_definition(deployment_id="draft", strategy_artifact=self.strategy,
            portfolio_artifact=None, environment="SMALL_LIVE", account_id="live-account-unresolved", symbol="BTCUSDT")
        registry_path = self.root / "deployments" / "deployment-registry.json"
        base = service._registry(self.root)
        cases = [
            ({}, []),
            ({"lifecycle": "before-live-small"}, ["DEPLOYMENT_NOT_DRAFT"]),
            ({"mode": "SMALL_LIVE"}, ["DEPLOYMENT_MODE_NOT_REVIEWABLE"]),
            ({"permissions": {"live_allowed": True}}, ["DEPLOYMENT_LIVE_PERMISSION_PRESENT"]),
            ({"permissions": {"live_allowed": "true"}}, []),
            ({"lifecycle": "before-live-small", "mode": "SMALL_LIVE", "permissions": {"live_small_eligible": True}},
             ["DEPLOYMENT_NOT_DRAFT", "DEPLOYMENT_MODE_NOT_REVIEWABLE", "DEPLOYMENT_LIVE_PERMISSION_PRESENT"]),
        ]
        for changes, expected in cases:
            with self.subTest(changes=changes):
                registry = deepcopy(base)
                registry["entries"]["draft"].update(changes)
                write(registry_path, registry)
                before = file_bytes(self.root)
                row = self.row()
                self.assertEqual([reason["code"] for reason in row["blockedReasons"]], expected)
                self.assertEqual(row["canImport"], not expected)
                self.assertEqual("importRequest" in row, not expected)
                self.assertFalse(row["registered"])
                self.assertEqual(file_bytes(self.root), before)
        write(registry_path, base)
        pins = service.candidate_pins(self.row()["identity"])
        registered = deepcopy(base)
        registered["entries"]["draft"].update({"lifecycle": "before-live-small", "mode": "SMALL_LIVE", "permissions": {**pins, "live_allowed": True}})
        write(registry_path, registered)
        before = file_bytes(self.root)
        row = self.row()
        self.assertTrue(row["registered"])
        self.assertFalse(row["canImport"])
        self.assertEqual(row["blockedReasons"], [])
        self.assertNotIn("importRequest", row)
        self.assertEqual(file_bytes(self.root), before)

    def test_root_resolution_respects_appdata_and_returns_structured_failure(self):
        with patch.dict(service.os.environ, {"APPDATA": str(self.root)}, clear=True), patch.object(service, "artifact_read_roots", return_value=[]), patch.object(Path, "home", side_effect=RuntimeError("home unavailable")):
            self.assertEqual(service.configured_artifact_roots(), [(self.root / "trading_programs" / "strategies").resolve()])
        with patch.object(service, "configured_artifact_roots", side_effect=RuntimeError("root unavailable")):
            self.assertFalse(service.list_paper_candidates()["ok"])

    def test_get_route_authenticates_and_import_has_separate_authenticated_route(self):
        source = (APP_ROOT / "live_trader" / "server.py").read_text(encoding="utf-8-sig")
        tree = ast.parse(source)
        method = next(node for cls in tree.body if isinstance(cls, ast.ClassDef) for node in cls.body if isinstance(node, ast.FunctionDef) and node.name == "do_GET")
        namespace = {"urlparse": urlparse, "_FUNCTIONAL_STATUS_PATHS": {"/api/paper-candidates"}, "_FUNCTIONAL_BOOTSTRAP_PATH": "/__lt_native_bootstrap", "state": types.SimpleNamespace(paper_candidate_evidence_inbox=Mock(return_value={"ok": True}), snapshot=Mock(side_effect=AssertionError("runtime snapshot forbidden")))}
        cls = ast.ClassDef(name="Harness", bases=[], keywords=[], body=[method], decorator_list=[])
        ast.fix_missing_locations(cls)
        exec(compile(ast.Module(body=[cls], type_ignores=[]), "<isolated-http>", "exec"), namespace)
        handler = namespace["Harness"]()
        handler._authorize_functional_http = Mock(return_value=True)
        handler.send_json = Mock()
        handler.path = "/api/paper-candidates"
        handler.do_GET()
        handler._authorize_functional_http.assert_called_with(require_origin=False)
        namespace["state"].snapshot.assert_not_called()
        self.assertIn('/api/paper-candidates/import', source)
        self.assertIn('import_paper_candidate_metadata', (APP_ROOT / "live_trader" / "state.py").read_text(encoding="utf-8-sig"))
        self.assertFalse(hasattr(service, "import_paper_candidate"))


    def test_import_post_denies_before_reading_body_and_only_routes_authorized_metadata(self):
        source = (APP_ROOT / "live_trader" / "server.py").read_text(encoding="utf-8-sig")
        tree = ast.parse(source)
        method = next(node for cls in tree.body if isinstance(cls, ast.ClassDef) for node in cls.body if isinstance(node, ast.FunctionDef) and node.name == "do_POST")
        mutation_paths = next(node.value for node in tree.body if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == "_FUNCTIONAL_MUTATION_PATHS" for target in node.targets))
        paths = eval(compile(ast.Expression(mutation_paths), "<paths>", "eval"), {"frozenset": frozenset})
        metadata = Mock(return_value={"ok": True, "authorizationGranted": False})
        namespace = {"urlparse": urlparse, "_FUNCTIONAL_STATUS_PATHS": set(), "_FUNCTIONAL_MUTATION_PATHS": paths, "state": types.SimpleNamespace(import_paper_candidate_metadata=metadata)}
        cls = ast.ClassDef(name="Harness", bases=[], keywords=[], body=[method], decorator_list=[])
        ast.fix_missing_locations(cls)
        exec(compile(ast.Module(body=[cls], type_ignores=[]), "<isolated-import-http>", "exec"), namespace)
        handler = namespace["Harness"]()
        handler.path = "/api/paper-candidates/import"
        handler._authorize_functional_http = Mock(return_value=False)
        handler.read_json = Mock(return_value={"evidenceId": "fixture"})
        handler.send_json = Mock()
        handler._send_functional_http_denial = Mock()
        handler.do_POST()
        handler._authorize_functional_http.assert_called_once_with(require_origin=True)
        handler.read_json.assert_not_called()
        metadata.assert_not_called()
        handler._authorize_functional_http.return_value = True
        handler.do_POST()
        metadata.assert_called_once_with({"evidenceId": "fixture"})
        handler.send_json.assert_called_once_with({"ok": True, "authorizationGranted": False})
        handler.path = "http://127.0.0.1/api/paper-candidates/import"
        handler.read_json.reset_mock()
        handler.do_POST()
        handler._send_functional_http_denial.assert_called_once()
        handler.read_json.assert_not_called()

if __name__ == "__main__":
    unittest.main()
