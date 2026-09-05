"""Exercise actual projection and mutation bodies without importing the app."""
import ast
from copy import deepcopy
from pathlib import Path
import sqlite3
import unittest
from unittest.mock import Mock
from typing import Any

SOURCE = Path(__file__).resolve().parents[1] / "live_trader" / "state.py"
TREE = ast.parse(SOURCE.read_text(encoding="utf-8-sig"))


def function(name, namespace):
    node = deepcopy(next(item for item in TREE.body if isinstance(item, ast.FunctionDef) and item.name == name))
    node.decorator_list = []
    namespace.update({"Any": Any, "sqlite3": sqlite3, "Path": Path})
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(SOURCE), "exec"), namespace)
    return namespace[name]


def projection_namespace(calculator):
    namespace = {"live_small_execution_summary": calculator}
    function("_strategy_rollout_context_key", namespace)
    function("_live_deployment_id", namespace)
    function("_canary_execution_for_display", namespace)
    return namespace


def strategy(identifier="current", *, eligible=True, deployment_id="deployment"):
    return {
        "strategy_id": identifier, "live_small_eligible": eligible,
        "artifact_source_path": "artifacts/strategy.json",
        "deployment_id": deployment_id,
    }


def context(deployment_id="deployment"):
    return {
        "deployment_index": {("artifacts", deployment_id): {"deploymentId": deployment_id}},
        "deployment_events": {"artifacts": [{"event": "entry"}]},
        "execution_events": [{"event": "filled"}],
        "order_gate_events": [{"event": "submit"}],
    }


class WorkflowProjectionTests(unittest.TestCase):
    def test_display_uses_exact_durable_calculator_and_current_strategy(self):
        evidence = {"successful": 3, "blocked": 1, "scope": {"eligible": True, "deploymentHash": "current"}}
        calculator = Mock(return_value=evidence)
        project = projection_namespace(calculator)["_canary_execution_for_display"]
        current, shared = strategy(), context()
        result = project(current, evidence_context=shared)
        calculator.assert_called_once_with(
            "current", normalized=current,
            deployment=shared["deployment_index"][("artifacts", "deployment")],
            deployment_events=shared["deployment_events"]["artifacts"],
            execution_events=shared["execution_events"],
            order_gate_events=shared["order_gate_events"],
        )
        self.assertIs(calculator.call_args.kwargs["execution_events"], shared["execution_events"])
        self.assertEqual(result, {**evidence, "verified": True})
        self.assertNotIn("verified", evidence)

    def test_unavailable_scope_malformed_result_or_database_failure_is_unknown(self):
        calculator = Mock()
        project = projection_namespace(calculator)["_canary_execution_for_display"]
        self.assertFalse(project({})["verified"])
        calculator.assert_not_called()
        for evidence in [
            None, [], "invalid",
            {"scope": {"eligible": False}, "successful": 999, "blocked": 0},
            {"scope": {"eligible": True}, "successful": True, "blocked": 0},
            {"scope": {"eligible": True}, "successful": 3, "blocked": -1},
        ]:
            calculator.return_value = evidence
            self.assertFalse(project(strategy(), evidence_context=context())["verified"])
        for failure in (OSError("unavailable"), sqlite3.OperationalError("locked")):
            calculator.side_effect = failure
            self.assertFalse(project(strategy(), evidence_context=context())["verified"])

    def test_missing_context_never_falls_back_to_individual_storage_reads(self):
        calculator = Mock(side_effect=AssertionError("missing context cannot trigger fallback reads"))
        project = projection_namespace(calculator)["_canary_execution_for_display"]
        for shared in (None, [], {}, {**context(), "execution_events": None},
                       {**context(), "deployment_index": {}},
                       {**context(), "deployment_events": {}}):
            self.assertFalse(project(strategy(), evidence_context=shared)["verified"])
        calculator.assert_not_called()

    def test_implicit_deployment_identity_matches_the_existing_canary_calculator(self):
        evidence = {"successful": 1, "blocked": 0, "scope": {"eligible": True}}
        calculator = Mock(return_value=evidence)
        project = projection_namespace(calculator)["_canary_execution_for_display"]
        current = strategy(deployment_id="")
        shared = context("dep:current:standalone:live")
        self.assertTrue(project(current, evidence_context=shared)["verified"])
        self.assertEqual(calculator.call_args.kwargs["deployment"]["deploymentId"], "dep:current:standalone:live")

    def test_snapshot_reads_each_shared_ledger_and_root_once_for_multiple_strategies(self):
        calculator = Mock(return_value={"successful": 3, "blocked": 0, "scope": {"eligible": True}})
        namespace = projection_namespace(calculator)
        deployment_store = Mock()
        deployment_store.list.return_value = [{"deploymentId": "one"}, {"deploymentId": "two"}]
        store_factory = Mock(return_value=deployment_store)
        event_reader = Mock(return_value=[])
        ledger = Mock()
        ledger.execution_event_rows.return_value = [{"event": "filled"}]
        ledger.order_gate_event_rows.return_value = [{"event": "submitted"}]
        namespace.update({
            "DeploymentStore": store_factory,
            "_deployment_event_rows": event_reader,
            "PROGRAM_LEDGER": ledger,
        })
        builder = function("_build_futures_rollout_evidence_context", namespace)
        namespace["_build_futures_rollout_evidence_context"] = Mock(wraps=builder)
        attach = function("_attach_canary_execution_for_display", namespace)
        rows = [strategy("one", deployment_id="one"), strategy("two", deployment_id="two"),
                strategy("paper-only", eligible=False)]
        before = deepcopy(rows)
        attach(rows)
        namespace["_build_futures_rollout_evidence_context"].assert_called_once()
        self.assertEqual(namespace["_build_futures_rollout_evidence_context"].call_args.kwargs, {"soak_report": {}})
        store_factory.assert_called_once_with(Path("artifacts"))
        deployment_store.list.assert_called_once()
        event_reader.assert_called_once_with(Path("artifacts"))
        ledger.execution_event_rows.assert_called_once_with(5000)
        ledger.order_gate_event_rows.assert_called_once_with(5000)
        self.assertEqual(calculator.call_count, 2)
        self.assertTrue(rows[0]["canary_execution"]["verified"])
        self.assertTrue(rows[1]["canary_execution"]["verified"])
        self.assertFalse(rows[2]["canary_execution"]["verified"])
        for old, current in zip(before, rows):
            self.assertEqual(old, {key: value for key, value in current.items() if key != "canary_execution"})

    def test_no_eligible_strategy_skips_reads_and_failed_batch_is_unknown(self):
        calculator = Mock(side_effect=AssertionError("unavailable batch cannot count fills"))
        namespace = projection_namespace(calculator)
        builder = Mock(side_effect=sqlite3.OperationalError("ledger locked"))
        namespace["_build_futures_rollout_evidence_context"] = builder
        attach = function("_attach_canary_execution_for_display", namespace)
        rows = [strategy(eligible=False)]
        attach(rows)
        builder.assert_not_called()
        self.assertFalse(rows[0]["canary_execution"]["verified"])
        rows = [strategy()]
        attach(rows)
        builder.assert_called_once()
        self.assertFalse(rows[0]["canary_execution"]["verified"])
        calculator.assert_not_called()

    def test_automatic_checklist_cannot_write_or_invalidate_preflight(self):
        state = {"checklist": {"api_keys_reviewed": False}, "config_revision": 7, "latest_preflight_snapshot_id": "unchanged"}
        before = deepcopy(state)
        persist = Mock(side_effect=AssertionError("automatic evidence must not be saved as operator input"))
        update = function("set_checklist_item", {
            "CHECKLIST_ITEMS": [{"key": "api_keys_reviewed", "label": "automatic"}],
            "MACHINE_VERIFIABLE_CHECKLIST_KEYS": {"api_keys_reviewed"},
            "STATE": state, "snapshot": lambda: {},
            "persist_operator_checklist_values": persist,
        })
        self.assertFalse(update("api_keys_reviewed", True)["ok"])
        self.assertEqual(before, state)
        persist.assert_not_called()

    def test_manual_checklist_persists_and_invalidates_existing_preflight(self):
        state = {"checklist": {}, "config_revision": 7, "latest_preflight_snapshot_id": "old"}
        persist = Mock()
        update = function("set_checklist_item", {
            "CHECKLIST_ITEMS": [{"key": "operator_takeover_ready", "label": "manual"}],
            "MACHINE_VERIFIABLE_CHECKLIST_KEYS": {"api_keys_reviewed"},
            "STATE": state, "snapshot": lambda: {}, "append_audit": Mock(),
            "persist_operator_checklist_values": persist,
        })
        self.assertTrue(update("operator_takeover_ready", True)["ok"])
        self.assertTrue(state["checklist"]["operator_takeover_ready"])
        self.assertEqual(8, state["config_revision"])
        self.assertEqual("", state["latest_preflight_snapshot_id"])
        persist.assert_called_once()


if __name__ == "__main__":
    unittest.main()