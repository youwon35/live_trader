"""Exercise the product projection against the real, isolated dispatch guards."""
from __future__ import annotations

import ast
from copy import deepcopy
import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest

APP_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = APP_ROOT / "live_trader" / "execution_availability.py"
spec = importlib.util.spec_from_file_location("_execution_availability_isolated", MODULE_PATH)
availability = importlib.util.module_from_spec(spec)
spec.loader.exec_module(availability)


def isolated_dispatch_guard(name: str, *, lock_held: bool):
    """Compile the real pre-dispatch prefix, with no app import or side effects."""
    tree = ast.parse((APP_ROOT / "live_trader" / "state.py").read_text(encoding="utf-8-sig"))
    function = deepcopy(next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == name))
    guard_index = next(
        index for index, node in enumerate(function.body)
        if isinstance(node, ast.If)
        and any(isinstance(child, ast.Call) and isinstance(child.func, ast.Name)
                and child.func.id == "_runtime_mode_lock_owned" for child in ast.walk(node.test))
    )
    function.decorator_list = []
    function.body = [
        *function.body[:guard_index + 1],
        ast.Return(value=ast.Constant(value=None)),
    ]
    module = ast.Module(
        body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), function],
        type_ignores=[],
    )
    ast.fix_missing_locations(module)
    namespace = {
        "_runtime_mode_lock_owned": lambda: lock_held,
        "broker_id_from_symbol": lambda *_args: "fallback",
        "_block_managed_order_before_dispatch": lambda _order, _managed, reason: {"reason": reason},
    }
    exec(compile(module, "<isolated-real-dispatch-guard>", "exec"), namespace)
    return namespace[name]


class ExecutionAvailabilityTests(unittest.TestCase):
    def test_projection_is_non_authorizing_and_caller_cannot_change_future_results(self):
        first = availability.ordinary_execution_availability()
        self.assertFalse(first["authorizationGranted"])
        self.assertEqual(first["schemaVersion"], "live-execution-availability-v1")
        route = first["ordinaryContinuous"]
        self.assertTrue(route["monitorSupported"])
        self.assertFalse(route["liveDispatchAvailable"])
        self.assertEqual(route["blockedModes"], ["SMALL_LIVE", "FULL_LIVE"])
        route["blockedModes"].clear()
        route["liveDispatchAvailable"] = True
        second = availability.ordinary_execution_availability()
        self.assertEqual(second["ordinaryContinuous"]["blockedModes"], ["SMALL_LIVE", "FULL_LIVE"])
        self.assertFalse(second["ordinaryContinuous"]["liveDispatchAvailable"])

    def test_reported_hold_matches_both_actual_final_dispatch_guards(self):
        reason = availability.ordinary_execution_availability()["ordinaryContinuous"]["reasonCode"]
        for lock_held in (True, False):
            submit = isolated_dispatch_guard("submit_order_intent", lock_held=lock_held)
            dispatch = isolated_dispatch_guard("dispatch_live_order_with_checkpoint", lock_held=lock_held)
            for broker in ("kis", "binance", "binance-futures", "upbit"):
                for mode in ("SMALL_LIVE", "FULL_LIVE"):
                    for dry_run in (True, False):
                        with self.subTest(lock_held=lock_held, broker=broker, mode=mode, dry_run=dry_run):
                            intent = SimpleNamespace(metadata={"broker_id": broker}, symbol="EXAMPLE", asset="TEST", mode=mode)
                            first = submit({}, intent, dry_run=dry_run, audit_event="isolated")
                            second = dispatch({"dry_run": dry_run}, intent, None, trace_id="isolated")
                            if lock_held and not dry_run:
                                self.assertEqual(first["reason"], reason + ":" + broker)
                                self.assertTrue(first["runtimeDispatchDisabled"])
                                self.assertEqual(second["reason"], reason + ":" + broker)
                            else:
                                self.assertIsNone(first)
                                self.assertIsNone(second)

    def test_controller_still_holds_the_lock_at_both_order_paths(self):
        tree = ast.parse((APP_ROOT / "live_trader" / "continuous_live.py").read_text(encoding="utf-8-sig"))
        controller = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "LiveContinuousController")
        methods = {node.name: node for node in controller.body if isinstance(node, ast.FunctionDef)}
        self.assertTrue(any(
            isinstance(node, ast.keyword) and node.arg == "operation_lock"
            and ast.unparse(node.value) == "state.RUNTIME_MODE_LOCK"
            for node in ast.walk(methods["start"])
        ))
        cycle = methods["_handle_cycle"]
        self.assertTrue(any(
            isinstance(node, ast.With)
            and any(ast.unparse(item.context_expr) == "state.RUNTIME_MODE_LOCK" for item in node.items)
            and any(isinstance(child, ast.Call) and ast.unparse(child.func) == "self._handle_cycle_locked" for child in ast.walk(node))
            for node in ast.walk(cycle)
        ))
        for name in ("_handle_cycle_locked", "_handle_portfolio_cycle_locked"):
            self.assertTrue(any(
                isinstance(node, ast.Call) and ast.unparse(node.func) == "state.submit_order_intent"
                for node in ast.walk(methods[name])
            ), name)

    def test_projection_has_no_app_or_environment_dependencies(self):
        tree = ast.parse(MODULE_PATH.read_text(encoding="utf-8-sig"))
        self.assertFalse(any(isinstance(node, (ast.Import, ast.ImportFrom)) for node in ast.walk(tree)))
        self.assertFalse(any(isinstance(node, ast.Call) for node in ast.walk(tree)))


if __name__ == "__main__":
    unittest.main()