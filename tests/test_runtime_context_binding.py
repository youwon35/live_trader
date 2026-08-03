from __future__ import annotations

import copy
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from live_trader import state
from live_trader.continuous_live import LiveContinuousController
from live_trader.order_management import OrderIntent


def _strategy(
    strategy_id: str,
    *,
    deployment_id: str,
    symbol: str = "BTCUSDT",
) -> dict:
    return {
        "id": strategy_id,
        "strategy_id": strategy_id,
        "deployment_id": deployment_id,
        "symbol": symbol,
        "timeframe": "1h",
        "broker_id": "binance",
        "provider": "binance",
        "plugin": "ma-cross",
        "parameters": {},
        "lifecycle_status": "before-live-small",
        "backtester_verified": True,
        "live_small_eligible": True,
        "live_eligible": False,
        "permissions": {"live_small_eligible": True},
    }


class RuntimeContextBindingTest(unittest.TestCase):
    def test_runtime_start_rejects_missing_selected_deployment_without_fallback(self) -> None:
        available = _strategy("available", deployment_id="dep-available")
        original_state = copy.deepcopy(state.STATE)
        try:
            with (
                patch.object(state, "portfolio_rows", return_value=[]),
                patch.object(state, "strategy_rows", return_value=[available]),
                patch.object(state, "snapshot", return_value={}),
                patch.object(state, "append_audit"),
                patch.object(state.LIVE_CONTINUOUS_CONTROLLER, "start") as start,
            ):
                result = state.start_continuous_runtime(
                    "crypto",
                    "MONITOR",
                    "",
                    "dep-missing",
                    "missing",
                )
        finally:
            state.STATE.clear()
            state.STATE.update(original_state)

        self.assertFalse(result["ok"])
        self.assertIn("일치하지 않습니다", result["reason"])
        start.assert_not_called()

    def test_explicit_standalone_selector_never_falls_back_to_first_eligible(self) -> None:
        controller = LiveContinuousController(Path("."))
        first = _strategy("first", deployment_id="dep-first")
        selected = _strategy("selected", deployment_id="dep-selected")

        with patch.object(state, "strategy_rows", return_value=[first, selected]):
            exact = controller._select_standalone_strategy(
                "crypto",
                "SMALL_LIVE",
                "selected",
                "dep-selected",
            )
            missing = controller._select_standalone_strategy(
                "crypto",
                "SMALL_LIVE",
                "missing",
                "dep-missing",
            )

        self.assertEqual("selected", exact["strategy_id"])
        self.assertIsNone(missing)

    def test_running_runtime_rejects_another_deployment_before_mode_transition(self) -> None:
        controller = LiveContinuousController(Path("."))
        spec = controller._standalone_spec(
            _strategy("selected", deployment_id="dep-selected")
        )
        supervisor = MagicMock()
        supervisor.running = True
        supervisor.snapshot.return_value = {"phase": "RUNNING", "running": True}
        supervisor.engine.specs = (spec,)
        controller.supervisor = supervisor
        controller.profile_id = "crypto"
        controller.mode = "MONITOR"
        controller.deployment_id = "dep-selected"
        controller.requested_strategy_id = "selected"
        controller.strategy_ids = ("selected",)
        controller.allowed_symbols = ("BTCUSDT",)

        with patch.object(state, "snapshot", return_value={}):
            result = controller.start(
                "crypto",
                "SMALL_LIVE",
                "",
                "other",
                "dep-other",
            )

        self.assertFalse(result["ok"])
        self.assertIn("컨텍스트", result["reason"])
        supervisor.engine.transition_mode.assert_not_called()

    def test_state_passes_resolved_governance_context_to_controller(self) -> None:
        original_state = copy.deepcopy(state.STATE)
        session = SimpleNamespace(
            session_id="session-1",
            deployment_id="dep-selected",
            metadata={"portfolioId": "portfolio-1", "strategyId": "selected"},
        )
        state.STATE.setdefault("active_runtime_session_ids", {})["crypto"] = (
            "previous-session"
        )

        def prepare(*_args):
            state.STATE["active_runtime_session_ids"]["crypto"] = session.session_id
            return session, "unit"

        restored_binding = ""
        try:
            with (
                patch.object(
                    state,
                    "_prepare_operational_runtime_session",
                    side_effect=prepare,
                ),
                patch.object(
                    state.LIVE_CONTINUOUS_CONTROLLER,
                    "start",
                    return_value={"ok": False, "reason": "context blocked"},
                ) as start,
                patch.object(state, "_finish_operational_runtime_start") as finish,
                patch.object(state, "snapshot", return_value={}),
            ):
                result = state.start_continuous_runtime(
                    "crypto",
                    "SMALL_LIVE",
                    "portfolio-1",
                    "dep-selected",
                    "selected",
                )
                restored_binding = str(
                    state.STATE["active_runtime_session_ids"].get("crypto") or ""
                )
        finally:
            state.STATE.clear()
            state.STATE.update(original_state)

        self.assertFalse(result["ok"])
        self.assertEqual("previous-session", restored_binding)
        start.assert_called_once_with(
            "crypto",
            "SMALL_LIVE",
            "portfolio-1",
            "selected",
            "dep-selected",
        )
        finish.assert_called_once_with(session, False, "context blocked")

    def test_mismatched_evaluation_cannot_create_order_intent(self) -> None:
        controller = LiveContinuousController(Path("."))
        spec = controller._standalone_spec(
            _strategy("selected", deployment_id="dep-selected")
        )
        controller.profile_id = "crypto"
        controller.mode = "SMALL_LIVE"
        controller.deployment_id = "dep-selected"
        controller.requested_strategy_id = "selected"
        controller.strategy_ids = ("selected",)
        controller.allowed_symbols = ("BTCUSDT",)
        controller.supervisor = MagicMock()
        controller.supervisor.engine.specs = (spec,)
        decision = SimpleNamespace(
            strategy_id="other",
            strategy_instance_id=spec.strategy_instance_id,
            signal="BUY",
            reason="unit mismatch",
            evaluation_key="evaluation-1",
            bar=SimpleNamespace(close=100.0, end_time="2026-08-01T00:00:00Z"),
        )

        with (
            patch.object(state, "append_audit"),
            patch.object(state, "submit_order_intent") as submit,
        ):
            result = controller._handle_cycle(
                SimpleNamespace(decisions=(decision,))
            )

        self.assertEqual("BLOCKED", result["results"][0]["action"])
        submit.assert_not_called()

    def test_portfolio_manifest_binds_every_strategy_symbol_pair(self) -> None:
        selected = _strategy("alpha", deployment_id="dep-alpha")
        selected["portfolio_gate"] = {
            "active": True,
            "allowed": True,
            "portfolioId": "portfolio-1",
        }
        beta = _strategy("beta", deployment_id="dep-beta", symbol="ETHUSDT")
        portfolio = {
            "id": "portfolio-1",
            "strategy_instances": [
                {
                    "sourceStrategyId": "alpha",
                    "strategyId": "alpha",
                    "symbol": "BTCUSDT",
                    "sourceArtifactHash": "source-alpha",
                },
                {
                    "sourceStrategyId": "beta",
                    "strategyId": "beta",
                    "symbol": "ETHUSDT",
                    "sourceArtifactHash": "source-beta",
                },
            ],
        }

        with (
            patch.object(state, "portfolio_rows", return_value=[portfolio]),
            patch.object(state, "load_strategy_artifacts", return_value=[selected, beta]),
        ):
            inputs = state._operational_manifest_inputs(selected)

        metadata = inputs["metadata"]
        self.assertEqual(["alpha", "beta"], metadata["strategyIds"])
        self.assertEqual(["BTCUSDT", "ETHUSDT"], metadata["allowedSymbols"])
        self.assertEqual(
            {("alpha", "BTCUSDT"), ("beta", "ETHUSDT")},
            {
                (item["strategyId"], item["symbol"])
                for item in metadata["strategyMembers"]
            },
        )
        self.assertTrue(inputs["portfolio_artifact_hash"])

    def test_cross_broker_portfolio_is_fail_closed_until_scoped_preflight_exists(self) -> None:
        selected = _strategy("alpha", deployment_id="dep-alpha")
        selected["portfolio_gate"] = {
            "active": True,
            "allowed": True,
            "portfolioId": "portfolio-mixed",
        }
        upbit = _strategy(
            "beta",
            deployment_id="dep-beta",
            symbol="KRW-ETH",
        )
        upbit["broker_id"] = "upbit"
        upbit["provider"] = "upbit"
        portfolio = {
            "id": "portfolio-mixed",
            "strategy_instances": [
                {"strategyId": "alpha", "symbol": "BTCUSDT"},
                {"strategyId": "beta", "symbol": "KRW-ETH"},
            ],
        }

        with (
            patch.object(state, "portfolio_rows", return_value=[portfolio]),
            patch.object(
                state,
                "load_strategy_artifacts",
                return_value=[selected, upbit],
            ),
        ):
            with self.assertRaisesRegex(ValueError, "cross-broker"):
                state._operational_manifest_inputs(selected)

        specs = (
            LiveContinuousController._standalone_spec(selected),
            LiveContinuousController._standalone_spec(upbit),
        )
        blocker = LiveContinuousController._single_broker_context_blocker(specs)
        self.assertIn("cross-broker", blocker)

    def test_dispatch_authorization_accepts_portfolio_member_and_rejects_outsider(self) -> None:
        current = _strategy("beta", deployment_id="dep-beta", symbol="ETHUSDT")
        current["portfolio_gate"] = {
            "active": True,
            "allowed": True,
            "portfolioId": "portfolio-1",
        }
        member_hash = state._operational_artifact_hash(current)
        manifest = SimpleNamespace(
            deployment_id="dep-alpha",
            broker_route="binance",
            strategy_artifact_hash="a" * 64,
            portfolio_artifact_hash="b" * 64,
            risk_policy_hash="c" * 64,
            config_revision=2,
            config_hash="d" * 64,
            metadata={
                "portfolioId": "portfolio-1",
                "brokerRoutes": ["binance"],
                "strategyMembers": [
                    {
                        "strategyId": "alpha",
                        "symbol": "BTCUSDT",
                        "brokerId": "binance",
                        "artifactHash": "e" * 64,
                    },
                    {
                        "strategyId": "beta",
                        "symbol": "ETHUSDT",
                        "brokerId": "binance",
                        "artifactHash": member_hash,
                    },
                ],
            },
        )
        intent = OrderIntent(
            strategy_id="beta",
            asset="코인",
            symbol="ETHUSDT",
            side="BUY",
            quantity=0.001,
            reference_price=3000.0,
            mode="SMALL_LIVE",
            reason="unit",
            metadata={"broker_id": "binance", "portfolio_id": "portfolio-1"},
        )
        original_sessions = copy.deepcopy(state.STATE.get("active_runtime_session_ids"))
        state.STATE["active_runtime_session_ids"] = {"crypto": "session-1"}
        try:
            with (
                patch.object(
                    state.OPERATIONAL_GOVERNANCE,
                    "runtime_authorization",
                    return_value={
                        "allowed": True,
                        "reasons": [],
                        "session": {"deploymentManifestHash": "manifest-1"},
                    },
                ),
                patch.object(
                    state.OPERATIONAL_GOVERNANCE,
                    "get_deployment_manifest_by_hash",
                    return_value=manifest,
                ),
                patch.object(state, "portfolio_rows", return_value=[]),
                patch.object(state, "strategy_rows", return_value=[current]),
                patch.object(
                    state,
                    "_operational_manifest_inputs",
                    return_value={
                        "deployment_id": manifest.deployment_id,
                        "strategy_artifact_hash": manifest.strategy_artifact_hash,
                        "portfolio_artifact_hash": manifest.portfolio_artifact_hash,
                        "account_fingerprint": "f" * 64,
                        "broker_route": manifest.broker_route,
                        "runtime_version": "live-trader-runtime-v2",
                        "build_hash": "1" * 64,
                        "execution_adapter": "binance-signed-adapter",
                        "execution_adapter_version": "v2",
                        "risk_policy_revision": 1,
                        "risk_policy_hash": manifest.risk_policy_hash,
                        "config_revision": manifest.config_revision,
                        "config_hash": manifest.config_hash,
                        "preflight_ttl_seconds": 300,
                        "metadata": dict(manifest.metadata),
                    },
                ),
            ):
                allowed = state.operational_runtime_dispatch_allowed(intent)
                outsider = state.operational_runtime_dispatch_allowed(
                    OrderIntent(
                        **{
                            **intent.__dict__,
                            "strategy_id": "outsider",
                        }
                    )
                )
                manifest.metadata["brokerRoutes"] = ["binance", "upbit"]
                mixed_route = state.operational_runtime_dispatch_allowed(intent)
        finally:
            state.STATE["active_runtime_session_ids"] = original_sessions

        self.assertTrue(allowed[0])
        self.assertEqual("operational-runtime-authorized", allowed[1])
        self.assertFalse(outsider[0])
        self.assertEqual("operational-strategy-context-mismatch", outsider[1])
        self.assertFalse(mixed_route[0])
        self.assertEqual(
            "operational-cross-broker-portfolio-blocked",
            mixed_route[1],
        )


if __name__ == "__main__":
    unittest.main()
