import copy
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from live_trader import state  # noqa: F401 - initializes the shared runtime path
from live_trader.continuous_live import (
    LiveContinuousController,
    LiveContinuousRuntimeManager,
)


class LiveContinuousControllerTest(unittest.TestCase):
    def test_standalone_strategy_preserves_binance_runtime_contract(self) -> None:
        strategy = {
            "strategy_id": "btc-live-small",
            "instance_id": "si-btc-live-small",
            "symbol": "BTCUSDT",
            "timeframe": "1h",
            "provider": "binance",
            "broker_id": "binance",
            "plugin": "ma-cross",
            "artifact_hash": "abc123",
            "parameters": {
                "shortMa": 36,
                "longMa": 240,
                "positionSize": 20,
                "paperOrderQuantity": 0.0001,
            },
        }

        spec = LiveContinuousController._standalone_spec(strategy)

        self.assertEqual("standalone:btc-live-small", spec.portfolio_id)
        self.assertEqual("si-btc-live-small", spec.strategy_instance_id)
        self.assertEqual("BTCUSDT", spec.instrument_id)
        self.assertEqual("1h", spec.timeframe)
        self.assertEqual("binance", spec.provider)
        self.assertEqual("binance", spec.broker_id)
        self.assertEqual(0.2, spec.target_weight)
        self.assertEqual(0.0001, spec.parameters["paperOrderQuantity"])

    def test_mode_permission_accepts_live_small_but_not_full_live(self) -> None:
        strategy = {
            "strategy_id": "btc-live-small",
            "symbol": "BTCUSDT",
            "timeframe": "1h",
            "plugin": "ma-cross",
            "parameters": {},
            "permissions": {
                "live_small_eligible": True,
                "live_eligible": False,
            },
        }
        spec = LiveContinuousController._standalone_spec(strategy)

        self.assertTrue(LiveContinuousController._spec_mode_allowed(spec, "MONITOR"))
        self.assertTrue(LiveContinuousController._spec_mode_allowed(spec, "SMALL_LIVE"))
        self.assertFalse(LiveContinuousController._spec_mode_allowed(spec, "FULL_LIVE"))

    def test_live_notional_does_not_reuse_large_paper_quantity(self) -> None:
        strategy = {
            "strategy_id": "eth-live-small",
            "symbol": "ETHUSDT",
            "timeframe": "5m",
            "plugin": "ma-cross",
            "parameters": {
                "paperOrderQuantity": 100,
                "liveOrderNotionalUsdt": 5.5,
            },
        }
        spec = LiveContinuousController._standalone_spec(strategy)

        self.assertAlmostEqual(
            5.5 / 4_000,
            LiveContinuousController._order_quantity(spec, 4_000),
        )

    def test_futures_fixed_quantity_is_preserved_with_live_notional_cap(self) -> None:
        strategy = {
            "strategy_id": "btc-futures-short",
            "symbol": "BTCUSDT",
            "timeframe": "1h",
            "broker_id": "binance-futures",
            "market_type": "futures",
            "plugin": "strategy_builder_custom",
            "executionSizing": {
                "mode": "fixed_quantity",
                "paperOrderQuantity": 0.001,
                "fractionalAllowed": True,
            },
            "parameters": {
                "paperOrderQuantity": 0.001,
            },
        }
        spec = LiveContinuousController._standalone_spec(strategy)

        self.assertEqual(
            0.001,
            LiveContinuousController._order_quantity(spec, 65_000),
        )

        oversized = LiveContinuousController._standalone_spec({
            **strategy,
            "executionSizing": {
                **strategy["executionSizing"],
                "paperOrderQuantity": 100,
            },
            "parameters": {
                "paperOrderQuantity": 100,
                "liveOrderNotionalUsdt": 100,
            },
        })
        self.assertAlmostEqual(
            100 / 65_000,
            LiveContinuousController._order_quantity(oversized, 65_000),
        )

    def test_short_direction_is_preserved_but_live_adapter_attestation_stays_off(self) -> None:
        strategy = {
            "strategy_id": "btc-short",
            "instance_id": "si-btc-short",
            "symbol": "BTCUSDT",
            "timeframe": "1h",
            "provider": "binance",
            "broker_id": "binance",
            "market_type": "futures",
            "plugin": "strategy_builder_custom",
            "parameters": {
                "customStrategyDefinition": {
                    "positionDirection": "short",
                    "entryRules": [],
                    "exitRules": [],
                },
                "liveOrderNotionalUsdt": 5.5,
            },
        }
        runtime_spec = LiveContinuousController._standalone_spec(strategy)
        controller = LiveContinuousController(Path("."))
        controller.profile_id = "crypto"
        controller.mode = "SMALL_LIVE"
        controller.supervisor = MagicMock()
        controller.supervisor.engine.specs = (runtime_spec,)
        decision = SimpleNamespace(
            strategy_id=runtime_spec.strategy_id,
            strategy_instance_id=runtime_spec.strategy_instance_id,
            signal="SELL",
            reason="short entry",
            evaluation_key="short-entry-1",
            bar=SimpleNamespace(
                close=100.0,
                end_time="2026-07-26T00:00:00Z",
            ),
        )
        original_state = copy.deepcopy(state.STATE)
        try:
            with (
                patch.object(state, "broker_position_quantity", return_value=0),
                patch.object(state, "snapshot", return_value={}),
                patch.object(state, "append_audit"),
                patch.object(
                    state,
                    "submit_order_intent",
                    return_value={"ok": False, "reason": "short adapter blocked"},
                ) as submit,
            ):
                controller._handle_cycle(SimpleNamespace(decisions=(decision,)))
        finally:
            state.STATE.clear()
            state.STATE.update(original_state)

        intent = submit.call_args.args[1]
        self.assertEqual("short", intent.metadata["position_direction"])
        self.assertEqual("futures", intent.metadata["market_type"])
        self.assertTrue(intent.metadata["short_entries_requested"])
        self.assertFalse(intent.metadata["broker_short_adapter_verified"])

    def test_short_cover_uses_reconciled_position_quantity(self) -> None:
        strategy = {
            "strategy_id": "btc-short-cover",
            "instance_id": "si-btc-short-cover",
            "symbol": "BTCUSDT",
            "timeframe": "1h",
            "provider": "binance",
            "broker_id": "binance",
            "market_type": "futures",
            "plugin": "strategy_builder_custom",
            "parameters": {
                "customStrategyDefinition": {
                    "positionDirection": "short",
                    "entryRules": [],
                    "exitRules": [],
                }
            },
        }
        runtime_spec = LiveContinuousController._standalone_spec(strategy)
        controller = LiveContinuousController(Path("."))
        controller.profile_id = "crypto"
        controller.mode = "SMALL_LIVE"
        controller.supervisor = MagicMock()
        controller.supervisor.engine.specs = (runtime_spec,)
        decision = SimpleNamespace(
            strategy_id=runtime_spec.strategy_id,
            strategy_instance_id=runtime_spec.strategy_instance_id,
            signal="BUY",
            reason="short cover",
            evaluation_key="short-cover-1",
            bar=SimpleNamespace(
                close=90.0,
                end_time="2026-07-26T01:00:00Z",
            ),
        )
        original_state = copy.deepcopy(state.STATE)
        try:
            with (
                patch.object(state, "broker_position_quantity", return_value=-0.25),
                patch.object(state, "snapshot", return_value={}),
                patch.object(state, "append_audit"),
                patch.object(
                    state,
                    "submit_order_intent",
                    return_value={"ok": True, "reason": "cover"},
                ) as submit,
            ):
                controller._handle_cycle(SimpleNamespace(decisions=(decision,)))
        finally:
            state.STATE.clear()
            state.STATE.update(original_state)

        intent = submit.call_args.args[1]
        self.assertEqual("BUY", intent.side)
        self.assertEqual(0.25, intent.quantity)
        self.assertTrue(intent.metadata["risk_reducing"])

    def test_forced_restore_assessment_ignores_cache_and_requires_fresh_read(self) -> None:
        spec = LiveContinuousController._standalone_spec({
            "strategy_id": "btc-live-small",
            "instance_id": "si-btc-live-small",
            "symbol": "BTCUSDT",
            "timeframe": "1h",
            "provider": "binance",
            "broker_id": "binance",
            "plugin": "ma-cross",
            "parameters": {},
        })
        original_reconciliation = copy.deepcopy(
            state.STATE.get("broker_reconciliation", {})
        )
        original_orders = copy.deepcopy(state.STATE.get("orders", []))
        state.STATE["broker_reconciliation"] = {
            "successful_position_brokers": ["binance"],
            "positions": [],
            "fetched_at": "2099-01-01 00:00:00",
        }
        state.STATE["orders"] = []
        router = MagicMock()
        router.list_positions.side_effect = RuntimeError("forced-read-failed")
        try:
            with (
                patch.object(state, "LiveBrokerRouter", return_value=router),
                patch.object(
                    state.PROGRAM_LEDGER,
                    "position_rows",
                    return_value=[],
                ),
                patch.object(
                    state.PROGRAM_LEDGER,
                    "execution_event_rows",
                    return_value=[],
                ),
            ):
                result = state.forced_restore_context_assessment(
                    (spec,),
                    portfolio_id=spec.portfolio_id,
                    portfolio_hash=spec.portfolio_hash,
                    strategy_identity_hash="identity",
                    checkpoint_seal=None,
                    evaluator_state={},
                )
        finally:
            state.STATE["broker_reconciliation"] = original_reconciliation
            state.STATE["orders"] = original_orders

        self.assertFalse(result["fresh"])
        self.assertFalse(result["brokerSnapshotComplete"])
        self.assertIn("forced-read-failed", result["reason"])

    def test_forced_restore_assessment_fails_closed_when_ledger_is_unreadable(self) -> None:
        spec = LiveContinuousController._standalone_spec({
            "strategy_id": "btc-live-small",
            "instance_id": "si-btc-live-small",
            "symbol": "BTCUSDT",
            "timeframe": "1h",
            "provider": "binance",
            "broker_id": "binance",
            "plugin": "ma-cross",
            "parameters": {},
        })
        original_reconciliation = copy.deepcopy(
            state.STATE.get("broker_reconciliation", {})
        )
        original_orders = copy.deepcopy(state.STATE.get("orders", []))
        state.STATE["orders"] = []
        router = MagicMock()
        router.list_positions.return_value = []
        try:
            with (
                patch.object(state, "LiveBrokerRouter", return_value=router),
                patch.object(
                    state.PROGRAM_LEDGER,
                    "position_rows",
                    side_effect=OSError("ledger-locked"),
                ),
            ):
                result = state.forced_restore_context_assessment(
                    (spec,),
                    portfolio_id=spec.portfolio_id,
                    portfolio_hash=spec.portfolio_hash,
                    strategy_identity_hash="identity",
                    checkpoint_seal=None,
                    evaluator_state={},
                )
        finally:
            state.STATE["broker_reconciliation"] = original_reconciliation
            state.STATE["orders"] = original_orders

        self.assertFalse(result["fresh"])
        self.assertFalse(result["brokerSnapshotComplete"])
        self.assertIn("program-ledger:OSError:ledger-locked", result["reason"])

    def test_forced_restore_assessment_requires_flat_ledger_and_zero_pending_orders(self) -> None:
        spec = LiveContinuousController._standalone_spec({
            "strategy_id": "btc-live-small",
            "instance_id": "si-btc-live-small",
            "symbol": "BTCUSDT",
            "timeframe": "1h",
            "provider": "binance",
            "broker_id": "binance",
            "plugin": "ma-cross",
            "parameters": {},
        })
        original_orders = copy.deepcopy(state.STATE.get("orders", []))
        original_reconciliation = copy.deepcopy(
            state.STATE.get("broker_reconciliation", {})
        )
        state.STATE["broker_reconciliation"] = {
            "positions": [{
                "broker_id": "binance",
                "symbol": "BTC",
                "broker_qty": 9.0,
            }],
            "successful_position_brokers": ["binance"],
            "fetched_at": "2000-01-01 00:00:00",
        }
        state.STATE["orders"] = [{
            "order_id": "pending-1",
            "broker_id": "binance",
            "symbol": "BTCUSDT",
            "state": "sent",
            "queue_state": "submitted",
        }]
        router = MagicMock()
        router.list_positions.return_value = []
        try:
            with (
                patch.object(state, "LiveBrokerRouter", return_value=router),
                patch.object(
                    state.PROGRAM_LEDGER,
                    "position_rows",
                    return_value=[{
                        "broker_id": "binance",
                        "symbol": "BTCUSDT",
                        "quantity": 0.01,
                        "value": 1.0,
                        "updated_at": "2026-07-26 00:00:00",
                        "source": "unit",
                    }],
                ),
                patch.object(
                    state.PROGRAM_LEDGER,
                    "execution_event_rows",
                    return_value=[],
                ),
            ):
                result = state.forced_restore_context_assessment(
                    (spec,),
                    portfolio_id=spec.portfolio_id,
                    portfolio_hash=spec.portfolio_hash,
                    strategy_identity_hash="identity",
                    checkpoint_seal=None,
                    evaluator_state={},
                )
                published_reconciliation = copy.deepcopy(
                    state.STATE["broker_reconciliation"]
                )
        finally:
            state.STATE["orders"] = original_orders
            state.STATE["broker_reconciliation"] = original_reconciliation

        self.assertTrue(result["fresh"])
        self.assertTrue(result["allFlat"])
        self.assertFalse(result["programLedgerFlat"])
        self.assertTrue(result["hasKnownPosition"])
        self.assertEqual(1, result["pendingOrUnknownOrderCount"])
        self.assertFalse(result["contextSealValid"])
        self.assertEqual([], published_reconciliation["positions"])
        self.assertIn(
            "binance",
            published_reconciliation["position_observations"],
        )

    def test_existing_broker_position_has_no_verifiable_restart_fill_generation(self) -> None:
        spec = LiveContinuousController._standalone_spec({
            "strategy_id": "btc-live-small",
            "instance_id": "si-btc-live-small",
            "symbol": "BTCUSDT",
            "timeframe": "1h",
            "provider": "binance",
            "broker_id": "binance",
            "plugin": "strategy_builder_custom",
            "parameters": {},
        })
        original_orders = copy.deepcopy(state.STATE.get("orders", []))
        original_reconciliation = copy.deepcopy(
            state.STATE.get("broker_reconciliation", {})
        )
        state.STATE["orders"] = []
        router = MagicMock()
        router.list_positions.return_value = [{
            "broker_id": "binance",
            "symbol": "BTC",
            "broker_qty": 0.01,
            "average_price": 0.0,
        }]
        evaluator_state = {
            "entries": [{
                "specIdentity": {
                    "strategyInstanceId": spec.strategy_instance_id,
                },
                "state": {
                    "lastHasPosition": True,
                    "entryContextKnown": True,
                    "entryPrice": 100.0,
                    "barsHeld": 3,
                    "evaluationCount": 10,
                    "lastBarKey": "bar-10",
                },
            }],
        }
        fill = {
            "event_id": "fill-1",
            "broker_id": "binance",
            "order_id": "order-1",
            "broker_order_id": "broker-order-1",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "quantity": 0.01,
            "price": 100.0,
            "state": "filled",
            "occurred_at": "2026-07-26 00:00:00",
        }
        try:
            with (
                patch.object(state, "LiveBrokerRouter", return_value=router),
                patch.object(
                    state.PROGRAM_LEDGER,
                    "position_rows",
                    return_value=[{
                        "broker_id": "binance",
                        "symbol": "BTCUSDT",
                        "quantity": 0.01,
                        "value": 1.0,
                        "updated_at": "2026-07-26 00:00:00",
                        "source": "unit",
                    }],
                ),
                patch.object(
                    state.PROGRAM_LEDGER,
                    "execution_event_rows",
                    return_value=[fill],
                ),
            ):
                first = state.forced_restore_context_assessment(
                    (spec,),
                    portfolio_id=spec.portfolio_id,
                    portfolio_hash=spec.portfolio_hash,
                    strategy_identity_hash="identity",
                    checkpoint_seal=None,
                    evaluator_state=evaluator_state,
                )
                second = state.forced_restore_context_assessment(
                    (spec,),
                    portfolio_id=spec.portfolio_id,
                    portfolio_hash=spec.portfolio_hash,
                    strategy_identity_hash="identity",
                    checkpoint_seal=first["contextSeal"],
                    evaluator_state=evaluator_state,
                )
        finally:
            state.STATE["orders"] = original_orders
            state.STATE["broker_reconciliation"] = original_reconciliation

        self.assertTrue(second["fresh"])
        self.assertTrue(second["hasKnownPosition"])
        self.assertFalse(second["positionContextComplete"])
        self.assertFalse(second["contextSealValid"])

    @staticmethod
    def _portfolio() -> dict:
        return {
            "id": "portfolio-btc",
            "lifecycle_status": "backtested",
            "source_path": "portfolio.json",
            "strategy_instances": [
                {
                    "sourceStrategyId": "btc-strategy",
                    "sourceArtifactHash": "artifact-hash",
                    "symbol": "BTCUSDT",
                }
            ],
        }

    @staticmethod
    def _strategy(lifecycle: str = "backtested") -> dict:
        return {
            "strategy_id": "btc-strategy",
            "artifact_hash": "artifact-hash",
            "lifecycle_status": lifecycle,
            "backtester_verified": True,
            "live_small_eligible": lifecycle == "before-live-small",
            "live_eligible": lifecycle == "live",
        }

    def test_monitor_ignores_portfolio_with_retired_component(self) -> None:
        controller = LiveContinuousController(Path("."))
        with (
            patch.object(state, "portfolio_rows", return_value=[self._portfolio()]),
            patch.object(state, "strategy_rows", return_value=[self._strategy("retired")]),
        ):
            selected = controller._select_portfolio("crypto", "", "MONITOR")

        self.assertIsNone(selected)

    def test_monitor_accepts_backtested_portfolio_components(self) -> None:
        controller = LiveContinuousController(Path("."))
        with (
            patch.object(state, "portfolio_rows", return_value=[self._portfolio()]),
            patch.object(state, "strategy_rows", return_value=[self._strategy("backtested")]),
        ):
            selected = controller._select_portfolio("crypto", "", "MONITOR")

        self.assertEqual("portfolio-btc", selected["id"])

    def test_small_live_requires_component_live_small_eligibility(self) -> None:
        controller = LiveContinuousController(Path("."))
        with (
            patch.object(state, "portfolio_rows", return_value=[self._portfolio()]),
            patch.object(state, "strategy_rows", return_value=[self._strategy("backtested")]),
        ):
            blocked = controller._select_portfolio("crypto", "", "SMALL_LIVE")
        with (
            patch.object(state, "portfolio_rows", return_value=[self._portfolio()]),
            patch.object(state, "strategy_rows", return_value=[self._strategy("before-live-small")]),
        ):
            allowed = controller._select_portfolio("crypto", "", "SMALL_LIVE")

        self.assertIsNone(blocked)
        self.assertEqual("portfolio-btc", allowed["id"])

    def test_auto_runtime_portfolio_skips_invalid_legacy_lock(self) -> None:
        controller = LiveContinuousController(Path("."))
        with tempfile.TemporaryDirectory() as temporary:
            legacy_path = Path(temporary) / "legacy.json"
            trusted_path = Path(temporary) / "trusted.json"
            legacy_path.write_text("{}", encoding="utf-8")
            trusted_path.write_text("{}", encoding="utf-8")
            candidates = [
                {"id": "legacy", "source_path": str(legacy_path)},
                {"id": "trusted", "source_path": str(trusted_path)},
            ]
            with (
                patch.object(
                    controller,
                    "_portfolio_candidates",
                    return_value=candidates,
                ),
                patch(
                    "live_trader.continuous_live.load_portfolio_runtime_path",
                    side_effect=[ValueError("legacy lock"), object()],
                ),
            ):
                selected = controller._select_runtime_portfolio(
                    "stock",
                    "",
                    "MONITOR",
                )

        self.assertEqual("trusted", selected["id"])

    def test_monitor_standalone_accepts_verified_backtested_strategy(
        self,
    ) -> None:
        controller = LiveContinuousController(Path("."))
        strategy = {
            "strategy_id": "stock-monitor",
            "symbol": "251340",
            "lifecycle_status": "backtested",
            "backtester_verified": True,
            "live_small_eligible": False,
            "live_eligible": False,
        }
        with patch.object(state, "strategy_rows", return_value=[strategy]):
            selected = controller._select_standalone_strategy(
                "stock",
                "MONITOR",
            )

        self.assertEqual("stock-monitor", selected["strategy_id"])

    def test_running_monitor_runtime_keeps_mode_when_restore_transition_is_blocked(self) -> None:
        strategy = {
            "strategy_id": "btc-live-small",
            "symbol": "BTCUSDT",
            "timeframe": "1h",
            "plugin": "ma-cross",
            "parameters": {},
            "permissions": {"live_small_eligible": True},
        }
        runtime_spec = LiveContinuousController._standalone_spec(strategy)
        engine = MagicMock()
        engine.specs = (runtime_spec,)
        engine.transition_mode.return_value = (
            "checkpoint restore가 거부되어 live mode 전환을 차단했습니다"
        )
        supervisor = MagicMock()
        supervisor.running = True
        supervisor.engine = engine
        supervisor.snapshot.return_value = {
            "running": True,
            "engine": {"restoreStatus": "REJECTED"},
        }
        controller = LiveContinuousController(Path("."))
        controller.supervisor = supervisor
        controller.profile_id = "crypto"
        controller.mode = "MONITOR"

        with (
            patch.object(state, "snapshot", return_value={}),
            patch.object(
                controller,
                "_restore_context_assessment",
                return_value={"fresh": False, "reason": "unit"},
            ),
        ):
            result = controller.start("crypto", "SMALL_LIVE")

        self.assertFalse(result["ok"])
        self.assertEqual("MONITOR", controller.mode)
        engine.transition_mode.assert_called_once_with(
            "SMALL_LIVE",
            restore_context={"fresh": False, "reason": "unit"},
        )

    def test_cycle_uses_controller_mode_even_when_global_mode_is_monitor(self) -> None:
        strategy = {
            "strategy_id": "btc-live-small",
            "instance_id": "si-btc-live-small",
            "symbol": "BTCUSDT",
            "timeframe": "1h",
            "provider": "binance",
            "broker_id": "binance",
            "plugin": "ma-cross",
            "parameters": {"liveOrderNotionalUsdt": 5.5},
        }
        runtime_spec = LiveContinuousController._standalone_spec(strategy)
        controller = LiveContinuousController(Path("."))
        controller.profile_id = "crypto"
        controller.mode = "SMALL_LIVE"
        controller.supervisor = MagicMock()
        controller.supervisor.engine.specs = (runtime_spec,)
        decision = SimpleNamespace(
            strategy_id=runtime_spec.strategy_id,
            strategy_instance_id=runtime_spec.strategy_instance_id,
            signal="BUY",
            reason="unit",
            evaluation_key="evaluation-1",
            bar=SimpleNamespace(
                close=100.0,
                end_time="2026-07-26T00:00:00Z",
            ),
        )
        original_state = copy.deepcopy(state.STATE)
        state.STATE["mode"] = "MONITOR"
        try:
            with (
                patch.object(state, "broker_position_quantity", return_value=0),
                patch.object(state, "snapshot", return_value={}),
                patch.object(state, "append_audit"),
                patch.object(
                    state,
                    "submit_order_intent",
                    return_value={"ok": True, "reason": "dry-run"},
                ) as submit,
            ):
                controller._handle_cycle(
                    SimpleNamespace(decisions=(decision,))
                )
        finally:
            state.STATE.clear()
            state.STATE.update(original_state)

        submitted_intent = submit.call_args.args[1]
        self.assertEqual("SMALL_LIVE", submitted_intent.mode)
        self.assertEqual("MARKET", submitted_intent.metadata["order_type"])
        self.assertEqual(
            "next-open-boundary",
            submitted_intent.metadata["execution_timing"],
        )
        self.assertEqual(
            "reference-and-sizing-only",
            submitted_intent.metadata["decision_price_role"],
        )

    def test_continuous_cycle_uses_adapter_native_order_type_contracts(self) -> None:
        cases = (
            ("BTCUSDT", "binance", "BUY", "MARKET"),
            ("KRW-BTC", "upbit", "BUY", "price"),
            ("KRW-BTC", "upbit", "SELL", "market"),
            ("069500.KS", "kis", "BUY", "01"),
            ("AAPL", "kis", "BUY", "00"),
        )
        original_state = copy.deepcopy(state.STATE)
        try:
            for symbol, broker_id, side, expected_order_type in cases:
                with self.subTest(
                    broker_id=broker_id,
                    symbol=symbol,
                    side=side,
                ):
                    strategy = {
                        "strategy_id": f"{broker_id}-{symbol}",
                        "instance_id": f"si-{broker_id}-{symbol}",
                        "symbol": symbol,
                        "timeframe": "1h",
                        "broker_id": broker_id,
                        "plugin": "ma-cross",
                        "parameters": {},
                    }
                    runtime_spec = LiveContinuousController._standalone_spec(
                        strategy
                    )
                    controller = LiveContinuousController(Path("."))
                    controller.profile_id = (
                        "crypto"
                        if broker_id in {"binance", "upbit"}
                        else "stock"
                    )
                    controller.mode = "SMALL_LIVE"
                    controller.supervisor = MagicMock()
                    controller.supervisor.engine.specs = (runtime_spec,)
                    decision = SimpleNamespace(
                        strategy_id=runtime_spec.strategy_id,
                        strategy_instance_id=(
                            runtime_spec.strategy_instance_id
                        ),
                        signal=side,
                        reason="unit",
                        evaluation_key=f"evaluation-{broker_id}-{symbol}",
                        bar=SimpleNamespace(
                            close=100.0,
                            end_time="2026-07-26T00:00:00Z",
                        ),
                    )
                    with (
                        patch.object(
                            state,
                            "broker_position_quantity",
                            return_value=0,
                        ),
                        patch.object(state, "snapshot", return_value={}),
                        patch.object(state, "append_audit"),
                        patch.object(
                            state,
                            "submit_order_intent",
                            return_value={"ok": True, "reason": "dry-run"},
                        ) as submit,
                    ):
                        controller._handle_cycle(
                            SimpleNamespace(decisions=(decision,))
                        )
                    intent = submit.call_args.args[1]
                    self.assertEqual(
                        expected_order_type,
                        intent.metadata["order_type"],
                    )
                    self.assertEqual(
                        "next-open-boundary",
                        intent.metadata["execution_timing"],
                    )
        finally:
            state.STATE.clear()
            state.STATE.update(original_state)

    def test_runtime_start_syncs_global_profile_only_after_success(self) -> None:
        original_state = copy.deepcopy(state.STATE)
        state.STATE["mode"] = "MONITOR"
        state.STATE["automation"]["crypto"]["mode"] = "MONITOR"
        state.STATE["automation"]["crypto"]["enabled"] = False
        try:
            with (
                patch.object(
                    state.LIVE_CONTINUOUS_CONTROLLER,
                    "start",
                    return_value={"ok": False, "reason": "restore-blocked"},
                ),
                patch.object(state, "snapshot", return_value={}),
            ):
                blocked = state.start_continuous_runtime(
                    "crypto",
                    "SMALL_LIVE",
                )
            self.assertFalse(blocked["ok"])
            self.assertEqual("MONITOR", state.STATE["mode"])
            self.assertEqual(
                "MONITOR",
                state.STATE["automation"]["crypto"]["mode"],
            )

            with (
                patch.object(
                    state.LIVE_CONTINUOUS_CONTROLLER,
                    "start",
                    return_value={"ok": True, "reason": "started"},
                ),
                patch.object(state, "snapshot", return_value={}),
            ):
                started = state.start_continuous_runtime(
                    "crypto",
                    "SMALL_LIVE",
                )
            self.assertTrue(started["ok"])
            self.assertEqual("SMALL_LIVE", state.STATE["mode"])
            self.assertEqual(
                "SMALL_LIVE",
                state.STATE["automation"]["crypto"]["mode"],
            )
            self.assertTrue(state.STATE["automation"]["crypto"]["enabled"])
        finally:
            state.STATE.clear()
            state.STATE.update(original_state)

    def test_controller_stop_releases_due_bar_lock_before_worker_join(self) -> None:
        controller = LiveContinuousController(Path("."))
        controller.profile_id = "crypto"
        controller.mode = "SMALL_LIVE"
        due_bar_acquired = threading.Event()

        engine = MagicMock()

        def transition_mode(mode: str) -> None:
            self.assertEqual("MONITOR", mode)
            self.assertTrue(state.RUNTIME_MODE_LOCK._is_owned())  # type: ignore[attr-defined]
            return None

        engine.transition_mode.side_effect = transition_mode

        class DueBarSupervisor:
            def __init__(self) -> None:
                self.engine = engine
                self.running = True
                self.worker: threading.Thread | None = None

            def stop(self) -> dict[str, object]:
                def flush_due_bar() -> None:
                    with state.RUNTIME_MODE_LOCK:
                        due_bar_acquired.set()

                self.worker = threading.Thread(target=flush_due_bar)
                self.worker.start()
                self.worker.join(0.5)
                alive = self.worker.is_alive()
                return {
                    "phase": "FAILED" if alive else "STOPPED",
                    "running": alive,
                    "lastError": "runtime-stop-timeout" if alive else "",
                }

        supervisor = DueBarSupervisor()
        controller.supervisor = supervisor  # type: ignore[assignment]
        with (
            patch.object(state, "snapshot", return_value={}),
            patch.object(state, "append_audit"),
        ):
            result = controller.stop()

        if supervisor.worker is not None:
            supervisor.worker.join(1.0)
        self.assertTrue(result["ok"])
        self.assertTrue(due_bar_acquired.is_set())
        self.assertEqual("MONITOR", controller.mode)

    def test_controller_stop_propagates_supervisor_timeout_as_failure(self) -> None:
        controller = LiveContinuousController(Path("."))
        controller.profile_id = "crypto"
        controller.mode = "SMALL_LIVE"
        supervisor = MagicMock()
        supervisor.engine.transition_mode.return_value = None
        supervisor.stop.return_value = {
            "phase": "FAILED",
            "running": True,
            "lastError": "runtime-stop-timeout",
        }
        controller.supervisor = supervisor

        with (
            patch.object(state, "snapshot", return_value={}),
            patch.object(state, "append_audit") as audit,
        ):
            result = controller.stop()

        self.assertFalse(result["ok"])
        self.assertEqual("runtime-stop-timeout", result["reason"])
        self.assertEqual("danger", audit.call_args.args[0])

    def test_controller_rejects_restart_while_failed_thread_is_alive(self) -> None:
        controller = LiveContinuousController(Path("."))
        supervisor = MagicMock()
        supervisor.running = True
        supervisor.snapshot.return_value = {
            "phase": "FAILED",
            "running": True,
            "lastError": "runtime-stop-timeout",
        }
        controller.supervisor = supervisor

        with patch.object(state, "snapshot", return_value={}):
            result = controller.start("crypto", "MONITOR")

        self.assertFalse(result["ok"])
        self.assertIn("FAILED", result["reason"])
        supervisor.engine.transition_mode.assert_not_called()

    def test_state_stop_does_not_hold_runtime_lock_across_controller_join(self) -> None:
        due_bar_acquired = threading.Event()
        workers: list[threading.Thread] = []

        def stop_after_due_bar(_profile_id: str) -> dict[str, object]:
            def flush_due_bar() -> None:
                with state.RUNTIME_MODE_LOCK:
                    due_bar_acquired.set()

            worker = threading.Thread(target=flush_due_bar)
            workers.append(worker)
            worker.start()
            worker.join(0.5)
            alive = worker.is_alive()
            return {
                "ok": not alive,
                "reason": (
                    "runtime-stop-timeout"
                    if alive
                    else "continuous runtime stopped"
                ),
                "runtime": {
                    "phase": "FAILED" if alive else "STOPPED",
                    "running": alive,
                },
            }

        with (
            patch.object(
                state.LIVE_CONTINUOUS_CONTROLLER,
                "stop",
                side_effect=stop_after_due_bar,
            ),
            patch.object(state, "sync_runtime_profile_mode"),
            patch.object(state, "snapshot", return_value={}),
        ):
            result = state.stop_continuous_runtime("crypto")

        for worker in workers:
            worker.join(1.0)
        self.assertTrue(result["ok"])
        self.assertTrue(due_bar_acquired.is_set())

    def test_public_start_and_stop_share_one_control_order_without_runtime_inversion(self) -> None:
        start_entered = threading.Event()
        release_start = threading.Event()
        stop_entered = threading.Event()
        results: dict[str, dict] = {}
        failures: list[BaseException] = []

        def controlled_start(*_args, **_kwargs):
            self.assertFalse(state.RUNTIME_MODE_LOCK._is_owned())  # type: ignore[attr-defined]
            start_entered.set()
            self.assertTrue(release_start.wait(2.0))
            return {"ok": True, "reason": "started"}

        def controlled_stop(*_args, **_kwargs):
            self.assertFalse(state.RUNTIME_MODE_LOCK._is_owned())  # type: ignore[attr-defined]
            stop_entered.set()
            return {
                "ok": True,
                "reason": "stopped",
                "runtime": {"phase": "STOPPED", "running": False},
            }

        def run(label: str, callback) -> None:
            try:
                results[label] = callback()
            except BaseException as exc:  # pragma: no cover - assertion relay
                failures.append(exc)

        with (
            patch.object(
                state.LIVE_CONTINUOUS_CONTROLLER,
                "start",
                side_effect=controlled_start,
            ),
            patch.object(
                state.LIVE_CONTINUOUS_CONTROLLER,
                "stop",
                side_effect=controlled_stop,
            ),
            patch.object(state, "snapshot", return_value={}),
        ):
            starter = threading.Thread(
                target=run,
                args=(
                    "start",
                    lambda: state.start_continuous_runtime(
                        "crypto",
                        "MONITOR",
                    ),
                ),
            )
            stopper = threading.Thread(
                target=run,
                args=(
                    "stop",
                    lambda: state.stop_continuous_runtime("crypto"),
                ),
            )
            starter.start()
            self.assertTrue(start_entered.wait(1.0))
            stopper.start()
            self.assertFalse(stop_entered.wait(0.1))
            release_start.set()
            starter.join(2.0)
            stopper.join(2.0)

        self.assertFalse(starter.is_alive())
        self.assertFalse(stopper.is_alive())
        self.assertFalse(failures)
        self.assertTrue(stop_entered.is_set())
        self.assertTrue(results["start"]["ok"])
        self.assertTrue(results["stop"]["ok"])

    def test_set_mode_enters_manager_without_holding_cycle_lock(self) -> None:
        readiness = {
            "summary": {"blocker_count": 0, "warning_count": 0},
            "watchdog": {"critical_count": 0},
        }

        def transition(mode: str) -> dict[str, object]:
            self.assertEqual("MONITOR", mode)
            self.assertFalse(state.RUNTIME_MODE_LOCK._is_owned())  # type: ignore[attr-defined]
            return {"ok": True, "results": {}}

        with (
            patch.object(state, "snapshot", return_value=readiness),
            patch.object(
                state.LIVE_CONTINUOUS_CONTROLLER,
                "transition_running",
                side_effect=transition,
            ),
            patch.object(state, "append_audit"),
        ):
            result = state.set_mode("MONITOR")

        self.assertTrue(result["ok"])

    def test_stop_api_preserves_supervisor_failure(self) -> None:
        failure = {
            "ok": False,
            "reason": "runtime-stop-timeout",
            "runtime": {
                "phase": "FAILED",
                "running": True,
                "lastError": "runtime-stop-timeout",
            },
        }
        with (
            patch.object(
                state.LIVE_CONTINUOUS_CONTROLLER,
                "stop",
                return_value=failure,
            ),
            patch.object(state, "sync_runtime_profile_mode"),
            patch.object(state, "snapshot", return_value={}),
        ):
            result = state.stop_continuous_runtime("crypto")

        self.assertFalse(result["ok"])
        self.assertEqual("runtime-stop-timeout", result["reason"])
        self.assertEqual("FAILED", result["runtime"]["phase"])

    def test_multi_profile_stop_aggregates_child_failure(self) -> None:
        manager = LiveContinuousRuntimeManager(Path("."))
        manager.controllers["stock"] = MagicMock()
        manager.controllers["crypto"] = MagicMock()
        manager.controllers["stock"].stop.return_value = {
            "ok": True,
            "runtime": {"phase": "STOPPED", "running": False},
        }
        manager.controllers["crypto"].stop.return_value = {
            "ok": False,
            "reason": "runtime-stop-timeout",
            "runtime": {"phase": "FAILED", "running": True},
        }
        manager.controllers["stock"].snapshot.return_value = {
            "running": False,
        }
        manager.controllers["crypto"].snapshot.return_value = {
            "running": True,
        }

        result = manager.stop()

        self.assertFalse(result["ok"])
        self.assertIn("crypto", result["reason"])

    def test_global_mode_stays_unchanged_when_runtime_transition_fails(self) -> None:
        original_state = copy.deepcopy(state.STATE)
        state.STATE["mode"] = "MONITOR"
        try:
            readiness = {
                "summary": {"blocker_count": 0, "warning_count": 0},
                "watchdog": {"critical_count": 0},
            }
            with (
                patch.object(state, "snapshot", return_value=readiness),
                patch.object(state, "append_audit"),
                patch.object(
                    state.LIVE_CONTINUOUS_CONTROLLER,
                    "transition_running",
                    return_value={
                        "ok": False,
                        "reason": "crypto restore blocked",
                        "results": {},
                    },
                ),
            ):
                result = state.set_mode("SMALL_LIVE")

            self.assertFalse(result["ok"])
            self.assertEqual("MONITOR", state.STATE["mode"])
        finally:
            state.STATE.clear()
            state.STATE.update(original_state)


if __name__ == "__main__":
    unittest.main()
