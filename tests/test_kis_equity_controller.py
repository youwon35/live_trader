from copy import deepcopy
import contextlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from live_trader import state
from live_trader.continuous_live import LiveContinuousController
from live_trader.native_order_ticket import validate_native_ticket
from trading_runtime import OpenBoundary, ClosedBar
from trading_runtime.realtime_feeds import KisWebSocketClosedBarFeed
import test_kis_equity_operational as fixtures


class KisEquityControllerTest(unittest.TestCase):
    def setUp(self):
        # Import-time wall stamps become stale in a broad security suite.
        # Each case still exercises the unchanged real 15-second guard.
        clock_patch = patch.object(fixtures, "NOW", datetime.now(timezone.utc))
        clock_patch.start()
        self.addCleanup(clock_patch.stop)
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.root = Path(self.folder.name)

    def test_native_equity_start_uses_owned_equity_runtime_without_spot_runtime(self):
        selected, contract, context = fixtures.fixture()
        controller = LiveContinuousController(self.root)
        native = Mock()
        native.deployment_id = "deployment"
        with patch("live_trader.kis_equity_runtime.NativeEquityRuntime", return_value=native) as create, patch("live_trader.native_spot_runtime.NativeSpotRuntime") as spot:
            self.assertIs(native, controller._ensure_native_runtime((selected,), "deployment"))
        create.assert_called_once_with(self.root, "deployment", (selected,))
        native.initialize_at_reviewed_manual_start.assert_called_once_with()
        spot.assert_not_called()
        self.assertEqual(controller._native_equity_mark, native.mark_provider)

    def test_equity_context_cannot_fall_back_to_spot_quantity_or_fixed_one_share(self):
        selected, contract, context = fixtures.fixture()
        selected.artifact["market_type"] = "stock"
        controller = LiveContinuousController(self.root)
        controller.deployment_id = "deployment"
        controller.native_context_provider = Mock(return_value={"status": "BLOCKED"})
        with patch.object(state, "account_risk_for_intent", side_effect=AssertionError("must not replace missing equity scope with account-wide cash")):
            result = controller._native_quantity_result(selected, "BUY", 100, 0, (selected,))
        self.assertEqual("BLOCKED", result["status"])
        with self.assertRaisesRegex(ValueError, "native quantity requires"):
            controller._order_quantity(selected, 100)

    def test_other_member_mark_uses_owned_production_ws_current_provider_bucket(self):
        selected, contract, context = fixtures.fixture()
        controller = LiveContinuousController(self.root)
        feed = object.__new__(KisWebSocketClosedBarFeed)
        feed.demo = False
        feed._websocket_owner_active = True
        feed._connected_at = fixtures.NOW.timestamp() - 5
        start = fixtures.NOW - timedelta(seconds=1)
        bar = ClosedBar(selected.instrument_id, selected.symbol, "kis", selected.timeframe,
                        start.isoformat(), (start + timedelta(minutes=5)).isoformat(),
                        100, 102, 100, 102, 1, received_time=fixtures.NOW.isoformat(), event_count=1, source_sequence="kis:fixture-current-provider-window")
        feed._active = {("member", selected.timeframe): bar}
        controller.supervisor = SimpleNamespace(feeds=(feed,), engine=SimpleNamespace(specs=(selected,)))
        mark = controller._native_equity_mark(selected)
        self.assertEqual("102.0" if isinstance(bar.close, float) else "102", mark["price"])
        self.assertEqual("KIS_WEBSOCKET", mark["attestation"])
        self.assertEqual(fixtures.NOW.isoformat(), mark["observedAt"])
        for change in ("demo", "no-owner", "old-provider-window", "rest-warmup"):
            with self.subTest(change=change):
                feed.demo = change == "demo"
                feed._websocket_owner_active = change != "no-owner"
                from dataclasses import replace
                if change == "old-provider-window":
                    feed._active = {("member", selected.timeframe): replace(bar, start_time=(fixtures.NOW - timedelta(minutes=5)).isoformat(), end_time=fixtures.NOW.isoformat())}
                elif change == "rest-warmup":
                    feed._active = {("member", selected.timeframe): replace(bar, evidence_class="HISTORICAL_WARMUP")}
                else:
                    feed._active = {("member", selected.timeframe): bar}
                with self.assertRaisesRegex(ValueError, "FRESH_MARK_REQUIRED"):
                    controller._native_equity_mark(selected)

    def test_monitor_observed_exact_boundary_can_mark_a_held_member_without_trading(self):
        selected, contract, context = fixtures.fixture()
        controller = LiveContinuousController(self.root)
        controller.supervisor = SimpleNamespace(feeds=(), engine=SimpleNamespace(specs=(selected,)))
        opening = OpenBoundary(selected.instrument_id, selected.symbol, "kis", selected.timeframe,
                               fixtures.NOW.isoformat(), 101, fixtures.NOW.isoformat(), "exact-source", "a" * 64, "KIS_WEBSOCKET", True)
        self.assertEqual("MONITOR", controller._handle_native_open_boundary(opening)["action"])
        self.assertEqual("101", controller._native_equity_mark(selected)["price"])

    def test_all_kis_venues_closed_signal_real_next_open_quantity_limit_ticket_without_order(self):
        for exchange in fixtures.service.VENUES:
            with self.subTest(exchange=exchange):
                selected, contract, context = fixtures.fixture(exchange)
                selected.artifact["market_type"] = "stock"
                original_source = deepcopy(selected.artifact)
                controller = LiveContinuousController(self.root / exchange)
                controller.profile_id = "stock"
                controller.mode = "SMALL_LIVE"
                controller.deployment_id = "deployment"
                controller.requested_strategy_id = selected.strategy_id
                controller.strategy_ids = (selected.strategy_id,)
                controller.allowed_symbols = (selected.symbol,)
                controller.supervisor = SimpleNamespace(engine=SimpleNamespace(specs=(selected,)))
                def provide(spec, side, price, specs, boundary):
                    if side == "POSITION":
                        return {"equityScope": "OWNED_DEPLOYMENT_ONLY", "ownedPositions": {selected.strategy_instance_id: "0"}}
                    return {**context, "exactOpenBoundary": boundary}
                controller.native_context_provider = provide
                controller._dispatcher = Mock(return_value=contextlib.nullcontext())
                captured = []
                signal = SimpleNamespace(strategy_id=selected.strategy_id, strategy_instance_id=selected.strategy_instance_id,
                                         signal="BUY", reason="fixture-source-signal", evaluation_key="evaluation-exact",
                                         bar=SimpleNamespace(end_time=fixtures.NOW.isoformat(), close=999))
                opening = OpenBoundary(instrument_id=selected.instrument_id, symbol=selected.symbol, provider="kis", timeframe=selected.timeframe,
                                       start_time=fixtures.NOW.isoformat(), open=100, observed_time=(fixtures.NOW + timedelta(milliseconds=1)).isoformat(),
                                       source_sequence="synthetic-next-open", raw_hash="a" * 64, attestation="KIS_WEBSOCKET", promotion_eligible=True)
                with patch.object(controller, "_submit_cycle_intent", side_effect=lambda intent, **kw: captured.append(intent) or {"ok": True, "reason": "FIXTURE_TICKET_ONLY"}), patch.object(state, "append_audit"), patch.dict(state.STATE, {"strategy_runner": {}}), patch("live_trader.native_open_boundary.next_tradable_boundary_start", return_value=fixtures.NOW):
                    staged = controller._handle_cycle_locked(SimpleNamespace(decisions=(signal,)))
                    self.assertEqual("WAIT_OPEN_BOUNDARY", staged["results"][0]["action"])
                    self.assertEqual([], captured)
                    result = controller._handle_native_open_boundary(opening)
                self.assertTrue(result["results"][0]["ok"], result)
                self.assertEqual(1, len(captured))
                intent = captured[0]
                wire = validate_native_ticket(intent)
                self.assertEqual("00", intent.metadata["order_type"])
                self.assertEqual(exchange, intent.metadata["exchange"])
                self.assertEqual(exchange, wire["exchange"])
                self.assertEqual("00", wire["orderClass"])
                self.assertEqual(3, intent.quantity)
                self.assertNotEqual(999, intent.reference_price)
                self.assertFalse(intent.metadata["native_quantity_model"]["actualPersonalFeeObserved"])
                self.assertFalse(wire["orderAuthorityGranted"])
                self.assertEqual(original_source, selected.artifact)


if __name__ == "__main__":
    unittest.main()
