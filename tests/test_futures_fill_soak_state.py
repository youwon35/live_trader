from __future__ import annotations

import copy
from contextlib import contextmanager
import threading
import unittest
from unittest.mock import patch

from live_trader import state
from live_trader import binance_order_authority as binance_authority


class FakeFillSoakSession:
    preview_payload: dict[str, object] = {
        "ready": True,
        "blockers": [],
        "available_usdt": "20",
        "equity_usdt": "20",
        "margin_type": "ISOLATED",
        "leverage": "1",
        "hedge_mode": True,
    }

    def __init__(self, config) -> None:
        self.config = config
        self.run_calls = []
        self.stop_requested = False
        self.finished = threading.Event()

    def preview(self) -> dict[str, object]:
        return dict(self.preview_payload)

    def status(self) -> dict[str, object]:
        return {
            "session_id": self.config.session_id,
            "phase": "FINISHED" if self.finished.is_set() else "CREATED",
            "stop_requested": self.stop_requested,
        }

    def run(self, authorization) -> dict[str, object]:
        self.run_calls.append(authorization)
        report = {
            "schema_version": "binance-usdm-fill-soak-v1",
            "session_id": self.config.session_id,
            "status": "PASS",
            "reason_ids": [],
            "progress": {
                "round_trips_completed": 3,
                "fill_count": 6,
            },
            "risk": {"max_drawdown_pct": "0.1"},
            "final_checks": {
                "duration_complete": True,
                "flat": True,
                "open_orders_clear": True,
            },
            "strategy_promotion_authorized": False,
        }
        self.finished.set()
        return report

    def request_stop(self) -> None:
        self.stop_requested = True


class FuturesFillSoakStateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.original_factory = state.BINANCE_FUTURES_FILL_SOAK_SESSION_FACTORY
        self.original_session = state.BINANCE_FUTURES_FILL_SOAK_SESSION
        self.original_thread = state.BINANCE_FUTURES_FILL_SOAK_THREAD
        self.original_internal = copy.deepcopy(
            state.BINANCE_FUTURES_FILL_SOAK_INTERNAL
        )
        state.BINANCE_FUTURES_FILL_SOAK_SESSION_FACTORY = (
            FakeFillSoakSession
        )
        state.BINANCE_FUTURES_FILL_SOAK_SESSION = None
        state.BINANCE_FUTURES_FILL_SOAK_THREAD = None
        state.BINANCE_FUTURES_FILL_SOAK_INTERNAL.update(
            {
                "phase": "idle",
                "status": "IDLE",
                "confirmation_token_hash": "",
                "confirmation_issued_epoch": 0.0,
                "confirmation_expires_epoch": 0.0,
                "confirmation_used": False,
                "preview": {},
                "final_report": {},
            }
        )

    def tearDown(self) -> None:
        thread = state.BINANCE_FUTURES_FILL_SOAK_THREAD
        if thread is not None:
            thread.join(timeout=2)
        state.BINANCE_FUTURES_FILL_SOAK_SESSION_FACTORY = (
            self.original_factory
        )
        state.BINANCE_FUTURES_FILL_SOAK_SESSION = self.original_session
        state.BINANCE_FUTURES_FILL_SOAK_THREAD = self.original_thread
        state.BINANCE_FUTURES_FILL_SOAK_INTERNAL.clear()
        state.BINANCE_FUTURES_FILL_SOAK_INTERNAL.update(
            self.original_internal
        )

    def test_preview_returns_one_time_token_but_status_never_exposes_it(
        self,
    ) -> None:
        response = state.preview_binance_futures_fill_soak("BTCUSDT")

        self.assertTrue(response["ok"])
        session = state.BINANCE_FUTURES_FILL_SOAK_SESSION
        self.assertIs(
            state._binance_futures_fill_soak_dispatch_boundary,
            session.dispatch_boundary,
        )
        token = response["authorization"]["confirmation_token"]
        self.assertGreater(len(token), 20)
        public_status = state.binance_futures_fill_soak_status()
        self.assertNotIn("confirmation_token", repr(public_status))
        self.assertNotIn(token, repr(public_status))
        self.assertTrue(public_status["confirmation_used"] is False)

    def test_invalid_token_never_starts_session(self) -> None:
        state.preview_binance_futures_fill_soak("BTCUSDT")
        session = state.BINANCE_FUTURES_FILL_SOAK_SESSION

        response = state.start_binance_futures_fill_soak(
            "wrong-token",
            confirmed=True,
        )

        self.assertFalse(response["ok"])
        self.assertEqual([], session.run_calls)

    def test_valid_token_is_consumed_before_background_run(self) -> None:
        preview = state.preview_binance_futures_fill_soak("BTCUSDT")
        token = preview["authorization"]["confirmation_token"]
        session = state.BINANCE_FUTURES_FILL_SOAK_SESSION
        issued = state.issue_safety_confirmation(
            "BINANCE_FUTURES_FILL_SOAK_START",
            {"symbol": "BTCUSDT"},
        )

        response = state.start_binance_futures_fill_soak(
            token,
            confirmed=True,
            safety_confirmation={
                "challengeId": issued["challengeId"],
                "token": issued["token"],
                "typedPhrase": issued["expectedPhrase"],
            },
        )
        state.BINANCE_FUTURES_FILL_SOAK_THREAD.join(timeout=2)

        self.assertTrue(response["ok"])
        self.assertTrue(session.finished.is_set())
        self.assertEqual(1, len(session.run_calls))
        self.assertEqual(
            "",
            state.BINANCE_FUTURES_FILL_SOAK_INTERNAL[
                "confirmation_token_hash"
            ],
        )
        self.assertTrue(
            state.BINANCE_FUTURES_FILL_SOAK_INTERNAL["confirmation_used"]
        )
        status = state.binance_futures_fill_soak_status()
        self.assertEqual("PASS", status["status"])
        self.assertFalse(
            status["final_report"]["strategy_promotion_authorized"]
        )

    def test_blocked_preview_cannot_issue_or_start(self) -> None:
        FakeFillSoakSession.preview_payload = {
            "ready": False,
            "blockers": ["available-usdt-invalid"],
        }
        try:
            response = state.preview_binance_futures_fill_soak("BTCUSDT")
        finally:
            FakeFillSoakSession.preview_payload = {
                "ready": True,
                "blockers": [],
                "available_usdt": "20",
                "equity_usdt": "20",
                "margin_type": "ISOLATED",
                "leverage": "1",
                "hedge_mode": True,
            }

        self.assertFalse(response["ok"])
        self.assertEqual({}, response["authorization"])
        self.assertEqual(
            "BLOCKED",
            response["fill_soak"]["status"],
        )

    @staticmethod
    def _production_strategy() -> dict[str, object]:
        return {
            "strategy_id": "binance-futures-qualified",
            "symbol": "BTCUSDT",
            "broker_id": "binance-futures",
            "strategy_instance_id": "bf-instance-1",
            "portfolio_gate": {},
        }

    @staticmethod
    def _entry_payload() -> dict[str, object]:
        return {
            "strategy_id": "binance-futures-qualified",
            "symbol": "BTCUSDT",
            "side": "SELL",
            "quantity": "0.001",
            "price": "6000",
            "order_type": "MARKET",
            "position_direction": "short",
            "risk_reducing": False,
            "reduce_only": False,
            "soak_leg": "entry",
            "canary_scope": {"eligible": True, "scopeId": "a" * 64},
        }

    def test_every_fill_soak_entry_revalidates_common_gate_scope_and_runtime(
        self,
    ) -> None:
        original_mode = state.STATE.get("mode")
        state.STATE["mode"] = "SMALL_LIVE"
        try:
            with patch.object(
                state,
                "strategy_rows",
                return_value=[self._production_strategy()],
            ), patch.object(
                state,
                "live_broker_dispatch_allowed",
                return_value=(True, ""),
            ) as broker_gate, patch.object(
                state,
                "exact_live_canary_scope_dispatch_allowed",
                return_value=(True, "", {"eligible": True, "scopeId": "a" * 64}),
            ) as scope_gate, patch.object(
                state,
                "operational_runtime_dispatch_allowed",
                return_value=(True, "operational-runtime-authorized", {}),
            ) as runtime_gate:
                result = state._authorize_binance_futures_fill_soak_dispatch(
                    self._entry_payload()
                )
        finally:
            state.STATE["mode"] = original_mode

        self.assertTrue(result[0])
        self.assertEqual("new-entry", result[2]["dispatchPath"])
        broker_gate.assert_called_once()
        self.assertFalse(broker_gate.call_args.kwargs["dry_run"])
        scope_gate.assert_called_once()
        runtime_gate.assert_called_once()

    def test_fill_soak_entry_propagates_each_mutable_safety_block(self) -> None:
        original_mode = state.STATE.get("mode")
        state.STATE["mode"] = "SMALL_LIVE"
        try:
            for reason in (
                "dry-run-broker-dispatch-forbidden",
                "risk-increasing-order-blocked",
                "operator-confirmation-required",
                "live-runtime-intent-mode-mismatch",
            ):
                with self.subTest(reason=reason), patch.object(
                    state,
                    "strategy_rows",
                    return_value=[self._production_strategy()],
                ), patch.object(
                    state,
                    "live_broker_dispatch_allowed",
                    return_value=(False, reason),
                ), patch.object(
                    state,
                    "exact_live_canary_scope_dispatch_allowed",
                ) as scope_gate, patch.object(
                    state,
                    "operational_runtime_dispatch_allowed",
                ) as runtime_gate:
                    result = state._authorize_binance_futures_fill_soak_dispatch(
                        self._entry_payload()
                    )
                self.assertFalse(result[0])
                self.assertEqual(reason, result[1])
                scope_gate.assert_not_called()
                runtime_gate.assert_not_called()
        finally:
            state.STATE["mode"] = original_mode

    def test_fill_soak_entry_blocks_changed_exact_canary_scope(self) -> None:
        original_mode = state.STATE.get("mode")
        state.STATE["mode"] = "SMALL_LIVE"
        try:
            with patch.object(
                state,
                "strategy_rows",
                return_value=[self._production_strategy()],
            ), patch.object(
                state,
                "live_broker_dispatch_allowed",
                return_value=(True, ""),
            ), patch.object(
                state,
                "exact_live_canary_scope_dispatch_allowed",
                return_value=(
                    False,
                    "live-canary-order-scope-changed:currentDeploymentRevision",
                    {"eligible": True, "scopeId": "b" * 64},
                ),
            ), patch.object(
                state,
                "operational_runtime_dispatch_allowed",
            ) as runtime_gate:
                result = state._authorize_binance_futures_fill_soak_dispatch(
                    self._entry_payload()
                )
        finally:
            state.STATE["mode"] = original_mode

        self.assertFalse(result[0])
        self.assertIn("scope-changed", result[1])
        runtime_gate.assert_not_called()

    def test_fill_soak_recovery_cover_uses_explicit_reduce_only_path(self) -> None:
        payload = {
            **self._entry_payload(),
            "side": "BUY",
            "risk_reducing": True,
            "reduce_only": True,
            "soak_leg": "recovery-cover",
        }
        with patch.object(
            state,
            "strategy_rows",
            return_value=[self._production_strategy()],
        ), patch.object(
            state,
            "futures_risk_reducing_verified",
            return_value=True,
        ) as reducing, patch.object(
            state,
            "live_broker_dispatch_allowed",
        ) as broker_gate, patch.object(
            state,
            "exact_live_canary_scope_dispatch_allowed",
        ) as scope_gate, patch.object(
            state,
            "operational_runtime_dispatch_allowed",
            return_value=(True, "operational-runtime-authorized", {}),
        ) as runtime_gate:
            result = state._authorize_binance_futures_fill_soak_dispatch(
                payload
            )

        self.assertTrue(result[0])
        self.assertTrue(result[2]["recoveryReduceOnly"])
        reducing.assert_called_once()
        broker_gate.assert_not_called()
        scope_gate.assert_not_called()
        runtime_gate.assert_called_once()

    def test_fill_soak_normal_send_and_kill_share_canonical_boundary_without_deadlock(
        self,
    ) -> None:
        normal_has_route = threading.Event()
        allow_normal_emergency = threading.Event()
        kill_entered = threading.Event()
        release_kill = threading.Event()
        errors: list[BaseException] = []
        emergency_entries: list[str] = []
        real_emergency = binance_authority.emergency_stop_dispatch_boundary

        @contextmanager
        def paused_emergency():
            name = threading.current_thread().name
            if name == "normal-binance-send":
                normal_has_route.set()
                if not allow_normal_emergency.wait(timeout=2):
                    raise AssertionError("normal sender was not released")
            with real_emergency():
                emergency_entries.append(name)
                yield {"active": False, "revision": "test-off"}

        @contextmanager
        def forbidden_emergency_first():
            raise AssertionError(
                "fill-soak attempted emergency before Binance route authority"
            )
            yield {}  # pragma: no cover

        snapshot = {
            "functionalAuthorityOpen": False,
            "functionalPhase": "IDLE",
            "functionalRevision": 1,
            "functionalSessionId": "",
            "functionalAccountFingerprint": "",
            "applicationInstanceLeaseHeld": True,
            "ordinaryRoutesClosed": False,
        }

        def run_normal() -> None:
            try:
                with binance_authority.ordinary_binance_final_mutation_boundary(
                    operation="FUTURES_PLACE_ORDER"
                ):
                    pass
            except BaseException as exc:  # pragma: no cover - watchdog evidence
                errors.append(exc)

        def run_soak() -> None:
            try:
                with state._binance_futures_fill_soak_dispatch_boundary():
                    # This is the exact nested boundary reached by
                    # LiveBrokerRouter.place_order().
                    with binance_authority.ordinary_binance_final_mutation_boundary(
                        operation="FUTURES_PLACE_ORDER"
                    ):
                        pass
            except BaseException as exc:  # pragma: no cover - watchdog evidence
                errors.append(exc)

        def run_kill() -> None:
            try:
                with real_emergency():
                    kill_entered.set()
                    if not release_kill.wait(timeout=2):
                        raise AssertionError("Kill boundary was not released")
            except BaseException as exc:  # pragma: no cover - watchdog evidence
                errors.append(exc)

        with patch.object(
            binance_authority, "_snapshot", return_value=snapshot
        ), patch.object(
            binance_authority,
            "emergency_stop_dispatch_boundary",
            paused_emergency,
        ), patch.object(
            state,
            "emergency_stop_dispatch_boundary",
            forbidden_emergency_first,
        ):
            normal = threading.Thread(
                target=run_normal, name="normal-binance-send", daemon=True
            )
            soak = threading.Thread(
                target=run_soak, name="futures-fill-soak", daemon=True
            )
            kill = threading.Thread(
                target=run_kill, name="durable-kill", daemon=True
            )
            normal.start()
            self.assertTrue(normal_has_route.wait(timeout=1))
            soak.start()
            kill.start()
            self.assertTrue(kill_entered.wait(timeout=1))
            release_kill.set()
            kill.join(timeout=1)
            allow_normal_emergency.set()
            normal.join(timeout=1)
            soak.join(timeout=1)

        self.assertFalse(normal.is_alive())
        self.assertFalse(soak.is_alive())
        self.assertFalse(kill.is_alive())
        self.assertEqual([], errors)
        self.assertEqual(
            ["normal-binance-send", "futures-fill-soak"],
            emergency_entries,
        )


if __name__ == "__main__":
    unittest.main()
