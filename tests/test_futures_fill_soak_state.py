from __future__ import annotations

import copy
import threading
import unittest

from live_trader import state


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

        response = state.start_binance_futures_fill_soak(
            token,
            confirmed=True,
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


if __name__ == "__main__":
    unittest.main()
