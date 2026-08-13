from __future__ import annotations

import os
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from live_trader.binance_order_authority import (
    BinanceOrderAuthorityError,
    _reset_binance_order_authority_reader_for_tests,
    binance_route_authority_serialization,
    functional_binance_final_mutation_boundary,
    ordinary_binance_final_mutation_boundary,
    register_binance_order_authority_reader,
)
from live_trader.emergency_stop import (
    _reset_emergency_stop_sticky_for_tests,
    engage_emergency_stop,
)


class BinanceOrderAuthorityTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.previous_stop_path = os.environ.get(
            "LIVE_TRADER_EMERGENCY_STOP_PATH"
        )
        os.environ["LIVE_TRADER_EMERGENCY_STOP_PATH"] = str(
            Path(self.temporary.name) / "emergency-stop.json"
        )
        _reset_emergency_stop_sticky_for_tests()
        _reset_binance_order_authority_reader_for_tests()
        self.snapshot = {
            "functionalAuthorityOpen": False,
            "functionalPhase": "IDLE",
            "functionalRevision": 0,
            "functionalSessionId": "",
            "functionalAccountFingerprint": "a" * 64,
            "applicationInstanceLeaseHeld": True,
            "ordinaryRoutesClosed": True,
        }
        register_binance_order_authority_reader(
            lambda: dict(self.snapshot)
        )

    def tearDown(self) -> None:
        _reset_binance_order_authority_reader_for_tests()
        state_module = sys.modules.get("live_trader.state")
        if state_module is not None:
            register_binance_order_authority_reader(
                state_module._binance_order_authority_snapshot
            )
        _reset_emergency_stop_sticky_for_tests()
        if self.previous_stop_path is None:
            os.environ.pop("LIVE_TRADER_EMERGENCY_STOP_PATH", None)
        else:
            os.environ["LIVE_TRADER_EMERGENCY_STOP_PATH"] = (
                self.previous_stop_path
            )
        self.temporary.cleanup()

    def activate(self, *, phase: str = "ACTIVE", revision: int = 7) -> None:
        self.snapshot.update(
            {
                "functionalAuthorityOpen": True,
                "functionalPhase": phase,
                "functionalRevision": revision,
                "functionalSessionId": "session-exact-1",
            }
        )

    def test_functional_pointer_blocks_every_ordinary_final_mutation(self) -> None:
        self.activate()
        with self.assertRaisesRegex(
            BinanceOrderAuthorityError, "functional authority"
        ):
            with ordinary_binance_final_mutation_boundary(
                operation="SPOT_PLACE_ORDER"
            ):
                self.fail("ordinary sender must not be reached")

    def test_futures_test_order_smoke_cannot_reach_signed_post(self) -> None:
        from live_trader.brokers import (
            BrokerNotReadyError,
            LiveBrokerRouter,
        )
        from live_trader.live_adapters import (
            BINANCE_FUTURES_POSITION_MODE_ENDPOINT,
            BINANCE_FUTURES_SYMBOL_CONFIG_ENDPOINT,
        )

        self.activate()
        seen: list[str] = []

        def send(builder, *, futures=False):
            self.assertTrue(futures)
            prepared = builder()
            seen.append(prepared.endpoint)
            payload = (
                {"dualSidePosition": False}
                if prepared.endpoint == BINANCE_FUTURES_POSITION_MODE_ENDPOINT
                else [
                    {
                        "symbol": "BTCUSDT",
                        "marginType": "ISOLATED",
                        "leverage": 1,
                    }
                ]
            )
            return {"ok": True, "statusCode": 200, "json": payload, "text": ""}

        normalized = {
            "symbol": "BTCUSDT",
            "side": "SELL",
            "order_type": "MARKET",
            "quantity": "0.001",
            "qty": "0.001",
            "position_direction": "short",
            "risk_reducing": False,
            "max_leverage": 1,
            "required_margin_type": "ISOLATED",
        }
        with (
            patch(
                "live_trader.brokers.normalize_binance_futures_intent",
                return_value=normalized,
            ),
            patch(
                "live_trader.brokers.send_binance_signed_request",
                side_effect=send,
            ),
            self.assertRaises(BrokerNotReadyError),
        ):
            LiveBrokerRouter().test_binance_futures_order(normalized)
        self.assertEqual(
            [
                BINANCE_FUTURES_POSITION_MODE_ENDPOINT,
                BINANCE_FUTURES_SYMBOL_CONFIG_ENDPOINT,
            ],
            seen,
        )

    def test_stop_is_serialized_with_entry_and_revision_is_rechecked(self) -> None:
        self.activate()
        sender_entered = threading.Event()
        release_sender = threading.Event()
        stop_finished = threading.Event()
        events: list[str] = []

        def sender() -> None:
            with functional_binance_final_mutation_boundary(
                session_id="session-exact-1",
                cleanup_only=False,
                expected_revision="binance-functional-control-7",
            ) as read:
                sender_entered.set()
                self.assertTrue(read()["active"])
                release_sender.wait(2)
                events.append("post")

        def stop() -> None:
            with binance_route_authority_serialization():
                self.snapshot.update(
                    {
                        "functionalPhase": "CLEANUP",
                        "functionalRevision": 8,
                    }
                )
                events.append("stop")
            stop_finished.set()

        sender_thread = threading.Thread(target=sender)
        stop_thread = threading.Thread(target=stop)
        sender_thread.start()
        self.assertTrue(sender_entered.wait(1))
        stop_thread.start()
        time.sleep(0.05)
        self.assertFalse(stop_finished.is_set())
        release_sender.set()
        sender_thread.join(2)
        stop_thread.join(2)
        self.assertEqual(["post", "stop"], events)
        with self.assertRaises(BinanceOrderAuthorityError):
            with functional_binance_final_mutation_boundary(
                session_id="session-exact-1",
                cleanup_only=False,
                expected_revision="binance-functional-control-7",
            ):
                pass

    def test_kill_blocks_entry_and_ordinary_but_allows_exact_cleanup(self) -> None:
        self.activate(phase="ACTIVE")
        killed = engage_emergency_stop("test kill", source="unit-test")
        self.assertTrue(killed["active"])
        with self.assertRaises(BinanceOrderAuthorityError):
            with ordinary_binance_final_mutation_boundary(
                operation="FUTURES_CANCEL_ORDER"
            ):
                pass
        with self.assertRaises(BinanceOrderAuthorityError):
            with functional_binance_final_mutation_boundary(
                session_id="session-exact-1",
                cleanup_only=False,
                expected_revision="binance-functional-control-7",
            ):
                pass
        self.snapshot.update(
            {"functionalPhase": "CLEANUP", "functionalRevision": 8}
        )
        with functional_binance_final_mutation_boundary(
            session_id="session-exact-1",
            cleanup_only=True,
            expected_revision="binance-functional-control-8",
        ) as read:
            self.assertTrue(read()["active"])
            self.assertTrue(read()["emergencyActive"])


if __name__ == "__main__":
    unittest.main()
