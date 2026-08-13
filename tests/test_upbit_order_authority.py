from __future__ import annotations

import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from live_trader import state
from live_trader.brokers import BrokerNotReadyError, LiveBrokerRouter
from live_trader.emergency_stop import (
    _reset_emergency_stop_sticky_for_tests,
    engage_emergency_stop,
)
from live_trader.upbit_order_authority import (
    UPBIT_ROUTE_AUTHORITY_LOCK,
    UpbitOrderAuthorityError,
    _reset_upbit_order_authority_reader_for_tests,
    ordinary_upbit_final_mutation_boundary,
    register_upbit_order_authority_reader,
    upbit_route_authority_serialization,
)


class UpbitOrderAuthorityTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.previous_stop_path = os.environ.get(
            "LIVE_TRADER_EMERGENCY_STOP_PATH"
        )
        os.environ["LIVE_TRADER_EMERGENCY_STOP_PATH"] = str(
            Path(self.temporary.name) / "emergency-stop.json"
        )
        _reset_emergency_stop_sticky_for_tests()
        _reset_upbit_order_authority_reader_for_tests()
        self.snapshot = {
            "functionalAuthorityOpen": False,
            "applicationInstanceLeaseHeld": True,
            "ordinaryMutationEnabled": True,
            "ordinaryRoutesClosed": False,
        }
        self.reader = lambda: dict(self.snapshot)
        register_upbit_order_authority_reader(self.reader)

    def tearDown(self) -> None:
        _reset_upbit_order_authority_reader_for_tests()
        register_upbit_order_authority_reader(
            state._upbit_order_authority_snapshot
        )
        _reset_emergency_stop_sticky_for_tests()
        if self.previous_stop_path is None:
            os.environ.pop("LIVE_TRADER_EMERGENCY_STOP_PATH", None)
        else:
            os.environ["LIVE_TRADER_EMERGENCY_STOP_PATH"] = (
                self.previous_stop_path
            )
        self.temporary.cleanup()

    def test_state_and_functional_service_use_the_public_project_lock(
        self,
    ) -> None:
        self.assertIs(
            state.UPBIT_ORDER_AUTHORITY_MUTATION_LOCK,
            UPBIT_ROUTE_AUTHORITY_LOCK,
        )
        self.assertFalse(UPBIT_ROUTE_AUTHORITY_LOCK.owned_by_current_thread())
        with upbit_route_authority_serialization():
            self.assertTrue(
                UPBIT_ROUTE_AUTHORITY_LOCK.owned_by_current_thread()
            )
            other_thread_owned: list[bool] = []
            ownership_reader = threading.Thread(
                target=lambda: other_thread_owned.append(
                    UPBIT_ROUTE_AUTHORITY_LOCK.owned_by_current_thread()
                )
            )
            ownership_reader.start()
            ownership_reader.join(1)
            self.assertFalse(ownership_reader.is_alive())
            self.assertEqual([False], other_thread_owned)
        self.assertFalse(UPBIT_ROUTE_AUTHORITY_LOCK.owned_by_current_thread())

    def test_active_or_unregistered_authority_blocks_direct_router_socket(
        self,
    ) -> None:
        self.snapshot["functionalAuthorityOpen"] = True
        with (
            patch("live_trader.brokers.real_orders_enabled", return_value=True),
            patch(
                "live_trader.brokers.build_upbit_order_request",
                return_value=object(),
            ),
            patch(
                "live_trader.brokers.build_upbit_cancel_order_request",
                return_value=object(),
            ),
            patch("live_trader.brokers.send_prepared_request") as send,
        ):
            with self.assertRaisesRegex(
                BrokerNotReadyError,
                "functional-authority-blocks-ordinary-mutation",
            ):
                LiveBrokerRouter().place_order({"broker_id": "upbit"})
            with self.assertRaisesRegex(
                BrokerNotReadyError,
                "functional-authority-blocks-ordinary-mutation",
            ):
                LiveBrokerRouter().cancel_order("upbit", "order-1")

            _reset_upbit_order_authority_reader_for_tests()
            with self.assertRaisesRegex(
                BrokerNotReadyError,
                "reader is not registered",
            ):
                LiveBrokerRouter().place_order({"broker_id": "upbit"})
            with self.assertRaisesRegex(
                BrokerNotReadyError,
                "reader is not registered",
            ):
                LiveBrokerRouter().cancel_order("upbit", "order-1")
        send.assert_not_called()

    def test_state_outer_and_router_inner_share_one_lease_and_one_send(
        self,
    ) -> None:
        reads = 0

        def reader():
            nonlocal reads
            reads += 1
            return dict(self.snapshot)

        _reset_upbit_order_authority_reader_for_tests()
        register_upbit_order_authority_reader(reader)
        sent: list[object] = []

        def send(prepared: object) -> dict[str, object]:
            self.assertTrue(
                UPBIT_ROUTE_AUTHORITY_LOCK.owned_by_current_thread()
            )
            sent.append(prepared)
            return {"ok": True}

        prepared = object()
        with (
            patch("live_trader.brokers.real_orders_enabled", return_value=True),
            patch(
                "live_trader.brokers.build_upbit_order_request",
                return_value=prepared,
            ),
            patch(
                "live_trader.brokers.send_prepared_request",
                side_effect=send,
            ),
        ):
            with state._ordinary_upbit_final_mutation_boundary("upbit"):
                result = LiveBrokerRouter().place_order(
                    {"broker_id": "upbit"}
                )
        self.assertEqual({"ok": True}, result)
        self.assertEqual([prepared], sent)
        # Outer pre/final reads plus the inherited router-edge final read.
        self.assertEqual(3, reads)

    def test_direct_cancel_has_one_final_read_boundary_and_one_send(
        self,
    ) -> None:
        reads = 0

        def reader():
            nonlocal reads
            reads += 1
            return dict(self.snapshot)

        _reset_upbit_order_authority_reader_for_tests()
        register_upbit_order_authority_reader(reader)
        prepared = object()
        with (
            patch("live_trader.brokers.real_orders_enabled", return_value=True),
            patch(
                "live_trader.brokers.build_upbit_cancel_order_request",
                return_value=prepared,
            ) as build,
            patch(
                "live_trader.brokers.send_prepared_request",
                return_value={"ok": True},
            ) as send,
        ):
            result = LiveBrokerRouter().cancel_order(
                "upbit", "identifier-1", identifier=True
            )
        self.assertEqual({"ok": True}, result)
        build.assert_called_once_with("identifier-1", identifier=True)
        send.assert_called_once_with(prepared)
        self.assertEqual(2, reads)

    def test_final_snapshot_change_blocks_before_socket(self) -> None:
        reads = 0

        def reader():
            nonlocal reads
            reads += 1
            return {
                **self.snapshot,
                "functionalAuthorityOpen": reads >= 2,
            }

        _reset_upbit_order_authority_reader_for_tests()
        register_upbit_order_authority_reader(reader)
        with (
            patch("live_trader.brokers.real_orders_enabled", return_value=True),
            patch(
                "live_trader.brokers.build_upbit_order_request",
                return_value=object(),
            ),
            patch("live_trader.brokers.send_prepared_request") as send,
            self.assertRaisesRegex(
                BrokerNotReadyError,
                "functional-authority-blocks-ordinary-mutation",
            ),
        ):
            LiveBrokerRouter().place_order({"broker_id": "upbit"})
        self.assertEqual(2, reads)
        send.assert_not_called()

    def test_activation_and_send_are_linearized_without_retry(self) -> None:
        sender_entered = threading.Event()
        release_sender = threading.Event()
        activation_finished = threading.Event()
        events: list[str] = []
        failures: list[BaseException] = []

        def send(_prepared: object) -> dict[str, object]:
            sender_entered.set()
            if not release_sender.wait(2):
                raise AssertionError("test sender release timed out")
            events.append("send")
            return {"ok": True}

        def place() -> None:
            try:
                LiveBrokerRouter().place_order({"broker_id": "upbit"})
            except BaseException as exc:  # pragma: no cover - diagnostic
                failures.append(exc)

        def activate() -> None:
            try:
                with upbit_route_authority_serialization():
                    self.snapshot["functionalAuthorityOpen"] = True
                    events.append("activate")
            except BaseException as exc:  # pragma: no cover - diagnostic
                failures.append(exc)
            finally:
                activation_finished.set()

        with (
            patch("live_trader.brokers.real_orders_enabled", return_value=True),
            patch(
                "live_trader.brokers.build_upbit_order_request",
                return_value=object(),
            ),
            patch(
                "live_trader.brokers.send_prepared_request",
                side_effect=send,
            ) as physical_send,
        ):
            place_thread = threading.Thread(target=place)
            activation_thread = threading.Thread(target=activate)
            place_thread.start()
            self.assertTrue(sender_entered.wait(1))
            activation_thread.start()
            self.assertFalse(activation_finished.wait(0.05))
            release_sender.set()
            place_thread.join(2)
            activation_thread.join(2)
            self.assertFalse(place_thread.is_alive())
            self.assertFalse(activation_thread.is_alive())
            self.assertEqual([], failures)
            self.assertEqual(["send", "activate"], events)

            with self.assertRaises(BrokerNotReadyError):
                LiveBrokerRouter().place_order({"broker_id": "upbit"})
        self.assertEqual(1, physical_send.call_count)

    def test_durable_emergency_blocks_place_and_cancel_with_socket_zero(
        self,
    ) -> None:
        self.assertTrue(
            engage_emergency_stop("authority test", source="unit-test")[
                "active"
            ]
        )
        with (
            patch("live_trader.brokers.real_orders_enabled", return_value=True),
            patch(
                "live_trader.brokers.build_upbit_order_request",
                return_value=object(),
            ),
            patch(
                "live_trader.brokers.build_upbit_cancel_order_request",
                return_value=object(),
            ),
            patch("live_trader.brokers.send_prepared_request") as send,
        ):
            with self.assertRaisesRegex(
                BrokerNotReadyError, "durable emergency stop"
            ):
                LiveBrokerRouter().place_order({"broker_id": "upbit"})
            with self.assertRaisesRegex(
                BrokerNotReadyError, "durable emergency stop"
            ):
                LiveBrokerRouter().cancel_order("upbit", "order-1")
        send.assert_not_called()

    def test_competing_state_provider_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            UpbitOrderAuthorityError,
            "already owned by another state graph",
        ):
            register_upbit_order_authority_reader(lambda: dict(self.snapshot))

    def test_raw_boundary_requires_exact_lease_and_process_arm(self) -> None:
        for field in (
            "applicationInstanceLeaseHeld",
            "ordinaryMutationEnabled",
        ):
            with self.subTest(field=field):
                self.snapshot[field] = False
                with self.assertRaises(UpbitOrderAuthorityError):
                    with ordinary_upbit_final_mutation_boundary(
                        operation="PLACE_ORDER"
                    ):
                        self.fail("unarmed mutation must not reach sender")
                self.snapshot[field] = True


if __name__ == "__main__":
    unittest.main()
