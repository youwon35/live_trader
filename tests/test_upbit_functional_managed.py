from __future__ import annotations

from datetime import timedelta
from pathlib import Path
import tempfile
import unittest

from live_trader.upbit_continuous_functional import (
    UpbitFunctionalLedger,
    _activate_for_test,
)
from live_trader.upbit_functional_managed import (
    ManagedUpbitFunctionalController,
)
from tests.test_upbit_continuous_functional import (
    FakeBoundaries,
    NOW,
    TEST_EXCLUSIVITY_VERIFIER,
    TEST_EXCLUSIVITY_VERIFIER_PIN,
    UpbitContinuousFunctionalTest,
    permit,
)


class ManagedUpbitFunctionalControllerTest(unittest.TestCase):
    @staticmethod
    def buy_bar() -> dict[str, object]:
        return UpbitContinuousFunctionalTest.bar("BUY")

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.permit = permit()
        self.fake = FakeBoundaries(self.permit)
        self.ledger = UpbitFunctionalLedger(
            Path(self.temp.name) / "managed.sqlite3",
            clock=self.fake.clock,
        )
        self.service = _activate_for_test(
            permit=self.permit,
            ledger=self.ledger,
            session_id=self.fake.session_id,
            truth_reader=self.fake.truth,
            post_order=self.fake.post,
            cancel_order=self.fake.cancel,
            lease_factory=self.fake.lease,
            runtime_reader=self.fake.runtime,
            immutable_selection_reader=self.fake.immutable_selection,
            runtime_capability_registrar=self.fake.register_capability,
            real_orders_reader=lambda: self.fake.real_orders,
            clock=self.fake.clock,
            account_exclusivity_verifier=TEST_EXCLUSIVITY_VERIFIER,
            account_exclusivity_verifier_pin=TEST_EXCLUSIVITY_VERIFIER_PIN,
        )
        self.fake.real_orders = True
        self.fake.runtime_updates.update(
            {"newEntriesBlocked": True, "realOrdersEnabled": True}
        )

        def cleanup_latch() -> None:
            self.fake.runtime_updates["newEntriesBlocked"] = True

        def disarm() -> None:
            self.fake.real_orders = False
            self.fake.runtime_updates.update(
                {"newEntriesBlocked": True, "realOrdersEnabled": False}
            )

        self.controller = ManagedUpbitFunctionalController(
            enter_cleanup_latch=cleanup_latch,
            disarm_real_orders=disarm,
            clock=self.fake.clock,
        )
        self.controller._attach_for_test(self.service)

    def test_managed_stop_flattens_owned_delta_disarms_and_finalizes(self) -> None:
        self.controller.on_finalized_bar(self.buy_bar())
        result = self.controller.stop_and_cleanup(reason="operator-stop")
        self.assertEqual(["CLEANUP_SELL"], result["actions"])
        self.assertEqual("FINALIZED", result["snapshot"]["status"])
        self.assertTrue(result["snapshot"]["newEntriesBlocked"])
        self.assertFalse(self.fake.real_orders)

    def test_real_capability_clear_callback_is_not_called_before_core_reset(self) -> None:
        clear_calls: list[str] = []
        controller = ManagedUpbitFunctionalController(
            enter_cleanup_latch=lambda: None,
            disarm_real_orders=lambda: (
                setattr(self.fake, "real_orders", False),
                self.fake.runtime_updates.update({"realOrdersEnabled": False}),
            ),
            clear_runtime_capability=lambda: (
                clear_calls.append("clear"),
                self.fake.register_capability(""),
            ),
            clock=self.fake.clock,
        )
        controller._attach_for_test(self.service)
        result = controller.stop_and_cleanup(reason="operator-stop")
        self.assertEqual("FINALIZED", result["snapshot"]["status"])
        self.assertEqual([], clear_calls)
        self.assertEqual("", self.fake.capability_hash)

    def test_post_activation_arm_failure_is_cleanup_revoked_and_detached(self) -> None:
        snapshot = self.controller.fail_closed_after_start(
            reason="lane-arm-failed"
        )
        durable = self.ledger.session(self.fake.session_id)
        self.assertEqual("FAILED_CLOSED", snapshot["status"])
        self.assertEqual("CLEANUP", durable["state"])
        self.assertEqual("", durable["capability_hash"])
        self.assertEqual("", self.fake.capability_hash)
        with self.assertRaisesRegex(
            Exception, "session-not-running"
        ):
            self.controller.monitor_once()

    def test_expiry_monitor_enters_cleanup_without_another_bar(self) -> None:
        self.controller.on_finalized_bar(self.buy_bar())
        self.fake.now = self.permit.ends_at + timedelta(seconds=1)
        result = self.controller.monitor_once()
        self.assertEqual("FINALIZED", result["snapshot"]["status"])
        self.assertEqual("permit-expired", result["snapshot"]["reason"])

    def test_monitor_detects_owner_loss_without_waiting_for_another_bar(self) -> None:
        self.controller.on_finalized_bar(self.buy_bar())
        self.fake.mark = self.fake.mark * 8 / 10
        result = self.controller.monitor_once()
        self.assertEqual("FINALIZED", result["snapshot"]["status"])
        self.assertEqual(
            "owner-loss-limit-reached",
            result["snapshot"]["reason"],
        )
        self.assertFalse(self.fake.real_orders)

    def test_partial_working_cleanup_is_bounded_and_remains_pending(self) -> None:
        self.controller.on_finalized_bar(self.buy_bar())

        def working_sell(
            payload,
            *,
            functional_capability: str,
            functional_action: str,
            claim_id: str,
            request_hash: str,
        ):
            self.fake.assert_capability(functional_capability)
            self.fake.post_calls += 1
            identifier = str(payload["identifier"])
            order_uuid = f"cleanup-working-{self.fake.post_calls:04d}"
            self.fake.open_orders.append(
                {
                    "market": "KRW-BTC",
                    "uuid": order_uuid,
                    "identifier": identifier,
                    "side": "ASK",
                    "state": "wait",
                }
            )
            return {
                "uuid": order_uuid,
                "identifier": identifier,
                "state": "wait",
            }

        self.service.post_order = working_sell
        result = self.controller.stop_and_cleanup(reason="operator-stop")
        self.assertTrue(result["pending"])
        self.assertEqual(
            [
                "CLEANUP_SELL",
                "CLEANUP_CANCEL",
                "CLEANUP_SELL",
                "CLEANUP_CANCEL",
                "CLEANUP_SELL",
                "CLEANUP_CANCEL",
            ],
            result["actions"],
        )
        self.assertEqual(3, self.fake.cancel_calls)
        self.assertEqual(4, self.fake.post_calls)
        self.assertEqual("CLEANUP", result["snapshot"]["status"])
        again = self.controller.monitor_once()
        self.assertTrue(again["pending"])
        self.assertEqual(
            [
                "CLEANUP_SELL",
                "CLEANUP_CANCEL",
                "CLEANUP_SELL",
                "CLEANUP_CANCEL",
                "CLEANUP_SELL",
                "CLEANUP_CANCEL",
            ],
            again["actions"],
        )
        self.assertEqual(6, self.fake.cancel_calls)
        self.assertEqual(7, self.fake.post_calls)
        exhausted = self.controller.monitor_once()
        self.assertTrue(exhausted["pending"])
        self.assertEqual([], exhausted["actions"])
        self.assertEqual(0, len(self.fake.open_orders))
        self.assertEqual(12, exhausted["plan"]["cleanupActionGenerationCount"])
        self.assertEqual(12, exhausted["plan"]["cleanupActionGenerationCap"])


if __name__ == "__main__":
    unittest.main()
