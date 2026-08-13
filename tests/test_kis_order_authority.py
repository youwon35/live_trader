from __future__ import annotations

import os
from pathlib import Path
import tempfile
import threading
import time
import unittest

from live_trader import kis_order_authority as kis_order_authority_module
from live_trader.emergency_stop import (
    _reset_emergency_stop_sticky_for_tests,
    engage_emergency_stop,
)
from live_trader.kis_order_authority import (
    KisOrderAuthorityError,
    _reset_kis_order_authority_reader_for_tests,
    functional_kis_final_mutation_boundary,
    kill_ordinary_kis_cancel_boundary,
    kis_functional_authority_open_fail_closed,
    kis_route_authority_serialization,
    ordinary_kis_final_mutation_boundary,
    register_kis_order_authority_reader,
)


class KisOrderAuthorityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.original_kis_authority_reader = (
            kis_order_authority_module._AUTHORITY_READER
        )
        self.original_kis_kill_cancel_journal_path = (
            kis_order_authority_module._KILL_CANCEL_JOURNAL_PATH
        )
        self.addCleanup(self._restore_kis_authority_provider)
        self.previous_stop_path = os.environ.get("LIVE_TRADER_EMERGENCY_STOP_PATH")
        os.environ["LIVE_TRADER_EMERGENCY_STOP_PATH"] = str(
            Path(self.temporary.name) / "emergency-stop.json"
        )
        _reset_emergency_stop_sticky_for_tests()
        _reset_kis_order_authority_reader_for_tests()
        self.snapshot = {
            "durableAuthorityReadable": True,
            "functionalAuthorityOpen": False,
            "functionalPhase": "IDLE",
            "functionalRevision": 0,
            "stateRevision": 1,
            "ownerEpochId": "owner-epoch-test-1",
            "ownerEpochHash": "e" * 64,
            "functionalSessionId": "",
            "functionalAccountFingerprint": "a" * 64,
            "credentialConfigurationHash": "b" * 64,
            "functionalMutationIntent": {},
            "killOrdinaryCancelAllowed": False,
            "killOrdinaryCancelRevision": 0,
            "killOrdinaryCancelIntent": {},
            "applicationInstanceLeaseHeld": True,
            "ordinaryRoutesClosed": False,
            "controlReservation": {},
        }
        self.read_count = 0

        def reader():
            self.read_count += 1
            return dict(self.snapshot)

        self.reader = reader
        register_kis_order_authority_reader(
            reader,
            kill_cancel_journal_path=(
                Path(self.temporary.name) / "kis-kill-cancel.sqlite3"
            ),
        )

    def tearDown(self) -> None:
        _reset_kis_order_authority_reader_for_tests()
        _reset_emergency_stop_sticky_for_tests()
        if self.previous_stop_path is None:
            os.environ.pop("LIVE_TRADER_EMERGENCY_STOP_PATH", None)
        else:
            os.environ["LIVE_TRADER_EMERGENCY_STOP_PATH"] = self.previous_stop_path

    def _restore_kis_authority_provider(self) -> None:
        _reset_kis_order_authority_reader_for_tests()
        if self.original_kis_authority_reader is not None:
            register_kis_order_authority_reader(
                self.original_kis_authority_reader,
                kill_cancel_journal_path=(
                    self.original_kis_kill_cancel_journal_path
                ),
            )

    def intent(self, operation: str, *, payload: str = "c", claim: str = "claim-1") -> dict:
        cancel = operation in {
            "CANCEL_ORDER",
            "CLEANUP_CANCEL",
            "KILL_ORDINARY_CANCEL",
        }
        return {
            "operation": operation,
            "claimId": claim,
            "ownedOrderKey": {
                "orderDate": "20260814" if cancel else "",
                "organizationNo": "00123" if cancel else "",
                "orderNo": "0000012345" if cancel else "",
            },
            "accountFingerprint": "a" * 64,
            "credentialConfigurationHash": "b" * 64,
            "endpoint": (
                "/uapi/domestic-stock/v1/trading/order-rvsecncl"
                if cancel
                else "/uapi/domestic-stock/v1/trading/order-cash"
            ),
            "payloadHash": payload * 64,
        }

    def read_intent(self, read, intent: dict):
        return read(
            endpoint=intent["endpoint"], payload_hash=intent["payloadHash"]
        )

    def authority(
        self, phase: str, *, revision: int = 7, intent: dict | None = None
    ) -> None:
        session = (
            ""
            if phase in {"ARMED_WAIT_PUBLIC", "BOOTSTRAP_ISSUED", "APPROVED"}
            else "kis-session-exact-1"
        )
        self.snapshot.update(
            {
                "functionalAuthorityOpen": True,
                "functionalPhase": phase,
                "functionalRevision": revision,
                "stateRevision": revision,
                "functionalSessionId": session,
                "functionalMutationIntent": dict(intent or {}),
                "ordinaryRoutesClosed": True,
            }
        )

    def control_reservation(
        self, kind: str, *, revision: int, phase: str
    ) -> dict[str, object]:
        return {
            "reservationId": f"kis-control-{kind.lower()}-1",
            "reservationKind": kind,
            "reservationRevision": revision,
            "stateRevision": revision,
            "phase": phase,
            "reservationBindingHash": "c" * 64,
        }

    def test_production_without_reader_and_unreadable_reader_fail_closed(self) -> None:
        place = self.intent("PLACE_ORDER")
        _reset_kis_order_authority_reader_for_tests()
        self.assertTrue(kis_functional_authority_open_fail_closed())
        with self.assertRaisesRegex(KisOrderAuthorityError, "not registered"):
            with ordinary_kis_final_mutation_boundary(
                operation="PLACE_ORDER", intent=place
            ):
                self.fail("mutation must not be reached")

        def unreadable():
            raise OSError("database unavailable")

        register_kis_order_authority_reader(unreadable)
        self.assertTrue(kis_functional_authority_open_fail_closed())
        with self.assertRaisesRegex(KisOrderAuthorityError, "unreadable"):
            with ordinary_kis_final_mutation_boundary(
                operation="CANCEL_ORDER", intent=self.intent("CANCEL_ORDER")
            ):
                self.fail("mutation must not be reached")

    def test_single_state_owned_reader_cannot_be_replaced(self) -> None:
        register_kis_order_authority_reader(self.reader)
        with self.assertRaisesRegex(KisOrderAuthorityError, "already owned"):
            register_kis_order_authority_reader(lambda: dict(self.snapshot))

    def test_preactivation_authority_cannot_carry_a_session(self) -> None:
        for revision, phase in enumerate(
            ("ARMED_WAIT_PUBLIC", "BOOTSTRAP_ISSUED", "APPROVED"), start=1
        ):
            with self.subTest(phase=phase):
                self.authority(phase, revision=revision)
                self.snapshot["functionalSessionId"] = "kis-session-too-early"
                self.assertTrue(kis_functional_authority_open_fail_closed())
                with self.assertRaisesRegex(
                    KisOrderAuthorityError, "preactivation.*session"
                ):
                    with ordinary_kis_final_mutation_boundary(
                        operation="PLACE_ORDER",
                        intent=self.intent("PLACE_ORDER"),
                    ):
                        pass

    def test_ordinary_is_blocked_for_armed_active_and_cleanup_authority(self) -> None:
        place = self.intent("PLACE_ORDER")
        for revision, phase in enumerate(
            (
                "ARMED_WAIT_PUBLIC",
                "BOOTSTRAP_ISSUED",
                "APPROVED",
                "ACTIVE",
                "CLEANUP",
                "RECONCILIATION_REQUIRED",
            ),
            start=1,
        ):
            with self.subTest(phase=phase):
                self.authority(phase, revision=revision)
                self.assertTrue(kis_functional_authority_open_fail_closed())
                with self.assertRaisesRegex(
                    KisOrderAuthorityError, "functional authority"
                ):
                    with ordinary_kis_final_mutation_boundary(
                        operation="PLACE_ORDER", intent=place
                    ):
                        self.fail("ordinary sender must not be reached")

    def test_ordinary_boundary_requires_application_lease_and_open_routes(self) -> None:
        place = self.intent("PLACE_ORDER")
        self.snapshot["applicationInstanceLeaseHeld"] = False
        with self.assertRaisesRegex(KisOrderAuthorityError, "application lease"):
            with ordinary_kis_final_mutation_boundary(
                operation="PLACE_ORDER", intent=place
            ):
                pass
        self.snapshot["applicationInstanceLeaseHeld"] = True
        with ordinary_kis_final_mutation_boundary(
            operation="PLACE_ORDER", intent=place
        ) as read:
            lease = self.read_intent(read, place)
            self.assertEqual("PLACE_ORDER", lease["operation"])
            self.assertFalse(lease["emergencyActive"])
            with self.assertRaisesRegex(KisOrderAuthorityError, "endpoint/payload"):
                read(endpoint=place["endpoint"], payload_hash="d" * 64)

    def test_active_control_reservation_blocks_ordinary_and_is_exact(self) -> None:
        place = self.intent("PLACE_ORDER")
        self.authority("ARMED_WAIT_PUBLIC", revision=2)
        self.snapshot["controlReservation"] = self.control_reservation(
            "START", revision=2, phase="ARMED_WAIT_PUBLIC"
        )
        with self.assertRaisesRegex(
            KisOrderAuthorityError, "active control reservation"
        ):
            with ordinary_kis_final_mutation_boundary(
                operation="PLACE_ORDER", intent=place
            ):
                pass

        self.snapshot["controlReservation"] = {
            **self.snapshot["controlReservation"],
            "stateRevision": 3,
        }
        with self.assertRaisesRegex(
            KisOrderAuthorityError, "reservation state revision"
        ):
            with ordinary_kis_final_mutation_boundary(
                operation="PLACE_ORDER", intent=place
            ):
                pass

        del self.snapshot["controlReservation"]
        with self.assertRaisesRegex(KisOrderAuthorityError, "incomplete"):
            with ordinary_kis_final_mutation_boundary(
                operation="PLACE_ORDER", intent=place
            ):
                pass
        self.snapshot["controlReservation"] = {}

    def test_only_stop_or_kill_reservation_allows_functional_cleanup(self) -> None:
        cleanup = self.intent("CLEANUP_CANCEL", claim="cleanup-control-1")
        self.authority("CLEANUP", revision=8, intent=cleanup)
        for kind in ("STOP", "KILL"):
            with self.subTest(kind=kind):
                self.snapshot["controlReservation"] = self.control_reservation(
                    kind, revision=8, phase="CLEANUP"
                )
                with functional_kis_final_mutation_boundary(
                    operation="CLEANUP_CANCEL",
                    session_id="kis-session-exact-1",
                    cleanup_only=True,
                    expected_revision=8,
                    intent=cleanup,
                ) as read:
                    self.assertTrue(self.read_intent(read, cleanup)["active"])

        self.snapshot["controlReservation"] = self.control_reservation(
            "SETTINGS", revision=8, phase="CLEANUP"
        )
        with self.assertRaisesRegex(
            KisOrderAuthorityError, "control reservation"
        ):
            with functional_kis_final_mutation_boundary(
                operation="CLEANUP_CANCEL",
                session_id="kis-session-exact-1",
                cleanup_only=True,
                expected_revision=8,
                intent=cleanup,
            ):
                pass
        self.snapshot["controlReservation"] = {}

    def test_functional_entry_and_cleanup_require_exact_phase_session_revision(self) -> None:
        entry = self.intent("NATURAL_BUY", claim="natural-buy-1")
        self.authority("ACTIVE", revision=7, intent=entry)
        with functional_kis_final_mutation_boundary(
            operation="NATURAL_BUY",
            session_id="kis-session-exact-1",
            cleanup_only=False,
            expected_revision=7,
            intent=entry,
        ) as read:
            lease = self.read_intent(read, entry)
            self.assertTrue(lease["active"])
            self.assertFalse(lease["cleanupOnly"])

        for session_id, revision, cleanup in (
            ("kis-session-wrong", 7, False),
            ("kis-session-exact-1", 8, False),
            ("kis-session-exact-1", 7, True),
        ):
            with self.subTest(session=session_id, revision=revision, cleanup=cleanup):
                with self.assertRaises(KisOrderAuthorityError):
                    attempted = self.intent(
                        "CLEANUP_SELL" if cleanup else "NATURAL_BUY",
                        claim="cleanup-sell-1" if cleanup else "natural-buy-1",
                    )
                    with functional_kis_final_mutation_boundary(
                        operation="CLEANUP_SELL" if cleanup else "NATURAL_BUY",
                        session_id=session_id,
                        cleanup_only=cleanup,
                        expected_revision=revision,
                        intent=attempted,
                    ):
                        pass

        cancel = self.intent("CLEANUP_CANCEL", claim="cleanup-cancel-1")
        self.authority("CLEANUP", revision=8, intent=cancel)
        with functional_kis_final_mutation_boundary(
            operation="CLEANUP_CANCEL",
            session_id="kis-session-exact-1",
            cleanup_only=True,
            expected_revision=8,
            intent=cancel,
        ) as read:
            self.assertTrue(self.read_intent(read, cancel)["cleanupOnly"])

        self.authority("RECONCILIATION_REQUIRED", revision=9, intent=cancel)
        with functional_kis_final_mutation_boundary(
            operation="CLEANUP_CANCEL",
            session_id="kis-session-exact-1",
            cleanup_only=True,
            expected_revision=9,
            intent=cancel,
        ) as read:
            self.assertEqual(
                "RECONCILIATION_REQUIRED",
                self.read_intent(read, cancel)["functionalPhase"],
            )

    def test_kill_blocks_ordinary_and_entry_but_allows_exact_cleanup(self) -> None:
        cancel = self.intent("CANCEL_ORDER", claim="ordinary-cancel-1")
        killed = engage_emergency_stop("KIS authority test", source="unit-test")
        self.assertTrue(killed["active"])
        with self.assertRaisesRegex(KisOrderAuthorityError, "emergency"):
            with ordinary_kis_final_mutation_boundary(
                operation="CANCEL_ORDER", intent=cancel
            ):
                pass

        entry = self.intent("NATURAL_BUY", claim="natural-buy-1")
        self.authority("ACTIVE", revision=7, intent=entry)
        with self.assertRaisesRegex(KisOrderAuthorityError, "emergency"):
            with functional_kis_final_mutation_boundary(
                operation="NATURAL_BUY",
                session_id="kis-session-exact-1",
                cleanup_only=False,
                expected_revision=7,
                intent=entry,
            ):
                pass

        cleanup = self.intent("CLEANUP_SELL", claim="cleanup-sell-1")
        self.authority("CLEANUP", revision=8, intent=cleanup)
        with functional_kis_final_mutation_boundary(
            operation="CLEANUP_SELL",
            session_id="kis-session-exact-1",
            cleanup_only=True,
            expected_revision=8,
            intent=cleanup,
        ) as read:
            self.assertTrue(self.read_intent(read, cleanup)["emergencyActive"])

        kill_cancel = self.intent(
            "KILL_ORDINARY_CANCEL", claim="ordinary-owned-order-1"
        )
        self.snapshot.update(
            {
                "killOrdinaryCancelAllowed": True,
                "killOrdinaryCancelRevision": 3,
                "killOrdinaryCancelIntent": kill_cancel,
            }
        )
        with kill_ordinary_kis_cancel_boundary(
            intent=kill_cancel, expected_revision=3
        ) as read:
            self.assertTrue(self.read_intent(read, kill_cancel)["emergencyActive"])
        tampered = {**kill_cancel, "payloadHash": "d" * 64}
        with self.assertRaisesRegex(KisOrderAuthorityError, "changed"):
            with kill_ordinary_kis_cancel_boundary(
                intent=tampered, expected_revision=3
            ):
                pass

    def test_kill_cancel_requires_active_kill_and_exact_durable_identity(self) -> None:
        kill_cancel = self.intent(
            "KILL_ORDINARY_CANCEL", claim="ordinary-owned-order-1"
        )
        self.snapshot.update(
            {
                "killOrdinaryCancelAllowed": True,
                "killOrdinaryCancelRevision": 3,
                "killOrdinaryCancelIntent": kill_cancel,
            }
        )
        with self.assertRaisesRegex(KisOrderAuthorityError, "requires durable emergency"):
            with kill_ordinary_kis_cancel_boundary(
                intent=kill_cancel, expected_revision=3
            ):
                pass

        engage_emergency_stop("KIS Kill-cancel test", source="unit-test")
        changed_account = {
            **kill_cancel,
            "accountFingerprint": "d" * 64,
        }
        with self.assertRaisesRegex(KisOrderAuthorityError, "identity/payload changed"):
            with kill_ordinary_kis_cancel_boundary(
                intent=changed_account, expected_revision=3
            ):
                pass

    def test_nested_cleanup_cannot_switch_exact_claim_operation_or_owned_order(self) -> None:
        cancel = self.intent("CLEANUP_CANCEL", claim="cleanup-cancel-1")
        self.authority("CLEANUP", revision=8, intent=cancel)
        with functional_kis_final_mutation_boundary(
            operation="CLEANUP_CANCEL",
            session_id="kis-session-exact-1",
            cleanup_only=True,
            expected_revision=8,
            intent=cancel,
        ) as outer:
            self.assertTrue(self.read_intent(outer, cancel)["active"])
            sell = self.intent("CLEANUP_SELL", claim="cleanup-sell-1")
            with self.assertRaisesRegex(
                KisOrderAuthorityError, "cannot change inherited authority"
            ):
                with functional_kis_final_mutation_boundary(
                    operation="CLEANUP_SELL",
                    session_id="kis-session-exact-1",
                    cleanup_only=True,
                    expected_revision=8,
                    intent=sell,
                ):
                    pass
            changed_order = {
                **cancel,
                "ownedOrderKey": {
                    **cancel["ownedOrderKey"],
                    "orderNo": "0000099999",
                },
            }
            with self.assertRaisesRegex(
                KisOrderAuthorityError, "cannot change inherited authority"
            ):
                with functional_kis_final_mutation_boundary(
                    operation="CLEANUP_CANCEL",
                    session_id="kis-session-exact-1",
                    cleanup_only=True,
                    expected_revision=8,
                    intent=changed_order,
                ):
                    pass

    def test_thread_local_nested_lease_is_exact_and_cannot_change_mode(self) -> None:
        place = self.intent("PLACE_ORDER", claim="ordinary-place-1")
        with ordinary_kis_final_mutation_boundary(
            operation="PLACE_ORDER", intent=place
        ) as outer:
            with ordinary_kis_final_mutation_boundary(
                operation="PLACE_ORDER", intent=place
            ) as inner:
                self.assertIs(outer, inner)
                self.assertEqual(
                    self.read_intent(outer, place)["intentHash"],
                    self.read_intent(inner, place)["intentHash"],
                )
            with self.assertRaisesRegex(KisOrderAuthorityError, "cannot change inherited authority"):
                changed = self.intent("CANCEL_ORDER", claim="ordinary-cancel-1")
                with ordinary_kis_final_mutation_boundary(
                    operation="CANCEL_ORDER", intent=changed
                ):
                    pass
            with self.assertRaisesRegex(KisOrderAuthorityError, "cannot change inherited authority"):
                entry = self.intent("NATURAL_BUY", claim="natural-buy-1")
                with functional_kis_final_mutation_boundary(
                    operation="NATURAL_BUY",
                    session_id="kis-session-exact-1",
                    cleanup_only=False,
                    expected_revision=7,
                    intent=entry,
                ):
                    pass

        entry = self.intent("NATURAL_BUY", claim="natural-buy-1")
        self.authority("ACTIVE", revision=7, intent=entry)
        with functional_kis_final_mutation_boundary(
            operation="NATURAL_BUY",
            session_id="kis-session-exact-1",
            cleanup_only=False,
            expected_revision=7,
            intent=entry,
        ) as outer_read:
            with functional_kis_final_mutation_boundary(
                operation="NATURAL_BUY",
                session_id="kis-session-exact-1",
                cleanup_only=False,
                expected_revision=7,
                intent=entry,
            ) as inner_read:
                self.assertIs(outer_read, inner_read)
            with self.assertRaisesRegex(KisOrderAuthorityError, "cannot change inherited authority"):
                cleanup = self.intent("CLEANUP_SELL", claim="cleanup-sell-1")
                with functional_kis_final_mutation_boundary(
                    operation="CLEANUP_SELL",
                    session_id="kis-session-exact-1",
                    cleanup_only=True,
                    expected_revision=7,
                    intent=cleanup,
                ):
                    pass
            with self.assertRaisesRegex(KisOrderAuthorityError, "cannot change inherited authority"):
                with ordinary_kis_final_mutation_boundary(
                    operation="PLACE_ORDER", intent=place
                ):
                    pass

    def test_inherited_lease_rejects_owner_epoch_and_state_revision_change(self) -> None:
        place = self.intent("PLACE_ORDER", claim="ordinary-owner-epoch-1")
        with ordinary_kis_final_mutation_boundary(
            operation="PLACE_ORDER", intent=place
        ) as read:
            self.assertEqual(1, self.read_intent(read, place)["stateRevision"])
            self.snapshot["stateRevision"] = 2
            with self.assertRaisesRegex(
                KisOrderAuthorityError, "owner epoch/state/account/credential"
            ):
                self.read_intent(read, place)

        self.snapshot["stateRevision"] = 2
        self.snapshot["ownerEpochId"] = "owner-epoch-test-2"
        self.snapshot["ownerEpochHash"] = "f" * 64
        with ordinary_kis_final_mutation_boundary(
            operation="PLACE_ORDER", intent=place
        ) as read:
            self.snapshot["ownerEpochHash"] = "e" * 64
            with self.assertRaisesRegex(
                KisOrderAuthorityError, "owner epoch/state/account/credential"
            ):
                self.read_intent(read, place)

    def test_route_watchdog_serializes_ordinary_functional_kill_and_settings(self) -> None:
        barrier = threading.Barrier(4)
        counter_lock = threading.Lock()
        active = 0
        maximum = 0
        completed: list[str] = []

        def worker(label: str) -> None:
            nonlocal active, maximum
            barrier.wait(timeout=2)
            with kis_route_authority_serialization():
                with counter_lock:
                    active += 1
                    maximum = max(maximum, active)
                time.sleep(0.01)
                completed.append(label)
                with counter_lock:
                    active -= 1

        threads = [
            threading.Thread(target=worker, args=(label,))
            for label in ("ordinary", "functional", "kill", "settings")
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(2)
            self.assertFalse(thread.is_alive(), "route watchdog detected deadlock")
        self.assertEqual(1, maximum)
        self.assertCountEqual(
            ["ordinary", "functional", "kill", "settings"], completed
        )

    def test_paused_sender_serializes_stop_and_releases_lock(self) -> None:
        entry = self.intent("NATURAL_BUY", claim="natural-buy-1")
        self.authority("ACTIVE", revision=7, intent=entry)
        sender_entered = threading.Event()
        release_sender = threading.Event()
        stop_finished = threading.Event()
        events: list[str] = []

        def sender() -> None:
            with functional_kis_final_mutation_boundary(
                operation="NATURAL_BUY",
                session_id="kis-session-exact-1",
                cleanup_only=False,
                expected_revision=7,
                intent=entry,
            ) as read:
                self.read_intent(read, entry)
                sender_entered.set()
                release_sender.wait(2)
                events.append("send")

        def stop() -> None:
            with kis_route_authority_serialization():
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
        self.assertFalse(sender_thread.is_alive())
        self.assertFalse(stop_thread.is_alive())
        self.assertEqual(["send", "stop"], events)

        acquired = threading.Event()
        try:
            with kis_route_authority_serialization():
                raise RuntimeError("injected")
        except RuntimeError:
            pass

        def after_exception() -> None:
            with kis_route_authority_serialization():
                acquired.set()

        thread = threading.Thread(target=after_exception)
        thread.start()
        thread.join(1)
        self.assertTrue(acquired.is_set())


if __name__ == "__main__":
    unittest.main()
