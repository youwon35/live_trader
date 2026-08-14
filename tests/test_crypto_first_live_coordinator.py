from __future__ import annotations

import hashlib
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest import mock

from live_trader.crypto_first_live_coordinator import (
    CryptoFirstLiveCoordinatorError,
    DurableCryptoFirstLiveCoordinator,
)


def h(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


class Clock:
    def __init__(self, value: float = 1_800_000_000.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


class CryptoFirstLiveCoordinatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.activation_patch = mock.patch(
            "live_trader.crypto_first_live_coordinator."
            "CRYPTO_FIRST_LIVE_ACTIVATION_RELEASED",
            True,
        )
        self.activation_patch.start()
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "coordinator.sqlite3"
        self.clock = Clock()
        self.owner_authority_available = True
        self.store = DurableCryptoFirstLiveCoordinator(
            self.path,
            clock=self.clock,
            reservation_evidence_verifier=lambda value: value.get("presented")
            == {"fresh": True, "workingOrders": 0},
            final_approval_consumer=lambda value: (
                value.get("presented") == {"oneUse": True}
            ),
            terminal_evidence_verifier=lambda value: value.get("presented")
            == {"finalFlat": True, "workingOrders": 0},
            startup_owner_absent_reader=lambda _value: True,
            installation_validator=lambda _value: True,
            owner_identity_verifier=lambda value: (
                self.owner_authority_available
                and value.get("ownerIdentity", {}).get("accountLeaseScope")
                == "crypto-first-live-account:"
                + str(value.get("lane"))
                + ":"
                + str(value.get("accountFingerprint"))
            ),
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()
        self.activation_patch.stop()

    def reserve(self, lane: str = "UPBIT") -> dict[str, object]:
        return self.store.reserve_inert(
            lane=lane,
            account_fingerprint=h(lane + "-account"),
            baseline_hash=h(lane + "-baseline"),
            code_hash=h(lane + "-code"),
            approval_id="approval-" + lane.lower() + "-0001",
            permit_hash=h(lane + "-permit"),
            reservation_evidence={"fresh": True, "workingOrders": 0},
            owner_identity=self.owner_identity(lane),
        )

    def owner_identity(self, lane: str = "UPBIT") -> dict[str, object]:
        return {
            "pid": 1234 if lane == "UPBIT" else 5678,
            "processStartEpoch": self.clock.value - 10,
            "bootId": "windows-boot-identity-0001",
            "applicationLeaseEpoch": self.clock.value - 5,
            "accountLeaseScope": (
                "crypto-first-live-account:"
                + lane
                + ":"
                + h(lane + "-account")
            ),
        }

    def test_inert_reservation_never_grants_network_authority(self) -> None:
        value = self.reserve()

        self.assertEqual("APPROVED_INERT", value["phase"])
        self.assertFalse(value["entryAuthorityOpen"])
        self.assertFalse(value["networkOrderPostAllowed"])
        self.assertNotIn("ownerTokenHash", value)
        self.assertTrue(value["ownerToken"])
        restarted = DurableCryptoFirstLiveCoordinator(
            self.path, clock=self.clock
        ).status()
        self.assertEqual(value["runId"], restarted["runId"])
        self.assertFalse(restarted["networkOrderPostAllowed"])

    def test_production_compile_latch_blocks_activation(self) -> None:
        value = self.reserve()
        with mock.patch(
            "live_trader.crypto_first_live_coordinator."
            "CRYPTO_FIRST_LIVE_ACTIVATION_RELEASED",
            False,
        ):
            self.assertFalse(self.store.status()["activationReleased"])
            with self.assertRaisesRegex(
                CryptoFirstLiveCoordinatorError, "activation-not-released"
            ):
                self.store.activate(
                    run_id=str(value["runId"]),
                    owner_token=str(value["ownerToken"]),
                    owner_epoch=int(value["ownerEpoch"]),
                    final_approval={"oneUse": True},
                )

    def test_two_lanes_concurrently_reserve_exactly_one_global_owner(self) -> None:
        barrier = threading.Barrier(2)
        wins: list[str] = []
        errors: list[str] = []

        def run(lane: str) -> None:
            local = DurableCryptoFirstLiveCoordinator(
                self.path,
                clock=self.clock,
                reservation_evidence_verifier=lambda value: value.get("presented")
                == {"fresh": True, "workingOrders": 0},
                installation_validator=lambda _value: True,
                owner_identity_verifier=lambda _value: True,
            )
            barrier.wait()
            try:
                result = local.reserve_inert(
                    lane=lane,
                    account_fingerprint=h(lane + "-account"),
                    baseline_hash=h(lane + "-baseline"),
                    code_hash=h(lane + "-code"),
                    approval_id="approval-" + lane.lower() + "-0001",
                    permit_hash=h(lane + "-permit"),
                    reservation_evidence={
                        "fresh": True,
                        "workingOrders": 0,
                    },
                    owner_identity=self.owner_identity(lane),
                )
                wins.append(str(result["lane"]))
            except CryptoFirstLiveCoordinatorError as exc:
                errors.append(str(exc))

        threads = [
            threading.Thread(target=run, args=("UPBIT",)),
            threading.Thread(target=run, args=("BINANCE_SPOT",)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)

        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(1, len(wins))
        self.assertEqual(
            ["crypto-first-live-global-owner-active"], errors
        )

    def test_stale_owner_cannot_activate_or_heartbeat(self) -> None:
        value = self.reserve()
        with self.assertRaisesRegex(
            CryptoFirstLiveCoordinatorError, "owner-changed"
        ):
            self.store.activate(
                run_id=str(value["runId"]),
                owner_token="wrong-owner-token",
                owner_epoch=int(value["ownerEpoch"]),
                final_approval={"oneUse": True},
            )

        active = self.store.activate(
            run_id=str(value["runId"]),
            owner_token=str(value["ownerToken"]),
            owner_epoch=int(value["ownerEpoch"]),
            final_approval={"oneUse": True},
        )
        self.assertTrue(active["entryAuthorityOpen"])
        self.assertFalse(active["networkOrderPostAllowed"])
        self.assertEqual(
            7200.0,
            float(active["hardStopEpoch"]) - self.clock.value,
        )
        with self.assertRaisesRegex(
            CryptoFirstLiveCoordinatorError, "owner-changed"
        ):
            self.store.heartbeat(
                run_id=str(value["runId"]),
                owner_token=str(value["ownerToken"]),
                owner_epoch=int(value["ownerEpoch"]) + 1,
            )

    def test_heartbeat_never_extends_exact_two_hour_hard_stop(self) -> None:
        value = self.reserve()
        active = self.store.activate(
            run_id=str(value["runId"]),
            owner_token=str(value["ownerToken"]),
            owner_epoch=int(value["ownerEpoch"]),
            final_approval={"oneUse": True},
        )
        hard_stop = float(active["hardStopEpoch"])
        self.clock.value += 30
        renewed = self.store.heartbeat(
            run_id=str(value["runId"]),
            owner_token=str(value["ownerToken"]),
            owner_epoch=int(value["ownerEpoch"]),
        )
        self.assertEqual(hard_stop, float(renewed["hardStopEpoch"]))
        self.assertLessEqual(
            float(renewed["ownerLeaseExpiresEpoch"]), hard_stop
        )

        self.clock.value = hard_stop
        stopped = self.store.audit_startup()
        self.assertEqual("CLEANUP_ONLY", stopped["phase"])
        self.assertFalse(stopped["entryAuthorityOpen"])
        self.assertTrue(
            self.store.events()[-1]["payload"]["hardStopReached"]
        )

    def test_lost_owner_authority_closes_before_activation(self) -> None:
        value = self.reserve()
        self.owner_authority_available = False
        with self.assertRaisesRegex(
            CryptoFirstLiveCoordinatorError, "owner-identity-unverified"
        ):
            self.store.activate(
                run_id=str(value["runId"]),
                owner_token=str(value["ownerToken"]),
                owner_epoch=int(value["ownerEpoch"]),
                final_approval={"oneUse": True},
            )
        status = self.store.status()
        self.assertEqual("RECONCILIATION_REQUIRED", status["phase"])
        self.assertFalse(status["entryAuthorityOpen"])

    def test_lost_owner_authority_closes_active_heartbeat(self) -> None:
        value = self.reserve()
        self.store.activate(
            run_id=str(value["runId"]),
            owner_token=str(value["ownerToken"]),
            owner_epoch=int(value["ownerEpoch"]),
            final_approval={"oneUse": True},
        )
        self.owner_authority_available = False
        with self.assertRaisesRegex(
            CryptoFirstLiveCoordinatorError, "owner-identity-unverified"
        ):
            self.store.heartbeat(
                run_id=str(value["runId"]),
                owner_token=str(value["ownerToken"]),
                owner_epoch=int(value["ownerEpoch"]),
            )
        status = self.store.status()
        self.assertEqual("RECONCILIATION_REQUIRED", status["phase"])
        self.assertFalse(status["entryAuthorityOpen"])

    def test_stop_revokes_entry_before_owner_cleanup_finishes(self) -> None:
        value = self.reserve()
        active = self.store.activate(
            run_id=str(value["runId"]),
            owner_token=str(value["ownerToken"]),
            owner_epoch=int(value["ownerEpoch"]),
            final_approval={"oneUse": True},
        )
        self.assertTrue(active["entryAuthorityOpen"])

        cleanup = self.store.revoke_entry(
            run_id=str(value["runId"]), reason="operator-stop"
        )
        self.assertEqual("CLEANUP_ONLY", cleanup["phase"])
        self.assertFalse(cleanup["entryAuthorityOpen"])
        final = self.store.finalize(
            run_id=str(value["runId"]),
            owner_token=str(value["ownerToken"]),
            owner_epoch=int(value["ownerEpoch"]),
            terminal_evidence={"finalFlat": True, "workingOrders": 0},
        )
        self.assertEqual("FINALIZED", final["phase"])
        self.assertFalse(final["entryAuthorityOpen"])

    def test_expired_owner_rotates_cleanup_only_never_entry(self) -> None:
        value = self.reserve()
        self.store.activate(
            run_id=str(value["runId"]),
            owner_token=str(value["ownerToken"]),
            owner_epoch=int(value["ownerEpoch"]),
            final_approval={"oneUse": True},
        )
        cleanup = self.store.revoke_entry(
            run_id=str(value["runId"]), reason="process-owner-lost"
        )
        self.clock.value = float(cleanup["ownerLeaseExpiresEpoch"]) + 1
        rotated = self.store.takeover_expired_cleanup(
            run_id=str(value["runId"]),
            expected_revision=int(cleanup["revision"]),
            owner_identity=self.owner_identity("UPBIT"),
        )

        self.assertEqual("CLEANUP_ONLY", rotated["phase"])
        self.assertFalse(rotated["entryAuthorityOpen"])
        self.assertGreater(rotated["ownerEpoch"], value["ownerEpoch"])
        with self.assertRaisesRegex(
            CryptoFirstLiveCoordinatorError, "reservation-not-inert"
        ):
            self.store.activate(
                run_id=str(value["runId"]),
                owner_token=str(rotated["ownerToken"]),
                owner_epoch=int(rotated["ownerEpoch"]),
                final_approval={"oneUse": True},
            )

    def test_reconciliation_required_is_terminal_hold(self) -> None:
        value = self.reserve()
        result = self.store.mark_reconciliation_required(
            run_id=str(value["runId"]), reason="truth-gap"
        )
        self.assertEqual("RECONCILIATION_REQUIRED", result["phase"])
        self.assertFalse(result["entryAuthorityOpen"])
        with self.assertRaisesRegex(
            CryptoFirstLiveCoordinatorError, "global-owner-active"
        ):
            self.reserve("BINANCE_SPOT")

    def test_append_only_event_chain_detects_tamper(self) -> None:
        value = self.reserve()
        self.store.activate(
            run_id=str(value["runId"]),
            owner_token=str(value["ownerToken"]),
            owner_epoch=int(value["ownerEpoch"]),
            final_approval={"oneUse": True},
        )
        events = self.store.events()
        self.assertEqual(["RESERVED_INERT", "ACTIVATED"], [
            item["payload"]["eventType"] for item in events
        ])

        conn = sqlite3.connect(self.path)
        try:
            conn.execute(
                "UPDATE crypto_first_live_events SET content_json='{}' "
                "WHERE revision=2"
            )
            conn.commit()
        finally:
            conn.close()
        with self.assertRaisesRegex(
            CryptoFirstLiveCoordinatorError, "event-chain-invalid"
        ):
            self.store.events()

    def test_unverified_reservation_activation_and_terminal_stay_closed(self) -> None:
        closed_path = Path(self.temporary.name) / "closed.sqlite3"
        closed = DurableCryptoFirstLiveCoordinator(
            closed_path,
            clock=self.clock,
            installation_validator=lambda _value: True,
            owner_identity_verifier=lambda _value: True,
        )
        with self.assertRaisesRegex(
            CryptoFirstLiveCoordinatorError, "reservation-evidence-unverified"
        ):
            closed.reserve_inert(
                lane="UPBIT",
                account_fingerprint=h("UPBIT-account"),
                baseline_hash=h("baseline"),
                code_hash=h("code"),
                approval_id="approval-upbit-closed-0001",
                permit_hash=h("permit"),
                reservation_evidence={"fresh": True, "workingOrders": 0},
                owner_identity=self.owner_identity("UPBIT"),
            )

        value = self.reserve()
        no_approval = DurableCryptoFirstLiveCoordinator(
            self.path,
            clock=self.clock,
            reservation_evidence_verifier=lambda _value: True,
            owner_identity_verifier=lambda _value: True,
        )
        with self.assertRaisesRegex(
            CryptoFirstLiveCoordinatorError, "final-approval-unverified"
        ):
            no_approval.activate(
                run_id=str(value["runId"]),
                owner_token=str(value["ownerToken"]),
                owner_epoch=int(value["ownerEpoch"]),
                final_approval={"oneUse": True},
            )

    def test_expired_active_status_closes_entry_and_startup_demotes(self) -> None:
        value = self.reserve()
        self.store.activate(
            run_id=str(value["runId"]),
            owner_token=str(value["ownerToken"]),
            owner_epoch=int(value["ownerEpoch"]),
            final_approval={"oneUse": True},
        )
        self.clock.value = float(value["ownerLeaseExpiresEpoch"]) + 1

        self.assertFalse(self.store.status()["entryAuthorityOpen"])
        audited = self.store.audit_startup()
        self.assertEqual("CLEANUP_ONLY", audited["phase"])
        self.assertFalse(audited["entryAuthorityOpen"])

    def test_clock_rollback_forces_reconciliation_required(self) -> None:
        self.reserve()
        self.clock.value -= 1

        status = self.store.audit_startup()
        self.assertEqual("RECONCILIATION_REQUIRED", status["phase"])
        self.assertFalse(status["entryAuthorityOpen"])

    def test_missing_database_requires_cross_lane_installation_audit(self) -> None:
        missing = Path(self.temporary.name) / "missing.sqlite3"
        with self.assertRaisesRegex(
            CryptoFirstLiveCoordinatorError, "installation-unverified"
        ):
            DurableCryptoFirstLiveCoordinator(missing, clock=self.clock)

        created = DurableCryptoFirstLiveCoordinator(
            missing,
            clock=self.clock,
            installation_validator=lambda value: (
                value["requiredCrossAudit"]
                == "UPBIT_AND_BINANCE_NONTERMINAL_POINTERS_ABSENT"
            ),
        )
        self.assertEqual("IDLE", created.status()["phase"])

    def test_existing_empty_schema_still_requires_cross_lane_audit(self) -> None:
        empty = Path(self.temporary.name) / "empty.sqlite3"
        DurableCryptoFirstLiveCoordinator(
            empty,
            clock=self.clock,
            installation_validator=lambda _value: True,
        )
        with self.assertRaisesRegex(
            CryptoFirstLiveCoordinatorError,
            "empty-store-authority-unverified",
        ):
            DurableCryptoFirstLiveCoordinator(empty, clock=self.clock)

    def test_owner_identity_is_authoritative_and_account_scoped(self) -> None:
        rejected = Path(self.temporary.name) / "owner-rejected.sqlite3"
        store = DurableCryptoFirstLiveCoordinator(
            rejected,
            clock=self.clock,
            installation_validator=lambda _value: True,
            reservation_evidence_verifier=lambda _value: True,
        )
        with self.assertRaisesRegex(
            CryptoFirstLiveCoordinatorError, "owner-identity-unverified"
        ):
            store.reserve_inert(
                lane="UPBIT",
                account_fingerprint=h("UPBIT-account"),
                baseline_hash=h("UPBIT-baseline"),
                code_hash=h("UPBIT-code"),
                approval_id="approval-upbit-owner-0001",
                permit_hash=h("UPBIT-permit"),
                reservation_evidence={"fresh": True},
                owner_identity=self.owner_identity("UPBIT"),
            )

        wrong_scope = self.owner_identity("UPBIT")
        wrong_scope["accountLeaseScope"] = (
            "crypto-first-live-account:UPBIT:" + h("other-account")
        )
        with self.assertRaisesRegex(
            CryptoFirstLiveCoordinatorError, "owner-identity-invalid"
        ):
            self.store.reserve_inert(
                lane="UPBIT",
                account_fingerprint=h("UPBIT-account"),
                baseline_hash=h("UPBIT-baseline"),
                code_hash=h("UPBIT-code"),
                approval_id="approval-upbit-owner-0002",
                permit_hash=h("UPBIT-permit"),
                reservation_evidence={"fresh": True, "workingOrders": 0},
                owner_identity=wrong_scope,
            )

    def test_deleted_control_row_cannot_become_idle(self) -> None:
        self.reserve()
        conn = sqlite3.connect(self.path)
        try:
            conn.execute("DELETE FROM crypto_first_live_control")
            conn.commit()
        finally:
            conn.close()
        with self.assertRaisesRegex(
            CryptoFirstLiveCoordinatorError,
            "control-event-integrity-invalid",
        ):
            self.store.status()

    def test_foreign_scope_control_row_is_rejected(self) -> None:
        self.reserve()
        conn = sqlite3.connect(self.path)
        try:
            conn.execute(
                "UPDATE crypto_first_live_control SET scope_key='FOREIGN_SCOPE'"
            )
            conn.commit()
        finally:
            conn.close()
        with self.assertRaisesRegex(
            CryptoFirstLiveCoordinatorError, "foreign-scope-row-rejected"
        ):
            self.store.status()

    def test_foreign_scope_event_row_is_rejected(self) -> None:
        self.reserve()
        conn = sqlite3.connect(self.path)
        try:
            conn.execute(
                "UPDATE crypto_first_live_events SET scope_key='FOREIGN_SCOPE'"
            )
            conn.commit()
        finally:
            conn.close()
        with self.assertRaisesRegex(
            CryptoFirstLiveCoordinatorError, "foreign-scope-row-rejected"
        ):
            self.store.status()


if __name__ == "__main__":
    unittest.main()
