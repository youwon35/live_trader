from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest import mock

from live_trader.crypto_first_live_coordinator import (
    CryptoFirstLiveCoordinatorError,
    DurableCryptoFirstLiveCoordinator,
)
from live_trader.crypto_first_live_high_water import (
    DurableCryptoFirstLiveHighWaterAnchor,
)
from live_trader.crypto_first_live_runtime import (
    CryptoFirstLiveRuntime,
    CryptoFirstLiveRuntimeError,
    InProcessRouteLockAuthority,
)
from live_trader.upbit_continuous_functional import (
    verify_upbit_global_first_live_authority,
)


def h(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class Clock:
    wall = 2_100_000_000.0
    mono_value = 70_000.0

    def time(self) -> float:
        return self.wall

    def mono(self) -> float:
        return self.mono_value

    def advance(self, seconds: float) -> None:
        self.wall += seconds
        self.mono_value += seconds


class Authorities:
    def __init__(self, clock: Clock) -> None:
        self.clock = clock
        self.owner_available = True

    @staticmethod
    def _echo(request, fields):
        return {field: request[field] for field in fields}

    def reservation(self, request):
        fields = (
            "scope", "runId", "lane", "sessionId", "permitId",
            "accountFingerprint", "baselineHash", "codeHash", "approvalId",
            "permitHash", "ownerIdentityHash", "coordinatorRevision",
            "publicationHash", "presentedHash",
        )
        return {
            "schemaVersion": "crypto-first-live-reservation-receipt/v1",
            **self._echo(request, fields),
            "evidenceId": "reservation-runtime-evidence-0001",
            "observedEpoch": self.clock.wall,
            "expiresEpoch": self.clock.wall + 20,
            "verified": True,
            "durable": True,
            "restartVerifiable": True,
        }

    def approval(self, request):
        fields = (
            "scope", "runId", "lane", "sessionId", "permitId",
            "accountFingerprint", "baselineHash", "codeHash", "approvalId",
            "permitHash", "reservationEvidenceHash",
            "reservationReceiptHash", "finalApprovalHash", "ownerEpoch",
            "coordinatorRevision", "publicationHash",
        )
        return {
            "schemaVersion": "crypto-first-live-final-approval-receipt/v1",
            **self._echo(request, fields),
            "approvalConsumptionId": "runtime-approval-consumption-0001",
            "consumed": True,
            "oneUse": True,
            "durable": True,
            "restartVerifiable": True,
        }

    def absence(self, request):
        fields = (
            "scope", "runId", "lane", "sessionId", "permitId",
            "accountFingerprint", "priorOwnerIdentityHash", "priorOwnerEpoch",
            "coordinatorRevision", "publicationHash",
        )
        return {
            "schemaVersion": "crypto-first-live-owner-absence-receipt/v1",
            **self._echo(request, fields),
            "absenceProofId": "runtime-owner-absence-proof-0001",
            "observedEpoch": self.clock.wall,
            "expiresEpoch": self.clock.wall + 20,
            "absent": True,
            "durable": True,
            "restartVerifiable": True,
        }

    def owner(self, _request) -> bool:
        return self.owner_available


class CryptoFirstLiveRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.clock = Clock()
        self.authorities = Authorities(self.clock)
        self.route = InProcessRouteLockAuthority()
        self.shared_route_lock = threading.RLock()
        self.anchor = DurableCryptoFirstLiveHighWaterAnchor(
            self.root / "high-water.sqlite3", clock=self.clock.time
        )
        self.coordinator = DurableCryptoFirstLiveCoordinator(
            self.root / "coordinator.sqlite3",
            clock=self.clock.time,
            monotonic_clock=self.clock.mono,
            high_water_anchor=self.anchor,
            reservation_evidence_verifier=self.authorities.reservation,
            final_approval_consumer=self.authorities.approval,
            startup_owner_absent_reader=self.authorities.absence,
            installation_validator=lambda _request: True,
            owner_identity_verifier=self.authorities.owner,
            route_lock_verifier=self.route.verify,
        )
        self.runtime = self.make_runtime()
        self.assertTrue(self.runtime.prepare()["prepared"])

    def tearDown(self) -> None:
        self.temp.cleanup()

    def make_runtime(self) -> CryptoFirstLiveRuntime:
        def identity(lane: str, account: str):
            return {
                "pid": 4242,
                "processStartEpoch": self.clock.wall - 10,
                "bootId": "runtime-test-boot-0001",
                "applicationLeaseEpoch": self.clock.wall - 5,
                "accountLeaseScope": (
                    f"crypto-first-live-account:{lane}:{account}"
                ),
            }

        return CryptoFirstLiveRuntime(
            coordinator=self.coordinator,
            route_lock_authority=self.route,
            account_lease_holder=lambda lane, account: {
                "acquired": True,
                "lane": lane,
                "accountFingerprint": account,
            },
            owner_identity_reader=identity,
            upbit_route_boundary=lambda: self.shared_route_lock,
            binance_route_boundary=lambda: self.shared_route_lock,
        )

    def reserve(self, lane: str = "BINANCE_SPOT") -> dict:
        suffix = lane.lower().replace("_", "-")
        return self.runtime.reserve_inert(
            lane=lane,
            session_id=f"session-{suffix}-runtime-0001",
            permit_id=f"permit-{suffix}-runtime-0001",
            account_fingerprint=h(lane + "-account"),
            baseline_hash=h(lane + "-baseline"),
            code_hash=h(lane + "-code"),
            approval_id=f"approval-{suffix}-runtime-0001",
            permit_hash=h(lane + "-permit"),
            reservation_evidence={"durable": True, "workingOrders": 0},
            route_scope_hash=h(lane + "-route"),
            broker_owner_identity_hash=h(lane + "-owner"),
        )

    def activate(self, reserved: dict) -> dict:
        with mock.patch.multiple(
            "live_trader.crypto_first_live_coordinator",
            CRYPTO_FIRST_LIVE_ACTIVATION_RELEASED=True,
            CRYPTO_FIRST_LIVE_WORM_ROLLBACK_RELEASED=True,
        ):
            return self.runtime.activate({
                "approvalId": reserved["approvalId"],
                "oneUse": True,
            })

    def binance_request(self, *, cleanup: bool) -> dict:
        status = self.coordinator.status()
        return {
            "purpose": "MUTATION_FINAL_PRE_MARKER",
            "session_id": status["sessionId"],
            "permit_id": status["permitId"],
            "permit_hash": status["permitHash"],
            "account_fingerprint": status["accountFingerprint"],
            "cleanup_only": cleanup,
        }

    def test_two_lane_reservation_race_has_one_durable_winner(self) -> None:
        barrier = threading.Barrier(2)
        wins: list[str] = []
        losses: list[str] = []

        def claim(lane: str) -> None:
            barrier.wait()
            try:
                wins.append(self.reserve(lane)["lane"])
            except Exception as exc:  # expected loser is a closed CAS
                losses.append(str(exc))

        threads = [
            threading.Thread(target=claim, args=(lane,))
            for lane in ("UPBIT", "BINANCE_SPOT")
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)
        self.assertEqual((1, 1), (len(wins), len(losses)))
        self.assertEqual(wins[0], self.coordinator.status()["lane"])
        self.assertNotIn("ownerToken", self.runtime.status())

    def test_owner_lease_loss_blocks_final_edge_without_sender(self) -> None:
        self.activate(self.reserve())
        self.authorities.owner_available = False
        sender = mock.Mock()
        with mock.patch.multiple(
            "live_trader.crypto_first_live_coordinator",
            CRYPTO_FIRST_LIVE_ACTIVATION_RELEASED=True,
            CRYPTO_FIRST_LIVE_WORM_ROLLBACK_RELEASED=True,
        ), self.assertRaises(
            (CryptoFirstLiveCoordinatorError, CryptoFirstLiveRuntimeError)
        ):
            self.runtime.binance_authority(
                **self.binance_request(cleanup=False)
            )
        sender.assert_not_called()
        self.assertEqual(
            "RECONCILIATION_REQUIRED", self.coordinator.status()["phase"]
        )

    def test_reconciliation_never_claims_entry_authority_revoked(self) -> None:
        self.activate(self.reserve())
        self.authorities.owner_available = False
        with mock.patch.multiple(
            "live_trader.crypto_first_live_coordinator",
            CRYPTO_FIRST_LIVE_ACTIVATION_RELEASED=True,
            CRYPTO_FIRST_LIVE_WORM_ROLLBACK_RELEASED=True,
        ), self.assertRaises(
            (CryptoFirstLiveCoordinatorError, CryptoFirstLiveRuntimeError)
        ):
            self.runtime.binance_authority(
                **self.binance_request(cleanup=False)
            )
        result = self.runtime.revoke_entry_before_cleanup("hostile-stop")
        self.assertFalse(result["ok"])
        self.assertFalse(result["entryAuthorityRevoked"])
        self.assertEqual("RECONCILIATION_REQUIRED", result["state"])
        self.assertIn("unverifiable", result["reason"])

    def test_stop_serializes_before_any_later_pre_post(self) -> None:
        self.activate(self.reserve())
        would_send: list[str] = []
        entered = threading.Event()
        release = threading.Event()

        def pre_post_wins() -> None:
            with mock.patch.multiple(
                "live_trader.crypto_first_live_coordinator",
                CRYPTO_FIRST_LIVE_ACTIVATION_RELEASED=True,
                CRYPTO_FIRST_LIVE_WORM_ROLLBACK_RELEASED=True,
            ):
                with self.shared_route_lock:
                    self.runtime.binance_authority(
                        **self.binance_request(cleanup=False)
                    )
                    entered.set()
                    release.wait(5)
                    would_send.append("already-authorized-before-stop")

        thread = threading.Thread(target=pre_post_wins)
        thread.start()
        self.assertTrue(entered.wait(5))
        stop_result: list[dict] = []
        stop_thread = threading.Thread(
            target=lambda: stop_result.append(
                self.runtime.revoke_entry_before_cleanup("operator-stop")
            )
        )
        stop_thread.start()
        self.assertTrue(stop_thread.is_alive())
        release.set()
        thread.join(5)
        stop_thread.join(5)
        self.assertEqual(["already-authorized-before-stop"], would_send)
        self.assertTrue(stop_result[0]["entryAuthorityRevoked"])
        sender = mock.Mock()
        with self.assertRaises(
            (CryptoFirstLiveCoordinatorError, CryptoFirstLiveRuntimeError)
        ):
            self.runtime.binance_authority(**self.binance_request(cleanup=False))
            sender()
        sender.assert_not_called()

    def test_startup_expiry_becomes_cleanup_only_without_disk_token(self) -> None:
        self.reserve()
        self.clock.advance(61)
        restarted = self.make_runtime()
        status = restarted.prepare()
        self.assertEqual("CLEANUP_ONLY", status["phase"])
        self.assertFalse(status["ownerTokenPersisted"])
        self.assertFalse(restarted.status()["processMemoryOwnerPresent"])

    def test_cleanup_projection_allowed_after_entry_revocation(self) -> None:
        self.activate(self.reserve())
        revoked = self.runtime.revoke_entry_before_cleanup("kill-switch")
        self.assertEqual("CLEANUP_ONLY", revoked["state"])
        cleanup = self.runtime.binance_authority(
            **self.binance_request(cleanup=True)
        )
        self.assertEqual("CLEANUP_ONLY", cleanup["phase"])
        self.assertFalse(cleanup["entryAuthorityOpen"])
        sender = mock.Mock()
        with self.assertRaises(
            (CryptoFirstLiveCoordinatorError, CryptoFirstLiveRuntimeError)
        ):
            self.runtime.binance_authority(**self.binance_request(cleanup=False))
            sender()
        sender.assert_not_called()

    def test_stop_revokes_approved_inert_before_broker_cleanup(self) -> None:
        self.reserve()
        sender = mock.Mock()
        revoked = self.runtime.revoke_entry_before_cleanup("operator-stop")
        self.assertEqual("CLEANUP_ONLY", revoked["state"])
        sender.assert_not_called()

    def test_stop_revokes_preparing_when_proof_seal_fails(self) -> None:
        self.coordinator.reservation_evidence_verifier = (
            lambda _request: (_ for _ in ()).throw(
                RuntimeError("detached-proof-unavailable")
            )
        )
        with self.assertRaisesRegex(RuntimeError, "proof-unavailable"):
            self.reserve()
        self.assertEqual("PREPARING", self.coordinator.status()["phase"])
        sender = mock.Mock()
        revoked = self.runtime.revoke_entry_before_cleanup("kill-switch")
        self.assertEqual("CLEANUP_ONLY", revoked["state"])
        sender.assert_not_called()

    def test_reentrant_prepare_retains_same_process_cleanup_bearer(self) -> None:
        self.reserve()
        before = self.runtime.status()
        repeated = self.runtime.prepare()
        after = self.runtime.status()
        self.assertTrue(repeated["prepared"])
        self.assertTrue(before["processMemoryOwnerPresent"])
        self.assertTrue(after["processMemoryOwnerPresent"])
        self.assertTrue(after["processMemoryOwnerMatches"])
        revoked = self.runtime.revoke_entry_before_cleanup("operator-stop")
        self.assertTrue(revoked["entryAuthorityRevoked"])

    def test_upbit_adapter_exact_projection_is_self_verified(self) -> None:
        self.activate(self.reserve("UPBIT"))
        status = self.coordinator.status()
        scope = SimpleNamespace(
            permit_id=status["permitId"],
            permit_hash=status["permitHash"],
            account_fingerprint=status["accountFingerprint"],
            route_scope_hash=h("UPBIT-route"),
            ends_at=datetime.fromtimestamp(
                status["hardStopEpoch"], tz=timezone.utc
            ),
        )
        now = datetime.fromtimestamp(self.clock.wall, tz=timezone.utc)
        with mock.patch.multiple(
            "live_trader.crypto_first_live_coordinator",
            CRYPTO_FIRST_LIVE_ACTIVATION_RELEASED=True,
            CRYPTO_FIRST_LIVE_WORM_ROLLBACK_RELEASED=True,
        ):
            value = verify_upbit_global_first_live_authority(
                self.runtime.upbit_authority,
                scope=scope,
                session_id=status["sessionId"],
                owner_identity_hash=h("UPBIT-owner"),
                action="ACTIVATE",
                cleanup=False,
                now=now,
            )
        self.assertEqual("ACTIVE", value["phase"])
        self.assertTrue(value["entryAuthorityOpen"])
        self.assertFalse(value["cleanupAuthorityOpen"])

    def test_root_release_and_network_stay_closed(self) -> None:
        status = self.runtime.status()
        self.assertTrue(status["safeHold"])
        self.assertFalse(status["networkOrderPostAllowed"])
        self.assertFalse(self.runtime.production_entry_released())


if __name__ == "__main__":
    unittest.main()
