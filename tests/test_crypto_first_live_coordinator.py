from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import tempfile
import threading
import unittest
from unittest import mock

from live_trader.crypto_first_live_coordinator import (
    CRYPTO_FIRST_LIVE_ACTIVATION_RELEASED,
    CryptoFirstLiveCoordinatorError,
    DurableCryptoFirstLiveCoordinator,
)
from live_trader.crypto_first_live_high_water import (
    CryptoFirstLiveHighWaterError,
    DurableCryptoFirstLiveHighWaterAnchor,
)


def h(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def canonical_h(value: dict) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def supervised_contract_for(value: dict, now: float) -> dict:
    approval_body = {
        "schemaVersion": (
            "crypto-first-live-supervised-user-approval-receipt/v1"
        ),
        "approvalId": value["approvalId"],
        "approvalBindingHash": h("supervised-binding"),
        "consumptionId": "supervised-consumption-00000001",
        "exactUserApproval": True,
        "consumed": True,
        "oneUse": True,
        "durable": True,
        "restartVerifiable": True,
        "approvedEpoch": now - 1,
    }
    receipt = {
        **approval_body,
        "receiptHash": canonical_h(approval_body),
    }
    body = {
        "schemaVersion": "crypto-first-live-supervised-non-promotion/v1",
        "mode": "SUPERVISED_NON_PROMOTION",
        "lane": value["lane"],
        "sessionId": value["sessionId"],
        "permitId": value["permitId"],
        "permitHash": value["permitHash"],
        "operatorApproval": receipt,
        "riskCaps": (
            {
                "currency": "KRW",
                "maxOrderNotional": "10000",
                "maxLoss": "1000",
                "activeSeconds": 7200,
            }
            if value["lane"] == "UPBIT"
            else {
                "currency": "USDT",
                "maxOrderNotional": "10",
                "maxLoss": "1",
                "activeSeconds": 7200,
            }
        ),
        "executionConstraints": {
            "singleLane": True,
            "foregroundMonitoringRequired": True,
            "dualDurableStoresRequired": True,
            "independentAccountOsLeaseRequired": True,
            "oneUseNetworkCapabilityOnly": True,
            "promotionEligible": False,
            "realE2EEligible": False,
            "productionPromotionAllowed": False,
        },
        "auditAnchor": {
            "schemaVersion": "crypto-first-live-supervised-audit-anchor/v1",
            "kind": "WINDOWS_EVENT_LOG_SIGNED",
            "authorityId": "windows-event-authority-00000001",
            "checkpointId": "windows-event-checkpoint-00000001",
            "receiptHash": h("supervised-audit-anchor"),
            "signatureVerified": True,
            "appendOnlyObserved": True,
            "durable": True,
            "restartVerifiable": True,
            "formalWorm": False,
        },
        "residualRisk": {
            "formalWormAbsent": True,
            "sameHostAdministratorCanClearOrRewriteAudit": True,
            "acceptedByUser": True,
            "nonPromotionOnly": True,
        },
    }
    return {**body, "contractHash": canonical_h(body)}


class DualClock:
    wall = 2_000_000_000.0
    mono_value = 50_000.0

    def time(self) -> float:
        return self.wall

    def mono(self) -> float:
        return self.mono_value

    def advance(self, seconds: float) -> None:
        self.wall += seconds
        self.mono_value += seconds


class Authorities:
    def __init__(self, clock: DualClock) -> None:
        self.clock = clock
        self.reservation_hook = None
        self.approval_hook = None
        self.terminal_hook = None
        self.route_hook = None
        self.owner_available = True

    @staticmethod
    def echo(request, fields):
        return {key: request[key] for key in fields}

    def reservation(self, request):
        request = dict(request)
        if self.reservation_hook:
            self.reservation_hook(request)
        fields = (
            "scope", "runId", "lane", "sessionId", "permitId",
            "accountFingerprint", "baselineHash", "codeHash", "approvalId",
            "permitHash", "ownerIdentityHash", "coordinatorRevision",
            "publicationHash", "presentedHash",
        )
        return {
            "schemaVersion": "crypto-first-live-reservation-receipt/v1",
            **self.echo(request, fields),
            "evidenceId": "reservation-evidence-00000001",
            "observedEpoch": self.clock.wall,
            "expiresEpoch": self.clock.wall + 20,
            "verified": True, "durable": True, "restartVerifiable": True,
        }

    def approval(self, request):
        request = dict(request)
        if self.approval_hook:
            self.approval_hook(request)
        fields = (
            "scope", "runId", "lane", "sessionId", "permitId",
            "accountFingerprint", "baselineHash", "codeHash", "approvalId",
            "permitHash", "reservationEvidenceHash",
            "reservationReceiptHash", "finalApprovalHash", "ownerEpoch",
            "coordinatorRevision", "publicationHash",
        )
        return {
            "schemaVersion": "crypto-first-live-final-approval-receipt/v1",
            **self.echo(request, fields),
            "approvalConsumptionId": "approval-consumption-00000001",
            "consumed": True, "oneUse": True, "durable": True,
            "restartVerifiable": True,
        }

    def terminal(self, request):
        request = dict(request)
        if self.terminal_hook:
            self.terminal_hook(request)
        fields = (
            "scope", "runId", "lane", "sessionId", "permitId",
            "accountFingerprint", "baselineHash", "codeHash", "approvalId",
            "permitHash", "ownerEpoch", "coordinatorRevision",
            "publicationHash", "terminalEvidenceHash",
        )
        return {
            "schemaVersion": "crypto-first-live-terminal-receipt/v1",
            **self.echo(request, fields),
            "terminalEvidenceId": "terminal-evidence-00000001",
            "verified": True, "durable": True, "restartVerifiable": True,
        }

    def absence(self, request):
        request = dict(request)
        fields = (
            "scope", "runId", "lane", "sessionId", "permitId",
            "accountFingerprint", "priorOwnerIdentityHash", "priorOwnerEpoch",
            "coordinatorRevision", "publicationHash",
        )
        return {
            "schemaVersion": "crypto-first-live-owner-absence-receipt/v1",
            **self.echo(request, fields),
            "absenceProofId": "owner-absence-proof-00000001",
            "observedEpoch": self.clock.wall,
            "expiresEpoch": self.clock.wall + 20,
            "absent": True, "durable": True, "restartVerifiable": True,
        }

    def route(self, request):
        request = dict(request)
        if self.route_hook:
            self.route_hook(request)
        fields = tuple(
            key for key in request if key not in {"schemaVersion", "presented"}
        )
        return {
            "schemaVersion": "crypto-first-live-route-lock-proof/v1",
            **self.echo(request, fields),
            "proofId": "common-route-lock-proof-00000001",
            "held": True, "exclusive": True,
        }

    def owner(self, _request) -> bool:
        return self.owner_available


class CryptoFirstLiveCoordinatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.path = root / "coordinator.sqlite3"
        self.anchor_path = root / "authority" / "high-water.sqlite3"
        self.clock = DualClock()
        self.authorities = Authorities(self.clock)
        self.anchor = DurableCryptoFirstLiveHighWaterAnchor(
            self.anchor_path, clock=self.clock.time
        )
        self.store = self.make_store()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def make_store(self, path=None, **overrides):
        args = {
            "clock": self.clock.time,
            "monotonic_clock": self.clock.mono,
            "high_water_anchor": self.anchor,
            "reservation_evidence_verifier": self.authorities.reservation,
            "final_approval_consumer": self.authorities.approval,
            "terminal_evidence_verifier": self.authorities.terminal,
            "startup_owner_absent_reader": self.authorities.absence,
            "installation_validator": lambda _request: True,
            "owner_identity_verifier": self.authorities.owner,
            "route_lock_verifier": self.authorities.route,
        }
        args.update(overrides)
        return DurableCryptoFirstLiveCoordinator(path or self.path, **args)

    def identity(self, lane="UPBIT"):
        return {
            "pid": 1234,
            "processStartEpoch": self.clock.wall - 10,
            "bootId": "windows-boot-identity-0001",
            "applicationLeaseEpoch": self.clock.wall - 5,
            "accountLeaseScope":
                f"crypto-first-live-account:{lane}:{h(lane + '-account')}",
        }

    def begin(self, lane="UPBIT"):
        return self.store.begin_reservation(
            lane=lane,
            session_id=f"session-{lane.lower()}-00000001",
            permit_id=f"permit-{lane.lower()}-00000001",
            account_fingerprint=h(lane + "-account"),
            baseline_hash=h(lane + "-baseline"),
            code_hash=h(lane + "-code"),
            approval_id=f"approval-{lane.lower()}-00000001",
            permit_hash=h(lane + "-permit"),
            owner_identity=self.identity(lane),
        )

    def reserve(self, lane="UPBIT"):
        claim = self.begin(lane)
        return self.store.seal_reservation(
            run_id=claim["runId"], owner_token=claim["ownerToken"],
            owner_epoch=claim["ownerEpoch"],
            reservation_evidence={"fresh": True, "workingOrders": 0},
        )

    def activate(self, value):
        with mock.patch.multiple(
            "live_trader.crypto_first_live_coordinator",
            CRYPTO_FIRST_LIVE_ACTIVATION_RELEASED=True,
            CRYPTO_FIRST_LIVE_WORM_ROLLBACK_RELEASED=True,
        ):
            return self.store.activate(
                run_id=value["runId"], owner_token=value["ownerToken"],
                owner_epoch=value["ownerEpoch"],
                final_approval={"oneUse": True},
            )

    def revoke(self, value):
        status = self.store.status()
        return self.store.revoke_entry(
            run_id=value["runId"], expected_revision=status["revision"],
            reason="operator-stop",
        )

    def test_claim_then_fresh_seal_stays_inert(self) -> None:
        self.assertFalse(CRYPTO_FIRST_LIVE_ACTIVATION_RELEASED)
        claim = self.begin()
        self.assertEqual("PREPARING", claim["phase"])
        self.assertFalse(claim["entryAuthorityOpen"])
        self.assertEqual(1, self.anchor.status()["revision"])
        sealed = self.store.seal_reservation(
            run_id=claim["runId"], owner_token=claim["ownerToken"],
            owner_epoch=claim["ownerEpoch"],
            reservation_evidence={"fresh": True},
        )
        self.assertEqual("APPROVED_INERT", sealed["phase"])
        self.assertFalse(sealed["networkOrderPostAllowed"])
        self.assertTrue(sealed["reservationReceiptHash"])

    def test_inert_heartbeat_renews_only_exact_approved_owner(self) -> None:
        value = self.reserve()
        before = self.store.status()
        self.clock.advance(20)
        renewed = self.store.heartbeat_inert(
            run_id=value["runId"],
            owner_token=value["ownerToken"],
            owner_epoch=value["ownerEpoch"],
            lane=value["lane"],
            session_id=value["sessionId"],
            permit_id=value["permitId"],
            permit_hash=value["permitHash"],
            account_fingerprint=value["accountFingerprint"],
            baseline_hash=value["baselineHash"],
            code_hash=value["codeHash"],
            owner_identity_hash=value["ownerIdentityHash"],
            expected_revision=before["revision"],
        )
        self.assertEqual("APPROVED_INERT", renewed["phase"])
        self.assertEqual(before["revision"] + 1, renewed["revision"])
        self.assertGreater(
            renewed["ownerLeaseExpiresEpoch"],
            before["ownerLeaseExpiresEpoch"],
        )
        self.assertEqual(0.0, renewed["hardStopEpoch"])
        self.assertFalse(renewed["entryAuthorityOpen"])
        self.assertFalse(renewed["networkCapabilityOpen"])
        self.assertFalse(renewed["promotionEligible"])
        self.assertFalse(renewed["realE2EEligible"])

    def test_inert_heartbeat_binding_tamper_is_socket_zero(self) -> None:
        value = self.reserve()
        revision = self.store.status()["revision"]
        with self.assertRaisesRegex(
            CryptoFirstLiveCoordinatorError,
            "inert-heartbeat-binding-changed",
        ):
            self.store.heartbeat_inert(
                run_id=value["runId"],
                owner_token=value["ownerToken"],
                owner_epoch=value["ownerEpoch"],
                lane=value["lane"],
                session_id=value["sessionId"],
                permit_id=value["permitId"],
                permit_hash=h("hostile-permit"),
                account_fingerprint=value["accountFingerprint"],
                baseline_hash=value["baselineHash"],
                code_hash=value["codeHash"],
                owner_identity_hash=value["ownerIdentityHash"],
                expected_revision=revision,
            )
        status = self.store.status()
        self.assertEqual("APPROVED_INERT", status["phase"])
        self.assertEqual(revision, status["revision"])

    def test_supervised_activation_validates_then_stays_durable_hold(
        self,
    ) -> None:
        value = self.reserve()
        before = self.store.status()
        contract = supervised_contract_for(value, self.clock.wall)
        with self.assertRaisesRegex(
            CryptoFirstLiveCoordinatorError,
            "supervised-non-promotion-release-held",
        ):
            self.store.activate_supervised_non_promotion(
                run_id=value["runId"],
                owner_token=value["ownerToken"],
                owner_epoch=value["ownerEpoch"],
                lane=value["lane"],
                session_id=value["sessionId"],
                permit_id=value["permitId"],
                permit_hash=value["permitHash"],
                account_fingerprint=value["accountFingerprint"],
                baseline_hash=value["baselineHash"],
                code_hash=value["codeHash"],
                owner_identity_hash=value["ownerIdentityHash"],
                expected_revision=before["revision"],
                approval_receipt=contract["operatorApproval"],
                supervised_contract=contract,
                supervised_contract_hash=contract["contractHash"],
            )
        after = self.store.status()
        self.assertEqual("APPROVED_INERT", after["phase"])
        self.assertEqual(before["revision"], after["revision"])
        self.assertFalse(after["entryAuthorityOpen"])
        conn = sqlite3.connect(self.path)
        try:
            self.assertEqual(
                0,
                conn.execute(
                    "SELECT COUNT(*) FROM "
                    "crypto_first_live_supervised_consumptions"
                ).fetchone()[0],
            )
        finally:
            conn.close()

    def test_supervised_flags_alone_still_cannot_create_active(self) -> None:
        value = self.reserve()
        before = self.store.status()
        contract = supervised_contract_for(value, self.clock.wall)
        with (
            mock.patch.multiple(
                "live_trader.crypto_first_live_coordinator",
                CRYPTO_FIRST_LIVE_SUPERVISED_NON_PROMOTION_RELEASED=True,
                CRYPTO_FIRST_LIVE_SUPERVISED_NETWORK_CAPABILITY_RELEASED=True,
            ),
            self.assertRaisesRegex(
                CryptoFirstLiveCoordinatorError,
                "active-transition-not-released",
            ),
        ):
            self.store.activate_supervised_non_promotion(
                run_id=value["runId"],
                owner_token=value["ownerToken"],
                owner_epoch=value["ownerEpoch"],
                lane=value["lane"],
                session_id=value["sessionId"],
                permit_id=value["permitId"],
                permit_hash=value["permitHash"],
                account_fingerprint=value["accountFingerprint"],
                baseline_hash=value["baselineHash"],
                code_hash=value["codeHash"],
                owner_identity_hash=value["ownerIdentityHash"],
                expected_revision=before["revision"],
                approval_receipt=contract["operatorApproval"],
                supervised_contract=contract,
                supervised_contract_hash=contract["contractHash"],
            )
        after = self.store.status()
        self.assertEqual("APPROVED_INERT", after["phase"])
        self.assertEqual(before["revision"], after["revision"])
        self.assertFalse(after["entryAuthorityOpen"])

    def test_supervised_activation_rejects_forged_receipt_before_hold(
        self,
    ) -> None:
        value = self.reserve()
        before = self.store.status()
        contract = supervised_contract_for(value, self.clock.wall)
        forged = dict(contract["operatorApproval"])
        forged["consumptionId"] = "supervised-consumption-hostile"
        with self.assertRaisesRegex(
            CryptoFirstLiveCoordinatorError,
            "contract-binding-changed",
        ):
            self.store.activate_supervised_non_promotion(
                run_id=value["runId"],
                owner_token=value["ownerToken"],
                owner_epoch=value["ownerEpoch"],
                lane=value["lane"],
                session_id=value["sessionId"],
                permit_id=value["permitId"],
                permit_hash=value["permitHash"],
                account_fingerprint=value["accountFingerprint"],
                baseline_hash=value["baselineHash"],
                code_hash=value["codeHash"],
                owner_identity_hash=value["ownerIdentityHash"],
                expected_revision=before["revision"],
                approval_receipt=forged,
                supervised_contract=contract,
                supervised_contract_hash=contract["contractHash"],
            )
        self.assertEqual(before["revision"], self.store.status()["revision"])

    def test_additive_supervised_schema_migrates_old_v3_store(self) -> None:
        conn = sqlite3.connect(self.path)
        try:
            conn.execute(
                "DROP TABLE crypto_first_live_supervised_consumptions"
            )
            conn.commit()
        finally:
            conn.close()
        reopened = self.make_store()
        self.assertEqual("IDLE", reopened.status()["phase"])
        conn = sqlite3.connect(self.path)
        try:
            columns = tuple(
                row[1]
                for row in conn.execute(
                    "PRAGMA table_xinfo("
                    "crypto_first_live_supervised_consumptions)"
                )
            )
        finally:
            conn.close()
        self.assertEqual(
            (
                "consumption_id",
                "approval_id",
                "run_id",
                "lane",
                "session_id",
                "permit_id",
                "permit_hash",
                "owner_identity_hash",
                "contract_hash",
                "receipt_hash",
                "coordinator_revision",
                "publication_hash",
                "consumed_epoch",
            ),
            columns,
        )

    def test_global_idle_reservation_blocks_new_lane_until_caller_commit(self) -> None:
        request = {
            "schemaVersion": "crypto-first-live-global-idle-reservation/v1",
            "purpose": "PROGRAM_LEDGER_BINANCE_CASH_TRANSFER_COMMIT",
            "accountFingerprint": h("idle-account"),
            "consumptionKey": h("idle-consumption"),
            "truthHash": h("idle-truth"),
        }
        attempting = threading.Event()
        finished = threading.Event()
        outcomes: list[object] = []

        def reserve_lane() -> None:
            attempting.set()
            try:
                outcomes.append(self.begin("BINANCE_SPOT"))
            except BaseException as exc:
                outcomes.append(exc)
            finally:
                finished.set()

        with self.store.global_idle_reservation(request) as receipt:
            self.assertEqual("IDLE", receipt["phase"])
            self.assertTrue(receipt["held"])
            self.assertTrue(receipt["exclusive"])
            thread = threading.Thread(target=reserve_lane)
            thread.start()
            self.assertTrue(attempting.wait(2))
            self.assertFalse(finished.wait(0.2))
        self.assertTrue(finished.wait(5))
        thread.join(5)
        self.assertEqual(1, len(outcomes))
        self.assertIsInstance(outcomes[0], dict)
        self.assertEqual("PREPARING", outcomes[0]["phase"])

    def test_global_idle_reservation_rejects_nonterminal_owner(self) -> None:
        self.begin("UPBIT")
        request = {
            "schemaVersion": "crypto-first-live-global-idle-reservation/v1",
            "purpose": "PROGRAM_LEDGER_BINANCE_CASH_TRANSFER_COMMIT",
            "accountFingerprint": h("idle-account"),
            "consumptionKey": h("idle-consumption"),
            "truthHash": h("idle-truth"),
        }
        with self.assertRaisesRegex(
            CryptoFirstLiveCoordinatorError, "global-idle-unavailable"
        ):
            with self.store.global_idle_reservation(request):
                self.fail("nonterminal owner must not grant IDLE")

    def test_reservation_receipt_publication_tamper_fails_closed(self) -> None:
        claim = self.begin()
        original = self.authorities.reservation

        def tamper(request):
            result = original(request)
            result["publicationHash"] = h("wrong")
            return result

        self.store.reservation_evidence_verifier = tamper
        with self.assertRaisesRegex(
            CryptoFirstLiveCoordinatorError, "reservation-receipt-invalid"
        ):
            self.store.seal_reservation(
                run_id=claim["runId"], owner_token=claim["ownerToken"],
                owner_epoch=claim["ownerEpoch"],
                reservation_evidence={"fresh": True},
            )
        self.assertEqual("PREPARING", self.store.status()["phase"])

    def test_expired_reservation_receipt_fails_closed(self) -> None:
        claim = self.begin()
        original = self.authorities.reservation

        def stale(request):
            result = original(request)
            result["observedEpoch"] -= 40
            return result

        self.store.reservation_evidence_verifier = stale
        with self.assertRaisesRegex(
            CryptoFirstLiveCoordinatorError, "reservation-receipt-invalid"
        ):
            self.store.seal_reservation(
                run_id=claim["runId"], owner_token=claim["ownerToken"],
                owner_epoch=claim["ownerEpoch"], reservation_evidence={"x": 1}
            )

    def test_two_lane_claim_race_has_one_winner(self) -> None:
        barrier = threading.Barrier(2)
        wins, errors = [], []

        def run(lane):
            local = self.make_store()
            barrier.wait()
            try:
                wins.append(local.begin_reservation(
                    lane=lane, session_id=f"session-{lane.lower()}-00000002",
                    permit_id=f"permit-{lane.lower()}-00000002",
                    account_fingerprint=h(lane + "-account"),
                    baseline_hash=h(lane + "-baseline"),
                    code_hash=h(lane + "-code"),
                    approval_id=f"approval-{lane.lower()}-00000002",
                    permit_hash=h(lane + "-permit"),
                    owner_identity=self.identity(lane),
                )["lane"])
            except Exception as exc:
                errors.append(str(exc))

        threads = [threading.Thread(target=run, args=(lane,)) for lane in (
            "UPBIT", "BINANCE_SPOT"
        )]
        for thread in threads: thread.start()
        for thread in threads: thread.join(10)
        self.assertEqual((1, 1), (len(wins), len(errors)))
        self.assertTrue(all(not thread.is_alive() for thread in threads))

    def test_activation_claim_and_one_use_receipt_are_exact(self) -> None:
        value = self.reserve()
        seen = {}
        original = self.authorities.approval

        def inspect(request):
            seen.update(request)
            self.assertEqual(
                "ACTIVATION_PREPARING", self.store.status()["phase"]
            )
            return original(request)

        self.store.final_approval_consumer = inspect
        active = self.activate(value)
        self.assertEqual("ACTIVE", active["phase"])
        self.assertTrue(active["finalApprovalReceiptHash"])
        self.assertEqual(value["sessionId"], seen["sessionId"])
        self.assertEqual(value["permitHash"], seen["permitHash"])
        self.assertEqual(7200, active["hardStopEpoch"] - self.clock.wall)
        self.assertEqual(
            7200, active["hardStopMonotonic"] - self.clock.mono_value
        )

    def test_bad_approval_receipt_can_never_activate(self) -> None:
        value = self.reserve()
        original = self.authorities.approval

        def bad(request):
            result = original(request)
            result["permitHash"] = h("other")
            return result

        self.store.final_approval_consumer = bad
        with mock.patch.multiple(
            "live_trader.crypto_first_live_coordinator",
            CRYPTO_FIRST_LIVE_ACTIVATION_RELEASED=True,
            CRYPTO_FIRST_LIVE_WORM_ROLLBACK_RELEASED=True,
        ):
            with self.assertRaisesRegex(
                CryptoFirstLiveCoordinatorError, "approval-receipt-invalid"
            ):
                self.store.activate(
                    run_id=value["runId"], owner_token=value["ownerToken"],
                    owner_epoch=value["ownerEpoch"],
                    final_approval={"oneUse": True},
                )
        self.assertEqual("ACTIVATION_PREPARING", self.store.status()["phase"])

    def test_compile_latch_blocks_before_consumer(self) -> None:
        value = self.reserve()
        called = []
        self.store.final_approval_consumer = lambda request: called.append(request)
        with self.assertRaisesRegex(
            CryptoFirstLiveCoordinatorError, "activation-not-released"
        ):
            self.store.activate(
                run_id=value["runId"], owner_token=value["ownerToken"],
                owner_epoch=value["ownerEpoch"], final_approval={"oneUse": True}
            )
        self.assertEqual([], called)

    def test_monotonic_deadline_stops_even_without_wall_deadline(self) -> None:
        value = self.reserve()
        active = self.activate(value)
        self.clock.mono_value = active["hardStopMonotonic"]
        stopped = self.store.heartbeat(
            run_id=value["runId"], owner_token=value["ownerToken"],
            owner_epoch=value["ownerEpoch"],
        )
        self.assertEqual("CLEANUP_ONLY", stopped["phase"])

    def test_monotonic_rollback_audit_reconciles(self) -> None:
        value = self.reserve()
        self.activate(value)
        self.clock.mono_value -= 1
        self.assertEqual(
            "RECONCILIATION_REQUIRED", self.store.audit_startup()["phase"]
        )

    def test_coordinator_rollback_is_detected_by_anchor(self) -> None:
        value = self.reserve()
        old = Path(self.temp.name) / "old.sqlite3"
        shutil.copy2(self.path, old)
        self.activate(value)
        shutil.copy2(old, self.path)
        with self.assertRaisesRegex(
            CryptoFirstLiveCoordinatorError, "local-rollback-detected"
        ):
            self.store.status()

    def test_anchor_outage_leaves_one_repairable_closed_commit(self) -> None:
        class Flaky:
            fail = True

            def __init__(self, target): self.target = target

            def __call__(self, request):
                if request["action"] == "ADVANCE" and self.fail:
                    self.fail = False
                    raise RuntimeError("anchor-outage")
                return self.target(request)

        # A new anchor is required because database identity is single-owner.
        root = Path(self.temp.name) / "pending"
        anchor = DurableCryptoFirstLiveHighWaterAnchor(
            root / "anchor.sqlite3", clock=self.clock.time
        )
        pending = self.make_store(
            path=root / "coordinator.sqlite3", high_water_anchor=Flaky(anchor)
        )
        with self.assertRaisesRegex(RuntimeError, "anchor-outage"):
            pending.begin_reservation(
                lane="UPBIT", session_id="session-upbit-pending-01",
                permit_id="permit-upbit-pending-01",
                account_fingerprint=h("UPBIT-account"),
                baseline_hash=h("pending-baseline"), code_hash=h("pending-code"),
                approval_id="approval-upbit-pending-01",
                permit_hash=h("pending-permit"), owner_identity=self.identity(),
            )
        with self.assertRaisesRegex(
            CryptoFirstLiveCoordinatorError, "publication-incomplete"
        ):
            pending.status()
        self.assertEqual(
            "PREPARING", pending.repair_pending_publication()["phase"]
        )

    def test_second_database_is_rejected_by_same_anchor(self) -> None:
        with self.assertRaisesRegex(
            CryptoFirstLiveHighWaterError, "database-replaced"
        ):
            self.make_store(path=Path(self.temp.name) / "replacement.sqlite3")

    def test_revoke_and_finalize_require_exact_revision(self) -> None:
        value = self.reserve()
        active = self.activate(value)
        with self.assertRaisesRegex(
            CryptoFirstLiveCoordinatorError, "cleanup-cas-changed"
        ):
            self.store.revoke_entry(
                run_id=value["runId"],
                expected_revision=active["revision"] - 1, reason="stale"
            )
        cleanup = self.revoke(value)
        with self.assertRaisesRegex(
            CryptoFirstLiveCoordinatorError, "finalize-requires-cleanup"
        ):
            self.store.finalize(
                run_id=value["runId"], owner_token=value["ownerToken"],
                owner_epoch=value["ownerEpoch"],
                expected_revision=cleanup["revision"] - 1,
                terminal_evidence={"finalFlat": True},
            )
        final = self.store.finalize(
            run_id=value["runId"], owner_token=value["ownerToken"],
            owner_epoch=value["ownerEpoch"],
            expected_revision=cleanup["revision"],
            terminal_evidence={"finalFlat": True},
        )
        self.assertEqual("FINALIZED", final["phase"])

    def test_takeover_requires_exact_absence_receipt(self) -> None:
        value = self.reserve()
        self.activate(value)
        cleanup = self.revoke(value)
        self.clock.advance(61)
        original = self.authorities.absence

        def bad(request):
            result = original(request)
            result["coordinatorRevision"] -= 1
            return result

        self.store.startup_owner_absent_reader = bad
        with self.assertRaisesRegex(
            CryptoFirstLiveCoordinatorError, "owner-absence-invalid"
        ):
            self.store.takeover_expired_cleanup(
                run_id=value["runId"], expected_revision=cleanup["revision"],
                owner_identity=self.identity(),
            )
        self.store.startup_owner_absent_reader = original
        rotated = self.store.takeover_expired_cleanup(
            run_id=value["runId"], expected_revision=cleanup["revision"],
            owner_identity=self.identity(),
        )
        self.assertGreater(rotated["ownerEpoch"], value["ownerEpoch"])

    def dispatch_args(self, value, active):
        return {
            "purpose": "ENTRY_ORDER", "lane": value["lane"],
            "run_id": value["runId"], "session_id": value["sessionId"],
            "permit_id": value["permitId"], "permit_hash": value["permitHash"],
            "account_fingerprint": value["accountFingerprint"],
            "baseline_hash": value["baselineHash"], "code_hash": value["codeHash"],
            "owner_token": value["ownerToken"], "owner_epoch": value["ownerEpoch"],
            "expected_revision": active["revision"],
            "route_lock_evidence": {"heldBy": "live-route-00000001"},
        }

    def test_dispatch_projection_and_route_lock_toctou(self) -> None:
        value = self.reserve()
        active = self.activate(value)
        args = self.dispatch_args(value, active)
        with mock.patch.multiple(
            "live_trader.crypto_first_live_coordinator",
            CRYPTO_FIRST_LIVE_ACTIVATION_RELEASED=True,
            CRYPTO_FIRST_LIVE_WORM_ROLLBACK_RELEASED=True,
        ):
            projection = self.store.assert_dispatch_authority(**args)
            self.assertTrue(projection["entryAuthorityOpen"])
            self.assertTrue(projection["authorityHash"])

            def race(_request):
                self.authorities.route_hook = None
                self.store.heartbeat(
                    run_id=value["runId"], owner_token=value["ownerToken"],
                    owner_epoch=value["ownerEpoch"],
                )

            self.authorities.route_hook = race
            with self.assertRaisesRegex(
                CryptoFirstLiveCoordinatorError, "publication-changed"
            ):
                self.store.assert_dispatch_authority(**args)

    def test_dispatch_rechecks_authoritative_owner_and_latches_hold(self) -> None:
        value = self.reserve()
        active = self.activate(value)
        self.authorities.owner_available = False
        with mock.patch.multiple(
            "live_trader.crypto_first_live_coordinator",
            CRYPTO_FIRST_LIVE_ACTIVATION_RELEASED=True,
            CRYPTO_FIRST_LIVE_WORM_ROLLBACK_RELEASED=True,
        ):
            with self.assertRaisesRegex(
                CryptoFirstLiveCoordinatorError, "owner-identity-unverified"
            ):
                self.store.assert_dispatch_authority(
                    **self.dispatch_args(value, active)
                )
        self.assertEqual(
            "RECONCILIATION_REQUIRED", self.store.status()["phase"]
        )

    def test_reservation_verifier_toctou_cannot_reopen(self) -> None:
        claim = self.begin()

        def race(request):
            self.authorities.reservation_hook = None
            self.store.revoke_entry(
                run_id=claim["runId"],
                expected_revision=request["coordinatorRevision"],
                reason="reservation-race",
            )

        self.authorities.reservation_hook = race
        with self.assertRaises(CryptoFirstLiveCoordinatorError):
            self.store.seal_reservation(
                run_id=claim["runId"], owner_token=claim["ownerToken"],
                owner_epoch=claim["ownerEpoch"],
                reservation_evidence={"fresh": True},
            )
        self.assertEqual("CLEANUP_ONLY", self.store.status()["phase"])

    def test_approval_consumer_toctou_cannot_reopen(self) -> None:
        value = self.reserve()

        def race(request):
            self.authorities.approval_hook = None
            self.store.revoke_entry(
                run_id=value["runId"],
                expected_revision=request["coordinatorRevision"],
                reason="approval-race",
            )

        self.authorities.approval_hook = race
        with mock.patch.multiple(
            "live_trader.crypto_first_live_coordinator",
            CRYPTO_FIRST_LIVE_ACTIVATION_RELEASED=True,
            CRYPTO_FIRST_LIVE_WORM_ROLLBACK_RELEASED=True,
        ):
            with self.assertRaisesRegex(
                CryptoFirstLiveCoordinatorError, "activation-cas-changed"
            ):
                self.store.activate(
                    run_id=value["runId"], owner_token=value["ownerToken"],
                    owner_epoch=value["ownerEpoch"],
                    final_approval={"oneUse": True},
                )
        self.assertEqual("CLEANUP_ONLY", self.store.status()["phase"])

    def test_approval_id_and_consumption_id_are_locally_one_use(self) -> None:
        first = self.reserve()
        self.activate(first)
        cleanup = self.revoke(first)
        self.store.finalize(
            run_id=first["runId"], owner_token=first["ownerToken"],
            owner_epoch=first["ownerEpoch"],
            expected_revision=cleanup["revision"],
            terminal_evidence={"finalFlat": True},
        )
        second = self.reserve()
        with mock.patch.multiple(
            "live_trader.crypto_first_live_coordinator",
            CRYPTO_FIRST_LIVE_ACTIVATION_RELEASED=True,
            CRYPTO_FIRST_LIVE_WORM_ROLLBACK_RELEASED=True,
        ):
            with self.assertRaisesRegex(
                CryptoFirstLiveCoordinatorError, "already-consumed"
            ):
                self.store.activate(
                    run_id=second["runId"], owner_token=second["ownerToken"],
                    owner_epoch=second["ownerEpoch"],
                    final_approval={"oneUse": True},
                )
        self.assertEqual(
            "ACTIVATION_PREPARING", self.store.status()["phase"]
        )

    def test_terminal_verifier_toctou_cannot_finalize_stale_revision(self) -> None:
        value = self.reserve()
        self.activate(value)
        cleanup = self.revoke(value)

        def race(_request):
            self.authorities.terminal_hook = None
            self.store.heartbeat(
                run_id=value["runId"], owner_token=value["ownerToken"],
                owner_epoch=value["ownerEpoch"],
            )

        self.authorities.terminal_hook = race
        with self.assertRaisesRegex(
            CryptoFirstLiveCoordinatorError, "finalize-cas-changed"
        ):
            self.store.finalize(
                run_id=value["runId"], owner_token=value["ownerToken"],
                owner_epoch=value["ownerEpoch"],
                expected_revision=cleanup["revision"],
                terminal_evidence={"finalFlat": True},
            )
        self.assertEqual("CLEANUP_ONLY", self.store.status()["phase"])

    def test_extra_sqlite_object_is_rejected(self) -> None:
        conn = sqlite3.connect(self.path)
        conn.execute("CREATE TABLE injected_authority(value TEXT)")
        conn.commit(); conn.close()
        with self.assertRaisesRegex(
            CryptoFirstLiveCoordinatorError, "sqlite-objects-mismatch"
        ):
            self.store.status()

    def test_both_hash_chains_reject_tamper(self) -> None:
        self.reserve()
        conn = sqlite3.connect(self.anchor_path)
        conn.execute(
            "UPDATE crypto_first_live_high_water_events SET content_json='{}' "
            "WHERE anchor_revision=2"
        )
        conn.commit(); conn.close()
        with self.assertRaisesRegex(
            CryptoFirstLiveHighWaterError, "event-chain-invalid"
        ):
            self.anchor.status()


if __name__ == "__main__":
    unittest.main()
