from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from live_trader.binance_spot_functional_approval import (
    BinanceSpotPermitApprovalError,
    DurableBinanceSpotApprovedPermitStore,
)
from live_trader.binance_spot_functional_backend import (
    issue_binance_spot_functional_permit,
)
from live_trader.binance_spot_continuous_functional import ExactBinding, ExactPermit
from tests.test_binance_spot_continuous_functional import Clock, binding, permit


class BinanceSpotFunctionalApprovalTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.clock = Clock()
        self.path = Path(self.temporary.name) / "approved-binance.sqlite3"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def attestation(self, payload: dict[str, object], **updates: object) -> dict[str, object]:
        result: dict[str, object] = {
            "approvalId": "operator-approval-binance-0001",
            "operatorId": "operator-test",
            "operatorAuthenticated": True,
            "operatorApproved": True,
            "permitId": payload["permitId"],
            "permitHash": payload["permitHash"],
            "accountFingerprint": binding()["accountFingerprint"],
            "executionRoute": "BINANCE_SPOT_CONTINUOUS",
            "symbol": "BTCUSDT",
            "approvedAt": self.clock.iso(),
            "nonce": "operator-nonce-binance-functional-000001",
            "activationResealAuthorized": True,
            "activeDurationSeconds": 7200,
            "exclusiveAccountConfirmed": True,
            "noManualTradingConfirmed": True,
            "noBotsConfirmed": True,
            "noOtherApiKeysConfirmed": True,
            "firstLiveBootstrapAuthorized": True,
            "firstLiveBootstrapRequired": False,
            "firstLiveBootstrapId": "",
            "firstLiveBootstrapHash": "",
            "firstLiveSessionNonceHash": "",
            "firstLiveCodeHash": "",
        }
        result.update(updates)
        return result

    def test_caller_self_hashed_permit_is_not_server_approval(self) -> None:
        payload = permit(self.clock)
        store = DurableBinanceSpotApprovedPermitStore(
            self.path, clock=self.clock
        )
        with self.assertRaisesRegex(
            BinanceSpotPermitApprovalError, "operator approval"
        ):
            store.approve(payload, self.attestation(payload))

    def test_authenticated_approval_is_single_use_and_session_bound(self) -> None:
        payload = permit(self.clock)
        store = DurableBinanceSpotApprovedPermitStore(
            self.path,
            approval_verifier=lambda attestation: (
                attestation.get("nonce")
                == "operator-nonce-binance-functional-000001"
            ),
            clock=self.clock,
        )
        approved = store.approve(payload, self.attestation(payload))
        self.assertEqual("APPROVED", approved["state"])
        resolved, claim_token = store.claim(
            permit_id=str(payload["permitId"]),
            permit_hash=str(payload["permitHash"]),
            owner_id="managed-owner-a",
        )
        self.assertEqual(payload, resolved)
        with self.assertRaisesRegex(
            BinanceSpotPermitApprovalError, "absent/consumed"
        ):
            store.claim(
                permit_id=str(payload["permitId"]),
                permit_hash=str(payload["permitHash"]),
                owner_id="managed-owner-b",
            )
        store.bind_session(
            permit_id=str(payload["permitId"]),
            claim_token=claim_token,
            session_id="bnsft-approved-session-000000000001",
        )
        active = store.resolve_active(
            session_id="bnsft-approved-session-000000000001"
        )
        self.assertEqual(payload, active)
        store.consume(session_id="bnsft-approved-session-000000000001")
        store.consume(session_id="bnsft-approved-session-000000000001")
        with self.assertRaisesRegex(
            BinanceSpotPermitApprovalError, "unavailable"
        ):
            store.resolve_active(
                session_id="bnsft-approved-session-000000000001"
            )

    def test_claim_atomically_reseals_exact_7200_seconds_from_activation(self) -> None:
        payload = permit(self.clock)
        store = DurableBinanceSpotApprovedPermitStore(
            self.path,
            approval_verifier=lambda _attestation: True,
            clock=self.clock,
        )
        store.approve(payload, self.attestation(payload))
        self.clock.value += 299
        activated, claim_token = store.claim(
            permit_id=str(payload["permitId"]),
            permit_hash=str(payload["permitHash"]),
            owner_id="managed-owner-activation-reseal",
            activation_permit_issuer=lambda exact_binding, now: (
                issue_binance_spot_functional_permit(
                    binding=(
                        exact_binding
                        if isinstance(exact_binding, ExactBinding)
                        else ExactBinding.parse(exact_binding)
                    ),
                    now_epoch=now,
                )
            ),
        )
        parsed = ExactPermit.parse(activated, now_epoch=self.clock())
        self.assertAlmostEqual(self.clock(), parsed.issued_epoch, places=3)
        self.assertEqual(7200, parsed.expires_epoch - parsed.issued_epoch)
        self.assertEqual(
            10800, parsed.cleanup_deadline_epoch - parsed.issued_epoch
        )
        self.assertNotEqual(payload["permitId"], activated["permitId"])
        with self.assertRaises(BinanceSpotPermitApprovalError):
            store.status(str(payload["permitId"]))
        store.bind_session(
            permit_id=parsed.permit_id,
            claim_token=claim_token,
            session_id="bnsft-activation-reseal-session-0001",
        )
        self.assertEqual(
            activated,
            store.resolve_active(
                session_id="bnsft-activation-reseal-session-0001"
            ),
        )

    def test_startup_auditor_consumes_lost_raw_claim_token_without_reuse(self) -> None:
        payload = permit(self.clock)
        store = DurableBinanceSpotApprovedPermitStore(
            self.path,
            approval_verifier=lambda _attestation: True,
            clock=self.clock,
        )
        store.approve(payload, self.attestation(payload))
        _resolved, lost_token = store.claim(
            permit_id=str(payload["permitId"]),
            permit_hash=str(payload["permitHash"]),
            owner_id="crashed-owner",
        )
        failed = store.startup_fail_lost_claim(
            permit_id=str(payload["permitId"]),
            permit_hash=str(payload["permitHash"]),
            detail="startup owner disappeared before core session",
        )
        self.assertEqual("FAILED", failed["state"])
        with self.assertRaises(BinanceSpotPermitApprovalError):
            store.bind_session(
                permit_id=str(payload["permitId"]),
                claim_token=lost_token,
                session_id="bnsft-replay-forbidden-0001",
            )

    def test_claim_only_hard_crash_is_found_without_raw_token_after_lease(self) -> None:
        payload = permit(self.clock)
        store = DurableBinanceSpotApprovedPermitStore(
            self.path,
            approval_verifier=lambda _attestation: True,
            clock=self.clock,
        )
        store.approve(payload, self.attestation(payload))
        _resolved, lost_token = store.claim(
            permit_id=str(payload["permitId"]),
            permit_hash=str(payload["permitHash"]),
            owner_id="crashed-before-control-arm",
        )
        early = store.audit_orphaned_claims()
        self.assertEqual([payload["permitId"]], early["pendingPermitIds"])
        self.clock.value += 61
        audited = store.audit_orphaned_claims()
        self.assertEqual([payload["permitId"]], audited["failedPermitIds"])
        self.assertFalse(audited["manualReviewRequired"])
        self.assertEqual("FAILED", store.status(str(payload["permitId"]))["state"])
        with self.assertRaises(BinanceSpotPermitApprovalError):
            store.bind_session(
                permit_id=str(payload["permitId"]),
                claim_token=lost_token,
                session_id="bnsft-claim-only-replay-forbidden",
            )

    def test_approved_before_claim_hard_crash_is_failed_after_owner_window(self) -> None:
        payload = permit(self.clock)
        store = DurableBinanceSpotApprovedPermitStore(
            self.path,
            approval_verifier=lambda _attestation: True,
            clock=self.clock,
        )
        store.approve(payload, self.attestation(payload))
        early = store.audit_orphaned_claims()
        self.assertEqual([payload["permitId"]], early["pendingPermitIds"])
        self.clock.value += 61
        audited = store.audit_orphaned_claims()
        self.assertEqual([payload["permitId"]], audited["failedPermitIds"])
        self.assertEqual(
            "FAILED", store.status(str(payload["permitId"]))["state"]
        )
        with self.assertRaisesRegex(
            BinanceSpotPermitApprovalError, "absent/consumed"
        ):
            store.claim(
                permit_id=str(payload["permitId"]),
                permit_hash=str(payload["permitHash"]),
                owner_id="restart-owner-must-use-new-approval",
            )


if __name__ == "__main__":
    unittest.main()
