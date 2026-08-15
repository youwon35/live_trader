from __future__ import annotations

import base64
import hashlib
import unittest

from Crypto.PublicKey import ECC
from Crypto.Signature import eddsa

from live_trader.binance_spot_functional_exclusivity import (
    BinanceSpotExclusivityError,
)
from live_trader.binance_spot_functional_supervised_exclusivity import (
    ASSURANCE_MODE,
)
from live_trader.binance_spot_supervised_authority_protocol import (
    PROCESS_AUDIT_SCHEMA_VERSION,
    SNAPSHOT_SCHEMA_VERSION,
    STREAM_AUDIT_SCHEMA_VERSION,
    PinnedBinanceSpotSupervisedAuthorityVerifier,
    authority_hash,
    canonical_authority_message,
)


NOW = 1_800_000_000.0
SESSION = "bnsft-independent-observer-session-0001"
PERMIT = "binance-independent-observer-permit-0001"
PERMIT_HASH = "1" * 64
CREDENTIAL = "2" * 64
PREFIX = "ftb-" + hashlib.sha256(SESSION.encode()).hexdigest()[:12] + "-"


def official() -> dict[str, object]:
    body = {
        "schemaVersion": "binance-spot-supervised-official-get-evidence/v1",
        "origin": "https://api.binance.com",
        "observedEpoch": NOW - 1,
        "apiRestrictions": {
            "enableReading": True,
            "enableSpotAndMarginTrading": True,
            "ipRestrict": True,
            "enableWithdrawals": True,
            "enableMargin": True,
            "enableFutures": True,
            "responseHash": "3" * 64,
        },
        "apiTradingStatus": {"locked": False, "responseHash": "4" * 64},
        "accountWideOpenOrders": {
            "scope": "ACCOUNT_WIDE_ALL_SYMBOLS",
            "openOrderCount": 0,
            "responseHash": "5" * 64,
        },
        "transport": {
            "physicalGetAttemptCount": 3,
            "retryCount": 0,
            "redirectCount": 0,
            "nonGetAttemptCount": 0,
            "mutationAttemptCount": 0,
        },
    }
    return {**body, "evidenceHash": authority_hash(body)}


def process() -> dict[str, object]:
    body = {
        "schemaVersion": PROCESS_AUDIT_SCHEMA_VERSION,
        "source": "WINDOWS_CIM_PROCESS_REGISTRY",
        "observedEpoch": NOW,
        "authorizedTraderProcessIdentityHash": "6" * 64,
        "authorizedFunctionalBotCount": 1,
        "otherRegisteredBotCount": 0,
        "observerProcessSeparate": True,
        "independentlyVerified": True,
    }
    return {**body, "auditHash": authority_hash(body)}


def stream(**updates: object) -> dict[str, object]:
    body = {
        "schemaVersion": STREAM_AUDIT_SCHEMA_VERSION,
        "transportKind": "SIGNED_WS_API_USER_DATA_STREAM",
        "listenKeyRequired": False,
        "subscriptionAuthenticated": True,
        "connected": True,
        "gapDetected": False,
        "crashDetected": False,
        "continuousCoverage": True,
        "sessionId": SESSION,
        "permitId": PERMIT,
        "permitHash": PERMIT_HASH,
        "ownerClientOrderPrefix": PREFIX,
        "subscribedEpoch": NOW - 1,
        "lastLivenessEpoch": NOW,
        "eventCount": 3,
        "orderEventCount": 2,
        "unownedOrderEventCount": 0,
        "eventChainHash": "7" * 64,
        "journalDatabaseIdentityHash": "8" * 64,
    }
    body.update(updates)
    return {**body, "auditHash": authority_hash(body)}


class AuthorityProtocolTest(unittest.TestCase):
    def setUp(self) -> None:
        self.key = ECC.generate(curve="Ed25519")
        self.verifier = PinnedBinanceSpotSupervisedAuthorityVerifier(
            public_key=self.key.public_key().export_key(format="PEM"),
            authority_id="binance-supervised-authority-0001",
            key_id="binance-supervised-key-0001",
            expected_credential_fingerprint=CREDENTIAL,
        )

    def snapshot(self, **updates: object) -> dict[str, object]:
        body = {
            "schemaVersion": SNAPSHOT_SCHEMA_VERSION,
            "assuranceMode": ASSURANCE_MODE,
            "authorityId": "binance-supervised-authority-0001",
            "keyId": "binance-supervised-key-0001",
            "authorityProcessIdentityHash": "9" * 64,
            "sessionId": SESSION,
            "permitId": PERMIT,
            "permitHash": PERMIT_HASH,
            "credentialFingerprint": CREDENTIAL,
            "ownerClientOrderPrefix": PREFIX,
            "coverageStartedEpoch": NOW - 1,
            "observedEpoch": NOW,
            "authoritySequence": 4,
            "previousSnapshotHash": "a" * 64,
            "officialBaseline": official(),
            "processAudit": process(),
            "userDataStreamAudit": stream(),
            "revoked": False,
            "cleanupOnlyRequired": False,
            "revokeReason": "",
            "otherApiKeyInventoryProven": False,
            "manualOrderCausalAuditIndependentlyVerified": True,
            "botRegistryIndependentlyVerified": True,
            "accountWideCausalClosureProven": False,
            "promotionEligible": False,
            "realE2EEligible": False,
            "productionPromotionAllowed": False,
        }
        body.update(updates)
        payload = {**body, "payloadHash": authority_hash(body)}
        signature = eddsa.new(self.key, "rfc8032").sign(
            canonical_authority_message(payload)
        )
        return {
            **payload,
            "signature": base64.urlsafe_b64encode(signature)
            .decode("ascii")
            .rstrip("="),
        }

    def verify(self, value: object, *, now: float = NOW) -> dict[str, object]:
        return self.verifier.verify_snapshot(
            value,
            session_id=SESSION,
            permit_id=PERMIT,
            permit_hash=PERMIT_HASH,
            credential_fingerprint=CREDENTIAL,
            owner_client_order_prefix=PREFIX,
            coverage_started_epoch=NOW,
            now_epoch=now,
        )

    def test_exact_snapshot_proves_manual_bot_audit_but_never_key_inventory(self) -> None:
        result = self.verify(self.snapshot())
        self.assertTrue(result["manualOrderCausalAuditIndependentlyVerified"])
        self.assertTrue(result["botRegistryIndependentlyVerified"])
        self.assertFalse(result["otherApiKeyInventoryProven"])
        self.assertFalse(result["accountWideCausalClosureProven"])
        self.assertFalse(result["realE2EEligible"])

    def test_revocation_gap_unowned_and_overclaim_are_rejected(self) -> None:
        for value in (
            self.snapshot(
                revoked=True,
                cleanupOnlyRequired=True,
                revokeReason="unowned order",
                manualOrderCausalAuditIndependentlyVerified=False,
            ),
            self.snapshot(userDataStreamAudit=stream(gapDetected=True)),
            self.snapshot(
                userDataStreamAudit=stream(unownedOrderEventCount=1)
            ),
            self.snapshot(otherApiKeyInventoryProven=True),
            self.snapshot(accountWideCausalClosureProven=True),
            self.snapshot(realE2EEligible=True),
        ):
            with self.subTest(value=value), self.assertRaises(
                BinanceSpotExclusivityError
            ):
                self.verify(value)

    def test_stale_or_tampered_signature_is_rejected(self) -> None:
        with self.assertRaises(BinanceSpotExclusivityError):
            self.verify(self.snapshot(), now=NOW + 6)
        tampered = self.snapshot()
        tampered["authoritySequence"] = 99
        with self.assertRaises(BinanceSpotExclusivityError):
            self.verify(tampered)

    def test_credential_pin_cannot_be_reselected_by_the_caller(self) -> None:
        with self.assertRaisesRegex(
            BinanceSpotExclusivityError, "credential pin changed"
        ):
            self.verifier.verify_snapshot(
                self.snapshot(),
                session_id=SESSION,
                permit_id=PERMIT,
                permit_hash=PERMIT_HASH,
                credential_fingerprint="f" * 64,
                owner_client_order_prefix=PREFIX,
                coverage_started_epoch=NOW,
                now_epoch=NOW,
            )

    def test_snapshot_sequence_replay_equivocation_and_adjacent_link_fail_closed(
        self,
    ) -> None:
        first = self.snapshot(authoritySequence=4)
        self.verify(first)
        # Exact repeated reads of the atomically published file are allowed.
        self.verify(first)

        equivocated = self.snapshot(
            authoritySequence=4,
            userDataStreamAudit=stream(eventCount=4),
        )
        with self.assertRaisesRegex(
            BinanceSpotExclusivityError, "sequence equivocated"
        ):
            self.verify(equivocated)

        bad_link = self.snapshot(
            authoritySequence=5,
            previousSnapshotHash="b" * 64,
        )
        with self.assertRaisesRegex(
            BinanceSpotExclusivityError, "adjacent hash link changed"
        ):
            self.verify(bad_link)

        next_snapshot = self.snapshot(
            authoritySequence=5,
            previousSnapshotHash=authority_hash(first),
        )
        self.verify(next_snapshot)
        with self.assertRaisesRegex(
            BinanceSpotExclusivityError, "continuity rolled back"
        ):
            self.verify(first)


if __name__ == "__main__":
    unittest.main()
