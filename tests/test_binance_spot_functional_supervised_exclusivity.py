from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import live_trader.binance_spot_functional_supervised_exclusivity as supervised

from live_trader import crypto_first_live_supervised_release as release
from live_trader.binance_spot_functional_exclusivity import (
    BinanceSpotExclusivityError,
)
from live_trader.binance_spot_functional_supervised_exclusivity import (
    ASSURANCE_MODE,
    BinanceSpotSupervisedOfficialGetProvider,
    BinanceSpotSupervisedExclusivityGuard,
    DurableBinanceSpotSupervisedExclusivityStore,
    LOCAL_AUDIT_SCHEMA_VERSION,
    OFFICIAL_EVIDENCE_SCHEMA_VERSION,
    USER_ATTESTATION_SCHEMA_VERSION,
)


NOW = 1_800_000_000.0
SESSION = "bnsft-supervised-session-0001"
PERMIT = "binance-supervised-permit-0001"
PERMIT_HASH = "1" * 64
CREDENTIAL = "2" * 64
BOUNDARY_HASH = "3" * 64
ANCHOR_HASH = "4" * 64


def digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )


def contract(now: float = NOW) -> dict[str, object]:
    issue = {
        "schemaVersion": "crypto-first-live-supervised-approval-issue/v1",
        "mode": ASSURANCE_MODE,
        "lane": "BINANCE_SPOT",
        "sessionId": SESSION,
        "permitId": PERMIT,
        "permitHash": PERMIT_HASH,
        "riskCaps": {
            "currency": "USDT",
            "maxOrderNotional": "10",
            "maxLoss": "1",
            "activeSeconds": 7200,
        },
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
            "authorityId": "windows-event-authority-0001",
            "checkpointId": "windows-event-checkpoint-0001",
            "receiptHash": ANCHOR_HASH,
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
    approval_body = {
        "schemaVersion": release.APPROVAL_RECEIPT_SCHEMA_VERSION,
        "approvalId": "supervised-approval-test-0001",
        "approvalBindingHash": digest(issue),
        "consumptionId": "supervised-consumption-test-0001",
        "exactUserApproval": True,
        "consumed": True,
        "oneUse": True,
        "durable": True,
        "restartVerifiable": True,
        "approvedEpoch": now,
    }
    approval = {**approval_body, "receiptHash": digest(approval_body)}
    return release.build_supervised_non_promotion_contract(
        issue, approval, clock=lambda: now
    )


def official(now: float = NOW, *, open_orders: int = 0) -> dict[str, object]:
    body = {
        "schemaVersion": OFFICIAL_EVIDENCE_SCHEMA_VERSION,
        "origin": "https://api.binance.com",
        "observedEpoch": now,
        "apiRestrictions": {
            "enableReading": True,
            "enableSpotAndMarginTrading": True,
            "ipRestrict": True,
            "enableWithdrawals": True,
            "enableMargin": True,
            "enableFutures": True,
            "responseHash": "5" * 64,
        },
        "apiTradingStatus": {"locked": False, "responseHash": "6" * 64},
        "accountWideOpenOrders": {
            "scope": "ACCOUNT_WIDE_ALL_SYMBOLS",
            "openOrderCount": open_orders,
            "responseHash": "7" * 64,
        },
        "transport": {
            "physicalGetAttemptCount": 3,
            "retryCount": 0,
            "redirectCount": 0,
            "nonGetAttemptCount": 0,
            "mutationAttemptCount": 0,
        },
    }
    return {**body, "evidenceHash": digest(body)}


def local_audit(now: float = NOW) -> dict[str, object]:
    body = {
        "schemaVersion": LOCAL_AUDIT_SCHEMA_VERSION,
        "source": "LOCAL_OS_LEASE_AND_SERVER_BOT_REGISTRY",
        "observedEpoch": now,
        "applicationInstanceLeaseHeld": True,
        "accountProcessLeaseHeld": True,
        "authorizedLiveTraderProcessCount": 1,
        "otherLiveTraderProcessCount": 0,
        "authorizedFunctionalBotCount": 1,
        "otherRegisteredBotCount": 0,
    }
    return {**body, "auditHash": digest(body)}


def attestation(now: float = NOW) -> dict[str, object]:
    body = {
        "schemaVersion": USER_ATTESTATION_SCHEMA_VERSION,
        "source": "AUTHENTICATED_SERVER_USER_CONFIRMATION",
        "sessionId": SESSION,
        "permitId": PERMIT,
        "permitHash": PERMIT_HASH,
        "authenticatedUser": True,
        "exactUserApproval": True,
        "noManualTrading": True,
        "noOtherBots": True,
        "otherApiKeyInventoryUnknown": True,
        "exactSessionAndCapsAccepted": True,
        "attestedEpoch": now,
        "auditAnchorReceiptHash": ANCHOR_HASH,
    }
    return {**body, "attestationHash": digest(body)}


def stream(now: float = NOW, *, bound: bool = False) -> dict[str, object]:
    return {
        "connected": True,
        "authenticated": True,
        "sequenceComplete": True,
        "gapDetected": False,
        "subscribedAt": iso(NOW - 2),
        "observedAt": iso(now),
        "externalActivityAbsent": True,
        "events": [],
        "sessionId": SESSION if bound else "",
        "permitId": PERMIT if bound else "",
        "permitHash": PERMIT_HASH if bound else "",
        "writerHeartbeatFresh": True,
        "durableJournal": True,
        "durableJournalEventCount": 0,
        "durableJournalSealHash": "8" * 64,
        "terminalMarkerAcknowledged": False,
        "terminalMarkerId": "",
        "terminalMarkerServerEpoch": 0.0,
        "terminalMarkerEpoch": 0.0,
    }


class SupervisedExclusivityTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.now = NOW
        self.bound = False
        self.official_value = official()
        self.guard = BinanceSpotSupervisedExclusivityGuard(
            store=DurableBinanceSpotSupervisedExclusivityStore(
                Path(self.temporary.name) / "supervised.sqlite3"
            ),
            contract_reader=lambda **_request: contract(),
            official_get_reader=lambda **_request: self.official_value,
            local_process_bot_audit_reader=(
                lambda **_request: local_audit(self.now)
            ),
            user_attestation_reader=lambda **_request: attestation(),
            stream_reader=lambda: stream(self.now, bound=self.bound),
            allow_inprocess_test_evidence=True,
            clock=lambda: self.now,
        )
        self.release = patch.multiple(
            release,
            SUPERVISED_NON_PROMOTION_RELEASED=True,
            SUPERVISED_NON_PROMOTION_NETWORK_CAPABILITY_RELEASED=True,
        )

    def verify(self, phase: str, boundary: str) -> dict[str, object]:
        return self.guard.verify_and_record(
            phase=phase,
            session_id=SESSION,
            permit_id=PERMIT,
            permit_hash=PERMIT_HASH,
            credential_fingerprint=CREDENTIAL,
            boundary_id=boundary,
            boundary_hash=BOUNDARY_HASH,
            coverage_started_epoch=NOW - 1,
        )

    def test_official_get_provider_release_false_calls_no_sender(self) -> None:
        sends: list[str] = []
        provider = BinanceSpotSupervisedOfficialGetProvider(
            sender=lambda _request: sends.append("sender")
            or {"ok": True, "json": {}},
            clock=lambda: NOW,
        )
        with self.assertRaisesRegex(
            BinanceSpotExclusivityError,
            "capability is closed",
        ):
            provider()
        self.assertEqual([], sends)

    def test_protected_capability_allows_only_exact_three_gets(self) -> None:
        endpoints: list[str] = []
        payloads = [
            {
                "enableReading": True,
                "enableSpotAndMarginTrading": True,
                "ipRestrict": True,
                "enableWithdrawals": True,
                "enableMargin": True,
                "enableFutures": True,
            },
            {"data": {"isLocked": False}},
            [],
        ]

        def sender(request):
            endpoints.append(request.endpoint)
            return {"ok": True, "json": payloads.pop(0)}

        with self.release, patch.object(
            supervised,
            "BINANCE_SPOT_SUPERVISED_GET_NETWORK_RELEASED",
            True,
        ):
            capability = supervised._protected_binance_spot_supervised_get_network_capability()
            with self.assertRaises(TypeError):
                json.dumps(capability)
            provider = BinanceSpotSupervisedOfficialGetProvider(
                sender=sender,
                clock=lambda: NOW,
                network_capability=capability,
            )
            with patch.dict(
                os.environ,
                {
                    "BINANCE_API_KEY": "api-key",
                    "BINANCE_API_SECRET": "secret",
                    "BINANCE_BASE_URL": "https://api.binance.com",
                },
                clear=False,
            ):
                evidence = provider()
        self.assertEqual(
            [
                supervised.API_RESTRICTIONS_ENDPOINT,
                supervised.API_TRADING_STATUS_ENDPOINT,
                supervised.BINANCE_SPOT_OPEN_ORDERS_ENDPOINT,
            ],
            endpoints,
        )
        self.assertEqual(3, evidence["transport"]["physicalGetAttemptCount"])

    def test_release_false_blocks_before_any_evidence_callback(self) -> None:
        calls: list[str] = []
        self.guard.official_get_reader = (
            lambda **_request: calls.append("network") or official()
        )
        with self.assertRaises(BinanceSpotExclusivityError):
            self.verify("BASELINE", SESSION + ":baseline")
        self.assertEqual([], calls)
        self.assertFalse(self.guard.status()["realE2EEligible"])

    def test_exact_supervised_baseline_is_durable_but_never_independent(self) -> None:
        with self.release:
            result = self.verify("BASELINE", SESSION + ":baseline")
        self.assertTrue(result["verified"])
        self.assertTrue(result["supervisedControlsVerified"])
        self.assertFalse(result["exclusiveAccountConfirmed"])
        self.assertFalse(result["accountWideCausalClosureProven"])
        self.assertFalse(result["promotionEligible"])
        self.assertFalse(result["realE2EEligible"])
        rows = self.guard.session_records(SESSION)
        self.assertEqual(1, len(rows))
        self.assertEqual(ASSURANCE_MODE, rows[0]["proof"]["assuranceMode"])

    def test_account_wide_open_order_blocks_without_durable_record(self) -> None:
        self.official_value = official(open_orders=1)
        with self.release, self.assertRaises(BinanceSpotExclusivityError):
            self.verify("BASELINE", SESSION + ":baseline")
        self.assertEqual([], self.guard.session_records(SESSION))

    def test_stream_must_be_bound_after_baseline(self) -> None:
        with self.release, self.assertRaises(BinanceSpotExclusivityError):
            self.verify("ACTIVATION", SESSION + ":activation")
        self.bound = True
        with self.release:
            result = self.verify("ACTIVATION", SESSION + ":activation")
        self.assertTrue(result["supervisedControlsVerified"])

    def test_two_hour_terminal_reuses_contract_but_refreshes_live_controls(self) -> None:
        with self.release:
            self.verify("BASELINE", SESSION + ":baseline")
        self.now = NOW + 7200
        self.bound = True
        self.official_value = official(self.now)
        with self.release:
            terminal = self.verify("TERMINAL", SESSION + ":terminal")
        self.assertTrue(terminal["restartVerifiable"])
        self.assertFalse(terminal["accountWideCausalClosureProven"])
        self.assertEqual(2, len(self.guard.session_records(SESSION)))

    def test_require_causal_closure_can_never_be_claimed(self) -> None:
        with self.release, self.assertRaises(BinanceSpotExclusivityError):
            self.guard.verify_and_record(
                phase="TERMINAL",
                session_id=SESSION,
                permit_id=PERMIT,
                permit_hash=PERMIT_HASH,
                credential_fingerprint=CREDENTIAL,
                boundary_id=SESSION + ":terminal",
                boundary_hash=BOUNDARY_HASH,
                coverage_started_epoch=NOW - 1,
                require_causal_closure=True,
            )

    def test_independent_health_is_fresh_and_never_overclaims(self) -> None:
        calls: list[dict[str, object]] = []

        class Verifier:
            @staticmethod
            def verify_snapshot(
                value: object, **request: object
            ) -> dict[str, object]:
                calls.append(dict(request))
                if value != {"signed": True}:
                    raise BinanceSpotExclusivityError("snapshot changed")
                return {
                    "authorityId": "binance-supervised-authority-0001",
                    "authoritySequence": 9,
                    "observedEpoch": NOW,
                    "payloadHash": "9" * 64,
                }

        guard = BinanceSpotSupervisedExclusivityGuard(
            store=DurableBinanceSpotSupervisedExclusivityStore(
                Path(self.temporary.name) / "independent-health.sqlite3"
            ),
            contract_reader=lambda **_: contract(),
            official_get_reader=None,
            local_process_bot_audit_reader=None,
            user_attestation_reader=lambda **_: attestation(),
            stream_reader=None,
            independent_authority_reader=lambda **_: {"signed": True},
            independent_authority_verifier=Verifier(),
            clock=lambda: NOW,
        )
        with self.release:
            result = guard.assert_continuous_health(
                session_id=SESSION,
                permit_id=PERMIT,
                permit_hash=PERMIT_HASH,
                credential_fingerprint=CREDENTIAL,
                coverage_started_epoch=NOW - 1,
                purpose="MUTATION_FINAL_PRE_MARKER",
            )
        self.assertTrue(result["healthy"])
        self.assertEqual(PERMIT_HASH, result["permitHash"])
        self.assertEqual(CREDENTIAL, result["credentialFingerprint"])
        self.assertFalse(result["otherApiKeyInventoryProven"])
        self.assertFalse(result["accountWideCausalClosureProven"])
        self.assertFalse(result["promotionEligible"])
        self.assertFalse(result["realE2EEligible"])
        self.assertEqual(1, len(calls))
        self.assertEqual(SESSION, calls[0]["session_id"])

    def test_independent_health_release_false_reads_no_snapshot(self) -> None:
        calls: list[str] = []

        class Verifier:
            @staticmethod
            def verify_snapshot(
                _value: object, **_request: object
            ) -> dict[str, object]:
                calls.append("verify")
                return {}

        guard = BinanceSpotSupervisedExclusivityGuard(
            store=DurableBinanceSpotSupervisedExclusivityStore(
                Path(self.temporary.name) / "release-false-health.sqlite3"
            ),
            contract_reader=lambda **_: contract(),
            official_get_reader=None,
            local_process_bot_audit_reader=None,
            user_attestation_reader=lambda **_: attestation(),
            stream_reader=None,
            independent_authority_reader=(
                lambda **_: calls.append("read") or {"signed": True}
            ),
            independent_authority_verifier=Verifier(),
            clock=lambda: NOW,
        )
        with self.assertRaises(BinanceSpotExclusivityError):
            guard.assert_continuous_health(
                session_id=SESSION,
                permit_id=PERMIT,
                permit_hash=PERMIT_HASH,
                credential_fingerprint=CREDENTIAL,
                coverage_started_epoch=NOW - 1,
            )
        self.assertEqual([], calls)


if __name__ == "__main__":
    unittest.main()
