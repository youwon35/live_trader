from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from live_trader.crypto_first_live_supervised_release import (
    CryptoFirstLiveSupervisedReleaseError,
    DurableSupervisedNonPromotionApprovalStore,
    build_supervised_non_promotion_contract,
    supervised_non_promotion_release_status,
    validate_supervised_non_promotion_contract,
)


NOW = 2_300_000_000.0


def digest(value) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def contract(lane: str = "UPBIT") -> dict:
    approval_body = {
        "schemaVersion": (
            "crypto-first-live-supervised-user-approval-receipt/v1"
        ),
        "approvalId": "supervised-approval-0001",
        "approvalBindingHash": digest({"binding": lane}),
        "consumptionId": "supervised-consumption-0001",
        "exactUserApproval": True,
        "consumed": True,
        "oneUse": True,
        "durable": True,
        "restartVerifiable": True,
        "approvedEpoch": NOW - 1,
    }
    body = {
        "schemaVersion": "crypto-first-live-supervised-non-promotion/v1",
        "mode": "SUPERVISED_NON_PROMOTION",
        "lane": lane,
        "sessionId": "supervised-session-0001",
        "permitId": "supervised-permit-0001",
        "permitHash": digest({"permit": lane}),
        "operatorApproval": {
            **approval_body,
            "receiptHash": digest(approval_body),
        },
        "riskCaps": (
            {
                "currency": "KRW",
                "maxOrderNotional": "10000",
                "maxLoss": "1000",
                "activeSeconds": 7200,
            }
            if lane == "UPBIT"
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
            "authorityId": "windows-event-authority-0001",
            "checkpointId": "windows-event-checkpoint-0001",
            "receiptHash": digest({"event": 1}),
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
    return {**body, "contractHash": digest(body)}


class SupervisedNonPromotionContractTests(unittest.TestCase):
    def test_exact_low_caps_and_residual_risk_are_accepted(self) -> None:
        for lane in ("UPBIT", "BINANCE_SPOT"):
            with self.subTest(lane=lane):
                value = validate_supervised_non_promotion_contract(
                    contract(lane), clock=lambda: NOW
                )
                self.assertEqual(lane, value["lane"])
                self.assertFalse(
                    value["executionConstraints"]["promotionEligible"]
                )

    def test_cap_raise_and_false_user_acceptance_fail_closed(self) -> None:
        for mutation in ("cap", "approval"):
            value = contract()
            if mutation == "cap":
                value["riskCaps"]["maxOrderNotional"] = "10001"
            else:
                value["residualRisk"]["acceptedByUser"] = False
            body = {
                key: item for key, item in value.items() if key != "contractHash"
            }
            value["contractHash"] = digest(body)
            with self.subTest(mutation=mutation), self.assertRaises(
                CryptoFirstLiveSupervisedReleaseError
            ):
                validate_supervised_non_promotion_contract(
                    value, clock=lambda: NOW
                )

    def test_contract_does_not_release_network_or_promotion(self) -> None:
        status = supervised_non_promotion_release_status()
        self.assertFalse(status["released"])
        self.assertFalse(status["oneUseNetworkCapabilityReleased"])
        self.assertFalse(status["formalExternalWorm"])
        self.assertFalse(status["promotionEligible"])
        self.assertFalse(status["realE2EEligible"])
        self.assertFalse(status["productionPromotionAllowed"])

    def test_contract_rejects_extra_http_style_material(self) -> None:
        value = contract()
        value["typedPhrase"] = "must-never-cross-this-boundary"
        with self.assertRaisesRegex(
            CryptoFirstLiveSupervisedReleaseError,
            "fields-not-exact",
        ):
            validate_supervised_non_promotion_contract(
                value, clock=lambda: NOW
            )

    def test_consumed_approval_receipt_expires_before_activation(self) -> None:
        value = contract()
        value["operatorApproval"]["approvedEpoch"] = NOW - 61
        receipt_body = {
            key: item
            for key, item in value["operatorApproval"].items()
            if key != "receiptHash"
        }
        value["operatorApproval"]["receiptHash"] = digest(receipt_body)
        body = {
            key: item for key, item in value.items() if key != "contractHash"
        }
        value["contractHash"] = digest(body)
        with self.assertRaisesRegex(
            CryptoFirstLiveSupervisedReleaseError,
            "operator-approval-invalid",
        ):
            validate_supervised_non_promotion_contract(
                value, clock=lambda: NOW
            )

    def test_durable_approval_is_exactly_once_and_restart_verifiable(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "supervised-approval.sqlite3"
            store = DurableSupervisedNonPromotionApprovalStore(
                path, clock=lambda: NOW
            )
            base = contract("BINANCE_SPOT")
            issue = {
                "schemaVersion": (
                    "crypto-first-live-supervised-approval-issue/v1"
                ),
                "mode": base["mode"],
                "lane": base["lane"],
                "sessionId": base["sessionId"],
                "permitId": base["permitId"],
                "permitHash": base["permitHash"],
                "riskCaps": base["riskCaps"],
                "executionConstraints": base["executionConstraints"],
                "auditAnchor": base["auditAnchor"],
                "residualRisk": base["residualRisk"],
            }
            candidate = store.issue(issue)
            consume = {
                "schemaVersion": (
                    "crypto-first-live-supervised-approval-consume/v1"
                ),
                "approvalId": candidate["approvalId"],
                "approvalBindingHash": candidate["approvalBindingHash"],
                "typedPhrase": candidate["typedPhrase"],
                "exactUserApproval": True,
            }
            receipt = store.consume(consume)
            with self.assertRaisesRegex(
                CryptoFirstLiveSupervisedReleaseError, "not-consumable"
            ):
                store.consume(consume)
            restarted = DurableSupervisedNonPromotionApprovalStore(
                path, clock=lambda: NOW
            )
            self.assertEqual("CONSUMED", restarted.status()["state"])
            self.assertEqual(
                "CONSUMED", restarted.status()["durableState"]
            )
            self.assertEqual(
                receipt["receiptHash"], restarted.status()["receiptHash"]
            )
            connection = sqlite3.connect(path)
            try:
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        """
                        UPDATE supervised_non_promotion_approvals
                        SET state='ISSUED', consumption_id='',
                            approved_epoch=0, receipt_json='', receipt_hash=''
                        WHERE approval_id=?
                        """,
                        (candidate["approvalId"],),
                    )
            finally:
                connection.close()
            built = build_supervised_non_promotion_contract(
                issue, receipt, clock=lambda: NOW
            )
            self.assertEqual("BINANCE_SPOT", built["lane"])
            self.assertFalse(
                built["executionConstraints"]["promotionEligible"]
            )


if __name__ == "__main__":
    unittest.main()
