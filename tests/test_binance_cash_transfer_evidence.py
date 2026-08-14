from __future__ import annotations

import copy
import hashlib
from datetime import datetime, timedelta, timezone
import unittest
from unittest import mock

from live_trader.binance_cash_transfer_evidence import (
    BINANCE_CASH_TRANSFER_EVIDENCE_RELEASED,
    BinanceCashTransferEvidenceVerifier,
    empty_consumption_high_water,
)


ACCOUNT = hashlib.sha256(b"configured-binance-account").hexdigest()
API_KEY = hashlib.sha256(b"configured-binance-api-key").hexdigest()
NOW = datetime(2026, 8, 15, 0, 0, 0, tzinfo=timezone.utc)
TRANSFER_TIMESTAMP = int(
    datetime(2026, 7, 31, 0, 0, 0, tzinfo=timezone.utc).timestamp()
    * 1000
)
TRAN_ID = 11415955596


def signed_get_envelope() -> dict[str, object]:
    observed = NOW.isoformat()
    request_timestamp = int(NOW.timestamp() * 1000)
    return {
        "schemaVersion": "binance-universal-transfer-signed-get/v1",
        "accountFingerprint": ACCOUNT,
        "apiKeyFingerprint": API_KEY,
        "method": "GET",
        "path": "/sapi/v1/asset/transfer",
        "securityType": "USER_DATA",
        "signed": True,
        "transferType": "MAIN_UMFUTURE",
        "queryStartTime": int(
            datetime(2026, 7, 1, tzinfo=timezone.utc).timestamp() * 1000
        ),
        "queryEndTime": int(
            datetime(2026, 8, 14, tzinfo=timezone.utc).timestamp() * 1000
        ),
        "pageSize": 100,
        "pages": [
            {
                "current": 1,
                "httpStatus": 200,
                "total": 1,
                "rows": [
                    {
                        "asset": "USDT",
                        "amount": "10.00000000",
                        "type": "MAIN_UMFUTURE",
                        "status": "CONFIRMED",
                        "tranId": TRAN_ID,
                        "timestamp": TRANSFER_TIMESTAMP,
                    }
                ],
                "requestTimestamp": request_timestamp,
                "receivedAt": observed,
                "requestHash": hashlib.sha256(b"signed-request").hexdigest(),
                "responseHash": hashlib.sha256(b"signed-response").hexdigest(),
            }
        ],
        "allPagesComplete": True,
        "requestCount": 1,
        "retryCount": 0,
        "redirectCount": 0,
        "mutationCount": 0,
        "observedAt": observed,
        "detachedCaptureHash": hashlib.sha256(
            b"detached-signed-get-capture"
        ).hexdigest(),
    }


def idle_barrier() -> dict[str, object]:
    return {
        "schemaVersion": "binance-cash-transfer-idle-barrier/v1",
        "barrierId": "global-idle-barrier-0001",
        "accountFingerprint": ACCOUNT,
        "apiKeyFingerprint": API_KEY,
        "coordinatorPhase": "IDLE",
        "coordinatorRevision": 7,
        "globalLeaseState": "IDLE",
        "activeOwnerCount": 0,
        "mutationInFlightCount": 0,
        "spotOpenOrderCount": 0,
        "futuresOpenOrderCount": 0,
        "futuresPositionCount": 0,
        "spotCash": "13.80494270",
        "futuresCash": "10.00000000",
        "observedAt": NOW.isoformat(),
        "detachedEvidenceHash": hashlib.sha256(
            b"detached-global-idle-barrier"
        ).hexdigest(),
    }


class BinanceCashTransferEvidenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.signed_calls: list[object] = []
        self.idle_calls: list[object] = []
        self.verifier = BinanceCashTransferEvidenceVerifier(
            configured_account_fingerprint=ACCOUNT,
            configured_api_key_fingerprint=API_KEY,
            signed_get_verifier=lambda value: (
                self.signed_calls.append(value) is None
                and value.get("detachedCaptureHash")
                == signed_get_envelope()["detachedCaptureHash"]
            ),
            idle_barrier_verifier=lambda value: (
                self.idle_calls.append(value) is None
                and value.get("detachedEvidenceHash")
                == idle_barrier()["detachedEvidenceHash"]
            ),
            clock=lambda: NOW,
        )
        self.release = mock.patch(
            "live_trader.binance_cash_transfer_evidence."
            "BINANCE_CASH_TRANSFER_EVIDENCE_RELEASED",
            True,
        )
        self.release.start()

    def tearDown(self) -> None:
        self.release.stop()

    def certify(
        self,
        *,
        envelope: dict[str, object] | None = None,
        barrier: dict[str, object] | None = None,
        prior: dict[str, object] | None = None,
    ) -> dict[str, object]:
        return self.verifier.certify(
            expected_tran_id=TRAN_ID,
            signed_get_envelope=envelope or signed_get_envelope(),
            idle_barrier_evidence=barrier or idle_barrier(),
            prior_consumed_high_water=prior
            or empty_consumption_high_water(
                account_fingerprint=ACCOUNT,
                api_key_fingerprint=API_KEY,
            ),
        )

    def test_official_get_and_idle_barrier_produce_durable_truth(self) -> None:
        result = self.certify()
        truth = result["truthEvidence"]

        self.assertEqual("MAIN_UMFUTURE", truth["officialTransfer"]["type"])
        self.assertEqual("CONFIRMED", truth["officialTransfer"]["status"])
        self.assertEqual("SUCCESS", truth["officialTransfer"]["result"])
        self.assertEqual("10", truth["officialTransfer"]["amount"])
        self.assertEqual(ACCOUNT, truth["accountFingerprint"])
        self.assertEqual(API_KEY, truth["apiKeyFingerprint"])
        self.assertEqual("IDLE", truth["coordinatorPhase"])
        self.assertEqual(1, len(self.signed_calls))
        self.assertEqual(1, len(self.idle_calls))

    def test_compile_latch_is_false_and_fail_closed(self) -> None:
        self.assertFalse(BINANCE_CASH_TRANSFER_EVIDENCE_RELEASED)
        with mock.patch(
            "live_trader.binance_cash_transfer_evidence."
            "BINANCE_CASH_TRANSFER_EVIDENCE_RELEASED",
            False,
        ):
            with self.assertRaisesRegex(ValueError, "evidence-not-released"):
                self.certify()

    def test_wrong_configured_account_or_api_key_is_rejected(self) -> None:
        for field, replacement, message in (
            ("accountFingerprint", "a" * 64, "configured-account-mismatch"),
            ("apiKeyFingerprint", "b" * 64, "configured-api-key-mismatch"),
        ):
            with self.subTest(field=field):
                envelope = signed_get_envelope()
                envelope[field] = replacement
                with self.assertRaisesRegex(ValueError, message):
                    self.certify(envelope=envelope)

    def test_only_signed_official_get_route_is_accepted(self) -> None:
        for field, replacement in (
            ("method", "POST"),
            ("path", "/sapi/v1/asset/transfer/anything"),
            ("securityType", "NONE"),
            ("signed", False),
            ("mutationCount", 1),
            ("redirectCount", 1),
            ("retryCount", 1),
        ):
            with self.subTest(field=field):
                envelope = signed_get_envelope()
                envelope[field] = replacement
                with self.assertRaisesRegex(ValueError, "signed-get-not-exact"):
                    self.certify(envelope=envelope)

    def test_stale_capture_and_stale_idle_barrier_are_rejected(self) -> None:
        stale = (NOW - timedelta(seconds=31)).isoformat()
        envelope = signed_get_envelope()
        envelope["observedAt"] = stale
        envelope["pages"][0]["receivedAt"] = stale
        with self.assertRaisesRegex(ValueError, "signed-get-not-exact"):
            self.certify(envelope=envelope)

        barrier = idle_barrier()
        barrier["observedAt"] = stale
        with self.assertRaisesRegex(ValueError, "idle-barrier-not-exact"):
            self.certify(barrier=barrier)

    def test_wrong_direction_asset_amount_status_or_id_is_rejected(self) -> None:
        cases = (
            ("type", "UMFUTURE_MAIN"),
            ("asset", "BTC"),
            ("amount", "9.99999999"),
            ("status", "PENDING"),
            ("tranId", TRAN_ID + 1),
        )
        for field, replacement in cases:
            with self.subTest(field=field):
                envelope = signed_get_envelope()
                envelope["pages"][0]["rows"][0][field] = replacement
                with self.assertRaisesRegex(
                    ValueError, "exact-official-record-not-unique"
                ):
                    self.certify(envelope=envelope)

    def test_incomplete_or_duplicate_pagination_is_rejected(self) -> None:
        incomplete = signed_get_envelope()
        incomplete["pages"][0]["total"] = 101
        with self.assertRaisesRegex(ValueError, "pagination-incomplete"):
            self.certify(envelope=incomplete)

        duplicate = signed_get_envelope()
        duplicate["pages"][0]["rows"].append(
            copy.deepcopy(duplicate["pages"][0]["rows"][0])
        )
        duplicate["pages"][0]["total"] = 2
        with self.assertRaisesRegex(ValueError, "duplicate-official-row"):
            self.certify(envelope=duplicate)

    def test_non_idle_or_nonzero_account_truth_is_rejected(self) -> None:
        for field, replacement in (
            ("coordinatorPhase", "ACTIVE"),
            ("globalLeaseState", "OWNED"),
            ("activeOwnerCount", 1),
            ("mutationInFlightCount", 1),
            ("spotOpenOrderCount", 1),
            ("futuresOpenOrderCount", 1),
            ("futuresPositionCount", 1),
        ):
            with self.subTest(field=field):
                barrier = idle_barrier()
                barrier[field] = replacement
                with self.assertRaisesRegex(ValueError, "idle-barrier-not-exact"):
                    self.certify(barrier=barrier)

    def test_replay_at_or_below_consumed_high_water_is_rejected(self) -> None:
        prior = empty_consumption_high_water(
            account_fingerprint=ACCOUNT,
            api_key_fingerprint=API_KEY,
        )
        prior.update(
            {
                "revision": 1,
                "transferTimestamp": TRANSFER_TIMESTAMP,
                "tranId": str(TRAN_ID),
                "consumptionKey": "c" * 64,
                "headHash": "d" * 64,
            }
        )
        with self.assertRaisesRegex(ValueError, "not-above-consumed-high-water"):
            self.certify(prior=prior)

    def test_detached_verifiers_are_mandatory_and_exact_true(self) -> None:
        for signed, idle, message in (
            (None, lambda _: True, "signed-get-verifier-required"),
            (lambda _: 1, lambda _: True, "signed-get-unverified"),
            (lambda _: True, None, "idle-verifier-required"),
            (lambda _: True, lambda _: 1, "idle-barrier-unverified"),
        ):
            with self.subTest(message=message):
                verifier = BinanceCashTransferEvidenceVerifier(
                    configured_account_fingerprint=ACCOUNT,
                    configured_api_key_fingerprint=API_KEY,
                    signed_get_verifier=signed,
                    idle_barrier_verifier=idle,
                    clock=lambda: NOW,
                )
                with self.assertRaisesRegex(ValueError, message):
                    verifier.certify(
                        expected_tran_id=TRAN_ID,
                        signed_get_envelope=signed_get_envelope(),
                        idle_barrier_evidence=idle_barrier(),
                        prior_consumed_high_water=empty_consumption_high_water(
                            account_fingerprint=ACCOUNT,
                            api_key_fingerprint=API_KEY,
                        ),
                    )


if __name__ == "__main__":
    unittest.main()
