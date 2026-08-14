from __future__ import annotations

"""Fail-closed evidence for one historical Binance Spot -> USD-M transfer.

This module never performs HTTP.  A caller must inject independent verifiers
for a detached, signed GET capture and for the global/coordinator IDLE barrier.

Official contract (Binance Wallet REST API):
https://developers.binance.com/en/docs/catalog/core-trading-wallet/api/rest-api/asset#query-user-universal-transfer-history

The official history endpoint is the signed USER_DATA
``GET /sapi/v1/asset/transfer``.  ``MAIN_UMFUTURE`` means Spot -> USD-M
Futures and a completed history row has status ``CONFIRMED``.  We preserve
that official value and derive the internal result ``SUCCESS`` from it.
"""

import hashlib
import json
import math
import secrets
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Mapping


BINANCE_CASH_TRANSFER_EVIDENCE_RELEASED = False

SIGNED_GET_SCHEMA = "binance-universal-transfer-signed-get/v1"
IDLE_BARRIER_SCHEMA = "binance-cash-transfer-idle-barrier/v1"
HIGH_WATER_SCHEMA = "binance-cash-transfer-consumption-high-water/v1"
TRUTH_SCHEMA = "binance-spot-futures-cash-transfer-truth/v2"
AUTHORITY_SCHEMA = "binance-cash-transfer-adjustment-authority/v2"
HIGH_WATER_REQUEST_SCHEMA = "binance-cash-transfer-high-water-authority/v1"

OFFICIAL_METHOD = "GET"
OFFICIAL_PATH = "/sapi/v1/asset/transfer"
OFFICIAL_SECURITY_TYPE = "USER_DATA"
OFFICIAL_TRANSFER_TYPE = "MAIN_UMFUTURE"
OFFICIAL_SUCCESS_STATUS = "CONFIRMED"
INTERNAL_SUCCESS_RESULT = "SUCCESS"
OFFICIAL_ASSET = "USDT"
EXACT_AMOUNT = Decimal("10")
PAGE_SIZE = 100
MAX_CAPTURE_AGE_SECONDS = 30.0
# Deliberately narrower than the official "last 6 months" maximum so a
# fixed-duration verifier never accidentally exceeds a shorter calendar span.
MAX_QUERY_WINDOW_MILLISECONDS = 180 * 24 * 60 * 60 * 1000
MAX_SIGNED_REQUEST_SKEW_MILLISECONDS = 60_000

_SIGNED_GET_FIELDS = {
    "schemaVersion",
    "accountFingerprint",
    "apiKeyFingerprint",
    "method",
    "path",
    "securityType",
    "signed",
    "transferType",
    "queryStartTime",
    "queryEndTime",
    "pageSize",
    "pages",
    "allPagesComplete",
    "requestCount",
    "retryCount",
    "redirectCount",
    "mutationCount",
    "observedAt",
    "detachedCaptureHash",
}
_PAGE_FIELDS = {
    "current",
    "httpStatus",
    "total",
    "rows",
    "requestTimestamp",
    "receivedAt",
    "requestHash",
    "responseHash",
}
_OFFICIAL_ROW_FIELDS = {
    "asset",
    "amount",
    "type",
    "status",
    "tranId",
    "timestamp",
}
_IDLE_BARRIER_FIELDS = {
    "schemaVersion",
    "barrierId",
    "accountFingerprint",
    "apiKeyFingerprint",
    "coordinatorPhase",
    "coordinatorRevision",
    "globalLeaseState",
    "activeOwnerCount",
    "mutationInFlightCount",
    "spotOpenOrderCount",
    "futuresOpenOrderCount",
    "futuresPositionCount",
    "spotCash",
    "futuresCash",
    "observedAt",
    "detachedEvidenceHash",
}
_HIGH_WATER_FIELDS = {
    "schemaVersion",
    "revision",
    "accountFingerprint",
    "apiKeyFingerprint",
    "transferType",
    "transferTimestamp",
    "tranId",
    "consumptionKey",
    "headHash",
}
_TRUTH_FIELDS = {
    "schemaVersion",
    "accountFingerprint",
    "apiKeyFingerprint",
    "spotCash",
    "futuresCash",
    "spotOpenOrderCount",
    "futuresOpenOrderCount",
    "futuresPositionCount",
    "signedGetComplete",
    "coordinatorPhase",
    "globalLeaseState",
    "observedAt",
    "officialTransfer",
    "consumptionKey",
    "priorConsumedHighWater",
    "signedGetEnvelopeHash",
    "idleBarrierHash",
    "signedGetEnvelope",
    "idleBarrierEvidence",
}
_NORMALIZED_TRANSFER_FIELDS = {
    "tranId",
    "asset",
    "amount",
    "type",
    "status",
    "result",
    "timestamp",
    "eventTime",
}


def canonical_json(value: Mapping[str, Any] | list[Any]) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def content_hash(value: Mapping[str, Any] | list[Any]) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _exact_hash(value: object, label: str) -> str:
    normalized = str(value or "").strip().lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError(f"{label}-invalid")
    return normalized


def _exact_fingerprint(value: object, label: str) -> str:
    return _exact_hash(value, label)


def _exact_positive_int(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{label}-invalid")
    return value


def _exact_nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{label}-invalid")
    return value


def _exact_decimal(value: object, label: str) -> Decimal:
    if type(value) not in {str, int}:
        raise ValueError(f"{label}-invalid")
    try:
        parsed = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{label}-invalid") from exc
    if not parsed.is_finite() or parsed < 0:
        raise ValueError(f"{label}-invalid")
    return parsed


def _utc(value: object, label: str) -> datetime:
    if type(value) is not str or not value.strip():
        raise ValueError(f"{label}-invalid")
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{label}-invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{label}-timezone-required")
    return parsed.astimezone(timezone.utc)


def _utc_text(value: object, label: str) -> str:
    return _utc(value, label).isoformat()


def _now_utc(clock: Callable[[], datetime]) -> datetime:
    current = clock()
    if not isinstance(current, datetime):
        raise ValueError("binance-transfer-evidence-clock-invalid")
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("binance-transfer-evidence-clock-timezone-required")
    return current.astimezone(timezone.utc)


def empty_consumption_high_water(
    *,
    account_fingerprint: str,
    api_key_fingerprint: str,
) -> dict[str, Any]:
    return {
        "schemaVersion": HIGH_WATER_SCHEMA,
        "revision": 0,
        "accountFingerprint": _exact_fingerprint(
            account_fingerprint, "binance-transfer-account-fingerprint"
        ),
        "apiKeyFingerprint": _exact_fingerprint(
            api_key_fingerprint, "binance-transfer-api-key-fingerprint"
        ),
        "transferType": OFFICIAL_TRANSFER_TYPE,
        "transferTimestamp": 0,
        "tranId": "",
        "consumptionKey": "",
        "headHash": "",
    }


class BinanceCashTransferEvidenceVerifier:
    """Strict, restart-verifiable adapter around injected detached proofs."""

    def __init__(
        self,
        *,
        configured_account_fingerprint: str,
        configured_api_key_fingerprint: str,
        signed_get_verifier: Callable[[Mapping[str, Any]], bool] | None,
        idle_barrier_verifier: Callable[[Mapping[str, Any]], bool] | None,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self.account_fingerprint = _exact_fingerprint(
            configured_account_fingerprint,
            "binance-transfer-configured-account-fingerprint",
        )
        self.api_key_fingerprint = _exact_fingerprint(
            configured_api_key_fingerprint,
            "binance-transfer-configured-api-key-fingerprint",
        )
        self.signed_get_verifier = signed_get_verifier
        self.idle_barrier_verifier = idle_barrier_verifier
        self.clock = clock

    def certify(
        self,
        *,
        expected_tran_id: int,
        signed_get_envelope: Mapping[str, Any],
        idle_barrier_evidence: Mapping[str, Any],
        prior_consumed_high_water: Mapping[str, Any],
    ) -> dict[str, Any]:
        if not BINANCE_CASH_TRANSFER_EVIDENCE_RELEASED:
            raise ValueError("binance-cash-transfer-evidence-not-released")
        truth = self._build_truth(
            expected_tran_id=expected_tran_id,
            signed_get_envelope=signed_get_envelope,
            idle_barrier_evidence=idle_barrier_evidence,
            prior_consumed_high_water=prior_consumed_high_water,
        )
        return {
            "truthEvidence": truth,
            "truthHash": content_hash(truth),
        }

    def verify_ledger_authority_request(
        self, request: Mapping[str, Any]
    ) -> bool:
        if not BINANCE_CASH_TRANSFER_EVIDENCE_RELEASED:
            return False
        try:
            if set(request) != {
                "schemaVersion",
                "sourceBrokerId",
                "destinationBrokerId",
                "sourceAccount",
                "destinationAccount",
                "accountFingerprint",
                "apiKeyFingerprint",
                "amount",
                "sourceCashBefore",
                "sourceCashAfter",
                "destinationCashBefore",
                "destinationCashAfter",
                "observedAt",
                "officialTransfer",
                "consumptionKey",
                "priorConsumedHighWater",
                "truthEvidence",
                "truthHash",
            }:
                return False
            if (
                request.get("schemaVersion") != AUTHORITY_SCHEMA
                or request.get("sourceBrokerId") != "binance"
                or request.get("destinationBrokerId") != "binance-futures"
                or request.get("sourceAccount") != "Binance Spot"
                or request.get("destinationAccount")
                != "Binance USD-M Futures"
                or request.get("amount") != "10"
            ):
                return False
            evidence = request.get("truthEvidence")
            if not isinstance(evidence, Mapping) or set(evidence) != _TRUTH_FIELDS:
                return False
            transfer = evidence.get("officialTransfer")
            if not isinstance(transfer, Mapping):
                return False
            rebuilt = self._build_truth(
                expected_tran_id=_exact_positive_int(
                    transfer.get("tranId"), "binance-transfer-id"
                ),
                signed_get_envelope=evidence.get("signedGetEnvelope", {}),
                idle_barrier_evidence=evidence.get(
                    "idleBarrierEvidence", {}
                ),
                prior_consumed_high_water=evidence.get(
                    "priorConsumedHighWater", {}
                ),
            )
            if canonical_json(rebuilt) != canonical_json(dict(evidence)):
                return False
            if not secrets.compare_digest(
                _exact_hash(request.get("truthHash"), "binance-truth-hash"),
                content_hash(rebuilt),
            ):
                return False
            expected_projection = {
                "accountFingerprint": rebuilt["accountFingerprint"],
                "apiKeyFingerprint": rebuilt["apiKeyFingerprint"],
                "observedAt": rebuilt["observedAt"],
                "officialTransfer": rebuilt["officialTransfer"],
                "consumptionKey": rebuilt["consumptionKey"],
                "priorConsumedHighWater": rebuilt[
                    "priorConsumedHighWater"
                ],
            }
            for field, expected in expected_projection.items():
                if request.get(field) != expected:
                    return False
            source_before = _exact_decimal(
                request.get("sourceCashBefore"), "binance-source-cash-before"
            )
            destination_before = _exact_decimal(
                request.get("destinationCashBefore"),
                "binance-destination-cash-before",
            )
            if (
                _exact_decimal(
                    request.get("sourceCashAfter"),
                    "binance-source-cash-after",
                )
                != _exact_decimal(
                    rebuilt["spotCash"], "binance-certified-spot-cash"
                )
                or _exact_decimal(
                    request.get("destinationCashAfter"),
                    "binance-destination-cash-after",
                )
                != _exact_decimal(
                    rebuilt["futuresCash"],
                    "binance-certified-futures-cash",
                )
                or source_before - EXACT_AMOUNT
                != _exact_decimal(
                    request.get("sourceCashAfter"),
                    "binance-source-cash-after",
                )
                or destination_before + EXACT_AMOUNT
                != _exact_decimal(
                    request.get("destinationCashAfter"),
                    "binance-destination-cash-after",
                )
            ):
                return False
            return True
        except (TypeError, ValueError, ArithmeticError):
            return False

    def verify_high_water_request(self, request: Mapping[str, Any]) -> bool:
        if not BINANCE_CASH_TRANSFER_EVIDENCE_RELEASED:
            return False
        try:
            if set(request) != {
                "schemaVersion",
                "accountFingerprint",
                "apiKeyFingerprint",
                "transferType",
                "transferTimestamp",
                "tranId",
                "consumptionKey",
                "priorConsumedHighWater",
                "truthHash",
            }:
                return False
            prior = self._validate_high_water(
                request.get("priorConsumedHighWater", {})
            )
            return (
                request.get("schemaVersion") == HIGH_WATER_REQUEST_SCHEMA
                and request.get("accountFingerprint")
                == self.account_fingerprint
                and request.get("apiKeyFingerprint") == self.api_key_fingerprint
                and request.get("transferType") == OFFICIAL_TRANSFER_TYPE
                and type(request.get("transferTimestamp")) is int
                and request.get("transferTimestamp") > 0
                and type(request.get("tranId")) is int
                and request.get("tranId") > 0
                and _exact_hash(
                    request.get("consumptionKey"),
                    "binance-transfer-consumption-key",
                )
                == request.get("consumptionKey")
                and _exact_hash(
                    request.get("truthHash"), "binance-transfer-truth-hash"
                )
                == request.get("truthHash")
                and prior == dict(request["priorConsumedHighWater"])
            )
        except (TypeError, ValueError):
            return False

    def _build_truth(
        self,
        *,
        expected_tran_id: int,
        signed_get_envelope: Mapping[str, Any],
        idle_barrier_evidence: Mapping[str, Any],
        prior_consumed_high_water: Mapping[str, Any],
    ) -> dict[str, Any]:
        expected_id = _exact_positive_int(
            expected_tran_id, "binance-transfer-id"
        )
        current = _now_utc(self.clock)
        envelope, official = self._validate_signed_get(
            signed_get_envelope, expected_id, current
        )
        barrier = self._validate_idle_barrier(
            idle_barrier_evidence, current
        )
        prior = self._validate_high_water(prior_consumed_high_water)
        observed = _utc_text(envelope["observedAt"], "binance-get-observed-at")
        barrier_observed = _utc(
            barrier["observedAt"], "binance-idle-observed-at"
        )
        if abs(
            (
                _utc(envelope["observedAt"], "binance-get-observed-at")
                - barrier_observed
            ).total_seconds()
        ) > 2.0:
            raise ValueError("binance-transfer-proof-observation-skew")
        event_time = datetime.fromtimestamp(
            official["timestamp"] / 1000, tz=timezone.utc
        ).isoformat()
        normalized_transfer = {
            "tranId": official["tranId"],
            "asset": OFFICIAL_ASSET,
            "amount": "10",
            "type": OFFICIAL_TRANSFER_TYPE,
            "status": OFFICIAL_SUCCESS_STATUS,
            "result": INTERNAL_SUCCESS_RESULT,
            "timestamp": official["timestamp"],
            "eventTime": event_time,
        }
        consumption_key = hashlib.sha256(
            (
                self.account_fingerprint
                + "\x00"
                + self.api_key_fingerprint
                + "\x00"
                + OFFICIAL_TRANSFER_TYPE
                + "\x00"
                + str(official["tranId"])
                + "\x00"
                + str(official["timestamp"])
            ).encode("utf-8")
        ).hexdigest()
        if prior["revision"] > 0:
            prior_order = (
                prior["transferTimestamp"],
                int(prior["tranId"]),
            )
            current_order = (official["timestamp"], official["tranId"])
            if current_order <= prior_order:
                raise ValueError("binance-transfer-not-above-consumed-high-water")
        truth = {
            "schemaVersion": TRUTH_SCHEMA,
            "accountFingerprint": self.account_fingerprint,
            "apiKeyFingerprint": self.api_key_fingerprint,
            "spotCash": str(
                _exact_decimal(barrier["spotCash"], "binance-spot-cash")
            ),
            "futuresCash": str(
                _exact_decimal(barrier["futuresCash"], "binance-futures-cash")
            ),
            "spotOpenOrderCount": 0,
            "futuresOpenOrderCount": 0,
            "futuresPositionCount": 0,
            "signedGetComplete": True,
            "coordinatorPhase": "IDLE",
            "globalLeaseState": "IDLE",
            "observedAt": observed,
            "officialTransfer": normalized_transfer,
            "consumptionKey": consumption_key,
            "priorConsumedHighWater": prior,
            "signedGetEnvelopeHash": content_hash(envelope),
            "idleBarrierHash": content_hash(barrier),
            "signedGetEnvelope": envelope,
            "idleBarrierEvidence": barrier,
        }
        if set(truth) != _TRUTH_FIELDS:
            raise AssertionError("binance-transfer-truth-construction-invalid")
        return truth

    def _validate_signed_get(
        self,
        raw: Mapping[str, Any],
        expected_tran_id: int,
        current: datetime,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        if not isinstance(raw, Mapping) or set(raw) != _SIGNED_GET_FIELDS:
            raise ValueError("binance-transfer-signed-get-fields-not-exact")
        envelope = dict(raw)
        account = _exact_fingerprint(
            envelope["accountFingerprint"],
            "binance-transfer-account-fingerprint",
        )
        api_key = _exact_fingerprint(
            envelope["apiKeyFingerprint"],
            "binance-transfer-api-key-fingerprint",
        )
        if not secrets.compare_digest(account, self.account_fingerprint):
            raise ValueError("binance-transfer-configured-account-mismatch")
        if not secrets.compare_digest(api_key, self.api_key_fingerprint):
            raise ValueError("binance-transfer-configured-api-key-mismatch")
        start = _exact_positive_int(
            envelope["queryStartTime"], "binance-transfer-query-start"
        )
        end = _exact_positive_int(
            envelope["queryEndTime"], "binance-transfer-query-end"
        )
        observed_at = _utc(
            envelope["observedAt"], "binance-transfer-get-observed-at"
        )
        observed_ms = int(observed_at.timestamp() * 1000)
        if (
            envelope["schemaVersion"] != SIGNED_GET_SCHEMA
            or envelope["method"] != OFFICIAL_METHOD
            or envelope["path"] != OFFICIAL_PATH
            or envelope["securityType"] != OFFICIAL_SECURITY_TYPE
            or envelope["signed"] is not True
            or envelope["transferType"] != OFFICIAL_TRANSFER_TYPE
            or envelope["pageSize"] != PAGE_SIZE
            or envelope["allPagesComplete"] is not True
            or envelope["retryCount"] != 0
            or envelope["redirectCount"] != 0
            or envelope["mutationCount"] != 0
            or end < start
            or end - start > MAX_QUERY_WINDOW_MILLISECONDS
            or end > observed_ms
            or observed_at > current
            or (current - observed_at).total_seconds()
            > MAX_CAPTURE_AGE_SECONDS
        ):
            raise ValueError("binance-transfer-signed-get-not-exact")
        _exact_hash(
            envelope["detachedCaptureHash"],
            "binance-transfer-detached-capture-hash",
        )
        pages = envelope["pages"]
        if type(pages) is not list or not pages:
            raise ValueError("binance-transfer-pages-required")
        if envelope["requestCount"] != len(pages):
            raise ValueError("binance-transfer-request-count-mismatch")
        totals: set[int] = set()
        all_rows: list[dict[str, Any]] = []
        seen_records: set[tuple[int, int, str]] = set()
        for index, raw_page in enumerate(pages, start=1):
            if not isinstance(raw_page, Mapping) or set(raw_page) != _PAGE_FIELDS:
                raise ValueError("binance-transfer-page-fields-not-exact")
            page = dict(raw_page)
            total = _exact_nonnegative_int(
                page["total"], "binance-transfer-page-total"
            )
            request_timestamp = _exact_positive_int(
                page["requestTimestamp"],
                "binance-transfer-request-timestamp",
            )
            received = _utc(
                page["receivedAt"], "binance-transfer-page-received-at"
            )
            if (
                page["current"] != index
                or page["httpStatus"] != 200
                or received > observed_at
                or (observed_at - received).total_seconds()
                > MAX_CAPTURE_AGE_SECONDS
                or abs(int(received.timestamp() * 1000) - request_timestamp)
                > MAX_SIGNED_REQUEST_SKEW_MILLISECONDS
            ):
                raise ValueError("binance-transfer-page-not-exact")
            _exact_hash(page["requestHash"], "binance-transfer-request-hash")
            _exact_hash(page["responseHash"], "binance-transfer-response-hash")
            rows = page["rows"]
            if type(rows) is not list or len(rows) > PAGE_SIZE:
                raise ValueError("binance-transfer-page-rows-invalid")
            totals.add(total)
            for raw_row in rows:
                if not isinstance(raw_row, Mapping) or set(raw_row) != _OFFICIAL_ROW_FIELDS:
                    raise ValueError(
                        "binance-transfer-official-row-fields-not-exact"
                    )
                row = dict(raw_row)
                tran_id = _exact_positive_int(
                    row["tranId"], "binance-transfer-row-id"
                )
                timestamp = _exact_positive_int(
                    row["timestamp"], "binance-transfer-row-timestamp"
                )
                _exact_decimal(row["amount"], "binance-transfer-row-amount")
                if timestamp < start or timestamp > end or timestamp > observed_ms:
                    raise ValueError("binance-transfer-row-time-outside-query")
                record_key = (tran_id, timestamp, str(row["type"]))
                if record_key in seen_records:
                    raise ValueError("binance-transfer-duplicate-official-row")
                seen_records.add(record_key)
                all_rows.append(row)
        if len(totals) != 1:
            raise ValueError("binance-transfer-page-total-inconsistent")
        total = next(iter(totals))
        expected_pages = max(1, math.ceil(total / PAGE_SIZE))
        if len(pages) != expected_pages or len(all_rows) != total:
            raise ValueError("binance-transfer-pagination-incomplete")
        verifier = self.signed_get_verifier
        if verifier is None:
            raise ValueError("binance-transfer-signed-get-verifier-required")
        try:
            accepted = verifier(envelope)
        except BaseException as exc:
            raise ValueError("binance-transfer-signed-get-unverified") from exc
        if accepted is not True:
            raise ValueError("binance-transfer-signed-get-unverified")
        matches = [
            row
            for row in all_rows
            if row["tranId"] == expected_tran_id
            and row["type"] == OFFICIAL_TRANSFER_TYPE
            and row["asset"] == OFFICIAL_ASSET
            and _exact_decimal(row["amount"], "binance-transfer-row-amount")
            == EXACT_AMOUNT
            and row["status"] == OFFICIAL_SUCCESS_STATUS
        ]
        if len(matches) != 1:
            raise ValueError("binance-transfer-exact-official-record-not-unique")
        return envelope, matches[0]

    def _validate_idle_barrier(
        self, raw: Mapping[str, Any], current: datetime
    ) -> dict[str, Any]:
        if not isinstance(raw, Mapping) or set(raw) != _IDLE_BARRIER_FIELDS:
            raise ValueError("binance-transfer-idle-barrier-fields-not-exact")
        barrier = dict(raw)
        observed = _utc(
            barrier["observedAt"], "binance-transfer-idle-observed-at"
        )
        if (
            barrier["schemaVersion"] != IDLE_BARRIER_SCHEMA
            or _exact_fingerprint(
                barrier["accountFingerprint"],
                "binance-transfer-idle-account-fingerprint",
            )
            != self.account_fingerprint
            or _exact_fingerprint(
                barrier["apiKeyFingerprint"],
                "binance-transfer-idle-api-key-fingerprint",
            )
            != self.api_key_fingerprint
            or type(barrier["barrierId"]) is not str
            or not barrier["barrierId"].strip()
            or len(barrier["barrierId"].strip()) > 160
            or barrier["coordinatorPhase"] != "IDLE"
            or type(barrier["coordinatorRevision"]) is not int
            or barrier["coordinatorRevision"] < 1
            or barrier["globalLeaseState"] != "IDLE"
            or barrier["activeOwnerCount"] != 0
            or barrier["mutationInFlightCount"] != 0
            or barrier["spotOpenOrderCount"] != 0
            or barrier["futuresOpenOrderCount"] != 0
            or barrier["futuresPositionCount"] != 0
            or observed > current
            or (current - observed).total_seconds()
            > MAX_CAPTURE_AGE_SECONDS
        ):
            raise ValueError("binance-transfer-idle-barrier-not-exact")
        _exact_decimal(barrier["spotCash"], "binance-transfer-spot-cash")
        _exact_decimal(
            barrier["futuresCash"], "binance-transfer-futures-cash"
        )
        _exact_hash(
            barrier["detachedEvidenceHash"],
            "binance-transfer-idle-evidence-hash",
        )
        verifier = self.idle_barrier_verifier
        if verifier is None:
            raise ValueError("binance-transfer-idle-verifier-required")
        try:
            accepted = verifier(barrier)
        except BaseException as exc:
            raise ValueError("binance-transfer-idle-barrier-unverified") from exc
        if accepted is not True:
            raise ValueError("binance-transfer-idle-barrier-unverified")
        return barrier

    def _validate_high_water(
        self, raw: Mapping[str, Any]
    ) -> dict[str, Any]:
        if not isinstance(raw, Mapping) or set(raw) != _HIGH_WATER_FIELDS:
            raise ValueError("binance-transfer-high-water-fields-not-exact")
        high = dict(raw)
        revision = _exact_nonnegative_int(
            high["revision"], "binance-transfer-high-water-revision"
        )
        if (
            high["schemaVersion"] != HIGH_WATER_SCHEMA
            or _exact_fingerprint(
                high["accountFingerprint"],
                "binance-transfer-high-water-account",
            )
            != self.account_fingerprint
            or _exact_fingerprint(
                high["apiKeyFingerprint"],
                "binance-transfer-high-water-api-key",
            )
            != self.api_key_fingerprint
            or high["transferType"] != OFFICIAL_TRANSFER_TYPE
        ):
            raise ValueError("binance-transfer-high-water-not-exact")
        if revision == 0:
            if (
                high["transferTimestamp"] != 0
                or high["tranId"] != ""
                or high["consumptionKey"] != ""
                or high["headHash"] != ""
            ):
                raise ValueError("binance-transfer-empty-high-water-invalid")
        else:
            _exact_positive_int(
                high["transferTimestamp"],
                "binance-transfer-high-water-timestamp",
            )
            if type(high["tranId"]) is not str or not high["tranId"].isdigit():
                raise ValueError("binance-transfer-high-water-id-invalid")
            _exact_hash(
                high["consumptionKey"],
                "binance-transfer-high-water-consumption-key",
            )
            _exact_hash(high["headHash"], "binance-transfer-high-water-head")
        return high


def truth_fields() -> frozenset[str]:
    """Expose the frozen ledger contract without returning a mutable set."""

    return frozenset(_TRUTH_FIELDS)


def normalized_transfer_fields() -> frozenset[str]:
    return frozenset(_NORMALIZED_TRANSFER_FIELDS)
