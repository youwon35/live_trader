from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

from .kis_domestic_functional_contract import (
    CLEANUP_END_LATEST,
    KST,
    LIVE_ORIGIN,
    MAX_GROSS_KRW,
    MAX_ORDER_KRW,
    ORDER_QUANTITY,
    OWNER_LOSS_LIMIT_KRW,
    PDNO,
    ROUTE,
)
from .kis_domestic_functional_get_client import (
    KisDomesticFunctionalGetBlocked,
    KisDomesticFunctionalGetClient,
)


KIS_DOMESTIC_FUNCTIONAL_TRUTH_PRODUCTION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_TRUTH_NETWORK_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_TRUTH_MUTATION_AVAILABLE = False


class KisDomesticFunctionalTruthBlocked(RuntimeError):
    pass


@dataclass(frozen=True)
class _Spec:
    name: str
    endpoint: str
    tr_id: str
    output_key: str
    summary_key: str | None
    cursor_fk: str
    cursor_nk: str


_SPECS = (
    _Spec(
        "balance",
        "/uapi/domestic-stock/v1/trading/inquire-balance",
        "TTTC8434R",
        "output1",
        "output2",
        "ctx_area_fk100",
        "ctx_area_nk100",
    ),
    _Spec(
        "dailyCcld",
        "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
        "TTTC0081R",
        "output1",
        "output2",
        "ctx_area_fk100",
        "ctx_area_nk100",
    ),
    _Spec(
        "workingOrders",
        "/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl",
        "TTTC0084R",
        "output",
        None,
        "ctx_area_fk100",
        "ctx_area_nk100",
    ),
    _Spec(
        "periodTradeProfit",
        "/uapi/domestic-stock/v1/trading/inquire-period-trade-profit",
        "TTTC8715R",
        "output1",
        "output2",
        "ctx_area_fk100",
        "ctx_area_nk100",
    ),
    _Spec(
        "periodProfit",
        "/uapi/domestic-stock/v1/trading/inquire-period-profit",
        "TTTC8708R",
        "output1",
        "output2",
        "ctx_area_fk100",
        "ctx_area_nk100",
    ),
    _Spec(
        "holiday",
        "/uapi/domestic-stock/v1/quotations/chk-holiday",
        "CTCA0903R",
        "output",
        None,
        "ctx_area_fk",
        "ctx_area_nk",
    ),
)
_SPEC_BY_NAME = {spec.name: spec for spec in _SPECS}
_OFFICIAL_ID = re.compile(r"^[0-9]{1,16}$", flags=re.ASCII)
_ORDER_DATE = re.compile(r"^[0-9]{8}$", flags=re.ASCII)


def _official_identity(
    order_date: Any,
    org_no: Any,
    odno: Any,
    *,
    label: str,
) -> tuple[str, str, str]:
    if type(order_date) is not str or not _ORDER_DATE.fullmatch(order_date):
        raise KisDomesticFunctionalTruthBlocked(f"{label} order date is not exact ASCII digits")
    for name, value in (("orgNo", org_no), ("odno", odno)):
        if type(value) is not str or not _OFFICIAL_ID.fullmatch(value):
            raise KisDomesticFunctionalTruthBlocked(
                f"{label} {name} is not exact ASCII digits 1..16"
            )
    return order_date, org_no, odno


def _identity_json(identity: tuple[str, str, str]) -> str:
    return _canonical(list(identity)).decode("utf-8")


def _identity_display(identity: tuple[str, str, str]) -> str:
    return ":".join(identity)


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _hmac_hex(key: bytes, value: Any) -> str:
    return hmac.new(key, _canonical(value), hashlib.sha256).hexdigest()


def _redact_account_fields(value: Any, account_fingerprint: str) -> Any:
    if isinstance(value, Mapping):
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            normalized = str(key).upper()
            if normalized in {"CANO", "ACNT_PRDT_CD", "ACCOUNT_NO", "ACCOUNT_PRODUCT_CODE"}:
                redacted[str(key)] = f"<redacted:{account_fingerprint}>"
            else:
                redacted[str(key)] = _redact_account_fields(item, account_fingerprint)
        return redacted
    if isinstance(value, list):
        return [_redact_account_fields(item, account_fingerprint) for item in value]
    return value


def _decimal(value: Any, label: str, *, nonnegative: bool = True) -> Decimal:
    if not isinstance(value, str) or not value.strip():
        raise KisDomesticFunctionalTruthBlocked(f"{label} must be an official numeric string")
    normalized = value.strip()
    if not re.fullmatch(r"[+-]?[0-9]+(?:\.[0-9]+)?", normalized):
        raise KisDomesticFunctionalTruthBlocked(f"{label} has unknown numeric grammar")
    try:
        result = Decimal(normalized)
    except InvalidOperation as exc:
        raise KisDomesticFunctionalTruthBlocked(f"{label} is not decimal") from exc
    if not result.is_finite():
        raise KisDomesticFunctionalTruthBlocked(f"{label} is not finite")
    if nonnegative and result < 0:
        raise KisDomesticFunctionalTruthBlocked(f"{label} cannot be negative")
    return result


def _integer(value: Any, label: str, *, nonnegative: bool = True) -> int:
    parsed = _decimal(value, label, nonnegative=nonnegative)
    if parsed != parsed.to_integral_value():
        raise KisDomesticFunctionalTruthBlocked(f"{label} must be an integer")
    return int(parsed)


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise KisDomesticFunctionalTruthBlocked(f"{label} must be an object")
    return value


def _rows(value: Any, label: str) -> list[Mapping[str, Any]]:
    if not isinstance(value, list):
        raise KisDomesticFunctionalTruthBlocked(f"{label} must be an array")
    return [_mapping(row, f"{label}[{index}]") for index, row in enumerate(value)]


def _single_summary(values: Sequence[Mapping[str, Any]], label: str) -> Mapping[str, Any]:
    if not values:
        raise KisDomesticFunctionalTruthBlocked(f"{label} summary is missing")
    first = dict(values[0])
    if any(dict(value) != first for value in values[1:]):
        raise KisDomesticFunctionalTruthBlocked(f"{label} summaries disagree across pages")
    return first


def _stable_causal_projection(capture: Mapping[str, Any]) -> dict[str, Any]:
    """Project only account/order facts that must be quiescent across reads.

    Live marks (`prpr`, `evlu_amt`, `tot_evlu_amt`, `nass_amt`) are retained in
    each signed raw capture but deliberately excluded here.  They can change
    solely because the market moved and are not causal evidence of an account
    mutation.
    """

    endpoints = _mapping(capture.get("endpoints"), "capture.endpoints")
    if set(endpoints) != set(_SPEC_BY_NAME):
        raise KisDomesticFunctionalTruthBlocked("capture endpoint set is incomplete")
    projection: dict[str, Any] = {}
    for spec in _SPECS:
        endpoint = _mapping(endpoints.get(spec.name), f"capture.{spec.name}")
        rows = [dict(row) for row in _rows(endpoint.get("rows"), f"{spec.name} rows")]
        summary = endpoint.get("summary")
        if spec.name == "balance":
            rows = [
                {key: value for key, value in row.items() if key not in {"prpr", "evlu_amt"}}
                for row in rows
            ]
            if isinstance(summary, Mapping):
                summary = {
                    key: value
                    for key, value in summary.items()
                    if key not in {"tot_evlu_amt", "nass_amt"}
                }
        pages = _rows(endpoint.get("pages"), f"{spec.name} pages")
        topology: list[dict[str, Any]] = []
        for page in pages:
            body = _mapping(page.get("body"), f"{spec.name} page body")
            page_rows = _rows(body.get(spec.output_key), f"{spec.name} page rows")
            topology.append(
                {
                    "pageIndex": page.get("pageIndex"),
                    "continuationSent": page.get("continuationSent"),
                    "continuationReceived": page.get("continuationReceived"),
                    "cursorReceived": page.get("cursorReceived"),
                    "rowCount": len(page_rows),
                }
            )
        projected: dict[str, Any] = {"rows": rows, "pagination": topology}
        if spec.summary_key is not None:
            projected["summary"] = dict(summary) if isinstance(summary, Mapping) else summary
        projection[spec.name] = projected
    return projection


_ACTION_KEYS = {
    "actionKind",
    "orderDate",
    "orgNo",
    "odno",
    "side",
    "quantity",
    "submittedGrossKrw",
}
_OWNED_RECORD_BODY_KEYS = {
    "schemaVersion",
    "route",
    "origin",
    "pdno",
    "accountFingerprint",
    "sessionId",
    "permitId",
    "permitHash",
    "phase",
    "baselineQuantity",
    "baselineCashKrw",
    "actions",
}
_PREACTIVATION_BASELINE_BODY_KEYS = {
    "schemaVersion",
    "route",
    "origin",
    "pdno",
    "accountFingerprint",
    "credentialConfigurationHash",
    "tradingDate",
    "capturedAt",
    "stableReadElapsedSeconds",
    "rawCaptureHashes",
    "causalProjectionHash",
    "normalized",
    "durableCasPersisted",
}
_PREACTIVATION_BASELINE_ENVELOPE_KEYS = {
    "body",
    "baselineHash",
    "serverAuthoritySignature",
}


def seal_owned_action_record(body: Mapping[str, Any]) -> dict[str, Any]:
    if set(body) != _OWNED_RECORD_BODY_KEYS:
        raise KisDomesticFunctionalTruthBlocked("owned action record body is not exact")
    copied = json.loads(_canonical(body).decode("utf-8"))
    return {"body": copied, "sealHash": _hash(copied)}


def _verified_owned_record(
    value: Mapping[str, Any],
    *,
    account_fingerprint: str,
    trading_date: date,
) -> tuple[Mapping[str, Any], tuple[Mapping[str, Any], ...]]:
    if set(value) != {"body", "sealHash"}:
        raise KisDomesticFunctionalTruthBlocked("owned action seal envelope is not exact")
    body = _mapping(value.get("body"), "owned action body")
    if set(body) != _OWNED_RECORD_BODY_KEYS:
        raise KisDomesticFunctionalTruthBlocked("owned action record body is not exact")
    seal_hash = value.get("sealHash")
    if not isinstance(seal_hash, str) or not hmac.compare_digest(seal_hash, _hash(body)):
        raise KisDomesticFunctionalTruthBlocked("owned action record seal hash mismatch")
    exact = {
        "schemaVersion": "kis-domestic-owned-actions/v1",
        "route": ROUTE,
        "origin": LIVE_ORIGIN,
        "pdno": PDNO,
        "accountFingerprint": account_fingerprint,
        "phase": "TERMINAL",
    }
    for key, expected in exact.items():
        if type(body.get(key)) is not type(expected) or body.get(key) != expected:
            raise KisDomesticFunctionalTruthBlocked(f"owned action record {key} mismatch")
    for key in ("sessionId", "permitId"):
        if not isinstance(body.get(key), str) or not body[key]:
            raise KisDomesticFunctionalTruthBlocked(f"owned action record {key} missing")
    permit_hash = body.get("permitHash")
    if not isinstance(permit_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", permit_hash):
        raise KisDomesticFunctionalTruthBlocked("owned action permitHash malformed")
    _decimal(body.get("baselineQuantity"), "owned baselineQuantity")
    _decimal(body.get("baselineCashKrw"), "owned baselineCashKrw", nonnegative=False)
    actions = _rows(body.get("actions"), "owned actions")
    if len(actions) not in {0, 1, 2}:
        raise KisDomesticFunctionalTruthBlocked("owned action cardinality exceeds BUY1+cleanup SELL1")
    expected_kinds = ["NATURAL_BUY", "CLEANUP_SELL"][: len(actions)]
    seen_keys: set[str] = set()
    for index, (action, expected_kind) in enumerate(zip(actions, expected_kinds)):
        if set(action) != _ACTION_KEYS:
            raise KisDomesticFunctionalTruthBlocked(f"owned action[{index}] fields are not exact")
        if action.get("actionKind") != expected_kind:
            raise KisDomesticFunctionalTruthBlocked("owned action sequence permits no reentry")
        expected_side = "02" if expected_kind == "NATURAL_BUY" else "01"
        if action.get("side") != expected_side or action.get("quantity") != "1":
            raise KisDomesticFunctionalTruthBlocked("owned action side/quantity is not exact")
        org = action.get("orgNo")
        odno = action.get("odno")
        identity = _official_identity(
            action.get("orderDate"), org, odno, label=f"owned action[{index}]"
        )
        if identity[0] != trading_date.strftime("%Y%m%d"):
            raise KisDomesticFunctionalTruthBlocked("owned action date mismatch")
        key = _identity_json(identity)
        if key in seen_keys:
            raise KisDomesticFunctionalTruthBlocked("owned action official identity duplicated")
        seen_keys.add(key)
        gross = _decimal(action.get("submittedGrossKrw"), "owned submittedGrossKrw")
        if gross > MAX_GROSS_KRW:
            raise KisDomesticFunctionalTruthBlocked("owned action gross cap exceeded")
    return body, tuple(actions)


class KisDomesticFunctionalTruthReader:
    def __init__(
        self,
        *,
        client: KisDomesticFunctionalGetClient,
        cano: str,
        account_product_code: str,
        trading_date: date,
        origin: str = LIVE_ORIGIN,
        clock: Callable[[], datetime] | None = None,
        max_pages: int = 20,
        max_stable_read_seconds: float = 120.0,
    ) -> None:
        if origin != LIVE_ORIGIN:
            raise KisDomesticFunctionalTruthBlocked("KIS live origin is not exact")
        if type(client) is not KisDomesticFunctionalGetClient:
            raise KisDomesticFunctionalTruthBlocked(
                "truth reader requires the exact trusted KIS functional GET client"
            )
        if not re.fullmatch(r"[0-9]{8}", cano):
            raise KisDomesticFunctionalTruthBlocked("CANO must be exactly eight digits")
        if not re.fullmatch(r"[0-9]{2}", account_product_code):
            raise KisDomesticFunctionalTruthBlocked("ACNT_PRDT_CD must be exactly two digits")
        account_fingerprint = client.account_fingerprint
        if not re.fullmatch(r"[0-9a-f]{64}", account_fingerprint):
            raise KisDomesticFunctionalTruthBlocked("trusted client account fingerprint is malformed")
        attestation = _mapping(client.authenticated_attestation(), "signed client attestation")
        expected_attestation = {
            "schemaVersion": "kis-authenticated-get-attestation/v1",
            "environment": "KIS_LIVE",
            "origin": LIVE_ORIGIN,
            "custtype": "P",
            "accountFingerprint": account_fingerprint,
            "credentialConfigurationHash": client.credential_configuration_hash,
            "authenticated": True,
            "allowedMethods": ["GET"],
        }
        if set(attestation) != set(expected_attestation) | {"signatureHash"}:
            raise KisDomesticFunctionalTruthBlocked("signed client attestation fields are not exact")
        for key, expected in expected_attestation.items():
            if type(attestation.get(key)) is not type(expected) or attestation.get(key) != expected:
                raise KisDomesticFunctionalTruthBlocked(f"signed client attestation {key} mismatch")
        if not isinstance(attestation.get("signatureHash"), str) or not re.fullmatch(
            r"[0-9a-f]{64}", attestation["signatureHash"]
        ):
            raise KisDomesticFunctionalTruthBlocked("signed client attestation signatureHash malformed")
        if not client.verify_authenticated_attestation(attestation):
            raise KisDomesticFunctionalTruthBlocked(
                "signed client attestation failed server-authority verification"
            )
        if not isinstance(trading_date, date) or isinstance(trading_date, datetime):
            raise KisDomesticFunctionalTruthBlocked("trading_date must be a date")
        if type(max_pages) is not int or not 1 <= max_pages <= 100:
            raise KisDomesticFunctionalTruthBlocked("max_pages is invalid")
        if (
            not isinstance(max_stable_read_seconds, (int, float))
            or isinstance(max_stable_read_seconds, bool)
            or not 0 < max_stable_read_seconds <= 120
        ):
            raise KisDomesticFunctionalTruthBlocked("stable-read SLA is invalid")
        self.client = client
        self.cano = cano
        self.account_product_code = account_product_code
        self.account_fingerprint = account_fingerprint
        self.signed_client_attestation = dict(attestation)
        self.credential_configuration_hash = client.credential_configuration_hash
        self.trading_date = trading_date
        self.origin = origin
        self.clock = clock or (lambda: datetime.now(KST))
        self.max_pages = max_pages
        self.max_stable_read_seconds = float(max_stable_read_seconds)

    def _now(self) -> datetime:
        value = self.clock()
        if not isinstance(value, datetime) or value.tzinfo is None:
            raise KisDomesticFunctionalTruthBlocked("truth clock must be timezone-aware")
        return value.astimezone(KST)

    def _verify_read_timing(
        self,
        *,
        started: datetime,
        finished: datetime,
        captures: Sequence[Mapping[str, Any]],
    ) -> tuple[float, float, int]:
        if finished < started:
            raise KisDomesticFunctionalTruthBlocked("truth clock moved backwards")
        elapsed = (finished - started).total_seconds()
        total_request_count = sum(
            int(capture.get("officialGetRequestCount", -1)) for capture in captures
        )
        if total_request_count < len(captures) * len(_SPECS):
            raise KisDomesticFunctionalTruthBlocked("official GET request count is incomplete")
        minimum_pacing_floor = max(0, total_request_count - 1) * 2.0
        if self.max_stable_read_seconds + 1e-9 < minimum_pacing_floor:
            raise KisDomesticFunctionalTruthBlocked(
                "stable-read SLA is shorter than the official GET pacing floor"
            )
        if elapsed > self.max_stable_read_seconds:
            raise KisDomesticFunctionalTruthBlocked("stable repeated truth read is stale")
        if (
            finished.date() != self.trading_date
            or finished.time().replace(tzinfo=None) > CLEANUP_END_LATEST
        ):
            raise KisDomesticFunctionalTruthBlocked(
                "stable truth read crossed the KST cleanup/date boundary"
            )
        return elapsed, minimum_pacing_floor, total_request_count

    def _verify_capture_signature(self, label: str, capture: Mapping[str, Any]) -> None:
        signed_body = dict(capture)
        signature = signed_body.pop("serverAuthoritySignature", None)
        durable = signed_body.pop("serverAuthorityEvidenceDurable", None)
        if durable is not False or not self.client.verify_capture_envelope(
            signed_body, signature
        ):
            raise KisDomesticFunctionalTruthBlocked(
                f"{label} raw capture failed server-authority verification"
            )

    def _query(self, name: str, fk: str, nk: str) -> dict[str, str]:
        ymd = self.trading_date.strftime("%Y%m%d")
        account = {"CANO": self.cano, "ACNT_PRDT_CD": self.account_product_code}
        if name == "balance":
            return {
                **account,
                "AFHR_FLPR_YN": "N",
                "OFL_YN": "",
                "INQR_DVSN": "02",
                "UNPR_DVSN": "01",
                "FUND_STTL_ICLD_YN": "N",
                "FNCG_AMT_AUTO_RDPT_YN": "N",
                "PRCS_DVSN": "00",
                "CTX_AREA_FK100": fk,
                "CTX_AREA_NK100": nk,
            }
        if name == "dailyCcld":
            return {
                **account,
                "INQR_STRT_DT": ymd,
                "INQR_END_DT": ymd,
                "SLL_BUY_DVSN_CD": "00",
                "PDNO": "",
                "CCLD_DVSN": "00",
                "INQR_DVSN": "00",
                "INQR_DVSN_3": "00",
                "ORD_GNO_BRNO": "",
                "ODNO": "",
                "INQR_DVSN_1": "",
                "EXCG_ID_DVSN_CD": "ALL",
                "CTX_AREA_FK100": fk,
                "CTX_AREA_NK100": nk,
            }
        if name == "workingOrders":
            return {
                **account,
                "INQR_DVSN_1": "1",
                "INQR_DVSN_2": "0",
                "CTX_AREA_FK100": fk,
                "CTX_AREA_NK100": nk,
            }
        if name == "periodTradeProfit":
            return {
                **account,
                "SORT_DVSN": "01",
                "INQR_STRT_DT": ymd,
                "INQR_END_DT": ymd,
                "CBLC_DVSN": "00",
                "PDNO": PDNO,
                "CTX_AREA_FK100": fk,
                "CTX_AREA_NK100": nk,
            }
        if name == "periodProfit":
            return {
                **account,
                "INQR_STRT_DT": ymd,
                "INQR_END_DT": ymd,
                "SORT_DVSN": "01",
                "INQR_DVSN": "00",
                "CBLC_DVSN": "00",
                "PDNO": PDNO,
                "CTX_AREA_FK100": fk,
                "CTX_AREA_NK100": nk,
            }
        if name == "holiday":
            return {"BASS_DT": ymd, "CTX_AREA_FK": fk, "CTX_AREA_NK": nk}
        raise KisDomesticFunctionalTruthBlocked("unknown KIS truth endpoint")

    def _read_pages(self, spec: _Spec) -> dict[str, Any]:
        pages: list[dict[str, Any]] = []
        combined_rows: list[Mapping[str, Any]] = []
        summaries: list[Mapping[str, Any]] = []
        fk = ""
        nk = ""
        continuation = ""
        seen: set[tuple[str, str]] = set()
        for page_index in range(self.max_pages):
            query = self._query(spec.name, fk, nk)
            try:
                response = self.client.get(
                    origin=self.origin,
                    endpoint=spec.endpoint,
                    tr_id=spec.tr_id,
                    query=dict(query),
                    continuation=continuation,
                    public_headers={
                        "custtype": "P",
                        "tr_id": spec.tr_id,
                        "tr_cont": continuation,
                    },
                )
            except KisDomesticFunctionalGetBlocked as exc:
                raise KisDomesticFunctionalTruthBlocked(
                    f"{spec.name} trusted GET client rejected the read: {exc}"
                ) from None
            response = _mapping(response, f"{spec.name} response")
            if set(response) != {"statusCode", "trCont", "body"}:
                raise KisDomesticFunctionalTruthBlocked(f"{spec.name} response envelope is not exact")
            if type(response["statusCode"]) is not int or response["statusCode"] != 200:
                raise KisDomesticFunctionalTruthBlocked(f"{spec.name} HTTP status is not 200")
            tr_cont = response["trCont"]
            if not isinstance(tr_cont, str) or tr_cont not in {"", "M", "F", "D", "E"}:
                raise KisDomesticFunctionalTruthBlocked(f"{spec.name} continuation state is unknown")
            body = _mapping(response["body"], f"{spec.name} body")
            if body.get("rt_cd") != "0":
                raise KisDomesticFunctionalTruthBlocked(f"{spec.name} rt_cd is not exact success")
            rows = _rows(body.get(spec.output_key), f"{spec.name}.{spec.output_key}")
            combined_rows.extend(rows)
            if spec.summary_key is not None:
                summary_value = body.get(spec.summary_key)
                if isinstance(summary_value, Mapping):
                    summaries.append(summary_value)
                elif isinstance(summary_value, list):
                    summaries.extend(_rows(summary_value, f"{spec.name}.{spec.summary_key}"))
                else:
                    raise KisDomesticFunctionalTruthBlocked(f"{spec.name} summary shape is unknown")
            next_fk = body.get(spec.cursor_fk, "")
            next_nk = body.get(spec.cursor_nk, "")
            if not isinstance(next_fk, str) or not isinstance(next_nk, str):
                raise KisDomesticFunctionalTruthBlocked(f"{spec.name} cursor is malformed")
            redacted_query = _redact_account_fields(query, self.account_fingerprint)
            redacted_body = _redact_account_fields(body, self.account_fingerprint)
            page_without_hash = {
                "schemaVersion": "kis-get-raw-page/v1",
                "method": "GET",
                "origin": self.origin,
                "endpoint": spec.endpoint,
                "trId": spec.tr_id,
                "pageIndex": page_index,
                "continuationSent": continuation,
                "accountFingerprint": self.account_fingerprint,
                "query": redacted_query,
                "queryItems": [[key, redacted_query[key]] for key in query],
                "redactedQueryHash": _hash(redacted_query),
                "queryAuthoritySignature": self.client.sign_capture_envelope(
                    {
                        "endpoint": spec.endpoint,
                        "trId": spec.tr_id,
                        "queryItems": [[key, value] for key, value in query.items()],
                        "continuation": continuation,
                        "accountFingerprint": self.account_fingerprint,
                        "credentialConfigurationHash": self.credential_configuration_hash,
                    }
                ),
                "publicRequestHeaders": {
                    "custtype": "P",
                    "tr_id": spec.tr_id,
                    "tr_cont": continuation,
                },
                "continuationReceived": tr_cont,
                "responseHeaders": {"tr_cont": tr_cont},
                "cursorReceived": {"fk": next_fk, "nk": next_nk},
                "body": redacted_body,
                "redactedBodyHash": _hash(redacted_body),
                "bodyAuthoritySignature": self.client.sign_capture_envelope(
                    {
                        "endpoint": spec.endpoint,
                        "trId": spec.tr_id,
                        "pageIndex": page_index,
                        "body": body,
                        "accountFingerprint": self.account_fingerprint,
                        "credentialConfigurationHash": self.credential_configuration_hash,
                    }
                ),
            }
            pages.append(
                {
                    **page_without_hash,
                    "envelopeHash": _hash(page_without_hash),
                    "serverAuthoritySignature": self.client.sign_capture_envelope(
                        page_without_hash
                    ),
                }
            )
            if tr_cont in {"", "D", "E"}:
                break
            pair = (next_fk, next_nk)
            if not next_fk and not next_nk:
                raise KisDomesticFunctionalTruthBlocked(f"{spec.name} continuation cursor is missing")
            if pair in seen:
                raise KisDomesticFunctionalTruthBlocked(f"{spec.name} continuation cursor repeated")
            seen.add(pair)
            fk, nk, continuation = next_fk, next_nk, "N"
        else:
            raise KisDomesticFunctionalTruthBlocked(f"{spec.name} pagination is truncated")
        if not pages or pages[-1]["continuationReceived"] not in {"", "D", "E"}:
            raise KisDomesticFunctionalTruthBlocked(f"{spec.name} pagination is incomplete")
        result: dict[str, Any] = {
            "pages": pages,
            "rows": _redact_account_fields([dict(row) for row in combined_rows], self.account_fingerprint),
        }
        if spec.summary_key is not None:
            result["summary"] = _redact_account_fields(
                dict(_single_summary(summaries, spec.name)), self.account_fingerprint
            )
        return result

    def _capture(self) -> dict[str, Any]:
        endpoints = {spec.name: self._read_pages(spec) for spec in _SPECS}
        request_count = sum(len(endpoint["pages"]) for endpoint in endpoints.values())
        sealed = {
            "schemaVersion": "kis-domestic-functional-raw-capture/v1",
            "route": ROUTE,
            "origin": self.origin,
            "pdno": PDNO,
            "accountFingerprint": self.account_fingerprint,
            "credentialConfigurationHash": self.credential_configuration_hash,
            "authenticatedGetAttestationHash": _hash(self.signed_client_attestation),
            "tradingDate": self.trading_date.isoformat(),
            "officialGetRequestCount": request_count,
            "endpoints": endpoints,
        }
        capture_body = {**sealed, "captureHash": _hash(sealed)}
        return {
            **capture_body,
            "serverAuthoritySignature": self.client.sign_capture_envelope(capture_body),
            "serverAuthorityEvidenceDurable": False,
        }

    def _absolute_cost_truth(self, endpoints: Mapping[str, Any]) -> dict[str, Any]:
        ymd = self.trading_date.strftime("%Y%m%d")
        trade_ep = _mapping(endpoints.get("periodTradeProfit"), "periodTradeProfit")
        trade_totals = self._profit_row_totals(
            _rows(trade_ep.get("rows"), "periodTradeProfit rows"),
            detailed=True,
            ymd=ymd,
        )
        trade_summary = self._profit_summary(
            _mapping(trade_ep.get("summary"), "periodTradeProfit summary"),
            buy_qty_key="buyqty_smtl",
            label="periodTradeProfit",
            require_loan_interest=False,
        )
        daily_ep = _mapping(endpoints.get("periodProfit"), "periodProfit")
        daily_totals = self._profit_row_totals(
            _rows(daily_ep.get("rows"), "periodProfit rows"),
            detailed=False,
            ymd=ymd,
        )
        daily_summary = self._profit_summary(
            _mapping(daily_ep.get("summary"), "periodProfit summary"),
            buy_qty_key="buy_qty_smtl",
            label="periodProfit",
            require_loan_interest=True,
        )
        for label, totals, summary in (
            ("periodTradeProfit", trade_totals, trade_summary),
            ("periodProfit", daily_totals, daily_summary),
        ):
            for field in (
                "buy_qty",
                "buy_amt",
                "sell_qty",
                "sell_amt",
                "fee",
                "tax",
                "realized",
            ):
                if totals[field] != summary[field]:
                    raise KisDomesticFunctionalTruthBlocked(
                        f"{label} {field} aggregate mismatch"
                    )
            if summary["loan_int"] is not None and totals["loan_int"] != summary["loan_int"]:
                raise KisDomesticFunctionalTruthBlocked(
                    f"{label} loan_int aggregate mismatch"
                )
        if trade_totals["loan_int"] != daily_totals["loan_int"]:
            raise KisDomesticFunctionalTruthBlocked("profit TR loan interest disagrees")
        comparable = {
            "buy_qty",
            "buy_amt",
            "sell_qty",
            "sell_amt",
            "sell_fee",
            "sell_tax",
            "fee",
            "tax",
            "buy_fee",
            "buy_tax",
            "realized",
        }
        if any(trade_summary[field] != daily_summary[field] for field in comparable):
            raise KisDomesticFunctionalTruthBlocked("profit TR summaries disagree")
        return {
            "tradeTotals": trade_totals,
            "tradeSummary": trade_summary,
            "dailyTotals": daily_totals,
            "dailySummary": daily_summary,
        }

    @staticmethod
    def _serialize_decimal_mapping(value: Mapping[str, Decimal | None]) -> dict[str, str | None]:
        return {
            key: None if item is None else format(item, "f")
            for key, item in value.items()
        }

    @staticmethod
    def _parse_decimal_mapping(
        value: Mapping[str, Any],
        *,
        label: str,
        expected_keys: set[str],
        nullable: set[str] = frozenset(),
    ) -> dict[str, Decimal | None]:
        if set(value) != expected_keys:
            raise KisDomesticFunctionalTruthBlocked(f"{label} fields are not exact")
        parsed: dict[str, Decimal | None] = {}
        for key in expected_keys:
            raw = value[key]
            if key in nullable and raw is None:
                parsed[key] = None
            else:
                parsed[key] = _decimal(
                    raw,
                    f"{label}.{key}",
                    nonnegative=key != "realized",
                )
        return parsed

    def _preactivation_projection(self, capture: Mapping[str, Any]) -> dict[str, Any]:
        endpoints = _mapping(capture.get("endpoints"), "capture.endpoints")
        if set(endpoints) != set(_SPEC_BY_NAME):
            raise KisDomesticFunctionalTruthBlocked("capture endpoint set is incomplete")
        ymd = self.trading_date.strftime("%Y%m%d")
        holiday = _rows(
            _mapping(endpoints["holiday"], "holiday").get("rows"),
            "holiday rows",
        )
        matches = [row for row in holiday if row.get("bass_dt") == ymd]
        if len(matches) != 1 or matches[0].get("opnd_yn") != "Y":
            raise KisDomesticFunctionalTruthBlocked(
                "exact trading date is not authoritatively open"
            )
        working = _rows(
            _mapping(endpoints["workingOrders"], "workingOrders").get("rows"),
            "workingOrders rows",
        )
        if working:
            raise KisDomesticFunctionalTruthBlocked(
                "preactivation account-wide working orders are not zero"
            )
        balance_ep = _mapping(endpoints["balance"], "balance")
        balance_rows = _rows(balance_ep.get("rows"), "balance rows")
        target_rows = [row for row in balance_rows if row.get("pdno") == PDNO]
        if len(target_rows) > 1:
            raise KisDomesticFunctionalTruthBlocked("target PDNO balance is duplicated")
        for index, row in enumerate(balance_rows):
            pdno = row.get("pdno")
            if not isinstance(pdno, str) or not re.fullmatch(r"[0-9]{6}", pdno):
                raise KisDomesticFunctionalTruthBlocked(
                    f"preactivation balance[{index}] PDNO is malformed"
                )
            for field in ("hldg_qty", "ord_psbl_qty", "pchs_avg_pric", "pchs_amt"):
                _decimal(row.get(field), f"preactivation balance[{index}].{field}")
        target = target_rows[0] if target_rows else None
        quantity = _decimal(target["hldg_qty"], "preactivation target.hldg_qty") if target else Decimal("0")
        orderable = _decimal(target["ord_psbl_qty"], "preactivation target.ord_psbl_qty") if target else Decimal("0")
        balance_summary = _mapping(balance_ep.get("summary"), "balance summary")
        cash = _decimal(
            balance_summary.get("dnca_tot_amt"),
            "preactivation balance summary.dnca_tot_amt",
            nonnegative=False,
        )
        day_amounts = {
            key: _decimal(
                balance_summary.get(key),
                f"preactivation balance summary.{key}",
                nonnegative=False,
            )
            for key in ("thdt_buy_amt", "thdt_sll_amt", "thdt_tlex_amt")
        }
        orders = _rows(
            _mapping(endpoints["dailyCcld"], "dailyCcld").get("rows"),
            "dailyCcld rows",
        )
        order_rows_by_key: dict[str, dict[str, Any]] = {}
        for index, row in enumerate(orders):
            order_date = row.get("ord_dt")
            branch = row.get("ord_gno_brno")
            odno = row.get("odno")
            pdno = row.get("pdno")
            identity = _official_identity(
                order_date,
                branch,
                odno,
                label=f"preactivation dailyCcld[{index}]",
            )
            if identity[0] != ymd or not isinstance(pdno, str) or not re.fullmatch(r"[0-9]{6}", pdno):
                raise KisDomesticFunctionalTruthBlocked(
                    f"preactivation dailyCcld[{index}] identity is incomplete"
                )
            key = _identity_json(identity)
            if key in order_rows_by_key:
                raise KisDomesticFunctionalTruthBlocked(
                    "preactivation official date:org:ODNO is duplicated"
                )
            order_rows_by_key[key] = dict(row)
        costs = self._absolute_cost_truth(endpoints)
        normalized = {
            "schemaVersion": "kis-domestic-preactivation-normalized/v1",
            "targetQuantity": format(quantity, "f"),
            "targetOrderableQuantity": format(orderable, "f"),
            "cashKrw": format(cash, "f"),
            "sameDayBalanceAmounts": self._serialize_decimal_mapping(day_amounts),
            "accountWideOrderRowsByKey": {
                key: order_rows_by_key[key] for key in sorted(order_rows_by_key)
            },
            "periodTradeProfitTotals": self._serialize_decimal_mapping(costs["tradeTotals"]),
            "periodTradeProfitSummary": self._serialize_decimal_mapping(costs["tradeSummary"]),
            "periodProfitTotals": self._serialize_decimal_mapping(costs["dailyTotals"]),
            "periodProfitSummary": self._serialize_decimal_mapping(costs["dailySummary"]),
            "accountWideWorkingOrdersZero": True,
            "tradingDayOpen": True,
        }
        return {**normalized, "normalizedHash": _hash(normalized)}

    def read_preactivation_baseline(self) -> dict[str, Any]:
        started = self._now()
        if started.date() != self.trading_date:
            raise KisDomesticFunctionalTruthBlocked(
                "preactivation truth read is stale for another KST date"
            )
        if started.time().replace(tzinfo=None) > CLEANUP_END_LATEST:
            raise KisDomesticFunctionalTruthBlocked(
                "preactivation truth read exceeded the 15:30 boundary"
            )
        first = self._capture()
        second = self._capture()
        self._verify_capture_signature("first preactivation", first)
        self._verify_capture_signature("second preactivation", second)
        finished = self._now()
        elapsed, pacing_floor, request_count = self._verify_read_timing(
            started=started,
            finished=finished,
            captures=(first, second),
        )
        first_projection = self._preactivation_projection(first)
        second_projection = self._preactivation_projection(second)
        if first_projection != second_projection:
            raise KisDomesticFunctionalTruthBlocked(
                "preactivation causal truth changed across repeated reads"
            )
        body = {
            "schemaVersion": "kis-domestic-preactivation-baseline/v1",
            "route": ROUTE,
            "origin": self.origin,
            "pdno": PDNO,
            "accountFingerprint": self.account_fingerprint,
            "credentialConfigurationHash": self.credential_configuration_hash,
            "tradingDate": self.trading_date.isoformat(),
            "capturedAt": finished.isoformat(),
            "stableReadElapsedSeconds": format(Decimal(str(elapsed)).normalize(), "f"),
            "rawCaptureHashes": [first["captureHash"], second["captureHash"]],
            "causalProjectionHash": first_projection["normalizedHash"],
            "normalized": first_projection,
            "durableCasPersisted": False,
        }
        baseline_hash = _hash(body)
        signed = {**body, "baselineHash": baseline_hash}
        return {
            "body": body,
            "baselineHash": baseline_hash,
            "serverAuthoritySignature": self.client.sign_capture_envelope(signed),
            "rawCaptures": [first, second],
            "officialGetRequestCount": request_count,
            "minimumGetPacingFloorSeconds": format(
                Decimal(str(pacing_floor)).normalize(), "f"
            ),
        }

    def read_fresh_quote_preflight(self) -> dict[str, Any]:
        started = self._now()
        if started.date() != self.trading_date:
            raise KisDomesticFunctionalTruthBlocked(
                "quote preflight is stale for another KST date"
            )
        endpoint = "/uapi/domestic-stock/v1/quotations/inquire-price"
        tr_id = "FHKST01010100"
        query = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": PDNO,
        }
        try:
            response = self.client.get(
                origin=self.origin,
                endpoint=endpoint,
                tr_id=tr_id,
                query=query,
                continuation="",
                public_headers={"custtype": "P", "tr_id": tr_id, "tr_cont": ""},
            )
        except KisDomesticFunctionalGetBlocked as exc:
            raise KisDomesticFunctionalTruthBlocked(
                f"quote trusted GET client rejected the read: {exc}"
            ) from None
        response = _mapping(response, "quote response")
        if set(response) != {"statusCode", "trCont", "body"}:
            raise KisDomesticFunctionalTruthBlocked("quote response envelope is not exact")
        if response["statusCode"] != 200 or response["trCont"] != "":
            raise KisDomesticFunctionalTruthBlocked("quote response status is not exact")
        body = _mapping(response["body"], "quote body")
        if body.get("rt_cd") != "0":
            raise KisDomesticFunctionalTruthBlocked("quote rt_cd is not exact success")
        output = _mapping(body.get("output"), "quote output")
        price = _decimal(output.get("stck_prpr"), "quote.output.stck_prpr")
        if price <= 0:
            raise KisDomesticFunctionalTruthBlocked("quote price is not positive")
        if price * ORDER_QUANTITY > MAX_ORDER_KRW:
            raise KisDomesticFunctionalTruthBlocked("fresh quote exceeds max order KRW")
        finished = self._now()
        if finished < started:
            raise KisDomesticFunctionalTruthBlocked("quote clock moved backwards")
        elapsed = (finished - started).total_seconds()
        if elapsed > 5:
            raise KisDomesticFunctionalTruthBlocked("fresh quote exceeded five seconds")
        redacted_body = _redact_account_fields(body, self.account_fingerprint)
        envelope = {
            "schemaVersion": "kis-domestic-functional-quote-preflight/v1",
            "method": "GET",
            "origin": self.origin,
            "endpoint": endpoint,
            "trId": tr_id,
            "query": query,
            "publicRequestHeaders": {
                "custtype": "P",
                "tr_id": tr_id,
                "tr_cont": "",
            },
            "accountFingerprint": self.account_fingerprint,
            "credentialConfigurationHash": self.credential_configuration_hash,
            "observedAt": finished.isoformat(),
            "elapsedSeconds": format(Decimal(str(elapsed)).normalize(), "f"),
            "body": redacted_body,
            "bodyHash": _hash(redacted_body),
            "priceKrw": format(price, "f"),
            "quantity": ORDER_QUANTITY,
            "notionalKrw": format(price * ORDER_QUANTITY, "f"),
            "orderCapSatisfied": True,
            "durableCasPersisted": False,
        }
        signed_body = {**envelope, "quoteHash": _hash(envelope)}
        return {
            **signed_body,
            "serverAuthoritySignature": self.client.sign_capture_envelope(signed_body),
        }

    def _verify_preactivation_baseline(
        self, value: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        envelope = _mapping(value, "preactivation baseline")
        required = _PREACTIVATION_BASELINE_ENVELOPE_KEYS | {
            "rawCaptures",
            "officialGetRequestCount",
            "minimumGetPacingFloorSeconds",
        }
        if set(envelope) != required:
            raise KisDomesticFunctionalTruthBlocked(
                "preactivation baseline envelope fields are not exact"
            )
        body = _mapping(envelope.get("body"), "preactivation baseline body")
        if set(body) != _PREACTIVATION_BASELINE_BODY_KEYS:
            raise KisDomesticFunctionalTruthBlocked(
                "preactivation baseline body fields are not exact"
            )
        baseline_hash = envelope.get("baselineHash")
        if (
            type(baseline_hash) is not str
            or not re.fullmatch(r"[0-9a-f]{64}", baseline_hash)
            or not hmac.compare_digest(baseline_hash, _hash(body))
        ):
            raise KisDomesticFunctionalTruthBlocked(
                "preactivation baseline hash mismatch"
            )
        signed = {**dict(body), "baselineHash": baseline_hash}
        if not self.client.verify_capture_envelope(
            signed, envelope.get("serverAuthoritySignature")
        ):
            raise KisDomesticFunctionalTruthBlocked(
                "preactivation baseline failed server-authority verification"
            )
        exact = {
            "schemaVersion": "kis-domestic-preactivation-baseline/v1",
            "route": ROUTE,
            "origin": self.origin,
            "pdno": PDNO,
            "accountFingerprint": self.account_fingerprint,
            "credentialConfigurationHash": self.credential_configuration_hash,
            "tradingDate": self.trading_date.isoformat(),
            "durableCasPersisted": False,
        }
        for key, expected in exact.items():
            if type(body.get(key)) is not type(expected) or body.get(key) != expected:
                raise KisDomesticFunctionalTruthBlocked(
                    f"preactivation baseline {key} mismatch"
                )
        normalized = _mapping(body.get("normalized"), "preactivation normalized")
        claimed_projection_hash = body.get("causalProjectionHash")
        if (
            type(claimed_projection_hash) is not str
            or not hmac.compare_digest(
                claimed_projection_hash, str(normalized.get("normalizedHash") or "")
            )
        ):
            raise KisDomesticFunctionalTruthBlocked(
                "preactivation causal projection hash mismatch"
            )
        return body

    def read(
        self,
        *,
        owned_action_record: Mapping[str, Any],
        preactivation_baseline: Mapping[str, Any],
    ) -> dict[str, Any]:
        baseline_body = self._verify_preactivation_baseline(preactivation_baseline)
        baseline_normalized = _mapping(
            baseline_body.get("normalized"), "preactivation normalized"
        )
        owned_body, owned_actions = _verified_owned_record(
            owned_action_record,
            account_fingerprint=self.account_fingerprint,
            trading_date=self.trading_date,
        )
        expected = {
            _identity_json(
                _official_identity(
                    action["orderDate"],
                    action["orgNo"],
                    action["odno"],
                    label="owned action",
                )
            )
            for action in owned_actions
        }
        baseline = _decimal(
            baseline_normalized.get("targetQuantity"), "baseline target quantity"
        )
        baseline_cash = _decimal(
            baseline_normalized.get("cashKrw"), "baseline cash", nonnegative=False
        )
        if _decimal(owned_body["baselineQuantity"], "owned baseline quantity") != baseline:
            raise KisDomesticFunctionalTruthBlocked(
                "owned action record baseline quantity disagrees with preactivation truth"
            )
        if _decimal(
            owned_body["baselineCashKrw"], "owned baseline cash", nonnegative=False
        ) != baseline_cash:
            raise KisDomesticFunctionalTruthBlocked(
                "owned action record baseline cash disagrees with preactivation truth"
            )
        started = self._now()
        if started.date() != self.trading_date:
            raise KisDomesticFunctionalTruthBlocked("truth read is stale for another KST date")
        if started.time().replace(tzinfo=None) > CLEANUP_END_LATEST:
            raise KisDomesticFunctionalTruthBlocked("truth read exceeded the 15:30 cleanup boundary")
        first = self._capture()
        second = self._capture()
        self._verify_capture_signature("first", first)
        self._verify_capture_signature("second", second)
        finished = self._now()
        elapsed, minimum_pacing_floor, total_request_count = self._verify_read_timing(
            started=started,
            finished=finished,
            captures=(first, second),
        )
        first_normalized = self._validate_capture(
            first,
            expected,
            baseline,
            baseline_cash,
            owned_actions,
            baseline_normalized,
        )
        second_normalized = self._validate_capture(
            second,
            expected,
            baseline,
            baseline_cash,
            owned_actions,
            baseline_normalized,
        )
        if first_normalized != second_normalized:
            raise KisDomesticFunctionalTruthBlocked(
                "official GET causal truth changed across repeated reads"
            )
        normalized = second_normalized
        evidence = {
            "schemaVersion": "kis-domestic-functional-truth/v1",
            "route": ROUTE,
            "origin": self.origin,
            "pdno": PDNO,
            "tradingDate": self.trading_date.isoformat(),
            "readStartedAt": started.isoformat(),
            "readFinishedAt": finished.isoformat(),
            "stableReadElapsedSeconds": format(Decimal(str(elapsed)).normalize(), "f"),
            "configuredStableReadSlaSeconds": format(
                Decimal(str(self.max_stable_read_seconds)), "f"
            ),
            "minimumGetPacingFloorSeconds": format(
                Decimal(str(minimum_pacing_floor)).normalize(), "f"
            ),
            "officialGetRequestCount": total_request_count,
            "stableRepeatedReads": True,
            "stableComparison": "PARSED_CAUSAL_PROJECTION",
            "dynamicMarketMarksExcludedFromStabilityComparison": True,
            "rawCapturesByteEqual": first["captureHash"] == second["captureHash"],
            "readCount": 2,
            "accountFingerprint": self.account_fingerprint,
            "credentialConfigurationHash": self.credential_configuration_hash,
            "authenticatedGetAttestation": self.signed_client_attestation,
            "serverAuthorityEvidenceDurable": False,
            "sessionId": owned_body["sessionId"],
            "permitId": owned_body["permitId"],
            "permitHash": owned_body["permitHash"],
            "ownedActionSealHash": owned_action_record["sealHash"],
            "preactivationBaselineHash": preactivation_baseline["baselineHash"],
            "preactivationBaselineDurableCasPersisted": False,
            "rawCaptureHashes": [first["captureHash"], second["captureHash"]],
            "rawCaptures": [first, second],
            "normalized": normalized,
            "mutationCount": 0,
            "httpMethods": ["GET"],
        }
        return {**evidence, "truthHash": _hash(evidence)}

    def _validate_capture(
        self,
        capture: Mapping[str, Any],
        expected_order_keys: set[str],
        baseline: Decimal,
        baseline_cash: Decimal,
        owned_actions: Sequence[Mapping[str, Any]],
        baseline_normalized: Mapping[str, Any],
    ) -> dict[str, Any]:
        endpoints = _mapping(capture.get("endpoints"), "capture.endpoints")
        if set(endpoints) != set(_SPEC_BY_NAME):
            raise KisDomesticFunctionalTruthBlocked("capture endpoint set is incomplete")
        holiday = _rows(_mapping(endpoints["holiday"], "holiday").get("rows"), "holiday rows")
        ymd = self.trading_date.strftime("%Y%m%d")
        holiday_matches = [row for row in holiday if row.get("bass_dt") == ymd]
        if len(holiday_matches) != 1 or holiday_matches[0].get("opnd_yn") != "Y":
            raise KisDomesticFunctionalTruthBlocked("exact trading date is not authoritatively open")

        balance_ep = _mapping(endpoints["balance"], "balance")
        balance_rows = _rows(balance_ep.get("rows"), "balance rows")
        target_rows: list[Mapping[str, Any]] = []
        for index, row in enumerate(balance_rows):
            pdno = row.get("pdno")
            if not isinstance(pdno, str) or not re.fullmatch(r"[0-9]{6}", pdno):
                raise KisDomesticFunctionalTruthBlocked(f"balance[{index}] PDNO is malformed")
            for field in ("hldg_qty", "ord_psbl_qty", "pchs_avg_pric", "pchs_amt", "prpr", "evlu_amt"):
                _decimal(row.get(field), f"balance[{index}].{field}")
            if pdno == PDNO:
                target_rows.append(row)
        if len(target_rows) > 1:
            raise KisDomesticFunctionalTruthBlocked("target PDNO balance is duplicated")
        target = target_rows[0] if target_rows else None
        final_quantity = _decimal(target["hldg_qty"], "target.hldg_qty") if target else Decimal("0")
        orderable_quantity = _decimal(target["ord_psbl_qty"], "target.ord_psbl_qty") if target else Decimal("0")
        balance_summary = _mapping(balance_ep.get("summary"), "balance summary")
        for field in ("dnca_tot_amt", "thdt_buy_amt", "thdt_sll_amt", "thdt_tlex_amt", "tot_evlu_amt", "nass_amt"):
            _decimal(balance_summary.get(field), f"balance summary.{field}", nonnegative=False)
        current_cash = _decimal(
            balance_summary.get("dnca_tot_amt"),
            "balance summary.dnca_tot_amt",
            nonnegative=False,
        )
        terminal_day_amounts = {
            key: _decimal(
                balance_summary.get(key),
                f"balance summary.{key}",
                nonnegative=False,
            )
            for key in ("thdt_buy_amt", "thdt_sll_amt", "thdt_tlex_amt")
        }
        baseline_day_amounts_raw = _mapping(
            baseline_normalized.get("sameDayBalanceAmounts"),
            "preactivation sameDayBalanceAmounts",
        )
        baseline_day_amounts = self._parse_decimal_mapping(
            baseline_day_amounts_raw,
            label="preactivation sameDayBalanceAmounts",
            expected_keys={"thdt_buy_amt", "thdt_sll_amt", "thdt_tlex_amt"},
        )

        working_ep = _mapping(endpoints["workingOrders"], "workingOrders")
        working_rows = _rows(working_ep.get("rows"), "workingOrders rows")
        if working_rows:
            raise KisDomesticFunctionalTruthBlocked(
                "account-wide TTTC0084R working orders are not zero"
            )

        order_ep = _mapping(endpoints["dailyCcld"], "dailyCcld")
        orders = _rows(order_ep.get("rows"), "dailyCcld rows")
        baseline_order_rows = _mapping(
            baseline_normalized.get("accountWideOrderRowsByKey"),
            "preactivation accountWideOrderRowsByKey",
        )
        for key, row_value in baseline_order_rows.items():
            row = _mapping(row_value, f"preactivation order {key}")
            identity = _official_identity(
                row.get("ord_dt"),
                row.get("ord_gno_brno"),
                row.get("odno"),
                label="preactivation official order",
            )
            if type(key) is not str or key != _identity_json(identity):
                raise KisDomesticFunctionalTruthBlocked(
                    "preactivation official order key is not canonical identity JSON"
                )
        owned_by_key = {
            _identity_json(
                _official_identity(
                    action["orderDate"],
                    action["orgNo"],
                    action["odno"],
                    label="owned action",
                )
            ): action
            for action in owned_actions
        }
        actual_keys: set[str] = set()
        terminal_keys: set[str] = set()
        buy_qty = buy_amt = sell_qty = sell_amt = Decimal("0")
        for index, row in enumerate(orders):
            order_date = row.get("ord_dt")
            branch = row.get("ord_gno_brno")
            odno = row.get("odno")
            pdno = row.get("pdno")
            try:
                identity = _official_identity(
                    order_date, branch, odno, label=f"dailyCcld[{index}]"
                )
            except KisDomesticFunctionalTruthBlocked:
                raise
            if identity[0] != ymd or not isinstance(pdno, str) or not re.fullmatch(r"[0-9]{6}", pdno):
                raise KisDomesticFunctionalTruthBlocked(f"dailyCcld[{index}] official order identity is incomplete")
            key = _identity_json(identity)
            if key in terminal_keys:
                raise KisDomesticFunctionalTruthBlocked("official date:org:ODNO is duplicated")
            terminal_keys.add(key)
            if key in baseline_order_rows:
                if dict(row) != dict(_mapping(baseline_order_rows[key], f"baseline order {key}")):
                    raise KisDomesticFunctionalTruthBlocked(
                        "preactivation account-wide order row changed after activation"
                    )
                continue
            if pdno != PDNO:
                raise KisDomesticFunctionalTruthBlocked(
                    f"dailyCcld[{index}] new nonowned PDNO activity"
                )
            if key not in owned_by_key:
                raise KisDomesticFunctionalTruthBlocked(
                    "account-wide order truth contains a nonowned order"
                )
            actual_keys.add(key)
            side = row.get("sll_buy_dvsn_cd")
            if side not in {"01", "02"}:
                raise KisDomesticFunctionalTruthBlocked(f"dailyCcld[{index}] side is unknown")
            requested = _integer(row.get("ord_qty"), f"dailyCcld[{index}].ord_qty")
            filled = _integer(row.get("tot_ccld_qty"), f"dailyCcld[{index}].tot_ccld_qty")
            remaining = _integer(row.get("rmn_qty"), f"dailyCcld[{index}].rmn_qty")
            canceled = _integer(row.get("cncl_cfrm_qty"), f"dailyCcld[{index}].cncl_cfrm_qty")
            rejected = _integer(row.get("rjct_qty"), f"dailyCcld[{index}].rjct_qty")
            amount = _decimal(row.get("tot_ccld_amt"), f"dailyCcld[{index}].tot_ccld_amt")
            average = _decimal(row.get("avg_prvs"), f"dailyCcld[{index}].avg_prvs")
            order_price = _decimal(row.get("ord_unpr"), f"dailyCcld[{index}].ord_unpr")
            owned_action = owned_by_key[key]
            if side != owned_action["side"] or requested != 1:
                raise KisDomesticFunctionalTruthBlocked("owned order side/quantity binding mismatch")
            if requested != filled + remaining + canceled + rejected:
                raise KisDomesticFunctionalTruthBlocked("order quantity state does not reconcile")
            if (filled == 0) != (amount == 0 and average == 0):
                raise KisDomesticFunctionalTruthBlocked("order fill amount/average does not reconcile")
            if filled and average * filled != amount:
                raise KisDomesticFunctionalTruthBlocked("order average does not equal cumulative amount")
            if amount > MAX_GROSS_KRW or (order_price > 0 and order_price * requested > MAX_GROSS_KRW):
                raise KisDomesticFunctionalTruthBlocked("order/gross cap exceeded")
            if side == "02":
                buy_qty += filled
                buy_amt += amount
            else:
                sell_qty += filled
                sell_amt += amount
        if actual_keys != expected_order_keys:
            raise KisDomesticFunctionalTruthBlocked("official date:org:ODNO set does not match exact owned set")
        if not set(baseline_order_rows).issubset(terminal_keys):
            raise KisDomesticFunctionalTruthBlocked(
                "preactivation account-wide order rows disappeared at terminal"
            )

        absolute_costs = self._absolute_cost_truth(endpoints)
        trade_totals = absolute_costs["tradeTotals"]
        trade_summary = absolute_costs["tradeSummary"]
        daily_totals = absolute_costs["dailyTotals"]
        daily_summary = absolute_costs["dailySummary"]
        baseline_trade_totals = self._parse_decimal_mapping(
            _mapping(
                baseline_normalized.get("periodTradeProfitTotals"),
                "preactivation periodTradeProfitTotals",
            ),
            label="preactivation periodTradeProfitTotals",
            expected_keys=set(trade_totals),
        )
        baseline_trade_summary = self._parse_decimal_mapping(
            _mapping(
                baseline_normalized.get("periodTradeProfitSummary"),
                "preactivation periodTradeProfitSummary",
            ),
            label="preactivation periodTradeProfitSummary",
            expected_keys=set(trade_summary),
            nullable={"loan_int"},
        )
        baseline_daily_totals = self._parse_decimal_mapping(
            _mapping(
                baseline_normalized.get("periodProfitTotals"),
                "preactivation periodProfitTotals",
            ),
            label="preactivation periodProfitTotals",
            expected_keys=set(daily_totals),
        )
        baseline_daily_summary = self._parse_decimal_mapping(
            _mapping(
                baseline_normalized.get("periodProfitSummary"),
                "preactivation periodProfitSummary",
            ),
            label="preactivation periodProfitSummary",
            expected_keys=set(daily_summary),
            nullable={"loan_int"},
        )

        def subtract(
            current: Mapping[str, Decimal | None],
            before: Mapping[str, Decimal | None],
            label: str,
        ) -> dict[str, Decimal | None]:
            delta: dict[str, Decimal | None] = {}
            for key, current_value in current.items():
                before_value = before[key]
                if current_value is None or before_value is None:
                    if current_value is not None or before_value is not None:
                        raise KisDomesticFunctionalTruthBlocked(
                            f"{label}.{key} optional field availability changed"
                        )
                    delta[key] = None
                    continue
                difference = current_value - before_value
                if key != "realized" and difference < 0:
                    raise KisDomesticFunctionalTruthBlocked(
                        f"{label}.{key} cumulative value moved backwards"
                    )
                delta[key] = difference
            return delta

        trade_delta_totals = subtract(
            trade_totals, baseline_trade_totals, "periodTradeProfit delta totals"
        )
        trade_delta_summary = subtract(
            trade_summary, baseline_trade_summary, "periodTradeProfit delta summary"
        )
        daily_delta_totals = subtract(
            daily_totals, baseline_daily_totals, "periodProfit delta totals"
        )
        daily_delta_summary = subtract(
            daily_summary, baseline_daily_summary, "periodProfit delta summary"
        )
        if trade_delta_totals != daily_delta_totals:
            raise KisDomesticFunctionalTruthBlocked("profit TR cumulative deltas disagree")
        comparable_fields = {
            "buy_qty",
            "buy_amt",
            "sell_qty",
            "sell_amt",
            "sell_fee",
            "sell_tax",
            "fee",
            "tax",
            "buy_fee",
            "buy_tax",
            "realized",
        }
        if any(
            trade_delta_summary[field] != daily_delta_summary[field]
            for field in comparable_fields
        ):
            raise KisDomesticFunctionalTruthBlocked("profit TR summary deltas disagree")
        if (buy_qty, buy_amt, sell_qty, sell_amt) != (
            trade_delta_summary["buy_qty"],
            trade_delta_summary["buy_amt"],
            trade_delta_summary["sell_qty"],
            trade_delta_summary["sell_amt"],
        ):
            raise KisDomesticFunctionalTruthBlocked("orders and exact cost truth disagree")
        day_amount_deltas = {
            key: terminal_day_amounts[key] - baseline_day_amounts[key]
            for key in terminal_day_amounts
        }
        if any(value < 0 for value in day_amount_deltas.values()):
            raise KisDomesticFunctionalTruthBlocked(
                "same-day balance amount moved backwards after activation"
            )
        if day_amount_deltas != {
            "thdt_buy_amt": buy_amt,
            "thdt_sll_amt": sell_amt,
            "thdt_tlex_amt": trade_delta_summary["fee"]
            + trade_delta_summary["tax"]
            + trade_delta_totals["loan_int"],
        }:
            raise KisDomesticFunctionalTruthBlocked(
                "same-day balance amount delta disagrees with exact owned costs"
            )
        expected_final = baseline + buy_qty - sell_qty
        if final_quantity != expected_final:
            raise KisDomesticFunctionalTruthBlocked("target PDNO balance delta does not reconcile")
        loan_interest = trade_delta_totals["loan_int"]
        assert isinstance(loan_interest, Decimal)
        computed_realized = (
            trade_delta_summary["sell_amt"]
            - trade_delta_summary["sell_fee"]
            - trade_delta_summary["sell_tax"]
            - trade_delta_summary["buy_amt"]
            - trade_delta_summary["buy_fee"]
            - trade_delta_summary["buy_tax"]
            - loan_interest
        )
        if computed_realized != trade_delta_summary["realized"]:
            raise KisDomesticFunctionalTruthBlocked("official realized P/L does not equal exact costs")
        expected_cash = baseline_cash + computed_realized
        if current_cash > expected_cash:
            raise KisDomesticFunctionalTruthBlocked(
                "unexpected favorable account cash delta is external/unknown activity"
            )
        adverse_cash_delta = max(Decimal("0"), expected_cash - current_cash)
        cashflow_loss = max(Decimal("0"), -computed_realized)
        official_loss = max(Decimal("0"), -trade_delta_summary["realized"])
        owner_loss = max(cashflow_loss, official_loss, adverse_cash_delta)
        return {
            "causalProjectionHash": _hash(_stable_causal_projection(capture)),
            "officialOrderKeys": sorted(
                _identity_display(tuple(json.loads(key))) for key in actual_keys
            ),
            "officialOrderSequence": [
                _identity_display(
                    _official_identity(
                        row["ord_dt"],
                        row["ord_gno_brno"],
                        row["odno"],
                        label="official order sequence",
                    )
                )
                for row in orders
            ],
            "baselineQuantity": format(baseline, "f"),
            "finalQuantity": format(final_quantity, "f"),
            "orderableQuantity": format(orderable_quantity, "f"),
            "buyFilledQuantity": format(buy_qty, "f"),
            "buyFilledAmountKrw": format(buy_amt, "f"),
            "sellFilledQuantity": format(sell_qty, "f"),
            "sellFilledAmountKrw": format(sell_amt, "f"),
            "totalFeeKrw": format(trade_delta_summary["fee"], "f"),
            "totalTaxKrw": format(trade_delta_summary["tax"], "f"),
            "loanInterestKrw": format(loan_interest, "f"),
            "periodTradeProfitSummaryLoanInterestPresent": trade_delta_summary["loan_int"] is not None,
            "periodProfitSummaryLoanInterestVerified": daily_delta_summary["loan_int"] == loan_interest,
            "realizedProfitLossKrw": format(trade_delta_summary["realized"], "f"),
            "preactivationCumulativeCostsSubtracted": True,
            "baselineCashKrw": format(baseline_cash, "f"),
            "finalCashKrw": format(current_cash, "f"),
            "expectedCashKrw": format(expected_cash, "f"),
            "adverseCashDeltaKrw": format(adverse_cash_delta, "f"),
            "ownerLossKrw": format(owner_loss, "f"),
            "ownerLossLimitSatisfied": owner_loss < OWNER_LOSS_LIMIT_KRW,
            "grossLimitSatisfied": max(buy_amt, sell_amt) <= MAX_GROSS_KRW,
            "tradingDayOpen": True,
            "paginationComplete": True,
            "stableRepeatedReads": True,
            "allTruthMethodsGetOnly": True,
        }

    def _profit_row_totals(self, rows: Sequence[Mapping[str, Any]], *, detailed: bool, ymd: str) -> dict[str, Decimal]:
        totals = {key: Decimal("0") for key in ("buy_qty", "buy_amt", "sell_qty", "sell_amt", "fee", "tax", "loan_int", "realized")}
        for index, row in enumerate(rows):
            label = "periodTradeProfit" if detailed else "periodProfit"
            if row.get("trad_dt") != ymd:
                raise KisDomesticFunctionalTruthBlocked(f"{label}[{index}] date mismatch")
            if detailed and row.get("pdno") != PDNO:
                raise KisDomesticFunctionalTruthBlocked(f"{label}[{index}] PDNO mismatch")
            keys = {
                "buy_qty": "buy_qty" if detailed else "buy_qty1",
                "buy_amt": "buy_amt",
                "sell_qty": "sll_qty" if detailed else "sll_qty1",
                "sell_amt": "sll_amt",
                "fee": "fee",
                "tax": "tl_tax",
                "loan_int": "loan_int",
                "realized": "rlzt_pfls",
            }
            for total_key, row_key in keys.items():
                totals[total_key] += _decimal(
                    row.get(row_key),
                    f"{label}[{index}].{row_key}",
                    nonnegative=total_key != "realized",
                )
        return totals

    def _profit_summary(
        self,
        row: Mapping[str, Any],
        *,
        buy_qty_key: str,
        label: str,
        require_loan_interest: bool,
    ) -> dict[str, Decimal | None]:
        fields = {
            "sell_qty": "sll_qty_smtl",
            "sell_amt": "sll_tr_amt_smtl",
            "sell_fee": "sll_fee_smtl",
            "sell_tax": "sll_tltx_smtl",
            "sell_settlement": "sll_excc_amt_smtl",
            "buy_qty": buy_qty_key,
            "buy_amt": "buy_tr_amt_smtl",
            "buy_fee": "buy_fee_smtl",
            "buy_tax": "buy_tax_smtl",
            "buy_settlement": "buy_excc_amt_smtl",
            "total_qty": "tot_qty",
            "total_amt": "tot_tr_amt",
            "fee": "tot_fee",
            "tax": "tot_tltx",
            "total_settlement": "tot_excc_amt",
            "realized": "tot_rlzt_pfls",
        }
        if require_loan_interest:
            fields["loan_int"] = "loan_int"
        parsed = {
            key: _decimal(row.get(source), f"{label}.{source}", nonnegative=key != "realized")
            for key, source in fields.items()
        }
        if parsed["total_qty"] != parsed["buy_qty"] + parsed["sell_qty"]:
            raise KisDomesticFunctionalTruthBlocked(f"{label} total quantity mismatch")
        if parsed["total_amt"] != parsed["buy_amt"] + parsed["sell_amt"]:
            raise KisDomesticFunctionalTruthBlocked(f"{label} total amount mismatch")
        if parsed["fee"] != parsed["buy_fee"] + parsed["sell_fee"]:
            raise KisDomesticFunctionalTruthBlocked(f"{label} total fee mismatch")
        if parsed["tax"] != parsed["buy_tax"] + parsed["sell_tax"]:
            raise KisDomesticFunctionalTruthBlocked(f"{label} total tax mismatch")
        if parsed["sell_settlement"] != parsed["sell_amt"] - parsed["sell_fee"] - parsed["sell_tax"]:
            raise KisDomesticFunctionalTruthBlocked(f"{label} sell settlement mismatch")
        if parsed["buy_settlement"] != parsed["buy_amt"] + parsed["buy_fee"] + parsed["buy_tax"]:
            raise KisDomesticFunctionalTruthBlocked(f"{label} buy settlement mismatch")
        if parsed["total_settlement"] != parsed["sell_settlement"] + parsed["buy_settlement"]:
            raise KisDomesticFunctionalTruthBlocked(f"{label} total settlement mismatch")
        return {
            "buy_qty": parsed["buy_qty"],
            "buy_amt": parsed["buy_amt"],
            "sell_qty": parsed["sell_qty"],
            "sell_amt": parsed["sell_amt"],
            "sell_fee": parsed["sell_fee"],
            "sell_tax": parsed["sell_tax"],
            "fee": parsed["fee"],
            "tax": parsed["tax"],
            "buy_fee": parsed["buy_fee"],
            "buy_tax": parsed["buy_tax"],
            "loan_int": parsed.get("loan_int"),
            "realized": parsed["realized"],
        }


def production_entrypoint_status() -> dict[str, Any]:
    return {
        "available": False,
        "networkAvailable": KIS_DOMESTIC_FUNCTIONAL_TRUTH_NETWORK_AVAILABLE,
        "mutationAvailable": KIS_DOMESTIC_FUNCTIONAL_TRUTH_MUTATION_AVAILABLE,
        "method": "GET_ONLY",
        "origin": LIVE_ORIGIN,
        "requiredTrIds": [spec.tr_id for spec in _SPECS],
        "rawArchiveDurableWriterAvailable": False,
        "durableOwnedClaimReaderAvailable": False,
        "naturalBreakoutRawWindowRecomputeAvailable": False,
        "activationToTerminal7200VerifierAvailable": False,
        "capabilityResetFinalFlatVerifierAvailable": False,
            "sameDaySignedGetSchemaObserved": False,
            "accountWideWorkingZeroSemanticsObserved": False,
        "terminalClassificationAvailable": False,
        "reason": "OFFLINE_MOCK_PARSER_ONLY_DURABLE_AND_SIGNED_GET_GATES_MISSING",
    }
