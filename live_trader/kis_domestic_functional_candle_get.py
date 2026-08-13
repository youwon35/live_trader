from __future__ import annotations

"""Fail-closed verifier for the official KIS domestic minute-chart GET.

Primary-source contract (reviewed 2026-08-14):

* KIS official OpenAPI sample, ``inquire_time_dailychartprice.py``:
  https://github.com/koreainvestment/open-trading-api/blob/main/examples_llm/domestic_stock/inquire_time_dailychartprice/inquire_time_dailychartprice.py
  It fixes ``FHKST03010230``, the endpoint and six query fields, documents at
  most 120 rows per call, and describes historical minute retention (not a
  native five-minute interval).
* KIS official field-check sample, ``chk_inquire_time_dailychartprice.py``:
  https://github.com/koreainvestment/open-trading-api/blob/main/examples_llm/domestic_stock/inquire_time_dailychartprice/chk_inquire_time_dailychartprice.py
  It names the minute-row date/time/OHLC/volume fields consumed below.
* KIS official same-day sample, ``inquire_time_itemchartprice.py``:
  https://github.com/koreainvestment/open-trading-api/blob/main/examples_llm/domestic_stock/inquire_time_itemchartprice/inquire_time_itemchartprice.py
  It warns that the newest minute's ``cntg_vol`` can carry the prior minute's
  value until the first execution and exposes no finalized flag.

The primary sources do *not* promise native 5m bars, an exchange/server time,
an explicit finalized bit, or a continuation contract for this endpoint.
Accordingly this module can independently normalize one signed <=120-row GET
into an 11x5 diagnostic OHLC window, but it can never authorize an order or a
functional-wiring PASS.  The H0STCNT0 comparable hashes deliberately cover
only timestamp+OHLC; equality still needs an independently verified websocket
archive and does not prove upstream packet completeness.
"""

import base64
import hashlib
import hmac
import json
import math
import re
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import urlencode
from zoneinfo import ZoneInfo


KIS_DOMESTIC_FUNCTIONAL_CANDLE_GET_PRODUCTION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_CANDLE_GET_NETWORK_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_CANDLE_GET_ORDER_AUTHORITY_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_CANDLE_GET_PROMOTION_AVAILABLE = False

ROUTE = "KIS_KR_LIVE_CONTINUOUS"
PDNO = "010140"
LIVE_ORIGIN = "https://openapi.koreainvestment.com:9443"
ENDPOINT = "/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice"
TR_ID = "FHKST03010230"
INTERVAL = "5m"
MINUTE_ROWS_PER_WINDOW = 55
BAR_COUNT = 11
OFFICIAL_MAX_ROWS_PER_CALL = 120
GET_PACING_SECONDS = Decimal("2.1")
MAX_LOCAL_CAPTURE_AGE_SECONDS = Decimal("5")

_KST = ZoneInfo("Asia/Seoul")
_SHA256 = re.compile(r"^[0-9a-f]{64}$", flags=re.ASCII)
_CAPTURE_ID = re.compile(r"^kis-candle-capture-[0-9a-f]{32}$", flags=re.ASCII)
_DATE = re.compile(r"^[0-9]{8}$", flags=re.ASCII)
_TIME = re.compile(r"^[0-9]{6}$", flags=re.ASCII)
_UNSIGNED_NUMBER = re.compile(r"^[0-9]+(?:\.[0-9]+)?$", flags=re.ASCII)

_AUTH_ATTESTATION_DOMAIN = b"kis-domestic-functional-authenticated-get/v1\x00"
_CAPTURE_DOMAIN = b"kis-domestic-functional-capture/v1\x00"
_BUNDLE_DOMAIN = b"kis-domestic-functional-candle-bundle/v1\x00"

_QUERY_ORDER = (
    "FID_COND_MRKT_DIV_CODE",
    "FID_INPUT_ISCD",
    "FID_INPUT_HOUR_1",
    "FID_INPUT_DATE_1",
    "FID_PW_DATA_INCU_YN",
    "FID_FAKE_TICK_INCU_YN",
)
_OUTPUT2_REQUIRED = frozenset(
    {
        "stck_bsop_date",
        "stck_cntg_hour",
        "stck_prpr",
        "stck_oprc",
        "stck_hgpr",
        "stck_lwpr",
        "cntg_vol",
        "acml_tr_pbmn",
    }
)
_BODY_KEYS = frozenset({"rt_cd", "msg_cd", "msg1", "output1", "output2"})
_ATTESTATION_KEYS = frozenset(
    {
        "schemaVersion",
        "environment",
        "origin",
        "custtype",
        "accountFingerprint",
        "credentialConfigurationHash",
        "authenticated",
        "allowedMethods",
        "signatureHash",
    }
)
_AUDIT_KEYS = frozenset(
    {
        "schemaVersion",
        "origin",
        "accountFingerprint",
        "credentialConfigurationHash",
        "serverAuthorityKeyIdHash",
        "serverAuthorityRestartVerifiable",
        "authenticationTokenReadCount",
        "oauthTokenIssuanceMayUsePost",
        "authenticationOauthPostDispatchCount",
        "authenticationOauthPostCountComplete",
        "authenticationOauthPostAuthOnly",
        "authenticationOauthHiddenRetryCount",
        "authenticationOauthRedirectFollowCount",
        "officialGetDispatchCount",
        "physicalOfficialGetAttemptCount",
        "physicalOfficialGetAttemptCountComplete",
        "hiddenGetRetryCount",
        "redirectFollowCount",
        "tradingPostDeleteDispatchCount",
        "minimumRequestIntervalSeconds",
        "pacingWaitSeconds",
        "dispatches",
        "signatureHash",
    }
)
_DISPATCH_KEYS_WITH_STATUS = frozenset(
    {
        "ordinal",
        "monotonicStartedAt",
        "endpoint",
        "trId",
        "continuation",
        "accountFingerprint",
        "queryHmacSha256",
        "method",
        "bodyAbsent",
        "physicalAttemptCount",
        "physicalAttemptCountComplete",
        "effectiveUrlExact",
        "redirectFollowed",
        "transportOutcome",
        "statusCode",
    }
)
_PAGE_KEYS = frozenset(
    {
        "schemaVersion",
        "captureId",
        "method",
        "origin",
        "endpoint",
        "trId",
        "queryItems",
        "publicRequestHeaders",
        "requestContinuation",
        "responseContinuation",
        "statusCode",
        "observedAt",
        "officialServerTime",
        "rawRequestBytesBase64",
        "rawRequestSha256",
        "rawResponseBytesBase64",
        "rawResponseSha256",
        "body",
        "bodyHash",
        "dispatchOrdinal",
        "queryHmacSha256",
        "serverAuthoritySignature",
    }
)
_BUNDLE_KEYS = frozenset(
    {
        "schemaVersion",
        "route",
        "pdno",
        "origin",
        "endpoint",
        "trId",
        "intervalRequested",
        "requestedFinalizedBarCount",
        "tradingDate",
        "requestedThroughTime",
        "accountFingerprint",
        "credentialConfigurationHash",
        "authorityKeyIdHash",
        "authenticatedGetAttestation",
        "signedClientAuditBefore",
        "signedClientAuditAfter",
        "pages",
    }
)


class KisDomesticFunctionalCandleGetBlocked(RuntimeError):
    pass


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _bytes_hash(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _signature(key: bytes, domain: bytes, value: Mapping[str, Any]) -> str:
    return hmac.new(key, domain + _canonical(value), hashlib.sha256).hexdigest()


def _utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _utc(raw: object, field: str) -> datetime:
    if type(raw) is not str or not raw.endswith("Z"):
        raise KisDomesticFunctionalCandleGetBlocked(f"{field} is not canonical UTC")
    try:
        parsed = datetime.fromisoformat(raw[:-1] + "+00:00")
    except ValueError:
        raise KisDomesticFunctionalCandleGetBlocked(
            f"{field} is not canonical UTC"
        ) from None
    if parsed.tzinfo is None or _utc_text(parsed) != raw:
        raise KisDomesticFunctionalCandleGetBlocked(f"{field} is not canonical UTC")
    return parsed.astimezone(timezone.utc)


def _trusted_now(clock: Callable[[], datetime]) -> datetime:
    now = clock()
    if type(now) is not datetime or now.tzinfo is None or now.utcoffset() is None:
        raise KisDomesticFunctionalCandleGetBlocked(
            "trusted clock is not an aware datetime"
        )
    converted = now.astimezone(timezone.utc)
    if not math.isfinite(converted.timestamp()):
        raise KisDomesticFunctionalCandleGetBlocked("trusted clock is invalid")
    return converted


def _decimal(raw: object, field: str, *, positive: bool = False) -> Decimal:
    if type(raw) is not str:
        raise KisDomesticFunctionalCandleGetBlocked(f"{field} is not a string")
    stripped = raw.strip()
    if not stripped or not _UNSIGNED_NUMBER.fullmatch(stripped):
        raise KisDomesticFunctionalCandleGetBlocked(f"{field} is malformed")
    try:
        value = Decimal(stripped)
    except InvalidOperation:
        raise KisDomesticFunctionalCandleGetBlocked(f"{field} is malformed") from None
    if not value.is_finite() or value < 0 or (positive and value <= 0):
        raise KisDomesticFunctionalCandleGetBlocked(f"{field} is out of range")
    return value


def _decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _decode_base64(raw: object, field: str) -> bytes:
    if type(raw) is not str or not raw:
        raise KisDomesticFunctionalCandleGetBlocked(f"{field} is missing")
    try:
        decoded = base64.b64decode(raw, validate=True)
    except (ValueError, TypeError):
        raise KisDomesticFunctionalCandleGetBlocked(f"{field} is invalid") from None
    if not decoded:
        raise KisDomesticFunctionalCandleGetBlocked(f"{field} is empty")
    return decoded


def _json_object_without_duplicate_keys(
    pairs: list[tuple[str, Any]],
) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise KisDomesticFunctionalCandleGetBlocked(
                "raw response JSON contains a duplicate object key"
            )
        value[key] = item
    return value


def _query_items(trading_date: str, through_time: str) -> list[list[str]]:
    return [
        ["FID_COND_MRKT_DIV_CODE", "J"],
        ["FID_INPUT_ISCD", PDNO],
        ["FID_INPUT_HOUR_1", through_time],
        ["FID_INPUT_DATE_1", trading_date],
        ["FID_PW_DATA_INCU_YN", "N"],
        ["FID_FAKE_TICK_INCU_YN", ""],
    ]


def canonical_public_request_bytes(
    *, trading_date: str, through_time: str
) -> bytes:
    """Return the exact secret-free request envelope archived by this protocol.

    This is deliberately not represented as the complete physical HTTP wire
    request because Authorization/app secrets must not be archived.  The live
    physical-wire capture seam is therefore an explicit production blocker.
    """

    items = _query_items(trading_date, through_time)
    query = urlencode([(key, value) for key, value in items], doseq=False)
    return _canonical(
        {
            "schemaVersion": "kis-domestic-functional-public-get-request/v1",
            "method": "GET",
            "origin": LIVE_ORIGIN,
            "endpoint": ENDPOINT,
            "requestTarget": ENDPOINT + "?" + query,
            "trId": TR_ID,
            "queryItems": items,
            "publicRequestHeaders": {
                "custtype": "P",
                "tr_id": TR_ID,
                "tr_cont": "",
            },
            "bodyAbsent": True,
        }
    )


def _verify_hmac(
    key: bytes,
    domain: bytes,
    body: Mapping[str, Any],
    candidate: object,
    field: str,
) -> None:
    if type(candidate) is not str or not _SHA256.fullmatch(candidate):
        raise KisDomesticFunctionalCandleGetBlocked(f"{field} is invalid")
    if not hmac.compare_digest(candidate, _signature(key, domain, body)):
        raise KisDomesticFunctionalCandleGetBlocked(f"{field} mismatch")


class KisDomesticFunctionalCandleGetVerifier:
    """Offline exact verifier; it owns no sender, token reader, or mutation API."""

    def __init__(
        self,
        *,
        server_authority_key: bytes,
        server_authority_key_id_hash: str,
        account_fingerprint: str,
        credential_configuration_hash: str,
        trusted_clock: Callable[[], datetime],
    ) -> None:
        if type(server_authority_key) is not bytes or len(server_authority_key) < 32:
            raise KisDomesticFunctionalCandleGetBlocked(
                "server authority key is unavailable"
            )
        for name, value in (
            ("server authority key id", server_authority_key_id_hash),
            ("account fingerprint", account_fingerprint),
            ("credential configuration hash", credential_configuration_hash),
        ):
            if type(value) is not str or not _SHA256.fullmatch(value):
                raise KisDomesticFunctionalCandleGetBlocked(f"{name} is invalid")
        if not callable(trusted_clock):
            raise KisDomesticFunctionalCandleGetBlocked("trusted clock is unavailable")
        self._key = bytes(server_authority_key)
        self._key_id_hash = server_authority_key_id_hash
        self.account_fingerprint = account_fingerprint
        self.credential_configuration_hash = credential_configuration_hash
        self._clock = trusted_clock

    def _verify_attestation(self, candidate: object) -> None:
        if not isinstance(candidate, Mapping) or set(candidate) != _ATTESTATION_KEYS:
            raise KisDomesticFunctionalCandleGetBlocked(
                "authenticated GET attestation fields are not exact"
            )
        signed = dict(candidate)
        signature = signed.pop("signatureHash")
        _verify_hmac(
            self._key,
            _AUTH_ATTESTATION_DOMAIN,
            signed,
            signature,
            "authenticated GET attestation signature",
        )
        expected = {
            "schemaVersion": "kis-authenticated-get-attestation/v1",
            "environment": "KIS_LIVE",
            "origin": LIVE_ORIGIN,
            "custtype": "P",
            "accountFingerprint": self.account_fingerprint,
            "credentialConfigurationHash": self.credential_configuration_hash,
            "authenticated": True,
            "allowedMethods": ["GET"],
        }
        if signed != expected:
            raise KisDomesticFunctionalCandleGetBlocked(
                "authenticated GET attestation binding mismatch"
            )

    def _verify_audit(self, candidate: object) -> dict[str, Any]:
        if not isinstance(candidate, Mapping) or set(candidate) != _AUDIT_KEYS:
            raise KisDomesticFunctionalCandleGetBlocked(
                "signed GET audit fields are not exact"
            )
        signed = dict(candidate)
        signature = signed.pop("signatureHash")
        _verify_hmac(
            self._key,
            _CAPTURE_DOMAIN,
            signed,
            signature,
            "signed GET audit signature",
        )
        expected = {
            "schemaVersion": "kis-domestic-functional-get-audit/v1",
            "origin": LIVE_ORIGIN,
            "accountFingerprint": self.account_fingerprint,
            "credentialConfigurationHash": self.credential_configuration_hash,
            "serverAuthorityKeyIdHash": self._key_id_hash,
            "serverAuthorityRestartVerifiable": True,
            "oauthTokenIssuanceMayUsePost": True,
            "authenticationOauthPostCountComplete": True,
            "authenticationOauthPostAuthOnly": True,
            "physicalOfficialGetAttemptCountComplete": True,
            "hiddenGetRetryCount": 0,
            "redirectFollowCount": 0,
            "authenticationOauthHiddenRetryCount": 0,
            "authenticationOauthRedirectFollowCount": 0,
            "tradingPostDeleteDispatchCount": 0,
        }
        for field, wanted in expected.items():
            if type(signed.get(field)) is not type(wanted) or signed.get(field) != wanted:
                raise KisDomesticFunctionalCandleGetBlocked(
                    f"signed GET audit {field} mismatch"
                )
        for field in (
            "authenticationTokenReadCount",
            "authenticationOauthPostDispatchCount",
            "officialGetDispatchCount",
            "physicalOfficialGetAttemptCount",
        ):
            if type(signed.get(field)) is not int or signed[field] < 0:
                raise KisDomesticFunctionalCandleGetBlocked(
                    f"signed GET audit {field} is invalid"
                )
        interval = signed.get("minimumRequestIntervalSeconds")
        if (
            type(interval) not in (int, float)
            or isinstance(interval, bool)
            or not math.isfinite(float(interval))
            or Decimal(str(interval)) != GET_PACING_SECONDS
        ):
            raise KisDomesticFunctionalCandleGetBlocked(
                "signed GET audit pacing policy mismatch"
            )
        wait = signed.get("pacingWaitSeconds")
        if (
            type(wait) not in (int, float)
            or isinstance(wait, bool)
            or not math.isfinite(float(wait))
            or float(wait) < 0
        ):
            raise KisDomesticFunctionalCandleGetBlocked(
                "signed GET audit pacing wait is invalid"
            )
        dispatches = signed.get("dispatches")
        if type(dispatches) is not list or len(dispatches) != signed[
            "officialGetDispatchCount"
        ]:
            raise KisDomesticFunctionalCandleGetBlocked(
                "signed GET audit dispatch count mismatch"
            )
        previous: Decimal | None = None
        for ordinal, row in enumerate(dispatches, start=1):
            if not isinstance(row, Mapping) or set(row) != _DISPATCH_KEYS_WITH_STATUS:
                raise KisDomesticFunctionalCandleGetBlocked(
                    "signed GET dispatch fields are not exact"
                )
            if type(row.get("ordinal")) is not int or row["ordinal"] != ordinal:
                raise KisDomesticFunctionalCandleGetBlocked(
                    "signed GET dispatch ordinal mismatch"
                )
            started = row.get("monotonicStartedAt")
            if (
                type(started) not in (int, float)
                or isinstance(started, bool)
                or not math.isfinite(float(started))
                or float(started) < 0
            ):
                raise KisDomesticFunctionalCandleGetBlocked(
                    "signed GET dispatch monotonic time is invalid"
                )
            current = Decimal(str(started))
            if previous is not None and current - previous < GET_PACING_SECONDS:
                raise KisDomesticFunctionalCandleGetBlocked(
                    "signed GET physical pacing interval is too short"
                )
            previous = current
        if signed["physicalOfficialGetAttemptCount"] != signed[
            "officialGetDispatchCount"
        ]:
            raise KisDomesticFunctionalCandleGetBlocked(
                "signed GET physical attempt count mismatch"
            )
        return signed

    def _verify_page(
        self,
        candidate: object,
        *,
        trading_date: str,
        through_time: str,
        expected_ordinal: int,
    ) -> tuple[dict[str, Any], datetime, list[Mapping[str, Any]]]:
        if not isinstance(candidate, Mapping) or set(candidate) != _PAGE_KEYS:
            raise KisDomesticFunctionalCandleGetBlocked(
                "candle page fields are not exact"
            )
        signed = dict(candidate)
        signature = signed.pop("serverAuthoritySignature")
        _verify_hmac(
            self._key,
            _CAPTURE_DOMAIN,
            signed,
            signature,
            "candle page signature",
        )
        query_items = _query_items(trading_date, through_time)
        exact = {
            "schemaVersion": "kis-domestic-functional-candle-get-page/v1",
            "method": "GET",
            "origin": LIVE_ORIGIN,
            "endpoint": ENDPOINT,
            "trId": TR_ID,
            "queryItems": query_items,
            "publicRequestHeaders": {
                "custtype": "P",
                "tr_id": TR_ID,
                "tr_cont": "",
            },
            "requestContinuation": "",
            "responseContinuation": "",
            "statusCode": 200,
            "officialServerTime": "",
            "dispatchOrdinal": expected_ordinal,
        }
        for field, wanted in exact.items():
            if type(signed.get(field)) is not type(wanted) or signed.get(field) != wanted:
                raise KisDomesticFunctionalCandleGetBlocked(
                    f"candle page {field} mismatch"
                )
        if type(signed.get("captureId")) is not str or not _CAPTURE_ID.fullmatch(
            signed["captureId"]
        ):
            raise KisDomesticFunctionalCandleGetBlocked("candle capture id is invalid")
        observed = _utc(signed.get("observedAt"), "candlePage.observedAt")
        raw_request = _decode_base64(
            signed.get("rawRequestBytesBase64"), "rawRequestBytesBase64"
        )
        if raw_request != canonical_public_request_bytes(
            trading_date=trading_date, through_time=through_time
        ):
            raise KisDomesticFunctionalCandleGetBlocked(
                "raw public request bytes mismatch"
            )
        if signed.get("rawRequestSha256") != _bytes_hash(raw_request):
            raise KisDomesticFunctionalCandleGetBlocked("raw request hash mismatch")
        upper_request = raw_request.upper()
        for forbidden in (b"AUTHORIZATION", b"APPSECRET", b"ACCESS_TOKEN"):
            if forbidden in upper_request:
                raise KisDomesticFunctionalCandleGetBlocked(
                    "raw request archive contains a secret header"
                )
        raw_response = _decode_base64(
            signed.get("rawResponseBytesBase64"), "rawResponseBytesBase64"
        )
        if signed.get("rawResponseSha256") != _bytes_hash(raw_response):
            raise KisDomesticFunctionalCandleGetBlocked("raw response hash mismatch")
        try:
            parsed_body = json.loads(
                raw_response.decode("utf-8"),
                object_pairs_hook=_json_object_without_duplicate_keys,
            )
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise KisDomesticFunctionalCandleGetBlocked(
                "raw response bytes are not exact UTF-8 JSON"
            ) from None
        body = signed.get("body")
        if not isinstance(body, Mapping) or parsed_body != body:
            raise KisDomesticFunctionalCandleGetBlocked(
                "raw response bytes/body mismatch"
            )
        if signed.get("bodyHash") != _hash(body):
            raise KisDomesticFunctionalCandleGetBlocked("candle body hash mismatch")
        if set(body) != _BODY_KEYS or body.get("rt_cd") != "0":
            raise KisDomesticFunctionalCandleGetBlocked(
                "official minute response envelope is not exact"
            )
        if not isinstance(body.get("output1"), Mapping):
            raise KisDomesticFunctionalCandleGetBlocked(
                "official minute output1 is not an object"
            )
        output2 = body.get("output2")
        if type(output2) is not list or not (
            MINUTE_ROWS_PER_WINDOW <= len(output2) <= OFFICIAL_MAX_ROWS_PER_CALL
        ):
            raise KisDomesticFunctionalCandleGetBlocked(
                "official minute output2 cannot cover the exact window"
            )
        expected_query_hmac = _signature(
            self._key,
            _CAPTURE_DOMAIN,
            {
                "endpoint": ENDPOINT,
                "trId": TR_ID,
                "queryItems": query_items,
                "continuation": "",
                "accountFingerprint": self.account_fingerprint,
            },
        )
        if signed.get("queryHmacSha256") != expected_query_hmac:
            raise KisDomesticFunctionalCandleGetBlocked(
                "candle page query HMAC mismatch"
            )
        return signed, observed, output2

    def _normalize_minutes(
        self,
        rows: Sequence[Mapping[str, Any]],
        *,
        trading_date: str,
        through_time: str,
    ) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        seen: set[str] = set()
        for index, row in enumerate(rows):
            if not isinstance(row, Mapping) or not _OUTPUT2_REQUIRED.issubset(row):
                raise KisDomesticFunctionalCandleGetBlocked(
                    "official minute row required fields are missing"
                )
            date_raw = row.get("stck_bsop_date")
            time_raw = row.get("stck_cntg_hour")
            if (
                type(date_raw) is not str
                or not _DATE.fullmatch(date_raw)
                or date_raw != trading_date
                or type(time_raw) is not str
                or not _TIME.fullmatch(time_raw)
            ):
                raise KisDomesticFunctionalCandleGetBlocked(
                    "official minute row trade timestamp is invalid"
                )
            try:
                local = datetime.strptime(date_raw + time_raw, "%Y%m%d%H%M%S").replace(
                    tzinfo=_KST
                )
            except ValueError:
                raise KisDomesticFunctionalCandleGetBlocked(
                    "official minute row trade timestamp is invalid"
                ) from None
            if local.second != 0 or not (
                time(9, 0) <= local.timetz().replace(tzinfo=None) < time(15, 30)
            ):
                raise KisDomesticFunctionalCandleGetBlocked(
                    "official minute row is outside the XKRX regular minute grid"
                )
            key = date_raw + ":" + time_raw
            if key in seen:
                raise KisDomesticFunctionalCandleGetBlocked(
                    "official minute row timestamp is duplicated"
                )
            seen.add(key)
            open_price = _decimal(row.get("stck_oprc"), f"output2[{index}].stck_oprc", positive=True)
            high = _decimal(row.get("stck_hgpr"), f"output2[{index}].stck_hgpr", positive=True)
            low = _decimal(row.get("stck_lwpr"), f"output2[{index}].stck_lwpr", positive=True)
            close = _decimal(row.get("stck_prpr"), f"output2[{index}].stck_prpr", positive=True)
            volume = _decimal(row.get("cntg_vol"), f"output2[{index}].cntg_vol")
            _decimal(row.get("acml_tr_pbmn"), f"output2[{index}].acml_tr_pbmn")
            if not (low <= open_price <= high and low <= close <= high):
                raise KisDomesticFunctionalCandleGetBlocked(
                    "official minute row OHLC ordering is invalid"
                )
            raw_subset = {field: row[field] for field in sorted(_OUTPUT2_REQUIRED)}
            normalized.append(
                {
                    "tradeTime": _utc_text(local.astimezone(timezone.utc)),
                    "officialTradeDate": date_raw,
                    "officialTradeTime": time_raw,
                    "open": _decimal_text(open_price),
                    "high": _decimal_text(high),
                    "low": _decimal_text(low),
                    "close": _decimal_text(close),
                    "volumeDiagnostic": _decimal_text(volume),
                    "rawRequiredFieldsHash": _hash(raw_subset),
                }
            )
        normalized.sort(key=lambda row: row["tradeTime"])
        if normalized[-1]["officialTradeTime"] != through_time:
            raise KisDomesticFunctionalCandleGetBlocked(
                "official minute page does not end at the requested cursor"
            )
        selected = normalized[-MINUTE_ROWS_PER_WINDOW:]
        first = _utc(selected[0]["tradeTime"], "minute.tradeTime")
        if first.astimezone(_KST).minute % 5 != 0:
            raise KisDomesticFunctionalCandleGetBlocked(
                "diagnostic five-minute window is not bucket aligned"
            )
        for previous, current in zip(selected, selected[1:]):
            if _utc(current["tradeTime"], "minute.tradeTime") - _utc(
                previous["tradeTime"], "minute.tradeTime"
            ) != timedelta(minutes=1):
                raise KisDomesticFunctionalCandleGetBlocked(
                    "official minute window is not contiguous"
                )
        return selected

    def _aggregate_bars(self, minutes: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        bars: list[dict[str, Any]] = []
        for offset in range(0, MINUTE_ROWS_PER_WINDOW, 5):
            group = list(minutes[offset : offset + 5])
            opened = _utc(group[0]["tradeTime"], "minute.tradeTime")
            closed = opened + timedelta(minutes=5)
            if any(
                _utc(row["tradeTime"], "minute.tradeTime")
                != opened + timedelta(minutes=index)
                for index, row in enumerate(group)
            ):
                raise KisDomesticFunctionalCandleGetBlocked(
                    "diagnostic five-minute group is not contiguous"
                )
            open_price = Decimal(group[0]["open"])
            high = max(Decimal(row["high"]) for row in group)
            low = min(Decimal(row["low"]) for row in group)
            close = Decimal(group[-1]["close"])
            volume = sum(
                (Decimal(row["volumeDiagnostic"]) for row in group), Decimal("0")
            )
            comparable = {
                "schemaVersion": "kis-h0stcnt0-rest-ohlc-link/v1",
                "pdno": PDNO,
                "openAt": _utc_text(opened),
                "closeAt": _utc_text(closed),
                "open": _decimal_text(open_price),
                "high": _decimal_text(high),
                "low": _decimal_text(low),
                "close": _decimal_text(close),
            }
            body = {
                **comparable,
                "source": "KIS_OFFICIAL_1M_GET_DIAGNOSTIC_AGGREGATION",
                "volumeDiagnostic": _decimal_text(volume),
                "officialMinuteTradeTimes": [row["tradeTime"] for row in group],
                "officialMinuteRequiredFieldHashes": [
                    row["rawRequiredFieldsHash"] for row in group
                ],
                "h0stcnt0ComparableHash": _hash(comparable),
            }
            bars.append({**body, "diagnosticBarHash": _hash(body)})
        if len(bars) != BAR_COUNT:
            raise KisDomesticFunctionalCandleGetBlocked(
                "diagnostic five-minute bar count mismatch"
            )
        return bars

    def verify(self, envelope: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(envelope, Mapping) or set(envelope) != {
            "body",
            "bundleHash",
            "signature",
        }:
            raise KisDomesticFunctionalCandleGetBlocked(
                "candle bundle envelope is not exact"
            )
        body = envelope.get("body")
        if not isinstance(body, Mapping) or set(body) != _BUNDLE_KEYS:
            raise KisDomesticFunctionalCandleGetBlocked(
                "candle bundle fields are not exact"
            )
        body = dict(body)
        if envelope.get("bundleHash") != _hash(body):
            raise KisDomesticFunctionalCandleGetBlocked("candle bundle hash mismatch")
        _verify_hmac(
            self._key,
            _BUNDLE_DOMAIN,
            body,
            envelope.get("signature"),
            "candle bundle signature",
        )
        exact = {
            "schemaVersion": "kis-domestic-functional-candle-get-bundle/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "origin": LIVE_ORIGIN,
            "endpoint": ENDPOINT,
            "trId": TR_ID,
            "intervalRequested": INTERVAL,
            "requestedFinalizedBarCount": BAR_COUNT,
            "accountFingerprint": self.account_fingerprint,
            "credentialConfigurationHash": self.credential_configuration_hash,
            "authorityKeyIdHash": self._key_id_hash,
        }
        for field, wanted in exact.items():
            if type(body.get(field)) is not type(wanted) or body.get(field) != wanted:
                raise KisDomesticFunctionalCandleGetBlocked(
                    f"candle bundle {field} mismatch"
                )
        trading_date = body.get("tradingDate")
        through_time = body.get("requestedThroughTime")
        if type(trading_date) is not str or not _DATE.fullmatch(trading_date):
            raise KisDomesticFunctionalCandleGetBlocked("trading date is invalid")
        if type(through_time) is not str or not _TIME.fullmatch(through_time):
            raise KisDomesticFunctionalCandleGetBlocked("requested cursor time is invalid")
        self._verify_attestation(body.get("authenticatedGetAttestation"))
        before = self._verify_audit(body.get("signedClientAuditBefore"))
        after = self._verify_audit(body.get("signedClientAuditAfter"))
        pages = body.get("pages")
        # The official sample does not document a continuation/cursor protocol.
        # One <=120-row response is sufficient for 55 rows; extra pages are not
        # silently blessed as official pagination.
        if type(pages) is not list or len(pages) != 1:
            raise KisDomesticFunctionalCandleGetBlocked(
                "official minute endpoint pagination is undocumented"
            )
        expected_ordinal = before["officialGetDispatchCount"] + 1
        page, observed, rows = self._verify_page(
            pages[0],
            trading_date=trading_date,
            through_time=through_time,
            expected_ordinal=expected_ordinal,
        )
        for field in (
            "authenticationTokenReadCount",
            "officialGetDispatchCount",
            "physicalOfficialGetAttemptCount",
        ):
            if after[field] != before[field] + 1:
                raise KisDomesticFunctionalCandleGetBlocked(
                    "signed GET audit delta is not exactly one"
                )
        oauth_delta = (
            after["authenticationOauthPostDispatchCount"]
            - before["authenticationOauthPostDispatchCount"]
        )
        if oauth_delta not in (0, 1):
            raise KisDomesticFunctionalCandleGetBlocked(
                "signed GET OAuth audit delta is invalid"
            )
        if (
            after["dispatches"][:-1] != before["dispatches"]
            or len(after["dispatches"]) != len(before["dispatches"]) + 1
            or after["minimumRequestIntervalSeconds"]
            != before["minimumRequestIntervalSeconds"]
            or after["pacingWaitSeconds"] < before["pacingWaitSeconds"]
        ):
            raise KisDomesticFunctionalCandleGetBlocked(
                "signed GET audit history mismatch"
            )
        dispatch = after["dispatches"][-1]
        dispatch_exact = {
            "ordinal": expected_ordinal,
            "endpoint": ENDPOINT,
            "trId": TR_ID,
            "continuation": "",
            "accountFingerprint": self.account_fingerprint,
            "queryHmacSha256": page["queryHmacSha256"],
            "method": "GET",
            "bodyAbsent": True,
            "physicalAttemptCount": 1,
            "physicalAttemptCountComplete": True,
            "effectiveUrlExact": True,
            "redirectFollowed": False,
            "transportOutcome": "RESPONSE",
            "statusCode": 200,
        }
        for field, wanted in dispatch_exact.items():
            if type(dispatch.get(field)) is not type(wanted) or dispatch.get(field) != wanted:
                raise KisDomesticFunctionalCandleGetBlocked(
                    f"signed GET causal dispatch {field} mismatch"
                )
        now = _trusted_now(self._clock)
        age = Decimal(str((now - observed).total_seconds()))
        if age < 0 or age > MAX_LOCAL_CAPTURE_AGE_SECONDS:
            raise KisDomesticFunctionalCandleGetBlocked(
                "local candle capture observation is stale or future"
            )
        if observed.astimezone(_KST).strftime("%Y%m%d") != trading_date:
            raise KisDomesticFunctionalCandleGetBlocked(
                "candle capture is not from the requested KST trading date"
            )
        minutes = self._normalize_minutes(
            rows, trading_date=trading_date, through_time=through_time
        )
        bars = self._aggregate_bars(minutes)
        last_close = _utc(bars[-1]["closeAt"], "bar.closeAt")
        if observed < last_close:
            raise KisDomesticFunctionalCandleGetBlocked(
                "local observation precedes the diagnostic window close"
            )
        if last_close.astimezone(_KST).timetz().replace(tzinfo=None) > time(13, 15):
            raise KisDomesticFunctionalCandleGetBlocked(
                "diagnostic window closes after the approved 13:15 boundary"
            )
        blockers = [
            "OFFICIAL_API_NATIVE_5M_INTERVAL_NOT_DOCUMENTED",
            "OFFICIAL_API_EXPLICIT_BAR_FINALIZATION_NOT_DOCUMENTED",
            "OFFICIAL_API_SERVER_TIME_NOT_AVAILABLE",
            "OFFICIAL_API_CONTINUATION_PAGINATION_NOT_DOCUMENTED",
            "NEWEST_MINUTE_VOLUME_MUTABILITY_DOCUMENTED_BY_KIS",
            "H0STCNT0_EQUIVALENCE_AND_UPSTREAM_COMPLETENESS_NOT_PROVEN",
            "SIGNED_GET_CLIENT_CANDLE_ROUTE_AND_RAW_WIRE_CAPTURE_NOT_WIRED",
            "PRODUCTION_AUTHORITY_REGISTRY_AND_DUAL_CLOCK_NOT_WIRED",
            "XKRX_OFFICIAL_TRADING_DAY_AND_SESSION_PROOF_NOT_JOINED",
            "SOURCE_ARM_GENERATION_OWNER_LINEAGE_NOT_JOINED",
            "INDEPENDENT_AUTHENTICATED_H0STCNT0_ARCHIVE_NOT_JOINED",
        ]
        result = {
            "schemaVersion": "kis-domestic-functional-candle-get-result/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "origin": LIVE_ORIGIN,
            "endpoint": ENDPOINT,
            "trId": TR_ID,
            "tradingDate": trading_date,
            "requestedInterval": INTERVAL,
            "officialNativeInterval": "1m",
            "accountFingerprint": self.account_fingerprint,
            "credentialConfigurationHash": self.credential_configuration_hash,
            "authorityKeyIdHash": self._key_id_hash,
            "captureId": page["captureId"],
            "dispatchOrdinal": page["dispatchOrdinal"],
            "queryHmacSha256": page["queryHmacSha256"],
            "signedClientAuditBeforeHash": _hash(body["signedClientAuditBefore"]),
            "signedClientAuditAfterHash": _hash(body["signedClientAuditAfter"]),
            "officialMinuteRowCount": len(rows),
            "selectedMinuteRowCount": len(minutes),
            "diagnosticBarCount": len(bars),
            "diagnosticBars": bars,
            "rawCaptureBundleHash": envelope["bundleHash"],
            "rawRequestSha256": page["rawRequestSha256"],
            "rawResponseSha256": page["rawResponseSha256"],
            "bodyHash": page["bodyHash"],
            "observedAt": page["observedAt"],
            "officialTradeTimeFirst": minutes[0]["tradeTime"],
            "officialTradeTimeLast": minutes[-1]["tradeTime"],
            "officialServerTime": "",
            "authenticatedSignedGetCaptureVerified": True,
            "singlePhysicalAttemptVerified": True,
            "hiddenRetryCount": 0,
            "redirectFollowCount": 0,
            "allRequiredRowsPresent": True,
            "singleDocumentedResponseCaptured": True,
            "officialPaginationCompletenessProven": False,
            "localObservationAfterWindowCloseDiagnostic": True,
            "diagnosticElevenBarWindowAvailable": True,
            "officialHistoricalMinuteRowsDocumented": True,
            "officialHistoricalFiveMinuteBarsGuaranteed": False,
            "officialNativeFiveMinuteBarsAvailable": False,
            "explicitFinalizationFlagAvailable": False,
            "officialServerTimeAvailable": False,
            "officialContinuationPaginationAvailable": False,
            "physicalWireRequestBytesAvailable": False,
            "productionAuthorityRegistryWired": False,
            "trustedDualClockLineageAvailable": False,
            "xkrxOfficialTradingDaySessionProofAvailable": False,
            "sourceArmGenerationOwnerLineageAvailable": False,
            "independentAuthenticatedH0stcnt0ArchiveAvailable": False,
            "h0stcnt0LinkAuthorityAvailable": False,
            "finalizedElevenBarAuthorityAvailable": False,
            "productionAvailable": False,
            "networkAvailable": False,
            "orderAuthorityAvailable": False,
            "promotionEligible": False,
            "blockedReasons": blockers,
        }
        return {**result, "resultHash": _hash(result)}


def production_entrypoint_status() -> dict[str, Any]:
    return {
        "schemaVersion": "kis-domestic-functional-candle-get-status/v1",
        "available": False,
        "productionAvailable": False,
        "networkAvailable": False,
        "orderAuthorityAvailable": False,
        "promotionAvailable": False,
        "route": ROUTE,
        "pdno": PDNO,
        "origin": LIVE_ORIGIN,
        "endpoint": ENDPOINT,
        "trId": TR_ID,
        "officialNativeFiveMinuteBarsAvailable": False,
        "explicitFinalizationFlagAvailable": False,
        "officialServerTimeAvailable": False,
        "officialContinuationPaginationAvailable": False,
        "signedGetClientCandleRouteWired": False,
        "productionAuthorityRegistryWired": False,
        "trustedDualClockLineageAvailable": False,
        "xkrxOfficialTradingDaySessionProofAvailable": False,
        "sourceArmGenerationOwnerLineageAvailable": False,
        "independentAuthenticatedH0stcnt0ArchiveAvailable": False,
        "reason": "OFFICIAL_KIS_API_DOCUMENTS_MINUTE_ROWS_ONLY_NO_AUTHORITATIVE_FINALIZED_5M_CONTRACT",
    }


__all__ = [
    "BAR_COUNT",
    "ENDPOINT",
    "INTERVAL",
    "KIS_DOMESTIC_FUNCTIONAL_CANDLE_GET_NETWORK_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_CANDLE_GET_ORDER_AUTHORITY_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_CANDLE_GET_PRODUCTION_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_CANDLE_GET_PROMOTION_AVAILABLE",
    "KisDomesticFunctionalCandleGetBlocked",
    "KisDomesticFunctionalCandleGetVerifier",
    "LIVE_ORIGIN",
    "PDNO",
    "ROUTE",
    "TR_ID",
    "canonical_public_request_bytes",
    "production_entrypoint_status",
]
