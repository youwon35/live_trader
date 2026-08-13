from __future__ import annotations

"""Official-read transport and strict truth assembly for Binance Spot tests.

Only Binance Spot GET endpoints are exposed here.  The ordinary live/smoke
router and every Futures, Margin, transfer, and withdrawal API remain outside
this module.  The client can safely retry only timestamp-error GETs; order and
cancel mutations are injected later through the durable claim boundary in
``binance_spot_continuous_functional`` and are never retried here.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import hmac
import json
import re
import threading
import time
from typing import Any, Callable, Mapping, Sequence
import urllib.parse

from .live_adapters import (
    BINANCE_BASE_URL,
    PreparedRequest,
    binance_timestamp_ms,
    env_value,
    missing_env,
    refresh_binance_time_offset,
    send_prepared_request,
    sign_binance_query,
)


BINANCE_SPOT_ACCOUNT_ENDPOINT = "/api/v3/account"
BINANCE_SPOT_OPEN_ORDERS_ENDPOINT = "/api/v3/openOrders"
BINANCE_SPOT_ALL_ORDERS_ENDPOINT = "/api/v3/allOrders"
BINANCE_SPOT_MY_TRADES_ENDPOINT = "/api/v3/myTrades"
BINANCE_SPOT_QUERY_ORDER_ENDPOINT = "/api/v3/order"
BINANCE_SPOT_EXCHANGE_INFO_ENDPOINT = "/api/v3/exchangeInfo"
BINANCE_SPOT_TICKER_PRICE_ENDPOINT = "/api/v3/ticker/price"
BINANCE_SPOT_AVG_PRICE_ENDPOINT = "/api/v3/avgPrice"
BINANCE_SPOT_KLINES_ENDPOINT = "/api/v3/klines"
BINANCE_SPOT_TIME_ENDPOINT = "/api/v3/time"

SIGNED_GET_ENDPOINTS = frozenset(
    {
        BINANCE_SPOT_ACCOUNT_ENDPOINT,
        BINANCE_SPOT_OPEN_ORDERS_ENDPOINT,
        BINANCE_SPOT_ALL_ORDERS_ENDPOINT,
        BINANCE_SPOT_MY_TRADES_ENDPOINT,
        BINANCE_SPOT_QUERY_ORDER_ENDPOINT,
    }
)
PUBLIC_GET_ENDPOINTS = frozenset(
    {
        BINANCE_SPOT_EXCHANGE_INFO_ENDPOINT,
        BINANCE_SPOT_TICKER_PRICE_ENDPOINT,
        BINANCE_SPOT_AVG_PRICE_ENDPOINT,
        BINANCE_SPOT_KLINES_ENDPOINT,
        BINANCE_SPOT_TIME_ENDPOINT,
    }
)
ALL_GET_ENDPOINTS = SIGNED_GET_ENDPOINTS | PUBLIC_GET_ENDPOINTS
TERMINAL_STATES = frozenset(
    {"FILLED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED"}
)
OPEN_STATES = frozenset({"NEW", "PENDING_NEW", "PARTIALLY_FILLED", "PENDING_CANCEL"})
MAX_PAGE_SIZE = 1000
MAX_HISTORY_PAGES = 100
MAX_TRUTH_AGE_SECONDS = 15.0
BINANCE_SPOT_PRODUCTION_ORIGIN = "https://api.binance.com"


class BinanceSpotTruthError(RuntimeError):
    """Official response set cannot prove a complete exact truth."""


def assert_binance_spot_production_origin(origin: object) -> str:
    """Pin credential-bearing production traffic to Binance Spot live."""

    normalized = _text(origin).rstrip("/")
    parsed = urllib.parse.urlsplit(normalized)
    if (
        normalized != BINANCE_SPOT_PRODUCTION_ORIGIN
        or parsed.scheme != "https"
        or parsed.hostname != "api.binance.com"
        or parsed.port not in {None, 443}
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path
        or parsed.query
        or parsed.fragment
    ):
        raise BinanceSpotTruthError(
            "production Binance Spot origin must be exact https://api.binance.com"
        )
    return normalized


def binance_api_key_fingerprint(api_key: object) -> str:
    normalized = _text(api_key)
    if not normalized:
        raise BinanceSpotTruthError("Binance API key is missing for fingerprinting")
    return hashlib.sha256(
        b"trading-system:binance-spot-account:v1\x00"
        + normalized.encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True)
class FunctionalReadRequest(PreparedRequest):
    """PreparedRequest whose diagnostic preview never exposes signatures."""

    def preview(self) -> dict[str, object]:
        parsed = urllib.parse.urlsplit(self.url)
        redacted_query = []
        for key, value in urllib.parse.parse_qsl(
            parsed.query, keep_blank_values=True
        ):
            redacted_query.append(
                (key, "***" if key.lower() == "signature" else value)
            )
        safe_url = urllib.parse.urlunsplit(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                urllib.parse.urlencode(redacted_query),
                parsed.fragment,
            )
        )
        return {
            "provider": self.provider,
            "method": self.method,
            "url": safe_url,
            "endpoint": self.endpoint,
            "headers": self.safe_headers,
            "body": {},
            "query": self.query or {},
            "blocked_reasons": list(self.blocked_reasons),
            "can_send": self.can_send,
        }


def _text(value: object) -> str:
    return str(value or "").strip()


def _upper(value: object) -> str:
    return _text(value).upper()


def _decimal(value: object, *, label: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise BinanceSpotTruthError(f"{label} must be a finite decimal") from exc
    if not result.is_finite():
        raise BinanceSpotTruthError(f"{label} must be a finite decimal")
    return result


def _decimal_text(value: Decimal) -> str:
    text = format(value.normalize(), "f")
    return "0" if text in {"", "-0"} else text


def _stable_payload_hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )


def _epoch(value: object, *, label: str) -> float:
    try:
        parsed = datetime.fromisoformat(_text(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise BinanceSpotTruthError(f"{label} must be ISO-8601") from exc
    if parsed.tzinfo is None:
        raise BinanceSpotTruthError(f"{label} must include a UTC offset")
    return parsed.astimezone(timezone.utc).timestamp()


def _strict_dict(value: object, *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise BinanceSpotTruthError(f"{label} response must be an object")
    return dict(value)


def _strict_rows(value: object, *, label: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or any(not isinstance(row, dict) for row in value):
        raise BinanceSpotTruthError(f"{label} response must be a list of objects")
    return [dict(row) for row in value]


def build_binance_spot_get_request(
    endpoint: str,
    query: Mapping[str, object] | None = None,
    *,
    allow_mock_origin: bool = False,
) -> PreparedRequest:
    """Build one allowlisted official Spot GET request with redacted preview."""

    normalized_endpoint = _text(endpoint)
    if normalized_endpoint not in ALL_GET_ENDPOINTS:
        raise BinanceSpotTruthError("endpoint is not in the Binance Spot GET allowlist")
    configured_base_url = env_value("BINANCE_BASE_URL") or BINANCE_BASE_URL
    if allow_mock_origin:
        base_url = configured_base_url.rstrip("/")
    else:
        # Validate before reading the API key/secret or producing a signature.
        base_url = assert_binance_spot_production_origin(configured_base_url)
    params = {
        str(key): (
            "true" if value is True else "false" if value is False else value
        )
        for key, value in dict(query or {}).items()
    }
    blocked: list[str] = []
    signed = normalized_endpoint in SIGNED_GET_ENDPOINTS
    api_key = env_value("BINANCE_API_KEY") if signed else ""
    api_secret = env_value("BINANCE_API_SECRET") if signed else ""
    if signed:
        blocked.extend(missing_env("BINANCE_API_KEY", "BINANCE_API_SECRET"))
        params["recvWindow"] = int(params.get("recvWindow") or 5000)
        if params["recvWindow"] <= 0 or params["recvWindow"] > 5000:
            blocked.append("recvWindow")
        params["timestamp"] = binance_timestamp_ms()
    encoded = (
        sign_binance_query(params, api_secret)
        if signed and api_secret
        else urllib.parse.urlencode(params)
    )
    safe_query = dict(params)
    if signed:
        safe_query["signature"] = "***" if api_secret else ""
    return FunctionalReadRequest(
        provider="binance-functional-read",
        method="GET",
        url=(
            f"{base_url.rstrip('/')}{normalized_endpoint}"
            + (f"?{encoded}" if encoded else "")
        ),
        endpoint=normalized_endpoint,
        headers={"X-MBX-APIKEY": api_key} if signed else {},
        safe_headers={"X-MBX-APIKEY_configured": bool(api_key)} if signed else {},
        body=None,
        query=safe_query,
        blocked_reasons=blocked,
    )


class OfficialBinanceSpotGetClient:
    """Final read-only HTTP edge.  It has no mutation method."""

    def __init__(
        self,
        *,
        sender: Callable[[PreparedRequest], Mapping[str, Any]] = send_prepared_request,
        expected_account_fingerprint: str = "",
        clock: Callable[[], float] = time.time,
        allow_mock_origin: bool = False,
    ) -> None:
        self.sender = sender
        self.expected_account_fingerprint = _text(
            expected_account_fingerprint
        ).lower()
        self.clock = clock
        self.allow_mock_origin = bool(allow_mock_origin)

    def _send_once(
        self,
        endpoint: str,
        query: Mapping[str, object] | None,
    ) -> Mapping[str, Any]:
        if endpoint in SIGNED_GET_ENDPOINTS:
            current_fingerprint = binance_api_key_fingerprint(
                env_value("BINANCE_API_KEY")
            )
            if (
                not self.expected_account_fingerprint
                or current_fingerprint != self.expected_account_fingerprint
            ):
                raise BinanceSpotTruthError(
                    "current Binance credential fingerprint changed"
                )
        prepared = build_binance_spot_get_request(
            endpoint, query, allow_mock_origin=self.allow_mock_origin
        )
        if prepared.method != "GET" or not prepared.can_send:
            raise BinanceSpotTruthError(
                "Binance Spot GET is not credential/config ready: "
                + ",".join(prepared.blocked_reasons)
            )
        response = self.sender(prepared)
        if not isinstance(response, Mapping):
            raise BinanceSpotTruthError("official Binance GET response is malformed")
        return response

    def _send_with_timestamp_retry(
        self,
        endpoint: str,
        query: Mapping[str, object] | None,
    ) -> tuple[Mapping[str, Any], object]:
        response = self._send_once(endpoint, query)
        payload = response.get("json")
        if (
            endpoint in SIGNED_GET_ENDPOINTS
            and isinstance(payload, dict)
            and int(payload.get("code") or 0) == -1021
        ):
            refresh_binance_time_offset()
            response = self._send_once(endpoint, query)
            payload = response.get("json")
        return response, payload

    def get(
        self,
        endpoint: str,
        query: Mapping[str, object] | None = None,
    ) -> object:
        if endpoint not in ALL_GET_ENDPOINTS:
            raise BinanceSpotTruthError("only official Binance Spot GET is permitted")

        response, payload = self._send_with_timestamp_retry(endpoint, query)
        if response.get("ok") is not True:
            raise BinanceSpotTruthError(
                f"official Binance GET failed: {endpoint}"
            )
        return payload

    def query_order_absence(self, *, client_order_id: str) -> dict[str, Any]:
        """Query one exact origClientOrderId and prove Binance error ``-2013``.

        This method never treats a generic HTTP failure as absence.  A found
        order is returned as ``notFound=False`` so the caller can fail closed;
        only Binance's exact unknown-order code is a nonacceptance observation.
        """

        normalized = _text(client_order_id)
        if re.fullmatch(
            r"ftb-[0-9a-f]{12}-(?:[bs]|[cf](?:[2-9]|1[0-2])?)",
            normalized,
        ) is None:
            raise BinanceSpotTruthError("exact functional client order id is invalid")
        query = {"symbol": "BTCUSDT", "origClientOrderId": normalized}
        response, payload = self._send_with_timestamp_retry(
            BINANCE_SPOT_QUERY_ORDER_ENDPOINT,
            query,
        )
        observed = float(self.clock())
        if response.get("ok") is True:
            row = _strict_dict(payload, label="queryOrder")
            if (
                _upper(row.get("symbol")) != "BTCUSDT"
                or _text(row.get("clientOrderId")) != normalized
                or not _text(row.get("orderId"))
            ):
                raise BinanceSpotTruthError(
                    "found exact order response identity is incomplete"
                )
            return {
                "complete": True,
                "symbol": "BTCUSDT",
                "origClientOrderId": normalized,
                "notFound": False,
                "errorCode": 0,
                "observedAt": _iso(observed),
            }
        if isinstance(payload, dict) and int(payload.get("code") or 0) == -2013:
            return {
                "complete": True,
                "symbol": "BTCUSDT",
                "origClientOrderId": normalized,
                "notFound": True,
                "errorCode": -2013,
                "observedAt": _iso(observed),
            }
        raise BinanceSpotTruthError(
            "exact order query did not prove Binance nonacceptance"
        )


@dataclass(frozen=True)
class UserStreamProof:
    observed_epoch: float
    subscribed_epoch: float
    events: tuple[dict[str, Any], ...]
    external_activity_absent: bool
    session_id: str
    permit_id: str
    permit_hash: str
    durable_journal_seal_hash: str
    durable_journal_event_count: int

    @classmethod
    def parse(
        cls,
        value: Mapping[str, Any],
        *,
        now_epoch: float,
        baseline_epoch: float,
    ) -> "UserStreamProof":
        if value.get("connected") is not True or value.get("authenticated") is not True:
            raise BinanceSpotTruthError("Binance user-data stream is not authenticated/connected")
        if value.get("sequenceComplete") is not True or value.get("gapDetected") is not False:
            raise BinanceSpotTruthError("Binance user-data stream has a gap")
        observed = _epoch(value.get("observedAt"), label="stream observedAt")
        subscribed = _epoch(value.get("subscribedAt"), label="stream subscribedAt")
        if now_epoch - observed < -1 or now_epoch - observed > MAX_TRUTH_AGE_SECONDS:
            raise BinanceSpotTruthError("Binance user-data stream proof is stale")
        if subscribed > baseline_epoch:
            raise BinanceSpotTruthError("user-data stream was subscribed after baseline")
        events = _strict_rows(value.get("events"), label="user-data stream events")
        session_id = _text(value.get("sessionId"))
        permit_id = _text(value.get("permitId"))
        permit_hash = _text(value.get("permitHash")).lower()
        journal_seal = _text(value.get("durableJournalSealHash")).lower()
        journal_event_count = int(value.get("durableJournalEventCount") or 0)
        if value.get("durableJournal") is True and (
            re.fullmatch(r"[0-9a-f]{64}", journal_seal) is None
            or journal_event_count < 0
            or journal_event_count != len(events)
        ):
            raise BinanceSpotTruthError(
                "durable user-stream journal seal/count is incomplete"
            )
        if any((session_id, permit_id, permit_hash)) and (
            not session_id.startswith("bnsft-")
            or not permit_id.startswith("functional-test-")
            or len(permit_hash) != 64
            or any(character not in "0123456789abcdef" for character in permit_hash)
        ):
            raise BinanceSpotTruthError(
                "user-data stream functional session/permit binding is incomplete"
            )
        seen: set[str] = set()
        normalized: list[dict[str, Any]] = []
        for event in events:
            event_id = _text(event.get("eventId"))
            event_type = _text(event.get("eventType"))
            event_time = float(event.get("eventTime") or 0) / 1000.0
            if not event_id or event_id in seen or event_time <= 0:
                raise BinanceSpotTruthError("user-data events need unique id/time")
            if event_type not in {
                "executionReport",
                "outboundAccountPosition",
                "balanceUpdate",
            }:
                raise BinanceSpotTruthError("unsupported user-data event type")
            if event_time < subscribed - 1 or event_time > now_epoch + 1:
                raise BinanceSpotTruthError("user-data event time is outside stream lifetime")
            seen.add(event_id)
            normalized.append(event)
        return cls(
            observed_epoch=observed,
            subscribed_epoch=subscribed,
            events=tuple(normalized),
            external_activity_absent=value.get("externalActivityAbsent") is True,
            session_id=session_id,
            permit_id=permit_id,
            permit_hash=permit_hash,
            durable_journal_seal_hash=journal_seal,
            durable_journal_event_count=journal_event_count,
        )


class BinanceSpotFunctionalUserStreamTracker:
    """Process-local no-gap proof for one pre-baseline user-data subscription.

    Binance Spot user-data events do not carry one account-wide monotonically
    increasing sequence number.  The only safe process-local continuity proof
    is therefore an authenticated subscription established before baseline
    with no disconnect, parser loss, or queue overflow afterwards.  A restart
    cannot inherit this proof; production lifecycle code must restore it from
    a durable event journal or remain unavailable.
    """

    def __init__(self, *, clock: Callable[[], float] = time.time) -> None:
        self.clock = clock
        self._lock = threading.RLock()
        self._connected = False
        self._authenticated = False
        self._subscribed_epoch = 0.0
        self._gap = True
        self._events: list[dict[str, Any]] = []
        self._seen: set[str] = set()
        self._external_activity_absent = False
        self._session_id = ""
        self._permit_id = ""
        self._permit_hash = ""

    def begin_authenticated_subscription(self, *, subscribed_epoch: float) -> None:
        now = float(self.clock())
        epoch = float(subscribed_epoch)
        if epoch <= 0 or epoch > now + 1:
            raise BinanceSpotTruthError("subscription epoch is invalid")
        with self._lock:
            self._connected = True
            self._authenticated = True
            self._subscribed_epoch = epoch
            self._gap = False
            self._events.clear()
            self._seen.clear()
            self._external_activity_absent = True
            self._session_id = ""
            self._permit_id = ""
            self._permit_hash = ""

    def bind_functional_session(
        self, *, session_id: str, permit_id: str, permit_hash: str
    ) -> None:
        normalized_hash = _text(permit_hash).lower()
        if (
            not _text(session_id).startswith("bnsft-")
            or not _text(permit_id).startswith("functional-test-")
            or len(normalized_hash) != 64
            or any(character not in "0123456789abcdef" for character in normalized_hash)
        ):
            raise BinanceSpotTruthError("stream functional binding is invalid")
        with self._lock:
            identity = (self._session_id, self._permit_id, self._permit_hash)
            requested = (_text(session_id), _text(permit_id), normalized_hash)
            if identity not in {("", "", ""), requested}:
                raise BinanceSpotTruthError("stream functional binding changed")
            self._session_id, self._permit_id, self._permit_hash = requested

    def mark_gap(self) -> None:
        with self._lock:
            self._gap = True
            self._external_activity_absent = False

    def mark_disconnected(self) -> None:
        with self._lock:
            self._connected = False
            self._gap = True
            self._external_activity_absent = False

    def ingest(self, payload: Mapping[str, Any], *, owner_prefix: str) -> dict[str, Any]:
        event = normalize_binance_user_stream_event(payload)
        event_id = _text(event.get("eventId"))
        with self._lock:
            if not self._connected or not self._authenticated or self._gap:
                raise BinanceSpotTruthError("stream event arrived without continuous authority")
            if event_id in self._seen:
                raise BinanceSpotTruthError("duplicate user-data event")
            self._seen.add(event_id)
            self._events.append(event)
            if event.get("eventType") == "balanceUpdate":
                self._external_activity_absent = False
            elif event.get("eventType") == "executionReport" and not (
                owner_prefix
                and _text(event.get("clientOrderId")).startswith(owner_prefix)
            ):
                self._external_activity_absent = False
        return event

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "connected": self._connected,
                "authenticated": self._authenticated,
                "sequenceComplete": not self._gap,
                "gapDetected": self._gap,
                "subscribedAt": _iso(self._subscribed_epoch),
                "observedAt": _iso(float(self.clock())),
                "externalActivityAbsent": self._external_activity_absent,
                "events": [dict(event) for event in self._events],
                "sessionId": self._session_id,
                "permitId": self._permit_id,
                "permitHash": self._permit_hash,
            }


def normalize_binance_user_stream_event(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize one official WebSocket API user-data event without secrets."""

    event = payload.get("event") if isinstance(payload.get("event"), dict) else payload
    event_type = _text(event.get("e"))
    event_time = int(event.get("E") or 0)
    if event_type == "executionReport":
        order_id = _text(event.get("i"))
        trade_id = _text(event.get("t"))
        execution_type = _upper(event.get("x"))
        status = _upper(event.get("X"))
        if not order_id or not event_time or not execution_type or not status:
            raise BinanceSpotTruthError("executionReport identity/status is incomplete")
        return {
            "eventId": f"execution:{order_id}:{trade_id}:{execution_type}:{event_time}",
            "eventType": event_type,
            "eventTime": event_time,
            "symbol": _upper(event.get("s")),
            "clientOrderId": _text(event.get("c")),
            "originalClientOrderId": _text(event.get("C")),
            "orderId": order_id,
            "tradeId": trade_id,
            "side": _upper(event.get("S")),
            "orderType": _upper(event.get("o")),
            "executionType": execution_type,
            "orderStatus": status,
            "lastQty": _text(event.get("l")),
            "lastQuoteQty": _text(event.get("Y")),
            "lastPrice": _text(event.get("L")),
            "commission": _text(event.get("n")),
            "commissionAsset": _upper(event.get("N")),
            "cumulativeQty": _text(event.get("z")),
            "cumulativeQuoteQty": _text(event.get("Z")),
        }
    if event_type == "outboundAccountPosition":
        balances = _strict_rows(event.get("B"), label="stream balance update")
        return {
            "eventId": f"account:{int(event.get('u') or 0)}:{event_time}",
            "eventType": event_type,
            "eventTime": event_time,
            "lastAccountUpdateTime": int(event.get("u") or 0),
            "balances": [
                {
                    "asset": _upper(row.get("a")),
                    "free": _text(row.get("f")),
                    "locked": _text(row.get("l")),
                }
                for row in balances
            ],
        }
    if event_type == "balanceUpdate":
        asset = _upper(event.get("a"))
        clear_time = int(event.get("T") or 0)
        if not asset or not clear_time:
            raise BinanceSpotTruthError("balanceUpdate identity is incomplete")
        return {
            "eventId": f"balance:{asset}:{clear_time}:{event_time}",
            "eventType": event_type,
            "eventTime": event_time,
            "asset": asset,
            "delta": _text(event.get("d")),
            "clearTime": clear_time,
        }
    raise BinanceSpotTruthError("unsupported Binance user-data stream event")


class BinanceSpotOfficialTruthReader:
    """Combine official REST reads and authenticated stream evidence."""

    def __init__(
        self,
        *,
        client: OfficialBinanceSpotGetClient,
        account_fingerprint: str,
        stream_reader: Callable[[], Mapping[str, Any]],
        clock: Callable[[], float] = time.time,
    ) -> None:
        fingerprint = _text(account_fingerprint).lower()
        if len(fingerprint) != 64 or any(ch not in "0123456789abcdef" for ch in fingerprint):
            raise BinanceSpotTruthError("account fingerprint must be SHA-256")
        self.client = client
        self.account_fingerprint = fingerprint
        self.stream_reader = stream_reader
        self.clock = clock
        if (
            self.client.expected_account_fingerprint
            != self.account_fingerprint
        ):
            raise BinanceSpotTruthError(
                "official GET client account fingerprint is not exact-bound"
            )

    def _read_history(
        self,
        *,
        endpoint: str,
        cursor_parameter: str,
        row_id_field: str,
        start_ms: int,
        end_ms: int,
        label: str,
        audit_sink: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        """Read a complete bounded history without treating a full page as complete."""

        collected: list[dict[str, Any]] = []
        seen_ids: set[int] = set()
        cursor: int | None = None
        previous_time = start_ms - 1
        for _ in range(MAX_HISTORY_PAGES):
            query: dict[str, object] = {
                "symbol": "BTCUSDT",
                "limit": MAX_PAGE_SIZE,
            }
            if cursor is None:
                query.update({"startTime": start_ms, "endTime": end_ms})
            else:
                # Binance documents orderId/fromId pagination as inclusive.
                # The cursor is last+1, so a repeated last row is never silently
                # accepted as forward progress.
                query[cursor_parameter] = cursor
            page = _strict_rows(self.client.get(endpoint, query), label=label)
            audit_page = {
                "endpoint": endpoint,
                "query": dict(query),
                "cursorParameter": cursor_parameter,
                "rowIdField": row_id_field,
                "pageIndex": len(audit_sink or ()),
                "responseRows": [dict(row) for row in page],
                "responseHash": _stable_payload_hash({"rows": list(page)}),
                "responseCount": len(page),
                "completion": "",
            }
            if not page:
                audit_page["completion"] = "EMPTY_PAGE"
                if audit_sink is not None:
                    audit_sink.append(audit_page)
                return collected
            page_ids: list[int] = []
            reached_after_window = False
            for row in page:
                raw_id = row.get(row_id_field)
                if isinstance(raw_id, bool):
                    raise BinanceSpotTruthError(f"{label} id is invalid")
                try:
                    row_id = int(str(raw_id))
                except (TypeError, ValueError) as exc:
                    raise BinanceSpotTruthError(f"{label} id is invalid") from exc
                row_time = int(row.get("time") or row.get("updateTime") or 0)
                if row_id < 0 or row_time <= 0:
                    raise BinanceSpotTruthError(f"{label} id/time is incomplete")
                if page_ids and row_id <= page_ids[-1]:
                    raise BinanceSpotTruthError(f"{label} page is not strictly ordered")
                if row_time < previous_time:
                    raise BinanceSpotTruthError(f"{label} history time moved backwards")
                page_ids.append(row_id)
                previous_time = row_time
                if row_time < start_ms:
                    raise BinanceSpotTruthError(f"{label} escaped the baseline window")
                if row_time > end_ms:
                    reached_after_window = True
                    continue
                if reached_after_window:
                    raise BinanceSpotTruthError(f"{label} history time is not monotonic")
                if row_id in seen_ids:
                    raise BinanceSpotTruthError(f"{label} pagination repeated an id")
                seen_ids.add(row_id)
                collected.append(dict(row))
            if reached_after_window or len(page) < MAX_PAGE_SIZE:
                audit_page["completion"] = (
                    "ROW_AFTER_CUTOFF" if reached_after_window else "SHORT_PAGE"
                )
                if audit_sink is not None:
                    audit_sink.append(audit_page)
                return collected
            next_cursor = page_ids[-1] + 1
            if cursor is not None and next_cursor <= cursor:
                raise BinanceSpotTruthError(f"{label} pagination made no progress")
            audit_page["completion"] = "CONTINUE"
            audit_page["nextCursor"] = next_cursor
            if audit_sink is not None:
                audit_sink.append(audit_page)
            cursor = next_cursor
        raise BinanceSpotTruthError(f"{label} exceeded the bounded pagination budget")

    def read(
        self,
        *,
        baseline_epoch: float,
        owner_prefix: str,
        _cleanup_recovery_only: bool = False,
        _startup_abort_identity: Mapping[str, str] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        cutoff = float(self.clock())
        start_ms = int(float(baseline_epoch) * 1000)
        end_ms = int(cutoff * 1000)
        if end_ms < start_ms or end_ms - start_ms > 3 * 60 * 60 * 1000:
            raise BinanceSpotTruthError("truth interval must be within the 3-hour cleanup bound")
        account = _strict_dict(
            self.client.get(BINANCE_SPOT_ACCOUNT_ENDPOINT, {"omitZeroBalances": False}),
            label="account",
        )
        open_orders = _strict_rows(
            self.client.get(BINANCE_SPOT_OPEN_ORDERS_ENDPOINT, {}),
            label="openOrders",
        )
        all_orders_pages: list[dict[str, Any]] = []
        all_orders = self._read_history(
            endpoint=BINANCE_SPOT_ALL_ORDERS_ENDPOINT,
            cursor_parameter="orderId",
            row_id_field="orderId",
            start_ms=start_ms,
            end_ms=end_ms,
            label="allOrders",
            audit_sink=all_orders_pages,
        )
        my_trades_pages: list[dict[str, Any]] = []
        trades = self._read_history(
            endpoint=BINANCE_SPOT_MY_TRADES_ENDPOINT,
            cursor_parameter="fromId",
            row_id_field="id",
            start_ms=start_ms,
            end_ms=end_ms,
            label="myTrades",
            audit_sink=my_trades_pages,
        )
        exchange_info = _strict_dict(
            self.client.get(BINANCE_SPOT_EXCHANGE_INFO_ENDPOINT, {"symbol": "BTCUSDT"}),
            label="exchangeInfo",
        )
        rule_truth = assemble_binance_spot_rules(
            exchange_info, account=account
        )
        ticker = _strict_dict(
            self.client.get(BINANCE_SPOT_TICKER_PRICE_ENDPOINT, {"symbol": "BTCUSDT"}),
            label="tickerPrice",
        )
        average: dict[str, Any] | None = None
        reference_price = _decimal(ticker.get("price"), label="ticker reference price")
        reference_source = "BINANCE_TICKER_PRICE"
        if int(rule_truth["avgPriceMins"]) > 0 and (
            rule_truth["minNotionalAppliesToMarket"] is True
            or rule_truth["maxNotionalAppliesToMarket"] is True
        ):
            average = _strict_dict(
                self.client.get(BINANCE_SPOT_AVG_PRICE_ENDPOINT, {"symbol": "BTCUSDT"}),
                label="avgPrice",
            )
            if int(average.get("mins") or -1) != int(rule_truth["avgPriceMins"]):
                raise BinanceSpotTruthError(
                    "official average-price horizon changed from exchange filter"
                )
            reference_price = _decimal(
                average.get("price"), label="average market reference price"
            )
            close_time_ms = int(average.get("closeTime") or 0)
            if (
                reference_price <= 0
                or close_time_ms <= 0
                or abs(cutoff - close_time_ms / 1000.0) > MAX_TRUTH_AGE_SECONDS
            ):
                raise BinanceSpotTruthError(
                    "official average market reference is stale/invalid"
                )
            reference_source = "BINANCE_AVG_PRICE"
        rule_truth.update(
            {
                "marketReferencePrice": _decimal_text(reference_price),
                "marketReferenceSource": reference_source,
                "rulesObservedAt": _iso(cutoff),
            }
        )
        observed = float(self.clock())
        if observed < cutoff or observed - cutoff > MAX_TRUTH_AGE_SECONDS:
            raise BinanceSpotTruthError(
                "official truth read exceeded the strict 15-second snapshot budget"
            )
        stream: UserStreamProof | None
        recovery: dict[str, Any] | None = None
        raw_stream = self.stream_reader()
        if not isinstance(raw_stream, Mapping):
            raise BinanceSpotTruthError("durable stream proof is malformed")
        if _cleanup_recovery_only and _startup_abort_identity is not None:
            raise BinanceSpotTruthError("truth recovery modes are mutually exclusive")
        if _cleanup_recovery_only:
            session_id = _text(raw_stream.get("sessionId"))
            permit_id = _text(raw_stream.get("permitId"))
            permit_hash = _text(raw_stream.get("permitHash")).lower()
            preserved_journal_seal = _text(
                raw_stream.get("durableJournalSealHash")
            ).lower()
            preserved_journal_count = int(
                raw_stream.get("durableJournalEventCount") or 0
            )
            if (
                raw_stream.get("gapDetected") is not True
                or not session_id.startswith("bnsft-")
                or not permit_id.startswith("functional-test-")
                or re.fullmatch(r"[0-9a-f]{64}", permit_hash) is None
                or re.fullmatch(
                    r"[0-9a-f]{64}", preserved_journal_seal
                ) is None
                or preserved_journal_count < 0
            ):
                raise BinanceSpotTruthError(
                    "cleanup recovery requires a preserved bound stream gap"
                )
            gap_hash = _stable_payload_hash(dict(raw_stream))
            rest_hash = _stable_payload_hash(
                {
                    "account": dict(account),
                    "openOrders": list(open_orders),
                    "allOrders": list(all_orders),
                    "myTrades": list(trades),
                    "ticker": dict(ticker),
                    "rules": dict(rule_truth),
                    "baselineEpoch": float(baseline_epoch),
                    "historyCutoffEpoch": cutoff,
                    "observedEpoch": observed,
                }
            )
            recovery = {
                "mode": "REST_RECONCILED_CLEANUP_ONLY",
                "preservedStreamGap": True,
                "completeRestReconciliation": True,
                "streamGapEvidenceHash": gap_hash,
                "restTruthHash": rest_hash,
                "sessionId": session_id,
                "permitId": permit_id,
                "permitHash": permit_hash,
                "preservedStreamJournalSealHash": preserved_journal_seal,
                "preservedStreamJournalEventCount": preserved_journal_count,
                "recoveryObservedAt": _iso(observed),
            }
            recovery["recoveryAttestationHash"] = _stable_payload_hash(recovery)
            stream = None
        elif _startup_abort_identity is not None:
            requested_session = _text(_startup_abort_identity.get("sessionId"))
            requested_permit = _text(_startup_abort_identity.get("permitId"))
            requested_hash = _text(_startup_abort_identity.get("permitHash")).lower()
            raw_identity = (
                _text(raw_stream.get("sessionId")),
                _text(raw_stream.get("permitId")),
                _text(raw_stream.get("permitHash")).lower(),
            )
            requested_identity = (
                requested_session,
                requested_permit,
                requested_hash,
            )
            if (
                not requested_session.startswith("bnsft-")
                or not requested_permit.startswith("functional-test-")
                or re.fullmatch(r"[0-9a-f]{64}", requested_hash) is None
                or raw_identity not in {("", "", ""), requested_identity}
            ):
                raise BinanceSpotTruthError(
                    "startup abort stream/core identity is inconsistent"
                )
            rest_hash = _stable_payload_hash(
                {
                    "account": dict(account),
                    "openOrders": list(open_orders),
                    "allOrders": list(all_orders),
                    "myTrades": list(trades),
                    "ticker": dict(ticker),
                    "rules": dict(rule_truth),
                    "baselineEpoch": float(baseline_epoch),
                    "historyCutoffEpoch": cutoff,
                    "observedEpoch": observed,
                }
            )
            recovery = {
                "mode": "STARTUP_ABORT_ONLY",
                "preservedStreamGap": raw_stream.get("gapDetected") is True,
                "completeRestReconciliation": True,
                "streamGapEvidenceHash": _stable_payload_hash(dict(raw_stream)),
                "restTruthHash": rest_hash,
                "sessionId": requested_session,
                "permitId": requested_permit,
                "permitHash": requested_hash,
                "recoveryObservedAt": _iso(observed),
            }
            recovery["recoveryAttestationHash"] = _stable_payload_hash(recovery)
            stream = None
        else:
            stream = UserStreamProof.parse(
                raw_stream,
                now_epoch=observed,
                baseline_epoch=float(baseline_epoch),
            )
        truth = assemble_binance_spot_truth(
            account=account,
            permission_proof=rule_truth,
            open_orders=open_orders,
            all_orders=all_orders,
            trades=trades,
            ticker=ticker,
            stream=stream,
            cleanup_recovery=recovery,
            account_fingerprint=self.account_fingerprint,
            owner_prefix=_text(owner_prefix),
            baseline_epoch=float(baseline_epoch),
            history_cutoff_epoch=cutoff,
            observed_epoch=observed,
        )
        # Preserve the exact credentialed/public response set that produced the
        # normalized truth.  It contains no request headers, API key, signature,
        # or secret.  Finalization stores this document in a dedicated durable
        # table, separate from producer-authored evidence, so the release
        # consumer can re-normalize and cross-check it independently.
        official_rest_snapshot: dict[str, Any] = {
            "schemaVersion": "binance-spot-functional-official-rest-set/v1",
            "baselineEpoch": float(baseline_epoch),
            "historyCutoffEpoch": cutoff,
            "observedEpoch": observed,
            "account": dict(account),
            "openOrders": [dict(row) for row in open_orders],
            "allOrders": [dict(row) for row in all_orders],
            "myTrades": [dict(row) for row in trades],
            "exchangeInfo": dict(exchange_info),
            "tickerPrice": dict(ticker),
            "averagePrice": dict(average) if average is not None else None,
            "normalizedRules": dict(rule_truth),
            "requestEnvelopes": {
                "account": {
                    "endpoint": BINANCE_SPOT_ACCOUNT_ENDPOINT,
                    "query": {"omitZeroBalances": False},
                    "responseHash": _stable_payload_hash(dict(account)),
                },
                "openOrders": {
                    "endpoint": BINANCE_SPOT_OPEN_ORDERS_ENDPOINT,
                    "query": {},
                    "responseHash": _stable_payload_hash(
                        {"rows": list(open_orders)}
                    ),
                },
                "allOrdersPages": all_orders_pages,
                "myTradesPages": my_trades_pages,
                "exchangeInfo": {
                    "endpoint": BINANCE_SPOT_EXCHANGE_INFO_ENDPOINT,
                    "query": {"symbol": "BTCUSDT"},
                    "responseHash": _stable_payload_hash(dict(exchange_info)),
                },
                "tickerPrice": {
                    "endpoint": BINANCE_SPOT_TICKER_PRICE_ENDPOINT,
                    "query": {"symbol": "BTCUSDT"},
                    "responseHash": _stable_payload_hash(dict(ticker)),
                },
                "averagePrice": (
                    {
                        "endpoint": BINANCE_SPOT_AVG_PRICE_ENDPOINT,
                        "query": {"symbol": "BTCUSDT"},
                        "responseHash": _stable_payload_hash(dict(average)),
                    }
                    if average is not None
                    else None
                ),
            },
        }
        truth["officialRestSnapshot"] = official_rest_snapshot
        truth["officialRestTruthHash"] = _stable_payload_hash(
            official_rest_snapshot
        )
        return truth, rule_truth

    def read_cleanup_recovery(
        self,
        *,
        baseline_epoch: float,
        owner_prefix: str,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Fresh complete REST truth with a preserved, never-cleared WS gap.

        The result is explicitly typed cleanup-only and can never satisfy a
        normal entry/PASS context.  It exists solely to cancel exact owned
        working orders and reduce the session-owned BTC delta after restart.
        """

        return self.read(
            baseline_epoch=baseline_epoch,
            owner_prefix=owner_prefix,
            _cleanup_recovery_only=True,
        )

    def read_startup_abort_attestation(
        self,
        *,
        baseline_epoch: float,
        owner_prefix: str,
        session_id: str,
        permit_id: str,
        permit_hash: str,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Fresh REST-only truth for an ARMED session that never activated."""

        return self.read(
            baseline_epoch=baseline_epoch,
            owner_prefix=owner_prefix,
            _startup_abort_identity={
                "sessionId": session_id,
                "permitId": permit_id,
                "permitHash": permit_hash,
            },
        )

    def read_nonacceptance_observation(
        self,
        *,
        baseline_epoch: float,
        owner_prefix: str,
        client_order_id: str,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        """Read full official truth, then query the exact client order id.

        The query is deliberately performed after the complete account-wide
        snapshot.  Both observations must remain within the strict freshness
        budget, and the authenticated stream must still be gapless when the
        full snapshot is assembled.
        """

        truth, rules = self.read(
            baseline_epoch=baseline_epoch,
            owner_prefix=owner_prefix,
        )
        proof = self.client.query_order_absence(client_order_id=client_order_id)
        proof_epoch = _epoch(proof.get("observedAt"), label="queryOrder observedAt")
        truth_epoch = _epoch(truth.get("observedAt"), label="truth observedAt")
        now = float(self.clock())
        if (
            proof_epoch < truth_epoch
            or proof_epoch - truth_epoch > MAX_TRUTH_AGE_SECONDS
            or now - proof_epoch < -1
            or now - proof_epoch > MAX_TRUTH_AGE_SECONDS
        ):
            raise BinanceSpotTruthError(
                "exact order lookup is not fresh relative to full official truth"
            )
        return truth, rules, proof


def _normalize_order(
    row: Mapping[str, Any], *, exact_symbol: bool
) -> dict[str, Any]:
    symbol = _upper(row.get("symbol"))
    order_id = _text(row.get("orderId"))
    client_id = _text(row.get("clientOrderId"))
    side = _upper(row.get("side"))
    order_type = _upper(row.get("type"))
    status = _upper(row.get("status"))
    if (
        (exact_symbol and symbol != "BTCUSDT")
        or not symbol
        or not order_id
        or not client_id
        or side not in {"BUY", "SELL"}
        or not order_type
        or status not in TERMINAL_STATES | OPEN_STATES
    ):
        raise BinanceSpotTruthError("official order row is incomplete/invalid")
    for field in ("origQty", "executedQty", "origQuoteOrderQty", "cummulativeQuoteQty"):
        if _decimal(row.get(field, "0"), label=f"order {field}") < 0:
            raise BinanceSpotTruthError(f"order {field} cannot be negative")
    return {
        "orderId": order_id,
        "clientOrderId": client_id,
        "symbol": symbol,
        "product": "SPOT",
        "side": side,
        "type": order_type,
        "status": "CANCELED" if status == "CANCELLED" else status,
        "origQty": _text(row.get("origQty") or "0"),
        "executedQty": _text(row.get("executedQty") or "0"),
        "origQuoteOrderQty": _text(row.get("origQuoteOrderQty") or "0"),
        "cummulativeQuoteQty": _text(row.get("cummulativeQuoteQty") or "0"),
        "time": int(row.get("time") or 0),
        "updateTime": int(row.get("updateTime") or 0),
        "isMargin": False,
        "reduceOnly": False,
    }


def _account_symbol_permission_proof(
    account: Mapping[str, Any], symbol: Mapping[str, Any]
) -> dict[str, Any]:
    """Prove Binance's documented AND-of-OR symbol permissions.

    Every inner ``permissionSets`` row is an OR set; the account must match at
    least one permission in every row.  The deprecated symbol ``permissions``
    field may be empty and is deliberately not used as authority.
    """

    if account.get("canTrade") is not True:
        raise BinanceSpotTruthError("Binance Spot account cannot trade")
    if _upper(account.get("accountType")) != "SPOT":
        raise BinanceSpotTruthError("Binance accountType must be exact SPOT")
    raw_account_permissions = account.get("permissions")
    if not isinstance(raw_account_permissions, list) or not raw_account_permissions:
        raise BinanceSpotTruthError("account permissions must be a nonempty list")
    account_permissions: list[str] = []
    for item in raw_account_permissions:
        permission = _upper(item)
        if not permission or permission in account_permissions:
            raise BinanceSpotTruthError(
                "account permissions must be unique nonempty strings"
            )
        account_permissions.append(permission)

    deprecated = symbol.get("permissions", [])
    if not isinstance(deprecated, list):
        raise BinanceSpotTruthError(
            "deprecated symbol permissions field is malformed"
        )
    raw_sets = symbol.get("permissionSets")
    if not isinstance(raw_sets, list) or not raw_sets:
        raise BinanceSpotTruthError(
            "symbol permissionSets must be a nonempty AND-of-OR list"
        )
    permission_sets: list[list[str]] = []
    account_permission_set = set(account_permissions)
    for raw_set in raw_sets:
        if not isinstance(raw_set, list) or not raw_set:
            raise BinanceSpotTruthError(
                "each symbol permission set must be a nonempty OR list"
            )
        normalized: list[str] = []
        for item in raw_set:
            permission = _upper(item)
            if not permission or permission in normalized:
                raise BinanceSpotTruthError(
                    "symbol permission set entries must be unique nonempty strings"
                )
            normalized.append(permission)
        if account_permission_set.isdisjoint(normalized):
            raise BinanceSpotTruthError(
                "account permissions do not satisfy every symbol permission set"
            )
        permission_sets.append(normalized)

    proof: dict[str, Any] = {
        "accountCanTrade": True,
        "accountType": "SPOT",
        "accountPermissions": sorted(account_permissions),
        "symbolPermissionSets": permission_sets,
        "permissionSemantics": "AND_OF_OR_SETS",
        "symbolPermissionsAuthorized": True,
    }
    proof["accountSymbolPermissionProofHash"] = _stable_payload_hash(proof)
    return proof


def assemble_binance_spot_rules(
    exchange_info: Mapping[str, Any], *, account: Mapping[str, Any]
) -> dict[str, Any]:
    symbols = _strict_rows(exchange_info.get("symbols"), label="exchangeInfo symbols")
    if len(symbols) != 1 or _upper(symbols[0].get("symbol")) != "BTCUSDT":
        raise BinanceSpotTruthError("exchangeInfo must contain exact BTCUSDT only")
    symbol = symbols[0]
    permission_proof = _account_symbol_permission_proof(account, symbol)
    filters = _strict_rows(symbol.get("filters"), label="exchangeInfo filters")
    by_type = {_upper(item.get("filterType")): item for item in filters}
    lot = by_type.get("MARKET_LOT_SIZE")
    quantity_filter_type = "MARKET_LOT_SIZE"
    if not isinstance(lot, dict) or _decimal(lot.get("stepSize"), label="stepSize") <= 0:
        lot = by_type.get("LOT_SIZE")
        quantity_filter_type = "LOT_SIZE"
    notional = by_type.get("NOTIONAL") or by_type.get("MIN_NOTIONAL")
    if not isinstance(lot, dict) or not isinstance(notional, dict):
        raise BinanceSpotTruthError("LOT_SIZE/MIN_NOTIONAL filters are required")
    min_notional = _decimal(notional.get("minNotional"), label="minNotional")
    max_notional = _decimal(
        notional.get("maxNotional") or "1000000000", label="maxNotional"
    )
    filter_type = _upper(notional.get("filterType"))
    if filter_type == "NOTIONAL":
        min_applies = notional.get("applyMinToMarket")
        max_applies = notional.get("applyMaxToMarket")
    elif filter_type == "MIN_NOTIONAL":
        min_applies = notional.get("applyToMarket")
        max_applies = False
    else:
        raise BinanceSpotTruthError("unknown notional filter type")
    if not isinstance(min_applies, bool) or not isinstance(max_applies, bool):
        raise BinanceSpotTruthError(
            "notional market applicability flags are required"
        )
    try:
        avg_price_mins = int(notional.get("avgPriceMins"))
    except (TypeError, ValueError) as exc:
        raise BinanceSpotTruthError("notional avgPriceMins is invalid") from exc
    if avg_price_mins < 0:
        raise BinanceSpotTruthError("notional avgPriceMins cannot be negative")
    return {
        **permission_proof,
        "exchangeInfoComplete": True,
        "symbol": "BTCUSDT",
        "status": _upper(symbol.get("status")),
        "spotTradingAllowed": symbol.get("isSpotTradingAllowed") is True,
        "quoteOrderQtyMarketAllowed": symbol.get("quoteOrderQtyMarketAllowed") is True,
        "marginMode": False,
        "futuresMode": False,
        "borrowMode": False,
        "withdrawalAction": False,
        "minQty": _text(lot.get("minQty")),
        "maxQty": _text(lot.get("maxQty")),
        "stepSize": _text(lot.get("stepSize")),
        "minNotional": _decimal_text(min_notional),
        "maxNotional": _decimal_text(max_notional),
        "minNotionalAppliesToMarket": min_applies,
        "maxNotionalAppliesToMarket": max_applies,
        "avgPriceMins": avg_price_mins,
        "marketReferencePrice": "",
        "marketReferenceSource": "",
        "quantityFilterType": quantity_filter_type,
        "notionalFilterType": filter_type,
    }


def assemble_binance_spot_truth(
    *,
    account: Mapping[str, Any],
    permission_proof: Mapping[str, Any],
    open_orders: Sequence[Mapping[str, Any]],
    all_orders: Sequence[Mapping[str, Any]],
    trades: Sequence[Mapping[str, Any]],
    ticker: Mapping[str, Any],
    stream: UserStreamProof | None,
    account_fingerprint: str,
    owner_prefix: str,
    baseline_epoch: float,
    observed_epoch: float,
    history_cutoff_epoch: float | None = None,
    cleanup_recovery: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    cutoff = (
        float(observed_epoch)
        if history_cutoff_epoch is None
        else float(history_cutoff_epoch)
    )
    if baseline_epoch > cutoff or cutoff > observed_epoch:
        raise BinanceSpotTruthError("history baseline/cutoff is invalid")
    expected_permission_proof = {
        field: permission_proof.get(field)
        for field in (
            "accountCanTrade",
            "accountType",
            "accountPermissions",
            "symbolPermissionSets",
            "permissionSemantics",
            "symbolPermissionsAuthorized",
        )
    }
    if (
        expected_permission_proof["accountCanTrade"] is not True
        or expected_permission_proof["accountType"] != "SPOT"
        or expected_permission_proof["permissionSemantics"]
        != "AND_OF_OR_SETS"
        or expected_permission_proof["symbolPermissionsAuthorized"] is not True
        or _text(permission_proof.get("accountSymbolPermissionProofHash")).lower()
        != _stable_payload_hash(expected_permission_proof)
    ):
        raise BinanceSpotTruthError(
            "account/symbol permission proof is incomplete or changed"
        )
    recovery = dict(cleanup_recovery or {})
    recovery_mode = _upper(recovery.get("mode")) if recovery else ""
    cleanup_recovery_only = recovery_mode == "REST_RECONCILED_CLEANUP_ONLY"
    startup_abort_only = recovery_mode == "STARTUP_ABORT_ONLY"
    recovery_only = cleanup_recovery_only or startup_abort_only
    if recovery_only:
        if stream is not None:
            raise BinanceSpotTruthError(
                "REST-only recovery cannot claim a gapless user stream"
            )
        if (
            recovery.get("completeRestReconciliation") is not True
            or re.fullmatch(
                r"[0-9a-f]{64}", _text(recovery.get("streamGapEvidenceHash")).lower()
            )
            is None
            or re.fullmatch(
                r"[0-9a-f]{64}", _text(recovery.get("recoveryAttestationHash")).lower()
            )
            is None
        ):
            raise BinanceSpotTruthError(
                "REST-only recovery attestation is incomplete"
            )
        if cleanup_recovery_only and recovery.get("preservedStreamGap") is not True:
            raise BinanceSpotTruthError(
                "cleanup recovery must preserve a durable stream gap"
            )
    elif recovery:
        raise BinanceSpotTruthError("REST recovery mode is unsupported")
    elif stream is None:
        raise BinanceSpotTruthError(
            "gapless user stream is required outside cleanup recovery"
        )
    balances = _strict_rows(account.get("balances"), label="account balances")
    normalized_balances: list[dict[str, str]] = []
    seen_assets: set[str] = set()
    for row in balances:
        asset = _upper(row.get("asset"))
        if not asset or asset in seen_assets:
            raise BinanceSpotTruthError("account balances require unique assets")
        free = _decimal(row.get("free"), label=f"{asset} free")
        locked = _decimal(row.get("locked"), label=f"{asset} locked")
        if free < 0 or locked < 0:
            raise BinanceSpotTruthError("account balance cannot be negative")
        seen_assets.add(asset)
        normalized_balances.append(
            {"asset": asset, "free": _decimal_text(free), "locked": _decimal_text(locked)}
        )
    if not {"BTC", "USDT"}.issubset(seen_assets):
        raise BinanceSpotTruthError("complete account response must include BTC and USDT")

    normalized_open = [
        _normalize_order(row, exact_symbol=False) for row in open_orders
    ]
    if any(row["status"] not in OPEN_STATES for row in normalized_open):
        raise BinanceSpotTruthError("openOrders contains a terminal row")
    if len({row["orderId"] for row in normalized_open}) != len(normalized_open):
        raise BinanceSpotTruthError("openOrders contains duplicate orderId")
    normalized_history = [
        _normalize_order(row, exact_symbol=True) for row in all_orders
    ]
    history_by_order: dict[str, dict[str, Any]] = {}
    for row in normalized_history:
        if row["orderId"] in history_by_order:
            raise BinanceSpotTruthError("allOrders contains duplicate orderId")
        history_by_order[row["orderId"]] = row
    normalized_closed = [
        row for row in normalized_history if row["status"] in TERMINAL_STATES
    ]
    mark = _decimal(ticker.get("price"), label="BTCUSDT ticker price")
    if _upper(ticker.get("symbol")) != "BTCUSDT" or mark <= 0:
        raise BinanceSpotTruthError("fresh BTCUSDT ticker is invalid")

    normalized_fills: list[dict[str, Any]] = []
    fee_quote_valuation_complete = True
    seen_trade_ids: set[str] = set()
    for trade in trades:
        symbol = _upper(trade.get("symbol") or "BTCUSDT")
        trade_id = _text(trade.get("id"))
        order_id = _text(trade.get("orderId"))
        order = history_by_order.get(order_id)
        if symbol != "BTCUSDT" or not trade_id or trade_id in seen_trade_ids or order is None:
            raise BinanceSpotTruthError("myTrades row cannot be bound to exact allOrders truth")
        qty = _decimal(trade.get("qty"), label="trade qty")
        quote_qty = _decimal(trade.get("quoteQty"), label="trade quoteQty")
        commission = _decimal(trade.get("commission"), label="trade commission")
        commission_asset = _upper(trade.get("commissionAsset"))
        if qty <= 0 or quote_qty <= 0 or commission < 0 or not commission_asset:
            raise BinanceSpotTruthError("myTrades quantity/fee truth is malformed")
        if commission_asset == "USDT":
            fee_quote = commission
        elif commission_asset == "BTC":
            fee_quote = commission * (quote_qty / qty)
        else:
            # Keep exact quantity/order truth available for risk-reducing
            # cleanup.  Binance does not include historical third-asset→USDT
            # valuation here, so this fee cannot contribute to a PASS/loss
            # proof; entry is separately refused when positive BNB funding is
            # present and an observed exception immediately forces cleanup.
            fee_quote = Decimal("0")
            fee_quote_valuation_complete = False
        side = "BUY" if trade.get("isBuyer") is True else "SELL"
        if side != order["side"]:
            raise BinanceSpotTruthError("myTrades side disagrees with allOrders")
        normalized_fills.append(
            {
                "tradeId": trade_id,
                "orderId": order_id,
                "clientOrderId": order["clientOrderId"],
                "symbol": "BTCUSDT",
                "side": side,
                "quantity": _decimal_text(qty),
                "quoteQuantity": _decimal_text(quote_qty),
                "commission": _decimal_text(commission),
                "commissionAsset": commission_asset,
                "feeQuoteValue": _decimal_text(fee_quote),
                "feeQuoteValueExact": commission_asset in {"BTC", "USDT"},
                "time": int(trade.get("time") or 0),
            }
        )
        seen_trade_ids.add(trade_id)

    stream_events = tuple(stream.events) if stream is not None else ()
    stream_execution = [
        event for event in stream_events if event.get("eventType") == "executionReport"
    ]
    if any(
        int(event.get("eventTime") or 0) > int(cutoff * 1000) + 1000
        for event in stream_events
    ):
        raise BinanceSpotTruthError(
            "user-data activity occurred after the REST history cutoff"
        )
    owned_orders = [
        row
        for row in [*normalized_open, *normalized_closed]
        if owner_prefix and _text(row.get("clientOrderId")).startswith(owner_prefix)
    ]
    for order in (owned_orders if not recovery_only else ()):
        matches = [
            event
            for event in stream_execution
            if _text(event.get("orderId")) == order["orderId"]
            and _text(event.get("clientOrderId")) == order["clientOrderId"]
            and _upper(event.get("symbol")) == "BTCUSDT"
        ]
        if not matches:
            raise BinanceSpotTruthError("owned REST order is absent from user-data stream")
        latest = max(matches, key=lambda item: int(item.get("eventTime") or 0))
        if _upper(latest.get("orderStatus")) != order["status"]:
            raise BinanceSpotTruthError("REST/user-stream terminal status disagrees")
    for fill in (normalized_fills if not recovery_only else ()):
        if owner_prefix and fill["clientOrderId"].startswith(owner_prefix):
            if not any(
                _text(event.get("tradeId")) == fill["tradeId"]
                and _text(event.get("orderId")) == fill["orderId"]
                and _decimal(event.get("lastQty"), label="stream lastQty")
                == _decimal(fill["quantity"], label="REST fill quantity")
                and _decimal(
                    event.get("commission"), label="stream commission"
                )
                == _decimal(fill["commission"], label="REST commission")
                and _upper(event.get("commissionAsset"))
                == fill["commissionAsset"]
                for event in stream_execution
            ):
                raise BinanceSpotTruthError("owned REST fill is absent from user-data stream")

    if not recovery_only and (owned_orders or any(
        owner_prefix and fill["clientOrderId"].startswith(owner_prefix)
        for fill in normalized_fills
    )):
        if not any(
            event.get("eventType") == "outboundAccountPosition"
            and int(event.get("eventTime") or 0) >= int(baseline_epoch * 1000)
            for event in stream_events
        ):
            raise BinanceSpotTruthError(
                "owned fill/order lacks user-stream account-position evidence"
            )

    baseline_ms = int(baseline_epoch * 1000)
    nonowned_activity = any(
        int(row.get("time") or row.get("updateTime") or 0) >= baseline_ms
        and not (owner_prefix and row["clientOrderId"].startswith(owner_prefix))
        for row in normalized_history
    )
    nonowned_activity = nonowned_activity or any(
        int(fill.get("time") or 0) >= baseline_ms
        and not (owner_prefix and fill["clientOrderId"].startswith(owner_prefix))
        for fill in normalized_fills
    )
    nonowned_activity = nonowned_activity or any(
        int(row.get("time") or row.get("updateTime") or 0) >= baseline_ms
        and not (owner_prefix and row["clientOrderId"].startswith(owner_prefix))
        for row in normalized_open
    )
    nonowned_activity = nonowned_activity or any(
        int(event.get("eventTime") or 0) >= baseline_ms
        and (
            event.get("eventType") == "balanceUpdate"
            or (
                event.get("eventType") == "executionReport"
                and not (
                    owner_prefix
                    and _text(event.get("clientOrderId")).startswith(owner_prefix)
                )
            )
        )
        for event in stream_events
    )
    external_absent = (
        (stream is not None and stream.external_activity_absent)
        or startup_abort_only
    ) and not nonowned_activity
    return {
        **expected_permission_proof,
        "accountSymbolPermissionProofHash": _stable_payload_hash(
            expected_permission_proof
        ),
        "observedAt": _iso(observed_epoch),
        "historyBaselineAt": _iso(baseline_epoch),
        "historyCutoffAt": _iso(cutoff),
        "broker": "BINANCE",
        "venue": "BINANCE_SPOT",
        "accountFingerprint": account_fingerprint,
        "accountComplete": True,
        "balancesComplete": True,
        "openOrdersComplete": True,
        "closedOrdersComplete": True,
        "fillsComplete": True,
        "feesComplete": True,
        "feeQuoteValuationComplete": fee_quote_valuation_complete,
        "balancesScope": "ACCOUNT_ALL_BALANCES",
        "openOrdersScope": "ACCOUNT_ALL_OPEN_ORDERS",
        "closedOrdersScope": "BTCUSDT_ALL_ORDERS_SINCE_BASELINE",
        "fillsScope": "BTCUSDT_ALL_TRADES_SINCE_BASELINE",
        "feesScope": "BTCUSDT_ALL_TRADE_FEES_SINCE_BASELINE",
        "balances": normalized_balances,
        "openOrders": normalized_open,
        "closedOrders": normalized_closed,
        "fills": normalized_fills,
        "markPrice": _decimal_text(mark),
        "externalActivityAbsent": external_absent,
        # Binance allOrders/myTrades are symbol-scoped and the generic time
        # RPC is not documented as a causal account-event barrier.  Keep the
        # terminal proof explicitly false until an official account-wide
        # causal primitive exists; core finalization then seals SAFE_INCOMPLETE.
        "accountWideCausalClosureProven": False,
        "restUserStreamCrossChecked": not recovery_only,
        "userStreamObservedAt": (
            _iso(stream.observed_epoch)
            if stream is not None
            else _text(recovery.get("recoveryObservedAt"))
        ),
        "streamSessionId": (
            stream.session_id if stream is not None else _text(recovery.get("sessionId"))
        ),
        "streamPermitId": (
            stream.permit_id if stream is not None else _text(recovery.get("permitId"))
        ),
        "streamPermitHash": (
            stream.permit_hash
            if stream is not None
            else _text(recovery.get("permitHash")).lower()
        ),
        "streamJournalSealHash": (
            stream.durable_journal_seal_hash
            if stream is not None
            else _text(recovery.get("preservedStreamJournalSealHash")).lower()
        ),
        "streamJournalEventCount": (
            stream.durable_journal_event_count
            if stream is not None
            else int(recovery.get("preservedStreamJournalEventCount") or 0)
        ),
        "cleanupRecoveryMode": (
            "REST_RECONCILED_CLEANUP_ONLY" if cleanup_recovery_only else ""
        ),
        "startupAbortAttestation": startup_abort_only,
        "preservedStreamGap": cleanup_recovery_only,
        "streamGapEvidenceHash": _text(
            recovery.get("streamGapEvidenceHash")
        ).lower(),
        "recoveryAttestationHash": _text(
            recovery.get("recoveryAttestationHash")
        ).lower(),
    }


__all__ = [
    "ALL_GET_ENDPOINTS",
    "BINANCE_SPOT_ACCOUNT_ENDPOINT",
    "BINANCE_SPOT_AVG_PRICE_ENDPOINT",
    "BINANCE_SPOT_ALL_ORDERS_ENDPOINT",
    "BINANCE_SPOT_EXCHANGE_INFO_ENDPOINT",
    "BINANCE_SPOT_KLINES_ENDPOINT",
    "BINANCE_SPOT_MY_TRADES_ENDPOINT",
    "BINANCE_SPOT_OPEN_ORDERS_ENDPOINT",
    "BINANCE_SPOT_QUERY_ORDER_ENDPOINT",
    "BINANCE_SPOT_PRODUCTION_ORIGIN",
    "BINANCE_SPOT_TICKER_PRICE_ENDPOINT",
    "BINANCE_SPOT_TIME_ENDPOINT",
    "BinanceSpotFunctionalUserStreamTracker",
    "BinanceSpotOfficialTruthReader",
    "BinanceSpotTruthError",
    "OfficialBinanceSpotGetClient",
    "UserStreamProof",
    "assemble_binance_spot_rules",
    "assemble_binance_spot_truth",
    "assert_binance_spot_production_origin",
    "binance_api_key_fingerprint",
    "build_binance_spot_get_request",
    "normalize_binance_user_stream_event",
]
