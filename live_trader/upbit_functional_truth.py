from __future__ import annotations

"""Read-only official Upbit truth builder for the continuous test lane.

The caller supplies an authenticated GET-only client.  This module never
constructs an order/cancel request and rejects pagination boundaries that
cannot prove absence.  Closed-order truth uses Upbit's documented maximum
1,000-row session query and fails closed when that maximum is reached because
the endpoint has no page parameter.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from typing import Any, Mapping, Protocol, Sequence

from .upbit_continuous_functional import SYMBOL, UpbitFunctionalBlocked


UPBIT_ACCOUNTS_ENDPOINT = "/v1/accounts"
UPBIT_ORDER_CHANCE_ENDPOINT = "/v1/orders/chance"
UPBIT_OPEN_ORDERS_ENDPOINT = "/v1/orders/open"
UPBIT_CLOSED_ORDERS_ENDPOINT = "/v1/orders/closed"
UPBIT_ORDER_DETAIL_ENDPOINT = "/v1/order"
UPBIT_TICKER_ENDPOINT = "/v1/ticker"
OPEN_PAGE_LIMIT = 100
CLOSED_LIMIT = 1000
MAX_OPEN_PAGES = 100
QUANTITY_STEP = Decimal("0.00000001")
QUANTITY_SCALE = 8
QUANTITY_RULE_SOURCE = "UPBIT OFFICIAL MARKET ORDER 8-DECIMAL POLICY"


class UpbitReadOnlyClient(Protocol):
    def get(
        self,
        endpoint: str,
        query: Sequence[tuple[str, str]],
    ) -> object: ...


class UpbitPrivateStreamReader(Protocol):
    def __call__(
        self,
        *,
        session_id: str,
        identifiers: tuple[str, ...],
    ) -> Mapping[str, Any]: ...


class UpbitAccountExclusivityProofReader(Protocol):
    def __call__(
        self,
        *,
        session_id: str,
        account_fingerprint: str,
        session_started_at: datetime,
        observation_started_at: datetime,
        observed_at: datetime,
    ) -> Mapping[str, Any]: ...


def _text(value: object) -> str:
    return str(value or "").strip()


def _upper(value: object) -> str:
    return _text(value).upper()


def _decimal(value: object, label: str, *, minimum: Decimal = Decimal("0")) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise UpbitFunctionalBlocked(f"upbit-official-{label}-invalid") from exc
    if not parsed.is_finite() or parsed < minimum:
        raise UpbitFunctionalBlocked(f"upbit-official-{label}-invalid")
    return parsed


def _decimal_text(value: Decimal) -> str:
    text = format(value.normalize(), "f")
    return "0" if text in {"", "-0"} else text


def _utc(value: datetime, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise UpbitFunctionalBlocked(f"upbit-official-{label}-timezone-missing")
    return value.astimezone(timezone.utc)


def _utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise UpbitFunctionalBlocked(f"upbit-official-{label}-not-object")
    if "error" in value:
        raise UpbitFunctionalBlocked(f"upbit-official-{label}-error")
    return {str(key): item for key, item in value.items()}


def _rows(value: object, label: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or any(not isinstance(row, Mapping) for row in value):
        raise UpbitFunctionalBlocked(f"upbit-official-{label}-not-list")
    return [dict(row) for row in value]


def _account_map(payload: object) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in _rows(payload, "accounts"):
        currency = _upper(row.get("currency"))
        if not currency or currency in result:
            raise UpbitFunctionalBlocked("upbit-official-account-currency-duplicate")
        _decimal(row.get("balance"), "account-balance")
        _decimal(row.get("locked"), "account-locked")
        result[currency] = row
    if "KRW" not in result or "BTC" not in result:
        raise UpbitFunctionalBlocked("upbit-official-krw-btc-accounts-required")
    return result


def _account_rows(accounts: Mapping[str, Mapping[str, Any]]) -> list[dict[str, str]]:
    rows = [
        {
            "currency": currency,
            "available": _decimal_text(
                _decimal(row.get("balance"), f"{currency.lower()}-balance")
            ),
            "locked": _decimal_text(
                _decimal(row.get("locked"), f"{currency.lower()}-locked")
            ),
        }
        for currency, row in accounts.items()
    ]
    return sorted(rows, key=lambda row: row["currency"])


def _account_rows_hash(rows: Sequence[Mapping[str, str]]) -> str:
    return hashlib.sha256(
        json.dumps(
            list(rows), sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
    ).hexdigest()


def _chance(payload: object, accounts: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    value = _mapping(payload, "order-chance")
    market = _mapping(value.get("market"), "order-chance-market")
    market_id = _upper(market.get("id") or market.get("market"))
    if market_id != SYMBOL:
        raise UpbitFunctionalBlocked("upbit-official-order-chance-market-mismatch")
    bid = _mapping(market.get("bid"), "order-chance-bid")
    ask = _mapping(market.get("ask"), "order-chance-ask")
    bid_types = market.get("bid_types")
    ask_types = market.get("ask_types")
    if (
        not isinstance(bid_types, list)
        or "price" not in {_text(item).lower() for item in bid_types}
        or not isinstance(ask_types, list)
        or "market" not in {_text(item).lower() for item in ask_types}
    ):
        raise UpbitFunctionalBlocked("upbit-official-required-order-types-unavailable")
    bid_account = _mapping(value.get("bid_account"), "order-chance-bid-account")
    ask_account = _mapping(value.get("ask_account"), "order-chance-ask-account")
    if _upper(bid_account.get("currency")) != "KRW" or _upper(ask_account.get("currency")) != "BTC":
        raise UpbitFunctionalBlocked("upbit-official-order-chance-currency-mismatch")
    quote_available = _decimal(bid_account.get("balance"), "bid-account-balance")
    base_available = _decimal(ask_account.get("balance"), "ask-account-balance")
    quote_account = accounts["KRW"]
    base_account = accounts["BTC"]
    if quote_available != _decimal(quote_account.get("balance"), "krw-account-balance"):
        raise UpbitFunctionalBlocked("upbit-official-quote-account-chance-mismatch")
    if base_available != _decimal(base_account.get("balance"), "btc-account-balance"):
        raise UpbitFunctionalBlocked("upbit-official-base-account-chance-mismatch")
    return {
        "quoteAvailable": quote_available,
        "baseAvailable": base_available,
        "baseTotal": base_available + _decimal(base_account.get("locked"), "btc-account-locked"),
        "bidMinTotal": _decimal(bid.get("min_total"), "bid-min-total", minimum=Decimal("0.00000001")),
        "askMinTotal": _decimal(ask.get("min_total"), "ask-min-total", minimum=Decimal("0.00000001")),
        "bidFeeRate": _decimal(value.get("bid_fee"), "bid-fee-rate"),
        "askFeeRate": _decimal(value.get("ask_fee"), "ask-fee-rate"),
    }


def _normalize_order(row: Mapping[str, Any]) -> dict[str, Any]:
    order = dict(row)
    uuid = _text(order.get("uuid"))
    identifier = _text(order.get("identifier"))
    market = _upper(order.get("market"))
    side = _upper(order.get("side"))
    state = _text(order.get("state")).lower()
    if (
        not uuid
        or not market
        or side not in {"BID", "ASK"}
        or state not in {"wait", "watch", "done", "cancel", "reject"}
    ):
        raise UpbitFunctionalBlocked("upbit-official-order-identity-or-state-incomplete")
    return {
        **order,
        "uuid": uuid,
        "identifier": identifier,
        "market": market,
        "side": side,
        "state": state,
    }


def _unique_orders(rows: Sequence[Mapping[str, Any]], label: str) -> list[dict[str, Any]]:
    normalized = [_normalize_order(row) for row in rows]
    uuids = [row["uuid"] for row in normalized]
    identifiers = [row["identifier"] for row in normalized if row["identifier"]]
    if len(uuids) != len(set(uuids)) or len(identifiers) != len(set(identifiers)):
        raise UpbitFunctionalBlocked(f"upbit-official-{label}-identity-duplicate")
    return normalized


def _fills(orders: Sequence[Mapping[str, Any]]) -> tuple[list[dict[str, str]], Decimal]:
    fills: list[dict[str, str]] = []
    total_fees = Decimal("0")
    for order in orders:
        raw_trades = order.get("trades")
        if not isinstance(raw_trades, list) or any(not isinstance(row, Mapping) for row in raw_trades):
            raise UpbitFunctionalBlocked("upbit-official-order-trades-incomplete")
        trades_count = int(_decimal(order.get("trades_count"), "trades-count"))
        if trades_count != len(raw_trades):
            raise UpbitFunctionalBlocked("upbit-official-order-trades-count-mismatch")
        paid_fee = _decimal(order.get("paid_fee"), "order-paid-fee")
        total_fees += paid_fee
        executed_volume = _decimal(
            order.get("executed_volume"), "order-executed-volume"
        )
        funds_values = [
            _decimal(row.get("funds"), "trade-funds", minimum=Decimal("0.00000001"))
            for row in raw_trades
        ]
        funds_total = sum(funds_values, Decimal("0"))
        volumes_total = sum(
            (
                _decimal(
                    row.get("volume"),
                    "trade-volume",
                    minimum=Decimal("0.00000001"),
                )
                for row in raw_trades
            ),
            Decimal("0"),
        )
        if executed_volume != volumes_total:
            raise UpbitFunctionalBlocked(
                "upbit-official-order-executed-volume-mismatch"
            )
        remaining_raw = order.get("remaining_volume")
        if remaining_raw not in {None, ""}:
            remaining_volume = _decimal(
                remaining_raw, "order-remaining-volume"
            )
            if (
                _text(order.get("state")).lower() == "done"
                and _text(order.get("ord_type")).lower() not in {"price"}
                and remaining_volume != 0
            ):
                raise UpbitFunctionalBlocked(
                    "upbit-official-terminal-order-remaining-volume-nonzero"
                )
        if order.get("executed_funds") is not None and _decimal(
            order.get("executed_funds"), "order-executed-funds"
        ) != funds_total:
            raise UpbitFunctionalBlocked(
                "upbit-official-order-executed-funds-mismatch"
            )
        allocated = Decimal("0")
        for index, (raw_trade, funds) in enumerate(zip(raw_trades, funds_values)):
            trade = dict(raw_trade)
            fee = (
                paid_fee - allocated
                if index == len(raw_trades) - 1
                else (paid_fee * funds / funds_total if funds_total else Decimal("0"))
            )
            allocated += fee
            trade_uuid = _text(trade.get("uuid") or trade.get("trade_uuid"))
            volume = _decimal(trade.get("volume"), "trade-volume", minimum=Decimal("0.00000001"))
            price = _decimal(
                trade.get("price"),
                "trade-price",
                minimum=Decimal("0.00000001"),
            )
            if price * volume != funds:
                raise UpbitFunctionalBlocked(
                    "upbit-official-trade-price-funds-mismatch"
                )
            if not trade_uuid:
                raise UpbitFunctionalBlocked("upbit-official-trade-uuid-missing")
            fills.append(
                {
                    "market": _upper(order.get("market")),
                    "tradeUuid": trade_uuid,
                    "orderUuid": _text(order.get("uuid")),
                    "identifier": _text(order.get("identifier")),
                    "side": _upper(order.get("side")),
                    "volume": _decimal_text(volume),
                    "funds": _decimal_text(funds),
                    "fee": _decimal_text(fee),
                }
            )
    trade_ids = [row["tradeUuid"] for row in fills]
    if len(trade_ids) != len(set(trade_ids)):
        raise UpbitFunctionalBlocked("upbit-official-trade-uuid-duplicate")
    return fills, total_fees


@dataclass(slots=True)
class OfficialUpbitFunctionalTruthReader:
    client: UpbitReadOnlyClient
    account_fingerprint: str
    session_started_at: datetime
    cleanup_deadline: datetime
    clock: Any
    private_stream_reader: UpbitPrivateStreamReader
    account_exclusivity_proof_reader: (
        UpbitAccountExclusivityProofReader | None
    ) = None
    cleanup_recovery: bool = False

    def _open_orders(
        self, raw_capture: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for page in range(1, MAX_OPEN_PAGES + 1):
            query = (
                ("states[]", "wait"),
                ("states[]", "watch"),
                ("page", str(page)),
                ("limit", str(OPEN_PAGE_LIMIT)),
                ("order_by", "asc"),
            )
            payload = self.client.get(
                UPBIT_OPEN_ORDERS_ENDPOINT,
                query,
            )
            if raw_capture is not None:
                raw_capture.setdefault("openOrderPages", []).append(
                    {
                        "page": page,
                        "endpoint": UPBIT_OPEN_ORDERS_ENDPOINT,
                        "query": [list(item) for item in query],
                        "payload": payload,
                    }
                )
            current = _rows(payload, "open-orders")
            rows.extend(current)
            if len(current) < OPEN_PAGE_LIMIT:
                return _unique_orders(rows, "open-orders")
        raise UpbitFunctionalBlocked("upbit-official-open-orders-pagination-exhausted")

    def _closed_orders(
        self,
        now: datetime,
        raw_capture: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        started_at = _utc(self.session_started_at, "session-started-at")
        if now < started_at or now - started_at > timedelta(days=7):
            raise UpbitFunctionalBlocked("upbit-official-closed-orders-window-invalid")
        query = (
                ("states[]", "done"),
                ("states[]", "cancel"),
                ("start_time", _utc_text(started_at)),
                ("end_time", _utc_text(now)),
                ("limit", str(CLOSED_LIMIT)),
                ("order_by", "asc"),
            )
        payload = self.client.get(UPBIT_CLOSED_ORDERS_ENDPOINT, query)
        if raw_capture is not None:
            raw_capture["closedOrders"] = {
                "endpoint": UPBIT_CLOSED_ORDERS_ENDPOINT,
                "query": [list(item) for item in query],
                "payload": payload,
            }
        rows = _rows(payload, "closed-orders")
        if len(rows) >= CLOSED_LIMIT:
            raise UpbitFunctionalBlocked("upbit-official-closed-orders-truncation-possible")
        return _unique_orders(rows, "closed-orders")

    def read_recovery_approval_attestation(
        self,
        *,
        session_id: str,
        identifiers: tuple[str, ...],
    ) -> dict[str, Any]:
        """Build fresh REST-only truth before a replacement WS is opened.

        This deliberately makes no private-stream continuity claim.  Its
        only purpose is to bind an owner-loss cleanup approval to complete
        official account/order/detail/fill/fee truth; the recovery graph must
        subsequently authenticate a new ``myOrder`` socket before mutation.
        """

        if not _text(session_id):
            raise UpbitFunctionalBlocked(
                "upbit-official-recovery-session-required"
            )
        started = _utc(self.clock(), "recovery-observation-started-at")
        deadline = _utc(self.cleanup_deadline, "cleanup-deadline")
        if started > deadline:
            raise UpbitFunctionalBlocked(
                "upbit-official-cleanup-deadline-expired"
            )
        accounts = _account_map(
            self.client.get(UPBIT_ACCOUNTS_ENDPOINT, ())
        )
        chance = _chance(
            self.client.get(
                UPBIT_ORDER_CHANCE_ENDPOINT, (("market", SYMBOL),)
            ),
            accounts,
        )
        ticker_rows = _rows(
            self.client.get(
                UPBIT_TICKER_ENDPOINT, (("markets", SYMBOL),)
            ),
            "ticker",
        )
        if (
            len(ticker_rows) != 1
            or _upper(ticker_rows[0].get("market")) != SYMBOL
        ):
            raise UpbitFunctionalBlocked(
                "upbit-official-ticker-scope-mismatch"
            )
        mark_price = _decimal(
            ticker_rows[0].get("trade_price"),
            "ticker-trade-price",
            minimum=Decimal("0.00000001"),
        )
        open_orders = self._open_orders()
        closed_orders = self._closed_orders(started)
        all_orders = [*open_orders, *closed_orders]
        all_uuids = [_text(row.get("uuid")) for row in all_orders]
        all_identifiers = [
            _text(row.get("identifier"))
            for row in all_orders
            if _text(row.get("identifier"))
        ]
        if (
            len(all_uuids) != len(set(all_uuids))
            or len(all_identifiers) != len(set(all_identifiers))
        ):
            raise UpbitFunctionalBlocked(
                "upbit-official-open-closed-order-overlap"
            )
        owned = {_text(identifier) for identifier in identifiers}
        if any(_text(row.get("identifier")) not in owned for row in all_orders):
            raise UpbitFunctionalBlocked(
                "upbit-official-external-account-order-activity"
            )
        order_index = {
            _text(row.get("identifier")): row
            for row in all_orders
            if _text(row.get("identifier"))
        }
        detailed_orders: list[dict[str, Any]] = []
        for listed in all_orders:
            if _upper(listed.get("market")) != SYMBOL:
                continue
            detail = _normalize_order(
                _mapping(
                    self.client.get(
                        UPBIT_ORDER_DETAIL_ENDPOINT,
                        (("uuid", _text(listed.get("uuid"))),),
                    ),
                    "recovery-order-detail-by-uuid",
                )
            )
            for field in ("uuid", "identifier", "market", "side", "state"):
                if _text(detail.get(field)).lower() != _text(
                    listed.get(field)
                ).lower():
                    raise UpbitFunctionalBlocked(
                        "upbit-official-order-detail-list-mismatch"
                    )
            detailed_orders.append(detail)
        identifier_truth: dict[str, dict[str, Any] | None] = {}
        details_by_identifier = {
            _text(row.get("identifier")): row
            for row in detailed_orders
            if _text(row.get("identifier"))
        }
        for identifier in identifiers:
            if not _text(identifier):
                raise UpbitFunctionalBlocked(
                    "upbit-official-identifier-empty"
                )
            raw = self.client.get(
                UPBIT_ORDER_DETAIL_ENDPOINT,
                (("identifier", identifier),),
            )
            if isinstance(raw, Mapping) and raw.get("_notFound") is True:
                if identifier in order_index:
                    raise UpbitFunctionalBlocked(
                        "upbit-official-identifier-absence-conflict"
                    )
                identifier_truth[identifier] = None
                continue
            detail = _normalize_order(
                _mapping(raw, "recovery-order-detail-by-identifier")
            )
            listed = order_index.get(identifier)
            if (
                detail["identifier"] != identifier
                or listed is None
                or detail["uuid"] != listed["uuid"]
                or details_by_identifier.get(identifier, {}).get("uuid")
                != detail["uuid"]
            ):
                raise UpbitFunctionalBlocked(
                    "upbit-official-order-detail-list-mismatch"
                )
            identifier_truth[identifier] = detail
        fills, total_fees = _fills(detailed_orders)
        observed = _utc(self.clock(), "recovery-observation-completed-at")
        duration = (observed - started).total_seconds()
        if duration < 0 or duration > 15:
            raise UpbitFunctionalBlocked(
                "upbit-official-truth-read-duration-exceeded"
            )
        return {
            "schemaVersion": "upbit-functional-rest-recovery-truth/v1",
            "broker": "UPBIT",
            "market": SYMBOL,
            "accountFingerprint": self.account_fingerprint,
            "sessionId": session_id,
            "observedAt": _utc_text(observed),
            "observationStartedAt": _utc_text(started),
            "truthReadDurationSeconds": duration,
            "accountComplete": True,
            "openOrdersComplete": True,
            "closedOrdersComplete": True,
            "fillsComplete": True,
            "feesComplete": True,
            "orderChanceComplete": True,
            "tickerComplete": True,
            "identifierTruthComplete": True,
            "privateStreamClaimed": False,
            "officialRestRecoveryOnly": True,
            "accountExternalActivityAbsent": True,
            "externalActivityScope": "UPBIT_ACCOUNT_ALL_MARKETS",
            "accountRows": _account_rows(accounts),
            "accountRowsHash": _account_rows_hash(_account_rows(accounts)),
            "quoteAvailable": _decimal_text(chance["quoteAvailable"]),
            "baseAvailable": _decimal_text(chance["baseAvailable"]),
            "baseTotal": _decimal_text(chance["baseTotal"]),
            "markPrice": _decimal_text(mark_price),
            "orderRules": {
                "bidMinTotal": _decimal_text(chance["bidMinTotal"]),
                "askMinTotal": _decimal_text(chance["askMinTotal"]),
                "quantityStep": _decimal_text(QUANTITY_STEP),
                "quantityScale": QUANTITY_SCALE,
                "bidFeeRate": _decimal_text(chance["bidFeeRate"]),
                "askFeeRate": _decimal_text(chance["askFeeRate"]),
            },
            "openOrders": open_orders,
            "closedOrders": closed_orders,
            "fills": fills,
            "totalFees": _decimal_text(total_fees),
            "identifierTruth": identifier_truth,
        }

    def __call__(
        self,
        *,
        session_id: str,
        phase: str,
        identifiers: tuple[str, ...],
    ) -> Mapping[str, Any]:
        if not _text(session_id) or not _text(phase):
            raise UpbitFunctionalBlocked("upbit-official-session-phase-required")
        now = _utc(self.clock(), "observation-time")
        deadline = _utc(self.cleanup_deadline, "cleanup-deadline")
        if now > deadline:
            raise UpbitFunctionalBlocked("upbit-official-cleanup-deadline-expired")
        raw_rest: dict[str, Any] = {
            "schemaVersion": "upbit-functional-official-rest-raw/v2",
            "sessionId": session_id,
            "accountFingerprint": self.account_fingerprint,
            "sessionStartedAt": _utc_text(
                _utc(self.session_started_at, "session-started-at")
            ),
            "observationCutoff": _utc_text(now),
            "openOrderPages": [],
            "detailsByUuid": [],
            "detailsByIdentifier": [],
        }
        accounts_payload = self.client.get(UPBIT_ACCOUNTS_ENDPOINT, ())
        raw_rest["accounts"] = {
            "endpoint": UPBIT_ACCOUNTS_ENDPOINT,
            "query": [],
            "payload": accounts_payload,
        }
        accounts = _account_map(accounts_payload)
        chance_payload = self.client.get(
            UPBIT_ORDER_CHANCE_ENDPOINT, (("market", SYMBOL),)
        )
        raw_rest["orderChance"] = {
            "endpoint": UPBIT_ORDER_CHANCE_ENDPOINT,
            "query": [["market", SYMBOL]],
            "payload": chance_payload,
        }
        chance = _chance(
            chance_payload,
            accounts,
        )
        ticker_payload = self.client.get(
            UPBIT_TICKER_ENDPOINT, (("markets", SYMBOL),)
        )
        raw_rest["ticker"] = {
            "endpoint": UPBIT_TICKER_ENDPOINT,
            "query": [["markets", SYMBOL]],
            "payload": ticker_payload,
        }
        ticker_rows = _rows(
            ticker_payload,
            "ticker",
        )
        if len(ticker_rows) != 1 or _upper(ticker_rows[0].get("market")) != SYMBOL:
            raise UpbitFunctionalBlocked("upbit-official-ticker-scope-mismatch")
        mark_price = _decimal(
            ticker_rows[0].get("trade_price"),
            "ticker-trade-price",
            minimum=Decimal("0.00000001"),
        )
        open_orders = self._open_orders(raw_rest)
        closed_orders = self._closed_orders(now, raw_rest)
        all_orders = [*open_orders, *closed_orders]
        all_uuids = [_text(row.get("uuid")) for row in all_orders]
        all_identifiers = [
            _text(row.get("identifier"))
            for row in all_orders
            if _text(row.get("identifier"))
        ]
        if (
            len(all_uuids) != len(set(all_uuids))
            or len(all_identifiers) != len(set(all_identifiers))
        ):
            raise UpbitFunctionalBlocked("upbit-official-open-closed-order-overlap")
        order_index = {
            _text(row.get("identifier")): row
            for row in all_orders
            if _text(row.get("identifier"))
        }
        owned_identifiers = {_text(identifier) for identifier in identifiers}
        if any(
            _text(row.get("identifier")) not in owned_identifiers
            for row in all_orders
        ):
            raise UpbitFunctionalBlocked(
                "upbit-official-external-account-order-activity"
            )
        detailed_orders: list[dict[str, Any]] = []
        detailed_by_uuid: dict[str, dict[str, Any]] = {}
        for listed in all_orders:
            if _upper(listed.get("market")) != SYMBOL:
                continue
            detail_payload = self.client.get(
                        UPBIT_ORDER_DETAIL_ENDPOINT,
                        (("uuid", _text(listed.get("uuid"))),),
                    )
            raw_rest["detailsByUuid"].append(
                {
                    "uuid": _text(listed.get("uuid")),
                    "endpoint": UPBIT_ORDER_DETAIL_ENDPOINT,
                    "query": [["uuid", _text(listed.get("uuid"))]],
                    "payload": detail_payload,
                }
            )
            detail = _normalize_order(
                _mapping(
                    detail_payload,
                    "order-detail-by-uuid",
                )
            )
            for field in ("uuid", "identifier", "market", "side", "state"):
                if _text(detail.get(field)).lower() != _text(
                    listed.get(field)
                ).lower():
                    raise UpbitFunctionalBlocked(
                        "upbit-official-order-detail-list-mismatch"
                    )
            detailed_orders.append(detail)
            detailed_by_uuid[detail["uuid"]] = detail
        identifier_truth: dict[str, dict[str, Any] | None] = {}
        for identifier in identifiers:
            if not _text(identifier):
                raise UpbitFunctionalBlocked("upbit-official-identifier-empty")
            detail = self.client.get(
                UPBIT_ORDER_DETAIL_ENDPOINT,
                (("identifier", identifier),),
            )
            raw_rest["detailsByIdentifier"].append(
                {
                    "identifier": identifier,
                    "endpoint": UPBIT_ORDER_DETAIL_ENDPOINT,
                    "query": [["identifier", identifier]],
                    "payload": detail,
                }
            )
            if isinstance(detail, Mapping) and detail.get("_notFound") is True:
                if identifier in order_index:
                    raise UpbitFunctionalBlocked("upbit-official-identifier-absence-conflict")
                identifier_truth[identifier] = None
                continue
            normalized = _normalize_order(_mapping(detail, "order-detail"))
            if normalized["identifier"] != identifier:
                raise UpbitFunctionalBlocked("upbit-official-order-detail-identifier-mismatch")
            listed = order_index.get(identifier)
            detailed = detailed_by_uuid.get(normalized["uuid"])
            if (
                listed is None
                or listed["uuid"] != normalized["uuid"]
                or detailed is None
                or detailed["identifier"] != identifier
            ):
                raise UpbitFunctionalBlocked("upbit-official-order-detail-list-mismatch")
            identifier_truth[identifier] = normalized
        fills, total_fees = _fills(detailed_orders)
        private = _mapping(
            self.private_stream_reader(
                session_id=session_id,
                identifiers=identifiers,
            ),
            "private-myorder-stream",
        )
        normal_stream = (
            private.get("connected") is True
            and private.get("authenticated") is True
            and private.get("eventsComplete") is True
            and private.get("gapDetected") is False
            and private.get("cleanupOnlyRecovery") is not True
        )
        recovery_stream = (
            self.cleanup_recovery
            and phase.upper() in {"CLEANUP", "FINAL", "FINAL_PRE_POST", "POST_SUBMIT"}
            and private.get("connected") is True
            and private.get("authenticated") is True
            and private.get("eventsComplete") is False
            and private.get("gapDetected") is True
            and private.get("cleanupOnlyRecovery") is True
        )
        if (
            not (normal_stream or recovery_stream)
            or private.get("externalActivityAbsent") is not True
            or _upper(private.get("channel")) != "MYORDER"
            or _text(private.get("accountFingerprint"))
            != self.account_fingerprint
        ):
            raise UpbitFunctionalBlocked(
                "upbit-official-private-myorder-attestation-invalid"
            )
        try:
            private_started_at = datetime.fromisoformat(
                _text(private.get("startedAt")).replace("Z", "+00:00")
            )
            private_observed_at = datetime.fromisoformat(
                _text(private.get("observedAt")).replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise UpbitFunctionalBlocked(
                "upbit-official-private-myorder-time-attestation-invalid"
            ) from exc
        if (
            private_started_at.tzinfo is None
            or private_started_at.astimezone(timezone.utc)
            > _utc(self.session_started_at, "session-started-at")
            or private_observed_at.tzinfo is None
            or abs((now - private_observed_at.astimezone(timezone.utc)).total_seconds())
            > 15
        ):
            raise UpbitFunctionalBlocked(
                "upbit-official-private-myorder-time-attestation-invalid"
            )
        private_events = _rows(
            private.get("events"),
            "private-myorder-events",
        )
        try:
            private_writer_generation = int(private.get("writerGeneration"))
            private_revision = int(private.get("journalRevision"))
            private_event_cursor = int(private.get("eventCursor"))
        except (TypeError, ValueError) as exc:
            raise UpbitFunctionalBlocked(
                "upbit-official-private-myorder-cursor-invalid"
            ) from exc
        private_last_event_id = _text(private.get("lastEventId"))
        private_event_head_hash = _text(private.get("eventHeadHash")).lower()
        if (
            private_writer_generation < 1
            or private_revision < 1
            or private_event_cursor != len(private_events)
            or private_last_event_id
            != (
                _text(private_events[-1].get("eventId"))
                if private_events
                else ""
            )
            or len(private_event_head_hash) != 64
        ):
            raise UpbitFunctionalBlocked(
                "upbit-official-private-myorder-cursor-invalid"
            )
        private_fill_keys = {
            (
                _text(row.get("orderUuid")),
                _text(row.get("tradeUuid")),
                _text(row.get("identifier")),
            )
            for row in private_events
            if _text(row.get("tradeUuid"))
        }
        for fill in fills:
            if not recovery_stream and _text(fill.get("identifier")) in identifiers and (
                _text(fill.get("orderUuid")),
                _text(fill.get("tradeUuid")),
                _text(fill.get("identifier")),
            ) not in private_fill_keys:
                raise UpbitFunctionalBlocked(
                    "upbit-official-private-myorder-fill-missing"
                )
        observed_at = _utc(self.clock(), "observation-completed-at")
        truth_read_seconds = (observed_at - now).total_seconds()
        if truth_read_seconds < 0 or truth_read_seconds > 15:
            raise UpbitFunctionalBlocked(
                "upbit-official-truth-read-duration-exceeded"
            )
        result = {
            "broker": "UPBIT",
            "market": SYMBOL,
            "accountFingerprint": self.account_fingerprint,
            "observedAt": _utc_text(observed_at),
            "observationStartedAt": _utc_text(now),
            "truthReadDurationSeconds": truth_read_seconds,
            "accountComplete": True,
            "openOrdersComplete": True,
            "closedOrdersComplete": True,
            "fillsComplete": True,
            "feesComplete": True,
            "orderChanceComplete": True,
            "tickerComplete": True,
            "identifierTruthComplete": True,
            "privateStreamComplete": normal_stream,
            "privateStreamGapDetected": recovery_stream,
            "privateStreamRecoveryAttested": recovery_stream,
            "privateStreamExternalActivityAbsent": True,
            "accountExternalActivityAbsent": True,
            "externalActivityScope": "UPBIT_ACCOUNT_ALL_MARKETS",
            "accountRows": _account_rows(accounts),
            "accountRowsHash": _account_rows_hash(_account_rows(accounts)),
            "accountSource": "GET /v1/accounts",
            "orderChanceSource": "GET /v1/orders/chance",
            "tickerSource": "GET /v1/ticker",
            "quantityRuleSource": QUANTITY_RULE_SOURCE,
            "openOrdersScope": "ACCOUNT_ALL_OPEN_ORDERS",
            "closedOrdersScope": "ACCOUNT_SESSION_INTERVAL",
            "fillsScope": "ACCOUNT_SESSION_INTERVAL",
            "identifierTruthScope": "ALL_OWNED_IDENTIFIERS",
            "privateStreamConnected": True,
            "privateStreamAuthenticated": True,
            "privateStreamSource": "UPBIT_WEBSOCKET_MYORDER",
            "privateStreamScope": "ACCOUNT_MYORDER_SESSION",
            "privateStreamWriterGeneration": private_writer_generation,
            "privateStreamRevision": private_revision,
            "privateStreamEventCursor": private_event_cursor,
            "privateStreamLastEventId": private_last_event_id,
            "privateStreamEventHeadHash": private_event_head_hash,
            "quoteAvailable": _decimal_text(chance["quoteAvailable"]),
            "baseAvailable": _decimal_text(chance["baseAvailable"]),
            "baseTotal": _decimal_text(chance["baseTotal"]),
            "markPrice": _decimal_text(mark_price),
            "orderRules": {
                "bidMinTotal": _decimal_text(chance["bidMinTotal"]),
                "askMinTotal": _decimal_text(chance["askMinTotal"]),
                "quantityStep": _decimal_text(QUANTITY_STEP),
                "quantityScale": QUANTITY_SCALE,
                "bidFeeRate": _decimal_text(chance["bidFeeRate"]),
                "askFeeRate": _decimal_text(chance["askFeeRate"]),
            },
            "openOrders": open_orders,
            "closedOrders": closed_orders,
            "fills": fills,
            "totalFees": _decimal_text(total_fees),
            "identifierTruth": identifier_truth,
            "privateStreamEvents": private_events,
            "officialRestRawSnapshot": raw_rest,
            "officialRestRawSnapshotHash": hashlib.sha256(
                json.dumps(
                    raw_rest,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode("utf-8")
            ).hexdigest(),
        }
        if self.account_exclusivity_proof_reader is not None:
            # Bind the detached proof to the exact timestamps serialized in
            # this truth record.  Millisecond normalization prevents a proof
            # from signing sub-millisecond values that the durable truth does
            # not retain.
            proof_started_at = datetime.fromisoformat(
                _utc_text(now).replace("Z", "+00:00")
            )
            proof_observed_at = datetime.fromisoformat(
                _utc_text(observed_at).replace("Z", "+00:00")
            )
            proof = self.account_exclusivity_proof_reader(
                session_id=session_id,
                account_fingerprint=self.account_fingerprint,
                session_started_at=_utc(
                    self.session_started_at, "session-started-at"
                ),
                observation_started_at=proof_started_at,
                observed_at=proof_observed_at,
            )
            if not isinstance(proof, Mapping):
                raise UpbitFunctionalBlocked(
                    "upbit-official-account-exclusivity-proof-not-object"
                )
            result["accountExclusivityProof"] = dict(proof)
        return result


__all__ = [
    "CLOSED_LIMIT",
    "OPEN_PAGE_LIMIT",
    "OfficialUpbitFunctionalTruthReader",
    "UpbitAccountExclusivityProofReader",
    "QUANTITY_RULE_SOURCE",
    "UPBIT_ACCOUNTS_ENDPOINT",
    "UPBIT_CLOSED_ORDERS_ENDPOINT",
    "UPBIT_OPEN_ORDERS_ENDPOINT",
    "UPBIT_ORDER_CHANCE_ENDPOINT",
    "UPBIT_ORDER_DETAIL_ENDPOINT",
    "UPBIT_TICKER_ENDPOINT",
]
