from __future__ import annotations

"""Durable, single-session bootstrap for the first Binance live E2E.

The permanent REAL_E2E release bit is evidence, not an input.  This store
therefore permits exactly one server-selected session to bypass *only* that
bit while every other production, isolation, emergency, account, publication,
and code-identity gate is already true.  The raw capability is never stored;
crash loss burns the bootstrap instead of making it reconstructable.
"""

from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import sqlite3
import threading
import time
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Mapping, Sequence

from .binance_spot_continuous_functional import ExactBinding
from .binance_spot_functional_transport import assemble_binance_spot_rules


ROUTE_KEY = "BINANCE_SPOT_CONTINUOUS:BTCUSDT:5m"
SCHEMA_VERSION = "binance-spot-first-live-bootstrap/v1"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_GATE_FIELDS = frozenset(
    {
        "allOtherProductionComponentsAvailable",
        "ordinaryBinanceRoutesClosed",
        "emergencyKillInactive",
        "applicationInstanceLeaseHeld",
        "operatorApprovalBound",
        "accountExclusivityVerifierReady",
        "accountExclusivityDurableProviderReady",
        "accountExclusivitySigningPrimitiveAbsent",
        "accountExclusivityAuthorityPinned",
        "accountIdentityPinned",
        "globalFirstLiveAuthorityReaderWired",
        "realE2EAvailable",
        "firstLiveBootstrapFeatureEnabled",
    }
)
_BOOTSTRAP_TABLE = "binance_spot_first_live_bootstraps"
_ROUTE_INDEX = "ux_binance_spot_first_live_ever_route"
_SCHEMA_OBJECT_PREFIX = "binance_spot_first_live_"
_BOOTSTRAP_SCHEMA_SQL = (
    """
    CREATE TABLE IF NOT EXISTS binance_spot_first_live_bootstraps (
        bootstrap_id TEXT PRIMARY KEY,
        bootstrap_hash TEXT NOT NULL UNIQUE,
        route_key TEXT NOT NULL,
        approval_id TEXT NOT NULL UNIQUE,
        initial_permit_id TEXT NOT NULL,
        initial_permit_hash TEXT NOT NULL,
        active_permit_id TEXT NOT NULL DEFAULT '',
        active_permit_hash TEXT NOT NULL DEFAULT '',
        session_id TEXT NOT NULL DEFAULT '',
        account_fingerprint TEXT NOT NULL,
        binding_hash TEXT NOT NULL,
        binding_json TEXT NOT NULL,
        code_hash TEXT NOT NULL,
        capability_hash TEXT NOT NULL,
        claim_token_hash TEXT NOT NULL DEFAULT '',
        record_json TEXT NOT NULL,
        state TEXT NOT NULL,
        issued_epoch REAL NOT NULL,
        expires_epoch REAL NOT NULL,
        activated_epoch REAL,
        active_ends_epoch REAL,
        terminal_observed_epoch REAL,
        updated_epoch REAL NOT NULL,
        final_evidence_hash TEXT NOT NULL DEFAULT '',
        e2e_evidence_eligible INTEGER NOT NULL DEFAULT 0,
        functional_wiring_passed INTEGER NOT NULL DEFAULT 0,
        detail TEXT NOT NULL DEFAULT ''
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS
        ux_binance_spot_first_live_ever_route
    ON binance_spot_first_live_bootstraps(route_key)
    """,
)


class BinanceSpotFirstLiveBootstrapError(RuntimeError):
    pass


def _text(value: object) -> str:
    return str(value or "").strip()


def _canonical(value: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _hash_text(value: str) -> str:
    return hashlib.sha256(_text(value).encode("utf-8")).hexdigest()


def _hash_document(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _normalize_sql(value: object) -> str:
    return " ".join(_text(value).split()).lower()


def _schema_snapshot(connection: sqlite3.Connection) -> dict[str, Any]:
    objects = [
        (str(row[0]), str(row[1]), str(row[2]), _normalize_sql(row[3]))
        for row in connection.execute(
            "SELECT name,type,tbl_name,sql FROM sqlite_master "
            "WHERE name LIKE ? OR tbl_name LIKE ? OR name=? "
            "ORDER BY type,name",
            (
                _SCHEMA_OBJECT_PREFIX + "%",
                _SCHEMA_OBJECT_PREFIX + "%",
                _ROUTE_INDEX,
            ),
        ).fetchall()
    ]
    tables = sorted({row[2] for row in objects if row[1] == "table"})
    columns = {
        table: [
            tuple(item)
            for item in connection.execute(
                f'PRAGMA table_xinfo("{table}")'
            ).fetchall()
        ]
        for table in tables
    }
    indexes = {
        table: sorted(
            (
                tuple(item)
                for item in connection.execute(
                    f'PRAGMA index_list("{table}")'
                ).fetchall()
            ),
            key=lambda item: str(item[1]),
        )
        for table in tables
    }
    index_names = sorted(
        {str(row[1]) for rows in indexes.values() for row in rows}
    )
    index_info = {
        name: [
            tuple(item)
            for item in connection.execute(
                f'PRAGMA index_xinfo("{name}")'
            ).fetchall()
        ]
        for name in index_names
    }
    foreign_keys = {
        table: [
            tuple(item)
            for item in connection.execute(
                f'PRAGMA foreign_key_list("{table}")'
            ).fetchall()
        ]
        for table in tables
    }
    table_flags = sorted(
        (
            tuple(row)
            for row in connection.execute("PRAGMA table_list").fetchall()
            if str(row[1]) in tables
        ),
        key=lambda item: str(item[1]),
    )
    return {
        "objects": objects,
        "columns": columns,
        "indexes": indexes,
        "indexInfo": index_info,
        "foreignKeys": foreign_keys,
        "tableFlags": table_flags,
    }


def _expected_schema() -> tuple[dict[str, Any], str]:
    connection = sqlite3.connect(":memory:")
    try:
        for statement in _BOOTSTRAP_SCHEMA_SQL:
            connection.execute(statement)
        snapshot = _schema_snapshot(connection)
        return snapshot, _hash_document(snapshot)
    finally:
        connection.close()


_EXPECTED_BOOTSTRAP_SCHEMA, BOOTSTRAP_DB_SCHEMA_FINGERPRINT = _expected_schema()


def _utc(epoch: float) -> str:
    return datetime.fromtimestamp(float(epoch), tz=timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )


def _terminal_decimal(value: object, *, label: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise BinanceSpotFirstLiveBootstrapError(
            f"terminal {label} is invalid"
        ) from exc
    if not parsed.is_finite():
        raise BinanceSpotFirstLiveBootstrapError(
            f"terminal {label} is non-finite"
        )
    return parsed


def _terminal_decimal_text(value: Decimal) -> str:
    text = format(value.normalize(), "f")
    return "0" if text in {"", "-0"} else text


def _stream_journal_seal(
    meta: Mapping[str, Any], events: Sequence[Mapping[str, Any]]
) -> str:
    """Recompute the stream journal's pre-retirement continuity seal."""

    material = {
        "routeKey": ROUTE_KEY,
        "accountFingerprint": _text(meta.get("account_fingerprint")),
        "writerId": _text(meta.get("writer_id")),
        "ownerPrefix": _text(meta.get("owner_prefix")),
        "sessionId": _text(meta.get("session_id")),
        "permitId": _text(meta.get("permit_id")),
        "permitHash": _text(meta.get("permit_hash")).lower(),
        "subscribedEpoch": float(meta.get("subscribed_epoch") or 0),
        "connected": bool(meta.get("connected")),
        "authenticated": bool(meta.get("authenticated")),
        "gapDetected": bool(meta.get("gap_detected")),
        "externalActivityAbsent": bool(meta.get("external_activity_absent")),
        "retired": bool(meta.get("retired")),
        "terminalMarkerId": _text(meta.get("terminal_marker_id")),
        "terminalMarkerServerEpoch": float(
            meta.get("terminal_marker_server_epoch") or 0
        ),
        "terminalMarkerEpoch": float(meta.get("terminal_marker_epoch") or 0),
        "events": [
            {
                "eventId": _text(row.get("event_id")),
                "eventEpoch": float(row.get("event_epoch") or 0),
                "payloadJson": _text(row.get("payload_json")),
            }
            for row in events
        ],
    }
    return _hash_document(material)


def _terminal_client_id(value: Mapping[str, Any]) -> str:
    return _text(value.get("clientOrderId") or value.get("origClientOrderId"))


def _bootstrap_normalize_order(
    row: Mapping[str, Any], *, exact_symbol: bool
) -> dict[str, Any]:
    symbol = _text(row.get("symbol")).upper()
    order_id = _text(row.get("orderId"))
    client_id = _text(row.get("clientOrderId"))
    side = _text(row.get("side")).upper()
    order_type = _text(row.get("type")).upper()
    status = _text(row.get("status")).upper()
    if status == "CANCELLED":
        status = "CANCELED"
    valid_statuses = {
        "NEW",
        "PENDING_NEW",
        "PARTIALLY_FILLED",
        "PENDING_CANCEL",
        "FILLED",
        "CANCELED",
        "REJECTED",
        "EXPIRED",
    }
    if (
        (exact_symbol and symbol != "BTCUSDT")
        or not symbol
        or not order_id
        or not client_id
        or side not in {"BUY", "SELL"}
        or not order_type
        or status not in valid_statuses
    ):
        raise BinanceSpotFirstLiveBootstrapError(
            "raw official order identity/status is invalid"
        )
    decimal_fields: dict[str, str] = {}
    for field in (
        "origQty",
        "executedQty",
        "origQuoteOrderQty",
        "cummulativeQuoteQty",
    ):
        value = _terminal_decimal(row.get(field, "0"), label=f"raw order {field}")
        if value < 0:
            raise BinanceSpotFirstLiveBootstrapError(
                "raw official order quantity is negative"
            )
        decimal_fields[field] = _text(row.get(field) or "0")
    return {
        "orderId": order_id,
        "clientOrderId": client_id,
        "symbol": symbol,
        "product": "SPOT",
        "side": side,
        "type": order_type,
        "status": status,
        **decimal_fields,
        "time": int(row.get("time") or 0),
        "updateTime": int(row.get("updateTime") or 0),
        "isMargin": False,
        "reduceOnly": False,
    }


def _raw_history_from_pages(
    *,
    snapshot: Mapping[str, Any],
    envelopes: Mapping[str, Any],
    key: str,
    endpoint: str,
    cursor_parameter: str,
    row_id_field: str,
) -> list[dict[str, Any]]:
    pages = envelopes.get(key)
    if not isinstance(pages, list) or not pages:
        raise BinanceSpotFirstLiveBootstrapError(
            f"raw official {key} pagination proof is absent"
        )
    start_ms = int(float(snapshot.get("baselineEpoch") or 0) * 1000)
    end_ms = int(float(snapshot.get("historyCutoffEpoch") or 0) * 1000)
    expected_cursor: int | None = None
    prior_time = start_ms - 1
    collected: list[dict[str, Any]] = []
    seen: set[int] = set()
    for index, value in enumerate(pages):
        if not isinstance(value, Mapping):
            raise BinanceSpotFirstLiveBootstrapError(
                f"raw official {key} page is malformed"
            )
        page = dict(value)
        rows = page.get("responseRows")
        query = page.get("query")
        if not isinstance(rows, list) or any(
            not isinstance(row, Mapping) for row in rows
        ) or not isinstance(query, Mapping):
            raise BinanceSpotFirstLiveBootstrapError(
                f"raw official {key} page rows/query are malformed"
            )
        expected_query: dict[str, Any] = {"symbol": "BTCUSDT", "limit": 1000}
        if expected_cursor is None:
            expected_query.update({"startTime": start_ms, "endTime": end_ms})
        else:
            expected_query[cursor_parameter] = expected_cursor
        completion = _text(page.get("completion")).upper()
        if (
            _text(page.get("endpoint")) != endpoint
            or dict(query) != expected_query
            or _text(page.get("cursorParameter")) != cursor_parameter
            or _text(page.get("rowIdField")) != row_id_field
            or int(page.get("pageIndex") or 0) != index
            or int(page.get("responseCount") or 0) != len(rows)
            or not secrets.compare_digest(
                _text(page.get("responseHash")).lower(),
                _hash_document({"rows": list(rows)}),
            )
            or completion
            not in {"EMPTY_PAGE", "SHORT_PAGE", "ROW_AFTER_CUTOFF", "CONTINUE"}
        ):
            raise BinanceSpotFirstLiveBootstrapError(
                f"raw official {key} request/page envelope changed"
            )
        page_ids: list[int] = []
        after_cutoff = False
        for row_value in rows:
            row = dict(row_value)
            try:
                row_id = int(str(row.get(row_id_field)))
            except (TypeError, ValueError) as exc:
                raise BinanceSpotFirstLiveBootstrapError(
                    f"raw official {key} row id is invalid"
                ) from exc
            row_time = int(row.get("time") or row.get("updateTime") or 0)
            if (
                row_id < 0
                or row_time <= 0
                or (page_ids and row_id <= page_ids[-1])
                or row_time < prior_time
                or row_time < start_ms
            ):
                raise BinanceSpotFirstLiveBootstrapError(
                    f"raw official {key} pagination order changed"
                )
            page_ids.append(row_id)
            prior_time = row_time
            if row_time > end_ms:
                after_cutoff = True
                continue
            if after_cutoff or row_id in seen:
                raise BinanceSpotFirstLiveBootstrapError(
                    f"raw official {key} window/identity changed"
                )
            seen.add(row_id)
            collected.append(row)
        is_last = index == len(pages) - 1
        if completion == "CONTINUE":
            if is_last or len(rows) != 1000 or not page_ids:
                raise BinanceSpotFirstLiveBootstrapError(
                    f"raw official {key} pagination is truncated"
                )
            expected_cursor = page_ids[-1] + 1
            if int(page.get("nextCursor") or -1) != expected_cursor:
                raise BinanceSpotFirstLiveBootstrapError(
                    f"raw official {key} cursor changed"
                )
        else:
            if not is_last:
                raise BinanceSpotFirstLiveBootstrapError(
                    f"raw official {key} terminated before its last page"
                )
            if completion == "EMPTY_PAGE" and rows:
                raise BinanceSpotFirstLiveBootstrapError(
                    f"raw official {key} empty-page proof changed"
                )
            if completion == "SHORT_PAGE" and len(rows) >= 1000:
                raise BinanceSpotFirstLiveBootstrapError(
                    f"raw official {key} short-page proof changed"
                )
            if completion == "ROW_AFTER_CUTOFF" and not after_cutoff:
                raise BinanceSpotFirstLiveBootstrapError(
                    f"raw official {key} cutoff proof changed"
                )
    return collected


def _verify_official_rest_snapshot(
    terminal_truth: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    snapshot_value = terminal_truth.get("officialRestSnapshot")
    if not isinstance(snapshot_value, Mapping):
        raise BinanceSpotFirstLiveBootstrapError(
            "raw official REST response set is absent"
        )
    snapshot = dict(snapshot_value)
    snapshot_hash = _text(terminal_truth.get("officialRestTruthHash")).lower()
    if (
        snapshot.get("schemaVersion")
        != "binance-spot-functional-official-rest-set/v1"
        or _SHA256_RE.fullmatch(snapshot_hash) is None
        or not secrets.compare_digest(_hash_document(snapshot), snapshot_hash)
        or float(snapshot.get("baselineEpoch") or 0)
        != float(terminal_truth.get("historyBaselineEpoch") or 0)
        or float(snapshot.get("historyCutoffEpoch") or 0)
        != float(terminal_truth.get("historyCutoffEpoch") or 0)
        or float(snapshot.get("observedEpoch") or 0)
        != float(terminal_truth.get("observedEpoch") or 0)
    ):
        raise BinanceSpotFirstLiveBootstrapError(
            "raw official REST response-set hash/cutoff changed"
        )
    account = snapshot.get("account")
    raw_open = snapshot.get("openOrders")
    exchange_info = snapshot.get("exchangeInfo")
    ticker = snapshot.get("tickerPrice")
    envelopes = snapshot.get("requestEnvelopes")
    if (
        not isinstance(account, Mapping)
        or not isinstance(raw_open, list)
        or any(not isinstance(row, Mapping) for row in raw_open)
        or not isinstance(exchange_info, Mapping)
        or not isinstance(ticker, Mapping)
        or not isinstance(envelopes, Mapping)
    ):
        raise BinanceSpotFirstLiveBootstrapError(
            "raw official REST response set is malformed"
        )
    simple_specs = (
        ("account", "/api/v3/account", {"omitZeroBalances": False}, account),
        ("openOrders", "/api/v3/openOrders", {}, {"rows": raw_open}),
        ("exchangeInfo", "/api/v3/exchangeInfo", {"symbol": "BTCUSDT"}, exchange_info),
        ("tickerPrice", "/api/v3/ticker/price", {"symbol": "BTCUSDT"}, ticker),
    )
    for key, endpoint, query, response in simple_specs:
        envelope = envelopes.get(key)
        if (
            not isinstance(envelope, Mapping)
            or _text(envelope.get("endpoint")) != endpoint
            or envelope.get("query") != query
            or not secrets.compare_digest(
                _text(envelope.get("responseHash")).lower(),
                _hash_document(dict(response)),
            )
        ):
            raise BinanceSpotFirstLiveBootstrapError(
                f"raw official {key} request envelope changed"
            )
    try:
        parsed_rules = assemble_binance_spot_rules(
            exchange_info, account=account
        )
    except Exception as exc:
        raise BinanceSpotFirstLiveBootstrapError(
            "raw official exchange/account rules cannot be re-parsed"
        ) from exc
    average = snapshot.get("averagePrice")
    average_envelope = envelopes.get("averagePrice")
    reference_price = _terminal_decimal(
        ticker.get("price"), label="raw ticker price"
    )
    reference_source = "BINANCE_TICKER_PRICE"
    average_required = bool(
        int(parsed_rules.get("avgPriceMins") or 0) > 0
        and (
            parsed_rules.get("minNotionalAppliesToMarket") is True
            or parsed_rules.get("maxNotionalAppliesToMarket") is True
        )
    )
    if average_required:
        if (
            not isinstance(average, Mapping)
            or not isinstance(average_envelope, Mapping)
            or _text(average_envelope.get("endpoint")) != "/api/v3/avgPrice"
            or average_envelope.get("query") != {"symbol": "BTCUSDT"}
            or not secrets.compare_digest(
                _text(average_envelope.get("responseHash")).lower(),
                _hash_document(dict(average)),
            )
            or int(average.get("mins") or -1)
            != int(parsed_rules.get("avgPriceMins") or -2)
        ):
            raise BinanceSpotFirstLiveBootstrapError(
                "raw official average-price rule envelope changed"
            )
        reference_price = _terminal_decimal(
            average.get("price"), label="raw average price"
        )
        reference_source = "BINANCE_AVG_PRICE"
    elif average is not None or average_envelope is not None:
        raise BinanceSpotFirstLiveBootstrapError(
            "unexpected raw average-price response is present"
        )
    parsed_rules.update(
        {
            "marketReferencePrice": _terminal_decimal_text(reference_price),
            "marketReferenceSource": reference_source,
            "rulesObservedAt": _utc(
                float(snapshot.get("historyCutoffEpoch") or 0)
            ),
        }
    )
    if parsed_rules != snapshot.get("normalizedRules"):
        raise BinanceSpotFirstLiveBootstrapError(
            "raw exchange/account rules disagree with normalized rules"
        )
    raw_all_orders = _raw_history_from_pages(
        snapshot=snapshot,
        envelopes=envelopes,
        key="allOrdersPages",
        endpoint="/api/v3/allOrders",
        cursor_parameter="orderId",
        row_id_field="orderId",
    )
    raw_trades = _raw_history_from_pages(
        snapshot=snapshot,
        envelopes=envelopes,
        key="myTradesPages",
        endpoint="/api/v3/myTrades",
        cursor_parameter="fromId",
        row_id_field="id",
    )
    if (
        snapshot.get("allOrders") != raw_all_orders
        or snapshot.get("myTrades") != raw_trades
    ):
        raise BinanceSpotFirstLiveBootstrapError(
            "raw official history rows disagree with page envelopes"
        )
    balances = account.get("balances")
    if not isinstance(balances, list) or any(
        not isinstance(row, Mapping) for row in balances
    ):
        raise BinanceSpotFirstLiveBootstrapError(
            "raw official account balances are malformed"
        )
    normalized_balances: list[dict[str, str]] = []
    seen_assets: set[str] = set()
    for row_value in balances:
        row = dict(row_value)
        asset = _text(row.get("asset")).upper()
        free = _terminal_decimal(row.get("free"), label="raw account free")
        locked = _terminal_decimal(row.get("locked"), label="raw account locked")
        if not asset or asset in seen_assets or free < 0 or locked < 0:
            raise BinanceSpotFirstLiveBootstrapError(
                "raw official account balance identity changed"
            )
        seen_assets.add(asset)
        normalized_balances.append(
            {
                "asset": asset,
                "free": _terminal_decimal_text(free),
                "locked": _terminal_decimal_text(locked),
            }
        )
    normalized_open = [
        _bootstrap_normalize_order(row, exact_symbol=False) for row in raw_open
    ]
    normalized_history = [
        _bootstrap_normalize_order(row, exact_symbol=True)
        for row in raw_all_orders
    ]
    normalized_closed = [
        row
        for row in normalized_history
        if row["status"] in {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}
    ]
    orders_by_id = {row["orderId"]: row for row in normalized_history}
    if len(orders_by_id) != len(normalized_history):
        raise BinanceSpotFirstLiveBootstrapError(
            "raw official allOrders contains a duplicate"
        )
    normalized_fills: list[dict[str, Any]] = []
    trade_ids: set[str] = set()
    for trade_value in raw_trades:
        trade = dict(trade_value)
        trade_id = _text(trade.get("id"))
        order_id = _text(trade.get("orderId"))
        order = orders_by_id.get(order_id)
        qty = _terminal_decimal(trade.get("qty"), label="raw trade qty")
        quote = _terminal_decimal(trade.get("quoteQty"), label="raw trade quote")
        fee = _terminal_decimal(
            trade.get("commission"), label="raw trade commission"
        )
        fee_asset = _text(trade.get("commissionAsset")).upper()
        is_buyer = trade.get("isBuyer")
        if type(is_buyer) is not bool:
            raise BinanceSpotFirstLiveBootstrapError(
                "raw official trade buyer-side flag is malformed"
            )
        side = "BUY" if is_buyer else "SELL"
        if (
            not trade_id
            or trade_id in trade_ids
            or order is None
            or qty <= 0
            or quote <= 0
            or fee < 0
            or fee_asset not in {"BTC", "USDT"}
            or side != order["side"]
        ):
            raise BinanceSpotFirstLiveBootstrapError(
                "raw official trade/order/fee binding changed"
            )
        fee_quote = fee if fee_asset == "USDT" else fee * (quote / qty)
        normalized_fills.append(
            {
                "tradeId": trade_id,
                "orderId": order_id,
                "clientOrderId": order["clientOrderId"],
                "symbol": "BTCUSDT",
                "side": side,
                "quantity": _terminal_decimal_text(qty),
                "quoteQuantity": _terminal_decimal_text(quote),
                "commission": _terminal_decimal_text(fee),
                "commissionAsset": fee_asset,
                "feeQuoteValue": _terminal_decimal_text(fee_quote),
                "feeQuoteValueExact": True,
                "time": int(trade.get("time") or 0),
            }
        )
        trade_ids.add(trade_id)
    try:
        normalized_truth_balances = list(terminal_truth.get("balances") or [])
        terminal_open = list(terminal_truth.get("accountOpenOrders") or [])
        terminal_closed = list(terminal_truth.get("closedOrders") or [])
        terminal_fills = list(terminal_truth.get("fills") or [])
    except TypeError as exc:
        raise BinanceSpotFirstLiveBootstrapError(
            "normalized terminal official rows are malformed"
        ) from exc
    if (
        _canonical({"rows": normalized_balances})
        != _canonical({"rows": normalized_truth_balances})
        or _canonical({"rows": normalized_open})
        != _canonical({"rows": terminal_open})
        or _canonical({"rows": normalized_closed})
        != _canonical({"rows": terminal_closed})
        or _canonical({"rows": normalized_fills})
        != _canonical({"rows": terminal_fills})
        or _terminal_decimal(ticker.get("price"), label="raw ticker price")
        != _terminal_decimal(terminal_truth.get("markPrice"), label="terminal mark")
        or parsed_rules != terminal_truth.get("rules")
    ):
        raise BinanceSpotFirstLiveBootstrapError(
            "normalized terminal truth disagrees with raw official responses"
        )
    return normalized_open, normalized_closed, normalized_fills


def _iso_milliseconds(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat(
        timespec="milliseconds"
    ).replace("+00:00", "Z")


def _verify_natural_strategy_evaluation(
    *,
    row: Mapping[str, Any],
    sealed_action: Mapping[str, Any],
    binding: Mapping[str, Any],
    session_id: str,
    permit_id: str,
    permit_hash: str,
    active_started: Decimal,
    active_ends: Decimal,
) -> None:
    try:
        evaluation = json.loads(_text(row.get("evaluation_json")))
        window = json.loads(_text(row.get("window_json")))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise BinanceSpotFirstLiveBootstrapError(
            "durable natural strategy evaluation is malformed"
        ) from exc
    if not isinstance(evaluation, Mapping) or not isinstance(window, Mapping):
        raise BinanceSpotFirstLiveBootstrapError(
            "durable natural strategy evaluation/window is not an object"
        )
    evaluation_value = dict(evaluation)
    window_value = dict(window)
    window_hash = _hash_document(window_value)
    evaluation_hash = _hash_document(evaluation_value)
    raw_rows = window_value.get("rawKlines")
    request = window_value.get("klineRequest")
    normalized_bars = window_value.get("bars")
    if (
        _text(row.get("evaluation_id"))
        != _text(sealed_action.get("evaluationId"))
        or _text(row.get("signal")).upper()
        != _text(sealed_action.get("kind")).upper()
        or _text(row.get("evaluation_hash")).lower() != evaluation_hash
        or _text(row.get("window_hash")).lower() != window_hash
        or _text(sealed_action.get("evaluationHash")).lower()
        != evaluation_hash
        or _text(sealed_action.get("officialWindowHash")).lower()
        != window_hash
        or evaluation_value.get("officialWindow") != window_value
        or _text(evaluation_value.get("officialWindowHash")).lower()
        != window_hash
        or _text(evaluation_value.get("barHash")).lower() != window_hash
        or _text(evaluation_value.get("symbol")).upper() != "BTCUSDT"
        or _text(evaluation_value.get("interval")) != "5m"
        or _text(evaluation_value.get("executionRoute"))
        != "BINANCE_SPOT_CONTINUOUS"
        or _text(evaluation_value.get("strategyArtifactId"))
        != _text(binding.get("strategyArtifactId"))
        or _text(evaluation_value.get("strategyArtifactHash")).lower()
        != _text(binding.get("strategyArtifactHash")).lower()
        or _text(evaluation_value.get("strategyArtifactFileSha256")).lower()
        != _text(binding.get("artifactFileSha256")).lower()
        or _text(evaluation_value.get("strategyInstanceId"))
        != _text(binding.get("strategyInstanceId"))
        or _text(evaluation_value.get("strategyInstanceHash")).lower()
        != _text(binding.get("strategyInstanceHash")).lower()
        or _text(evaluation_value.get("strategyInstanceFileSha256")).lower()
        != _text(binding.get("instanceFileSha256")).lower()
        or _text(evaluation_value.get("publicationProofHash")).lower()
        != _text(binding.get("publicationProofHash")).lower()
        or _text(evaluation_value.get("publicationProofFileSha256")).lower()
        != _text(binding.get("publicationProofFileSha256")).lower()
        or _text(evaluation_value.get("accountFingerprint")).lower()
        != _text(binding.get("accountFingerprint")).lower()
        or _text(evaluation_value.get("bindingHash")).lower()
        != _hash_document(binding)
        or _text(evaluation_value.get("sessionId")) != _text(session_id)
        or _text(evaluation_value.get("permitId")) != _text(permit_id)
        or _text(evaluation_value.get("permitHash")).lower()
        != _text(permit_hash).lower()
        or evaluation_value.get("finalized") is not True
        or evaluation_value.get("strategyEvaluationComplete") is not True
        or evaluation_value.get("naturalSignal") is not True
        or evaluation_value.get("forced") is not False
        or _text(evaluation_value.get("barSource")).upper()
        != "BINANCE_SPOT_KLINE"
        or _text(evaluation_value.get("strategyPluginId"))
        != "moving_average_cross"
        or int(evaluation_value.get("strategyShortMa") or 0) != 3
        or int(evaluation_value.get("strategyLongMa") or 0) != 10
        or window_value.get("schemaVersion")
        != "binance-spot-official-finalized-5m-window-v1"
        or _text(window_value.get("symbol")).upper() != "BTCUSDT"
        or _text(window_value.get("interval")) != "5m"
        or _text(window_value.get("source")).upper() != "BINANCE_SPOT_KLINE"
        or window_value.get("finalized") is not True
        or window_value.get("closed") is not True
        or not isinstance(raw_rows, list)
        or not isinstance(normalized_bars, list)
        or len(normalized_bars) != 11
        or not isinstance(request, Mapping)
        or _text(request.get("endpoint")) != "/api/v3/klines"
        or request.get("query")
        != {"symbol": "BTCUSDT", "interval": "5m", "limit": 13}
        or _text(window_value.get("rawKlinesHash")).lower()
        != _hash_document({"rows": raw_rows})
    ):
        raise BinanceSpotFirstLiveBootstrapError(
            "natural strategy/action/binding hash lineage changed"
        )
    server_time = int(window_value.get("serverTime") or 0)
    canonical_server_observed = _iso_milliseconds(server_time)
    try:
        evaluation_observed = Decimal(
            str(
                datetime.fromisoformat(
                    _text(evaluation_value.get("observedAt")).replace(
                        "Z", "+00:00"
                    )
                ).timestamp()
            )
        )
    except (TypeError, ValueError) as exc:
        raise BinanceSpotFirstLiveBootstrapError(
            "natural evaluation observation time is malformed"
        ) from exc
    server_observed = Decimal(server_time) / Decimal("1000")
    if (
        _text(window_value.get("observedAt")) != canonical_server_observed
        or _text(evaluation_value.get("observedAt"))
        != canonical_server_observed
        or evaluation_observed != server_observed
    ):
        raise BinanceSpotFirstLiveBootstrapError(
            "natural evaluation/window official observation lineage changed"
        )
    raw_normalized: list[dict[str, Any]] = []
    for raw in raw_rows:
        if not isinstance(raw, list) or len(raw) < 12:
            raise BinanceSpotFirstLiveBootstrapError(
                "raw official finalized kline row is malformed"
            )
        try:
            opened_ms = int(raw[0])
            closed_ms = int(raw[6])
            trade_count = int(raw[8])
        except (TypeError, ValueError) as exc:
            raise BinanceSpotFirstLiveBootstrapError(
                "raw official finalized kline identity is invalid"
            ) from exc
        opened = _terminal_decimal(raw[1], label="kline open")
        high = _terminal_decimal(raw[2], label="kline high")
        low = _terminal_decimal(raw[3], label="kline low")
        close = _terminal_decimal(raw[4], label="kline close")
        volume = _terminal_decimal(raw[5], label="kline volume")
        if (
            opened_ms % 300000 != 0
            or closed_ms != opened_ms + 299999
            or high < max(opened, close)
            or low > min(opened, close)
            or high < low
            or trade_count < 0
        ):
            raise BinanceSpotFirstLiveBootstrapError(
                "raw official finalized kline semantics changed"
            )
        if closed_ms >= server_time:
            continue
        raw_normalized.append(
            {
                "barId": f"BTCUSDT-5m-{opened_ms}",
                "openTime": _iso_milliseconds(opened_ms),
                "barCloseAt": _iso_milliseconds(opened_ms + 300000),
                "open": _terminal_decimal_text(opened),
                "high": _terminal_decimal_text(high),
                "low": _terminal_decimal_text(low),
                "close": _terminal_decimal_text(close),
                "volume": _terminal_decimal_text(volume),
                "tradeCount": trade_count,
                "finalized": True,
                "closed": True,
            }
        )
    if (
        server_time <= 0
        or len(raw_normalized) < 11
        or raw_normalized[-11:] != normalized_bars
    ):
        raise BinanceSpotFirstLiveBootstrapError(
            "normalized strategy bars disagree with raw official klines"
        )
    opens = [int(_text(bar.get("barId")).rsplit("-", 1)[1]) for bar in normalized_bars]
    closes = [
        _terminal_decimal(bar.get("close"), label="strategy close")
        for bar in normalized_bars
    ]
    final_close_ms = opens[-1] + 300000
    bar_close_epoch = Decimal(final_close_ms) / Decimal("1000")
    evaluation_created = _terminal_decimal(
        row.get("created_epoch"), label="strategy evaluation created epoch"
    )
    if (
        any(current - prior != 300000 for prior, current in zip(opens, opens[1:]))
        or final_close_ms != (server_time // 300000) * 300000
        or _text(window_value.get("barId")) != _text(normalized_bars[-1].get("barId"))
        or _text(window_value.get("barCloseAt"))
        != _text(normalized_bars[-1].get("barCloseAt"))
        or _text(evaluation_value.get("barCloseAt"))
        != _text(window_value.get("barCloseAt"))
        or Decimal(str(float(row.get("bar_close_epoch") or 0)))
        != Decimal(str(final_close_ms / 1000))
        or Decimal(str(float(sealed_action.get("barCloseEpoch") or 0)))
        != Decimal(str(final_close_ms / 1000))
        or not (
            active_started
            <= bar_close_epoch
            <= server_observed
            <= evaluation_created
            < active_ends
        )
    ):
        raise BinanceSpotFirstLiveBootstrapError(
            "natural finalized-bar boundary lineage changed"
        )
    previous_short = sum(closes[-4:-1], Decimal("0")) / Decimal("3")
    previous_long = sum(closes[-11:-1], Decimal("0")) / Decimal("10")
    current_short = sum(closes[-3:], Decimal("0")) / Decimal("3")
    current_long = sum(closes[-10:], Decimal("0")) / Decimal("10")
    expected_signal = (
        "BUY"
        if previous_short <= previous_long and current_short > current_long
        else (
            "SELL"
            if previous_short >= previous_long and current_short < current_long
            else "HOLD"
        )
    )
    expected_evaluation_id = "binance-ma-eval-" + _hash_document(
        {
            "windowHash": window_hash,
            "strategyArtifactHash": _text(binding.get("strategyArtifactHash")),
            "strategyInstanceHash": _text(binding.get("strategyInstanceHash")),
        }
    )[:32]
    if (
        expected_signal != _text(row.get("signal")).upper()
        or expected_signal != _text(evaluation_value.get("signal")).upper()
        or expected_signal not in {"BUY", "SELL"}
        or expected_evaluation_id != _text(row.get("evaluation_id"))
        or expected_evaluation_id != _text(evaluation_value.get("evaluationId"))
    ):
        raise BinanceSpotFirstLiveBootstrapError(
            "natural BUY/SELL label disagrees with MA(3/10) crossover"
        )


def compute_binance_spot_functional_code_hash(
    paths: Sequence[str | Path],
) -> str:
    """Hash exact path labels and bytes for every live authority component."""

    digest = hashlib.sha256()
    normalized = sorted(
        (Path(path).resolve() for path in paths),
        key=lambda path: str(path).casefold(),
    )
    if not normalized:
        raise BinanceSpotFirstLiveBootstrapError("production code set is empty")
    if len(set(normalized)) != len(normalized):
        raise BinanceSpotFirstLiveBootstrapError(
            "production code set contains duplicate paths"
        )
    try:
        common_root = Path(
            os.path.commonpath([str(path.parent) for path in normalized])
        )
    except ValueError as exc:
        raise BinanceSpotFirstLiveBootstrapError(
            "production code paths do not share one source root"
        ) from exc
    for path in normalized:
        if not path.is_file():
            raise BinanceSpotFirstLiveBootstrapError(
                f"production code file is missing: {path.name}"
            )
        try:
            path_label = path.relative_to(common_root).as_posix()
        except ValueError as exc:
            raise BinanceSpotFirstLiveBootstrapError(
                "production code path escaped its source root"
            ) from exc
        label = path_label.encode("utf-8")
        body = path.read_bytes()
        digest.update(len(label).to_bytes(4, "big"))
        digest.update(label)
        digest.update(len(body).to_bytes(8, "big"))
        digest.update(body)
    return digest.hexdigest()


def default_binance_spot_functional_code_paths() -> tuple[Path, ...]:
    """Return every transitive production authority/signal/send source file."""

    root = Path(__file__).resolve().parent
    names = (
        "binance_order_authority.py",
        "binance_spot_continuous_functional.py",
        "binance_spot_functional_approval.py",
        "binance_spot_functional_backend.py",
        "binance_spot_functional_bootstrap.py",
        "binance_spot_functional_exclusivity.py",
        "binance_spot_functional_exclusivity_provider.py",
        "binance_spot_functional_lifecycle.py",
        "binance_spot_functional_mutation.py",
        "binance_spot_functional_preparation.py",
        "binance_spot_functional_scheduler.py",
        "binance_spot_functional_state.py",
        "binance_spot_functional_strategy.py",
        "binance_spot_functional_transport.py",
        "binance_spot_publication.py",
        "binance_spot_stream_journal.py",
        "brokers.py",
        "continuous_live.py",
        "crypto_first_live_coordinator.py",
        "crypto_first_live_high_water.py",
        "crypto_first_live_runtime.py",
        "emergency_stop.py",
        "env_loader.py",
        "env_settings.py",
        "execution_streams.py",
        "functional_http_session.py",
        "functional_test.py",
        "live_adapters.py",
        "process_safety.py",
        "safety_confirmation.py",
        "server.py",
        "state.py",
    )
    repository_root = root.parents[2]
    shared_permit = (
        repository_root
        / "packages"
        / "trading_runtime"
        / "trading_runtime"
        / "functional_test.py"
    )
    return tuple(root / name for name in names) + (shared_permit,)


def default_binance_spot_functional_code_hash() -> str:
    return compute_binance_spot_functional_code_hash(
        default_binance_spot_functional_code_paths()
    )


class DurableBinanceSpotFirstLiveBootstrapStore:
    """Route-global one-shot authority with no reconstructable raw secret."""

    def __init__(
        self,
        path: str | Path,
        *,
        gate_reader: Callable[[], Mapping[str, Any]],
        server_record_signer: Callable[[Mapping[str, Any]], Mapping[str, Any]],
        code_hash_reader: Callable[[], str] = default_binance_spot_functional_code_hash,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.gate_reader = gate_reader
        self.server_record_signer = server_record_signer
        self.code_hash_reader = code_hash_reader
        self.clock = clock
        self._lock = threading.RLock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.path), timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=5000")
        return connection

    @staticmethod
    def _assert_schema_exact(connection: sqlite3.Connection) -> None:
        try:
            snapshot = _schema_snapshot(connection)
            fingerprint = _hash_document(snapshot)
        except (sqlite3.DatabaseError, TypeError, ValueError) as exc:
            raise BinanceSpotFirstLiveBootstrapError(
                "bootstrap SQLite schema fingerprint is unavailable"
            ) from exc
        if (
            snapshot != _EXPECTED_BOOTSTRAP_SCHEMA
            or not secrets.compare_digest(
                fingerprint, BOOTSTRAP_DB_SCHEMA_FINGERPRINT
            )
        ):
            raise BinanceSpotFirstLiveBootstrapError(
                "bootstrap SQLite schema fingerprint mismatch"
            )

    def _begin_verified_write(self, connection: sqlite3.Connection) -> None:
        try:
            connection.execute("BEGIN IMMEDIATE")
            self._assert_schema_exact(connection)
        except Exception:
            connection.rollback()
            raise

    def _initialize(self) -> None:
        with self._lock, closing(self._connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    "SELECT 1 FROM sqlite_master "
                    "WHERE name LIKE ? OR tbl_name LIKE ? OR name=? LIMIT 1",
                    (
                        _SCHEMA_OBJECT_PREFIX + "%",
                        _SCHEMA_OBJECT_PREFIX + "%",
                        _ROUTE_INDEX,
                    ),
                ).fetchone()
                if existing is None:
                    for statement in _BOOTSTRAP_SCHEMA_SQL:
                        connection.execute(statement)
                # Known legacy layouts are deliberately not altered in place.
                # A missing/wrong named index or additive column can conceal an
                # earlier issue, so only the exact current fingerprint proceeds.
                self._assert_schema_exact(connection)
                connection.commit()
            except BinanceSpotFirstLiveBootstrapError:
                connection.rollback()
                raise
            except sqlite3.DatabaseError as exc:
                connection.rollback()
                raise BinanceSpotFirstLiveBootstrapError(
                    "bootstrap SQLite schema fingerprint initialization failed"
                ) from exc

    @staticmethod
    def _assert_gates(
        value: Mapping[str, Any], *, require_operator_contract: bool
    ) -> None:
        if set(value) != _GATE_FIELDS:
            raise BinanceSpotFirstLiveBootstrapError(
                "first-live prerequisite fields are not exact"
            )
        operator_fields = {"operatorApprovalBound"}
        required_true = _GATE_FIELDS - {"realE2EAvailable"}
        if not require_operator_contract:
            # ISSUED is inert.  The exact bootstrap id/hash/session-nonce hash
            # must exist before the operator can approve them, so these four
            # fields become mandatory only at the atomic CLAIM boundary.
            required_true -= operator_fields
        if any(value.get(field) is not True for field in required_true):
            raise BinanceSpotFirstLiveBootstrapError(
                "a non-E2E production/isolation prerequisite is unavailable"
            )
        if value.get("realE2EAvailable") is not False:
            raise BinanceSpotFirstLiveBootstrapError(
                "first-live bootstrap is valid only before permanent E2E release"
            )

    def _fresh_gates(
        self, *, require_operator_contract: bool
    ) -> dict[str, Any]:
        value = dict(self.gate_reader())
        self._assert_gates(
            value, require_operator_contract=require_operator_contract
        )
        return value

    def issue(
        self,
        *,
        binding: ExactBinding,
        approval_id: str,
        permit_id: str,
        permit_hash: str,
    ) -> tuple[dict[str, Any], str]:
        now = float(self.clock())
        self._fresh_gates(require_operator_contract=False)
        normalized_permit_hash = _text(permit_hash).lower()
        if (
            len(_text(approval_id)) < 12
            or len(_text(permit_id)) < 12
            or _SHA256_RE.fullmatch(normalized_permit_hash) is None
        ):
            raise BinanceSpotFirstLiveBootstrapError(
                "bootstrap approval/permit identity is invalid"
            )
        code_hash = _text(self.code_hash_reader()).lower()
        if _SHA256_RE.fullmatch(code_hash) is None:
            raise BinanceSpotFirstLiveBootstrapError(
                "production code hash is invalid"
            )
        bootstrap_id = "binance-first-live-" + secrets.token_hex(18)
        raw_capability = secrets.token_urlsafe(48)
        body: dict[str, Any] = {
            "schemaVersion": SCHEMA_VERSION,
            "bootstrapId": bootstrap_id,
            "approvalId": _text(approval_id),
            "permitId": _text(permit_id),
            "permitHash": normalized_permit_hash,
            "accountFingerprint": binding.account_fingerprint,
            "bindingHash": _hash_document(binding.payload()),
            "strategyArtifactId": binding.strategy_artifact_id,
            "strategyArtifactHash": binding.strategy_artifact_hash,
            "artifactFileSha256": binding.artifact_file_sha256,
            "strategyInstanceId": binding.strategy_instance_id,
            "strategyInstanceHash": binding.strategy_instance_hash,
            "instanceFileSha256": binding.instance_file_sha256,
            "publicationProofHash": binding.publication_proof_hash,
            "publicationProofFileSha256": binding.publication_proof_file_sha256,
            "productionCodeHash": code_hash,
            "exchange": "BINANCE_SPOT",
            "executionRoute": "BINANCE_SPOT_CONTINUOUS",
            "symbol": "BTCUSDT",
            "interval": "5m",
            "maxOrderNotional": "10",
            "maxGrossExposure": "10",
            "maxOwnerLoss": "1",
            "maxBuyOrders": 1,
            "maxSellOrders": 1,
            "noReentry": True,
            "activeDurationSeconds": 7200,
            "exclusiveAccountRequired": True,
            "singleUse": True,
            "issuedAt": _utc(now),
            "expiresAt": _utc(now + 300),
            "capabilityHash": _hash_text(raw_capability),
        }
        signed = dict(self.server_record_signer(body))
        unsigned = dict(signed)
        signature = _text(unsigned.pop("serverSignature", ""))
        if unsigned != body or not signature:
            raise BinanceSpotFirstLiveBootstrapError(
                "server bootstrap record signature is invalid"
            )
        signed["serverSignature"] = signature
        bootstrap_hash = _hash_document(signed)
        with self._lock, closing(self._connect()) as connection:
            self._begin_verified_write(connection)
            try:
                connection.execute(
                    """
                    INSERT INTO binance_spot_first_live_bootstraps (
                        bootstrap_id, bootstrap_hash, route_key, approval_id,
                        initial_permit_id, initial_permit_hash,
                        account_fingerprint, binding_hash, binding_json,
                        code_hash, capability_hash, record_json, state,
                        issued_epoch, expires_epoch, updated_epoch, detail
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ISSUED',
                              ?, ?, ?, ?)
                    """,
                    (
                        bootstrap_id,
                        bootstrap_hash,
                        ROUTE_KEY,
                        _text(approval_id),
                        _text(permit_id),
                        normalized_permit_hash,
                        binding.account_fingerprint,
                        _hash_document(binding.payload()),
                        _canonical(binding.payload()),
                        code_hash,
                        _hash_text(raw_capability),
                        _canonical(signed),
                        now,
                        now + 300,
                        now,
                        "server-issued first-live capability; raw secret not durable",
                    ),
                )
                connection.commit()
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise BinanceSpotFirstLiveBootstrapError(
                    "another first-live capability is active or replayed"
                ) from exc
        return self.status(bootstrap_id), raw_capability

    def status(self, bootstrap_id: str) -> dict[str, Any]:
        with closing(self._connect()) as connection:
            self._assert_schema_exact(connection)
            row = connection.execute(
                "SELECT * FROM binance_spot_first_live_bootstraps WHERE bootstrap_id=?",
                (_text(bootstrap_id),),
            ).fetchone()
        if row is None:
            raise BinanceSpotFirstLiveBootstrapError("bootstrap is missing")
        value = dict(row)
        value["session_nonce_hash"] = value.pop("capability_hash", "")
        value.pop("claim_token_hash", None)
        value.pop("record_json", None)
        value.pop("binding_json", None)
        return value

    def pointer_for_approval(self, approval_id: str) -> dict[str, Any] | None:
        with closing(self._connect()) as connection:
            self._assert_schema_exact(connection)
            rows = connection.execute(
                """
                SELECT bootstrap_id FROM binance_spot_first_live_bootstraps
                WHERE approval_id=? AND state IN ('ISSUED','CLAIMED','ACTIVE')
                """,
                (_text(approval_id),),
            ).fetchall()
        if len(rows) > 1:
            raise BinanceSpotFirstLiveBootstrapError(
                "multiple active bootstrap records require manual review"
            )
        return self.status(rows[0]["bootstrap_id"]) if rows else None

    def active_terminal_pointer_for_session(
        self, session_id: str
    ) -> dict[str, Any] | None:
        """Return terminalization lineage only; this never grants entry authority."""

        normalized_session = _text(session_id)
        if not normalized_session:
            raise BinanceSpotFirstLiveBootstrapError(
                "bootstrap recovery session is invalid"
            )
        with closing(self._connect()) as connection:
            self._assert_schema_exact(connection)
            rows = connection.execute(
                """
                SELECT bootstrap_id FROM binance_spot_first_live_bootstraps
                WHERE session_id=? AND state='ACTIVE'
                """,
                (normalized_session,),
            ).fetchall()
        if len(rows) > 1:
            raise BinanceSpotFirstLiveBootstrapError(
                "multiple active terminal bootstrap records require manual review"
            )
        return self.status(rows[0]["bootstrap_id"]) if rows else None

    def claim(
        self,
        *,
        bootstrap_id: str,
        raw_capability: str,
        approval_id: str,
        permit_id: str,
        permit_hash: str,
    ) -> str:
        now = float(self.clock())
        self._fresh_gates(require_operator_contract=True)
        code_hash = _text(self.code_hash_reader()).lower()
        claim_token = secrets.token_urlsafe(40)
        with self._lock, closing(self._connect()) as connection:
            self._begin_verified_write(connection)
            row = connection.execute(
                "SELECT * FROM binance_spot_first_live_bootstraps WHERE bootstrap_id=?",
                (_text(bootstrap_id),),
            ).fetchone()
            if (
                row is None
                or _text(row["state"]).upper() != "ISSUED"
                or now >= float(row["expires_epoch"])
                or not secrets.compare_digest(
                    _text(row["capability_hash"]), _hash_text(raw_capability)
                )
                or not secrets.compare_digest(
                    _text(row["approval_id"]), _text(approval_id)
                )
                or not secrets.compare_digest(
                    _text(row["initial_permit_id"]), _text(permit_id)
                )
                or not secrets.compare_digest(
                    _text(row["initial_permit_hash"]), _text(permit_hash).lower()
                )
                or not secrets.compare_digest(_text(row["code_hash"]), code_hash)
            ):
                connection.rollback()
                raise BinanceSpotFirstLiveBootstrapError(
                    "first-live capability is stale, changed, or already consumed"
                )
            cursor = connection.execute(
                """
                UPDATE binance_spot_first_live_bootstraps
                SET state='CLAIMED', claim_token_hash=?, updated_epoch=?,
                    detail='single-use first-live capability claimed before activation'
                WHERE bootstrap_id=? AND state='ISSUED'
                """,
                (_hash_text(claim_token), now, _text(bootstrap_id)),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise BinanceSpotFirstLiveBootstrapError(
                    "first-live claim CAS changed"
                )
            connection.commit()
        return claim_token

    def bind_session(
        self,
        *,
        bootstrap_id: str,
        claim_token: str,
        approval_id: str,
        active_permit_id: str,
        active_permit_hash: str,
        session_id: str,
        binding: ExactBinding,
        activated_epoch: float,
        active_ends_epoch: float,
    ) -> dict[str, Any]:
        if len(_text(session_id)) < 12:
            raise BinanceSpotFirstLiveBootstrapError("bootstrap session is invalid")
        code_hash = _text(self.code_hash_reader()).lower()
        try:
            activated = Decimal(str(activated_epoch))
            active_ends = Decimal(str(active_ends_epoch))
        except (InvalidOperation, ValueError) as exc:
            raise BinanceSpotFirstLiveBootstrapError(
                "bootstrap activation window is invalid"
            ) from exc
        if (
            not activated.is_finite()
            or not active_ends.is_finite()
            or active_ends - activated != Decimal("7200")
        ):
            raise BinanceSpotFirstLiveBootstrapError(
                "bootstrap activation window is not exact 7200 seconds"
            )
        with self._lock, closing(self._connect()) as connection:
            self._begin_verified_write(connection)
            bootstrap_key = _text(bootstrap_id)
            approval_key = _text(approval_id)
            claim_token_hash = _hash_text(claim_token)
            binding_hash = _hash_document(binding.payload())
            row = connection.execute(
                "SELECT * FROM binance_spot_first_live_bootstraps "
                "WHERE bootstrap_id=?",
                (bootstrap_key,),
            ).fetchone()
            if (
                row is None
                or _text(row["state"]).upper() != "CLAIMED"
                or not secrets.compare_digest(
                    _text(row["approval_id"]), approval_key
                )
                or not secrets.compare_digest(
                    _text(row["claim_token_hash"]), claim_token_hash
                )
                or not secrets.compare_digest(
                    _text(row["account_fingerprint"]),
                    binding.account_fingerprint,
                )
                or not secrets.compare_digest(
                    _text(row["binding_hash"]), binding_hash
                )
                or not secrets.compare_digest(
                    _text(row["code_hash"]), code_hash
                )
            ):
                connection.rollback()
                raise BinanceSpotFirstLiveBootstrapError(
                    "first-live session bind changed"
                )
            def decimal_or_nan(value: object) -> Decimal:
                try:
                    return Decimal(str(value))
                except (InvalidOperation, TypeError, ValueError):
                    return Decimal("NaN")

            trusted_now = decimal_or_nan(self.clock())
            issued = decimal_or_nan(row["issued_epoch"])
            expires = decimal_or_nan(row["expires_epoch"])
            # While the row is CLAIMED, updated_epoch is the durable claim
            # timestamp written by claim() in its own immediate transaction.
            claimed = decimal_or_nan(row["updated_epoch"])
            temporal_detail = ""
            if any(
                not value.is_finite()
                for value in (trusted_now, issued, expires, claimed)
            ) or not (issued <= claimed <= activated <= trusted_now):
                temporal_detail = (
                    "first-live claim time lineage invalid before session bind"
                )
            elif trusted_now >= expires:
                temporal_detail = "first-live claim expired before session bind"
            if temporal_detail:
                cursor = connection.execute(
                    """
                    UPDATE binance_spot_first_live_bootstraps
                    SET state='FAILED', capability_hash='', claim_token_hash='',
                        detail=?, updated_epoch=?
                    WHERE bootstrap_id=? AND state='CLAIMED'
                        AND approval_id=? AND claim_token_hash=?
                        AND account_fingerprint=? AND binding_hash=?
                        AND code_hash=?
                    """,
                    (
                        temporal_detail,
                        float(
                            next(
                                (
                                    value
                                    for value in (trusted_now, claimed, issued)
                                    if value.is_finite()
                                ),
                                Decimal("0"),
                            )
                        ),
                        bootstrap_key,
                        approval_key,
                        claim_token_hash,
                        binding.account_fingerprint,
                        binding_hash,
                        code_hash,
                    ),
                )
                if cursor.rowcount != 1:
                    connection.rollback()
                    raise BinanceSpotFirstLiveBootstrapError(
                        "first-live session bind changed"
                    )
                connection.commit()
                raise BinanceSpotFirstLiveBootstrapError(temporal_detail)
            cursor = connection.execute(
                """
                UPDATE binance_spot_first_live_bootstraps
                SET state='ACTIVE', active_permit_id=?, active_permit_hash=?,
                    session_id=?, activated_epoch=?, active_ends_epoch=?,
                    claim_token_hash='', updated_epoch=?,
                    detail='first-live capability bound to exact resealed session'
                WHERE bootstrap_id=? AND state='CLAIMED'
                    AND approval_id=? AND claim_token_hash=?
                    AND account_fingerprint=? AND binding_hash=? AND code_hash=?
                """,
                (
                    _text(active_permit_id),
                    _text(active_permit_hash).lower(),
                    _text(session_id),
                    float(activated),
                    float(active_ends),
                    float(trusted_now),
                    bootstrap_key,
                    approval_key,
                    claim_token_hash,
                    binding.account_fingerprint,
                    binding_hash,
                    code_hash,
                ),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise BinanceSpotFirstLiveBootstrapError(
                    "first-live session bind changed"
                )
            connection.commit()
        return self.status(bootstrap_id)

    def fail(self, *, bootstrap_id: str, detail: str) -> dict[str, Any]:
        with self._lock, closing(self._connect()) as connection:
            self._begin_verified_write(connection)
            cursor = connection.execute(
                """
                UPDATE binance_spot_first_live_bootstraps
                SET state='FAILED', capability_hash='', claim_token_hash='',
                    detail=?, updated_epoch=?
                WHERE bootstrap_id=? AND state IN ('ISSUED','CLAIMED','ACTIVE')
                """,
                (_text(detail)[:500], float(self.clock()), _text(bootstrap_id)),
            )
            if cursor.rowcount not in {0, 1}:
                connection.rollback()
                raise BinanceSpotFirstLiveBootstrapError(
                    "bootstrap failure transition changed"
                )
            connection.commit()
        return self.status(bootstrap_id)

    def fail_orphans_after_process_loss(self) -> list[str]:
        """Burn every raw-secret-bearing record after old-process absence proof."""

        with self._lock, closing(self._connect()) as connection:
            self._begin_verified_write(connection)
            rows = connection.execute(
                """
                SELECT bootstrap_id FROM binance_spot_first_live_bootstraps
                WHERE state IN ('ISSUED','CLAIMED')
                """
            ).fetchall()
            ids = [_text(row["bootstrap_id"]) for row in rows]
            if ids:
                cursor = connection.execute(
                    """
                    UPDATE binance_spot_first_live_bootstraps
                    SET state='FAILED', capability_hash='', claim_token_hash='',
                        detail='process-loss audit burned unrecoverable raw bootstrap',
                        updated_epoch=?
                    WHERE state IN ('ISSUED','CLAIMED')
                    """,
                    (float(self.clock()),),
                )
                if cursor.rowcount != len(ids):
                    connection.rollback()
                    raise BinanceSpotFirstLiveBootstrapError(
                        "process-loss bootstrap burn cardinality changed"
                    )
            connection.commit()
        return ids

    def _verify_durable_terminal_execution(
        self,
        *,
        bootstrap_id: str,
        session_id: str,
        evidence: Mapping[str, Any],
        evidence_hash: str,
        connection: sqlite3.Connection | None = None,
    ) -> None:
        """Recompute wiring from immutable ledger rows and raw final truth.

        The bootstrap never treats the core's summary booleans as execution
        proof.  It independently binds the final evidence to the same SQLite
        session/actions, then recomputes the exact natural legs, caps, fills,
        fees, baseline delta, loss, and account-wide working-order zero.
        """

        owned_connection = connection is None
        verify_connection = connection or self._connect()
        try:
            try:
                session = verify_connection.execute(
                    """
                    SELECT state, capability_hash, final_new_entries_blocked,
                           baseline_base, baseline_quote, final_evidence_json,
                           final_evidence_hash,permit_id,permit_hash,binding_json,
                           binding_hash,started_epoch,expires_epoch
                    FROM binance_spot_functional_sessions
                    WHERE session_id=?
                    """,
                    (_text(session_id),),
                ).fetchone()
                bootstrap = verify_connection.execute(
                    """SELECT * FROM binance_spot_first_live_bootstraps
                    WHERE bootstrap_id=?""",
                    (_text(bootstrap_id),),
                ).fetchone()
                approval = verify_connection.execute(
                    """SELECT * FROM binance_spot_functional_approvals
                    WHERE session_id=?""",
                    (_text(session_id),),
                ).fetchone()
                terminal_row = verify_connection.execute(
                    """SELECT truth_json,truth_hash,observed_epoch,
                    stream_journal_seal_hash,stream_journal_event_count
                    FROM binance_spot_functional_terminal_truth
                    WHERE session_id=?""",
                    (_text(session_id),),
                ).fetchone()
                actions = verify_connection.execute(
                    """
                    SELECT claim_id, action_kind, client_order_id, state,
                           sealed_action_json, response_hash, broker_order_id,
                           created_epoch, post_marker_epoch
                    FROM binance_spot_functional_actions
                    WHERE session_id=? ORDER BY created_epoch, claim_id
                    """,
                    (_text(session_id),),
                ).fetchall()
                evaluations = verify_connection.execute(
                    """SELECT * FROM
                    binance_spot_functional_strategy_evaluations
                    WHERE session_id=? ORDER BY bar_close_epoch,evaluation_id""",
                    (_text(session_id),),
                ).fetchall()
                archives = verify_connection.execute(
                    """SELECT * FROM binance_spot_stream_journal_archives
                    WHERE session_id=? AND final_evidence_hash=?""",
                    (_text(session_id), _text(evidence_hash).lower()),
                ).fetchall()
                archive_events = (
                    verify_connection.execute(
                        """SELECT event_id,event_epoch,payload_json FROM
                        binance_spot_stream_journal_archive_events
                        WHERE archive_id=? ORDER BY event_epoch,event_id""",
                        (_text(archives[0]["archive_id"]),),
                    ).fetchall()
                    if len(archives) == 1
                    else []
                )
            except sqlite3.DatabaseError as exc:
                raise BinanceSpotFirstLiveBootstrapError(
                    "durable terminal ledger is unavailable"
                ) from exc
        finally:
            if owned_connection:
                verify_connection.close()
        if (
            session is None
            or bootstrap is None
            or approval is None
            or _text(session["state"]).upper() != "FINALIZED"
            or _text(session["capability_hash"])
            or int(session["final_new_entries_blocked"] or 0) != 1
            or not secrets.compare_digest(
                _text(session["final_evidence_hash"]).lower(),
                _text(evidence_hash).lower(),
            )
            or not secrets.compare_digest(
                _text(session["final_evidence_json"]), _canonical(evidence)
            )
        ):
            raise BinanceSpotFirstLiveBootstrapError(
                "durable final capability/evidence seal changed"
            )
        try:
            terminal_truth = json.loads(_text(terminal_row["truth_json"]))
            binding = json.loads(_text(session["binding_json"]))
            bootstrap_binding = json.loads(_text(bootstrap["binding_json"]))
            bootstrap_record = json.loads(_text(bootstrap["record_json"]))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise BinanceSpotFirstLiveBootstrapError(
                "durable terminal official truth is malformed"
            ) from exc
        if not all(
            isinstance(item, Mapping)
            for item in (
                terminal_truth,
                binding,
                bootstrap_binding,
                bootstrap_record,
            )
        ):
            raise BinanceSpotFirstLiveBootstrapError(
                "durable terminal official truth objects are malformed"
            )
        terminal_exclusivity_proof = terminal_truth.get(
            "accountExclusivityProof"
        )
        evidence_exclusivity_proof = evidence.get(
            "accountExclusivityProof"
        )
        terminal_causal_component = (
            terminal_exclusivity_proof.get("accountWideCausalAudit")
            if isinstance(terminal_exclusivity_proof, Mapping)
            else None
        )
        exclusivity_hash = _text(
            evidence.get("accountExclusivityProofHash")
        ).lower()
        terminal_hash = _text(terminal_row["truth_hash"]).lower()
        trusted_now = _terminal_decimal(self.clock(), label="trusted current epoch")
        if (
            terminal_row is None
            or not isinstance(terminal_exclusivity_proof, Mapping)
            or not isinstance(evidence_exclusivity_proof, Mapping)
            or dict(terminal_exclusivity_proof)
            != dict(evidence_exclusivity_proof)
            or _SHA256_RE.fullmatch(exclusivity_hash) is None
            or not secrets.compare_digest(
                _hash_document(terminal_exclusivity_proof), exclusivity_hash
            )
            or not secrets.compare_digest(
                _text(
                    terminal_truth.get("accountExclusivityProofHash")
                ).lower(),
                exclusivity_hash,
            )
            or not isinstance(terminal_causal_component, Mapping)
            or (
                terminal_causal_component.get("causalClosureProven") is True
            )
            != (terminal_truth.get("accountWideCausalClosureProven") is True)
            or (
                terminal_truth.get("accountWideCausalClosureProven") is True
            )
            != (evidence.get("accountWideCausalClosureProven") is True)
            or (
                terminal_truth.get("accountExclusivityPhaseChainComplete")
                is True
            )
            != (
                evidence.get("accountExclusivityPhaseChainComplete") is True
            )
            or (
                terminal_truth.get("accountExclusivityRestartVerifiable")
                is True
            )
            != (
                evidence.get("accountExclusivityRestartVerifiable") is True
            )
            or not secrets.compare_digest(
                _text(
                    terminal_truth.get("accountExclusivityPhaseChainHash")
                ).lower(),
                _text(
                    evidence.get("accountExclusivityPhaseChainHash")
                ).lower(),
            )
            or int(
                terminal_truth.get("accountExclusivityPhaseProofCount") or 0
            )
            != int(evidence.get("accountExclusivityPhaseProofCount") or 0)
            or int(
                terminal_truth.get(
                    "accountExclusivityPhaseProofRequiredCount"
                )
                or 0
            )
            != int(
                evidence.get("accountExclusivityPhaseProofRequiredCount") or 0
            )
            or int(evidence.get("accountExclusivityPhaseProofCount") or 0)
            != 3
            + sum(
                1
                for action in actions
                if _text(action["action_kind"]).upper() in {"BUY", "SELL"}
            )
            or int(
                evidence.get("accountExclusivityPhaseProofRequiredCount") or 0
            )
            != 3
            + sum(
                1
                for action in actions
                if _text(action["action_kind"]).upper() in {"BUY", "SELL"}
            )
            or _text(bootstrap["state"]).upper() != "ACTIVE"
            or _text(bootstrap["session_id"]) != _text(session_id)
            or _text(bootstrap["active_permit_id"]) != _text(session["permit_id"])
            or not secrets.compare_digest(
                _text(bootstrap["active_permit_hash"]).lower(),
                _text(session["permit_hash"]).lower(),
            )
            or _text(bootstrap["account_fingerprint"]).lower()
            != _text(binding.get("accountFingerprint")).lower()
            or not secrets.compare_digest(
                _text(bootstrap["binding_hash"]).lower(),
                _text(session["binding_hash"]).lower(),
            )
            or not secrets.compare_digest(
                _text(bootstrap["binding_json"]), _text(session["binding_json"])
            )
            or bootstrap_binding != binding
            or not secrets.compare_digest(
                _hash_document(bootstrap_record),
                _text(bootstrap["bootstrap_hash"]).lower(),
            )
            or _text(bootstrap_record.get("bootstrapId")) != _text(bootstrap_id)
            or _text(bootstrap_record.get("approvalId"))
            != _text(bootstrap["approval_id"])
            or _text(bootstrap_record.get("permitId"))
            != _text(bootstrap["initial_permit_id"])
            or _text(bootstrap_record.get("permitHash")).lower()
            != _text(bootstrap["initial_permit_hash"]).lower()
            or _text(bootstrap_record.get("capabilityHash")).lower()
            != _text(bootstrap["capability_hash"]).lower()
            or _text(bootstrap_record.get("accountFingerprint")).lower()
            != _text(bootstrap["account_fingerprint"]).lower()
            or _text(bootstrap_record.get("bindingHash")).lower()
            != _text(bootstrap["binding_hash"]).lower()
            or _text(bootstrap_record.get("productionCodeHash")).lower()
            != _text(bootstrap["code_hash"]).lower()
            or _SHA256_RE.fullmatch(_text(bootstrap["capability_hash"]).lower())
            is None
            or _text(bootstrap["code_hash"]).lower()
            != _text(self.code_hash_reader()).lower()
            or _text(approval["state"]).upper() != "CONSUMED"
            or _text(approval["permit_id"]) != _text(session["permit_id"])
            or _text(approval["permit_hash"]).lower()
            != _text(session["permit_hash"]).lower()
            or _text(approval["session_id"]) != _text(session_id)
            or _text(approval["approval_id"])
            != _text(bootstrap["approval_id"])
            or _text(approval["account_fingerprint"]).lower()
            != _text(bootstrap["account_fingerprint"]).lower()
            or _text(approval["strategy_artifact_hash"]).lower()
            != _text(binding.get("strategyArtifactHash")).lower()
            or _text(approval["strategy_instance_hash"]).lower()
            != _text(binding.get("strategyInstanceHash")).lower()
            or _text(approval["route_key"]) != ROUTE_KEY
            or int(approval["first_live_bootstrap_required"] or 0) != 1
            or _text(approval["first_live_bootstrap_id"])
            != _text(bootstrap_id)
            or _text(approval["first_live_bootstrap_hash"]).lower()
            != _text(bootstrap["bootstrap_hash"]).lower()
            or _text(approval["first_live_session_nonce_hash"]).lower()
            != _text(bootstrap["capability_hash"]).lower()
            or _text(approval["first_live_code_hash"]).lower()
            != _text(bootstrap["code_hash"]).lower()
            or _SHA256_RE.fullmatch(terminal_hash) is None
            or not secrets.compare_digest(
                _hash_document(terminal_truth), terminal_hash
            )
            or not secrets.compare_digest(
                terminal_hash,
                _text(evidence.get("terminalOfficialTruthHash")).lower(),
            )
            or terminal_truth.get("schemaVersion")
            != "binance-spot-functional-terminal-official-truth/v1"
            or _text(terminal_truth.get("sessionId")) != _text(session_id)
            or _text(terminal_truth.get("permitId")) != _text(session["permit_id"])
            or not secrets.compare_digest(
                _text(terminal_truth.get("permitHash")).lower(),
                _text(session["permit_hash"]).lower(),
            )
            or _text(terminal_truth.get("accountFingerprint")).lower()
            != _text(binding.get("accountFingerprint")).lower()
            or float(terminal_row["observed_epoch"] or 0)
            != float(terminal_truth.get("observedEpoch") or 0)
            or _text(terminal_row["stream_journal_seal_hash"]).lower()
            != _text(terminal_truth.get("streamJournalSealHash")).lower()
            or int(terminal_row["stream_journal_event_count"] or 0)
            != int(terminal_truth.get("streamJournalEventCount") or 0)
            or terminal_truth.get("feeQuoteValuationComplete") is not True
            or terminal_truth.get("externalActivityAbsent") is not True
            or Decimal(str(session["started_epoch"]))
            != _terminal_decimal(
                evidence.get("activatedEpoch"), label="evidence activation"
            )
            or Decimal(str(bootstrap["activated_epoch"]))
            != Decimal(str(session["started_epoch"]))
            or Decimal(str(session["expires_epoch"]))
            != _terminal_decimal(
                evidence.get("activeEndsEpoch"), label="evidence active end"
            )
            or Decimal(str(bootstrap["active_ends_epoch"]))
            != Decimal(str(session["expires_epoch"]))
            or Decimal(str(session["started_epoch"]))
            != _terminal_decimal(
                terminal_truth.get("historyBaselineEpoch"),
                label="official history baseline",
            )
            or _terminal_decimal(
                terminal_truth.get("historyCutoffEpoch"),
                label="official history cutoff",
            )
            < Decimal(str(session["expires_epoch"]))
            or _terminal_decimal(
                terminal_truth.get("observedEpoch"),
                label="official terminal observation",
            )
            != _terminal_decimal(
                evidence.get("terminalObservedEpoch"),
                label="evidence terminal observation",
            )
            or _terminal_decimal(
                terminal_truth.get("observedEpoch"),
                label="official terminal observation",
            )
            < _terminal_decimal(
                terminal_truth.get("historyCutoffEpoch"),
                label="official history cutoff",
            )
            or _terminal_decimal(
                terminal_truth.get("observedEpoch"),
                label="official terminal observation",
            )
            - _terminal_decimal(
                terminal_truth.get("historyCutoffEpoch"),
                label="official history cutoff",
            )
            > Decimal("15")
            or trusted_now < Decimal(str(session["expires_epoch"]))
            or _terminal_decimal(
                terminal_truth.get("observedEpoch"),
                label="official terminal observation",
            )
            > trusted_now + Decimal("1")
        ):
            raise BinanceSpotFirstLiveBootstrapError(
                "durable terminal official truth identity/hash changed"
            )
        unsigned_bootstrap = dict(bootstrap_record)
        signature = _text(unsigned_bootstrap.pop("serverSignature", ""))
        try:
            resigned_bootstrap = dict(self.server_record_signer(unsigned_bootstrap))
        except Exception as exc:
            raise BinanceSpotFirstLiveBootstrapError(
                "bootstrap server signature cannot be re-verified"
            ) from exc
        if (
            not signature
            or resigned_bootstrap != dict(bootstrap_record)
            or _text(resigned_bootstrap.get("serverSignature")) != signature
        ):
            raise BinanceSpotFirstLiveBootstrapError(
                "bootstrap server signature changed"
            )
        open_orders, closed_orders, fills = _verify_official_rest_snapshot(
            terminal_truth
        )
        if len(archives) != 1:
            raise BinanceSpotFirstLiveBootstrapError(
                "exact terminal user-stream archive is absent"
            )
        archive = dict(archives[0])
        stream_rows = [dict(row) for row in archive_events]
        try:
            stream_meta = json.loads(_text(archive.get("meta_json")))
            stream_payloads = [
                json.loads(_text(row.get("payload_json"))) for row in stream_rows
            ]
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise BinanceSpotFirstLiveBootstrapError(
                "terminal user-stream archive is malformed"
            ) from exc
        expected_seal = _text(terminal_truth.get("streamJournalSealHash")).lower()
        expected_count = int(terminal_truth.get("streamJournalEventCount") or 0)
        attestation = {
            "routeKey": ROUTE_KEY,
            "accountFingerprint": _text(
                terminal_truth.get("accountFingerprint")
            ).lower(),
            "sessionId": _text(session_id),
            "permitId": _text(session["permit_id"]),
            "permitHash": _text(session["permit_hash"]).lower(),
            "finalEvidenceHash": _text(evidence_hash).lower(),
            "terminalReason": "FINALIZED",
        }
        archive_material = {
            "attestation": attestation,
            "meta": stream_meta,
            "events": stream_rows,
        }
        if (
            not isinstance(stream_meta, Mapping)
            or any(not isinstance(payload, Mapping) for payload in stream_payloads)
            or _text(archive.get("route_key")) != ROUTE_KEY
            or _text(archive.get("account_fingerprint")).lower()
            != attestation["accountFingerprint"]
            or _text(archive.get("permit_id")) != attestation["permitId"]
            or not secrets.compare_digest(
                _text(archive.get("permit_hash")).lower(),
                attestation["permitHash"],
            )
            or not secrets.compare_digest(
                _text(archive.get("archive_hash")).lower(),
                _hash_document(archive_material),
            )
            or _SHA256_RE.fullmatch(expected_seal) is None
            or not secrets.compare_digest(
                _stream_journal_seal(stream_meta, stream_rows), expected_seal
            )
            or expected_count != len(stream_rows)
            or _text(stream_meta.get("session_id")) != _text(session_id)
            or _text(stream_meta.get("permit_id")) != attestation["permitId"]
            or not secrets.compare_digest(
                _text(stream_meta.get("permit_hash")).lower(),
                attestation["permitHash"],
            )
            or not bool(stream_meta.get("connected"))
            or not bool(stream_meta.get("authenticated"))
            or bool(stream_meta.get("gap_detected"))
            or not bool(stream_meta.get("external_activity_absent"))
            or bool(stream_meta.get("retired"))
            or not _text(stream_meta.get("terminal_marker_id"))
            or float(stream_meta.get("terminal_marker_server_epoch") or 0) <= 0
        ):
            raise BinanceSpotFirstLiveBootstrapError(
                "terminal user-stream archive identity/head/seal changed"
            )
        owner_prefix = (
            "ftb-" + hashlib.sha256(_text(session_id).encode()).hexdigest()[:12] + "-"
        )
        baseline_ms = int(
            float(terminal_truth.get("historyBaselineEpoch") or 0) * 1000
        )
        cutoff_ms = int(
            float(terminal_truth.get("historyCutoffEpoch") or 0) * 1000
        )
        seen_stream_ids: set[str] = set()
        for row, payload in zip(stream_rows, stream_payloads):
            event_id = _text(payload.get("eventId"))
            event_time = int(payload.get("eventTime") or 0)
            event_type = _text(payload.get("eventType"))
            if (
                not event_id
                or event_id in seen_stream_ids
                or event_id != _text(row.get("event_id"))
                or event_time <= 0
                or abs(event_time / 1000 - float(row.get("event_epoch") or 0))
                > 0.001
                or event_time < baseline_ms - 1000
                or event_time > cutoff_ms + 1000
                or event_type
                not in {
                    "executionReport",
                    "outboundAccountPosition",
                    "balanceUpdate",
                }
                or event_type == "balanceUpdate"
                or (
                    event_type == "executionReport"
                    and not _text(payload.get("clientOrderId")).startswith(
                        owner_prefix
                    )
                )
            ):
                raise BinanceSpotFirstLiveBootstrapError(
                    "terminal user-stream event chain contains a gap/external row"
                )
            seen_stream_ids.add(event_id)
        durable_actions: list[dict[str, Any]] = []
        sealed_actions: list[dict[str, Any]] = []
        for row in actions:
            try:
                sealed = json.loads(_text(row["sealed_action_json"]))
            except (TypeError, ValueError) as exc:
                raise BinanceSpotFirstLiveBootstrapError(
                    "durable sealed action is malformed"
                ) from exc
            if not isinstance(sealed, dict):
                raise BinanceSpotFirstLiveBootstrapError(
                    "durable sealed action is not an object"
                )
            durable_actions.append(
                {
                    "claimId": _text(row["claim_id"]),
                    "actionKind": _text(row["action_kind"]).upper(),
                    "clientOrderId": _text(row["client_order_id"]),
                    "state": _text(row["state"]).upper(),
                    "sealedActionHash": hashlib.sha256(
                        _text(row["sealed_action_json"]).encode("utf-8")
                    ).hexdigest(),
                    "responseHash": _text(row["response_hash"]).lower(),
                    "brokerOrderId": _text(row["broker_order_id"]),
                    "createdEpoch": float(row["created_epoch"] or 0),
                    "postMarkerEpoch": float(row["post_marker_epoch"] or 0),
                }
            )
            sealed_actions.append(sealed)
        natural = [
            (durable, sealed)
            for durable, sealed in zip(durable_actions, sealed_actions)
            if durable["actionKind"] in {"BUY", "SELL"}
        ]
        evaluations_by_id = {
            _text(row["evaluation_id"]): dict(row) for row in evaluations
        }
        if len(evaluations_by_id) != len(evaluations):
            raise BinanceSpotFirstLiveBootstrapError(
                "durable strategy evaluation identity is duplicated"
            )
        if (
            len(durable_actions) != 2
            or len(natural) != 2
            or [item[0]["actionKind"] for item in natural].count("BUY") != 1
            or [item[0]["actionKind"] for item in natural].count("SELL") != 1
            or any(item[0]["state"] != "RECONCILED" for item in natural)
            or any(
                not item[0]["brokerOrderId"]
                or _SHA256_RE.fullmatch(item[0]["responseHash"]) is None
                for item in natural
            )
        ):
            raise BinanceSpotFirstLiveBootstrapError(
                "durable natural BUY1/SELL1 proof is incomplete"
            )
        for durable, sealed in natural:
            evaluation = evaluations_by_id.get(
                _text(sealed.get("evaluationId"))
            )
            if (
                _text(sealed.get("kind")).upper() != durable["actionKind"]
                or _text(sealed.get("clientOrderId"))
                != durable["clientOrderId"]
                or _text(sealed.get("symbol")).upper() != "BTCUSDT"
                or _text(sealed.get("product")).upper() != "SPOT"
                or _text(sealed.get("orderType")).upper() != "MARKET"
                or sealed.get("functionalOnly") is not True
                or sealed.get("cleanupOnly") is not False
                or evaluation is None
                or float(evaluation.get("created_epoch") or 0)
                > float(durable["createdEpoch"]) + 0.001
                or not (
                    Decimal(str(session["started_epoch"]))
                    <= Decimal(str(durable["createdEpoch"]))
                    <= Decimal(str(durable["postMarkerEpoch"]))
                    < Decimal(str(session["expires_epoch"]))
                )
            ):
                raise BinanceSpotFirstLiveBootstrapError(
                    "durable natural action shape changed"
                )
            _verify_natural_strategy_evaluation(
                row=evaluation,
                sealed_action=sealed,
                binding=binding,
                session_id=_text(session_id),
                permit_id=_text(session["permit_id"]),
                permit_hash=_text(session["permit_hash"]),
                active_started=Decimal(str(session["started_epoch"])),
                active_ends=Decimal(str(session["expires_epoch"])),
            )
            if durable["actionKind"] == "BUY":
                quote_cap = _terminal_decimal(
                    sealed.get("quoteOrderQty"), label="BUY quoteOrderQty"
                )
                if quote_cap <= 0 or quote_cap > Decimal("10"):
                    raise BinanceSpotFirstLiveBootstrapError(
                        "durable BUY exceeds the 10 USDT cap"
                    )
            else:
                quantity = _terminal_decimal(
                    sealed.get("quantity"), label="SELL quantity"
                )
                if quantity <= 0:
                    raise BinanceSpotFirstLiveBootstrapError(
                        "durable SELL quantity is not positive"
                    )
        referenced_evaluations = {
            _text(sealed.get("evaluationId")) for _durable, sealed in natural
        }
        non_hold_evaluations = {
            evaluation_id
            for evaluation_id, row in evaluations_by_id.items()
            if _text(row.get("signal")).upper() in {"BUY", "SELL"}
        }
        if non_hold_evaluations != referenced_evaluations:
            raise BinanceSpotFirstLiveBootstrapError(
                "durable natural action/evaluation set is not exact"
            )
        if open_orders != [] or not isinstance(closed_orders, list) or not isinstance(fills, list):
            raise BinanceSpotFirstLiveBootstrapError(
                "terminal account working/order/fill proof is incomplete"
            )
        closed_by_client: dict[str, Mapping[str, Any]] = {}
        for row in closed_orders:
            if not isinstance(row, Mapping):
                raise BinanceSpotFirstLiveBootstrapError(
                    "terminal closed order proof is malformed"
                )
            client_id = _terminal_client_id(row)
            if not client_id or client_id in closed_by_client:
                raise BinanceSpotFirstLiveBootstrapError(
                    "terminal closed orders are not uniquely identified"
                )
            closed_by_client[client_id] = row
        bought = sold = buy_quote = sell_quote = Decimal("0")
        base_fees = cash_fees = Decimal("0")
        seen_trades: set[str] = set()
        for durable, _sealed in natural:
            client_id = durable["clientOrderId"]
            order = closed_by_client.get(client_id)
            if (
                order is None
                or _text(order.get("status")).upper() != "FILLED"
                or _text(order.get("side")).upper() != durable["actionKind"]
                or _text(order.get("symbol")).upper() != "BTCUSDT"
                or _text(order.get("orderId")) != durable["brokerOrderId"]
            ):
                raise BinanceSpotFirstLiveBootstrapError(
                    "terminal natural order is not exact FILLED truth"
                )
            executed_qty = _terminal_decimal(
                order.get("executedQty"), label="order executedQty"
            )
            executed_quote = _terminal_decimal(
                order.get("cummulativeQuoteQty"),
                label="order cummulativeQuoteQty",
            )
            matching = [
                fill
                for fill in fills
                if isinstance(fill, Mapping)
                and _terminal_client_id(fill) == client_id
            ]
            fill_qty = fill_quote = Decimal("0")
            if not matching:
                raise BinanceSpotFirstLiveBootstrapError(
                    "terminal natural order has no fills"
                )
            for fill in matching:
                trade_id = _text(fill.get("tradeId"))
                side = _text(fill.get("side")).upper()
                if (
                    not trade_id
                    or trade_id in seen_trades
                    or side != durable["actionKind"]
                    or _text(fill.get("symbol")).upper() != "BTCUSDT"
                ):
                    raise BinanceSpotFirstLiveBootstrapError(
                        "terminal fill identity is invalid"
                    )
                seen_trades.add(trade_id)
                qty = _terminal_decimal(fill.get("quantity"), label="fill quantity")
                quote = _terminal_decimal(
                    fill.get("quoteQuantity"), label="fill quote quantity"
                )
                fee = _terminal_decimal(
                    fill.get("commission"), label="fill commission"
                )
                fee_asset = _text(fill.get("commissionAsset")).upper()
                exact_fee = fill.get(
                    "feeQuoteValueExact", fee_asset in {"BTC", "USDT"}
                ) is True
                fee_quote = _terminal_decimal(
                    fill.get("feeQuoteValue"), label="fill fee quote value"
                )
                if (
                    qty <= 0
                    or quote <= 0
                    or fee < 0
                    or fee_quote < 0
                    or not fee_asset
                    or not exact_fee
                    or not any(
                        _text(event.get("eventType")) == "executionReport"
                        and _text(event.get("clientOrderId")) == client_id
                        and _text(event.get("orderId"))
                        == durable["brokerOrderId"]
                        and _text(event.get("tradeId")) == trade_id
                        and _terminal_decimal(
                            event.get("lastQty"), label="stream fill quantity"
                        )
                        == qty
                        and _terminal_decimal(
                            event.get("commission") or "0",
                            label="stream fill commission",
                        )
                        == fee
                        and _text(event.get("commissionAsset")).upper()
                        == fee_asset
                        for event in stream_payloads
                    )
                ):
                    raise BinanceSpotFirstLiveBootstrapError(
                        "terminal fill/fee valuation is incomplete"
                    )
                fill_qty += qty
                fill_quote += quote
                if fee_asset == "BTC":
                    base_fees += fee
                elif fee_asset == "USDT":
                    cash_fees += fee_quote
                if side == "BUY":
                    bought += qty
                    buy_quote += quote
                else:
                    sold += qty
                    sell_quote += quote
            if (
                executed_qty <= 0
                or executed_quote <= 0
                or fill_qty != executed_qty
                or fill_quote != executed_quote
            ):
                raise BinanceSpotFirstLiveBootstrapError(
                    "terminal fills disagree with FILLED order aggregates"
                )
        if not any(
            _text(event.get("eventType")) == "outboundAccountPosition"
            and int(event.get("eventTime") or 0) >= baseline_ms
            for event in stream_payloads
        ):
            raise BinanceSpotFirstLiveBootstrapError(
                "terminal stream lacks account-position evidence for the fills"
            )
        if set(closed_by_client) != {
            durable["clientOrderId"] for durable, _sealed in natural
        } or any(
            _terminal_client_id(fill)
            not in {durable["clientOrderId"] for durable, _sealed in natural}
            for fill in fills
        ):
            raise BinanceSpotFirstLiveBootstrapError(
                "terminal official history contains an extra order/fill"
            )
        active_start_ms = int(Decimal(str(session["started_epoch"])) * 1000)
        active_end_ms = int(Decimal(str(session["expires_epoch"])) * 1000)
        if any(
            int(order.get("time") or 0) < active_start_ms
            or int(order.get("time") or 0) >= active_end_ms
            or int(order.get("updateTime") or 0) < int(order.get("time") or 0)
            or int(order.get("updateTime") or 0) >= active_end_ms
            for order in closed_orders
        ) or any(
            int(fill.get("time") or 0) < active_start_ms
            or int(fill.get("time") or 0) >= active_end_ms
            for fill in fills
        ):
            raise BinanceSpotFirstLiveBootstrapError(
                "natural broker order/fill occurred outside the active window"
            )
        natural_identity = {
            durable["clientOrderId"]: durable["brokerOrderId"]
            for durable, _sealed in natural
        }
        execution_events = [
            event
            for event in stream_payloads
            if _text(event.get("eventType")) == "executionReport"
        ]
        for event in execution_events:
            client_id = _text(event.get("clientOrderId"))
            if (
                client_id not in natural_identity
                or _text(event.get("orderId")) != natural_identity[client_id]
                or _text(event.get("executionType")).upper()
                not in {"NEW", "TRADE"}
                or _text(event.get("orderStatus")).upper()
                not in {"NEW", "PARTIALLY_FILLED", "FILLED"}
                or int(event.get("eventTime") or 0) < active_start_ms
                or int(event.get("eventTime") or 0) >= active_end_ms
            ):
                raise BinanceSpotFirstLiveBootstrapError(
                    "terminal stream contains an extra/cancelled owned order event"
                )
        actual_stream_trade_keys: list[tuple[str, ...]] = []
        seen_stream_trade_ids: set[str] = set()
        for event in execution_events:
            if _text(event.get("executionType")).upper() != "TRADE":
                continue
            trade_id = _text(event.get("tradeId"))
            if not trade_id or trade_id in seen_stream_trade_ids:
                raise BinanceSpotFirstLiveBootstrapError(
                    "terminal stream trade identity is duplicated"
                )
            seen_stream_trade_ids.add(trade_id)
            actual_stream_trade_keys.append(
                (
                    _text(event.get("clientOrderId")),
                    _text(event.get("orderId")),
                    trade_id,
                    _terminal_decimal_text(
                        _terminal_decimal(
                            event.get("lastQty"), label="stream trade quantity"
                        )
                    ),
                    _terminal_decimal_text(
                        _terminal_decimal(
                            event.get("lastQuoteQty"),
                            label="stream trade quote quantity",
                        )
                    ),
                    _terminal_decimal_text(
                        _terminal_decimal(
                            event.get("commission") or "0",
                            label="stream trade commission",
                        )
                    ),
                    _text(event.get("commissionAsset")).upper(),
                    _text(event.get("orderStatus")).upper(),
                )
            )
        expected_stream_trade_keys: list[tuple[str, ...]] = []
        for client_id in natural_identity:
            client_fills = sorted(
                [fill for fill in fills if _terminal_client_id(fill) == client_id],
                key=lambda row: (int(row.get("time") or 0), _text(row.get("tradeId"))),
            )
            for index, fill in enumerate(client_fills):
                expected_stream_trade_keys.append(
                    (
                        client_id,
                        _text(fill.get("orderId")),
                        _text(fill.get("tradeId")),
                        _terminal_decimal_text(
                            _terminal_decimal(
                                fill.get("quantity"), label="official fill quantity"
                            )
                        ),
                        _terminal_decimal_text(
                            _terminal_decimal(
                                fill.get("quoteQuantity"),
                                label="official fill quote quantity",
                            )
                        ),
                        _terminal_decimal_text(
                            _terminal_decimal(
                                fill.get("commission"),
                                label="official fill commission",
                            )
                        ),
                        _text(fill.get("commissionAsset")).upper(),
                        (
                            "FILLED"
                            if index == len(client_fills) - 1
                            else "PARTIALLY_FILLED"
                        ),
                    )
                )
        if sorted(actual_stream_trade_keys) != sorted(expected_stream_trade_keys):
            raise BinanceSpotFirstLiveBootstrapError(
                "terminal stream trades disagree with official myTrades exactly"
            )
        account_events = [
            event
            for event in stream_payloads
            if _text(event.get("eventType")) == "outboundAccountPosition"
        ]
        latest_account = max(
            account_events, key=lambda row: int(row.get("eventTime") or 0)
        )
        latest_balances = {
            _text(row.get("asset")).upper(): (
                _terminal_decimal_text(
                    _terminal_decimal(row.get("free"), label="stream balance free")
                ),
                _terminal_decimal_text(
                    _terminal_decimal(
                        row.get("locked"), label="stream balance locked"
                    )
                ),
            )
            for row in latest_account.get("balances", [])
            if isinstance(row, Mapping)
        }
        terminal_balances = {
            _text(row.get("asset")).upper(): (
                _terminal_decimal_text(
                    _terminal_decimal(row.get("free"), label="terminal balance free")
                ),
                _terminal_decimal_text(
                    _terminal_decimal(
                        row.get("locked"), label="terminal balance locked"
                    )
                ),
            )
            for row in terminal_truth.get("balances", [])
            if isinstance(row, Mapping)
        }
        if any(
            latest_balances.get(asset) != terminal_balances.get(asset)
            for asset in ("BTC", "USDT")
        ):
            raise BinanceSpotFirstLiveBootstrapError(
                "terminal stream/account BTC-USDT balances disagree"
            )
        baseline = _terminal_decimal(
            session["baseline_base"], label="durable baseline BTC"
        )
        if baseline != _terminal_decimal(
            terminal_truth.get("baselineBase"), label="official baseline BTC"
        ):
            raise BinanceSpotFirstLiveBootstrapError(
                "terminal baseline BTC changed"
            )
        final_base = _terminal_decimal(
            terminal_truth.get("finalBaseTotal"), label="final BTC"
        )
        baseline_quote = _terminal_decimal(
            session["baseline_quote"], label="durable baseline USDT"
        )
        final_quote = _terminal_decimal(
            terminal_truth.get("finalQuoteTotal"), label="final USDT"
        )
        balance_totals = {
            _text(row.get("asset")).upper(): (
                _terminal_decimal(row.get("free"), label="terminal balance free")
                + _terminal_decimal(
                    row.get("locked"), label="terminal balance locked"
                )
            )
            for row in terminal_truth.get("balances", [])
            if isinstance(row, Mapping)
        }
        mark_price = _terminal_decimal(
            terminal_truth.get("markPrice"), label="mark price"
        )
        owned = bought - sold - base_fees
        if (
            owned < 0
            or final_base - baseline != owned
            or balance_totals.get("BTC") != final_base
            or balance_totals.get("USDT")
            != final_quote
            or final_quote
            != baseline_quote - buy_quote - cash_fees + sell_quote
            or mark_price <= 0
        ):
            raise BinanceSpotFirstLiveBootstrapError(
                "terminal owned BTC/baseline attribution changed"
            )
        owner_loss = max(
            Decimal("0"),
            -(sell_quote + owned * mark_price - buy_quote - cash_fees),
        )
        if (
            buy_quote > Decimal("10")
            or owner_loss >= Decimal("1")
            or owner_loss
            != _terminal_decimal(evidence.get("ownerLoss"), label="owner loss")
        ):
            raise BinanceSpotFirstLiveBootstrapError(
                "terminal cap/loss proof changed"
            )

    def consume_terminal(
        self,
        *,
        bootstrap_id: str,
        session_id: str,
        permit_id: str,
        permit_hash: str,
        evidence: Mapping[str, Any],
        evidence_hash: str,
    ) -> dict[str, Any]:
        canonical_evidence_hash = _hash_document(evidence)
        normalized_evidence_hash = _text(evidence_hash).lower()
        if (
            _SHA256_RE.fullmatch(normalized_evidence_hash) is None
            or not secrets.compare_digest(
                canonical_evidence_hash, normalized_evidence_hash
            )
        ):
            raise BinanceSpotFirstLiveBootstrapError(
                "terminal evidence hash is invalid"
            )
        outcome = _text(evidence.get("outcome")).upper()
        try:
            actual_runtime = Decimal(str(evidence.get("actualRuntimeSeconds")))
            exchange_runtime = Decimal(
                str(evidence.get("exchangeRuntimeSeconds"))
            )
            monotonic_runtime = Decimal(
                str(evidence.get("monotonicRuntimeSeconds"))
            )
            evidence_activated = Decimal(str(evidence.get("activatedEpoch")))
            evidence_ends = Decimal(str(evidence.get("activeEndsEpoch")))
            terminal_observed = Decimal(
                str(evidence.get("terminalObservedEpoch"))
            )
            owner_loss = Decimal(str(evidence.get("ownerLoss")))
        except (InvalidOperation, ValueError) as exc:
            raise BinanceSpotFirstLiveBootstrapError(
                "terminal runtime/loss lineage is invalid"
            ) from exc
        if not all(
            value.is_finite()
            for value in (
                actual_runtime,
                exchange_runtime,
                monotonic_runtime,
                evidence_activated,
                evidence_ends,
                terminal_observed,
                owner_loss,
            )
        ):
            raise BinanceSpotFirstLiveBootstrapError(
                "terminal runtime/loss lineage is non-finite"
            )
        trusted_now = _terminal_decimal(
            self.clock(), label="trusted terminal current epoch"
        )
        if trusted_now < evidence_ends or terminal_observed > trusted_now + Decimal("1"):
            raise BinanceSpotFirstLiveBootstrapError(
                "terminal evidence is future-dated or the 7200-second window is incomplete"
            )
        safely_terminal = bool(
            evidence.get("functionalCapabilityReset") is True
            and evidence.get("newEntriesBlocked") is True
            and evidence.get("promotionEligible") is False
            and evidence.get("useAsPromotionEvidence") is False
            and evidence.get("preexistingBaselinePreserved") is True
            and evidence.get("orderableResidualZero") is True
            and evidence.get("openOrdersZero") is True
        )
        functional_wiring_passed = bool(
            safely_terminal
            and evidence.get("functionalWiringPassed") is True
            and evidence.get("exactTwoHourRuntimeComplete") is True
            and evidence.get("runtimeClockConsistencyProven") is True
            and actual_runtime >= Decimal("7200")
            and exchange_runtime >= Decimal("7200")
            and monotonic_runtime >= Decimal("7200")
            and evidence_ends - evidence_activated == Decimal("7200")
            and terminal_observed >= evidence_ends
            and evidence.get("naturalBuyFilled") is True
            and evidence.get("naturalSellFilled") is True
            and evidence.get("fullRoundTripWiringPassed") is True
            and evidence.get("orderCapsAndNoReentryProven") is True
            and evidence.get("feesQuoteExact") is True
            and owner_loss < Decimal("1")
            and evidence.get("baselineRestoredWithinExchangePrecision") is True
            and evidence.get("externalActivityAbsent") is True
            and evidence.get("privateStreamGapRecoveredCleanupOnly") is False
            and evidence.get("exclusiveAccountOperatorAttested") is False
            and evidence.get("exclusiveAccountIndependentlyProven") is True
            and evidence.get("noManualTradingAttested") is False
            and evidence.get("noManualTradingIndependentlyProven") is True
            and evidence.get("noExternalBotsAttested") is False
            and evidence.get("noExternalBotsIndependentlyProven") is True
            and evidence.get("noOtherApiKeysAttested") is False
            and evidence.get("noOtherApiKeysIndependentlyProven") is True
            and evidence.get("accountExclusivityProofDurable") is True
            and _SHA256_RE.fullmatch(
                _text(evidence.get("accountExclusivityProofHash")).lower()
            )
            is not None
            and evidence.get("accountExclusivityPhaseChainComplete") is True
            and evidence.get("accountExclusivityRestartVerifiable") is True
            and _SHA256_RE.fullmatch(
                _text(evidence.get("accountExclusivityPhaseChainHash")).lower()
            )
            is not None
            and int(evidence.get("accountExclusivityPhaseProofCount") or 0) >= 4
            and int(evidence.get("accountExclusivityPhaseProofCount") or 0)
            == int(
                evidence.get("accountExclusivityPhaseProofRequiredCount") or 0
            )
            and outcome == "PASS_FULL_ROUND_TRIP"
        )
        eligible = bool(
            functional_wiring_passed
            and outcome == "PASS_FULL_ROUND_TRIP"
            and evidence.get("accountWideCausalClosureProven") is True
            and evidence.get("otherApiKeysAbsenceAuthoritativelyProven") is True
        )
        if not safely_terminal:
            raise BinanceSpotFirstLiveBootstrapError(
                "bootstrap terminal evidence is not safely sealed"
        )
        code_hash = _text(self.code_hash_reader()).lower()
        with self._lock, closing(self._connect()) as connection:
            self._begin_verified_write(connection)
            try:
                if evidence.get("functionalWiringPassed") is True:
                    self._verify_durable_terminal_execution(
                        bootstrap_id=bootstrap_id,
                        session_id=session_id,
                        evidence=evidence,
                        evidence_hash=normalized_evidence_hash,
                        connection=connection,
                    )
            except Exception:
                connection.rollback()
                raise
            row = connection.execute(
                """
                SELECT activated_epoch, active_ends_epoch
                FROM binance_spot_first_live_bootstraps
                WHERE bootstrap_id=? AND state='ACTIVE'
                """,
                (_text(bootstrap_id),),
            ).fetchone()
            if (
                row is None
                or Decimal(str(row["activated_epoch"])) != evidence_activated
                or Decimal(str(row["active_ends_epoch"])) != evidence_ends
            ):
                connection.rollback()
                raise BinanceSpotFirstLiveBootstrapError(
                    "terminal activation lineage changed"
                )
            cursor = connection.execute(
                """
                UPDATE binance_spot_first_live_bootstraps
                SET state='CONSUMED', capability_hash='', claim_token_hash='',
                    final_evidence_hash=?, e2e_evidence_eligible=?,
                    functional_wiring_passed=?,
                    terminal_observed_epoch=?, updated_epoch=?,
                    detail=?
                WHERE bootstrap_id=? AND state='ACTIVE' AND session_id=?
                    AND active_permit_id=? AND active_permit_hash=? AND code_hash=?
                """,
                (
                    normalized_evidence_hash,
                    1 if eligible else 0,
                    1 if functional_wiring_passed else 0,
                    float(terminal_observed),
                    float(self.clock()),
                    (
                        "terminal first-live evidence eligible for separate release review"
                        if eligible
                        else "terminal first-live capability consumed without release evidence"
                    ),
                    _text(bootstrap_id),
                    _text(session_id),
                    _text(permit_id),
                    _text(permit_hash).lower(),
                    code_hash,
                ),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise BinanceSpotFirstLiveBootstrapError(
                    "bootstrap terminal identity changed"
                )
            connection.commit()
        return self.status(bootstrap_id)


__all__ = [
    "BinanceSpotFirstLiveBootstrapError",
    "DurableBinanceSpotFirstLiveBootstrapStore",
    "BOOTSTRAP_DB_SCHEMA_FINGERPRINT",
    "ROUTE_KEY",
    "SCHEMA_VERSION",
    "compute_binance_spot_functional_code_hash",
    "default_binance_spot_functional_code_paths",
    "default_binance_spot_functional_code_hash",
]
