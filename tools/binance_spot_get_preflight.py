from __future__ import annotations

"""One-shot, redacted Binance Spot GET-only diagnostic.

This command has no mutation builder.  It accepts only the exact official
production origin and emits counts and SHA-256 digests, never raw broker IDs,
balances, credentials, signatures, orders, trades, or transfer rows.
"""

from collections import Counter
import hashlib
import json
import os
import urllib.parse

from live_trader.binance_spot_functional_transport import (
    BINANCE_SPOT_ACCOUNT_ENDPOINT,
    BINANCE_SPOT_ALL_ORDERS_ENDPOINT,
    BINANCE_SPOT_EXCHANGE_INFO_ENDPOINT,
    BINANCE_SPOT_MY_TRADES_ENDPOINT,
    BINANCE_SPOT_OPEN_ORDERS_ENDPOINT,
    BINANCE_SPOT_PRODUCTION_ORIGIN,
    BINANCE_SPOT_TICKER_PRICE_ENDPOINT,
    assert_binance_spot_production_origin,
    binance_api_key_fingerprint,
    build_binance_spot_get_request,
)
from live_trader.live_adapters import (
    PreparedRequest,
    _clear_binance_time_offset_cache,
    binance_timestamp_ms,
    env_value,
    refresh_binance_time_offset,
    send_prepared_request,
    sign_binance_query,
)


TRANSFER_ENDPOINT = "/sapi/v1/asset/transfer"
TIME_ENDPOINT = "/api/v3/time"
DIAGNOSTIC_GATE = "BINANCE_SPOT_DIAGNOSTIC_SIGNED_GET_PREFLIGHT_ENABLED"
TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
TRANSFER_WINDOW_START_MS = 1_785_283_200_000  # 2026-07-29T00:00:00Z
TRANSFER_WINDOW_END_MS = 1_785_887_999_000  # 2026-08-04T23:59:59Z
ALLOWED_ENDPOINTS = frozenset(
    {
        BINANCE_SPOT_ACCOUNT_ENDPOINT,
        BINANCE_SPOT_OPEN_ORDERS_ENDPOINT,
        BINANCE_SPOT_ALL_ORDERS_ENDPOINT,
        BINANCE_SPOT_MY_TRADES_ENDPOINT,
        BINANCE_SPOT_EXCHANGE_INFO_ENDPOINT,
        BINANCE_SPOT_TICKER_PRICE_ENDPOINT,
        TRANSFER_ENDPOINT,
        TIME_ENDPOINT,
    }
)


def _digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


class Preflight:
    def __init__(self) -> None:
        if os.getenv(DIAGNOSTIC_GATE, "").lower() not in TRUE_VALUES:
            raise RuntimeError("diagnostic gate is not enabled")
        if os.getenv("LIVE_TRADER_ENABLE_REAL_ORDERS", "").lower() in TRUE_VALUES:
            raise RuntimeError("ordinary real-order gate must be false")
        if (
            os.getenv("BINANCE_SPOT_FUNCTIONAL_LIVE_ENABLED", "").lower()
            in TRUE_VALUES
        ):
            raise RuntimeError("functional live gate must be false")
        self.origin = assert_binance_spot_production_origin(
            env_value("BINANCE_BASE_URL") or BINANCE_SPOT_PRODUCTION_ORIGIN
        )
        self.api_key = env_value("BINANCE_API_KEY")
        self.api_secret = env_value("BINANCE_API_SECRET")
        if not self.api_key or not self.api_secret:
            raise RuntimeError("Binance credentials unavailable")
        self.credential_fingerprint = binance_api_key_fingerprint(self.api_key)
        self.attempts: list[dict[str, object]] = []
        self.timestamp_retry_count = 0
        self.timestamp_sync_count = 0
        _clear_binance_time_offset_cache()

    def _send_exact(self, prepared: PreparedRequest) -> dict[str, object]:
        parsed = urllib.parse.urlsplit(prepared.url)
        if (
            prepared.method != "GET"
            or prepared.endpoint not in ALLOWED_ENDPOINTS
            or parsed.scheme != "https"
            or parsed.netloc != "api.binance.com"
            or parsed.hostname != "api.binance.com"
            or parsed.port is not None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path != prepared.endpoint
            or parsed.fragment
            or prepared.body is not None
            or not prepared.can_send
        ):
            raise RuntimeError("diagnostic request shape/origin is not exact")
        response = send_prepared_request(prepared, timeout_seconds=10.0)
        self.attempts.append(
            {
                "endpoint": prepared.endpoint,
                "method": "GET",
                "statusCode": int(response.get("statusCode") or 0),
                "ok": response.get("ok") is True,
                "redirectBlocked": response.get("redirectBlocked") is True,
            }
        )
        if response.get("redirectBlocked") is True:
            raise RuntimeError("redirect blocked")
        return response

    def _sync_timestamp_once(self) -> None:
        self.timestamp_retry_count += 1
        self.timestamp_sync_count += 1
        refresh_binance_time_offset(timeout_seconds=5.0, futures=False)
        # The cache is cleared in __init__, so this successful call is exactly
        # one physical official time GET.
        self.attempts.append(
            {
                "endpoint": TIME_ENDPOINT,
                "method": "GET",
                "statusCode": 200,
                "ok": True,
                "redirectBlocked": False,
            }
        )

    def get(self, endpoint: str, query: dict[str, object]) -> object:
        for attempt in range(2):
            response = self._send_exact(
                build_binance_spot_get_request(endpoint, query)
            )
            payload = response.get("json")
            code = (
                int(payload.get("code") or 0)
                if isinstance(payload, dict)
                else 0
            )
            if code == -1021 and attempt == 0:
                self._sync_timestamp_once()
                continue
            if response.get("ok") is not True:
                raise RuntimeError(
                    "official Binance GET failed:" + endpoint + ":" + str(code)
                )
            return payload
        raise RuntimeError("timestamp retry exhausted:" + endpoint)

    def _transfer_request(self, page: int) -> PreparedRequest:
        params = {
            "type": "MAIN_UMFUTURE",
            "startTime": TRANSFER_WINDOW_START_MS,
            "endTime": TRANSFER_WINDOW_END_MS,
            "current": page,
            "size": 100,
            "recvWindow": 5000,
            "timestamp": binance_timestamp_ms(),
        }
        encoded = sign_binance_query(params, self.api_secret)
        return PreparedRequest(
            provider="binance-diagnostic-signed-get",
            method="GET",
            url=self.origin + TRANSFER_ENDPOINT + "?" + encoded,
            endpoint=TRANSFER_ENDPOINT,
            headers={"X-MBX-APIKEY": self.api_key},
            safe_headers={"X-MBX-APIKEY_configured": True},
            body=None,
            query={**params, "signature": "***"},
            blocked_reasons=[],
        )

    def transfer_page(self, page: int) -> tuple[int, list[dict[str, object]]]:
        for attempt in range(2):
            response = self._send_exact(self._transfer_request(page))
            payload = response.get("json")
            code = (
                int(payload.get("code") or 0)
                if isinstance(payload, dict)
                else 0
            )
            if code == -1021 and attempt == 0:
                self._sync_timestamp_once()
                continue
            if response.get("ok") is not True or not isinstance(payload, dict):
                raise RuntimeError(
                    "official Binance transfer GET failed:" + str(code)
                )
            rows = payload.get("rows")
            total = payload.get("total")
            if (
                not isinstance(rows, list)
                or isinstance(total, bool)
                or not isinstance(total, int)
                or any(not isinstance(row, dict) for row in rows)
            ):
                raise RuntimeError("transfer response shape invalid")
            return total, [dict(row) for row in rows]
        raise RuntimeError("timestamp retry exhausted:" + TRANSFER_ENDPOINT)

    def run(self) -> dict[str, object]:
        account = self.get(
            BINANCE_SPOT_ACCOUNT_ENDPOINT, {"omitZeroBalances": False}
        )
        open_orders = self.get(
            BINANCE_SPOT_OPEN_ORDERS_ENDPOINT, {"symbol": "BTCUSDT"}
        )
        all_orders = self.get(
            BINANCE_SPOT_ALL_ORDERS_ENDPOINT,
            {"symbol": "BTCUSDT", "limit": 1000},
        )
        trades = self.get(
            BINANCE_SPOT_MY_TRADES_ENDPOINT,
            {"symbol": "BTCUSDT", "limit": 1000},
        )
        exchange = self.get(
            BINANCE_SPOT_EXCHANGE_INFO_ENDPOINT, {"symbol": "BTCUSDT"}
        )
        ticker = self.get(
            BINANCE_SPOT_TICKER_PRICE_ENDPOINT, {"symbol": "BTCUSDT"}
        )
        if not isinstance(account, dict):
            raise RuntimeError("account response malformed")
        histories = (
            (open_orders, "openOrders"),
            (all_orders, "allOrders"),
            (trades, "myTrades"),
        )
        for value, label in histories:
            if not isinstance(value, list) or any(
                not isinstance(row, dict) for row in value
            ):
                raise RuntimeError(label + " response malformed")
        symbols = exchange.get("symbols") if isinstance(exchange, dict) else None
        if (
            not isinstance(symbols, list)
            or len(symbols) != 1
            or not isinstance(symbols[0], dict)
        ):
            raise RuntimeError("exchangeInfo BTCUSDT response malformed")
        if not isinstance(ticker, dict) or ticker.get("symbol") != "BTCUSDT":
            raise RuntimeError("ticker response malformed")

        transfer_total, transfer_rows = self.transfer_page(1)
        page = 2
        while len(transfer_rows) < transfer_total:
            if page > 100:
                raise RuntimeError("transfer pagination exceeded exact maximum")
            current_total, rows = self.transfer_page(page)
            if current_total != transfer_total or not rows:
                raise RuntimeError("transfer pagination incomplete")
            transfer_rows.extend(rows)
            page += 1
        if len(transfer_rows) != transfer_total:
            raise RuntimeError("transfer total mismatch")

        balances = account.get("balances")
        permissions = account.get("permissions")
        if not isinstance(balances, list) or not isinstance(permissions, list):
            raise RuntimeError("account balances/permissions malformed")
        nonzero_balances = 0
        for row in balances:
            if not isinstance(row, dict):
                raise RuntimeError("balance row malformed")
            try:
                if float(row.get("free") or 0) != 0 or float(
                    row.get("locked") or 0
                ) != 0:
                    nonzero_balances += 1
            except (TypeError, ValueError) as exc:
                raise RuntimeError("balance amount malformed") from exc

        symbol = symbols[0]
        filters = symbol.get("filters")
        if not isinstance(filters, list):
            raise RuntimeError("symbol filters malformed")
        matching_transfer_count = sum(
            1
            for row in transfer_rows
            if str(row.get("type") or "").upper() == "MAIN_UMFUTURE"
            and str(row.get("asset") or "").upper() == "USDT"
            and str(row.get("status") or "").upper() == "CONFIRMED"
            and str(row.get("amount") or "").strip()
            in {"10", "10.0", "10.00", "10.00000000"}
        )
        order_statuses = Counter(
            str(row.get("status") or "UNKNOWN").upper() for row in all_orders
        )
        trade_sides = Counter(
            "BUY"
            if row.get("isBuyer") is True
            else "SELL"
            if row.get("isBuyer") is False
            else "UNKNOWN"
            for row in trades
        )
        endpoint_counts = Counter(
            str(row["endpoint"]) for row in self.attempts
        )
        return {
            "schemaVersion": "binance-spot-signed-get-preflight-redacted/v1",
            "ok": True,
            "originExact": True,
            "credentialConfigured": True,
            "credentialIdentityStable": (
                binance_api_key_fingerprint(env_value("BINANCE_API_KEY"))
                == self.credential_fingerprint
            ),
            "account": {
                "canTrade": account.get("canTrade") is True,
                "canWithdraw": account.get("canWithdraw") is True,
                "canDeposit": account.get("canDeposit") is True,
                "permissionCount": len(permissions),
                "permissionsHash": _digest(
                    sorted(str(item) for item in permissions)
                ),
                "balanceAssetCount": len(balances),
                "nonzeroBalanceAssetCount": nonzero_balances,
                "responseHash": _digest(account),
            },
            "btcUsdt": {
                "openOrderCount": len(open_orders),
                "openOrdersHash": _digest(open_orders),
                "recentOrderCount": len(all_orders),
                "recentOrderStatusCounts": dict(sorted(order_statuses.items())),
                "recentOrdersHash": _digest(all_orders),
                "recentTradeCount": len(trades),
                "recentTradeSideCounts": dict(sorted(trade_sides.items())),
                "recentTradesHash": _digest(trades),
                "symbolStatus": str(symbol.get("status") or ""),
                "spotTradingAllowed": symbol.get("isSpotTradingAllowed") is True,
                "filterCount": len(filters),
                "exchangeRuleHash": _digest(symbol),
                "tickerHash": _digest(ticker),
            },
            "historicalSpotToUsdMTransfer": {
                "windowStartMs": TRANSFER_WINDOW_START_MS,
                "windowEndMs": TRANSFER_WINDOW_END_MS,
                "pageCount": page - 1,
                "recordCount": transfer_total,
                "exactConfirmed10UsdtMatchCount": matching_transfer_count,
                "recordsHash": _digest(transfer_rows),
            },
            "transport": {
                "physicalGetAttemptCount": len(self.attempts),
                "endpointCounts": dict(sorted(endpoint_counts.items())),
                "timestampSyncCount": self.timestamp_sync_count,
                "timestampRetryCount": self.timestamp_retry_count,
                "redirectCount": sum(
                    1 for row in self.attempts if row["redirectBlocked"]
                ),
                "nonGetAttemptCount": sum(
                    1 for row in self.attempts if row["method"] != "GET"
                ),
                "mutationAttemptCount": 0,
            },
            "releaseFlagsChanged": False,
            "candidateCreated": False,
            "permitCreated": False,
        }


def main() -> int:
    try:
        result = Preflight().run()
    except Exception as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "errorType": type(exc).__name__,
                    "errorHash": hashlib.sha256(
                        str(exc).encode("utf-8")
                    ).hexdigest(),
                    "mutationAttemptCount": 0,
                    "releaseFlagsChanged": False,
                    "candidateCreated": False,
                    "permitCreated": False,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 1
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
