from __future__ import annotations

"""Two-request redacted Binance API-key permission/lock diagnostic."""

import hashlib
import json
import os
import urllib.parse

from live_trader.binance_spot_functional_transport import (
    BINANCE_SPOT_PRODUCTION_ORIGIN,
    assert_binance_spot_production_origin,
)
from live_trader.live_adapters import (
    PreparedRequest,
    binance_timestamp_ms,
    env_value,
    send_prepared_request,
    sign_binance_query,
)


GATE = "BINANCE_ACCOUNT_PERMISSION_SIGNED_GET_ENABLED"
TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
RESTRICTIONS = "/sapi/v1/account/apiRestrictions"
TRADING_STATUS = "/sapi/v1/account/apiTradingStatus"
ALLOWLIST = frozenset({RESTRICTIONS, TRADING_STATUS})


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


def _exact_bool(value: object, label: str) -> bool:
    if type(value) is not bool:
        raise RuntimeError(label + " is not an exact boolean")
    return value


def main() -> int:
    attempts: list[dict[str, object]] = []
    try:
        if os.getenv(GATE, "").lower() not in TRUE_VALUES:
            raise RuntimeError("diagnostic gate is not enabled")
        if (
            os.getenv("LIVE_TRADER_ENABLE_REAL_ORDERS", "").lower()
            in TRUE_VALUES
            or os.getenv("BINANCE_SPOT_FUNCTIONAL_LIVE_ENABLED", "").lower()
            in TRUE_VALUES
        ):
            raise RuntimeError("all live order gates must remain false")
        origin = assert_binance_spot_production_origin(
            env_value("BINANCE_BASE_URL") or BINANCE_SPOT_PRODUCTION_ORIGIN
        )
        api_key = env_value("BINANCE_API_KEY")
        api_secret = env_value("BINANCE_API_SECRET")
        if not api_key or not api_secret:
            raise RuntimeError("Binance credentials unavailable")

        def get_once(endpoint: str) -> dict[str, object]:
            if endpoint not in ALLOWLIST:
                raise RuntimeError("endpoint is not allowlisted")
            query = {"recvWindow": 5000, "timestamp": binance_timestamp_ms()}
            encoded = sign_binance_query(query, api_secret)
            request = PreparedRequest(
                provider="binance-account-permission-diagnostic",
                method="GET",
                url=origin + endpoint + "?" + encoded,
                endpoint=endpoint,
                headers={"X-MBX-APIKEY": api_key},
                safe_headers={"X-MBX-APIKEY_configured": True},
                body=None,
                query={**query, "signature": "***"},
                blocked_reasons=[],
            )
            parsed = urllib.parse.urlsplit(request.url)
            if (
                request.method != "GET"
                or parsed.scheme != "https"
                or parsed.netloc != "api.binance.com"
                or parsed.path != endpoint
                or parsed.fragment
                or request.body is not None
            ):
                raise RuntimeError("request origin/shape changed")
            response = send_prepared_request(request, timeout_seconds=10.0)
            attempts.append(
                {
                    "endpoint": endpoint,
                    "method": "GET",
                    "statusCode": int(response.get("statusCode") or 0),
                    "redirectBlocked": response.get("redirectBlocked") is True,
                }
            )
            if (
                response.get("ok") is not True
                or response.get("redirectBlocked") is True
                or not isinstance(response.get("json"), dict)
            ):
                raise RuntimeError("signed GET failed without retry:" + endpoint)
            return dict(response["json"])

        restrictions = get_once(RESTRICTIONS)
        trading = get_once(TRADING_STATUS)
        trading_data = trading.get("data")
        if not isinstance(trading_data, dict):
            raise RuntimeError("trading status data is malformed")
        result = {
            "schemaVersion": "binance-account-permission-get-redacted/v1",
            "ok": True,
            "originExact": True,
            "permissions": {
                "enableReading": _exact_bool(
                    restrictions.get("enableReading"), "enableReading"
                ),
                "enableSpotAndMarginTrading": _exact_bool(
                    restrictions.get("enableSpotAndMarginTrading"),
                    "enableSpotAndMarginTrading",
                ),
                "ipRestrict": _exact_bool(
                    restrictions.get("ipRestrict"), "ipRestrict"
                ),
                "enableWithdrawals": _exact_bool(
                    restrictions.get("enableWithdrawals"),
                    "enableWithdrawals",
                ),
                "enableMargin": _exact_bool(
                    restrictions.get("enableMargin"), "enableMargin"
                ),
                "enableFutures": _exact_bool(
                    restrictions.get("enableFutures"), "enableFutures"
                ),
                "responseHash": _digest(restrictions),
            },
            "tradingStatus": {
                "locked": _exact_bool(trading_data.get("isLocked"), "isLocked"),
                "responseHash": _digest(trading),
            },
            "transport": {
                "physicalGetAttemptCount": len(attempts),
                "nonGetAttemptCount": sum(
                    1 for row in attempts if row["method"] != "GET"
                ),
                "redirectCount": sum(
                    1 for row in attempts if row["redirectBlocked"]
                ),
                "retryCount": 0,
                "mutationAttemptCount": 0,
            },
            "releaseFlagsChanged": False,
            "candidateCreated": False,
            "permitCreated": False,
        }
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0
    except Exception as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "errorType": type(exc).__name__,
                    "errorHash": hashlib.sha256(
                        str(exc).encode("utf-8")
                    ).hexdigest(),
                    "physicalGetAttemptCount": len(attempts),
                    "retryCount": 0,
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


if __name__ == "__main__":
    raise SystemExit(main())
