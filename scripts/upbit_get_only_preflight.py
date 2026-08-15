from __future__ import annotations

"""One-shot official Upbit GET-only readiness snapshot.

This script has no mutation builder and never changes a live/release flag.  It
loads the two Upbit secrets from the current-user encrypted store only for this
process, uses the production GET-only client, and emits aggregate/redacted
evidence.  Redirects are never followed and requests are never retried.
"""

import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import hmac
import json
import os
from pathlib import Path
import sys
from typing import Any, Mapping
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from live_trader.env_loader import live_secret_name, live_secret_store
from live_trader.live_adapters import PreparedRequest, send_prepared_request
from live_trader.upbit_account_exclusivity import (
    upbit_spot_credential_binding_sha256,
)
from live_trader.upbit_continuous_functional import _stable_hash
from live_trader.upbit_functional_transport import (
    OfficialUpbitFunctionalGetClient,
    upbit_credential_fingerprint,
)
from live_trader.upbit_functional_truth import (
    CLOSED_LIMIT,
    MAX_OPEN_PAGES,
    OPEN_PAGE_LIMIT,
    QUANTITY_SCALE,
    QUANTITY_STEP,
    SYMBOL,
    UPBIT_ACCOUNTS_ENDPOINT,
    UPBIT_API_KEYS_ENDPOINT,
    UPBIT_CLOSED_ORDERS_ENDPOINT,
    UPBIT_OPEN_ORDERS_ENDPOINT,
    UPBIT_ORDER_CHANCE_ENDPOINT,
    UPBIT_TICKER_ENDPOINT,
    _account_map,
    _account_rows,
    _chance,
    _decimal,
    _decimal_text,
    _rows,
    _unique_orders,
    _upper,
    _utc_text,
)


OFFICIAL_ORIGIN = "https://api.upbit.com"
GET_ONLY_GATE = "UPBIT_GET_ONLY_PREFLIGHT_ENABLED"


class CountingGetOnlySender:
    def __init__(self) -> None:
        self.physical_attempt_count = 0
        self.authenticated_get_count = 0
        self.redirect_count = 0
        self.endpoint_counts: Counter[str] = Counter()

    def __call__(self, request: PreparedRequest) -> Mapping[str, Any]:
        parsed = urlsplit(request.url)
        if (
            request.method != "GET"
            or request.body is not None
            or request.provider != "upbit-functional-read"
            or f"{parsed.scheme}://{parsed.netloc}" != OFFICIAL_ORIGIN
            or parsed.path != request.endpoint
            or request.safe_headers.get("authorization_configured") is not True
        ):
            raise RuntimeError("upbit-get-only-preflight-request-shape-invalid")
        self.physical_attempt_count += 1
        self.authenticated_get_count += 1
        self.endpoint_counts[request.endpoint] += 1
        response = dict(send_prepared_request(request, timeout_seconds=10.0))
        if response.get("redirectBlocked") is True:
            self.redirect_count += 1
        return response


def _read_all_open(client: OfficialUpbitFunctionalGetClient) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for page in range(1, MAX_OPEN_PAGES + 1):
        current = _rows(
            client.get(
                UPBIT_OPEN_ORDERS_ENDPOINT,
                (
                    ("states[]", "wait"),
                    ("states[]", "watch"),
                    ("page", str(page)),
                    ("limit", str(OPEN_PAGE_LIMIT)),
                    ("order_by", "asc"),
                ),
            ),
            "preflight-open-orders",
        )
        rows.extend(current)
        if len(current) < OPEN_PAGE_LIMIT:
            return _unique_orders(rows, "preflight-open-orders")
    raise RuntimeError("upbit-get-only-preflight-open-pagination-exhausted")


def _read_recent_closed(
    client: OfficialUpbitFunctionalGetClient,
    *,
    now: datetime,
) -> list[dict[str, Any]]:
    start = now - timedelta(days=7) + timedelta(seconds=1)
    rows = _rows(
        client.get(
            UPBIT_CLOSED_ORDERS_ENDPOINT,
            (
                ("states[]", "done"),
                ("states[]", "cancel"),
                ("start_time", _utc_text(start)),
                ("end_time", _utc_text(now)),
                ("limit", str(CLOSED_LIMIT)),
                ("order_by", "asc"),
            ),
        ),
        "preflight-recent-closed-orders",
    )
    if len(rows) >= CLOSED_LIMIT:
        raise RuntimeError("upbit-get-only-preflight-closed-truncation-possible")
    return _unique_orders(rows, "preflight-recent-closed-orders")


def _api_key_inventory(
    client: OfficialUpbitFunctionalGetClient,
    *,
    access_key: str,
    now: datetime,
) -> dict[str, Any]:
    rows = _rows(
        client.get(UPBIT_API_KEYS_ENDPOINT, ()),
        "preflight-api-key-inventory",
    )
    raw_keys: list[str] = []
    expiries: list[datetime] = []
    expiry_hash_inputs: list[str] = []
    for row in rows:
        raw_access_key = str(row.get("access_key") or "").strip()
        expire_text = str(row.get("expire_at") or "").strip()
        if not raw_access_key or not expire_text:
            raise RuntimeError(
                "upbit-get-only-preflight-api-key-row-incomplete"
            )
        try:
            expires_at = datetime.fromisoformat(
                expire_text.replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise RuntimeError(
                "upbit-get-only-preflight-api-key-expiry-invalid"
            ) from exc
        if expires_at.tzinfo is None or expires_at.utcoffset() is None:
            raise RuntimeError(
                "upbit-get-only-preflight-api-key-expiry-timezone-missing"
            )
        raw_keys.append(raw_access_key)
        expiries.append(expires_at.astimezone(timezone.utc))
        expiry_hash_inputs.append(
            hashlib.sha256(expire_text.encode("utf-8")).hexdigest()
        )
    if len(raw_keys) != len(set(raw_keys)):
        raise RuntimeError("upbit-get-only-preflight-api-key-duplicate")
    current_match_count = sum(
        1 for item in raw_keys if hmac.compare_digest(item, access_key)
    )
    listed_count = len(rows)
    nonexpired_count = sum(1 for item in expiries if item > now)
    current_nonexpired_match_count = sum(
        1
        for item, expires_at in zip(raw_keys, expiries)
        if expires_at > now and hmac.compare_digest(item, access_key)
    )
    expired_count = listed_count - nonexpired_count
    unknown_listed_count = listed_count - current_match_count
    unknown_nonexpired_count = (
        nonexpired_count - current_nonexpired_match_count
    )
    expiry_summary = {
        "listedKeyCount": listed_count,
        "nonexpiredKeyCount": nonexpired_count,
        "expiredKeyCount": expired_count,
        "expiryValueHashes": sorted(expiry_hash_inputs),
    }
    return {
        "listedKeyCount": listed_count,
        "activeKeyCount": nonexpired_count,
        "nonexpiredKeyCount": nonexpired_count,
        "expiredKeyCount": expired_count,
        "currentCredentialMatchCount": current_match_count,
        "currentNonexpiredCredentialMatchCount": (
            current_nonexpired_match_count
        ),
        "unknownListedKeyCount": unknown_listed_count,
        "unknownKeyCount": unknown_nonexpired_count,
        "unknownNonexpiredKeyCount": unknown_nonexpired_count,
        "expirySummaryHash": _stable_hash(expiry_summary),
    }


def run_api_keys_only() -> dict[str, Any]:
    if os.environ.get(GET_ONLY_GATE, "").strip().lower() != "true":
        raise RuntimeError("upbit-get-only-preflight-gate-disabled")
    if os.environ.get("LIVE_TRADER_ENABLE_REAL_ORDERS", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        raise RuntimeError("upbit-get-only-preflight-global-orders-must-be-off")
    store = live_secret_store()
    access_key = store.get(live_secret_name("UPBIT_ACCESS_KEY"))
    secret_key = store.get(live_secret_name("UPBIT_SECRET_KEY"))
    if not access_key or not secret_key:
        raise RuntimeError("upbit-get-only-preflight-dpapi-credentials-missing")
    previous_access = os.environ.get("UPBIT_ACCESS_KEY")
    previous_secret = os.environ.get("UPBIT_SECRET_KEY")
    previous_origin = os.environ.get("UPBIT_BASE_URL")
    sender = CountingGetOnlySender()
    try:
        os.environ["UPBIT_ACCESS_KEY"] = access_key
        os.environ["UPBIT_SECRET_KEY"] = secret_key
        os.environ["UPBIT_BASE_URL"] = OFFICIAL_ORIGIN
        account_fingerprint = upbit_credential_fingerprint(access_key)
        client = OfficialUpbitFunctionalGetClient(
            expected_account_fingerprint=account_fingerprint,
            sender=sender,
        )
        inventory = _api_key_inventory(
            client,
            access_key=access_key,
            now=datetime.now(timezone.utc),
        )
        summary = {
            "schemaVersion": "upbit-official-api-key-get-only-preflight/v1",
            "observedAt": _utc_text(datetime.now(timezone.utc)),
            "origin": OFFICIAL_ORIGIN,
            "accountFingerprint": account_fingerprint,
            "apiKeyInventory": inventory,
            "transport": {
                "physicalAttemptCount": sender.physical_attempt_count,
                "authenticatedGetCount": sender.authenticated_get_count,
                "retryCount": 0,
                "redirectCount": sender.redirect_count,
                "postCount": 0,
                "deleteCount": 0,
                "cancelCount": 0,
                "orderMutationCount": 0,
                "endpointCounts": dict(sorted(sender.endpoint_counts.items())),
            },
            "releaseFlagsChanged": False,
            "candidateCreated": False,
            "permitCreated": False,
        }
        return {**summary, "evidenceHash": _stable_hash(summary)}
    finally:
        access_key = ""
        secret_key = ""
        for name, value in (
            ("UPBIT_ACCESS_KEY", previous_access),
            ("UPBIT_SECRET_KEY", previous_secret),
            ("UPBIT_BASE_URL", previous_origin),
        ):
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def run() -> dict[str, Any]:
    if os.environ.get(GET_ONLY_GATE, "").strip().lower() != "true":
        raise RuntimeError("upbit-get-only-preflight-gate-disabled")
    if os.environ.get("LIVE_TRADER_ENABLE_REAL_ORDERS", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        raise RuntimeError("upbit-get-only-preflight-global-orders-must-be-off")

    store = live_secret_store()
    access_key = store.get(live_secret_name("UPBIT_ACCESS_KEY"))
    secret_key = store.get(live_secret_name("UPBIT_SECRET_KEY"))
    if not access_key or not secret_key:
        raise RuntimeError("upbit-get-only-preflight-dpapi-credentials-missing")
    previous_access = os.environ.get("UPBIT_ACCESS_KEY")
    previous_secret = os.environ.get("UPBIT_SECRET_KEY")
    previous_origin = os.environ.get("UPBIT_BASE_URL")
    sender = CountingGetOnlySender()
    try:
        os.environ["UPBIT_ACCESS_KEY"] = access_key
        os.environ["UPBIT_SECRET_KEY"] = secret_key
        os.environ["UPBIT_BASE_URL"] = OFFICIAL_ORIGIN
        account_fingerprint = upbit_credential_fingerprint(access_key)
        credential_binding = upbit_spot_credential_binding_sha256(
            access_key, secret_key
        )
        client = OfficialUpbitFunctionalGetClient(
            expected_account_fingerprint=account_fingerprint,
            sender=sender,
        )
        observed_at = datetime.now(timezone.utc)
        accounts = _account_map(client.get(UPBIT_ACCOUNTS_ENDPOINT, ()))
        chance = _chance(
            client.get(
                UPBIT_ORDER_CHANCE_ENDPOINT, (("market", SYMBOL),)
            ),
            accounts,
        )
        ticker = _rows(
            client.get(UPBIT_TICKER_ENDPOINT, (("markets", SYMBOL),)),
            "preflight-ticker",
        )
        if len(ticker) != 1 or _upper(ticker[0].get("market")) != SYMBOL:
            raise RuntimeError("upbit-get-only-preflight-ticker-scope-invalid")
        mark_price = _decimal(
            ticker[0].get("trade_price"),
            "preflight-ticker-trade-price",
            minimum=Decimal("0.00000001"),
        )
        open_orders = _read_all_open(client)
        closed_orders = _read_recent_closed(client, now=observed_at)
        account_rows = {
            row["currency"]: row for row in _account_rows(accounts)
        }
        summary = {
            "schemaVersion": "upbit-official-get-only-preflight/v1",
            "observedAt": _utc_text(datetime.now(timezone.utc)),
            "origin": OFFICIAL_ORIGIN,
            "market": SYMBOL,
            "accountFingerprint": account_fingerprint,
            "credentialBindingSha256": credential_binding,
            "accountCurrencyCount": len(account_rows),
            "balances": {
                currency: account_rows[currency]
                for currency in ("KRW", "BTC")
            },
            "orderRules": {
                "bidMinTotal": _decimal_text(chance["bidMinTotal"]),
                "askMinTotal": _decimal_text(chance["askMinTotal"]),
                "quantityStep": _decimal_text(QUANTITY_STEP),
                "quantityScale": QUANTITY_SCALE,
                "bidFeeRate": _decimal_text(chance["bidFeeRate"]),
                "askFeeRate": _decimal_text(chance["askFeeRate"]),
            },
            "markPrice": _decimal_text(mark_price),
            "openOrderCount": len(open_orders),
            "openOrderStateCounts": dict(
                sorted(Counter(row["state"] for row in open_orders).items())
            ),
            "recentClosedOrderCount": len(closed_orders),
            "recentClosedStateCounts": dict(
                sorted(Counter(row["state"] for row in closed_orders).items())
            ),
            "recentWindowSeconds": int(
                (timedelta(days=7) - timedelta(seconds=1)).total_seconds()
            ),
            "transport": {
                "physicalAttemptCount": sender.physical_attempt_count,
                "authenticatedGetCount": sender.authenticated_get_count,
                "retryCount": 0,
                "redirectCount": sender.redirect_count,
                "postCount": 0,
                "deleteCount": 0,
                "cancelCount": 0,
                "orderMutationCount": 0,
                "endpointCounts": dict(sorted(sender.endpoint_counts.items())),
            },
            "releaseFlagsChanged": False,
            "candidateCreated": False,
            "permitCreated": False,
        }
        return {**summary, "evidenceHash": _stable_hash(summary)}
    finally:
        access_key = ""
        secret_key = ""
        for name, value in (
            ("UPBIT_ACCESS_KEY", previous_access),
            ("UPBIT_SECRET_KEY", previous_secret),
            ("UPBIT_BASE_URL", previous_origin),
        ):
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def plan() -> dict[str, Any]:
    return {
        "schemaVersion": "upbit-official-get-only-preflight-plan/v1",
        "origin": OFFICIAL_ORIGIN,
        "methods": ["GET"],
        "maxPhysicalGetCount": MAX_OPEN_PAGES + 4,
        "retryCount": 0,
        "redirectFollowCount": 0,
        "mutationCount": 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", action="store_true")
    parser.add_argument("--api-keys-only", action="store_true")
    args = parser.parse_args()
    if args.plan and args.api_keys_only:
        parser.error("--plan and --api-keys-only are mutually exclusive")
    result = (
        plan()
        if args.plan
        else run_api_keys_only()
        if args.api_keys_only
        else run()
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
