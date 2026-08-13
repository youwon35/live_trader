from __future__ import annotations

from datetime import datetime, timezone
import copy
import os
import urllib.parse
import unittest
from unittest.mock import patch

from live_trader.binance_spot_continuous_functional import AccountTruth, ExactBinding, SymbolRules
from live_trader.binance_spot_functional_transport import (
    ALL_GET_ENDPOINTS,
    BINANCE_SPOT_ACCOUNT_ENDPOINT,
    BINANCE_SPOT_AVG_PRICE_ENDPOINT,
    BINANCE_SPOT_ALL_ORDERS_ENDPOINT,
    BINANCE_SPOT_EXCHANGE_INFO_ENDPOINT,
    BINANCE_SPOT_MY_TRADES_ENDPOINT,
    BINANCE_SPOT_OPEN_ORDERS_ENDPOINT,
    BINANCE_SPOT_QUERY_ORDER_ENDPOINT,
    BINANCE_SPOT_TICKER_PRICE_ENDPOINT,
    BinanceSpotFunctionalUserStreamTracker,
    BinanceSpotOfficialTruthReader,
    BinanceSpotTruthError,
    OfficialBinanceSpotGetClient,
    UserStreamProof,
    assemble_binance_spot_truth,
    assemble_binance_spot_rules,
    build_binance_spot_get_request,
    normalize_binance_user_stream_event,
)


NOW = 1_800_000_000.0
BASELINE = NOW - 600
FINGERPRINT = "c" * 64
OWNER_PREFIX = "ftb-0123456789ab-"


def iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )


def binding_payload() -> dict[str, object]:
    return {
        "strategyArtifactId": "crypto-binance-btc-functional-v1",
        "strategyArtifactHash": "a" * 64,
        "artifactFileSha256": "1" * 64,
        "strategyInstanceId": "crypto-binance-btc-functional-instance-v1",
        "strategyInstanceHash": "b" * 64,
        "instanceFileSha256": "2" * 64,
        "publicationProofHash": "3" * 64,
        "publicationProofFileSha256": "4" * 64,
        "accountFingerprint": FINGERPRINT,
        "broker": "BINANCE",
        "venue": "BINANCE_SPOT",
        "asset": "CRYPTO",
        "market": "CRYPTO_SPOT",
        "executionRoute": "BINANCE_SPOT_CONTINUOUS",
        "symbol": "BTCUSDT",
        "baseAsset": "BTC",
        "quoteAsset": "USDT",
        "interval": "5m",
    }


def account_payload() -> dict[str, object]:
    return {
        "canTrade": True,
        "accountType": "SPOT",
        "permissions": ["SPOT"],
        "balances": [
            {"asset": "BTC", "free": "0.00115", "locked": "0"},
            {"asset": "USDT", "free": "91", "locked": "0"},
            {"asset": "BNB", "free": "0", "locked": "0"},
        ],
    }


def order_payload(**updates: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "symbol": "BTCUSDT",
        "orderId": 101,
        "clientOrderId": OWNER_PREFIX + "b",
        "price": "0",
        "origQty": "0.00015",
        "executedQty": "0.00015",
        "origQuoteOrderQty": "10",
        "cummulativeQuoteQty": "9",
        "status": "FILLED",
        "timeInForce": "GTC",
        "type": "MARKET",
        "side": "BUY",
        "time": int((BASELINE + 60) * 1000),
        "updateTime": int((BASELINE + 61) * 1000),
    }
    payload.update(updates)
    return payload


def trade_payload(**updates: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "symbol": "BTCUSDT",
        "id": 501,
        "orderId": 101,
        "price": "60000",
        "qty": "0.00015",
        "quoteQty": "9",
        "commission": "0.01",
        "commissionAsset": "USDT",
        "time": int((BASELINE + 61) * 1000),
        "isBuyer": True,
        "isMaker": False,
    }
    payload.update(updates)
    return payload


def exchange_info_payload() -> dict[str, object]:
    return {
        "timezone": "UTC",
        "serverTime": int(NOW * 1000),
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "status": "TRADING",
                "baseAsset": "BTC",
                "quoteAsset": "USDT",
                "isSpotTradingAllowed": True,
                "quoteOrderQtyMarketAllowed": True,
                "permissions": [],
                "permissionSets": [["SPOT", "MARGIN"]],
                "filters": [
                    {
                        "filterType": "LOT_SIZE",
                        "minQty": "0.00001",
                        "maxQty": "100",
                        "stepSize": "0.00001",
                    },
                    {
                        "filterType": "MARKET_LOT_SIZE",
                        "minQty": "0",
                        "maxQty": "10",
                        "stepSize": "0",
                    },
                    {
                        "filterType": "NOTIONAL",
                        "minNotional": "5",
                        "maxNotional": "1000000",
                        "applyMinToMarket": True,
                        "applyMaxToMarket": False,
                        "avgPriceMins": 0,
                    },
                ],
            }
        ],
    }


def raw_execution_event(**updates: object) -> dict[str, object]:
    event: dict[str, object] = {
        "e": "executionReport",
        "E": int((BASELINE + 61) * 1000),
        "s": "BTCUSDT",
        "c": OWNER_PREFIX + "b",
        "C": "",
        "S": "BUY",
        "o": "MARKET",
        "x": "TRADE",
        "X": "FILLED",
        "i": 101,
        "t": 501,
        "l": "0.00015",
        "L": "60000",
        "n": "0.01",
        "N": "USDT",
        "z": "0.00015",
        "Z": "9",
    }
    event.update(updates)
    return {"subscriptionId": 7, "event": event}


def raw_account_event() -> dict[str, object]:
    return {
        "subscriptionId": 7,
        "event": {
            "e": "outboundAccountPosition",
            "E": int((BASELINE + 62) * 1000),
            "u": int((BASELINE + 62) * 1000),
            "B": [
                {"a": "BTC", "f": "0.00115", "l": "0"},
                {"a": "USDT", "f": "91", "l": "0"},
            ],
        },
    }


def stream_snapshot(*, events: list[dict[str, object]] | None = None, **updates: object) -> dict[str, object]:
    normalized_events = events
    if normalized_events is None:
        normalized_events = [
            normalize_binance_user_stream_event(raw_execution_event()),
            normalize_binance_user_stream_event(raw_account_event()),
        ]
    payload: dict[str, object] = {
        "connected": True,
        "authenticated": True,
        "sequenceComplete": True,
        "gapDetected": False,
        "subscribedAt": iso(BASELINE - 10),
        "observedAt": iso(NOW),
        "externalActivityAbsent": True,
        "events": normalized_events,
    }
    payload.update(updates)
    return payload


class FakeClient:
    def __init__(self) -> None:
        self.expected_account_fingerprint = FINGERPRINT
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.payloads: dict[str, object] = {
            BINANCE_SPOT_ACCOUNT_ENDPOINT: account_payload(),
            BINANCE_SPOT_OPEN_ORDERS_ENDPOINT: [],
            BINANCE_SPOT_ALL_ORDERS_ENDPOINT: [order_payload()],
            BINANCE_SPOT_MY_TRADES_ENDPOINT: [trade_payload()],
            BINANCE_SPOT_EXCHANGE_INFO_ENDPOINT: exchange_info_payload(),
            BINANCE_SPOT_TICKER_PRICE_ENDPOINT: {
                "symbol": "BTCUSDT",
                "price": "60000",
            },
            BINANCE_SPOT_AVG_PRICE_ENDPOINT: {
                "mins": 5,
                "price": "59950",
                "closeTime": int(NOW * 1000),
            },
        }

    def get(self, endpoint: str, query: dict[str, object]) -> object:
        self.calls.append((endpoint, dict(query)))
        return self.payloads[endpoint]


class BinanceSpotFunctionalTransportTest(unittest.TestCase):
    def test_request_allowlist_is_get_only_and_preview_redacts_signature(self) -> None:
        with patch.dict(
            os.environ,
            {"BINANCE_API_KEY": "api-key", "BINANCE_API_SECRET": "secret"},
            clear=False,
        ), patch(
            "live_trader.binance_spot_functional_transport.binance_timestamp_ms",
            return_value=1_800_000_000_000,
        ):
            request = build_binance_spot_get_request(
                BINANCE_SPOT_ALL_ORDERS_ENDPOINT,
                {"symbol": "BTCUSDT", "limit": 1000},
            )
        self.assertEqual("GET", request.method)
        self.assertIn("signature=", request.url)
        preview = request.preview()
        self.assertNotIn("secret", str(preview))
        self.assertIn("signature=%2A%2A%2A", preview["url"])
        with self.assertRaises(BinanceSpotTruthError):
            build_binance_spot_get_request("/sapi/v1/capital/withdraw/apply")
        with self.assertRaises(BinanceSpotTruthError):
            build_binance_spot_get_request("/fapi/v1/order")

    def test_signed_get_serializes_boolean_query_values_lowercase(self) -> None:
        with patch.dict(
            os.environ,
            {"BINANCE_API_KEY": "api-key", "BINANCE_API_SECRET": "secret"},
            clear=False,
        ), patch(
            "live_trader.binance_spot_functional_transport.binance_timestamp_ms",
            return_value=1_800_000_000_000,
        ):
            request = build_binance_spot_get_request(
                BINANCE_SPOT_ACCOUNT_ENDPOINT,
                {"omitZeroBalances": False},
            )
        parsed = urllib.parse.urlsplit(request.url)
        query = dict(urllib.parse.parse_qsl(parsed.query))
        self.assertEqual("false", query["omitZeroBalances"])
        self.assertIn("omitZeroBalances=false", parsed.query)
        self.assertNotIn("omitZeroBalances=False", parsed.query)
        self.assertRegex(query["signature"], r"^[0-9a-f]{64}$")
        self.assertEqual("false", request.query["omitZeroBalances"])

    def test_production_get_origin_rejects_http_testnet_and_other_hosts(self) -> None:
        for origin in (
            "http://api.binance.com",
            "https://testnet.binance.vision",
            "https://api.binance.com.evil.example",
            "https://api.binance.com:8443",
        ):
            with self.subTest(origin=origin), patch.dict(
                os.environ,
                {
                    "BINANCE_BASE_URL": origin,
                    "BINANCE_API_KEY": "must-not-be-sent",
                    "BINANCE_API_SECRET": "must-not-be-signed",
                },
                clear=False,
            ), self.assertRaisesRegex(BinanceSpotTruthError, "exact https"):
                build_binance_spot_get_request(BINANCE_SPOT_ACCOUNT_ENDPOINT)

        with patch.dict(
            os.environ, {"BINANCE_BASE_URL": "http://mock.local"}, clear=False
        ):
            mock = build_binance_spot_get_request(
                BINANCE_SPOT_TICKER_PRICE_ENDPOINT,
                {"symbol": "BTCUSDT"},
                allow_mock_origin=True,
            )
        self.assertTrue(mock.url.startswith("http://mock.local/"))

    def test_official_client_has_no_post_or_cancel_surface(self) -> None:
        client = OfficialBinanceSpotGetClient(sender=lambda _: {"ok": True, "json": {}})
        self.assertFalse(hasattr(client, "post"))
        self.assertFalse(hasattr(client, "cancel"))
        self.assertFalse(hasattr(client, "withdraw"))

    def test_exact_signed_order_query_only_accepts_unknown_order_code(self) -> None:
        fingerprint = __import__(
            "live_trader.binance_spot_functional_transport",
            fromlist=["binance_api_key_fingerprint"],
        ).binance_api_key_fingerprint("api-key")
        responses = [
            {"ok": False, "json": {"code": -2013, "msg": "Order does not exist."}}
        ]
        client = OfficialBinanceSpotGetClient(
            sender=lambda request: responses.pop(0),
            expected_account_fingerprint=fingerprint,
            clock=lambda: NOW,
        )
        with patch.dict(
            os.environ,
            {"BINANCE_API_KEY": "api-key", "BINANCE_API_SECRET": "secret"},
            clear=False,
        ):
            proof = client.query_order_absence(
                client_order_id=OWNER_PREFIX + "b"
            )
        self.assertTrue(proof["notFound"])
        self.assertEqual(-2013, proof["errorCode"])
        self.assertIn(BINANCE_SPOT_QUERY_ORDER_ENDPOINT, ALL_GET_ENDPOINTS)

        client = OfficialBinanceSpotGetClient(
            sender=lambda request: {
                "ok": False,
                "json": {"code": -1000, "msg": "Internal error"},
            },
            expected_account_fingerprint=fingerprint,
            clock=lambda: NOW,
        )
        with patch.dict(
            os.environ,
            {"BINANCE_API_KEY": "api-key", "BINANCE_API_SECRET": "secret"},
            clear=False,
        ), self.assertRaises(BinanceSpotTruthError):
            client.query_order_absence(client_order_id=OWNER_PREFIX + "b")

    def test_official_response_set_builds_strict_core_truth_and_rules(self) -> None:
        fake = FakeClient()
        reader = BinanceSpotOfficialTruthReader(
            client=fake,  # type: ignore[arg-type]
            account_fingerprint=FINGERPRINT,
            stream_reader=lambda: stream_snapshot(),
            clock=lambda: NOW,
        )
        truth, rule_truth = reader.read(
            baseline_epoch=BASELINE,
            owner_prefix=OWNER_PREFIX,
        )
        parsed = AccountTruth.parse(
            truth,
            binding=ExactBinding.parse(binding_payload()),
            now_epoch=NOW,
        )
        parsed_rules = SymbolRules.parse(rule_truth)
        self.assertEqual("0.00115", str(parsed.base_total))
        self.assertEqual("0.01", truth["fills"][0]["feeQuoteValue"])
        self.assertTrue(truth["restUserStreamCrossChecked"])
        self.assertEqual("0.00001", str(parsed_rules.step_size))
        endpoints = [endpoint for endpoint, _ in fake.calls]
        self.assertEqual(
            [
                BINANCE_SPOT_ACCOUNT_ENDPOINT,
                BINANCE_SPOT_OPEN_ORDERS_ENDPOINT,
                BINANCE_SPOT_ALL_ORDERS_ENDPOINT,
                BINANCE_SPOT_MY_TRADES_ENDPOINT,
                BINANCE_SPOT_EXCHANGE_INFO_ENDPOINT,
                BINANCE_SPOT_TICKER_PRICE_ENDPOINT,
            ],
            endpoints,
        )
        self.assertNotIn("symbol", fake.calls[1][1])

    def test_realistic_notional_filter_uses_official_average_price_horizon(self) -> None:
        fake = FakeClient()
        exchange = exchange_info_payload()
        notional = exchange["symbols"][0]["filters"][2]  # type: ignore[index]
        notional["avgPriceMins"] = 5  # type: ignore[index]
        fake.payloads[BINANCE_SPOT_EXCHANGE_INFO_ENDPOINT] = exchange
        reader = BinanceSpotOfficialTruthReader(
            client=fake,  # type: ignore[arg-type]
            account_fingerprint=FINGERPRINT,
            stream_reader=lambda: stream_snapshot(),
            clock=lambda: NOW,
        )
        _, rule_truth = reader.read(
            baseline_epoch=BASELINE,
            owner_prefix=OWNER_PREFIX,
        )
        parsed = SymbolRules.parse(rule_truth)
        self.assertEqual(5, parsed.avg_price_mins)
        self.assertEqual("BINANCE_AVG_PRICE", parsed.market_reference_source)
        self.assertEqual("59950", str(parsed.market_reference_price))
        self.assertEqual("LOT_SIZE", parsed.quantity_filter_type)
        self.assertIn(
            BINANCE_SPOT_AVG_PRICE_ENDPOINT,
            [endpoint for endpoint, _ in fake.calls],
        )

    def test_current_trading_group_permission_set_is_authorized(self) -> None:
        account = account_payload()
        account["permissions"] = ["TRD_GRP_057"]
        exchange = exchange_info_payload()
        symbol = exchange["symbols"][0]  # type: ignore[index]
        symbol["permissions"] = []  # type: ignore[index]
        symbol["permissionSets"] = [  # type: ignore[index]
            ["SPOT", "MARGIN", "TRD_GRP_057"]
        ]
        proof = assemble_binance_spot_rules(exchange, account=account)
        self.assertTrue(proof["symbolPermissionsAuthorized"])
        self.assertEqual("SPOT", proof["accountType"])
        self.assertEqual(["TRD_GRP_057"], proof["accountPermissions"])
        self.assertEqual("AND_OF_OR_SETS", proof["permissionSemantics"])

    def test_permission_sets_require_one_account_match_in_every_inner_set(self) -> None:
        account = account_payload()
        account["permissions"] = ["SPOT", "TRD_GRP_057"]
        exchange = exchange_info_payload()
        symbol = exchange["symbols"][0]  # type: ignore[index]
        symbol["permissionSets"] = [  # type: ignore[index]
            ["SPOT", "MARGIN"],
            ["TRD_GRP_056", "TRD_GRP_057"],
        ]
        proof = assemble_binance_spot_rules(exchange, account=account)
        self.assertEqual(2, len(proof["symbolPermissionSets"]))

        missing_second = copy.deepcopy(account)
        missing_second["permissions"] = ["SPOT"]
        with self.assertRaisesRegex(
            BinanceSpotTruthError, "every symbol permission set"
        ):
            assemble_binance_spot_rules(exchange, account=missing_second)

    def test_permission_sets_and_account_identity_are_strictly_malformed_closed(self) -> None:
        cases: list[tuple[dict[str, object], dict[str, object]]] = []
        for malformed in ([], [[]], ["SPOT"], [[""]], [["SPOT", "SPOT"]]):
            exchange = exchange_info_payload()
            exchange["symbols"][0]["permissionSets"] = malformed  # type: ignore[index]
            cases.append((account_payload(), exchange))
        wrong_type = account_payload()
        wrong_type["accountType"] = "MARGIN"
        cases.append((wrong_type, exchange_info_payload()))
        empty_permissions = account_payload()
        empty_permissions["permissions"] = []
        cases.append((empty_permissions, exchange_info_payload()))

        for account, exchange in cases:
            with self.subTest(account=account, exchange=exchange), self.assertRaises(
                BinanceSpotTruthError
            ):
                assemble_binance_spot_rules(exchange, account=account)

    def test_missing_market_applicability_flag_fails_closed(self) -> None:
        payload = exchange_info_payload()
        notional = payload["symbols"][0]["filters"][2]  # type: ignore[index]
        del notional["applyMinToMarket"]  # type: ignore[index]
        fake = FakeClient()
        fake.payloads[BINANCE_SPOT_EXCHANGE_INFO_ENDPOINT] = payload
        reader = BinanceSpotOfficialTruthReader(
            client=fake,  # type: ignore[arg-type]
            account_fingerprint=FINGERPRINT,
            stream_reader=lambda: stream_snapshot(),
            clock=lambda: NOW,
        )
        with self.assertRaisesRegex(BinanceSpotTruthError, "applicability"):
            reader.read(baseline_epoch=BASELINE, owner_prefix=OWNER_PREFIX)

    def test_stream_gap_stale_or_missing_owned_event_blocks_truth(self) -> None:
        for update in (
            {"gapDetected": True},
            {"sequenceComplete": False},
            {"observedAt": iso(NOW - 16)},
            {"events": []},
        ):
            with self.subTest(update=update):
                fake = FakeClient()
                reader = BinanceSpotOfficialTruthReader(
                    client=fake,  # type: ignore[arg-type]
                    account_fingerprint=FINGERPRINT,
                    stream_reader=lambda update=update: stream_snapshot(**update),
                    clock=lambda: NOW,
                )
                with self.assertRaises(BinanceSpotTruthError):
                    reader.read(
                        baseline_epoch=BASELINE,
                        owner_prefix=OWNER_PREFIX,
                    )

    def test_sticky_stream_gap_has_separate_rest_cleanup_only_truth(self) -> None:
        session_id = "bnsft-rest-recovery-000000000001"
        permit_id = "functional-test-binance-rest-recovery-0001"
        permit_hash = "9" * 64
        gap = stream_snapshot(
            connected=False,
            authenticated=False,
            sequenceComplete=False,
            gapDetected=True,
            externalActivityAbsent=False,
            sessionId=session_id,
            permitId=permit_id,
            permitHash=permit_hash,
            durableJournal=True,
            durableJournalEventCount=2,
            durableJournalSealHash="8" * 64,
        )
        reader = BinanceSpotOfficialTruthReader(
            client=FakeClient(),  # type: ignore[arg-type]
            account_fingerprint=FINGERPRINT,
            stream_reader=lambda: gap,
            clock=lambda: NOW,
        )
        with self.assertRaises(BinanceSpotTruthError):
            reader.read(baseline_epoch=BASELINE, owner_prefix=OWNER_PREFIX)
        recovered, _rules = reader.read_cleanup_recovery(
            baseline_epoch=BASELINE,
            owner_prefix=OWNER_PREFIX,
        )
        parsed = AccountTruth.parse(
            recovered,
            binding=ExactBinding.parse(binding_payload()),
            now_epoch=NOW,
        )
        self.assertTrue(parsed.cleanup_recovery_only)
        self.assertFalse(parsed.external_activity_absent)
        self.assertFalse(recovered["restUserStreamCrossChecked"])
        self.assertEqual("REST_RECONCILED_CLEANUP_ONLY", recovered["cleanupRecoveryMode"])
        self.assertEqual(session_id, parsed.stream_session_id)
        self.assertEqual(64, len(parsed.stream_gap_evidence_hash))
        self.assertEqual(64, len(parsed.recovery_attestation_hash))

    def test_rest_stream_fee_or_status_mismatch_blocks(self) -> None:
        mismatches = (
            raw_execution_event(N="BTC"),
            raw_execution_event(n="0.02"),
            raw_execution_event(X="PARTIALLY_FILLED"),
        )
        for event in mismatches:
            with self.subTest(event=event):
                fake = FakeClient()
                events = [
                    normalize_binance_user_stream_event(event),
                    normalize_binance_user_stream_event(raw_account_event()),
                ]
                reader = BinanceSpotOfficialTruthReader(
                    client=fake,  # type: ignore[arg-type]
                    account_fingerprint=FINGERPRINT,
                    stream_reader=lambda events=events: stream_snapshot(events=events),
                    clock=lambda: NOW,
                )
                with self.assertRaises(BinanceSpotTruthError):
                    reader.read(
                        baseline_epoch=BASELINE,
                        owner_prefix=OWNER_PREFIX,
                    )

    def test_third_asset_fee_remains_cleanup_readable_but_not_exact_valued(self) -> None:
        fake = FakeClient()
        fake.payloads[BINANCE_SPOT_MY_TRADES_ENDPOINT] = [
            trade_payload(commission="0.00001", commissionAsset="BNB")
        ]
        events = [
            normalize_binance_user_stream_event(
                raw_execution_event(n="0.00001", N="BNB")
            ),
            normalize_binance_user_stream_event(raw_account_event()),
        ]
        reader = BinanceSpotOfficialTruthReader(
            client=fake,  # type: ignore[arg-type]
            account_fingerprint=FINGERPRINT,
            stream_reader=lambda: stream_snapshot(events=events),
            clock=lambda: NOW,
        )
        truth_value, _ = reader.read(
            baseline_epoch=BASELINE, owner_prefix=OWNER_PREFIX
        )
        self.assertFalse(truth_value["feeQuoteValuationComplete"])
        self.assertFalse(truth_value["fills"][0]["feeQuoteValueExact"])
        self.assertEqual("BNB", truth_value["fills"][0]["commissionAsset"])

    def test_full_history_page_requires_forward_pagination(self) -> None:
        fake = FakeClient()
        fake.payloads[BINANCE_SPOT_ALL_ORDERS_ENDPOINT] = [
            order_payload(orderId=index + 1, clientOrderId=f"external-{index:08d}")
            for index in range(1000)
        ]
        reader = BinanceSpotOfficialTruthReader(
            client=fake,  # type: ignore[arg-type]
            account_fingerprint=FINGERPRINT,
            stream_reader=lambda: stream_snapshot(),
            clock=lambda: NOW,
        )
        with self.assertRaisesRegex(BinanceSpotTruthError, "pagination repeated"):
            reader.read(baseline_epoch=BASELINE, owner_prefix=OWNER_PREFIX)

    def test_full_history_pages_continue_with_exact_inclusive_cursors(self) -> None:
        class PagedClient(FakeClient):
            def get(self, endpoint: str, query: dict[str, object]) -> object:
                self.calls.append((endpoint, dict(query)))
                if endpoint == BINANCE_SPOT_ALL_ORDERS_ENDPOINT:
                    start = int(query.get("orderId") or 1)
                    stop = 1001 if start == 1 else 1002
                    return [
                        order_payload(
                            orderId=index,
                            clientOrderId=f"external-{index:08d}",
                            status="CANCELED",
                            executedQty="0",
                            cummulativeQuoteQty="0",
                        )
                        for index in range(start, stop)
                    ]
                if endpoint == BINANCE_SPOT_MY_TRADES_ENDPOINT:
                    return []
                return self.payloads[endpoint]

        fake = PagedClient()
        reader = BinanceSpotOfficialTruthReader(
            client=fake,  # type: ignore[arg-type]
            account_fingerprint=FINGERPRINT,
            stream_reader=lambda: stream_snapshot(events=[]),
            clock=lambda: NOW,
        )
        truth, _ = reader.read(baseline_epoch=BASELINE, owner_prefix=OWNER_PREFIX)
        self.assertEqual(1001, len(truth["closedOrders"]))
        order_queries = [
            query
            for endpoint, query in fake.calls
            if endpoint == BINANCE_SPOT_ALL_ORDERS_ENDPOINT
        ]
        self.assertEqual(2, len(order_queries))
        self.assertEqual(1001, order_queries[1]["orderId"])
        self.assertNotIn("startTime", order_queries[1])

    def test_user_stream_tracker_fails_closed_after_disconnect_or_external_event(self) -> None:
        tracker = BinanceSpotFunctionalUserStreamTracker(clock=lambda: NOW)
        tracker.begin_authenticated_subscription(subscribed_epoch=BASELINE - 10)
        tracker.ingest(raw_execution_event(), owner_prefix=OWNER_PREFIX)
        tracker.ingest(raw_account_event(), owner_prefix=OWNER_PREFIX)
        parsed = UserStreamProof.parse(
            tracker.snapshot(), now_epoch=NOW, baseline_epoch=BASELINE
        )
        self.assertTrue(parsed.external_activity_absent)

        tracker.ingest(
            raw_execution_event(c="operator-external", i=909, t=9909),
            owner_prefix=OWNER_PREFIX,
        )
        self.assertFalse(tracker.snapshot()["externalActivityAbsent"])
        tracker.mark_disconnected()
        with self.assertRaisesRegex(BinanceSpotTruthError, "connected"):
            UserStreamProof.parse(
                tracker.snapshot(), now_epoch=NOW, baseline_epoch=BASELINE
            )

    def test_nonowned_activity_is_reported_and_never_canceled(self) -> None:
        stream = UserStreamProof.parse(
            stream_snapshot(), now_epoch=NOW, baseline_epoch=BASELINE
        )
        external = order_payload(
            orderId=999,
            clientOrderId="operator-external-0001",
            status="NEW",
            executedQty="0",
            cummulativeQuoteQty="0",
        )
        account = account_payload()
        permission = assemble_binance_spot_rules(
            exchange_info_payload(), account=account
        )
        truth = assemble_binance_spot_truth(
            account=account,
            permission_proof=permission,
            open_orders=[external],
            all_orders=[order_payload()],
            trades=[trade_payload()],
            ticker={"symbol": "BTCUSDT", "price": "60000"},
            stream=stream,
            account_fingerprint=FINGERPRINT,
            owner_prefix=OWNER_PREFIX,
            baseline_epoch=BASELINE,
            observed_epoch=NOW,
        )
        self.assertFalse(truth["externalActivityAbsent"])
        self.assertEqual("operator-external-0001", truth["openOrders"][0]["clientOrderId"])


if __name__ == "__main__":
    unittest.main()
