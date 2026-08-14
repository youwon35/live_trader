from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import unittest

from live_trader.upbit_continuous_functional import (
    UpbitFunctionalBlocked,
    UpbitTruth,
)
from live_trader.upbit_functional_truth import (
    CLOSED_LIMIT,
    OfficialUpbitFunctionalTruthReader,
    UPBIT_ACCOUNTS_ENDPOINT,
    UPBIT_CLOSED_ORDERS_ENDPOINT,
    UPBIT_OPEN_ORDERS_ENDPOINT,
    UPBIT_ORDER_CHANCE_ENDPOINT,
    UPBIT_ORDER_DETAIL_ENDPOINT,
    UPBIT_TICKER_ENDPOINT,
)


NOW = datetime(2026, 8, 13, 2, 0, tzinfo=timezone.utc)
ACCOUNT = "c" * 64


def order(index: int, *, identifier: str | None = None, state: str = "done"):
    return {
        "market": "KRW-BTC",
        "uuid": f"order-uuid-{index:04d}",
        "identifier": identifier or f"functional-identifier-{index:04d}",
        "side": "bid",
        "state": state,
        "trades_count": 0,
        "trades": [],
        "paid_fee": "0",
        "executed_volume": "0",
        "executed_funds": "0",
        "remaining_volume": "0",
    }


class FakeReadClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[tuple[str, str], ...]]] = []
        self.accounts = [
            {"currency": "KRW", "balance": "50000", "locked": "0"},
            {"currency": "BTC", "balance": "0.01", "locked": "0"},
        ]
        self.chance = {
            "bid_fee": "0.0005",
            "ask_fee": "0.0005",
            "market": {
                "id": "KRW-BTC",
                "bid": {"min_total": "5000"},
                "ask": {"min_total": "5000"},
                "bid_types": ["limit", "price"],
                "ask_types": ["limit", "market"],
            },
            "bid_account": {"currency": "KRW", "balance": "50000", "locked": "0"},
            "ask_account": {"currency": "BTC", "balance": "0.01", "locked": "0"},
        }
        self.ticker = [{"market": "KRW-BTC", "trade_price": "100000000"}]
        self.open_pages: dict[int, list[dict]] = {1: []}
        self.closed: list[dict] = []
        self.details: dict[str, object] = {}

    def get(self, endpoint: str, query):
        query = tuple(query)
        self.calls.append((endpoint, query))
        if endpoint == UPBIT_ACCOUNTS_ENDPOINT:
            return self.accounts
        if endpoint == UPBIT_ORDER_CHANCE_ENDPOINT:
            return self.chance
        if endpoint == UPBIT_TICKER_ENDPOINT:
            return self.ticker
        if endpoint == UPBIT_OPEN_ORDERS_ENDPOINT:
            page = int(dict(query)["page"])
            return self.open_pages.get(page, [])
        if endpoint == UPBIT_CLOSED_ORDERS_ENDPOINT:
            return self.closed
        if endpoint == UPBIT_ORDER_DETAIL_ENDPOINT:
            params = dict(query)
            if "identifier" in params:
                return self.details.get(params["identifier"], {"_notFound": True})
            uuid = params["uuid"]
            return next(
                (
                    dict(row)
                    for row in [
                        *sum(self.open_pages.values(), []),
                        *self.closed,
                    ]
                    if row.get("uuid") == uuid
                ),
                {"_notFound": True},
            )
        raise AssertionError(f"unexpected endpoint: {endpoint}")


class OfficialUpbitFunctionalTruthReaderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.client = FakeReadClient()
        self.reader = OfficialUpbitFunctionalTruthReader(
            client=self.client,
            account_fingerprint=ACCOUNT,
            session_started_at=NOW - timedelta(hours=1),
            cleanup_deadline=NOW + timedelta(hours=1),
            clock=lambda: NOW,
            private_stream_reader=self.private_stream,
        )

    def private_stream(self, *, session_id: str, identifiers: tuple[str, ...]):
        events = []
        for row in self.client.closed:
            for trade in row.get("trades", []):
                events.append(
                    {
                        "eventId": trade["uuid"],
                        "orderUuid": row["uuid"],
                        "tradeUuid": trade["uuid"],
                        "identifier": row["identifier"],
                        "market": row["market"],
                    }
                )
        return {
            "connected": True,
            "authenticated": True,
            "eventsComplete": True,
            "gapDetected": False,
            "externalActivityAbsent": True,
            "channel": "myOrder",
            "accountFingerprint": ACCOUNT,
            "startedAt": (NOW - timedelta(hours=2)).isoformat().replace("+00:00", "Z"),
            "observedAt": NOW.isoformat().replace("+00:00", "Z"),
            "writerGeneration": 1,
            "journalRevision": 1,
            "eventCursor": len(events),
            "lastEventId": events[-1]["eventId"] if events else "",
            "eventHeadHash": "b" * 64,
            "events": events,
        }

    def read(self, identifiers=()):
        return self.reader(
            session_id="upbit-functional-session-0001",
            phase="PREFLIGHT",
            identifiers=tuple(identifiers),
        )

    def test_empty_account_truth_is_complete_fresh_official_and_parseable(self) -> None:
        result = self.read()
        parsed = UpbitTruth.parse(result, account_fingerprint=ACCOUNT, now=NOW)
        self.assertEqual(Decimal("50000"), parsed.quote_available)
        self.assertEqual(Decimal("0.01"), parsed.base_total)
        self.assertEqual(Decimal("0.00000001"), parsed.rules.quantity_step)
        self.assertEqual({}, parsed.identifier_truth)
        endpoints = [endpoint for endpoint, _query in self.client.calls]
        self.assertEqual(
            [
                UPBIT_ACCOUNTS_ENDPOINT,
                UPBIT_ORDER_CHANCE_ENDPOINT,
                UPBIT_TICKER_ENDPOINT,
                UPBIT_OPEN_ORDERS_ENDPOINT,
                UPBIT_CLOSED_ORDERS_ENDPOINT,
            ],
            endpoints,
        )

    def test_detached_exclusivity_proof_is_bound_to_exact_truth_interval(
        self,
    ) -> None:
        calls = []

        def proof_reader(**kwargs):
            calls.append(dict(kwargs))
            return {
                "schemaVersion": "upbit-functional-account-exclusivity-proof/v1",
                "sessionId": kwargs["session_id"],
                "payloadHash": "a" * 64,
            }

        self.reader.account_exclusivity_proof_reader = proof_reader
        result = self.read()
        self.assertEqual(1, len(calls))
        call = calls[0]
        self.assertEqual("upbit-functional-session-0001", call["session_id"])
        self.assertEqual(ACCOUNT, call["account_fingerprint"])
        self.assertEqual(NOW - timedelta(hours=1), call["session_started_at"])
        self.assertEqual(NOW, call["observation_started_at"])
        self.assertEqual(NOW, call["observed_at"])
        self.assertEqual(
            "a" * 64,
            result["accountExclusivityProof"]["payloadHash"],
        )

    def test_non_object_exclusivity_proof_fails_closed(self) -> None:
        self.reader.account_exclusivity_proof_reader = (
            lambda **_kwargs: "not-a-proof"
        )
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "proof-not-object"
        ):
            self.read()

    def test_open_order_query_is_account_wide_both_states_and_fully_paginated(self) -> None:
        self.client.open_pages[1] = [order(index, state="wait") for index in range(100)]
        for row in self.client.open_pages[1]:
            row["market"] = "KRW-ETH"
        self.client.open_pages[2] = []
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "external-account-order-activity"
        ):
            self.read()
        open_calls = [query for endpoint, query in self.client.calls if endpoint == UPBIT_OPEN_ORDERS_ENDPOINT]
        self.assertEqual(2, len(open_calls))
        first = list(open_calls[0])
        self.assertNotIn(("market", "KRW-BTC"), first)
        self.assertIn(("states[]", "wait"), first)
        self.assertIn(("states[]", "watch"), first)

    def test_closed_order_limit_boundary_fails_closed_as_possible_truncation(self) -> None:
        self.client.closed = [{} for _ in range(CLOSED_LIMIT)]
        with self.assertRaisesRegex(UpbitFunctionalBlocked, "truncation-possible"):
            self.read()

    def test_identifier_lookup_absence_and_exact_filled_fee_truth(self) -> None:
        identifier = "functional-identifier-owned-0001"
        filled = order(1, identifier=identifier)
        filled.update(
            {
                "trades_count": 1,
                "paid_fee": "5",
                "executed_volume": "0.0001",
                "executed_funds": "10000",
                "remaining_volume": "0",
                "trades": [
                    {
                        "uuid": "trade-uuid-0001",
                        "price": "100000000",
                        "volume": "0.0001",
                        "funds": "10000",
                    }
                ],
            }
        )
        self.client.closed = [filled]
        self.client.details[identifier] = dict(filled)
        result = self.read((identifier, "functional-unused-identifier"))
        parsed = UpbitTruth.parse(result, account_fingerprint=ACCOUNT, now=NOW)
        self.assertEqual(filled["uuid"], parsed.identifier_truth[identifier]["uuid"])
        self.assertIsNone(parsed.identifier_truth["functional-unused-identifier"])
        self.assertEqual(Decimal("5"), parsed.total_fees)
        self.assertEqual("5", parsed.fills[0]["fee"])
        detail_queries = [
            dict(query)
            for endpoint, query in self.client.calls
            if endpoint == UPBIT_ORDER_DETAIL_ENDPOINT
        ]
        self.assertIn({"uuid": filled["uuid"]}, detail_queries)
        self.assertIn({"identifier": identifier}, detail_queries)

    def test_external_order_without_identifier_is_parsed_then_blocks_lane(self) -> None:
        filled = order(1)
        filled.update(
            {
                "identifier": None,
                "trades_count": 1,
                "paid_fee": "5",
                "executed_volume": "0.0001",
                "executed_funds": "10000",
                "remaining_volume": "0",
                "trades": [
                    {
                        "uuid": "trade-uuid-external-0001",
                        "price": "100000000",
                        "volume": "0.0001",
                        "funds": "10000",
                    }
                ],
            }
        )
        self.client.closed = [filled]
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked,
            "external-account-order-activity",
        ):
            self.read()

    def test_detail_not_in_complete_lists_and_balance_chance_mismatch_fail(self) -> None:
        identifier = "functional-identifier-owned-0001"
        self.client.details[identifier] = order(1, identifier=identifier)
        with self.assertRaisesRegex(UpbitFunctionalBlocked, "detail-list-mismatch"):
            self.read((identifier,))

        self.client.details = {}
        self.client.chance["bid_account"]["balance"] = "49999"
        with self.assertRaisesRegex(UpbitFunctionalBlocked, "account-chance-mismatch"):
            self.read()

    def test_open_closed_identifier_collision_fails_before_indexing(self) -> None:
        identifier = "functional-colliding-identifier-0001"
        self.client.open_pages[1] = [
            order(1, identifier=identifier, state="wait")
        ]
        self.client.closed = [order(2, identifier=identifier)]
        with self.assertRaisesRegex(UpbitFunctionalBlocked, "open-closed-order-overlap"):
            self.read()

    def test_private_stream_must_contain_every_owned_fill(self) -> None:
        identifier = "functional-identifier-owned-0001"
        filled = order(1, identifier=identifier)
        filled.update(
            {
                "trades_count": 1,
                "paid_fee": "5",
                "executed_volume": "0.0001",
                "executed_funds": "10000",
                "remaining_volume": "0",
                "trades": [
                    {
                        "uuid": "trade-uuid-0001",
                        "price": "100000000",
                        "volume": "0.0001",
                        "funds": "10000",
                    }
                ],
            }
        )
        self.client.closed = [filled]
        self.client.details[identifier] = dict(filled)
        self.reader.private_stream_reader = lambda **_kwargs: {
            "connected": True,
            "authenticated": True,
            "eventsComplete": True,
            "gapDetected": False,
            "externalActivityAbsent": True,
            "channel": "myOrder",
            "accountFingerprint": ACCOUNT,
            "startedAt": (NOW - timedelta(hours=2)).isoformat().replace("+00:00", "Z"),
            "observedAt": NOW.isoformat().replace("+00:00", "Z"),
            "writerGeneration": 1,
            "journalRevision": 1,
            "eventCursor": 0,
            "lastEventId": "",
            "eventHeadHash": "b" * 64,
            "events": [],
        }
        with self.assertRaisesRegex(UpbitFunctionalBlocked, "myorder-fill-missing"):
            self.read((identifier,))

    def test_terminal_order_trade_aggregates_must_be_internally_exact(self) -> None:
        identifier = "functional-identifier-owned-aggregate-0001"
        filled = order(7, identifier=identifier)
        filled.update(
            {
                "trades_count": 1,
                "paid_fee": "5",
                "executed_volume": "0.0002",
                "executed_funds": "10000",
                "remaining_volume": "0",
                "trades": [
                    {
                        "uuid": "trade-uuid-aggregate-0001",
                        "price": "100000000",
                        "volume": "0.0001",
                        "funds": "10000",
                    }
                ],
            }
        )
        self.client.closed = [filled]
        self.client.details[identifier] = filled
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "executed-volume-mismatch"
        ):
            self.read((identifier,))

        filled["executed_volume"] = "0.0001"
        filled["executed_funds"] = "9999"
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "executed-funds-mismatch"
        ):
            self.read((identifier,))

        filled["executed_funds"] = "10000"
        filled["remaining_volume"] = "0.0001"
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked, "remaining-volume-nonzero"
        ):
            self.read((identifier,))

    def test_invalid_private_stream_timestamp_is_typed_fail_closed(self) -> None:
        self.reader.private_stream_reader = lambda **_kwargs: {
            "connected": True,
            "authenticated": True,
            "eventsComplete": True,
            "gapDetected": False,
            "externalActivityAbsent": True,
            "channel": "myOrder",
            "accountFingerprint": ACCOUNT,
            "startedAt": "not-a-time",
            "observedAt": NOW.isoformat().replace("+00:00", "Z"),
            "writerGeneration": 1,
            "journalRevision": 1,
            "eventCursor": 0,
            "lastEventId": "",
            "eventHeadHash": "b" * 64,
            "events": [],
        }
        with self.assertRaisesRegex(
            UpbitFunctionalBlocked,
            "private-myorder-time-attestation-invalid",
        ):
            self.read()

    def test_reader_exposes_no_order_or_cancel_method(self) -> None:
        self.assertFalse(hasattr(self.reader, "post"))
        self.assertFalse(hasattr(self.reader, "cancel"))


if __name__ == "__main__":
    unittest.main()
