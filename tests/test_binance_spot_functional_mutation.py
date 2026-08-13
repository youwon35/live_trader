from __future__ import annotations

import os
import hashlib
import json
import unittest
from unittest.mock import patch
import urllib.parse

from live_trader.binance_spot_continuous_functional import EVIDENCE_CLASS
from live_trader.binance_spot_functional_transport import binance_api_key_fingerprint
from live_trader.binance_spot_functional_mutation import (
    BinanceSpotFunctionalMutationEdge,
    BinanceSpotFunctionalMutationNotSent,
    BinanceSpotFunctionalMutationOutcomeUnknown,
    PRODUCTION_MUTATION_AVAILABLE,
    build_binance_spot_functional_mutation_request,
)


API_KEY = "api-key"
ACCOUNT_FINGERPRINT = binance_api_key_fingerprint(API_KEY)
SESSION_ID = "bnsft-mutation-boundary-session-0001"
OWNER = f"ftb-{hashlib.sha256(SESSION_ID.encode()).hexdigest()[:12]}-"
CAPABILITY = "functional-capability-mutation-test-00000001"
CAPABILITY_HASH = hashlib.sha256(CAPABILITY.encode()).hexdigest()
PERMIT_ID = "functional-test-binance-mutation-0001"
PERMIT_HASH = "a" * 64
AUTHORITY_REVISION = "binance-functional-control-7"


def exact_authority(**updates: object) -> dict[str, object]:
    result: dict[str, object] = {
        "realOrdersEnabled": True,
        "dryRun": False,
        "killSwitch": False,
        "newEntriesBlocked": True,
        "ordinaryLiveAllowed": False,
        "smokeAllowed": False,
        "functionalOnlyRouting": True,
        "activePermitId": PERMIT_ID,
        "activePermitHash": PERMIT_HASH,
        "activeSessionId": SESSION_ID,
        "functionalCapabilityHash": CAPABILITY_HASH,
        "cleanupOnlyAuthority": False,
        "cleanupSessionId": "",
        "cleanupCapabilityHash": "",
        "authorityRevision": AUTHORITY_REVISION,
    }
    result.update(updates)
    return result


def call_context(**updates: object) -> dict[str, object]:
    action = updates.pop("action", buy_action()) if "buy_action" in globals() else {}
    result: dict[str, object] = {
        "claim_id": "claim-mutation-0001",
        "sealed_action_hash": hashlib.sha256(
            json.dumps(
                action,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest(),
        "functional_capability": CAPABILITY,
        "session_id": SESSION_ID,
        "permit_id": PERMIT_ID,
        "permit_hash": PERMIT_HASH,
        "account_fingerprint": ACCOUNT_FINGERPRINT,
        "authority_revision": AUTHORITY_REVISION,
    }
    result.update(updates)
    return result


def durable_claim(action: dict[str, object] | None = None, **updates: object) -> dict[str, object]:
    sealed = dict(action or buy_action())
    result: dict[str, object] = {
        "claim_id": "claim-mutation-0001",
        "session_id": SESSION_ID,
        "action_kind": "BUY",
        "client_order_id": sealed["clientOrderId"],
        "state": "SUBMITTING",
        "sealed_action_json": json.dumps(
            sealed,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
    }
    result.update(updates)
    return result


def buy_action(**updates: object) -> dict[str, object]:
    result: dict[str, object] = {
        "kind": "BUY",
        "product": "SPOT",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "orderType": "MARKET",
        "quantity": "0",
        "quoteOrderQty": "10",
        "clientOrderId": OWNER + "b",
        "evaluationId": "eval-natural-0001",
        "evaluationHash": "b" * 64,
        "officialWindowHash": "c" * 64,
        "barCloseEpoch": 1_800_000_000.0,
        "functionalOnly": True,
        "cleanupOnly": False,
        "evidenceClass": EVIDENCE_CLASS,
    }
    result.update(updates)
    return result


def cleanup_sell_action(**updates: object) -> dict[str, object]:
    result: dict[str, object] = {
        "kind": "SELL",
        "product": "SPOT",
        "symbol": "BTCUSDT",
        "side": "SELL",
        "orderType": "MARKET",
        "quantity": "0.00015",
        "quoteOrderQty": "0",
        "clientOrderId": OWNER + "f",
        "functionalOnly": True,
        "cleanupOnly": True,
        "evidenceClass": EVIDENCE_CLASS,
    }
    result.update(updates)
    return result


def cancel_action(**updates: object) -> dict[str, object]:
    result: dict[str, object] = {
        "kind": "CANCEL",
        "product": "SPOT",
        "symbol": "BTCUSDT",
        "brokerOrderId": "101",
        "origClientOrderId": OWNER + "b",
        "clientOrderId": OWNER + "c",
        "functionalOnly": True,
        "cleanupOnly": True,
        "evidenceClass": EVIDENCE_CLASS,
    }
    result.update(updates)
    return result


class BinanceSpotFunctionalMutationTest(unittest.TestCase):
    def ready_env(self):
        return patch.dict(
            os.environ,
            {
                "BINANCE_API_KEY": API_KEY,
                "BINANCE_API_SECRET": "secret",
                "LIVE_TRADER_ENABLE_REAL_ORDERS": "false",
                "BINANCE_SPOT_FUNCTIONAL_LIVE_ENABLED": "true",
            },
            clear=False,
        )

    def test_exact_buy_uses_quote_cap_and_owned_new_client_order_id(self) -> None:
        with self.ready_env(), patch(
            "live_trader.binance_spot_functional_mutation.binance_timestamp_ms",
            return_value=1_800_000_000_000,
        ):
            request = build_binance_spot_functional_mutation_request(buy_action())
        self.assertEqual("POST", request.method)
        query = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(request.url).query))
        self.assertEqual("10", query["quoteOrderQty"])
        self.assertEqual(OWNER + "b", query["newClientOrderId"])
        self.assertNotIn("quantity", query)
        self.assertNotIn("reduceOnly", query)
        self.assertNotIn("positionSide", query)
        self.assertNotIn("secret", str(request.preview()))

    def test_cleanup_sell_can_exceed_entry_notional_but_is_exact_base_only(self) -> None:
        with self.ready_env():
            request = build_binance_spot_functional_mutation_request(
                cleanup_sell_action()
            )
        query = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(request.url).query))
        self.assertEqual("SELL", query["side"])
        self.assertEqual("0.00015", query["quantity"])
        self.assertNotIn("quoteOrderQty", query)

    def test_cancel_sends_both_owned_broker_and_client_identity(self) -> None:
        with self.ready_env():
            request = build_binance_spot_functional_mutation_request(cancel_action())
        query = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(request.url).query))
        self.assertEqual("DELETE", request.method)
        self.assertEqual("101", query["orderId"])
        self.assertEqual(OWNER + "b", query["origClientOrderId"])
        self.assertEqual(OWNER + "c", query["newClientOrderId"])

    def test_third_cleanup_sell_and_cancel_generation_reach_exact_mock_edge(self) -> None:
        cleanup_authority = exact_authority(
            killSwitch=True,
            cleanupOnlyAuthority=True,
            cleanupSessionId=SESSION_ID,
            cleanupCapabilityHash=CAPABILITY_HASH,
        )
        for action, action_kind, response in (
            (
                cleanup_sell_action(clientOrderId=OWNER + "f3"),
                "CLEANUP_SELL_3",
                {
                    "orderId": "303",
                    "clientOrderId": OWNER + "f3",
                    "symbol": "BTCUSDT",
                    "side": "SELL",
                    "type": "MARKET",
                    "status": "NEW",
                },
            ),
            (
                cancel_action(
                    clientOrderId=OWNER + "c3",
                    origClientOrderId=OWNER + "f3",
                    brokerOrderId="303",
                ),
                "CANCEL_3",
                {
                    "orderId": "303",
                    "origClientOrderId": OWNER + "f3",
                    "symbol": "BTCUSDT",
                    "status": "CANCELED",
                },
            ),
        ):
            with self.subTest(action_kind=action_kind):
                claim = durable_claim(
                    action,
                    action_kind=action_kind,
                    client_order_id=action["clientOrderId"],
                )
                events: list[str] = []

                def marker(claim_id: str) -> None:
                    self.assertEqual("claim-mutation-0001", claim_id)
                    claim["state"] = "POST_MAY_HAVE_CROSSED"
                    events.append("mark")

                edge = BinanceSpotFunctionalMutationEdge(
                    authority_reader=lambda: cleanup_authority,
                    claim_reader=lambda _: claim,
                    claim_marker=marker,
                    sender=lambda request, response=response: (
                        events.append(request.method) or {"ok": True, "json": response}
                    ),
                    allow_mock_transport=True,
                )
                with self.ready_env():
                    receipt = edge(action, lambda: None, **call_context(action=action))
                self.assertEqual(response["orderId"], receipt["orderId"])
                self.assertEqual(
                    ["mark", "DELETE" if action_kind.startswith("CANCEL") else "POST"],
                    events,
                )

    def test_cross_product_extra_fields_and_looser_buy_are_not_sent(self) -> None:
        for action in (
            buy_action(quoteOrderQty="10.01"),
            buy_action(symbol="ETHUSDT"),
            buy_action(reduceOnly=True),
            buy_action(product="MARGIN"),
            cancel_action(brokerOrderId="not-numeric"),
        ):
            with self.subTest(action=action):
                with self.assertRaises(BinanceSpotFunctionalMutationNotSent):
                    build_binance_spot_functional_mutation_request(action)

    def test_production_unavailable_or_missing_gate_never_marks_transport(self) -> None:
        self.assertFalse(PRODUCTION_MUTATION_AVAILABLE)
        markers: list[str] = []
        edge = BinanceSpotFunctionalMutationEdge(
            authority_reader=exact_authority,
            claim_reader=lambda _: durable_claim(),
            sender=lambda _: {"ok": True, "json": {}},
        )
        with patch.dict(os.environ, {}, clear=True), self.assertRaises(
            BinanceSpotFunctionalMutationNotSent
        ):
            edge(
                buy_action(),
                lambda: markers.append("sent"),
                **call_context(),
            )
        self.assertEqual([], markers)

    def test_mock_edge_marks_once_immediately_before_single_send(self) -> None:
        events: list[str] = []
        claim = durable_claim()

        def sender(request: object) -> dict[str, object]:
            events.append("send")
            return {
                "ok": True,
                "json": {
                    "orderId": "101",
                    "clientOrderId": OWNER + "b",
                    "symbol": "BTCUSDT",
                    "side": "BUY",
                    "type": "MARKET",
                    "status": "FILLED",
                },
            }

        edge = BinanceSpotFunctionalMutationEdge(
            authority_reader=exact_authority,
            claim_reader=lambda _: claim,
            sender=sender,
            allow_mock_transport=True,
        )
        with self.ready_env():
            def marker() -> None:
                claim["state"] = "POST_MAY_HAVE_CROSSED"
                events.append("mark")

            receipt = edge(
                buy_action(), marker, **call_context()
            )
        self.assertEqual(["mark", "send"], events)
        self.assertEqual("101", receipt["orderId"])

    def test_after_marker_timeout_is_explicitly_unknown_and_never_retried(self) -> None:
        calls = 0
        events: list[str] = []
        claim = durable_claim()

        def sender(_: object) -> dict[str, object]:
            nonlocal calls
            calls += 1
            raise TimeoutError("read timeout")

        edge = BinanceSpotFunctionalMutationEdge(
            authority_reader=exact_authority,
            claim_reader=lambda _: claim,
            sender=sender,
            allow_mock_transport=True,
        )
        with self.ready_env(), self.assertRaises(
            BinanceSpotFunctionalMutationOutcomeUnknown
        ):
            def marker() -> None:
                claim["state"] = "POST_MAY_HAVE_CROSSED"
                events.append("mark")

            edge(buy_action(), marker, **call_context())
        self.assertEqual(["mark"], events)
        self.assertEqual(1, calls)

    def test_direct_shaped_action_without_exact_context_cannot_send(self) -> None:
        sends: list[str] = []
        edge = BinanceSpotFunctionalMutationEdge(
            authority_reader=exact_authority,
            claim_reader=lambda _: durable_claim(),
            sender=lambda _: sends.append("send") or {"ok": True, "json": {}},
            allow_mock_transport=True,
        )
        with self.ready_env(), self.assertRaises(TypeError):
            edge(buy_action(), lambda: None)  # type: ignore[call-arg]
        self.assertEqual([], sends)

    def test_api_key_swap_immediately_before_edge_is_not_sent(self) -> None:
        markers: list[str] = []
        sends: list[str] = []
        edge = BinanceSpotFunctionalMutationEdge(
            authority_reader=exact_authority,
            claim_reader=lambda _: durable_claim(),
            sender=lambda _: sends.append("send") or {"ok": True, "json": {}},
            allow_mock_transport=True,
        )
        with self.ready_env(), patch.dict(
            os.environ, {"BINANCE_API_KEY": "rotated-api-key"}, clear=False
        ), self.assertRaises(BinanceSpotFunctionalMutationNotSent):
            edge(
                buy_action(),
                lambda: markers.append("mark"),
                **call_context(),
            )
        self.assertEqual([], markers)
        self.assertEqual([], sends)

    def test_raw_capability_cannot_bypass_missing_or_tampered_durable_claim(self) -> None:
        sends: list[str] = []
        markers: list[str] = []
        for claim in (
            durable_claim(state="CLAIMED"),
            durable_claim(
                sealed_action_json=json.dumps(buy_action(quoteOrderQty="9"))
            ),
            durable_claim(session_id="bnsft-different-session-0001"),
        ):
            with self.subTest(claim=claim):
                edge = BinanceSpotFunctionalMutationEdge(
                    authority_reader=exact_authority,
                    claim_reader=lambda _, claim=claim: claim,
                    sender=lambda _: sends.append("send")
                    or {"ok": True, "json": {}},
                    allow_mock_transport=True,
                )
                with self.ready_env(), self.assertRaises(
                    BinanceSpotFunctionalMutationNotSent
                ):
                    edge(
                        buy_action(),
                        lambda: markers.append("mark"),
                        **call_context(),
                    )
        self.assertEqual([], markers)
        self.assertEqual([], sends)

    def test_backend_marker_cas_is_used_even_when_caller_marker_is_noop(self) -> None:
        claim = durable_claim()
        events: list[str] = []

        def backend_marker(claim_id: str) -> None:
            self.assertEqual("claim-mutation-0001", claim_id)
            self.assertEqual("SUBMITTING", claim["state"])
            claim["state"] = "POST_MAY_HAVE_CROSSED"
            events.append("durable-cas")

        edge = BinanceSpotFunctionalMutationEdge(
            authority_reader=exact_authority,
            claim_reader=lambda _: claim,
            claim_marker=backend_marker,
            sender=lambda _: events.append("send")
            or {
                "ok": True,
                "json": {
                    "orderId": "101",
                    "clientOrderId": OWNER + "b",
                    "symbol": "BTCUSDT",
                    "side": "BUY",
                    "type": "MARKET",
                    "status": "FILLED",
                },
            },
            allow_mock_transport=True,
        )
        with self.ready_env():
            edge(
                buy_action(),
                lambda: events.append("caller-marker"),
                **call_context(),
            )
        self.assertEqual(["durable-cas", "send"], events)

    def test_global_real_orders_true_closes_functional_edge(self) -> None:
        with self.ready_env(), patch.dict(
            os.environ, {"LIVE_TRADER_ENABLE_REAL_ORDERS": "true"}, clear=False
        ):
            request = build_binance_spot_functional_mutation_request(buy_action())
        self.assertIn(
            "LIVE_TRADER_ENABLE_REAL_ORDERS_MUST_REMAIN_FALSE",
            request.blocked_reasons,
        )

    def test_production_mutation_wrong_origin_contains_no_key_or_signature(self) -> None:
        for origin in (
            "http://api.binance.com",
            "https://testnet.binance.vision",
            "https://api.binance.com.evil.example",
        ):
            with self.subTest(origin=origin), self.ready_env(), patch.dict(
                os.environ, {"BINANCE_BASE_URL": origin}, clear=False
            ):
                request = build_binance_spot_functional_mutation_request(
                    buy_action(),
                    expected_account_fingerprint=ACCOUNT_FINGERPRINT,
                )
            self.assertFalse(request.can_send)
            self.assertIn(
                "BINANCE_SPOT_PRODUCTION_ORIGIN_CHANGED",
                request.blocked_reasons,
            )
            self.assertNotIn(API_KEY, request.url)
            self.assertNotIn("signature=", request.url)
            self.assertEqual({}, request.headers)


if __name__ == "__main__":
    unittest.main()
