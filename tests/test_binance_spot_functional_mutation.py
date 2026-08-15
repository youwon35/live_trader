from __future__ import annotations

import os
import hashlib
import json
import unittest
from contextlib import contextmanager
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


def exact_global_authority(**updates: object) -> dict[str, object]:
    projection: dict[str, object] = {
        "schemaVersion": "crypto-first-live-binance-authority-snapshot/v1",
        "scope": "CRYPTO_FIRST_LIVE_GLOBAL",
        "lane": "BINANCE_SPOT",
        "phase": "ACTIVE",
        "runId": "crypto-run-binance-mutation-0001",
        "sessionId": SESSION_ID,
        "permitId": PERMIT_ID,
        "permitHash": PERMIT_HASH,
        "accountFingerprint": ACCOUNT_FINGERPRINT,
        "ownerLeaseActive": True,
        "entryAuthorityOpen": True,
        "hardStopEpoch": 1_800_007_200.0,
        "revision": 1,
        "observedEpoch": 1_800_000_000.0,
    }
    projection.update(updates)
    return {
        **projection,
        "authorityHash": hashlib.sha256(
            json.dumps(
                projection,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest(),
    }


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
    def setUp(self) -> None:
        release = patch(
            "live_trader.binance_spot_functional_mutation."
            "PRODUCTION_MUTATION_AVAILABLE",
            True,
        )
        release.start()
        self.addCleanup(release.stop)

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

    def released_edge(
        self,
        *,
        claim_reader,
        sender,
        authority_reader=exact_authority,
        claim_marker=None,
        dispatch_lease_factory=None,
        global_first_live_dispatch_reservation=None,
        final_exclusivity_health_reader=None,
        clock=lambda: 1_800_000_000.0,
    ) -> BinanceSpotFunctionalMutationEdge:
        if claim_marker is None:
            def claim_marker(claim_id: str) -> None:
                claim = claim_reader(claim_id)
                claim["state"] = "POST_MAY_HAVE_CROSSED"

        if dispatch_lease_factory is None:
            @contextmanager
            def dispatch_lease_factory(**request: object):
                yield lambda: {
                    "active": True,
                    "sessionId": request["session_id"],
                    "claimId": request["claim_id"],
                    "ordinaryRoutesClosed": True,
                }

        if global_first_live_dispatch_reservation is None:
            @contextmanager
            def global_first_live_dispatch_reservation(**_request: object):
                yield exact_global_authority()

        return BinanceSpotFunctionalMutationEdge(
            authority_reader=authority_reader,
            claim_reader=claim_reader,
            claim_marker=claim_marker,
            dispatch_lease_factory=dispatch_lease_factory,
            global_first_live_dispatch_reservation=(
                global_first_live_dispatch_reservation
            ),
            final_exclusivity_health_reader=final_exclusivity_health_reader,
            sender=sender,
            clock=clock,
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

                edge = self.released_edge(
                    authority_reader=lambda: cleanup_authority,
                    claim_reader=lambda _: claim,
                    claim_marker=marker,
                    sender=lambda request, response=response: (
                        events.append(request.method) or {"ok": True, "json": response}
                    ),
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

    def test_release_false_blocks_wrapper_lambda_before_marker_and_sender(self) -> None:
        self.assertFalse(PRODUCTION_MUTATION_AVAILABLE)
        claim = durable_claim()
        markers: list[str] = []
        sends: list[str] = []
        edge = self.released_edge(
            authority_reader=exact_authority,
            claim_reader=lambda _: claim,
            claim_marker=lambda _: markers.append("durable-marker"),
            sender=lambda _: sends.append("sender")
            or {"ok": True, "json": {}},
        )
        with self.ready_env(), patch(
            "live_trader.binance_spot_functional_mutation."
            "PRODUCTION_MUTATION_AVAILABLE",
            False,
        ), self.assertRaises(BinanceSpotFunctionalMutationNotSent):
            edge(
                buy_action(),
                lambda: markers.append("caller-marker"),
                **call_context(),
            )
        self.assertEqual([], markers)
        self.assertEqual([], sends)

    def test_public_mock_transport_boolean_is_absent(self) -> None:
        with self.assertRaises(TypeError):
            BinanceSpotFunctionalMutationEdge(
                authority_reader=exact_authority,
                claim_reader=lambda _: durable_claim(),
                sender=lambda _: {"ok": True, "json": {}},
                allow_mock_transport=True,  # type: ignore[call-arg]
            )

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

        edge = self.released_edge(
            authority_reader=exact_authority,
            claim_reader=lambda _: claim,
            claim_marker=lambda _: (
                claim.__setitem__("state", "POST_MAY_HAVE_CROSSED"),
                events.append("mark"),
            ),
            sender=sender,
        )
        with self.ready_env():
            receipt = edge(
                buy_action(), lambda: None, **call_context()
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

        edge = self.released_edge(
            authority_reader=exact_authority,
            claim_reader=lambda _: claim,
            claim_marker=lambda _: (
                claim.__setitem__("state", "POST_MAY_HAVE_CROSSED"),
                events.append("mark"),
            ),
            sender=sender,
        )
        with self.ready_env(), self.assertRaises(
            BinanceSpotFunctionalMutationOutcomeUnknown
        ):
            edge(buy_action(), lambda: None, **call_context())
        self.assertEqual(["mark"], events)
        self.assertEqual(1, calls)

    def test_direct_shaped_action_without_exact_context_cannot_send(self) -> None:
        sends: list[str] = []
        edge = self.released_edge(
            authority_reader=exact_authority,
            claim_reader=lambda _: durable_claim(),
            sender=lambda _: sends.append("send") or {"ok": True, "json": {}},
        )
        with self.ready_env(), self.assertRaises(TypeError):
            edge(buy_action(), lambda: None)  # type: ignore[call-arg]
        self.assertEqual([], sends)

    def test_api_key_swap_immediately_before_edge_is_not_sent(self) -> None:
        markers: list[str] = []
        sends: list[str] = []
        edge = self.released_edge(
            authority_reader=exact_authority,
            claim_reader=lambda _: durable_claim(),
            sender=lambda _: sends.append("send") or {"ok": True, "json": {}},
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
                edge = self.released_edge(
                    authority_reader=exact_authority,
                    claim_reader=lambda _, claim=claim: claim,
                    sender=lambda _: sends.append("send")
                    or {"ok": True, "json": {}},
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

        edge = self.released_edge(
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
        )
        with self.ready_env():
            edge(
                buy_action(),
                lambda: events.append("caller-marker"),
                **call_context(),
            )
        self.assertEqual(["durable-cas", "send"], events)

    def test_global_authority_is_verified_before_durable_marker_and_send(self) -> None:
        claim = durable_claim()
        events: list[str] = []

        @contextmanager
        def global_reservation(**request: object):
            self.assertEqual("MUTATION_FINAL_PRE_MARKER", request["purpose"])
            self.assertEqual(SESSION_ID, request["session_id"])
            self.assertEqual(PERMIT_ID, request["permit_id"])
            self.assertEqual(PERMIT_HASH, request["permit_hash"])
            self.assertEqual(ACCOUNT_FINGERPRINT, request["account_fingerprint"])
            self.assertFalse(request["cleanup_only"])
            events.append("global-authority")
            yield exact_global_authority()

        def marker(claim_id: str) -> None:
            self.assertEqual("claim-mutation-0001", claim_id)
            claim["state"] = "POST_MAY_HAVE_CROSSED"
            events.append("durable-marker")

        edge = self.released_edge(
            authority_reader=exact_authority,
            claim_reader=lambda _: claim,
            claim_marker=marker,
            global_first_live_dispatch_reservation=global_reservation,
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
            clock=lambda: 1_800_000_000.0,
        )
        with self.ready_env():
            edge(buy_action(), lambda: None, **call_context())
        self.assertEqual(
            ["global-authority", "durable-marker", "send"], events
        )

    def test_swapped_stale_or_closed_global_authority_never_marks_or_sends(self) -> None:
        hostile_snapshots = (
            exact_global_authority(sessionId="bnsft-swapped-session-0001"),
            exact_global_authority(observedEpoch=1_799_999_990.0),
            exact_global_authority(entryAuthorityOpen=False),
            exact_global_authority(hardStopEpoch=1_800_000_000.0),
        )
        for snapshot in hostile_snapshots:
            with self.subTest(snapshot=snapshot):
                claim = durable_claim()
                events: list[str] = []

                @contextmanager
                def hostile_reservation(**_request: object):
                    yield snapshot

                edge = self.released_edge(
                    authority_reader=exact_authority,
                    claim_reader=lambda _: claim,
                    claim_marker=lambda _: events.append("durable-marker"),
                    global_first_live_dispatch_reservation=(
                        hostile_reservation
                    ),
                    sender=lambda _: events.append("send")
                    or {"ok": True, "json": {}},
                    clock=lambda: 1_800_000_000.0,
                )
                with self.ready_env(), self.assertRaises(
                    BinanceSpotFunctionalMutationNotSent
                ):
                    edge(buy_action(), lambda: None, **call_context())
                self.assertEqual([], events)

    def test_global_reservation_failure_never_marks_or_sends(self) -> None:
        claim = durable_claim()
        events: list[str] = []

        def failed_reservation(**_request: object):
            raise TimeoutError("coordinator authority unavailable")

        edge = self.released_edge(
            authority_reader=exact_authority,
            claim_reader=lambda _: claim,
            claim_marker=lambda _: events.append("durable-marker"),
            global_first_live_dispatch_reservation=failed_reservation,
            sender=lambda _: events.append("send") or {"ok": True, "json": {}},
            clock=lambda: 1_800_000_000.0,
        )
        with self.ready_env(), self.assertRaisesRegex(
            BinanceSpotFunctionalMutationNotSent, "failed closed"
        ):
            edge(buy_action(), lambda: None, **call_context())
        self.assertEqual([], events)

    def test_global_dispatch_reservation_is_held_through_marker_and_sender(
        self,
    ) -> None:
        claim = durable_claim()
        events: list[str] = []
        reservation_active = False

        @contextmanager
        def reservation(**request: object):
            nonlocal reservation_active
            self.assertEqual("MUTATION_FINAL_PRE_MARKER", request["purpose"])
            self.assertEqual(SESSION_ID, request["session_id"])
            self.assertFalse(request["cleanup_only"])
            reservation_active = True
            events.append("reservation-enter")
            try:
                yield exact_global_authority()
            finally:
                reservation_active = False
                events.append("reservation-exit")

        def marker(_claim_id: str) -> None:
            self.assertTrue(reservation_active)
            claim["state"] = "POST_MAY_HAVE_CROSSED"
            events.append("durable-marker")

        def sender(_request: object) -> dict[str, object]:
            self.assertTrue(reservation_active)
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

        edge = self.released_edge(
            authority_reader=exact_authority,
            claim_reader=lambda _: claim,
            claim_marker=marker,
            global_first_live_dispatch_reservation=reservation,
            sender=sender,
            clock=lambda: 1_800_000_000.0,
        )
        with self.ready_env():
            edge(buy_action(), lambda: None, **call_context())
        self.assertFalse(reservation_active)
        self.assertEqual(
            [
                "reservation-enter",
                "durable-marker",
                "send",
                "reservation-exit",
            ],
            events,
        )

    def test_global_reservation_is_outermost_to_match_stop_lock_order(
        self,
    ) -> None:
        claim = durable_claim()
        events: list[str] = []
        global_active = False
        local_active = False

        @contextmanager
        def reservation(**_request: object):
            nonlocal global_active
            self.assertFalse(local_active)
            global_active = True
            events.append("global-enter")
            try:
                yield exact_global_authority()
            finally:
                self.assertFalse(local_active)
                events.append("global-exit")
                global_active = False

        @contextmanager
        def local_lease(**_request: object):
            nonlocal local_active
            self.assertTrue(global_active)
            local_active = True
            events.append("local-enter")
            try:
                yield lambda: {
                    "active": True,
                    "sessionId": SESSION_ID,
                    "claimId": "claim-mutation-0001",
                    "ordinaryRoutesClosed": True,
                }
            finally:
                events.append("local-exit")
                local_active = False

        def marker(_claim_id: str) -> None:
            self.assertTrue(global_active)
            self.assertTrue(local_active)
            claim["state"] = "POST_MAY_HAVE_CROSSED"
            events.append("durable-marker")

        def sender(_request: object) -> dict[str, object]:
            self.assertTrue(global_active)
            self.assertTrue(local_active)
            events.append("send")
            return {
                "ok": True,
                "json": {"orderId": "101", "clientOrderId": OWNER + "b"},
            }

        edge = self.released_edge(
            authority_reader=exact_authority,
            claim_reader=lambda _: claim,
            claim_marker=marker,
            dispatch_lease_factory=local_lease,
            global_first_live_dispatch_reservation=reservation,
            sender=sender,
            clock=lambda: 1_800_000_000.0,
        )
        with self.ready_env():
            edge(buy_action(), lambda: None, **call_context())
        self.assertEqual(
            [
                "global-enter",
                "local-enter",
                "durable-marker",
                "send",
                "local-exit",
                "global-exit",
            ],
            events,
        )

    def test_bad_global_dispatch_reservation_never_marks_or_sends(self) -> None:
        claim = durable_claim()
        events: list[str] = []

        @contextmanager
        def reservation(**_request: object):
            events.append("reservation-enter")
            try:
                yield exact_global_authority(ownerLeaseActive=False)
            finally:
                events.append("reservation-exit")

        edge = self.released_edge(
            authority_reader=exact_authority,
            claim_reader=lambda _: claim,
            claim_marker=lambda _: events.append("durable-marker"),
            global_first_live_dispatch_reservation=reservation,
            sender=lambda _: events.append("send") or {"ok": True, "json": {}},
            clock=lambda: 1_800_000_000.0,
        )
        with self.ready_env(), self.assertRaises(
            BinanceSpotFunctionalMutationNotSent
        ):
            edge(buy_action(), lambda: None, **call_context())
        self.assertEqual(
            ["reservation-enter", "reservation-exit"], events
        )

    def test_observer_failure_inside_both_route_locks_never_marks_or_sends(
        self,
    ) -> None:
        claim = durable_claim()
        events: list[str] = []

        @contextmanager
        def reservation(**_request: object):
            events.append("global-enter")
            try:
                yield exact_global_authority()
            finally:
                events.append("global-exit")

        @contextmanager
        def local_lease(**_request: object):
            events.append("local-enter")
            try:
                yield lambda: {
                    "active": True,
                    "sessionId": SESSION_ID,
                    "claimId": "claim-mutation-0001",
                    "ordinaryRoutesClosed": True,
                }
            finally:
                events.append("local-exit")

        def failed_health(**request: object) -> dict[str, object]:
            self.assertEqual("MUTATION_FINAL_PRE_MARKER", request["purpose"])
            events.append("observer-health")
            raise TimeoutError("signed observer snapshot is stale")

        edge = self.released_edge(
            authority_reader=exact_authority,
            claim_reader=lambda _: claim,
            claim_marker=lambda _: events.append("durable-marker"),
            dispatch_lease_factory=local_lease,
            global_first_live_dispatch_reservation=reservation,
            final_exclusivity_health_reader=failed_health,
            sender=lambda _: events.append("send") or {"ok": True, "json": {}},
            clock=lambda: 1_800_000_000.0,
        )
        with self.ready_env(), self.assertRaises(
            BinanceSpotFunctionalMutationNotSent
        ):
            edge(buy_action(), lambda: None, **call_context())
        self.assertEqual(
            [
                "global-enter",
                "local-enter",
                "observer-health",
                "local-exit",
                "global-exit",
            ],
            events,
        )
        self.assertEqual("SUBMITTING", claim["state"])

    def test_observer_revoke_never_blocks_exact_session_cleanup(self) -> None:
        action = cleanup_sell_action()
        claim = durable_claim(action, action_kind="CLEANUP_SELL")
        events: list[str] = []

        def marker(_claim_id: str) -> None:
            claim["state"] = "POST_MAY_HAVE_CROSSED"
            events.append("durable-marker")

        edge = self.released_edge(
            authority_reader=lambda: exact_authority(
                killSwitch=True,
                cleanupOnlyAuthority=True,
                cleanupSessionId=SESSION_ID,
                cleanupCapabilityHash=CAPABILITY_HASH,
            ),
            claim_reader=lambda _: claim,
            claim_marker=marker,
            final_exclusivity_health_reader=lambda **_: (_ for _ in ()).throw(
                AssertionError("cleanup must not require healthy observer")
            ),
            sender=lambda _: events.append("send")
            or {
                "ok": True,
                "json": {
                    "orderId": "202",
                    "clientOrderId": OWNER + "f",
                },
            },
            clock=lambda: 1_800_000_000.0,
        )
        with self.ready_env():
            result = edge(
                action,
                lambda: None,
                **call_context(action=action),
            )
        self.assertEqual(OWNER + "f", result["clientOrderId"])
        self.assertEqual(["durable-marker", "send"], events)

    def test_reservation_exit_failure_after_marker_is_outcome_unknown(
        self,
    ) -> None:
        claim = durable_claim()
        events: list[str] = []

        @contextmanager
        def reservation(**_request: object):
            yield exact_global_authority()
            events.append("reservation-exit-failed")
            raise RuntimeError("route unlock evidence unavailable")

        def marker(_claim_id: str) -> None:
            claim["state"] = "POST_MAY_HAVE_CROSSED"
            events.append("durable-marker")

        edge = self.released_edge(
            authority_reader=exact_authority,
            claim_reader=lambda _: claim,
            claim_marker=marker,
            global_first_live_dispatch_reservation=reservation,
            sender=lambda _: events.append("send")
            or {
                "ok": True,
                "json": {
                    "orderId": "101",
                    "clientOrderId": OWNER + "b",
                },
            },
            clock=lambda: 1_800_000_000.0,
        )
        with self.ready_env(), self.assertRaises(
            BinanceSpotFunctionalMutationOutcomeUnknown
        ):
            edge(buy_action(), lambda: None, **call_context())
        self.assertEqual(
            ["durable-marker", "send", "reservation-exit-failed"], events
        )

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
