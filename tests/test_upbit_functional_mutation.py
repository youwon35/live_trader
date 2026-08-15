from __future__ import annotations

import hashlib
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import inspect
import os
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from live_trader.upbit_functional_mutation import (
    UPBIT_FUNCTIONAL_MUTATION_AVAILABLE,
    UpbitFunctionalMutationEdge,
    UpbitFunctionalMutationNotSent,
    UpbitFunctionalMutationOutcomeUnknown,
    build_upbit_functional_cancel_request,
    build_upbit_functional_order_request,
)
from live_trader.upbit_continuous_functional import _stable_hash


SESSION = "upbit-functional-session-mutation-0001"
ACCOUNT = "c" * 64
RAW_CAPABILITY = "raw-upbit-functional-capability-secret-0000000001"
CAPABILITY_HASH = hashlib.sha256(RAW_CAPABILITY.encode()).hexdigest()
IDENTIFIER = (
    "uft-"
    + hashlib.sha256(SESSION.encode("utf-8")).hexdigest()[:8]
    + "-"
    + "a" * 19
)
PERMIT_ID = "upbit-permit-mutation-0001"
PERMIT_HASH = "b" * 64
ROUTE_SCOPE_HASH = "d" * 64
SESSION_SCOPE_HASH = "e" * 64
CLAIM_ID = "upbit-claim-mutation-0001"
NOW = datetime(2026, 8, 15, 1, 0, tzinfo=timezone.utc)
OWNER = "f" * 64


def buy_payload(**updates: str) -> dict[str, str]:
    result = {
        "market": "KRW-BTC",
        "side": "bid",
        "ord_type": "price",
        "price": "10000",
        "identifier": IDENTIFIER,
    }
    result.update(updates)
    return result


def authority(**updates: object) -> dict[str, object]:
    result: dict[str, object] = {
        "executionPurpose": "FUNCTIONAL_TEST",
        "executionRoute": "UPBIT_KRW_SPOT_CONTINUOUS",
        "functionalTestSessionId": SESSION,
        "functionalTestPermitId": PERMIT_ID,
        "functionalTestPermitHash": PERMIT_HASH,
        "functionalTestRouteScopeHash": ROUTE_SCOPE_HASH,
        "functionalTestSessionScopeHash": SESSION_SCOPE_HASH,
        "functionalTestAccountFingerprint": ACCOUNT,
        "functionalCapabilityHash": CAPABILITY_HASH,
        "functionalMutationEnabled": True,
        "functionalOnlyRouting": True,
        "ordinaryRoutesClosed": True,
        "upbitSmokeRouteClosed": True,
        "newEntriesBlocked": True,
    }
    result.update(updates)
    return result


def global_authority(**updates: object) -> dict[str, object]:
    body: dict[str, object] = {
        "schemaVersion": "upbit-global-first-live-dispatch-authority/v1",
        "scope": "CRYPTO_FIRST_LIVE_GLOBAL",
        "lane": "UPBIT",
        "phase": "ACTIVE",
        "runId": SESSION,
        "sessionId": SESSION,
        "permitId": PERMIT_ID,
        "permitHash": PERMIT_HASH,
        "accountFingerprint": ACCOUNT,
        "routeScopeHash": ROUTE_SCOPE_HASH,
        "ownerIdentityHash": OWNER,
        "ownerLeaseActive": True,
        "entryAuthorityOpen": True,
        "cleanupAuthorityOpen": False,
        "hardStopEpoch": (NOW + timedelta(hours=2)).timestamp(),
        "ownerLeaseExpiresEpoch": NOW.timestamp() + 30,
        "revision": 9,
        "observedEpoch": NOW.timestamp(),
        "killSwitch": False,
        "stopRequested": False,
    }
    body.update(updates)
    return {**body, "authorityHash": _stable_hash(body)}


class UpbitFunctionalMutationTest(unittest.TestCase):
    @staticmethod
    def edge(
        *,
        sender,
        request_hash: str,
        authority_reader=authority,
        action: str = "STRATEGY_BUY",
        session_state: str = "ACTIVE",
        global_reader=None,
        global_reserver=None,
        post_boundary_marker=None,
        claim_identifier: str = IDENTIFIER,
        marker_is_durable: bool = True,
    ) -> UpbitFunctionalMutationEdge:
        scope = SimpleNamespace(
            permit_id=PERMIT_ID,
            permit_hash=PERMIT_HASH,
            route_scope_hash=ROUTE_SCOPE_HASH,
            account_fingerprint=ACCOUNT,
            ends_at=NOW + timedelta(hours=2),
        )
        durable_claim: dict[str, object] = {
            "session_id": SESSION,
            "permit_id": PERMIT_ID,
            "permit_hash": PERMIT_HASH,
            "scope_hash": SESSION_SCOPE_HASH,
            "session_state": session_state,
            "capability_hash": CAPABILITY_HASH,
            "claim_id": CLAIM_ID,
            "slot": action,
            "side": "BID",
            "identifier": claim_identifier,
            "target_identifier": "",
            "request_hash": request_hash,
            "claim_state": "CLAIMED_PRE_POST",
        }

        def read_claim(claim_id: str) -> dict[str, object]:
            return {**durable_claim, "claim_id": claim_id}

        marker_impl = post_boundary_marker or (
            lambda claim_id, _request_hash: {
                "claimId": claim_id,
                "state": "POST_MAY_HAVE_CROSSED",
            }
        )

        def mark_durably(claim_id: str, actual_request_hash: str):
            marked = marker_impl(claim_id, actual_request_hash)
            if (
                marker_is_durable
                and isinstance(marked, dict)
                and marked.get("claimId") == claim_id
                and marked.get("state") == "POST_MAY_HAVE_CROSSED"
            ):
                durable_claim["claim_state"] = "POST_MAY_HAVE_CROSSED"
            return marked

        if global_reader is None:
            global_reader = lambda _request: global_authority()
        if global_reserver is None:
            @contextmanager
            def default_reserver(_request):
                yield global_authority()

            global_reserver = default_reserver
        return UpbitFunctionalMutationEdge(
            session_id=SESSION,
            account_fingerprint=ACCOUNT,
            permit_id=PERMIT_ID,
            permit_hash=PERMIT_HASH,
            route_scope_hash=ROUTE_SCOPE_HASH,
            session_scope_hash=SESSION_SCOPE_HASH,
            authority_reader=authority_reader,
            claim_reader=read_claim,
            post_boundary_marker=mark_durably,
            global_first_live_authority_reader=global_reader,
            global_first_live_dispatch_reserver=global_reserver,
            global_first_live_owner_identity_hash=OWNER,
            global_first_live_scope=scope,
            clock=lambda: NOW,
            sender=sender,
        )

    def ready_env(self, **updates: str):
        values = {
            "UPBIT_ACCESS_KEY": "upbit-access",
            "UPBIT_SECRET_KEY": "upbit-secret",
            "UPBIT_BASE_URL": "https://api.upbit.com",
            "UPBIT_FUNCTIONAL_LIVE_ENABLED": "true",
            "LIVE_TRADER_ENABLE_REAL_ORDERS": "false",
        }
        values.update(updates)
        return patch.dict(os.environ, values, clear=False)

    def test_exact_quote_buy_and_fractional_sell_have_no_cross_product_fields(self) -> None:
        with self.ready_env():
            buy = build_upbit_functional_order_request(buy_payload())
            sell = build_upbit_functional_order_request(
                {
                    "market": "KRW-BTC",
                    "side": "ask",
                    "ord_type": "market",
                    "volume": "0.00012345",
                    "identifier": IDENTIFIER,
                }
            )
        self.assertEqual("POST", buy.method)
        self.assertEqual("10000", buy.body["price"])
        self.assertNotIn("volume", buy.body)
        self.assertEqual("0.00012345", sell.body["volume"])
        self.assertNotIn("price", sell.body)
        self.assertNotIn("margin", str(buy.preview()).lower())
        self.assertNotIn("withdraw", str(buy.preview()).lower())
        self.assertNotIn("upbit-secret", str(buy.preview()))

    def test_production_mutation_rejects_nonofficial_origin_before_signing(self) -> None:
        for origin in (
            "http://api.upbit.com",
            "https://upbit.example.test",
            "https://api.upbit.com:443",
            "https://api.upbit.com/redirect",
        ):
            with self.subTest(origin=origin), self.ready_env(
                UPBIT_BASE_URL=origin
            ), patch(
                "live_trader.upbit_functional_mutation."
                "build_upbit_functional_authorization",
                side_effect=AssertionError("must block before signing"),
            ), self.assertRaisesRegex(
                UpbitFunctionalMutationNotSent, "origin-not-official"
            ):
                build_upbit_functional_order_request(buy_payload())

        with self.ready_env(UPBIT_BASE_URL="https://upbit.example.test"):
            mock = build_upbit_functional_order_request(
                buy_payload(), allow_mock_origin=True
            )
        self.assertTrue(mock.url.startswith("https://upbit.example.test/"))

    def test_cancel_is_identifier_only_and_does_not_expose_other_surfaces(self) -> None:
        with self.ready_env():
            request = build_upbit_functional_cancel_request(IDENTIFIER)
        self.assertEqual("DELETE", request.method)
        self.assertEqual({"identifier": IDENTIFIER}, request.query)
        self.assertNotIn("uuid=", request.url)
        with self.assertRaises(UpbitFunctionalMutationNotSent):
            build_upbit_functional_cancel_request("external-order")

    def test_valid_shape_from_another_session_is_rejected_before_sender(self) -> None:
        other = (
            "uft-"
            + hashlib.sha256((SESSION + "-other").encode("utf-8")).hexdigest()[:8]
            + "-"
            + "a" * 19
        )
        payload = buy_payload(identifier=other)
        request_hash = _stable_hash(
            {key: value for key, value in payload.items() if key != "identifier"}
        )
        sends: list[object] = []
        edge = self.edge(
            sender=lambda request: sends.append(request) or {},
            request_hash=request_hash,
            claim_identifier=other,
        )
        with self.ready_env(), patch(
            "live_trader.upbit_functional_mutation."
            "UPBIT_FUNCTIONAL_MUTATION_AVAILABLE",
            True,
        ), self.assertRaisesRegex(
            UpbitFunctionalMutationNotSent,
            "identifier-session-prefix-mismatch",
        ):
            edge.post(
                payload,
                functional_capability=RAW_CAPABILITY,
                functional_action="STRATEGY_BUY",
                claim_id=CLAIM_ID,
                request_hash=request_hash,
            )
        self.assertEqual([], sends)

    def test_global_real_orders_true_and_loose_payloads_are_not_sendable(self) -> None:
        with self.ready_env(LIVE_TRADER_ENABLE_REAL_ORDERS="true"):
            request = build_upbit_functional_order_request(buy_payload())
        self.assertIn(
            "LIVE_TRADER_ENABLE_REAL_ORDERS_MUST_REMAIN_FALSE",
            request.blocked_reasons,
        )
        for payload in (
            buy_payload(price="10001"),
            buy_payload(market="KRW-ETH"),
            buy_payload(product="MARGIN"),
            buy_payload(side="ask"),
        ):
            with self.subTest(payload=payload), self.assertRaises(
                UpbitFunctionalMutationNotSent
            ):
                build_upbit_functional_order_request(payload)

    def test_production_unavailable_and_forged_capability_block_before_sender(self) -> None:
        self.assertFalse(UPBIT_FUNCTIONAL_MUTATION_AVAILABLE)
        sends: list[object] = []
        request_hash = _stable_hash(
            {key: value for key, value in buy_payload().items() if key != "identifier"}
        )
        edge = self.edge(
            sender=lambda request: sends.append(request) or {},
            request_hash=request_hash,
        )
        with self.ready_env(), self.assertRaisesRegex(
            UpbitFunctionalMutationNotSent, "production-unavailable"
        ):
            edge.post(
                buy_payload(),
                functional_capability=RAW_CAPABILITY,
                functional_action="STRATEGY_BUY",
                claim_id=CLAIM_ID,
                request_hash=request_hash,
            )
        self.assertEqual([], sends)

        test_edge = self.edge(
            sender=lambda request: sends.append(request) or {},
            request_hash=request_hash,
        )
        with self.ready_env(), patch(
            "live_trader.upbit_functional_mutation."
            "UPBIT_FUNCTIONAL_MUTATION_AVAILABLE",
            True,
        ), self.assertRaisesRegex(
            UpbitFunctionalMutationNotSent, "capability-invalid"
        ):
            test_edge.post(
                buy_payload(),
                functional_capability="forged",
                functional_action="STRATEGY_BUY",
                claim_id=CLAIM_ID,
                request_hash=request_hash,
            )
        self.assertEqual([], sends)

    def test_public_mock_boolean_is_gone_and_wrapper_cannot_cross_release(self) -> None:
        self.assertNotIn(
            "allow_mock_transport",
            inspect.signature(UpbitFunctionalMutationEdge).parameters,
        )
        sender_calls = 0
        marker_calls = 0

        def real_sender(_request):
            nonlocal sender_calls
            sender_calls += 1
            raise AssertionError("closed release must not reach sender")

        def marker(claim_id, _request_hash):
            nonlocal marker_calls
            marker_calls += 1
            return {
                "claimId": claim_id,
                "state": "POST_MAY_HAVE_CROSSED",
            }

        request_hash = _stable_hash(
            {
                key: value
                for key, value in buy_payload().items()
                if key != "identifier"
            }
        )
        edge = self.edge(
            # This is the former hostile shape: the public edge sees a lambda,
            # while the lambda would delegate to a real raw transport sender.
            sender=lambda request: real_sender(request),
            request_hash=request_hash,
            post_boundary_marker=marker,
        )
        with patch(
            "live_trader.upbit_functional_mutation."
            "build_upbit_functional_authorization",
            side_effect=AssertionError("release closes before signing"),
        ), self.assertRaisesRegex(
            UpbitFunctionalMutationNotSent, "production-unavailable"
        ):
            edge.post(
                buy_payload(),
                functional_capability=RAW_CAPABILITY,
                functional_action="STRATEGY_BUY",
                claim_id=CLAIM_ID,
                request_hash=request_hash,
            )
        self.assertEqual(0, sender_calls)
        self.assertEqual(0, marker_calls)

    def test_required_durable_owner_lease_blocks_before_sender(self) -> None:
        sends: list[object] = []
        request_hash = _stable_hash(
            {
                key: value
                for key, value in buy_payload().items()
                if key != "identifier"
            }
        )
        edge = self.edge(
            sender=lambda request: sends.append(request) or {},
            request_hash=request_hash,
            authority_reader=lambda: authority(
                durableOwnerLeaseRequired=True,
                durableOwnerLeaseActive=False,
            ),
        )
        with self.ready_env(), patch(
            "live_trader.upbit_functional_mutation."
            "UPBIT_FUNCTIONAL_MUTATION_AVAILABLE",
            True,
        ), self.assertRaisesRegex(
            UpbitFunctionalMutationNotSent, "durable-owner-lease-inactive"
        ):
            edge.post(
                buy_payload(),
                functional_capability=RAW_CAPABILITY,
                functional_action="STRATEGY_BUY",
                claim_id=CLAIM_ID,
                request_hash=request_hash,
            )
        self.assertEqual([], sends)

    def test_fully_authorized_test_edge_sends_once_and_timeout_is_unknown(self) -> None:
        sends = 0

        def accepted(request: object) -> dict[str, object]:
            nonlocal sends
            sends += 1
            body = request.body  # type: ignore[attr-defined]
            return {
                "ok": True,
                "statusCode": 201,
                "json": {
                    "uuid": "broker-order-uuid-0001",
                    "identifier": body["identifier"],
                    "market": body["market"],
                    "side": body["side"],
                    "state": "done",
                },
            }

        request_hash = _stable_hash(
            {key: value for key, value in buy_payload().items() if key != "identifier"}
        )
        edge = self.edge(
            sender=accepted,
            request_hash=request_hash,
        )
        with self.ready_env(), patch(
            "live_trader.upbit_functional_mutation."
            "UPBIT_FUNCTIONAL_MUTATION_AVAILABLE",
            True,
        ):
            receipt = edge.post(
                buy_payload(),
                functional_capability=RAW_CAPABILITY,
                functional_action="STRATEGY_BUY",
                claim_id=CLAIM_ID,
                request_hash=request_hash,
            )
        self.assertEqual("broker-order-uuid-0001", receipt["uuid"])
        self.assertEqual(1, sends)

        calls = 0

        def timeout(_request: object):
            nonlocal calls
            calls += 1
            raise TimeoutError("unknown after urlopen")

        unknown = self.edge(
            sender=timeout,
            request_hash=request_hash,
        )
        with self.ready_env(), patch(
            "live_trader.upbit_functional_mutation."
            "UPBIT_FUNCTIONAL_MUTATION_AVAILABLE",
            True,
        ), self.assertRaises(
            UpbitFunctionalMutationOutcomeUnknown
        ):
            unknown.post(
                buy_payload(),
                functional_capability=RAW_CAPABILITY,
                functional_action="STRATEGY_BUY",
                claim_id=CLAIM_ID,
                request_hash=request_hash,
            )
        self.assertEqual(1, calls)

    def test_global_first_live_fence_rechecks_at_final_sender_edge(self) -> None:
        sends: list[object] = []
        request_hash = _stable_hash(
            {
                key: value
                for key, value in buy_payload().items()
                if key != "identifier"
            }
        )

        def closed_fence(request):
            body = {
                "schemaVersion": (
                    "upbit-global-first-live-dispatch-authority/v1"
                ),
                "scope": "CRYPTO_FIRST_LIVE_GLOBAL",
                "lane": "UPBIT",
                "phase": "ACTIVE",
                "runId": SESSION,
                "sessionId": SESSION,
                "permitId": PERMIT_ID,
                "permitHash": PERMIT_HASH,
                "accountFingerprint": ACCOUNT,
                "routeScopeHash": ROUTE_SCOPE_HASH,
                "ownerIdentityHash": OWNER,
                "ownerLeaseActive": True,
                "entryAuthorityOpen": False,
                "cleanupAuthorityOpen": False,
                "hardStopEpoch": (NOW + timedelta(hours=2)).timestamp(),
                "ownerLeaseExpiresEpoch": NOW.timestamp() + 30,
                "revision": 9,
                "observedEpoch": NOW.timestamp(),
                "killSwitch": False,
                "stopRequested": False,
            }
            return {**body, "authorityHash": _stable_hash(body)}

        edge = self.edge(
            sender=lambda request: sends.append(request) or {},
            request_hash=request_hash,
            global_reader=closed_fence,
        )
        with self.ready_env(), patch(
            "live_trader.upbit_functional_mutation."
            "UPBIT_FUNCTIONAL_MUTATION_AVAILABLE",
            True,
        ), self.assertRaisesRegex(
            UpbitFunctionalMutationNotSent,
            "global-first-live-dispatch-fence-closed",
        ):
            edge.post(
                buy_payload(),
                functional_capability=RAW_CAPABILITY,
                functional_action="STRATEGY_BUY",
                claim_id=CLAIM_ID,
                request_hash=request_hash,
            )
        self.assertEqual([], sends)

    def test_production_global_fence_requires_route_reserver(self) -> None:
        request_hash = _stable_hash(
            {
                key: value
                for key, value in buy_payload().items()
                if key != "identifier"
            }
        )
        with self.assertRaisesRegex(
            ValueError, "dispatch-reserver-required"
        ):
            self.edge(
                sender=lambda _request: {},
                request_hash=request_hash,
                global_reader=lambda _request: global_authority(),
                global_reserver=False,
            )

    def test_route_reservation_covers_marker_and_sender(self) -> None:
        events: list[str] = []
        request_hash = _stable_hash(
            {
                key: value
                for key, value in buy_payload().items()
                if key != "identifier"
            }
        )

        @contextmanager
        def reserve(request):
            self.assertEqual(
                {
                    "schemaVersion",
                    "scope",
                    "lane",
                    "action",
                    "cleanup",
                    "runId",
                    "sessionId",
                    "permitId",
                    "permitHash",
                    "accountFingerprint",
                    "routeScopeHash",
                    "ownerIdentityHash",
                    "claimId",
                    "requestHash",
                },
                set(request),
            )
            self.assertEqual("STRATEGY_BUY", request["action"])
            self.assertFalse(request["cleanup"])
            self.assertEqual(CLAIM_ID, request["claimId"])
            self.assertEqual(request_hash, request["requestHash"])
            events.append("reservation-enter")
            try:
                yield global_authority()
            finally:
                events.append("reservation-exit")

        def marker(claim_id, _request_hash):
            events.append("marker")
            return {
                "claimId": claim_id,
                "state": "POST_MAY_HAVE_CROSSED",
            }

        def sender(request):
            events.append("sender")
            return {
                "ok": True,
                "json": {
                    "uuid": "broker-order-uuid-reserved-0001",
                    "identifier": request.body["identifier"],
                    "market": request.body["market"],
                    "side": request.body["side"],
                },
            }

        edge = self.edge(
            sender=sender,
            request_hash=request_hash,
            global_reader=lambda _request: global_authority(),
            global_reserver=reserve,
            post_boundary_marker=marker,
        )
        with self.ready_env(), patch(
            "live_trader.upbit_functional_mutation."
            "UPBIT_FUNCTIONAL_MUTATION_AVAILABLE",
            True,
        ):
            result = edge.post(
                buy_payload(),
                functional_capability=RAW_CAPABILITY,
                functional_action="STRATEGY_BUY",
                claim_id=CLAIM_ID,
                request_hash=request_hash,
            )
        self.assertEqual("broker-order-uuid-reserved-0001", result["uuid"])
        self.assertEqual(
            ["reservation-enter", "marker", "sender", "reservation-exit"],
            events,
        )

    def test_invalid_reserved_snapshot_blocks_before_marker(self) -> None:
        events: list[str] = []
        request_hash = _stable_hash(
            {
                key: value
                for key, value in buy_payload().items()
                if key != "identifier"
            }
        )

        @contextmanager
        def reserve(_request):
            events.append("reservation-enter")
            try:
                yield global_authority(ownerLeaseActive=False)
            finally:
                events.append("reservation-exit")

        edge = self.edge(
            sender=lambda _request: events.append("sender") or {},
            request_hash=request_hash,
            global_reader=lambda _request: global_authority(),
            global_reserver=reserve,
            post_boundary_marker=lambda claim_id, _request_hash: (
                events.append("marker")
                or {
                    "claimId": claim_id,
                    "state": "POST_MAY_HAVE_CROSSED",
                }
            ),
        )
        with self.ready_env(), patch(
            "live_trader.upbit_functional_mutation."
            "UPBIT_FUNCTIONAL_MUTATION_AVAILABLE",
            True,
        ), self.assertRaisesRegex(
            UpbitFunctionalMutationNotSent, "dispatch-reservation-closed"
        ):
            edge.post(
                buy_payload(),
                functional_capability=RAW_CAPABILITY,
                functional_action="STRATEGY_BUY",
                claim_id=CLAIM_ID,
                request_hash=request_hash,
            )
        self.assertEqual(
            ["reservation-enter", "reservation-exit"], events
        )

    def test_nondurable_marker_blocks_before_sender_inside_reservation(self) -> None:
        events: list[str] = []
        request_hash = _stable_hash(
            {
                key: value
                for key, value in buy_payload().items()
                if key != "identifier"
            }
        )

        @contextmanager
        def reserve(_request):
            events.append("reservation-enter")
            try:
                yield global_authority()
            finally:
                events.append("reservation-exit")

        edge = self.edge(
            sender=lambda _request: events.append("sender") or {},
            request_hash=request_hash,
            global_reserver=reserve,
            post_boundary_marker=lambda claim_id, _request_hash: (
                events.append("marker")
                or {
                    "claimId": claim_id,
                    "state": "POST_MAY_HAVE_CROSSED",
                }
            ),
            marker_is_durable=False,
        )
        with self.ready_env(), patch(
            "live_trader.upbit_functional_mutation."
            "UPBIT_FUNCTIONAL_MUTATION_AVAILABLE",
            True,
        ), self.assertRaisesRegex(
            UpbitFunctionalMutationNotSent,
            "post-boundary-durable-claim_state-mismatch",
        ):
            edge.post(
                buy_payload(),
                functional_capability=RAW_CAPABILITY,
                functional_action="STRATEGY_BUY",
                claim_id=CLAIM_ID,
                request_hash=request_hash,
            )
        self.assertEqual(
            ["reservation-enter", "marker", "reservation-exit"],
            events,
        )


if __name__ == "__main__":
    unittest.main()
