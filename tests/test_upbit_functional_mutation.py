from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
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
IDENTIFIER = "uft-" + "a" * 28
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


class UpbitFunctionalMutationTest(unittest.TestCase):
    @staticmethod
    def edge(
        *,
        sender,
        request_hash: str,
        allow_mock_transport: bool = False,
        authority_reader=authority,
        action: str = "STRATEGY_BUY",
        session_state: str = "ACTIVE",
        global_reader=None,
    ) -> UpbitFunctionalMutationEdge:
        scope = SimpleNamespace(
            permit_id=PERMIT_ID,
            permit_hash=PERMIT_HASH,
            route_scope_hash=ROUTE_SCOPE_HASH,
            account_fingerprint=ACCOUNT,
            ends_at=NOW + timedelta(hours=2),
        )
        return UpbitFunctionalMutationEdge(
            session_id=SESSION,
            account_fingerprint=ACCOUNT,
            permit_id=PERMIT_ID,
            permit_hash=PERMIT_HASH,
            route_scope_hash=ROUTE_SCOPE_HASH,
            session_scope_hash=SESSION_SCOPE_HASH,
            authority_reader=authority_reader,
            claim_reader=lambda claim_id: {
                "session_id": SESSION,
                "permit_id": PERMIT_ID,
                "permit_hash": PERMIT_HASH,
                "scope_hash": SESSION_SCOPE_HASH,
                "session_state": session_state,
                "capability_hash": CAPABILITY_HASH,
                "claim_id": claim_id,
                "slot": action,
                "side": "BID",
                "identifier": IDENTIFIER,
                "target_identifier": "",
                "request_hash": request_hash,
                "claim_state": "CLAIMED_PRE_POST",
            },
            post_boundary_marker=lambda claim_id, _request_hash: {
                "claimId": claim_id,
                "state": "POST_MAY_HAVE_CROSSED",
            },
            global_first_live_authority_reader=global_reader,
            global_first_live_owner_identity_hash=(
                OWNER if global_reader is not None else ""
            ),
            global_first_live_scope=(
                scope if global_reader is not None else None
            ),
            clock=(lambda: NOW) if global_reader is not None else None,
            sender=sender,
            allow_mock_transport=allow_mock_transport,
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

        mock = self.edge(
            sender=lambda request: sends.append(request) or {},
            request_hash=request_hash,
            allow_mock_transport=True,
        )
        with self.ready_env(), self.assertRaisesRegex(
            UpbitFunctionalMutationNotSent, "capability-invalid"
        ):
            mock.post(
                buy_payload(),
                functional_capability="forged",
                functional_action="STRATEGY_BUY",
                claim_id=CLAIM_ID,
                request_hash=request_hash,
            )
        self.assertEqual([], sends)

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
            allow_mock_transport=True,
            authority_reader=lambda: authority(
                durableOwnerLeaseRequired=True,
                durableOwnerLeaseActive=False,
            ),
        )
        with self.ready_env(), self.assertRaisesRegex(
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

    def test_mock_edge_sends_once_and_timeout_is_outcome_unknown(self) -> None:
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
            allow_mock_transport=True,
        )
        with self.ready_env():
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
            allow_mock_transport=True,
        )
        with self.ready_env(), self.assertRaises(
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
            allow_mock_transport=True,
            global_reader=closed_fence,
        )
        with self.ready_env(), self.assertRaisesRegex(
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


if __name__ == "__main__":
    unittest.main()
