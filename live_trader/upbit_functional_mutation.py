from __future__ import annotations

"""No-retry mutation edge for the isolated KRW-BTC functional lane.

The ordinary Upbit router is never called.  Global real orders must remain
off, while an exact functional authority snapshot and the service's raw
session capability are revalidated immediately before the single HTTP call.
"""

from contextlib import AbstractContextManager
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import hashlib
import os
import re
import secrets
from typing import Any, Callable, Mapping, Sequence
import urllib.parse

from .live_adapters import PreparedRequest, UPBIT_BASE_URL, env_value, send_prepared_request
from .upbit_continuous_functional import (
    EVIDENCE_CLASS,
    EXECUTION_PURPOSE,
    EXECUTION_ROUTE,
    GLOBAL_FIRST_LIVE_AUTHORITY_REQUEST_SCHEMA_VERSION,
    GLOBAL_FIRST_LIVE_SCOPE,
    GlobalFirstLiveAuthorityReader,
    SYMBOL,
    UpbitBrokerPostNotSent,
    UpbitFunctionalBlocked,
    UpbitFunctionalError,
    UpbitPermitScope,
    _stable_hash,
    upbit_functional_session_identifier_prefix,
    verify_upbit_global_first_live_authority,
)
from .upbit_functional_transport import (
    build_upbit_functional_authorization,
    resolve_upbit_functional_base_url,
)


UPBIT_FUNCTIONAL_MUTATION_AVAILABLE = False
UPBIT_ORDER_ENDPOINT = "/v1/orders"
UPBIT_CANCEL_ENDPOINT = "/v1/order"
_IDENTIFIER_RE = re.compile(r"^uft-[0-9a-f]{8}-[0-9a-f]{19}$")
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_BUY_FIELDS = frozenset({"market", "side", "ord_type", "price", "identifier"})
_SELL_FIELDS = frozenset({"market", "side", "ord_type", "volume", "identifier"})

GlobalFirstLiveDispatchReserver = Callable[
    [Mapping[str, Any]], AbstractContextManager[Mapping[str, Any]]
]


class UpbitFunctionalMutationNotSent(UpbitBrokerPostNotSent):
    pass


class UpbitGlobalFirstLiveFenceNotSent(UpbitFunctionalMutationNotSent):
    """The shared owner vanished after core preflight but before transport."""

    requires_cleanup = True


class UpbitFunctionalMutationOutcomeUnknown(UpbitFunctionalError):
    pass


def _assert_compile_release_gate() -> None:
    """Keep every mutation path closed unless the frozen release latch is open."""

    if UPBIT_FUNCTIONAL_MUTATION_AVAILABLE is not True:
        raise UpbitFunctionalMutationNotSent(
            "upbit-functional-mutation-production-unavailable"
        )


def _text(value: object) -> str:
    return str(value or "").strip()


def _decimal(value: object, label: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise UpbitFunctionalMutationNotSent(f"{label}-invalid") from exc
    if not parsed.is_finite() or parsed <= 0:
        raise UpbitFunctionalMutationNotSent(f"{label}-invalid")
    return parsed


def _decimal_text(value: Decimal) -> str:
    result = format(value.normalize(), "f")
    return "0" if result in {"", "-0"} else result


def _global_real_orders_are_off() -> bool:
    return _text(os.getenv("LIVE_TRADER_ENABLE_REAL_ORDERS")).lower() not in {
        "1",
        "true",
        "yes",
        "on",
    }


@dataclass(frozen=True)
class UpbitFunctionalMutationRequest(PreparedRequest):
    def preview(self) -> dict[str, object]:
        return {
            "provider": self.provider,
            "method": self.method,
            "url": self.url,
            "endpoint": self.endpoint,
            "headers": self.safe_headers,
            "body": dict(self.body or {}),
            "query": dict(self.query or {}),
            "blocked_reasons": list(self.blocked_reasons),
            "can_send": self.can_send,
        }


def _credentials_and_gate() -> tuple[str, str, list[str]]:
    access_key = env_value("UPBIT_ACCESS_KEY")
    secret_key = env_value("UPBIT_SECRET_KEY")
    blocked: list[str] = []
    if not access_key:
        blocked.append("UPBIT_ACCESS_KEY")
    if not secret_key:
        blocked.append("UPBIT_SECRET_KEY")
    if _text(os.getenv("UPBIT_FUNCTIONAL_LIVE_ENABLED")).lower() not in {
        "1",
        "true",
        "yes",
        "on",
    }:
        blocked.append("UPBIT_FUNCTIONAL_LIVE_ENABLED")
    if not _global_real_orders_are_off():
        blocked.append("LIVE_TRADER_ENABLE_REAL_ORDERS_MUST_REMAIN_FALSE")
    return access_key, secret_key, blocked


def _validate_order(payload: Mapping[str, str]) -> dict[str, str]:
    body = {_text(key): _text(value) for key, value in payload.items()}
    side = body.get("side", "").lower()
    expected_fields = _BUY_FIELDS if side == "bid" else _SELL_FIELDS
    if frozenset(body) != expected_fields:
        raise UpbitFunctionalMutationNotSent("upbit-mutation-fields-not-exact")
    if body.get("market", "").upper() != SYMBOL:
        raise UpbitFunctionalMutationNotSent("upbit-mutation-market-mismatch")
    if _IDENTIFIER_RE.fullmatch(body.get("identifier", "")) is None:
        raise UpbitFunctionalMutationNotSent("upbit-mutation-identifier-invalid")
    if side == "bid":
        if body.get("ord_type", "").lower() != "price":
            raise UpbitFunctionalMutationNotSent("upbit-mutation-buy-type-invalid")
        price = _decimal(body.get("price"), "upbit-mutation-buy-price")
        if price < Decimal("5000") or price > Decimal("10000"):
            raise UpbitFunctionalMutationNotSent("upbit-mutation-buy-cap-invalid")
        if price != price.to_integral_value():
            raise UpbitFunctionalMutationNotSent("upbit-mutation-buy-whole-krw-required")
        body["price"] = _decimal_text(price)
    elif side == "ask":
        if body.get("ord_type", "").lower() != "market":
            raise UpbitFunctionalMutationNotSent("upbit-mutation-sell-type-invalid")
        volume = _decimal(body.get("volume"), "upbit-mutation-sell-volume")
        if max(0, -volume.as_tuple().exponent) > 8:
            raise UpbitFunctionalMutationNotSent("upbit-mutation-sell-precision-invalid")
        body["volume"] = _decimal_text(volume)
    else:
        raise UpbitFunctionalMutationNotSent("upbit-mutation-side-invalid")
    body["market"] = SYMBOL
    body["side"] = side
    body["ord_type"] = body["ord_type"].lower()
    return body


def build_upbit_functional_order_request(
    payload: Mapping[str, str],
    *,
    allow_mock_origin: bool = False,
) -> UpbitFunctionalMutationRequest:
    body = _validate_order(payload)
    try:
        base_url = resolve_upbit_functional_base_url(
            allow_mock_origin=allow_mock_origin
        )
    except Exception as exc:
        raise UpbitFunctionalMutationNotSent(
            "upbit-mutation-api-origin-not-official"
        ) from exc
    access_key, secret_key, blocked = _credentials_and_gate()
    ordered: Sequence[tuple[str, str]] = tuple(body.items())
    authorization = build_upbit_functional_authorization(
        access_key, secret_key, ordered
    )
    return UpbitFunctionalMutationRequest(
        provider="upbit-functional-mutation",
        method="POST",
        url=f"{base_url}{UPBIT_ORDER_ENDPOINT}",
        endpoint=UPBIT_ORDER_ENDPOINT,
        headers={
            "Authorization": authorization,
            "Content-Type": "application/json",
        },
        safe_headers={
            "authorization_configured": bool(authorization),
            "content-type": "application/json",
        },
        body=body,
        query=None,
        blocked_reasons=blocked,
    )


def build_upbit_functional_cancel_request(
    identifier: str,
    *,
    allow_mock_origin: bool = False,
) -> UpbitFunctionalMutationRequest:
    normalized = _text(identifier)
    if _IDENTIFIER_RE.fullmatch(normalized) is None:
        raise UpbitFunctionalMutationNotSent("upbit-cancel-identifier-invalid")
    try:
        base_url = resolve_upbit_functional_base_url(
            allow_mock_origin=allow_mock_origin
        )
    except Exception as exc:
        raise UpbitFunctionalMutationNotSent(
            "upbit-mutation-api-origin-not-official"
        ) from exc
    access_key, secret_key, blocked = _credentials_and_gate()
    ordered = (("identifier", normalized),)
    authorization = build_upbit_functional_authorization(
        access_key, secret_key, ordered
    )
    encoded = urllib.parse.urlencode(ordered)
    return UpbitFunctionalMutationRequest(
        provider="upbit-functional-mutation",
        method="DELETE",
        url=(
            f"{base_url}{UPBIT_CANCEL_ENDPOINT}?{encoded}"
        ),
        endpoint=UPBIT_CANCEL_ENDPOINT,
        headers={"Authorization": authorization},
        safe_headers={"authorization_configured": bool(authorization)},
        body=None,
        query={"identifier": normalized},
        blocked_reasons=blocked,
    )


class UpbitFunctionalMutationEdge:
    """Final single-send edge, bound to exact session capability and authority."""

    def __init__(
        self,
        *,
        session_id: str,
        account_fingerprint: str,
        permit_id: str,
        permit_hash: str,
        route_scope_hash: str,
        session_scope_hash: str,
        authority_reader: Callable[[], Mapping[str, Any]],
        claim_reader: Callable[[str], Mapping[str, Any]],
        post_boundary_marker: Callable[[str, str], Mapping[str, Any]],
        global_first_live_authority_reader: (
            GlobalFirstLiveAuthorityReader | None
        ) = None,
        global_first_live_dispatch_reserver: (
            GlobalFirstLiveDispatchReserver | None
        ) = None,
        global_first_live_owner_identity_hash: str = "",
        global_first_live_scope: UpbitPermitScope | None = None,
        clock: Callable[[], Any] | None = None,
        sender: Callable[[PreparedRequest], Mapping[str, Any]] = send_prepared_request,
    ) -> None:
        for callback, label in (
            (authority_reader, "final-authority-reader"),
            (claim_reader, "durable-claim-reader"),
            (post_boundary_marker, "durable-post-boundary-marker"),
            (
                global_first_live_authority_reader,
                "global-first-live-authority-reader",
            ),
            (
                global_first_live_dispatch_reserver,
                "global-first-live-dispatch-reserver",
            ),
            (clock, "clock"),
            (sender, "sender"),
        ):
            if not callable(callback):
                raise ValueError(f"upbit-mutation-{label}-required")
        self.session_id = _text(session_id)
        self.account_fingerprint = _text(account_fingerprint).lower()
        self.permit_id = _text(permit_id)
        self.permit_hash = _text(permit_hash).lower()
        self.route_scope_hash = _text(route_scope_hash).lower()
        self.session_scope_hash = _text(session_scope_hash).lower()
        for value, label in (
            (self.permit_hash, "permit-hash"),
            (self.route_scope_hash, "route-scope-hash"),
            (self.session_scope_hash, "session-scope-hash"),
        ):
            if _HASH_RE.fullmatch(value) is None:
                raise ValueError(f"upbit-mutation-{label}-invalid")
        if not self.permit_id:
            raise ValueError("upbit-mutation-permit-id-invalid")
        self.authority_reader = authority_reader
        self.claim_reader = claim_reader
        self.post_boundary_marker = post_boundary_marker
        self.global_first_live_authority_reader = (
            global_first_live_authority_reader
        )
        self.global_first_live_dispatch_reserver = (
            global_first_live_dispatch_reserver
        )
        self.global_first_live_owner_identity_hash = _text(
            global_first_live_owner_identity_hash
        ).lower()
        self.global_first_live_scope = global_first_live_scope
        self.clock = clock
        if (
            global_first_live_scope is None
            or global_first_live_scope.permit_id != self.permit_id
            or global_first_live_scope.permit_hash != self.permit_hash
            or global_first_live_scope.route_scope_hash
            != self.route_scope_hash
            or global_first_live_scope.account_fingerprint
            != self.account_fingerprint
            or _HASH_RE.fullmatch(
                self.global_first_live_owner_identity_hash
            )
            is None
        ):
            raise ValueError(
                "upbit-mutation-global-first-live-wiring-invalid"
            )
        self.sender = sender

    def _global_first_live_request(
        self,
        *,
        action: str,
        claim_id: str,
        request_hash: str,
    ) -> dict[str, Any]:
        """Build the exact request accepted by the shared route reservation."""

        normalized_action = _text(action).upper()
        return {
            "schemaVersion": (
                GLOBAL_FIRST_LIVE_AUTHORITY_REQUEST_SCHEMA_VERSION
            ),
            "scope": GLOBAL_FIRST_LIVE_SCOPE,
            "lane": "UPBIT",
            "action": normalized_action,
            "cleanup": normalized_action
            in {"CLEANUP_SELL", "CLEANUP_CANCEL"},
            "runId": self.session_id,
            "sessionId": self.session_id,
            "permitId": self.permit_id,
            "permitHash": self.permit_hash,
            "accountFingerprint": self.account_fingerprint,
            "routeScopeHash": self.route_scope_hash,
            "ownerIdentityHash": self.global_first_live_owner_identity_hash,
            "claimId": _text(claim_id),
            "requestHash": _text(request_hash).lower(),
        }

    def _verify_reserved_global_snapshot(
        self,
        *,
        request: Mapping[str, Any],
        snapshot: Mapping[str, Any],
        action: str,
        claim_id: str,
        request_hash: str,
    ) -> None:
        expected_request = dict(request)

        def reserved_reader(actual: Mapping[str, Any]) -> Mapping[str, Any]:
            if dict(actual) != expected_request:
                raise UpbitFunctionalBlocked(
                    "upbit-global-first-live-reservation-request-changed"
                )
            return dict(snapshot)

        verify_upbit_global_first_live_authority(
            reserved_reader,
            scope=self.global_first_live_scope,
            session_id=self.session_id,
            owner_identity_hash=self.global_first_live_owner_identity_hash,
            action=action,
            cleanup=action in {"CLEANUP_SELL", "CLEANUP_CANCEL"},
            now=self.clock(),
            claim_id=claim_id,
            request_hash=request_hash,
        )

    def _assert_authority(
        self,
        raw_capability: str,
        *,
        functional_action: str,
        claim_id: str,
        request_hash: str,
        sealed_payload: Mapping[str, str],
        method: str,
    ) -> None:
        authority = dict(self.authority_reader())
        exact = {
            "executionPurpose": EXECUTION_PURPOSE,
            "executionRoute": EXECUTION_ROUTE,
            "functionalTestSessionId": self.session_id,
            "functionalTestPermitId": self.permit_id,
            "functionalTestPermitHash": self.permit_hash,
            "functionalTestRouteScopeHash": self.route_scope_hash,
            "functionalTestSessionScopeHash": self.session_scope_hash,
            "functionalTestAccountFingerprint": self.account_fingerprint,
        }
        for field, expected in exact.items():
            if not secrets.compare_digest(_text(authority.get(field)), expected):
                raise UpbitFunctionalMutationNotSent(
                    f"upbit-mutation-authority-{field}-mismatch"
                )
        capability_hash = _text(authority.get("functionalCapabilityHash")).lower()
        actual_hash = hashlib.sha256(_text(raw_capability).encode("utf-8")).hexdigest()
        if (
            _HASH_RE.fullmatch(capability_hash) is None
            or not secrets.compare_digest(actual_hash, capability_hash)
        ):
            raise UpbitFunctionalMutationNotSent(
                "upbit-mutation-functional-capability-invalid"
            )
        if any(
            authority.get(field) is not True
            for field in (
                "functionalMutationEnabled",
                "functionalOnlyRouting",
                "ordinaryRoutesClosed",
                "upbitSmokeRouteClosed",
                "newEntriesBlocked",
            )
        ):
            raise UpbitFunctionalMutationNotSent(
                "upbit-mutation-functional-only-authority-incomplete"
            )
        if (
            authority.get("durableOwnerLeaseRequired") is True
            and authority.get("durableOwnerLeaseActive") is not True
        ):
            raise UpbitFunctionalMutationNotSent(
                "upbit-mutation-durable-owner-lease-inactive"
            )
        if not _global_real_orders_are_off():
            raise UpbitFunctionalMutationNotSent(
                "upbit-mutation-global-real-orders-must-stay-off"
            )
        try:
            durable = dict(self.claim_reader(_text(claim_id)))
        except Exception as exc:
            raise UpbitFunctionalMutationNotSent(
                "upbit-mutation-durable-authority-unavailable"
            ) from exc
        durable_exact = {
            "session_id": self.session_id,
            "permit_id": self.permit_id,
            "permit_hash": self.permit_hash,
            "scope_hash": self.session_scope_hash,
            "claim_id": _text(claim_id),
            "slot": _text(functional_action),
            "request_hash": _text(request_hash).lower(),
            "claim_state": "CLAIMED_PRE_POST",
            "capability_hash": capability_hash,
        }
        for field, expected in durable_exact.items():
            if not secrets.compare_digest(
                _text(durable.get(field)), expected
            ):
                raise UpbitFunctionalMutationNotSent(
                    f"upbit-mutation-durable-{field}-mismatch"
                )
        request_payload = dict(sealed_payload)
        if _text(functional_action) != "CLEANUP_CANCEL":
            request_payload.pop("identifier", None)
        actual_request_hash = _stable_hash(request_payload)
        if (
            _HASH_RE.fullmatch(_text(request_hash).lower()) is None
            or not secrets.compare_digest(
                actual_request_hash, _text(request_hash).lower()
            )
        ):
            raise UpbitFunctionalMutationNotSent(
                "upbit-mutation-request-hash-mismatch"
            )
        action = _text(functional_action)
        session_state = _text(durable.get("session_state"))
        side = _text(durable.get("side")).upper()
        if action == "STRATEGY_BUY":
            allowed = (
                session_state == "ACTIVE"
                and side == "BID"
                and method == "POST"
                and authority.get("killSwitch") is not True
                and authority.get("cleanupOnly") is not True
            )
        elif action == "STRATEGY_SELL":
            allowed = (
                session_state == "ACTIVE"
                and side == "ASK"
                and method == "POST"
                and authority.get("killSwitch") is not True
                and authority.get("cleanupOnly") is not True
            )
        elif action == "CLEANUP_SELL":
            allowed = (
                session_state == "CLEANUP"
                and side == "ASK"
                and method == "POST"
            )
        elif action == "CLEANUP_CANCEL":
            allowed = (
                session_state == "CLEANUP"
                and side == "CANCEL"
                and method == "DELETE"
            )
        else:
            allowed = False
        if not allowed:
            raise UpbitFunctionalMutationNotSent(
                "upbit-mutation-action-phase-authority-invalid"
            )
        expected_identifier = (
            _text(durable.get("target_identifier"))
            if action == "CLEANUP_CANCEL"
            else _text(durable.get("identifier"))
        )
        if not secrets.compare_digest(
            _text(sealed_payload.get("identifier")), expected_identifier
        ):
            raise UpbitFunctionalMutationNotSent(
                "upbit-mutation-durable-identifier-mismatch"
            )
        if not _text(sealed_payload.get("identifier")).startswith(
            upbit_functional_session_identifier_prefix(self.session_id)
        ):
            raise UpbitFunctionalMutationNotSent(
                "upbit-mutation-identifier-session-prefix-mismatch"
            )
        try:
            verify_upbit_global_first_live_authority(
                self.global_first_live_authority_reader,
                scope=self.global_first_live_scope,
                session_id=self.session_id,
                owner_identity_hash=(
                    self.global_first_live_owner_identity_hash
                ),
                action=action,
                cleanup=action in {
                    "CLEANUP_SELL",
                    "CLEANUP_CANCEL",
                },
                now=self.clock(),
                claim_id=claim_id,
                request_hash=request_hash,
            )
        except UpbitFunctionalBlocked as exc:
            raise UpbitGlobalFirstLiveFenceNotSent(
                "upbit-mutation-global-first-live-dispatch-fence-closed"
            ) from exc

    def _assert_durable_post_boundary(
        self,
        *,
        claim_id: str,
        request_hash: str,
    ) -> None:
        """Re-read the durable claim after the marker and before transport."""

        try:
            durable = dict(self.claim_reader(_text(claim_id)))
        except Exception as exc:
            raise UpbitFunctionalMutationNotSent(
                "upbit-mutation-post-boundary-durable-read-failed"
            ) from exc
        exact = {
            "session_id": self.session_id,
            "permit_id": self.permit_id,
            "permit_hash": self.permit_hash,
            "scope_hash": self.session_scope_hash,
            "claim_id": _text(claim_id),
            "request_hash": _text(request_hash).lower(),
            "claim_state": "POST_MAY_HAVE_CROSSED",
        }
        for field, expected in exact.items():
            if not secrets.compare_digest(_text(durable.get(field)), expected):
                raise UpbitFunctionalMutationNotSent(
                    f"upbit-mutation-post-boundary-durable-{field}-mismatch"
                )

    def _send_once(
        self,
        prepared: UpbitFunctionalMutationRequest,
        *,
        raw_capability: str,
        functional_action: str,
        claim_id: str,
        request_hash: str,
        sealed_payload: Mapping[str, str],
    ) -> dict[str, Any]:
        # This check intentionally precedes authority reads, marker writes and
        # request transport.  Tests must patch the frozen constant explicitly;
        # no sender wrapper or public mock boolean can open the edge.
        _assert_compile_release_gate()
        self._assert_authority(
            raw_capability,
            functional_action=functional_action,
            claim_id=claim_id,
            request_hash=request_hash,
            sealed_payload=sealed_payload,
            method=prepared.method,
        )
        if not prepared.can_send:
            raise UpbitFunctionalMutationNotSent(
                "upbit-mutation-request-not-ready:"
                + ",".join(prepared.blocked_reasons)
            )
        def mark_and_send() -> Mapping[str, Any]:
            try:
                marker = self.post_boundary_marker(claim_id, request_hash)
            except Exception as exc:
                raise UpbitFunctionalMutationNotSent(
                    "upbit-mutation-post-boundary-marker-failed"
                ) from exc
            if (
                not isinstance(marker, Mapping)
                or _text(marker.get("claimId")) != _text(claim_id)
                or _text(marker.get("state")) != "POST_MAY_HAVE_CROSSED"
            ):
                raise UpbitFunctionalMutationNotSent(
                    "upbit-mutation-post-boundary-marker-invalid"
                )
            self._assert_durable_post_boundary(
                claim_id=claim_id,
                request_hash=request_hash,
            )
            try:
                return self.sender(prepared)
            except Exception as exc:
                raise UpbitFunctionalMutationOutcomeUnknown(
                    f"upbit-mutation-transport-raised:{type(exc).__name__}"
                ) from exc

        reserver = self.global_first_live_dispatch_reserver
        reservation_request = self._global_first_live_request(
            action=functional_action,
            claim_id=claim_id,
            request_hash=request_hash,
        )
        try:
            with reserver(dict(reservation_request)) as snapshot:
                if not isinstance(snapshot, Mapping):
                    raise UpbitFunctionalBlocked(
                        "upbit-global-first-live-reservation-snapshot-invalid"
                    )
                self._verify_reserved_global_snapshot(
                    request=reservation_request,
                    snapshot=snapshot,
                    action=functional_action,
                    claim_id=claim_id,
                    request_hash=request_hash,
                )
                # The shared canonical route lock remains held across the
                # durable ambiguity marker, its independent durable re-read,
                # and the only HTTP mutation call. STOP/Kill cannot race it.
                response = mark_and_send()
        except UpbitFunctionalMutationOutcomeUnknown:
            raise
        except UpbitFunctionalMutationNotSent:
            raise
        except UpbitFunctionalBlocked as exc:
            raise UpbitGlobalFirstLiveFenceNotSent(
                "upbit-mutation-global-first-live-dispatch-reservation-closed"
            ) from exc
        except Exception as exc:
            raise UpbitGlobalFirstLiveFenceNotSent(
                "upbit-mutation-global-first-live-dispatch-reservation-unavailable"
            ) from exc
        if not isinstance(response, Mapping) or response.get("ok") is not True:
            raise UpbitFunctionalMutationOutcomeUnknown(
                "upbit-mutation-broker-acceptance-unproven"
            )
        payload = response.get("json")
        if not isinstance(payload, Mapping):
            raise UpbitFunctionalMutationOutcomeUnknown(
                "upbit-mutation-broker-response-not-object"
            )
        return dict(payload)

    def post(
        self,
        payload: Mapping[str, str],
        *,
        functional_capability: str,
        functional_action: str,
        claim_id: str,
        request_hash: str,
    ) -> Mapping[str, Any]:
        _assert_compile_release_gate()
        prepared = build_upbit_functional_order_request(
            payload,
        )
        sealed_payload = {
            key: value for key, value in payload.items() if key != "identifier"
        }
        result = self._send_once(
            prepared,
            raw_capability=functional_capability,
            functional_action=functional_action,
            claim_id=claim_id,
            request_hash=request_hash,
            sealed_payload={**sealed_payload, "identifier": _text(payload.get("identifier"))},
        )
        if (
            not _text(result.get("uuid"))
            or _text(result.get("identifier")) != _text(payload.get("identifier"))
            or _text(result.get("market")).upper() != SYMBOL
            or _text(result.get("side")).lower() != _text(payload.get("side")).lower()
        ):
            raise UpbitFunctionalMutationOutcomeUnknown(
                "upbit-mutation-order-receipt-identity-mismatch"
            )
        return result

    def cancel(
        self,
        *,
        identifier: str,
        functional_capability: str,
        functional_action: str,
        claim_id: str,
        request_hash: str,
    ) -> Mapping[str, Any]:
        _assert_compile_release_gate()
        prepared = build_upbit_functional_cancel_request(
            identifier,
        )
        result = self._send_once(
            prepared,
            raw_capability=functional_capability,
            functional_action=functional_action,
            claim_id=claim_id,
            request_hash=request_hash,
            sealed_payload={"identifier": _text(identifier)},
        )
        if (
            not _text(result.get("uuid"))
            or _text(result.get("identifier")) != _text(identifier)
            or _text(result.get("state")).lower() not in {"cancel", "done"}
        ):
            raise UpbitFunctionalMutationOutcomeUnknown(
                "upbit-mutation-cancel-receipt-identity-mismatch"
            )
        return result


__all__ = [
    "UPBIT_FUNCTIONAL_MUTATION_AVAILABLE",
    "GlobalFirstLiveDispatchReserver",
    "UpbitFunctionalMutationEdge",
    "UpbitFunctionalMutationNotSent",
    "UpbitGlobalFirstLiveFenceNotSent",
    "UpbitFunctionalMutationOutcomeUnknown",
    "UpbitFunctionalMutationRequest",
    "build_upbit_functional_cancel_request",
    "build_upbit_functional_order_request",
]
