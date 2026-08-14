from __future__ import annotations

"""Exact, no-retry mutation edge for the Binance Spot functional lane.

The module is deliberately unreachable from the ordinary Binance broker and
smoke routes.  Production remains hard-disabled until the managed lifecycle,
durable user-stream journal, and red-team E2E are complete.  Tests may inject a
sender explicitly; no test in this module performs network I/O.
"""

from dataclasses import dataclass
from contextlib import nullcontext
from decimal import Decimal, InvalidOperation
import hashlib
import os
import json
import re
import secrets
import time
from typing import Any, Callable, Mapping
import urllib.parse

from .binance_spot_continuous_functional import EVIDENCE_CLASS
from .binance_spot_functional_exclusivity import (
    BinanceSpotExclusivityError,
    verify_global_first_live_authority,
)
from .binance_spot_functional_transport import (
    assert_binance_spot_production_origin,
    binance_api_key_fingerprint,
)
from .live_adapters import (
    BINANCE_BASE_URL,
    BINANCE_ORDER_ENDPOINT,
    PreparedRequest,
    binance_timestamp_ms,
    env_value,
    missing_env,
    send_prepared_request,
    sign_binance_query,
)


PRODUCTION_MUTATION_AVAILABLE = False
_OWNER_CLIENT_ID = re.compile(
    r"^ftb-[0-9a-f]{12}-(?:[bs]|[cf](?:[2-9]|1[0-2])?)$"
)
_CLEANUP_CANCEL_CLIENT_ID = re.compile(
    r"^ftb-[0-9a-f]{12}-c(?:[2-9]|1[0-2])?$"
)
_CLEANUP_FLATTEN_CLIENT_ID = re.compile(
    r"^ftb-[0-9a-f]{12}-f(?:[2-9]|1[0-2])?$"
)
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_TRADE_FIELDS = frozenset(
    {
        "kind",
        "product",
        "symbol",
        "side",
        "orderType",
        "quantity",
        "quoteOrderQty",
        "clientOrderId",
        "evaluationId",
        "evaluationHash",
        "officialWindowHash",
        "barCloseEpoch",
        "functionalOnly",
        "cleanupOnly",
        "evidenceClass",
    }
)
_CLEANUP_SELL_FIELDS = _TRADE_FIELDS - {
    "evaluationId",
    "evaluationHash",
    "officialWindowHash",
    "barCloseEpoch",
}
_CANCEL_FIELDS = frozenset(
    {
        "kind",
        "product",
        "symbol",
        "brokerOrderId",
        "origClientOrderId",
        "clientOrderId",
        "functionalOnly",
        "cleanupOnly",
        "evidenceClass",
    }
)


class BinanceSpotFunctionalMutationError(RuntimeError):
    pass


class BinanceSpotFunctionalMutationNotSent(BinanceSpotFunctionalMutationError):
    """Local preflight failed before the boundary marker was invoked."""


class BinanceSpotFunctionalMutationOutcomeUnknown(BinanceSpotFunctionalMutationError):
    """The marker was invoked and broker acceptance cannot be proven."""


@dataclass(frozen=True)
class FunctionalMutationRequest(PreparedRequest):
    def preview(self) -> dict[str, object]:
        parsed = urllib.parse.urlsplit(self.url)
        redacted = [
            (key, "***" if key.lower() == "signature" else value)
            for key, value in urllib.parse.parse_qsl(
                parsed.query, keep_blank_values=True
            )
        ]
        return {
            "provider": self.provider,
            "method": self.method,
            "url": urllib.parse.urlunsplit(
                (
                    parsed.scheme,
                    parsed.netloc,
                    parsed.path,
                    urllib.parse.urlencode(redacted),
                    parsed.fragment,
                )
            ),
            "endpoint": self.endpoint,
            "headers": self.safe_headers,
            "body": {},
            "query": self.query or {},
            "blocked_reasons": list(self.blocked_reasons),
            "can_send": self.can_send,
        }


def _text(value: object) -> str:
    return str(value or "").strip()


def _canonical(value: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _action_hash(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _decimal(value: object, *, label: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise BinanceSpotFunctionalMutationNotSent(
            f"{label} is not a finite decimal"
        ) from exc
    if not result.is_finite():
        raise BinanceSpotFunctionalMutationNotSent(
            f"{label} is not a finite decimal"
        )
    return result


def _global_real_orders_are_off() -> bool:
    return _text(os.getenv("LIVE_TRADER_ENABLE_REAL_ORDERS")).lower() not in {
        "1",
        "true",
        "yes",
        "on",
    }


def _validate_action(action: Mapping[str, Any]) -> tuple[str, dict[str, object]]:
    kind = _text(action.get("kind")).upper()
    expected_fields = (
        _CANCEL_FIELDS
        if kind == "CANCEL"
        else _CLEANUP_SELL_FIELDS
        if kind == "SELL" and action.get("cleanupOnly") is True
        else _TRADE_FIELDS
    )
    if set(action) != expected_fields:
        raise BinanceSpotFunctionalMutationNotSent(
            "sealed functional action fields changed"
        )
    if (
        _text(action.get("product")).upper() != "SPOT"
        or _text(action.get("symbol")).upper() != "BTCUSDT"
        or action.get("functionalOnly") is not True
        or _text(action.get("evidenceClass")) != EVIDENCE_CLASS
    ):
        raise BinanceSpotFunctionalMutationNotSent(
            "functional Spot action identity changed"
        )
    client_id = _text(action.get("clientOrderId"))
    if _OWNER_CLIENT_ID.fullmatch(client_id) is None:
        raise BinanceSpotFunctionalMutationNotSent(
            "functional client order id is invalid"
        )

    if kind == "CANCEL":
        if (
            action.get("cleanupOnly") is not True
            or _CLEANUP_CANCEL_CLIENT_ID.fullmatch(client_id) is None
        ):
            raise BinanceSpotFunctionalMutationNotSent(
                "cancel must be cleanup-only with the owned cancel id"
            )
        order_id = _text(action.get("brokerOrderId"))
        original_client_id = _text(action.get("origClientOrderId"))
        if (
            not order_id.isdigit()
            or _OWNER_CLIENT_ID.fullmatch(original_client_id) is None
            or _CLEANUP_CANCEL_CLIENT_ID.fullmatch(original_client_id) is not None
        ):
            raise BinanceSpotFunctionalMutationNotSent(
                "cancel target is not the exact owned broker/client pair"
            )
        return kind, {
            "symbol": "BTCUSDT",
            "orderId": order_id,
            "origClientOrderId": original_client_id,
            "newClientOrderId": client_id,
        }

    side = _text(action.get("side")).upper()
    order_type = _text(action.get("orderType")).upper()
    if kind not in {"BUY", "SELL"} or side != kind or order_type != "MARKET":
        raise BinanceSpotFunctionalMutationNotSent(
            "only exact BUY/SELL Spot MARKET actions are supported"
        )
    quantity = _decimal(action.get("quantity"), label="quantity")
    quote_order_qty = _decimal(
        action.get("quoteOrderQty"), label="quoteOrderQty"
    )
    query: dict[str, object] = {
        "symbol": "BTCUSDT",
        "side": kind,
        "type": "MARKET",
        "newClientOrderId": client_id,
        "newOrderRespType": "FULL",
    }
    if kind == "BUY":
        if (
            action.get("cleanupOnly") is not False
            or not client_id.endswith("-b")
            or quantity != 0
            or quote_order_qty <= 0
            or quote_order_qty > Decimal("10")
        ):
            raise BinanceSpotFunctionalMutationNotSent(
                "BUY must be one non-cleanup quoteOrderQty order capped at 10 USDT"
            )
        query["quoteOrderQty"] = format(quote_order_qty, "f")
    else:
        if (
            (
                _CLEANUP_FLATTEN_CLIENT_ID.fullmatch(client_id) is None
                if action.get("cleanupOnly") is True
                else not client_id.endswith("-s")
            )
            or quantity <= 0
            or quote_order_qty != 0
        ):
            raise BinanceSpotFunctionalMutationNotSent(
                "SELL must carry the exact positive session-owned base quantity"
            )
        query["quantity"] = format(quantity, "f")
    return kind, query


def build_binance_spot_functional_mutation_request(
    action: Mapping[str, Any],
    *,
    expected_account_fingerprint: str = "",
    allow_mock_origin: bool = False,
) -> FunctionalMutationRequest:
    kind, query = _validate_action(action)
    blocked = missing_env("BINANCE_API_KEY", "BINANCE_API_SECRET")
    if not _global_real_orders_are_off():
        blocked.append("LIVE_TRADER_ENABLE_REAL_ORDERS_MUST_REMAIN_FALSE")
    if env_value("BINANCE_SPOT_FUNCTIONAL_LIVE_ENABLED").lower() not in {
        "1",
        "true",
        "yes",
        "on",
    }:
        blocked.append("BINANCE_SPOT_FUNCTIONAL_LIVE_ENABLED")
    configured_base_url = env_value("BINANCE_BASE_URL") or BINANCE_BASE_URL
    origin_valid = True
    if allow_mock_origin:
        base_url = configured_base_url.rstrip("/")
    else:
        try:
            base_url = assert_binance_spot_production_origin(
                configured_base_url
            )
        except Exception:
            base_url = ""
            origin_valid = False
            blocked.append("BINANCE_SPOT_PRODUCTION_ORIGIN_CHANGED")
    # Never even place credentials/signature in a request object for a wrong
    # production origin.  Mock-only injected transports may opt into a custom
    # origin without changing the production contract.
    api_key = env_value("BINANCE_API_KEY") if origin_valid or allow_mock_origin else ""
    api_secret = (
        env_value("BINANCE_API_SECRET") if origin_valid or allow_mock_origin else ""
    )
    if expected_account_fingerprint:
        try:
            current_fingerprint = binance_api_key_fingerprint(api_key)
        except Exception:
            current_fingerprint = ""
        if not secrets.compare_digest(
            current_fingerprint, _text(expected_account_fingerprint).lower()
        ):
            blocked.append("BINANCE_ACCOUNT_FINGERPRINT_CHANGED")
    query.update({"recvWindow": 5000, "timestamp": binance_timestamp_ms()})
    encoded = (
        sign_binance_query(query, api_secret)
        if api_secret
        else urllib.parse.urlencode(query)
    )
    method = "DELETE" if kind == "CANCEL" else "POST"
    safe_query = {**query, "signature": "***" if api_secret else ""}
    return FunctionalMutationRequest(
        provider="binance-functional-mutation",
        method=method,
        url=(
            f"{base_url}"
            f"{BINANCE_ORDER_ENDPOINT}?{encoded}"
        ),
        endpoint=BINANCE_ORDER_ENDPOINT,
        headers={"X-MBX-APIKEY": api_key} if api_key else {},
        safe_headers={"X-MBX-APIKEY_configured": bool(api_key)},
        body=None,
        query=safe_query,
        blocked_reasons=blocked,
    )


class BinanceSpotFunctionalMutationEdge:
    """Marker-aware final transport; it never retries a mutation."""

    functional_marker_aware = True
    functional_exact_context_aware = True

    def __init__(
        self,
        *,
        authority_reader: Callable[[], Mapping[str, Any]],
        claim_reader: Callable[[str], Mapping[str, Any]],
        claim_marker: Callable[[str], None] | None = None,
        dispatch_lease_factory: Callable[..., Any] | None = None,
        global_first_live_authority_reader: (
            Callable[..., Mapping[str, Any]] | None
        ) = None,
        sender: Callable[[PreparedRequest], Mapping[str, Any]] = send_prepared_request,
        allow_mock_transport: bool = False,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if allow_mock_transport and sender is send_prepared_request:
            raise ValueError("mock transport requires an explicitly injected sender")
        self.sender = sender
        self.authority_reader = authority_reader
        self.claim_reader = claim_reader
        self.claim_marker = claim_marker
        self.dispatch_lease_factory = dispatch_lease_factory
        self.global_first_live_authority_reader = (
            global_first_live_authority_reader
        )
        self.allow_mock_transport = bool(allow_mock_transport)
        self.clock = clock

    def _assert_exact_authority(
        self,
        *,
        action: Mapping[str, Any],
        functional_capability: str,
        session_id: str,
        permit_id: str,
        permit_hash: str,
        account_fingerprint: str,
        authority_revision: str,
    ) -> None:
        current = dict(self.authority_reader())
        capability_hash = hashlib.sha256(
            _text(functional_capability).encode("utf-8")
        ).hexdigest()
        expected_prefix = (
            f"ftb-{hashlib.sha256(_text(session_id).encode()).hexdigest()[:12]}-"
        )
        client_id = _text(action.get("clientOrderId"))
        if not client_id.startswith(expected_prefix):
            raise BinanceSpotFunctionalMutationNotSent(
                "action is not owned by the exact functional session"
            )
        exact = {
            "activePermitId": _text(permit_id),
            "activePermitHash": _text(permit_hash).lower(),
            "activeSessionId": _text(session_id),
            "functionalCapabilityHash": capability_hash,
            "authorityRevision": _text(authority_revision),
        }
        for field, expected in exact.items():
            actual = _text(current.get(field))
            if field.endswith("Hash"):
                actual = actual.lower()
            if not expected or not secrets.compare_digest(actual, expected):
                raise BinanceSpotFunctionalMutationNotSent(
                    f"functional authority changed at {field}"
                )
        if _HASH_RE.fullmatch(capability_hash) is None:
            raise BinanceSpotFunctionalMutationNotSent(
                "functional capability is invalid"
            )
        if any(
            current.get(field) is not expected
            for field, expected in {
                "realOrdersEnabled": True,
                "dryRun": False,
                "newEntriesBlocked": True,
                "ordinaryLiveAllowed": False,
                "smokeAllowed": False,
                "functionalOnlyRouting": True,
            }.items()
        ):
            raise BinanceSpotFunctionalMutationNotSent(
                "functional-only authority shape is incomplete"
            )
        cleanup = action.get("cleanupOnly") is True
        if cleanup:
            if (
                current.get("cleanupOnlyAuthority") is not True
                or _text(current.get("cleanupSessionId")) != _text(session_id)
                or not secrets.compare_digest(
                    _text(current.get("cleanupCapabilityHash")).lower(),
                    capability_hash,
                )
            ):
                raise BinanceSpotFunctionalMutationNotSent(
                    "exact cleanup-only authority is missing"
                )
        elif current.get("killSwitch") is True:
            raise BinanceSpotFunctionalMutationNotSent(
                "entry mutation is blocked by cleanup authority"
            )
        if not _global_real_orders_are_off():
            raise BinanceSpotFunctionalMutationNotSent(
                "global ordinary real-orders gate must remain false"
            )
        try:
            current_fingerprint = binance_api_key_fingerprint(
                env_value("BINANCE_API_KEY")
            )
        except Exception as exc:
            raise BinanceSpotFunctionalMutationNotSent(
                "current Binance credential fingerprint is unavailable"
            ) from exc
        if not secrets.compare_digest(
            current_fingerprint, _text(account_fingerprint).lower()
        ):
            raise BinanceSpotFunctionalMutationNotSent(
                "current Binance credential fingerprint changed"
            )

    def __call__(
        self,
        action: Mapping[str, Any],
        mark_may_have_been_sent: Callable[[], None],
        *,
        claim_id: str,
        sealed_action_hash: str,
        functional_capability: str,
        session_id: str,
        permit_id: str,
        permit_hash: str,
        account_fingerprint: str,
        authority_revision: str,
    ) -> Mapping[str, Any]:
        lease_context = (
            self.dispatch_lease_factory(
                session_id=_text(session_id),
                claim_id=_text(claim_id),
                cleanup_only=action.get("cleanupOnly") is True,
                authority_revision=_text(authority_revision),
            )
            if self.dispatch_lease_factory is not None
            else nullcontext(lambda: {"active": True})
        )
        if not self.allow_mock_transport and self.dispatch_lease_factory is None:
            raise BinanceSpotFunctionalMutationNotSent(
                "production mutation edge lacks a cross-route dispatch lease"
            )
        with lease_context as lease_reader:
            if not callable(lease_reader):
                raise BinanceSpotFunctionalMutationNotSent(
                    "functional dispatch lease reader is malformed"
                )
            lease = dict(lease_reader())
            if (
                lease.get("active") is not True
                or _text(lease.get("sessionId")) not in {"", _text(session_id)}
                or _text(lease.get("claimId")) not in {"", _text(claim_id)}
                or lease.get("ordinaryRoutesClosed", True) is not True
            ):
                raise BinanceSpotFunctionalMutationNotSent(
                    "cross-route functional dispatch lease is inactive"
                )
            return self._dispatch_under_lease(
                action,
                mark_may_have_been_sent,
                claim_id=claim_id,
                sealed_action_hash=sealed_action_hash,
                functional_capability=functional_capability,
                session_id=session_id,
                permit_id=permit_id,
                permit_hash=permit_hash,
                account_fingerprint=account_fingerprint,
                authority_revision=authority_revision,
                lease_reader=lease_reader,
            )

    def _dispatch_under_lease(
        self,
        action: Mapping[str, Any],
        mark_may_have_been_sent: Callable[[], None],
        *,
        claim_id: str,
        sealed_action_hash: str,
        functional_capability: str,
        session_id: str,
        permit_id: str,
        permit_hash: str,
        account_fingerprint: str,
        authority_revision: str,
        lease_reader: Callable[[], Mapping[str, Any]],
    ) -> Mapping[str, Any]:
        claim = dict(self.claim_reader(_text(claim_id)))
        try:
            durable_action = json.loads(_text(claim.get("sealed_action_json")))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise BinanceSpotFunctionalMutationNotSent(
                "durable functional claim action is malformed"
            ) from exc
        if not isinstance(durable_action, dict):
            raise BinanceSpotFunctionalMutationNotSent(
                "durable functional claim action is malformed"
            )
        client_order_id = _text(action.get("clientOrderId"))
        cleanup_match = re.search(r"-([fc])([2-9]|1[0-2])?$", client_order_id)
        if cleanup_match is not None:
            generation = int(cleanup_match.group(2) or 1)
            base_kind = (
                "CLEANUP_SELL" if cleanup_match.group(1) == "f" else "CANCEL"
            )
            expected_kind = (
                base_kind if generation == 1 else f"{base_kind}_{generation}"
            )
        else:
            expected_kind = _text(action.get("kind")).upper()
        exact_action_hash = _action_hash(dict(action))
        if (
            _text(claim.get("claim_id")) != _text(claim_id)
            or _text(claim.get("session_id")) != _text(session_id)
            or _text(claim.get("state")).upper() != "SUBMITTING"
            or _text(claim.get("action_kind")).upper() != expected_kind
            or _text(claim.get("client_order_id"))
            != _text(action.get("clientOrderId"))
            or durable_action != dict(action)
            or _text(sealed_action_hash).lower() != exact_action_hash
        ):
            raise BinanceSpotFunctionalMutationNotSent(
                "exact durable SUBMITTING claim/action seal is absent"
            )
        self._assert_exact_authority(
            action=action,
            functional_capability=functional_capability,
            session_id=session_id,
            permit_id=permit_id,
            permit_hash=permit_hash,
            account_fingerprint=account_fingerprint,
            authority_revision=authority_revision,
        )
        global_reader = self.global_first_live_authority_reader
        if global_reader is None and not self.allow_mock_transport:
            raise BinanceSpotFunctionalMutationNotSent(
                "production mutation edge lacks global first-live authority"
            )
        if global_reader is not None:
            cleanup_only = action.get("cleanupOnly") is True
            try:
                global_snapshot = global_reader(
                    purpose="MUTATION_FINAL_PRE_MARKER",
                    session_id=_text(session_id),
                    permit_id=_text(permit_id),
                    permit_hash=_text(permit_hash).lower(),
                    account_fingerprint=_text(account_fingerprint).lower(),
                    cleanup_only=cleanup_only,
                )
                verify_global_first_live_authority(
                    global_snapshot,
                    purpose="MUTATION_FINAL_PRE_MARKER",
                    session_id=session_id,
                    permit_id=permit_id,
                    permit_hash=permit_hash,
                    account_fingerprint=account_fingerprint,
                    cleanup_only=cleanup_only,
                    now_epoch=float(self.clock()),
                )
            except BinanceSpotExclusivityError as exc:
                raise BinanceSpotFunctionalMutationNotSent(str(exc)) from exc
            except Exception as exc:
                raise BinanceSpotFunctionalMutationNotSent(
                    "global first-live authority reader failed closed"
                ) from exc
        prepared = build_binance_spot_functional_mutation_request(
            action,
            expected_account_fingerprint=account_fingerprint,
            allow_mock_origin=self.allow_mock_transport,
        )
        if not prepared.can_send:
            raise BinanceSpotFunctionalMutationNotSent(
                "mutation request is not credential/gate ready: "
                + ",".join(prepared.blocked_reasons)
            )
        if not self.allow_mock_transport and not PRODUCTION_MUTATION_AVAILABLE:
            raise BinanceSpotFunctionalMutationNotSent(
                "Binance functional mutation production edge is unavailable"
            )
        # Contract: every local validation precedes this marker.  The sender is
        # invoked exactly once after it, and no timestamp retry exists here.
        if self.claim_marker is not None:
            self.claim_marker(_text(claim_id))
        elif self.allow_mock_transport:
            mark_may_have_been_sent()
        else:
            raise BinanceSpotFunctionalMutationNotSent(
                "production mutation edge has no backend-owned durable marker CAS"
            )
        marked = dict(self.claim_reader(_text(claim_id)))
        if (
            _text(marked.get("claim_id")) != _text(claim_id)
            or _text(marked.get("session_id")) != _text(session_id)
            or _text(marked.get("state")).upper()
            != "POST_MAY_HAVE_CROSSED"
            or _text(marked.get("sealed_action_json"))
            != _text(claim.get("sealed_action_json"))
        ):
            raise BinanceSpotFunctionalMutationNotSent(
                "durable may-have-crossed marker CAS is absent"
            )
        final_lease = dict(lease_reader())
        if (
            final_lease.get("active") is not True
            or _text(final_lease.get("sessionId"))
            not in {"", _text(session_id)}
            or _text(final_lease.get("claimId"))
            not in {"", _text(claim_id)}
            or final_lease.get("ordinaryRoutesClosed", True) is not True
        ):
            raise BinanceSpotFunctionalMutationOutcomeUnknown(
                "dispatch lease changed after durable boundary marker"
            )
        try:
            response = self.sender(prepared)
        except Exception as exc:
            raise BinanceSpotFunctionalMutationOutcomeUnknown(
                f"transport raised after boundary marker: {type(exc).__name__}"
            ) from exc
        if not isinstance(response, Mapping) or response.get("ok") is not True:
            raise BinanceSpotFunctionalMutationOutcomeUnknown(
                "broker acceptance is unproven after boundary marker"
            )
        payload = response.get("json")
        if not isinstance(payload, Mapping):
            raise BinanceSpotFunctionalMutationOutcomeUnknown(
                "broker response body is not an order object"
            )
        return dict(payload)


__all__ = [
    "BinanceSpotFunctionalMutationEdge",
    "BinanceSpotFunctionalMutationError",
    "BinanceSpotFunctionalMutationNotSent",
    "BinanceSpotFunctionalMutationOutcomeUnknown",
    "FunctionalMutationRequest",
    "PRODUCTION_MUTATION_AVAILABLE",
    "build_binance_spot_functional_mutation_request",
]
