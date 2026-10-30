"""Broker edge for explicit, non-promotion short functional sessions.

Nothing is armed at import.  Private observations remain inside the adapter;
only allowlisted results reach the service.  Mutations use LiveBrokerRouter's
existing real-order and final exchange-authority boundaries, never a raw HTTP
transport.  The caller must durably reserve an order before calling submit.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN
import hashlib
import json
import re
import threading
import time
from contextlib import nullcontext
from typing import Any
from trading_runtime.futures_accounting import margin_budget
from .short_live_futures import FuturesProtectionError, FuturesProtectionManager, policy as futures_policy
from .short_live_read_diagnostics import sanitize_read_diagnostics

ROUTES = {
    "KIS_KR_CASH": ("kis", "KIS", "KRW"),
    "UPBIT_KRW_SPOT": ("upbit", "UPBIT", "KRW"),
    "BINANCE_SPOT": ("binance", "BINANCE", "USDT"),
    "BINANCE_USDM_FUTURES": ("binance-futures", "BINANCE", "USDT"),
}
MAX_EXCHANGE_KRW = Decimal("100000")
MAX_LOSS_KRW = Decimal("50000")
_ACTIVE = {"NEW", "OPEN", "WAIT", "WATCH", "ACCEPTED", "PARTIALLY_FILLED", "PENDING_CANCEL", "UNKNOWN"}
_CLIENT = re.compile(r"^[A-Za-z0-9_-]{8,36}$")


class ShortLiveBrokerBlocked(ValueError):
    """Fixed codes only; never interpolate an account, remote body or secret."""


def number(value: Any, *, positive: bool = False) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise ShortLiveBrokerBlocked("NUMBER_UNKNOWN")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        raise ShortLiveBrokerBlocked("NUMBER_INVALID") from None
    if not result.is_finite() or (positive and result <= 0):
        raise ShortLiveBrokerBlocked("NUMBER_INVALID")
    return result


def decimal_text(value: Decimal) -> str:
    return format(value, "f")


def valid_asset_identifier(value: Any) -> bool:
    """Treat official UTF-8 asset identifiers as opaque, case-sensitive text.

    Binance permits non-ASCII asset names even in otherwise ASCII requests.
    Keep their exact code points for inventory and fee joins: do not uppercase
    or Unicode-normalize. Bound display size and reject whitespace/controls.
    This identifier is never a path or an executable expression.
    """
    return (isinstance(value, str) and 0 < len(value) <= 256 and value.isprintable()
            and not any(character.isspace() for character in value))


def epoch(value: Any) -> float:
    try:
        if isinstance(value, bool):
            raise ValueError
        if isinstance(value, (int, float)):
            result = float(value)
        else:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                raise ValueError
            result = parsed.timestamp()
        if not number(result).is_finite():
            raise ValueError
        return result
    except (TypeError, ValueError, OverflowError):
        raise ShortLiveBrokerBlocked("OBSERVATION_TIME_UNKNOWN") from None


class SharedExchangeBudget:
    """One exchange ceiling, including both Binance wallets.

    The service supplies durable baseline/session ownership. This lock makes
    simultaneous adapters atomic inside the same genuine app process.
    Reservations with an UNKNOWN reply are retained, not recycled.
    """
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.reservations: dict[tuple[str, str], Decimal] = {}

    def reserve(self, exchange: str, client_id: str, amount: Decimal, observed: Decimal) -> None:
        with self.lock:
            key = (exchange, client_id)
            if key in self.reservations:
                raise ShortLiveBrokerBlocked("ORDER_ALREADY_RESERVED")
            held = sum((v for (name, _), v in self.reservations.items() if name == exchange), Decimal(0))
            if amount < 0 or observed + held + amount > MAX_EXCHANGE_KRW:
                raise ShortLiveBrokerBlocked("EXCHANGE_SHARED_BUDGET_EXCEEDED")
            self.reservations[key] = amount

    def release_terminal(self, exchange: str, client_id: str) -> None:
        with self.lock:
            self.reservations.pop((exchange, client_id), None)


class OfficialBrokerTransport:
    """Only existing normal broker mutation methods may cross this edge."""
    def __init__(self, router: Any = None, *, enabled_reader: Callable[[], bool] | None = None,
                 order_reader: Callable[..., Mapping[str, Any]] | None = None) -> None:
        if router is None:
            from .brokers import LiveBrokerRouter
            router = LiveBrokerRouter()
        self.router = router
        self._enabled = enabled_reader
        self.order_reader = order_reader

    def enabled(self) -> bool:
        if self._enabled is not None:
            return self._enabled() is True
        from .brokers import real_orders_enabled
        return real_orders_enabled() is True

    def submit(self, route: str, intent: dict[str, Any]) -> dict[str, Any]:
        if not self.enabled():
            raise ShortLiveBrokerBlocked("REAL_ORDERS_DISABLED")
        return self.router.place_order(intent)

    def submit_authorized(self, route, intent, order, guard):
        from .short_live_order_authority import short_live_dispatch
        with short_live_dispatch(order, intent, guard):
            return self.submit(route, intent)

    def cancel(self, route: str, broker_id: str, row: Mapping[str, Any]) -> dict[str, Any]:
        if not self.enabled():
            raise ShortLiveBrokerBlocked("REAL_ORDERS_DISABLED")
        provider = ROUTES[route][0]
        context = {"symbol": row.get("symbol", "")}
        if provider == "kis":
            context.update(order_date=row.get("orderDate"), organization_no=row.get("organizationNo"),
                           quantity=row.get("remainingQuantity"), side=row.get("side"), market="KR", asset="KR_STOCK", order_type="00")
        return self.router.cancel_order(provider, broker_id, **context)

    def cancel_authorized(self, route, broker_id, row, order, guard):
        from .short_live_order_authority import short_live_dispatch
        context = {"symbol": row.get("symbol", "")}
        if ROUTES[route][0] == "kis":
            context.update(order_date=row.get("orderDate"), organization_no=row.get("organizationNo"),
                           quantity=row.get("remainingQuantity"), side=row.get("side"), market="KR", asset="KR_STOCK", order_type="00")
        native = {"broker_id": ROUTES[route][0], "broker_order_id": broker_id, **context}
        with short_live_dispatch(order, native, guard):
            return self.cancel(route, broker_id, row)

    def read_order(self, route: str, row: Mapping[str, Any]) -> Mapping[str, Any]:
        if self.order_reader is None:
            raise ShortLiveBrokerBlocked("EXACT_ORDER_READER_REQUIRED")
        return self.order_reader(route, row)


class ShortLiveBrokerAdapter:
    def __init__(self, route: str, *, snapshot_reader: Callable[..., Mapping[str, Any]],
                 market_reader: Callable[..., Mapping[str, Any]], transport: Any,
                 authority_guard: Callable[[Mapping[str, Any]], bool],
                 reserve_cleanup_order: Callable[..., bool] | None = None,
                 reserve_cleanup_cancel: Callable[..., bool] | None = None,
                 observe_cleanup_order: Callable[..., bool] | None = None,
                 protection_reader=None, protection_transport=None, force_order_reader=None,
                 reserve_protection=None, record_protection=None,
                 budget: SharedExchangeBudget | None = None, clock: Callable[[], float] = time.time,
                 coordination=None, coordination_settings=None) -> None:
        if route not in ROUTES:
            raise ShortLiveBrokerBlocked("ROUTE_UNSUPPORTED")
        self.route, self.provider, self.exchange, self.currency = route, *ROUTES[route]
        self.snapshot_reader, self.market_reader = snapshot_reader, market_reader
        self.transport, self.authority_guard = transport, authority_guard
        self.reserve_cleanup_order, self.reserve_cleanup_cancel = reserve_cleanup_order, reserve_cleanup_cancel
        self.observe_cleanup_order = observe_cleanup_order
        self.budget, self.clock = budget or SharedExchangeBudget(), clock
        self._manifest: dict[str, Any] | None = None
        self._claims: dict[str, dict[str, Any]] = {}
        self._lock = threading.RLock()
        self._baseline_equity_native: Decimal | None = None
        self._baseline_equity_observed_at = None
        self._fill_rows: dict[str, dict[str, Any]] = {}
        self._spot_baseline: dict[str, Decimal] | None = None
        self._spot_baseline_account: str | None = None
        self._observed_futures_settings: dict[str, str] = {}
        self._observed_market_fx: Decimal | None = None
        self._external_commission_quantities: dict[str, dict[str, Decimal]] = {}
        self._external_commission_costs: dict[str, Decimal] = {}
        self._futures_policy = None
        self._observed_futures_by_symbol = {}
        self._bound_futures_by_symbol = {}
        self._force_order_reader = force_order_reader
        self._futures_incidents = []
        self._session_start_ms = int(clock() * 1000)
        self._first_entry_ms = None
        self._cancel_attempts = set()
        self._coordination = coordination
        self._coordination_settings = dict(coordination_settings or {})
        self._coordination_owner = ""
        self._coordination_claims = []
        self._coordination_rejections = {}
        self._protection = (FuturesProtectionManager(clock=clock, reader=protection_reader,
            transport=protection_transport, guard=authority_guard, reserve=reserve_protection,
            record=record_protection) if route == "BINANCE_USDM_FUTURES" and all(callable(x) for x in (protection_reader, reserve_protection, record_protection))
            and protection_transport is not None else None)

    def claim_coordination(self, manifest):
        """Called only after genuine user start, before any feed or order."""
        if self._coordination is None:
            return []  # Explicit injected simulation adapters have no live registry.
        m = self._scope(manifest)
        owner = "live-short:" + str(m["sessionId"])
        self._coordination_claims = self._coordination.claim(owner, self.route, m["symbols"], self._coordination_settings)
        self._coordination_owner = owner
        return list(self._coordination_claims)

    def rollback_unsubmitted_coordination(self):
        if self._coordination is not None and self._coordination_claims:
            self._coordination.rollback_unsubmitted(self._coordination_owner, self._coordination_claims)
            self._coordination_claims = []
            self._coordination_owner = ""

    def _coordination_context(self, funds=None):
        if self._coordination is None:
            return nullcontext()
        if not self._coordination_owner or not self._coordination_claims:
            raise ShortLiveBrokerBlocked("COORDINATION_SESSION_NOT_CLAIMED")
        return self._coordination.dispatch_context(self._coordination_owner, budget_evidence=funds)

    def _settle_coordination(self, snapshot, *, release=False):
        if self._coordination is None or not self._coordination_claims:
            return
        if (snapshot["openOrders"] or snapshot.get("openAlgoOrders")
                or any(number(p["quantity"]) != 0 for p in snapshot["positions"])
                or self._protection and not self._protection.retirement_verified()):
            if release:
                raise ShortLiveBrokerBlocked("COORDINATION_EXPOSURE_NOT_FLAT")
            return
        terminal, observed_times = {}, [epoch(snapshot["observedAt"])]
        for client, row in self._coordination_rejections.items():
            # An explicit broker rejection has no broker order to GET later.
            # Preserve its immutable response hash; the fresh account snapshot
            # proves flat now. UNKNOWN/no-response never enters this mapping.
            terminal[client] = "REJECTED"
        for client, row in self._fill_rows.items():
            if self._claims.get(client, {}).get("externalExecution"):
                continue
            if row.get("known") is not True:
                raise ShortLiveBrokerBlocked("COORDINATION_ORDER_TRUTH_REQUIRED")
            status = str(row.get("status") or "").upper().replace("CANCELLED", "CANCELED")
            if status not in {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}:
                if release:
                    raise ShortLiveBrokerBlocked("COORDINATION_ORDER_NOT_TERMINAL")
                return
            terminal[client] = status
            observed_times.append(epoch(row["observedAt"]))
        if self._protection:
            for client, claim in self._protection.claims.items():
                row = claim.get("observation", {})
                if claim.get("lastReadVerified") is not True or row.get("known") is not True:
                    raise ShortLiveBrokerBlocked("COORDINATION_PROTECTION_TRUTH_REQUIRED")
                status = str(row.get("status") or "").upper().replace("CANCELLED", "CANCELED")
                terminal[client] = "FILLED" if status == "FINISHED" else status
                observed_times.append(epoch(row["observedAt"]))
        truth = {"observedAt": min(observed_times), "ownedPositionFlat": True,
                 "pendingOrderCount": 0, "terminalOrders": terminal,
                 "evidenceHash": hashlib.sha256(json.dumps({"snapshot": snapshot,
                    "orders": self._fill_rows, "rejections": self._coordination_rejections,
                    "protections": self._protection.claims if self._protection else {}},
                    sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()}
        self._coordination.reconcile_release(self._coordination_owner, self.route,
            self._manifest["symbols"], truth, release_scope=release)
        if release:
            self._coordination_claims = []

    def release_coordination(self, session):
        if self._coordination is not None and self._coordination_claims:
            # Independent current observation after trusted worker retirement;
            # never claim flat from elapsed time or a previous terminal flag.
            self._settle_coordination(self._snapshot(self._scope(session)), release=True)
        return True

    def _capital_committed(self, positions, fx):
        if self.route != "BINANCE_USDM_FUTURES":
            return sum((abs(number(row["quantity"])) * number(row["markPrice"], positive=True) * fx for row in positions), Decimal(0))
        leverage, _ = futures_policy(self._futures_policy)
        # Entry capital is stable when mark price rises. Actual isolated wallet
        # can be larger; never silently count that extra collateral as free.
        return sum((max(number(row.get("isolatedWallet")), abs(number(row["quantity"])) *
                        number(row.get("entryPrice"), positive=True) / leverage) * fx
                    for row in positions if number(row["quantity"]) != 0), Decimal(0))

    def _cost_factor(self, market):
        fee = number(market["feeRate"])
        if self.route != "BINANCE_USDM_FUTURES":
            return Decimal(1), fee
        leverage, _ = futures_policy(self._futures_policy)
        # Entry + exit commission and funding allowance consume real capital.
        # Funding changes are also measured by actual account equity at runtime.
        return Decimal(1) / leverage, 2 * fee + abs(number(market["fundingRate"])) * 2

    def _add_external_fill(self, actual, *, incident_type, position_side, source_id):
        if actual.get("known") is not True:
            raise ShortLiveBrokerBlocked("FUTURES_EXTERNAL_EXECUTION_OR_FEE_UNKNOWN")
        self._fresh(actual.get("observedAt"))
        qty = number(actual.get("filledQuantity"))
        if qty <= 0:
            return
        if actual.get("feeNative") is None:
            raise ShortLiveBrokerBlocked("FUTURES_EXTERNAL_EXECUTION_OR_FEE_UNKNOWN")
        broker_id = str(actual.get("brokerOrderId", ""))
        symbol = str(actual.get("symbol", ""))
        side = actual.get("side")
        if not broker_id or side not in {"BUY", "SELL"}:
            raise ShortLiveBrokerBlocked("FUTURES_EXTERNAL_EXECUTION_IDENTITY_UNKNOWN")
        client = "external-" + symbol + "-" + position_side + "-" + broker_id
        prior = self._fill_rows.get(client, {})
        if number(prior.get("filledQuantity", 0)) > qty:
            raise ShortLiveBrokerBlocked("FUTURES_EXTERNAL_CUMULATIVE_FILL_REGRESSED")
        self._claims[client] = {"externalExecution": True, "status": actual.get("status"),
            "clientOrderId": client, "actualClientOrderId": actual.get("clientOrderId", ""),
            "symbol": symbol, "quantity": str(qty), "side": side, "brokerOrderId": broker_id}
        self._fill_rows[client] = dict(actual)
        incident = {"type": incident_type, "symbol": symbol, "positionSide": position_side,
                    "quantity": str(qty), "brokerOrderId": broker_id, "sourceId": str(source_id),
                    "side": side, "fillPrice": str(actual.get("fillPrice")), "feeNative": str(actual["feeNative"]),
                    "verified": True, "observedAt": actual["observedAt"]}
        self._futures_incidents = [row for row in self._futures_incidents
                                  if (row.get("symbol"), row.get("positionSide"), row.get("brokerOrderId")) !=
                                     (symbol, position_side, broker_id)]
        self._futures_incidents.append(incident)

    def _poll_futures_protection(self):
        if self._protection is None:
            return
        try:
            for item in self._protection.poll():
                self._add_external_fill(item["order"], incident_type="PROTECTIVE_STOP",
                    position_side=item["order"].get("positionSide", "BOTH"), source_id=item["claim"]["algoId"])
        except FuturesProtectionError as exc:
            # A read outage must block entries, but should not prevent a fresh
            # account read and exact reducing cleanup of existing exposure.
            self._protection._incident(str(exc))

    def _detect_forced_close(self, symbol, expected, actual):
        if (self.route != "BINANCE_USDM_FUTURES" or not callable(self._force_order_reader)
                or self._first_entry_ms is None or expected == 0 or abs(actual) >= abs(expected)
                or (actual != 0 and (actual > 0) != (expected > 0))):
            return False
        # A manual trade is not liquidation. Require official forceOrders and
        # exact order/fill/fee evidence in the lifetime of our fresh session.
        events = self._force_order_reader(symbol, self._first_entry_ms)
        if events.get("known") is not True:
            raise ShortLiveBrokerBlocked("FUTURES_FORCE_ORDER_HISTORY_UNKNOWN")
        self._fresh(events.get("observedAt"))
        desired_side = "SELL" if expected > 0 else "BUY"
        mode = self._bound_futures_by_symbol.get(symbol, {}).get("positionMode")
        desired_position = ("LONG" if expected > 0 else "SHORT") if mode == "HEDGE" else "BOTH"
        selected = [row for row in events.get("events", [])
                    if row.get("symbol") == symbol and row.get("side") == desired_side
                    and row.get("positionSide") == desired_position
                    and row.get("incidentType") in {"LIQUIDATION", "ADL"}
                    and self._first_entry_ms <= int(row.get("time", -1)) <= int(self.clock()*1000)
                    and number(row.get("filledQuantity", 0)) > 0]
        def previous_quantity(row):
            key = "external-" + symbol + "-" + desired_position + "-" + str(row.get("brokerOrderId"))
            return number(self._fill_rows.get(key, {}).get("filledQuantity", 0))
        if (not selected or any(number(row["filledQuantity"]) < previous_quantity(row) for row in selected)
                or sum((number(row["filledQuantity"]) - previous_quantity(row) for row in selected), Decimal(0)) != abs(expected - actual)):
            return False
        for row in selected:
            observed = dict(self.transport.read_order(self.route, row))
            if (str(observed.get("brokerOrderId")) != str(row.get("brokerOrderId"))
                    or observed.get("symbol") != symbol or observed.get("side") != desired_side
                    or observed.get("positionSide") != desired_position
                    or number(observed.get("filledQuantity")) != number(row["filledQuantity"])):
                raise ShortLiveBrokerBlocked("FUTURES_FORCE_EXECUTION_IDENTITY_CHANGED")
            self._add_external_fill(observed, incident_type=row["incidentType"],
                                    position_side=desired_position, source_id=row["brokerOrderId"])
        return True

    def _fresh(self, value: Any, age: float = 30, *, code: str = "OBSERVATION_STALE") -> None:
        delta = self.clock() - epoch(value)
        if delta < -2 or delta > age:
            raise ShortLiveBrokerBlocked(code)

    def _scope(self, value: Mapping[str, Any]) -> dict[str, Any]:
        if self._manifest is None:
            raise ShortLiveBrokerBlocked("SESSION_NOT_PREPARED")
        m = self._manifest
        for key in ("sessionId", "manifestHash"):
            if str(value.get(key) or "") != str(m[key]):
                raise ShortLiveBrokerBlocked("EXACT_SESSION_SCOPE_REQUIRED")
        if value.get("route", self.route) != self.route:
            raise ShortLiveBrokerBlocked("EXACT_ROUTE_REQUIRED")
        return m

    def _owned(self, row: Mapping[str, Any], m: Mapping[str, Any]) -> bool:
        if row.get("sessionId") == m.get("sessionId") and row.get("manifestHash") == m.get("manifestHash"):
            return True
        client_id = str(row.get("clientOrderId") or "")
        claim = self._claims.get(client_id)
        if claim is None and row.get("brokerOrderId"):
            matches = [c for c in self._claims.values() if c.get("brokerOrderId") == str(row["brokerOrderId"])]
            claim = matches[0] if len(matches) == 1 else None
            if self.provider == "kis" and claim is not None:
                if (not claim.get("orderDate") or not claim.get("organizationNo")
                        or str(row.get("organizationNo")) != str(claim["organizationNo"])
                        or (row.get("orderDate") and str(row["orderDate"]) != str(claim["orderDate"]))):
                    return False
                if isinstance(row, dict): row["orderDate"] = claim["orderDate"]
        return bool(claim and claim.get("brokerOrderId")
                    and str(row.get("brokerOrderId")) == claim["brokerOrderId"])

    def _preserve_spot_inventory(self, s: dict[str, Any], m: Mapping[str, Any],
                                 net: Mapping[str, Decimal], *, establish: bool) -> dict[str, Any]:
        """Separate immutable pre-test units from exact session-fill units.

        Outside symbols need no price. They never enter buying power, session
        exposure, cleanup or equity. Any unexplained quantity change blocks.
        """
        if self.provider not in {"binance", "upbit"}:
            return s
        if "spotInventory" not in s and "spotInventoryComplete" not in s and self._spot_baseline is None:
            # Older injected readers retain the strict zero-position contract;
            # only a complete production inventory enables baseline support.
            return s
        raw = s.get("spotInventory")
        if (s.get("spotInventoryComplete") is not True or not isinstance(raw, Mapping)
                or s.get("equityScope") != "QUOTE_AND_SELECTED_SPOT"):
            raise ShortLiveBrokerBlocked("COMPLETE_SPOT_INVENTORY_REQUIRED")
        current: dict[str, Decimal] = {}
        for asset, value in raw.items():
            if not valid_asset_identifier(asset) or asset == self.currency:
                raise ShortLiveBrokerBlocked("SPOT_INVENTORY_INVALID")
            qty = number(value)
            if qty < 0:
                raise ShortLiveBrokerBlocked("SPOT_INVENTORY_INVALID")
            if qty:
                current[asset] = qty
        symbols = {symbol: symbol.removesuffix("USDT") if self.provider == "binance" else symbol.removeprefix("KRW-")
                   for symbol in m["symbols"]}
        if len(set(symbols.values())) != len(symbols):
            raise ShortLiveBrokerBlocked("SPOT_SYMBOL_INVENTORY_CONFLICT")
        if self._spot_baseline is None:
            if not establish or self._claims:
                raise ShortLiveBrokerBlocked("SPOT_BASELINE_REQUIRED")
            baseline = dict(current)
        else:
            if s["accountId"] != self._spot_baseline_account:
                raise ShortLiveBrokerBlocked("SPOT_BASELINE_ACCOUNT_CHANGED")
            baseline = self._spot_baseline
        expected = dict(baseline)
        for symbol, qty in net.items():
            if symbol not in symbols or qty < 0:
                raise ShortLiveBrokerBlocked("SPOT_SESSION_INVENTORY_INVALID")
            asset = symbols[symbol]
            expected[asset] = expected.get(asset, Decimal(0)) + qty
        external_debits: dict[str, Decimal] = {}
        for client_id, row in self._fill_rows.items():
            quantities = row.get("externalCommissionQuantities", {})
            costs = row.get("externalCommissionNative", {})
            if not isinstance(quantities, Mapping) or not isinstance(costs, Mapping) or quantities.keys() != costs.keys():
                raise ShortLiveBrokerBlocked("EXTERNAL_COMMISSION_UNKNOWN")
            if quantities and row.get("externalCommissionKnown") is not True:
                raise ShortLiveBrokerBlocked("EXTERNAL_COMMISSION_UNKNOWN")
            if quantities and number(row.get("filledQuantity")) <= 0:
                raise ShortLiveBrokerBlocked("EXTERNAL_COMMISSION_WITHOUT_OWNED_FILL")
            previous = self._external_commission_quantities.get(client_id, {})
            current_debits: dict[str, Decimal] = {}
            total_cost = Decimal(0)
            incremental_cost = Decimal(0)
            for asset, raw_qty in quantities.items():
                if not valid_asset_identifier(asset) or asset == self.currency:
                    raise ShortLiveBrokerBlocked("EXTERNAL_COMMISSION_INVALID")
                if asset in symbols.values():
                    # Cross-strategy asset fees require sleeve allocation;
                    # never consume another strategy's units implicitly.
                    raise ShortLiveBrokerBlocked("CROSS_SYMBOL_COMMISSION_ALLOCATION_UNSUPPORTED")
                qty, cost = number(raw_qty), number(costs[asset])
                if qty < 0 or cost < 0 or (qty > 0 and cost <= 0):
                    raise ShortLiveBrokerBlocked("EXTERNAL_COMMISSION_INVALID")
                current_debits[asset] = qty
                total_cost += cost
                if qty:
                    incremental_cost += max(Decimal(0), qty - previous.get(asset, Decimal(0))) * cost / qty
                external_debits[asset] = external_debits.get(asset, Decimal(0)) + qty
            if any(current_debits.get(asset, Decimal(0)) < qty for asset, qty in previous.items()):
                raise ShortLiveBrokerBlocked("EXTERNAL_COMMISSION_CUMULATIVE_CHANGED")
            self._external_commission_quantities[client_id] = current_debits
            # Exact official commission quantities, valued by actual quotes.
            # A later fall in that asset's price cannot erase a seen fee loss.
            self._external_commission_costs[client_id] = max(
                self._external_commission_costs.get(client_id, Decimal(0)) + incremental_cost, total_cost)
        for asset, qty in external_debits.items():
            expected[asset] = expected.get(asset, Decimal(0)) - qty
            if expected[asset] < 0:
                raise ShortLiveBrokerBlocked("EXTERNAL_COMMISSION_EXCEEDS_BASELINE")
        if any(current.get(asset, Decimal(0)) != expected.get(asset, Decimal(0))
               for asset in current.keys() | expected.keys()):
            raise ShortLiveBrokerBlocked("PRESERVED_SPOT_INVENTORY_CHANGED")
        actual: dict[str, Mapping[str, Any]] = {}
        for row in s["positions"]:
            if not isinstance(row, Mapping) or row.get("symbol") not in symbols or row["symbol"] in actual:
                raise ShortLiveBrokerBlocked("SPOT_POSITION_INVENTORY_CONFLICT")
            actual[row["symbol"]] = row
        normalized = []
        preserved_value = Decimal(0)
        for symbol, asset in symbols.items():
            row = actual.get(symbol)
            qty = number(row["quantity"]) if row is not None else Decimal(0)
            if qty != current.get(asset, Decimal(0)):
                raise ShortLiveBrokerBlocked("SPOT_POSITION_INVENTORY_CONFLICT")
            if qty:
                price = number(row.get("markPrice"), positive=True)
                preserved_value += baseline.get(asset, Decimal(0)) * price
                owned_qty = net.get(symbol, Decimal(0))
                if owned_qty:
                    normalized.append({**dict(row), "quantity": decimal_text(owned_qty),
                                       "sessionId": m["sessionId"], "manifestHash": m["manifestHash"]})
        equity = number(s["equityNative"]) - preserved_value - sum(self._external_commission_costs.values(), Decimal(0))
        if equity < 0:
            raise ShortLiveBrokerBlocked("SPOT_SCOPED_EQUITY_INVALID")
        s["positions"] = normalized
        s["equityNative"] = decimal_text(equity)
        s["_spotBaselineCandidate"] = dict(baseline)
        s["preservedAssetSymbols"] = sorted(baseline)
        quantities = {asset: decimal_text(qty) for asset, qty in sorted(baseline.items())}
        material = {"route": self.route, "accountId": s["accountId"],
                    "inventory": {asset: value.rstrip("0").rstrip(".") if "." in value else value
                                  for asset, value in quantities.items()}}
        s["preservedAssetsBaselineHash"] = hashlib.sha256(
            json.dumps(material, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        return s

    def _snapshot(self, m: Mapping[str, Any], *, establish_spot_baseline: bool = False) -> dict[str, Any]:
        if self.route == "BINANCE_USDM_FUTURES":
            self._poll_futures_protection()
        s = dict(self.snapshot_reader(self.route, tuple(m["symbols"])))
        if s.get("known") is not True or s.get("ordersComplete") is not True or s.get("positionsComplete") is not True:
            reason = next((r for r in s.get("blockers", []) if isinstance(r, str) and re.fullmatch(r"[A-Z][A-Z0-9_]{2,100}", r)), None)
            error = ShortLiveBrokerBlocked(reason or "COMPLETE_ACCOUNT_TRUTH_REQUIRED")
            error.readDiagnostics = sanitize_read_diagnostics(s.get("readDiagnostics"))
            raise error
        try:
            self._fresh(s.get("observedAt"), code="ACCOUNT_OBSERVATION_STALE")
        except ShortLiveBrokerBlocked as exc:
            exc.readDiagnostics = sanitize_read_diagnostics(s.get("readDiagnostics"))
            raise
        account = str(s.get("accountId") or "")
        if not account or (m.get("accountId") and account != m["accountId"]):
            raise ShortLiveBrokerBlocked("EXACT_ACCOUNT_REQUIRED")
        for key in ("availableNative", "equityNative"):
            if number(s.get(key)) < 0:
                raise ShortLiveBrokerBlocked("ACCOUNT_VALUE_INVALID")
        if s.get("currency") != self.currency:
            raise ShortLiveBrokerBlocked("ACCOUNT_CURRENCY_MISMATCH")
        if not isinstance(s.get("positions"), list) or not isinstance(s.get("openOrders"), list):
            raise ShortLiveBrokerBlocked("ACCOUNT_ROWS_UNKNOWN")
        # Join current official order truth to locally reserved exact IDs.
        # A matching symbol/PID alone never makes an existing holding owned.
        for client_id, claim in self._claims.items():
            if claim.get("externalExecution"):
                continue  # Exact stop/force execution is reconciled separately.
            if not claim.get("brokerOrderId") and claim.get("status") != "UNKNOWN":
                continue
            observed = self._read_claim_truth(claim)
            self._fill_rows[client_id] = observed
            for key in ("orderDate", "organizationNo"):
                if observed.get(key):
                    claim[key] = observed[key]
            state = str(observed.get("status") or "UNKNOWN").upper()
            if state == "UNKNOWN":
                raise ShortLiveBrokerBlocked("OWNED_ORDER_STATE_UNKNOWN")
        net: dict[str, Decimal] = {}
        for client_id, row in self._fill_rows.items():
            claim = self._claims[client_id]
            qty = number(row.get("filledQuantity"))
            signed = qty if str(claim["side"]).upper() == "BUY" else -qty
            if self.provider == "binance" and qty > 0:
                if row.get("baseCommissionKnown") is not True:
                    raise ShortLiveBrokerBlocked("ACTUAL_BASE_COMMISSION_UNKNOWN")
                base_fee = number(row.get("baseCommissionQuantity"))
                if base_fee < 0 or base_fee > qty:
                    raise ShortLiveBrokerBlocked("ACTUAL_BASE_COMMISSION_INVALID")
                signed -= base_fee
            net[claim["symbol"]] = net.get(claim["symbol"], Decimal(0)) + signed
        s = self._preserve_spot_inventory(s, m, net, establish=establish_spot_baseline)
        if self.route == "BINANCE_USDM_FUTURES":
            algos = s.get("openAlgoOrders")
            if not isinstance(algos, list):
                raise ShortLiveBrokerBlocked("FUTURES_ALGO_ORDERS_UNKNOWN")
            if any(self._protection is None or not self._protection.owns(row) for row in algos):
                raise ShortLiveBrokerBlocked("UNOWNED_FUTURES_CONDITIONAL_ORDER")
            for symbol, quantity in list(net.items()):
                actual = sum((number(row["quantity"]) for row in s["positions"] if row.get("symbol") == symbol), Decimal(0))
                if actual != quantity and self._detect_forced_close(symbol, quantity, actual):
                    net[symbol] = actual  # Exact official force fills were added above, never inferred.
            # Do not net the opposite hedge side into the session's holdings.
            for row in s["positions"]:
                if number(row.get("quantity", 0)) == 0:
                    continue
                specs = [spec for spec in m.get("specs", []) if spec["symbol"] == row.get("symbol")]
                if specs and ((number(row["quantity"]) < 0) != (specs[0].get("positionDirection") == "short")):
                    raise ShortLiveBrokerBlocked("UNOWNED_FUTURES_POSITION_SIDE")
        for row in s["positions"]:
            if not isinstance(row, Mapping):
                raise ShortLiveBrokerBlocked("POSITION_ROW_INVALID")
            qty = number(row.get("quantity"))
            if qty != 0 and not self._owned(row, m):
                if qty != net.get(str(row.get("symbol")), Decimal(0)):
                    raise ShortLiveBrokerBlocked("UNOWNED_ACCOUNT_POSITION")
                row.update(sessionId=m["sessionId"], manifestHash=m["manifestHash"])
        for row in s["openOrders"]:
            if not isinstance(row, Mapping) or not self._owned(row, m):
                raise ShortLiveBrokerBlocked("UNOWNED_ACCOUNT_ORDER")
        for symbol, quantity in net.items():
            actual = sum((number(row["quantity"]) for row in s["positions"] if row.get("symbol") == symbol), Decimal(0))
            if actual != quantity:
                raise ShortLiveBrokerBlocked("OWNED_FILL_POSITION_MISMATCH")
        if self._protection and self._claims:
            self._protection.check_positions(s["positions"])
        return s

    def _market(self, symbol: str, *, require_session_open: bool = False, allow_setting_drift: bool = False) -> dict[str, Any]:
        q = dict(self.market_reader(self.route, symbol))
        if q.get("known") is not True or q.get("symbol") != symbol or q.get("quoteCurrency") != self.currency:
            reason = next((r for r in q.get("blockers", []) if isinstance(r, str) and re.fullmatch(r"[A-Z][A-Z0-9_]{2,100}", r)), None)
            error = ShortLiveBrokerBlocked(reason or "EXACT_MARKET_TRUTH_REQUIRED")
            error.readDiagnostics = sanitize_read_diagnostics(q.get("readDiagnostics"))
            raise error
        try:
            self._fresh(q.get("observedAt"), 10, code="MARKET_OBSERVATION_STALE")
            if q.get("sourceQuoteObservedAt") is not None:
                self._fresh(q["sourceQuoteObservedAt"], 10, code="SOURCE_QUOTE_OBSERVATION_STALE")
            self._fresh(q.get("fxObservedAt"), 15, code="FX_OBSERVATION_STALE")
        except ShortLiveBrokerBlocked as exc:
            from .native_fx_observation import recheck_fx_diagnostics
            exc.readDiagnostics = recheck_fx_diagnostics(
                sanitize_read_diagnostics(q.get("readDiagnostics")),
                checked_at=datetime.fromtimestamp(self.clock(), timezone.utc))
            raise
        for key in ("nativePrice", "krwPerQuote", "lotStep", "minQuantity"):
            number(q.get(key), positive=True)
        if number(q.get("feeRate")) < 0 or number(q.get("feeRate")) > Decimal("0.1"):
            raise ShortLiveBrokerBlocked("FEE_RATE_UNKNOWN")
        if number(q.get("minNotional")) < 0:
            raise ShortLiveBrokerBlocked("MIN_NOTIONAL_UNKNOWN")
        if self.route == "KIS_KR_CASH":
            if require_session_open and q.get("sessionOpen") is not True:
                raise ShortLiveBrokerBlocked("KRX_SESSION_CLOSED")
            if number(q.get("sellTaxRate")) < 0:
                raise ShortLiveBrokerBlocked("SELL_TAX_UNKNOWN")
        if self.route == "BINANCE_USDM_FUTURES":
            self._observed_market_fx = number(q["krwPerQuote"], positive=True)
            self._observed_futures_settings = {
                "marginMode": q.get("marginMode") if q.get("marginMode") in {"ISOLATED", "CROSSED", "CROSS"} else "UNKNOWN",
                "leverage": decimal_text(number(q.get("leverage"))),
                "positionMode": q.get("positionMode") if q.get("positionMode") in {"ONE_WAY", "HEDGE"} else "UNKNOWN"}
            leverage, _ = futures_policy(self._futures_policy)
            if q.get("marginMode") != "ISOLATED" or number(q.get("leverage")) != leverage:
                if not allow_setting_drift:
                    raise ShortLiveBrokerBlocked("FUTURES_SETTINGS_DIFFER_FROM_PREPARED_POLICY")
                self._protection._incident("FUTURES_SETTINGS_CHANGED_DURING_SESSION", symbol=symbol)
            if q.get("positionMode") not in {"ONE_WAY", "HEDGE"}:
                raise ShortLiveBrokerBlocked("FUTURES_POSITION_MODE_UNKNOWN")
            if q.get("isAutoAddMargin") not in (True, False) or type(q.get("isAutoAddMargin")) is not bool:
                if not allow_setting_drift:
                    raise ShortLiveBrokerBlocked("FUTURES_AUTO_ADD_MARGIN_UNKNOWN")
                self._protection._incident("FUTURES_AUTO_ADD_MARGIN_UNKNOWN", symbol=symbol)
            elif q.get("isAutoAddMargin") is True:
                if not allow_setting_drift:
                    raise ShortLiveBrokerBlocked("FUTURES_AUTO_ADD_MARGIN_MUST_BE_DISABLED")
                self._protection._incident("FUTURES_AUTO_ADD_MARGIN_ENABLED", symbol=symbol)
            number(q.get("fundingRate"))
            self._observed_futures_by_symbol[symbol] = dict(self._observed_futures_settings)
            bound = self._bound_futures_by_symbol.get(symbol)
            if bound and bound != self._observed_futures_settings:
                if not allow_setting_drift:
                    raise ShortLiveBrokerBlocked("FUTURES_SETTINGS_CHANGED_DURING_SESSION")
                self._protection._incident("FUTURES_SETTINGS_CHANGED_DURING_SESSION", symbol=symbol)
        return q

    def _accounting_preview(self, manifest, markets, capital):
        if self.route != "BINANCE_USDM_FUTURES": return None
        leverage=int(futures_policy(self._futures_policy)[0])
        specs=manifest.get("specs",[])
        total=number(manifest.get("marginBudgetAllocationDenominator",max(Decimal(1),sum((number(spec.get("targetWeight",1)) for spec in specs),Decimal(0)))))
        rows=[]
        for quote in markets:
            weight=sum((number(spec.get("targetWeight",1))*number(spec.get("positionSizeFraction",1))
                for spec in specs if spec["symbol"]==quote["symbol"]),Decimal(0)) / total if total else Decimal(1)/len(markets)
            budget=capital*weight
            if budget<=0:continue
            values=margin_budget(budget,leverage,number(quote["nativePrice"])*number(quote["krwPerQuote"]),
                entry_fee_rate=quote["feeRate"],exit_fee_rate=quote["feeRate"],funding_reserve_rate=2*abs(number(quote["fundingRate"])))
            rows.append({"symbol":quote["symbol"],"capitalBudgetKrw":decimal_text(budget),
                "positionNotionalKrw":decimal_text(values["positionNotional"]),"initialMarginKrw":decimal_text(values["initialMargin"]),
                "entryFeeEstimateKrw":decimal_text(values["entryFee"]),"costReserveKrw":decimal_text(values["costReserve"]),
                "exchangeLeverage":quote["leverage"],
                "exchangeSymbolMaximumLeverage":max(int(t["initialLeverage"]) for t in quote["leverageBrackets"])})
        return {"sizingBasis":"MARGIN_BUDGET","executionLeverage":leverage,
            "maxLeverageMultiplier":manifest.get("executionModelBinding",{}).get("maxLeverageMultiplier"),
            "capitalBudgetKrw":decimal_text(capital),"costsOn":"NOTIONAL","previews":rows,
            "previewBasis":"ALL_SELECTED_SLEEVES_ENTER_AT_CURRENT_PRICE_BEFORE_QUANTITY_ROUNDING"}

    def readiness(self, manifest: Mapping[str, Any]) -> dict[str, Any]:
        available_minutes = None
        native_minimums = {}
        funding: dict[str, Any] = {}
        self._observed_futures_settings = {}
        self._observed_market_fx = None
        try:
            m = deepcopy(dict(manifest))
            if self.route == "BINANCE_USDM_FUTURES":
                self._futures_policy = deepcopy(m.get("futuresPolicy"))
                selected_leverage, _ = futures_policy(self._futures_policy)
                from trading_runtime.futures_execution_policy import SAFE_MAX_LEVERAGE_MULTIPLIER
                if selected_leverage > SAFE_MAX_LEVERAGE_MULTIPLIER:
                    raise ShortLiveBrokerBlocked("FUTURES_SHARED_POLICY_LEVERAGE_EXCEEDED")
                if self._protection is None:
                    raise ShortLiveBrokerBlocked("FUTURES_PROTECTION_TRANSPORT_UNAVAILABLE")
            if self._manifest is not None and self._manifest != m:
                raise ShortLiveBrokerBlocked("ADAPTER_ALREADY_BOUND")
            if m.get("route") != self.route or not m.get("sessionId") or not re.fullmatch(r"[0-9a-f]{64}", str(m.get("manifestHash") or "")):
                raise ShortLiveBrokerBlocked("MANIFEST_SCOPE_INVALID")
            if not isinstance(m.get("symbols"), list) or not m["symbols"] or len(set(m["symbols"])) != len(m["symbols"]):
                raise ShortLiveBrokerBlocked("EXACT_SYMBOLS_REQUIRED")
            if number(m.get("brokerBudgetKrw", 100000), positive=True) > MAX_EXCHANGE_KRW:
                raise ShortLiveBrokerBlocked("EXCHANGE_BUDGET_TOO_LARGE")
            if number(m.get("routeBudgetKrw", 100000), positive=True) > number(m.get("brokerBudgetKrw", 100000)):
                raise ShortLiveBrokerBlocked("ROUTE_BUDGET_TOO_LARGE")
            if not 0 < number(m.get("dailyLossStopKrw", 50000)) <= MAX_LOSS_KRW:
                raise ShortLiveBrokerBlocked("LOSS_LIMIT_INVALID")
            s = self._snapshot(m, establish_spot_baseline=True)
            funding = {"fundingBasis": "AVAILABLE_QUOTE_ONLY", "quoteCurrency": self.currency,
                       "availableQuoteNative": decimal_text(number(s["availableNative"]))}
            for key in ("preservedAssetSymbols", "preservedAssetsBaselineHash"):
                if key in s:
                    funding[key] = deepcopy(s[key])
            markets = [self._market(symbol, require_session_open=True) for symbol in m["symbols"]]
            if self.provider == "binance":
                selected_assets = {symbol.removesuffix("USDT") for symbol in m["symbols"]}
                if any(q.get("commissionDiscountEnabled") is True
                       and q.get("commissionDiscountAsset") in selected_assets
                       and q["symbol"].removesuffix("USDT") != q.get("commissionDiscountAsset") for q in markets):
                    raise ShortLiveBrokerBlocked("CROSS_SYMBOL_COMMISSION_ALLOCATION_UNSUPPORTED")
            for market in markets:
                self._fresh(market["observedAt"], 10, code="MARKET_OBSERVATION_STALE")
                self._fresh(market["fxObservedAt"], 15, code="FX_OBSERVATION_STALE")
                if market.get("sourceQuoteObservedAt") is not None:
                    self._fresh(market["sourceQuoteObservedAt"], 10, code="SOURCE_QUOTE_OBSERVATION_STALE")
            if self.route == "KIS_KR_CASH":
                remaining = number(markets[0].get("sessionRemainingSeconds"))
                available_minutes = decimal_text(max(Decimal(0), remaining - 120) / 60)
                if number(m.get("durationHours", 6)) * 3600 + 120 > remaining:
                    raise ShortLiveBrokerBlocked("KRX_TEST_DURATION_EXCEEDS_SESSION")
            # All are actual, independently fresh observations. Their rates
            # may legitimately differ between requests; never invent a common
            # quote or make an earlier observation fresh with a later time.
            fx = min(number(market["krwPerQuote"], positive=True) for market in markets)
            available = number(s["availableNative"]) * fx
            funding.update(availableKrw=decimal_text(available), effectiveRouteBudgetKrw=decimal_text(
                min(available, number(m.get("effectiveRouteBudgetKrw", m.get("routeBudgetKrw", 100000))))))
            if available <= 0:
                raise ShortLiveBrokerBlocked("NO_AVAILABLE_BALANCE")
            ceiling = min(available, number(m.get("effectiveRouteBudgetKrw", m.get("routeBudgetKrw", 100000))))
            for q in markets:
                step, price = number(q["lotStep"]), number(q["nativePrice"])
                minimum = max(number(q["minQuantity"]), number(q["minNotional"]) / price)
                units = (minimum / step).to_integral_value(rounding="ROUND_CEILING") * step
                if self.provider == "kis" and number(s.get("maximumOrderQuantities", {}).get(q["symbol"])) < units:
                    raise ShortLiveBrokerBlocked("NATIVE_ORDERABLE_QUANTITY_BELOW_MINIMUM")
                costs = number(q["feeRate"]) + number(q.get("sellTaxRate", 0))
                margin_factor, reserve_costs = self._cost_factor(q)
                required = units * price * number(q["krwPerQuote"]) * (margin_factor + reserve_costs + number(q.get("sellTaxRate", 0)))
                if self.route == "BINANCE_USDM_FUTURES":
                    from .short_live_futures import stop_price
                    direction = next((spec.get("positionDirection", "long") for spec in m.get("specs", []) if spec["symbol"] == q["symbol"]), "long")
                    stop_price({"referencePrice": price, "quantity": ceiling/(margin_factor+reserve_costs)/price/number(q["krwPerQuote"]),
                                "positionDirection": direction}, q, self._futures_policy)
                native_minimums[q["symbol"]] = decimal_text(required)
                if required > ceiling:
                    raise ShortLiveBrokerBlocked("AVAILABLE_BELOW_NATIVE_MINIMUM")
                specs = m.get("specs")
                if isinstance(specs, list) and specs:
                    weight = Decimal(0)
                    symbol_weight = Decimal(0)
                    for spec in specs:
                        if not isinstance(spec, Mapping):
                            raise ShortLiveBrokerBlocked("ALLOCATION_SPEC_INVALID")
                        allocation_weight = number(spec.get("targetWeight"), positive=True)
                        parameters = spec.get("parameters", {})
                        if not isinstance(parameters, Mapping):
                            raise ShortLiveBrokerBlocked("POSITION_SIZE_INVALID")
                        try:
                            position_size = number(parameters.get("positionSize", 100))
                        except ShortLiveBrokerBlocked:
                            raise ShortLiveBrokerBlocked("POSITION_SIZE_INVALID") from None
                        if not 0 <= position_size <= 100:
                            raise ShortLiveBrokerBlocked("POSITION_SIZE_INVALID")
                        # Preserve the source strategy's position sizing. The
                        # original allocation sum remains the denominator;
                        # unused capital must never inflate a five-percent leg.
                        weight += allocation_weight
                        if spec.get("symbol") == q["symbol"]:
                            symbol_weight += allocation_weight * position_size / 100
                    if symbol_weight <= 0 or required > ceiling * symbol_weight / max(Decimal(1), weight):
                        raise ShortLiveBrokerBlocked("ALLOCATION_BELOW_NATIVE_MINIMUM")
            with self._lock:
                if self._manifest is not None and self._manifest != m:
                    raise ShortLiveBrokerBlocked("ADAPTER_ALREADY_BOUND")
                self._manifest = m
                if self.route == "BINANCE_USDM_FUTURES":
                    self._bound_futures_by_symbol = deepcopy(self._observed_futures_by_symbol)
                if self._spot_baseline is None and "_spotBaselineCandidate" in s:
                    self._spot_baseline = dict(s["_spotBaselineCandidate"])
                    self._spot_baseline_account = s["accountId"]
                if self._baseline_equity_native is None:
                    self._baseline_equity_native = number(s["equityNative"])
                    self._baseline_equity_observed_at = s["observedAt"]
            return {"ready": True, "blockers": [], "accountId": s["accountId"], **funding,
                    "availableKrw": decimal_text(available), "existingExposureKrw": "0",
                    "scopeOwnershipVerified": True, "quoteObservedAt": markets[0]["observedAt"],
                    "boundedCost": True, "equityTracked": True,
                    "boundedCostAllowanceVerified": True, "actualEquityLossTracked": True,
                    "availableSessionMinutes": available_minutes,
                    "minimumOrderKrwBySymbol": native_minimums,
                    "fxObservedAt": markets[0]["fxObservedAt"], "promotionEligible": False,
                    "observedFuturesSettingsBySymbol": deepcopy(self._observed_futures_by_symbol),
                    "futuresPolicy": deepcopy(self._futures_policy),
                    "executionAccounting": self._accounting_preview(m, markets, ceiling)}
        except Exception as exc:
            code = str(exc) if isinstance(exc, (ShortLiveBrokerBlocked, FuturesProtectionError)) else "BROKER_READINESS_UNKNOWN"
            if funding and self._observed_market_fx is not None:
                try:
                    available = number(funding["availableQuoteNative"]) * self._observed_market_fx
                    funding.update(availableKrw=decimal_text(available), effectiveRouteBudgetKrw=decimal_text(
                        min(available, number(manifest.get("effectiveRouteBudgetKrw", manifest.get("routeBudgetKrw", 100000))))))
                except ShortLiveBrokerBlocked:
                    pass
            if self._observed_futures_settings:
                funding.update(observedFuturesSettings=dict(self._observed_futures_settings),
                               expectedFuturesSettings=deepcopy(self._futures_policy),
                               observedFuturesSettingsBySymbol=deepcopy(self._observed_futures_by_symbol))
            return {"ready": False, "blockers": [code], "scopeOwnershipVerified": False, **funding,
                    "readDiagnostics": sanitize_read_diagnostics(getattr(exc, "readDiagnostics", None)),
                    "availableSessionMinutes": available_minutes, "minimumOrderKrwBySymbol": native_minimums,
                    "promotionEligible": False}

    def size_order(self, proposal: Mapping[str, Any], *, _coordination_capture=None) -> dict[str, Any]:
        try:
            m = self._scope(proposal)
            symbol, side = str(proposal.get("symbol") or ""), str(proposal.get("side") or "").upper()
            if symbol not in m["symbols"] or side not in {"BUY", "SELL"}:
                raise ShortLiveBrokerBlocked("ORDER_SCOPE_INVALID")
            s, q = self._snapshot(m), self._market(symbol, require_session_open=True,
                                                  allow_setting_drift=proposal.get("riskReducing") is True)
            price, fx = number(q["nativePrice"], positive=True), number(q["krwPerQuote"], positive=True)
            if proposal.get("_reservedLimitPrice") is not None:
                price = number(proposal["_reservedLimitPrice"], positive=True)
            if q.get("priceTick") is not None:
                tick = number(q["priceTick"], positive=True)
                price = (price / tick).to_integral_value(rounding=ROUND_DOWN) * tick
            fee = number(q["feeRate"])
            tax = number(q.get("sellTaxRate", 0)) if side == "SELL" else Decimal(0)
            requested = abs(number(proposal.get("deltaKrw", proposal.get("notionalKrw")), positive=True))
            route_limit = number(m.get("effectiveRouteBudgetKrw", m.get("routeBudgetKrw", 100000)), positive=True)
            available = number(s["availableNative"]) * fx
            positions = [r for r in s["positions"] if number(r["quantity"]) != 0]
            exposure = sum((abs(number(r["quantity"])) * number(r.get("markPrice"), positive=True) * fx for r in positions), Decimal(0))
            committed = self._capital_committed(positions, fx)
            margin_factor, reserve_costs = self._cost_factor(q)
            leverage = futures_policy(self._futures_policy)[0] if self.route == "BINANCE_USDM_FUTURES" else Decimal(1)
            reducing = proposal.get("riskReducing") is True
            matching = [r for r in positions if r.get("symbol") == symbol]
            direction = str(proposal.get("positionDirection") or "long").lower()
            if self.route != "BINANCE_USDM_FUTURES" and direction != "long":
                raise ShortLiveBrokerBlocked("SPOT_SHORT_FORBIDDEN")
            expected_entry = "SELL" if direction == "short" else "BUY"
            if direction not in {"long", "short"} or (not reducing and side != expected_entry):
                raise ShortLiveBrokerBlocked("DIRECTION_SIDE_MISMATCH")
            if reducing:
                if len(matching) != 1 or side == expected_entry:
                    raise ShortLiveBrokerBlocked("OWNED_REDUCING_POSITION_REQUIRED")
                owned = abs(number(matching[0]["quantity"]))
                requested_qty = proposal.get("closeQuantity")
                raw_qty = owned if requested_qty is not None and number(requested_qty) == owned else min(owned, requested / (price * fx))
            else:
                if self._futures_incidents or (self._protection and self._protection.incidents):
                    raise ShortLiveBrokerBlocked("FUTURES_INCIDENT_NEW_ENTRIES_BLOCKED")
                requested_cap = requested
                if self.route == "BINANCE_USDM_FUTURES" and proposal.get("capitalBudgetKrw") is not None:
                    sleeve_budget = number(proposal["capitalBudgetKrw"], positive=True)
                    requested_cap = min(requested_cap, margin_budget(sleeve_budget, int(leverage), price * fx,
                        entry_fee_rate=fee, exit_fee_rate=fee, funding_reserve_rate=abs(number(q["fundingRate"])) * 2)["positionNotional"])
                cap = min(requested_cap, (route_limit - committed) / (margin_factor + reserve_costs + tax),
                          available / (margin_factor + reserve_costs + tax))
                if cap <= 0:
                    raise ShortLiveBrokerBlocked("AVAILABLE_BUDGET_EXHAUSTED")
                raw_qty = cap / (price * fx)
            market_exit = self.route == "BINANCE_USDM_FUTURES" and proposal.get("operation") == "EMERGENCY_CLOSE"
            step = number(q.get("marketQuantityStep") if market_exit else q["lotStep"], positive=True)
            min_qty = number(q.get("marketMinQuantity") if market_exit else q["minQuantity"])
            if min_qty < 0:
                raise ShortLiveBrokerBlocked("ORDER_MINIMUM_QUANTITY_UNKNOWN")
            max_qty = q.get("marketMaxQuantity") if market_exit else q.get("maxQuantity")
            if proposal.get("_reservedQuantity") is not None:
                reserved_qty = number(proposal["_reservedQuantity"], positive=True)
                if (reserved_qty - min_qty) % step != 0 or (reducing and reserved_qty > owned):
                    raise ShortLiveBrokerBlocked("RESERVED_NATIVE_QUANTITY_NO_LONGER_VALID")
                raw_qty = reserved_qty
            if self.provider == "kis" and not reducing:
                maximum = s.get("maximumOrderQuantities", {}).get(symbol)
                if maximum is None:
                    raise ShortLiveBrokerBlocked("NATIVE_ORDERABLE_QUANTITY_UNKNOWN")
                raw_qty = min(raw_qty, number(maximum))
            qty = min_qty + ((raw_qty - min_qty) / step).to_integral_value(rounding=ROUND_DOWN) * step
            if raw_qty < min_qty:
                raise ShortLiveBrokerBlocked("ORDER_BELOW_EXCHANGE_MINIMUM")
            if qty <= 0 or qty < min_qty or (qty * price < number(q["minNotional"]) and
                    not (reducing and self.route == "BINANCE_USDM_FUTURES")):
                raise ShortLiveBrokerBlocked("ORDER_BELOW_EXCHANGE_MINIMUM")
            if max_qty is not None and qty > number(max_qty, positive=True):
                raise ShortLiveBrokerBlocked("ORDER_ABOVE_EXCHANGE_MAXIMUM")
            notional = qty * price * fx
            cost = notional * (fee + tax)
            required_capital = notional * (margin_factor + reserve_costs + tax)
            if _coordination_capture is not None:
                _coordination_capture.update(currency=self.currency, required=decimal_text(required_capital / fx),
                    available=decimal_text(number(s["availableNative"])), observedAt=s["observedAt"], unitScale=8)
            if not reducing and required_capital > available:
                raise ShortLiveBrokerBlocked("AVAILABLE_BUDGET_EXHAUSTED")
            projected = max(Decimal(0), exposure - notional) if reducing else exposure + notional + cost
            projected_capital = max(Decimal(0), committed - notional * margin_factor) if reducing else committed + required_capital
            if not reducing and projected_capital > route_limit:
                raise ShortLiveBrokerBlocked("ROUTE_EXPOSURE_EXCEEDED")
            result = dict(proposal)
            result.update(ready=True, blockers=[], accountId=s["accountId"], quantity=decimal_text(qty),
                          referencePrice=decimal_text(price), estimatedNotionalKrw=decimal_text(notional),
                          projectedGrossExposureKrw=decimal_text(projected), feeKrw=decimal_text(cost),
                          observedAt=q["observedAt"], krwPerQuote=decimal_text(fx),
                          fxObservedAt=q["fxObservedAt"],
                          leverage=decimal_text(leverage), requiredCapitalKrw=decimal_text(required_capital),
                          projectedCommittedCapitalKrw=decimal_text(projected_capital),
                          positionMode=q.get("positionMode", ""), promotionEligible=False)
            if self.route == "BINANCE_USDM_FUTURES":
                result["executionAccounting"] = {"sizingBasis":"MARGIN_BUDGET", "executionLeverage":int(leverage),
                    "capitalBudgetKrw":str(proposal.get("capitalBudgetKrw", required_capital)),
                    "positionNotionalKrw":decimal_text(notional), "initialMarginKrw":decimal_text(notional/leverage),
                    "entryFeeEstimateKrw":decimal_text(notional*fee),
                    "exitFeeReserveKrw":decimal_text(notional*fee),
                    "fundingReserveKrw":decimal_text(notional*abs(number(q["fundingRate"]))*2),
                    "costReserveKrw":decimal_text(notional*(reserve_costs-fee)),
                    "capitalRequiredKrw":decimal_text(required_capital), "costsOn":"NOTIONAL",
                    "exchangeLeverage":q["leverage"], "maxLeverageMultiplier":m.get("executionModelBinding",{}).get("maxLeverageMultiplier"),
                    "exchangeSymbolMaximumLeverage":max(int(row["initialLeverage"]) for row in q["leverageBrackets"]),
                    "reducingUsesActualOwnedQuantity":reducing}
            return result
        except Exception as exc:
            return {"ready": False, "blockers": [str(exc) if isinstance(exc, (ShortLiveBrokerBlocked, FuturesProtectionError)) else "ORDER_SIZING_UNKNOWN"],
                    "readDiagnostics": sanitize_read_diagnostics(getattr(exc, "readDiagnostics", None))}

    @staticmethod
    def _normalize_reply(response: Any, order: Mapping[str, Any], fx: Decimal) -> dict[str, Any]:
        if not isinstance(response, Mapping):
            return {"status": "UNKNOWN"}
        body = response.get("json", response)
        if not isinstance(body, Mapping):
            return {"status": "UNKNOWN"}
        if response.get("outcomeAmbiguous") is True or response.get("redirectBlocked") is True:
            return {"status": "UNKNOWN"}
        if response.get("statusCode") is not None and (number(response["statusCode"]) == 0 or number(response["statusCode"]) >= 500):
            return {"status": "UNKNOWN"}
        if body.get("rt_cd") is not None and str(body.get("rt_cd")) != "0":
            return {"status": "REJECTED"}
        if body.get("code") is not None and number(body["code"]) < 0:
            return {"status": "UNKNOWN" if number(body["code"]) in {-1006, -1007} else "REJECTED"}
        from .order_coordination import explicit_upbit_order_rejection
        if explicit_upbit_order_rejection(body):
            return {"status": "REJECTED"}
        if response.get("ok") is False:
            return {"status": "UNKNOWN"}
        output = body.get("output") if isinstance(body.get("output"), Mapping) else {}
        broker_id = str(body.get("orderId") or body.get("uuid") or output.get("ODNO") or output.get("odno") or "")
        if not broker_id:
            return {"status": "UNKNOWN"}
        client_id = body.get("clientOrderId") or body.get("identifier")
        if client_id is not None and str(client_id) != order["clientOrderId"]:
            return {"status": "UNKNOWN"}
        state = str(body.get("status") or body.get("state") or "ACCEPTED").upper()
        filled = number(body.get("executedQty", body.get("executed_volume", 0)))
        status = "FILLED" if state in {"FILLED", "DONE"} and filled == number(order["quantity"]) else (
            "REJECTED" if state in {"REJECTED", "EXPIRED"} else "ACCEPTED")
        price = body.get("avgPrice") or body.get("price") or order["referencePrice"]
        fee = body.get("paid_fee")
        # An absent fee remains unknown; estimated fees are not actual fills.
        return {"status": status, "brokerOrderId": broker_id, "clientOrderId": order["clientOrderId"],
                "filledQuantity": decimal_text(filled), "fillPrice": str(price),
                "feeKrw": decimal_text(number(fee) * fx) if fee is not None else None,
                "baseCommissionQuantity": "0" if order.get("route") != "BINANCE_SPOT" else None,
                "baseCommissionKnown": order.get("route") != "BINANCE_SPOT",
                "actualFeeKnown": fee is not None}

    def submit(self, order: Mapping[str, Any]) -> dict[str, Any]:
        client_id = str(order.get("clientOrderId") or "")
        try:
            self._scope(order)
            if not _CLIENT.fullmatch(client_id):
                raise ShortLiveBrokerBlocked("EXACT_CLIENT_ORDER_ID_REQUIRED")
            with self._lock:
                if client_id in self._claims:
                    return deepcopy(self._claims[client_id])
                if self.authority_guard(order) is not True or self.transport.enabled() is not True:
                    raise ShortLiveBrokerBlocked("FRESH_MANUAL_AUTHORITY_REQUIRED")
                self._fresh(order.get("observedAt"), 10)
                self._fresh(order.get("fxObservedAt"), 15)
                reserved_notional = order.get("estimatedNotionalKrw", order.get("notionalKrw"))
                coordination_funds = {}
                checked = self.size_order({**dict(order), "notionalKrw": reserved_notional,
                                           "deltaKrw": reserved_notional, "_reservedLimitPrice": order.get("referencePrice"),
                                           "_reservedQuantity": order.get("quantity")}, _coordination_capture=coordination_funds)
                if checked.get("ready") is not True:
                    error = ShortLiveBrokerBlocked(str((checked.get("blockers") or ["ORDER_RECHECK_FAILED"])[0]))
                    error.readDiagnostics = sanitize_read_diagnostics(checked.get("readDiagnostics"))
                    raise error
                if number(order.get("quantity")) != number(checked["quantity"]) or str(order.get("accountId")) != checked["accountId"]:
                    raise ShortLiveBrokerBlocked("ORDER_CHANGED_AT_DISPATCH")
                reducing = order.get("riskReducing") is True
                reserve = Decimal(0) if reducing else number(checked["requiredCapitalKrw"])
                if self.route == "BINANCE_USDM_FUTURES" and not reducing:
                    # A completed round trip must retire its close-all stop
                    # before a fresh entry can create a new protection identity.
                    truth = self._snapshot(self._scope(order))
                    if (self._claims and not truth["openOrders"] and
                            not any(number(p["quantity"]) for p in truth["positions"])):
                        with self._coordination_context():
                            self._protection.finish_flat()
                        self._settle_coordination(self._snapshot(self._scope(order)))
                    with self._coordination_context(coordination_funds):
                        self._protection.ensure({**dict(order), "positionMode": checked["positionMode"]},
                            self._market(order["symbol"]), self._futures_policy)
                self.budget.reserve(self.exchange, client_id, reserve, Decimal(0))
                claim = {"status": "UNKNOWN", "clientOrderId": client_id, "symbol": order["symbol"],
                         "quantity": str(order["quantity"]), "side": order["side"], "brokerOrderId": ""}
                if self.route == "BINANCE_USDM_FUTURES":
                    claim["positionSide"] = (order.get("positionDirection", "long").upper()
                                             if checked["positionMode"] == "HEDGE" else "BOTH")
                self._claims[client_id] = claim
                if self.route == "BINANCE_USDM_FUTURES" and not reducing and self._first_entry_ms is None:
                    self._first_entry_ms = int(self.clock() * 1000)
                native = {"broker_id": self.provider, "symbol": order["symbol"], "side": order["side"],
                          "quantity": str(order["quantity"]), "price": checked["referencePrice"],
                          "order_type": "MARKET" if order.get("operation") == "EMERGENCY_CLOSE" else "LIMIT", "identifier": client_id, "new_client_order_id": client_id,
                          "execution_purpose": "FUNCTIONAL_TEST_ONLY", "session_id": order["sessionId"],
                          "manifest_hash": order["manifestHash"], "risk_reducing": reducing,
                          "reduce_only": reducing, "position_direction": order.get("positionDirection", "long"),
                          "position_mode": checked.get("positionMode", ""),
                          "max_leverage": int(number(checked["leverage"])), "expected_leverage": checked["leverage"],
                          "required_margin_type": "ISOLATED"}
                if self.provider == "kis":
                    native.update(asset="KR_STOCK", asset_class="KR_STOCK", order_type="00")
                sender = getattr(self.transport, "submit_authorized", None)
                with self._coordination_context(coordination_funds):
                    response = sender(self.route, native, dict(order), self.authority_guard) if callable(sender) else self.transport.submit(self.route, native)
                result = self._normalize_reply(response, order, number(checked["krwPerQuote"]))
                result = {**claim, **result, "promotionEligible": False}
                self._claims[client_id] = result
                if result["status"] == "REJECTED":
                    if self._coordination is not None:
                        receipt = self._coordination.coordinator.find_intent(
                            owner_id=self._coordination_owner, intent_key=client_id)
                        if receipt and receipt["state"] == "REJECTED":
                            self._coordination_rejections[client_id] = {
                                "observedAt": self.clock(), "status": "REJECTED",
                                "responseHash": hashlib.sha256(json.dumps(response, sort_keys=True,
                                    default=str, separators=(",", ":")).encode()).hexdigest()}
                    self.budget.release_terminal(self.exchange, client_id)
                elif result.get("brokerOrderId"):
                    # Exactly one observational read; never resubmit or invent
                    # a fill when a limit order is still working.
                    observed = dict(self.transport.read_order(self.route, result))
                    if (observed.get("known") is True and str(observed.get("brokerOrderId")) == result["brokerOrderId"]
                            and observed.get("symbol") == order["symbol"]):
                        self._fresh(observed.get("observedAt"))
                        self._fill_rows[client_id] = observed
                        result["filledQuantity"] = str(observed.get("filledQuantity"))
                        result["fillPrice"] = str(observed.get("fillPrice"))
                        result["status"] = ("FILLED" if observed.get("status") == "FILLED"
                                            and number(observed.get("filledQuantity")) == number(order["quantity"])
                                            else "REJECTED" if observed.get("status") in {"REJECTED", "CANCELED", "EXPIRED"}
                                            and number(observed.get("filledQuantity")) == 0 else "ACCEPTED")
                        if observed.get("feeNative") is not None:
                            result["feeKrw"] = decimal_text(number(observed["feeNative"]) * number(checked["krwPerQuote"]))
                            result["actualFeeKnown"] = True
                        result["baseCommissionQuantity"] = observed.get("baseCommissionQuantity")
                        result["baseCommissionKnown"] = observed.get("baseCommissionKnown") is True
                        self._claims[client_id] = result
                return deepcopy(result)
        except Exception as exc:
            diagnostics = sanitize_read_diagnostics(getattr(exc, "readDiagnostics", None))
            with self._lock:
                if client_id in self._claims:
                    # Preserve the same claim and first diagnostic on duplicate observations; never resubmit.
                    if diagnostics and not self._claims[client_id].get("readDiagnostics"):
                        self._claims[client_id]["readDiagnostics"] = diagnostics
                    return deepcopy(self._claims[client_id])
            return {"status": "REJECTED", "blockers": [str(exc) if isinstance(exc, (ShortLiveBrokerBlocked, FuturesProtectionError)) else "ORDER_DISPATCH_BLOCKED"],
                    "clientOrderId": client_id, "readDiagnostics": diagnostics}

    def _read_claim_truth(self, claim):
        recovering = not claim.get("brokerOrderId")
        if recovering and (self.route != "BINANCE_USDM_FUTURES" or claim.get("status") != "UNKNOWN"):
            raise ShortLiveBrokerBlocked("ORDER_REPLY_UNKNOWN")
        observed = dict(self.transport.read_order(self.route, deepcopy(claim)))
        broker_id = str(observed.get("brokerOrderId") or "")
        if (observed.get("known") is not True or not broker_id
                or (not recovering and broker_id != str(claim["brokerOrderId"]))
                or observed.get("symbol") != claim["symbol"]
                or observed.get("clientOrderId") not in (None, "", claim["clientOrderId"])):
            raise ShortLiveBrokerBlocked("EXACT_OWNED_ORDER_TRUTH_REQUIRED")
        if self.route == "BINANCE_USDM_FUTURES":
            if (observed.get("side") != claim["side"] or observed.get("positionSide") != claim["positionSide"]
                    or (recovering and (observed.get("clientOrderId") != claim["clientOrderId"]
                        or number(observed.get("quantity"), positive=True) != number(claim["quantity"], positive=True)))):
                raise ShortLiveBrokerBlocked("EXACT_OWNED_ORDER_TRUTH_REQUIRED")
        self._fresh(observed.get("observedAt"))
        if observed.get("status") == "UNKNOWN":
            raise ShortLiveBrokerBlocked("OWNED_ORDER_STATE_UNKNOWN")
        claim["brokerOrderId"] = broker_id
        return observed

    def observe_order(self, client_id: str) -> dict[str, Any]:
        """One exact official GET; never replay a POST or infer a fill."""
        try:
            with self._lock:
                claim = self._claims.get(str(client_id))
                if claim is None:
                    raise ShortLiveBrokerBlocked("EXACT_ORDER_CLAIM_UNKNOWN")
                observed = self._read_claim_truth(claim)
                filled = number(observed.get("filledQuantity"))
                if self.provider == "binance" and filled > 0 and observed.get("baseCommissionKnown") is not True:
                    raise ShortLiveBrokerBlocked("ACTUAL_BASE_COMMISSION_UNKNOWN")
                if filled < number(self._fill_rows.get(client_id, {}).get("filledQuantity", 0)) or filled > number(claim["quantity"]):
                    raise ShortLiveBrokerBlocked("CUMULATIVE_FILL_CHANGED")
                status = str(observed.get("status") or "UNKNOWN").upper()
                state = "FILLED" if status == "FILLED" and filled == number(claim["quantity"]) else (
                    "CANCELED" if status in {"CANCELED", "CANCELLED", "EXPIRED"} else "REJECTED" if status == "REJECTED" else
                    "PARTIAL" if status in _ACTIVE | {"PARTIAL"} and filled > 0 else "ACCEPTED" if status in _ACTIVE else "UNKNOWN")
                if state == "UNKNOWN":
                    raise ShortLiveBrokerBlocked("EXACT_ORDER_STATE_UNKNOWN")
                q = self._market(claim["symbol"], allow_setting_drift=True)
                fee = observed.get("feeNative")
                result = {"status": state, "clientOrderId": client_id, "brokerOrderId": claim["brokerOrderId"],
                          "filledQuantity": decimal_text(filled), "fillPrice": str(observed.get("fillPrice")),
                          "feeKrw": decimal_text(number(fee) * number(q["krwPerQuote"])) if fee is not None else None,
                          "actualFeeKnown": fee is not None}
                result["baseCommissionQuantity"] = observed.get("baseCommissionQuantity", "0" if self.provider != "binance" else None)
                result["baseCommissionKnown"] = observed.get("baseCommissionKnown") is True or self.provider != "binance"
                self._fill_rows[client_id] = observed
                claim.update(result)
                for key in ("orderDate", "organizationNo"):
                    if observed.get(key): claim[key] = observed[key]
                return deepcopy(result)
        except Exception:
            return {"status": "UNKNOWN", "clientOrderId": client_id, "actualFeeKnown": False}

    def reconcile(self, session: Mapping[str, Any]) -> dict[str, Any]:
        try:
            m = self._scope(session)
            s = self._snapshot(m)
            actual_positions = [p for p in s["positions"] if number(p["quantity"]) != 0]
            quotes = ({symbol: {"krwPerQuote": "1", "nativePrice": "0"} for symbol in m["symbols"]}
                      if self.currency == "KRW" and not actual_positions else
                      {symbol: self._market(symbol, allow_setting_drift=True) for symbol in m["symbols"]})
            fx = number(next(iter(quotes.values()))["krwPerQuote"])
            committed = self._capital_committed(actual_positions, fx)
            equity = number(s["equityNative"]) * fx
            exposure = sum((abs(number(p["quantity"])) * number(p.get("markPrice", quotes[p["symbol"]]["nativePrice"])) * fx
                            for p in s["positions"] if number(p["quantity"]) != 0), Decimal(0))
            positions = {p["symbol"]: {"quantity": str(p["quantity"]),
                         "exposureKrw": decimal_text(abs(number(p["quantity"])) * number(p.get("markPrice", quotes[p["symbol"]]["nativePrice"])) * fx)}
                         for p in s["positions"] if number(p["quantity"]) != 0}
            if "realizedPnlNative" in s and "unrealizedPnlNative" in s:
                realized_loss = max(Decimal(0), -number(s["realizedPnlNative"]) * fx)
                unrealized_loss = max(Decimal(0), -number(s["unrealizedPnlNative"]) * fx)
            elif self._baseline_equity_native is not None:
                # Account equity includes actual fees, funding and unrealized
                # positions. Measure its decline in the native quote first;
                # FX changes on idle cash must not conceal a trading loss or
                # manufacture one. Convert that decline using fresh actual FX.
                # This remains an account-equity guard, not exact realized PnL.
                realized_loss = max(Decimal(0), self._baseline_equity_native - number(s["equityNative"])) * fx
                unrealized_loss = Decimal(0)
            else:
                raise ShortLiveBrokerBlocked("LOSS_BASELINE_UNKNOWN")
            if not s["openOrders"] and not positions and all(
                    str(self._fill_rows.get(cid, {}).get("status")) in {"FILLED", "REJECTED", "CANCELED", "CANCELLED", "EXPIRED"}
                    for cid in self._claims):
                for cid in self._claims:
                    self.budget.release_terminal(self.exchange, cid)
            self._settle_coordination(s)
            from .short_live_monitor import public_observed_at
            return {"known": True, "ready": True, "blockers": [], "accountId": s["accountId"],
                    "accountObservedAt": public_observed_at(s["observedAt"]), "pendingOrders": deepcopy(s["openOrders"]),
                    "ownedOrders": deepcopy(s["openOrders"]), "positions": deepcopy(s["positions"]),
                    "ownedPositions": deepcopy([p for p in s["positions"] if number(p["quantity"]) != 0]),
                    "positionsBySymbol": positions, "realizedLossKrw": decimal_text(realized_loss),
                    "unrealizedLossKrw": decimal_text(unrealized_loss), "equityKrw": decimal_text(equity),
                    # Display-only signed change from the existing native-equity
                    # baseline. Never substitute this for the unchanged loss guard.
                    # Fresh FX converts both amounts, so idle FX movement is not PnL.
                    "monitorEquity": {
                        "basis": "OBSERVED_NATIVE_EQUITY_CHANGE",
                        "baselineObservedAt": public_observed_at(self._baseline_equity_observed_at),
                        "fxObservedAt": public_observed_at(next(iter(quotes.values())).get("fxObservedAt")),
                        "fxBasis": "IDENTITY_KRW" if self.currency == "KRW" else "OBSERVED_FX",
                        "baselineNative": decimal_text(self._baseline_equity_native) if self._baseline_equity_native is not None else None,
                        "currentNative": decimal_text(number(s["equityNative"])),
                        "quoteCurrency": self.currency,
                        "krwPerQuote": decimal_text(fx),
                        "preservedSpotExcluded": self._spot_baseline is not None,
                        "cashFlowAdjusted": False,
                        "realizedPnlKrw": decimal_text(number(s["realizedPnlNative"]) * fx) if "realizedPnlNative" in s else None,
                        "unrealizedPnlKrw": decimal_text(number(s["unrealizedPnlNative"]) * fx) if "unrealizedPnlNative" in s else None,
                    },
                    "exposureKrw": decimal_text(exposure), "scopeOwnershipVerified": True,
                    "committedCapitalKrw": decimal_text(committed), "notionalExposureKrw": decimal_text(exposure),
                    "executionAccounting": ({"sizingBasis":"MARGIN_BUDGET",
                        "executionLeverage":int(futures_policy(self._futures_policy)[0]),
                        "maxLeverageMultiplier":m.get("executionModelBinding",{}).get("maxLeverageMultiplier"),
                        "capitalBudgetKrw":m.get("effectiveRouteBudgetKrw",m.get("routeBudgetKrw")),
                        "initialMarginKrw":decimal_text(committed),"positionNotionalKrw":decimal_text(exposure),
                        "costsOn":"NOTIONAL", "fundingModel":"ACTUAL_RATE_ABSOLUTE_TWO_ALLOWANCES",
                        "costReserveKrw":decimal_text(sum((abs(number(p["quantity"]))*number(p["entryPrice"])*fx*
                            (number(quotes[p["symbol"]]["feeRate"])+2*abs(number(quotes[p["symbol"]]["fundingRate"]))) for p in actual_positions),Decimal(0))),
                        "exchangeLeverageBySymbol":deepcopy(self._observed_futures_by_symbol)} if self.route=="BINANCE_USDM_FUTURES" else None),
                    "protections": self._protection.public_rows() if self._protection else [],
                    "ownedProtectionOrders": deepcopy(s.get("openAlgoOrders", [])),
                    "protectionRetirementVerified": self._protection.retirement_verified() if self._protection else True,
                    "futuresIncidents": deepcopy(self._futures_incidents) + deepcopy(self._protection.incidents if self._protection else []),
                    "observedFuturesSettingsBySymbol": deepcopy(self._observed_futures_by_symbol),
                    "futuresPositions": deepcopy(actual_positions) if self.route == "BINANCE_USDM_FUTURES" else [],
                    "lossBasis": ("EXACT_SCOPE_PNL" if "realizedPnlNative" in s else
                                  "SESSION_NATIVE_EQUITY_EXCLUDING_PRESERVED_SPOT_WITH_VERIFIED_FEES"
                                  if self._spot_baseline is not None else "ACCOUNT_NATIVE_EQUITY_DECLINE_AT_FRESH_FX"),
                    "promotionEligible": False}
        except Exception as exc:
            return {"known": False, "ready": False, "blockers": [str(exc) if isinstance(exc, (ShortLiveBrokerBlocked, FuturesProtectionError)) else "RECONCILIATION_UNKNOWN"],
                    "readDiagnostics": sanitize_read_diagnostics(getattr(exc, "readDiagnostics", None)),
                    "futuresIncidents": deepcopy(self._futures_incidents) + deepcopy(self._protection.incidents if self._protection else [])}

    def cleanup(self, session: Mapping[str, Any]) -> dict[str, Any]:
        current = self.reconcile(session)
        if current.get("known") is not True:
            return {**current, "complete": False}
        try:
            m = self._scope(session)
            for row in current["pendingOrders"]:
                if not self._owned(row, m) or not row.get("brokerOrderId"):
                    raise ShortLiveBrokerBlocked("EXACT_OWNED_CANCEL_REQUIRED")
                auth = {**dict(session), "route": self.route, "riskReducing": True, "operation": "CANCEL", "brokerOrderId": row["brokerOrderId"]}
                auth.update(symbol=row.get("symbol"), accountId=m.get("accountId") or current.get("accountId"))
                if self.provider == "kis":
                    auth.update(orderDate=row.get("orderDate"), organizationNo=row.get("organizationNo"),
                                remainingQuantity=row.get("remainingQuantity"))
                cancel_key = (str(row.get("symbol")), str(row["brokerOrderId"]))
                if cancel_key not in self._cancel_attempts:
                    if not callable(self.reserve_cleanup_cancel) or self.reserve_cleanup_cancel(auth) is not True:
                        raise ShortLiveBrokerBlocked("DURABLE_CANCEL_RESERVATION_REQUIRED")
                    if self.authority_guard(auth) is not True:
                        raise ShortLiveBrokerBlocked("CLEANUP_AUTHORITY_REQUIRED")
                    self._cancel_attempts.add(cancel_key)
                    cancel = getattr(self.transport, "cancel_authorized", None)
                    with self._coordination_context():
                        if callable(cancel): cancel(self.route, str(row["brokerOrderId"]), row, auth, self.authority_guard)
                        else: self.transport.cancel(self.route, str(row["brokerOrderId"]), row)
            # Cancellation is only an acknowledgement. Re-read before flatten.
            current = self.reconcile(session)
            if current.get("known") is not True or current.get("pendingOrders"):
                return {**current, "complete": False}
            for row in current["positions"]:
                qty = number(row["quantity"])
                if not qty:
                    continue
                q = self._market(str(row["symbol"]), allow_setting_drift=True)
                direction = "short" if qty < 0 or row.get("positionDirection") == "short" else "long"
                key = hashlib.sha256((str(m["sessionId"]) + str(row["symbol"]) + direction).encode()).hexdigest()[:24]
                client = "sl-clean-" + key
                if client in self._claims:
                    # Continue observing the same cleanup intent. A partial
                    # fill or lost response never creates another close order.
                    self.observe_cleanup_order(client, self.observe_order(client))
                    continue
                proposal = {**dict(session), "route": self.route, "symbol": row["symbol"], "side": "BUY" if direction == "short" else "SELL",
                            "positionDirection": direction, "riskReducing": True, "clientOrderId": client,
                            "closeQuantity": decimal_text(abs(qty)),
                            "notionalKrw": decimal_text(abs(qty) * number(q["nativePrice"]) * number(q["krwPerQuote"]))}
                if self.route == "BINANCE_USDM_FUTURES":
                    proposal["operation"] = "EMERGENCY_CLOSE"
                sized = self.size_order(proposal)
                if sized.get("ready") is not True:
                    raise ShortLiveBrokerBlocked("CLEANUP_SIZING_BLOCKED")
                if not callable(self.observe_cleanup_order):
                    raise ShortLiveBrokerBlocked("DURABLE_CLEANUP_OBSERVER_REQUIRED")
                if not callable(self.reserve_cleanup_order) or self.reserve_cleanup_order(sized) is not True:
                    raise ShortLiveBrokerBlocked("DURABLE_CLEANUP_RESERVATION_REQUIRED")
                reply = self.submit(sized)
                if self.observe_cleanup_order(sized["clientOrderId"], reply) is not True:
                    raise ShortLiveBrokerBlocked("DURABLE_CLEANUP_OBSERVATION_FAILED")
            final = self.reconcile(session)
            if (self._protection and final.get("known") is True and not final.get("pendingOrders")
                    and not final.get("positionsBySymbol")):
                with self._coordination_context():
                    self._protection.finish_flat()
                final = self.reconcile(session)
            complete = (final.get("known") is True and not final.get("pendingOrders") and not final.get("positionsBySymbol")
                        and final.get("protectionRetirementVerified") is True)
            return {**final, "complete": complete, "ownedOrdersRemaining": len(final.get("pendingOrders", [])),
                    "ownedPositionsRemaining": len(final.get("positionsBySymbol", {}))}
        except Exception as exc:
            return {"known": False, "complete": False, "blockers": [str(exc) if isinstance(exc, ShortLiveBrokerBlocked) else "CLEANUP_UNKNOWN"]}

    def retire_flat_protection(self, session):
        current = self.reconcile(session)
        if current.get("known") is not True or current.get("pendingOrders") or current.get("ownedPositions"):
            return False
        if self._protection:
            with self._coordination_context():
                self._protection.finish_flat()
        final = self.reconcile(session)
        return (final.get("known") is True and not final.get("ownedProtectionOrders")
                and not final.get("pendingOrders") and not final.get("ownedPositions")
                and final.get("protectionRetirementVerified") is True)

    def continue_cleanup(self, session):
        """Read the same durable intents, dispatching each cancel/close once."""
        return self.cleanup(session)


def create_production_short_live_broker(route: str, *, settings: Mapping[str, Any],
                                      authority_guard: Callable[[Mapping[str, Any]], bool],
                                      fx_reader: Callable[[str], Mapping[str, Any]] | None = None,
                                      budget: SharedExchangeBudget | None = None,
                                      ownership_reader: Callable[..., Mapping[str, Any]] | None = None,
                                      cost_reader: Callable[..., Mapping[str, Any]] | None = None,
                                      reserve_cleanup_order: Callable[..., bool] | None = None,
                                      reserve_cleanup_cancel: Callable[..., bool] | None = None,
                                      observe_cleanup_order: Callable[..., bool] | None = None,
                                      reserve_protection=None, record_protection=None,
                                      router: Any = None, clock: Callable[[], float] = time.time) -> ShortLiveBrokerAdapter:
    from .short_live_broker_readers import ProductionShortLiveReaders
    from .futures_protection_transport import FuturesProtectionTransport
    from .order_coordination import LiveOrderCoordination
    readers = ProductionShortLiveReaders(settings=settings, fx_reader=fx_reader,
                                        ownership_reader=ownership_reader, cost_reader=cost_reader, clock=clock)
    return ShortLiveBrokerAdapter(route, snapshot_reader=readers.snapshot, market_reader=readers.market,
                                  transport=OfficialBrokerTransport(router, order_reader=readers.get_order), authority_guard=authority_guard,
                                  reserve_cleanup_order=reserve_cleanup_order, reserve_cleanup_cancel=reserve_cleanup_cancel,
                                  observe_cleanup_order=observe_cleanup_order,
                                  protection_reader=readers.protection_order,
                                  protection_transport=FuturesProtectionTransport(router),
                                  force_order_reader=readers.futures_force_orders,
                                  reserve_protection=reserve_protection, record_protection=record_protection,
                                  budget=budget, clock=clock, coordination=LiveOrderCoordination(),
                                  coordination_settings=settings)
