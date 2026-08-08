from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import threading
from typing import Any, Iterable, Mapping, Sequence


LIVE_PORTFOLIO_LEDGER_SCHEMA = "live-portfolio-sleeve-ledger-v1"
LIVE_PORTFOLIO_PLAN_SCHEMA = "live-portfolio-net-plan-v2"
LIVE_PORTFOLIO_RECONCILIATION_SCHEMA = (
    "live-portfolio-restart-reconciliation-v1"
)
_ZERO = Decimal("0")
_ONE = Decimal("1")
_MONEY_QUANTUM = Decimal("0.00000001")
_DOMESTIC_SYMBOL = re.compile(r"^[0-9]{6}$")


def _text(value: Any, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise ValueError(f"{field} is required")
    if any(ord(char) < 32 for char in result):
        raise ValueError(f"{field} contains a control character")
    return result


def canonical_kis_symbol(value: Any) -> str:
    text = _text(value, "symbol").upper()
    if text.startswith("KRX:"):
        text = text[4:]
    if text.endswith((".KS", ".KQ")):
        text = text[:-3]
    if not _DOMESTIC_SYMBOL.fullmatch(text):
        raise ValueError(f"unsupported KIS domestic symbol: {value}")
    return text


def _decimal(value: Any, field: str, *, minimum: Decimal | None = None) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a finite decimal")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a finite decimal") from exc
    if not result.is_finite():
        raise ValueError(f"{field} must be a finite decimal")
    if minimum is not None and result < minimum:
        raise ValueError(f"{field} must be >= {minimum}")
    return result


def _quantity(value: Any, field: str, *, allow_zero: bool = True) -> Decimal:
    result = _decimal(value, field, minimum=_ZERO)
    if result != result.to_integral_value():
        raise ValueError(f"{field} must be a whole-share KIS quantity")
    if not allow_zero and result == _ZERO:
        raise ValueError(f"{field} must be greater than zero")
    return result


def _signed_quantity(value: Any, field: str) -> Decimal:
    result = _decimal(value, field)
    if result != result.to_integral_value():
        raise ValueError(f"{field} must be a whole-share KIS quantity")
    return result


def _decimal_text(value: Decimal) -> str:
    return "0" if value == _ZERO else format(value.normalize(), "f")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _occurred_at(value: Any = "") -> str:
    if value is None or (isinstance(value, str) and not value.strip()):
        return _now()
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(
                "occurred_at must be an ISO-8601 timestamp with timezone"
            ) from exc
    else:
        raise ValueError(
            "occurred_at must be an ISO-8601 string or timezone-aware datetime"
        )
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("occurred_at must include an explicit timezone")
    return parsed.astimezone(timezone.utc).isoformat(
        timespec="microseconds"
    ).replace("+00:00", "Z")


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class SleeveTarget:
    intent_id: str
    sleeve_id: str
    symbol: str
    target_quantity: Decimal | int | str

    def __post_init__(self) -> None:
        object.__setattr__(self, "intent_id", _text(self.intent_id, "intent_id"))
        object.__setattr__(self, "sleeve_id", _text(self.sleeve_id, "sleeve_id"))
        object.__setattr__(self, "symbol", canonical_kis_symbol(self.symbol))
        object.__setattr__(
            self,
            "target_quantity",
            _quantity(self.target_quantity, "target_quantity"),
        )


@dataclass(frozen=True)
class SleeveDelta:
    intent_id: str
    sleeve_id: str
    current_quantity: Decimal
    target_quantity: Decimal
    signed_quantity: Decimal


@dataclass(frozen=True)
class SleeveAllocation:
    intent_id: str
    sleeve_id: str
    signed_quantity: Decimal


@dataclass(frozen=True)
class SymbolNetPlan:
    plan_id: str
    schema_version: str
    scope_id: str
    portfolio_id: str
    portfolio_hash: str
    symbol: str
    side: str
    quantity: Decimal
    reference_price: Decimal
    deltas: tuple[SleeveDelta, ...]
    internal_allocations: tuple[SleeveAllocation, ...]
    allocations: tuple[SleeveAllocation, ...]

    @property
    def internal_only(self) -> bool:
        return self.quantity == _ZERO and bool(self.internal_allocations)

    @property
    def has_internal_cross(self) -> bool:
        return bool(self.internal_allocations)

    def metadata(self) -> dict[str, Any]:
        return {
            "schemaVersion": self.schema_version,
            "scopeId": self.scope_id,
            "planId": self.plan_id,
            "portfolioId": self.portfolio_id,
            "portfolioHash": self.portfolio_hash,
            "symbol": self.symbol,
            "side": self.side,
            "quantity": _decimal_text(self.quantity),
            "referencePrice": _decimal_text(self.reference_price),
            "sleeveDeltas": [
                {
                    "intentId": item.intent_id,
                    "sleeveId": item.sleeve_id,
                    "currentQuantity": _decimal_text(item.current_quantity),
                    "targetQuantity": _decimal_text(item.target_quantity),
                    "signedQuantity": _decimal_text(item.signed_quantity),
                }
                for item in self.deltas
            ],
            "internalAllocations": [
                {
                    "intentId": item.intent_id,
                    "sleeveId": item.sleeve_id,
                    "signedQuantity": _decimal_text(item.signed_quantity),
                }
                for item in self.internal_allocations
            ],
            "allocations": [
                {
                    "intentId": item.intent_id,
                    "sleeveId": item.sleeve_id,
                    "signedQuantity": _decimal_text(item.signed_quantity),
                }
                for item in self.allocations
            ],
        }


def _split_internal_and_external_allocations(
    deltas: Sequence[SleeveDelta],
) -> tuple[tuple[SleeveAllocation, ...], tuple[SleeveAllocation, ...]]:
    """Deterministically cross opposing sleeves before reaching the broker."""

    positives = [item for item in deltas if item.signed_quantity > _ZERO]
    negatives = [item for item in deltas if item.signed_quantity < _ZERO]
    internal: dict[tuple[str, str], Decimal] = {
        (item.sleeve_id, item.intent_id): _ZERO for item in deltas
    }
    positive_remaining = [item.signed_quantity for item in positives]
    negative_remaining = [-item.signed_quantity for item in negatives]
    positive_index = 0
    negative_index = 0
    while (
        positive_index < len(positives)
        and negative_index < len(negatives)
    ):
        crossed = min(
            positive_remaining[positive_index],
            negative_remaining[negative_index],
        )
        positive = positives[positive_index]
        negative = negatives[negative_index]
        internal[(positive.sleeve_id, positive.intent_id)] += crossed
        internal[(negative.sleeve_id, negative.intent_id)] -= crossed
        positive_remaining[positive_index] -= crossed
        negative_remaining[negative_index] -= crossed
        if positive_remaining[positive_index] == _ZERO:
            positive_index += 1
        if negative_remaining[negative_index] == _ZERO:
            negative_index += 1

    internal_allocations: list[SleeveAllocation] = []
    external_allocations: list[SleeveAllocation] = []
    for item in deltas:
        crossed = internal[(item.sleeve_id, item.intent_id)]
        residual = item.signed_quantity - crossed
        if crossed != _ZERO:
            internal_allocations.append(
                SleeveAllocation(item.intent_id, item.sleeve_id, crossed)
            )
        if residual != _ZERO:
            external_allocations.append(
                SleeveAllocation(item.intent_id, item.sleeve_id, residual)
            )
    if sum(
        (item.signed_quantity for item in internal_allocations), _ZERO
    ) != _ZERO:
        raise RuntimeError("internal sleeve cross does not conserve quantity")
    expected_net = sum((item.signed_quantity for item in deltas), _ZERO)
    if sum(
        (item.signed_quantity for item in external_allocations), _ZERO
    ) != expected_net:
        raise RuntimeError("external sleeve allocation does not match net quantity")
    return tuple(internal_allocations), tuple(external_allocations)


def build_symbol_net_plan(
    *,
    scope_id: str,
    portfolio_id: str,
    portfolio_hash: str,
    targets: Iterable[SleeveTarget],
    current_positions: Mapping[tuple[str, str], Any],
    broker_quantity: Any,
    reference_price: Any = 0,
) -> SymbolNetPlan:
    """Build one deterministic KIS broker order for one symbol.

    Sleeve targets are absolute.  Opposing sleeve changes are retained in the
    allocation while only their signed sum reaches KIS.
    """

    normalized_scope = _text(scope_id, "scope_id")
    normalized_portfolio = _text(portfolio_id, "portfolio_id")
    normalized_hash = _text(portfolio_hash, "portfolio_hash").lower()
    items = tuple(targets)
    if not items:
        raise ValueError("at least one sleeve target is required")
    symbols = {item.symbol for item in items}
    if len(symbols) != 1:
        raise ValueError("a symbol plan cannot mix symbols")
    identities = [(item.sleeve_id, item.symbol) for item in items]
    if len(identities) != len(set(identities)):
        raise ValueError("only one target per sleeve and symbol is allowed")
    intent_ids = [item.intent_id for item in items]
    if len(intent_ids) != len(set(intent_ids)):
        raise ValueError("intent_id must be unique")
    symbol = next(iter(symbols))
    deltas: list[SleeveDelta] = []
    for item in sorted(items, key=lambda target: (target.sleeve_id, target.intent_id)):
        current = _quantity(
            current_positions.get((item.sleeve_id, symbol), 0),
            f"current position {item.sleeve_id}/{symbol}",
        )
        signed = item.target_quantity - current
        if current + signed < _ZERO:
            raise ValueError(f"target would short sleeve {item.sleeve_id}/{symbol}")
        deltas.append(
            SleeveDelta(
                intent_id=item.intent_id,
                sleeve_id=item.sleeve_id,
                current_quantity=current,
                target_quantity=item.target_quantity,
                signed_quantity=signed,
            )
        )
    net = sum((item.signed_quantity for item in deltas), _ZERO)
    available = _quantity(broker_quantity, f"broker holding {symbol}")
    if net < _ZERO and -net > available:
        raise ValueError(
            f"broker holding is insufficient for {symbol}: need {-net}, have {available}"
        )
    side = "BUY" if net > _ZERO else "SELL" if net < _ZERO else "NONE"
    internal_allocations, allocations = (
        _split_internal_and_external_allocations(deltas)
    )
    sealed_reference_price = _decimal(
        reference_price,
        "reference_price",
        minimum=_ZERO,
    )
    if internal_allocations and sealed_reference_price <= _ZERO:
        raise ValueError("an internal sleeve cross requires a positive reference price")
    payload = {
        "schemaVersion": LIVE_PORTFOLIO_PLAN_SCHEMA,
        "scopeId": normalized_scope,
        "portfolioId": normalized_portfolio,
        "portfolioHash": normalized_hash,
        "symbol": symbol,
        "side": side,
        "quantity": _decimal_text(abs(net)),
        "referencePrice": _decimal_text(sealed_reference_price),
        "sleeveDeltas": [
            {
                "intentId": item.intent_id,
                "sleeveId": item.sleeve_id,
                "currentQuantity": _decimal_text(item.current_quantity),
                "targetQuantity": _decimal_text(item.target_quantity),
                "signedQuantity": _decimal_text(item.signed_quantity),
            }
            for item in deltas
        ],
        "internalAllocations": [
            {
                "intentId": item.intent_id,
                "sleeveId": item.sleeve_id,
                "signedQuantity": _decimal_text(item.signed_quantity),
            }
            for item in internal_allocations
        ],
        "allocations": [
            {
                "intentId": item.intent_id,
                "sleeveId": item.sleeve_id,
                "signedQuantity": _decimal_text(item.signed_quantity),
            }
            for item in allocations
        ],
    }
    return SymbolNetPlan(
        plan_id=_hash(payload),
        schema_version=LIVE_PORTFOLIO_PLAN_SCHEMA,
        scope_id=normalized_scope,
        portfolio_id=normalized_portfolio,
        portfolio_hash=normalized_hash,
        symbol=symbol,
        side=side,
        quantity=abs(net),
        reference_price=sealed_reference_price,
        deltas=tuple(deltas),
        internal_allocations=internal_allocations,
        allocations=allocations,
    )


def _allocations_from_metadata(
    payload: Mapping[str, Any],
    field: str,
    *,
    allow_empty: bool,
) -> tuple[SleeveAllocation, ...]:
    raw = payload.get(field)
    if not isinstance(raw, list) or (not allow_empty and not raw):
        raise ValueError(f"portfolio execution {field} are required")
    result = tuple(
        SleeveAllocation(
            intent_id=_text(item.get("intentId"), "intentId"),
            sleeve_id=_text(item.get("sleeveId"), "sleeveId"),
            signed_quantity=_signed_quantity(
                item.get("signedQuantity"), "signedQuantity"
            ),
        )
        for item in raw
        if isinstance(item, Mapping)
    )
    if len(result) != len(raw) or any(
        item.signed_quantity == _ZERO for item in result
    ):
        raise ValueError(f"portfolio execution {field} are invalid")
    identities = [(item.sleeve_id, item.intent_id) for item in result]
    if len(identities) != len(set(identities)):
        raise ValueError(f"portfolio execution {field} identity is duplicated")
    return result


def validate_symbol_net_plan_metadata(payload: Mapping[str, Any]) -> SymbolNetPlan:
    """Validate and reconstruct the immutable plan carried to the POST edge."""

    if not isinstance(payload, Mapping):
        raise ValueError("portfolio execution metadata must be an object")
    schema_version = _text(payload.get("schemaVersion"), "schemaVersion")
    if schema_version != LIVE_PORTFOLIO_PLAN_SCHEMA:
        raise ValueError("portfolio execution schema is unsupported")
    scope_id = _text(payload.get("scopeId"), "scopeId")
    portfolio_id = _text(payload.get("portfolioId"), "portfolioId")
    portfolio_hash = _text(payload.get("portfolioHash"), "portfolioHash").lower()
    symbol = canonical_kis_symbol(payload.get("symbol"))
    side = _text(payload.get("side"), "side").upper()
    quantity = _quantity(payload.get("quantity"), "quantity")
    reference_price = _decimal(
        payload.get("referencePrice"),
        "referencePrice",
        minimum=_ZERO,
    )
    raw_deltas = payload.get("sleeveDeltas")
    if not isinstance(raw_deltas, list) or not raw_deltas:
        raise ValueError("portfolio execution sleeveDeltas are required")
    deltas: list[SleeveDelta] = []
    for raw in raw_deltas:
        if not isinstance(raw, Mapping):
            raise ValueError("portfolio execution sleeve delta is invalid")
        current = _quantity(raw.get("currentQuantity"), "currentQuantity")
        target = _quantity(raw.get("targetQuantity"), "targetQuantity")
        signed = _signed_quantity(raw.get("signedQuantity"), "signedQuantity")
        if target - current != signed:
            raise ValueError("portfolio execution sleeve delta does not conserve target")
        deltas.append(
            SleeveDelta(
                intent_id=_text(raw.get("intentId"), "intentId"),
                sleeve_id=_text(raw.get("sleeveId"), "sleeveId"),
                current_quantity=current,
                target_quantity=target,
                signed_quantity=signed,
            )
        )
    identities = [(item.sleeve_id, item.intent_id) for item in deltas]
    if len(identities) != len(set(identities)):
        raise ValueError("portfolio execution sleeve delta identity is duplicated")
    internal_allocations = _allocations_from_metadata(
        payload,
        "internalAllocations",
        allow_empty=True,
    )
    allocations = _allocations_from_metadata(
        payload,
        "allocations",
        allow_empty=True,
    )
    expected_internal, expected_allocations = (
        _split_internal_and_external_allocations(deltas)
    )
    if internal_allocations != expected_internal:
        raise ValueError(
            "portfolio execution internal allocations do not match sleeve deltas"
        )
    if allocations != expected_allocations:
        raise ValueError("portfolio execution allocations do not match sleeve deltas")
    if internal_allocations and reference_price <= _ZERO:
        raise ValueError(
            "portfolio execution internal cross requires a positive reference price"
        )
    net = sum((item.signed_quantity for item in deltas), _ZERO)
    expected_side = "BUY" if net > _ZERO else "SELL" if net < _ZERO else "NONE"
    if side != expected_side or quantity != abs(net):
        raise ValueError("portfolio execution side/quantity does not match sleeve net")
    canonical = {
        "schemaVersion": schema_version,
        "scopeId": scope_id,
        "portfolioId": portfolio_id,
        "portfolioHash": portfolio_hash,
        "symbol": symbol,
        "side": side,
        "quantity": _decimal_text(quantity),
        "referencePrice": _decimal_text(reference_price),
        "sleeveDeltas": [
            {
                "intentId": item.intent_id,
                "sleeveId": item.sleeve_id,
                "currentQuantity": _decimal_text(item.current_quantity),
                "targetQuantity": _decimal_text(item.target_quantity),
                "signedQuantity": _decimal_text(item.signed_quantity),
            }
            for item in deltas
        ],
        "internalAllocations": [
            {
                "intentId": item.intent_id,
                "sleeveId": item.sleeve_id,
                "signedQuantity": _decimal_text(item.signed_quantity),
            }
            for item in internal_allocations
        ],
        "allocations": [
            {
                "intentId": item.intent_id,
                "sleeveId": item.sleeve_id,
                "signedQuantity": _decimal_text(item.signed_quantity),
            }
            for item in allocations
        ],
    }
    plan_id = _text(payload.get("planId"), "planId")
    if _hash(canonical) != plan_id:
        raise ValueError("portfolio execution plan seal is invalid")
    return SymbolNetPlan(
        plan_id=plan_id,
        schema_version=schema_version,
        scope_id=scope_id,
        portfolio_id=portfolio_id,
        portfolio_hash=portfolio_hash,
        symbol=symbol,
        side=side,
        quantity=quantity,
        reference_price=reference_price,
        deltas=tuple(deltas),
        internal_allocations=internal_allocations,
        allocations=allocations,
    )


def _proportional_signed_quantities(
    allocations: Sequence[SleeveAllocation],
    cumulative_filled: Decimal,
    side: str,
) -> list[Decimal]:
    net = sum((item.signed_quantity for item in allocations), _ZERO)
    sign = _ONE if side == "BUY" else -_ONE
    if net * sign <= _ZERO:
        raise ValueError("allocation net does not match broker side")
    if cumulative_filled > abs(net):
        raise ValueError("cumulative fill exceeds accepted order")
    ratio = cumulative_filled / abs(net)
    ideals = [item.signed_quantity * ratio for item in allocations]
    actual = [
        abs(value).to_integral_value(rounding=ROUND_DOWN)
        * (_ONE if value >= _ZERO else -_ONE)
        for value in ideals
    ]
    difference = cumulative_filled * sign - sum(actual, _ZERO)
    while difference != _ZERO:
        direction = _ONE if difference > _ZERO else -_ONE
        candidates: list[tuple[Decimal, str, str, int]] = []
        for index, (allocation, ideal, allocated) in enumerate(
            zip(allocations, ideals, actual)
        ):
            if (
                direction > _ZERO
                and allocation.signed_quantity > _ZERO
                and allocated < allocation.signed_quantity
            ):
                score = ideal - allocated
            elif (
                direction < _ZERO
                and allocation.signed_quantity < _ZERO
                and allocated > allocation.signed_quantity
            ):
                score = allocated - ideal
            else:
                continue
            candidates.append(
                (score, allocation.sleeve_id, allocation.intent_id, index)
            )
        if not candidates:
            raise RuntimeError("unable to conserve sleeve fill quantity")
        candidates.sort(key=lambda item: (-item[0], item[1], item[2]))
        actual[candidates[0][3]] += direction
        difference -= direction
    return actual


def _allocate_money(total: Decimal, weights: Sequence[Decimal]) -> list[Decimal]:
    if total == _ZERO:
        return [_ZERO for _item in weights]
    denominator = sum(weights, _ZERO)
    if denominator <= _ZERO:
        raise ValueError("fee allocation requires a non-zero fill")
    result = [
        (total * weight / denominator).quantize(
            _MONEY_QUANTUM, rounding=ROUND_DOWN
        )
        for weight in weights
    ]
    result[-1] += total - sum(result, _ZERO)
    return result


@dataclass(frozen=True)
class PortfolioReconciliation:
    scope_id: str
    matched: bool
    ready: bool
    ledger_holdings: dict[str, Decimal]
    broker_holdings: dict[str, Decimal]
    external_holdings: dict[str, Decimal]
    mismatches: tuple[dict[str, str], ...]
    pending_orders: tuple[str, ...]
    observed_at: str
    content_hash: str


@dataclass(frozen=True)
class ExecutionSyncReport:
    applied_fills: int
    applied_statuses: int
    ignored_events: int
    applied_fee_adjustments: int = 0


class LivePortfolioLedger:
    """Append-only KIS portfolio/sleeve ledger with deterministic recovery."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def _initialize(self) -> None:
        with closing(self._connect()) as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS live_portfolio_scopes_v1 (
                    scope_id TEXT PRIMARY KEY,
                    portfolio_id TEXT NOT NULL,
                    portfolio_hash TEXT NOT NULL,
                    account_id TEXT NOT NULL,
                    broker_id TEXT NOT NULL CHECK(broker_id = 'kis'),
                    currency TEXT NOT NULL CHECK(currency = 'KRW'),
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS live_portfolio_orders_v1 (
                    plan_id TEXT PRIMARY KEY,
                    scope_id TEXT NOT NULL,
                    broker_order_id TEXT NOT NULL,
                    local_order_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL CHECK(side IN ('BUY', 'SELL')),
                    quantity TEXT NOT NULL,
                    allocations_json TEXT NOT NULL,
                    accepted_at TEXT NOT NULL,
                    UNIQUE(scope_id, broker_order_id),
                    FOREIGN KEY(scope_id) REFERENCES live_portfolio_scopes_v1(scope_id)
                );
                CREATE TABLE IF NOT EXISTS live_portfolio_events_v1 (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    scope_id TEXT NOT NULL,
                    level TEXT NOT NULL CHECK(level IN ('PORTFOLIO', 'SLEEVE')),
                    sleeve_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    previous_hash TEXT NOT NULL,
                    event_hash TEXT NOT NULL UNIQUE,
                    FOREIGN KEY(scope_id) REFERENCES live_portfolio_scopes_v1(scope_id)
                );
                CREATE INDEX IF NOT EXISTS live_portfolio_events_scope_sequence_v1
                    ON live_portfolio_events_v1(scope_id, sequence);
                CREATE TRIGGER IF NOT EXISTS live_portfolio_scopes_no_update_v1
                BEFORE UPDATE ON live_portfolio_scopes_v1
                BEGIN SELECT RAISE(ABORT, 'live portfolio scopes are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS live_portfolio_scopes_no_delete_v1
                BEFORE DELETE ON live_portfolio_scopes_v1
                BEGIN SELECT RAISE(ABORT, 'live portfolio scopes are append-only'); END;
                CREATE TRIGGER IF NOT EXISTS live_portfolio_orders_no_update_v1
                BEFORE UPDATE ON live_portfolio_orders_v1
                BEGIN SELECT RAISE(ABORT, 'live portfolio orders are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS live_portfolio_orders_no_delete_v1
                BEFORE DELETE ON live_portfolio_orders_v1
                BEGIN SELECT RAISE(ABORT, 'live portfolio orders are append-only'); END;
                CREATE TRIGGER IF NOT EXISTS live_portfolio_events_no_update_v1
                BEFORE UPDATE ON live_portfolio_events_v1
                BEGIN SELECT RAISE(ABORT, 'live portfolio events are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS live_portfolio_events_no_delete_v1
                BEFORE DELETE ON live_portfolio_events_v1
                BEGIN SELECT RAISE(ABORT, 'live portfolio events are append-only'); END;
                """
            )

    def register_scope(
        self,
        *,
        scope_id: str,
        portfolio_id: str,
        portfolio_hash: str,
        account_id: str,
    ) -> None:
        scope = {
            "scope_id": _text(scope_id, "scope_id"),
            "portfolio_id": _text(portfolio_id, "portfolio_id"),
            "portfolio_hash": _text(portfolio_hash, "portfolio_hash").lower(),
            "account_id": _text(account_id, "account_id"),
            "broker_id": "kis",
            "currency": "KRW",
        }
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM live_portfolio_scopes_v1 WHERE scope_id = ?",
                (scope["scope_id"],),
            ).fetchone()
            if row is None:
                connection.execute(
                    """
                    INSERT INTO live_portfolio_scopes_v1
                    (scope_id, portfolio_id, portfolio_hash, account_id,
                     broker_id, currency, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (*scope.values(), _now()),
                )
            else:
                for field, expected in scope.items():
                    if str(row[field]) != expected:
                        raise ValueError(f"portfolio scope mismatch: {field}")
            connection.commit()

    @staticmethod
    def _event_semantics(event: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "scopeId": event["scope_id"],
            "level": event["level"],
            "sleeveId": event["sleeve_id"],
            "eventType": event["event_type"],
            "symbol": event["symbol"],
            "payload": event["payload"],
            "occurredAt": event["occurred_at"],
        }

    def _append_events(
        self,
        connection: sqlite3.Connection,
        events: Sequence[Mapping[str, Any]],
    ) -> int:
        if not events:
            return 0
        existing: list[sqlite3.Row | None] = []
        for event in events:
            row = connection.execute(
                "SELECT * FROM live_portfolio_events_v1 WHERE event_id = ?",
                (_text(event.get("event_id"), "event_id"),),
            ).fetchone()
            if row is not None:
                stored = {
                    "scopeId": row["scope_id"],
                    "level": row["level"],
                    "sleeveId": row["sleeve_id"],
                    "eventType": row["event_type"],
                    "symbol": row["symbol"],
                    "payload": json.loads(row["payload_json"]),
                    "occurredAt": row["occurred_at"],
                }
                if stored != self._event_semantics(event):
                    raise ValueError(
                        f"event_id collision with different content: {event['event_id']}"
                    )
            existing.append(row)
        if any(row is not None for row in existing):
            if not all(row is not None for row in existing):
                raise ValueError(
                    "partial idempotent event group; reconciliation is required"
                )
            return 0
        scope_id = _text(events[0].get("scope_id"), "scope_id")
        if any(event.get("scope_id") != scope_id for event in events):
            raise ValueError("an event group cannot mix scopes")
        previous_row = connection.execute(
            """
            SELECT event_hash FROM live_portfolio_events_v1
            WHERE scope_id = ? ORDER BY sequence DESC LIMIT 1
            """,
            (scope_id,),
        ).fetchone()
        previous_hash = previous_row["event_hash"] if previous_row else "0" * 64
        for event in events:
            payload_json = _canonical_json(event["payload"])
            seal = {
                "schemaVersion": LIVE_PORTFOLIO_LEDGER_SCHEMA,
                "eventId": event["event_id"],
                **self._event_semantics(event),
                "previousHash": previous_hash,
            }
            event_hash = _hash(seal)
            connection.execute(
                """
                INSERT INTO live_portfolio_events_v1
                (event_id, scope_id, level, sleeve_id, event_type, symbol,
                 payload_json, occurred_at, previous_hash, event_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event["event_id"],
                    scope_id,
                    event["level"],
                    event["sleeve_id"],
                    event["event_type"],
                    event["symbol"],
                    payload_json,
                    event["occurred_at"],
                    previous_hash,
                    event_hash,
                ),
            )
            previous_hash = event_hash
        return len(events)

    @staticmethod
    def _allocations_from_payload(payload: Mapping[str, Any]) -> tuple[SleeveAllocation, ...]:
        return _allocations_from_metadata(
            payload,
            "allocations",
            allow_empty=False,
        )

    @staticmethod
    def _internal_cross_events(
        plan: SymbolNetPlan,
        *,
        occurred_at: str,
    ) -> list[dict[str, Any]]:
        if not plan.internal_allocations:
            return []
        if plan.reference_price <= _ZERO:
            raise ValueError("internal cross reference price must be positive")
        events: list[dict[str, Any]] = [{
            "event_id": f"{plan.scope_id}:cross:{plan.plan_id}:portfolio",
            "scope_id": plan.scope_id,
            "level": "PORTFOLIO",
            "sleeve_id": "",
            "event_type": "INTERNAL_CROSS",
            "symbol": plan.symbol,
            "payload": {
                "planId": plan.plan_id,
                "price": _decimal_text(plan.reference_price),
                "netQuantity": "0",
            },
            "occurred_at": occurred_at,
        }]
        for item in plan.internal_allocations:
            events.append({
                "event_id": (
                    f"{plan.scope_id}:cross:{plan.plan_id}:"
                    f"{item.sleeve_id}:{item.intent_id}"
                ),
                "scope_id": plan.scope_id,
                "level": "SLEEVE",
                "sleeve_id": item.sleeve_id,
                "event_type": "SLEEVE_INTERNAL_CROSS",
                "symbol": plan.symbol,
                "payload": {
                    "planId": plan.plan_id,
                    "intentId": item.intent_id,
                    "signedQuantity": _decimal_text(item.signed_quantity),
                    "price": _decimal_text(plan.reference_price),
                    "fee": "0",
                    "costStatus": "FINAL",
                },
                "occurred_at": occurred_at,
            })
        return events

    def record_accepted_order(
        self,
        plan: SymbolNetPlan,
        *,
        broker_order_id: str,
        local_order_id: str = "",
        occurred_at: Any = "",
    ) -> bool:
        if plan.quantity <= _ZERO or plan.side not in {"BUY", "SELL"}:
            raise ValueError("only a non-zero broker plan can be accepted")
        broker_id = _text(broker_order_id, "broker_order_id")
        local_id = str(local_order_id or "").strip()
        at = _occurred_at(occurred_at)
        metadata = plan.metadata()
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM live_portfolio_orders_v1 WHERE plan_id = ?",
                (plan.plan_id,),
            ).fetchone()
            if row is None:
                collision = connection.execute(
                    """
                    SELECT plan_id FROM live_portfolio_orders_v1
                    WHERE scope_id = ? AND broker_order_id = ?
                    """,
                    (plan.scope_id, broker_id),
                ).fetchone()
                if collision is not None:
                    raise ValueError("broker order id belongs to another plan")
                connection.execute(
                    """
                    INSERT INTO live_portfolio_orders_v1
                    (plan_id, scope_id, broker_order_id, local_order_id, symbol,
                     side, quantity, allocations_json, accepted_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        plan.plan_id,
                        plan.scope_id,
                        broker_id,
                        local_id,
                        plan.symbol,
                        plan.side,
                        _decimal_text(plan.quantity),
                        _canonical_json(metadata["allocations"]),
                        at,
                    ),
                )
                created = True
            else:
                # The first durable ACK time is part of the immutable event.
                # Replays must reuse it rather than hashing the replay time.
                at = str(row["accepted_at"])
                expected = (
                    plan.scope_id,
                    broker_id,
                    local_id,
                    plan.symbol,
                    plan.side,
                    _decimal_text(plan.quantity),
                    _canonical_json(metadata["allocations"]),
                )
                stored = tuple(
                    str(row[field])
                    for field in (
                        "scope_id",
                        "broker_order_id",
                        "local_order_id",
                        "symbol",
                        "side",
                        "quantity",
                        "allocations_json",
                    )
                )
                if stored != expected:
                    raise ValueError("plan id is already bound to another order")
                created = False
            event = {
                "event_id": f"{plan.scope_id}:order:{broker_id}",
                "scope_id": plan.scope_id,
                "level": "PORTFOLIO",
                "sleeve_id": "",
                "event_type": "BROKER_ORDER_ACCEPTED",
                "symbol": plan.symbol,
                "payload": {
                    **metadata,
                    "brokerOrderId": broker_id,
                    "localOrderId": local_id,
                },
                "occurred_at": at,
            }
            self._append_events(
                connection,
                [
                    event,
                    *self._internal_cross_events(plan, occurred_at=at),
                ],
            )
            connection.commit()
            return created

    def recover_accepted_orders(
        self, scope_id: str, dispatch_rows: Iterable[Mapping[str, Any]]
    ) -> int:
        """Rebuild an ACK mapping from the durable pre/post-dispatch journal."""

        recovered = 0
        for row in dispatch_rows:
            payload = row.get("portfolio_execution")
            if not isinstance(payload, Mapping):
                continue
            if str(payload.get("scopeId") or "") != scope_id:
                continue
            broker_order_id = str(row.get("broker_order_id") or "").strip()
            if broker_order_id in {"", "-"}:
                continue
            plan = validate_symbol_net_plan_metadata(payload)
            if plan.scope_id != scope_id:
                raise ValueError("recovered portfolio order scope is invalid")
            if plan.side not in {"BUY", "SELL"} or plan.quantity <= _ZERO:
                raise ValueError("recovered portfolio order allocation is invalid")
            recovered += int(
                self.record_accepted_order(
                    plan,
                    broker_order_id=broker_order_id,
                    local_order_id=str(row.get("order_id") or ""),
                    # The shared dispatch journal historically stored local,
                    # timezone-naive text.  It is provenance, not a safe
                    # hash-chain clock; stamp the first recovery in UTC.
                    occurred_at="",
                )
            )
        return recovered

    def record_internal_cross(
        self,
        plan: SymbolNetPlan,
        *,
        price: Any,
        occurred_at: Any = "",
    ) -> int:
        if not plan.internal_only:
            raise ValueError("internal cross requires a zero-net plan")
        fill_price = _decimal(
            price if price not in {None, ""} else plan.reference_price,
            "price",
            minimum=_ZERO,
        )
        if fill_price == _ZERO:
            raise ValueError("price must be greater than zero")
        if fill_price != plan.reference_price:
            raise ValueError("internal cross price does not match sealed plan")
        at = _occurred_at(occurred_at)
        events = self._internal_cross_events(plan, occurred_at=at)
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            count = self._append_events(connection, events)
            connection.commit()
            return count

    @staticmethod
    def _row_allocations(row: sqlite3.Row) -> tuple[SleeveAllocation, ...]:
        return LivePortfolioLedger._allocations_from_payload(
            {"allocations": json.loads(row["allocations_json"])}
        )

    def _filled_quantities(
        self, connection: sqlite3.Connection, scope_id: str
    ) -> dict[str, Decimal]:
        rows = connection.execute(
            """
            SELECT payload_json FROM live_portfolio_events_v1
            WHERE scope_id = ? AND level = 'PORTFOLIO'
              AND event_type = 'BROKER_FILL'
            """,
            (scope_id,),
        ).fetchall()
        result: dict[str, Decimal] = {}
        for row in rows:
            payload = json.loads(row["payload_json"])
            plan_id = str(payload.get("planId") or "")
            result[plan_id] = result.get(plan_id, _ZERO) + _quantity(
                payload.get("brokerQuantity"), "stored broker fill"
            )
        return result

    def apply_execution_events(
        self, scope_id: str, events: Iterable[Mapping[str, Any]]
    ) -> ExecutionSyncReport:
        normalized_scope = _text(scope_id, "scope_id")
        candidate_rows = [
            dict(item) for item in events if isinstance(item, Mapping)
        ]
        applied_fills = 0
        applied_statuses = 0
        applied_fee_adjustments = 0
        ignored = 0
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            orders = {
                str(row["broker_order_id"]): row
                for row in connection.execute(
                    "SELECT * FROM live_portfolio_orders_v1 WHERE scope_id = ?",
                    (normalized_scope,),
                ).fetchall()
            }
            filled = self._filled_quantities(connection, normalized_scope)
            rows: list[dict[str, Any]] = []
            for event in candidate_rows:
                if str(event.get("broker_id") or "").strip().lower() != "kis":
                    ignored += 1
                    continue
                broker_order_id = str(
                    event.get("broker_order_id")
                    or event.get("brokerOrderId")
                    or ""
                ).strip()
                if broker_order_id not in orders:
                    ignored += 1
                    continue
                event["_broker_order_id"] = broker_order_id
                event["_canonical_occurred_at"] = _occurred_at(
                    event.get("occurred_at")
                )
                rows.append(event)
            rows.sort(
                key=lambda item: (
                    str(item["_canonical_occurred_at"]),
                    str(item.get("event_id") or ""),
                )
            )
            for event in rows:
                event_at = str(event["_canonical_occurred_at"])
                broker_order_id = str(event["_broker_order_id"])
                order = orders[broker_order_id]
                source_event_id = _text(event.get("event_id"), "execution event_id")
                symbol = canonical_kis_symbol(event.get("symbol") or order["symbol"])
                if symbol != str(order["symbol"]):
                    raise ValueError("execution symbol does not match accepted order")
                side = str(event.get("side") or "").strip().upper()
                if side and side != str(order["side"]):
                    raise ValueError("execution side does not match accepted order")
                state = str(event.get("state") or event.get("status") or "").upper()
                quantity = _quantity(event.get("quantity") or 0, "execution quantity")
                price = _decimal(event.get("price") or 0, "execution price", minimum=_ZERO)
                raw = event.get("raw") if isinstance(event.get("raw"), Mapping) else {}
                fee = _decimal(
                    event.get("fee")
                    if event.get("fee") not in {None, ""}
                    else raw.get("fee") or 0,
                    "execution fee",
                    minimum=_ZERO,
                )
                plan_id = str(order["plan_id"])
                event_prefix = f"{normalized_scope}:execution:{source_event_id}"
                if quantity > _ZERO:
                    already_recorded = connection.execute(
                        "SELECT payload_json FROM live_portfolio_events_v1 WHERE event_id = ?",
                        (f"{event_prefix}:portfolio",),
                    ).fetchone()
                    if already_recorded is not None:
                        stored_fill = json.loads(already_recorded["payload_json"])
                        stored_quantity = _quantity(
                            stored_fill.get("brokerQuantity"),
                            "stored broker fill quantity",
                            allow_zero=False,
                        )
                        stored_price = _decimal(
                            stored_fill.get("price"),
                            "stored broker fill price",
                            minimum=_ZERO,
                        )
                        if stored_quantity != quantity or stored_price != price:
                            raise ValueError(
                                "execution event replay changed immutable fill quantity/price"
                            )
                        adjustment_rows = connection.execute(
                            """
                            SELECT payload_json FROM live_portfolio_events_v1
                            WHERE scope_id = ? AND level = 'PORTFOLIO'
                              AND event_type = 'FEE_ADJUSTMENT'
                            """,
                            (normalized_scope,),
                        ).fetchall()
                        current_fee = _decimal(
                            stored_fill.get("fee") or 0,
                            "stored fill fee",
                            minimum=_ZERO,
                        )
                        for adjustment_row in adjustment_rows:
                            adjustment = json.loads(
                                adjustment_row["payload_json"]
                            )
                            if (
                                str(adjustment.get("sourceEventId") or "")
                                == source_event_id
                            ):
                                current_fee += _decimal(
                                    adjustment.get("feeDelta") or 0,
                                    "stored fee adjustment",
                                )
                        # A reconnect may replay the original fee-less KIS
                        # notice after an exact cost revision.  Zero is an
                        # absence of cost evidence here, never a rollback.
                        if fee == _ZERO and current_fee > _ZERO:
                            continue
                        if fee != current_fee:
                            fee_delta = fee - current_fee
                            stored_allocations = [
                                item
                                for item in stored_fill.get("allocations", [])
                                if isinstance(item, Mapping)
                            ]
                            if not stored_allocations:
                                raise ValueError(
                                    "stored fill has no sleeve allocation for fee revision"
                                )
                            allocated_deltas = _allocate_money(
                                fee_delta,
                                [
                                    abs(
                                        _signed_quantity(
                                            item.get("signedQuantity"),
                                            "stored signed fill",
                                        )
                                    )
                                    for item in stored_allocations
                                ],
                            )
                            revision_id = _hash({
                                "scopeId": normalized_scope,
                                "sourceEventId": source_event_id,
                                "quantity": _decimal_text(quantity),
                                "price": _decimal_text(price),
                                "previousFee": _decimal_text(current_fee),
                                "revisedFee": _decimal_text(fee),
                            })
                            revision_prefix = (
                                f"{event_prefix}:fee-adjustment:{revision_id}"
                            )
                            fee_allocations = [
                                {
                                    "intentId": str(item.get("intentId") or ""),
                                    "sleeveId": str(item.get("sleeveId") or ""),
                                    "sourceSignedQuantity": str(
                                        item.get("signedQuantity") or "0"
                                    ),
                                    "feeDelta": _decimal_text(delta),
                                }
                                for item, delta in zip(
                                    stored_allocations, allocated_deltas
                                )
                            ]
                            revision_events: list[dict[str, Any]] = [{
                                "event_id": f"{revision_prefix}:portfolio",
                                "scope_id": normalized_scope,
                                "level": "PORTFOLIO",
                                "sleeve_id": "",
                                "event_type": "FEE_ADJUSTMENT",
                                "symbol": symbol,
                                "payload": {
                                    "planId": plan_id,
                                    "brokerOrderId": broker_order_id,
                                    "sourceEventId": source_event_id,
                                    "brokerQuantity": _decimal_text(quantity),
                                    "price": _decimal_text(price),
                                    "previousFee": _decimal_text(current_fee),
                                    "revisedFee": _decimal_text(fee),
                                    "feeDelta": _decimal_text(fee_delta),
                                    "costStatus": "FINAL",
                                    "allocations": fee_allocations,
                                },
                                "occurred_at": event_at,
                            }]
                            for allocation in fee_allocations:
                                revision_events.append({
                                    "event_id": (
                                        f"{revision_prefix}:sleeve:"
                                        f"{allocation['sleeveId']}:"
                                        f"{allocation['intentId']}"
                                    ),
                                    "scope_id": normalized_scope,
                                    "level": "SLEEVE",
                                    "sleeve_id": allocation["sleeveId"],
                                    "event_type": "SLEEVE_FEE_ADJUSTMENT",
                                    "symbol": symbol,
                                    "payload": {
                                        "planId": plan_id,
                                        "brokerOrderId": broker_order_id,
                                        "sourceEventId": source_event_id,
                                        **allocation,
                                        "costStatus": "FINAL",
                                    },
                                    "occurred_at": event_at,
                                })
                            applied_fee_adjustments += int(
                                bool(
                                    self._append_events(
                                        connection, revision_events
                                    )
                                )
                            )
                        continue
                    if price <= _ZERO:
                        raise ValueError("a broker fill requires a positive price")
                    order_quantity = _quantity(order["quantity"], "accepted quantity")
                    previous = filled.get(plan_id, _ZERO)
                    cumulative = previous + quantity
                    if cumulative > order_quantity:
                        raise ValueError("cumulative execution exceeds accepted order")
                    allocations = self._row_allocations(order)
                    cumulative_signed = _proportional_signed_quantities(
                        allocations, cumulative, str(order["side"])
                    )
                    previous_signed = (
                        _proportional_signed_quantities(
                            allocations, previous, str(order["side"])
                        )
                        if previous > _ZERO
                        else [_ZERO for _item in allocations]
                    )
                    increments = [
                        current - prior
                        for current, prior in zip(cumulative_signed, previous_signed)
                    ]
                    active = [
                        (allocation, signed)
                        for allocation, signed in zip(allocations, increments)
                        if signed != _ZERO
                    ]
                    fees = _allocate_money(fee, [abs(item[1]) for item in active])
                    allocation_payload = [
                        {
                            "intentId": allocation.intent_id,
                            "sleeveId": allocation.sleeve_id,
                            "signedQuantity": _decimal_text(signed),
                            "price": _decimal_text(price),
                            "fee": _decimal_text(allocated_fee),
                        }
                        for (allocation, signed), allocated_fee in zip(active, fees)
                    ]
                    cost_status = "FINAL" if fee > _ZERO else "UNKNOWN"
                    ledger_events: list[dict[str, Any]] = [{
                        "event_id": f"{event_prefix}:portfolio",
                        "scope_id": normalized_scope,
                        "level": "PORTFOLIO",
                        "sleeve_id": "",
                        "event_type": "BROKER_FILL",
                        "symbol": symbol,
                        "payload": {
                            "planId": plan_id,
                            "brokerOrderId": broker_order_id,
                            "sourceEventId": source_event_id,
                            "side": order["side"],
                            "brokerQuantity": _decimal_text(quantity),
                            "price": _decimal_text(price),
                            "fee": _decimal_text(fee),
                            "costStatus": cost_status,
                            "allocations": allocation_payload,
                        },
                        "occurred_at": event_at,
                    }]
                    for allocation in allocation_payload:
                        ledger_events.append({
                            "event_id": (
                                f"{event_prefix}:sleeve:"
                                f"{allocation['sleeveId']}:{allocation['intentId']}"
                            ),
                            "scope_id": normalized_scope,
                            "level": "SLEEVE",
                            "sleeve_id": allocation["sleeveId"],
                            "event_type": "SLEEVE_FILL",
                            "symbol": symbol,
                            "payload": {
                                "planId": plan_id,
                                "brokerOrderId": broker_order_id,
                                "sourceEventId": source_event_id,
                                **allocation,
                                "costStatus": cost_status,
                            },
                            "occurred_at": event_at,
                        })
                    inserted = self._append_events(connection, ledger_events)
                    if inserted:
                        filled[plan_id] = cumulative
                        applied_fills += 1
                if state in {"CANCELED", "CANCELLED", "REJECTED", "EXPIRED"}:
                    status_event = {
                        "event_id": f"{event_prefix}:status",
                        "scope_id": normalized_scope,
                        "level": "PORTFOLIO",
                        "sleeve_id": "",
                        "event_type": "BROKER_ORDER_TERMINAL",
                        "symbol": symbol,
                        "payload": {
                            "planId": plan_id,
                            "brokerOrderId": broker_order_id,
                            "sourceEventId": source_event_id,
                            "state": "CANCELED" if state == "CANCELLED" else state,
                        },
                        "occurred_at": event_at,
                    }
                    applied_statuses += int(
                        bool(self._append_events(connection, [status_event]))
                    )
            connection.commit()
        return ExecutionSyncReport(
            applied_fills,
            applied_statuses,
            ignored,
            applied_fee_adjustments,
        )

    def _events(self, scope_id: str) -> list[sqlite3.Row]:
        with closing(self._connect()) as connection:
            return connection.execute(
                """
                SELECT * FROM live_portfolio_events_v1
                WHERE scope_id = ? ORDER BY sequence
                """,
                (_text(scope_id, "scope_id"),),
            ).fetchall()

    def verify_hash_chain(self, scope_id: str) -> tuple[int, str]:
        previous = "0" * 64
        count = 0
        for row in self._events(scope_id):
            if row["previous_hash"] != previous:
                raise ValueError(f"portfolio ledger hash chain broke at {row['sequence']}")
            payload = json.loads(row["payload_json"])
            semantics = {
                "scopeId": row["scope_id"],
                "level": row["level"],
                "sleeveId": row["sleeve_id"],
                "eventType": row["event_type"],
                "symbol": row["symbol"],
                "payload": payload,
                "occurredAt": row["occurred_at"],
            }
            expected = _hash({
                "schemaVersion": LIVE_PORTFOLIO_LEDGER_SCHEMA,
                "eventId": row["event_id"],
                **semantics,
                "previousHash": previous,
            })
            if expected != row["event_hash"]:
                raise ValueError(f"portfolio ledger event hash failed at {row['sequence']}")
            previous = row["event_hash"]
            count += 1
        return count, previous

    def sleeve_holdings(self, scope_id: str) -> dict[str, dict[str, Decimal]]:
        result: dict[str, dict[str, Decimal]] = {}
        for row in self._events(scope_id):
            if row["level"] != "SLEEVE" or row["event_type"] not in {
                "SLEEVE_FILL",
                "SLEEVE_INTERNAL_CROSS",
            }:
                continue
            payload = json.loads(row["payload_json"])
            quantity = _signed_quantity(payload.get("signedQuantity"), "signedQuantity")
            positions = result.setdefault(str(row["sleeve_id"]), {})
            symbol = str(row["symbol"])
            positions[symbol] = positions.get(symbol, _ZERO) + quantity
            if positions[symbol] < _ZERO:
                raise ValueError(f"sleeve ledger reconstructs a short: {row['sleeve_id']}/{symbol}")
        return result

    def sleeve_quantity(self, scope_id: str, sleeve_id: str, symbol: str) -> Decimal:
        return self.sleeve_holdings(scope_id).get(
            _text(sleeve_id, "sleeve_id"), {}
        ).get(canonical_kis_symbol(symbol), _ZERO)

    def balances(self, scope_id: str) -> dict[str, Any]:
        sleeves: dict[str, dict[str, Decimal]] = {}
        for row in self._events(scope_id):
            if row["level"] != "SLEEVE":
                continue
            payload = json.loads(row["payload_json"])
            if row["event_type"] == "SLEEVE_FEE_ADJUSTMENT":
                fee_delta = _decimal(
                    payload.get("feeDelta") or 0,
                    "fee adjustment",
                )
                row_balance = sleeves.setdefault(
                    str(row["sleeve_id"]),
                    {"cashFlow": _ZERO, "fees": _ZERO, "turnover": _ZERO},
                )
                row_balance["cashFlow"] -= fee_delta
                row_balance["fees"] += fee_delta
                continue
            if row["event_type"] not in {
                "SLEEVE_FILL",
                "SLEEVE_INTERNAL_CROSS",
            }:
                continue
            quantity = _signed_quantity(payload.get("signedQuantity"), "signedQuantity")
            price = _decimal(payload.get("price"), "price", minimum=_ZERO)
            fee = _decimal(payload.get("fee") or 0, "fee", minimum=_ZERO)
            row_balance = sleeves.setdefault(
                str(row["sleeve_id"]),
                {"cashFlow": _ZERO, "fees": _ZERO, "turnover": _ZERO},
            )
            row_balance["cashFlow"] += -(quantity * price) - fee
            row_balance["fees"] += fee
            row_balance["turnover"] += abs(quantity * price)
        portfolio = {
            key: sum((values[key] for values in sleeves.values()), _ZERO)
            for key in ("cashFlow", "fees", "turnover")
        }
        return {"portfolio": portfolio, "sleeves": sleeves}

    def pending_orders(self, scope_id: str) -> tuple[str, ...]:
        normalized_scope = _text(scope_id, "scope_id")
        with closing(self._connect()) as connection:
            orders = connection.execute(
                "SELECT * FROM live_portfolio_orders_v1 WHERE scope_id = ?",
                (normalized_scope,),
            ).fetchall()
            filled = self._filled_quantities(connection, normalized_scope)
            terminal_rows = connection.execute(
                """
                SELECT payload_json FROM live_portfolio_events_v1
                WHERE scope_id = ? AND event_type = 'BROKER_ORDER_TERMINAL'
                """,
                (normalized_scope,),
            ).fetchall()
        terminal = {
            str(json.loads(row["payload_json"]).get("planId") or "")
            for row in terminal_rows
        }
        return tuple(
            str(row["broker_order_id"])
            for row in orders
            if filled.get(str(row["plan_id"]), _ZERO)
            < _quantity(row["quantity"], "accepted quantity")
            and str(row["plan_id"]) not in terminal
        )

    def reconcile_restart(
        self,
        *,
        scope_id: str,
        broker_holdings: Mapping[str, Any],
        managed_symbols: Iterable[str],
        persist: bool = True,
        observed_at: Any = "",
    ) -> PortfolioReconciliation:
        normalized_scope = _text(scope_id, "scope_id")
        count, head = self.verify_hash_chain(normalized_scope)
        managed = {canonical_kis_symbol(item) for item in managed_symbols}
        sleeves = self.sleeve_holdings(normalized_scope)
        ledger: dict[str, Decimal] = {}
        for positions in sleeves.values():
            for symbol, quantity in positions.items():
                ledger[symbol] = ledger.get(symbol, _ZERO) + quantity
        broker: dict[str, Decimal] = {}
        for raw_symbol, raw_quantity in broker_holdings.items():
            symbol = canonical_kis_symbol(raw_symbol)
            broker[symbol] = broker.get(symbol, _ZERO) + _quantity(
                raw_quantity, f"broker holding {symbol}"
            )
        external = {
            symbol: quantity
            for symbol, quantity in broker.items()
            if symbol not in managed and quantity != _ZERO
        }
        comparison_symbols = managed | set(ledger)
        mismatches = tuple(
            {
                "symbol": symbol,
                "ledgerQuantity": _decimal_text(ledger.get(symbol, _ZERO)),
                "brokerQuantity": _decimal_text(broker.get(symbol, _ZERO)),
            }
            for symbol in sorted(comparison_symbols)
            if ledger.get(symbol, _ZERO) != broker.get(symbol, _ZERO)
        )
        pending = self.pending_orders(normalized_scope)
        at = _occurred_at(observed_at)
        payload = {
            "schemaVersion": LIVE_PORTFOLIO_RECONCILIATION_SCHEMA,
            "scopeId": normalized_scope,
            "matched": not mismatches,
            # KIS cash holdings cannot be attributed safely when another
            # program/manual trader owns any non-zero symbol in this account.
            # The first multi-sleeve implementation therefore uses an
            # explicit dedicated-account policy, including other symbols.
            "ready": not mismatches and not pending and not external,
            "ledgerHoldings": {
                key: _decimal_text(value) for key, value in sorted(ledger.items())
            },
            "brokerHoldings": {
                key: _decimal_text(value) for key, value in sorted(broker.items())
            },
            "externalHoldings": {
                key: _decimal_text(value) for key, value in sorted(external.items())
            },
            "mismatches": list(mismatches),
            "pendingOrders": list(pending),
            "ledgerEventCount": count,
            "ledgerHeadHash": head,
            "observedAt": at,
        }
        content_hash = _hash(payload)
        result = PortfolioReconciliation(
            scope_id=normalized_scope,
            matched=not mismatches,
            ready=not mismatches and not pending and not external,
            ledger_holdings=ledger,
            broker_holdings=broker,
            external_holdings=external,
            mismatches=mismatches,
            pending_orders=pending,
            observed_at=at,
            content_hash=content_hash,
        )
        if persist:
            event = {
                "event_id": f"{normalized_scope}:reconcile:{content_hash}",
                "scope_id": normalized_scope,
                "level": "PORTFOLIO",
                "sleeve_id": "",
                "event_type": "RESTART_RECONCILIATION",
                "symbol": "",
                "payload": {**payload, "contentHash": content_hash},
                "occurred_at": at,
            }
            with self._lock, closing(self._connect()) as connection:
                connection.execute("BEGIN IMMEDIATE")
                self._append_events(connection, [event])
                connection.commit()
        return result
