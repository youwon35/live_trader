from __future__ import annotations

import json
import hashlib
import math
import secrets
import sqlite3
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from contextlib import contextmanager
from typing import Any, Callable, Mapping

from .binance_cash_transfer_evidence import (
    AUTHORITY_SCHEMA as BINANCE_TRANSFER_AUTHORITY_SCHEMA,
    HIGH_WATER_REQUEST_SCHEMA as BINANCE_TRANSFER_HIGH_WATER_REQUEST_SCHEMA,
    HIGH_WATER_SCHEMA as BINANCE_TRANSFER_HIGH_WATER_SCHEMA,
    INTERNAL_SUCCESS_RESULT as BINANCE_TRANSFER_SUCCESS_RESULT,
    OFFICIAL_SUCCESS_STATUS as BINANCE_TRANSFER_SUCCESS_STATUS,
    OFFICIAL_TRANSFER_TYPE as BINANCE_TRANSFER_TYPE,
    TRUTH_SCHEMA as BINANCE_TRANSFER_TRUTH_SCHEMA,
    content_hash as binance_transfer_content_hash,
    normalized_transfer_fields as binance_transfer_record_fields,
    truth_fields as binance_transfer_truth_fields,
)


BINANCE_CASH_TRANSFER_ADJUSTMENT_RELEASED = False
BINANCE_CASH_TRANSFER_CONSUMPTION_RELEASED = False
MAX_CASH_TRANSFER_TRUTH_AGE_SECONDS = 30.0
CRYPTO_FIRST_LIVE_GLOBAL_IDLE_RESERVATION_SCHEMA = (
    "crypto-first-live-global-idle-reservation/v1"
)


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _utc_observation_text(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError("functional-test-equity-observed-at-required")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(
            "functional-test-equity-observed-at-invalid"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("functional-test-equity-observed-at-timezone-required")
    return parsed.astimezone(timezone.utc).isoformat()


def _functional_equity_scope_key(permit_id: str, account_fingerprint: str) -> str:
    return hashlib.sha256(
        f"{permit_id}\x00{account_fingerprint}".encode("utf-8")
    ).hexdigest()


def _functional_equity_integrity_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def numeric_value(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def normalized_position_side(row: dict[str, Any]) -> str:
    broker_id = str(row.get("broker_id") or "").strip().lower()
    if broker_id != "binance-futures":
        return ""
    value = str(
        row.get("position_side")
        or row.get("positionSide")
        or ""
    ).strip().upper()
    return value if value in {"BOTH", "LONG", "SHORT"} else ""


class ProgramLedger:
    """Small local ledger for live account/position/event reconciliation."""

    def __init__(
        self,
        path: Path,
        *,
        cash_transfer_authority_verifier: (
            Callable[[Mapping[str, Any]], bool] | None
        ) = None,
        cash_transfer_high_water_verifier: (
            Callable[[Mapping[str, Any]], bool] | None
        ) = None,
        cash_transfer_global_idle_reserver: (
            Callable[[Mapping[str, Any]], Any] | None
        ) = None,
    ) -> None:
        self.path = Path(path)
        self.cash_transfer_authority_verifier = (
            cash_transfer_authority_verifier
        )
        self.cash_transfer_high_water_verifier = (
            cash_transfer_high_water_verifier
        )
        self.cash_transfer_global_idle_reserver = (
            cash_transfer_global_idle_reserver
        )
        self.ensure_schema()

    def connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    @contextmanager
    def connection(self):
        conn = self.connect()
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def ensure_schema(self) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS cash_balances (
                    broker_id TEXT NOT NULL,
                    account TEXT NOT NULL,
                    currency TEXT NOT NULL,
                    cash REAL NOT NULL,
                    updated_at TEXT NOT NULL,
                    source TEXT NOT NULL,
                    PRIMARY KEY (broker_id, account, currency)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS positions (
                    broker_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    position_side TEXT NOT NULL DEFAULT '',
                    asset TEXT NOT NULL,
                    currency TEXT NOT NULL,
                    quantity REAL NOT NULL,
                    value REAL NOT NULL,
                    updated_at TEXT NOT NULL,
                    source TEXT NOT NULL,
                    PRIMARY KEY (broker_id, symbol, position_side)
                )
                """
            )
            position_columns = {
                str(row[1]): int(row[5] or 0)
                for row in conn.execute(
                    "PRAGMA table_info(positions)"
                ).fetchall()
            }
            position_primary_key = [
                name
                for name, _ in sorted(
                    (
                        (str(row[1]), int(row[5] or 0))
                        for row in conn.execute(
                            "PRAGMA table_info(positions)"
                        ).fetchall()
                        if int(row[5] or 0) > 0
                    ),
                    key=lambda item: item[1],
                )
            ]
            if (
                "position_side" not in position_columns
                or position_primary_key
                != ["broker_id", "symbol", "position_side"]
            ):
                side_expression = (
                    "COALESCE(position_side, '')"
                    if "position_side" in position_columns
                    else "''"
                )
                conn.execute(
                    """
                    CREATE TABLE positions_position_side_v2 (
                        broker_id TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        position_side TEXT NOT NULL DEFAULT '',
                        asset TEXT NOT NULL,
                        currency TEXT NOT NULL,
                        quantity REAL NOT NULL,
                        value REAL NOT NULL,
                        updated_at TEXT NOT NULL,
                        source TEXT NOT NULL,
                        PRIMARY KEY (broker_id, symbol, position_side)
                    )
                    """
                )
                conn.execute(
                    f"""
                    INSERT OR REPLACE INTO positions_position_side_v2
                    (broker_id, symbol, position_side, asset, currency,
                     quantity, value, updated_at, source)
                    SELECT broker_id, symbol, {side_expression}, asset,
                           currency, quantity, value, updated_at, source
                    FROM positions
                    """
                )
                conn.execute("DROP TABLE positions")
                conn.execute(
                    "ALTER TABLE positions_position_side_v2 RENAME TO positions"
                )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS execution_events (
                    event_id TEXT PRIMARY KEY,
                    broker_id TEXT NOT NULL,
                    order_id TEXT NOT NULL,
                    broker_order_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    quantity REAL NOT NULL,
                    price REAL NOT NULL,
                    state TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    trace_id TEXT NOT NULL DEFAULT '',
                    raw_json TEXT NOT NULL
                )
                """
            )
            columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(execution_events)").fetchall()}
            if "trace_id" not in columns:
                conn.execute("ALTER TABLE execution_events ADD COLUMN trace_id TEXT NOT NULL DEFAULT ''")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS order_gate_events (
                    event_id TEXT PRIMARY KEY,
                    order_id TEXT NOT NULL,
                    strategy_id TEXT NOT NULL,
                    broker_id TEXT NOT NULL,
                    broker_order_id TEXT NOT NULL DEFAULT '',
                    mode TEXT NOT NULL DEFAULT '',
                    dry_run INTEGER NOT NULL DEFAULT 0,
                    execution_purpose TEXT NOT NULL DEFAULT '',
                    promotion_eligible INTEGER NOT NULL DEFAULT 1,
                    state TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    canary_scope_json TEXT NOT NULL
                )
                """
            )
            gate_columns = {
                str(row[1])
                for row in conn.execute(
                    "PRAGMA table_info(order_gate_events)"
                ).fetchall()
            }
            if "broker_order_id" not in gate_columns:
                conn.execute(
                    "ALTER TABLE order_gate_events "
                    "ADD COLUMN broker_order_id TEXT NOT NULL DEFAULT ''"
                )
            if "mode" not in gate_columns:
                conn.execute(
                    "ALTER TABLE order_gate_events "
                    "ADD COLUMN mode TEXT NOT NULL DEFAULT ''"
                )
            if "dry_run" not in gate_columns:
                conn.execute(
                    "ALTER TABLE order_gate_events "
                    "ADD COLUMN dry_run INTEGER NOT NULL DEFAULT 0"
                )
            if "execution_purpose" not in gate_columns:
                conn.execute(
                    "ALTER TABLE order_gate_events "
                    "ADD COLUMN execution_purpose TEXT NOT NULL DEFAULT ''"
                )
            if "promotion_eligible" not in gate_columns:
                conn.execute(
                    "ALTER TABLE order_gate_events "
                    "ADD COLUMN promotion_eligible INTEGER NOT NULL DEFAULT 1"
                )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS order_dispatch_journal (
                    order_id TEXT PRIMARY KEY,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    broker_id TEXT NOT NULL,
                    broker_order_id TEXT NOT NULL DEFAULT '',
                    state TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    order_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS functional_test_order_reservations (
                    idempotency_key TEXT PRIMARY KEY,
                    permit_id TEXT NOT NULL,
                    order_id TEXT NOT NULL,
                    reserved_at TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'RESERVED'
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS
                    functional_test_reservations_permit_idx
                ON functional_test_order_reservations
                   (permit_id, reserved_at, idempotency_key)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS
                    order_dispatch_journal_state_updated_idx
                ON order_dispatch_journal (state, updated_at)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS functional_test_equity_scopes (
                    scope_key TEXT PRIMARY KEY,
                    permit_id TEXT NOT NULL,
                    account_fingerprint TEXT NOT NULL,
                    starting_equity REAL NOT NULL,
                    peak_equity REAL NOT NULL,
                    current_equity REAL NOT NULL,
                    worst_drawdown REAL NOT NULL,
                    first_observed_at TEXT NOT NULL,
                    last_observed_at TEXT NOT NULL,
                    observation_count INTEGER NOT NULL,
                    latest_observation_hash TEXT NOT NULL,
                    integrity_hash TEXT NOT NULL,
                    UNIQUE (permit_id, account_fingerprint)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS functional_test_equity_observations (
                    event_id TEXT PRIMARY KEY,
                    scope_key TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    equity REAL NOT NULL,
                    peak_equity REAL NOT NULL,
                    drawdown REAL NOT NULL,
                    worst_drawdown REAL NOT NULL,
                    previous_hash TEXT NOT NULL,
                    content_hash TEXT NOT NULL UNIQUE,
                    FOREIGN KEY (scope_key)
                        REFERENCES functional_test_equity_scopes(scope_key)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS
                    functional_test_equity_observations_scope_idx
                ON functional_test_equity_observations
                   (scope_key, observed_at, event_id)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS functional_test_authority_events (
                    event_id TEXT PRIMARY KEY,
                    permit_id TEXT NOT NULL,
                    account_fingerprint TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    reason TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS cash_transfer_adjustments (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    adjustment_id TEXT NOT NULL UNIQUE,
                    source_broker_id TEXT NOT NULL,
                    source_account TEXT NOT NULL,
                    destination_broker_id TEXT NOT NULL,
                    destination_account TEXT NOT NULL,
                    currency TEXT NOT NULL,
                    amount_text TEXT NOT NULL,
                    source_cash_before_text TEXT NOT NULL,
                    source_cash_after_text TEXT NOT NULL,
                    destination_cash_before_text TEXT NOT NULL,
                    destination_cash_after_text TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    truth_evidence_json TEXT NOT NULL,
                    truth_hash TEXT NOT NULL UNIQUE,
                    previous_hash TEXT NOT NULL,
                    content_json TEXT NOT NULL,
                    content_hash TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS cash_transfer_adjustments_created_idx
                ON cash_transfer_adjustments(created_at, sequence)
                """
            )
            conn.execute(
                """
                CREATE TRIGGER IF NOT EXISTS cash_transfer_adjustments_no_update
                BEFORE UPDATE ON cash_transfer_adjustments
                BEGIN
                    SELECT RAISE(ABORT, 'cash-transfer-adjustments-append-only');
                END
                """
            )
            conn.execute(
                """
                CREATE TRIGGER IF NOT EXISTS cash_transfer_adjustments_no_delete
                BEFORE DELETE ON cash_transfer_adjustments
                BEGIN
                    SELECT RAISE(ABORT, 'cash-transfer-adjustments-append-only');
                END
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS binance_cash_transfer_consumptions (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    consumption_key TEXT NOT NULL UNIQUE,
                    adjustment_id TEXT NOT NULL UNIQUE,
                    account_fingerprint TEXT NOT NULL,
                    api_key_fingerprint TEXT NOT NULL,
                    transfer_type TEXT NOT NULL,
                    transfer_timestamp INTEGER NOT NULL,
                    transfer_id TEXT NOT NULL,
                    truth_hash TEXT NOT NULL UNIQUE,
                    previous_hash TEXT NOT NULL,
                    content_json TEXT NOT NULL,
                    content_hash TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (adjustment_id)
                        REFERENCES cash_transfer_adjustments(adjustment_id)
                )
                """
            )
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS
                    binance_cash_transfer_consumptions_official_idx
                ON binance_cash_transfer_consumptions(
                    account_fingerprint, api_key_fingerprint,
                    transfer_type, transfer_timestamp, transfer_id
                )
                """
            )
            conn.execute(
                """
                CREATE TRIGGER IF NOT EXISTS
                    binance_cash_transfer_consumptions_no_update
                BEFORE UPDATE ON binance_cash_transfer_consumptions
                BEGIN
                    SELECT RAISE(
                        ABORT,
                        'binance-cash-transfer-consumptions-append-only'
                    );
                END
                """
            )
            conn.execute(
                """
                CREATE TRIGGER IF NOT EXISTS
                    binance_cash_transfer_consumptions_no_delete
                BEFORE DELETE ON binance_cash_transfer_consumptions
                BEGIN
                    SELECT RAISE(
                        ABORT,
                        'binance-cash-transfer-consumptions-append-only'
                    );
                END
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS
                    functional_test_authority_events_scope_idx
                ON functional_test_authority_events
                   (permit_id, account_fingerprint, occurred_at, event_id)
                """
            )

    def cash_rows(self) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT broker_id, account, currency, cash, updated_at, source
                FROM cash_balances
                ORDER BY broker_id, account, currency
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def position_rows(self) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT broker_id, symbol, position_side, asset, currency,
                       quantity, value, updated_at, source
                FROM positions
                ORDER BY broker_id, symbol, position_side
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def execution_event_rows(self, limit: int = 50) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT event_id, broker_id, order_id, broker_order_id, symbol, side,
                       quantity, price, state, occurred_at, trace_id, raw_json
                FROM execution_events
                ORDER BY occurred_at DESC, event_id DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            try:
                item["raw"] = json.loads(str(item.pop("raw_json") or "{}"))
            except json.JSONDecodeError:
                item["raw"] = {}
            result.append(item)
        return result

    def record_order_gate_event(self, order: dict[str, Any]) -> None:
        """Append a local gate outcome without overwriting earlier evidence."""

        order_id = str(order.get("order_id") or "").strip()
        if not order_id:
            return
        occurred_at = str(
            order.get("updated_at")
            or order.get("created_at")
            or order.get("time")
            or now_text()
        )
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO order_gate_events
                (event_id, order_id, strategy_id, broker_id, broker_order_id,
                 mode, dry_run, execution_purpose, promotion_eligible,
                 state, occurred_at, canary_scope_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    (
                        f"order-gate:{order_id}:"
                        f"{str(order.get('state') or 'event')}:"
                        f"{uuid.uuid4().hex}"
                    ),
                    order_id,
                    str(order.get("strategy_id") or ""),
                    str(order.get("broker_id") or ""),
                    str(order.get("broker_order_id") or ""),
                    str(order.get("mode") or ""),
                    1 if bool(order.get("dry_run")) else 0,
                    str(order.get("execution_purpose") or ""),
                    0 if order.get("promotion_eligible") is False else 1,
                    str(order.get("state") or ""),
                    occurred_at,
                    json.dumps(
                        order.get("canary_scope")
                        if isinstance(order.get("canary_scope"), dict)
                        else {},
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                ),
            )

    def order_gate_event_rows(self, limit: int = 5000) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT event_id, order_id, strategy_id, broker_id, state,
                       broker_order_id, mode, dry_run, occurred_at,
                       execution_purpose, promotion_eligible,
                       canary_scope_json
                FROM order_gate_events
                ORDER BY occurred_at DESC, event_id DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["promotion_eligible"] = bool(
                item.get("promotion_eligible", 1)
            )
            try:
                item["canary_scope"] = json.loads(
                    str(item.pop("canary_scope_json") or "{}")
                )
            except json.JSONDecodeError:
                item["canary_scope"] = {}
            result.append(item)
        return result

    def reserve_functional_test_order(
        self,
        *,
        permit_id: str,
        idempotency_key: str,
        order_id: str,
        maximum_orders: int,
        reserved_at: str = "",
    ) -> dict[str, Any]:
        """Atomically consume one permit order slot, never releasing it."""

        normalized_permit = str(permit_id or "").strip()
        normalized_key = str(idempotency_key or "").strip()
        normalized_order = str(order_id or "").strip()
        cap = int(maximum_orders)
        if not normalized_permit:
            raise ValueError("functional-test-permit-id-required")
        if not normalized_key:
            raise ValueError("functional-test-idempotency-key-required")
        if not normalized_order:
            raise ValueError("functional-test-order-id-required")
        if cap <= 0:
            raise ValueError("functional-test-maximum-orders-invalid")
        with self.connection() as conn:
            # SQLite serializes all competing permit counters before either
            # the count or insert. A second ThreadingHTTPServer request sees
            # the first committed reservation and cannot oversubscribe.
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                """
                SELECT idempotency_key, permit_id, order_id, reserved_at, state
                FROM functional_test_order_reservations
                WHERE idempotency_key = ?
                """,
                (normalized_key,),
            ).fetchone()
            if existing is not None:
                item = dict(existing)
                return {
                    # A prior reservation may have reached the broker before a
                    # crash. It is an irreversible attempt and never grants a
                    # replay token, even for the same idempotency key.
                    "allowed": False,
                    "created": False,
                    "count": int(
                        conn.execute(
                            """
                            SELECT COUNT(*)
                            FROM functional_test_order_reservations
                            WHERE permit_id = ?
                            """,
                            (normalized_permit,),
                        ).fetchone()[0]
                    ),
                    "reservation": item,
                    "reason": (
                        "functional-test-order-slot-already-reserved"
                        if item["permit_id"] == normalized_permit
                        else "functional-test-idempotency-permit-mismatch"
                    ),
                }
            count = int(
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM functional_test_order_reservations
                    WHERE permit_id = ?
                    """,
                    (normalized_permit,),
                ).fetchone()[0]
            )
            if count >= cap:
                return {
                    "allowed": False,
                    "created": False,
                    "count": count,
                    "reservation": {},
                    "reason": "functional-test-order-count-reservation-exhausted",
                }
            timestamp = str(reserved_at or now_text())
            conn.execute(
                """
                INSERT INTO functional_test_order_reservations
                    (idempotency_key, permit_id, order_id, reserved_at, state)
                VALUES (?, ?, ?, ?, 'RESERVED')
                """,
                (
                    normalized_key,
                    normalized_permit,
                    normalized_order,
                    timestamp,
                ),
            )
            return {
                "allowed": True,
                "created": True,
                "count": count + 1,
                "reservation": {
                    "idempotency_key": normalized_key,
                    "permit_id": normalized_permit,
                    "order_id": normalized_order,
                    "reserved_at": timestamp,
                    "state": "RESERVED",
                },
                "reason": "functional-test-order-slot-reserved",
            }

    def functional_test_reservation_count(
        self,
        permit_id: str,
        *,
        exclude_idempotency_key: str = "",
    ) -> int:
        normalized_permit = str(permit_id or "").strip()
        if not normalized_permit:
            return 0
        excluded = str(exclude_idempotency_key or "").strip()
        with self.connection() as conn:
            if excluded:
                row = conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM functional_test_order_reservations
                    WHERE permit_id = ? AND idempotency_key <> ?
                    """,
                    (normalized_permit, excluded),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM functional_test_order_reservations
                    WHERE permit_id = ?
                    """,
                    (normalized_permit,),
                ).fetchone()
        return int(row[0] if row is not None else 0)

    def update_functional_test_reservation(
        self,
        idempotency_key: str,
        state: str,
    ) -> bool:
        """Record outcome without returning the consumed permit slot."""

        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE functional_test_order_reservations
                SET state = ?
                WHERE idempotency_key = ?
                """,
                (
                    str(state or "UNKNOWN").strip().upper(),
                    str(idempotency_key or "").strip(),
                ),
            )
        return cursor.rowcount == 1

    def functional_test_reservation_rows(
        self,
        permit_id: str = "",
    ) -> list[dict[str, Any]]:
        normalized_permit = str(permit_id or "").strip()
        with self.connection() as conn:
            if normalized_permit:
                rows = conn.execute(
                    """
                    SELECT idempotency_key, permit_id, order_id, reserved_at, state
                    FROM functional_test_order_reservations
                    WHERE permit_id = ?
                    ORDER BY reserved_at, idempotency_key
                    """,
                    (normalized_permit,),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT idempotency_key, permit_id, order_id, reserved_at, state
                    FROM functional_test_order_reservations
                    ORDER BY reserved_at, idempotency_key
                    """
                ).fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def _exact_decimal(value: object, label: str) -> Decimal:
        text = str(value or "").strip()
        try:
            parsed = Decimal(text)
        except (InvalidOperation, ValueError) as exc:
            raise ValueError(f"{label}-invalid") from exc
        if not parsed.is_finite() or parsed < 0:
            raise ValueError(f"{label}-invalid")
        return parsed

    @staticmethod
    def _decimal_equal(left: object, right: Decimal) -> bool:
        try:
            return Decimal(str(left)) == right
        except (InvalidOperation, ValueError):
            return False

    def apply_binance_spot_futures_cash_transfer_adjustment(
        self,
        *,
        source_account: str,
        destination_account: str,
        amount: object,
        source_cash_before: object,
        source_cash_after: object,
        destination_cash_before: object,
        destination_cash_after: object,
        observed_at: str,
        truth_evidence: dict[str, Any],
        truth_hash: str,
    ) -> dict[str, Any]:
        """Atomically reclassify one proven Spot -> USD-M USDT transfer.

        This intentionally cannot seed a whole broker snapshot.  Both durable
        cash legs must still equal the caller's exact pre-transfer values, and
        the supplied fresh official truth must prove the exact post-transfer
        values.  Any mismatch or one-leg SQLite failure rolls back everything.
        """

        if not BINANCE_CASH_TRANSFER_ADJUSTMENT_RELEASED:
            raise ValueError("binance-cash-transfer-adjustment-not-released")
        if not BINANCE_CASH_TRANSFER_CONSUMPTION_RELEASED:
            raise ValueError("binance-cash-transfer-consumption-not-released")

        source_name = str(source_account or "").strip()
        destination_name = str(destination_account or "").strip()
        if source_name != "Binance Spot" or destination_name != "Binance USD-M Futures":
            raise ValueError("binance-cash-transfer-account-scope-invalid")
        amount_value = self._exact_decimal(amount, "transfer-amount")
        if amount_value != Decimal("10"):
            raise ValueError("binance-cash-transfer-amount-must-be-exact-10-usdt")
        source_before = self._exact_decimal(
            source_cash_before, "source-cash-before"
        )
        source_after = self._exact_decimal(
            source_cash_after, "source-cash-after"
        )
        destination_before = self._exact_decimal(
            destination_cash_before, "destination-cash-before"
        )
        destination_after = self._exact_decimal(
            destination_cash_after, "destination-cash-after"
        )
        if source_before - amount_value != source_after:
            raise ValueError("binance-cash-transfer-source-arithmetic-mismatch")
        if destination_before + amount_value != destination_after:
            raise ValueError(
                "binance-cash-transfer-destination-arithmetic-mismatch"
            )
        observed = _utc_observation_text(observed_at)
        observed_datetime = datetime.fromisoformat(observed)
        current_datetime = datetime.now(timezone.utc)
        evidence = dict(truth_evidence)
        if set(evidence) != set(binance_transfer_truth_fields()):
            raise ValueError("binance-cash-transfer-truth-fields-not-exact")
        account_fingerprint = str(
            evidence.get("accountFingerprint") or ""
        ).strip().lower()
        api_key_fingerprint = str(
            evidence.get("apiKeyFingerprint") or ""
        ).strip().lower()
        for label, fingerprint in (
            ("account", account_fingerprint),
            ("api-key", api_key_fingerprint),
        ):
            if len(fingerprint) != 64 or any(
                character not in "0123456789abcdef"
                for character in fingerprint
            ):
                raise ValueError(
                    f"binance-cash-transfer-{label}-fingerprint-invalid"
                )
        transfer = evidence.get("officialTransfer")
        if (
            not isinstance(transfer, dict)
            or set(transfer) != set(binance_transfer_record_fields())
        ):
            raise ValueError("binance-cash-transfer-record-fields-not-exact")
        transfer_id_value = transfer.get("tranId")
        transfer_timestamp = transfer.get("timestamp")
        if type(transfer_id_value) is not int or transfer_id_value <= 0:
            raise ValueError("binance-cash-transfer-record-id-invalid")
        if type(transfer_timestamp) is not int or transfer_timestamp <= 0:
            raise ValueError("binance-cash-transfer-record-time-invalid")
        transfer_id = str(transfer_id_value)
        transfer_event = _utc_observation_text(transfer.get("eventTime"))
        transfer_datetime = datetime.fromisoformat(transfer_event)
        expected_event = datetime.fromtimestamp(
            transfer_timestamp / 1000, tz=timezone.utc
        ).isoformat()
        consumption_key = str(
            evidence.get("consumptionKey") or ""
        ).strip().lower()
        expected_consumption_key = hashlib.sha256(
            (
                account_fingerprint
                + "\x00"
                + api_key_fingerprint
                + "\x00"
                + BINANCE_TRANSFER_TYPE
                + "\x00"
                + transfer_id
                + "\x00"
                + str(transfer_timestamp)
            ).encode("utf-8")
        ).hexdigest()
        prior_high_water = evidence.get("priorConsumedHighWater")
        if not isinstance(prior_high_water, dict):
            raise ValueError("binance-cash-transfer-high-water-invalid")
        if (
            evidence.get("schemaVersion") != BINANCE_TRANSFER_TRUTH_SCHEMA
            or evidence.get("signedGetComplete") is not True
            or evidence.get("coordinatorPhase") != "IDLE"
            or evidence.get("globalLeaseState") != "IDLE"
            or evidence.get("spotOpenOrderCount") != 0
            or evidence.get("futuresOpenOrderCount") != 0
            or evidence.get("futuresPositionCount") != 0
            or not self._decimal_equal(evidence.get("spotCash"), source_after)
            or not self._decimal_equal(
                evidence.get("futuresCash"), destination_after
            )
            or _utc_observation_text(evidence.get("observedAt")) != observed
            or transfer.get("asset") != "USDT"
            or not self._decimal_equal(transfer.get("amount"), amount_value)
            or transfer.get("type") != BINANCE_TRANSFER_TYPE
            or transfer.get("status") != BINANCE_TRANSFER_SUCCESS_STATUS
            or transfer.get("result") != BINANCE_TRANSFER_SUCCESS_RESULT
            or transfer_event != expected_event
            or transfer_datetime > observed_datetime
            or observed_datetime > current_datetime
            or (
                current_datetime - observed_datetime
            ).total_seconds() > MAX_CASH_TRANSFER_TRUTH_AGE_SECONDS
            or consumption_key != expected_consumption_key
            or str(evidence.get("signedGetEnvelopeHash") or "").strip()
            != binance_transfer_content_hash(evidence["signedGetEnvelope"])
            or str(evidence.get("idleBarrierHash") or "").strip()
            != binance_transfer_content_hash(evidence["idleBarrierEvidence"])
        ):
            raise ValueError("binance-cash-transfer-truth-not-exact")
        truth_evidence_json = json.dumps(
            evidence,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        normalized_truth_hash = str(truth_hash or "").strip()
        if (
            len(normalized_truth_hash) != 64
            or any(character not in "0123456789abcdef" for character in normalized_truth_hash)
        ):
            raise ValueError("binance-cash-transfer-truth-hash-invalid")
        if not secrets.compare_digest(
            normalized_truth_hash,
            hashlib.sha256(truth_evidence_json.encode("utf-8")).hexdigest(),
        ):
            raise ValueError("binance-cash-transfer-truth-hash-mismatch")

        authority_request = {
            "schemaVersion": BINANCE_TRANSFER_AUTHORITY_SCHEMA,
            "sourceBrokerId": "binance",
            "destinationBrokerId": "binance-futures",
            "sourceAccount": source_name,
            "destinationAccount": destination_name,
            "accountFingerprint": account_fingerprint,
            "apiKeyFingerprint": api_key_fingerprint,
            "amount": str(amount_value),
            "sourceCashBefore": str(source_before),
            "sourceCashAfter": str(source_after),
            "destinationCashBefore": str(destination_before),
            "destinationCashAfter": str(destination_after),
            "observedAt": observed,
            "officialTransfer": dict(transfer),
            "consumptionKey": consumption_key,
            "priorConsumedHighWater": dict(prior_high_water),
            "truthEvidence": evidence,
            "truthHash": normalized_truth_hash,
        }
        verifier = self.cash_transfer_authority_verifier
        try:
            authority_verified = (
                verifier(authority_request) if verifier is not None else False
            )
        except BaseException as exc:
            raise ValueError(
                "binance-cash-transfer-authority-unverified"
            ) from exc
        if authority_verified is not True:
            raise ValueError("binance-cash-transfer-authority-unverified")
        high_water_request = {
            "schemaVersion": BINANCE_TRANSFER_HIGH_WATER_REQUEST_SCHEMA,
            "accountFingerprint": account_fingerprint,
            "apiKeyFingerprint": api_key_fingerprint,
            "transferType": BINANCE_TRANSFER_TYPE,
            "transferTimestamp": transfer_timestamp,
            "tranId": transfer_id_value,
            "consumptionKey": consumption_key,
            "priorConsumedHighWater": dict(prior_high_water),
            "truthHash": normalized_truth_hash,
        }
        high_water_verifier = self.cash_transfer_high_water_verifier
        try:
            high_water_verified = (
                high_water_verifier(high_water_request)
                if high_water_verifier is not None
                else False
            )
        except BaseException as exc:
            raise ValueError(
                "binance-cash-transfer-high-water-unverified"
            ) from exc
        if high_water_verified is not True:
            raise ValueError("binance-cash-transfer-high-water-unverified")

        idle_request = {
            "schemaVersion": (
                CRYPTO_FIRST_LIVE_GLOBAL_IDLE_RESERVATION_SCHEMA
            ),
            "purpose": "PROGRAM_LEDGER_BINANCE_CASH_TRANSFER_COMMIT",
            "accountFingerprint": account_fingerprint,
            "consumptionKey": consumption_key,
            "truthHash": normalized_truth_hash,
        }
        idle_reserver = self.cash_transfer_global_idle_reserver
        if idle_reserver is None:
            raise ValueError(
                "binance-cash-transfer-global-idle-reserver-unavailable"
            )
        try:
            idle_manager = idle_reserver(idle_request)
            idle_receipt = idle_manager.__enter__()
        except BaseException as exc:
            raise ValueError(
                "binance-cash-transfer-global-idle-reservation-unverified"
            ) from exc
        idle_entered = True
        try:
            receipt = dict(idle_receipt)
            receipt_hash = str(receipt.pop("reservationHash", "")).strip()
            if (
                set(receipt) != {
                    "schemaVersion",
                    "scope",
                    "purpose",
                    "phase",
                    "coordinatorStoredPhase",
                    "coordinatorDatabaseId",
                    "coordinatorRevision",
                    "publicationHash",
                    "accountFingerprint",
                    "consumptionKey",
                    "truthHash",
                    "reservationId",
                    "acquiredEpoch",
                    "held",
                    "exclusive",
                    "durableAuthority",
                    "restartVerifiable",
                }
                or receipt.get("schemaVersion")
                != CRYPTO_FIRST_LIVE_GLOBAL_IDLE_RESERVATION_SCHEMA
                or receipt.get("scope") != "CRYPTO_FIRST_LIVE_GLOBAL"
                or receipt.get("purpose") != idle_request["purpose"]
                or receipt.get("phase") != "IDLE"
                or receipt.get("coordinatorStoredPhase")
                not in {"IDLE", "FINALIZED"}
                or receipt.get("accountFingerprint") != account_fingerprint
                or receipt.get("consumptionKey") != consumption_key
                or receipt.get("truthHash") != normalized_truth_hash
                or receipt.get("held") is not True
                or receipt.get("exclusive") is not True
                or receipt.get("durableAuthority") is not True
                or receipt.get("restartVerifiable") is not True
                or isinstance(receipt.get("coordinatorRevision"), bool)
                or not isinstance(receipt.get("coordinatorRevision"), int)
                or int(receipt["coordinatorRevision"]) < 0
                or not str(receipt.get("coordinatorDatabaseId") or "").strip()
                or not str(receipt.get("reservationId") or "").strip()
                or not math.isfinite(float(receipt.get("acquiredEpoch")))
                or float(receipt["acquiredEpoch"]) <= 0
                or receipt_hash != binance_transfer_content_hash(receipt)
            ):
                raise ValueError(
                    "binance-cash-transfer-global-idle-reservation-invalid"
                )
        except BaseException as exc:
            try:
                idle_manager.__exit__(type(exc), exc, exc.__traceback__)
            finally:
                idle_entered = False
            raise

        adjustment_id = "cash-transfer-adjustment-" + uuid.uuid4().hex
        created_at = now_text()
        try:
            conn = self.connect()
        except BaseException as exc:
            if idle_entered:
                try:
                    idle_manager.__exit__(type(exc), exc, exc.__traceback__)
                finally:
                    idle_entered = False
            raise
        try:
            conn.execute("BEGIN IMMEDIATE")
            source_row = conn.execute(
                """
                SELECT cash FROM cash_balances
                WHERE broker_id='binance' AND account=? AND currency='USDT'
                """,
                (source_name,),
            ).fetchone()
            destination_row = conn.execute(
                """
                SELECT cash FROM cash_balances
                WHERE broker_id='binance-futures' AND account=? AND currency='USDT'
                """,
                (destination_name,),
            ).fetchone()
            if source_row is None or destination_row is None:
                raise ValueError("binance-cash-transfer-durable-legs-missing")
            durable_high_water = self._cash_transfer_consumption_high_water_conn(
                conn,
                account_fingerprint=account_fingerprint,
                api_key_fingerprint=api_key_fingerprint,
            )
            if json.dumps(
                durable_high_water,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ) != json.dumps(
                prior_high_water,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ):
                raise ValueError(
                    "binance-cash-transfer-consumed-high-water-cas-changed"
                )
            duplicate = conn.execute(
                """
                SELECT 1 FROM binance_cash_transfer_consumptions
                WHERE consumption_key=? OR (
                    account_fingerprint=? AND api_key_fingerprint=?
                    AND transfer_type=? AND transfer_timestamp=?
                    AND transfer_id=?
                )
                LIMIT 1
                """,
                (
                    consumption_key,
                    account_fingerprint,
                    api_key_fingerprint,
                    BINANCE_TRANSFER_TYPE,
                    transfer_timestamp,
                    transfer_id,
                ),
            ).fetchone()
            if duplicate is not None:
                raise ValueError(
                    "binance-cash-transfer-record-already-consumed"
                )
            if not self._decimal_equal(source_row["cash"], source_before):
                raise ValueError("binance-cash-transfer-source-cas-changed")
            if not self._decimal_equal(
                destination_row["cash"], destination_before
            ):
                raise ValueError("binance-cash-transfer-destination-cas-changed")
            previous_row = conn.execute(
                """
                SELECT content_hash FROM cash_transfer_adjustments
                ORDER BY sequence DESC LIMIT 1
                """
            ).fetchone()
            previous_hash = str(previous_row[0]) if previous_row is not None else ""
            content = {
                "schemaVersion": "program-ledger-cash-transfer-adjustment/v2",
                "adjustmentId": adjustment_id,
                "sourceBrokerId": "binance",
                "sourceAccount": source_name,
                "destinationBrokerId": "binance-futures",
                "destinationAccount": destination_name,
                "currency": "USDT",
                "amount": str(amount_value),
                "sourceCashBefore": str(source_before),
                "sourceCashAfter": str(source_after),
                "destinationCashBefore": str(destination_before),
                "destinationCashAfter": str(destination_after),
                "observedAt": observed,
                "truthHash": normalized_truth_hash,
                "consumptionKey": consumption_key,
                "previousHash": previous_hash,
            }
            content_json = json.dumps(
                content,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            content_hash = hashlib.sha256(
                content_json.encode("utf-8")
            ).hexdigest()
            prior_consumption_row = conn.execute(
                """
                SELECT content_hash FROM binance_cash_transfer_consumptions
                ORDER BY sequence DESC LIMIT 1
                """
            ).fetchone()
            prior_consumption_hash = (
                str(prior_consumption_row[0])
                if prior_consumption_row is not None
                else ""
            )
            consumption_content = {
                "schemaVersion":
                    "program-ledger-binance-transfer-consumption/v1",
                "consumptionKey": consumption_key,
                "adjustmentId": adjustment_id,
                "accountFingerprint": account_fingerprint,
                "apiKeyFingerprint": api_key_fingerprint,
                "transferType": BINANCE_TRANSFER_TYPE,
                "transferTimestamp": transfer_timestamp,
                "tranId": transfer_id,
                "truthHash": normalized_truth_hash,
                "previousHash": prior_consumption_hash,
            }
            consumption_content_json = json.dumps(
                consumption_content,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            consumption_content_hash = hashlib.sha256(
                consumption_content_json.encode("utf-8")
            ).hexdigest()
            source_updated = conn.execute(
                """
                UPDATE cash_balances SET cash=?, updated_at=?, source=?
                WHERE broker_id='binance' AND account=? AND currency='USDT'
                  AND cash=?
                """,
                (
                    float(source_after),
                    created_at,
                    "cash_transfer_adjustment",
                    source_name,
                    float(source_before),
                ),
            ).rowcount
            destination_updated = conn.execute(
                """
                UPDATE cash_balances SET cash=?, updated_at=?, source=?
                WHERE broker_id='binance-futures' AND account=?
                  AND currency='USDT' AND cash=?
                """,
                (
                    float(destination_after),
                    created_at,
                    "cash_transfer_adjustment",
                    destination_name,
                    float(destination_before),
                ),
            ).rowcount
            if source_updated != 1 or destination_updated != 1:
                raise ValueError("binance-cash-transfer-two-leg-cas-changed")
            conn.execute(
                """
                INSERT INTO cash_transfer_adjustments(
                    adjustment_id, source_broker_id, source_account,
                    destination_broker_id, destination_account, currency,
                    amount_text, source_cash_before_text,
                    source_cash_after_text, destination_cash_before_text,
                    destination_cash_after_text, observed_at, truth_hash,
                    truth_evidence_json, previous_hash, content_json,
                    content_hash, created_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    adjustment_id,
                    "binance",
                    source_name,
                    "binance-futures",
                    destination_name,
                    "USDT",
                    str(amount_value),
                    str(source_before),
                    str(source_after),
                    str(destination_before),
                    str(destination_after),
                    observed,
                    normalized_truth_hash,
                    truth_evidence_json,
                    previous_hash,
                    content_json,
                    content_hash,
                    created_at,
                ),
            )
            conn.execute(
                """
                INSERT INTO binance_cash_transfer_consumptions(
                    consumption_key, adjustment_id, account_fingerprint,
                    api_key_fingerprint, transfer_type,
                    transfer_timestamp, transfer_id, truth_hash,
                    previous_hash, content_json, content_hash, created_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    consumption_key,
                    adjustment_id,
                    account_fingerprint,
                    api_key_fingerprint,
                    BINANCE_TRANSFER_TYPE,
                    transfer_timestamp,
                    transfer_id,
                    normalized_truth_hash,
                    prior_consumption_hash,
                    consumption_content_json,
                    consumption_content_hash,
                    created_at,
                ),
            )
            conn.execute("COMMIT")
        except BaseException as exc:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            if idle_entered:
                try:
                    idle_manager.__exit__(type(exc), exc, exc.__traceback__)
                finally:
                    idle_entered = False
            raise
        finally:
            conn.close()
        if idle_entered:
            suppressed = idle_manager.__exit__(None, None, None)
            idle_entered = False
            if suppressed is not False and suppressed is not None:
                raise ValueError(
                    "binance-cash-transfer-global-idle-release-invalid"
                )
        return {
            "ok": True,
            "adjustmentId": adjustment_id,
            "contentHash": content_hash,
            "consumptionKey": consumption_key,
            "consumptionHeadHash": consumption_content_hash,
            "sourceCash": str(source_after),
            "destinationCash": str(destination_after),
        }

    @staticmethod
    def _validated_cash_transfer_consumption_rows_conn(
        conn: sqlite3.Connection,
    ) -> list[dict[str, Any]]:
        rows = conn.execute(
            """
            SELECT sequence, consumption_key, adjustment_id,
                   account_fingerprint, api_key_fingerprint,
                   transfer_type, transfer_timestamp, transfer_id,
                   truth_hash, previous_hash, content_json,
                   content_hash, created_at
            FROM binance_cash_transfer_consumptions
            ORDER BY sequence
            """
        ).fetchall()
        result: list[dict[str, Any]] = []
        previous_hash = ""
        previous_sequence = 0
        seen_keys: set[str] = set()
        seen_records: set[tuple[str, str, str, int, str]] = set()
        for raw in rows:
            row = dict(raw)
            sequence = int(row["sequence"])
            content_json = str(row["content_json"])
            calculated_hash = hashlib.sha256(
                content_json.encode("utf-8")
            ).hexdigest()
            try:
                content = json.loads(content_json)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "binance-cash-transfer-consumption-chain-invalid"
                ) from exc
            record_key = (
                str(row["account_fingerprint"]),
                str(row["api_key_fingerprint"]),
                str(row["transfer_type"]),
                int(row["transfer_timestamp"]),
                str(row["transfer_id"]),
            )
            if (
                sequence <= previous_sequence
                or str(row["previous_hash"]) != previous_hash
                or str(row["consumption_key"]) in seen_keys
                or record_key in seen_records
                or not secrets.compare_digest(
                    str(row["content_hash"]), calculated_hash
                )
                or not isinstance(content, dict)
                or set(content)
                != {
                    "schemaVersion",
                    "consumptionKey",
                    "adjustmentId",
                    "accountFingerprint",
                    "apiKeyFingerprint",
                    "transferType",
                    "transferTimestamp",
                    "tranId",
                    "truthHash",
                    "previousHash",
                }
                or content.get("schemaVersion")
                != "program-ledger-binance-transfer-consumption/v1"
                or content.get("consumptionKey") != row["consumption_key"]
                or content.get("adjustmentId") != row["adjustment_id"]
                or content.get("accountFingerprint")
                != row["account_fingerprint"]
                or content.get("apiKeyFingerprint")
                != row["api_key_fingerprint"]
                or content.get("transferType") != row["transfer_type"]
                or content.get("transferTimestamp")
                != row["transfer_timestamp"]
                or content.get("tranId") != row["transfer_id"]
                or content.get("truthHash") != row["truth_hash"]
                or content.get("previousHash") != previous_hash
            ):
                raise ValueError(
                    "binance-cash-transfer-consumption-chain-invalid"
                )
            adjustment = conn.execute(
                """
                SELECT truth_hash, content_json
                FROM cash_transfer_adjustments WHERE adjustment_id=?
                """,
                (row["adjustment_id"],),
            ).fetchone()
            if adjustment is None:
                raise ValueError(
                    "binance-cash-transfer-consumption-adjustment-missing"
                )
            try:
                adjustment_content = json.loads(str(adjustment["content_json"]))
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "binance-cash-transfer-consumption-adjustment-invalid"
                ) from exc
            if (
                str(adjustment["truth_hash"]) != row["truth_hash"]
                or not isinstance(adjustment_content, dict)
                or adjustment_content.get("consumptionKey")
                != row["consumption_key"]
            ):
                raise ValueError(
                    "binance-cash-transfer-consumption-adjustment-invalid"
                )
            result.append(row)
            seen_keys.add(str(row["consumption_key"]))
            seen_records.add(record_key)
            previous_hash = calculated_hash
            previous_sequence = sequence
        adjustment_ids = {
            str(row[0])
            for row in conn.execute(
                "SELECT adjustment_id FROM cash_transfer_adjustments"
            ).fetchall()
        }
        consumption_adjustment_ids = {
            str(row["adjustment_id"]) for row in result
        }
        if adjustment_ids != consumption_adjustment_ids:
            raise ValueError(
                "binance-cash-transfer-consumption-lineage-incomplete"
            )
        return result

    @classmethod
    def _cash_transfer_consumption_high_water_conn(
        cls,
        conn: sqlite3.Connection,
        *,
        account_fingerprint: str,
        api_key_fingerprint: str,
    ) -> dict[str, Any]:
        rows = cls._validated_cash_transfer_consumption_rows_conn(conn)
        scoped = [
            row
            for row in rows
            if row["account_fingerprint"] == account_fingerprint
            and row["api_key_fingerprint"] == api_key_fingerprint
            and row["transfer_type"] == BINANCE_TRANSFER_TYPE
        ]
        if not scoped:
            return {
                "schemaVersion": BINANCE_TRANSFER_HIGH_WATER_SCHEMA,
                "revision": 0,
                "accountFingerprint": account_fingerprint,
                "apiKeyFingerprint": api_key_fingerprint,
                "transferType": BINANCE_TRANSFER_TYPE,
                "transferTimestamp": 0,
                "tranId": "",
                "consumptionKey": "",
                "headHash": "",
            }
        prior_order = (0, 0)
        for row in scoped:
            try:
                current_order = (
                    int(row["transfer_timestamp"]),
                    int(row["transfer_id"]),
                )
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "binance-cash-transfer-consumption-order-invalid"
                ) from exc
            if current_order <= prior_order:
                raise ValueError(
                    "binance-cash-transfer-consumption-order-invalid"
                )
            prior_order = current_order
        latest = scoped[-1]
        return {
            "schemaVersion": BINANCE_TRANSFER_HIGH_WATER_SCHEMA,
            "revision": len(scoped),
            "accountFingerprint": account_fingerprint,
            "apiKeyFingerprint": api_key_fingerprint,
            "transferType": BINANCE_TRANSFER_TYPE,
            "transferTimestamp": int(latest["transfer_timestamp"]),
            "tranId": str(latest["transfer_id"]),
            "consumptionKey": str(latest["consumption_key"]),
            "headHash": str(latest["content_hash"]),
        }

    def cash_transfer_consumption_high_water(
        self,
        *,
        account_fingerprint: str,
        api_key_fingerprint: str,
    ) -> dict[str, Any]:
        account = str(account_fingerprint or "").strip().lower()
        api_key = str(api_key_fingerprint or "").strip().lower()
        for label, value in (("account", account), ("api-key", api_key)):
            if len(value) != 64 or any(
                character not in "0123456789abcdef" for character in value
            ):
                raise ValueError(
                    f"binance-cash-transfer-{label}-fingerprint-invalid"
                )
        with self.connection() as conn:
            return self._cash_transfer_consumption_high_water_conn(
                conn,
                account_fingerprint=account,
                api_key_fingerprint=api_key,
            )

    def cash_transfer_consumption_rows(self) -> list[dict[str, Any]]:
        with self.connection() as conn:
            return self._validated_cash_transfer_consumption_rows_conn(conn)

    def cash_transfer_adjustment_rows(self) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT sequence, adjustment_id, source_broker_id, source_account,
                       destination_broker_id, destination_account, currency,
                       amount_text, observed_at, truth_hash, previous_hash,
                       truth_evidence_json, content_json, content_hash, created_at
                FROM cash_transfer_adjustments
                ORDER BY sequence
                """
            ).fetchall()
        result: list[dict[str, Any]] = []
        previous_hash = ""
        previous_sequence = 0
        for raw in rows:
            row = dict(raw)
            sequence = int(row["sequence"])
            evidence_json = str(row["truth_evidence_json"])
            content_json = str(row["content_json"])
            content_hash = hashlib.sha256(content_json.encode("utf-8")).hexdigest()
            truth_hash = hashlib.sha256(evidence_json.encode("utf-8")).hexdigest()
            if (
                sequence <= previous_sequence
                or str(row["previous_hash"]) != previous_hash
                or not secrets.compare_digest(
                    str(row["content_hash"]), content_hash
                )
                or not secrets.compare_digest(str(row["truth_hash"]), truth_hash)
            ):
                raise ValueError("cash-transfer-adjustment-chain-invalid")
            content = json.loads(content_json)
            evidence = json.loads(evidence_json)
            if (
                not isinstance(content, dict)
                or not isinstance(evidence, dict)
                or content.get("adjustmentId") != row["adjustment_id"]
                or content.get("previousHash") != previous_hash
                or content.get("truthHash") != row["truth_hash"]
            ):
                raise ValueError("cash-transfer-adjustment-chain-invalid")
            result.append(row)
            previous_hash = content_hash
            previous_sequence = sequence
        return result

    @staticmethod
    def _validate_functional_test_equity_identity(
        permit_id: str,
        account_fingerprint: str,
    ) -> tuple[str, str, str]:
        normalized_permit = str(permit_id or "").strip()
        normalized_account = str(account_fingerprint or "").strip().lower()
        if not normalized_permit:
            raise ValueError("functional-test-equity-permit-id-required")
        if len(normalized_account) < 16:
            raise ValueError(
                "functional-test-equity-account-fingerprint-invalid"
            )
        return (
            normalized_permit,
            normalized_account,
            _functional_equity_scope_key(
                normalized_permit,
                normalized_account,
            ),
        )

    @staticmethod
    def _functional_test_equity_payload(
        row: sqlite3.Row | dict[str, Any],
    ) -> dict[str, Any]:
        source = dict(row)
        return {
            "scope_key": str(source.get("scope_key") or ""),
            "permit_id": str(source.get("permit_id") or ""),
            "account_fingerprint": str(
                source.get("account_fingerprint") or ""
            ),
            "starting_equity": float(source.get("starting_equity") or 0.0),
            "peak_equity": float(source.get("peak_equity") or 0.0),
            "current_equity": float(source.get("current_equity") or 0.0),
            "worst_drawdown": float(source.get("worst_drawdown") or 0.0),
            "first_observed_at": str(source.get("first_observed_at") or ""),
            "last_observed_at": str(source.get("last_observed_at") or ""),
            "observation_count": int(source.get("observation_count") or 0),
            "latest_observation_hash": str(
                source.get("latest_observation_hash") or ""
            ),
        }

    @classmethod
    def _validated_functional_test_equity_row(
        cls,
        row: sqlite3.Row | dict[str, Any],
        *,
        latest_observation: sqlite3.Row | dict[str, Any] | None,
        history_count: int,
    ) -> dict[str, Any]:
        source = dict(row)
        payload = cls._functional_test_equity_payload(source)
        numeric = (
            payload["starting_equity"],
            payload["peak_equity"],
            payload["current_equity"],
            payload["worst_drawdown"],
        )
        if (
            not all(math.isfinite(value) for value in numeric)
            or payload["starting_equity"] <= 0
            or payload["peak_equity"] < payload["starting_equity"]
            or payload["current_equity"] < 0
            or payload["worst_drawdown"] < 0
            or payload["observation_count"] <= 0
            or history_count != payload["observation_count"]
            or latest_observation is None
        ):
            raise ValueError("functional-test-equity-scope-corrupt")
        expected_integrity = _functional_equity_integrity_hash(payload)
        if str(source.get("integrity_hash") or "") != expected_integrity:
            raise ValueError("functional-test-equity-scope-integrity-failed")
        latest = dict(latest_observation)
        if (
            str(latest.get("content_hash") or "")
            != payload["latest_observation_hash"]
            or str(latest.get("observed_at") or "")
            != payload["last_observed_at"]
            or not math.isclose(
                float(latest.get("equity") or 0.0),
                payload["current_equity"],
                rel_tol=0.0,
                abs_tol=1e-9,
            )
            or not math.isclose(
                float(latest.get("peak_equity") or 0.0),
                payload["peak_equity"],
                rel_tol=0.0,
                abs_tol=1e-9,
            )
            or not math.isclose(
                float(latest.get("worst_drawdown") or 0.0),
                payload["worst_drawdown"],
                rel_tol=0.0,
                abs_tol=1e-9,
            )
        ):
            raise ValueError("functional-test-equity-history-mismatch")
        payload["cumulative_loss"] = payload["worst_drawdown"]
        return payload

    def functional_test_equity_scope(
        self,
        *,
        permit_id: str,
        account_fingerprint: str,
        maximum_age_seconds: float | None = None,
        now: object = None,
    ) -> dict[str, Any]:
        """Read one durable permit/account loss scope and verify its chain."""

        normalized_permit, normalized_account, scope_key = (
            self._validate_functional_test_equity_identity(
                permit_id,
                account_fingerprint,
            )
        )
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT * FROM functional_test_equity_scopes
                WHERE scope_key = ?
                """,
                (scope_key,),
            ).fetchone()
            if row is None:
                raise ValueError("functional-test-equity-scope-missing")
            if (
                str(row["permit_id"]) != normalized_permit
                or str(row["account_fingerprint"]).lower()
                != normalized_account
            ):
                raise ValueError("functional-test-equity-scope-binding-mismatch")
            latest = conn.execute(
                """
                SELECT * FROM functional_test_equity_observations
                WHERE scope_key = ?
                ORDER BY observed_at DESC, rowid DESC
                LIMIT 1
                """,
                (scope_key,),
            ).fetchone()
            count = int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM functional_test_equity_observations
                    WHERE scope_key = ?
                    """,
                    (scope_key,),
                ).fetchone()[0]
            )
        result = self._validated_functional_test_equity_row(
            row,
            latest_observation=latest,
            history_count=count,
        )
        if maximum_age_seconds is not None:
            current_text = _utc_observation_text(
                now or datetime.now(timezone.utc).isoformat()
            )
            age = (
                datetime.fromisoformat(current_text)
                - datetime.fromisoformat(result["last_observed_at"])
            ).total_seconds()
            if age < -1.0 or age > max(0.0, float(maximum_age_seconds)):
                raise ValueError("functional-test-equity-scope-stale")
            result["age_seconds"] = max(0.0, age)
        return result

    def observe_functional_test_equity(
        self,
        *,
        permit_id: str,
        account_fingerprint: str,
        current_equity: float,
        observed_at: object,
        allow_create: bool = False,
    ) -> dict[str, Any]:
        """Append an equity point and update immutable permit loss watermarks."""

        normalized_permit, normalized_account, scope_key = (
            self._validate_functional_test_equity_identity(
                permit_id,
                account_fingerprint,
            )
        )
        equity = float(current_equity)
        if not math.isfinite(equity) or equity < 0:
            raise ValueError("functional-test-equity-value-invalid")
        observed = _utc_observation_text(observed_at)
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                """
                SELECT * FROM functional_test_equity_scopes
                WHERE scope_key = ?
                """,
                (scope_key,),
            ).fetchone()
            if existing is None:
                if not allow_create:
                    raise ValueError("functional-test-equity-scope-missing")
                if equity <= 0:
                    raise ValueError(
                        "functional-test-equity-starting-value-invalid"
                    )
                starting = equity
                peak = equity
                worst = 0.0
                count = 1
                first_observed = observed
                previous_hash = ""
            else:
                if (
                    str(existing["permit_id"]) != normalized_permit
                    or str(existing["account_fingerprint"]).lower()
                    != normalized_account
                ):
                    raise ValueError(
                        "functional-test-equity-scope-binding-mismatch"
                    )
                last_observed = str(existing["last_observed_at"] or "")
                if observed < last_observed:
                    raise ValueError(
                        "functional-test-equity-observation-regressed"
                    )
                if observed == last_observed:
                    if math.isclose(
                        equity,
                        float(existing["current_equity"]),
                        rel_tol=0.0,
                        abs_tol=1e-9,
                    ):
                        return self.functional_test_equity_scope(
                            permit_id=normalized_permit,
                            account_fingerprint=normalized_account,
                        )
                    raise ValueError(
                        "functional-test-equity-observation-conflict"
                    )
                starting = float(existing["starting_equity"])
                peak = max(float(existing["peak_equity"]), equity)
                worst = max(
                    float(existing["worst_drawdown"]),
                    max(0.0, peak - equity),
                )
                count = int(existing["observation_count"]) + 1
                first_observed = str(existing["first_observed_at"])
                previous_hash = str(
                    existing["latest_observation_hash"] or ""
                )
            drawdown = max(0.0, peak - equity)
            event_payload = {
                "scopeKey": scope_key,
                "permitId": normalized_permit,
                "accountFingerprint": normalized_account,
                "observedAt": observed,
                "equity": equity,
                "peakEquity": peak,
                "drawdown": drawdown,
                "worstDrawdown": worst,
                "previousHash": previous_hash,
            }
            event_hash = _functional_equity_integrity_hash(event_payload)
            event_id = f"functional-equity:{scope_key}:{uuid.uuid4().hex}"
            conn.execute(
                """
                INSERT INTO functional_test_equity_observations
                (event_id, scope_key, observed_at, equity, peak_equity,
                 drawdown, worst_drawdown, previous_hash, content_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    scope_key,
                    observed,
                    equity,
                    peak,
                    drawdown,
                    worst,
                    previous_hash,
                    event_hash,
                ),
            )
            scope_payload = {
                "scope_key": scope_key,
                "permit_id": normalized_permit,
                "account_fingerprint": normalized_account,
                "starting_equity": starting,
                "peak_equity": peak,
                "current_equity": equity,
                "worst_drawdown": worst,
                "first_observed_at": first_observed,
                "last_observed_at": observed,
                "observation_count": count,
                "latest_observation_hash": event_hash,
            }
            integrity_hash = _functional_equity_integrity_hash(scope_payload)
            conn.execute(
                """
                INSERT INTO functional_test_equity_scopes
                (scope_key, permit_id, account_fingerprint, starting_equity,
                 peak_equity, current_equity, worst_drawdown,
                 first_observed_at, last_observed_at, observation_count,
                 latest_observation_hash, integrity_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(scope_key) DO UPDATE SET
                    peak_equity=excluded.peak_equity,
                    current_equity=excluded.current_equity,
                    worst_drawdown=excluded.worst_drawdown,
                    last_observed_at=excluded.last_observed_at,
                    observation_count=excluded.observation_count,
                    latest_observation_hash=excluded.latest_observation_hash,
                    integrity_hash=excluded.integrity_hash
                """,
                (
                    scope_key,
                    normalized_permit,
                    normalized_account,
                    starting,
                    peak,
                    equity,
                    worst,
                    first_observed,
                    observed,
                    count,
                    event_hash,
                    integrity_hash,
                ),
            )
        return self.functional_test_equity_scope(
            permit_id=normalized_permit,
            account_fingerprint=normalized_account,
        )

    def close_functional_test_authority(
        self,
        *,
        permit_id: str,
        account_fingerprint: str,
        reason: str,
        occurred_at: object,
    ) -> dict[str, Any]:
        """Durably and irreversibly close one exact permit/account scope."""

        normalized_permit, normalized_account, _scope_key = (
            self._validate_functional_test_equity_identity(
                permit_id,
                account_fingerprint,
            )
        )
        timestamp = _utc_observation_text(occurred_at)
        detail = str(reason or "functional-test-stop-requested").strip()
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                """
                SELECT event_id, permit_id, account_fingerprint, event_type,
                       occurred_at, reason
                FROM functional_test_authority_events
                WHERE permit_id = ? AND account_fingerprint = ?
                  AND event_type = 'AUTHORITY_CLOSED'
                ORDER BY occurred_at DESC, event_id DESC
                LIMIT 1
                """,
                (normalized_permit, normalized_account),
            ).fetchone()
            if existing is not None:
                return {**dict(existing), "closed": True, "created": False}
            event = {
                "event_id": f"functional-authority:{uuid.uuid4().hex}",
                "permit_id": normalized_permit,
                "account_fingerprint": normalized_account,
                "event_type": "AUTHORITY_CLOSED",
                "occurred_at": timestamp,
                "reason": detail,
            }
            conn.execute(
                """
                INSERT INTO functional_test_authority_events
                (event_id, permit_id, account_fingerprint, event_type,
                 occurred_at, reason)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                tuple(event.values()),
            )
        return {**event, "closed": True, "created": True}

    def functional_test_authority_status(
        self,
        *,
        permit_id: str,
        account_fingerprint: str,
    ) -> dict[str, Any]:
        normalized_permit, normalized_account, _scope_key = (
            self._validate_functional_test_equity_identity(
                permit_id,
                account_fingerprint,
            )
        )
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT event_id, permit_id, account_fingerprint, event_type,
                       occurred_at, reason
                FROM functional_test_authority_events
                WHERE permit_id = ? AND account_fingerprint = ?
                ORDER BY occurred_at DESC, event_id DESC
                LIMIT 1
                """,
                (normalized_permit, normalized_account),
            ).fetchone()
        if row is None:
            return {
                "permit_id": normalized_permit,
                "account_fingerprint": normalized_account,
                "closed": False,
            }
        item = dict(row)
        item["closed"] = str(item.get("event_type") or "") == "AUTHORITY_CLOSED"
        return item

    @staticmethod
    def _dispatch_order_from_row(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        item = dict(row)
        try:
            order = json.loads(str(item.get("order_json") or "{}"))
        except json.JSONDecodeError:
            order = {}
        if not isinstance(order, dict):
            order = {}
        order.setdefault("order_id", str(item.get("order_id") or ""))
        order.setdefault(
            "idempotency_key",
            str(item.get("idempotency_key") or ""),
        )
        order.setdefault("broker_id", str(item.get("broker_id") or ""))
        order.setdefault(
            "broker_order_id",
            str(item.get("broker_order_id") or ""),
        )
        order["state"] = str(item.get("state") or order.get("state") or "")
        order.setdefault("created_at", str(item.get("created_at") or ""))
        order["updated_at"] = str(
            item.get("updated_at") or order.get("updated_at") or ""
        )
        return order

    def checkpoint_order_dispatch(
        self,
        order: dict[str, Any],
    ) -> dict[str, Any]:
        """Durably reserve an idempotency key before any broker side effect."""

        order_id = str(order.get("order_id") or "").strip()
        idempotency_key = str(order.get("idempotency_key") or "").strip()
        broker_id = str(order.get("broker_id") or "").strip().lower()
        if not order_id or not idempotency_key or not broker_id:
            raise ValueError(
                "order_id, idempotency_key, and broker_id are required"
            )
        created_at = str(
            order.get("created_at")
            or order.get("time")
            or now_text()
        )
        updated_at = str(order.get("updated_at") or created_at)
        payload = json.dumps(
            order,
            ensure_ascii=False,
            sort_keys=True,
        )
        with self.connection() as conn:
            try:
                conn.execute(
                    """
                    INSERT INTO order_dispatch_journal
                    (order_id, idempotency_key, broker_id, broker_order_id,
                     state, created_at, updated_at, order_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        order_id,
                        idempotency_key,
                        broker_id,
                        str(order.get("broker_order_id") or ""),
                        str(order.get("state") or "dispatch_pending"),
                        created_at,
                        updated_at,
                        payload,
                    ),
                )
            except sqlite3.IntegrityError:
                row = conn.execute(
                    """
                    SELECT order_id, idempotency_key, broker_id,
                           broker_order_id, state, created_at, updated_at,
                           order_json
                    FROM order_dispatch_journal
                    WHERE order_id = ? OR idempotency_key = ?
                    LIMIT 1
                    """,
                    (order_id, idempotency_key),
                ).fetchone()
                return {
                    "created": False,
                    "order": (
                        self._dispatch_order_from_row(row)
                        if row is not None
                        else {}
                    ),
                }
        return {"created": True, "order": dict(order)}

    def update_order_dispatch(self, order: dict[str, Any]) -> bool:
        """Persist the post-dispatch state; never creates missing evidence."""

        order_id = str(order.get("order_id") or "").strip()
        idempotency_key = str(order.get("idempotency_key") or "").strip()
        if not order_id or not idempotency_key:
            return False
        updated_at = str(order.get("updated_at") or now_text())
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE order_dispatch_journal
                SET broker_order_id = ?, state = ?, updated_at = ?,
                    order_json = ?
                WHERE order_id = ? AND idempotency_key = ?
                """,
                (
                    str(order.get("broker_order_id") or ""),
                    str(order.get("state") or ""),
                    updated_at,
                    json.dumps(
                        order,
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    order_id,
                    idempotency_key,
                ),
            )
        return cursor.rowcount == 1

    def order_dispatch_for_idempotency_key(
        self,
        idempotency_key: str,
    ) -> dict[str, Any] | None:
        normalized = str(idempotency_key or "").strip()
        if not normalized:
            return None
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT order_id, idempotency_key, broker_id, broker_order_id,
                       state, created_at, updated_at, order_json
                FROM order_dispatch_journal
                WHERE idempotency_key = ?
                LIMIT 1
                """,
                (normalized,),
            ).fetchone()
        return (
            self._dispatch_order_from_row(row)
            if row is not None
            else None
        )

    def order_dispatch_rows(self, limit: int = 5000) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT order_id, idempotency_key, broker_id, broker_order_id,
                       state, created_at, updated_at, order_json
                FROM order_dispatch_journal
                ORDER BY updated_at DESC, order_id DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        return [self._dispatch_order_from_row(row) for row in rows]

    def existing_execution_event_ids(self, event_ids: list[str]) -> set[str]:
        unique_ids = list(dict.fromkeys(str(item) for item in event_ids if str(item)))
        if not unique_ids:
            return set()
        placeholders = ",".join("?" for _ in unique_ids)
        with self.connection() as conn:
            rows = conn.execute(
                f"SELECT event_id FROM execution_events WHERE event_id IN ({placeholders})",
                unique_ids,
            ).fetchall()
        return {str(row["event_id"]) for row in rows}

    def replace_cash_rows(self, rows: list[dict[str, Any]], source: str) -> int:
        updated_at = now_text()
        prepared: list[tuple[str, str, str, float, str, str]] = []
        for row in rows:
            broker_id = str(row.get("broker_id") or "").strip()
            if not broker_id:
                continue
            account = str(row.get("account") or broker_id).strip()
            currency = str(row.get("currency") or "").strip()
            cash = numeric_value(row.get("broker_cash", row.get("cash", 0.0)))
            prepared.append((broker_id, account, currency, cash, updated_at, source))
        with self.connection() as conn:
            conn.execute("DELETE FROM cash_balances")
            conn.executemany(
                """
                INSERT OR REPLACE INTO cash_balances
                (broker_id, account, currency, cash, updated_at, source)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                prepared,
            )
        return len(prepared)

    def sync_cash_rows(self, rows: list[dict[str, Any]], broker_ids: list[str], source: str) -> int:
        updated_at = now_text()
        selected_brokers = [broker_id for broker_id in {str(item).strip() for item in broker_ids} if broker_id]
        prepared: list[tuple[str, str, str, float, str, str]] = []
        for row in rows:
            broker_id = str(row.get("broker_id") or "").strip()
            if not broker_id:
                continue
            account = str(row.get("account") or broker_id).strip()
            currency = str(row.get("currency") or "").strip()
            cash = numeric_value(row.get("broker_cash", row.get("cash", 0.0)))
            prepared.append((broker_id, account, currency, cash, updated_at, source))

        with self.connection() as conn:
            if selected_brokers:
                placeholders = ",".join("?" for _ in selected_brokers)
                conn.execute(f"DELETE FROM cash_balances WHERE broker_id IN ({placeholders})", selected_brokers)
            conn.executemany(
                """
                INSERT OR REPLACE INTO cash_balances
                (broker_id, account, currency, cash, updated_at, source)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                prepared,
            )
        return len(prepared)

    def replace_position_rows(self, rows: list[dict[str, Any]], source: str) -> int:
        updated_at = now_text()
        prepared: list[tuple[str, str, str, str, str, float, float, str, str]] = []
        for row in rows:
            broker_id = str(row.get("broker_id") or "").strip()
            symbol = str(row.get("symbol") or "").strip()
            if not broker_id or not symbol:
                continue
            prepared.append(
                (
                    broker_id,
                    symbol,
                    normalized_position_side(row),
                    str(row.get("asset") or ""),
                    str(row.get("currency") or ""),
                    numeric_value(row.get("broker_qty", row.get("quantity", 0.0))),
                    numeric_value(row.get("broker_value", row.get("value", 0.0))),
                    updated_at,
                    source,
                )
            )
        with self.connection() as conn:
            conn.execute("DELETE FROM positions")
            conn.executemany(
                """
                INSERT OR REPLACE INTO positions
                (broker_id, symbol, position_side, asset, currency, quantity,
                 value, updated_at, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                prepared,
            )
        return len(prepared)

    def sync_position_rows(self, rows: list[dict[str, Any]], broker_ids: list[str], source: str) -> int:
        updated_at = now_text()
        selected_brokers = [broker_id for broker_id in {str(item).strip() for item in broker_ids} if broker_id]
        prepared: list[tuple[str, str, str, str, str, float, float, str, str]] = []
        for row in rows:
            broker_id = str(row.get("broker_id") or "").strip()
            symbol = str(row.get("symbol") or "").strip()
            if not broker_id or not symbol:
                continue
            prepared.append(
                (
                    broker_id,
                    symbol,
                    normalized_position_side(row),
                    str(row.get("asset") or ""),
                    str(row.get("currency") or ""),
                    numeric_value(row.get("broker_qty", row.get("quantity", 0.0))),
                    numeric_value(row.get("broker_value", row.get("value", 0.0))),
                    updated_at,
                    source,
                )
            )

        with self.connection() as conn:
            if selected_brokers:
                placeholders = ",".join("?" for _ in selected_brokers)
                conn.execute(f"DELETE FROM positions WHERE broker_id IN ({placeholders})", selected_brokers)
            conn.executemany(
                """
                INSERT OR REPLACE INTO positions
                (broker_id, symbol, position_side, asset, currency, quantity,
                 value, updated_at, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                prepared,
            )
        return len(prepared)

    def sync_broker_snapshot(
        self,
        accounts: list[dict[str, Any]],
        positions: list[dict[str, Any]],
        broker_ids: list[str],
        source: str,
    ) -> dict[str, Any]:
        return {
            "updated_at": now_text(),
            "cash_count": self.sync_cash_rows(accounts, broker_ids, source),
            "position_count": self.sync_position_rows(positions, broker_ids, source),
            "source": source,
        }

    def seed_from_broker_snapshot(
        self,
        accounts: list[dict[str, Any]],
        positions: list[dict[str, Any]],
        source: str = "broker_snapshot",
    ) -> dict[str, Any]:
        updated_at = now_text()
        prepared_cash: list[tuple[str, str, str, float, str, str]] = []
        for row in accounts:
            broker_id = str(row.get("broker_id") or "").strip()
            if not broker_id:
                continue
            prepared_cash.append(
                (
                    broker_id,
                    str(row.get("account") or broker_id).strip(),
                    str(row.get("currency") or "").strip(),
                    numeric_value(row.get("broker_cash", row.get("cash", 0.0))),
                    updated_at,
                    source,
                )
            )
        prepared_positions: list[
            tuple[str, str, str, str, str, float, float, str, str]
        ] = []
        for row in positions:
            broker_id = str(row.get("broker_id") or "").strip()
            symbol = str(row.get("symbol") or "").strip()
            if not broker_id or not symbol:
                continue
            prepared_positions.append(
                (
                    broker_id,
                    symbol,
                    normalized_position_side(row),
                    str(row.get("asset") or ""),
                    str(row.get("currency") or ""),
                    numeric_value(
                        row.get("broker_qty", row.get("quantity", 0.0))
                    ),
                    numeric_value(
                        row.get("broker_value", row.get("value", 0.0))
                    ),
                    updated_at,
                    source,
                )
            )
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("DELETE FROM cash_balances")
            conn.executemany(
                """
                INSERT OR REPLACE INTO cash_balances
                (broker_id, account, currency, cash, updated_at, source)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                prepared_cash,
            )
            conn.execute("DELETE FROM positions")
            conn.executemany(
                """
                INSERT OR REPLACE INTO positions
                (broker_id, symbol, position_side, asset, currency, quantity,
                 value, updated_at, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                prepared_positions,
            )
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()
        return {
            "updated_at": updated_at,
            "cash_count": len(prepared_cash),
            "position_count": len(prepared_positions),
            "source": source,
        }

    def record_execution_events(self, events: list[dict[str, Any]]) -> int:
        prepared: list[tuple[str, str, str, str, str, str, float, float, str, str, str, str]] = []
        for event in events:
            broker_id = str(event.get("broker_id") or "").strip()
            occurred_at = str(event.get("occurred_at") or event.get("time") or now_text())
            order_id = str(event.get("order_id") or "").strip()
            broker_order_id = str(event.get("broker_order_id") or event.get("brokerOrderId") or "").strip()
            symbol = str(event.get("symbol") or "").strip()
            side = str(event.get("side") or "").strip().upper()
            state = str(event.get("state") or event.get("status") or "event").strip()
            event_id = str(event.get("event_id") or event.get("id") or f"{broker_id}:{order_id}:{broker_order_id}:{symbol}:{occurred_at}")
            if not broker_id or not event_id:
                continue
            prepared.append(
                (
                    event_id,
                    broker_id,
                    order_id,
                    broker_order_id,
                    symbol,
                    side,
                    numeric_value(event.get("quantity", event.get("qty", 0.0))),
                    numeric_value(event.get("price", 0.0)),
                    state,
                    occurred_at,
                    str(event.get("trace_id") or event.get("traceId") or ""),
                    json.dumps(event, ensure_ascii=False, sort_keys=True),
                )
            )
        with self.connection() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO execution_events
                (event_id, broker_id, order_id, broker_order_id, symbol, side,
                 quantity, price, state, occurred_at, trace_id, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                prepared,
            )
        return len(prepared)

    def summary(self) -> dict[str, Any]:
        with self.connection() as conn:
            cash = conn.execute("SELECT COUNT(*) AS count, MAX(updated_at) AS updated_at FROM cash_balances").fetchone()
            positions = conn.execute("SELECT COUNT(*) AS count, MAX(updated_at) AS updated_at FROM positions").fetchone()
            events = conn.execute("SELECT COUNT(*) AS count, MAX(occurred_at) AS updated_at FROM execution_events").fetchone()
        return {
            "cash_count": int(cash["count"] or 0),
            "position_count": int(positions["count"] or 0),
            "execution_event_count": int(events["count"] or 0),
            "last_cash_update": cash["updated_at"] or "-",
            "last_position_update": positions["updated_at"] or "-",
            "last_execution_event": events["updated_at"] or "-",
            "path": str(self.path),
        }
