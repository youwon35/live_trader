from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path
from contextlib import contextmanager
from typing import Any


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


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

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
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
                CREATE INDEX IF NOT EXISTS
                    order_dispatch_journal_state_updated_idx
                ON order_dispatch_journal (state, updated_at)
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
                 mode, dry_run, state, occurred_at, canary_scope_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            try:
                item["canary_scope"] = json.loads(
                    str(item.pop("canary_scope_json") or "{}")
                )
            except json.JSONDecodeError:
                item["canary_scope"] = {}
            result.append(item)
        return result

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
        return {
            "updated_at": now_text(),
            "cash_count": self.replace_cash_rows(accounts, source),
            "position_count": self.replace_position_rows(positions, source),
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
