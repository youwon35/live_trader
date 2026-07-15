from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from trading_runtime import AuditEvent


AUDIT_COLUMNS = (
    "event_id",
    "occurred_at",
    "app",
    "category",
    "scope",
    "level",
    "source",
    "message",
    "strategy_id",
    "dataset_id",
    "symbol",
    "order_id",
    "risk_gate",
    "decision",
    "state",
    "reason",
    "run_id",
    "passport_id",
    "trace_id",
    "payload_json",
)


class SQLiteAuditEventStore:
    """Durable append-only audit/replay store for live trading decisions."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def append(self, event: AuditEvent) -> None:
        row = event.to_dict()
        payload_json = json.dumps(row.get("payload", {}), ensure_ascii=False, sort_keys=True)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path)
        try:
            self._ensure_schema(connection)
            connection.execute(
                """
                insert or replace into audit_events (
                    event_id, occurred_at, app, category, scope, level, source, message,
                    strategy_id, dataset_id, symbol, order_id, risk_gate, decision, state,
                    reason, run_id, passport_id, trace_id, payload_json
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["event_id"],
                    row["occurred_at"],
                    row["app"],
                    row["category"],
                    row["scope"],
                    row["level"],
                    row["source"],
                    row["message"],
                    row["strategy_id"],
                    row["dataset_id"],
                    row["symbol"],
                    row["order_id"],
                    row["risk_gate"],
                    row["decision"],
                    row["state"],
                    row["reason"],
                    row["run_id"],
                    row["passport_id"],
                    row["trace_id"],
                    payload_json,
                ),
            )
            connection.commit()
        finally:
            connection.close()

    def list_events(self, *, limit: int = 500, newest_first: bool = True) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        order = "desc" if newest_first else "asc"
        connection = sqlite3.connect(self.path)
        try:
            self._ensure_schema(connection)
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                f"select {', '.join(AUDIT_COLUMNS)} from audit_events order by occurred_at {order}, rowid {order} limit ?",
                (int(limit),),
            ).fetchall()
            return [self._row_to_event(row) for row in rows]
        finally:
            connection.close()

    def count(self) -> int:
        if not self.path.exists():
            return 0
        connection = sqlite3.connect(self.path)
        try:
            self._ensure_schema(connection)
            row = connection.execute("select count(*) from audit_events").fetchone()
            return int(row[0] if row else 0)
        finally:
            connection.close()

    @staticmethod
    def _ensure_schema(connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            create table if not exists audit_events (
                event_id text primary key,
                occurred_at text not null,
                app text not null,
                category text not null,
                scope text not null default '',
                level text not null,
                source text not null,
                message text not null,
                strategy_id text not null default '',
                dataset_id text not null default '',
                symbol text not null default '',
                order_id text not null default '',
                risk_gate text not null default '',
                decision text not null default '',
                state text not null default '',
                reason text not null default '',
                run_id text not null default '',
                passport_id text not null default '',
                trace_id text not null default '',
                payload_json text not null default '{}'
            )
            """
        )
        columns = {row[1] for row in connection.execute("pragma table_info(audit_events)").fetchall()}
        for column in ("run_id", "passport_id", "trace_id"):
            if column not in columns:
                connection.execute(f"alter table audit_events add column {column} text not null default ''")
        connection.execute("create index if not exists idx_audit_events_time on audit_events(occurred_at)")
        connection.execute("create index if not exists idx_audit_events_order on audit_events(order_id)")
        connection.execute("create index if not exists idx_audit_events_strategy on audit_events(strategy_id)")
        connection.execute("create index if not exists idx_audit_events_run on audit_events(run_id)")

    @staticmethod
    def _row_to_event(row: sqlite3.Row) -> dict[str, Any]:
        data = {key: row[key] for key in AUDIT_COLUMNS if key != "payload_json"}
        try:
            data["payload"] = json.loads(row["payload_json"] or "{}")
        except json.JSONDecodeError:
            data["payload"] = {}
        return data
