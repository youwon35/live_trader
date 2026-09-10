"""Durable, one-attempt intents dispatched only after a live cycle unlocks.

This journal is not a restart replay queue. A new owner can inspect old rows,
but cannot claim them. The ordinary OMS and broker authority remain mandatory.
"""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import asdict, replace
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import secrets
import sqlite3
import threading
from typing import Any, Callable, Iterator

from .order_management import OrderIntent

_META = "continuous_dispatch"
_MAX_PAYLOAD = 128 * 1024
_CONTROL_NAMES = frozenset({
    "apply_watchdog_fail_closed", "set_mode", "set_flag",
    "save_environment_settings", "promote_strategy_to_live",
    "set_strategy_lifecycle_status", "set_automation_profile",
    "set_risk_setting", "start_continuous_runtime", "start_functional_test_runtime",
    "start_upbit_functional_backend_state", "start_binance_spot_functional_backend_state",
    "recover_upbit_functional_backend_state", "recover_binance_spot_functional_backend_state",
    "start_binance_futures_fill_soak", "run_recovery_drill",
    "seed_program_ledger_from_broker_snapshot",
})
_DISPATCH_ATTEMPT_LOCK = threading.RLock()
_AUTHORITY_LOCK = threading.RLock()
_AUTHORITY = (secrets.token_hex(16), False)
_CONTROLS: set[str] = set()
_DISPATCH_CONTEXT: ContextVar[tuple[dict[str, str], str] | None] = ContextVar(
    "live_continuous_dispatch_context", default=None,
)


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def begin_control() -> str:
    """Caller serializes this short publication with the final SAFETY fence."""
    global _AUTHORITY
    with _AUTHORITY_LOCK:
        token = secrets.token_hex(16)
        _CONTROLS.add(token)
        _AUTHORITY = (secrets.token_hex(16), True)
        return token


def end_control(token: str) -> None:
    global _AUTHORITY
    with _AUTHORITY_LOCK:
        _CONTROLS.discard(token)
        _AUTHORITY = (secrets.token_hex(16), bool(_CONTROLS))


@contextmanager
def continuous_control_boundary(name: str) -> Iterator[None]:
    token = begin_control() if name in _CONTROL_NAMES else ""
    try:
        yield
    finally:
        if token:
            end_control(token)


def continuous_dispatch_allowed(intent: OrderIntent) -> tuple[bool, str]:
    metadata = intent.metadata if isinstance(intent.metadata, dict) else {}
    seal = metadata.get(_META)
    if seal is None:
        return True, ""
    context = _DISPATCH_CONTEXT.get()
    if not isinstance(seal, dict) or context is None or seal != context[0]:
        return False, "continuous-dispatch-capability-missing"
    epoch, blocked = _AUTHORITY
    if blocked or seal.get("epoch") != epoch:
        return False, "continuous-dispatch-control-generation-changed"
    original = json.loads(context[1])
    derived = {"trace_id", "traceId", "risk_reducing", "risk_reducing_claim_rejected", "capital_rollout", "functional_test_reservation_id", _META}
    if set(metadata) - set(original.get("metadata", {})) - derived:
        return False, "continuous-dispatch-metadata-key-added"
    current = asdict(intent)
    for key, value in original.items():
        if key != "metadata" and current.get(key) != value:
            return False, "continuous-dispatch-intent-changed"
    # submit_order_intent recomputes this untrusted flag and adds trace/permit
    # facts. Every other input to sizing, routing and portfolio identity stays sealed.
    for key, value in original.get("metadata", {}).items():
        if key not in {"risk_reducing", "risk_reducing_claim_rejected"} and metadata.get(key) != value:
            return False, "continuous-dispatch-metadata-changed"
    return True, ""


class IntentJournal:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.owner = secrets.token_hex(16)
        self._lock = threading.RLock()
        self._initialized = False

    @contextmanager
    def connection(self):
        with self._lock:
            if not self._initialized:
                self.path.parent.mkdir(parents=True, exist_ok=True)
            connection = sqlite3.connect(self.path, timeout=5.0)
            connection.row_factory = sqlite3.Row
            try:
                connection.execute("PRAGMA synchronous=FULL")
                version = connection.execute("PRAGMA user_version").fetchone()[0]
                if version not in {0, 1}:
                    raise ValueError("continuous-intent-journal-version-unsupported")
                if not self._initialized:
                    connection.execute("CREATE TABLE IF NOT EXISTS continuous_intents (command_id TEXT PRIMARY KEY, owner TEXT NOT NULL, epoch TEXT NOT NULL, payload TEXT NOT NULL, payload_hash TEXT NOT NULL, state TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, result TEXT NOT NULL)")
                    connection.execute("PRAGMA user_version=1")
                    connection.commit()
                    self._initialized = True
                with connection:
                    yield connection
            finally:
                connection.close()

    def enqueue(self, intent: OrderIntent) -> dict[str, Any]:
        payload = asdict(intent)
        metadata = payload.get("metadata") or {}
        if _META in metadata:
            raise ValueError("continuous-dispatch-seal-cannot-be-inherited")
        evaluation = str(metadata.get("runtime_evaluation_key") or "").strip()
        if not evaluation or intent.mode not in {"SMALL_LIVE", "FULL_LIVE"}:
            raise ValueError("continuous-intent-evaluation-and-live-mode-required")
        serialized = _canonical(payload)
        if len(serialized.encode("utf-8")) > _MAX_PAYLOAD:
            raise ValueError("continuous-intent-payload-too-large")
        identity = {
            "evaluation": evaluation, "strategy": intent.strategy_id,
            "broker": metadata.get("broker_id"), "symbol": intent.symbol,
            "side": intent.side, "deployment": metadata.get("deployment_id"),
            "portfolio": metadata.get("portfolio_id"),
        }
        command_id = hashlib.sha256(_canonical(identity).encode("utf-8")).hexdigest()
        payload_hash = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
        epoch, blocked = _AUTHORITY
        initial_state = "BLOCKED" if blocked else "QUEUED"
        with self.connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if self._unresolved(connection):
                initial_state = "BLOCKED"
            cursor = connection.execute(
                "INSERT OR IGNORE INTO continuous_intents VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (command_id, self.owner, epoch, serialized, payload_hash, initial_state, _now(), _now(), "{}"),
            )
            row = connection.execute("SELECT * FROM continuous_intents WHERE command_id=?", (command_id,)).fetchone()
        return {**dict(row), "created": cursor.rowcount == 1}

    def claim(self, command_id: str) -> dict[str, Any] | None:
        with self.connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT * FROM continuous_intents WHERE command_id=?", (command_id,)).fetchone()
            if row is None or row["owner"] != self.owner or row["state"] != "QUEUED":
                return None
            epoch, blocked = _AUTHORITY
            if blocked or row["epoch"] != epoch or self._unresolved(connection):
                connection.execute("UPDATE continuous_intents SET state='BLOCKED', updated_at=? WHERE command_id=?", (_now(), command_id))
                return None
            if hashlib.sha256(row["payload"].encode("utf-8")).hexdigest() != row["payload_hash"]:
                raise ValueError("continuous-intent-journal-hash-mismatch")
            connection.execute("UPDATE continuous_intents SET state='DISPATCHING', updated_at=? WHERE command_id=?", (_now(), command_id))
            return dict(row)

    def finish(self, command_id: str, status: str, result: dict[str, Any] | None = None) -> None:
        if status not in {"COMPLETED", "BLOCKED", "UNKNOWN", "ABORTED"}:
            raise ValueError("continuous-intent-invalid-outcome")
        order = (result or {}).get("order") or {}
        summary = {"ok": (result or {}).get("ok") is True, "orderId": str(order.get("order_id") or ""), "orderState": str(order.get("state") or "")}
        with self.connection() as connection:
            cursor = connection.execute(
                "UPDATE continuous_intents SET state=?, result=?, updated_at=? WHERE command_id=? AND owner=? AND state IN ('QUEUED','DISPATCHING')",
                (status, _canonical(summary), _now(), command_id, self.owner),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("continuous-intent-outcome-cas-failed")

    def _unresolved(self, connection) -> bool:
        return connection.execute(
            "SELECT 1 FROM continuous_intents WHERE state='UNKNOWN' OR (state='DISPATCHING' AND owner<>?) LIMIT 1",
            (self.owner,),
        ).fetchone() is not None

    def reconciliation_required(self) -> bool:
        if not self.path.exists():
            if self._initialized:
                raise OSError("continuous-intent-journal-disappeared")
            return False
        with self.connection() as connection:
            return self._unresolved(connection)

    def reconcile(self, dispatch_rows: list[dict[str, Any]]) -> int:
        """Close uncertainty only using matching durable order evidence.

        Production calls this after fresh broker reconciliation and sleeve-ledger
        recovery. Missing records, pending/unknown results and wrong seals stay held.
        """
        if not self.path.exists():
            return 0
        resolved = 0
        known = {"acknowledged", "partially_filled", "filled", "cancelled", "canceled", "rejected", "risk_blocked", "adapter_blocked"}
        with self.connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            for order in dispatch_rows:
                seal = order.get("continuous_dispatch")
                if not isinstance(seal, dict) or order.get("state") not in known:
                    continue
                row = connection.execute("SELECT * FROM continuous_intents WHERE command_id=?", (str(seal.get("commandId") or ""),)).fetchone()
                if row is None or row["state"] not in {"UNKNOWN", "DISPATCHING"}:
                    continue
                if row["state"] == "DISPATCHING" and row["owner"] == self.owner:
                    continue
                expected = {"commandId": row["command_id"], "owner": row["owner"], "epoch": row["epoch"], "payloadHash": row["payload_hash"]}
                if seal != expected:
                    continue
                if hashlib.sha256(row["payload"].encode("utf-8")).hexdigest() != row["payload_hash"]:
                    continue
                if order.get("state") in {"acknowledged", "partially_filled", "filled"} and str(order.get("broker_order_id") or "").strip() in {"", "-"}:
                    continue
                body = json.loads(row["payload"])
                if order.get("symbol") != body.get("symbol") or order.get("broker_id") != body.get("metadata", {}).get("broker_id"):
                    continue
                if body.get("metadata", {}).get("portfolio_execution") != order.get("portfolio_execution"):
                    continue
                connection.execute("UPDATE continuous_intents SET state='RESOLVED', updated_at=? WHERE command_id=?", (_now(), row["command_id"]))
                resolved += 1
        return resolved

    def rows(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        with self.connection() as connection:
            rows = [dict(row) for row in connection.execute("SELECT command_id, owner, state, created_at, updated_at, result FROM continuous_intents ORDER BY created_at DESC LIMIT 100")]
        for row in rows:
            # A new controller/process never steals another owner's pending row.
            # Its durable owner identity makes restart status deterministic even
            # if another process still owns the original dispatch attempt.
            if row.pop("owner") != self.owner:
                row["state"] = {"QUEUED": "INTERRUPTED", "DISPATCHING": "UNKNOWN"}.get(row["state"], row["state"])
        return rows


class ContinuousDispatcher:
    """Reusable supervisor lock: complete/checkpoint the cycle, unlock, dispatch."""
    def __init__(self, path: Path, lock: Any, submit: Callable, failure: Callable) -> None:
        self.journal = IntentJournal(path)
        self.lock = lock
        self.submit = submit
        self.failure = failure
        self._local = threading.local()
        self.last_results: list[dict[str, Any]] = []

    def __enter__(self):
        self.lock.acquire()
        depth = getattr(self._local, "depth", 0)
        if depth == 0:
            self._local.batch = []
            self._local.observers = []
        self._local.depth = depth + 1
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        depth = self._local.depth - 1
        self._local.depth = depth
        batch = self._local.batch if depth == 0 else []
        observers = self._local.observers if depth == 0 else []
        if depth == 0:
            self._local.batch = []
        self.lock.release()
        if not batch:
            return False
        if exc_type is not None or self.lock.owned_by_current_thread():
            for command_id, _callback in batch:
                try:
                    self.journal.finish(command_id, "ABORTED")
                except Exception as error:
                    self.failure(error)
            return False
        self.last_results = [self.dispatch(command_id, callback) for command_id, callback in batch]
        outcomes = {command_id: result for (command_id, _), result in zip(batch, self.last_results)}
        for observer in observers:
            observer(outcomes)
        return False

    def defer(self, intent: OrderIntent, callback: Callable | None = None) -> dict[str, Any]:
        if not self.lock.owned_by_current_thread() or not getattr(self._local, "depth", 0):
            raise RuntimeError("continuous-intent-requires-cycle-boundary")
        try:
            row = self.journal.enqueue(intent)
        except Exception as error:
            self.failure(error)
            raise
        if row["created"] and row["state"] == "QUEUED":
            self._local.batch.append((row["command_id"], callback))
            return {"ok": True, "reason": "continuous-intent-durably-queued", "deferred": True, "commandId": row["command_id"]}
        return {"ok": False, "reason": "continuous-intent-duplicate-or-control-blocked", "duplicate": not row["created"], "commandId": row["command_id"]}

    def observe(self, callback: Callable) -> None:
        if not getattr(self._local, "depth", 0):
            raise RuntimeError("continuous-observer-requires-cycle-boundary")
        self._local.observers.append(callback)

    def dispatch(self, command_id: str, callback: Callable | None = None) -> dict[str, Any]:
        if self.lock.owned_by_current_thread():
            raise RuntimeError("continuous-dispatch-cycle-lock-owned")
        # Preserve the old whole-cycle serialization between live profiles,
        # without ever holding RUNTIME or making controls wait on this lock.
        with _DISPATCH_ATTEMPT_LOCK:
            return self._dispatch_serialized(command_id, callback)

    def _dispatch_serialized(self, command_id: str, callback: Callable | None) -> dict[str, Any]:
        claimed = False
        try:
            row = self.journal.claim(command_id)
            if row is None:
                return {"ok": False, "reason": "continuous-intent-not-claimable"}
            claimed = True
            intent = OrderIntent(**json.loads(row["payload"]))
            seal = {"commandId": command_id, "epoch": row["epoch"], "payloadHash": row["payload_hash"], "owner": self.journal.owner}
            intent = replace(intent, metadata={**intent.metadata, _META: seal})
            token = _DISPATCH_CONTEXT.set((seal, row["payload"]))
            try:
                allowed, reason = continuous_dispatch_allowed(intent)
                result = self.submit(intent) if allowed else {"ok": False, "reason": reason}
                if callback is not None:
                    callback(intent, result)
            finally:
                _DISPATCH_CONTEXT.reset(token)
            order_state = str((result.get("order") or {}).get("state") or "")
            status = "UNKNOWN" if order_state in {"unknown", "dispatch_pending"} else ("COMPLETED" if result.get("ok") is True else "BLOCKED")
            self.journal.finish(command_id, status, result)
            return result
        except Exception as error:
            if claimed:
                try:
                    self.journal.finish(command_id, "UNKNOWN")
                except Exception:
                    pass
            self.failure(error)
            return {"ok": False, "reason": "continuous-dispatch-outcome-unknown", "errorType": type(error).__name__}
