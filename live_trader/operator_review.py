"""Local operator annotations and captured preflight comparisons; no trading I/O."""
from __future__ import annotations
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3


def record_key(record: dict) -> str:
    event_id = record.get("event_id") or record.get("eventId")
    identity = {"event_id": event_id} if event_id else {
        key: record.get(key) for key in ("timestamp", "occurred_at", "created_at", "time", "source", "event", "message", "detail", "level", "order_id", "strategy_id", "session_id")
    }
    return hashlib.sha256(json.dumps(identity, sort_keys=True, ensure_ascii=False, default=str).encode()).hexdigest()


class OperatorReviewStore:
    def __init__(self, path: Path):
        self.path = Path(path)

    @contextmanager
    def connection(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(self.path, timeout=2)
        db.row_factory = sqlite3.Row
        try:
            with db:
                db.execute("CREATE TABLE IF NOT EXISTS notes (record_key TEXT PRIMARY KEY, text TEXT NOT NULL, revision INTEGER NOT NULL, updated_at TEXT NOT NULL)")
                db.execute("CREATE TABLE IF NOT EXISTS note_history (id INTEGER PRIMARY KEY, record_key TEXT NOT NULL, text TEXT NOT NULL, revision INTEGER NOT NULL, updated_at TEXT NOT NULL)")
                db.execute("CREATE TABLE IF NOT EXISTS preflights (id INTEGER PRIMARY KEY, scope TEXT NOT NULL, recorded_at TEXT NOT NULL, manifest_hash TEXT NOT NULL, payload TEXT NOT NULL)")
                yield db
        finally:
            db.close()

    @staticmethod
    def validate_key(key):
        if not isinstance(key,str) or not re.fullmatch(r"[0-9a-f]{64}",key):
            raise ValueError("실행 기록 식별값을 확인할 수 없습니다.")

    def note(self, key):
        self.validate_key(key)
        with self.connection() as db:
            row = db.execute("SELECT * FROM notes WHERE record_key=?",(key,)).fetchone()
            history = db.execute("SELECT text,revision,updated_at FROM note_history WHERE record_key=? ORDER BY revision DESC LIMIT 10",(key,)).fetchall()
        return {**(dict(row) if row else {"record_key":key,"text":"","revision":0,"updated_at":""}), "history":[dict(row) for row in history]}

    def save_note(self, key, text, revision):
        self.validate_key(key)
        if not isinstance(text,str) or len(text)>500 or any(ord(c)<32 and c not in "\n\t" for c in text):
            raise ValueError("메모는 제어문자 없이 500자 이내로 입력하세요.")
        if type(revision) is not int or revision<0:
            raise ValueError("메모 버전을 확인할 수 없습니다.")
        now=datetime.now(timezone.utc).isoformat()
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            row=db.execute("SELECT revision FROM notes WHERE record_key=?",(key,)).fetchone()
            actual=int(row[0]) if row else 0
            if revision!=actual:
                raise ValueError("다른 창에서 메모가 바뀌었습니다. 다시 불러온 뒤 저장하세요.")
            db.execute("INSERT OR REPLACE INTO notes VALUES (?,?,?,?)",(key,text.strip(),actual+1,now))
            db.execute("INSERT INTO note_history(record_key,text,revision,updated_at) VALUES (?,?,?,?)",(key,text.strip(),actual+1,now))
        return self.note(key)

    def capture_preflight(self, scope, checks, manifest_hash="", risk_settings=None):
        rows=[{key:item.get(key) for key in ("label","status","detail")} for item in checks if isinstance(item,dict)]
        if not rows:
            return
        payload={"checks":rows,"riskSettings":dict(risk_settings or {})}
        with self.connection() as db:
            db.execute("INSERT INTO preflights(scope,recorded_at,manifest_hash,payload) VALUES (?,?,?,?)",(str(scope or "global"),datetime.now(timezone.utc).isoformat(),str(manifest_hash or ""),json.dumps(payload,ensure_ascii=False,allow_nan=False)))
            db.execute("DELETE FROM preflights WHERE scope=? AND id NOT IN (SELECT id FROM preflights WHERE scope=? ORDER BY id DESC LIMIT 40)",(str(scope or "global"),str(scope or "global")))

    def preflights(self, scope):
        with self.connection() as db:
            rows=db.execute("SELECT id,scope,recorded_at,manifest_hash,payload FROM preflights WHERE scope=? ORDER BY id DESC LIMIT 2",(str(scope or "global"),)).fetchall()
        return [{**{key:row[key] for key in ("id","scope","recorded_at","manifest_hash")},**json.loads(row["payload"])} for row in rows]

    def risk_at_manifest(self, manifest_hash):
        if not manifest_hash: return None
        with self.connection() as db:
            row=db.execute("SELECT payload FROM preflights WHERE manifest_hash=? ORDER BY id DESC LIMIT 1",(manifest_hash,)).fetchone()
        return json.loads(row[0]).get("riskSettings") if row else None


def manifest_weights(manifest, portfolio):
    """Weights from the exact verified portfolio supplied by the caller, never current unrelated rows."""
    members = (manifest.get("metadata") or {}).get("strategyMembers") or []
    if not isinstance(portfolio, dict):
        return {str(row.get("strategyInstanceId") or row.get("strategyId")) + ":" + str(row.get("symbol")): None for row in members}
    import math
    def number(value):
        if value is None or isinstance(value, bool): return None
        try: result = float(value)
        except (TypeError, ValueError): return None
        return result if math.isfinite(result) else None
    result = {}
    for member in members:
        iid, sid, symbol = member.get("strategyInstanceId"), member.get("strategyId"), member.get("symbol")
        instance = next((row for row in portfolio.get("strategy_instances", []) if (row.get("instanceId") == iid if iid else row.get("strategyId") == sid and row.get("symbol") == symbol)), {})
        policy = next((row for row in (portfolio.get("portfolio_policy") or {}).get("allocations", []) if iid and row.get("strategyInstanceId") == iid), None)
        target = next((row for row in portfolio.get("target_portfolio", []) if row.get("strategyId") == sid and row.get("symbol") == symbol), {})
        allocation = instance.get("allocation") or {}
        weight = number((policy if policy is not None else target).get("targetWeight"))
        if weight is None: weight = number(allocation.get("scoreTargetWeight"))
        if weight is None: weight = number(allocation.get("normalizedWeight"))
        fraction = number((policy or {}).get("positionSizeFraction"))
        if fraction is None: fraction = number(instance.get("positionSizeFraction"))
        if fraction is None: fraction = 1
        result[str(iid or sid) + ":" + str(symbol)] = weight * fraction if weight is not None else None
    return result


def read_order_audit(path, order_id, offset=0):
    """Read a page of this exact order's immutable audit history, including older events."""
    if not isinstance(order_id, str) or not order_id.strip() or len(order_id) > 200:
        raise ValueError("주문 식별값을 확인하세요.")
    if type(offset) is not int or offset < 0 or offset > 100000:
        raise ValueError("조사 기록 조회 위치가 올바르지 않습니다.")
    path = Path(path)
    if not path.exists(): return {"events": [], "total": 0, "nextOffset": None}
    db = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=2)
    db.row_factory = sqlite3.Row
    try:
        count = db.execute("SELECT COUNT(*) FROM audit_events WHERE order_id=?", (order_id,)).fetchone()[0]
        rows = db.execute("SELECT event_id,occurred_at,category,level,source,message,order_id,reason,state,trace_id FROM audit_events WHERE order_id=? ORDER BY occurred_at DESC,event_id DESC LIMIT 50 OFFSET ?", (order_id,offset)).fetchall()
        return {"events": [dict(row) for row in rows], "total": count, "nextOffset": offset+len(rows) if offset+len(rows)<count else None}
    finally:
        db.close()
