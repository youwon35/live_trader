"""Append-only cash-operation facts. Existing account holdings never become ours."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import json
import sqlite3

from trading_runtime.artifact_governance import stable_sha256
from .toss_adapter import number, exact_owned_order


def terminal_fill(order, proof):
    row = exact_owned_order(order, proof)
    if row["state"] not in {"filled", "canceled", "rejected"}:
        raise ValueError("TOSS_ORDER_NOT_TERMINAL")
    quantity = number(row["filled_quantity"])
    execution = row["execution"]
    if not quantity:
        if any(execution.get(key) not in (None, "0", 0) for key in ("filledAmount", "commission", "tax")):
            raise ValueError("TOSS_ZERO_FILL_COST_UNCONFIRMED")
        amount = fee = Decimal(0)
    else:
        amount = number(execution.get("filledAmount"), positive=True)
        # Null fees/taxes are unknown, not a free trade. Aggregate official
        # filledAmount avoids multiplying a rounded average price by quantity.
        fee = number(execution.get("commission")) + number(execution.get("tax"))
        seen = datetime.fromisoformat(str(execution.get("filledAt") or "").replace("Z", "+00:00"))
        if seen.tzinfo is None:
            raise ValueError("TOSS_FILL_TIMESTAMP_MISSING")
    return {"orderId": row["broker_order_id"], "symbol": row["symbol"],
            "side": row["side"], "currency": row["currency"], "quantity": str(quantity),
            "amount": str(amount), "fee": str(fee), "state": row["state"],
            "filledAt": execution.get("filledAt"), "rawHash": stable_sha256(order)}


class TossCashLedger:
    """One owner ID is never recycled, even after process failure or unknown POST."""
    def __init__(self, path):
        self.path = Path(path)

    def _open(self, write=False):
        if not write:
            return sqlite3.connect(self.path.resolve().as_uri() + "?mode=ro", uri=True) if self.path.exists() else None
        self.path.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(self.path, timeout=5)
        db.execute("CREATE TABLE IF NOT EXISTS facts (key TEXT PRIMARY KEY, owner TEXT NOT NULL, kind TEXT NOT NULL, payload TEXT NOT NULL, digest TEXT NOT NULL)")
        for op in ("UPDATE", "DELETE"):
            db.execute(f"CREATE TRIGGER IF NOT EXISTS facts_no_{op.lower()} BEFORE {op} ON facts BEGIN SELECT RAISE(ABORT, 'toss-ledger-append-only'); END")
        return db

    def append(self, owner, kind, key, value):
        raw = json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False)
        digest = stable_sha256(value)
        db = self._open(True)
        try:
            with db:
                db.execute("BEGIN IMMEDIATE")
                prior = db.execute("SELECT owner,kind,digest FROM facts WHERE key=?", (key,)).fetchone()
                if prior:
                    if prior != (owner, kind, digest):
                        raise ValueError("TOSS_LEDGER_IDENTITY_CONFLICT")
                    return False
                db.execute("INSERT INTO facts VALUES (?,?,?,?,?)", (key, owner, kind, raw, digest))
        finally:
            db.close()
        return True

    def facts(self, owner=None):
        db = self._open()
        if db is None:
            return []
        try:
            rows = db.execute("SELECT owner,kind,payload,digest FROM facts" + (" WHERE owner=?" if owner else "") + " ORDER BY rowid", (owner,) if owner else ()).fetchall()
        finally:
            db.close()
        result = []
        for who, kind, raw, digest in rows:
            payload = json.loads(raw)
            if stable_sha256(payload) != digest:
                raise ValueError("TOSS_LEDGER_FACT_CHANGED")
            result.append({"owner": who, "kind": kind, "payload": payload})
        return result

    def unfinished(self):
        rows = self.facts()
        started = {row["owner"] for row in rows if row["kind"] == "START"}
        flat = {row["owner"] for row in rows if row["kind"] == "RELEASED_FLAT"}
        return sorted(started - flat)

    def initialize(self, owner, scope, budgets):
        if not owner.startswith("live-runtime:toss-") or not budgets or any(c not in {"KRW", "USD"} for c in budgets):
            raise ValueError("TOSS_CASH_SCOPE_REQUIRED")
        amounts = {key: str(number(value, positive=True)) for key, value in budgets.items()}
        return self.append(owner, "START", owner + ":start", {"scope": scope, "budgets": amounts})

    def register(self, owner, client_id, request, spec_id):
        return self.append(owner, "INTENT", owner + ":intent:" + client_id,
                           {"clientOrderId": client_id, "request": request, "strategyInstanceId": spec_id})

    def acknowledge(self, owner, client_id, response):
        if not response.get("orderId") or response.get("clientOrderId") != client_id:
            raise ValueError("TOSS_ACK_IDENTITY_UNKNOWN")
        return self.append(owner, "ACK", owner + ":ack:" + client_id,
                           {"clientOrderId": client_id, "orderId": response["orderId"], "rawHash": stable_sha256(response)})

    def orders(self, owner):
        rows = self.facts(owner)
        acks = {r["payload"]["clientOrderId"]: r["payload"] for r in rows if r["kind"] == "ACK"}
        fills = {r["payload"]["orderId"]: r["payload"] for r in rows if r["kind"] == "TERMINAL"}
        result = []
        for fact in rows:
            if fact["kind"] != "INTENT":
                continue
            row = dict(fact["payload"])
            ack = acks.get(row["clientOrderId"])
            row["ack"] = ack
            row["terminal"] = fills.get(ack["orderId"]) if ack else None
            result.append(row)
        return result

    def reconcile_order(self, owner, entry, raw):
        ack, request = entry.get("ack"), entry["request"]
        if not ack:
            raise ValueError("TOSS_ORDER_ACK_UNKNOWN_NO_RETRY")
        proof = {**request, "clientOrderId": entry["clientOrderId"],
                 "response": {"brokerOrderId": ack["orderId"], "responseHash": ack["rawHash"]}}
        fill = terminal_fill(raw, proof)
        return self.append(owner, "TERMINAL", owner + ":terminal:" + ack["orderId"],
                           {**fill, "strategyInstanceId": entry["strategyInstanceId"]})

    def project(self, owner):
        rows = self.facts(owner)
        starts = [r["payload"] for r in rows if r["kind"] == "START"]
        if len(starts) != 1:
            raise ValueError("TOSS_OWNED_PRINCIPAL_MISSING")
        cash = {key: number(value) for key, value in starts[0]["budgets"].items()}
        positions = {}
        for row in rows:
            if row["kind"] != "TERMINAL":
                continue
            f = row["payload"]
            key = f["strategyInstanceId"]
            qty, amount, fee = number(f["quantity"]), number(f["amount"]), number(f["fee"])
            if f["currency"] not in cash:
                raise ValueError("TOSS_FILL_CURRENCY_OUTSIDE_BUDGET")
            positions[key] = positions.get(key, Decimal(0)) + (qty if f["side"] == "BUY" else -qty)
            cash[f["currency"]] += (-amount-fee if f["side"] == "BUY" else amount-fee)
            if positions[key] < 0 or cash[f["currency"]] < 0:
                raise ValueError("TOSS_OWNED_OVERSPEND_OR_OVERSELL")
        orders = self.orders(owner)
        return {"cash": {k: str(v) for k, v in cash.items()},
                "positions": {k: str(v) for k, v in positions.items()},
                "unresolvedOrders": sum(r["terminal"] is None for r in orders),
                "orderCount": len(orders), "terminalCount": sum(r["terminal"] is not None for r in orders),
                "fillCount": sum(number(r["terminal"]["quantity"]) > 0 for r in orders if r["terminal"]),
                "scope": starts[0]["scope"], "initialBudgets": starts[0]["budgets"]}
