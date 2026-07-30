from __future__ import annotations

import json
import math
import os
from datetime import datetime
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "live-account-risk-budget-v1"


def _number(value: object) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _account_key(row: dict[str, Any]) -> str:
    broker_id = str(row.get("broker_id") or "").strip().lower()
    currency = str(row.get("currency") or "").strip().upper()
    return f"{broker_id}:{currency}" if broker_id and currency else ""


def _read(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False),
        encoding="utf-8",
    )
    temporary.replace(path)


def load_account_risk_budget(path: Path) -> dict[str, Any]:
    payload = _read(path)
    budgets = (
        payload.get("budgets")
        if isinstance(payload.get("budgets"), dict)
        else {}
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "updated_at": str(payload.get("updated_at") or ""),
        "budgets": {
            str(key): dict(value)
            for key, value in budgets.items()
            if isinstance(value, dict)
        },
        "error": "",
    }


def update_account_risk_budget(
    path: Path,
    accounts: list[dict[str, Any]],
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Persist a restart-safe equity baseline for each broker/currency.

    The baseline rolls at the computer's local calendar day because the Live
    Trader operator and its daily loss setting use that same local day. A
    broker deposit/withdrawal is intentionally not guessed from account
    snapshots; an operator should reset the budget explicitly after a
    confirmed cash movement.
    """

    observed_at = now or datetime.now().astimezone()
    if observed_at.tzinfo is None:
        observed_at = observed_at.astimezone()
    day = observed_at.date().isoformat()
    current = load_account_risk_budget(path)
    budgets = dict(current["budgets"])

    for row in accounts:
        if not isinstance(row, dict):
            continue
        key = _account_key(row)
        equity = _number(
            row.get("broker_equity")
            if row.get("broker_equity") is not None
            else row.get("broker_cash")
        )
        available = _number(row.get("broker_cash"))
        if not key or equity is None or equity < 0 or available is None:
            continue
        previous = budgets.get(key) if isinstance(budgets.get(key), dict) else {}
        starting_equity = _number(previous.get("starting_equity"))
        same_day = (
            str(previous.get("day") or "") == day
            and starting_equity is not None
        )
        if not same_day:
            starting_equity = equity
        pnl = equity - starting_equity
        pnl_pct = (
            (pnl / starting_equity) * 100.0
            if starting_equity > 0
            else 0.0
        )
        previous_minimum = (
            _number(previous.get("minimum_daily_pnl_pct"))
            if same_day
            else None
        )
        budgets[key] = {
            "broker_id": str(row.get("broker_id") or "").strip().lower(),
            "currency": str(row.get("currency") or "").strip().upper(),
            "day": day,
            "starting_equity": starting_equity,
            "current_equity": equity,
            "available_cash": available,
            "daily_pnl": pnl,
            "daily_pnl_pct": pnl_pct,
            "minimum_daily_pnl_pct": (
                min(previous_minimum, pnl_pct)
                if previous_minimum is not None
                else pnl_pct
            ),
            "observed_at": observed_at.isoformat(timespec="seconds"),
            "observed_epoch": observed_at.timestamp(),
        }

    document = {
        "schema_version": SCHEMA_VERSION,
        "updated_at": observed_at.isoformat(timespec="seconds"),
        "budgets": budgets,
    }
    _write(path, document)
    return {**document, "error": ""}


def broker_account_risk(
    snapshot: dict[str, Any],
    broker_id: str,
    *,
    currency: str = "",
    now_epoch: float | None = None,
    maximum_age_seconds: float = 120.0,
) -> dict[str, Any]:
    budgets = (
        snapshot.get("budgets")
        if isinstance(snapshot.get("budgets"), dict)
        else {}
    )
    normalized_broker = str(broker_id or "").strip().lower()
    normalized_currency = str(currency or "").strip().upper()
    candidates = [
        dict(value)
        for value in budgets.values()
        if isinstance(value, dict)
        and str(value.get("broker_id") or "").strip().lower()
        == normalized_broker
        and (
            not normalized_currency
            or str(value.get("currency") or "").strip().upper()
            == normalized_currency
        )
    ]
    if not candidates:
        return {
            "known": False,
            "fresh": False,
            "reason": "account-risk-budget-missing",
        }
    selected = max(
        candidates,
        key=lambda item: _number(item.get("observed_epoch")) or 0.0,
    )
    observed_epoch = _number(selected.get("observed_epoch"))
    current_epoch = (
        float(now_epoch)
        if now_epoch is not None
        else datetime.now().astimezone().timestamp()
    )
    age_seconds = (
        max(0.0, current_epoch - observed_epoch)
        if observed_epoch is not None
        else float("inf")
    )
    return {
        **selected,
        "known": True,
        "fresh": age_seconds <= maximum_age_seconds,
        "age_seconds": age_seconds,
        "reason": (
            "ok"
            if age_seconds <= maximum_age_seconds
            else "account-risk-budget-stale"
        ),
    }
