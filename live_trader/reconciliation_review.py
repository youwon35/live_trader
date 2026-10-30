"""Operator acknowledgement of exact historical balance differences.

This is an annotation, never a cash-flow receipt, ledger adjustment or permission.
The raw reconciliation and every execution guard remain unchanged.
"""
from __future__ import annotations
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import re

SCHEMA = "reconciliation-operator-review/v1"
REASONS = {
    "DEPOSIT": "입금 내역을 직접 확인함",
    "WITHDRAWAL": "출금 내역을 직접 확인함",
    "TRANSFER": "계좌·지갑 간 이체를 직접 확인함",
    "EXTERNAL_TRADE": "직접 거래·수수료 내역을 확인함",
    "EXISTING_HOLDING": "기존 보유 자산임을 확인함",
    "OTHER_REVIEWED": "외부 변동을 확인했고 이 차이에 문제가 없음",
}
BROKERS = {"kis", "binance", "binance-futures", "upbit"}


def _rows(value):
    return value if isinstance(value, list) else []


def _record(value):
    return value if isinstance(value, dict) else {}


def _number(value):
    return type(value) in (int, float) and math.isfinite(value)


def _time(value):
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        # Legacy local records are from this Korean desktop application.
        # Explicit offsets are normalized, never stripped before comparison.
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone(timedelta(hours=9)))
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return None


def _hash(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def configured_review_scopes(environ):
    """Hash configured identity without exposing or querying credentials."""
    result = {}
    for broker in BROKERS:
        if broker == "kis":
            fields = [environ.get("KIS_ACCOUNT_NO"), environ.get("KIS_ACCOUNT_PRODUCT_CODE")]
        elif broker in {"binance", "binance-futures"}:
            fields = [environ.get("BINANCE_API_KEY")]
        else:
            fields = [environ.get("UPBIT_ACCESS_KEY")]
        if all(isinstance(value, str) and value.strip() for value in fields):
            result[broker] = _hash({"broker": broker, "configuredIdentity": fields})
    return result


def review_candidates(snapshot, scopes, *, now=None):
    """Use recorded broker facts, not the renderer's transport connection flag."""
    snapshot = _record(snapshot)
    now = now or datetime.now(timezone.utc)
    reconciliation = _record(snapshot.get("reconciliation"))
    ledger = _record(snapshot.get("program_ledger"))
    baseline = _record(ledger.get("state"))
    observed = _time(_record(reconciliation.get("summary")).get("last_run"))
    summary = _record(reconciliation.get("summary"))
    accounts, positions, errors = (reconciliation.get(name) for name in ("accounts", "positions", "errors"))
    if (observed is None or observed > now or not all(isinstance(rows, list) for rows in (accounts, positions, errors))
            or not all(isinstance(row, dict) for row in accounts + positions)):
        return []
    for name, count in (("account_count", len(accounts)), ("position_count", len(positions)),
                        ("error_count", len(errors)),
                        ("mismatch_count", sum(row.get("status") == "mismatch" for row in accounts + positions)),
                        ("api_required_count", sum(row.get("status") == "api_required" for row in accounts + positions))):
        if type(summary.get(name)) is not int or summary[name] != count:
            return []
    if any(row.get("status") not in {"pass", "mismatch", "api_required", "capability_unavailable"} for row in accounts + positions):
        return []
    cache = _record(snapshot.get("broker_observations"))
    # Unknown or actual app executions are not historical balance annotations.
    # They must retain their normal execution/position reconciliation path.
    if (type(ledger.get("fill_event_count")) is not int or ledger["fill_event_count"] != 0
            or type(ledger.get("other_event_count")) is not int or ledger["other_event_count"] != 0
            or type(ledger.get("snapshot_event_count")) is not int or ledger["snapshot_event_count"] <= 0):
        return []
    # Any incomplete observation keeps all differences visible. Reading a
    # historical annotation never turns an API failure into a successful read.
    if errors:
        return []
    result = []
    for kind, raw in (("cash", reconciliation.get("accounts")), ("position", reconciliation.get("positions"))):
        items = _rows(raw)
        for row in items:
            if not isinstance(row, dict) or row.get("status") != "mismatch" or row.get("comparison_issue"):
                continue
            broker = row.get("broker_id")
            scope = scopes.get(broker)
            if broker not in BROKERS or not isinstance(scope, str) or not re.fullmatch(r"[0-9a-f]{64}", scope):
                continue
            if _record(cache.get("account_review_scopes")).get(broker) != scope:
                continue  # Credentials changed since the observed account read.
            currency = str(row.get("currency") or "").strip().upper()
            symbol = str(row.get("symbol") or "").strip()
            side = str(row.get("position_side") or "").strip()
            matching = [r for r in items if isinstance(r, dict) and r.get("broker_id") == broker
                        and (r.get("currency") == row.get("currency") if kind == "cash"
                             else r.get("symbol") == symbol and str(r.get("position_side") or "").strip() == side)]
            if len(matching) != 1:
                continue
            account_rows = [r for r in accounts if r.get("broker_id") == broker]
            if len(account_rows) != 1:
                continue
            account_row = account_rows[0]
            account_observed = _time(account_row.get("observed_at"))
            if (account_row.get("lookup_status") not in {"success", "stale"}
                    or account_row.get("observation_source") != "BROKER_ACCOUNT_SNAPSHOT"
                    or account_observed is None or account_observed > now
                    or account_row.get("comparison_issue") or not str(account_row.get("account") or "").strip()):
                continue
            classification = ""
            if kind == "cash":
                before, after = row.get("program_cash_value"), row.get("broker_cash_value")
                stamp = _time(row.get("program_updated_at"))
                account = str(row.get("account") or "").strip()
                if (row.get("program_source") == "event_poll" and account and currency
                        and currency == str(row.get("program_currency") or "").strip().upper()
                        and _number(before) and _number(after) and before != after
                        and stamp is not None and stamp <= account_observed
                        and "last_baseline" in baseline and not baseline["last_baseline"]
                        and "last_baseline_source" in baseline and not baseline["last_baseline_source"]):
                    classification = "HISTORICAL_BALANCE_REFERENCE"
            else:
                before, after = row.get("program_qty_value"), row.get("broker_qty_value")
                account = str(account_row.get("account") or "").strip()
                position_observed = _time(_record(_record(cache.get("position_observations")).get(broker)).get("observedAt"))
                successful = cache.get("successful_position_brokers")
                cached_rows = cache.get("positions")
                if (not isinstance(successful, list) or broker not in successful
                        or position_observed is None or position_observed > now
                        or not isinstance(cached_rows, list)):
                    continue
                cached_matches = [r for r in cached_rows if isinstance(r, dict) and r.get("broker_id") == broker and r.get("symbol") == symbol]
                if len(cached_matches) != 1 or cached_matches[0].get("broker_qty") != after:
                    continue
                positions = ledger.get("positions")
                if not isinstance(positions, list):
                    continue
                if (broker in {"binance", "upbit"} and row.get("program_source") == "missing"
                        and _number(before) and before == 0 and _number(after) and after > 0 and symbol
                        and ledger.get("fill_event_count") == 0 and ledger.get("other_event_count") == 0
                        and type(ledger.get("snapshot_event_count")) is int and ledger["snapshot_event_count"] > 0
                        and all(isinstance(r, dict) and r.get("source") == "event_poll" for r in positions)
                        and not any(r.get("broker_id") == broker and r.get("symbol") == symbol for r in positions)):
                    classification = "UNMANAGED_SPOT_INVENTORY"
            if not classification:
                continue
            binding = {"schema": SCHEMA, "kind": kind, "brokerId": broker, "accountScope": scope,
                       "accountRowHash": _hash(account), "currency": currency, "symbol": symbol, "side": side,
                       "programSource": row.get("program_source"), "baselineAt": row.get("program_updated_at") or "",
                       "before": before, "after": after, "classification": classification,
                       "baselineState": {k: baseline.get(k) for k in ("last_baseline", "last_baseline_source")},
                       "fillCount": ledger.get("fill_event_count"), "otherCount": ledger.get("other_event_count")}
            result.append({"fingerprint": _hash(binding), **binding,
                           "observedAt": _record(reconciliation.get("summary")).get("last_run"),
                           "cashFlowVerified": False, "executionAuthorityGranted": False})
    return result


def review_document(snapshot, scopes, store):
    candidates = review_candidates(snapshot, scopes)
    try:
        records = store.balance_reviews()
    except (OSError, ValueError, RuntimeError, __import__("sqlite3").Error):
        return {"schemaVersion": SCHEMA, "items": [], "error": "확인 기록을 읽지 못했습니다. 원래 차이를 표시합니다.",
                "executionAuthorityGranted": False}
    by_key = {row["fingerprint"]: row for row in records}
    result = []
    for item in candidates:
        review = by_key.get(item["fingerprint"], {})
        result.append({**item, "acknowledged": review.get("action") == "ACKNOWLEDGE",
                       "reviewedAt": review.get("recorded_at"), "reviewReason": review.get("reason"),
                       "reviewReasonLabel": REASONS.get(review.get("reason"), "")})
    return {"schemaVersion": SCHEMA, "items": result,
            "acknowledgedCount": sum(item["acknowledged"] for item in result),
            "cashFlowVerified": False, "executionAuthorityGranted": False,
            "detail": "사용자가 확인한 동일 계좌·통화·금액의 과거 차이만 접어 둡니다. 원장·주문 허용은 변경하지 않습니다."}


def acknowledge_review(payload, snapshot, scopes, store):
    if (not isinstance(payload, dict) or set(payload) != {"fingerprint", "reason", "action", "confirmed"}
            or payload.get("confirmed") is not True or not isinstance(payload.get("reason"), str)
            or payload.get("reason") not in REASONS or not isinstance(payload.get("action"), str)
            or payload.get("action") not in {"ACKNOWLEDGE", "REOPEN"}):
        raise ValueError("항목과 확인 사유를 선택하고 직접 확인해 주세요.")
    fingerprint = payload.get("fingerprint")
    if not isinstance(fingerprint, str) or not re.fullmatch(r"[0-9a-f]{64}", fingerprint):
        raise ValueError("확인할 차이의 식별값이 올바르지 않습니다.")
    candidates = review_candidates(snapshot, scopes)
    matches = [row for row in candidates if row["fingerprint"] == fingerprint]
    if len(matches) != 1:
        raise ValueError("계좌·금액·기준이 바뀌었거나 확인 가능한 과거 차이가 아닙니다. 현재 항목을 다시 확인하세요.")
    return store.save_balance_review(matches[0], payload["reason"], payload["action"])

