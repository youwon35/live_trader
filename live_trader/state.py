from __future__ import annotations

import os
import csv
import html
from dataclasses import dataclass
from datetime import datetime, timedelta
from io import StringIO
from typing import Any, Literal

from .brokers import broker_adapter_contract, broker_diagnostics, broker_readiness, real_orders_enabled
from .contracts import can_live_use_artifact, load_strategy_artifacts, sample_strategy_artifacts


Mode = Literal["MONITOR", "SMALL_LIVE", "FULL_LIVE"]
CheckStatus = Literal["pass", "warn", "fail"]


RISK_SETTING_META: dict[str, dict[str, object]] = {
    "daily_loss_limit_pct": {
        "label": "일일 손실 한도",
        "unit": "%",
        "min": -20.0,
        "max": -0.1,
        "step": 0.1,
        "detail": "계좌 기준 일일 손실이 이 값을 넘으면 신규 진입과 LIVE 전환을 차단합니다.",
    },
    "strategy_capital_limit_krw": {
        "label": "전략별 자본 한도",
        "unit": "KRW",
        "min": 100000.0,
        "max": 1000000000.0,
        "step": 100000.0,
        "detail": "단일 전략이 사용할 수 있는 최대 명목 금액입니다.",
    },
    "duplicate_order_cooldown_sec": {
        "label": "중복 주문 쿨다운",
        "unit": "초",
        "min": 10.0,
        "max": 3600.0,
        "step": 10.0,
        "detail": "같은 전략/심볼/방향 주문을 다시 받을 때 필요한 최소 간격입니다.",
    },
    "max_slippage_bps": {
        "label": "슬리피지 한도",
        "unit": "bps",
        "min": 1.0,
        "max": 500.0,
        "step": 1.0,
        "detail": "호가 기반 예상 슬리피지가 이 값을 넘으면 주문을 차단합니다.",
    },
    "max_symbol_exposure_pct": {
        "label": "종목별 최대 노출",
        "unit": "%",
        "min": 1.0,
        "max": 100.0,
        "step": 1.0,
        "detail": "단일 종목이 전체 계좌에서 차지할 수 있는 최대 비중입니다.",
    },
    "max_open_orders": {
        "label": "최대 열린 주문",
        "unit": "건",
        "min": 1.0,
        "max": 100.0,
        "step": 1.0,
        "detail": "동시에 열려 있을 수 있는 주문 수를 제한합니다.",
    },
}

DEFAULT_RISK_SETTINGS: dict[str, float] = {
    "daily_loss_limit_pct": -2.0,
    "strategy_capital_limit_krw": 20000000.0,
    "duplicate_order_cooldown_sec": 180.0,
    "max_slippage_bps": 50.0,
    "max_symbol_exposure_pct": 20.0,
    "max_open_orders": 5.0,
}

CHECKLIST_ITEMS: tuple[dict[str, object], ...] = (
    {
        "key": "api_keys_reviewed",
        "label": "API 키/계좌 확인",
        "detail": "실계좌 키가 코드가 아닌 환경 변수에만 있는지 확인합니다.",
        "required": True,
    },
    {
        "key": "risk_limits_reviewed",
        "label": "리스크 한도 검토",
        "detail": "손실/노출/슬리피지 한도를 오늘 운용 계획에 맞게 확인합니다.",
        "required": True,
    },
    {
        "key": "position_reconcile_reviewed",
        "label": "포지션 대조 확인",
        "detail": "프로그램 포지션과 브로커 계좌 포지션 불일치 가능성을 확인합니다.",
        "required": True,
    },
    {
        "key": "notification_channel_reviewed",
        "label": "알림 채널 확인",
        "detail": "장애/주문 차단 알림을 받을 채널을 확인합니다.",
        "required": False,
    },
    {
        "key": "operator_takeover_ready",
        "label": "수동 개입 준비",
        "detail": "긴급 정지 후 수동 청산/주문 취소 절차를 준비합니다.",
        "required": True,
    },
)

RETRY_POLICY_META: dict[str, dict[str, object]] = {
    "max_attempts": {
        "label": "최대 재시도",
        "unit": "회",
        "type": "number",
        "min": 1.0,
        "max": 10.0,
        "step": 1.0,
        "detail": "같은 주문 의도가 실패했을 때 자동/수동 재시도가 가능한 최대 횟수입니다.",
    },
    "backoff_sec": {
        "label": "재시도 대기",
        "unit": "초",
        "type": "number",
        "min": 5.0,
        "max": 3600.0,
        "step": 5.0,
        "detail": "실패 후 다음 재시도 가능 시각까지 기다리는 기본 시간입니다.",
    },
    "retry_on_network_error": {
        "label": "네트워크 오류 재시도",
        "unit": "",
        "type": "boolean",
        "detail": "브로커 API 네트워크 오류일 때 재시도 대상으로 표시합니다.",
    },
    "retry_on_rate_limit": {
        "label": "레이트 리밋 재시도",
        "unit": "",
        "type": "boolean",
        "detail": "거래소 레이트 리밋 응답일 때 재시도 대상으로 표시합니다.",
    },
}

DEFAULT_RETRY_POLICY: dict[str, float | bool] = {
    "max_attempts": 3.0,
    "backoff_sec": 30.0,
    "retry_on_network_error": True,
    "retry_on_rate_limit": True,
}

FINAL_ORDER_STATES = {"dry_run", "sent", "filled", "canceled", "retry_exhausted"}


@dataclass(frozen=True)
class Check:
    label: str
    status: CheckStatus
    detail: str

    def to_dict(self) -> dict[str, str]:
        return {"label": self.label, "status": self.status, "detail": self.detail}


STATE: dict[str, Any] = {
    "mode": "MONITOR",
    "dry_run": True,
    "kill_switch": False,
    "new_entries_blocked": True,
    "operator_confirmed": False,
    "risk_settings": dict(DEFAULT_RISK_SETTINGS),
    "checklist": {str(item["key"]): False for item in CHECKLIST_ITEMS},
    "retry_policy": dict(DEFAULT_RETRY_POLICY),
    "orders": [],
    "audit": [
        {
            "time": "08:57:04",
            "level": "warn",
            "event": "실거래 주문 어댑터 대기",
            "detail": "KIS/Binance signed order adapter가 구현/검증되기 전까지 모든 주문은 차단됩니다.",
        },
        {
            "time": "08:55:42",
            "level": "info",
            "event": "계약 로드",
            "detail": "trading-contracts의 live_allowed 권한을 Python 게이트에서 미러링합니다.",
        },
    ],
}


def now_text() -> str:
    return datetime.now().strftime("%H:%M:%S")


def future_text(seconds: float) -> str:
    return (datetime.now() + timedelta(seconds=max(0.0, seconds))).strftime("%H:%M:%S")


def env_enabled(name: str, default: bool = False) -> bool:
    value = os.getenv(name, "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on"}


def strategy_rows() -> list[dict[str, Any]]:
    artifacts = load_strategy_artifacts()
    if not artifacts:
        artifacts = sample_strategy_artifacts()
    rows = []
    for artifact in artifacts:
        live_allowed = can_live_use_artifact(artifact)
        fail_reasons = artifact.get("permissions", {}).get("fail_reasons", [])
        rows.append(
            {
                **artifact,
                "live_allowed": live_allowed,
                "permission_label": "LIVE OK" if live_allowed else "LIVE BLOCKED",
                "block_reason": "; ".join(fail_reasons) if fail_reasons else ("실거래 허용" if live_allowed else "live_allowed 권한이 없습니다."),
            }
        )
    return rows


def market_sessions() -> list[dict[str, str]]:
    return [
        {"label": "KRX", "state": "watch", "time": "KST", "detail": "정규장/동시호가 구분 필요"},
        {"label": "NYSE", "state": "closed", "time": "ET", "detail": "정규장 외 주문 품질 주의"},
        {"label": "Binance", "state": "open", "time": "24/7", "detail": "레이트 리밋/급변동 감시"},
        {"label": "Risk Engine", "state": "blocked", "time": "LIVE GATE", "detail": "API/권한 준비 전 차단"},
    ]


def readiness_checks(strategies: list[dict[str, Any]], brokers: list[dict[str, object]]) -> list[Check]:
    live_ready_count = sum(1 for strategy in strategies if strategy.get("live_allowed") is True)
    missing_brokers = [broker for broker in brokers if broker["status"] == "missing_credentials"]
    adapter_blocked = [broker for broker in brokers if broker["live_order_adapter_ready"] is not True]
    checklist = checklist_rows()
    checklist_missing = [item for item in checklist if item["required"] and not item["checked"]]
    checks = [
        Check(
            "Dry Run 보호",
            "pass" if STATE["dry_run"] else "warn",
            "Dry Run이 켜져 있어 주문은 브로커로 전송되지 않습니다." if STATE["dry_run"] else "Dry Run이 꺼져 있습니다. 실제 주문 전송 게이트가 더 엄격하게 적용됩니다.",
        ),
        Check(
            "실거래 라우트 환경",
            "pass" if real_orders_enabled() else "fail",
            "LIVE_TRADER_ENABLE_REAL_ORDERS=true가 필요합니다." if not real_orders_enabled() else "실거래 라우트 환경 변수가 켜져 있습니다.",
        ),
        Check(
            "운영 체크리스트",
            "fail" if checklist_missing else "pass",
            f"필수 체크리스트 {len(checklist_missing)}개가 남아 있습니다." if checklist_missing else "필수 운영 체크리스트가 완료되었습니다.",
        ),
        Check(
            "브로커 API 키",
            "fail" if missing_brokers else "pass",
            "필수 API 환경 변수가 비어 있습니다." if missing_brokers else "필수 API 환경 변수가 채워져 있습니다.",
        ),
        Check(
            "주문 어댑터 구현",
            "fail" if adapter_blocked else "pass",
            "KIS/Binance 실주문 서명/전송 어댑터 구현 및 감사가 필요합니다." if adapter_blocked else "주문 어댑터가 준비되었습니다.",
        ),
        Check(
            "전략 live_allowed",
            "pass" if live_ready_count else "fail",
            f"실거래 허용 전략 {live_ready_count}개" if live_ready_count else "Backtester/Paper 승인 artifact의 live_allowed=true가 필요합니다.",
        ),
        Check(
            "운용자 확인",
            "pass" if STATE["operator_confirmed"] else "warn",
            "운용자가 수동 확인을 완료했습니다." if STATE["operator_confirmed"] else "첫 주문 전 수동 확인이 필요합니다.",
        ),
        Check(
            "Kill Switch",
            "fail" if STATE["kill_switch"] else "pass",
            "Kill Switch가 켜져 있습니다." if STATE["kill_switch"] else "긴급 정지 상태가 아닙니다.",
        ),
        Check(
            "신규 진입",
            "warn" if STATE["new_entries_blocked"] else "pass",
            "신규 진입 차단이 켜져 있어 매수 주문은 막힙니다." if STATE["new_entries_blocked"] else "신규 진입이 허용되어 있습니다.",
        ),
        Check(
            "포지션 대조",
            "warn",
            "실계좌 포지션 대조 API가 연결되면 프로그램 포지션과 브로커 포지션을 비교해야 합니다.",
        ),
    ]
    return checks


def risk_checks() -> list[dict[str, str]]:
    settings = STATE["risk_settings"]
    return [
        {
            "label": "일일 손실 한도",
            "value": f"{settings['daily_loss_limit_pct']:.1f}%",
            "status": "pass" if settings["daily_loss_limit_pct"] < 0 else "fail",
            "detail": "한도 도달 시 신규 진입과 FULL LIVE를 차단합니다.",
        },
        {
            "label": "전략별 자본 한도",
            "value": f"{settings['strategy_capital_limit_krw']:,.0f}",
            "status": "pass",
            "detail": "주문 전 예상 노출을 전략 한도와 비교합니다.",
        },
        {
            "label": "중복 주문 쿨다운",
            "value": f"{settings['duplicate_order_cooldown_sec']:.0f}초",
            "status": "pass",
            "detail": "같은 전략/심볼/방향 주문 반복을 차단합니다.",
        },
        {"label": "데이터 지연", "value": "15초", "status": "warn", "detail": "실시간 시세 WebSocket 연결 후 실제 지연 시간을 측정해야 합니다."},
        {
            "label": "슬리피지 한도",
            "value": f"{settings['max_slippage_bps']:.0f} bps",
            "status": "pass",
            "detail": "호가 기반 예상 슬리피지가 한도를 넘으면 차단합니다.",
        },
        {
            "label": "종목별 노출",
            "value": f"{settings['max_symbol_exposure_pct']:.0f}%",
            "status": "pass",
            "detail": "단일 종목 노출이 한도를 넘으면 차단합니다.",
        },
        {
            "label": "열린 주문 수",
            "value": f"{settings['max_open_orders']:.0f}건",
            "status": "pass",
            "detail": "동시에 열린 주문 수가 한도를 넘으면 차단합니다.",
        },
        {"label": "포지션 불일치", "value": "주문 금지", "status": "warn", "detail": "브로커 계좌 조회 API 연결 전에는 대조가 경고 상태입니다."},
    ]


def positions() -> list[dict[str, str]]:
    return [
        {"symbol": "069500.KS", "asset": "한국주식", "program_qty": "0", "broker_qty": "-", "status": "대조 필요"},
        {"symbol": "BTCUSDT", "asset": "코인", "program_qty": "0", "broker_qty": "-", "status": "API 필요"},
        {"symbol": "SPY", "asset": "미국 ETF", "program_qty": "0", "broker_qty": "-", "status": "API 필요"},
    ]


def risk_setting_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for key, meta in RISK_SETTING_META.items():
        rows.append(
            {
                "key": key,
                "value": STATE["risk_settings"][key],
                **meta,
            }
        )
    return rows


def checklist_rows() -> list[dict[str, object]]:
    values = STATE["checklist"]
    return [
        {
            "key": str(item["key"]),
            "label": str(item["label"]),
            "detail": str(item["detail"]),
            "required": bool(item["required"]),
            "checked": bool(values.get(str(item["key"]), False)),
        }
        for item in CHECKLIST_ITEMS
    ]


def retry_policy_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for key, meta in RETRY_POLICY_META.items():
        rows.append(
            {
                "key": key,
                "value": STATE["retry_policy"][key],
                **meta,
            }
        )
    return rows


def can_retry_order(order: dict[str, Any]) -> bool:
    if order.get("state") in FINAL_ORDER_STATES:
        return False
    if order.get("queue_state") == "canceled":
        return False
    max_attempts = int(float(STATE["retry_policy"]["max_attempts"]))
    return int(order.get("attempts", 0)) < max_attempts


def order_rows() -> list[dict[str, Any]]:
    return [
        {
            **order,
            "retryable": can_retry_order(order),
            "max_attempts": int(float(STATE["retry_policy"]["max_attempts"])),
        }
        for order in STATE["orders"]
    ]


def dry_run_ledger_rows() -> list[dict[str, Any]]:
    return [order for order in order_rows() if order.get("dry_run") is True][:20]


def order_queue_summary() -> dict[str, int]:
    rows = order_rows()
    return {
        "total": len(rows),
        "blocked": sum(1 for order in rows if order["state"] == "risk_blocked"),
        "dry_run": sum(1 for order in rows if order.get("dry_run") is True),
        "retryable": sum(1 for order in rows if order["retryable"]),
        "canceled": sum(1 for order in rows if order["state"] == "canceled"),
    }


def snapshot() -> dict[str, Any]:
    brokers = [broker.to_dict() for broker in broker_readiness()]
    strategies = strategy_rows()
    checks = readiness_checks(strategies, brokers)
    blockers = [check for check in checks if check.status == "fail"]
    warnings = [check for check in checks if check.status == "warn"]
    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode": STATE["mode"],
        "dry_run": STATE["dry_run"],
        "kill_switch": STATE["kill_switch"],
        "new_entries_blocked": STATE["new_entries_blocked"],
        "operator_confirmed": STATE["operator_confirmed"],
        "summary": {
            "status": "blocked" if blockers else ("watch" if warnings else "ready"),
            "blocker_count": len(blockers),
            "warning_count": len(warnings),
            "live_strategy_count": sum(1 for strategy in strategies if strategy["live_allowed"]),
            "broker_ready_count": sum(1 for broker in brokers if broker["order_ready"]),
        },
        "sessions": market_sessions(),
        "readiness": [check.to_dict() for check in checks],
        "risk_checks": risk_checks(),
        "risk_settings": risk_setting_rows(),
        "checklist": checklist_rows(),
        "retry_policy": retry_policy_rows(),
        "order_queue": order_queue_summary(),
        "brokers": brokers,
        "broker_diagnostics": broker_diagnostics(),
        "broker_adapter_contract": broker_adapter_contract(),
        "strategies": strategies,
        "orders": order_rows(),
        "dry_run_ledger": dry_run_ledger_rows(),
        "positions": positions(),
        "audit": list(reversed(STATE["audit"][-30:])),
    }


def append_audit(level: str, event: str, detail: str) -> None:
    STATE["audit"].append({"time": now_text(), "level": level, "event": event, "detail": detail})


def set_mode(mode: str) -> dict[str, Any]:
    data = snapshot()
    blockers = data["summary"]["blocker_count"]
    normalized = mode if mode in {"MONITOR", "SMALL_LIVE", "FULL_LIVE"} else "MONITOR"
    if normalized != "MONITOR" and blockers:
        append_audit("danger", "모드 전환 차단", f"{normalized} 전환은 readiness blocker {blockers}개 때문에 차단되었습니다.")
        return {"ok": False, "reason": f"readiness blocker {blockers}개가 남아 있습니다.", "snapshot": snapshot()}
    if normalized == "FULL_LIVE" and data["summary"]["warning_count"]:
        append_audit("warn", "FULL LIVE 차단", "경고 항목이 남아 있어 FULL LIVE 전환을 차단했습니다.")
        return {"ok": False, "reason": "FULL LIVE는 경고 0개일 때만 허용됩니다.", "snapshot": snapshot()}
    STATE["mode"] = normalized
    append_audit("info", "모드 전환", f"운용 모드가 {normalized}(으)로 변경되었습니다.")
    return {"ok": True, "reason": "mode changed", "snapshot": snapshot()}


def set_flag(name: str, value: bool) -> dict[str, Any]:
    if name not in {"kill_switch", "new_entries_blocked", "operator_confirmed", "dry_run"}:
        return {"ok": False, "reason": "unknown flag", "snapshot": snapshot()}
    STATE[name] = bool(value)
    label = {
        "kill_switch": "Kill Switch",
        "new_entries_blocked": "신규 진입 차단",
        "operator_confirmed": "운용자 확인",
        "dry_run": "Dry Run 보호",
    }[name]
    level = "info" if name == "dry_run" and value else ("warn" if value else "info")
    append_audit(level, label, f"{label} 값이 {value}(으)로 변경되었습니다.")
    if name == "kill_switch" and value:
        STATE["mode"] = "MONITOR"
        STATE["new_entries_blocked"] = True
    return {"ok": True, "reason": "flag changed", "snapshot": snapshot()}


def set_risk_setting(name: str, value: object) -> dict[str, Any]:
    meta = RISK_SETTING_META.get(name)
    if not meta:
        return {"ok": False, "reason": "unknown risk setting", "snapshot": snapshot()}
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return {"ok": False, "reason": "risk setting must be numeric", "snapshot": snapshot()}
    minimum = float(meta["min"])
    maximum = float(meta["max"])
    if numeric < minimum or numeric > maximum:
        return {"ok": False, "reason": f"{meta['label']} 값은 {minimum:g}~{maximum:g} 범위여야 합니다.", "snapshot": snapshot()}
    if name == "max_open_orders":
        numeric = float(int(numeric))
    STATE["risk_settings"][name] = numeric
    append_audit("warn", "리스크 한도 변경", f"{meta['label']} 값이 {numeric:g}{meta['unit']}(으)로 변경되었습니다.")
    return {"ok": True, "reason": "risk setting changed", "snapshot": snapshot()}


def set_checklist_item(name: str, value: bool) -> dict[str, Any]:
    keys = {str(item["key"]): item for item in CHECKLIST_ITEMS}
    item = keys.get(name)
    if not item:
        return {"ok": False, "reason": "unknown checklist item", "snapshot": snapshot()}
    STATE["checklist"][name] = bool(value)
    append_audit("info" if value else "warn", "운영 체크리스트", f"{item['label']} 확인 값이 {bool(value)}(으)로 변경되었습니다.")
    return {"ok": True, "reason": "checklist changed", "snapshot": snapshot()}


def set_retry_policy(name: str, value: object) -> dict[str, Any]:
    meta = RETRY_POLICY_META.get(name)
    if not meta:
        return {"ok": False, "reason": "unknown retry policy", "snapshot": snapshot()}
    if meta["type"] == "boolean":
        STATE["retry_policy"][name] = bool(value)
        append_audit("info", "재시도 정책 변경", f"{meta['label']} 값이 {bool(value)}(으)로 변경되었습니다.")
        return {"ok": True, "reason": "retry policy changed", "snapshot": snapshot()}
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return {"ok": False, "reason": "retry policy must be numeric", "snapshot": snapshot()}
    minimum = float(meta["min"])
    maximum = float(meta["max"])
    if numeric < minimum or numeric > maximum:
        return {"ok": False, "reason": f"{meta['label']} 값은 {minimum:g}~{maximum:g} 범위여야 합니다.", "snapshot": snapshot()}
    STATE["retry_policy"][name] = float(int(numeric))
    append_audit("info", "재시도 정책 변경", f"{meta['label']} 값이 {int(numeric)}{meta['unit']}(으)로 변경되었습니다.")
    return {"ok": True, "reason": "retry policy changed", "snapshot": snapshot()}


def run_broker_check(broker_id: str) -> dict[str, Any]:
    diagnostics = broker_diagnostics(broker_id)
    if not diagnostics:
        return {"ok": False, "reason": "unknown broker", "snapshot": snapshot()}
    item = diagnostics[0]
    append_audit(
        "danger" if item["fail_count"] else "warn" if item["warn_count"] else "info",
        "브로커 연결 점검",
        f"{item['name']}: fail {item['fail_count']}개, warn {item['warn_count']}개",
    )
    return {
        "ok": True,
        "reason": f"브로커 점검 완료: fail {item['fail_count']}개, warn {item['warn_count']}개",
        "diagnostics": diagnostics,
        "snapshot": snapshot(),
    }


def export_audit(format_name: str) -> dict[str, Any]:
    normalized = format_name.lower().strip()
    if normalized not in {"csv", "html"}:
        return {"ok": False, "reason": "unsupported export format", "snapshot": snapshot()}

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    append_audit("info", "감사 로그 내보내기", f"{normalized.upper()} 감사 로그를 생성했습니다.")
    rows = STATE["audit"]

    if normalized == "csv":
        stream = StringIO()
        writer = csv.writer(stream)
        writer.writerow(["time", "level", "event", "detail"])
        for row in rows:
            writer.writerow([row["time"], row["level"], row["event"], row["detail"]])
        return {
            "ok": True,
            "format": "csv",
            "filename": f"live-trader-audit-{timestamp}.csv",
            "mime": "text/csv;charset=utf-8",
            "content": stream.getvalue(),
            "snapshot": snapshot(),
        }

    body_rows = "\n".join(
        "<tr>"
        f"<td>{html.escape(str(row['time']))}</td>"
        f"<td>{html.escape(str(row['level']))}</td>"
        f"<td>{html.escape(str(row['event']))}</td>"
        f"<td>{html.escape(str(row['detail']))}</td>"
        "</tr>"
        for row in rows
    )
    content = (
        "<!doctype html><meta charset='utf-8'>"
        "<title>Live Trader Audit</title>"
        "<style>body{font-family:Segoe UI,sans-serif;padding:24px;color:#111827}"
        "table{width:100%;border-collapse:collapse}th,td{border:1px solid #cbd5e1;padding:8px;text-align:left}"
        "th{background:#eff6ff}</style>"
        "<h1>Live Trader Audit</h1>"
        f"<p>Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>"
        "<table><thead><tr><th>Time</th><th>Level</th><th>Event</th><th>Detail</th></tr></thead>"
        f"<tbody>{body_rows}</tbody></table>"
    )
    return {
        "ok": True,
        "format": "html",
        "filename": f"live-trader-audit-{timestamp}.html",
        "mime": "text/html;charset=utf-8",
        "content": content,
        "snapshot": snapshot(),
    }


def evaluate_order_gate(checks: dict[str, Any], side: str, dry_run: bool) -> tuple[bool, str, str, str]:
    blocker_count = int(checks["summary"]["blocker_count"])
    if STATE["new_entries_blocked"] and side == "BUY":
        return False, "risk_blocked", "blocked", "신규 진입 차단이 켜져 있어 BUY 주문을 차단했습니다."
    if blocker_count:
        return False, "risk_blocked", "blocked", f"readiness blocker {blocker_count}개가 남아 있어 실거래 주문을 제출하지 않았습니다."
    if dry_run:
        return True, "dry_run", "simulated", "Dry Run 보호가 켜져 있어 브로커 전송 없이 주문 의도를 감사 로그에만 기록했습니다."
    return False, "adapter_blocked", "held", "실제 주문 어댑터 안전 검증 전이므로 브로커 전송을 차단했습니다."


def next_order_id(state_name: str, dry_run: bool) -> str:
    if state_name == "dry_run" or dry_run:
        prefix = "LIVE-DRY"
    elif state_name == "canceled":
        prefix = "LIVE-CXL"
    else:
        prefix = "LIVE-BLOCK"
    return f"{prefix}-{len(STATE['orders']) + 1:04d}"


def submit_test_intent() -> dict[str, Any]:
    checks = snapshot()
    strategy = next((item for item in checks["strategies"] if item["live_allowed"]), checks["strategies"][0])
    side = "BUY"
    dry_run = bool(STATE["dry_run"])
    ok, order_state, queue_state, reason = evaluate_order_gate(checks, side, dry_run)
    retry_backoff = float(STATE["retry_policy"]["backoff_sec"])
    order = {
        "time": now_text(),
        "created_at": now_text(),
        "updated_at": now_text(),
        "order_id": next_order_id(order_state, dry_run),
        "strategy_id": strategy["strategy_id"],
        "symbol": strategy["symbol"],
        "side": side,
        "qty": "1",
        "notional": "테스트",
        "state": order_state,
        "queue_state": queue_state,
        "attempts": 1,
        "next_retry_at": future_text(retry_backoff) if order_state in {"risk_blocked", "adapter_blocked"} else "-",
        "broker_order_id": "-",
        "dry_run": dry_run,
        "reason": reason,
    }
    STATE["orders"].insert(0, order)
    STATE["orders"] = STATE["orders"][:50]
    append_audit("info" if ok else "danger", "주문 게이트", f"{order['symbol']} {side}: {reason}")
    return {"ok": ok, "reason": reason, "snapshot": snapshot()}


def find_order(order_id: str) -> dict[str, Any] | None:
    return next((order for order in STATE["orders"] if order.get("order_id") == order_id), None)


def retry_order(order_id: str) -> dict[str, Any]:
    order = find_order(order_id)
    if not order:
        return {"ok": False, "reason": "order not found", "snapshot": snapshot()}
    if not can_retry_order(order):
        append_audit("warn", "주문 재시도 차단", f"{order_id} 주문은 재시도 대상이 아닙니다.")
        return {"ok": False, "reason": "order is not retryable", "snapshot": snapshot()}

    order["attempts"] = int(order.get("attempts", 0)) + 1
    order["updated_at"] = now_text()
    checks = snapshot()
    ok, state_name, queue_state, reason = evaluate_order_gate(checks, str(order.get("side", "BUY")), bool(order.get("dry_run", STATE["dry_run"])))
    if not ok and order["attempts"] >= int(float(STATE["retry_policy"]["max_attempts"])):
        state_name = "retry_exhausted"
        queue_state = "failed"
        reason = f"재시도 {order['attempts']}회가 모두 소진되었습니다. 마지막 사유: {reason}"
    order["state"] = state_name
    order["queue_state"] = queue_state
    order["reason"] = reason
    order["next_retry_at"] = future_text(float(STATE["retry_policy"]["backoff_sec"])) if can_retry_order(order) else "-"
    append_audit("info" if ok else "warn", "주문 재시도", f"{order_id}: {reason}")
    return {"ok": ok, "reason": reason, "snapshot": snapshot()}


def cancel_order(order_id: str) -> dict[str, Any]:
    order = find_order(order_id)
    if not order:
        return {"ok": False, "reason": "order not found", "snapshot": snapshot()}
    if order.get("state") == "canceled":
        return {"ok": True, "reason": "order already canceled", "snapshot": snapshot()}
    order["state"] = "canceled"
    order["queue_state"] = "canceled"
    order["updated_at"] = now_text()
    order["next_retry_at"] = "-"
    order["reason"] = "운용자 요청으로 주문 큐 항목을 취소했습니다."
    append_audit("warn", "주문 취소", f"{order_id} 주문 큐 항목을 취소했습니다.")
    return {"ok": True, "reason": "order canceled", "snapshot": snapshot()}
