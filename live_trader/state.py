from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from .brokers import broker_readiness, real_orders_enabled
from .contracts import can_live_use_artifact, load_strategy_artifacts, sample_strategy_artifacts


Mode = Literal["MONITOR", "SMALL_LIVE", "FULL_LIVE"]
CheckStatus = Literal["pass", "warn", "fail"]


@dataclass(frozen=True)
class Check:
    label: str
    status: CheckStatus
    detail: str

    def to_dict(self) -> dict[str, str]:
        return {"label": self.label, "status": self.status, "detail": self.detail}


STATE: dict[str, Any] = {
    "mode": "MONITOR",
    "kill_switch": False,
    "new_entries_blocked": True,
    "operator_confirmed": False,
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
    checks = [
        Check(
            "실거래 라우트 환경",
            "pass" if real_orders_enabled() else "fail",
            "LIVE_TRADER_ENABLE_REAL_ORDERS=true가 필요합니다." if not real_orders_enabled() else "실거래 라우트 환경 변수가 켜져 있습니다.",
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
    return [
        {"label": "일일 손실 한도", "value": "-2.0%", "status": "pass", "detail": "한도 도달 시 신규 진입과 FULL LIVE를 차단합니다."},
        {"label": "전략별 자본 한도", "value": "20,000,000", "status": "pass", "detail": "주문 전 예상 노출을 전략 한도와 비교합니다."},
        {"label": "중복 주문 쿨다운", "value": "180초", "status": "pass", "detail": "같은 전략/심볼/방향 주문 반복을 차단합니다."},
        {"label": "데이터 지연", "value": "15초", "status": "warn", "detail": "실시간 시세 WebSocket 연결 후 실제 지연 시간을 측정해야 합니다."},
        {"label": "슬리피지 한도", "value": "50 bps", "status": "pass", "detail": "호가 기반 예상 슬리피지가 한도를 넘으면 차단합니다."},
        {"label": "포지션 불일치", "value": "주문 금지", "status": "warn", "detail": "브로커 계좌 조회 API 연결 전에는 대조가 경고 상태입니다."},
    ]


def positions() -> list[dict[str, str]]:
    return [
        {"symbol": "069500.KS", "asset": "한국주식", "program_qty": "0", "broker_qty": "-", "status": "대조 필요"},
        {"symbol": "BTCUSDT", "asset": "코인", "program_qty": "0", "broker_qty": "-", "status": "API 필요"},
        {"symbol": "SPY", "asset": "미국 ETF", "program_qty": "0", "broker_qty": "-", "status": "API 필요"},
    ]


def snapshot() -> dict[str, Any]:
    brokers = [broker.to_dict() for broker in broker_readiness()]
    strategies = strategy_rows()
    checks = readiness_checks(strategies, brokers)
    blockers = [check for check in checks if check.status == "fail"]
    warnings = [check for check in checks if check.status == "warn"]
    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode": STATE["mode"],
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
        "brokers": brokers,
        "strategies": strategies,
        "orders": STATE["orders"],
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
    if name not in {"kill_switch", "new_entries_blocked", "operator_confirmed"}:
        return {"ok": False, "reason": "unknown flag", "snapshot": snapshot()}
    STATE[name] = bool(value)
    label = {
        "kill_switch": "Kill Switch",
        "new_entries_blocked": "신규 진입 차단",
        "operator_confirmed": "운용자 확인",
    }[name]
    append_audit("warn" if value else "info", label, f"{label} 값이 {value}(으)로 변경되었습니다.")
    if name == "kill_switch" and value:
        STATE["mode"] = "MONITOR"
        STATE["new_entries_blocked"] = True
    return {"ok": True, "reason": "flag changed", "snapshot": snapshot()}


def submit_test_intent() -> dict[str, Any]:
    checks = snapshot()
    strategy = next((item for item in checks["strategies"] if item["live_allowed"]), checks["strategies"][0])
    side = "BUY"
    blocked_reason = "readiness blocker가 남아 있어 실거래 주문을 제출하지 않았습니다."
    if STATE["new_entries_blocked"] and side == "BUY":
        blocked_reason = "신규 진입 차단이 켜져 있어 BUY 주문을 차단했습니다."
    order = {
        "time": now_text(),
        "order_id": f"LIVE-BLOCK-{len(STATE['orders']) + 1:04d}",
        "strategy_id": strategy["strategy_id"],
        "symbol": strategy["symbol"],
        "side": side,
        "qty": "1",
        "notional": "테스트",
        "state": "risk_blocked",
        "reason": blocked_reason,
    }
    STATE["orders"].insert(0, order)
    STATE["orders"] = STATE["orders"][:50]
    append_audit("danger", "주문 차단", f"{order['symbol']} {side}: {blocked_reason}")
    return {"ok": False, "reason": blocked_reason, "snapshot": snapshot()}
