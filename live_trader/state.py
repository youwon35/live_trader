from __future__ import annotations

import os
import csv
import html
from dataclasses import dataclass
from datetime import datetime, timedelta
from io import StringIO
from typing import Any, Literal

from .brokers import broker_adapter_contract, broker_diagnostics, broker_readiness, real_orders_enabled
from .contracts import can_live_use_artifact, load_strategy_artifacts, sample_strategy_artifacts, strategy_plugin_status
from .live_adapters import build_binance_spot_order_request, build_kis_live_order_request, build_upbit_order_request
from .order_management import OrderIntent, OrderSide
from .risk_engine import PreTradeContext, PreTradeRiskGate, PreTradeRiskReport, RecentOrder


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

POSITION_RECONCILIATION_BOOK: tuple[dict[str, object], ...] = (
    {
        "symbol": "069500.KS",
        "asset": "한국주식",
        "broker_id": "kis",
        "broker_name": "한국투자증권",
        "program_qty": 0.0,
        "broker_qty": None,
        "program_value": 0.0,
        "broker_value": None,
        "currency": "KRW",
        "tolerance_qty": 0.0,
    },
    {
        "symbol": "BTCUSDT",
        "asset": "코인",
        "broker_id": "binance",
        "broker_name": "Binance",
        "program_qty": 0.0,
        "broker_qty": None,
        "program_value": 0.0,
        "broker_value": None,
        "currency": "USDT",
        "tolerance_qty": 0.000001,
    },
    {
        "symbol": "SPY",
        "asset": "미국 ETF",
        "broker_id": "kis",
        "broker_name": "한국투자증권",
        "program_qty": 0.0,
        "broker_qty": None,
        "program_value": 0.0,
        "broker_value": None,
        "currency": "USD",
        "tolerance_qty": 0.0,
    },
)

ACCOUNT_RECONCILIATION_BOOK: tuple[dict[str, object], ...] = (
    {
        "broker_id": "kis",
        "broker_name": "한국투자증권",
        "account": "KIS 실계좌",
        "currency": "KRW/USD",
        "program_cash": None,
        "broker_cash": None,
        "detail": "KIS 잔고/예수금 조회 어댑터가 연결되어야 계좌 대조를 통과합니다.",
    },
    {
        "broker_id": "binance",
        "broker_name": "Binance",
        "account": "Binance Spot",
        "currency": "USDT",
        "program_cash": None,
        "broker_cash": None,
        "detail": "Binance signed account endpoint가 연결되어야 현금성 잔고를 대조합니다.",
    },
)

FINAL_ORDER_STATES = {"dry_run", "sent", "filled", "canceled", "retry_exhausted"}
AUDIT_LOG_LIMIT = 500


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
    "automation": {
        "stock": {"enabled": False, "provider": "kis", "mode": "MONITOR", "last_action": "대기"},
        "crypto": {"enabled": False, "provider": "binance", "mode": "MONITOR", "last_action": "대기"},
    },
    "reconciliation_last_run": None,
    "preflight_last_run": None,
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
        verification = artifact.get("verification") if isinstance(artifact.get("verification"), dict) else {}
        backtester_verification = verification.get("backtester") if isinstance(verification.get("backtester"), dict) else {}
        paper_verification = verification.get("paper_trader") if isinstance(verification.get("paper_trader"), dict) else {}
        rows.append(
            {
                **artifact,
                "live_allowed": live_allowed,
                "permission_label": "LIVE OK" if live_allowed else "LIVE BLOCKED",
                "block_reason": "; ".join(fail_reasons) if fail_reasons else ("실거래 허용" if live_allowed else "live_allowed 권한이 없습니다."),
                "backtester_verified": backtester_verification.get("status") == "pass",
                "paper_trader_verified": paper_verification.get("status") == "pass",
                "backtester_label": str(backtester_verification.get("label", "Backtester 정보 없음")),
                "paper_trader_label": str(paper_verification.get("label", "Paper 미검증")),
            }
        )
    return rows


def automation_profiles(strategies: list[dict[str, Any]] | None = None, brokers: list[dict[str, object]] | None = None) -> list[dict[str, Any]]:
    strategies = strategies if strategies is not None else strategy_rows()
    brokers = brokers if brokers is not None else [broker.to_dict() for broker in broker_readiness()]
    broker_map = {str(broker["broker_id"]): broker for broker in brokers}
    stock_strategies = [strategy for strategy in strategies if strategy_broker_id(strategy) == "kis"]
    crypto_strategies = [strategy for strategy in strategies if strategy_broker_id(strategy) in {"binance", "upbit"}]
    stock_provider = str(STATE["automation"]["stock"].get("provider") or "kis")
    crypto_provider = str(STATE["automation"]["crypto"].get("provider") or "binance")
    return [
        {
            "id": "stock",
            "title": "주식/ETF 자동화",
            "provider": stock_provider,
            "provider_label": "한국투자증권 Open API",
            "broker_ids": ["kis"],
            "asset_scope": ["한국주식", "미국주식", "금 ETF", "오일 ETF"],
            "mode": str(STATE["automation"]["stock"].get("mode") or "MONITOR"),
            "enabled": bool(STATE["automation"]["stock"]["enabled"]),
            "last_action": STATE["automation"]["stock"]["last_action"],
            "ready": bool(broker_map.get("kis", {}).get("order_ready")),
            "strategy_count": len(stock_strategies),
            "live_strategy_count": sum(1 for strategy in stock_strategies if strategy.get("live_allowed")),
            "detail": "KIS 실계좌 API로 주식/ETF 주문을 라우팅합니다.",
            "sample_request": build_adapter_preview("kis", stock_strategies),
        },
        {
            "id": "crypto",
            "title": "코인 자동화",
            "provider": crypto_provider,
            "provider_label": "Binance API" if crypto_provider == "binance" else "Upbit API",
            "broker_ids": ["binance", "upbit"],
            "asset_scope": ["Binance 현물", "Upbit KRW 마켓"],
            "mode": str(STATE["automation"]["crypto"].get("mode") or "MONITOR"),
            "enabled": bool(STATE["automation"]["crypto"]["enabled"]),
            "last_action": STATE["automation"]["crypto"]["last_action"],
            "ready": bool(broker_map.get(crypto_provider, {}).get("order_ready")),
            "strategy_count": len(crypto_strategies),
            "live_strategy_count": sum(1 for strategy in crypto_strategies if strategy.get("live_allowed")),
            "detail": "선택한 코인 거래소 API로 코인 주문을 라우팅합니다.",
            "sample_request": build_adapter_preview(crypto_provider, crypto_strategies),
        },
    ]


def strategy_broker_id(strategy: dict[str, Any]) -> str:
    asset = f"{strategy.get('asset', '')} {strategy.get('symbol', '')}".lower()
    if any(token in asset for token in ("crypto", "coin", "btc", "eth", "usdt", "코인")):
        return "binance"
    return "kis"


def build_adapter_preview(provider: str, strategies: list[dict[str, Any]]) -> dict[str, Any]:
    strategy = next((item for item in strategies if item.get("live_allowed")), strategies[0] if strategies else {})
    symbol = str(strategy.get("symbol") or ("BTCUSDT" if provider == "binance" else "KRW-BTC" if provider == "upbit" else "069500.KS"))
    intent = {
        "broker_id": provider,
        "strategy_id": strategy.get("strategy_id", "sample"),
        "symbol": symbol,
        "market": symbol if provider == "upbit" else "",
        "asset": strategy.get("asset", ""),
        "side": "BUY",
        "quantity": 1,
        "price": 1000 if provider != "binance" else 1,
        "order_type": "LIMIT" if provider == "binance" else "limit" if provider == "upbit" else "00",
    }
    if provider == "kis":
        return build_kis_live_order_request(intent).preview()
    if provider == "upbit":
        return build_upbit_order_request(intent).preview()
    return build_binance_spot_order_request(intent, test=True).preview()


def market_sessions() -> list[dict[str, str]]:
    return [
        {"label": "KRX", "state": "watch", "time": "KST", "detail": "정규장/동시호가 구분 필요"},
        {"label": "NYSE", "state": "closed", "time": "ET", "detail": "정규장 외 주문 품질 주의"},
        {"label": "Binance", "state": "open", "time": "24/7", "detail": "레이트 리밋/급변동 감시"},
        {"label": "Risk Engine", "state": "blocked", "time": "LIVE GATE", "detail": "API/권한 준비 전 차단"},
    ]


def readiness_checks(
    strategies: list[dict[str, Any]],
    brokers: list[dict[str, object]],
    reconciliation_summary: dict[str, Any],
) -> list[Check]:
    live_ready_count = sum(1 for strategy in strategies if strategy.get("live_allowed") is True)
    missing_brokers = [broker for broker in brokers if broker["status"] == "missing_credentials"]
    adapter_blocked = [broker for broker in brokers if broker["live_order_adapter_ready"] is not True]
    checklist = checklist_rows()
    checklist_missing = [item for item in checklist if item["required"] and not item["checked"]]
    reconcile_blocking = int(reconciliation_summary["api_required_count"]) + int(reconciliation_summary["mismatch_count"])
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
            "fail" if reconcile_blocking else "pass",
            (
                f"포지션/계좌 대조 차단 {reconcile_blocking}개가 남아 있습니다."
                if reconcile_blocking
                else "프로그램 포지션과 브로커 포지션이 대조되었습니다."
            ),
        ),
    ]
    return checks


def risk_checks(reconciliation_summary: dict[str, Any] | None = None) -> list[dict[str, str]]:
    settings = STATE["risk_settings"]
    if reconciliation_summary is None:
        reconciliation_summary = reconciliation_snapshot()["summary"]
    reconcile_status = str(reconciliation_summary["status"])
    reconcile_risk_status = "pass" if reconcile_status == "pass" else "fail" if reconcile_status == "fail" else "warn"
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
        {
            "label": "포지션 불일치",
            "value": str(reconciliation_summary["status_label"]),
            "status": reconcile_risk_status,
            "detail": "포지션/계좌 대조가 깨끗하지 않으면 실주문 제출을 차단합니다.",
        },
    ]


def positions() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for item in POSITION_RECONCILIATION_BOOK:
        broker_qty = item["broker_qty"]
        program_qty = float(item["program_qty"])
        tolerance_qty = float(item["tolerance_qty"])
        if broker_qty is None:
            status = "api_required"
            status_label = "API 필요"
            delta_qty = "-"
            detail = "브로커 포지션 조회 어댑터가 아직 연결되지 않았습니다."
        else:
            numeric_broker_qty = float(broker_qty)
            delta = program_qty - numeric_broker_qty
            delta_qty = format_quantity(delta)
            if abs(delta) <= tolerance_qty:
                status = "pass"
                status_label = "일치"
                detail = "프로그램 포지션과 브로커 포지션이 허용 오차 안에 있습니다."
            else:
                status = "mismatch"
                status_label = "불일치"
                detail = "프로그램 포지션과 브로커 포지션 수량이 다릅니다."
        rows.append(
            {
                "symbol": str(item["symbol"]),
                "asset": str(item["asset"]),
                "broker_id": str(item["broker_id"]),
                "broker_name": str(item["broker_name"]),
                "currency": str(item["currency"]),
                "program_qty": format_quantity(program_qty),
                "broker_qty": format_quantity(float(broker_qty)) if broker_qty is not None else "API 필요",
                "delta_qty": delta_qty,
                "status": status,
                "status_label": status_label,
                "detail": detail,
            }
        )
    return rows


def account_reconciliation_rows() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for item in ACCOUNT_RECONCILIATION_BOOK:
        broker_cash = item["broker_cash"]
        program_cash = item["program_cash"]
        if broker_cash is None or program_cash is None:
            status = "api_required"
            status_label = "API 필요"
            delta_cash = "-"
        else:
            numeric_program_cash = float(program_cash)
            numeric_broker_cash = float(broker_cash)
            delta = numeric_program_cash - numeric_broker_cash
            delta_cash = format_money(delta, str(item["currency"]))
            status = "pass" if abs(delta) <= 1.0 else "mismatch"
            status_label = "일치" if status == "pass" else "불일치"
        rows.append(
            {
                "broker_id": str(item["broker_id"]),
                "broker_name": str(item["broker_name"]),
                "account": str(item["account"]),
                "currency": str(item["currency"]),
                "program_cash": format_money(program_cash, str(item["currency"])) if program_cash is not None else "대조 대기",
                "broker_cash": format_money(broker_cash, str(item["currency"])) if broker_cash is not None else "API 필요",
                "delta_cash": delta_cash,
                "status": status,
                "status_label": status_label,
                "detail": str(item["detail"]),
            }
        )
    return rows


def reconciliation_snapshot() -> dict[str, Any]:
    position_rows = positions()
    account_rows = account_reconciliation_rows()
    api_required_count = sum(1 for item in position_rows + account_rows if item["status"] == "api_required")
    mismatch_count = sum(1 for item in position_rows + account_rows if item["status"] == "mismatch")
    pass_count = sum(1 for item in position_rows + account_rows if item["status"] == "pass")
    status = "fail" if mismatch_count else "warn" if api_required_count else "pass"
    status_label = "불일치" if mismatch_count else "API 필요" if api_required_count else "정상"
    return {
        "summary": {
            "status": status,
            "status_label": status_label,
            "last_run": STATE["reconciliation_last_run"] or "미실행",
            "position_count": len(position_rows),
            "account_count": len(account_rows),
            "api_required_count": api_required_count,
            "mismatch_count": mismatch_count,
            "pass_count": pass_count,
        },
        "positions": position_rows,
        "accounts": account_rows,
        "next_actions": reconciliation_next_actions(api_required_count, mismatch_count),
    }


def reconciliation_next_actions(api_required_count: int, mismatch_count: int) -> list[str]:
    actions: list[str] = []
    if api_required_count:
        actions.append("KIS/Binance 계좌·포지션 조회 어댑터 연결")
    if mismatch_count:
        actions.append("불일치 포지션 수동 확인 후 주문 잠금 유지")
    actions.append("대조 결과가 정상일 때만 SMALL_LIVE 승인 검토")
    return actions


def format_quantity(value: float) -> str:
    if abs(value - int(value)) < 0.0000001:
        return f"{int(value)}"
    return f"{value:.8f}".rstrip("0").rstrip(".")


def format_money(value: object, currency: str) -> str:
    numeric = float(value)
    if currency == "KRW":
        return f"{numeric:,.0f} KRW"
    return f"{numeric:,.2f} {currency}"


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


def operation_report(
    reconciliation: dict[str, Any],
    checks: list[Check],
    preflight: list[dict[str, str]],
) -> dict[str, Any]:
    blocker_count = sum(1 for check in checks if check.status == "fail")
    warning_count = sum(1 for check in checks if check.status == "warn")
    hard_stop_count = sum(1 for check in preflight if check["status"] == "fail")
    queue = order_queue_summary()
    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "sections": [
            {
                "label": "Live Readiness",
                "status": "fail" if blocker_count else "warn" if warning_count else "pass",
                "value": f"blocker {blocker_count} / warn {warning_count}",
                "detail": "실거래 모드 전환을 막는 readiness 상태입니다.",
            },
            {
                "label": "포지션·계좌 대조",
                "status": str(reconciliation["summary"]["status"]),
                "value": str(reconciliation["summary"]["status_label"]),
                "detail": f"API 필요 {reconciliation['summary']['api_required_count']}개, 불일치 {reconciliation['summary']['mismatch_count']}개",
            },
            {
                "label": "주문 큐",
                "status": "warn" if queue["retryable"] or queue["blocked"] else "pass",
                "value": f"total {queue['total']}",
                "detail": f"차단 {queue['blocked']}건, 재시도 가능 {queue['retryable']}건, 취소 {queue['canceled']}건",
            },
            {
                "label": "최종 Preflight",
                "status": "fail" if hard_stop_count else "pass",
                "value": f"hard stop {hard_stop_count}",
                "detail": "실제 브로커 어댑터 연결 전 최종 승인 패키지입니다.",
            },
        ],
    }


def final_preflight_checks(
    strategies: list[dict[str, Any]],
    brokers: list[dict[str, object]],
    reconciliation: dict[str, Any],
) -> list[dict[str, str]]:
    checklist = checklist_rows()
    checklist_missing = [item for item in checklist if item["required"] and not item["checked"]]
    live_ready_count = sum(1 for strategy in strategies if strategy.get("live_allowed") is True)
    adapter_blocked = [broker for broker in brokers if broker["live_order_adapter_ready"] is not True]
    missing_brokers = [broker for broker in brokers if broker["status"] == "missing_credentials"]
    reconcile_summary = reconciliation["summary"]
    queue = order_queue_summary()
    return [
        {
            "label": "실거래 환경 변수",
            "status": "pass" if real_orders_enabled() else "fail",
            "detail": "LIVE_TRADER_ENABLE_REAL_ORDERS=true가 설정되어야 실거래 라우트 검토가 가능합니다.",
        },
        {
            "label": "브로커 API 키",
            "status": "fail" if missing_brokers else "pass",
            "detail": f"필수 브로커 환경 변수 누락 {len(missing_brokers)}개 그룹",
        },
        {
            "label": "브로커 주문 어댑터",
            "status": "fail" if adapter_blocked else "pass",
            "detail": "KIS/Binance signed order adapter가 구현·테스트·감사되어야 합니다.",
        },
        {
            "label": "포지션·계좌 대조",
            "status": "pass" if reconcile_summary["status"] == "pass" else "fail",
            "detail": f"{reconcile_summary['status_label']} 상태입니다. 마지막 대조: {reconcile_summary['last_run']}",
        },
        {
            "label": "전략 승인",
            "status": "pass" if live_ready_count else "fail",
            "detail": f"live_allowed 전략 {live_ready_count}개",
        },
        {
            "label": "필수 운영 체크리스트",
            "status": "fail" if checklist_missing else "pass",
            "detail": f"미완료 필수 항목 {len(checklist_missing)}개",
        },
        {
            "label": "Kill Switch",
            "status": "fail" if STATE["kill_switch"] else "pass",
            "detail": "Kill Switch가 켜져 있으면 MONITOR 외 모드로 전환할 수 없습니다.",
        },
        {
            "label": "운용자 확인",
            "status": "pass" if STATE["operator_confirmed"] else "warn",
            "detail": "첫 주문 전 운용자의 수동 확인이 필요합니다.",
        },
        {
            "label": "Dry Run 보호",
            "status": "pass" if STATE["dry_run"] else "warn",
            "detail": "Dry Run이 꺼진 상태에서는 실제 전송 차단 조건을 더 엄격히 확인해야 합니다.",
        },
        {
            "label": "주문 큐 잔여",
            "status": "warn" if queue["retryable"] else "pass",
            "detail": f"재시도 가능 주문 {queue['retryable']}건",
        },
    ]


def launch_report(preflight: list[dict[str, str]]) -> dict[str, Any]:
    hard_stops = [check for check in preflight if check["status"] == "fail"]
    warnings = [check for check in preflight if check["status"] == "warn"]
    if hard_stops:
        small_live_status = "blocked"
        full_live_status = "blocked"
        lock_reason = f"hard stop {len(hard_stops)}개가 남아 있습니다."
    elif warnings:
        small_live_status = "review_required"
        full_live_status = "blocked"
        lock_reason = f"warning {len(warnings)}개 수동 검토가 필요합니다."
    else:
        small_live_status = "ready"
        full_live_status = "ready"
        lock_reason = "모든 최종 점검을 통과했습니다."
    return {
        "last_run": STATE["preflight_last_run"] or "미실행",
        "hard_stop_count": len(hard_stops),
        "warning_count": len(warnings),
        "real_order_lock": "locked" if hard_stops or warnings else "ready",
        "small_live_status": small_live_status,
        "full_live_status": full_live_status,
        "lock_reason": lock_reason,
        "next_actions": [check["label"] for check in hard_stops[:6]] or [check["label"] for check in warnings[:6]] or ["SMALL_LIVE 소액 승인 검토"],
    }


def snapshot() -> dict[str, Any]:
    brokers = [broker.to_dict() for broker in broker_readiness()]
    strategies = strategy_rows()
    automations = automation_profiles(strategies, brokers)
    reconciliation = reconciliation_snapshot()
    checks = readiness_checks(strategies, brokers, reconciliation["summary"])
    preflight = final_preflight_checks(strategies, brokers, reconciliation)
    report = operation_report(reconciliation, checks, preflight)
    launch = launch_report(preflight)
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
        "risk_checks": risk_checks(reconciliation["summary"]),
        "risk_settings": risk_setting_rows(),
        "checklist": checklist_rows(),
        "retry_policy": retry_policy_rows(),
        "order_queue": order_queue_summary(),
        "brokers": brokers,
        "broker_diagnostics": broker_diagnostics(),
        "broker_adapter_contract": broker_adapter_contract(),
        "automation_profiles": automations,
        "strategies": strategies,
        "strategy_plugin_sources": strategy_plugin_status(),
        "orders": order_rows(),
        "dry_run_ledger": dry_run_ledger_rows(),
        "reconciliation": reconciliation,
        "accounts": reconciliation["accounts"],
        "positions": reconciliation["positions"],
        "operation_report": report,
        "final_preflight": preflight,
        "launch_report": launch,
        "audit": list(reversed(STATE["audit"][-30:])),
    }


def append_audit(level: str, event: str, detail: str) -> None:
    STATE["audit"].append({
        "time": now_text(),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "level": level,
        "event": event,
        "detail": detail,
    })
    if len(STATE["audit"]) > AUDIT_LOG_LIMIT:
        del STATE["audit"][: len(STATE["audit"]) - AUDIT_LOG_LIMIT]


def audit_clip(text: object, limit: int = 160) -> str:
    cleaned = " ".join(str(text).split())
    if len(cleaned) <= limit:
        return cleaned
    return f"{cleaned[: max(0, limit - 3)].rstrip()}..."


def risk_report_audit_summary(report: PreTradeRiskReport) -> str:
    counts = {"pass": 0, "warn": 0, "fail": 0}
    for check in report.checks:
        status = str(check.status)
        if status in counts:
            counts[status] += 1

    important_checks = list(report.blockers)
    if not important_checks:
        important_checks = [check for check in report.checks if check.status == "warn"]

    parts = [
        f"risk pass {counts['pass']} / warn {counts['warn']} / fail {counts['fail']}",
        "제출 허용" if report.can_submit else "제출 차단",
    ]
    if important_checks:
        checks_text = "; ".join(
            f"{check.label}: {audit_clip(check.detail, 90)}"
            for check in important_checks[:3]
        )
        parts.append(f"핵심 체크 {checks_text}")
    else:
        parts.append(f"요약 {audit_clip(report.summary, 120)}")
    return " | ".join(parts)


def order_gate_audit_detail(order: dict[str, Any], reason: str, report: PreTradeRiskReport) -> str:
    order_id = str(order.get("order_id", "-"))
    symbol = str(order.get("symbol", "-"))
    side = str(order.get("side", "-"))
    state_name = str(order.get("state", "-"))
    queue_state = str(order.get("queue_state", "-"))
    return (
        f"{order_id} {symbol} {side} {state_name}/{queue_state}: "
        f"{audit_clip(reason)} | {risk_report_audit_summary(report)}"
    )


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


def set_automation_profile(profile_id: str, enabled: bool, provider: str | None = None, mode: str | None = None) -> dict[str, Any]:
    profile_id = profile_id if profile_id in {"stock", "crypto"} else ""
    if not profile_id:
        return {"ok": False, "reason": "unknown automation profile", "snapshot": snapshot()}
    if provider:
        normalized_provider = provider.lower().strip()
        if profile_id == "stock" and normalized_provider != "kis":
            return {"ok": False, "reason": "주식/ETF 자동화 provider는 kis만 허용합니다.", "snapshot": snapshot()}
        if profile_id == "crypto" and normalized_provider not in {"binance", "upbit"}:
            return {"ok": False, "reason": "코인 자동화 provider는 binance 또는 upbit만 허용합니다.", "snapshot": snapshot()}
        STATE["automation"][profile_id]["provider"] = normalized_provider

    next_mode = str(mode or ("SMALL_LIVE" if enabled else "MONITOR")).strip().upper()
    if next_mode not in {"MONITOR", "SMALL_LIVE", "FULL_LIVE"}:
        return {"ok": False, "reason": "automation mode must be MONITOR, SMALL_LIVE, or FULL_LIVE", "snapshot": snapshot()}
    enabled = next_mode != "MONITOR" if mode is not None else enabled

    data = snapshot()
    profile = next((item for item in data["automation_profiles"] if item["id"] == profile_id), None)
    if not profile:
        return {"ok": False, "reason": "automation profile not found", "snapshot": snapshot()}
    if next_mode != "MONITOR":
        if data["summary"]["blocker_count"]:
            reason = f"readiness blocker {data['summary']['blocker_count']}개 때문에 {next_mode} 전환이 차단되었습니다."
            STATE["automation"][profile_id]["last_action"] = reason
            append_audit("danger", "자동화 시작 차단", f"{profile['title']}: {reason}")
            return {"ok": False, "reason": reason, "snapshot": snapshot()}
        if profile["live_strategy_count"] <= 0:
            reason = "live_allowed=true 전략이 없어 자동화 전환이 차단되었습니다."
            STATE["automation"][profile_id]["last_action"] = reason
            append_audit("danger", "자동화 시작 차단", f"{profile['title']}: {reason}")
            return {"ok": False, "reason": reason, "snapshot": snapshot()}
        if next_mode == "FULL_LIVE" and data["summary"]["warning_count"]:
            reason = f"warning {data['summary']['warning_count']}개 때문에 FULL_LIVE 전환이 차단되었습니다."
            STATE["automation"][profile_id]["last_action"] = reason
            append_audit("warn", "자동화 FULL LIVE 차단", f"{profile['title']}: {reason}")
            return {"ok": False, "reason": reason, "snapshot": snapshot()}
    STATE["automation"][profile_id]["enabled"] = bool(enabled)
    STATE["automation"][profile_id]["mode"] = next_mode
    action = f"{next_mode} 전환"
    STATE["automation"][profile_id]["last_action"] = f"{action} {now_text()}"
    append_audit("info" if next_mode == "MONITOR" else "warn", f"{profile['title']} {action}", f"{profile['provider_label']} 라우트의 자동화 모드를 {next_mode}(으)로 기록했습니다.")
    return {"ok": True, "reason": f"{profile['title']} {action}", "snapshot": snapshot()}


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


def run_reconciliation() -> dict[str, Any]:
    STATE["reconciliation_last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    reconciliation = reconciliation_snapshot()
    summary = reconciliation["summary"]
    blocking_count = int(summary["api_required_count"]) + int(summary["mismatch_count"])
    append_audit(
        "danger" if blocking_count else "info",
        "포지션/계좌 대조",
        f"{summary['status_label']}: API 필요 {summary['api_required_count']}개, 불일치 {summary['mismatch_count']}개",
    )
    return {
        "ok": True,
        "reason": f"대조 완료: {summary['status_label']} 상태",
        "reconciliation": reconciliation,
        "snapshot": snapshot(),
    }


def run_final_preflight() -> dict[str, Any]:
    STATE["preflight_last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data = snapshot()
    hard_stop_count = int(data["launch_report"]["hard_stop_count"])
    warning_count = int(data["launch_report"]["warning_count"])
    append_audit(
        "danger" if hard_stop_count else "warn" if warning_count else "info",
        "최종 Preflight",
        f"hard stop {hard_stop_count}개, warning {warning_count}개. {data['launch_report']['lock_reason']}",
    )
    return {
        "ok": True,
        "reason": f"최종 점검 완료: hard stop {hard_stop_count}개, warning {warning_count}개",
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
    ok, state_name, queue_state, reason, _report = evaluate_order_gate_with_report(checks, side, dry_run)
    return ok, state_name, queue_state, reason


def evaluate_order_gate_with_report(
    checks: dict[str, Any],
    side: str,
    dry_run: bool,
    intent: OrderIntent | None = None,
) -> tuple[bool, str, str, str, PreTradeRiskReport]:
    intent = intent or default_order_intent(checks, side)
    report = PreTradeRiskGate().evaluate(intent, pre_trade_context(checks, intent, dry_run))
    if report.can_submit:
        if dry_run:
            return True, "dry_run", "simulated", "Dry Run 보호가 켜져 있어 브로커 전송 없이 주문 의도를 감사 로그에만 기록했습니다.", report
        return False, "adapter_blocked", "held", "실제 주문 전송 레이어가 아직 안전 검증 전이므로 브로커 전송을 차단했습니다.", report

    adapter_labels = {"운용 모드", "실거래 환경 변수", "실주문 어댑터"}
    non_adapter_blockers = [check for check in report.blockers if check.label not in adapter_labels]
    if non_adapter_blockers:
        return False, "risk_blocked", "blocked", report.summary, report
    adapter_blocker = next((check for check in report.blockers if check.label == "실주문 어댑터"), report.blockers[0])
    return False, "adapter_blocked", "held", adapter_blocker.detail, report


def default_order_intent(checks: dict[str, Any], side: str) -> OrderIntent:
    strategies = checks.get("strategies", [])
    strategy = next((item for item in strategies if item.get("live_allowed")), strategies[0] if strategies else {})
    normalized_side: OrderSide = "SELL" if str(side).upper() == "SELL" else "BUY"
    symbol = str(strategy.get("symbol") or "TEST")
    asset = str(strategy.get("asset") or asset_from_symbol(symbol))
    return OrderIntent(
        strategy_id=str(strategy.get("strategy_id") or "manual-test"),
        asset=asset,
        symbol=symbol,
        side=normalized_side,
        quantity=1.0,
        reference_price=1000.0,
        mode=current_mode(),
        reason="live test intent",
        metadata={"broker_id": strategy_broker_id(strategy) if strategy else broker_id_from_symbol(symbol, asset)},
    )


def pre_trade_context(checks: dict[str, Any], intent: OrderIntent, dry_run: bool) -> PreTradeContext:
    settings = STATE["risk_settings"]
    reconciliation = checks.get("reconciliation") if isinstance(checks.get("reconciliation"), dict) else {}
    reconciliation_summary = reconciliation.get("summary") if isinstance(reconciliation.get("summary"), dict) else {}
    positions_matched = str(reconciliation_summary.get("status", "pass")) == "pass"
    return PreTradeContext(
        mode=current_mode(),
        dry_run=dry_run,
        halted=bool(STATE["kill_switch"]),
        new_entries_blocked=bool(STATE["new_entries_blocked"]),
        readiness_blockers=int(checks.get("summary", {}).get("blocker_count", 0)),
        readiness_warnings=int(checks.get("summary", {}).get("warning_count", 0)),
        real_orders_enabled=real_orders_enabled(),
        live_order_adapter_verified=broker_ready_for_intent(checks, intent),
        max_order_value=float(settings["strategy_capital_limit_krw"]),
        cooldown_seconds=int(float(settings["duplicate_order_cooldown_sec"])),
        max_symbol_weight_pct=float(settings["max_symbol_exposure_pct"]),
        daily_loss_limit_pct=float(settings["daily_loss_limit_pct"]),
        strategy_capital_limit=float(settings["strategy_capital_limit_krw"]),
        max_slippage_bps=float(settings["max_slippage_bps"]),
        max_open_orders=int(float(settings["max_open_orders"])),
        open_order_count=open_order_count(),
        positions_matched=positions_matched,
        recent_orders=recent_orders_for_risk(),
    )


def current_mode() -> Mode:
    value = str(STATE.get("mode", "MONITOR")).upper()
    if value in {"MONITOR", "SMALL_LIVE", "FULL_LIVE"}:
        return value  # type: ignore[return-value]
    return "MONITOR"


def asset_from_symbol(symbol: str) -> str:
    text = symbol.upper()
    if any(token in text for token in ("BTC", "ETH", "USDT", "KRW-")):
        return "코인"
    if text.endswith(".KS") or text.isdigit():
        return "한국주식"
    return "미국주식"


def broker_id_from_symbol(symbol: str, asset: str) -> str:
    text = f"{symbol} {asset}".lower()
    if "krw-" in text or "upbit" in text:
        return "upbit"
    if any(token in text for token in ("btc", "eth", "usdt", "코인", "crypto")):
        return "binance"
    return "kis"


def broker_ready_for_intent(checks: dict[str, Any], intent: OrderIntent) -> bool:
    broker_id = str(intent.metadata.get("broker_id") or broker_id_from_symbol(intent.symbol, intent.asset))
    for broker in checks.get("brokers", []):
        if str(broker.get("broker_id")) == broker_id:
            return bool(broker.get("order_ready"))
    return False


def open_order_count() -> int:
    return sum(1 for order in STATE["orders"] if order.get("state") not in FINAL_ORDER_STATES and order.get("queue_state") != "canceled")


def recent_orders_for_risk() -> tuple[RecentOrder, ...]:
    rows: list[RecentOrder] = []
    for order in STATE["orders"][:50]:
        occurred_at = parse_order_time(str(order.get("created_at") or order.get("time") or ""))
        if occurred_at is None:
            continue
        side: OrderSide = "SELL" if str(order.get("side", "")).upper() == "SELL" else "BUY"
        rows.append(
            RecentOrder(
                strategy_id=str(order.get("strategy_id", "")),
                symbol=str(order.get("symbol", "")),
                side=side,
                occurred_at=occurred_at,
                state=str(order.get("state", "")),
            )
        )
    return tuple(rows)


def parse_order_time(value: str) -> datetime | None:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%H:%M:%S"):
        try:
            parsed = datetime.strptime(value, fmt)
            if fmt == "%H:%M:%S":
                now = datetime.now()
                parsed = parsed.replace(year=now.year, month=now.month, day=now.day)
            return parsed
        except ValueError:
            continue
    return None


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
    intent = default_order_intent(checks, side)
    ok, order_state, queue_state, reason, risk_report = evaluate_order_gate_with_report(checks, side, dry_run, intent)
    retry_backoff = float(STATE["retry_policy"]["backoff_sec"])
    order = {
        "time": now_text(),
        "created_at": now_text(),
        "updated_at": now_text(),
        "order_id": next_order_id(order_state, dry_run),
        "strategy_id": strategy["strategy_id"],
        "symbol": strategy["symbol"],
        "asset": intent.asset,
        "side": side,
        "qty": f"{intent.quantity:g}",
        "reference_price": f"{intent.reference_price:g}",
        "notional": f"{intent.notional:,.0f}",
        "state": order_state,
        "queue_state": queue_state,
        "attempts": 1,
        "next_retry_at": future_text(retry_backoff) if order_state in {"risk_blocked", "adapter_blocked"} else "-",
        "broker_order_id": "-",
        "dry_run": dry_run,
        "reason": reason,
        "risk_report": risk_report.to_dict(),
    }
    STATE["orders"].insert(0, order)
    STATE["orders"] = STATE["orders"][:50]
    append_audit("info" if ok else "danger", "주문 게이트", order_gate_audit_detail(order, reason, risk_report))
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
    retry_intent = OrderIntent(
        strategy_id=str(order.get("strategy_id", "manual-retry")),
        asset=str(order.get("asset") or asset_from_symbol(str(order.get("symbol", "")))),
        symbol=str(order.get("symbol", "")),
        side="SELL" if str(order.get("side", "")).upper() == "SELL" else "BUY",
        quantity=float(order.get("qty", 1) or 1),
        reference_price=float(order.get("reference_price", 1000) or 1000),
        mode=current_mode(),
        reason=f"{order_id} retry",
        metadata={"broker_id": broker_id_from_symbol(str(order.get("symbol", "")), str(order.get("asset", "")))},
    )
    ok, state_name, queue_state, reason, risk_report = evaluate_order_gate_with_report(
        checks,
        str(order.get("side", "BUY")),
        bool(order.get("dry_run", STATE["dry_run"])),
        retry_intent,
    )
    if not ok and order["attempts"] >= int(float(STATE["retry_policy"]["max_attempts"])):
        state_name = "retry_exhausted"
        queue_state = "failed"
        reason = f"재시도 {order['attempts']}회가 모두 소진되었습니다. 마지막 사유: {reason}"
    order["state"] = state_name
    order["queue_state"] = queue_state
    order["reason"] = reason
    order["risk_report"] = risk_report.to_dict()
    order["next_retry_at"] = future_text(float(STATE["retry_policy"]["backoff_sec"])) if can_retry_order(order) else "-"
    append_audit("info" if ok else "warn", "주문 재시도", order_gate_audit_detail(order, reason, risk_report))
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
