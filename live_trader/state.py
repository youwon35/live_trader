from __future__ import annotations

import os
import csv
import html
import json
import secrets
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
import sys
from typing import Any, Literal


for parent in Path(__file__).resolve().parents:
    shared_runtime = parent / "packages" / "trading_runtime"
    if shared_runtime.exists():
        if str(shared_runtime) not in sys.path:
            sys.path.insert(0, str(shared_runtime))
        break

from trading_runtime import (
    AuditEvent,
    DeploymentStore,
    DurableControlState,
    EvidenceStore,
    MarketEvent,
    OrderManagementSystem,
    build_idempotency_key,
    audit_event_from_order_gate,
    build_audit_event,
    artifact_content_hash,
    build_lineage_manifest,
    build_live_execution_evidence,
    normalize_broker_execution,
    rebalance_decision,
    PolicyReplayEngine,
    ReplayEvent,
    DeRiskInputs,
    automatic_de_risk,
    operational_readiness,
    RecoveryJournal,
    ShadowLiveEngine,
    ShadowOrder,
    InstrumentTradePolicy,
    MultiStrategySleeveCoordinator,
    SleeveSignal,
    ExecutionSample,
    assess_recovery_drill,
    calibrate_execution,
    record_flight_event,
)
from .brokers import BrokerNotReadyError, LiveBrokerRouter, broker_adapter_contract, broker_diagnostics, broker_readiness, real_orders_enabled
from .program_ledger import ProgramLedger
from .contracts import (
    IGNORED_STRATEGY_FILE_NAMES,
    can_live_small_use_artifact,
    can_live_use_artifact,
    enrich_strategy_artifact_runtime,
    lifecycle_rank,
    load_portfolio_artifacts,
    load_strategy_artifacts,
    normalize_lifecycle_status,
    normalize_strategy_artifact,
    sample_strategy_artifacts,
    strategy_artifact_dirs,
    strategy_plugin_status,
    strategy_revalidation_status,
)
from .audit_store import SQLiteAuditEventStore
from .env_loader import default_runtime_data_root
from .execution_streams import ExecutionStreamManager
from .live_adapters import build_binance_spot_order_request, build_kis_live_order_request, build_upbit_order_request
from .order_management import OrderIntent, OrderSide
from .risk_engine import PreTradeContext, PreTradeRiskGate, PreTradeRiskReport, RecentOrder, RiskCheck
from trading_runtime.strategy_runner import StrategyExecutionResult, StrategyExecutionRunner, StrategyMarketData
from trading_runtime.market_calendar import market_session_state


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
    {
        "broker_id": "upbit",
        "broker_name": "Upbit",
        "account": "Upbit KRW",
        "currency": "KRW",
        "program_cash": None,
        "broker_cash": None,
        "detail": "Upbit 전체 계좌 조회 API가 연결되어야 원화 잔고를 대조합니다.",
    },
)

FINAL_ORDER_STATES = {"dry_run", "sent", "filled", "canceled", "retry_exhausted"}
AUDIT_LOG_LIMIT = 500
APP_DATA_ROOT = default_runtime_data_root()
AUDIT_DB_PATH = Path(os.environ.get("LIVE_TRADER_AUDIT_DB") or APP_DATA_ROOT / "logs" / "live_trader_audit.sqlite3")
AUDIT_STORE = SQLiteAuditEventStore(AUDIT_DB_PATH)
PROGRAM_LEDGER_PATH = Path(
    os.environ.get("LIVE_TRADER_PROGRAM_LEDGER_DB") or APP_DATA_ROOT / "logs" / "live_trader_program_ledger.sqlite3"
)
PROGRAM_LEDGER = ProgramLedger(PROGRAM_LEDGER_PATH)
LIVE_OMS = OrderManagementSystem()
DEFAULT_WATCHDOG_SETTINGS: dict[str, float] = {
    "heartbeat_timeout_sec": 45.0,
    "market_data_stale_sec": 90.0,
    "max_recent_orders_per_min": 6.0,
    "max_retryable_orders": 3.0,
    "max_blocked_orders": 5.0,
}


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
    "strategy_runner": {
        "last_run": "미실행",
        "last_profile": "-",
        "last_strategy": "-",
        "last_signal": "-",
        "last_action": "대기",
    },
    "strategy_sleeves": {},
    "watchdog": {
        "last_run": "미실행",
        "status": "idle",
        "status_label": "대기",
        "last_action": "대기",
        "last_trip": "-",
        "trip_count": 0,
        "settings": dict(DEFAULT_WATCHDOG_SETTINGS),
        "checks": [],
    },
    "broker_reconciliation": {
        "fetched_at": None,
        "accounts": [],
        "positions": [],
        "errors": [],
        "successful_account_brokers": [],
        "successful_position_brokers": [],
    },
    "program_ledger": {
        "last_baseline": None,
        "last_baseline_source": "",
        "last_event_sync": None,
    },
    "execution_events": {
        "last_poll": None,
        "errors": [],
        "event_count": 0,
        "recorded_count": 0,
        "synced_cash_count": 0,
        "synced_position_count": 0,
    },
    "reconciliation_last_run": None,
    "preflight_last_run": None,
    "orders": [],
    "persisted_idempotency_keys": [],
    "shadow_evidence": [],
    "upbit_smoke_order": {
        "status": "idle",
        "status_label": "미리보기 필요",
        "market": "KRW-BTC",
        "notional_krw": 5000,
        "confirmation_token": "",
        "identifier": "",
        "expires_at": "",
        "used": False,
        "detail": "실제 주문 전 주문 가능 정보와 원화 잔고를 먼저 조회합니다.",
    },
    "recovery_status": {"verified": False, "safeMode": True, "generation": 0, "detail": "복구 훈련 미실행"},
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
            "detail": "strategy lifecycle/evidence 기반 live eligibility를 Python 게이트에서 미러링합니다.",
        },
    ],
}

RUNTIME_RECOVERY_ROOT = Path(os.getenv("LIVE_TRADER_RECOVERY_DIR") or APP_DATA_ROOT / "logs" / "recovery-journal")
RECOVERY_JOURNAL = RecoveryJournal(RUNTIME_RECOVERY_ROOT)
SHADOW_ENGINE = ShadowLiveEngine()
LIVE_MULTI_COORDINATOR = MultiStrategySleeveCoordinator(conflict_policy="net")
from .continuous_live import LiveContinuousRuntimeManager  # noqa: E402

LIVE_CONTINUOUS_CONTROLLER = LiveContinuousRuntimeManager(APP_DATA_ROOT)
LIVE_EXECUTION_STREAMS = ExecutionStreamManager(APP_DATA_ROOT)


def now_text() -> str:
    return datetime.now().strftime("%H:%M:%S")


def future_text(seconds: float) -> str:
    return (datetime.now() + timedelta(seconds=max(0.0, seconds))).strftime("%H:%M:%S")


def parse_state_datetime(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text or text in {"-", "미실행", "대기"}:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%H:%M:%S"):
        try:
            parsed = datetime.strptime(text, fmt)
            if fmt == "%H:%M:%S":
                now = datetime.now()
                parsed = parsed.replace(year=now.year, month=now.month, day=now.day)
            return parsed
        except ValueError:
            continue
    return None


def seconds_since(value: object, now: datetime | None = None) -> int | None:
    parsed = parse_state_datetime(value)
    if parsed is None:
        return None
    return max(0, int(((now or datetime.now()) - parsed).total_seconds()))


def env_enabled(name: str, default: bool = False) -> bool:
    value = os.getenv(name, "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on"}


def strategy_rows(portfolios: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    artifacts = load_strategy_artifacts()
    if not artifacts:
        artifacts = sample_strategy_artifacts()
    portfolio_artifacts = portfolios if portfolios is not None else portfolio_rows()
    rows = []
    for artifact in artifacts:
        live_eligible = can_live_use_artifact(artifact)
        live_small_eligible = can_live_small_use_artifact(artifact)
        live_allowed = live_eligible or live_small_eligible
        portfolio_gate = portfolio_gate_for_strategy(artifact, portfolio_artifacts, mode=current_mode())
        paper_portfolio_evidence_gate = paper_portfolio_evidence_gate_for_strategy(artifact, portfolio_gate)
        if portfolio_gate.get("active") and portfolio_gate.get("allowed") is not True:
            live_allowed = False
            live_small_eligible = False
            live_eligible = False
        if paper_portfolio_evidence_gate.get("required") and paper_portfolio_evidence_gate.get("ready") is not True:
            live_allowed = False
            live_small_eligible = False
            live_eligible = False
        fail_reasons = artifact.get("permissions", {}).get("fail_reasons", [])
        block_reasons = list(fail_reasons)
        if portfolio_gate.get("active") and portfolio_gate.get("allowed") is not True:
            block_reasons.append(str(portfolio_gate.get("detail") or "Portfolio artifact gate blocked strategy."))
        if paper_portfolio_evidence_gate.get("required") and paper_portfolio_evidence_gate.get("ready") is not True:
            block_reasons.append(str(paper_portfolio_evidence_gate.get("detail") or "Portfolio paper evidence gate blocked strategy."))
        verification = artifact.get("verification") if isinstance(artifact.get("verification"), dict) else {}
        backtester_verification = verification.get("backtester") if isinstance(verification.get("backtester"), dict) else {}
        paper_verification = verification.get("paper_trader") if isinstance(verification.get("paper_trader"), dict) else {}
        rows.append(
            {
                **artifact,
                "live_allowed": live_allowed,
                "live_small_eligible": live_small_eligible,
                "live_eligible": live_eligible,
                "permission_label": "FULL LIVE OK" if live_eligible else "LIVE-SMALL OK" if live_small_eligible else "LIVE BLOCKED",
                "block_reason": "; ".join(block_reasons) if block_reasons else ("정식 실거래 가능" if live_eligible else "소액 실거래 준비 가능" if live_small_eligible else "lifecycle/evidence 기준을 통과하지 못했습니다."),
                "backtester_verified": backtester_verification.get("status") == "pass",
                "paper_trader_verified": paper_verification.get("status") == "pass",
                "backtester_label": str(backtester_verification.get("label", "Backtester 정보 없음")),
                "paper_trader_label": str(paper_verification.get("label", "Paper 미검증")),
                "portfolio_gate": portfolio_gate,
                "paper_portfolio_evidence_gate": paper_portfolio_evidence_gate,
            }
        )
    return rows


def paper_portfolio_evidence_gate_for_strategy(strategy: dict[str, Any], portfolio_gate: dict[str, Any]) -> dict[str, Any]:
    if not portfolio_gate.get("active"):
        return {"required": False, "ready": True, "detail": "Portfolio artifact gate 비활성"}
    if portfolio_gate.get("allowed") is not True:
        return {"required": True, "ready": False, "detail": "Portfolio hard gate 통과 전 evidence 평가 보류"}
    evidence = strategy.get("paper_portfolio_evidence") if isinstance(strategy.get("paper_portfolio_evidence"), dict) else {}
    portfolio_id = str(portfolio_gate.get("portfolioId") or "")
    if not evidence:
        return {
            "required": True,
            "ready": False,
            "portfolioId": portfolio_id,
            "detail": f"{portfolio_id or '선택 Portfolio'} 기준 Paper portfolio evidence가 없습니다.",
        }
    if str(evidence.get("portfolioId") or "") != portfolio_id:
        return {
            "required": True,
            "ready": False,
            "portfolioId": portfolio_id,
            "evidencePortfolioId": str(evidence.get("portfolioId") or ""),
            "detail": "Paper portfolio evidence가 현재 Live portfolio artifact와 일치하지 않습니다.",
        }
    filled_count = int(safe_float(evidence.get("filledCount"), 0.0))
    rejected_count = int(safe_float(evidence.get("rejectedCount"), 0.0))
    ready = evidence.get("ready") is True and str(evidence.get("status") or "").lower() == "submitted" and filled_count > 0 and rejected_count == 0
    return {
        "required": True,
        "ready": ready,
        "portfolioId": portfolio_id,
        "portfolioName": str(evidence.get("portfolioName") or portfolio_gate.get("portfolioName") or ""),
        "status": str(evidence.get("status") or ""),
        "filledCount": filled_count,
        "rejectedCount": rejected_count,
        "targetWeight": evidence.get("targetWeight"),
        "detail": (
            f"Portfolio paper evidence 통과: filled {filled_count}건"
            if ready
            else f"Portfolio paper evidence 차단: status={evidence.get('status') or '-'}, filled={filled_count}, rejected={rejected_count}"
        ),
    }


def portfolio_rows() -> list[dict[str, Any]]:
    return load_portfolio_artifacts()


def safe_float(value: Any, fallback: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def normalize_weight_percent(value: Any, fallback: float) -> float:
    number = safe_float(value, fallback)
    if number <= 1.0:
        return number * 100
    return number


def portfolio_strategy_value(item: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = item.get(key)
        if value not in (None, ""):
            return str(value)
    return ""


def portfolio_fx_freshness(portfolio: dict[str, Any], symbol: str) -> dict[str, Any]:
    """Return the auditable FX quote age used by a portfolio strategy."""

    portfolio_body = portfolio.get("portfolio") if isinstance(portfolio.get("portfolio"), dict) else {}
    base_currency = str(portfolio_body.get("baseCurrency") or portfolio.get("base_currency") or "KRW").upper()
    upper_symbol = str(symbol or "").upper()
    currency = next((suffix for suffix in ("USDT", "USDC", "USD", "KRW") if upper_symbol.endswith(suffix)), base_currency)
    if currency == base_currency:
        return {"currency": currency, "baseCurrency": base_currency, "rate": 1.0, "asOf": datetime.now(timezone.utc).date().isoformat(), "ageDays": 0, "fresh": True, "limitDays": 7, "source": "same-currency"}
    fx_policy = portfolio_body.get("fxPolicy") if isinstance(portfolio_body.get("fxPolicy"), dict) else {}
    if not fx_policy:
        fx_policy = portfolio.get("fx_policy") if isinstance(portfolio.get("fx_policy"), dict) else portfolio.get("fxPolicy") if isinstance(portfolio.get("fxPolicy"), dict) else {}
    for conversion in fx_policy.get("conversions", []):
        if not isinstance(conversion, dict):
            continue
        if str(conversion.get("currency") or "").upper() != currency or str(conversion.get("baseCurrency") or "").upper() != base_currency:
            continue
        as_of = str(conversion.get("sourceDate") or conversion.get("asOf") or "")[:10]
        age_days = 999999
        if as_of:
            try:
                age_days = max(0, (datetime.now(timezone.utc).date() - datetime.fromisoformat(as_of).date()).days)
            except ValueError:
                age_days = 999999
        limit_days = max(1, int(safe_float(conversion.get("freshnessLimitDays"), 7)))
        return {
            "currency": currency,
            "baseCurrency": base_currency,
            "rate": safe_float(conversion.get("rate"), 0.0),
            "asOf": as_of,
            "ageDays": age_days,
            "fresh": age_days <= limit_days and safe_float(conversion.get("rate"), 0.0) > 0,
            "limitDays": limit_days,
            "source": str(conversion.get("seriesId") or conversion.get("mode") or "artifact-fx"),
        }
    return {"currency": currency, "baseCurrency": base_currency, "rate": 0.0, "asOf": "", "ageDays": 999999, "fresh": False, "limitDays": 7, "source": "missing-fx"}


def portfolio_gate_for_strategy(
    strategy: dict[str, Any],
    portfolios: list[dict[str, Any]] | None = None,
    *,
    mode: Mode | None = None,
) -> dict[str, Any]:
    portfolio_artifacts = portfolios if portfolios is not None else portfolio_rows()
    if not portfolio_artifacts:
        return {"active": False, "allowed": True, "detail": "Portfolio artifact 저장소가 없어 단일 전략 기준으로 평가합니다."}

    strategy_id = str(strategy.get("strategy_id") or "")
    symbol = str(strategy.get("symbol") or "")
    for portfolio in portfolio_artifacts:
        match = portfolio_match_for_strategy(portfolio, strategy_id, symbol, mode=mode or current_mode())
        if match is not None:
            return match
    return {
        "active": True,
        "allowed": False,
        "detail": f"유효한 Portfolio artifact가 있지만 {strategy_id}/{symbol} 조합이 live universe에 없습니다.",
        "portfolioCount": len(portfolio_artifacts),
    }


def portfolio_match_for_strategy(portfolio: dict[str, Any], strategy_id: str, symbol: str, *, mode: Mode) -> dict[str, Any] | None:
    instances = portfolio.get("strategy_instances") if isinstance(portfolio.get("strategy_instances"), list) else []
    targets = portfolio.get("target_portfolio") if isinstance(portfolio.get("target_portfolio"), list) else []
    matching_instance = next(
        (
            item
            for item in instances
            if isinstance(item, dict)
            and portfolio_strategy_value(item, "strategyId", "strategy_id") == strategy_id
            and (not portfolio_strategy_value(item, "symbol") or portfolio_strategy_value(item, "symbol") == symbol)
        ),
        None,
    )
    matching_target = next(
        (
            item
            for item in targets
            if isinstance(item, dict)
            and portfolio_strategy_value(item, "strategyId", "strategy_id") == strategy_id
            and (not portfolio_strategy_value(item, "symbol") or portfolio_strategy_value(item, "symbol") == symbol)
        ),
        None,
    )
    if matching_instance is None and matching_target is None:
        return None

    allocation = matching_instance.get("allocation") if isinstance(matching_instance, dict) and isinstance(matching_instance.get("allocation"), dict) else {}
    portfolio_policy = portfolio.get("portfolio_policy") if isinstance(portfolio.get("portfolio_policy"), dict) else {}
    advanced_operations = portfolio.get("advanced_operations") if isinstance(portfolio.get("advanced_operations"), dict) else {}
    operational_bundle = portfolio.get("operational_readiness_bundle") if isinstance(portfolio.get("operational_readiness_bundle"), dict) else {}
    mandate = advanced_operations.get("mandate") if isinstance(advanced_operations.get("mandate"), dict) else {}
    automatic_de_risk = advanced_operations.get("automaticDeRisk") if isinstance(advanced_operations.get("automaticDeRisk"), dict) else {}
    capital_multiplier = max(0.0, min(1.0, safe_float(automatic_de_risk.get("capitalMultiplier"), 1.0)))
    policy_allocations = portfolio_policy.get("allocations") if isinstance(portfolio_policy.get("allocations"), list) else []
    policy_profiles = portfolio_policy.get("profiles") if isinstance(portfolio_policy.get("profiles"), list) else []
    instance_id = portfolio_strategy_value(matching_instance or {}, "instanceId", "strategyInstanceId")
    policy_allocation = next(
        (item for item in policy_allocations if isinstance(item, dict) and str(item.get("strategyInstanceId") or "") == instance_id),
        None,
    )
    policy_profile = next(
        (item for item in policy_profiles if isinstance(item, dict) and str(item.get("strategyInstanceId") or "") == instance_id),
        None,
    )
    configured_target_weight = safe_float(
        (policy_allocation or {}).get("targetWeight") if isinstance(policy_allocation, dict) else ((matching_target or {}).get("targetWeight") if isinstance(matching_target, dict) else None),
        safe_float(allocation.get("scoreTargetWeight"), safe_float(allocation.get("normalizedWeight"), 0.0)),
    )
    position_size_fraction = safe_float(
        (policy_allocation or {}).get("positionSizeFraction") if isinstance(policy_allocation, dict) else None,
        safe_float((matching_instance or {}).get("positionSizeFraction"), 1.0),
    )
    position_size_fraction = max(0.0, min(1.0, position_size_fraction))
    target_weight = configured_target_weight * position_size_fraction
    policy_target_weight = target_weight
    target_weight *= capital_multiplier
    capacity_entries = advanced_operations.get("capacity") if isinstance(advanced_operations.get("capacity"), list) else []
    capacity = next((item for item in capacity_entries if isinstance(item, dict) and str(item.get("strategyInstanceId") or "") == instance_id), {})
    risk_policy = portfolio.get("risk_policy") if isinstance(portfolio.get("risk_policy"), dict) else {}
    max_symbol_weight = normalize_weight_percent(risk_policy.get("maxSingleSymbolWeight"), 100.0)
    max_strategy_weight = normalize_weight_percent(risk_policy.get("maxStrategyWeight"), 100.0)
    effective_symbol_limit = min(max_symbol_weight, target_weight * 100 if target_weight > 0 else max_symbol_weight)
    permissions = portfolio.get("permissions") if isinstance(portfolio.get("permissions"), dict) else {}
    lifecycle_status = normalize_lifecycle_status(portfolio.get("lifecycle_status"))
    required_status = "live" if mode == "FULL_LIVE" else "before-live-small"
    permission_ok = (
        permissions.get("live_allowed") is True
        or permissions.get("live_export_allowed") is True
        or (mode != "FULL_LIVE" and permissions.get("live_small_allowed") is True)
    )
    blockers: list[str] = []
    fx_freshness = portfolio_fx_freshness(portfolio, symbol)
    if lifecycle_status in {"paused", "retired"}:
        blockers.append(f"lifecycle={lifecycle_status}")
    if lifecycle_rank(lifecycle_status) < lifecycle_rank(required_status):
        blockers.append(f"lifecycle={lifecycle_status}, required={required_status}")
    if not permission_ok:
        blockers.append("live_export_allowed=false")
    failed_checks = [
        str(check.get("label") or check.get("id") or "risk")
        for check in portfolio.get("risk_checks", [])
        if isinstance(check, dict) and str(check.get("status") or "").lower() == "fail"
    ]
    if failed_checks:
        blockers.append("riskChecks=" + ", ".join(failed_checks[:3]))
    if target_weight <= 0:
        blockers.append("targetWeight=0")
    if fx_freshness.get("fresh") is not True:
        blockers.append(f"fx-stale:{fx_freshness.get('asOf') or 'timestamp-missing'}:{fx_freshness.get('ageDays')}d")
    policy_limit_blockers: list[str] = []
    limits = portfolio_policy.get("limits") if isinstance(portfolio_policy.get("limits"), list) else []
    if portfolio_policy and policy_allocation is None:
        policy_limit_blockers.append("portfolio-policy-allocation-missing")
    for limit in limits:
        if not isinstance(limit, dict):
            continue
        level = str(limit.get("level") or "")
        key = str(limit.get("key") or "")
        maximum = safe_float(limit.get("maximumWeight"), 1.0)
        total = 0.0
        for item in policy_allocations:
            if not isinstance(item, dict):
                continue
            profile = next((candidate for candidate in policy_profiles if isinstance(candidate, dict) and str(candidate.get("strategyInstanceId") or "") == str(item.get("strategyInstanceId") or "")), {})
            values = {
                "account": "default",
                "asset_class": str(profile.get("assetClass") or "UNKNOWN"),
                "risk_cluster": str(item.get("returnSource") or profile.get("returnSource") or "OTHER"),
                "instrument": str(profile.get("instrumentId") or "UNKNOWN"),
                "strategy": str(item.get("strategyInstanceId") or ""),
            }
            if level != "order" and (key == "*" or values.get(level) == key):
                total += abs(
                    safe_float(item.get("targetWeight"), 0.0)
                    * max(0.0, min(1.0, safe_float(item.get("positionSizeFraction"), 1.0)))
                )
        if level != "order" and total > maximum + 1e-12:
            policy_limit_blockers.append(f"{level}:{key}:exposure-limit")
    blockers.extend(policy_limit_blockers)

    return {
        "active": True,
        "allowed": not blockers,
        "detail": "Portfolio hard gate 통과" if not blockers else "Portfolio hard gate 차단: " + "; ".join(blockers),
        "portfolioId": str(portfolio.get("id") or ""),
        "portfolioName": str(portfolio.get("name") or ""),
        "portfolioPath": str(portfolio.get("source_path") or ""),
        "lifecycleStatus": lifecycle_status,
        "requiredLifecycleStatus": required_status,
        "targetWeight": target_weight,
        "policyTargetWeight": policy_target_weight,
        "configuredTargetWeight": configured_target_weight,
        "positionSizeFraction": position_size_fraction,
        "fxFreshness": fx_freshness,
        "maxSymbolWeightPct": effective_symbol_limit,
        "maxStrategyWeightPct": max_strategy_weight,
        "strategyCapitalRatio": min(target_weight, max_strategy_weight / 100 if max_strategy_weight > 0 else target_weight),
        "instance": matching_instance or {},
        "target": matching_target or {},
        "policyAllocation": policy_allocation or {},
        "policyProfile": policy_profile or {},
        "portfolioPolicyHash": str(portfolio.get("portfolio_policy_hash") or portfolio_policy.get("policyHash") or ""),
        "rebalancePolicy": portfolio_policy.get("rebalancePolicy") if isinstance(portfolio_policy.get("rebalancePolicy"), dict) else {},
        "policyLimitBlockers": policy_limit_blockers,
        "advancedOperationsHash": str(portfolio.get("advanced_operations_hash") or advanced_operations.get("contentHash") or ""),
        "mandateCompliant": mandate.get("compliant") is True if mandate else True,
        "mandateBreaches": mandate.get("breaches") if isinstance(mandate.get("breaches"), list) else [],
        "automaticDeRiskAction": str(automatic_de_risk.get("action") or "KEEP"),
        "capitalMultiplier": capital_multiplier,
        "capacity": capacity,
        "stressPassed": (advanced_operations.get("stressLibrary") or {}).get("passed") is True if isinstance(advanced_operations.get("stressLibrary"), dict) else True,
        "decisionQuality": advanced_operations.get("decisionQuality") if isinstance(advanced_operations.get("decisionQuality"), dict) else {},
        "contagionGraph": advanced_operations.get("contagionGraph") if isinstance(advanced_operations.get("contagionGraph"), dict) else {},
        "championChallenger": advanced_operations.get("championChallenger") if isinstance(advanced_operations.get("championChallenger"), dict) else {},
        "operationalReadinessBundle": operational_bundle,
        "operationalReadinessHash": str(portfolio.get("operational_readiness_hash") or operational_bundle.get("contentHash") or ""),
    }


def portfolio_gate_for_intent(checks: dict[str, Any], intent: OrderIntent) -> dict[str, Any]:
    strategy = strategy_for_order_intent(checks, intent)
    if strategy:
        gate = strategy.get("portfolio_gate") if isinstance(strategy.get("portfolio_gate"), dict) else None
        if gate is not None:
            selected_gate = dict(gate)
        else:
            selected_gate = None
    else:
        selected_gate = None
    if selected_gate is None:
        selected_gate = portfolio_gate_for_strategy(
            {"strategy_id": intent.strategy_id, "symbol": intent.symbol},
            checks.get("portfolios") if isinstance(checks.get("portfolios"), list) else [],
            mode=current_mode(),
        )
    policy = selected_gate.get("rebalancePolicy") if isinstance(selected_gate.get("rebalancePolicy"), dict) else {}
    if not policy or not selected_gate.get("active") or selected_gate.get("allowed") is not True:
        return selected_gate
    metadata = intent.metadata if isinstance(intent.metadata, dict) else {}
    if selected_gate.get("advancedOperationsHash"):
        reconciliation = checks.get("reconciliation") if isinstance(checks.get("reconciliation"), dict) else {}
        reconciliation_summary = reconciliation.get("summary") if isinstance(reconciliation.get("summary"), dict) else {}
        dynamic_de_risk = automatic_de_risk(DeRiskInputs(
            data_quality_ok=metadata.get("data_quality_ok") is not False,
            implementation_match=metadata.get("implementation_match") is not False,
            reconciliation_mismatches=0 if str(reconciliation_summary.get("status") or "pass") == "pass" else 1,
            cost_ratio=safe_float(metadata.get("execution_cost_ratio"), 1.0),
            mdd_ratio=safe_float(metadata.get("mdd_ratio"), 1.0),
            correlation=safe_float(metadata.get("cross_strategy_correlation"), 0.0),
            broker_available=bool(metadata.get("broker_available", broker_ready_for_intent(checks, intent))),
        ))
        combined_multiplier = min(safe_float(selected_gate.get("capitalMultiplier"), 1.0), safe_float(dynamic_de_risk.get("capitalMultiplier"), 1.0))
        selected_gate["dynamicDeRisk"] = dynamic_de_risk
        selected_gate["capitalMultiplier"] = combined_multiplier
        selected_gate["targetWeight"] = safe_float(selected_gate.get("policyTargetWeight"), selected_gate.get("targetWeight")) * combined_multiplier
    required_inputs = ("current_weight", "portfolio_equity", "expected_alpha_bps", "expected_cost_bps")
    missing = [key for key in required_inputs if metadata.get(key) is None]
    if missing:
        selected_gate["allowed"] = False
        selected_gate["rebalanceDecision"] = {"action": "HOLD", "reason": "rebalance-inputs-missing", "missing": missing}
        selected_gate["detail"] = "Portfolio policy 차단: 리밸런싱 경제성 입력 누락(" + ", ".join(missing) + ")"
        return selected_gate
    decision = rebalance_decision(
        current_weight=safe_float(metadata.get("current_weight"), 0.0),
        target_weight=safe_float(selected_gate.get("targetWeight"), 0.0),
        portfolio_equity=safe_float(metadata.get("portfolio_equity"), 0.0),
        expected_alpha_bps=safe_float(metadata.get("expected_alpha_bps"), 0.0),
        expected_cost_bps=safe_float(metadata.get("expected_cost_bps"), 0.0),
        deadband_weight=safe_float(policy.get("deadbandWeight"), 0.0025),
        minimum_notional=safe_float(policy.get("minimumNotional"), 0.0),
        risk_limit_breached=metadata.get("risk_limit_breached") is True,
    )
    selected_gate["rebalanceDecision"] = {
        "action": decision.action,
        "reason": decision.reason,
        "tradeWeight": decision.trade_weight,
        "expectedNotional": decision.expected_notional,
        "priority": decision.priority,
    }
    if decision.action != "TRADE":
        selected_gate["allowed"] = False
        selected_gate["detail"] = f"Portfolio policy 차단: {decision.reason}"
        return selected_gate
    risk_reducing = abs(safe_float(selected_gate.get("targetWeight"), 0.0)) < abs(safe_float(metadata.get("current_weight"), 0.0))
    advanced_blockers: list[str] = []
    capacity = selected_gate.get("capacity") if isinstance(selected_gate.get("capacity"), dict) else {}
    maximum_order_notional = safe_float(capacity.get("maximumOrderNotional"), 0.0)
    if maximum_order_notional > 0 and intent.notional > maximum_order_notional and not risk_reducing:
        advanced_blockers.append("order-capacity-exceeded")
    if selected_gate.get("mandateCompliant") is False and not risk_reducing:
        advanced_blockers.extend(str(reason) for reason in selected_gate.get("mandateBreaches", []))
    if selected_gate.get("stressPassed") is False and not risk_reducing:
        advanced_blockers.append("portfolio-stress-library-failed")
    if safe_float(selected_gate.get("capitalMultiplier"), 1.0) <= 0 and not risk_reducing:
        advanced_blockers.append("automatic-de-risk-freeze")
    operational_bundle = selected_gate.get("operationalReadinessBundle") if isinstance(selected_gate.get("operationalReadinessBundle"), dict) else {}
    runtime_readiness = checks.get("operational_readiness") if isinstance(checks.get("operational_readiness"), dict) else {}
    if operational_bundle and runtime_readiness.get("liveEligible") is not True and not risk_reducing:
        advanced_blockers.append(f"operational-readiness={runtime_readiness.get('score', 0)}/{runtime_readiness.get('threshold', 85)}")
    if advanced_blockers:
        selected_gate["allowed"] = False
        selected_gate["advancedOperationBlockers"] = list(dict.fromkeys(advanced_blockers))
        selected_gate["detail"] = "Advanced portfolio policy 차단: " + "; ".join(selected_gate["advancedOperationBlockers"])
    return selected_gate


def policy_replay_for_intent(checks: dict[str, Any], intent: OrderIntent, alternative: dict[str, Any] | None = None) -> dict[str, Any]:
    alternative = alternative or {}
    original_gate = portfolio_gate_for_intent(checks, intent)
    metadata = intent.metadata if isinstance(intent.metadata, dict) else {}
    event = ReplayEvent(
        event_id=str(metadata.get("event_id") or f"{intent.strategy_id}:{intent.symbol}:{intent.side}"),
        occurred_at=str(metadata.get("occurred_at") or datetime.now(timezone.utc).isoformat()),
        original_policy_version=str(original_gate.get("portfolioPolicyHash") or "legacy"),
        original_decision="TRADE" if original_gate.get("allowed") is True else "HOLD",
        inputs={
            "currentWeight": metadata.get("current_weight"),
            "targetWeight": original_gate.get("targetWeight"),
            "portfolioEquity": metadata.get("portfolio_equity"),
            "expectedAlphaBps": metadata.get("expected_alpha_bps"),
            "expectedCostBps": metadata.get("expected_cost_bps"),
            "notional": intent.notional,
        },
        original_reasons=(str(original_gate.get("detail") or ""),),
    )

    def evaluator(inputs: dict[str, Any]) -> dict[str, Any]:
        decision = rebalance_decision(
            current_weight=safe_float(inputs.get("currentWeight"), 0.0),
            target_weight=safe_float(inputs.get("targetWeight"), 0.0) * max(0.0, min(1.0, safe_float(alternative.get("capitalMultiplier"), 1.0))),
            portfolio_equity=safe_float(inputs.get("portfolioEquity"), 0.0),
            expected_alpha_bps=safe_float(inputs.get("expectedAlphaBps"), 0.0),
            expected_cost_bps=safe_float(inputs.get("expectedCostBps"), 0.0) + safe_float(alternative.get("costBufferBps"), 0.0),
            deadband_weight=safe_float(alternative.get("deadbandWeight"), 0.0025),
            minimum_notional=safe_float(alternative.get("minimumNotional"), 0.0),
        )
        return {"decision": "TRADE" if decision.action == "TRADE" else "HOLD", "reasons": [decision.reason]}

    return PolicyReplayEngine().replay([event], alternative_policy_version=str(alternative.get("policyVersion") or "live-alternative-v1"), evaluator=evaluator)


def apply_portfolio_gate_to_context(context: PreTradeContext, gate: dict[str, Any], intent: OrderIntent) -> PreTradeContext:
    if not gate.get("active") or gate.get("allowed") is not True:
        return context
    base_equity = max(float(context.portfolio_equity or 0.0), context.strategy_capital_limit, intent.notional, 1.0)
    target_weight = safe_float(gate.get("targetWeight"), 0.0)
    strategy_ratio = safe_float(gate.get("strategyCapitalRatio"), target_weight)
    max_symbol_weight_pct = safe_float(gate.get("maxSymbolWeightPct"), context.max_symbol_weight_pct)
    strategy_capital_limit = base_equity * strategy_ratio if strategy_ratio > 0 else context.strategy_capital_limit
    return replace(
        context,
        portfolio_equity=base_equity,
        max_symbol_weight_pct=min(context.max_symbol_weight_pct, max_symbol_weight_pct),
        strategy_capital_limit=min(context.strategy_capital_limit, strategy_capital_limit),
    )


def find_strategy_artifact_payload(strategy_id: str) -> tuple[Path | None, Path | None, dict[str, Any] | None, dict[str, Any] | None]:
    target = str(strategy_id or "").strip()
    if not target:
        return None, None, None, None
    for folder in strategy_artifact_dirs():
        if not folder.exists():
            continue
        for path in sorted(folder.glob("*.json")):
            if path.name in IGNORED_STRATEGY_FILE_NAMES:
                continue
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict):
                continue
            try:
                normalized = normalize_strategy_artifact(enrich_strategy_artifact_runtime(folder, path, payload))
            except (KeyError, TypeError, ValueError):
                continue
            if str(normalized.get("strategy_id")) == target:
                return folder, path, payload, normalized
    return None, None, None, None


def update_strategy_registry(strategy_dir: Path, payload: dict[str, Any], artifact_path: Path) -> None:
    registry_path = strategy_dir / "strategy-registry.json"
    try:
        registry = json.loads(registry_path.read_text(encoding="utf-8")) if registry_path.exists() else {}
    except (OSError, json.JSONDecodeError):
        registry = {}
    if not isinstance(registry, dict):
        registry = {}
    entries = registry.get("entries") if isinstance(registry.get("entries"), dict) else {}
    release = payload.get("release") if isinstance(payload.get("release"), dict) else {}
    promotion = payload.get("promotion") if isinstance(payload.get("promotion"), dict) else {}
    permissions = payload.get("permissions") if isinstance(payload.get("permissions"), dict) else {}
    release_id = str(release.get("releaseId") or payload.get("releaseId") or payload.get("id") or payload.get("strategy_id"))
    entries[release_id] = {
        "releaseId": release_id,
        "artifactId": payload.get("id") or payload.get("strategy_id"),
        "strategyId": payload.get("strategyId") or payload.get("strategy_id") or payload.get("id"),
        "name": payload.get("name"),
        "symbol": payload.get("symbol") or ((payload.get("dataset") or {}).get("symbol") if isinstance(payload.get("dataset"), dict) else payload.get("symbol")),
        "timeframe": payload.get("timeframe"),
        "plugin": payload.get("plugin"),
        "stage": (payload.get("lifecycle") if isinstance(payload.get("lifecycle"), dict) else {}).get("status")
        or payload.get("lifecycleStatus")
        or payload.get("status")
        or promotion.get("stage")
        or payload.get("promotionStage"),
        "stageLabel": promotion.get("stageLabel"),
        "lifecycleStatus": (payload.get("lifecycle") if isinstance(payload.get("lifecycle"), dict) else {}).get("status")
        or payload.get("lifecycleStatus")
        or payload.get("status"),
        "parameterHash": release.get("parameterHash"),
        "parameterSummary": payload.get("parameterSummary") or promotion.get("parameterSummary"),
        "artifactPath": str(artifact_path),
        "trader_export_allowed": permissions.get("trader_export_allowed", payload.get("trader_export_allowed")),
        "paper_trader_verified": permissions.get("paper_trader_verified", payload.get("paper_trader_verified")),
        "live_small_eligible": permissions.get("live_small_eligible", payload.get("live_small_eligible")),
        "live_eligible": permissions.get("live_eligible", payload.get("live_eligible")),
        "live_allowed": permissions.get("live_allowed", payload.get("live_allowed")),
        "updatedAt": payload.get("updatedAt"),
    }
    registry.update(
        {
            "schemaVersion": "strategy-registry-v1",
            "updatedAt": datetime.now().astimezone().replace(microsecond=0).isoformat(),
            "entries": entries,
        }
    )
    registry_path.write_text(json.dumps(registry, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def append_strategy_promotion_log(strategy_dir: Path, payload: dict[str, Any], artifact_path: Path, actor: str) -> None:
    promotion = payload.get("promotion") if isinstance(payload.get("promotion"), dict) else {}
    release = payload.get("release") if isinstance(payload.get("release"), dict) else {}
    event = {
        "schemaVersion": "promotion-event-v1",
        "at": promotion.get("promotedAt") or payload.get("updatedAt"),
        "releaseId": release.get("releaseId") or payload.get("releaseId") or payload.get("id"),
        "artifactId": payload.get("id") or payload.get("strategy_id"),
        "strategyId": payload.get("strategyId") or payload.get("strategy_id") or payload.get("id"),
        "name": payload.get("name"),
        "from": (promotion.get("history") or [{}])[-1].get("from") if isinstance(promotion.get("history"), list) else "",
        "to": (payload.get("lifecycle") if isinstance(payload.get("lifecycle"), dict) else {}).get("status")
        or payload.get("lifecycleStatus")
        or payload.get("status")
        or promotion.get("stage")
        or payload.get("promotionStage"),
        "by": actor,
        "artifactPath": str(artifact_path),
    }
    with (strategy_dir / "promotion-log.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, ensure_ascii=False, default=str) + "\n")


def live_small_execution_summary(strategy_id: str) -> dict[str, int]:
    successful = 0
    blocked = 0
    for order in STATE["orders"]:
        if str(order.get("strategy_id")) != strategy_id:
            continue
        state_name = str(order.get("state", "")).lower()
        queue_state = str(order.get("queue_state", "")).lower()
        if bool(order.get("dry_run")):
            continue
        if state_name in {"sent", "filled"} or queue_state in {"sent", "filled"}:
            successful += 1
        if state_name in {"risk_blocked", "adapter_blocked", "failed", "rejected"} or queue_state in {"blocked", "risk_blocked", "failed", "rejected"}:
            blocked += 1
    return {"successful": successful, "blocked": blocked}


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
            "full_live_strategy_count": sum(1 for strategy in stock_strategies if strategy.get("live_eligible")),
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
            "full_live_strategy_count": sum(1 for strategy in crypto_strategies if strategy.get("live_eligible")),
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
    krx = market_session_state("XKRX", regular_open="09:00", regular_close="15:30")
    nyse = market_session_state("XNYS", regular_open="09:30", regular_close="16:00")
    return [
        {"label": "KRX", "state": str(krx["state"]), "time": "KST", "detail": str(krx["detail"])},
        {"label": "NYSE", "state": str(nyse["state"]), "time": "ET", "detail": str(nyse["detail"])},
        {"label": "Binance", "state": "open", "time": "24/7", "detail": "레이트 리밋/급변동 감시"},
        {"label": "Risk Engine", "state": "blocked", "time": "LIVE GATE", "detail": "API/권한 준비 전 차단"},
    ]


def readiness_checks(
    strategies: list[dict[str, Any]],
    brokers: list[dict[str, object]],
    reconciliation_summary: dict[str, Any],
) -> list[Check]:
    live_ready_count = sum(1 for strategy in strategies if strategy.get("live_allowed") is True)
    full_live_ready_count = sum(1 for strategy in strategies if strategy.get("live_eligible") is True)
    full_live_ready_count = sum(1 for strategy in strategies if strategy.get("live_eligible") is True)
    missing_brokers = [broker for broker in brokers if broker["status"] == "missing_credentials"]
    adapter_blocked = [broker for broker in brokers if broker["live_order_adapter_ready"] is not True]
    checklist = checklist_rows()
    checklist_missing = [item for item in checklist if item["required"] and not item["checked"]]
    reconcile_blocking = int(reconciliation_summary["api_required_count"]) + int(reconciliation_summary["mismatch_count"])
    central_control = durable_control_snapshot()
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
            "전략 lifecycle eligibility",
            "pass" if live_ready_count else "fail",
            f"Live-Small 이상 전략 {live_ready_count}개 · Full Live {full_live_ready_count}개" if live_ready_count else "before-live-small 이상 lifecycle과 검증 evidence가 필요합니다.",
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
            "중앙 Control Plane",
            "fail" if central_control["halted"] else "pass",
            " · ".join(central_control["reasons"]) if central_control["halted"] else "Hub 중앙 Kill 정책이 신규 주문을 차단하지 않습니다.",
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


def broker_reconciliation_errors() -> dict[str, str]:
    errors = STATE.get("broker_reconciliation", {}).get("errors", [])
    if not isinstance(errors, list):
        return {}
    return {str(item.get("broker_id")): str(item.get("detail")) for item in errors if isinstance(item, dict)}


def live_account_rows() -> dict[str, dict[str, object]]:
    rows = STATE.get("broker_reconciliation", {}).get("accounts", [])
    if not isinstance(rows, list):
        return {}
    return {str(item.get("broker_id")): item for item in rows if isinstance(item, dict)}


def live_position_rows() -> dict[tuple[str, str], dict[str, object]]:
    rows = STATE.get("broker_reconciliation", {}).get("positions", [])
    if not isinstance(rows, list):
        return {}
    return {
        (str(item.get("broker_id")), str(item.get("symbol"))): item
        for item in rows
        if isinstance(item, dict) and item.get("broker_id") and item.get("symbol")
    }


def program_cash_rows() -> dict[str, dict[str, Any]]:
    return {str(item.get("broker_id")): item for item in PROGRAM_LEDGER.cash_rows() if item.get("broker_id")}


def program_position_rows() -> dict[tuple[str, str], dict[str, Any]]:
    return {
        (str(item.get("broker_id")), str(item.get("symbol"))): item
        for item in PROGRAM_LEDGER.position_rows()
        if item.get("broker_id") and item.get("symbol")
    }


def program_ledger_snapshot() -> dict[str, Any]:
    summary = PROGRAM_LEDGER.summary()
    return {
        **summary,
        "state": dict(STATE.get("program_ledger", {})),
        "cash": PROGRAM_LEDGER.cash_rows(),
        "positions": PROGRAM_LEDGER.position_rows(),
        "execution_events": PROGRAM_LEDGER.execution_event_rows(20),
    }


def execution_event_snapshot() -> dict[str, Any]:
    state = STATE.get("execution_events", {})
    return {
        "last_poll": state.get("last_poll"),
        "errors": list(state.get("errors", [])),
        "event_count": int(state.get("event_count", 0)),
        "recorded_count": int(state.get("recorded_count", 0)),
        "synced_cash_count": int(state.get("synced_cash_count", 0)),
        "synced_position_count": int(state.get("synced_position_count", 0)),
        "recent": PROGRAM_LEDGER.execution_event_rows(20),
        "streams": LIVE_EXECUTION_STREAMS.snapshot(),
    }


def successful_position_brokers() -> set[str]:
    rows = STATE.get("broker_reconciliation", {}).get("successful_position_brokers", [])
    return {str(item) for item in rows} if isinstance(rows, list) else set()


def execution_calibration_snapshot() -> dict[str, Any]:
    samples = []
    for evidence in STATE.get("shadow_evidence", []):
        decision = evidence.get("decision") if isinstance(evidence, dict) and isinstance(evidence.get("decision"), dict) else {}
        decision_price = safe_float(decision.get("decision_price"), 0.0)
        virtual_fill = safe_float(decision.get("virtual_fill_price"), 0.0)
        if decision_price > 0 and virtual_fill > 0:
            samples.append(ExecutionSample(
                "shadow-live", str(decision.get("side") or "BUY"), decision_price, virtual_fill,
                safe_float(decision.get("expected_cost_bps"), 0.0), int(safe_float(decision.get("latency_ms"), 0)),
            ))
    return calibrate_execution(
        samples,
        maximum_model_error_bps=10.0,
        maximum_p95_slippage_bps=safe_float(STATE.get("risk_settings", {}).get("max_slippage_bps"), 30.0),
    )


def positions() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    broker_rows = live_position_rows()
    ledger_rows = program_position_rows()
    errors = broker_reconciliation_errors()
    successful_brokers = successful_position_brokers()
    for item in POSITION_RECONCILIATION_BOOK:
        key = (str(item["broker_id"]), str(item["symbol"]))
        broker_row = broker_rows.pop(key, None)
        ledger_row = ledger_rows.pop(key, None)
        broker_qty = broker_row.get("broker_qty") if broker_row else item["broker_qty"]
        broker_id = str(item["broker_id"])
        has_complete_zero_snapshot = broker_id in successful_brokers and not (
            broker_id == "kis" and str(item["currency"]).upper() != "KRW"
        )
        if broker_qty is None and has_complete_zero_snapshot:
            broker_qty = 0.0
        program_qty = float(ledger_row["quantity"] if ledger_row else item["program_qty"])
        tolerance_qty = float(item["tolerance_qty"])
        if broker_qty is None:
            status = "api_required"
            status_label = "API 필요"
            delta_qty = "-"
            detail = errors.get(broker_id, "브로커 포지션 조회 결과가 아직 없습니다.")
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
                "broker_value_display": format_money(broker_row.get("broker_value"), str(item["currency"])) if broker_row and safe_float(broker_row.get("broker_value"), 0.0) > 0 else "평가 대기",
                "average_price_display": format_money(broker_row.get("average_price"), str(item["currency"])) if broker_row and safe_float(broker_row.get("average_price"), 0.0) > 0 else "-",
                "current_price_display": format_money(broker_row.get("current_price"), str(item["currency"])) if broker_row and safe_float(broker_row.get("current_price"), 0.0) > 0 else "-",
                "delta_qty": delta_qty,
                "status": status,
                "status_label": status_label,
                "program_source": str(ledger_row.get("source") if ledger_row else "sample"),
                "detail": detail,
            }
        )
    for key, ledger_row in sorted(ledger_rows.items()):
        broker_row = broker_rows.pop(key, None)
        broker_qty = broker_row.get("broker_qty") if broker_row else None
        if broker_qty is None and str(ledger_row.get("broker_id")) in successful_brokers:
            broker_qty = 0.0
        program_qty = float(ledger_row.get("quantity") or 0.0)
        if broker_qty is None:
            status = "api_required"
            status_label = "API 필요"
            delta_qty = "-"
            detail = errors.get(str(ledger_row.get("broker_id")), "브로커 포지션 조회 결과가 아직 없습니다.")
        else:
            numeric_broker_qty = float(broker_qty)
            delta = program_qty - numeric_broker_qty
            delta_qty = format_quantity(delta)
            status = "pass" if abs(delta) <= 0.000001 else "mismatch"
            status_label = "일치" if status == "pass" else "불일치"
            detail = "프로그램 원장 포지션과 브로커 포지션을 대조했습니다."
        rows.append(
            {
                "symbol": str(ledger_row.get("symbol")),
                "asset": str(ledger_row.get("asset") or ""),
                "broker_id": str(ledger_row.get("broker_id")),
                "broker_name": str(ledger_row.get("broker_id")),
                "currency": str(ledger_row.get("currency") or ""),
                "program_qty": format_quantity(program_qty),
                "broker_qty": format_quantity(float(broker_qty)) if broker_qty is not None else "API 필요",
                "broker_value_display": format_money(broker_row.get("broker_value"), str(ledger_row.get("currency") or "")) if broker_row and safe_float(broker_row.get("broker_value"), 0.0) > 0 else "평가 대기",
                "average_price_display": format_money(broker_row.get("average_price"), str(ledger_row.get("currency") or "")) if broker_row and safe_float(broker_row.get("average_price"), 0.0) > 0 else "-",
                "current_price_display": format_money(broker_row.get("current_price"), str(ledger_row.get("currency") or "")) if broker_row and safe_float(broker_row.get("current_price"), 0.0) > 0 else "-",
                "delta_qty": delta_qty,
                "status": status,
                "status_label": status_label,
                "program_source": str(ledger_row.get("source") or "program_ledger"),
                "detail": detail,
            }
        )
    for (_, _), broker_row in sorted(broker_rows.items()):
        broker_qty = float(broker_row.get("broker_qty") or 0.0)
        if broker_qty <= 0:
            continue
        rows.append(
            {
                "symbol": str(broker_row.get("symbol")),
                "asset": str(broker_row.get("asset") or ""),
                "broker_id": str(broker_row.get("broker_id")),
                "broker_name": str(broker_row.get("broker_name") or broker_row.get("broker_id")),
                "currency": str(broker_row.get("currency") or ""),
                "program_qty": "0",
                "broker_qty": format_quantity(broker_qty),
                "broker_value_display": format_money(broker_row.get("broker_value"), str(broker_row.get("currency") or "")) if safe_float(broker_row.get("broker_value"), 0.0) > 0 else "평가 대기",
                "average_price_display": format_money(broker_row.get("average_price"), str(broker_row.get("currency") or "")) if safe_float(broker_row.get("average_price"), 0.0) > 0 else "-",
                "current_price_display": format_money(broker_row.get("current_price"), str(broker_row.get("currency") or "")) if safe_float(broker_row.get("current_price"), 0.0) > 0 else "-",
                "delta_qty": format_quantity(-broker_qty),
                "status": "mismatch",
                "status_label": "불일치",
                "program_source": "missing",
                "detail": f"브로커에는 보유 수량이 있지만 프로그램 포지션 원장에는 없습니다. {broker_row.get('detail', '')}".strip(),
            }
        )
    return rows


def account_reconciliation_rows() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    live_accounts = live_account_rows()
    errors = broker_reconciliation_errors()
    ledger_accounts = program_cash_rows()
    for item in ACCOUNT_RECONCILIATION_BOOK:
        live_account = live_accounts.get(str(item["broker_id"]), {})
        ledger_account = ledger_accounts.get(str(item["broker_id"]))
        broker_cash = live_account.get("broker_cash", item["broker_cash"])
        program_cash = ledger_account.get("cash") if ledger_account else item["program_cash"]
        currency = str(live_account.get("currency") or item["currency"])
        if broker_cash is None:
            status = "api_required"
            status_label = "API 필요"
            delta_cash = "-"
            detail = errors.get(str(item["broker_id"]), str(item["detail"]))
        elif program_cash is None:
            status = "api_required"
            status_label = "원장 필요"
            delta_cash = "-"
            detail = "브로커 현금성 잔고는 조회됐지만 프로그램 현금 원장이 아직 없어 대조를 완료할 수 없습니다."
        else:
            numeric_program_cash = float(program_cash)
            numeric_broker_cash = float(broker_cash)
            delta = numeric_program_cash - numeric_broker_cash
            delta_cash = format_money(delta, currency)
            status = "pass" if abs(delta) <= 1.0 else "mismatch"
            status_label = "일치" if status == "pass" else "불일치"
            detail = str(live_account.get("detail") or item["detail"])
        rows.append(
            {
                "broker_id": str(item["broker_id"]),
                "broker_name": str(live_account.get("broker_name") or item["broker_name"]),
                "account": str(live_account.get("account") or item["account"]),
                "currency": currency,
                "program_cash": format_money(program_cash, currency) if program_cash is not None else "대조 대기",
                "broker_cash": format_money(broker_cash, currency) if broker_cash is not None else "API 필요",
                "delta_cash": delta_cash,
                "status": status,
                "status_label": status_label,
                "program_source": str(ledger_account.get("source") if ledger_account else "missing"),
                "detail": detail,
            }
        )
    return rows


def reconciliation_snapshot() -> dict[str, Any]:
    position_rows = positions()
    account_rows = account_reconciliation_rows()
    errors = STATE.get("broker_reconciliation", {}).get("errors", [])
    errors = errors if isinstance(errors, list) else []
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
            "error_count": len(errors),
        },
        "positions": position_rows,
        "accounts": account_rows,
        "errors": errors,
        "next_actions": reconciliation_next_actions(api_required_count, mismatch_count),
    }


def reconciliation_next_actions(api_required_count: int, mismatch_count: int) -> list[str]:
    actions: list[str] = []
    if api_required_count:
        actions.append("브로커 계좌·포지션 조회 또는 프로그램 현금 원장 연결")
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
    if str(order.get("oms_status") or "").upper() in {"SUBMITTING", "ACKNOWLEDGED", "PARTIALLY_FILLED", "CANCEL_PENDING", "UNKNOWN"}:
        return False
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


def active_watchdog_broker_ids() -> set[str]:
    broker_ids: set[str] = set()
    stock = STATE["automation"]["stock"]
    crypto = STATE["automation"]["crypto"]
    if stock.get("enabled") or str(stock.get("mode", "MONITOR")).upper() != "MONITOR":
        broker_ids.add("kis")
    if crypto.get("enabled") or str(crypto.get("mode", "MONITOR")).upper() != "MONITOR":
        broker_ids.add(str(crypto.get("provider") or "binance"))
    if current_mode() != "MONITOR":
        broker_ids.update({"kis", str(crypto.get("provider") or "binance")})
    return {broker_id for broker_id in broker_ids if broker_id}


def live_exposure_active() -> bool:
    if current_mode() != "MONITOR":
        return True
    return any(
        bool(profile.get("enabled")) or str(profile.get("mode", "MONITOR")).upper() != "MONITOR"
        for profile in STATE["automation"].values()
    )


def recent_order_count(seconds: int, now: datetime | None = None) -> int:
    current = now or datetime.now()
    cutoff = current - timedelta(seconds=max(1, seconds))
    count = 0
    for order in STATE["orders"]:
        occurred_at = parse_state_datetime(order.get("created_at") or order.get("time"))
        if occurred_at and occurred_at >= cutoff:
            count += 1
    return count


def watchdog_check(label: str, status: CheckStatus, detail: str, value: object = "-") -> dict[str, str]:
    return {"label": label, "status": status, "detail": detail, "value": str(value)}


def watchdog_snapshot(
    brokers: list[dict[str, object]] | None = None,
    reconciliation_summary: dict[str, Any] | None = None,
    queue: dict[str, int] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    current = now or datetime.now()
    settings = STATE["watchdog"].get("settings", DEFAULT_WATCHDOG_SETTINGS)
    brokers = brokers if brokers is not None else [broker.to_dict() for broker in broker_readiness()]
    reconciliation_summary = reconciliation_summary if reconciliation_summary is not None else reconciliation_snapshot()["summary"]
    queue = queue if queue is not None else order_queue_summary()
    active_brokers = active_watchdog_broker_ids()
    active_live = live_exposure_active()

    checks: list[dict[str, str]] = []
    heartbeat_timeout = int(float(settings.get("heartbeat_timeout_sec", DEFAULT_WATCHDOG_SETTINGS["heartbeat_timeout_sec"])))
    heartbeat_age = seconds_since(STATE["watchdog"].get("last_run"), current)
    if heartbeat_age is None:
        heartbeat_status: CheckStatus = "fail" if active_live else "warn"
        heartbeat_detail = "Watchdog 점검 이력이 없습니다. 자동화 전 첫 점검이 필요합니다."
        heartbeat_value = "미실행"
    elif heartbeat_age > heartbeat_timeout:
        heartbeat_status = "fail" if active_live else "warn"
        heartbeat_detail = f"마지막 Watchdog 점검이 {heartbeat_age}초 전입니다. 허용 {heartbeat_timeout}초를 넘었습니다."
        heartbeat_value = f"{heartbeat_age}초"
    else:
        heartbeat_status = "pass"
        heartbeat_detail = f"Watchdog heartbeat가 {heartbeat_timeout}초 한도 안에 있습니다."
        heartbeat_value = f"{heartbeat_age}초"
    checks.append(watchdog_check("Watchdog heartbeat", heartbeat_status, heartbeat_detail, heartbeat_value))

    stale_limit = int(float(settings.get("market_data_stale_sec", DEFAULT_WATCHDOG_SETTINGS["market_data_stale_sec"])))
    runner_age = seconds_since(STATE["strategy_runner"].get("last_run"), current)
    if active_live and runner_age is None:
        data_status: CheckStatus = "fail"
        data_detail = "자동화가 활성화됐지만 최근 전략/시장 데이터 점검 시간이 없습니다."
        data_value = "미실행"
    elif active_live and runner_age is not None and runner_age > stale_limit:
        data_status = "fail"
        data_detail = f"최근 전략/시장 데이터가 {runner_age}초 전입니다. 허용 {stale_limit}초를 넘었습니다."
        data_value = f"{runner_age}초"
    elif runner_age is None:
        data_status = "warn"
        data_detail = "아직 전략 사이클이 실행되지 않았습니다. 자동화 전 신호 경로를 점검하세요."
        data_value = "미실행"
    else:
        data_status = "pass"
        data_detail = "최근 전략/시장 데이터 점검 시간이 허용 범위 안에 있습니다."
        data_value = f"{runner_age}초"
    checks.append(watchdog_check("시장 데이터 신선도", data_status, data_detail, data_value))

    broker_map = {str(broker.get("broker_id")): broker for broker in brokers}
    if active_brokers:
        blocked = [broker_map.get(broker_id, {"broker_id": broker_id, "order_ready": False}) for broker_id in sorted(active_brokers)]
        unavailable = [broker for broker in blocked if not broker.get("order_ready")]
        broker_status: CheckStatus = "fail" if unavailable else "pass"
        broker_detail = (
            f"활성 라우트 브로커 준비 실패: {', '.join(str(broker.get('broker_id')) for broker in unavailable)}"
            if unavailable
            else "활성 자동화 브로커가 주문 게이트 기준으로 준비되어 있습니다."
        )
        broker_value = f"{len(blocked) - len(unavailable)}/{len(blocked)}"
    else:
        unavailable = [broker for broker in brokers if not broker.get("order_ready")]
        broker_status = "warn" if unavailable else "pass"
        broker_detail = "활성 자동화 라우트는 없지만 준비되지 않은 브로커가 있습니다." if unavailable else "비활성 상태이며 브로커 준비 경고가 없습니다."
        broker_value = "비활성"
    checks.append(watchdog_check("브로커/API 상태", broker_status, broker_detail, broker_value))

    recent_limit = int(float(settings.get("max_recent_orders_per_min", DEFAULT_WATCHDOG_SETTINGS["max_recent_orders_per_min"])))
    recent_count = recent_order_count(60, current)
    recent_status: CheckStatus = "fail" if recent_count > recent_limit else "pass"
    recent_detail = (
        f"최근 60초 주문 의도 {recent_count}건이 한도 {recent_limit}건을 초과했습니다."
        if recent_status == "fail"
        else f"최근 60초 주문 의도 {recent_count}건이 한도 안에 있습니다."
    )
    checks.append(watchdog_check("과도 주문 감시", recent_status, recent_detail, f"{recent_count}/{recent_limit}"))

    retry_limit = int(float(settings.get("max_retryable_orders", DEFAULT_WATCHDOG_SETTINGS["max_retryable_orders"])))
    retryable = int(queue.get("retryable", 0))
    retry_status: CheckStatus = "fail" if retryable > retry_limit else "warn" if retryable else "pass"
    retry_detail = (
        f"재시도 가능 주문 {retryable}건이 한도 {retry_limit}건을 초과했습니다."
        if retry_status == "fail"
        else f"재시도 가능 주문 {retryable}건입니다."
    )
    checks.append(watchdog_check("재시도 큐", retry_status, retry_detail, f"{retryable}/{retry_limit}"))

    blocked_limit = int(float(settings.get("max_blocked_orders", DEFAULT_WATCHDOG_SETTINGS["max_blocked_orders"])))
    blocked = int(queue.get("blocked", 0))
    blocked_status: CheckStatus = "fail" if blocked > blocked_limit else "warn" if blocked else "pass"
    blocked_detail = (
        f"차단 주문 {blocked}건이 한도 {blocked_limit}건을 초과했습니다."
        if blocked_status == "fail"
        else f"차단 주문 {blocked}건입니다."
    )
    checks.append(watchdog_check("차단 주문 누적", blocked_status, blocked_detail, f"{blocked}/{blocked_limit}"))

    reconcile_status = str(reconciliation_summary.get("status", "warn"))
    if reconcile_status == "pass":
        reconcile_check_status: CheckStatus = "pass"
    elif reconcile_status == "fail":
        reconcile_check_status = "fail"
    else:
        reconcile_check_status = "warn"
    checks.append(
        watchdog_check(
            "포지션·계좌 대조",
            reconcile_check_status,
            f"대조 상태는 {reconciliation_summary.get('status_label', '확인 필요')}입니다.",
            reconciliation_summary.get("last_run", "미실행"),
        )
    )

    event_state = execution_event_snapshot()
    event_age = seconds_since(event_state.get("last_poll"), current)
    event_errors = event_state.get("errors", [])
    if active_live and event_age is None:
        event_status: CheckStatus = "fail"
        event_detail = "자동화가 활성화됐지만 체결/계좌 이벤트 동기화 이력이 없습니다."
        event_value = "미실행"
    elif active_live and event_age is not None and event_age > heartbeat_timeout:
        event_status = "fail"
        event_detail = f"마지막 체결 이벤트 동기화가 {event_age}초 전입니다. 허용 {heartbeat_timeout}초를 넘었습니다."
        event_value = f"{event_age}초"
    elif event_errors:
        event_status = "warn"
        event_detail = f"체결 이벤트 어댑터 확인 필요 {len(event_errors)}건"
        event_value = f"{len(event_errors)} 오류"
    else:
        event_status = "pass"
        event_detail = "체결/계좌 이벤트 동기화 경계가 정상입니다."
        event_value = f"{event_age}초" if event_age is not None else "대기"
    checks.append(watchdog_check("체결 이벤트 동기화", event_status, event_detail, event_value))

    critical = [check for check in checks if check["status"] == "fail"]
    warnings = [check for check in checks if check["status"] == "warn"]
    status: CheckStatus = "fail" if critical else "warn" if warnings else "pass"
    status_label = "차단" if status == "fail" else "주의" if status == "warn" else "정상"
    next_actions = [check["label"] for check in critical[:5]] or [check["label"] for check in warnings[:5]] or ["Watchdog 정상"]
    return {
        "last_run": STATE["watchdog"].get("last_run", "미실행"),
        "status": status,
        "status_label": status_label,
        "last_action": STATE["watchdog"].get("last_action", "대기"),
        "last_trip": STATE["watchdog"].get("last_trip", "-"),
        "trip_count": int(STATE["watchdog"].get("trip_count", 0)),
        "active_brokers": sorted(active_brokers),
        "active_live": active_live,
        "critical_count": len(critical),
        "warning_count": len(warnings),
        "checks": checks,
        "next_actions": next_actions,
    }


def apply_watchdog_fail_closed(report: dict[str, Any]) -> bool:
    if int(report.get("critical_count", 0)) <= 0:
        return False

    changed = False
    if STATE["new_entries_blocked"] is not True:
        STATE["new_entries_blocked"] = True
        changed = True
    if current_mode() != "MONITOR":
        STATE["mode"] = "MONITOR"
        changed = True
    for profile_id, profile in STATE["automation"].items():
        if profile.get("enabled") or str(profile.get("mode", "MONITOR")).upper() != "MONITOR":
            profile["enabled"] = False
            profile["mode"] = "MONITOR"
            profile["last_action"] = "Watchdog fail-closed로 MONITOR 전환"
            changed = True

    if changed:
        STATE["watchdog"]["trip_count"] = int(STATE["watchdog"].get("trip_count", 0)) + 1
        STATE["watchdog"]["last_trip"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        append_audit(
            "danger",
            "Watchdog Fail Closed",
            f"critical {report['critical_count']}개: {', '.join(report['next_actions'][:4])}. MONITOR와 신규 진입 차단을 적용했습니다.",
        )
    return changed


def run_watchdog(include_snapshot: bool = True) -> dict[str, Any]:
    brokers = [broker.to_dict() for broker in broker_readiness()]
    reconciliation = reconciliation_snapshot()
    queue = order_queue_summary()
    previous_status = str(STATE["watchdog"].get("status", "idle"))
    now = datetime.now()
    STATE["watchdog"]["last_run"] = now.strftime("%Y-%m-%d %H:%M:%S")
    report = watchdog_snapshot(brokers, reconciliation["summary"], queue, now=now)
    STATE["watchdog"]["status"] = report["status"]
    STATE["watchdog"]["status_label"] = report["status_label"]
    STATE["watchdog"]["checks"] = report["checks"]
    STATE["watchdog"]["last_action"] = (
        f"critical {report['critical_count']} / warning {report['warning_count']}"
        if report["critical_count"] or report["warning_count"]
        else "정상"
    )
    changed = apply_watchdog_fail_closed(report)
    if previous_status != report["status"] and not changed:
        level = "danger" if report["status"] == "fail" else "warn" if report["status"] == "warn" else "info"
        append_audit(level, "Watchdog", f"{report['status_label']}: {STATE['watchdog']['last_action']}")
    payload = {"ok": report["critical_count"] == 0, "watchdog": watchdog_snapshot(brokers, reconciliation["summary"], queue)}
    if include_snapshot:
        payload["snapshot"] = snapshot()
    return payload


def operation_report(
    reconciliation: dict[str, Any],
    checks: list[Check],
    preflight: list[dict[str, str]],
    watchdog: dict[str, Any] | None = None,
) -> dict[str, Any]:
    blocker_count = sum(1 for check in checks if check.status == "fail")
    warning_count = sum(1 for check in checks if check.status == "warn")
    hard_stop_count = sum(1 for check in preflight if check["status"] == "fail")
    queue = order_queue_summary()
    watchdog = watchdog or watchdog_snapshot(queue=queue, reconciliation_summary=reconciliation["summary"])
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
            {
                "label": "Watchdog",
                "status": str(watchdog["status"]),
                "value": f"critical {watchdog['critical_count']} / warn {watchdog['warning_count']}",
                "detail": str(watchdog["last_action"]),
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
    full_live_ready_count = sum(1 for strategy in strategies if strategy.get("live_eligible") is True)
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
            "detail": f"Live-Small 이상 {live_ready_count}개 · Full Live {full_live_ready_count}개",
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
    portfolios = portfolio_rows()
    strategies = strategy_rows(portfolios)
    automations = automation_profiles(strategies, brokers)
    reconciliation = reconciliation_snapshot()
    queue = order_queue_summary()
    watchdog = watchdog_snapshot(brokers, reconciliation["summary"], queue)
    checks = readiness_checks(strategies, brokers, reconciliation["summary"])
    preflight = final_preflight_checks(strategies, brokers, reconciliation)
    report = operation_report(reconciliation, checks, preflight, watchdog)
    launch = launch_report(preflight)
    blockers = [check for check in checks if check.status == "fail"]
    warnings = [check for check in checks if check.status == "warn"]
    blocker_count = len(blockers) + int(watchdog["critical_count"])
    warning_count = len(warnings) + int(watchdog["warning_count"])
    operational = runtime_operational_readiness(strategies, portfolios)
    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode": STATE["mode"],
        "dry_run": STATE["dry_run"],
        "kill_switch": STATE["kill_switch"],
        "new_entries_blocked": STATE["new_entries_blocked"],
        "operator_confirmed": STATE["operator_confirmed"],
        "central_control": durable_control_snapshot(),
        "summary": {
            "status": "blocked" if blocker_count else ("watch" if warning_count else "ready"),
            "blocker_count": blocker_count,
            "warning_count": warning_count,
            "live_strategy_count": sum(1 for strategy in strategies if strategy["live_allowed"]),
            "full_live_strategy_count": sum(1 for strategy in strategies if strategy.get("live_eligible")),
            "broker_ready_count": sum(1 for broker in brokers if broker["order_ready"]),
        },
        "sessions": market_sessions(),
        "readiness": [check.to_dict() for check in checks],
        "watchdog": watchdog,
        "risk_checks": risk_checks(reconciliation["summary"]),
        "risk_settings": risk_setting_rows(),
        "checklist": checklist_rows(),
        "retry_policy": retry_policy_rows(),
        "order_queue": queue,
        "brokers": brokers,
        "broker_diagnostics": broker_diagnostics(),
        "broker_adapter_contract": broker_adapter_contract(),
        "automation_profiles": automations,
        "continuous_runtime": LIVE_CONTINUOUS_CONTROLLER.snapshot(),
        "execution_streams": LIVE_EXECUTION_STREAMS.snapshot(),
        "strategy_runner": dict(STATE["strategy_runner"]),
        "strategy_sleeves": dict(STATE.get("strategy_sleeves", {})),
        "multi_strategy": {
            "enabled": True,
            "conflictPolicy": "net",
            "sleeveCount": len(STATE.get("strategy_sleeves", {})),
            "brokerOrderPolicy": "one-net-order-per-instrument",
            "shortPolicy": "explicit-margin-or-futures-only",
        },
        "strategies": strategies,
        "portfolios": portfolios,
        "strategy_plugin_sources": strategy_plugin_status(),
        "orders": order_rows(),
        "dry_run_ledger": dry_run_ledger_rows(),
        "reconciliation": reconciliation,
        "program_ledger": program_ledger_snapshot(),
        "execution_events": execution_event_snapshot(),
        "upbit_smoke_order": dict(STATE.get("upbit_smoke_order", {})),
        "execution_calibration": execution_calibration_snapshot(),
        "policy_replays": list(STATE.get("policy_replays", [])),
        "shadow_live": {"brokerSubmissionBlocked": True, "evidence": list(STATE.get("shadow_evidence", []))[:20], "count": len(STATE.get("shadow_evidence", []))},
        "runtime_recovery": dict(STATE.get("recovery_status", {})),
        "operational_readiness": operational,
        "accounts": reconciliation["accounts"],
        "positions": reconciliation["positions"],
        "operation_report": report,
        "final_preflight": preflight,
        "launch_report": launch,
        "audit": list(reversed(STATE["audit"][-30:])),
    }


def audit_level_for_common_event(level: str) -> str:
    normalized = str(level or "info").strip().lower()
    if normalized in {"danger", "error", "fail", "failed"}:
        return "ERROR"
    if normalized in {"warn", "warning"}:
        return "WARN"
    if normalized in {"critical"}:
        return "CRITICAL"
    if normalized in {"debug"}:
        return "DEBUG"
    return "INFO"


def audit_category_for_event(event: str) -> str:
    text = str(event or "")
    if any(token in text for token in ("주문", "Order", "order")):
        return "ORDER"
    if any(token in text for token in ("리스크", "Risk", "risk", "Watchdog")):
        return "RISK"
    if any(token in text for token in ("전략", "자동화", "Runner", "Strategy")):
        return "STRATEGY"
    if any(token in text for token in ("설정", "정책", "한도", "체크리스트")):
        return "SETTINGS"
    return "SYSTEM"


def persist_audit_event(record: AuditEvent) -> None:
    try:
        AUDIT_STORE.append(record)
        record_flight_event(
            app="live_trader",
            event_type=f"{record.category}:{record.source}",
            level=record.level,
            message=record.message,
            payload={
                "eventId": record.event_id,
                "decision": record.decision,
                "state": record.state,
                "strategyId": record.strategy_id,
                "symbol": record.symbol,
                "orderId": record.order_id,
                "reason": record.reason,
            },
        )
    except Exception as exc:  # pragma: no cover - persistence must never block trading.
        errors = STATE.setdefault("audit_persist_errors", [])
        errors.append(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {type(exc).__name__}: {exc}")
        if len(errors) > 10:
            del errors[: len(errors) - 10]


def append_audit(level: str, event: str, detail: str, *, audit_record: AuditEvent | None = None) -> None:
    now = datetime.now()
    STATE["audit"].append({
        "time": now.strftime("%H:%M:%S"),
        "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
        "level": level,
        "event": event,
        "detail": detail,
    })
    if len(STATE["audit"]) > AUDIT_LOG_LIMIT:
        del STATE["audit"][: len(STATE["audit"]) - AUDIT_LOG_LIMIT]
    persist_audit_event(
        audit_record
        or build_audit_event(
            app="live_trader",
            category=audit_category_for_event(event),  # type: ignore[arg-type]
            level=audit_level_for_common_event(level),  # type: ignore[arg-type]
            source=event,
            message=detail,
            occurred_at=now,
            scope=current_mode(),
        )
    )


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
    watchdog_critical = int(data.get("watchdog", {}).get("critical_count", 0))
    normalized = mode if mode in {"MONITOR", "SMALL_LIVE", "FULL_LIVE"} else "MONITOR"
    if normalized != "MONITOR" and watchdog_critical:
        reason = f"Watchdog critical {watchdog_critical}개 때문에 {normalized} 전환이 차단되었습니다."
        append_audit("danger", "모드 전환 차단", reason)
        return {"ok": False, "reason": reason, "snapshot": snapshot()}
    if normalized != "MONITOR" and blockers:
        append_audit("danger", "모드 전환 차단", f"{normalized} 전환은 readiness blocker {blockers}개 때문에 차단되었습니다.")
        return {"ok": False, "reason": f"readiness blocker {blockers}개가 남아 있습니다.", "snapshot": snapshot()}
    if normalized == "FULL_LIVE" and data["summary"]["warning_count"]:
        append_audit("warn", "FULL LIVE 차단", "경고 항목이 남아 있어 FULL LIVE 전환을 차단했습니다.")
        return {"ok": False, "reason": "FULL LIVE는 경고 0개일 때만 허용됩니다.", "snapshot": snapshot()}
    STATE["mode"] = normalized
    append_audit("info", "모드 전환", f"운용 모드가 {normalized}(으)로 변경되었습니다.")
    return {"ok": True, "reason": "mode changed", "snapshot": snapshot()}


def set_flag(name: str, value: bool, *, confirmed: bool = False) -> dict[str, Any]:
    if name not in {"kill_switch", "new_entries_blocked", "operator_confirmed", "dry_run"}:
        return {"ok": False, "reason": "unknown flag", "snapshot": snapshot()}
    risky_release = (
        (name == "kill_switch" and STATE.get(name) is True and not value)
        or (name == "new_entries_blocked" and STATE.get(name) is True and not value)
        or (name == "dry_run" and STATE.get(name) is True and not value)
    )
    if risky_release and not confirmed:
        label = {
            "kill_switch": "Kill Switch 해제",
            "new_entries_blocked": "신규 진입 차단 해제",
            "dry_run": "Dry Run 보호 해제",
        }[name]
        reason = f"{label}는 명시 확인이 필요합니다."
        append_audit("warn", f"{label} 거부", reason)
        return {"ok": False, "reason": reason, "snapshot": snapshot()}
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


def raw_portfolio_payload(strategy_dir: Path, portfolio_id: str) -> dict[str, Any] | None:
    if not portfolio_id:
        return None
    folder = strategy_dir / "portfolios"
    if not folder.exists():
        return None
    for path in folder.glob("*.json"):
        if path.name in {"portfolio-registry.json", "package.json", "package-lock.json"}:
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict) and str(payload.get("id") or payload.get("portfolioId") or path.stem) == portfolio_id:
            return payload
    return None


def ensure_live_deployment(
    strategy_dir: Path,
    strategy_payload: dict[str, Any],
    normalized: dict[str, Any],
) -> tuple[DeploymentStore, dict[str, Any], dict[str, Any] | None]:
    store = DeploymentStore(strategy_dir)
    portfolio_id = str((normalized.get("paper_portfolio_evidence") or {}).get("portfolioId") or "")
    deployment_id = str(normalized.get("deployment_id") or f"dep:{normalized.get('strategy_id')}:{portfolio_id or 'standalone'}:live")
    current = store.get(deployment_id)
    portfolio_payload = raw_portfolio_payload(strategy_dir, portfolio_id)
    if current is not None:
        return store, current, portfolio_payload

    definition = store.create_definition(
        deployment_id=deployment_id,
        strategy_artifact=strategy_payload,
        portfolio_artifact=portfolio_payload,
        account_id="live-account-unresolved",
        environment="SMALL_LIVE",
        symbol=str(normalized.get("symbol") or "UNKNOWN"),
        instrument_id=str(normalized.get("instrument_id") or ""),
        route="crypto" if str(normalized.get("asset") or "").upper() == "CRYPTO" else "stock",
    )
    lifecycle = normalize_lifecycle_status(normalized.get("lifecycle_status") or "draft")
    if lifecycle != "draft":
        current = store.transition(
            definition["deploymentId"],
            lifecycle=lifecycle,
            mode="MONITOR",
            permissions=dict(normalized.get("permissions") or {}),
            actor="live_trader-migration",
            reason="legacy strategy lifecycle seeded into live deployment",
        )
    else:
        current = store.get(definition["deploymentId"])
    return store, current or {}, portfolio_payload


def promote_strategy_to_live(strategy_id: str) -> dict[str, Any]:
    strategy_id = str(strategy_id or "").strip()
    if not strategy_id:
        return {"ok": False, "reason": "strategy_id is required", "snapshot": snapshot()}

    strategy_dir, artifact_path, payload, normalized = find_strategy_artifact_payload(strategy_id)
    if strategy_dir is None or artifact_path is None or payload is None or normalized is None:
        reason = f"전략 artifact를 찾을 수 없습니다: {strategy_id}"
        append_audit("danger", "Live 승급 차단", reason)
        return {"ok": False, "reason": reason, "snapshot": snapshot()}

    lineage_blockers = (normalized.get("lineage") or {}).get("blockingIssues") if isinstance(normalized.get("lineage"), dict) else []
    if lineage_blockers:
        reason = f"Professional Flow lineage 무결성 실패: {', '.join(str(item) for item in lineage_blockers)}"
        append_audit("danger", "Live 승급 차단", reason)
        return {"ok": False, "reason": reason, "snapshot": snapshot()}

    deployment_store, deployment, portfolio_payload = ensure_live_deployment(strategy_dir, payload, normalized)
    current_status = normalize_lifecycle_status(deployment.get("lifecycle") or normalized.get("lifecycle_status"))
    if current_status == "live":
        return {"ok": True, "reason": "이미 live 상태입니다.", "snapshot": snapshot()}
    if current_status != "before-live-small":
        reason = f"before-live-small 상태에서만 live로 승급할 수 있습니다. 현재 상태: {current_status}"
        append_audit("danger", "Live 승급 차단", reason)
        return {"ok": False, "reason": reason, "snapshot": snapshot()}
    deployment_permissions = dict(deployment.get("permissions") or {})
    if deployment_permissions.get("live_small_eligible") is not True and normalized.get("live_small_eligible") is not True:
        reason = "live_small_eligible=true 전략만 소액 실거래 후 live로 승급할 수 있습니다."
        append_audit("danger", "Live 승급 차단", reason)
        return {"ok": False, "reason": reason, "snapshot": snapshot()}

    execution = live_small_execution_summary(strategy_id)
    if execution["successful"] <= 0:
        reason = "소액 실거래 성공 주문이 아직 없습니다. SMALL_LIVE에서 1건 이상 실제 전송/체결을 확인해야 합니다."
        append_audit("warn", "Live 승급 대기", reason)
        return {"ok": False, "reason": reason, "snapshot": snapshot()}
    if execution["blocked"] > 0:
        reason = f"소액 실거래 중 차단/실패 주문 {execution['blocked']}건이 있어 live 승급을 막았습니다."
        append_audit("danger", "Live 승급 차단", reason)
        return {"ok": False, "reason": reason, "snapshot": snapshot()}

    data = snapshot()
    if not data.get("operator_confirmed"):
        reason = "운용자 확인을 먼저 완료해야 live 승급이 가능합니다."
        append_audit("warn", "Live 승급 대기", reason)
        return {"ok": False, "reason": reason, "snapshot": data}
    if int(data.get("summary", {}).get("blocker_count", 0) or 0) > 0:
        reason = f"readiness blocker {data['summary']['blocker_count']}개가 남아 있어 live 승급을 차단했습니다."
        append_audit("danger", "Live 승급 차단", reason)
        return {"ok": False, "reason": reason, "snapshot": data}
    fail_reasons = [
        reason
        for reason in (deployment.get("permissions") or normalized.get("permissions") or {}).get("fail_reasons", []) or []
        if str(reason) not in {"live-activation-required", "before-live-small 승급 후 소액 실거래 확인이 필요합니다."}
    ]
    permissions = dict(deployment.get("permissions") or normalized.get("permissions") or {})
    permissions.update({
        "paper_trader_verified": True,
        "live_small_eligible": True,
        "live_eligible": True,
        "live_allowed": True,
        "fail_reasons": fail_reasons,
    })
    now = datetime.now().astimezone().replace(microsecond=0).isoformat()
    evidence_id = f"live-{strategy_id}-{datetime.now().strftime('%Y%m%dT%H%M%S%f')}"
    paper_evidence = normalized.get("paper_portfolio_evidence") if isinstance(normalized.get("paper_portfolio_evidence"), dict) else {}
    live_evidence = build_live_execution_evidence(
        evidence_id=evidence_id,
        strategy_artifact=payload,
        portfolio_artifact=portfolio_payload,
        deployment_id=deployment["deploymentId"],
        environment="SMALL_LIVE",
        mode="SMALL_LIVE",
        runtime_version="live-trader-v1",
        ended_at=now,
        successful_orders=execution["successful"],
        blocked_orders=execution["blocked"],
        details={
            "operatorConfirmed": True,
            "readinessBlockers": 0,
            "lineageManifest": build_lineage_manifest(
                stage="live",
                producer="live_trader",
                created_at=now,
                inputs={
                    "strategyArtifactHash": artifact_content_hash(payload),
                    "paperEvidenceHash": str(paper_evidence.get("evidenceHash") or normalized.get("permissions", {}).get("paperEvidenceHash") or "legacy-paper-evidence"),
                    "runtimeVersion": "live-trader-v1",
                    "brokerRoute": strategy_broker_id(normalized),
                },
                policies={"environment": "SMALL_LIVE", "mode": "SMALL_LIVE", "deploymentRevision": deployment.get("revision")},
                parent={"stage": "paper", "contentHash": normalized.get("lineage", {}).get("paper", {}).get("contentHash") or "legacy"},
            ),
        },
    )
    live_record = EvidenceStore(strategy_dir).save_live(live_evidence)
    permissions.update(
        {
            "liveEvidenceId": evidence_id,
            "liveEvidenceHash": live_evidence["integrity"]["contentHash"],
        }
    )
    deployment_store.transition(
        deployment["deploymentId"],
        lifecycle="live",
        mode=str(STATE.get("mode") or "SMALL_LIVE"),
        permissions=permissions,
        actor="live_trader",
        reason=f"SMALL_LIVE 성공 주문 {execution['successful']}건과 readiness gate 통과 / evidence={live_record.path.name}",
    )
    append_audit(
        "warn",
        "Live 승급",
        f"{payload.get('name') or strategy_id} 전략을 live 상태로 승급했습니다. 성공 주문 {execution['successful']}건.",
    )
    return {"ok": True, "reason": "live 승급 완료", "snapshot": snapshot()}


def set_strategy_lifecycle_status(strategy_id: str, action: str) -> dict[str, Any]:
    strategy_id = str(strategy_id or "").strip()
    action = str(action or "").strip().lower()
    if not strategy_id:
        return {"ok": False, "reason": "strategy_id is required", "snapshot": snapshot()}
    if action not in {"pause", "retire", "resume"}:
        return {"ok": False, "reason": "action must be pause, retire, or resume", "snapshot": snapshot()}

    strategy_dir, artifact_path, payload, normalized = find_strategy_artifact_payload(strategy_id)
    if strategy_dir is None or artifact_path is None or payload is None or normalized is None:
        reason = f"전략 artifact를 찾을 수 없습니다: {strategy_id}"
        append_audit("danger", "전략 lifecycle 변경 차단", reason)
        return {"ok": False, "reason": reason, "snapshot": snapshot()}

    deployment_store, deployment, _portfolio_payload = ensure_live_deployment(strategy_dir, payload, normalized)
    current_status = normalize_lifecycle_status(deployment.get("lifecycle") or normalized.get("lifecycle_status"))
    permissions = dict(deployment.get("permissions") or normalized.get("permissions") or {})

    if action == "resume":
        if current_status != "paused":
            return {"ok": False, "reason": "paused 상태에서만 재개할 수 있습니다.", "snapshot": snapshot()}
        target_status = normalize_lifecycle_status(permissions.get("pausedFrom") or "before-live-small")
        if target_status in {"paused", "retired", "unknown"}:
            target_status = "before-live-small"
        paused_permissions = permissions.get("pausedPermissions") if isinstance(permissions.get("pausedPermissions"), dict) else {}
        if paused_permissions:
            paused_from = permissions.get("pausedFrom")
            permissions = dict(paused_permissions)
            permissions["pausedFrom"] = paused_from
        permissions["fail_reasons"] = [
            reason
            for reason in permissions.get("fail_reasons", [])
            if str(reason) not in {"lifecycle-paused", "lifecycle-retired"}
        ]
        audit_level = "info"
        audit_label = "전략 재개"
        audit_reason = f"{payload.get('name') or strategy_id} 전략을 {target_status} 단계로 재개했습니다."
    else:
        target_status = "paused" if action == "pause" else "retired"
        if action == "pause":
            permissions["pausedFrom"] = current_status
            permissions["pausedPermissions"] = dict(permissions)
        fail_reasons = list(permissions.get("fail_reasons", payload.get("fail_reasons", [])) or [])
        fail_reasons.append(f"lifecycle-{target_status}")
        fail_reasons = list(dict.fromkeys(str(reason) for reason in fail_reasons))
        permissions.update({
            "live_small_eligible": False,
            "live_eligible": False,
            "live_allowed": False,
            "fail_reasons": fail_reasons,
        })
        audit_level = "warn" if action == "pause" else "danger"
        audit_label = "전략 일시중지" if action == "pause" else "전략 폐기/보관"
        audit_reason = f"{payload.get('name') or strategy_id} 전략을 {target_status} 상태로 변경하고 실거래 권한을 차단했습니다."

    deployment_store.transition(
        deployment["deploymentId"],
        lifecycle=target_status,
        mode="MONITOR" if target_status in {"paused", "retired"} else str(STATE.get("mode") or "MONITOR"),
        permissions=permissions,
        actor="live_trader",
        reason=audit_reason,
    )
    append_audit(audit_level, audit_label, audit_reason)
    return {"ok": True, "reason": audit_reason, "snapshot": snapshot()}


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
        watchdog_critical = int(data.get("watchdog", {}).get("critical_count", 0))
        if watchdog_critical:
            reason = f"Watchdog critical {watchdog_critical}개 때문에 {next_mode} 전환이 차단되었습니다."
            STATE["automation"][profile_id]["last_action"] = reason
            append_audit("danger", "자동화 시작 차단", f"{profile['title']}: {reason}")
            return {"ok": False, "reason": reason, "snapshot": snapshot()}
        if data["summary"]["blocker_count"]:
            reason = f"readiness blocker {data['summary']['blocker_count']}개 때문에 {next_mode} 전환이 차단되었습니다."
            STATE["automation"][profile_id]["last_action"] = reason
            append_audit("danger", "자동화 시작 차단", f"{profile['title']}: {reason}")
            return {"ok": False, "reason": reason, "snapshot": snapshot()}
        required_count = profile["full_live_strategy_count"] if next_mode == "FULL_LIVE" else profile["live_strategy_count"]
        if required_count <= 0:
            reason = (
                "live_eligible=true 전략이 없어 FULL_LIVE 전환이 차단되었습니다."
                if next_mode == "FULL_LIVE"
                else "live_small_eligible=true 전략이 없어 SMALL_LIVE 전환이 차단되었습니다."
            )
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


def refresh_broker_reconciliation() -> dict[str, Any]:
    router = LiveBrokerRouter()
    data: dict[str, Any] = {
        "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "accounts": [],
        "positions": [],
        "errors": [],
        "successful_account_brokers": [],
        "successful_position_brokers": [],
    }
    for broker_id in ("kis", "binance", "upbit"):
        try:
            account_snapshot = router.get_account_snapshot(broker_id)
            accounts = account_snapshot.get("accounts", []) if isinstance(account_snapshot, dict) else []
            if isinstance(accounts, list):
                data["accounts"].extend(accounts)
                data["successful_account_brokers"].append(broker_id)
        except (BrokerNotReadyError, RuntimeError) as exc:
            data["errors"].append({"broker_id": broker_id, "scope": "account", "detail": str(exc)})

        try:
            positions_snapshot = router.list_positions(broker_id)
            if isinstance(positions_snapshot, list):
                data["positions"].extend(positions_snapshot)
                data["successful_position_brokers"].append(broker_id)
        except (BrokerNotReadyError, RuntimeError) as exc:
            data["errors"].append({"broker_id": broker_id, "scope": "positions", "detail": str(exc)})
    STATE["broker_reconciliation"] = data
    return data


def seed_program_ledger_from_broker_snapshot(refresh_if_empty: bool = True) -> dict[str, Any]:
    broker_data = STATE.get("broker_reconciliation", {})
    accounts = broker_data.get("accounts", []) if isinstance(broker_data, dict) else []
    positions_data = broker_data.get("positions", []) if isinstance(broker_data, dict) else []
    if refresh_if_empty and not accounts and not positions_data:
        broker_data = refresh_broker_reconciliation()
        accounts = broker_data.get("accounts", [])
        positions_data = broker_data.get("positions", [])
    if not isinstance(accounts, list):
        accounts = []
    if not isinstance(positions_data, list):
        positions_data = []
    if not accounts and not positions_data:
        return {
            "ok": False,
            "reason": "브로커 스냅샷이 없어 프로그램 원장 기준을 만들 수 없습니다.",
            "program_ledger": program_ledger_snapshot(),
            "snapshot": snapshot(),
        }
    result = PROGRAM_LEDGER.seed_from_broker_snapshot(accounts, positions_data, source="broker_snapshot")
    STATE["program_ledger"]["last_baseline"] = result["updated_at"]
    STATE["program_ledger"]["last_baseline_source"] = "broker_snapshot"
    reconciliation = reconciliation_snapshot()
    append_audit(
        "warn",
        "프로그램 원장 기준 저장",
        f"브로커 스냅샷 기준으로 현금 {result['cash_count']}개, 포지션 {result['position_count']}개를 원장에 저장했습니다.",
    )
    return {
        "ok": True,
        "reason": f"프로그램 원장 기준 저장 완료: 현금 {result['cash_count']}개, 포지션 {result['position_count']}개",
        "program_ledger": program_ledger_snapshot(),
        "reconciliation": reconciliation,
        "snapshot": snapshot(),
    }


def poll_execution_events(broker_id: str = "all") -> dict[str, Any]:
    broker_ids = ("kis", "binance", "upbit") if broker_id.strip().lower() in {"", "all"} else (broker_id.strip().lower(),)
    router = LiveBrokerRouter()
    events: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    snapshot_accounts: list[dict[str, Any]] = []
    snapshot_positions: list[dict[str, Any]] = []
    successful_snapshot_brokers: set[str] = set()
    for selected_broker in broker_ids:
        try:
            for row in LIVE_EXECUTION_STREAMS.drain(selected_broker):
                normalized = normalize_broker_execution(selected_broker, row)
                events.append({
                    "broker_id": selected_broker,
                    "event_id": normalized.event_id,
                    "order_id": normalized.client_order_id,
                    "broker_order_id": normalized.broker_order_id,
                    "symbol": normalized.symbol,
                    "side": str(row.get("side") or ""),
                    "quantity": normalized.filled_quantity,
                    "price": normalized.fill_price,
                    "fee": normalized.fee,
                    "state": normalized.status,
                    "occurred_at": normalized.occurred_at,
                    "instrument_id": normalized.instrument_id,
                    "raw": normalized.raw,
                })
            result = router.poll_execution_events(selected_broker)
            rows = result.get("events", []) if isinstance(result, dict) else result
            if isinstance(rows, list):
                for row in rows:
                    if isinstance(row, dict):
                        normalized = normalize_broker_execution(selected_broker, row)
                        events.append({
                            "broker_id": selected_broker,
                            "event_id": normalized.event_id,
                            "order_id": normalized.client_order_id,
                            "broker_order_id": normalized.broker_order_id,
                            "symbol": normalized.symbol,
                            "side": str(row.get("side") or ""),
                            "quantity": normalized.filled_quantity,
                            "price": normalized.fill_price,
                            "fee": normalized.fee,
                            "state": normalized.status,
                            "occurred_at": normalized.occurred_at,
                            "instrument_id": normalized.instrument_id,
                            "raw": normalized.raw,
                        })
            accounts = result.get("accounts", []) if isinstance(result, dict) else []
            positions_data = result.get("positions", []) if isinstance(result, dict) else []
            received_snapshot = False
            if isinstance(accounts, list):
                account_rows = [item for item in accounts if isinstance(item, dict)]
                snapshot_accounts.extend(account_rows)
                received_snapshot = received_snapshot or bool(account_rows)
            if isinstance(positions_data, list):
                position_rows = [item for item in positions_data if isinstance(item, dict)]
                snapshot_positions.extend(position_rows)
                received_snapshot = received_snapshot or bool(position_rows)
            if received_snapshot:
                successful_snapshot_brokers.add(selected_broker)
        except (BrokerNotReadyError, RuntimeError) as exc:
            errors.append({"broker_id": selected_broker, "detail": str(exc)})
    recorded = PROGRAM_LEDGER.record_execution_events(events)
    synced = (
        PROGRAM_LEDGER.sync_broker_snapshot(
            snapshot_accounts,
            snapshot_positions,
            sorted(successful_snapshot_brokers),
            source="event_poll",
        )
        if successful_snapshot_brokers
        else {"cash_count": 0, "position_count": 0}
    )
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    STATE["execution_events"] = {
        "last_poll": now,
        "errors": errors,
        "event_count": len(events),
        "recorded_count": recorded,
        "synced_cash_count": int(synced.get("cash_count", 0)),
        "synced_position_count": int(synced.get("position_count", 0)),
    }
    STATE["program_ledger"]["last_event_sync"] = now
    append_audit(
        "warn" if errors else "info",
        "체결 이벤트 동기화",
        (
            f"체결/계좌 이벤트 {len(events)}건 수신, {recorded}건 저장, "
            f"원장 현금 {synced.get('cash_count', 0)}개/포지션 {synced.get('position_count', 0)}개 동기화, "
            f"오류 {len(errors)}건"
        ),
    )
    return {
        "ok": not errors,
        "reason": f"체결 이벤트 동기화: 저장 {recorded}건, 오류 {len(errors)}건",
        "errors": errors,
        "program_ledger": program_ledger_snapshot(),
        "execution_events": execution_event_snapshot(),
        "snapshot": snapshot(),
    }


UPBIT_SMOKE_MARKET = "KRW-BTC"
UPBIT_SMOKE_MIN_KRW = 5_000
UPBIT_SMOKE_MAX_KRW = 10_000
UPBIT_SMOKE_PREVIEW_TTL_SECONDS = 600


def _upbit_smoke_order_view(**updates: Any) -> dict[str, Any]:
    current = dict(STATE.get("upbit_smoke_order", {}))
    current.update(updates)
    STATE["upbit_smoke_order"] = current
    return current


def preview_upbit_smoke_order(notional_krw: object = UPBIT_SMOKE_MIN_KRW) -> dict[str, Any]:
    amount = int(safe_float(notional_krw, 0.0))
    if amount < UPBIT_SMOKE_MIN_KRW or amount > UPBIT_SMOKE_MAX_KRW:
        reason = f"Upbit 점검 주문은 {UPBIT_SMOKE_MIN_KRW:,}~{UPBIT_SMOKE_MAX_KRW:,}원만 허용합니다."
        _upbit_smoke_order_view(status="blocked", status_label="차단", detail=reason, confirmation_token="")
        return {"ok": False, "reason": reason, "snapshot": snapshot()}

    router = LiveBrokerRouter()
    try:
        account_snapshot = router.get_account_snapshot("upbit")
        chance = router.get_upbit_order_chance(UPBIT_SMOKE_MARKET)
    except (BrokerNotReadyError, RuntimeError) as exc:
        reason = str(exc)
        _upbit_smoke_order_view(status="blocked", status_label="조회 실패", detail=reason, confirmation_token="")
        append_audit("danger", "Upbit 소액 주문 미리보기", reason)
        return {"ok": False, "reason": reason, "snapshot": snapshot()}

    accounts = account_snapshot.get("accounts", []) if isinstance(account_snapshot, dict) else []
    krw_account = accounts[0] if isinstance(accounts, list) and accounts and isinstance(accounts[0], dict) else {}
    available_krw = safe_float(krw_account.get("broker_cash"), 0.0)
    market_info = chance.get("market") if isinstance(chance.get("market"), dict) else {}
    bid_rules = market_info.get("bid") if isinstance(market_info.get("bid"), dict) else {}
    bid_account = chance.get("bid_account") if isinstance(chance.get("bid_account"), dict) else {}
    chance_balance = safe_float(bid_account.get("balance"), available_krw)
    if chance_balance > 0:
        available_krw = min(available_krw, chance_balance) if available_krw > 0 else chance_balance
    min_total = safe_float(bid_rules.get("min_total"), float(UPBIT_SMOKE_MIN_KRW))
    max_total = safe_float(bid_rules.get("max_total"), 0.0)
    bid_fee_rate = safe_float(chance.get("bid_fee"), 0.0)
    required_krw = amount * (1.0 + max(0.0, bid_fee_rate))
    blocked_reasons: list[str] = []
    if min_total > 0 and amount < min_total:
        blocked_reasons.append(f"거래소 최소 주문 {min_total:,.0f}원 미만")
    if max_total > 0 and amount > max_total:
        blocked_reasons.append(f"거래소 최대 주문 {max_total:,.0f}원 초과")
    if available_krw + 1e-9 < required_krw:
        blocked_reasons.append(f"수수료 포함 필요 금액 {required_krw:,.0f}원 대비 잔고 부족")

    now = datetime.now(timezone.utc)
    identifier = f"lt-smoke-{now.strftime('%Y%m%dT%H%M%S')}-{secrets.token_hex(4)}"
    order_intent = {
        "broker_id": "upbit",
        "market": UPBIT_SMOKE_MARKET,
        "symbol": UPBIT_SMOKE_MARKET,
        "side": "BUY",
        "order_type": "price",
        "notional": amount,
        "identifier": identifier,
    }
    prepared = build_upbit_order_request(order_intent)
    if not prepared.can_send:
        blocked_reasons.append("주문 요청 설정 누락: " + ", ".join(prepared.blocked_reasons))
    ready = not blocked_reasons
    confirmation_token = secrets.token_urlsafe(24) if ready else ""
    expires = now + timedelta(seconds=UPBIT_SMOKE_PREVIEW_TTL_SECONDS)
    preview = prepared.preview()
    detail = (
        f"{UPBIT_SMOKE_MARKET} 시장가 매수 {amount:,}원 · 예상 수수료율 {bid_fee_rate:.4%} · "
        f"주문 가능 KRW {available_krw:,.0f}원"
    )
    if blocked_reasons:
        detail = " · ".join(blocked_reasons)
    state_row = _upbit_smoke_order_view(
        status="ready" if ready else "blocked",
        status_label="확인 대기" if ready else "차단",
        market=UPBIT_SMOKE_MARKET,
        side="BUY",
        order_type="시장가 매수",
        notional_krw=amount,
        available_krw=available_krw,
        required_krw=required_krw,
        minimum_krw=min_total,
        maximum_krw=max_total,
        fee_rate=bid_fee_rate,
        identifier=identifier,
        confirmation_token=confirmation_token,
        expires_at=expires.isoformat().replace("+00:00", "Z"),
        expires_epoch=expires.timestamp(),
        used=False,
        broker_order_id="",
        broker_state="",
        detail=detail,
        request_preview=preview,
        blocked_reasons=blocked_reasons,
    )
    append_audit(
        "info" if ready else "danger",
        "Upbit 소액 주문 미리보기",
        detail + (" · 실제 주문 전송 없음" if ready else ""),
    )
    return {"ok": ready, "reason": detail, "preview": state_row, "snapshot": snapshot()}


def submit_upbit_smoke_order(confirmation_token: object, *, confirmed: bool) -> dict[str, Any]:
    preview = dict(STATE.get("upbit_smoke_order", {}))
    token = str(confirmation_token or "")
    if not confirmed or not token or not secrets.compare_digest(token, str(preview.get("confirmation_token") or "")):
        return {"ok": False, "reason": "정확한 주문 미리보기의 1회 확인 토큰이 필요합니다.", "snapshot": snapshot()}
    if preview.get("status") != "ready" or bool(preview.get("used")):
        return {"ok": False, "reason": "이미 사용되었거나 전송 가능한 미리보기가 아닙니다.", "snapshot": snapshot()}
    if safe_float(preview.get("expires_epoch"), 0.0) <= datetime.now(timezone.utc).timestamp():
        _upbit_smoke_order_view(status="expired", status_label="만료", confirmation_token="", detail="미리보기가 만료되었습니다. 다시 조회하세요.")
        return {"ok": False, "reason": "미리보기가 만료되었습니다. 다시 조회하세요.", "snapshot": snapshot()}
    if STATE.get("kill_switch"):
        return {"ok": False, "reason": "긴급 차단이 켜져 있어 실제 주문을 전송할 수 없습니다.", "snapshot": snapshot()}
    if not real_orders_enabled():
        return {"ok": False, "reason": "LIVE_TRADER_ENABLE_REAL_ORDERS=true가 필요합니다.", "snapshot": snapshot()}

    amount = int(safe_float(preview.get("notional_krw"), 0.0))
    if amount < UPBIT_SMOKE_MIN_KRW or amount > UPBIT_SMOKE_MAX_KRW:
        return {"ok": False, "reason": "서버의 소액 주문 하드 한도를 벗어났습니다.", "snapshot": snapshot()}

    router = LiveBrokerRouter()
    try:
        fresh_account = router.get_account_snapshot("upbit")
        accounts = fresh_account.get("accounts", []) if isinstance(fresh_account, dict) else []
        row = accounts[0] if isinstance(accounts, list) and accounts and isinstance(accounts[0], dict) else {}
        fresh_cash = safe_float(row.get("broker_cash"), 0.0)
        required_krw = safe_float(preview.get("required_krw"), float(amount))
        if fresh_cash + 1e-9 < required_krw:
            raise BrokerNotReadyError(f"전송 직전 잔고 {fresh_cash:,.0f}원이 필요 금액 {required_krw:,.0f}원보다 적습니다.")
    except (BrokerNotReadyError, RuntimeError) as exc:
        reason = str(exc)
        _upbit_smoke_order_view(status="blocked", status_label="전송 직전 차단", detail=reason, confirmation_token="")
        append_audit("danger", "Upbit 실제 소액 주문", reason)
        return {"ok": False, "reason": reason, "snapshot": snapshot()}

    # 네트워크 결과가 불명확해도 같은 identifier로 재전송하지 않도록 전송 직전에 소진 처리합니다.
    _upbit_smoke_order_view(status="submitting", status_label="전송 중", used=True, confirmation_token="")
    intent = {
        "broker_id": "upbit",
        "market": str(preview.get("market") or UPBIT_SMOKE_MARKET),
        "symbol": str(preview.get("market") or UPBIT_SMOKE_MARKET),
        "side": "BUY",
        "order_type": "price",
        "notional": amount,
        "identifier": str(preview.get("identifier") or ""),
    }
    response = router.place_order(intent)
    payload = response.get("json") if isinstance(response.get("json"), dict) else {}
    if not bool(response.get("ok")):
        safe_error = str(payload.get("error") or response.get("text") or "Upbit 주문 요청 실패")[:500]
        _upbit_smoke_order_view(status="rejected", status_label="거절", detail=safe_error, broker_response=payload)
        append_audit("danger", "Upbit 실제 소액 주문", f"거래소 거절 · {safe_error}")
        return {"ok": False, "reason": safe_error, "response": payload, "snapshot": snapshot()}

    order_uuid = str(payload.get("uuid") or "").strip()
    broker_state = str(payload.get("state") or "wait").strip().lower()
    order_detail = payload
    if order_uuid:
        try:
            order_detail = router.get_upbit_order(order_uuid)
            broker_state = str(order_detail.get("state") or broker_state).strip().lower()
        except (BrokerNotReadyError, RuntimeError):
            pass
    trades = order_detail.get("trades") if isinstance(order_detail.get("trades"), list) else []
    executed_volume = sum(safe_float(item.get("volume"), 0.0) for item in trades if isinstance(item, dict))
    executed_funds = sum(safe_float(item.get("funds"), 0.0) for item in trades if isinstance(item, dict))
    paid_fee = safe_float(order_detail.get("paid_fee"), 0.0)
    filled_after_cancel = broker_state == "cancel" and executed_funds > 0
    final_status = "filled" if broker_state == "done" or filled_after_cancel else "acknowledged"
    detail = (
        f"Upbit 주문 접수 {order_uuid or '-'} · 상태 {broker_state or '-'} · "
        f"체결금액 {executed_funds:,.0f}원 · 수수료 {paid_fee:,.2f}원"
    )
    _upbit_smoke_order_view(
        status=final_status,
        status_label="체결·잔여취소" if filled_after_cancel else "체결" if final_status == "filled" else "접수",
        broker_order_id=order_uuid,
        broker_state=broker_state,
        executed_volume=executed_volume,
        executed_funds=executed_funds,
        paid_fee=paid_fee,
        detail=detail,
        broker_response=order_detail,
    )
    append_audit("info", "Upbit 실제 소액 주문", detail)
    poll_execution_events("upbit")
    run_reconciliation()
    return {"ok": True, "reason": detail, "order": dict(STATE["upbit_smoke_order"]), "snapshot": snapshot()}


def refresh_upbit_smoke_order() -> dict[str, Any]:
    current = dict(STATE.get("upbit_smoke_order", {}))
    order_uuid = str(current.get("broker_order_id") or "").strip()
    if not order_uuid:
        return {"ok": False, "reason": "조회할 Upbit 주문 UUID가 없습니다.", "snapshot": snapshot()}
    try:
        detail = LiveBrokerRouter().get_upbit_order(order_uuid)
    except (BrokerNotReadyError, RuntimeError) as exc:
        return {"ok": False, "reason": str(exc), "snapshot": snapshot()}
    broker_state = str(detail.get("state") or "").strip().lower()
    trades = detail.get("trades") if isinstance(detail.get("trades"), list) else []
    executed_volume = sum(safe_float(item.get("volume"), 0.0) for item in trades if isinstance(item, dict))
    executed_funds = sum(safe_float(item.get("funds"), 0.0) for item in trades if isinstance(item, dict))
    paid_fee = safe_float(detail.get("paid_fee"), 0.0)
    filled_after_cancel = broker_state == "cancel" and executed_funds > 0
    final_status = "filled" if broker_state == "done" or filled_after_cancel else "acknowledged"
    detail_text = f"Upbit 주문 {order_uuid} · 상태 {broker_state or '-'} · 체결금액 {executed_funds:,.0f}원 · 수수료 {paid_fee:,.2f}원"
    _upbit_smoke_order_view(
        status=final_status,
        status_label="체결·잔여취소" if filled_after_cancel else "체결" if final_status == "filled" else "접수",
        broker_state=broker_state,
        executed_volume=executed_volume,
        executed_funds=executed_funds,
        paid_fee=paid_fee,
        detail=detail_text,
        broker_response=detail,
    )
    poll_execution_events("upbit")
    run_reconciliation()
    return {"ok": True, "reason": detail_text, "order": dict(STATE["upbit_smoke_order"]), "snapshot": snapshot()}


def run_reconciliation() -> dict[str, Any]:
    STATE["reconciliation_last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    broker_data = refresh_broker_reconciliation()
    reconciliation = reconciliation_snapshot()
    summary = reconciliation["summary"]
    blocking_count = int(summary["api_required_count"]) + int(summary["mismatch_count"])
    append_audit(
        "danger" if blocking_count else "info",
        "포지션/계좌 대조",
        f"{summary['status_label']}: API/원장 필요 {summary['api_required_count']}개, 불일치 {summary['mismatch_count']}개, 조회 오류 {len(broker_data['errors'])}개",
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


def strategy_for_order_intent(checks: dict[str, Any], intent: OrderIntent) -> dict[str, Any]:
    strategies = checks.get("strategies") if isinstance(checks.get("strategies"), list) else []
    return next((strategy for strategy in strategies if str(strategy.get("strategy_id")) == intent.strategy_id), {})


def evaluate_order_gate_with_report(
    checks: dict[str, Any],
    side: str,
    dry_run: bool,
    intent: OrderIntent | None = None,
) -> tuple[bool, str, str, str, PreTradeRiskReport]:
    intent = intent or default_order_intent(checks, side)
    portfolio_gate = portfolio_gate_for_intent(checks, intent)
    context = apply_portfolio_gate_to_context(pre_trade_context(checks, intent, dry_run), portfolio_gate, intent)
    report = PreTradeRiskGate().evaluate(intent, context)
    if portfolio_gate.get("active"):
        portfolio_status: CheckStatus = "pass" if portfolio_gate.get("allowed") is True else "fail"
        report = PreTradeRiskReport(
            report.checked_at,
            (
                RiskCheck("Portfolio Artifact", portfolio_status, str(portfolio_gate.get("detail") or "")),
                *report.checks,
            ),
        )
    strategy = strategy_for_order_intent(checks, intent)
    paper_portfolio_evidence_gate = strategy.get("paper_portfolio_evidence_gate") if isinstance(strategy.get("paper_portfolio_evidence_gate"), dict) else {}
    if paper_portfolio_evidence_gate.get("required"):
        evidence_status: CheckStatus = "pass" if paper_portfolio_evidence_gate.get("ready") is True else "fail"
        report = PreTradeRiskReport(
            report.checked_at,
            (
                RiskCheck("Portfolio Paper Evidence", evidence_status, str(paper_portfolio_evidence_gate.get("detail") or "")),
                *report.checks,
            ),
        )
    revalidation = strategy_revalidation_status(strategy, lifecycle_status=strategy.get("lifecycle_status")) if strategy else {}
    if intent.side == "BUY" and revalidation.get("expired") is True:
        report = PreTradeRiskReport(
            report.checked_at,
            (*report.checks, RiskCheck("전략 재검증", "fail", str(revalidation.get("detail")))),
        )
    watchdog = checks.get("watchdog") if isinstance(checks.get("watchdog"), dict) else {}
    watchdog_critical = int(watchdog.get("critical_count", 0) or 0)
    if watchdog_critical:
        detail = f"Watchdog critical {watchdog_critical}개: {', '.join(watchdog.get('next_actions', [])[:4])}"
        report = PreTradeRiskReport(report.checked_at, (*report.checks, RiskCheck("Watchdog", "fail", detail)))
    # Dry-run is an offline safety simulation and remains usable outside market
    # hours.  Paper Trader enforces its own exchange-session clock; this gate is
    # for orders that could otherwise reach the live adapter.
    calendar_state = order_intent_market_session(intent) if not dry_run else None
    if calendar_state is not None:
        calendar_status: CheckStatus = "pass" if calendar_state.get("orderable") is True else "fail"
        report = PreTradeRiskReport(
            report.checked_at,
            (*report.checks, RiskCheck("거래소 세션", calendar_status, str(calendar_state.get("detail") or "시장 일정 확인 실패"))),
        )
    if report.can_submit:
        if dry_run:
            return True, "dry_run", "simulated", "Dry Run 보호가 켜져 있어 브로커 전송 없이 주문 의도를 감사 로그에만 기록했습니다.", report
        return False, "adapter_blocked", "held", "실제 주문 전송 레이어가 아직 안전 검증 전이므로 브로커 전송을 차단했습니다.", report

    adapter_labels = {"운용 모드", "실거래 환경 변수", "실주문 어댑터"}
    non_adapter_blockers = [check for check in report.blockers if check.label not in adapter_labels]
    adapter_blockers = [check for check in report.blockers if check.label in adapter_labels]
    # Until the live adapter itself is verified, retain that stronger/earlier
    # hold reason even when the exchange is also closed.  The failed session
    # check remains visible in the report and becomes the active blocker once
    # the adapter gates pass.
    if adapter_blockers and all(check.label == "거래소 세션" for check in non_adapter_blockers):
        adapter_blocker = next((check for check in adapter_blockers if check.label == "실주문 어댑터"), adapter_blockers[0])
        return False, "adapter_blocked", "held", adapter_blocker.detail, report
    if non_adapter_blockers:
        return False, "risk_blocked", "blocked", report.summary, report
    adapter_blocker = next((check for check in report.blockers if check.label == "실주문 어댑터"), report.blockers[0])
    return False, "adapter_blocked", "held", adapter_blocker.detail, report


def order_intent_market_session(intent: OrderIntent) -> dict[str, object] | None:
    text = f"{intent.asset} {intent.symbol} {intent.metadata.get('broker_id', '')}".lower()
    if any(token in text for token in ("crypto", "btc", "eth", "usdt", "binance", "upbit")):
        return None
    if any(token in text for token in ("kr_stock", "stock_kr", ".ks", ".kq", "kis")):
        return market_session_state("XKRX", regular_open="09:00", regular_close="15:30")
    if any(token in text for token in ("us_stock", "stock_us", "nyse", "nasdaq", "amex")):
        return market_session_state("XNYS", regular_open="09:30", regular_close="16:00")
    return None


class LiveArtifactSignalProvider:
    plugin_id = "live_artifact_signal"
    label = "Live Artifact Signal"

    def signal_for_price(
        self,
        *,
        artifact: Any,
        price: float,
        context: Any,
    ) -> OrderSide | None:
        _ = price, context
        return explicit_artifact_signal(artifact)

    def signal_for_market(
        self,
        *,
        artifact: Any,
        market_data: StrategyMarketData,
        context: Any,
    ) -> OrderSide | None:
        _ = market_data, context
        return explicit_artifact_signal(artifact)


def explicit_artifact_signal(artifact: Any) -> OrderSide | None:
    values: list[Any] = []
    if isinstance(artifact, dict):
        values.extend(
            artifact.get(key)
            for key in ("signal", "last_signal", "test_signal", "manual_signal", "side", "order_side")
        )
        signals = artifact.get("signals")
        if isinstance(signals, dict):
            values.extend(signals.get(key) for key in ("current", "latest", "last", "signal"))
    for value in values:
        normalized = str(value or "").strip().upper()
        if normalized in {"BUY", "SELL"}:
            return normalized  # type: ignore[return-value]
    return None


def strategy_market_data(strategy: dict[str, Any]) -> StrategyMarketData:
    event = strategy_market_event(strategy)
    return StrategyMarketData(
        price=event.price,
        close_price=event.price,
        volume=event.quantity,
        occurred_at=event.event_time,
        instrument_id=event.instrument_id,
        provider=event.provider,
        received_at=event.received_time,
        latency_seconds=event.latency_seconds,
    )


def strategy_market_event(strategy: dict[str, Any]) -> MarketEvent:
    price = strategy_float(
        strategy,
        "reference_price",
        "last_price",
        "price",
        "close",
        "close_price",
        default=1.0 if strategy_broker_id(strategy) == "binance" else 1000.0,
    )
    symbol = str(strategy.get("symbol") or "UNKNOWN")
    provider = strategy_broker_id(strategy)
    now = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    return MarketEvent(
        instrument_id=str(strategy.get("instrument_id") or f"symbol:{symbol}"),
        symbol=symbol,
        provider=provider,
        market=str(strategy.get("market") or provider).upper(),
        market_type=str(strategy.get("market_type") or strategy.get("asset") or "UNKNOWN").upper(),
        event_time=str(strategy.get("price_time") or strategy.get("occurred_at") or now),
        received_time=now,
        price=price,
        quantity=max(0.0, strategy_float(strategy, "volume", default=0.0)),
        event_id=str(strategy.get("market_event_id") or ""),
        metadata={"source": "strategy-artifact-reference-price"},
    )


def strategy_float(strategy: dict[str, Any], *keys: str, default: float = 0.0) -> float:
    for key in keys:
        value = strategy.get(key)
        if value in (None, ""):
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return float(default)


def select_strategy_for_profile(checks: dict[str, Any], profile_id: str) -> dict[str, Any] | None:
    strategies = select_strategies_for_profile(checks, profile_id)
    return strategies[0] if strategies else None


def select_strategies_for_profile(checks: dict[str, Any], profile_id: str) -> list[dict[str, Any]]:
    normalized_profile = "stock" if profile_id == "stock" else "crypto"
    provider = str(STATE["automation"][normalized_profile].get("provider") or ("kis" if normalized_profile == "stock" else "binance"))
    eligibility_key = "live_eligible" if current_mode() == "FULL_LIVE" else "live_allowed"
    strategies = [strategy for strategy in checks.get("strategies", []) if strategy.get(eligibility_key)]
    if normalized_profile == "stock":
        matched = [strategy for strategy in strategies if strategy_broker_id(strategy) == "kis"]
    else:
        matched = [strategy for strategy in strategies if strategy_broker_id(strategy) == provider]
        if not matched:
            matched = [strategy for strategy in strategies if strategy_broker_id(strategy) in {"binance", "upbit"}]
    if len(matched) <= 1:
        return matched
    portfolio_ids = {
        str(instance.get("strategyId") or instance.get("strategy_id") or "")
        for portfolio in checks.get("portfolios", []) if isinstance(portfolio, dict)
        for instance in portfolio.get("strategy_instances", []) if isinstance(instance, dict)
    }
    portfolio_matched = [strategy for strategy in matched if str(strategy.get("strategy_id") or "") in portfolio_ids or str(strategy.get("plugin") or "") in portfolio_ids]
    return portfolio_matched or matched[:1]


def strategy_execution_for_profile(checks: dict[str, Any], profile_id: str) -> StrategyExecutionResult | None:
    executions = strategy_executions_for_profile(checks, profile_id)
    return executions[0][1] if executions else None


def strategy_executions_for_profile(checks: dict[str, Any], profile_id: str) -> list[tuple[dict[str, Any], StrategyExecutionResult]]:
    strategies = select_strategies_for_profile(checks, profile_id)
    if not strategies:
        return []
    normalized_profile = "stock" if profile_id == "stock" else "crypto"
    runner = StrategyExecutionRunner(lambda _plugin_id: LiveArtifactSignalProvider())
    return [
        (
            strategy,
            runner.run(
                artifact=strategy, market_data=strategy_market_data(strategy), mode=current_mode(),
                stream_id=f"live:{normalized_profile}:{strategy.get('strategy_id', 'unknown')}",
                quantity=strategy_float(strategy, "order_quantity", "quantity", default=1.0),
                metadata={"broker_id": strategy_broker_id(strategy), "profile_id": normalized_profile, "runner": "StrategyExecutionRunner"},
                reason_prefix=f"{strategy.get('name') or strategy.get('strategy_id') or '전략'} live runner signal",
            ),
        )
        for strategy in strategies
    ]


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
        halted=bool(STATE["kill_switch"]) or durable_control_halt_active(intent),
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


def durable_global_kill_active() -> bool:
    path = str(os.environ.get("TRADING_CONTROL_STATE_PATH") or "").strip()
    if not path:
        return False
    try:
        return bool(DurableControlState(path).read().get("globalKill"))
    except (OSError, ValueError, json.JSONDecodeError):
        return True


def durable_control_halt_active(intent: OrderIntent | None = None) -> bool:
    path = str(os.environ.get("TRADING_CONTROL_STATE_PATH") or "").strip()
    if not path:
        return False
    if intent is None:
        return durable_global_kill_active()
    broker_id = str(intent.metadata.get("broker_id") or broker_id_from_symbol(intent.symbol, intent.asset))
    assessment = DurableControlState(path).halt_assessment(
        app="live_trader",
        route=broker_id,
        strategy=intent.strategy_id,
        instrument=intent.symbol,
    )
    return bool(assessment.get("halted"))


def durable_control_snapshot() -> dict[str, Any]:
    path = str(os.environ.get("TRADING_CONTROL_STATE_PATH") or "").strip()
    if not path:
        return {"configured": False, "halted": False, "globalKill": False, "scopedKills": {}, "reasons": [], "path": ""}
    try:
        state = DurableControlState(path).read()
        assessment = DurableControlState(path).halt_assessment(app="live_trader")
        return {
            "configured": True,
            "halted": bool(assessment.get("halted")),
            "globalKill": state.get("globalKill") is True,
            "scopedKills": state.get("scopedKills") or {},
            "reasons": assessment.get("reasons") or [],
            "path": path,
        }
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return {"configured": True, "halted": True, "globalKill": True, "scopedKills": {}, "reasons": [f"control-state-unavailable:{type(exc).__name__}"], "path": path}


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


def order_gate_audit_record(
    *,
    source: str,
    intent: OrderIntent,
    order: dict[str, Any],
    risk_report: PreTradeRiskReport,
    runner_report: StrategyExecutionResult | None = None,
    retry: bool = False,
) -> AuditEvent:
    payload: dict[str, Any] = {
        "order": dict(order),
        "queue_state": str(order.get("queue_state", "")),
        "dry_run": bool(order.get("dry_run", False)),
        "retry": retry,
    }
    if runner_report is not None:
        payload["runner_report"] = runner_report.to_dict()
    return audit_event_from_order_gate(
        app="live_trader",
        intent=intent,
        report=risk_report,
        order_id=str(order.get("order_id", "")),
        state=str(order.get("state", "")),
        source=source,
        payload=payload,
    )


def submit_order_intent(
    checks: dict[str, Any],
    intent: OrderIntent,
    *,
    dry_run: bool,
    audit_event: str,
    runner_report: StrategyExecutionResult | None = None,
) -> dict[str, Any]:
    RECOVERY_JOURNAL.save(recovery_state_payload(), reason="before-order", idempotency_keys=[str(item.get("idempotency_key") or "") for item in STATE.get("orders", [])])
    ok, order_state, queue_state, reason, risk_report = evaluate_order_gate_with_report(checks, intent.side, dry_run, intent)
    portfolio_gate = portfolio_gate_for_intent(checks, intent)
    retry_backoff = float(STATE["retry_policy"]["backoff_sec"])
    metadata = intent.metadata if isinstance(intent.metadata, dict) else {}
    target_revision = int(metadata.get("target_revision") or (len(STATE["orders"]) + 1))
    idempotency_key = build_idempotency_key(
        portfolio_id=str(metadata.get("portfolio_id") or portfolio_gate.get("portfolio_id") or "default-portfolio"),
        strategy_instance_id=str(metadata.get("strategy_instance_id") or intent.strategy_id),
        instrument_id=str(metadata.get("instrument_id") or intent.symbol),
        target_revision=target_revision,
        purpose=str(metadata.get("order_purpose") or "REBALANCE"),
    )
    if idempotency_key in set(STATE.get("persisted_idempotency_keys", [])):
        existing = next((item for item in STATE.get("orders", []) if item.get("idempotency_key") == idempotency_key), None)
        append_audit("warn", audit_event, f"재시작 이후 중복 주문 차단: idempotency_key={idempotency_key}")
        return {"ok": existing is not None, "reason": "persistent-duplicate-idempotency-key", "order": existing or {}, "duplicate": True, "snapshot": snapshot()}
    managed_order, oms_created = LIVE_OMS.create(intent, idempotency_key)
    if not oms_created:
        existing = next((item for item in STATE.get("orders", []) if item.get("oms_order_id") == managed_order.order_id), None)
        append_audit("warn", audit_event, f"중복 주문 차단: idempotency_key={idempotency_key}")
        return {"ok": existing is not None, "reason": "duplicate-idempotency-key", "order": existing or {}, "duplicate": True, "snapshot": snapshot()}
    if oms_created:
        if ok:
            LIVE_OMS.transition(managed_order.order_id, "RISK_APPROVED", "PreTradeRiskGate passed")
        else:
            LIVE_OMS.transition(managed_order.order_id, "REJECTED", reason)
    order = {
        "time": now_text(),
        "created_at": now_text(),
        "updated_at": now_text(),
        "order_id": next_order_id(order_state, dry_run),
        "oms_order_id": managed_order.order_id,
        "idempotency_key": idempotency_key,
        "idempotency_duplicate": not oms_created,
        "oms_status": managed_order.status,
        "strategy_id": intent.strategy_id,
        "symbol": intent.symbol,
        "asset": intent.asset,
        "side": intent.side,
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
        "portfolio_gate": portfolio_gate if portfolio_gate.get("active") else {},
    }
    if runner_report is not None:
        order["runner_report"] = runner_report.to_dict()
    if ok and not dry_run:
        broker_id = str(metadata.get("broker_id") or broker_id_from_symbol(intent.symbol, intent.asset)).lower()
        broker_payload = {
            "broker_id": broker_id,
            "symbol": intent.symbol,
            "asset": intent.asset,
            "side": intent.side,
            "quantity": intent.quantity,
            "qty": intent.quantity,
            "price": intent.reference_price,
            "notional": intent.notional,
            "order_type": str(metadata.get("order_type") or "LIMIT"),
            "identifier": idempotency_key,
            "exchange": str(metadata.get("exchange") or ""),
        }
        try:
            LIVE_OMS.transition(managed_order.order_id, "SUBMITTING", "broker request dispatch")
            broker_response = LiveBrokerRouter().place_order(broker_payload)
            order["broker_response"] = broker_response
            response_ok = broker_response.get("ok") is True
            response_payload = broker_response.get("json") if isinstance(broker_response.get("json"), dict) else {}
            broker_order_id = str(
                response_payload.get("uuid")
                or response_payload.get("orderId")
                or response_payload.get("clientOrderId")
                or (response_payload.get("output") or {}).get("ODNO")
                or ""
            )
            if response_ok and broker_order_id:
                LIVE_OMS.acknowledge(managed_order.order_id, broker_order_id)
                order.update({
                    "state": "acknowledged",
                    "queue_state": "submitted",
                    "broker_order_id": broker_order_id,
                    "reason": "broker-acknowledged",
                    "next_retry_at": "-",
                })
                reason = "broker-acknowledged"
            elif response_ok:
                LIVE_OMS.mark_unknown(managed_order.order_id, "broker response missing order id")
                order.update({"state": "unknown", "queue_state": "reconcile_required", "reason": "broker-response-missing-order-id", "next_retry_at": "-"})
                reason = "broker-response-missing-order-id"
                ok = False
            elif int(safe_float(broker_response.get("statusCode"), 0.0)) == 0:
                LIVE_OMS.mark_unknown(managed_order.order_id, "network outcome unknown; reconcile before retry")
                order.update({"state": "unknown", "queue_state": "reconcile_required", "reason": "network-outcome-unknown", "next_retry_at": "-"})
                reason = "network-outcome-unknown"
                ok = False
            else:
                LIVE_OMS.transition(managed_order.order_id, "REJECTED", "broker rejected request", {"response": broker_response})
                order.update({"state": "broker_rejected", "queue_state": "failed", "reason": str(broker_response.get("text") or "broker-rejected")[:500], "next_retry_at": "-"})
                reason = str(order["reason"])
                ok = False
        except BrokerNotReadyError as exc:
            LIVE_OMS.transition(managed_order.order_id, "REJECTED", str(exc))
            order.update({"state": "adapter_blocked", "queue_state": "failed", "reason": str(exc), "next_retry_at": "-"})
            reason = str(exc)
            ok = False
        order["oms_status"] = LIVE_OMS.orders[managed_order.order_id].status
    STATE["orders"].insert(0, order)
    STATE["orders"] = STATE["orders"][:50]
    STATE.setdefault("persisted_idempotency_keys", []).append(idempotency_key)
    STATE["persisted_idempotency_keys"] = list(dict.fromkeys(STATE["persisted_idempotency_keys"]))[-5000:]
    checkpoint = RECOVERY_JOURNAL.save(recovery_state_payload(), reason="after-order", idempotency_keys=[str(item.get("idempotency_key") or "") for item in STATE.get("orders", [])])
    STATE["recovery_status"] = {"verified": True, "safeMode": False, "generation": checkpoint["generation"], "detail": "주문 전후 원자적 체크포인트 저장 완료"}
    append_audit(
        "info" if ok else "danger",
        audit_event,
        order_gate_audit_detail(order, reason, risk_report),
        audit_record=order_gate_audit_record(
            source=audit_event,
            intent=intent,
            order=order,
            risk_report=risk_report,
            runner_report=runner_report,
        ),
    )
    return {"ok": ok, "reason": reason, "order": order, "snapshot": snapshot()}


def start_continuous_runtime(profile_id: str, mode: str, portfolio_id: str = "") -> dict[str, Any]:
    return LIVE_CONTINUOUS_CONTROLLER.start(profile_id, mode, portfolio_id)


def stop_continuous_runtime(profile_id: str = "") -> dict[str, Any]:
    return LIVE_CONTINUOUS_CONTROLLER.stop(profile_id)


def start_execution_streams(broker_id: str = "all") -> dict[str, Any]:
    brokers = ("kis", "upbit") if str(broker_id).lower().strip() in {"", "all"} else (str(broker_id).lower().strip(),)
    result = LIVE_EXECUTION_STREAMS.start(brokers)
    append_audit("info", "체결 스트림", f"실시간 체결 감시 시작: {', '.join(brokers)}")
    return {"ok": True, "reason": "execution streams started", "streams": result, "snapshot": snapshot()}


def stop_execution_streams() -> dict[str, Any]:
    result = LIVE_EXECUTION_STREAMS.stop()
    append_audit("info", "체결 스트림", "실시간 체결 감시 정지")
    return {"ok": True, "reason": "execution streams stopped", "streams": result, "snapshot": snapshot()}


def submit_test_intent() -> dict[str, Any]:
    checks = snapshot()
    intent = default_order_intent(checks, "BUY")
    return submit_order_intent(checks, intent, dry_run=bool(STATE["dry_run"]), audit_event="주문 게이트")


def recovery_state_payload() -> dict[str, Any]:
    return {
        "mode": STATE.get("mode"), "dry_run": STATE.get("dry_run"), "kill_switch": STATE.get("kill_switch"),
        "new_entries_blocked": STATE.get("new_entries_blocked"), "orders": list(STATE.get("orders", [])),
        "strategy_runner": dict(STATE.get("strategy_runner", {})),
    }


def run_recovery_drill() -> dict[str, Any]:
    checkpoint = RECOVERY_JOURNAL.save(recovery_state_payload(), reason="operator-recovery-drill", idempotency_keys=[str(item.get("idempotency_key") or "") for item in STATE.get("orders", [])])
    loaded = RECOVERY_JOURNAL.load_latest()
    verified = loaded.get("contentHash") == checkpoint.get("contentHash") and loaded.get("state") == checkpoint.get("state")
    idempotency_keys = [str(item.get("idempotency_key") or "") for item in STATE.get("orders", []) if item.get("idempotency_key")]
    reconciliation_status = reconciliation_snapshot().get("summary", {}).get("status")
    broker_reconciled = reconciliation_status == "pass"
    assurance = assess_recovery_drill(
        checkpoint_saved=bool(checkpoint.get("contentHash")),
        checkpoint_hash_verified=verified,
        idempotency_verified=len(idempotency_keys) == len(set(idempotency_keys)),
        broker_reconciled=broker_reconciled,
    )
    STATE["recovery_status"] = {
        "verified": assurance["status"] == "pass", "safeMode": bool(loaded.get("safeMode")) or assurance["safeModeRequired"],
        "generation": int(loaded.get("generation") or 0),
        "detail": "복구·멱등성·브로커 대조 훈련 통과" if assurance["status"] == "pass" else "복구 훈련 미통과: 안전 모드에서 신규 위험 차단",
        "corruptCheckpoints": loaded.get("corruptCheckpoints", []),
        "assurance": assurance,
    }
    if assurance["status"] != "pass":
        STATE["new_entries_blocked"] = True
        STATE["mode"] = "MONITOR"
    append_audit("info" if verified else "danger", "Recovery Drill", STATE["recovery_status"]["detail"])
    return {"ok": assurance["status"] == "pass", "recovery": dict(STATE["recovery_status"]), "snapshot": snapshot()}


def restore_runtime_from_checkpoint() -> dict[str, Any]:
    loaded = RECOVERY_JOURNAL.load_latest()
    generation = int(loaded.get("generation") or 0)
    if generation <= 0:
        STATE["recovery_status"] = {"verified": False, "safeMode": True, "generation": 0, "detail": "유효한 체크포인트 없음: MONITOR 유지", "corruptCheckpoints": loaded.get("corruptCheckpoints", [])}
        return dict(STATE["recovery_status"])
    recovered = loaded.get("state") if isinstance(loaded.get("state"), dict) else {}
    STATE["orders"] = list(recovered.get("orders", []))[:50] if isinstance(recovered.get("orders"), list) else []
    if isinstance(recovered.get("strategy_runner"), dict):
        STATE["strategy_runner"].update(recovered["strategy_runner"])
    STATE["persisted_idempotency_keys"] = list(dict.fromkeys(str(item) for item in loaded.get("idempotencyKeys", []) if item))
    STATE["mode"] = "MONITOR"
    STATE["dry_run"] = True
    STATE["new_entries_blocked"] = True
    STATE["recovery_status"] = {
        "verified": not loaded.get("safeMode"), "safeMode": True, "generation": generation,
        "detail": "체크포인트 복구 완료. 브로커 reconciliation 전까지 MONITOR/신규 진입 차단",
        "corruptCheckpoints": loaded.get("corruptCheckpoints", []),
    }
    append_audit("warn", "Startup Recovery", STATE["recovery_status"]["detail"])
    return dict(STATE["recovery_status"])


def run_shadow_live(payload: dict[str, Any]) -> dict[str, Any]:
    checks = snapshot()
    intent = default_order_intent(checks, str(payload.get("side") or "BUY"))
    reference = safe_float(payload.get("decision_price"), intent.reference_price)
    virtual_fill = safe_float(payload.get("virtual_fill_price"), reference)
    paper_fill = safe_float(payload.get("paper_fill_price"), 0.0)
    evidence = SHADOW_ENGINE.execute(
        ShadowOrder(
            decision_id=str(payload.get("decision_id") or f"shadow-{len(STATE.get('shadow_evidence', [])) + 1}"),
            strategy_id=intent.strategy_id,
            instrument_id=str((intent.metadata or {}).get("instrument_id") or intent.symbol),
            side=intent.side, quantity=intent.quantity, decision_price=reference, virtual_fill_price=virtual_fill,
            expected_cost_bps=safe_float(payload.get("expected_cost_bps"), safe_float((intent.metadata or {}).get("expected_cost_bps"), 0.0)),
            latency_ms=int(safe_float(payload.get("latency_ms"), 0)),
        ), paper_fill_price=paper_fill or None,
    )
    STATE.setdefault("shadow_evidence", []).insert(0, evidence)
    STATE["shadow_evidence"] = STATE["shadow_evidence"][:100]
    append_audit("info", "Shadow Live", f"브로커 전송 없이 가상 체결 기록: {evidence['contentHash'][:12]}")
    return {"ok": True, "evidence": evidence, "snapshot": snapshot()}


def runtime_operational_readiness(strategies: list[dict[str, Any]], portfolios: list[dict[str, Any]]) -> dict[str, Any]:
    bundles = [item.get("operational_readiness_bundle") for item in portfolios if isinstance(item.get("operational_readiness_bundle"), dict) and item.get("operational_readiness_bundle")]
    if not bundles:
        return {"schemaVersion": "operational-readiness-v1", "score": 0, "status": "NOT_REQUIRED", "liveEligible": False, "components": []}
    base = bundles[0]
    source = base.get("operationalReadiness") if isinstance(base.get("operationalReadiness"), dict) else {}
    components = source.get("components") if isinstance(source.get("components"), list) else []
    paper_ready = any((item.get("paper_portfolio_evidence_gate") or {}).get("ready") is True for item in strategies)
    execution_component = next((item for item in components if isinstance(item, dict) and item.get("id") == "executionQuality"), {})
    execution_ok = bool(execution_component.get("passed") or safe_float(execution_component.get("awarded"), 0) > 0 or STATE.get("shadow_evidence"))
    capacity_ok = all(
        not (item.get("advanced_operations") or {}).get("capacity")
        or all(entry.get("allowed") is True for entry in (item.get("advanced_operations") or {}).get("capacity", []) if isinstance(entry, dict))
        for item in portfolios
    )
    return operational_readiness({
        "dataIntegrity": next((component.get("passed") for component in components if isinstance(component, dict) and component.get("id") == "dataIntegrity"), False),
        "artifactReproducibility": bool(base.get("contentHash")), "paperObservation": paper_ready,
        "executionQuality": execution_ok, "capacityHeadroom": capacity_ok,
        "recoveryVerified": STATE.get("recovery_status", {}).get("verified") is True and STATE.get("recovery_status", {}).get("safeMode") is False,
        "policyReplayPassed": bool(STATE.get("policy_replays")),
        "operatorDrill": bool(STATE.get("preflight_last_run")) and all(STATE.get("checklist", {}).values()),
    })


def run_policy_replay(payload: dict[str, Any]) -> dict[str, Any]:
    checks = snapshot()
    intent = default_order_intent(checks, str(payload.get("side") or "BUY"))
    intent = replace(
        intent,
        quantity=max(0.0, safe_float(payload.get("quantity"), intent.quantity)),
        reference_price=max(0.0, safe_float(payload.get("referencePrice"), intent.reference_price)),
        metadata={
            **intent.metadata,
            "event_id": str(payload.get("eventId") or "live-policy-replay"),
            "current_weight": safe_float(payload.get("currentWeight"), 0.0),
            "portfolio_equity": safe_float(payload.get("portfolioEquity"), float(STATE["risk_settings"]["strategy_capital_limit_krw"])),
            "expected_alpha_bps": safe_float(payload.get("expectedAlphaBps"), 10.0),
            "expected_cost_bps": safe_float(payload.get("expectedCostBps"), 5.0),
        },
    )
    alternative = payload.get("alternative") if isinstance(payload.get("alternative"), dict) else {}
    bundle = policy_replay_for_intent(checks, intent, alternative)
    STATE.setdefault("policy_replays", [])
    STATE["policy_replays"].insert(0, bundle)
    STATE["policy_replays"] = STATE["policy_replays"][:20]
    append_audit("info", "정책 Replay", f"결정 변경 {bundle['changedDecisionCount']} / {bundle['eventCount']}")
    return {"ok": True, "bundle": bundle, "snapshot": checks}


def run_strategy_cycle(profile_id: str) -> dict[str, Any]:
    checks = snapshot()
    normalized_profile = "stock" if profile_id == "stock" else "crypto"
    strategy_executions = strategy_executions_for_profile(checks, normalized_profile)
    if not strategy_executions:
        reason = f"{normalized_profile} 프로필에 live_small_eligible/live_eligible 전략이 없습니다."
        STATE["strategy_runner"].update({
            "last_run": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "last_profile": normalized_profile,
            "last_strategy": "-",
            "last_signal": "-",
            "last_action": reason,
        })
        append_audit("warn", "전략 Runner", reason)
        return {"ok": False, "reason": reason, "snapshot": snapshot()}

    if len(strategy_executions) > 1:
        return run_multi_strategy_cycle(checks, normalized_profile, strategy_executions)

    execution = strategy_executions[0][1]

    STATE["strategy_runner"].update({
        "last_run": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "last_profile": normalized_profile,
        "last_strategy": execution.artifact_id,
        "last_signal": execution.signal or "-",
        "last_action": execution.reason,
    })
    if execution.intent is None:
        append_audit("info", "전략 Runner", f"{execution.artifact_id}: {execution.reason}")
        return {"ok": True, "reason": execution.reason, "runner_report": execution.to_dict(), "snapshot": snapshot()}

    result = submit_order_intent(
        checks,
        execution.intent,
        dry_run=bool(STATE["dry_run"]),
        audit_event="전략 Runner",
        runner_report=execution,
    )
    STATE["strategy_runner"]["last_action"] = f"{execution.signal} -> {result['reason']}"
    result["runner_report"] = execution.to_dict()
    return result


def run_multi_strategy_cycle(checks: dict[str, Any], profile_id: str, strategy_executions: list[tuple[dict[str, Any], StrategyExecutionResult]]) -> dict[str, Any]:
    observed_at = datetime.now(timezone.utc).isoformat()
    signals: list[SleeveSignal] = []
    policies: dict[str, InstrumentTradePolicy] = {}
    execution_by_instance: dict[str, tuple[dict[str, Any], StrategyExecutionResult]] = {}
    for index, (strategy, execution) in enumerate(strategy_executions):
        gate = strategy.get("portfolio_gate") if isinstance(strategy.get("portfolio_gate"), dict) else {}
        instance = gate.get("instance") if isinstance(gate.get("instance"), dict) else {}
        instance_id = str(instance.get("instanceId") or f"{strategy.get('strategy_id', 'strategy')}:{index}")
        instrument_id = str(strategy.get("instrument_id") or strategy.get("symbol") or instance_id)
        allocation = safe_float(gate.get("policyTargetWeight"), safe_float(gate.get("targetWeight"), 1 / max(1, len(strategy_executions))))
        signals.append(SleeveSignal(
            strategy_instance_id=instance_id,
            strategy_id=str(strategy.get("strategy_id") or execution.artifact_id),
            deployment_id=str(strategy.get("deployment_id") or ""),
            instrument_id=instrument_id,
            symbol=str(strategy.get("symbol") or "UNKNOWN"),
            signal=str(execution.signal or "HOLD"),
            occurred_at=observed_at,
            reference_price=max(0.0, strategy_float(strategy, "reference_price", "last_price", default=0.0)),
            allocation_weight=max(0.0, allocation),
            confidence=safe_float(strategy.get("score"), 100.0) / 100 if str(strategy.get("score") or "").replace(".", "", 1).isdigit() else 1.0,
            reason=execution.reason,
        ))
        policies[instrument_id] = InstrumentTradePolicy(
            instrument_id=instrument_id,
            asset_class=str(strategy.get("asset") or "UNKNOWN"),
            market_type=str(strategy.get("market_type") or "spot"),
            allow_short=strategy.get("allow_short") is True,
            maximum_absolute_weight=min(1.0, max(0.0, safe_float(gate.get("maxSymbolWeightPct"), 100.0) / 100)),
        )
        execution_by_instance[instance_id] = (strategy, execution)

    current_sleeves = {str(key): safe_float(value, 0.0) for key, value in STATE.get("strategy_sleeves", {}).items()}
    current_weights: dict[str, float] = {}
    current_quantities: dict[str, float] = {}
    for signal in signals:
        current_weights[signal.instrument_id] = current_weights.get(signal.instrument_id, 0.0) + current_sleeves.get(signal.strategy_instance_id, 0.0)
        current_quantities.setdefault(signal.instrument_id, broker_position_quantity(signal.symbol))
    portfolio_equity = max(float(STATE["risk_settings"]["strategy_capital_limit_krw"]), 1.0)
    plans = LIVE_MULTI_COORDINATOR.coordinate(
        signals, policies=policies, current_sleeve_weights=current_sleeves,
        current_net_weights=current_weights, current_net_quantities=current_quantities,
        portfolio_equity=portfolio_equity,
    )
    order_results: list[dict[str, Any]] = []
    for plan in plans:
        if plan.blocked or plan.side == "HOLD" or plan.quantity <= 0:
            continue
        preferred_signal = next((signal for signal in signals if signal.instrument_id == plan.instrument_id and signal.signal.upper() == plan.side), None)
        lead_signal = preferred_signal or next(signal for signal in signals if signal.instrument_id == plan.instrument_id)
        strategy, execution = execution_by_instance[lead_signal.strategy_instance_id]
        source_intent = execution.intent or default_order_intent(checks, plan.side)
        intent = replace(
            source_intent,
            strategy_id=str(strategy.get("strategy_id") or execution.artifact_id),
            side=plan.side,
            quantity=plan.quantity,
            reference_price=lead_signal.reference_price,
            metadata={
                **(source_intent.metadata if isinstance(source_intent.metadata, dict) else {}),
                "multi_strategy": True,
                "strategy_instance_id": f"net:{plan.instrument_id}",
                "strategy_instance_ids": list(plan.sleeve_targets),
                "sleeve_targets": plan.sleeve_targets,
                "target_revision": plan.target_revision,
                "instrument_id": plan.instrument_id,
                "current_weight": plan.current_weight,
                "portfolio_equity": portfolio_equity,
                "expected_alpha_bps": 10.0,
                "expected_cost_bps": 5.0,
                "risk_reducing": abs(plan.target_weight) < abs(plan.current_weight),
                "shortable": plan.shortable,
            },
            reason=f"multi-strategy net target: {len(plan.sleeve_targets)} sleeves",
        )
        result = submit_order_intent(checks, intent, dry_run=bool(STATE["dry_run"]), audit_event="Multi-Strategy Runner", runner_report=execution)
        result["coordinated_plan"] = plan.to_dict()
        order_results.append(result)
        if result.get("ok"):
            STATE.setdefault("strategy_sleeves", {}).update(plan.sleeve_targets)
    STATE["strategy_runner"].update({
        "last_run": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "last_profile": profile_id,
        "last_strategy": f"{len(strategy_executions)} sleeves", "last_signal": "NET",
        "last_action": f"{len(plans)} instrument targets · {len(order_results)} net orders",
    })
    append_audit("info", "Multi-Strategy Runner", f"{len(strategy_executions)}개 Sleeve → {len(plans)}개 종목 목표 → {len(order_results)}개 순주문")
    return {
        "ok": all(item.get("ok") for item in order_results) if order_results else True,
        "reason": f"multi-strategy coordinated: {len(strategy_executions)} sleeves, {len(order_results)} net orders",
        "runner_reports": [execution.to_dict() for _, execution in strategy_executions],
        "plans": [plan.to_dict() for plan in plans],
        "orders": order_results,
        "snapshot": snapshot(),
    }


def broker_position_quantity(symbol: str) -> float:
    positions = STATE.get("broker_reconciliation", {}).get("positions", [])
    return sum(
        safe_float(item.get("quantity"), safe_float(item.get("qty"), 0.0))
        for item in positions if isinstance(item, dict) and str(item.get("symbol") or "") == symbol
    )


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
    append_audit(
        "info" if ok else "warn",
        "주문 재시도",
        order_gate_audit_detail(order, reason, risk_report),
        audit_record=order_gate_audit_record(
            source="주문 재시도",
            intent=retry_intent,
            order=order,
            risk_report=risk_report,
            retry=True,
        ),
    )
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
