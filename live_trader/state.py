from __future__ import annotations

import os
import csv
import html
import hashlib
import json
import secrets
import threading
import time
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from io import StringIO
from pathlib import Path
import sys
from typing import Any, Literal
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


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
    artifact_reference,
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
    AutomaticPromotionDecision,
    DecisionTraceStore,
    PositionTruth,
    PromotionMetrics,
    PromotionPolicy,
    build_restart_recovery_plan,
    build_trace_id,
    evaluate_automatic_promotion,
    reconcile_broker_truth,
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
from .live_adapters import (
    BINANCE_BASE_URL,
    BINANCE_FUTURES_BASE_URL,
    BINANCE_FUTURES_TEST_ORDER_ENDPOINT,
    binance_symbol_rules,
    build_binance_futures_order_request,
    build_binance_spot_order_request,
    build_kis_live_order_request,
    build_upbit_order_request,
    env_value,
    http_json,
)
from .futures_canary import (
    build_futures_canary_test_intents,
    derive_canary_quantity,
    evaluate_futures_canary_preflight,
    normalize_usdm_symbol,
)
from .order_management import OrderIntent, OrderSide
from .risk_engine import PreTradeContext, PreTradeRiskGate, PreTradeRiskReport, RecentOrder, RiskCheck
from trading_runtime.strategy_runner import StrategyExecutionResult, StrategyExecutionRunner, StrategyMarketData
from trading_runtime.market_calendar import market_session_state
from trading_runtime.artifact_metadata import ArtifactMetadataStore
from trading_runtime.telegram_notifications import (
    TelegramDispatcher,
    format_order_lifecycle_notification,
)


Mode = Literal["MONITOR", "SMALL_LIVE", "FULL_LIVE"]
CheckStatus = Literal["pass", "warn", "fail"]
RUNTIME_MODE_LOCK = threading.RLock()
BINANCE_FUTURES_CANARY_LOCK = threading.RLock()
# Serializes public runtime control operations without participating in a
# closed-bar cycle.  Public calls acquire this lock before manager/controller
# locks; they never hold RUNTIME_MODE_LOCK while entering the manager.
RUNTIME_CONTROL_LOCK = threading.RLock()
RUNTIME_MODE_RANK = {"MONITOR": 0, "SMALL_LIVE": 1, "FULL_LIVE": 2}
PROFESSIONAL_PROMOTION_POLICY = PromotionPolicy()

TELEGRAM_DISPATCHER = TelegramDispatcher(
    "live_trader",
    env_file=Path(__file__).resolve().parents[1] / ".env",
)

TELEGRAM_SAFETY_AUDIT_EVENTS = {
    "Kill Switch",
    "신규 진입 차단",
    "Watchdog Fail Closed",
    "Recovery Drill",
    "Startup Recovery",
}
TELEGRAM_HEALTH_AUDIT_EVENTS = {
    "Watchdog",
    "포지션/계좌 대조",
}
TELEGRAM_CONNECTIVITY_AUDIT_EVENTS = {
    "체결 스트림",
    "체결 이벤트 동기화",
}


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
MACHINE_VERIFIABLE_CHECKLIST_KEYS = {
    "api_keys_reviewed",
    "position_reconcile_reviewed",
}

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
        "broker_id": "binance-futures",
        "broker_name": "Binance USD-M Futures",
        "account": "Binance Futures",
        "currency": "USDT",
        "program_cash": None,
        "broker_cash": None,
        "detail": "Binance USD-M Futures signed account endpoint가 연결되어야 선물 지갑 잔고를 대조합니다.",
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
DOCTOR_DIAGNOSTIC_HISTORY_LIMIT = 50
BROKER_SNAPSHOT_ACTIVE_INTERVAL_SECONDS = 30.0
BROKER_SNAPSHOT_IDLE_INTERVAL_SECONDS = 300.0
BROKER_SNAPSHOT_MAX_BACKOFF_SECONDS = 1800.0
EXECUTION_SUMMARY_AUDIT_INTERVAL_SECONDS = 900.0
APP_DATA_ROOT = default_runtime_data_root()
DOCTOR_DIAGNOSTICS_PATH = Path(
    os.environ.get("LIVE_TRADER_DOCTOR_DIAGNOSTICS")
    or APP_DATA_ROOT / "logs" / "doctor-diagnostics.json"
)
OPERATOR_CHECKLIST_PATH = Path(
    os.environ.get("LIVE_TRADER_OPERATOR_CHECKLIST")
    or APP_DATA_ROOT / "logs" / "operator-checklist.json"
)
AUDIT_DB_PATH = Path(os.environ.get("LIVE_TRADER_AUDIT_DB") or APP_DATA_ROOT / "logs" / "live_trader_audit.sqlite3")
AUDIT_STORE = SQLiteAuditEventStore(AUDIT_DB_PATH)
PROGRAM_LEDGER_PATH = Path(
    os.environ.get("LIVE_TRADER_PROGRAM_LEDGER_DB") or APP_DATA_ROOT / "logs" / "live_trader_program_ledger.sqlite3"
)
PROGRAM_LEDGER = ProgramLedger(PROGRAM_LEDGER_PATH)
DECISION_TRACE_STORE = DecisionTraceStore(APP_DATA_ROOT / "logs" / "decision_trace.jsonl")
LIVE_OMS = OrderManagementSystem()
DEFAULT_WATCHDOG_SETTINGS: dict[str, float] = {
    "heartbeat_timeout_sec": 45.0,
    "market_data_stale_sec": 90.0,
    "max_recent_orders_per_min": 6.0,
    "max_retryable_orders": 3.0,
    "max_blocked_orders": 5.0,
}
DOCTOR_DIAGNOSTICS_LOCK = threading.RLock()
OPERATOR_CHECKLIST_LOCK = threading.RLock()


def read_json_document(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def write_json_document(path: Path, payload: dict[str, Any]) -> None:
    """Atomically persist a strict JSON document in the runtime data root."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False),
        encoding="utf-8",
    )
    temporary.replace(path)


def load_operator_checklist_values() -> dict[str, bool]:
    with OPERATOR_CHECKLIST_LOCK:
        payload = read_json_document(OPERATOR_CHECKLIST_PATH)
    values = payload.get("values") if isinstance(payload.get("values"), dict) else {}
    return {
        str(item["key"]): bool(values.get(str(item["key"]), False))
        for item in CHECKLIST_ITEMS
    }


def persist_operator_checklist_values(values: dict[str, object]) -> None:
    document = {
        "schema_version": 1,
        "updated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "values": {
            str(item["key"]): bool(values.get(str(item["key"]), False))
            for item in CHECKLIST_ITEMS
        },
    }
    with OPERATOR_CHECKLIST_LOCK:
        write_json_document(OPERATOR_CHECKLIST_PATH, document)


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
    "broker_truth_blocked": True,
    "manual_new_entries_blocked": False,
    "operator_confirmed": False,
    "risk_settings": dict(DEFAULT_RISK_SETTINGS),
    "checklist": load_operator_checklist_values(),
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
        "observed_cash_count": 0,
        "observed_position_count": 0,
        "snapshot_changed_brokers": [],
        "snapshot_skipped_brokers": [],
    },
    "broker_snapshot_poll": {
        "brokers": {},
        "last_summary_audit_monotonic": 0.0,
        "last_summary_signature": "",
    },
    "reconciliation_last_run": None,
    "preflight_last_run": None,
    "orders": [],
    "order_trace_index": {},
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
    "binance_smoke_order": {
        "status": "idle",
        "status_label": "미리보기 필요",
        "symbol": "BTCUSDT",
        "quantity": 0.0001,
        "confirmation_token": "",
        "expires_at": "",
        "used": False,
        "detail": "전략 신호와 분리된 브로커 제출 경로 점검 주문입니다.",
    },
    "binance_futures_canary": {
        "status": "idle",
        "evaluated": False,
        "ready_for_test": False,
        "test_blockers": [],
        "start_blockers": ["live-start-not-implemented"],
        "detail": "계정·증거금·포지션·미체결 주문을 fresh 조회한 뒤 test order만 허용합니다.",
    },
    "recovery_status": {"verified": False, "safeMode": True, "generation": 0, "detail": "복구 훈련 미실행"},
    "automatic_promotion_signatures": {},
    "audit": [
        {
            "time": "08:57:04",
            "level": "info",
            "event": "실거래 안전 게이트",
            "detail": "서명 주문 어댑터는 구현되어 있으며 환경 잠금·전략 승인·대조·리스크 게이트를 모두 통과해야 전송됩니다.",
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


SENSITIVE_SNAPSHOT_KEYS = {
    "authorization",
    "accesstoken",
    "refreshtoken",
    "token",
    "bottoken",
    "confirmationtoken",
    "signature",
    "queryhash",
    "apikey",
    "apisecret",
    "appkey",
    "appsecret",
    "accesskey",
    "secretkey",
    "xmbxapikey",
    "cano",
    "accountno",
    "accountnumber",
    "accountid",
    "htsid",
    "jwt",
}


@lru_cache(maxsize=512)
def _normalized_sensitive_key_text(value: str) -> str:
    return "".join(character for character in value.lower() if character.isalnum())


def normalized_sensitive_key(value: object) -> str:
    return _normalized_sensitive_key_text(str(value or ""))


def redact_url_query(value: str) -> str:
    if "://" not in value or "?" not in value:
        return value
    try:
        parsed = urlsplit(value)
    except ValueError:
        return value
    if not parsed.scheme or not parsed.netloc or not parsed.query:
        return value
    changed = False
    query: list[tuple[str, str]] = []
    for key, item in parse_qsl(parsed.query, keep_blank_values=True):
        if normalized_sensitive_key(key) in SENSITIVE_SNAPSHOT_KEYS:
            query.append((key, "***"))
            changed = True
        else:
            query.append((key, item))
    if not changed:
        return value
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query), parsed.fragment))


def redact_sensitive_payload(value: object, *, key_hint: str = "") -> object:
    normalized_key = normalized_sensitive_key(key_hint)
    if normalized_key in SENSITIVE_SNAPSHOT_KEYS:
        if value in (None, ""):
            return value
        return "***"
    if isinstance(value, dict):
        return {
            str(key): redact_sensitive_payload(item, key_hint=str(key))
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_sensitive_payload(item) for item in value]
    if isinstance(value, tuple):
        return [redact_sensitive_payload(item) for item in value]
    if isinstance(value, str):
        return redact_url_query(value)
    return value


def future_text(seconds: float) -> str:
    return (datetime.now() + timedelta(seconds=max(0.0, seconds))).strftime("%H:%M:%S")


def parse_state_datetime(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text or text in {"-", "미실행", "대기"}:
        return None
    if "T" in text:
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            return parsed.astimezone().replace(tzinfo=None) if parsed.tzinfo else parsed
        except ValueError:
            pass
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
        "active": False,
        "allowed": True,
        "detail": f"일치하는 Portfolio artifact가 없어 {strategy_id}/{symbol}을(를) 단일 전략으로 평가합니다.",
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
    metadata = intent.metadata if isinstance(intent.metadata, dict) else {}
    selected_portfolio_id = str(metadata.get("portfolio_id") or metadata.get("portfolioId") or "").strip()
    if selected_portfolio_id:
        portfolios = checks.get("portfolios") if isinstance(checks.get("portfolios"), list) else []
        selected_portfolio = next(
            (
                item
                for item in portfolios
                if isinstance(item, dict)
                and str(item.get("id") or item.get("portfolio_id") or item.get("portfolioId") or "") == selected_portfolio_id
            ),
            None,
        )
        if selected_portfolio is None:
            return {
                "active": True,
                "allowed": False,
                "portfolioId": selected_portfolio_id,
                "detail": f"명시적으로 선택한 Portfolio artifact {selected_portfolio_id}를 찾을 수 없습니다.",
            }
        selected_match = portfolio_match_for_strategy(
            selected_portfolio,
            intent.strategy_id,
            intent.symbol,
            mode=intent.mode,
        )
        if selected_match is None:
            return {
                "active": True,
                "allowed": False,
                "portfolioId": selected_portfolio_id,
                "detail": f"선택 Portfolio artifact에 {intent.strategy_id}/{intent.symbol} 조합이 없습니다.",
            }
        selected_gate = selected_match
    else:
        selected_gate = None
    strategy = strategy_for_order_intent(checks, intent)
    if selected_gate is None and strategy:
        gate = strategy.get("portfolio_gate") if isinstance(strategy.get("portfolio_gate"), dict) else None
        if gate is not None:
            selected_gate = dict(gate)
    if selected_gate is None:
        selected_gate = portfolio_gate_for_strategy(
            {"strategy_id": intent.strategy_id, "symbol": intent.symbol},
            checks.get("portfolios") if isinstance(checks.get("portfolios"), list) else [],
            mode=intent.mode,
        )
    policy = selected_gate.get("rebalancePolicy") if isinstance(selected_gate.get("rebalancePolicy"), dict) else {}
    if not policy or not selected_gate.get("active") or selected_gate.get("allowed") is not True:
        return selected_gate
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
    ArtifactMetadataStore().update(event["artifactId"], "strategy", mark_promoted=True)


CANARY_SCOPE_SCHEMA_VERSION = "live-canary-scope-v1"
CANARY_SCOPE_FIELDS = (
    "strategyId",
    "strategyArtifactId",
    "strategyArtifactHash",
    "strategyContentHash",
    "deploymentId",
    "deploymentRevision",
    "beforeLiveSmallAt",
)


def _canary_datetime(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.astimezone()
    return parsed.astimezone(timezone.utc)


def _live_deployment_id(normalized: dict[str, Any]) -> str:
    paper_evidence = (
        normalized.get("paper_portfolio_evidence")
        if isinstance(normalized.get("paper_portfolio_evidence"), dict)
        else {}
    )
    portfolio_id = str(paper_evidence.get("portfolioId") or "")
    return str(
        normalized.get("deployment_id")
        or (
            f"dep:{normalized.get('strategy_id')}:"
            f"{portfolio_id or 'standalone'}:live"
        )
    )


def current_live_canary_scope(
    strategy_id: str,
    *,
    materialize: bool = False,
    strategy_payload: dict[str, Any] | None = None,
    normalized: dict[str, Any] | None = None,
    deployment: dict[str, Any] | None = None,
) -> dict[str, Any]:
    target = str(strategy_id or "").strip()
    strategy_dir: Path | None = None
    payload = strategy_payload
    normalized_payload = normalized
    if payload is None or normalized_payload is None:
        (
            strategy_dir,
            _artifact_path,
            resolved_payload,
            resolved_normalized,
        ) = find_strategy_artifact_payload(target)
        payload = payload or resolved_payload
        normalized_payload = normalized_payload or resolved_normalized
    elif target:
        strategy_dir, _path, _payload, _normalized = find_strategy_artifact_payload(
            target
        )
    if (
        not target
        or strategy_dir is None
        or not isinstance(payload, dict)
        or not isinstance(normalized_payload, dict)
    ):
        return {
            "schemaVersion": CANARY_SCOPE_SCHEMA_VERSION,
            "eligible": False,
            "issues": ["strategy-artifact-context-missing"],
        }

    current_deployment = deployment
    if current_deployment is None:
        if materialize:
            _store, current_deployment, _portfolio = ensure_live_deployment(
                strategy_dir,
                payload,
                normalized_payload,
            )
        else:
            current_deployment = DeploymentStore(strategy_dir).get(
                _live_deployment_id(normalized_payload)
            )
    if not isinstance(current_deployment, dict):
        return {
            "schemaVersion": CANARY_SCOPE_SCHEMA_VERSION,
            "eligible": False,
            "issues": ["live-deployment-missing"],
        }

    current_reference = artifact_reference(payload)
    deployed_reference = (
        current_deployment.get("strategyArtifact")
        if isinstance(current_deployment.get("strategyArtifact"), dict)
        else {}
    )
    revision = int(safe_float(current_deployment.get("revision"), 0.0))
    entered_at = str(current_deployment.get("updatedAt") or "")
    issues: list[str] = []
    if normalize_lifecycle_status(current_deployment.get("lifecycle")) != "before-live-small":
        issues.append("deployment-not-before-live-small")
    if revision <= 0:
        issues.append("deployment-revision-missing")
    if _canary_datetime(entered_at) is None:
        issues.append("before-live-small-time-invalid")
    for key in ("artifactId", "artifactHash", "contentHash"):
        if (
            not str(deployed_reference.get(key) or "")
            or str(deployed_reference.get(key) or "")
            != str(current_reference.get(key) or "")
        ):
            issues.append(f"current-strategy-{key}-mismatch")

    scope = {
        "schemaVersion": CANARY_SCOPE_SCHEMA_VERSION,
        "strategyId": target,
        "strategyArtifactId": str(current_reference.get("artifactId") or ""),
        "strategyArtifactHash": str(current_reference.get("artifactHash") or ""),
        "strategyContentHash": str(current_reference.get("contentHash") or ""),
        "deploymentId": str(current_deployment.get("deploymentId") or ""),
        "deploymentRevision": revision,
        "beforeLiveSmallAt": entered_at,
        "eligible": not issues,
        "issues": issues,
    }
    scope["scopeId"] = hashlib.sha256(
        json.dumps(
            {
                key: scope.get(key)
                for key in CANARY_SCOPE_FIELDS
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    return scope


def _canary_scope_matches(
    order_scope: object,
    current_scope: dict[str, Any],
) -> bool:
    if not isinstance(order_scope, dict):
        return False
    if str(order_scope.get("schemaVersion") or "") != CANARY_SCOPE_SCHEMA_VERSION:
        return False
    return all(
        str(order_scope.get(key) or "")
        == str(current_scope.get(key) or "")
        for key in CANARY_SCOPE_FIELDS
    )


def live_small_execution_summary(
    strategy_id: str,
    *,
    strategy_payload: dict[str, Any] | None = None,
    normalized: dict[str, Any] | None = None,
    deployment: dict[str, Any] | None = None,
) -> dict[str, Any]:
    scope = current_live_canary_scope(
        strategy_id,
        strategy_payload=strategy_payload,
        normalized=normalized,
        deployment=deployment,
    )
    empty = {
        "successful": 0,
        "blocked": 0,
        "fills": 0,
        "scope": scope,
    }
    if scope.get("eligible") is not True:
        return empty
    boundary = _canary_datetime(scope.get("beforeLiveSmallAt"))
    if boundary is None:
        return empty

    scoped_orders: dict[tuple[str, str], dict[str, Any]] = {}
    for order in STATE["orders"]:
        if (
            str(order.get("strategy_id") or "") != str(strategy_id)
            or bool(order.get("dry_run"))
            or str(order.get("mode") or "").upper() != "SMALL_LIVE"
            or not _canary_scope_matches(order.get("canary_scope"), scope)
        ):
            continue
        broker_id = str(order.get("broker_id") or "").strip().lower()
        broker_order_id = str(order.get("broker_order_id") or "").strip()
        created_at = _canary_datetime(
            order.get("created_at") or order.get("time")
        )
        if (
            not broker_id
            or broker_order_id in {"", "-"}
            or created_at is None
            or created_at < boundary
        ):
            continue
        scoped_orders[(broker_id, broker_order_id)] = order

    fill_identities: set[tuple[str, str]] = set()
    rejected_identities: set[tuple[str, str]] = set()
    for event in PROGRAM_LEDGER.execution_event_rows(5000):
        event_id = str(event.get("event_id") or "").strip()
        broker_id = str(event.get("broker_id") or "").strip().lower()
        broker_order_id = str(event.get("broker_order_id") or "").strip()
        identity = (broker_id, broker_order_id)
        if (
            not event_id
            or broker_order_id in {"", "-"}
            or identity not in scoped_orders
        ):
            continue
        occurred_at = _canary_datetime(event.get("occurred_at"))
        if occurred_at is None or occurred_at < boundary:
            continue
        state_name = str(event.get("state") or "").strip().lower()
        raw_event = (
            event.get("raw")
            if isinstance(event.get("raw"), dict)
            else {}
        )
        filled_quantity = max(
            safe_float(event.get("quantity"), 0.0),
            safe_float(
                raw_event.get("reported_cumulative_quantity"),
                0.0,
            ),
        )
        if (
            state_name in {"filled", "done", "executed"}
            and filled_quantity > 0
        ):
            # Count one canary per broker order, even when a broker emits
            # multiple partial-fill/update events for the same order.
            fill_identities.add(identity)
        elif state_name in {"rejected", "expired", "failed", "canceled"}:
            rejected_identities.add(identity)
    return {
        "successful": len(fill_identities),
        "blocked": len(rejected_identities),
        "fills": len(fill_identities),
        "scope": scope,
    }


def automation_profiles(strategies: list[dict[str, Any]] | None = None, brokers: list[dict[str, object]] | None = None) -> list[dict[str, Any]]:
    strategies = strategies if strategies is not None else strategy_rows()
    brokers = brokers if brokers is not None else [broker.to_dict() for broker in broker_readiness()]
    broker_map = {str(broker["broker_id"]): broker for broker in brokers}
    stock_strategies = [strategy for strategy in strategies if strategy_broker_id(strategy) == "kis"]
    crypto_strategies = [
        strategy
        for strategy in strategies
        if strategy_broker_id(strategy)
        in {"binance", "binance-futures", "upbit"}
    ]
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
            "sample_request": redact_sensitive_payload(build_adapter_preview("kis", stock_strategies)),
        },
        {
            "id": "crypto",
            "title": "코인 자동화",
            "provider": crypto_provider,
            "provider_label": (
                "Binance USD-M Futures"
                if crypto_provider == "binance-futures"
                else "Binance API"
                if crypto_provider == "binance"
                else "Upbit API"
            ),
            "broker_ids": ["binance", "binance-futures", "upbit"],
            "asset_scope": [
                "Binance 현물",
                "Binance USD-M 선물",
                "Upbit KRW 마켓",
            ],
            "mode": str(STATE["automation"]["crypto"].get("mode") or "MONITOR"),
            "enabled": bool(STATE["automation"]["crypto"]["enabled"]),
            "last_action": STATE["automation"]["crypto"]["last_action"],
            "ready": bool(broker_map.get(crypto_provider, {}).get("order_ready")),
            "strategy_count": len(crypto_strategies),
            "live_strategy_count": sum(1 for strategy in crypto_strategies if strategy.get("live_allowed")),
            "full_live_strategy_count": sum(1 for strategy in crypto_strategies if strategy.get("live_eligible")),
            "detail": "선택한 코인 거래소 API로 코인 주문을 라우팅합니다.",
            "sample_request": redact_sensitive_payload(build_adapter_preview(crypto_provider, crypto_strategies)),
        },
    ]


def broker_id_from_hint(value: object) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    if "upbit" in text:
        return "upbit"
    if "binance" in text and (
        "future" in text
        or "perpetual" in text
        or "usd-m" in text
    ):
        return "binance-futures"
    if "binance" in text:
        return "binance"
    if text == "kis" or "korea investment" in text or "한국투자" in text:
        return "kis"
    return ""


def strategy_broker_id(strategy: dict[str, Any]) -> str:
    dataset = strategy.get("dataset") if isinstance(strategy.get("dataset"), dict) else {}
    data_artifact = (
        strategy.get("dataArtifact")
        if isinstance(strategy.get("dataArtifact"), dict)
        else strategy.get("data_artifact")
        if isinstance(strategy.get("data_artifact"), dict)
        else {}
    )
    trader_contract = (
        strategy.get("traderContract")
        if isinstance(strategy.get("traderContract"), dict)
        else strategy.get("trader_contract")
        if isinstance(strategy.get("trader_contract"), dict)
        else {}
    )
    scope = trader_contract.get("scope") if isinstance(trader_contract.get("scope"), dict) else {}
    market_type = str(
        strategy.get("marketType")
        or strategy.get("market_type")
        or dataset.get("marketType")
        or dataset.get("market_type")
        or data_artifact.get("marketType")
        or data_artifact.get("market_type")
        or ""
    ).strip().lower()
    if market_type in {"future", "futures", "perpetual"}:
        return "binance-futures"

    for hint in (
        dataset.get("provider"),
        strategy.get("dataset_provider"),
        strategy.get("marketDataProvider"),
        strategy.get("market_data_provider"),
        data_artifact.get("marketDataProvider"),
        data_artifact.get("provider"),
        strategy.get("brokerId"),
        strategy.get("broker_id"),
        scope.get("brokerId"),
    ):
        broker_id = broker_id_from_hint(hint)
        if broker_id:
            return broker_id

    raw_allowed = (
        scope.get("allowed_brokers")
        or scope.get("allowedBrokers")
        or strategy.get("allowed_brokers")
        or strategy.get("allowedBrokers")
        or []
    )
    allowed = {
        broker_id
        for item in raw_allowed
        if (broker_id := broker_id_from_hint(item))
    } if isinstance(raw_allowed, (list, tuple, set)) else set()
    if len(allowed) == 1:
        return next(iter(allowed))

    symbol = str(strategy.get("symbol") or dataset.get("symbol") or data_artifact.get("symbol") or "").strip().upper()
    if symbol.startswith("KRW-"):
        return "upbit"
    top_level_provider = broker_id_from_hint(strategy.get("provider"))
    if top_level_provider:
        return top_level_provider
    asset = f"{strategy.get('asset', '')} {symbol}".lower()
    if any(token in asset for token in ("crypto", "coin", "btc", "eth", "usdt", "코인")):
        return "binance"
    return "kis"


def build_adapter_preview(provider: str, strategies: list[dict[str, Any]]) -> dict[str, Any]:
    strategy = next((item for item in strategies if item.get("live_allowed")), strategies[0] if strategies else {})
    symbol = str(
        strategy.get("symbol")
        or (
            "BTCUSDT.PERP"
            if provider == "binance-futures"
            else "BTCUSDT"
            if provider == "binance"
            else "KRW-BTC"
            if provider == "upbit"
            else "069500.KS"
        )
    )
    local_code = symbol.upper().removesuffix(".KS").removesuffix(".KQ")
    order_type = (
        "MARKET"
        if provider in {"binance", "binance-futures"}
        else "price"
        if provider == "upbit"
        else "01"
        if local_code.isdigit() and len(local_code) == 6
        else "00"
    )
    intent = {
        "broker_id": provider,
        "strategy_id": strategy.get("strategy_id", "sample"),
        "symbol": symbol,
        "market": symbol if provider == "upbit" else "",
        "asset": strategy.get("asset", ""),
        "side": "BUY",
        "quantity": 1,
        "price": (
            0
            if provider == "kis" and order_type == "01"
            else 1000
            if provider not in {"binance", "binance-futures"}
            else 1
        ),
        "order_type": order_type,
    }
    if provider == "kis":
        return build_kis_live_order_request(intent).preview()
    if provider == "upbit":
        return build_upbit_order_request(intent).preview()
    if provider == "binance-futures":
        return build_binance_futures_order_request(
            {
                **intent,
                "position_direction": "short",
            },
            hedge_mode=False,
            test=True,
        ).preview()
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
    missing_brokers = [broker for broker in brokers if broker["status"] == "missing_credentials"]
    adapter_blocked = [broker for broker in brokers if broker["live_order_adapter_ready"] is not True]
    checklist = checklist_rows(reconciliation_summary)
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
    active_live = live_exposure_active()
    market_data_age = seconds_since(STATE["strategy_runner"].get("last_run"))
    stale_limit = int(
        float(
            STATE["watchdog"].get("settings", DEFAULT_WATCHDOG_SETTINGS).get(
                "market_data_stale_sec",
                DEFAULT_WATCHDOG_SETTINGS["market_data_stale_sec"],
            )
        )
    )
    if not active_live:
        data_delay_status: CheckStatus = "pass"
        data_delay_value = "MONITOR"
        data_delay_detail = "실거래 자동화가 비활성이라 데이터 신선도 경고를 적용하지 않습니다."
    elif market_data_age is None:
        data_delay_status = "warn"
        data_delay_value = "미실행"
        data_delay_detail = "활성 자동화의 최근 완료 봉 처리 이력이 없습니다."
    elif market_data_age > stale_limit:
        data_delay_status = "fail"
        data_delay_value = f"{market_data_age}초"
        data_delay_detail = f"시장 데이터가 허용 지연 {stale_limit}초를 넘었습니다."
    else:
        data_delay_status = "pass"
        data_delay_value = f"{market_data_age}초"
        data_delay_detail = f"시장 데이터가 허용 지연 {stale_limit}초 안에 있습니다."
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
        {
            "label": "데이터 지연",
            "value": data_delay_value,
            "status": data_delay_status,
            "detail": data_delay_detail,
        },
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


def normalized_reconciliation_position_side(
    item: dict[str, Any],
    *,
    source: str,
) -> str:
    broker_id = str(item.get("broker_id") or "").strip().lower()
    if broker_id != "binance-futures":
        return ""
    value = str(
        item.get("position_side")
        or item.get("positionSide")
        or ""
    ).strip().upper()
    if value in {"BOTH", "LONG", "SHORT"}:
        return value
    return "LEGACY" if source == "program" else "UNSPECIFIED"


def position_reconciliation_key(
    item: dict[str, Any],
    *,
    source: str,
) -> tuple[str, str, str]:
    return (
        str(item.get("broker_id") or ""),
        str(item.get("symbol") or ""),
        normalized_reconciliation_position_side(item, source=source),
    )


def live_position_rows() -> dict[tuple[str, str, str], dict[str, object]]:
    rows = STATE.get("broker_reconciliation", {}).get("positions", [])
    if not isinstance(rows, list):
        return {}
    return {
        position_reconciliation_key(item, source="broker"): item
        for item in rows
        if isinstance(item, dict) and item.get("broker_id") and item.get("symbol")
    }


def program_cash_rows() -> dict[str, dict[str, Any]]:
    return {str(item.get("broker_id")): item for item in PROGRAM_LEDGER.cash_rows() if item.get("broker_id")}


def program_position_rows() -> dict[tuple[str, str, str], dict[str, Any]]:
    return {
        position_reconciliation_key(item, source="program"): item
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
        "observed_cash_count": int(state.get("observed_cash_count", 0)),
        "observed_position_count": int(state.get("observed_position_count", 0)),
        "snapshot_changed_brokers": list(state.get("snapshot_changed_brokers", [])),
        "snapshot_skipped_brokers": list(state.get("snapshot_skipped_brokers", [])),
        "snapshot_poll": redact_sensitive_payload(STATE.get("broker_snapshot_poll", {})),
        "recent": PROGRAM_LEDGER.execution_event_rows(20),
        "streams": LIVE_EXECUTION_STREAMS.snapshot(),
    }


def broker_position_truth_snapshot(reconciliation: dict[str, Any] | None = None) -> dict[str, Any]:
    def truth_symbol(item: dict[str, Any], source: str) -> str:
        symbol = str(item.get("symbol") or "")
        side = normalized_reconciliation_position_side(item, source=source)
        return f"{symbol}::{side}" if side else symbol

    expected = [
        PositionTruth(
            broker_id=str(item.get("broker_id") or ""),
            symbol=truth_symbol(item, "program"),
            quantity=safe_float(item.get("quantity"), 0.0),
            captured_at=str(item.get("updated_at") or ""),
        )
        for item in PROGRAM_LEDGER.position_rows()
        if item.get("broker_id") and item.get("symbol")
    ]
    broker = [
        PositionTruth(
            broker_id=str(item.get("broker_id") or ""),
            symbol=truth_symbol(item, "broker"),
            quantity=safe_float(item.get("broker_qty", item.get("quantity")), 0.0),
            captured_at=str(item.get("updated_at") or STATE.get("broker_reconciliation", {}).get("fetched_at") or ""),
        )
        for item in STATE.get("broker_reconciliation", {}).get("positions", [])
        if isinstance(item, dict) and item.get("broker_id") and item.get("symbol")
    ]
    report = reconcile_broker_truth(expected=expected, broker=broker)
    reconciliation = reconciliation or reconciliation_snapshot()
    summary = reconciliation.get("summary") if isinstance(reconciliation.get("summary"), dict) else {}
    api_required = int(summary.get("api_required_count") or 0)
    raw_mismatches = int(summary.get("mismatch_count") or 0)
    effective_mismatches = max(int(report.get("mismatchCount") or 0), api_required + raw_mismatches)
    if effective_mismatches:
        report = {
            **report,
            "matched": False,
            "newEntriesBlocked": True,
            "mismatchCount": effective_mismatches,
            "effectiveBlockers": [
                *(["broker-api-snapshot-required"] if api_required else []),
                *(["program-broker-position-mismatch"] if raw_mismatches else []),
            ],
        }
    return report


def restart_recovery_plan_snapshot(
    reconciliation: dict[str, Any],
    continuous_runtime: dict[str, Any],
    broker_truth: dict[str, Any],
) -> dict[str, Any]:
    unknown_orders = [
        str(item.get("order_id") or item.get("oms_order_id") or "")
        for item in STATE.get("orders", [])
        if str(item.get("state") or "").lower() in {"unknown", "acknowledged", "submitted"}
        or str(item.get("queue_state") or "").lower() in {"reconcile_required", "submitted"}
    ]
    gap_count = int(continuous_runtime.get("gapCount") or continuous_runtime.get("gap_count") or 0)
    return build_restart_recovery_plan(
        checkpoint_valid=bool(STATE.get("recovery_status", {}).get("verified")),
        unknown_order_ids=unknown_orders,
        reconciliation={"mismatchCount": int(broker_truth.get("mismatchCount") or 0)},
        missing_bar_ids=[f"continuous-gap-{index + 1}" for index in range(min(gap_count, 100))],
        duplicate_keys_loaded=isinstance(STATE.get("persisted_idempotency_keys"), list),
    )


def successful_position_brokers() -> set[str]:
    rows = STATE.get("broker_reconciliation", {}).get("successful_position_brokers", [])
    return {str(item) for item in rows} if isinstance(rows, list) else set()


RESTORE_CONTEXT_MAX_AGE_SECONDS = 30.0
RESTORE_CONTEXT_LOCK = threading.RLock()


def _restore_context_hash(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _restore_account_scope(broker_id: str) -> str:
    normalized = str(broker_id or "").strip().lower()
    if normalized == "kis":
        configured = ":".join(
            (
                os.getenv("KIS_ACCOUNT_NO", ""),
                os.getenv("KIS_ACCOUNT_PRODUCT_CODE", ""),
            )
        )
    elif normalized in {"binance", "binance-futures"}:
        configured = os.getenv("BINANCE_API_KEY", "")
    elif normalized == "upbit":
        configured = os.getenv("UPBIT_ACCESS_KEY", "")
    else:
        configured = ""
    opaque = configured or f"{normalized}:configured-account"
    return f"{normalized}:{hashlib.sha256(opaque.encode('utf-8')).hexdigest()[:16]}"


def _canonical_restore_instrument(broker_id: str, symbol: str) -> str:
    normalized_broker = str(broker_id or "").strip().lower()
    text = str(symbol or "").strip().upper().replace(" ", "")
    if normalized_broker == "kis":
        return text.removesuffix(".KS").removesuffix(".KQ")
    if normalized_broker == "upbit":
        compact = text.replace("_", "-")
        if "-" not in compact and compact.startswith("KRW") and len(compact) > 3:
            compact = f"KRW-{compact[3:]}"
        return compact
    if normalized_broker in {"binance", "binance-futures"}:
        compact = text.replace("-", "").replace("_", "")
        for quote in ("USDT", "USDC", "FDUSD", "BUSD", "TUSD", "BTC", "ETH"):
            if compact.endswith(quote) and len(compact) > len(quote):
                return compact[: -len(quote)]
        return compact
    return text.replace("-", "").replace("_", "")


def _restore_spec_route(spec: Any) -> dict[str, Any]:
    broker_id = str(getattr(spec, "broker_id", "") or "").strip().lower()
    symbol = str(getattr(spec, "symbol", "") or "").strip().upper()
    canonical = _canonical_restore_instrument(broker_id, symbol)
    exchange = ""
    capability_known = bool(broker_id and canonical)
    capability_reason = ""
    if broker_id == "kis":
        if symbol.endswith((".KS", ".KQ")) or canonical.isdigit():
            exchange = "KRX"
        else:
            exchange = str(
                getattr(spec, "parameters", {}).get("exchange")
                or getattr(spec, "artifact", {}).get("exchange")
                or "NASD"
            ).upper()
    elif broker_id in {"binance", "binance-futures"}:
        exchange = (
            "BINANCE_FUTURES"
            if broker_id == "binance-futures"
            else "BINANCE_SPOT"
        )
    elif broker_id == "upbit":
        exchange = "UPBIT_SPOT"
    else:
        capability_known = False
        capability_reason = f"unsupported-broker:{broker_id or 'missing'}"
    spec_payload = spec.to_dict() if callable(getattr(spec, "to_dict", None)) else {
        "strategyInstanceId": str(getattr(spec, "strategy_instance_id", "") or ""),
        "artifactHash": str(getattr(spec, "artifact_hash", "") or ""),
        "brokerId": broker_id,
        "symbol": symbol,
    }
    return {
        "strategyInstanceId": str(getattr(spec, "strategy_instance_id", "") or ""),
        "artifactHash": str(getattr(spec, "artifact_hash", "") or ""),
        "brokerId": broker_id,
        "accountScope": _restore_account_scope(broker_id),
        "exchange": exchange,
        "canonicalInstrument": canonical,
        "specIdentityHash": _restore_context_hash(spec_payload),
        "capabilityKnown": capability_known,
        "capabilityReason": capability_reason,
    }


def _restore_quantity(row: dict[str, Any]) -> float:
    return safe_float(
        row.get("broker_qty"),
        safe_float(row.get("quantity"), safe_float(row.get("qty"), 0.0)),
    )


def _restore_order_is_pending_or_unknown(order: dict[str, Any]) -> bool:
    state_name = str(order.get("state") or "").strip().lower()
    queue_state = str(order.get("queue_state") or "").strip().lower()
    terminal_states = {
        "dry_run",
        "filled",
        "canceled",
        "cancelled",
        "expired",
        "retry_exhausted",
        "adapter_blocked",
        "risk_blocked",
        "broker_rejected",
        "rejected",
        "failed",
    }
    terminal_queues = {
        "filled",
        "canceled",
        "cancelled",
        "completed",
        "expired",
        "failed",
        "blocked",
        "risk_blocked",
    }
    return state_name not in terminal_states and queue_state not in terminal_queues


def _restore_evaluator_contexts(
    evaluator_state: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    contexts: dict[str, dict[str, Any]] = {}
    entries = evaluator_state.get("entries")
    for entry in entries if isinstance(entries, list) else []:
        if not isinstance(entry, dict):
            continue
        identity = entry.get("specIdentity")
        runtime_state = entry.get("state")
        if not isinstance(identity, dict) or not isinstance(runtime_state, dict):
            continue
        instance_id = str(identity.get("strategyInstanceId") or "")
        if instance_id:
            contexts[instance_id] = dict(runtime_state)
    return contexts


def _restore_context_seal_matches(
    saved_seal: Any,
    current_seal: dict[str, Any],
    *,
    current_positions_are_flat: bool,
    position_context_complete: bool,
) -> bool:
    if not isinstance(saved_seal, dict):
        return False
    saved_body = saved_seal.get("body")
    if not isinstance(saved_body, dict):
        return False
    if (
        str(saved_seal.get("bodyHash") or "") != _restore_context_hash(saved_body)
        or str(current_seal.get("bodyHash") or "")
        != _restore_context_hash(current_seal.get("body"))
    ):
        return False
    if saved_seal.get("bodyHash") != current_seal.get("bodyHash"):
        return False
    # Existing positions require a broker-verifiable fill/order generation.
    # The current list_positions adapters expose quantity (and sometimes
    # average price), but no immutable fill identity.  Until that capability
    # exists, restart with exposure is deliberately liquidation-only.
    return current_positions_are_flat or position_context_complete


def _publish_forced_restore_positions(
    broker_rows_by_id: dict[str, list[dict[str, Any]]],
    *,
    observed_at: str,
) -> None:
    """Atomically replace only brokers proven by the forced read.

    The assessment never *reads* this cache as recovery evidence.  Publishing
    its successful result is nevertheless required so the evaluator and
    pre-trade reconciliation immediately observe the same broker truth used
    for the mode transition.
    """

    if not broker_rows_by_id:
        return
    successful = set(broker_rows_by_id)
    cache = STATE.setdefault("broker_reconciliation", {})
    previous_positions = (
        cache.get("positions", [])
        if isinstance(cache.get("positions"), list)
        else []
    )
    replacement_rows = [
        {
            **row,
            "broker_id": broker_id,
            "observed_at": observed_at,
        }
        for broker_id, rows in broker_rows_by_id.items()
        for row in rows
    ]
    cache["positions"] = [
        row
        for row in previous_positions
        if isinstance(row, dict)
        and str(row.get("broker_id") or "").strip().lower()
        not in successful
    ] + replacement_rows
    previous_success = cache.get("successful_position_brokers", [])
    cache["successful_position_brokers"] = sorted(
        {
            str(item).strip().lower()
            for item in previous_success
            if str(item).strip()
        }
        | successful
    )
    observations = (
        dict(cache.get("position_observations"))
        if isinstance(cache.get("position_observations"), dict)
        else {}
    )
    for broker_id, rows in broker_rows_by_id.items():
        observations[broker_id] = {
            "observedAt": observed_at,
            "generation": _restore_context_hash({
                "brokerId": broker_id,
                "positions": sorted(
                    [
                        {
                            "symbol": str(row.get("symbol") or ""),
                            "positionSide": normalized_reconciliation_position_side(
                                row,
                                source="broker",
                            ),
                            "quantity": _restore_quantity(row),
                            "averagePrice": safe_float(
                                row.get("average_price"),
                                0.0,
                            ),
                        }
                        for row in rows
                    ],
                    key=lambda item: (
                        item["symbol"],
                        item["positionSide"],
                    ),
                ),
            }),
        }
    cache["position_observations"] = observations
    cache["fetched_at"] = observed_at


def forced_restore_context_assessment(
    specs: tuple[Any, ...],
    *,
    portfolio_id: str,
    portfolio_hash: str,
    strategy_identity_hash: str,
    checkpoint_seal: dict[str, Any] | None,
    evaluator_state: dict[str, Any] | None,
) -> dict[str, Any]:
    """Force fresh broker truth and build an exact restart context seal.

    This path never trusts the long-lived reconciliation cache.  Every
    required broker is queried synchronously while RESTORE_CONTEXT_LOCK is
    held, then compared with the program ledger and in-flight order set at
    exact broker/account/exchange/instrument scope.
    """

    resolved_specs = tuple(specs)
    routes = [_restore_spec_route(spec) for spec in resolved_specs]
    broker_ids = sorted(
        {
            str(route.get("brokerId") or "")
            for route in routes
            if str(route.get("brokerId") or "")
        }
    )
    started_monotonic = time.monotonic()
    observed_at = datetime.now(timezone.utc).isoformat(
        timespec="milliseconds"
    ).replace("+00:00", "Z")
    broker_rows_by_id: dict[str, list[dict[str, Any]]] = {}
    errors: list[str] = []
    with RESTORE_CONTEXT_LOCK:
        router = LiveBrokerRouter()
        for broker_id in broker_ids:
            try:
                rows = router.list_positions(broker_id)
                if not isinstance(rows, list):
                    raise BrokerNotReadyError(
                        "position snapshot response is not a list"
                    )
                broker_rows_by_id[broker_id] = [
                    dict(item) for item in rows if isinstance(item, dict)
                ]
            except Exception as exc:
                errors.append(
                    f"{broker_id}:{type(exc).__name__}:{audit_clip(str(exc), 160)}"
                )
        elapsed_seconds = max(0.0, time.monotonic() - started_monotonic)
        observed_at = datetime.now(timezone.utc).isoformat(
            timespec="milliseconds"
        ).replace("+00:00", "Z")
        _publish_forced_restore_positions(
            broker_rows_by_id,
            observed_at=observed_at,
        )

        try:
            ledger_positions = [
                dict(item) for item in PROGRAM_LEDGER.position_rows()
                if isinstance(item, dict)
            ]
            ledger_events = [
                dict(item) for item in PROGRAM_LEDGER.execution_event_rows(500)
                if isinstance(item, dict)
            ]
        except Exception as exc:
            ledger_positions = []
            ledger_events = []
            errors.append(
                "program-ledger:"
                f"{type(exc).__name__}:{audit_clip(str(exc), 160)}"
            )
        all_orders = [
            dict(item) for item in STATE.get("orders", [])
            if isinstance(item, dict)
        ]

    route_keys = {
        (
            str(route["brokerId"]),
            str(route["canonicalInstrument"]),
        )
        for route in routes
    }

    pending_orders: list[dict[str, Any]] = []
    for order in all_orders:
        if not _restore_order_is_pending_or_unknown(order):
            continue
        broker_request = (
            order.get("broker_request")
            if isinstance(order.get("broker_request"), dict)
            else {}
        )
        broker_id = str(
            order.get("broker_id")
            or broker_request.get("broker_id")
            or ""
        ).strip().lower()
        symbol = str(order.get("symbol") or broker_request.get("symbol") or "")
        if not broker_id or not symbol:
            pending_orders.append(order)
            continue
        if (
            broker_id,
            _canonical_restore_instrument(broker_id, symbol),
        ) in route_keys:
            pending_orders.append(order)

    evaluator_payload = (
        dict(evaluator_state) if isinstance(evaluator_state, dict) else {}
    )
    evaluator_contexts = _restore_evaluator_contexts(evaluator_payload)
    spec_contexts: list[dict[str, Any]] = []
    broker_generation_rows: list[dict[str, Any]] = []
    ledger_generation_rows: list[dict[str, Any]] = []
    ledger_generation_events: list[dict[str, Any]] = []
    all_broker_flat = True
    all_ledger_flat = True
    has_known_position = False
    all_position_context_complete = True

    for route in routes:
        broker_id = str(route["brokerId"])
        canonical = str(route["canonicalInstrument"])
        matching_broker = [
            row
            for row in broker_rows_by_id.get(broker_id, [])
            if _canonical_restore_instrument(
                broker_id,
                str(row.get("symbol") or ""),
            ) == canonical
        ]
        matching_ledger = [
            row
            for row in ledger_positions
            if str(row.get("broker_id") or "").strip().lower() == broker_id
            and _canonical_restore_instrument(
                broker_id,
                str(row.get("symbol") or ""),
            ) == canonical
        ]
        matching_events = [
            event
            for event in ledger_events
            if str(event.get("broker_id") or "").strip().lower() == broker_id
            and _canonical_restore_instrument(
                broker_id,
                str(event.get("symbol") or ""),
            ) == canonical
        ]
        active_broker_sides = {
            normalized_reconciliation_position_side(
                row,
                source="broker",
            )
            for row in matching_broker
            if abs(_restore_quantity(row)) > 1e-12
        }
        dual_side_ambiguous = (
            broker_id == "binance-futures"
            and len(active_broker_sides) > 1
        )
        if dual_side_ambiguous:
            error = (
                "binance-futures:dual-side-position-ambiguous:"
                f"{canonical}:{','.join(sorted(active_broker_sides))}"
            )
            if error not in errors:
                errors.append(error)
        broker_qty = sum(_restore_quantity(row) for row in matching_broker)
        ledger_qty = sum(
            safe_float(row.get("quantity"), 0.0)
            for row in matching_ledger
        )
        tolerance = (
            1e-12
            if broker_id in {"binance", "binance-futures", "upbit"}
            else 0.0
        )
        broker_flat = (
            abs(broker_qty) <= tolerance
            and not dual_side_ambiguous
        )
        ledger_flat = abs(ledger_qty) <= tolerance
        all_broker_flat = all_broker_flat and broker_flat
        all_ledger_flat = all_ledger_flat and ledger_flat
        has_known_position = has_known_position or not broker_flat or not ledger_flat

        latest_fill = next(
            (
                event
                for event in matching_events
                if str(event.get("state") or "").lower() == "filled"
                and str(event.get("side") or "").upper() in {"BUY", "SELL"}
            ),
            {},
        )
        evaluator_context = evaluator_contexts.get(
            str(route["strategyInstanceId"]),
            {},
        )
        position_context_complete = broker_flat and ledger_flat
        if not position_context_complete:
            position_context_complete = bool(
                evaluator_context.get("lastHasPosition") is True
                and evaluator_context.get("entryContextKnown") is True
                and safe_float(evaluator_context.get("entryPrice"), 0.0) > 0
                and isinstance(evaluator_context.get("barsHeld"), int)
                and isinstance(evaluator_context.get("evaluationCount"), int)
                and str(evaluator_context.get("lastBarKey") or "")
                and str(
                    latest_fill.get("broker_order_id")
                    or latest_fill.get("order_id")
                    or ""
                )
                and safe_float(latest_fill.get("quantity"), 0.0) > 0
                and safe_float(latest_fill.get("price"), 0.0) > 0
                and str(latest_fill.get("occurred_at") or "")
            )
            # list_positions has no immutable fill/order generation.  Even a
            # locally complete context cannot prove that an external
            # sell/re-buy of the same quantity did not occur during downtime.
            position_context_complete = False
        all_position_context_complete = (
            all_position_context_complete and position_context_complete
        )

        broker_generation_rows.extend(
            {
                "brokerId": broker_id,
                "accountScope": route["accountScope"],
                "exchange": route["exchange"],
                "canonicalInstrument": canonical,
                "positionSide": normalized_reconciliation_position_side(
                    row,
                    source="broker",
                ),
                "quantity": _restore_quantity(row),
                "averagePrice": safe_float(row.get("average_price"), 0.0),
            }
            for row in matching_broker
        )
        ledger_generation_rows.extend(
            {
                "brokerId": broker_id,
                "canonicalInstrument": canonical,
                "positionSide": normalized_reconciliation_position_side(
                    row,
                    source="program",
                ),
                "quantity": safe_float(row.get("quantity"), 0.0),
                "value": safe_float(row.get("value"), 0.0),
                "updatedAt": str(row.get("updated_at") or ""),
                "source": str(row.get("source") or ""),
            }
            for row in matching_ledger
        )
        ledger_generation_events.extend(
            {
                "eventId": str(event.get("event_id") or ""),
                "orderId": str(event.get("order_id") or ""),
                "brokerOrderId": str(event.get("broker_order_id") or ""),
                "brokerId": broker_id,
                "canonicalInstrument": canonical,
                "side": str(event.get("side") or ""),
                "quantity": safe_float(event.get("quantity"), 0.0),
                "price": safe_float(event.get("price"), 0.0),
                "state": str(event.get("state") or ""),
                "occurredAt": str(event.get("occurred_at") or ""),
            }
            for event in matching_events
        )
        spec_contexts.append(
            {
                **route,
                "brokerQuantity": broker_qty,
                "programQuantity": ledger_qty,
                "evaluatorContext": evaluator_context,
                "latestFill": {
                    "eventId": str(latest_fill.get("event_id") or ""),
                    "orderId": str(latest_fill.get("order_id") or ""),
                    "brokerOrderId": str(
                        latest_fill.get("broker_order_id") or ""
                    ),
                    "quantity": safe_float(latest_fill.get("quantity"), 0.0),
                    "price": safe_float(latest_fill.get("price"), 0.0),
                    "occurredAt": str(latest_fill.get("occurred_at") or ""),
                },
                "positionContextComplete": position_context_complete,
                "dualSideAmbiguous": dual_side_ambiguous,
            }
        )

    broker_generation = _restore_context_hash(
        sorted(
            broker_generation_rows,
            key=lambda item: (
                item["brokerId"],
                item["canonicalInstrument"],
                item["positionSide"],
                item["quantity"],
            ),
        )
    )
    ledger_generation = _restore_context_hash(
        {
            "positions": sorted(
                ledger_generation_rows,
                key=lambda item: (
                    item["brokerId"],
                    item["canonicalInstrument"],
                    item["positionSide"],
                ),
            ),
            "fills": sorted(
                ledger_generation_events,
                key=lambda item: (
                    item["occurredAt"],
                    item["eventId"],
                ),
            ),
        }
    )
    order_generation = _restore_context_hash(
        sorted(
            [
                {
                    "orderId": str(item.get("order_id") or ""),
                    "brokerId": str(item.get("broker_id") or ""),
                    "symbol": str(item.get("symbol") or ""),
                    "state": str(item.get("state") or ""),
                    "queueState": str(item.get("queue_state") or ""),
                    "brokerOrderId": str(item.get("broker_order_id") or ""),
                }
                for item in pending_orders
            ],
            key=lambda item: (
                item["brokerId"],
                item["symbol"],
                item["orderId"],
            ),
        )
    )
    context_body = {
        "schemaVersion": "live-restore-context-v1",
        "portfolioId": portfolio_id,
        "portfolioHash": portfolio_hash,
        "strategyIdentityHash": strategy_identity_hash,
        "brokerSnapshotGeneration": broker_generation,
        "programLedgerGeneration": ledger_generation,
        "orderGeneration": order_generation,
        "pendingOrUnknownOrderCount": len(pending_orders),
        "evaluatorStateHash": _restore_context_hash(evaluator_payload),
        "specs": sorted(
            spec_contexts,
            key=lambda item: (
                str(item["strategyInstanceId"]),
                str(item["brokerId"]),
                str(item["canonicalInstrument"]),
            ),
        ),
    }
    current_seal = {
        "schemaVersion": "live-restore-context-seal-v1",
        "body": context_body,
        "bodyHash": _restore_context_hash(context_body),
    }
    all_specs_known = bool(routes) and all(
        route.get("capabilityKnown") is True for route in routes
    )
    snapshot_complete = (
        all_specs_known
        and not errors
        and set(broker_rows_by_id) == set(broker_ids)
        and elapsed_seconds <= RESTORE_CONTEXT_MAX_AGE_SECONDS
    )
    all_flat = snapshot_complete and all_broker_flat
    program_ledger_flat = all_ledger_flat
    context_seal_valid = (
        snapshot_complete
        and len(pending_orders) == 0
        and _restore_context_seal_matches(
            checkpoint_seal,
            current_seal,
            current_positions_are_flat=all_flat and program_ledger_flat,
            position_context_complete=all_position_context_complete,
        )
    )
    reasons = [
        *errors,
        *[
            str(route.get("capabilityReason") or "")
            for route in routes
            if route.get("capabilityKnown") is not True
        ],
    ]
    if elapsed_seconds > RESTORE_CONTEXT_MAX_AGE_SECONDS:
        reasons.append(
            f"forced-position-read-timeout:{elapsed_seconds:.3f}s"
        )
    if pending_orders:
        reasons.append(f"pending-or-unknown-orders:{len(pending_orders)}")
    return {
        "schemaVersion": "live-restore-attestation-v1",
        "portfolioId": portfolio_id,
        "portfolioHash": portfolio_hash,
        "strategyIdentityHash": strategy_identity_hash,
        "observedAt": observed_at,
        "ageSeconds": 0.0,
        "fresh": snapshot_complete,
        "allSpecsKnown": all_specs_known,
        "brokerSnapshotComplete": snapshot_complete,
        "allFlat": all_flat,
        "programLedgerFlat": program_ledger_flat,
        "hasKnownPosition": has_known_position,
        "pendingOrUnknownOrderCount": len(pending_orders),
        "contextSealValid": context_seal_valid,
        "positionContextComplete": all_position_context_complete,
        "contextSeal": current_seal,
        "reason": ";".join(item for item in reasons if item) or "ok",
    }


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
        key = (str(item["broker_id"]), str(item["symbol"]), "")
        broker_row = broker_rows.pop(key, None)
        ledger_row = ledger_rows.pop(key, None)
        broker_qty = broker_row.get("broker_qty") if broker_row else item["broker_qty"]
        broker_id = str(item["broker_id"])
        has_complete_zero_snapshot = broker_id in successful_brokers
        if broker_qty is None and has_complete_zero_snapshot:
            broker_qty = 0.0
        program_qty = float(ledger_row["quantity"] if ledger_row else item["program_qty"])
        tolerance_qty = float(item["tolerance_qty"])
        capability_unavailable = bool(item.get("capability")) and broker_qty is None and abs(program_qty) <= tolerance_qty
        if capability_unavailable:
            status = "capability_unavailable"
            status_label = "미지원"
            delta_qty = "-"
            detail = "KIS 해외주식 잔고 대조 capability가 아직 구현되지 않았습니다. 실제 해외 포지션 원장이 생기면 fail-closed로 차단합니다."
        elif broker_qty is None:
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
                "position_side": normalized_reconciliation_position_side(
                    broker_row or ledger_row or item,
                    source="broker" if broker_row else "program",
                ),
                "program_qty": format_quantity(program_qty),
                "broker_qty": format_quantity(float(broker_qty)) if broker_qty is not None else "미지원" if capability_unavailable else "API 필요",
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
        position_side = normalized_reconciliation_position_side(
            ledger_row,
            source="program",
        )
        legacy_side = (
            str(ledger_row.get("broker_id") or "").strip().lower()
            == "binance-futures"
            and position_side == "LEGACY"
        )
        if (
            broker_qty is None
            and str(ledger_row.get("broker_id")) in successful_brokers
            and not legacy_side
        ):
            broker_qty = 0.0
        program_qty = float(ledger_row.get("quantity") or 0.0)
        if legacy_side:
            status = "mismatch"
            status_label = "불일치"
            delta_qty = "-"
            detail = (
                "레거시 프로그램 원장에 position side가 없어 Binance "
                "Futures Hedge Mode LONG/SHORT 포지션과 안전하게 대조할 수 "
                "없습니다. 계좌·포지션을 새로 갱신해 원장을 side별로 "
                "재구성해야 합니다."
            )
        elif broker_qty is None:
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
                "position_side": position_side,
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
    for (_, _, _), broker_row in sorted(broker_rows.items()):
        broker_qty = float(broker_row.get("broker_qty") or 0.0)
        if abs(broker_qty) <= 0.000000000001:
            continue
        position_side = normalized_reconciliation_position_side(
            broker_row,
            source="broker",
        )
        side_unspecified = (
            str(broker_row.get("broker_id") or "").strip().lower()
            == "binance-futures"
            and position_side == "UNSPECIFIED"
        )
        rows.append(
            {
                "symbol": str(broker_row.get("symbol")),
                "asset": str(broker_row.get("asset") or ""),
                "broker_id": str(broker_row.get("broker_id")),
                "broker_name": str(broker_row.get("broker_name") or broker_row.get("broker_id")),
                "currency": str(broker_row.get("currency") or ""),
                "position_side": position_side,
                "program_qty": "0",
                "broker_qty": format_quantity(broker_qty),
                "broker_value_display": format_money(broker_row.get("broker_value"), str(broker_row.get("currency") or "")) if safe_float(broker_row.get("broker_value"), 0.0) > 0 else "평가 대기",
                "average_price_display": format_money(broker_row.get("average_price"), str(broker_row.get("currency") or "")) if safe_float(broker_row.get("average_price"), 0.0) > 0 else "-",
                "current_price_display": format_money(broker_row.get("current_price"), str(broker_row.get("currency") or "")) if safe_float(broker_row.get("current_price"), 0.0) > 0 else "-",
                "delta_qty": format_quantity(-broker_qty),
                "status": "mismatch",
                "status_label": "불일치",
                "program_source": "missing",
                "detail": (
                    "Binance Futures 포지션 side가 없어 LONG/SHORT를 구분할 "
                    "수 없습니다. 신규 진입을 차단하고 계정의 positionSide를 "
                    "다시 조회해야 합니다."
                    if side_unspecified
                    else (
                        "브로커에는 보유 수량이 있지만 프로그램 포지션 "
                        f"원장에는 없습니다. {broker_row.get('detail', '')}"
                    ).strip()
                ),
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
    capability_gap_count = sum(1 for item in position_rows + account_rows if item["status"] == "capability_unavailable")
    mismatch_count = sum(1 for item in position_rows + account_rows if item["status"] == "mismatch")
    pass_count = sum(1 for item in position_rows + account_rows if item["status"] == "pass")
    blocking_count = api_required_count + mismatch_count
    status = "fail" if mismatch_count else "warn" if api_required_count or capability_gap_count else "pass"
    status_label = "불일치" if mismatch_count else "API 필요" if api_required_count else "일부 미지원" if capability_gap_count else "정상"
    return {
        "summary": {
            "status": status,
            "status_label": status_label,
            "last_run": STATE["reconciliation_last_run"] or "미실행",
            "position_count": len(position_rows),
            "account_count": len(account_rows),
            "api_required_count": api_required_count,
            "capability_gap_count": capability_gap_count,
            "mismatch_count": mismatch_count,
            "blocking_count": blocking_count,
            "pass_count": pass_count,
            "error_count": len(errors),
        },
        "positions": position_rows,
        "accounts": account_rows,
        "errors": errors,
        "next_actions": reconciliation_next_actions(api_required_count, mismatch_count, capability_gap_count),
    }


def reconciliation_summary_for_broker(broker_id: str) -> dict[str, Any]:
    normalized = str(broker_id or "").strip().lower()
    data = reconciliation_snapshot()
    rows = [
        item
        for item in [*data["positions"], *data["accounts"]]
        if str(item.get("broker_id") or "").strip().lower() == normalized
    ]
    broker_error = broker_reconciliation_errors().get(normalized, "")
    api_required_count = sum(1 for item in rows if item["status"] == "api_required")
    capability_gap_count = sum(1 for item in rows if item["status"] == "capability_unavailable")
    if broker_error or not rows:
        api_required_count += 1
    mismatch_count = sum(1 for item in rows if item["status"] == "mismatch")
    pass_count = sum(1 for item in rows if item["status"] == "pass")
    blocking_count = api_required_count + mismatch_count
    status = "fail" if mismatch_count else "warn" if api_required_count or capability_gap_count else "pass"
    return {
        "status": status,
        "status_label": "불일치" if mismatch_count else "API 필요" if api_required_count else "일부 미지원" if capability_gap_count else "정상",
        "api_required_count": api_required_count,
        "capability_gap_count": capability_gap_count,
        "mismatch_count": mismatch_count,
        "blocking_count": blocking_count,
        "pass_count": pass_count,
        "row_count": len(rows),
        "broker_id": normalized,
        "broker_error": broker_error,
    }


def reconciliation_next_actions(api_required_count: int, mismatch_count: int, capability_gap_count: int = 0) -> list[str]:
    actions: list[str] = []
    if api_required_count:
        actions.append("브로커 계좌·포지션 조회 또는 프로그램 현금 원장 연결")
    if mismatch_count:
        actions.append("불일치 포지션 수동 확인 후 주문 잠금 유지")
    if capability_gap_count:
        actions.append("미지원 시장은 해당 broker capability 구현 전 MONITOR로 유지")
    actions.append("대조 결과가 정상일 때만 SMALL_LIVE 승인 검토")
    return actions


def reconciliation_blocker_count(summary: dict[str, Any]) -> int:
    if "blocking_count" in summary:
        return int(summary.get("blocking_count") or 0)
    return int(summary.get("api_required_count") or 0) + int(summary.get("mismatch_count") or 0)


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


def automatic_checklist_evidence(
    reconciliation_summary: dict[str, Any] | None = None,
) -> dict[str, tuple[bool, str]]:
    """Return objective checklist facts that can be verified by the runtime.

    Human acknowledgements (risk-plan review and manual takeover readiness)
    intentionally remain manual.  API/account and reconciliation checks are
    derived from fresh broker evidence so a restart cannot turn a completed
    machine-verifiable check red.
    """

    broker_rows = [broker.to_dict() for broker in broker_readiness()]
    credentials_ready = bool(broker_rows) and all(
        not broker.get("missing_env")
        for broker in broker_rows
    )
    broker_cache = STATE.get("broker_reconciliation", {})
    successful_accounts = {
        str(item)
        for item in (
            broker_cache.get("successful_account_brokers", [])
            if isinstance(broker_cache, dict)
            else []
        )
    }
    expected_accounts = {"kis", "binance", "binance-futures", "upbit"}
    account_errors = [
        item
        for item in (
            broker_cache.get("errors", [])
            if isinstance(broker_cache, dict)
            and isinstance(broker_cache.get("errors"), list)
            else []
        )
        if str(item.get("scope") or "") in {"account", "snapshot"}
    ]
    account_verified = (
        credentials_ready
        and expected_accounts.issubset(successful_accounts)
        and not account_errors
    )

    summary = (
        reconciliation_summary
        if isinstance(reconciliation_summary, dict)
        else reconciliation_snapshot().get("summary", {})
    )
    reconciliation_verified = (
        str(summary.get("status")) == "pass"
        and int(summary.get("blocking_count") or 0) == 0
        and str(summary.get("last_run") or "") not in {"", "미실행"}
    )
    return {
        "api_keys_reviewed": (
            account_verified,
            (
                "필수 인증정보와 KIS·Binance Spot/Futures·Upbit 실계좌 조회를 확인했습니다."
                if account_verified
                else "필수 인증정보와 4개 실계좌 조회가 모두 성공해야 자동 확인됩니다."
            ),
        ),
        "position_reconcile_reviewed": (
            reconciliation_verified,
            (
                f"최근 자동 대조가 정상입니다: {summary.get('last_run')}."
                if reconciliation_verified
                else "최근 계좌·포지션 자동 대조가 정상이어야 자동 확인됩니다."
            ),
        ),
    }


def checklist_rows(
    reconciliation_summary: dict[str, Any] | None = None,
) -> list[dict[str, object]]:
    values = STATE["checklist"]
    automatic = automatic_checklist_evidence(reconciliation_summary)
    rows: list[dict[str, object]] = []
    for item in CHECKLIST_ITEMS:
        key = str(item["key"])
        manual_checked = bool(values.get(key, False))
        automatic_checked, evidence = automatic.get(key, (False, ""))
        machine_verifiable = key in MACHINE_VERIFIABLE_CHECKLIST_KEYS
        checked = automatic_checked if machine_verifiable else manual_checked
        source = (
            "automatic"
            if machine_verifiable and automatic_checked
            else "failed"
            if machine_verifiable
            else "manual"
            if manual_checked
            else "pending"
        )
        rows.append(
            {
                "key": key,
                "label": str(item["label"]),
                "detail": evidence if machine_verifiable and evidence else str(item["detail"]),
                "required": bool(item["required"]),
                "checked": checked,
                "manual_checked": manual_checked,
                "automatic_checked": automatic_checked,
                "source": source,
                "evidence": evidence,
            }
        )
    return rows


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
        heartbeat_status: CheckStatus = "fail" if active_live else "pass"
        heartbeat_detail = (
            "Watchdog 점검 이력이 없습니다. 활성 자동화는 시작할 수 없습니다."
            if active_live
            else "MONITOR 상태에서는 Watchdog heartbeat 미실행을 경고로 계산하지 않습니다."
        )
        heartbeat_value = "미실행"
    elif heartbeat_age > heartbeat_timeout:
        heartbeat_status = "fail" if active_live else "pass"
        heartbeat_detail = (
            f"마지막 Watchdog 점검이 {heartbeat_age}초 전이며 활성 운용 한도 {heartbeat_timeout}초를 넘었습니다."
            if active_live
            else f"MONITOR 상태이므로 {heartbeat_age}초 전 heartbeat를 운용 경고로 계산하지 않습니다."
        )
        heartbeat_value = f"{heartbeat_age}초"
    else:
        heartbeat_status = "pass"
        heartbeat_detail = f"Watchdog heartbeat가 {heartbeat_timeout}초 한도 안에 있습니다."
        heartbeat_value = f"{heartbeat_age}초"
    checks.append(watchdog_check("Watchdog heartbeat", heartbeat_status, heartbeat_detail, heartbeat_value))

    stale_limit = int(float(settings.get("market_data_stale_sec", DEFAULT_WATCHDOG_SETTINGS["market_data_stale_sec"])))
    runner_age = seconds_since(STATE["strategy_runner"].get("last_run"), current)
    continuous = LIVE_CONTINUOUS_CONTROLLER.snapshot()
    continuous_profiles = continuous.get("profiles") if isinstance(continuous.get("profiles"), dict) else {}
    live_continuous_profiles = [
        profile
        for profile in continuous_profiles.values()
        if isinstance(profile, dict)
        and profile.get("running") is True
        and str(profile.get("phase") or "").upper() == "RUNNING"
    ]
    continuous_heartbeat_ages = [
        age
        for age in (seconds_since(profile.get("lastHeartbeat"), current) for profile in live_continuous_profiles)
        if age is not None
    ]
    continuous_fresh = bool(continuous_heartbeat_ages) and min(continuous_heartbeat_ages) <= heartbeat_timeout
    if active_live and continuous_fresh:
        data_status = "pass"
        data_detail = "연속 실행기가 완료 봉을 감시 중이며 feed heartbeat가 정상입니다."
        data_value = f"{min(continuous_heartbeat_ages)}초"
    elif active_live and runner_age is None:
        data_status: CheckStatus = "fail"
        data_detail = "자동화가 활성화됐지만 최근 전략/시장 데이터 점검 시간이 없습니다."
        data_value = "미실행"
    elif active_live and runner_age is not None and runner_age > stale_limit:
        data_status = "fail"
        data_detail = f"최근 전략/시장 데이터가 {runner_age}초 전입니다. 허용 {stale_limit}초를 넘었습니다."
        data_value = f"{runner_age}초"
    elif runner_age is None:
        data_status = "pass"
        data_detail = "MONITOR 상태에서는 전략 사이클 미실행을 시장 데이터 경고로 계산하지 않습니다."
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
        broker_status = "pass"
        broker_detail = (
            "활성 자동화 라우트가 없어 브로커 주문 준비 상태를 운용 경고로 계산하지 않습니다."
            if unavailable
            else "비활성 상태이며 브로커 준비 경고가 없습니다."
        )
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
    execution_streams = LIVE_EXECUTION_STREAMS.snapshot()
    stream_brokers = execution_streams.get("brokers") if isinstance(execution_streams.get("brokers"), dict) else {}
    active_streams_ready = bool(active_brokers) and all(
        isinstance(stream_brokers.get(broker_id), dict)
        and stream_brokers[broker_id].get("running") is True
        and stream_brokers[broker_id].get("connected") is True
        for broker_id in active_brokers
    )
    if active_live and active_streams_ready:
        event_status = "pass"
        event_detail = "활성 브로커의 사설 체결 스트림이 연결되어 있습니다."
        event_value = f"{len(active_brokers)}/{len(active_brokers)}"
    elif active_live and event_age is None:
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
    with RUNTIME_CONTROL_LOCK:
        runtime_transition = LIVE_CONTINUOUS_CONTROLLER.transition_running(
            "MONITOR"
        )
    if (runtime_transition.get("results") or {}):
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
    reconcile_summary = reconciliation["summary"]
    checklist = checklist_rows(reconcile_summary)
    checklist_missing = [item for item in checklist if item["required"] and not item["checked"]]
    live_ready_count = sum(1 for strategy in strategies if strategy.get("live_allowed") is True)
    full_live_ready_count = sum(1 for strategy in strategies if strategy.get("live_eligible") is True)
    adapter_blocked = [broker for broker in brokers if broker["live_order_adapter_ready"] is not True]
    missing_brokers = [broker for broker in brokers if broker["status"] == "missing_credentials"]
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
        "hard_stops": hard_stops,
        "warnings": warnings,
    }


DOCTOR_ISSUE_META: dict[tuple[str, str], tuple[str, str, str]] = {
    ("preflight", "실거래 환경 변수"): (
        "PREFLIGHT_REAL_ORDER_ROUTE_DISABLED",
        "settings",
        "설정에서 실거래 라우트를 명시적으로 확인해 활성화하고 프로그램을 다시 점검하세요.",
    ),
    ("preflight", "브로커 API 키"): (
        "PREFLIGHT_BROKER_CREDENTIALS_MISSING",
        "settings",
        "누락된 브로커 인증정보를 보호 저장소에 저장한 뒤 계좌 조회를 다시 실행하세요.",
    ),
    ("preflight", "브로커 주문 어댑터"): (
        "PREFLIGHT_BROKER_ADAPTER_UNAVAILABLE",
        "settings",
        "해당 브로커의 서명 주문 어댑터 구현·테스트 상태를 확인하세요.",
    ),
    ("preflight", "포지션·계좌 대조"): (
        "PREFLIGHT_RECONCILIATION_BLOCKED",
        "accounts",
        "계좌와 보유 포지션을 새로고침해 자동 대조하고 불일치 또는 조회 오류를 해소하세요.",
    ),
    ("preflight", "전략 승인"): (
        "PREFLIGHT_STRATEGY_LIFECYCLE_INELIGIBLE",
        "gate",
        "Shadow와 Paper의 전진 검증 evidence를 충족한 전략만 Live-Small 단계로 승격하세요.",
    ),
    ("preflight", "필수 운영 체크리스트"): (
        "PREFLIGHT_OPERATOR_CHECKLIST_INCOMPLETE",
        "gate",
        "자동 확인되지 않는 운용자 검토 항목을 실제 절차 수행 후 직접 확인하세요.",
    ),
    ("preflight", "Kill Switch"): (
        "PREFLIGHT_KILL_SWITCH_ACTIVE",
        "overview",
        "차단 원인을 확인하고 안전할 때만 Kill Switch를 해제하세요.",
    ),
    ("preflight", "운용자 확인"): (
        "PREFLIGHT_OPERATOR_CONFIRMATION_REQUIRED",
        "gate",
        "첫 실주문 전 운용 범위와 손실 한도를 확인하고 운용자 확인을 완료하세요.",
    ),
    ("preflight", "Dry Run 보호"): (
        "PREFLIGHT_DRY_RUN_DISABLED",
        "gate",
        "실제 제출이 필요하지 않은 점검 중에는 Dry Run을 유지하세요.",
    ),
    ("preflight", "주문 큐 잔여"): (
        "PREFLIGHT_RETRYABLE_ORDERS_PENDING",
        "logs",
        "재시도 주문의 원인과 현재 브로커 상태를 확인한 뒤 재시도 또는 취소하세요.",
    ),
    ("risk", "데이터 지연"): (
        "RISK_MARKET_DATA_STALE",
        "automation",
        "활성 전략 런타임의 완료 봉 처리와 feed heartbeat를 복구하세요.",
    ),
    ("risk", "포지션 불일치"): (
        "RISK_POSITION_TRUTH_UNVERIFIED",
        "accounts",
        "브로커 계좌·포지션을 새로고침하고 프로그램 원장과 자동 대조하세요.",
    ),
    ("watchdog", "Watchdog heartbeat"): (
        "WATCHDOG_HEARTBEAT_STALE",
        "automation",
        "Watchdog를 다시 실행하고 백그라운드 감시 스레드가 정상인지 확인하세요.",
    ),
    ("watchdog", "시장 데이터 신선도"): (
        "WATCHDOG_MARKET_DATA_STALE",
        "automation",
        "전략 런타임과 완료 봉 feed를 시작하거나 마지막 오류를 해결하세요.",
    ),
    ("watchdog", "브로커/API 상태"): (
        "WATCHDOG_BROKER_ROUTE_UNAVAILABLE",
        "settings",
        "활성 자동화에 선택한 브로커의 인증정보·계좌 조회·라우트 잠금을 확인하세요.",
    ),
    ("watchdog", "체결 이벤트 동기화"): (
        "WATCHDOG_EXECUTION_SYNC_STALE",
        "accounts",
        "사설 체결 스트림 또는 read-only 폴링을 복구한 뒤 계좌 대조를 다시 실행하세요.",
    ),
}


def doctor_issue_metadata(
    source: str,
    label: str,
) -> tuple[str, str, str]:
    configured = DOCTOR_ISSUE_META.get((source, label))
    if configured:
        return configured
    digest = hashlib.sha256(f"{source}:{label}".encode("utf-8")).hexdigest()[:10].upper()
    related_tab = "automation" if source == "watchdog" else "gate"
    return (
        f"{source.upper()}_{digest}",
        related_tab,
        "세부 evidence를 확인하고 관련 탭에서 원인을 해소한 뒤 Doctor를 다시 실행하세요.",
    )


def make_doctor_issue(
    *,
    source: str,
    label: str,
    status: str,
    evidence: str,
    problem: str | None = None,
    issue_code: str | None = None,
    related_tab: str | None = None,
    remediation: str | None = None,
) -> dict[str, str]:
    configured_code, configured_tab, configured_remediation = doctor_issue_metadata(source, label)
    normalized = str(status).lower()
    return {
        "issue_code": issue_code or configured_code,
        "severity": "hard_stop" if normalized == "fail" else "warning",
        "source": source,
        "problem": problem or label,
        "evidence": audit_clip(evidence, 1000),
        "remediation": remediation or configured_remediation,
        "related_tab": related_tab or configured_tab,
    }


def build_doctor_diagnostic_snapshot(data: dict[str, Any]) -> dict[str, Any]:
    issues: list[dict[str, str]] = []
    seen: set[str] = set()

    def add(issue: dict[str, str]) -> None:
        code = issue["issue_code"]
        if code in seen:
            return
        seen.add(code)
        issues.append(issue)

    for check in data.get("final_preflight", []):
        if not isinstance(check, dict) or check.get("status") not in {"fail", "warn"}:
            continue
        add(
            make_doctor_issue(
                source="preflight",
                label=str(check.get("label") or "최종 점검"),
                status=str(check.get("status")),
                problem=f"{check.get('label')}: 점검을 통과하지 못했습니다.",
                evidence=str(check.get("detail") or ""),
            )
        )

    for item in data.get("checklist", []):
        if not isinstance(item, dict) or item.get("checked") is True:
            continue
        required = item.get("required") is True
        label = str(item.get("label") or item.get("key") or "운영 체크리스트")
        key = str(item.get("key") or "unknown").upper()
        add(
            make_doctor_issue(
                source="checklist",
                label=label,
                status="fail" if required else "warn",
                problem=f"{label}: {'필수 확인이 남았습니다.' if required else '선택 확인이 남았습니다.'}",
                evidence=str(item.get("evidence") or item.get("detail") or ""),
                issue_code=f"CHECKLIST_{key}_PENDING",
                related_tab="gate",
                remediation=(
                    "자동 확인 대상은 계좌를 새로고침하고, 운용자 판단 대상은 실제 절차를 수행한 뒤 체크하세요."
                ),
            )
        )

    for check in data.get("risk_checks", []):
        if not isinstance(check, dict) or check.get("status") not in {"fail", "warn"}:
            continue
        add(
            make_doctor_issue(
                source="risk",
                label=str(check.get("label") or "리스크 점검"),
                status=str(check.get("status")),
                problem=f"{check.get('label')}: 리스크 점검이 필요합니다.",
                evidence=f"{check.get('detail') or ''} 현재 값: {check.get('value') or '-'}",
            )
        )

    watchdog = data.get("watchdog") if isinstance(data.get("watchdog"), dict) else {}
    for check in watchdog.get("checks", []):
        if not isinstance(check, dict) or check.get("status") not in {"fail", "warn"}:
            continue
        add(
            make_doctor_issue(
                source="watchdog",
                label=str(check.get("label") or "Watchdog"),
                status=str(check.get("status")),
                problem=f"{check.get('label')}: 감시 상태를 확인해야 합니다.",
                evidence=f"{check.get('detail') or ''} 관측값: {check.get('value') or '-'}",
            )
        )

    reconciliation = data.get("reconciliation") if isinstance(data.get("reconciliation"), dict) else {}
    for index, error in enumerate(reconciliation.get("errors", [])):
        if not isinstance(error, dict):
            continue
        broker_id = str(error.get("broker_id") or "unknown")
        scope = str(error.get("scope") or "snapshot")
        add(
            make_doctor_issue(
                source="reconciliation",
                label=f"{broker_id} {scope}",
                status="fail",
                problem=f"{broker_id} 계좌·포지션 조회 오류",
                evidence=str(error.get("detail") or "브로커 조회 오류"),
                issue_code=f"RECONCILIATION_{broker_id.upper().replace('-', '_')}_{scope.upper()}_{index + 1}",
                related_tab="accounts",
                remediation="브로커 권한·네트워크·계정 범위를 확인하고 계좌를 다시 새로고침하세요.",
            )
        )

    issues.sort(key=lambda issue: (0 if issue["severity"] == "hard_stop" else 1, issue["issue_code"]))
    hard_stop_count = sum(1 for issue in issues if issue["severity"] == "hard_stop")
    warning_count = len(issues) - hard_stop_count
    generated_at = datetime.now().astimezone().isoformat(timespec="microseconds")
    digest = hashlib.sha256(
        json.dumps(issues, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    reconciliation_summary = reconciliation.get("summary") if isinstance(reconciliation.get("summary"), dict) else {}
    return {
        "schema_version": 1,
        "run_id": f"{datetime.now().strftime('%Y%m%d-%H%M%S-%f')}-{digest[:10]}",
        "generated_at": generated_at,
        "summary": {
            "status": "blocked" if hard_stop_count else "warning" if warning_count else "pass",
            "hard_stop_count": hard_stop_count,
            "warning_count": warning_count,
            "issue_count": len(issues),
        },
        "context": {
            "mode": str(data.get("mode") or "MONITOR"),
            "dry_run": bool(data.get("dry_run", True)),
            "real_orders_enabled": real_orders_enabled(),
            "reconciliation_status": str(reconciliation_summary.get("status") or "unknown"),
            "reconciliation_last_run": str(reconciliation_summary.get("last_run") or "미실행"),
            "live_strategy_count": int(data.get("summary", {}).get("live_strategy_count") or 0),
            "full_live_strategy_count": int(data.get("summary", {}).get("full_live_strategy_count") or 0),
        },
        "issues": issues,
    }


def doctor_diagnostics_document(*, include_history: bool = True) -> dict[str, Any]:
    with DOCTOR_DIAGNOSTICS_LOCK:
        payload = read_json_document(DOCTOR_DIAGNOSTICS_PATH)
    history = payload.get("history") if isinstance(payload.get("history"), list) else []
    latest = payload.get("latest") if isinstance(payload.get("latest"), dict) else None
    return {
        "schema_version": 1,
        "path": str(DOCTOR_DIAGNOSTICS_PATH),
        "history_limit": DOCTOR_DIAGNOSTIC_HISTORY_LIMIT,
        "history_count": len(history),
        "latest": latest,
        "history": history if include_history else [],
    }


def persist_doctor_diagnostic_snapshot(data: dict[str, Any]) -> dict[str, Any]:
    report = build_doctor_diagnostic_snapshot(data)
    with DOCTOR_DIAGNOSTICS_LOCK:
        current = read_json_document(DOCTOR_DIAGNOSTICS_PATH)
        history = current.get("history") if isinstance(current.get("history"), list) else []
        history = [
            item
            for item in history
            if isinstance(item, dict) and item.get("run_id") != report["run_id"]
        ]
        history.append(report)
        history = history[-DOCTOR_DIAGNOSTIC_HISTORY_LIMIT:]
        document = {
            "schema_version": 1,
            "updated_at": report["generated_at"],
            "latest": report,
            "history": history,
        }
        safe_document = redact_sensitive_payload(document)
        write_json_document(DOCTOR_DIAGNOSTICS_PATH, safe_document)
    return doctor_diagnostics_document()


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
    continuous_runtime = LIVE_CONTINUOUS_CONTROLLER.snapshot()
    broker_truth = broker_position_truth_snapshot(reconciliation)
    restart_recovery = restart_recovery_plan_snapshot(reconciliation, continuous_runtime, broker_truth)
    payload = {
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
        "checklist": checklist_rows(reconciliation["summary"]),
        "retry_policy": retry_policy_rows(),
        "order_queue": queue,
        "brokers": brokers,
        "broker_diagnostics": broker_diagnostics(),
        "broker_adapter_contract": broker_adapter_contract(),
        "automation_profiles": automations,
        "continuous_runtime": continuous_runtime,
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
        "broker_position_truth": broker_truth,
        "program_ledger": program_ledger_snapshot(),
        "execution_events": execution_event_snapshot(),
        "upbit_smoke_order": dict(STATE.get("upbit_smoke_order", {})),
        "binance_smoke_order": dict(STATE.get("binance_smoke_order", {})),
        "binance_futures_canary": binance_futures_canary_status(),
        "execution_calibration": execution_calibration_snapshot(),
        "policy_replays": list(STATE.get("policy_replays", [])),
        "shadow_live": {"brokerSubmissionBlocked": True, "evidence": list(STATE.get("shadow_evidence", []))[:20], "count": len(STATE.get("shadow_evidence", []))},
        "runtime_recovery": dict(STATE.get("recovery_status", {})),
        "restart_recovery_plan": restart_recovery,
        "operational_readiness": operational,
        "automatic_promotion": dict(STATE.get("automatic_promotion", {})),
        "accounts": reconciliation["accounts"],
        "positions": reconciliation["positions"],
        "operation_report": report,
        "final_preflight": preflight,
        "launch_report": launch,
        "doctor_diagnostics": doctor_diagnostics_document(include_history=False),
        "audit": list(reversed(STATE["audit"][-30:])),
    }
    return redact_sensitive_payload(payload)  # type: ignore[return-value]


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
    queue_live_audit_telegram(level, event, detail)


def queue_live_audit_telegram(level: str, event: str, detail: str) -> bool:
    """Queue only actionable Live safety alerts; fills use notify_new_live_fills."""

    normalized_level = str(level or "").strip().lower()
    normalized_event = str(event or "").strip()
    is_critical = normalized_level in {"danger", "error", "critical"}
    is_safety_event = (
        normalized_event in TELEGRAM_SAFETY_AUDIT_EVENTS
        or (
            normalized_event in TELEGRAM_HEALTH_AUDIT_EVENTS
            and normalized_level in {"warn", "warning", "danger", "error", "critical"}
        )
    )
    is_connectivity_failure = (
        normalized_event in TELEGRAM_CONNECTIVITY_AUDIT_EVENTS
        and normalized_level in {"warn", "warning", "danger", "error", "critical"}
    )
    is_connectivity_recovery = (
        normalized_event in TELEGRAM_CONNECTIVITY_AUDIT_EVENTS
        and "연결 복구" in str(detail or "")
    )
    if not (is_critical or is_safety_event or is_connectivity_failure or is_connectivity_recovery):
        return False

    try:
        return TELEGRAM_DISPATCHER.send_async(
            "\n".join(
                [
                    "⚠️ <b>[실전 트레이더] 확인 필요</b>",
                    f"이벤트: {html.escape(normalized_event)}",
                    f"운용 상태: {html.escape(str(STATE.get('mode') or 'MONITOR'))}",
                    f"거래 영향: Kill Switch {'ON' if STATE.get('kill_switch') else 'OFF'} · 신규 진입 {'차단' if STATE.get('new_entries_blocked') else '허용'}",
                    "",
                    f"내용: {html.escape(str(detail or '')[:1200])}",
                ]
            ),
            dedupe_key=f"live-alert:{normalized_event}:{str(detail or '')[:120]}",
            dedupe_seconds=600,
            severity="critical" if is_critical else "warning",
            event_type=(
                "recovery"
                if is_connectivity_recovery
                else "failure"
                if is_critical or is_connectivity_failure
                else "safety"
            ),
        )
    except Exception:
        # Telegram 관제 실패는 감사 기록이나 주문/운용 경로를 중단시키지 않는다.
        return False


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


def normalize_runtime_mode(mode: object) -> Mode:
    normalized = str(mode or "MONITOR").strip().upper()
    if normalized in RUNTIME_MODE_RANK:
        return normalized  # type: ignore[return-value]
    return "MONITOR"


def effective_automation_mode() -> Mode:
    active = [
        normalize_runtime_mode(profile.get("mode"))
        for profile in STATE["automation"].values()
        if profile.get("enabled")
        and normalize_runtime_mode(profile.get("mode")) != "MONITOR"
    ]
    return max(
        active,
        key=lambda value: RUNTIME_MODE_RANK[value],
        default="MONITOR",
    )


def sync_runtime_profile_mode(
    profile_id: str,
    mode: object,
    *,
    action: str,
) -> None:
    normalized_profile = "stock" if profile_id == "stock" else "crypto"
    normalized_mode = normalize_runtime_mode(mode)
    profile = STATE["automation"][normalized_profile]
    profile["mode"] = normalized_mode
    profile["enabled"] = normalized_mode != "MONITOR"
    profile["last_action"] = action
    STATE["mode"] = effective_automation_mode()


def set_mode(mode: str) -> dict[str, Any]:
    with RUNTIME_CONTROL_LOCK:
        return _set_mode_serialized(mode)


def _set_mode_serialized(mode: str) -> dict[str, Any]:
    data = snapshot()
    blockers = data["summary"]["blocker_count"]
    watchdog_critical = int(data.get("watchdog", {}).get("critical_count", 0))
    normalized = normalize_runtime_mode(mode)
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
    runtime_transition = LIVE_CONTINUOUS_CONTROLLER.transition_running(
        normalized
    )
    if not runtime_transition.get("ok"):
        reason = (
            "continuous runtime mode 전환 실패로 global mode를 유지했습니다: "
            f"{runtime_transition.get('reason') or 'unknown'}"
        )
        append_audit("danger", "모드 전환 차단", reason)
        return {
            "ok": False,
            "reason": reason,
            "runtime": runtime_transition,
            "snapshot": snapshot(),
        }
    with RUNTIME_MODE_LOCK:
        STATE["mode"] = normalized
        transitioned_profiles = set(
            (runtime_transition.get("results") or {}).keys()
        )
        for profile_id in transitioned_profiles:
            profile = STATE["automation"][profile_id]
            profile["mode"] = normalized
            profile["enabled"] = normalized != "MONITOR"
            profile["last_action"] = f"Global mode 원자적 {normalized} 전환"
        if normalized == "MONITOR":
            for profile in STATE["automation"].values():
                profile["mode"] = "MONITOR"
                profile["enabled"] = False
                profile["last_action"] = "Global MONITOR fail-closed 전환"
    append_audit("info", "모드 전환", f"운용 모드가 {normalized}(으)로 변경되었습니다.")
    return {
        "ok": True,
        "reason": "mode changed",
        "runtime": runtime_transition,
        "snapshot": snapshot(),
    }


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
    if name == "new_entries_blocked":
        STATE["manual_new_entries_blocked"] = bool(value)
    label = {
        "kill_switch": "Kill Switch",
        "new_entries_blocked": "신규 진입 차단",
        "operator_confirmed": "운용자 확인",
        "dry_run": "Dry Run 보호",
    }[name]
    level = "info" if name == "dry_run" and value else ("warn" if value else "info")
    append_audit(level, label, f"{label} 값이 {value}(으)로 변경되었습니다.")
    if name == "kill_switch" and value:
        with RUNTIME_CONTROL_LOCK:
            LIVE_CONTINUOUS_CONTROLLER.transition_running("MONITOR")
            with RUNTIME_MODE_LOCK:
                STATE["new_entries_blocked"] = True
                for profile_id in ("stock", "crypto"):
                    sync_runtime_profile_mode(
                        profile_id,
                        "MONITOR",
                        action="Kill Switch fail-closed MONITOR 전환",
                    )
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
    deployment_id = _live_deployment_id(normalized)
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

    execution = live_small_execution_summary(
        strategy_id,
        strategy_payload=payload,
        normalized=normalized,
        deployment=deployment,
    )
    minimum_canary_fills = PROFESSIONAL_PROMOTION_POLICY.minimum_canary_fills
    if execution["fills"] < minimum_canary_fills:
        reason = (
            "소액 실거래 체결 표본이 "
            f"{execution['fills']}건입니다. SMALL_LIVE에서 최소 "
            f"{minimum_canary_fills}건의 실제 체결을 확인해야 합니다."
        )
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
        successful_orders=execution["fills"],
        blocked_orders=execution["blocked"],
        details={
            "operatorConfirmed": True,
            "readinessBlockers": 0,
            "canaryScope": execution.get("scope"),
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
        reason=f"SMALL_LIVE 실제 체결 {execution['fills']}건과 readiness gate 통과 / evidence={live_record.path.name}",
    )
    append_audit(
        "warn",
        "Live 승급",
        f"{payload.get('name') or strategy_id} 전략을 live 상태로 승급했습니다. 실제 체결 {execution['fills']}건.",
    )
    return {"ok": True, "reason": "live 승급 완료", "snapshot": snapshot()}


RESUME_PAPER_MINIMUM_OBSERVATION_DAYS = 30
RESUME_PAPER_MINIMUM_REGIME_COUNT = 2
RESUME_PAPER_MINIMUM_ORDER_COUNT = 5
RESUME_REPROMOTION_REASON = "resume-current-paper-live-forward-repromotion-required"


def _resume_evidence_number(value: object, fallback: int = -1) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return fallback


def _resume_evidence_datetime(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def paper_live_forward_resume_assessment(
    strategy_dir: Path,
    payload: dict[str, Any],
    normalized: dict[str, Any],
    deployment: dict[str, Any],
) -> dict[str, Any]:
    """Re-evaluate immutable Paper evidence before restoring Live-Small permission."""

    blockers: list[str] = []
    try:
        current_reference = artifact_reference(payload)
    except ValueError:
        return {
            "ready": False,
            "blockers": ["current-artifact-reference-invalid"],
            "evidenceId": "",
            "artifactHash": "",
        }

    current_artifact_id = str(current_reference.get("artifactId") or "")
    current_artifact_hash = str(current_reference.get("artifactHash") or "")
    portfolio_reference = (
        deployment.get("portfolioArtifact")
        if isinstance(deployment.get("portfolioArtifact"), dict)
        else {}
    )
    current_portfolio_id = str(portfolio_reference.get("artifactId") or "")
    current_portfolio_hash = str(portfolio_reference.get("artifactHash") or "")

    records = EvidenceStore(strategy_dir).list_paper()
    matching_id = [
        record
        for record in records
        if str((record.payload.get("strategyArtifact") or {}).get("artifactId") or "")
        == current_artifact_id
    ]
    if not matching_id:
        blockers.append("paper-live-forward-evidence-missing")
        return {
            "ready": False,
            "blockers": blockers,
            "evidenceId": "",
            "artifactHash": current_artifact_hash,
        }

    matching_hash = [
        record
        for record in matching_id
        if str((record.payload.get("strategyArtifact") or {}).get("artifactHash") or "")
        == current_artifact_hash
    ]
    if not matching_hash:
        blockers.append("paper-evidence-artifact-hash-mismatch")
        return {
            "ready": False,
            "blockers": blockers,
            "evidenceId": "",
            "artifactHash": current_artifact_hash,
        }

    matching_portfolio = []
    for record in matching_hash:
        evidence_portfolio = (
            record.payload.get("portfolioArtifact")
            if isinstance(record.payload.get("portfolioArtifact"), dict)
            else {}
        )
        evidence_portfolio_id = str(evidence_portfolio.get("artifactId") or "")
        evidence_portfolio_hash = str(evidence_portfolio.get("artifactHash") or "")
        if evidence_portfolio_id != current_portfolio_id:
            continue
        if current_portfolio_hash and evidence_portfolio_hash != current_portfolio_hash:
            continue
        matching_portfolio.append(record)
    if not matching_portfolio:
        blockers.append("paper-evidence-portfolio-hash-mismatch")
        return {
            "ready": False,
            "blockers": blockers,
            "evidenceId": "",
            "artifactHash": current_artifact_hash,
        }

    evidence_record = max(
        matching_portfolio,
        key=lambda item: str(
            item.payload.get("endedAt")
            or item.payload.get("createdAt")
            or item.path.name
        ),
    )
    evidence = evidence_record.payload
    evidence_id = str(evidence.get("evidenceId") or "")
    if not evidence_record.valid:
        blockers.extend(
            f"paper-evidence-invalid:{issue}"
            for issue in (evidence_record.issues or ("integrity",))
        )
    if str(evidence.get("evidenceType") or "") != "paper-portfolio":
        blockers.append("paper-evidence-type-invalid")
    if str(evidence.get("result") or "").upper() != "PASS":
        blockers.append("paper-evidence-result-not-pass")
    if str(evidence.get("status") or "").lower() != "submitted":
        blockers.append("paper-evidence-not-submitted")
    if not str(evidence.get("runtimeVersion") or "").startswith("paper-trader"):
        blockers.append("paper-runtime-version-untrusted")
    evidence_ended_at = _resume_evidence_datetime(
        evidence.get("endedAt") or evidence.get("createdAt")
    )
    if evidence_ended_at is None:
        blockers.append("paper-evidence-ended-at-missing")
    elif evidence_ended_at > datetime.now(timezone.utc) + timedelta(minutes=5):
        blockers.append("paper-evidence-ended-at-in-future")

    metrics = evidence.get("metrics") if isinstance(evidence.get("metrics"), dict) else {}
    details = evidence.get("details") if isinstance(evidence.get("details"), dict) else {}
    evidence_policy = (
        details.get("evidencePolicy")
        if isinstance(details.get("evidencePolicy"), dict)
        else {}
    )
    if (
        str(evidence_policy.get("promotionSource") or "")
        != "continuous-live-forward-closed-bar-v1"
    ):
        blockers.append("paper-live-forward-source-missing")

    observed_days = _resume_evidence_number(metrics.get("forwardObservedDays"))
    regime_count = _resume_evidence_number(metrics.get("forwardRegimeCount"))
    order_count = _resume_evidence_number(
        metrics.get("paperOrderCount"),
        _resume_evidence_number(evidence.get("orderCount"), 0),
    )
    if observed_days < RESUME_PAPER_MINIMUM_OBSERVATION_DAYS:
        blockers.append(
            f"paper-observation-days:{max(0, observed_days)}/"
            f"{RESUME_PAPER_MINIMUM_OBSERVATION_DAYS}"
        )
    if regime_count < RESUME_PAPER_MINIMUM_REGIME_COUNT:
        blockers.append(
            f"paper-market-regimes:{max(0, regime_count)}/"
            f"{RESUME_PAPER_MINIMUM_REGIME_COUNT}"
        )
    if order_count < RESUME_PAPER_MINIMUM_ORDER_COUNT:
        blockers.append(
            f"paper-order-count:{max(0, order_count)}/"
            f"{RESUME_PAPER_MINIMUM_ORDER_COUNT}"
        )

    operational = (
        details.get("operationalReadiness")
        if isinstance(details.get("operationalReadiness"), dict)
        else {}
    )
    recovery_verified = metrics.get("recoveryVerified")
    if recovery_verified is None:
        recovery_verified = operational.get("recoveryVerified")
    if recovery_verified is not True:
        blockers.append("paper-recovery-drill-not-attested")

    lifecycle_policy = (
        details.get("lifecyclePolicy")
        if isinstance(details.get("lifecyclePolicy"), dict)
        else {}
    )
    lifecycle_inputs = (
        lifecycle_policy.get("inputs")
        if isinstance(lifecycle_policy.get("inputs"), dict)
        else {}
    )
    reconciliation_value = metrics.get("reconciliationMismatches")
    if reconciliation_value is None:
        reconciliation_value = lifecycle_inputs.get("reconciliation_mismatches")
    if _resume_evidence_number(reconciliation_value) != 0:
        blockers.append("paper-reconciliation-not-attested-zero")
    if (
        str(lifecycle_policy.get("action") or "").upper() != "PROMOTE"
        or normalize_lifecycle_status(lifecycle_policy.get("targetStage"))
        != "before-live-small"
    ):
        blockers.append("paper-professional-promotion-not-attested")

    current_contract = normalize_strategy_artifact(payload)
    capabilities = (
        current_contract.get("capabilities")
        if isinstance(current_contract.get("capabilities"), dict)
        else {}
    )
    if capabilities.get("finalTestPassed") is not True:
        blockers.append("current-final-test-not-passed")
    if capabilities.get("blockingFailReasons"):
        blockers.extend(
            f"current-capability:{reason}"
            for reason in capabilities.get("blockingFailReasons", [])
        )
    revalidation = (
        current_contract.get("revalidation")
        if isinstance(current_contract.get("revalidation"), dict)
        else {}
    )
    if revalidation.get("expired") is True:
        blockers.append("current-revalidation-expired")
    last_revalidated_at = _resume_evidence_datetime(
        revalidation.get("lastRevalidatedAt")
    )
    if (
        last_revalidated_at is not None
        and evidence_ended_at is not None
        and evidence_ended_at < last_revalidated_at
    ):
        blockers.append("paper-evidence-stale-before-current-revalidation")
    candidate = (
        current_contract.get("portfolio_candidate")
        if isinstance(current_contract.get("portfolio_candidate"), dict)
        else {}
    )
    if candidate.get("required") and candidate.get("approved") is not True:
        blockers.append("current-portfolio-candidate-not-approved")
    lineage = (
        current_contract.get("lineage")
        if isinstance(current_contract.get("lineage"), dict)
        else {}
    )
    if lineage.get("blockingIssues"):
        blockers.extend(
            f"current-lineage:{issue}"
            for issue in lineage.get("blockingIssues", [])
        )

    portfolio_gate = portfolio_gate_for_strategy(
        current_contract,
        load_portfolio_artifacts(limit=10_000),
        mode="SMALL_LIVE",
    )
    if current_portfolio_id and not portfolio_gate.get("active"):
        blockers.append("current-portfolio-artifact-gate-missing")
    elif portfolio_gate.get("active") and portfolio_gate.get("allowed") is not True:
        blockers.append(
            "current-portfolio-gate:"
            + str(portfolio_gate.get("detail") or "blocked")
        )
    paper_portfolio_gate = paper_portfolio_evidence_gate_for_strategy(
        normalized,
        portfolio_gate,
    )
    if (
        paper_portfolio_gate.get("required")
        and paper_portfolio_gate.get("ready") is not True
    ):
        blockers.append(
            "current-paper-portfolio-evidence:"
            + str(paper_portfolio_gate.get("detail") or "blocked")
        )

    return {
        "ready": not blockers,
        "blockers": list(dict.fromkeys(blockers)),
        "evidenceId": evidence_id,
        "evidenceHash": str((evidence.get("integrity") or {}).get("contentHash") or ""),
        "artifactHash": current_artifact_hash,
        "portfolioId": current_portfolio_id,
        "portfolioHash": current_portfolio_hash,
        "observedDays": max(0, observed_days),
        "regimeCount": max(0, regime_count),
        "orderCount": max(0, order_count),
    }


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
        paused_from = str(
            permissions.get("pausedFrom")
            or (normalized.get("lifecycle") or {}).get("pausedFrom")
            or ""
        )
        target_status = normalize_lifecycle_status(paused_from)
        if target_status in {"paused", "retired", "unknown"}:
            target_status = "backtested"
        paused_permissions = permissions.get("pausedPermissions") if isinstance(permissions.get("pausedPermissions"), dict) else {}
        if paused_permissions:
            permissions = dict(paused_permissions)
            permissions["pausedFrom"] = paused_from
        permissions.pop("pausedPermissions", None)
        permissions["fail_reasons"] = [
            reason
            for reason in permissions.get("fail_reasons", [])
            if str(reason) not in {"lifecycle-paused", "lifecycle-retired"}
            and not str(reason).startswith(RESUME_REPROMOTION_REASON)
            and str(reason) != "resume-live-canary-repromotion-required"
        ]
        live_capable_resume = lifecycle_rank(target_status) >= lifecycle_rank("before-live-small")
        if live_capable_resume:
            resume_evidence = paper_live_forward_resume_assessment(
                strategy_dir,
                payload,
                normalized,
                deployment,
            )
            permissions["resumeEvidence"] = resume_evidence
            if resume_evidence.get("ready") is True:
                resumed_from_live = lifecycle_rank(target_status) >= lifecycle_rank("live")
                target_status = "before-live-small"
                permissions.update(
                    {
                        "paper_trader_verified": True,
                        "live_small_eligible": True,
                        "live_eligible": False,
                        "live_allowed": False,
                        "paperEvidenceId": resume_evidence.get("evidenceId"),
                        "paperEvidenceHash": resume_evidence.get("evidenceHash"),
                    }
                )
                permissions.pop("liveEvidenceId", None)
                permissions.pop("liveEvidenceHash", None)
                if resumed_from_live:
                    permissions["fail_reasons"].append(
                        "resume-live-canary-repromotion-required"
                    )
                    audit_reason = (
                        f"{payload.get('name') or strategy_id} 전략의 current-hash "
                        "Paper live-forward 증거와 Portfolio gate를 재검증했습니다. "
                        "기존 Live 권한은 복구하지 않고 before-live-small로 재개해 "
                        "소액 실거래 승급을 다시 요구합니다."
                    )
                else:
                    audit_reason = (
                        f"{payload.get('name') or strategy_id} 전략의 current-hash "
                        "Paper live-forward 증거와 Portfolio gate를 재검증해 "
                        "before-live-small로 안전 재개했습니다."
                    )
                audit_level = "info"
            else:
                target_status = "papered"
                blocker_text = ", ".join(
                    str(item)
                    for item in resume_evidence.get("blockers", [])[:6]
                ) or "paper-live-forward-evidence-missing"
                permissions["fail_reasons"].append(
                    f"{RESUME_REPROMOTION_REASON}: {blocker_text}"
                )
                permissions.update(
                    {
                        "paper_trader_verified": False,
                        "live_small_eligible": False,
                        "live_eligible": False,
                        "live_allowed": False,
                    }
                )
                for evidence_key in (
                    "paperEvidenceId",
                    "paperEvidenceHash",
                    "liveEvidenceId",
                    "liveEvidenceHash",
                ):
                    permissions.pop(evidence_key, None)
                audit_level = "warn"
                audit_reason = (
                    f"{payload.get('name') or strategy_id} 전략은 {blocker_text} 때문에 "
                    "이전 Live 권한을 복구하지 않았습니다. papered 단계로 안전 재개했으며 "
                    "Paper Trader에서 current-hash 연속 관찰 증거로 다시 승급해야 합니다."
                )
        else:
            permissions.update(
                {
                    "live_small_eligible": False,
                    "live_eligible": False,
                    "live_allowed": False,
                }
            )
            audit_level = "info"
            audit_reason = (
                f"{payload.get('name') or strategy_id} 전략을 {target_status} 단계로 "
                "재개했습니다. 비-Live 단계이므로 실거래 권한은 복구하지 않았습니다."
            )
        permissions["fail_reasons"] = list(
            dict.fromkeys(str(reason) for reason in permissions.get("fail_reasons", []))
        )
        audit_label = "전략 재개"
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


def profile_readiness_blocker_count(
    data: dict[str, Any],
    profile_id: str,
    provider_id: str,
    mode: str,
    reconciliation_summary: dict[str, Any] | None = None,
) -> int:
    provider = str(provider_id or "").strip().lower()
    required_key = "live_eligible" if str(mode).upper() == "FULL_LIVE" else "live_small_eligible"
    strategies = [
        item
        for item in data.get("strategies", [])
        if isinstance(item, dict)
        and strategy_broker_id(item) == provider
        and item.get(required_key) is True
    ]
    broker = next(
        (
            item
            for item in data.get("brokers", [])
            if isinstance(item, dict) and str(item.get("broker_id") or "").lower() == provider
        ),
        None,
    )
    checklist_missing = [
        item
        for item in checklist_rows()
        if item["required"] and not item["checked"]
    ]
    reconciliation = reconciliation_summary or reconciliation_summary_for_broker(provider)
    central_control = durable_control_snapshot()
    return sum(
        (
            0 if real_orders_enabled() else 1,
            0 if not checklist_missing else 1,
            0 if broker and broker.get("order_ready") is True else 1,
            0 if strategies else 1,
            1 if STATE["kill_switch"] else 0,
            1 if central_control["halted"] else 0,
            0 if reconciliation_blocker_count(reconciliation) == 0 else 1,
        )
    )


def set_automation_profile(profile_id: str, enabled: bool, provider: str | None = None, mode: str | None = None) -> dict[str, Any]:
    profile_id = profile_id if profile_id in {"stock", "crypto"} else ""
    if not profile_id:
        return {"ok": False, "reason": "unknown automation profile", "snapshot": snapshot()}
    if provider:
        normalized_provider = provider.lower().strip()
        if profile_id == "stock" and normalized_provider != "kis":
            return {"ok": False, "reason": "주식/ETF 자동화 provider는 kis만 허용합니다.", "snapshot": snapshot()}
        if profile_id == "crypto" and normalized_provider not in {
            "binance",
            "binance-futures",
            "upbit",
        }:
            return {
                "ok": False,
                "reason": "코인 자동화 provider는 Binance 현물·선물 또는 Upbit만 허용합니다.",
                "snapshot": snapshot(),
            }
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
        provider_id = str(STATE["automation"][profile_id].get("provider") or ("kis" if profile_id == "stock" else "binance"))
        profile_blockers = profile_readiness_blocker_count(data, profile_id, provider_id, next_mode)
        if profile_blockers:
            reason = f"{provider_id} 범위 readiness blocker {profile_blockers}개 때문에 {next_mode} 전환이 차단되었습니다."
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
    with RUNTIME_CONTROL_LOCK:
        runtime_snapshot = LIVE_CONTINUOUS_CONTROLLER.snapshot()
        runtime_profile = (
            (runtime_snapshot.get("profiles") or {}).get(profile_id) or {}
        )
        runtime_transition: dict[str, Any] | None = None
        if runtime_profile.get("running"):
            runtime_transition = LIVE_CONTINUOUS_CONTROLLER.start(
                profile_id,
                next_mode,
            )
            if not runtime_transition.get("ok"):
                reason = (
                    "continuous runtime mode 전환 실패로 automation/global "
                    f"mode를 유지했습니다: {runtime_transition.get('reason')}"
                )
                STATE["automation"][profile_id]["last_action"] = reason
                append_audit(
                    "danger",
                    "자동화 시작 차단",
                    f"{profile['title']}: {reason}",
                )
                return {
                    "ok": False,
                    "reason": reason,
                    "runtime": runtime_transition,
                    "snapshot": snapshot(),
                }
        with RUNTIME_MODE_LOCK:
            STATE["automation"][profile_id]["enabled"] = bool(enabled)
            STATE["automation"][profile_id]["mode"] = next_mode
            STATE["mode"] = effective_automation_mode()
    action = f"{next_mode} 전환"
    STATE["automation"][profile_id]["last_action"] = f"{action} {now_text()}"
    append_audit("info" if next_mode == "MONITOR" else "warn", f"{profile['title']} {action}", f"{profile['provider_label']} 라우트의 자동화 모드를 {next_mode}(으)로 기록했습니다.")
    return {
        "ok": True,
        "reason": f"{profile['title']} {action}",
        "runtime": runtime_transition,
        "snapshot": snapshot(),
    }


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
    previous = bool(STATE["checklist"].get(name, False))
    STATE["checklist"][name] = bool(value)
    try:
        persist_operator_checklist_values(STATE["checklist"])
    except OSError as exc:
        STATE["checklist"][name] = previous
        return {
            "ok": False,
            "reason": f"체크리스트 저장에 실패했습니다: {exc}",
            "snapshot": snapshot(),
        }
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
    for broker_id in ("kis", "binance", "binance-futures", "upbit"):
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


def execution_event_trace_context(event: dict[str, Any]) -> tuple[str, dict[str, Any] | None]:
    identifiers = {
        str(event.get("order_id") or ""),
        str(event.get("broker_order_id") or ""),
        str((event.get("raw") or {}).get("identifier") or "") if isinstance(event.get("raw"), dict) else "",
    }
    order = next(
        (
            item
            for item in STATE.get("orders", [])
            if identifiers.intersection({
                str(item.get("order_id") or ""),
                str(item.get("broker_order_id") or ""),
                str(item.get("idempotency_key") or ""),
            }) - {""}
        ),
        None,
    )
    if order is None:
        trace_index = STATE.get("order_trace_index", {})
        if isinstance(trace_index, dict):
            for identifier in identifiers - {""}:
                indexed = trace_index.get(identifier)
                if isinstance(indexed, dict) and indexed.get("trace_id"):
                    order = indexed
                    break
    return (str((order or {}).get("trace_id") or ""), order)


def record_new_execution_traces(events: list[dict[str, Any]], existing_event_ids: set[str]) -> None:
    for event in events:
        event_id = str(event.get("event_id") or "")
        trace_id, order = execution_event_trace_context(event)
        if trace_id:
            event["trace_id"] = trace_id
            if order is not None:
                event["strategy_id"] = str(order.get("strategy_id") or "")
        if not trace_id or event_id in existing_event_ids:
            continue
        state_name = str(event.get("state") or "").lower()
        if state_name in {"partially_filled", "partial", "partially-filled"}:
            stage = "PARTIALLY_FILLED"
        elif state_name in {"filled", "done", "executed"}:
            stage = "FILLED"
        elif state_name in {"rejected", "expired", "failed", "canceled"}:
            stage = "BLOCKED"
        elif state_name in {"account_snapshot", "position_snapshot"}:
            continue
        else:
            stage = "BROKER_ACKNOWLEDGED"
        DECISION_TRACE_STORE.append(
            trace_id=trace_id,
            stage=stage,
            decision=state_name.upper() or "EVENT",
            output_payload=event,
            occurred_at=str(event.get("occurred_at") or ""),
        )
        if stage == "FILLED":
            DECISION_TRACE_STORE.append(
                trace_id=trace_id,
                stage="POSITION_APPLIED",
                decision=str(event.get("side") or "FILL"),
                output_payload={
                    "brokerId": event.get("broker_id"),
                    "symbol": event.get("symbol"),
                    "filledQuantity": event.get("quantity"),
                },
            )


def deduplicate_execution_events(
    events: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Keep the first broker event for each stable event id."""

    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for event in events:
        event_id = str(event.get("event_id") or "").strip()
        if event_id and event_id in seen:
            continue
        if event_id:
            seen.add(event_id)
        result.append(event)
    return result


def _event_contract_value(
    event: dict[str, Any],
    raw: dict[str, Any],
    key: str,
) -> Any:
    if key in raw:
        return raw.get(key)
    return event.get(key)


def _positive_watermark_delta(total: float, previous: float) -> float:
    difference = total - previous
    tolerance = max(1e-12, abs(total) * 1e-12)
    return difference if difference > tolerance else 0.0


def execution_event_increment(
    event: dict[str, Any],
    order: dict[str, Any],
    managed: Any,
) -> tuple[float, float, float]:
    """Resolve a broker event to an incremental fill quantity, price and fee.

    Binance execution reports already carry the last-fill delta and therefore
    pass through unchanged.  Upbit MyOrder carries order-level cumulative
    watermarks even when it also includes per-trade values.  The watermarks
    cap the applied delta so a terminal snapshot, reconnect replay, or
    out-of-order trade cannot book the same fill twice.
    """

    raw = event.get("raw") if isinstance(event.get("raw"), dict) else {}
    quantity = max(0.0, safe_float(event.get("quantity"), 0.0))
    price = max(0.0, safe_float(event.get("price"), 0.0))
    fee = max(0.0, safe_float(event.get("fee"), 0.0))
    if str(event.get("broker_id") or "").strip().lower() != "upbit":
        return quantity, price, fee

    previous_quantity = max(
        0.0,
        safe_float(
            managed.filled_quantity
            if managed is not None
            else order.get("filled_quantity"),
            0.0,
        ),
    )
    previous_average_price = max(
        0.0,
        safe_float(
            managed.average_fill_price
            if managed is not None
            else order.get("average_fill_price"),
            0.0,
        ),
    )
    previous_fee = max(
        0.0,
        safe_float(
            managed.fee
            if managed is not None
            else order.get("fee"),
            0.0,
        ),
    )

    quantity_mode = str(
        _event_contract_value(event, raw, "quantity_mode") or "delta"
    ).strip().lower()
    cumulative_quantity_value = _event_contract_value(
        event,
        raw,
        "cumulative_quantity",
    )
    if cumulative_quantity_value not in {None, ""}:
        cumulative_quantity = max(
            0.0,
            safe_float(cumulative_quantity_value, 0.0),
        )
        watermark_delta = _positive_watermark_delta(
            cumulative_quantity,
            previous_quantity,
        )
        if quantity_mode == "cumulative":
            quantity = watermark_delta
            cumulative_average_price = max(
                0.0,
                safe_float(
                    _event_contract_value(
                        event,
                        raw,
                        "cumulative_average_price",
                    ),
                    0.0,
                ),
            )
            if quantity > 0 and cumulative_average_price > 0:
                cumulative_notional = (
                    cumulative_quantity * cumulative_average_price
                )
                previous_notional = (
                    previous_quantity * previous_average_price
                )
                delta_notional = cumulative_notional - previous_notional
                if delta_notional > 0:
                    price = delta_notional / quantity
        else:
            quantity = min(quantity, watermark_delta)

    fee_mode = str(
        _event_contract_value(event, raw, "fee_mode") or "delta"
    ).strip().lower()
    cumulative_fee_value = _event_contract_value(
        event,
        raw,
        "cumulative_fee",
    )
    if cumulative_fee_value not in {None, ""}:
        cumulative_fee = max(
            0.0,
            safe_float(cumulative_fee_value, 0.0),
        )
        fee_watermark_delta = _positive_watermark_delta(
            cumulative_fee,
            previous_fee,
        )
        fee = (
            fee_watermark_delta
            if fee_mode == "cumulative"
            else min(fee, fee_watermark_delta)
        )

    return quantity, price, fee


def apply_execution_events_to_local_orders(
    events: list[dict[str, Any]],
    existing_event_ids: set[str] | None = None,
) -> int:
    """Advance the local order/OMS state from deduplicated broker events."""

    known_event_ids = set(existing_event_ids or ())
    updated = 0
    for event in events:
        event_id = str(event.get("event_id") or "").strip()
        if not event_id or event_id in known_event_ids:
            continue
        known_event_ids.add(event_id)
        identifiers = {
            str(event.get("order_id") or "").strip(),
            str(event.get("broker_order_id") or "").strip(),
            str((event.get("raw") or {}).get("identifier") or "").strip()
            if isinstance(event.get("raw"), dict)
            else "",
        } - {"", "-"}
        order = next(
            (
                item
                for item in STATE.get("orders", [])
                if identifiers.intersection(
                    {
                        str(item.get("order_id") or "").strip(),
                        str(item.get("oms_order_id") or "").strip(),
                        str(item.get("broker_order_id") or "").strip(),
                        str(item.get("idempotency_key") or "").strip(),
                    }
                    - {"", "-"}
                )
            ),
            None,
        )
        if order is None:
            continue
        broker_order_id = str(
            event.get("broker_order_id")
            or order.get("broker_order_id")
            or ""
        ).strip()
        if broker_order_id:
            order["broker_order_id"] = broker_order_id
        state_name = str(
            event.get("state")
            or event.get("status")
            or ""
        ).strip().lower()
        raw = event.get("raw") if isinstance(event.get("raw"), dict) else {}
        raw_state_name = str(
            raw.get("state")
            or raw.get("status")
            or ""
        ).strip().lower()
        if state_name == "unknown" and raw_state_name:
            state_name = raw_state_name
        fill_states = {
            "partial",
            "partially_filled",
            "partially-filled",
            "filled",
            "done",
            "executed",
        }
        oms_order_id = str(order.get("oms_order_id") or "")
        managed = LIVE_OMS.orders.get(oms_order_id)
        quantity, price, fee = execution_event_increment(
            event,
            order,
            managed,
        )
        if str(event.get("broker_id") or "").strip().lower() == "upbit":
            event["reported_quantity"] = safe_float(
                event.get("quantity"),
                0.0,
            )
            event["reported_fee"] = safe_float(event.get("fee"), 0.0)
            event["reported_cumulative_quantity"] = _event_contract_value(
                event,
                raw,
                "cumulative_quantity",
            )
            event["reported_cumulative_fee"] = _event_contract_value(
                event,
                raw,
                "cumulative_fee",
            )
            event["quantity"] = quantity
            event["price"] = price
            event["fee"] = fee
            event["applied_quantity_mode"] = "incremental"
        final_fill = state_name in {
            "filled",
            "done",
            "executed",
        }
        has_fill_delta = quantity > 0 and price > 0
        if state_name in fill_states and (has_fill_delta or final_fill):
            previous_quantity = safe_float(order.get("filled_quantity"), 0.0)
            previous_notional = (
                safe_float(order.get("average_fill_price"), 0.0)
                * previous_quantity
            )
            if has_fill_delta:
                next_quantity = previous_quantity + quantity
                order["filled_quantity"] = next_quantity
                order["average_fill_price"] = (
                    previous_notional + quantity * price
                ) / next_quantity
                order["fee"] = safe_float(order.get("fee"), 0.0) + fee
            if managed is not None:
                try:
                    if managed.status == "SUBMITTING" and broker_order_id:
                        LIVE_OMS.acknowledge(
                            oms_order_id,
                            broker_order_id,
                        )
                    remaining = max(
                        0.0,
                        managed.intent.quantity
                        - managed.filled_quantity,
                    )
                    applied_quantity = min(quantity, remaining)
                    if has_fill_delta and applied_quantity > 0:
                        managed = LIVE_OMS.apply_fill(
                            oms_order_id,
                            quantity=applied_quantity,
                            price=price,
                            fee=fee,
                            broker_order_id=broker_order_id,
                        )
                except ValueError as exc:
                    order["reconciliation_warning"] = str(exc)
                order["filled_quantity"] = managed.filled_quantity
                order["average_fill_price"] = managed.average_fill_price
                order["fee"] = managed.fee
            fill_quantity_mismatch = False
            if (
                managed is not None
                and final_fill
                and managed.status != "FILLED"
            ):
                fill_quantity_mismatch = True
                order["state"] = "unknown"
                order["queue_state"] = "reconcile_required"
                order["reason"] = "broker-final-fill-quantity-mismatch"
                order["reconciliation_warning"] = (
                    "브로커는 FILLED를 보고했지만 누적 체결 수량이 "
                    "원 주문 수량과 일치하지 않습니다."
                )
                if not managed.terminal and managed.status != "UNKNOWN":
                    try:
                        managed = LIVE_OMS.mark_unknown(
                            oms_order_id,
                            "broker final fill quantity mismatch",
                        )
                    except ValueError as exc:
                        order["reconciliation_warning"] = str(exc)
                final_fill = False
            order["state"] = (
                "filled"
                if final_fill
                else "unknown"
                if fill_quantity_mismatch
                else "partially_filled"
            )
            order["queue_state"] = (
                "completed"
                if final_fill
                else "reconcile_required"
                if fill_quantity_mismatch
                else "submitted"
            )
            order["reason"] = (
                "broker-filled"
                if final_fill
                else "broker-final-fill-quantity-mismatch"
                if fill_quantity_mismatch
                else "broker-partially-filled"
            )
        elif state_name in {"accepted", "acknowledged", "new", "open"}:
            if managed is not None and not managed.terminal:
                try:
                    if managed.status == "SUBMITTING":
                        managed = LIVE_OMS.acknowledge(
                            oms_order_id,
                            broker_order_id,
                        )
                    elif managed.status == "UNKNOWN":
                        managed = LIVE_OMS.reconcile_unknown(
                            oms_order_id,
                            {
                                "brokerOrderId": broker_order_id,
                                "status": "ACKNOWLEDGED",
                            },
                        )
                except ValueError as exc:
                    order["reconciliation_warning"] = str(exc)
            order["state"] = "acknowledged"
            order["queue_state"] = "submitted"
            order["reason"] = "broker-acknowledged"
        elif state_name in {"rejected", "expired", "failed", "canceled"}:
            target = {
                "rejected": "REJECTED",
                "failed": "REJECTED",
                "expired": "EXPIRED",
                "canceled": "CANCELED",
            }[state_name]
            if managed is not None and not managed.terminal:
                try:
                    managed = LIVE_OMS.transition(
                        oms_order_id,
                        target,
                        f"broker event {state_name}",
                        {"eventId": event_id},
                    )
                except ValueError as exc:
                    order["reconciliation_warning"] = str(exc)
            order["state"] = (
                "broker_rejected"
                if state_name in {"rejected", "failed"}
                else state_name
            )
            order["queue_state"] = (
                "canceled"
                if state_name == "canceled"
                else "failed"
            )
            order["reason"] = f"broker-{state_name}"
        else:
            continue
        if managed is not None:
            order["oms_status"] = managed.status
        order["updated_at"] = now_text()
        order["last_execution_event_id"] = event_id
        updated += 1
    return updated


def automatic_live_promotion_sweep(*, fresh_broker_ids: set[str] | None = None) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    reconciliation = reconciliation_snapshot()
    broker_truth = broker_position_truth_snapshot(reconciliation)
    recovery_verified = bool(STATE.get("recovery_status", {}).get("verified"))
    fresh_brokers = set(fresh_broker_ids) if fresh_broker_ids is not None else successful_position_brokers()
    evidence_path = APP_DATA_ROOT / "logs" / "automatic-promotion-evidence.jsonl"
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    for strategy in strategy_rows():
        stage = normalize_lifecycle_status(strategy.get("lifecycle_status"))
        if stage not in {"before-live-small", "live"}:
            continue
        strategy_id = str(strategy.get("strategy_id") or "")
        execution = live_small_execution_summary(strategy_id)
        total = execution["successful"] + execution["blocked"]
        decision: AutomaticPromotionDecision = evaluate_automatic_promotion(
            stage,
            PromotionMetrics(
                artifact_locked=True,
                backtest_passed=True,
                walk_forward_pass_ratio=1.0,
                final_test_passed=True,
                closed_trade_count=20,
                maximum_drawdown=0.0,
                data_quality_score=100.0,
                canary_fill_count=execution.get("fills", 0),
                paper_reject_rate=(execution["blocked"] / total) if total else 0.0,
                reconciliation_mismatches=int(broker_truth.get("mismatchCount") or 0),
                recovery_verified=recovery_verified,
            ),
        )
        evidence = {**decision.evidence, "strategyId": strategy_id, "sourceApp": "live_trader"}
        signature = json.dumps(
            {
                "strategyId": strategy_id,
                "action": decision.action,
                "targetStage": decision.target_stage,
                "blockers": list(decision.blockers),
                "fills": execution.get("fills", 0),
                "blocked": execution["blocked"],
                "reconciliation": broker_truth.get("mismatchCount"),
                "recoveryVerified": recovery_verified,
            },
            sort_keys=True,
        )
        previous_signatures = STATE.setdefault("automatic_promotion_signatures", {})
        if previous_signatures.get(strategy_id) != signature:
            with evidence_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(evidence, ensure_ascii=False, sort_keys=True) + "\n")
            previous_signatures[strategy_id] = signature
        item = {
            "strategyId": strategy_id,
            "action": decision.action,
            "targetStage": decision.target_stage,
            "blockers": list(decision.blockers),
            "evidenceHash": decision.evidence["contentHash"],
            "freshBrokerTruth": strategy_broker_id(strategy) in fresh_brokers,
        }
        may_mutate = bool(item["freshBrokerTruth"])
        if decision.action == "PROMOTE" and may_mutate:
            promotion = promote_strategy_to_live(strategy_id)
            item["promoted"] = bool(promotion.get("ok"))
            item["reason"] = str(promotion.get("reason") or "")
        elif decision.action == "PAUSE" and may_mutate:
            pause = set_strategy_lifecycle_status(strategy_id, "pause")
            item["paused"] = bool(pause.get("ok"))
            item["reason"] = str(pause.get("reason") or "")
        elif decision.action in {"PROMOTE", "PAUSE"}:
            item["reason"] = "fresh-broker-position-snapshot-required"
        results.append(item)
    STATE["automatic_promotion"] = {
        "lastRun": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "results": results,
        "evidencePath": str(evidence_path),
    }
    return results


def live_order_message_key(
    order: dict[str, Any] | None = None,
    event: dict[str, Any] | None = None,
) -> str:
    order = order or {}
    event = event or {}
    broker_id = str(
        order.get("broker_id")
        or event.get("broker_id")
        or broker_id_from_symbol(
            str(order.get("symbol") or event.get("symbol") or ""),
            str(order.get("asset") or event.get("asset") or ""),
        )
        or "broker"
    ).lower()
    identifier = str(
        event.get("broker_order_id")
        or order.get("broker_order_id")
        or event.get("order_id")
        or order.get("oms_order_id")
        or order.get("order_id")
        or order.get("idempotency_key")
        or ""
    ).strip()
    if identifier in {"", "-"}:
        identifier = hashlib.sha256(
            (
                f"{broker_id}|{order.get('strategy_id') or event.get('strategy_id') or ''}|"
                f"{order.get('symbol') or event.get('symbol') or ''}|"
                f"{order.get('side') or event.get('side') or ''}|"
                f"{order.get('created_at') or event.get('occurred_at') or ''}"
            ).encode("utf-8")
        ).hexdigest()
    return f"live-order:{broker_id}:{identifier}"


def queue_live_order_lifecycle_notification(
    order: dict[str, Any],
    *,
    status: str,
    reason: str = "",
    message_final: bool = False,
) -> bool:
    broker_id = str(
        order.get("broker_id")
        or broker_id_from_symbol(str(order.get("symbol") or ""), str(order.get("asset") or ""))
        or "broker"
    )
    portfolio_gate = order.get("portfolio_gate") if isinstance(order.get("portfolio_gate"), dict) else {}
    return TELEGRAM_DISPATCHER.send_async(
        format_order_lifecycle_notification(
            app_name="Live Trader",
            environment=str(STATE.get("mode") or "LIVE"),
            broker=broker_id.upper(),
            status=status,
            side=str(order.get("side") or ""),
            symbol=str(order.get("symbol") or ""),
            quantity=order.get("qty") or "-",
            price=order.get("reference_price"),
            strategy=str(order.get("strategy_id") or ""),
            portfolio=str(portfolio_gate.get("portfolio_id") or ""),
            order_id=str(order.get("broker_order_id") or order.get("order_id") or ""),
            occurred_at=str(order.get("updated_at") or order.get("time") or ""),
            reason=reason,
        ),
        dedupe_key=f"live-order-state:{order.get('order_id')}:{status}",
        dedupe_seconds=604800,
        severity="warning",
        event_type="failure" if status in {"failed", "error", "rejected"} else "trade",
        message_key=live_order_message_key(order),
        message_final=message_final,
    )


def notify_new_live_fills(events: list[dict[str, Any]], existing_event_ids: set[str]) -> int:
    positions_by_key = {
        (
            str(item.get("broker_id") or "").lower(),
            str(item.get("symbol") or "").upper(),
            normalized_reconciliation_position_side(
                item,
                source="program",
            ),
        ): item
        for item in PROGRAM_LEDGER.position_rows()
    }
    cash_by_broker: dict[str, list[dict[str, Any]]] = {}
    for item in PROGRAM_LEDGER.cash_rows():
        cash_by_broker.setdefault(str(item.get("broker_id") or "").lower(), []).append(item)
    sent = 0
    for event in events:
        event_id = str(event.get("event_id") or "")
        state_name = str(event.get("state") or "").lower()
        quantity = safe_float(event.get("quantity"), 0.0)
        if not event_id or event_id in existing_event_ids:
            continue
        if state_name not in {"partial", "partially_filled", "filled", "done", "executed"} or quantity <= 0:
            continue
        broker_id = str(event.get("broker_id") or "").lower()
        symbol = str(event.get("symbol") or "").upper()
        side = str(event.get("side") or (event.get("raw") or {}).get("side") or "").upper()
        event_position_side = normalized_reconciliation_position_side(
            {
                "broker_id": broker_id,
                "position_side": (
                    event.get("position_side")
                    or event.get("positionSide")
                    or (event.get("raw") or {}).get("position_side")
                    or (event.get("raw") or {}).get("positionSide")
                ),
            },
            source="broker",
        )
        position = positions_by_key.get(
            (broker_id, symbol, event_position_side),
            {},
        )
        if not position and broker_id != "binance-futures":
            position = positions_by_key.get((broker_id, symbol, ""), {})
        position_after = safe_float(position.get("quantity"), 0.0)
        if side == "BUY":
            position_before = max(0.0, position_after - quantity)
        elif side == "SELL":
            position_before = position_after + quantity
        else:
            position_before = None
        cash_rows = cash_by_broker.get(broker_id, [])
        cash_summary = ", ".join(
            f"{safe_float(item.get('cash'), 0.0):,.4f} {item.get('currency') or ''}".strip()
            for item in cash_rows
        ) or "-"
        _trace_id, order = execution_event_trace_context(event)
        portfolio_gate = (order or {}).get("portfolio_gate") if isinstance((order or {}).get("portfolio_gate"), dict) else {}
        strategy_id = str((order or {}).get("strategy_id") or event.get("strategy_id") or "")
        price = safe_float(event.get("price"), 0.0)
        terminal_fill = state_name in {"filled", "done", "executed"}
        notification_status = (
            "closed"
            if terminal_fill and side == "SELL" and position_after <= 0
            else "filled"
            if terminal_fill
            else "partial"
        )
        message = format_order_lifecycle_notification(
            app_name="Live Trader",
            environment=str(STATE.get("mode") or "LIVE"),
            broker=broker_id.upper(),
            status=notification_status,
            side=side or "FILL",
            symbol=symbol,
            quantity=f"{quantity:.12g}",
            filled_quantity=f"{quantity:.12g}",
            average_price=f"{price:,.12g}" if price > 0 else None,
            strategy=strategy_id,
            portfolio=str(portfolio_gate.get("portfolio_id") or ""),
            cash_after=cash_summary,
            equity_after=(
                f"{sum(safe_float(item.get('cash'), 0.0) for item in cash_rows) + sum(safe_float(item.get('value'), 0.0) for item in PROGRAM_LEDGER.position_rows() if str(item.get('broker_id') or '').lower() == broker_id):,.4f}"
                if cash_rows
                else None
            ),
            position_before=f"{position_before:.12g}" if position_before is not None else None,
            position_after=f"{position_after:.12g}",
            order_id=str(event.get("broker_order_id") or event.get("order_id") or ""),
            occurred_at=str(event.get("occurred_at") or ""),
        )
        if TELEGRAM_DISPATCHER.send_async(
            message,
            dedupe_key=f"live-fill:{broker_id}:{event_id}",
            dedupe_seconds=604800,
            severity="warning",
            event_type="trade",
            message_key=live_order_message_key(order, event),
            message_final=terminal_fill,
        ):
            sent += 1
    return sent


def live_runtime_is_active() -> bool:
    try:
        continuous = LIVE_CONTINUOUS_CONTROLLER.snapshot()
        streams = LIVE_EXECUTION_STREAMS.snapshot()
    except Exception:
        return False
    return bool(continuous.get("running")) or bool(streams.get("running"))


def broker_snapshot_poll_interval_seconds() -> float:
    runtime_active = live_runtime_is_active()
    env_key = (
        "LIVE_TRADER_BROKER_SNAPSHOT_ACTIVE_SECONDS"
        if runtime_active
        else "LIVE_TRADER_BROKER_SNAPSHOT_IDLE_SECONDS"
    )
    default = (
        BROKER_SNAPSHOT_ACTIVE_INTERVAL_SECONDS
        if runtime_active
        else BROKER_SNAPSHOT_IDLE_INTERVAL_SECONDS
    )
    try:
        configured = float(os.environ.get(env_key, default))
    except (TypeError, ValueError):
        configured = default
    minimum = 10.0 if default == BROKER_SNAPSHOT_ACTIVE_INTERVAL_SECONDS else 60.0
    return max(minimum, configured)


def broker_poll_control() -> dict[str, Any]:
    control = STATE.setdefault(
        "broker_snapshot_poll",
        {"brokers": {}, "last_summary_audit_monotonic": 0.0, "last_summary_signature": ""},
    )
    if not isinstance(control.get("brokers"), dict):
        control["brokers"] = {}
    if not isinstance(control.get("connectivity"), dict):
        control["connectivity"] = {}
    return control


def record_connectivity_state(
    channel: str,
    broker_id: str,
    *,
    healthy: bool,
    recovery_marker: int | None = None,
) -> bool:
    """Alert only after a known down state or stream reconnect marker recovers."""

    normalized_channel = str(channel or "").strip().lower()
    normalized_broker = str(broker_id or "").strip().lower()
    if not normalized_channel or not normalized_broker:
        return False

    connectivity = broker_poll_control()["connectivity"]
    key = f"{normalized_channel}:{normalized_broker}"
    previous = connectivity.get(key, {}) if isinstance(connectivity.get(key), dict) else {}
    previous_status = str(previous.get("status") or "")
    current_status = "healthy" if healthy else "down"
    recovery_count = int(safe_float(previous.get("recovery_count"), 0.0))
    previous_marker = int(safe_float(previous.get("recovery_marker"), 0.0))
    marker = previous_marker if recovery_marker is None else max(0, int(recovery_marker))
    recovered = current_status == "healthy" and (
        previous_status == "down" or marker > previous_marker
    )
    if recovered:
        recovery_count += 1
    connectivity[key] = {
        "channel": normalized_channel,
        "broker_id": normalized_broker,
        "status": current_status,
        "recovery_count": recovery_count,
        "recovery_marker": marker,
    }
    if not recovered:
        return False

    if normalized_channel == "execution_stream":
        event = "체결 스트림"
        boundary = "사설 체결 스트림"
    else:
        event = "체결 이벤트 동기화"
        boundary = "체결/계좌 API"
    append_audit(
        "info",
        event,
        f"{normalized_broker.upper()} {boundary} 연결 복구 · 상태 전이 {recovery_count}회",
    )
    return True


def observe_execution_stream_connectivity(broker_ids: tuple[str, ...]) -> int:
    """Observe private streams; connecting/stopped states are not treated as failures."""

    try:
        snapshot_payload = LIVE_EXECUTION_STREAMS.snapshot()
    except Exception:
        return 0
    brokers = snapshot_payload.get("brokers") if isinstance(snapshot_payload.get("brokers"), dict) else {}
    recovered = 0
    for broker_id in broker_ids:
        status = brokers.get(broker_id)
        if not isinstance(status, dict) or status.get("running") is not True:
            continue
        reconnect_count = max(0, int(safe_float(status.get("reconnectCount"), 0.0)))
        if status.get("connected") is True:
            recovered += int(
                record_connectivity_state(
                    "execution_stream",
                    broker_id,
                    healthy=True,
                    recovery_marker=reconnect_count,
                )
            )
        elif str(status.get("lastError") or "").strip():
            record_connectivity_state(
                "execution_stream",
                broker_id,
                healthy=False,
                recovery_marker=reconnect_count,
            )
    return recovered


def broker_snapshot_is_due(broker_id: str, now_monotonic: float, *, force: bool = False) -> bool:
    if force:
        return True
    item = broker_poll_control()["brokers"].get(broker_id, {})
    return now_monotonic >= safe_float(item.get("next_allowed_monotonic"), 0.0)


def stable_broker_snapshot_digest(
    broker_id: str,
    accounts: list[dict[str, Any]],
    positions_data: list[dict[str, Any]],
) -> str:
    payload = {
        "broker": broker_id,
        "accounts": sorted(
            [
                {
                    "account": str(item.get("account") or ""),
                    "currency": str(item.get("currency") or ""),
                    "cash": safe_float(item.get("broker_cash", item.get("cash")), 0.0),
                }
                for item in accounts
            ],
            key=lambda item: (item["account"], item["currency"]),
        ),
        "positions": sorted(
            [
                {
                    "symbol": str(item.get("symbol") or ""),
                    "position_side": normalized_reconciliation_position_side(
                        item,
                        source="broker",
                    ),
                    "currency": str(item.get("currency") or ""),
                    "quantity": safe_float(item.get("broker_qty", item.get("quantity")), 0.0),
                    "value": safe_float(item.get("broker_value", item.get("value")), 0.0),
                }
                for item in positions_data
            ],
            key=lambda item: (
                item["symbol"],
                item["position_side"],
                item["currency"],
            ),
        ),
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def mark_broker_snapshot_success(
    broker_id: str,
    now_monotonic: float,
    digest: str,
    *,
    interval_seconds: float,
) -> bool:
    control = broker_poll_control()
    brokers = control["brokers"]
    previous = brokers.get(broker_id, {})
    changed = str(previous.get("snapshot_digest") or "") != digest
    brokers[broker_id] = {
        **previous,
        "last_attempt_monotonic": now_monotonic,
        "last_success_monotonic": now_monotonic,
        "next_allowed_monotonic": now_monotonic + interval_seconds,
        "failure_count": 0,
        "last_error": "",
        "snapshot_digest": digest,
        "last_status": "changed" if changed else "unchanged",
    }
    return changed


def mark_broker_snapshot_failure(
    broker_id: str,
    now_monotonic: float,
    detail: str,
    *,
    interval_seconds: float,
) -> None:
    control = broker_poll_control()
    brokers = control["brokers"]
    previous = brokers.get(broker_id, {})
    failures = int(safe_float(previous.get("failure_count"), 0.0)) + 1
    backoff = min(
        BROKER_SNAPSHOT_MAX_BACKOFF_SECONDS,
        interval_seconds * (2 ** min(max(0, failures - 1), 5)),
    )
    brokers[broker_id] = {
        **previous,
        "last_attempt_monotonic": now_monotonic,
        "next_allowed_monotonic": now_monotonic + backoff,
        "failure_count": failures,
        "last_error": audit_clip(detail, 240),
        "last_status": "backoff",
        "backoff_seconds": backoff,
    }


def merge_broker_reconciliation_cache(
    accounts: list[dict[str, Any]],
    positions_data: list[dict[str, Any]],
    successful_brokers: set[str],
    errors: list[dict[str, str]],
) -> None:
    if not successful_brokers and not errors:
        return
    cache = STATE.setdefault("broker_reconciliation", {})
    previous_accounts = cache.get("accounts", []) if isinstance(cache.get("accounts"), list) else []
    previous_positions = cache.get("positions", []) if isinstance(cache.get("positions"), list) else []
    previous_errors = cache.get("errors", []) if isinstance(cache.get("errors"), list) else []
    touched = set(successful_brokers) | {str(item.get("broker_id") or "") for item in errors}
    cache["accounts"] = [
        item for item in previous_accounts
        if str(item.get("broker_id") or "") not in touched
    ] + list(accounts)
    cache["positions"] = [
        item for item in previous_positions
        if str(item.get("broker_id") or "") not in touched
    ] + list(positions_data)
    cache["errors"] = [
        item for item in previous_errors
        if str(item.get("broker_id") or "") not in touched
    ] + [
        {"broker_id": item["broker_id"], "scope": "snapshot", "detail": item["detail"]}
        for item in errors
    ]
    failed_brokers = {str(item.get("broker_id") or "") for item in errors}
    account_success = (
        set(cache.get("successful_account_brokers", [])) - failed_brokers
    ) | set(successful_brokers)
    position_success = (
        set(cache.get("successful_position_brokers", [])) - failed_brokers
    ) | set(successful_brokers)
    cache["successful_account_brokers"] = sorted(account_success)
    cache["successful_position_brokers"] = sorted(position_success)
    cache["fetched_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def should_append_execution_sync_audit(
    *,
    now_monotonic: float,
    signature: str,
    has_new_events: bool,
    has_snapshot_changes: bool,
    has_errors: bool,
    attempted_snapshot: bool,
) -> bool:
    control = broker_poll_control()
    previous_signature = str(control.get("last_summary_signature") or "")
    last_audit = safe_float(control.get("last_summary_audit_monotonic"), 0.0)
    periodic_due = attempted_snapshot and now_monotonic - last_audit >= EXECUTION_SUMMARY_AUDIT_INTERVAL_SECONDS
    should_append = (
        has_new_events
        or has_snapshot_changes
        or (has_errors and signature != previous_signature)
        or periodic_due
    )
    if should_append:
        control["last_summary_signature"] = signature
        control["last_summary_audit_monotonic"] = now_monotonic
    return should_append


def poll_execution_events(
    broker_id: str = "all",
    *,
    force_snapshot: bool | None = None,
) -> dict[str, Any]:
    broker_ids = (
        "kis",
        "binance",
        "binance-futures",
        "upbit",
    ) if broker_id.strip().lower() in {"", "all"} else (
        broker_id.strip().lower(),
    )
    force = len(broker_ids) == 1 if force_snapshot is None else bool(force_snapshot)
    router = LiveBrokerRouter()
    events: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    snapshot_accounts: list[dict[str, Any]] = []
    snapshot_positions: list[dict[str, Any]] = []
    successful_snapshot_brokers: set[str] = set()
    changed_snapshot_brokers: set[str] = set()
    skipped_snapshot_brokers: set[str] = set()
    attempted_snapshot_brokers: set[str] = set()
    now_monotonic = time.monotonic()
    poll_interval = broker_snapshot_poll_interval_seconds()
    observe_execution_stream_connectivity(broker_ids)
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
            if not broker_snapshot_is_due(selected_broker, now_monotonic, force=force):
                skipped_snapshot_brokers.add(selected_broker)
                continue
            attempted_snapshot_brokers.add(selected_broker)
            result = router.poll_execution_events(selected_broker)
            rows = result.get("events", []) if isinstance(result, dict) else result
            if isinstance(rows, list):
                for row in rows:
                    if isinstance(row, dict):
                        row_state = str(row.get("state") or row.get("status") or "").strip().lower()
                        if row_state in {"account_snapshot", "position_snapshot"}:
                            continue
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
            account_rows: list[dict[str, Any]] = []
            position_rows: list[dict[str, Any]] = []
            received_snapshot = isinstance(result, dict) and (
                "accounts" in result or "positions" in result
            )
            if isinstance(accounts, list):
                account_rows = [item for item in accounts if isinstance(item, dict)]
                snapshot_accounts.extend(account_rows)
            if isinstance(positions_data, list):
                position_rows = [item for item in positions_data if isinstance(item, dict)]
                snapshot_positions.extend(position_rows)
            if received_snapshot:
                successful_snapshot_brokers.add(selected_broker)
                digest = stable_broker_snapshot_digest(selected_broker, account_rows, position_rows)
                if mark_broker_snapshot_success(
                    selected_broker,
                    now_monotonic,
                    digest,
                    interval_seconds=poll_interval,
                ):
                    changed_snapshot_brokers.add(selected_broker)
            else:
                mark_broker_snapshot_success(
                    selected_broker,
                    now_monotonic,
                    stable_broker_snapshot_digest(selected_broker, [], []),
                    interval_seconds=poll_interval,
                )
            record_connectivity_state("broker_api", selected_broker, healthy=True)
        except Exception as exc:  # Broker boundary: one adapter must not stop all monitoring.
            detail = f"{type(exc).__name__}: {exc}"[:500]
            errors.append({"broker_id": selected_broker, "detail": detail})
            mark_broker_snapshot_failure(
                selected_broker,
                now_monotonic,
                detail,
                interval_seconds=poll_interval,
            )
            record_connectivity_state("broker_api", selected_broker, healthy=False)
    events = deduplicate_execution_events(events)
    existing_event_ids = PROGRAM_LEDGER.existing_execution_event_ids([
        str(event.get("event_id") or "") for event in events
    ])
    new_events = [
        event
        for event in events
        if str(event.get("event_id") or "") not in existing_event_ids
    ]
    local_order_updates = apply_execution_events_to_local_orders(
        new_events,
        existing_event_ids,
    )
    record_new_execution_traces(new_events, existing_event_ids)
    recorded = PROGRAM_LEDGER.record_execution_events(new_events)
    changed_accounts = [
        item for item in snapshot_accounts
        if str(item.get("broker_id") or "") in changed_snapshot_brokers
    ]
    changed_positions = [
        item for item in snapshot_positions
        if str(item.get("broker_id") or "") in changed_snapshot_brokers
    ]
    # Broker snapshots are observations, not program-ledger truth.  Copying a
    # changed broker snapshot into the expected ledger here makes the
    # reconciliation that immediately follows compare the broker with its own
    # just-copied values, hiding deposits, manual orders, or other drift.
    # Establishing a broker-derived baseline remains an explicit operator
    # action through seed_program_ledger_from_broker_snapshot().
    observed = {
        "cash_count": len(changed_accounts),
        "position_count": len(changed_positions),
    }
    merge_broker_reconciliation_cache(
        snapshot_accounts,
        snapshot_positions,
        successful_snapshot_brokers,
        errors,
    )
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    STATE["execution_events"] = {
        "last_poll": now,
        "errors": errors,
        "event_count": len(events),
        "recorded_count": recorded,
        "local_order_update_count": local_order_updates,
        "synced_cash_count": 0,
        "synced_position_count": 0,
        "observed_cash_count": int(observed["cash_count"]),
        "observed_position_count": int(observed["position_count"]),
        "snapshot_changed_brokers": sorted(changed_snapshot_brokers),
        "snapshot_skipped_brokers": sorted(skipped_snapshot_brokers),
    }
    STATE["program_ledger"]["last_event_sync"] = now
    telegram_fill_count = notify_new_live_fills(events, existing_event_ids)
    new_event_count = len(new_events)
    audit_signature = hashlib.sha256(
        json.dumps(
            {
                "errors": errors,
                "newEventCount": new_event_count,
                "changedBrokers": sorted(changed_snapshot_brokers),
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    if should_append_execution_sync_audit(
        now_monotonic=now_monotonic,
        signature=audit_signature,
        has_new_events=bool(new_event_count),
        has_snapshot_changes=bool(changed_snapshot_brokers),
        has_errors=bool(errors),
        attempted_snapshot=bool(attempted_snapshot_brokers),
    ):
        append_audit(
            "warn" if errors else "info",
            "체결 이벤트 동기화",
            (
                f"신규 체결 이벤트 {new_event_count}건, "
                f"변경된 계좌 snapshot {len(changed_snapshot_brokers)}개 broker, "
                f"브로커 현금 {observed['cash_count']}개/포지션 {observed['position_count']}개 관측"
                "(프로그램 원장 미변경), "
                f"오류 {len(errors)}건"
            ),
        )
    automatic_results = (
        automatic_live_promotion_sweep(fresh_broker_ids=set(successful_snapshot_brokers))
        if new_event_count
        else []
    )
    return {
        "ok": not errors,
        "reason": f"체결 이벤트 동기화: 저장 {recorded}건, 오류 {len(errors)}건",
        "errors": errors,
        "program_ledger": program_ledger_snapshot(),
        "execution_events": execution_event_snapshot(),
        "local_order_update_count": local_order_updates,
        "telegram_fill_count": telegram_fill_count,
        "automatic_promotion": automatic_results,
        "snapshot": snapshot(),
    }


BINANCE_SMOKE_SYMBOL = "BTCUSDT"
BINANCE_SMOKE_QUANTITY = 0.0001
BINANCE_SMOKE_MIN_USDT = 5.0
BINANCE_SMOKE_MAX_USDT = 10.0
BINANCE_SMOKE_PREVIEW_TTL_SECONDS = 600
BINANCE_FUTURES_CANARY_TEST_TTL_SECONDS = 300


def binance_futures_canary_status() -> dict[str, Any]:
    with BINANCE_FUTURES_CANARY_LOCK:
        current = dict(STATE.get("binance_futures_canary", {}))
    current.pop("confirmation_token", None)
    current.pop("test_context", None)
    current.pop("test_requests", None)
    return current


def _binance_futures_ticker_price(symbol: str) -> float:
    normalized_symbol = normalize_usdm_symbol(symbol)
    base_url = (
        env_value("BINANCE_FUTURES_BASE_URL")
        or BINANCE_FUTURES_BASE_URL
    )
    response = http_json(
        "GET",
        (
            f"{base_url.rstrip('/')}/fapi/v1/ticker/price?"
            f"symbol={normalized_symbol}"
        ),
        body=None,
        headers={},
        timeout_seconds=10.0,
    )
    payload = (
        response.get("json")
        if isinstance(response.get("json"), dict)
        else {}
    )
    price = safe_float(payload.get("price"), 0.0)
    if response.get("ok") is not True or price <= 0:
        raise RuntimeError(
            "Binance Futures 현재가를 확인하지 못했습니다."
        )
    return price


def _artifact_custom_definition(strategy: dict[str, Any]) -> dict[str, Any]:
    parameters = (
        strategy.get("parameters")
        if isinstance(strategy.get("parameters"), dict)
        else {}
    )
    contract = (
        strategy.get("strategyContract")
        if isinstance(strategy.get("strategyContract"), dict)
        else strategy.get("strategy_contract")
        if isinstance(strategy.get("strategy_contract"), dict)
        else {}
    )
    for candidate in (
        parameters.get("customStrategyDefinition"),
        parameters.get("custom_strategy_definition"),
        strategy.get("customStrategyDefinition"),
        strategy.get("custom_strategy_definition"),
        contract.get("customStrategyDefinition"),
    ):
        if isinstance(candidate, dict):
            return candidate
    return {}


def _evaluate_binance_futures_canary(
    strategy_id: object,
    notional_usdt: object = 6.0,
) -> tuple[dict[str, Any], dict[str, str]]:
    selected_id = str(strategy_id or "").strip()
    strategies = strategy_rows()
    strategy = next(
        (
            item
            for item in strategies
            if str(item.get("strategy_id") or item.get("id") or "")
            == selected_id
        ),
        None,
    )
    symbol = normalize_usdm_symbol(
        (strategy or {}).get("symbol") or "BTCUSDT"
    )
    definition = _artifact_custom_definition(strategy or {})
    direction = str(
        definition.get("positionDirection")
        or (strategy or {}).get("position_direction")
        or (strategy or {}).get("positionDirection")
        or "long"
    ).strip().lower()
    lifecycle = normalize_lifecycle_status(
        (strategy or {}).get("lifecycleStatus")
        or (strategy or {}).get("status")
        or (
            (strategy or {}).get("lifecycle", {}).get("status")
            if isinstance((strategy or {}).get("lifecycle"), dict)
            else ""
        )
    )
    scope = (
        current_live_canary_scope(selected_id, materialize=False)
        if strategy is not None
        else {}
    )
    broker_id = strategy_broker_id(strategy) if strategy is not None else ""
    market_type = str(
        (strategy or {}).get("market_type")
        or (strategy or {}).get("marketType")
        or ""
    ).strip().lower()
    strategy_gate = {
        "found": strategy is not None,
        "broker_id": broker_id,
        "symbol": symbol,
        "symbol_matches": bool(symbol),
        "market_type": market_type,
        "position_direction": direction,
        "short_authorized": (
            strategy is not None
            and direction == "short"
            and broker_id == "binance-futures"
            and market_type
            in {"future", "futures", "perpetual", "swap"}
        ),
        "lifecycle_status": lifecycle,
        "live_small_eligible": (
            (strategy or {}).get("live_small_eligible") is True
        ),
        "deployment_provenance_ok": bool(
            scope.get("deploymentId")
            and scope.get("strategyArtifactHash")
            and scope.get("strategyId")
        ),
        "canary_scope_ok": scope.get("eligible") is True,
    }
    observation: dict[str, Any] = {
        "account": {},
        "position_mode": {},
        "symbol_config": {},
        "position_count": None,
        "open_order_count": None,
    }
    observation_errors: list[str] = []
    price: float | None = None
    rules: dict[str, Any] = {}
    try:
        observation = (
            LiveBrokerRouter()
            .get_binance_futures_canary_observation(symbol)
        )
    except (BrokerNotReadyError, RuntimeError):
        observation_errors.extend(
            (
                "account-observation-failed",
                "position-mode-observation-failed",
                "symbol-config-observation-failed",
                "positions-observation-failed",
                "open-orders-observation-failed",
            )
        )
    try:
        price = _binance_futures_ticker_price(symbol)
    except RuntimeError:
        observation_errors.append("ticker-observation-failed")
    try:
        rules = binance_symbol_rules(symbol, futures=True)
    except RuntimeError:
        observation_errors.append("exchange-rules-observation-failed")
    quantity_result = derive_canary_quantity(
        target_notional_usdt=notional_usdt,
        price=price,
        min_qty=rules.get("minQty"),
        max_qty=rules.get("maxQty"),
        step_size=rules.get("stepSize"),
        exchange_min_notional=rules.get("minNotional"),
    )
    report = evaluate_futures_canary_preflight(
        strategy_gate=strategy_gate,
        account=(
            observation.get("account")
            if isinstance(observation.get("account"), dict)
            else {}
        ),
        position_mode=(
            observation.get("position_mode")
            if isinstance(observation.get("position_mode"), dict)
            else {}
        ),
        symbol_config=(
            observation.get("symbol_config")
            if isinstance(observation.get("symbol_config"), dict)
            else {}
        ),
        position_count=(
            int(observation["position_count"])
            if isinstance(observation.get("position_count"), int)
            else None
        ),
        open_order_count=(
            int(observation["open_order_count"])
            if isinstance(observation.get("open_order_count"), int)
            else None
        ),
        requested_notional_usdt=notional_usdt,
        quantity_result=quantity_result,
        real_orders_enabled=real_orders_enabled(),
        observation_errors=tuple(dict.fromkeys(observation_errors)),
    )
    report["strategy"] = {
        "strategy_id": selected_id,
        "symbol": symbol,
        "broker_id": broker_id,
        "market_type": market_type,
        "position_direction": direction,
        "lifecycle_status": lifecycle,
        "short_authorized": strategy_gate["short_authorized"],
    }
    report["detail"] = (
        "test order 사전점검 통과"
        if report["ready_for_test"]
        else "차단: " + ", ".join(report["test_blockers"])
    )
    context = {
        "strategy_id": selected_id,
        "symbol": symbol,
        "requested_notional_usdt": str(notional_usdt),
        "quantity": str(
            report.get("order_plan", {}).get("quantity") or ""
        ),
        "estimated_notional_usdt": str(
            report.get("order_plan", {}).get(
                "estimated_notional_usdt"
            )
            or ""
        ),
        "scope_id": str(scope.get("scopeId") or ""),
    }
    return report, context


def preview_binance_futures_canary(
    strategy_id: object,
    notional_usdt: object = 6.0,
) -> dict[str, Any]:
    with BINANCE_FUTURES_CANARY_LOCK:
        report, context = _evaluate_binance_futures_canary(
            strategy_id,
            notional_usdt,
        )
        now = datetime.now(timezone.utc)
        expires = now + timedelta(
            seconds=BINANCE_FUTURES_CANARY_TEST_TTL_SECONDS
        )
        token = (
            secrets.token_urlsafe(32)
            if report["ready_for_test"]
            else ""
        )
        stored = {
            **report,
            "confirmation_token": token,
            "expires_at": (
                expires.isoformat().replace("+00:00", "Z")
                if token
                else ""
            ),
            "expires_epoch": expires.timestamp() if token else 0.0,
            "used": False,
            "test_context": context if token else {},
            "test_result": {},
        }
        STATE["binance_futures_canary"] = stored
    append_audit(
        "info" if report["ready_for_test"] else "warn",
        "Binance Futures canary 사전점검",
        (
            f"{context.get('strategy_id') or '-'} "
            f"{context.get('symbol') or '-'} · "
            f"test blockers {len(report['test_blockers'])}개 · "
            f"start blockers {len(report['start_blockers'])}개"
        ),
    )
    return {
        "ok": report["ready_for_test"],
        "reason": report["detail"],
        "canary": binance_futures_canary_status(),
        "test_authorization": (
            {
                "confirmation_token": token,
                "expires_at": stored["expires_at"],
            }
            if token
            else {}
        ),
    }


def _sanitize_binance_futures_test_leg(
    leg: str,
    intent: dict[str, object],
    response: dict[str, object],
) -> dict[str, object]:
    payload = (
        response.get("json")
        if isinstance(response.get("json"), dict)
        else {}
    )
    exchange_code = payload.get("code")
    message = str(payload.get("msg") or "")[:160]
    return {
        "leg": leg,
        "side": str(intent.get("side") or ""),
        "position_side": "SHORT",
        "risk_reducing": intent.get("risk_reducing") is True,
        "ok": response.get("ok") is True,
        "status_code": int(
            safe_float(response.get("statusCode"), 0.0)
        ),
        "exchange_code": (
            str(exchange_code)[:40]
            if exchange_code not in (None, "")
            else None
        ),
        "message": message,
    }


def _binance_futures_test_result(
    *,
    status: str,
    context: dict[str, str],
    legs: list[dict[str, object]] | None = None,
    reason_id: str = "",
) -> dict[str, object]:
    return {
        "status": status,
        "endpoint": BINANCE_FUTURES_TEST_ORDER_ENDPOINT,
        "submitted_to_matching_engine": False,
        "strategy_id": context.get("strategy_id", ""),
        "symbol": context.get("symbol", ""),
        "quantity": context.get("quantity", ""),
        "estimated_notional_usdt": context.get(
            "estimated_notional_usdt",
            "",
        ),
        "reason_id": reason_id,
        "legs": list(legs or []),
        "validated_at": (
            datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
        ),
    }


def test_binance_futures_canary_order(
    confirmation_token: object,
    *,
    confirmed: bool,
) -> dict[str, Any]:
    token = str(confirmation_token or "")
    with BINANCE_FUTURES_CANARY_LOCK:
        current = dict(STATE.get("binance_futures_canary", {}))
        stored_token = str(current.get("confirmation_token") or "")
        if (
            not confirmed
            or not token
            or not stored_token
            or not secrets.compare_digest(token, stored_token)
        ):
            return {
                "ok": False,
                "reason": (
                    "정확한 Binance Futures test order 미리보기의 "
                    "1회 확인 토큰이 필요합니다."
                ),
                "canary": binance_futures_canary_status(),
            }
        if (
            current.get("ready_for_test") is not True
            or current.get("status") != "test_ready"
            or current.get("used") is True
        ):
            return {
                "ok": False,
                "reason": "이미 사용되었거나 test order 가능한 미리보기가 아닙니다.",
                "canary": binance_futures_canary_status(),
            }
        if (
            safe_float(current.get("expires_epoch"), 0.0)
            <= datetime.now(timezone.utc).timestamp()
        ):
            current.update(
                {
                    "status": "expired",
                    "used": True,
                    "confirmation_token": "",
                    "detail": (
                        "test order 미리보기가 만료되었습니다. "
                        "다시 사전점검하세요."
                    ),
                }
            )
            STATE["binance_futures_canary"] = current
            return {
                "ok": False,
                "reason": current["detail"],
                "canary": binance_futures_canary_status(),
            }
        context = (
            dict(current.get("test_context", {}))
            if isinstance(current.get("test_context"), dict)
            else {}
        )
        if not all(
            context.get(key)
            for key in (
                "strategy_id",
                "symbol",
                "requested_notional_usdt",
                "quantity",
                "scope_id",
            )
        ):
            current.update(
                {
                    "status": "blocked",
                    "used": True,
                    "confirmation_token": "",
                    "detail": "test order 미리보기 문맥이 완전하지 않습니다.",
                }
            )
            STATE["binance_futures_canary"] = current
            return {
                "ok": False,
                "reason": current["detail"],
                "canary": binance_futures_canary_status(),
            }

        # Consume before any action-time broker observation or signed POST.
        # A timeout or partial validation must never make the token reusable.
        current.update(
            {
                "status": "test_validating",
                "used": True,
                "confirmation_token": "",
                "detail": "action-time 사전점검 재검증 중",
            }
        )
        STATE["binance_futures_canary"] = current

        fresh_report, fresh_context = _evaluate_binance_futures_canary(
            context["strategy_id"],
            context["requested_notional_usdt"],
        )
        bound_fields = (
            "strategy_id",
            "symbol",
            "requested_notional_usdt",
            "quantity",
            "scope_id",
        )
        context_matches = all(
            fresh_context.get(key) == context.get(key)
            for key in bound_fields
        )
        if fresh_report.get("ready_for_test") is not True or not context_matches:
            reason_id = (
                "preview-context-changed"
                if fresh_report.get("ready_for_test") is True
                else "action-time-preflight-blocked"
            )
            if reason_id not in fresh_report["test_blockers"]:
                fresh_report["test_blockers"].append(reason_id)
            if reason_id not in fresh_report["start_blockers"]:
                fresh_report["start_blockers"].append(reason_id)
            test_result = _binance_futures_test_result(
                status="blocked",
                context=context,
                reason_id=reason_id,
            )
            fresh_report.update(
                {
                    "status": "blocked",
                    "ready_for_test": False,
                    "used": True,
                    "confirmation_token": "",
                    "expires_at": current.get("expires_at", ""),
                    "expires_epoch": current.get("expires_epoch", 0.0),
                    "test_context": context,
                    "test_result": test_result,
                    "detail": (
                        "action-time 사전점검이 변경되어 test order를 "
                        "전송하지 않았습니다."
                    ),
                }
            )
            STATE["binance_futures_canary"] = fresh_report
            append_audit(
                "warn",
                "Binance Futures test order",
                (
                    f"{context['strategy_id']} {context['symbol']} · "
                    f"{reason_id} · matching engine 전송 없음"
                ),
            )
            return {
                "ok": False,
                "reason": fresh_report["detail"],
                "test": test_result,
                "canary": binance_futures_canary_status(),
            }

        fingerprint = hashlib.sha256(token.encode("utf-8")).hexdigest()
        entry, cover = build_futures_canary_test_intents(
            strategy_id=context["strategy_id"],
            symbol=context["symbol"],
            quantity=context["quantity"],
            token_fingerprint=fingerprint,
        )
        router = LiveBrokerRouter()
        legs: list[dict[str, object]] = []
        for leg_name, intent in (("entry", entry), ("cover", cover)):
            try:
                response = router.test_binance_futures_order(intent)
                leg_result = _sanitize_binance_futures_test_leg(
                    leg_name,
                    intent,
                    response,
                )
            except (BrokerNotReadyError, RuntimeError):
                leg_result = {
                    "leg": leg_name,
                    "side": str(intent.get("side") or ""),
                    "position_side": "SHORT",
                    "risk_reducing": (
                        intent.get("risk_reducing") is True
                    ),
                    "ok": False,
                    "status_code": 0,
                    "exchange_code": None,
                    "message": "Binance Futures test order 사전검사 실패",
                }
            legs.append(leg_result)
            if leg_result["ok"] is not True:
                break

        succeeded = len(legs) == 2 and all(
            leg.get("ok") is True for leg in legs
        )
        test_result = _binance_futures_test_result(
            status="validated" if succeeded else "failed",
            context=context,
            legs=legs,
            reason_id="" if succeeded else "broker-test-rejected",
        )
        fresh_report.update(
            {
                "status": (
                    "test_validated" if succeeded else "test_failed"
                ),
                "used": True,
                "confirmation_token": "",
                "expires_at": current.get("expires_at", ""),
                "expires_epoch": current.get("expires_epoch", 0.0),
                "test_context": context,
                "test_result": test_result,
                "detail": (
                    "SELL/BUY SHORT test order 검증 통과 · "
                    "matching engine 전송 없음"
                    if succeeded
                    else "Binance Futures test order 검증 실패"
                ),
            }
        )
        STATE["binance_futures_canary"] = fresh_report
        append_audit(
            "info" if succeeded else "warn",
            "Binance Futures test order",
            (
                f"{context['strategy_id']} {context['symbol']} · "
                f"{len(legs)}/2 legs · "
                "matching engine 전송 없음"
            ),
        )
        return {
            "ok": succeeded,
            "reason": fresh_report["detail"],
            "test": test_result,
            "canary": binance_futures_canary_status(),
        }


def _binance_smoke_order_view(**updates: Any) -> dict[str, Any]:
    current = dict(STATE.get("binance_smoke_order", {}))
    current.update(updates)
    STATE["binance_smoke_order"] = current
    return current


def _binance_ticker_price() -> float:
    base_url = env_value("BINANCE_BASE_URL") or BINANCE_BASE_URL
    response = http_json(
        "GET",
        f"{base_url.rstrip('/')}/api/v3/ticker/price?symbol={BINANCE_SMOKE_SYMBOL}",
        body=None,
        headers={},
        timeout_seconds=10.0,
    )
    payload = response.get("json") if isinstance(response.get("json"), dict) else {}
    price = safe_float(payload.get("price"), 0.0)
    if response.get("ok") is not True or price <= 0:
        raise RuntimeError(str(response.get("text") or "Binance BTCUSDT 현재가 조회 실패"))
    return price


def preview_binance_smoke_order(strategy_id: str) -> dict[str, Any]:
    strategy = next(
        (
            item
            for item in strategy_rows()
            if str(item.get("strategy_id") or "") == str(strategy_id or "")
            and item.get("live_small_eligible") is True
            and strategy_broker_id(item) == "binance"
            and str(item.get("symbol") or "").upper() == BINANCE_SMOKE_SYMBOL
        ),
        None,
    )
    if strategy is None:
        reason = "선택한 Binance Strategy Instance가 before-live-small 및 검증 evidence를 통과하지 못했습니다."
        _binance_smoke_order_view(status="blocked", status_label="차단", detail=reason, confirmation_token="")
        return {"ok": False, "reason": reason, "snapshot": snapshot()}

    router = LiveBrokerRouter()
    try:
        account_snapshot = router.get_account_snapshot("binance")
        price = _binance_ticker_price()
    except (BrokerNotReadyError, RuntimeError) as exc:
        reason = str(exc)
        _binance_smoke_order_view(status="blocked", status_label="조회 실패", detail=reason, confirmation_token="")
        append_audit("danger", "Binance 소액 주문 미리보기", reason)
        return {"ok": False, "reason": reason, "snapshot": snapshot()}

    accounts = account_snapshot.get("accounts", []) if isinstance(account_snapshot, dict) else []
    usdt_row = next(
        (
            item
            for item in accounts
            if isinstance(item, dict) and str(item.get("currency") or "").upper() == "USDT"
        ),
        {},
    )
    available_usdt = safe_float(usdt_row.get("broker_cash"), 0.0)
    notional_usdt = BINANCE_SMOKE_QUANTITY * price
    blocked_reasons: list[str] = []
    if notional_usdt < BINANCE_SMOKE_MIN_USDT:
        blocked_reasons.append(f"거래소 최소 주문 {BINANCE_SMOKE_MIN_USDT:g} USDT 미만")
    if notional_usdt > BINANCE_SMOKE_MAX_USDT:
        blocked_reasons.append(f"점검 주문 하드 한도 {BINANCE_SMOKE_MAX_USDT:g} USDT 초과")
    if available_usdt + 1e-9 < notional_usdt * 1.01:
        blocked_reasons.append("수수료 여유분을 포함한 USDT 잔고 부족")

    prepared = build_binance_spot_order_request(
        {
            "broker_id": "binance",
            "symbol": BINANCE_SMOKE_SYMBOL,
            "side": "BUY",
            "quantity": BINANCE_SMOKE_QUANTITY,
            "order_type": "MARKET",
        }
    )
    if not prepared.can_send:
        blocked_reasons.append("주문 요청 설정 누락: " + ", ".join(prepared.blocked_reasons))
    ready = not blocked_reasons
    now = datetime.now(timezone.utc)
    expires = now + timedelta(seconds=BINANCE_SMOKE_PREVIEW_TTL_SECONDS)
    token = secrets.token_urlsafe(24) if ready else ""
    detail = (
        f"{BINANCE_SMOKE_SYMBOL} 시장가 매수 {BINANCE_SMOKE_QUANTITY:g} BTC · "
        f"현재가 {price:,.2f} USDT · 예상 {notional_usdt:.2f} USDT · 가용 {available_usdt:.2f} USDT"
        if ready
        else " · ".join(blocked_reasons)
    )
    preview = _binance_smoke_order_view(
        status="ready" if ready else "blocked",
        status_label="확인 대기" if ready else "차단",
        strategy_id=str(strategy.get("strategy_id") or ""),
        symbol=BINANCE_SMOKE_SYMBOL,
        side="BUY",
        order_type="MARKET",
        quantity=BINANCE_SMOKE_QUANTITY,
        price=price,
        notional_usdt=notional_usdt,
        available_usdt=available_usdt,
        confirmation_token=token,
        expires_at=expires.isoformat().replace("+00:00", "Z"),
        expires_epoch=expires.timestamp(),
        used=False,
        broker_order_id="",
        detail=detail,
        request_preview=prepared.preview(),
        blocked_reasons=blocked_reasons,
    )
    append_audit("info" if ready else "danger", "Binance 소액 주문 미리보기", detail + (" · 실제 주문 전송 없음" if ready else ""))
    return {"ok": ready, "reason": detail, "preview": preview, "snapshot": snapshot()}


def submit_binance_smoke_order(confirmation_token: object, *, confirmed: bool) -> dict[str, Any]:
    preview = dict(STATE.get("binance_smoke_order", {}))
    token = str(confirmation_token or "")
    if not confirmed or not token or not secrets.compare_digest(token, str(preview.get("confirmation_token") or "")):
        return {"ok": False, "reason": "정확한 Binance 주문 미리보기의 1회 확인 토큰이 필요합니다.", "snapshot": snapshot()}
    if preview.get("status") != "ready" or bool(preview.get("used")):
        return {"ok": False, "reason": "이미 사용되었거나 전송 가능한 미리보기가 아닙니다.", "snapshot": snapshot()}
    if safe_float(preview.get("expires_epoch"), 0.0) <= datetime.now(timezone.utc).timestamp():
        _binance_smoke_order_view(status="expired", status_label="만료", confirmation_token="", detail="미리보기가 만료되었습니다.")
        return {"ok": False, "reason": "미리보기가 만료되었습니다. 다시 조회하세요.", "snapshot": snapshot()}
    if STATE.get("kill_switch") or STATE.get("new_entries_blocked"):
        return {"ok": False, "reason": "긴급/신규 진입 차단이 켜져 있어 실제 주문을 전송할 수 없습니다.", "snapshot": snapshot()}
    if STATE.get("dry_run") or current_mode() != "SMALL_LIVE":
        return {"ok": False, "reason": "Dry Run을 해제하고 SMALL_LIVE에서만 점검 주문을 보낼 수 있습니다.", "snapshot": snapshot()}
    if not real_orders_enabled():
        return {"ok": False, "reason": "LIVE_TRADER_ENABLE_REAL_ORDERS=true가 필요합니다.", "snapshot": snapshot()}

    try:
        fresh_price = _binance_ticker_price()
        fresh_account = LiveBrokerRouter().get_account_snapshot("binance")
        accounts = fresh_account.get("accounts", []) if isinstance(fresh_account, dict) else []
        usdt_row = next(
            (
                item
                for item in accounts
                if isinstance(item, dict) and str(item.get("currency") or "").upper() == "USDT"
            ),
            {},
        )
        fresh_cash = safe_float(usdt_row.get("broker_cash"), 0.0)
        fresh_notional = BINANCE_SMOKE_QUANTITY * fresh_price
        if fresh_notional < BINANCE_SMOKE_MIN_USDT or fresh_notional > BINANCE_SMOKE_MAX_USDT:
            raise BrokerNotReadyError(f"전송 직전 주문 금액 {fresh_notional:.2f} USDT가 5~10 USDT 하드 한도를 벗어났습니다.")
        if fresh_cash + 1e-9 < fresh_notional * 1.01:
            raise BrokerNotReadyError("전송 직전 수수료 여유분을 포함한 USDT 잔고가 부족합니다.")
    except (BrokerNotReadyError, RuntimeError) as exc:
        reason = str(exc)
        _binance_smoke_order_view(status="blocked", status_label="전송 직전 차단", detail=reason, confirmation_token="")
        append_audit("danger", "Binance 실제 소액 주문", reason)
        return {"ok": False, "reason": reason, "snapshot": snapshot()}

    _binance_smoke_order_view(status="submitting", status_label="전송 중", used=True, confirmation_token="")
    strategy_id = str(preview.get("strategy_id") or "")
    intent = OrderIntent(
        strategy_id=strategy_id,
        asset="CRYPTO",
        symbol=BINANCE_SMOKE_SYMBOL,
        side="BUY",
        quantity=BINANCE_SMOKE_QUANTITY,
        reference_price=fresh_price,
        mode="SMALL_LIVE",
        reason="승급 전략 기반 Binance 브로커 제출 경로 소액 검증(전략 신호 아님)",
        metadata={
            "broker_id": "binance",
            "profile_id": "crypto",
            "strategy_instance_id": strategy_id,
            "instrument_id": BINANCE_SMOKE_SYMBOL,
            "target_revision": int(datetime.now(timezone.utc).timestamp()),
            "order_purpose": "BROKER_SMOKE",
            "order_type": "MARKET",
        },
    )
    result = submit_order_intent(snapshot(), intent, dry_run=False, audit_event="Binance Broker Smoke")
    order = result.get("order") if isinstance(result.get("order"), dict) else {}
    _binance_smoke_order_view(
        status="acknowledged" if result.get("ok") else "rejected",
        status_label="접수" if result.get("ok") else "거절/차단",
        broker_order_id=str(order.get("broker_order_id") or ""),
        detail=str(result.get("reason") or ""),
        order_id=str(order.get("order_id") or ""),
        order_state=str(order.get("state") or ""),
    )
    return {**result, "smoke_order": dict(STATE["binance_smoke_order"])}


UPBIT_SMOKE_MARKET = "KRW-BTC"
UPBIT_SMOKE_MIN_KRW = 5_000
UPBIT_SMOKE_MAX_KRW = 10_000
UPBIT_SMOKE_PREVIEW_TTL_SECONDS = 600


def _upbit_smoke_order_view(**updates: Any) -> dict[str, Any]:
    current = dict(STATE.get("upbit_smoke_order", {}))
    current.update(updates)
    STATE["upbit_smoke_order"] = current
    return current


def approved_upbit_smoke_strategy(strategy_id: object) -> dict[str, Any] | None:
    selected_id = str(strategy_id or "").strip()
    if not selected_id:
        return None
    minimum_stage = lifecycle_rank("before-live-small")
    return next(
        (
            item
            for item in strategy_rows()
            if str(item.get("strategy_id") or "") == selected_id
            and str(item.get("symbol") or "").strip().upper() == UPBIT_SMOKE_MARKET
            and strategy_broker_id(item) == "upbit"
            and item.get("live_small_eligible") is True
            and lifecycle_rank(normalize_lifecycle_status(item.get("lifecycle_status"))) >= minimum_stage
            and normalize_lifecycle_status(item.get("lifecycle_status")) not in {"paused", "retired"}
        ),
        None,
    )


def preview_upbit_smoke_order(
    strategy_id: object,
    notional_krw: object = UPBIT_SMOKE_MIN_KRW,
) -> dict[str, Any]:
    strategy = approved_upbit_smoke_strategy(strategy_id)
    if strategy is None:
        reason = "선택한 Upbit KRW-BTC Strategy Instance가 before-live-small 및 검증 evidence를 통과하지 못했습니다."
        _upbit_smoke_order_view(
            status="blocked",
            status_label="차단",
            strategy_id=str(strategy_id or ""),
            detail=reason,
            confirmation_token="",
        )
        return {"ok": False, "reason": reason, "snapshot": snapshot()}

    amount = int(safe_float(notional_krw, 0.0))
    if amount < UPBIT_SMOKE_MIN_KRW or amount > UPBIT_SMOKE_MAX_KRW:
        reason = f"Upbit 점검 주문은 {UPBIT_SMOKE_MIN_KRW:,}~{UPBIT_SMOKE_MAX_KRW:,}원만 허용합니다."
        _upbit_smoke_order_view(
            status="blocked",
            status_label="차단",
            strategy_id=str(strategy.get("strategy_id") or ""),
            detail=reason,
            confirmation_token="",
        )
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
        "strategy_id": str(strategy.get("strategy_id") or ""),
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
        strategy_id=str(strategy.get("strategy_id") or ""),
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
    strategy = approved_upbit_smoke_strategy(preview.get("strategy_id"))
    if strategy is None:
        return {
            "ok": False,
            "reason": "미리보기 전략의 Upbit before-live-small/live_small_eligible 승인이 더 이상 유효하지 않습니다.",
            "snapshot": snapshot(),
        }
    if STATE.get("kill_switch") or STATE.get("new_entries_blocked"):
        return {"ok": False, "reason": "긴급/신규 진입 차단이 켜져 있어 실제 주문을 전송할 수 없습니다.", "snapshot": snapshot()}
    if STATE.get("dry_run") or current_mode() != "SMALL_LIVE":
        return {"ok": False, "reason": "Dry Run을 해제하고 SMALL_LIVE에서만 점검 주문을 보낼 수 있습니다.", "snapshot": snapshot()}
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
        "strategy_id": str(strategy.get("strategy_id") or ""),
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
    broker_truth = broker_position_truth_snapshot(reconciliation)
    was_broker_blocked = bool(STATE.get("broker_truth_blocked"))
    STATE["broker_truth_blocked"] = bool(broker_truth.get("newEntriesBlocked"))
    if STATE["broker_truth_blocked"]:
        STATE["new_entries_blocked"] = True
    elif was_broker_blocked and not STATE.get("kill_switch") and not STATE.get("manual_new_entries_blocked"):
        STATE["new_entries_blocked"] = False
    append_audit(
        "danger" if int(summary["mismatch_count"]) or broker_data["errors"] else "warn" if int(summary["api_required_count"]) or int(summary.get("capability_gap_count") or 0) else "info",
        "포지션/계좌 대조",
        (
            f"{summary['status_label']}: API/원장 필요 {summary['api_required_count']}개, "
            f"미지원 capability {summary.get('capability_gap_count', 0)}개, "
            f"불일치 {summary['mismatch_count']}개, 조회 오류 {len(broker_data['errors'])}개"
        ),
    )
    automatic_results = automatic_live_promotion_sweep()
    return {
        "ok": True,
        "reason": f"대조 완료: {summary['status_label']} 상태",
        "reconciliation": reconciliation,
        "broker_position_truth": broker_truth,
        "automatic_promotion": automatic_results,
        "snapshot": snapshot(),
    }


def run_final_preflight() -> dict[str, Any]:
    STATE["preflight_last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data = snapshot()
    hard_stop_count = int(data["launch_report"]["hard_stop_count"])
    warning_count = int(data["launch_report"]["warning_count"])
    doctor_diagnostics = persist_doctor_diagnostic_snapshot(data)
    append_audit(
        "danger" if hard_stop_count else "warn" if warning_count else "info",
        "최종 Preflight",
        f"hard stop {hard_stop_count}개, warning {warning_count}개. {data['launch_report']['lock_reason']}",
    )
    return {
        "ok": True,
        "reason": f"최종 점검 완료: hard stop {hard_stop_count}개, warning {warning_count}개",
        "doctor_diagnostics": doctor_diagnostics,
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
    quote_error = (
        kis_overseas_next_open_quote_error(intent)
        if not dry_run
        else ""
    )
    if quote_error:
        report = PreTradeRiskReport(
            report.checked_at,
            (
                *report.checks,
                RiskCheck("실행 가격 수명주기", "fail", quote_error),
            ),
        )
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
        return True, "approved", "ready", "Pre-trade 위험 게이트와 실주문 어댑터 검증을 통과했습니다.", report

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
    metadata = intent.metadata if isinstance(intent.metadata, dict) else {}
    broker_id = str(
        metadata.get("broker_id")
        or broker_id_from_symbol(intent.symbol, intent.asset)
    ).strip().lower()
    asset = str(intent.asset or "").strip().lower()
    symbol = str(intent.symbol or "").strip().upper()
    if broker_id in {"binance", "binance-futures", "upbit"} or any(
        token in asset for token in ("crypto", "코인")
    ):
        return None
    local_code = symbol.removesuffix(".KS").removesuffix(".KQ")
    domestic = (
        (local_code.isdigit() and len(local_code) == 6)
        or symbol.endswith((".KS", ".KQ"))
        or any(
            token in asset
            for token in ("한국", "kr_stock", "stock_kr", "korean")
        )
    )
    if domestic:
        return market_session_state("XKRX", regular_open="09:00", regular_close="15:30")
    if broker_id == "kis" or any(
        token in f"{asset} {symbol}".lower()
        for token in ("미국", "us_stock", "stock_us", "nyse", "nasdaq", "amex")
    ):
        return market_session_state("XNYS", regular_open="09:30", regular_close="16:00")
    return None


def kis_overseas_next_open_quote_error(intent: OrderIntent) -> str:
    """Require a fresh priced quote before an automated KIS overseas order."""

    metadata = intent.metadata if isinstance(intent.metadata, dict) else {}
    broker_id = str(
        metadata.get("broker_id")
        or broker_id_from_symbol(intent.symbol, intent.asset)
    ).strip().lower()
    if broker_id != "kis":
        return ""
    symbol = str(intent.symbol or "").strip().upper()
    local_code = symbol.removesuffix(".KS").removesuffix(".KQ")
    domestic = (
        (local_code.isdigit() and len(local_code) == 6)
        or symbol.endswith((".KS", ".KQ"))
    )
    if domestic or str(metadata.get("execution_timing") or "") != "next-open-boundary":
        return ""
    if str(metadata.get("order_type") or "") != "00":
        return "KIS 미국주식 자동 주문은 신선한 지정가(ORD_DVSN=00)만 허용합니다."
    quote_price = safe_float(metadata.get("fresh_quote_price"), 0.0)
    quote_time_text = str(metadata.get("fresh_quote_observed_at") or "")
    try:
        quote_time = datetime.fromisoformat(
            quote_time_text.replace("Z", "+00:00")
        )
        if quote_time.tzinfo is None:
            quote_time = quote_time.replace(tzinfo=timezone.utc)
        quote_age = max(
            0.0,
            (
                datetime.now(timezone.utc)
                - quote_time.astimezone(timezone.utc)
            ).total_seconds(),
        )
    except (TypeError, ValueError):
        quote_age = float("inf")
    if (
        metadata.get("fresh_quote_verified") is not True
        or quote_price <= 0
        or quote_age > 5.0
    ):
        return (
            "KIS 미국주식 next-open 주문은 5초 이내 실시간 호가와 "
            "미체결 취소/부분체결 수명주기 확인 전까지 차단됩니다."
        )
    return ""


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
        default=(
            1.0
            if strategy_broker_id(strategy)
            in {"binance", "binance-futures"}
            else 1000.0
        ),
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
            matched = [
                strategy
                for strategy in strategies
                if strategy_broker_id(strategy)
                in {"binance", "binance-futures", "upbit"}
            ]
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
                metadata={
                    "broker_id": strategy_broker_id(strategy),
                    "profile_id": normalized_profile,
                    "runner": "StrategyExecutionRunner",
                    "market_type": str(strategy.get("market_type") or "spot").lower(),
                    "position_direction": str(strategy.get("position_direction") or "long").lower(),
                    "short_entries_requested": strategy.get("allow_short_requested") is True,
                    "broker_short_adapter_verified": (
                        strategy_broker_id(strategy) == "binance-futures"
                        and str(
                            strategy.get("market_type") or ""
                        ).lower()
                        in {"future", "futures", "perpetual"}
                    ),
                    "max_leverage": 1.0,
                    "required_margin_type": "ISOLATED",
                },
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


def intent_readiness_blocker_count(
    checks: dict[str, Any],
    intent: OrderIntent,
    reconciliation_summary: dict[str, Any] | None = None,
) -> int:
    if not checks.get("strategies") or not checks.get("brokers"):
        return int(checks.get("summary", {}).get("blocker_count", 0))
    metadata = intent.metadata if isinstance(intent.metadata, dict) else {}
    broker_id = str(metadata.get("broker_id") or broker_id_from_symbol(intent.symbol, intent.asset)).strip().lower()
    profile_id = str(
        metadata.get("profile_id")
        or (
            "crypto"
            if broker_id in {"binance", "binance-futures", "upbit"}
            else "stock"
        )
    )
    scoped = reconciliation_summary or reconciliation_summary_for_broker(broker_id)
    blockers = profile_readiness_blocker_count(
        checks,
        profile_id,
        broker_id,
        intent.mode,
        reconciliation_summary=scoped,
    )
    if intent_requires_unavailable_capability(intent, scoped):
        blockers += 1
    return blockers


def intent_requires_unavailable_capability(intent: OrderIntent, reconciliation_summary: dict[str, Any]) -> bool:
    if int(reconciliation_summary.get("capability_gap_count") or 0) == 0:
        return False
    metadata = intent.metadata if isinstance(intent.metadata, dict) else {}
    broker_id = str(metadata.get("broker_id") or broker_id_from_symbol(intent.symbol, intent.asset)).strip().lower()
    if broker_id != "kis":
        return False
    symbol = str(intent.symbol or "").strip().upper()
    local_code = symbol.split(":")[-1].split(".")[0]
    asset = str(intent.asset or "").strip().lower()
    domestic_asset = any(token in asset for token in ("한국", "kr-stock", "kr_stock", "korean"))
    domestic_symbol = (len(local_code) == 6 and local_code.isdigit()) or symbol.endswith((".KS", ".KQ"))
    return not (domestic_asset or domestic_symbol)


def pre_trade_context(checks: dict[str, Any], intent: OrderIntent, dry_run: bool) -> PreTradeContext:
    settings = STATE["risk_settings"]
    metadata = intent.metadata if isinstance(intent.metadata, dict) else {}
    has_scoped_context = bool(checks.get("strategies")) and bool(checks.get("brokers"))
    if has_scoped_context:
        broker_id = str(metadata.get("broker_id") or broker_id_from_symbol(intent.symbol, intent.asset))
        reconciliation_summary = reconciliation_summary_for_broker(broker_id)
    else:
        reconciliation = checks.get("reconciliation") if isinstance(checks.get("reconciliation"), dict) else {}
        reconciliation_summary = reconciliation.get("summary") if isinstance(reconciliation.get("summary"), dict) else {}
    positions_matched = reconciliation_blocker_count(reconciliation_summary) == 0
    return PreTradeContext(
        mode=intent.mode,
        dry_run=dry_run,
        halted=bool(STATE["kill_switch"]) or durable_control_halt_active(intent),
        new_entries_blocked=bool(STATE["new_entries_blocked"]),
        readiness_blockers=int(intent_readiness_blocker_count(checks, intent, reconciliation_summary)),
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
        position_quantity=broker_position_quantity(
            intent.symbol,
            str(
                metadata.get("broker_id")
                or broker_id_from_symbol(intent.symbol, intent.asset)
            ),
            str(
                metadata.get("position_side")
                or metadata.get("positionSide")
                or (
                    "SHORT"
                    if str(metadata.get("position_direction") or "").lower()
                    == "short"
                    else "LONG"
                    if str(metadata.get("position_direction") or "").lower()
                    == "long"
                    else ""
                )
            ),
        ),
        risk_reducing_verified=metadata.get("risk_reducing") is True,
        market_type=str(metadata.get("market_type") or "spot").lower(),
        short_entries_allowed=(
            metadata.get("short_entries_requested") is True
            and metadata.get("broker_short_adapter_verified") is True
        ),
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
    return sum(
        1
        for order in STATE["orders"]
        if _restore_order_is_pending_or_unknown(order)
    )


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
    elif state_name == "approved":
        prefix = "LIVE-ORDER"
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


def live_broker_payload(
    intent: OrderIntent,
    *,
    idempotency_key: str,
) -> dict[str, Any]:
    metadata = intent.metadata if isinstance(intent.metadata, dict) else {}
    broker_id = str(
        metadata.get("broker_id")
        or broker_id_from_symbol(intent.symbol, intent.asset)
    ).lower()
    order_type = str(metadata.get("order_type") or "LIMIT")
    broker_price = intent.reference_price
    if broker_id == "upbit":
        if order_type.lower() == "price":
            broker_price = intent.notional
        elif order_type.lower() == "market":
            broker_price = 0.0
    elif broker_id == "kis":
        if order_type == "01":
            broker_price = 0.0
        elif (
            order_type == "00"
            and metadata.get("fresh_quote_verified") is True
        ):
            broker_price = safe_float(
                metadata.get("fresh_quote_price"),
                intent.reference_price,
            )
    return {
        "broker_id": broker_id,
        "symbol": intent.symbol,
        "asset": intent.asset,
        "side": intent.side,
        "quantity": intent.quantity,
        "qty": intent.quantity,
        "price": broker_price,
        "notional": intent.notional,
        "order_type": order_type,
        "identifier": idempotency_key,
        "exchange": str(metadata.get("exchange") or ""),
        "position_direction": str(
            metadata.get("position_direction") or "long"
        ),
        "market_type": str(metadata.get("market_type") or "spot"),
        "risk_reducing": metadata.get("risk_reducing") is True,
        "max_leverage": safe_float(
            metadata.get("max_leverage"),
            1.0,
        ),
        "required_margin_type": str(
            metadata.get("required_margin_type") or "ISOLATED"
        ),
    }


def submit_order_intent(
    checks: dict[str, Any],
    intent: OrderIntent,
    *,
    dry_run: bool,
    audit_event: str,
    runner_report: StrategyExecutionResult | None = None,
) -> dict[str, Any]:
    metadata = dict(intent.metadata) if isinstance(intent.metadata, dict) else {}
    bar_time = str(metadata.get("confirmed_bar_end") or datetime.now(timezone.utc).isoformat())
    trace_id = str(metadata.get("trace_id") or metadata.get("traceId") or build_trace_id(
        strategy_instance_id=str(metadata.get("strategy_instance_id") or intent.strategy_id),
        instrument_id=str(metadata.get("instrument_id") or intent.symbol),
        bar_time=bar_time,
        deployment_id=str(metadata.get("portfolio_id") or "live-trader"),
        revision=int(metadata.get("target_revision") or 0),
    ))
    metadata.update({"trace_id": trace_id, "traceId": trace_id})
    intent = replace(intent, metadata=metadata)
    RECOVERY_JOURNAL.save(recovery_state_payload(), reason="before-order", idempotency_keys=[str(item.get("idempotency_key") or "") for item in STATE.get("orders", [])])
    ok, order_state, queue_state, reason, risk_report = evaluate_order_gate_with_report(checks, intent.side, dry_run, intent)
    portfolio_gate = portfolio_gate_for_intent(checks, intent)
    retry_backoff = float(STATE["retry_policy"]["backoff_sec"])
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
        DECISION_TRACE_STORE.append(
            trace_id=trace_id,
            stage="BLOCKED",
            decision="DUPLICATE",
            reasons=("persistent-duplicate-idempotency-key",),
            output_payload={"idempotencyKey": idempotency_key},
        )
        append_audit("warn", audit_event, f"재시작 이후 중복 주문 차단: idempotency_key={idempotency_key}")
        return {"ok": existing is not None, "reason": "persistent-duplicate-idempotency-key", "order": existing or {}, "duplicate": True, "snapshot": snapshot()}
    managed_order, oms_created = LIVE_OMS.create(intent, idempotency_key)
    if not oms_created:
        existing = next((item for item in STATE.get("orders", []) if item.get("oms_order_id") == managed_order.order_id), None)
        append_audit("warn", audit_event, f"중복 주문 차단: idempotency_key={idempotency_key}")
        return {"ok": existing is not None, "reason": "duplicate-idempotency-key", "order": existing or {}, "duplicate": True, "snapshot": snapshot()}
    DECISION_TRACE_STORE.append(
        trace_id=trace_id,
        stage="BAR_CLOSED",
        decision="EVALUATE",
        input_payload={"barTime": bar_time, "symbol": intent.symbol, "referencePrice": intent.reference_price},
        occurred_at=bar_time,
    )
    DECISION_TRACE_STORE.append(
        trace_id=trace_id,
        stage="SIGNAL_DECIDED",
        decision=intent.side,
        reasons=(intent.reason,),
        input_payload={"strategyId": intent.strategy_id},
        output_payload=runner_report.to_dict() if runner_report is not None else {"intent": intent.side},
        occurred_at=bar_time,
    )
    DECISION_TRACE_STORE.append(
        trace_id=trace_id,
        stage="TARGET_ALLOCATED",
        decision="ALLOCATE",
        output_payload={"quantity": intent.quantity, "notional": intent.notional, "portfolioGate": portfolio_gate},
    )
    if oms_created:
        if ok:
            LIVE_OMS.transition(managed_order.order_id, "RISK_APPROVED", "PreTradeRiskGate passed")
        else:
            LIVE_OMS.transition(managed_order.order_id, "REJECTED", reason)
    canary_scope = (
        current_live_canary_scope(
            intent.strategy_id,
            materialize=True,
        )
        if ok and not dry_run and intent.mode == "SMALL_LIVE"
        else {}
    )
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
        "mode": intent.mode,
        "canary_scope": (
            canary_scope
            if canary_scope.get("eligible") is True
            else {}
        ),
        "reason": reason,
        "trace_id": trace_id,
        "risk_report": risk_report.to_dict(),
        "portfolio_gate": portfolio_gate if portfolio_gate.get("active") else {},
    }
    if runner_report is not None:
        order["runner_report"] = runner_report.to_dict()
    DECISION_TRACE_STORE.append(
        trace_id=trace_id,
        stage="RISK_DECIDED",
        decision="ALLOW" if ok else "BLOCK",
        reasons=(reason,),
        output_payload=risk_report.to_dict(),
    )
    DECISION_TRACE_STORE.append(
        trace_id=trace_id,
        stage="ORDER_CREATED" if ok else "BLOCKED",
        decision=order_state.upper(),
        reasons=(reason,),
        output_payload={"orderId": order["order_id"], "omsOrderId": order["oms_order_id"]},
    )
    if ok and not dry_run:
        broker_payload = live_broker_payload(
            intent,
            idempotency_key=idempotency_key,
        )
        broker_id = str(broker_payload["broker_id"])
        order["broker_id"] = broker_id
        order["broker_request"] = dict(broker_payload)
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
                DECISION_TRACE_STORE.append(
                    trace_id=trace_id,
                    stage="BROKER_ACKNOWLEDGED",
                    decision="ACKNOWLEDGED",
                    output_payload={"brokerId": broker_id, "brokerOrderId": broker_order_id},
                )
            elif response_ok:
                LIVE_OMS.mark_unknown(managed_order.order_id, "broker response missing order id")
                order.update({"state": "unknown", "queue_state": "reconcile_required", "reason": "broker-response-missing-order-id", "next_retry_at": "-"})
                reason = "broker-response-missing-order-id"
                ok = False
                DECISION_TRACE_STORE.append(
                    trace_id=trace_id,
                    stage="BLOCKED",
                    decision="UNKNOWN",
                    reasons=(reason,),
                    output_payload={"brokerId": broker_id},
                )
            elif int(safe_float(broker_response.get("statusCode"), 0.0)) == 0:
                LIVE_OMS.mark_unknown(managed_order.order_id, "network outcome unknown; reconcile before retry")
                order.update({"state": "unknown", "queue_state": "reconcile_required", "reason": "network-outcome-unknown", "next_retry_at": "-"})
                reason = "network-outcome-unknown"
                ok = False
                DECISION_TRACE_STORE.append(
                    trace_id=trace_id,
                    stage="BLOCKED",
                    decision="UNKNOWN",
                    reasons=(reason,),
                    output_payload={"brokerId": broker_id},
                )
            else:
                LIVE_OMS.transition(managed_order.order_id, "REJECTED", "broker rejected request", {"response": broker_response})
                order.update({"state": "broker_rejected", "queue_state": "failed", "reason": str(broker_response.get("text") or "broker-rejected")[:500], "next_retry_at": "-"})
                reason = str(order["reason"])
                ok = False
                DECISION_TRACE_STORE.append(
                    trace_id=trace_id,
                    stage="BLOCKED",
                    decision="BROKER_REJECTED",
                    reasons=(reason,),
                    output_payload={"brokerId": broker_id},
                )
        except BrokerNotReadyError as exc:
            LIVE_OMS.transition(managed_order.order_id, "REJECTED", str(exc))
            order.update({"state": "adapter_blocked", "queue_state": "failed", "reason": str(exc), "next_retry_at": "-"})
            reason = str(exc)
            ok = False
            DECISION_TRACE_STORE.append(
                trace_id=trace_id,
                stage="BLOCKED",
                decision="ADAPTER_BLOCKED",
                reasons=(reason,),
                output_payload={"brokerId": broker_id},
            )
        order["oms_status"] = LIVE_OMS.orders[managed_order.order_id].status
    STATE["orders"].insert(0, order)
    STATE["orders"] = STATE["orders"][:50]
    trace_index = STATE.setdefault("order_trace_index", {})
    if not isinstance(trace_index, dict):
        trace_index = {}
        STATE["order_trace_index"] = trace_index
    trace_context = {"trace_id": trace_id, "strategy_id": intent.strategy_id}
    for identifier in {
        str(order.get("order_id") or ""),
        str(order.get("oms_order_id") or ""),
        str(order.get("broker_order_id") or ""),
        str(order.get("idempotency_key") or ""),
    } - {""}:
        trace_index[identifier] = trace_context
    while len(trace_index) > 5000:
        trace_index.pop(next(iter(trace_index)))
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
    if not dry_run and order.get("broker_id"):
        state_name = str(order.get("state") or "").lower()
        if state_name == "acknowledged":
            queue_live_order_lifecycle_notification(
                order,
                status="acknowledged",
            )
        elif state_name in {"broker_rejected", "adapter_blocked", "unknown"}:
            queue_live_order_lifecycle_notification(
                order,
                status="rejected" if state_name == "broker_rejected" else "error",
                reason=str(order.get("reason") or reason),
                message_final=True,
            )
    return {"ok": ok, "reason": reason, "order": order, "snapshot": snapshot()}


def start_continuous_runtime(profile_id: str, mode: str, portfolio_id: str = "") -> dict[str, Any]:
    normalized_profile = "stock" if profile_id == "stock" else "crypto"
    normalized_mode = normalize_runtime_mode(mode)
    with RUNTIME_CONTROL_LOCK:
        result = LIVE_CONTINUOUS_CONTROLLER.start(
            normalized_profile,
            normalized_mode,
            portfolio_id,
        )
        if result.get("ok"):
            with RUNTIME_MODE_LOCK:
                sync_runtime_profile_mode(
                    normalized_profile,
                    normalized_mode,
                    action=f"Continuous runtime {normalized_mode} 동기화",
                )
                result["snapshot"] = snapshot()
    if portfolio_id and result.get("ok"):
        ArtifactMetadataStore().update(portfolio_id, "portfolio", mark_used=True)
    return result


def stop_continuous_runtime(profile_id: str = "") -> dict[str, Any]:
    normalized_profile = (
        profile_id if profile_id in {"stock", "crypto"} else ""
    )
    # Controller.stop() first transitions the engine to MONITOR under
    # RUNTIME_MODE_LOCK, then releases that lock before joining the worker.
    # Holding it here across stop() would deadlock a worker that is flushing a
    # due bar through the supervisor's operation_lock.
    with RUNTIME_CONTROL_LOCK:
        result = LIVE_CONTINUOUS_CONTROLLER.stop(normalized_profile)
        with RUNTIME_MODE_LOCK:
            targets = (
                (normalized_profile,)
                if normalized_profile
                else ("stock", "crypto")
            )
            for target in targets:
                sync_runtime_profile_mode(
                    target,
                    "MONITOR",
                    action=(
                        "Continuous runtime 정지 동기화"
                        if result.get("ok")
                        else "Continuous runtime 정지 실패 fail-closed MONITOR"
                    ),
                )
            result["snapshot"] = snapshot()
    return result


def start_execution_streams(broker_id: str = "all") -> dict[str, Any]:
    brokers = (
        "kis",
        "binance",
        "binance-futures",
        "upbit",
    ) if str(broker_id).lower().strip() in {"", "all"} else (
        str(broker_id).lower().strip(),
    )
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
        "order_trace_index": dict(STATE.get("order_trace_index", {})),
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
        STATE["mode"] = "MONITOR"
        STATE["dry_run"] = True
        STATE["new_entries_blocked"] = True
        STATE["recovery_status"] = {
            "verified": False,
            "safeMode": True,
            "generation": 0,
            "detail": "유효한 체크포인트 없음: MONITOR/신규 진입 차단 유지",
            "corruptCheckpoints": loaded.get("corruptCheckpoints", []),
        }
        append_audit("warn", "Startup Recovery", STATE["recovery_status"]["detail"])
        return dict(STATE["recovery_status"])
    recovered = loaded.get("state") if isinstance(loaded.get("state"), dict) else {}
    STATE["orders"] = list(recovered.get("orders", []))[:50] if isinstance(recovered.get("orders"), list) else []
    STATE["order_trace_index"] = dict(recovered.get("order_trace_index", {})) if isinstance(recovered.get("order_trace_index"), dict) else {}
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
    broker_by_instrument: dict[str, str] = {}
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
        broker_by_instrument[instrument_id] = strategy_broker_id(strategy)
        execution_by_instance[instance_id] = (strategy, execution)

    current_sleeves = {str(key): safe_float(value, 0.0) for key, value in STATE.get("strategy_sleeves", {}).items()}
    current_weights: dict[str, float] = {}
    current_quantities: dict[str, float] = {}
    for signal in signals:
        current_weights[signal.instrument_id] = current_weights.get(signal.instrument_id, 0.0) + current_sleeves.get(signal.strategy_instance_id, 0.0)
        current_quantities.setdefault(
            signal.instrument_id,
            broker_position_quantity(
                signal.symbol,
                broker_by_instrument.get(signal.instrument_id, ""),
            ),
        )
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


def broker_position_quantity(
    symbol: str,
    broker_id: str = "",
    position_side: str = "",
) -> float:
    positions = STATE.get("broker_reconciliation", {}).get("positions", [])
    normalized_symbol = (
        str(symbol or "")
        .upper()
        .removesuffix(".PERP")
        .replace("-", "")
    )
    normalized_broker = str(broker_id or "").strip().lower()
    aliases = {normalized_symbol}
    for quote_currency in ("USDT", "USDC", "BUSD", "BTC", "ETH"):
        if normalized_symbol.endswith(quote_currency) and len(normalized_symbol) > len(quote_currency):
            aliases.add(normalized_symbol[: -len(quote_currency)])
            break
    matching_positions = [
        item
        for item in positions
        if isinstance(item, dict)
        and (
            not normalized_broker
            or str(item.get("broker_id") or "").strip().lower()
            == normalized_broker
        )
        and (
            str(item.get("symbol") or "")
            .upper()
            .removesuffix(".PERP")
            .replace("-", "")
            in aliases
        )
    ]
    if normalized_broker == "binance-futures":
        requested_side = str(position_side or "").strip().upper()
        if requested_side in {"LONG", "SHORT", "BOTH"}:
            exact = [
                item
                for item in matching_positions
                if normalized_reconciliation_position_side(
                    item,
                    source="broker",
                )
                == requested_side
            ]
            if exact:
                matching_positions = exact
            else:
                one_way = [
                    item
                    for item in matching_positions
                    if normalized_reconciliation_position_side(
                        item,
                        source="broker",
                    )
                    == "BOTH"
                ]
                matching_positions = one_way
        else:
            active_sides = {
                normalized_reconciliation_position_side(
                    item,
                    source="broker",
                )
                for item in matching_positions
                if abs(
                    safe_float(
                        item.get("quantity"),
                        safe_float(
                            item.get("qty"),
                            safe_float(item.get("broker_qty"), 0.0),
                        ),
                    )
                )
                > 1e-12
            }
            if len(active_sides) > 1:
                raise RuntimeError(
                    "dual-side-position-ambiguous:"
                    f"{normalized_symbol}:{','.join(sorted(active_sides))}"
                )
    return sum(
        safe_float(
            item.get("quantity"),
            safe_float(item.get("qty"), safe_float(item.get("broker_qty"), 0.0)),
        )
        for item in matching_positions
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
        metadata={
            "broker_id": str(
                order.get("broker_id")
                or broker_id_from_symbol(
                    str(order.get("symbol", "")),
                    str(order.get("asset", "")),
                )
            ),
            **{
                key: value
                for key, value in (
                    order.get("broker_request")
                    if isinstance(order.get("broker_request"), dict)
                    else {}
                ).items()
                if key
                in {
                    "position_direction",
                    "market_type",
                    "risk_reducing",
                    "max_leverage",
                    "required_margin_type",
                }
            },
        },
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
    broker_order_id = str(order.get("broker_order_id") or "").strip()
    submitted_to_broker = (
        not bool(order.get("dry_run"))
        and broker_order_id not in {"", "-"}
        and str(order.get("queue_state") or "").lower() in {"submitted", "sent", "partially_filled"}
    )
    if submitted_to_broker:
        broker_request = order.get("broker_request") if isinstance(order.get("broker_request"), dict) else {}
        broker_response = order.get("broker_response") if isinstance(order.get("broker_response"), dict) else {}
        response_payload = broker_response.get("json") if isinstance(broker_response.get("json"), dict) else {}
        output = response_payload.get("output") if isinstance(response_payload.get("output"), dict) else {}
        broker_id = str(order.get("broker_id") or broker_request.get("broker_id") or broker_id_from_symbol(str(order.get("symbol") or ""), str(order.get("asset") or ""))).lower()
        try:
            cancel_response = LiveBrokerRouter().cancel_order(
                broker_id,
                broker_order_id,
                symbol=str(order.get("symbol") or broker_request.get("symbol") or ""),
                asset=str(order.get("asset") or broker_request.get("asset") or ""),
                quantity=order.get("qty") or broker_request.get("quantity") or 0,
                exchange=str(broker_request.get("exchange") or ""),
                organization_no=str(output.get("KRX_FWDG_ORD_ORGNO") or output.get("KRX_FWDG_ORD_ORG_NO") or ""),
            )
        except (BrokerNotReadyError, RuntimeError) as exc:
            reason = f"브로커 주문 취소 실패: {exc}"
            append_audit("error", "주문 취소 실패", f"{order_id} · {reason}")
            return {"ok": False, "reason": reason, "snapshot": snapshot()}
        if cancel_response.get("ok") is not True:
            reason = str(cancel_response.get("text") or cancel_response.get("status") or "broker cancel rejected")
            append_audit("error", "주문 취소 실패", f"{order_id} · {reason}")
            return {"ok": False, "reason": reason, "snapshot": snapshot()}
        order["broker_cancel_response"] = cancel_response
    order["state"] = "canceled"
    order["queue_state"] = "canceled"
    order["updated_at"] = now_text()
    order["next_retry_at"] = "-"
    order["reason"] = "브로커 주문과 로컬 큐를 취소했습니다." if submitted_to_broker else "운용자 요청으로 주문 큐 항목을 취소했습니다."
    append_audit("warn", "주문 취소", f"{order_id} {'브로커 주문 및 ' if submitted_to_broker else ''}주문 큐 항목을 취소했습니다.")
    if submitted_to_broker:
        queue_live_order_lifecycle_notification(
            order,
            status="cancelled",
            reason=str(order["reason"]),
            message_final=True,
        )
    return {"ok": True, "reason": "order canceled", "snapshot": snapshot()}
