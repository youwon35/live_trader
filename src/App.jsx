import * as React from "react";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  BadgeCheck,
  Bell,
  CircleStop,
  Clock3,
  Download,
  FileClock,
  FlaskConical,
  LayoutDashboard,
  ListChecks,
  Lock,
  LockKeyhole,
  Moon,
  Network,
  Palette,
  PanelLeft,
  Pause,
  Play,
  Power,
  Radio,
  RefreshCw,
  RefreshCcw,
  Save,
  Search,
  Settings,
  ShieldAlert,
  ShieldCheck,
  SlidersHorizontal,
  Star,
  Sun,
  TerminalSquare,
  Trash2,
  Unlock,
  WalletCards,
} from "lucide-react";
import {
  cancelOrder,
  evaluateValidationSmallLive,
  engageNativeEmergencyStop,
  getEnvSettings,
  getNativeEmergencyStopStatus,
  getSnapshot,
  getTelegramConnection,
  getUiSettings,
  getValidationSmallLive,
  isApiConnectionFailure,
  isApiRequestTimeout,
  loadArtifactMetadata,
  loadSharedSearchPresets,
  applyBinanceFuturesSettings,
  previewBinanceFuturesOrderRisk,
  previewBinanceFuturesSettings,
  previewBinanceFuturesFillSoak,
  registerSafetyConfirmationPresenter,
  runFinalPreflight,
  runReconciliation,
  runStrategyCycle,
  startContinuousRuntime,
  startBinanceFuturesFillSoak,
  stopContinuousRuntime,
  stopBinanceFuturesFillSoak,
  runWatchdog,
  promoteStrategyToLive,
  retryOrder,
  setFlag,
  setStrategyLifecycle,
  setAutomationProfile,
  syncExecutionEvents,
  setRetryPolicy,
  setRiskSetting,
  saveUiSettings,
  saveSharedSearchPresets,
  seedProgramLedgerBaseline,
  updateArtifactMetadata,
  saveEnvSettings,
  submitTestIntent,
  runPolicyReplay,
  runShadowLive,
  runRecoveryDrill,
} from "./api";
import {
  futuresRiskLeverageOptions,
  futuresRiskStrategyDefaults,
  isFuturesRiskStrategy,
  shouldHydrateRiskStrategy,
} from "./futuresRiskSimulator";
import { livePollingIntervals } from "./polling";
import {
  buildRetryMatrix,
  projectAccountReconciliation,
  projectExecutionQuality,
  projectOrderTimeline,
} from "./liveWorkspaceModel";
import {
  ACCOUNT_REFRESH_INTERVAL_MS,
  createAccountRefreshCoordinator,
} from "./accountRefresh";
import {
  buildAccountVisualization,
  formatAllocationValue,
} from "./accountVisualization";
import {
  buildCurrentDeploymentOptions,
  deploymentContextMatchesPreflight,
  deploymentRuntimeProfile,
  governedDeploymentIdentity,
  strategyDeploymentIdentity,
} from "./deploymentSelection";
import { buildOrderCsvRows, ORDER_CSV_COLUMNS } from "./orderCsv";
import { createActionButton } from "../../../packages/design/action-button.js";
import FunctionalTestWorkspace from "./FunctionalTestWorkspace";
import { createBrokerAccountWorkspace } from "../../../packages/design/account-workspace.js";
import { createAppearanceSettingsPanel } from "../../../packages/design/appearance-settings-panel.js";
import {
  formatBarCountdown,
  nextClosedBarSummary,
} from "../../../packages/design/bar-schedule.js";
import { readGuidedFlowStep, writeGuidedFlowStep } from "../../../packages/design/guided-flow.js";
import {
  LAYOUT_RESIZE_DIRECTIONS,
  applyLayoutTransformOffset,
  captureLayoutPeerDimensions,
  clearLayoutTransformOffset,
  freezeLayoutPeerDimensions,
  layoutAlignedOffset,
  layoutDropTarget,
  layoutElementOverlapsPeers,
  LAYOUT_EDIT_EXIT_EVENT,
  repairLayoutPanelOverlaps,
  layoutSwapDimensions,
  layoutSwapOffsets,
  readLayoutTransformOffset,
} from "../../../packages/design/layout-editing.js";
import { createNestedTabs } from "../../../packages/design/nested-tabs.js";
import { createMasterDetailLog } from "../../../packages/design/master-detail-log.js";
import {
  buildPromotionReadinessQueue,
  normalizePromotionLifecycle,
  promotionQueueSummary,
} from "../../../packages/design/promotion-readiness.js";
import { createStatusPill } from "../../../packages/design/status-pill.js";
import { createTelegramConnectionStatus } from "../../../packages/design/telegram-connection-status.js";
import {
  createEmptyState,
  createIconButton,
  createMetricCard,
  createMetricGrid,
  createPanelHeader,
  createStatusCard,
  createStatusRow,
  createToggleSwitch,
  semanticSurfaceProps,
} from "../../../packages/design/ui-primitives.js";
import designTokens from "../../../packages/design/design_tokens.json";
import {
  createStrategySearchPreset,
  mergeStrategySearchPresets,
  normalizeStrategySearchPresetDocument,
} from "../../../packages/trading-contracts/src/index.js";

const ActionButton = createActionButton(React);
const AppearanceSettingsPanel = createAppearanceSettingsPanel(React);
const BrokerAccountWorkspace = createBrokerAccountWorkspace(React);
const MasterDetailLog = createMasterDetailLog(React);
const StatusPill = createStatusPill(React);
const TelegramConnectionStatus = createTelegramConnectionStatus(React);
const EmptyState = createEmptyState(React);
const IconButton = createIconButton(React);
const MetricCard = createMetricCard(React);
const MetricGrid = createMetricGrid(React);
const NestedTabs = createNestedTabs(React);
const PanelHeader = createPanelHeader(React);
const StatusCard = createStatusCard(React);
const SharedStatusRow = createStatusRow(React);
const ToggleSwitch = createToggleSwitch(React);
const navItems = [
  { id: "overview", label: "운영 현황", icon: LayoutDashboard },
  { id: "gate", label: "배포·승급", icon: ListChecks },
  { id: "functional-test", label: "기능시험", icon: FlaskConical },
  { id: "accounts", label: "계좌·포지션", icon: WalletCards },
  { id: "orders", label: "주문·체결", icon: FileClock },
  { id: "risk", label: "리스크·안전", icon: ShieldAlert },
  { id: "automation", label: "실거래 운영", icon: Power },
  { id: "incidents", label: "감사 기록", icon: Bell },
  { id: "audit", label: "기술 로그", icon: TerminalSquare },
  { id: "settings", label: "설정·진단", icon: Settings },
];

const pageProfiles = {
  overview: {
    title: "운영 현황",
    eyebrow: "LIVE CONTROL PLANE",
    summary: "현재 Deployment, Session, Preflight, 위험·주문·사고 상태를 한눈에 확인합니다.",
  },
  gate: {
    title: "배포·승급",
    eyebrow: "DEPLOYMENT & PROMOTION",
    summary: "검증된 Portfolio Artifact를 배포 단위로 고정하고 계보·Evidence·승급 조건을 확인합니다.",
  },
  "functional-test": {
    title: "기능시험",
    eyebrow: "KIS LIVE FUNCTIONAL TEST",
    summary: "승급 Evidence와 분리된 기간형 KIS 실전 기능시험 허가와 당일 활성화를 준비합니다.",
  },
  accounts: {
    title: "계좌·포지션",
    eyebrow: "BROKER TRUTH",
    summary: "Broker Snapshot, 실시간 체결 상태, 프로그램 원장을 구분해 계좌와 포지션을 대조합니다.",
  },
  orders: {
    title: "주문·체결",
    eyebrow: "ORDER & FILL LEDGER",
    summary: "주문 의도부터 ACK·부분체결·완전체결·원장 대조까지 전체 상태와 실행 품질을 추적합니다.",
  },
  risk: {
    title: "리스크·안전",
    eyebrow: "RISK GATEWAY",
    summary: "현재 사용량, Soft Warning·Hard Block, Reduce-only, 재시도·Kill 정책을 관리합니다.",
  },
  automation: {
    title: "실거래 운영",
    eyebrow: "RUNTIME SESSION",
    summary: "Monitor → Canary → Limited Live → Full Live 순서와 구성 요소별 실행 상태를 관리합니다.",
  },
  incidents: {
    title: "감사 기록",
    eyebrow: "AUDIT RECORDS",
    summary: "잠금·배포·Preflight·모드·Risk·주문·Kill·Secret 변경을 append-only 감사 이벤트로 추적합니다.",
  },
  audit: {
    title: "기술 로그",
    eyebrow: "ENGINEERING LOG",
    summary: "Scope·Level·Source·Correlation ID로 개발 및 운영 로그를 검색하고 분석합니다.",
  },
  settings: {
    title: "설정·진단",
    eyebrow: "BROKER & RUNTIME",
    summary: "화면·레이아웃, 브로커 연결, Telegram 알림과 Runtime 자체 검사를 관리합니다.",
  },
};

const fallbackSnapshot = {
  api_connected: false,
  generated_at: "-",
  mode: "MONITOR",
  dry_run: true,
  kill_switch: false,
  emergency_stop: { active: false, durable: false, status: "unknown", available: false },
  new_entries_blocked: true,
  operator_confirmed: false,
  summary: { status: "blocked", blocker_count: 1, warning_count: 0, live_strategy_count: 0, broker_ready_count: 0 },
  sessions: [],
  readiness: [{ label: "Python API", status: "fail", detail: "Python server connection is required." }],
  risk_checks: [],
  risk_settings: [],
  checklist: [],
  retry_policy: [],
  order_queue: { total: 0, active: 0, blocked: 0, dry_run: 0, retryable: 0, canceled: 0 },
  retry_policy_matrix: [],
  incidents: [],
  live_governance: {
    deploymentId: "",
    manifest: null,
    latestPreflight: null,
    preflightValidity: { valid: false, reasons: ["api-disconnected"] },
    activeSession: null,
    incidents: [],
  },
  watchdog: {
    last_run: "미실행",
    status: "unknown",
    status_label: "확인 불가",
    last_action: "대기",
    last_trip: "-",
    trip_count: 0,
    active_brokers: [],
    active_live: false,
    critical_count: 0,
    warning_count: 0,
    checks: [],
    next_actions: [],
  },
  dry_run_ledger: [],
  brokers: [],
  broker_diagnostics: [],
  broker_adapter_contract: [],
  automation_profiles: [],
  strategies: [],
  orders: [],
  positions: [],
  accounts: [],
  reconciliation: {
    summary: {
      status: "unknown",
      status_label: "확인 불가",
      last_run: "미실행",
      position_count: 0,
      account_count: 0,
      api_required_count: 0,
      mismatch_count: 0,
      pass_count: 0,
      error_count: 0,
    },
    positions: [],
    accounts: [],
    next_actions: [],
  },
  program_ledger: {
    cash_count: 0,
    position_count: 0,
    execution_event_count: 0,
  },
  execution_events: { last_poll: null, errors: [], event_count: 0, recorded_count: 0, recent: [] },
  broker_position_truth: { sourceOfTruth: "broker", matched: false, newEntriesBlocked: true, mismatchCount: 0, lines: [] },
  restart_recovery_plan: { canResume: false, requiredMode: "MONITOR", newEntriesBlocked: true, riskReductionAllowed: true, blockers: ["checkpoint-invalid"], actions: ["load-last-valid-checkpoint"] },
  automatic_promotion: { lastRun: null, results: [] },
  upbit_smoke_order: {
    status: "idle",
    status_label: "미리보기 필요",
    market: "KRW-BTC",
    notional_krw: 5000,
    detail: "실제 주문 전 주문 가능 정보와 원화 잔고를 먼저 조회합니다.",
  },
  execution_calibration: { status: "review", sampleCount: 0, meanAbsoluteModelErrorBps: null, p95AbsoluteSlippageBps: null },
  operation_report: { generated_at: "-", sections: [] },
  final_preflight: [],
  launch_report: {
    last_run: "미실행",
    hard_stop_count: 0,
    warning_count: 0,
    real_order_lock: "locked",
    small_live_status: "blocked",
    full_live_status: "blocked",
    lock_reason: "Python server connection is required.",
    next_actions: [],
  },
  audit: [],
};

const PANEL_SIZE_STORAGE_KEY = "live-trader.panelSizes.v1";
const PANEL_POSITION_STORAGE_KEY = "live-trader.panelPositions.v1";
const LAYOUT_MODE_STORAGE_KEY = "live-trader.layoutMode.v1";
const SIDEBAR_COLLAPSED_STORAGE_KEY = "live-trader.sidebarCollapsed.v1";
const APPEARANCE_STORAGE_KEY = "live-trader.appearance.v1";
const STOCK_ACCENT_BASELINE_STORAGE_KEY = "live-trader.stockAccentBaseline.v1";
const NOTIFICATION_ACK_STORAGE_KEY = "live-trader.notifications.ack.v1";
const LEGACY_THEME_STORAGE_KEY = "live-trader.ui-theme.v1";
const STRATEGY_SAVED_SEARCHES_KEY = "live-trader.strategySavedSearches.v1";
const LAYOUT_RESET_EVENT = "live-trader-layout-reset";
const LAYOUT_RESTORE_EVENT = "live-trader-layout-restore";
const LAYOUT_BASELINE_EVENT = "live-trader-layout-baseline";
const LAYOUT_SNAP_SIZE = 8;
const LAYOUT_COLLISION_GAP = 8;
const LAYOUT_MAX_DIMENSION = 100000;
const LAYOUT_MAX_OFFSET = 100000;
const MIN_PANEL_WIDTH = 180;
const MIN_PANEL_HEIGHT = 80;
const DEFAULT_STRATEGY_DISCOVERY_FILTERS = {
  query: "",
  stage: "all",
  timeframe: "all",
  plugin: "all",
  failure: "all",
  quick: "all",
  sort: "updated-desc",
};

function disconnectedSnapshot(nativeEmergency = {}, previousSnapshot = {}) {
  const reportedEmergency = {
    available: nativeEmergency.available === true,
    status_available: nativeEmergency.status_available === true,
    active: nativeEmergency.active === true,
    durable: nativeEmergency.durable === true,
    status: String(nativeEmergency.status || "unavailable"),
    ...(nativeEmergency.reason ? { reason: String(nativeEmergency.reason) } : {}),
  };
  const preserveActiveKill = previousSnapshot.kill_switch === true && reportedEmergency.active !== true;
  const emergency = preserveActiveKill
    ? {
        ...(previousSnapshot.emergency_stop || {}),
        available: reportedEmergency.available,
        status_available: reportedEmergency.status_available,
        active: true,
        durable: previousSnapshot.emergency_stop?.durable === true,
        status: "previously-engaged-fail-closed",
        reason: "API 복구 전에는 이전 Kill 활성 상태를 해제로 간주하지 않습니다.",
      }
    : reportedEmergency;
  return {
    ...fallbackSnapshot,
    kill_switch: emergency.active,
    emergency_stop: emergency,
  };
}

const SAFETY_CONFIRMATION_ACTION_LABELS = {
  KILL_SWITCH_OFF: "전역 Kill 해제",
  DRY_RUN_OFF: "Dry Run 보호 해제",
  NEW_ENTRIES_BLOCKED_OFF: "신규 진입 차단 해제",
  REAL_ORDERS_ENABLE: "실전 주문 라우트 활성화",
  FUNCTIONAL_TEST_START: "KIS 실전 기능시험 시작",
  UPBIT_FUNCTIONAL_START: "Upbit 2시간 기능시험 시작",
  UPBIT_FUNCTIONAL_STOP: "Upbit 기능시험 중지·정리",
  UPBIT_FUNCTIONAL_RECOVER: "Upbit 기능시험 복구",
  BINANCE_SPOT_FUNCTIONAL_START: "Binance Spot 2시간 기능시험 시작",
  BINANCE_SPOT_FUNCTIONAL_STOP: "Binance Spot 기능시험 중지·정리",
  BINANCE_SPOT_FUNCTIONAL_RECOVER: "Binance Spot 기능시험 복구",
  BINANCE_FUTURES_FILL_SOAK_START: "Binance Futures 실체결 Soak 시작",
};

const SAFETY_CONFIRMATION_CONTEXT_LABELS = {
  action: "작업",
  account: "계좌",
  accountHint: "계좌 식별",
  accountFingerprint: "계좌 Fingerprint",
  accountLast4: "계좌 끝 4자리",
  provider: "브로커",
  target: "대상",
  targetKey: "대상 Key",
  deploymentId: "배포",
  strategyId: "전략",
  symbol: "종목",
  symbols: "종목",
  maxAmount: "최대 금액",
  maxNotional: "최대 주문 금액",
  maxOrderNotional: "최대 주문 금액",
  settingKeys: "변경 설정",
  environment: "환경",
  sessionId: "기능시험 세션",
};

function safetyConfirmationContextRows(displayContext) {
  if (Array.isArray(displayContext)) {
    return displayContext.map((item, index) => ({
      label: String(item?.label || item?.key || `항목 ${index + 1}`),
      value: String(item?.value ?? item?.detail ?? "-"),
    }));
  }
  return Object.entries(displayContext || {}).map(([key, value]) => ({
    label: SAFETY_CONFIRMATION_CONTEXT_LABELS[key] || key,
    value: Array.isArray(value)
      ? value.join(", ")
      : value && typeof value === "object"
        ? JSON.stringify(value)
        : String(value ?? "-"),
  }));
}

function SafetyConfirmationModal({ challenge, onResolve, onEmergencyKill }) {
  const [typedPhrase, setTypedPhrase] = useState("");
  const [now, setNow] = useState(() => Date.now());
  const dialogRef = useRef(null);
  const resolvedRef = useRef(false);
  const expiresAtMs = Date.parse(challenge.expiresAt || "");
  const expired = !Number.isFinite(expiresAtMs) || now >= expiresAtMs;
  const remainingSeconds = Number.isFinite(expiresAtMs)
    ? Math.max(0, Math.ceil((expiresAtMs - now) / 1000))
    : 0;
  const contextRows = safetyConfirmationContextRows(challenge.displayContext);
  const phraseMatches = typedPhrase === challenge.expectedPhrase;

  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 250);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    const dialog = dialogRef.current;
    const previouslyFocused = document.activeElement;
    const keepFocusInside = (event) => {
      if (event.key === "Escape") {
        event.preventDefault();
        resolveOnce({ confirmed: false });
        return;
      }
      if (event.key !== "Tab" || !dialog) return;
      const focusable = [...dialog.querySelectorAll(
        "button:not(:disabled), input:not(:disabled), select:not(:disabled), textarea:not(:disabled), [tabindex]:not([tabindex='-1'])",
      )].filter((element) => !element.hidden);
      if (!focusable.length) {
        event.preventDefault();
        dialog.focus();
        return;
      }
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    };
    dialog?.addEventListener("keydown", keepFocusInside);
    return () => {
      dialog?.removeEventListener("keydown", keepFocusInside);
      if (previouslyFocused instanceof HTMLElement && previouslyFocused.isConnected) {
        previouslyFocused.focus();
      }
    };
  }, []);

  useEffect(() => {
    if (!expired || resolvedRef.current) return;
    resolvedRef.current = true;
    onResolve({ confirmed: false, expired: true });
  }, [expired, onResolve]);

  function resolveOnce(decision) {
    if (resolvedRef.current) return;
    resolvedRef.current = true;
    onResolve(decision);
  }

  function submit(event) {
    event.preventDefault();
    if (expired || !phraseMatches) return;
    resolveOnce({ confirmed: true, typedPhrase });
  }

  function engageEmergencyKill() {
    if (resolvedRef.current) return;
    resolvedRef.current = true;
    onResolve({ confirmed: false, emergencyKill: true });
    onEmergencyKill();
  }

  return (
    <div className="safety-confirmation-backdrop" role="presentation">
      <section
        aria-describedby="safety-confirmation-description"
        aria-labelledby="safety-confirmation-title"
        aria-modal="true"
        className="safety-confirmation-modal"
        ref={dialogRef}
        role="dialog"
        tabIndex={-1}
      >
        <header>
          <div className="safety-confirmation-icon"><ShieldAlert size={21} /></div>
          <div>
            <span>IDENTITY-BOUND · ONE TIME</span>
            <h2 id="safety-confirmation-title">
              {SAFETY_CONFIRMATION_ACTION_LABELS[challenge.action] || "위험 설정 변경"}
            </h2>
          </div>
          <StatusPill tone={expired || remainingSeconds <= 15 ? "danger" : "warning"}>
            {expired ? "만료" : `${remainingSeconds}초`}
          </StatusPill>
        </header>

        <p id="safety-confirmation-description">
          서버가 현재 계좌·대상·한도를 묶어 만든 일회용 확인입니다. 아래 범위가 맞을 때만 정확한 문구를 입력하세요.
        </p>

        <dl className="safety-confirmation-context">
          {contextRows.length ? contextRows.map((row) => (
            <div key={`${row.label}-${row.value}`}>
              <dt>{row.label}</dt>
              <dd>{row.value}</dd>
            </div>
          )) : (
            <div><dt>서버 범위</dt><dd>서버가 현재 authoritative context를 실행 시점에 다시 확인합니다.</dd></div>
          )}
          <div><dt>만료 시각</dt><dd>{Number.isFinite(expiresAtMs) ? new Date(expiresAtMs).toLocaleString("ko-KR") : "확인 불가"}</dd></div>
        </dl>

        <form onSubmit={submit}>
          <label>
            <span>아래 문구를 그대로 입력</span>
            <code>{challenge.expectedPhrase}</code>
            <input
              autoComplete="off"
              autoFocus
              disabled={expired}
              onChange={(event) => setTypedPhrase(event.target.value)}
              placeholder={challenge.expectedPhrase}
              spellCheck="false"
              value={typedPhrase}
            />
          </label>
          <div className="safety-confirmation-actions">
            <button className="danger-button" onClick={engageEmergencyKill} type="button">
              <CircleStop size={16} /> 전역 Kill 즉시 실행
            </button>
            <button
              className="secondary-button"
              onClick={() => resolveOnce({ confirmed: false })}
              type="button"
            >
              취소
            </button>
            <button className="primary-button" disabled={expired || !phraseMatches} type="submit">
              범위 확인 후 1회 실행
            </button>
          </div>
        </form>
      </section>
    </div>
  );
}
const MIN_LIVE_CANARY_FILLS = 3;
const EMPTY_FUTURES_PANEL_SNAPSHOT = Object.freeze({});

function liveStrategyBarSchedule(strategy, fallbackProvider = "") {
  if (!strategy) return null;
  const symbol = String(strategy.symbol || strategy.instrument_id || "").trim();
  const timeframe = String(strategy.timeframe || strategy.interval || "").trim();
  if (!symbol || !timeframe) return null;
  return {
    asset: strategy.asset || strategy.asset_class || "",
    market: strategy.market || strategy.market_id || "",
    provider: strategy.market_data_provider
      || strategy.provider
      || strategy.broker_id
      || fallbackProvider,
    symbol,
    timeframe,
    timeZone: strategy.timezone || strategy.time_zone || "",
  };
}

function useLiveClosedBarCountdown(schedules, enabled = true) {
  const scheduleKey = useMemo(
    () => schedules
      .map((schedule) => [
        schedule.market || "",
        schedule.provider || "",
        schedule.symbol || "",
        schedule.timeframe || "",
        schedule.timeZone || "",
      ].join(":"))
      .sort()
      .join("|"),
    [schedules],
  );
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    if (!enabled || !scheduleKey) return undefined;
    let timer = 0;
    const tick = () => setNow(Date.now());
    const scheduleTimer = () => {
      window.clearInterval(timer);
      tick();
      timer = window.setInterval(tick, document.hidden ? 15_000 : 1_000);
    };
    scheduleTimer();
    document.addEventListener("visibilitychange", scheduleTimer);
    return () => {
      window.clearInterval(timer);
      document.removeEventListener("visibilitychange", scheduleTimer);
    };
  }, [enabled, scheduleKey]);

  return useMemo(
    () => nextClosedBarSummary(schedules, new Date(now)),
    [now, scheduleKey, schedules],
  );
}

function LiveClosedBarCountdown({ schedules = [] }) {
  const countdown = useLiveClosedBarCountdown(schedules, schedules.length > 0);
  if (!countdown) {
    return (
      <div className="closed-bar-countdown unavailable">
        <Clock3 size={17} aria-hidden="true" />
        <div>
          <span>다음 확정 봉</span>
          <strong>주기 정보 대기</strong>
          <em>실행 전략의 provider·symbol·timeframe을 확인하세요.</em>
        </div>
      </div>
    );
  }
  return (
    <div className="closed-bar-countdown" aria-live="off">
      <Clock3 size={17} aria-hidden="true" />
      <div>
        <span>다음 확정 봉까지</span>
        <strong>{formatBarCountdown(countdown.secondsRemaining)}</strong>
        <em>
          {countdown.schedule.symbol || "Portfolio"} · {countdown.schedule.timeframe}
          {" · "}{countdown.timeZone}
          {countdown.scheduleCount > 1 ? ` · ${countdown.scheduleCount}개 일정 중 가장 빠른 봉` : ""}
          {countdown.isSessionEstimate ? " · 주말 반영, 공휴일·조기폐장 전 예상" : ""}
        </em>
      </div>
    </div>
  );
}

function readStoredValue(key, fallback = null) {
  try {
    const raw = window.localStorage.getItem(key);
    return raw ? JSON.parse(raw) : fallback;
  } catch {
    return fallback;
  }
}

function collectUiSettings() {
  return {
    appearance: readAppearance(),
    layoutMode: readLayoutMode(),
    sidebarCollapsed: readSidebarCollapsed(),
    panelSizes: readStoredValue(PANEL_SIZE_STORAGE_KEY, {}),
    panelPositions: readStoredValue(PANEL_POSITION_STORAGE_KEY, {}),
  };
}

function persistUiSettings(partial = {}) {
  const payload = { ...collectUiSettings(), ...partial };
  return saveUiSettings(payload).catch(() => null);
}

const accentPalettes = Object.fromEntries(
  Object.entries(designTokens.accent?.palettes ?? {}).map(([id, palette]) => [
    id,
    {
      ...palette,
      label: palette.labelKo ?? palette.label ?? id,
    },
  ]),
);
const fallbackAccentId = designTokens.accent?.default ?? "blue";
const fallbackAccentSwatch = accentPalettes[fallbackAccentId]?.swatch ?? "#2f80ed";
const STOCK_CURRENT_ACCENT = "#8fa7c1";

function normalizeHexColor(value, fallback = fallbackAccentSwatch) {
  const text = String(value || "").trim();
  const shortMatch = text.match(/^#?([0-9a-f]{3})$/i);
  if (shortMatch) {
    return `#${shortMatch[1]
      .split("")
      .map((char) => `${char}${char}`)
      .join("")}`.toLowerCase();
  }
  const match = text.match(/^#?([0-9a-f]{6})$/i);
  return match ? `#${match[1]}`.toLowerCase() : fallback;
}

function hexToRgb(hex) {
  const normalized = normalizeHexColor(hex);
  return {
    r: parseInt(normalized.slice(1, 3), 16),
    g: parseInt(normalized.slice(3, 5), 16),
    b: parseInt(normalized.slice(5, 7), 16),
  };
}

function relativeLuminanceChannel(value) {
  const channel = value / 255;
  return channel <= 0.03928 ? channel / 12.92 : ((channel + 0.055) / 1.055) ** 2.4;
}

function accentContrastText(color) {
  const { r, g, b } = hexToRgb(color);
  const luminance =
    0.2126 * relativeLuminanceChannel(r) +
    0.7152 * relativeLuminanceChannel(g) +
    0.0722 * relativeLuminanceChannel(b);
  return luminance >= 0.45 ? "#0f172a" : "#ffffff";
}

function accentColorForContrast(accent, customAccent) {
  if (accent === "custom") return normalizeHexColor(customAccent);
  return normalizeHexColor(accentPalettes[accent]?.swatch ?? fallbackAccentSwatch, fallbackAccentSwatch);
}

function customAccentVars(color) {
  const primary = normalizeHexColor(color);
  const { r, g, b } = hexToRgb(primary);
  return {
    "--custom-accent": primary,
    "--primary": primary,
    "--primary-hover": `color-mix(in srgb, ${primary} 70%, #000000)`,
    "--primary-border": `color-mix(in srgb, ${primary} 76%, #ffffff)`,
    "--command-primary-background": `color-mix(in srgb, ${primary} 42%, #07111f)`,
    "--command-primary-hover-background": `color-mix(in srgb, ${primary} 34%, #07111f)`,
    "--command-primary-border": `color-mix(in srgb, ${primary} 82%, #ffffff)`,
    "--icon-blue": `color-mix(in srgb, ${primary} 68%, #ffffff)`,
    "--secondary": `color-mix(in srgb, ${primary} 48%, #55d6be)`,
    "--primary-gradient": primary,
    "--progress-gradient": primary,
    "--focus-primary": `0 0 0 3px rgba(${r}, ${g}, ${b}, 0.2)`,
  };
}

const customAccentVarNames = Object.keys(customAccentVars(fallbackAccentSwatch));

const appearanceThemeOptions = [
  { id: "dark", label: "다크", icon: Moon },
  { id: "light", label: "화이트", icon: Sun },
];

const defaultAppearance = {
  theme: "dark",
  accent: "custom",
  customAccent: STOCK_CURRENT_ACCENT,
};

function stockAccentBaselinePending() {
  try {
    return window.localStorage.getItem(STOCK_ACCENT_BASELINE_STORAGE_KEY) !== "applied";
  } catch {
    return true;
  }
}

function markStockAccentBaselineApplied() {
  try {
    window.localStorage.setItem(STOCK_ACCENT_BASELINE_STORAGE_KEY, "applied");
  } catch {
    // Storage-restricted runtimes still receive the baseline for this session.
  }
}

function readStoredMap(key) {
  try {
    const raw = window.localStorage.getItem(key);
    return raw ? JSON.parse(raw) : {};
  } catch {
    return {};
  }
}

function writeStoredMap(key, map) {
  try {
    window.localStorage.setItem(key, JSON.stringify(map));
  } catch {
    // Embedded WebView storage can be unavailable; the visible edit still works for the session.
  }
  persistUiSettings({
    [key === PANEL_SIZE_STORAGE_KEY ? "panelSizes" : "panelPositions"]: map,
  });
}

function normalizeLayoutMode(mode) {
  return mode === "edit" ? "edit" : "locked";
}

function readLayoutMode() {
  try {
    return normalizeLayoutMode(window.localStorage.getItem(LAYOUT_MODE_STORAGE_KEY));
  } catch {
    return "locked";
  }
}

function readSidebarCollapsed() {
  try {
    return window.localStorage.getItem(SIDEBAR_COLLAPSED_STORAGE_KEY) === "true";
  } catch {
    return false;
  }
}

function saveSidebarCollapsed(value) {
  try {
    window.localStorage.setItem(SIDEBAR_COLLAPSED_STORAGE_KEY, value ? "true" : "false");
  } catch {
    // The current session can still toggle the sidebar if WebView storage is unavailable.
  }
}

function applyLayoutMode(mode) {
  const nextMode = normalizeLayoutMode(mode);
  try {
    document.documentElement.dataset.layoutMode = nextMode;
  } catch {
    // React state still carries the mode when document access is unavailable.
  }
  try {
    window.localStorage.setItem(LAYOUT_MODE_STORAGE_KEY, nextMode);
  } catch {
    // Non-fatal in locked-down WebView environments.
  }
  return nextMode;
}

function normalizeAppearance(appearance = {}) {
  const legacyAccentSwatch = accentPalettes[appearance.accent]?.swatch ?? fallbackAccentSwatch;
  const accent = appearance.accent === "custom" || accentPalettes[appearance.accent] ? appearance.accent : defaultAppearance.accent;
  return {
    theme: appearance.theme === "light" ? "light" : "dark",
    accent,
    customAccent: normalizeHexColor(appearance.customAccent, legacyAccentSwatch),
  };
}

function settingsBooleanValue(value, fallback = false) {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;
  if (value === null || value === undefined || value === "") return fallback;
  const normalized = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "on", "y"].includes(normalized)) return true;
  if (["0", "false", "no", "off", "n"].includes(normalized)) return false;
  return fallback;
}

function applyCustomAccent(root, appearance) {
  if (appearance.accent !== "custom") {
    customAccentVarNames.forEach((name) => root.style.removeProperty(name));
    return;
  }
  Object.entries(customAccentVars(appearance.customAccent)).forEach(([name, value]) => {
    root.style.setProperty(name, value);
  });
}

function applyAccentContrast(root, appearance) {
  const contrast = accentContrastText(accentColorForContrast(appearance.accent, appearance.customAccent));
  root.style.setProperty("--accent-contrast-text", contrast);
  root.style.setProperty("--command-button-primary-text", contrast);
}

function readAppearance() {
  try {
    const raw = window.localStorage.getItem(APPEARANCE_STORAGE_KEY);
    const storedAppearance = raw
      ? JSON.parse(raw)
      : { theme: window.localStorage.getItem(LEGACY_THEME_STORAGE_KEY) };
    if (stockAccentBaselinePending()) {
      return normalizeAppearance({
        ...storedAppearance,
        accent: "custom",
        customAccent: STOCK_CURRENT_ACCENT,
      });
    }
    return normalizeAppearance(storedAppearance);
  } catch {
    return defaultAppearance;
  }
}

function applyAppearance(appearance) {
  const nextAppearance = normalizeAppearance(appearance);
  try {
    const root = document.documentElement;
    root.dataset.uiTheme = nextAppearance.theme;
    root.dataset.accent = nextAppearance.accent;
    applyCustomAccent(root, nextAppearance);
    applyAccentContrast(root, nextAppearance);
  } catch {
    // Appearance remains in React state if document access is unavailable.
  }
  try {
    window.localStorage.setItem(APPEARANCE_STORAGE_KEY, JSON.stringify(nextAppearance));
  } catch {
    // Ignore storage failures in embedded runtimes.
  }
  return nextAppearance;
}

function restoreUiSettings(settings = {}) {
  if (settings.panelSizes) {
    try {
      window.localStorage.setItem(PANEL_SIZE_STORAGE_KEY, JSON.stringify(settings.panelSizes));
    } catch {
      // Keep startup usable even if the embedded runtime blocks storage.
    }
  }
  if (settings.panelPositions) {
    try {
      window.localStorage.setItem(PANEL_POSITION_STORAGE_KEY, JSON.stringify(settings.panelPositions));
    } catch {
      // Keep startup usable even if the embedded runtime blocks storage.
    }
  }
  if (settings.layoutMode) applyLayoutMode(settings.layoutMode);
  if (typeof settings.sidebarCollapsed === "boolean") saveSidebarCollapsed(settings.sidebarCollapsed);
  if (settings.appearance) applyAppearance(settings.appearance);
}

function normalizeSearchText(value) {
  return String(value ?? "").toLowerCase().trim();
}

function searchMatches(query, ...values) {
  if (!query) return false;
  return values.some((value) => normalizeSearchText(value).includes(query));
}

function addSearchResult(results, query, item) {
  if (results.length >= 12) return;
  if (searchMatches(query, item.label, item.detail, item.meta)) results.push(item);
}

function buildSearchResults(snapshot, queryValue) {
  const query = normalizeSearchText(queryValue);
  if (!query) return [];

  const results = [];
  (snapshot.strategies ?? []).forEach((strategy) => {
    addSearchResult(results, query, {
      id: `strategy-${strategy.strategy_id}`,
      type: "전략",
      label: `${strategy.name} · ${strategy.symbol}`,
      detail: strategy.block_reason || strategy.lifecycle_status,
      meta: [strategy.strategy_id, strategy.permission_label, strategy.lifecycle_status].join(" "),
      targetNav: "gate",
      tone: strategy.live_allowed ? "success" : "danger",
    });
  });

  (snapshot.orders ?? []).forEach((order) => {
    addSearchResult(results, query, {
      id: `order-${order.order_id}`,
      type: "주문",
      label: `${order.order_id} · ${order.symbol}`,
      detail: `${order.state} · ${order.reason}`,
      meta: [order.strategy_id, order.queue_state, order.side].join(" "),
      targetNav: "orders",
      tone: statusTone(order.state),
    });
  });

  (snapshot.brokers ?? []).forEach((broker) => {
    addSearchResult(results, query, {
      id: `broker-${broker.broker_id}`,
      type: "브로커",
      label: broker.name,
      detail: broker.detail,
      meta: [broker.status, broker.role, ...(broker.missing_env ?? [])].join(" "),
      targetNav: "settings",
      tone: statusTone(broker.status),
    });
  });

  (snapshot.readiness ?? []).forEach((check) => {
    addSearchResult(results, query, {
      id: `readiness-${check.label}`,
      type: "게이트",
      label: check.label,
      detail: check.detail,
      meta: check.status,
      targetNav: "gate",
      tone: statusTone(check.status),
    });
  });

  (snapshot.watchdog?.checks ?? []).forEach((check) => {
    addSearchResult(results, query, {
      id: `watchdog-${check.label}`,
      type: "Watchdog",
      label: check.label,
      detail: check.detail,
      meta: [check.status, check.value].join(" "),
      targetNav: "automation",
      tone: statusTone(check.status),
    });
  });

  (snapshot.positions ?? []).forEach((position) => {
    const positionSide = String(position.position_side || position.positionSide || "").toUpperCase();
    addSearchResult(results, query, {
      id: `position-${position.broker_id}-${position.symbol}-${positionSide || "NET"}`,
      type: "포지션",
      label: `${position.symbol}${positionSide ? ` · ${positionSide}` : ""}`,
      detail: `${position.status_label} · ${position.broker_name}`,
      meta: [position.asset, position.currency, position.detail].join(" "),
      targetNav: "accounts",
      tone: statusTone(position.status),
    });
  });

  (snapshot.accounts ?? []).forEach((account) => {
    addSearchResult(results, query, {
      id: `account-${account.broker_id}`,
      type: "계좌",
      label: account.account,
      detail: `${account.status_label} · ${account.broker_name}`,
      meta: [account.currency, account.detail].join(" "),
      targetNav: "accounts",
      tone: statusTone(account.status),
    });
  });

  (snapshot.final_preflight ?? []).forEach((check) => {
    addSearchResult(results, query, {
      id: `preflight-${check.label}`,
      type: "최종점검",
      label: check.label,
      detail: check.detail,
      meta: check.status,
      targetNav: "overview",
      tone: statusTone(check.status),
    });
  });

  (snapshot.audit ?? []).slice(0, 10).forEach((item, index) => {
    addSearchResult(results, query, {
      id: `audit-${item.time}-${index}`,
      type: "감사",
      label: item.event,
      detail: `${item.time} · ${item.detail}`,
      meta: item.level,
      targetNav: "audit",
      tone: statusTone(item.level),
    });
  });

  (snapshot.incidents ?? snapshot.live_governance?.incidents ?? []).slice(0, 10).forEach((incident, index) => {
    addSearchResult(results, query, {
      id: `incident-${incident.incident_id || incident.incidentId || index}`,
      type: "사고",
      label: incident.title || incident.code || "운영 사고",
      detail: incident.detail || incident.impact || incident.status,
      meta: [incident.severity, incident.status, incident.source || incident.scope].join(" "),
      targetNav: "incidents",
      tone: String(incident.severity || "").toUpperCase() === "CRITICAL" ? "danger" : "warning",
    });
  });

  return results;
}

function buildNotificationItems(snapshot, error) {
  const items = [];
  const push = (item) => {
    if (items.length < 14) items.push(item);
  };

  if (error) {
    const disconnected = snapshot.api_connected === false;
    push({
      id: disconnected ? "api-error" : "action-error",
      tone: "danger",
      title: disconnected ? "API 연결 오류" : "작업 거부/오류",
      detail: error,
      targetNav: "overview",
    });
  }
  if (snapshot.kill_switch) {
    push({ id: "kill-switch", tone: "danger", title: "긴급 차단 활성화", detail: "모든 실거래 모드가 MONITOR로 고정됩니다.", targetNav: "overview" });
  }
  if (snapshot.summary?.blocker_count) {
    push({
      id: "blockers",
      tone: "danger",
      title: `Readiness blocker ${snapshot.summary.blocker_count}개`,
      detail: "실거래 주문 제출 전 hard stop을 해소해야 합니다.",
      targetNav: "gate",
    });
  }
  if (snapshot.summary?.warning_count) {
    push({
      id: "warnings",
      tone: "warning",
      title: `Warning ${snapshot.summary.warning_count}개`,
      detail: "FULL LIVE 전환 전 수동 검토가 필요합니다.",
      targetNav: "gate",
    });
  }
  if (snapshot.watchdog?.critical_count) {
    push({
      id: "watchdog-critical",
      tone: "danger",
      title: `Watchdog critical ${snapshot.watchdog.critical_count}개`,
      detail: snapshot.watchdog.next_actions?.slice(0, 3).join(", ") || "Watchdog 차단 조건을 확인하세요.",
      targetNav: "automation",
    });
  } else if (snapshot.watchdog?.warning_count) {
    push({
      id: "watchdog-warning",
      tone: "warning",
      title: `Watchdog warning ${snapshot.watchdog.warning_count}개`,
      detail: snapshot.watchdog.next_actions?.slice(0, 3).join(", ") || "Watchdog 경고 조건을 확인하세요.",
      targetNav: "automation",
    });
  }

  (snapshot.readiness ?? [])
    .filter((check) => check.status !== "pass")
    .slice(0, 4)
    .forEach((check) => {
      push({ id: `ready-${check.label}`, tone: statusTone(check.status), title: check.label, detail: check.detail, targetNav: "gate" });
    });

  (snapshot.brokers ?? [])
    .filter((broker) => !broker.order_ready)
    .slice(0, 3)
    .forEach((broker) => {
      push({
        id: `broker-${broker.broker_id}`,
        tone: "danger",
        title: `${broker.name} 준비 필요`,
        detail: broker.missing_env?.length ? `${broker.missing_env.length}개 환경 변수가 비어 있습니다.` : broker.detail,
        targetNav: "settings",
      });
    });

  if (snapshot.order_queue?.retryable) {
    push({
      id: "retryable-orders",
      tone: "warning",
      title: `재시도 가능 주문 ${snapshot.order_queue.retryable}건`,
      detail: "주문 큐에서 재시도 또는 취소 처리가 필요합니다.",
      targetNav: "gate",
    });
  }

  if (snapshot.reconciliation?.summary?.status && snapshot.reconciliation.summary.status !== "pass") {
    push({
      id: "reconciliation",
      tone: statusTone(snapshot.reconciliation.summary.status),
      title: `포지션·계좌 대조 ${snapshot.reconciliation.summary.status_label}`,
      detail: `API 필요 ${snapshot.reconciliation.summary.api_required_count}개, 불일치 ${snapshot.reconciliation.summary.mismatch_count}개`,
      targetNav: "accounts",
    });
  }

  (snapshot.final_preflight ?? [])
    .filter((check) => check.status !== "pass")
    .slice(0, 3)
    .forEach((check) => {
      push({ id: `preflight-${check.label}`, tone: statusTone(check.status), title: check.label, detail: check.detail, targetNav: "overview" });
    });

  if (!items.length) {
    push({ id: "clear", tone: "success", title: "주요 알림 없음", detail: "현재 표시할 blocker 또는 warning이 없습니다.", targetNav: "overview" });
  }
  return items;
}

function notificationFingerprint(items) {
  return items
    .filter((item) => item.id !== "clear")
    .map((item) => [item.id, item.tone, item.title, item.detail].join(":"))
    .join("|");
}

function normalizePanelKey(value) {
  return String(value || "panel")
    .replace(/\s+/g, "-")
    .replace(/[^a-zA-Z0-9가-힣_.-]/g, "")
    .slice(0, 96);
}

function panelLayoutKey(panel) {
  if (panel.dataset.layoutKey) return panel.dataset.layoutKey;
  const title = panel.querySelector(".panel-header h2")?.textContent ?? "panel";
  const stableClasses = Array.from(panel.classList)
    .filter((name) => !["panel", "resizable-panel", "dragging-panel"].includes(name))
    .join(".");
  const key = normalizePanelKey(`${stableClasses || "panel"}-${title}`);
  panel.dataset.layoutKey = key;
  return key;
}

function clampNumber(value, min, max, fallback) {
  const numberValue = Number(value);
  if (!Number.isFinite(numberValue)) return fallback;
  return Math.min(max, Math.max(min, Math.round(numberValue)));
}

function snapLayoutDimension(value, axis) {
  const snapped = Math.round(Number(value) / LAYOUT_SNAP_SIZE) * LAYOUT_SNAP_SIZE;
  const min = axis === "width" ? MIN_PANEL_WIDTH : MIN_PANEL_HEIGHT;
  return clampNumber(snapped, min, LAYOUT_MAX_DIMENSION, min);
}

function snapStoredSlotDimension(value, axis) {
  const snapped = Math.round(Number(value) / LAYOUT_SNAP_SIZE) * LAYOUT_SNAP_SIZE;
  const min = axis === "width" ? 160 : 64;
  return clampNumber(snapped, min, LAYOUT_MAX_DIMENSION, min);
}

function snapLayoutOffset(value, axis) {
  const snapped = Math.round(Number(value) / LAYOUT_SNAP_SIZE) * LAYOUT_SNAP_SIZE;
  return clampNumber(snapped, -LAYOUT_MAX_OFFSET, LAYOUT_MAX_OFFSET, 0);
}

function panelOverlapsPeers(activePanel) {
  return layoutElementOverlapsPeers(activePanel, ".panel", LAYOUT_COLLISION_GAP);
}

function applyPanelOffset(panel, position = {}) {
  return applyLayoutTransformOffset(panel, position.x, position.y, {
    max: LAYOUT_MAX_OFFSET,
    // Pointer movement is snapped before this call.  Slot exchanges can need
    // a one-pixel alignment correction after a CSS grid reflow, so do not
    // quantize persisted transforms a second time here.
    snap: 1,
  });
}

function clearPanelOffset(panel) {
  clearLayoutTransformOffset(panel);
}

function currentPanelOffset(panel, storedPosition = {}) {
  return readLayoutTransformOffset(panel, storedPosition, {
    max: LAYOUT_MAX_OFFSET,
    snap: 1,
  });
}

function repairLivePanelOverlaps(root) {
  if (!(root instanceof Element)) return [];
  const repairs = repairLayoutPanelOverlaps(".panel", { root, gap: LAYOUT_COLLISION_GAP });
  if (!repairs.length) return repairs;

  const sizeStore = readStoredMap(PANEL_SIZE_STORAGE_KEY);
  const positionStore = readStoredMap(PANEL_POSITION_STORAGE_KEY);
  repairs.forEach(({ element, offsetReset, sizeReset }) => {
    const key = panelLayoutKey(element);
    if (offsetReset) delete positionStore[key];
    if (sizeReset) delete sizeStore[key];
  });
  writeStoredMap(PANEL_SIZE_STORAGE_KEY, sizeStore);
  writeStoredMap(PANEL_POSITION_STORAGE_KEY, positionStore);
  window.dispatchEvent(new Event(LAYOUT_RESTORE_EVENT));
  return repairs;
}

function resolvePanelCollision(panel) {
  const workspace = panel.closest(".page-view") ?? panel.parentElement;
  if (!workspace) return null;
  const panelRect = panel.getBoundingClientRect();
  const currentOffset = currentPanelOffset(panel);
  const overlappingPeers = Array.from(workspace.querySelectorAll(".panel"))
    .filter((peer) => (
      peer !== panel
      && !panel.contains(peer)
      && !peer.contains(panel)
      && peer.getBoundingClientRect().width > 0
      && peer.getBoundingClientRect().height > 0
    ))
    .filter((peer) => {
      const peerRect = peer.getBoundingClientRect();
      return panelRect.left < peerRect.right
        && panelRect.right > peerRect.left
        && panelRect.top < peerRect.bottom
        && panelRect.bottom > peerRect.top;
    });
  if (!overlappingPeers.length) return currentOffset;

  const candidates = overlappingPeers.flatMap((peer) => {
    const peerRect = peer.getBoundingClientRect();
    return [
      { x: currentOffset.x + Math.floor(peerRect.left - panelRect.right), y: currentOffset.y },
      { x: currentOffset.x + Math.ceil(peerRect.right - panelRect.left), y: currentOffset.y },
      { x: currentOffset.x, y: currentOffset.y + Math.floor(peerRect.top - panelRect.bottom) },
      { x: currentOffset.x, y: currentOffset.y + Math.ceil(peerRect.bottom - panelRect.top) },
    ];
  }).sort((left, right) => (
    Math.abs(left.x - currentOffset.x) + Math.abs(left.y - currentOffset.y)
    - Math.abs(right.x - currentOffset.x) - Math.abs(right.y - currentOffset.y)
  ));

  for (const candidate of candidates) {
    const applied = applyPanelOffset(panel, candidate);
    if (!panelOverlapsPeers(panel)) return applied;
  }
  applyPanelOffset(panel, currentOffset);
  return null;
}

function isInteractiveLayoutTarget(target) {
  if (!(target instanceof Element)) return false;
  return Boolean(
    target.closest(
      [
        "button",
        "input",
        "select",
        "textarea",
        "a",
        "summary",
        "[role='button']",
        ".panel-resize-edge",
        ".panel-resize-corner",
      ].join(", "),
    ),
  );
}

function ensurePanelHandles(panel) {
  panel.classList.add("resizable-panel");
  panelLayoutKey(panel);

  const sizes = readStoredMap(PANEL_SIZE_STORAGE_KEY);
  const positions = readStoredMap(PANEL_POSITION_STORAGE_KEY);
  const key = panelLayoutKey(panel);
  const size = sizes[key];
  const position = positions[key];

  const isLayoutEditing = document.documentElement.dataset.layoutMode === "edit";
  panel.style.transform = "";
  panel.style.removeProperty("min-height");
  if (size && Number.isFinite(size.width)) {
    panel.style.width = `${snapStoredSlotDimension(size.width, "width")}px`;
  }
  if (size && Number.isFinite(size.height)) {
    const storedHeight = snapStoredSlotDimension(size.height, "height");
    if (isLayoutEditing) {
      panel.style.height = `${storedHeight}px`;
    } else {
      // A saved edit height is an operator preference, not a clipping mask.
      // Locked views must always grow when live data adds rows or controls.
      panel.style.removeProperty("height");
      panel.style.minHeight = `${storedHeight}px`;
    }
  } else {
    panel.style.removeProperty("height");
  }
  applyPanelOffset(panel, position);
  window.requestAnimationFrame(() => {
    if (!panel.isConnected || !panelOverlapsPeers(panel)) return;
    panel.style.removeProperty("width");
    clearPanelOffset(panel);
  });

  const existingDirections = new Set(
    Array.from(panel.querySelectorAll(":scope > .panel-resize-edge, :scope > .panel-resize-corner"))
      .map((handle) => handle.dataset.resizeDirection)
      .filter(Boolean),
  );
  if (LAYOUT_RESIZE_DIRECTIONS.every((direction) => existingDirections.has(direction))) return;
  panel.querySelectorAll(":scope > .panel-resize-edge, :scope > .panel-resize-corner").forEach((handle) => handle.remove());

  const directionWords = { n: "north", e: "east", s: "south", w: "west" };
  LAYOUT_RESIZE_DIRECTIONS.forEach((direction) => {
    const directionClass = [...direction].map((part) => directionWords[part]).join("-");
    const className = direction.length === 1
      ? `panel-resize-edge panel-resize-${directionClass}`
      : `panel-resize-corner panel-resize-${directionClass}`;
    const handle = document.createElement("span");
    handle.className = className;
    handle.dataset.resizeDirection = direction;
    handle.setAttribute("role", "separator");
    handle.setAttribute("aria-orientation", direction === "e" || direction === "w" ? "vertical" : "horizontal");
    handle.setAttribute("aria-label", `패널 ${direction} 방향 크기 조절`);
    panel.appendChild(handle);
  });
}

function useEditablePanels(rootRef) {
  useEffect(() => {
    const root = rootRef.current;
    if (!root) return undefined;
    let captureNewBaselines = false;

    const enhancePanels = () => {
      const storedSizes = readStoredMap(PANEL_SIZE_STORAGE_KEY);
      let capturedBaseline = false;
      root.querySelectorAll(".panel").forEach((panel) => {
        ensurePanelHandles(panel);
        if (!captureNewBaselines || document.documentElement.dataset.layoutMode !== "edit") return;
        const key = panelLayoutKey(panel);
        if (storedSizes[key]) return;
        const rect = panel.getBoundingClientRect();
        if (rect.width <= 0 || rect.height <= 0) return;
        storedSizes[key] = {
          width: snapStoredSlotDimension(rect.width, "width"),
          height: snapStoredSlotDimension(rect.height, "height"),
        };
        capturedBaseline = true;
      });
      if (capturedBaseline) writeStoredMap(PANEL_SIZE_STORAGE_KEY, storedSizes);
    };

    const capturePanelBaselines = () => {
      captureNewBaselines = true;
      enhancePanels();
    };

    const resetLayout = () => {
      captureNewBaselines = false;
      root.querySelectorAll(".panel").forEach((panel) => {
        panel.style.width = "";
        panel.style.height = "";
        panel.style.removeProperty("min-height");
        clearPanelOffset(panel);
      });
      root.querySelectorAll(".command-grid, .content-grid").forEach((grid) => {
        grid.style.gridTemplateColumns = "";
      });
    };

    const startResize = (event, panel, direction) => {
      if (document.documentElement.dataset.layoutMode !== "edit" || event.button !== 0) return;
      event.preventDefault();
      event.stopPropagation();
      event.target.setPointerCapture?.(event.pointerId);

      const key = panelLayoutKey(panel);
      const startX = event.clientX;
      const startY = event.clientY;
      const frozenPeerSlots = freezeLayoutPeerDimensions(
        captureLayoutPeerDimensions(panel, ".panel", {
          max: LAYOUT_MAX_DIMENSION,
          minHeight: MIN_PANEL_HEIGHT,
          minWidth: MIN_PANEL_WIDTH,
          snap: LAYOUT_SNAP_SIZE,
        }),
      );
      const bounds = panel.getBoundingClientRect();
      const positions = readStoredMap(PANEL_POSITION_STORAGE_KEY);
      const startOffset = currentPanelOffset(panel, positions[key]);
      const affectsWidth = direction.includes("e") || direction.includes("w");
      const affectsHeight = direction.includes("n") || direction.includes("s");
      let lastValidLayout = {
        width: bounds.width,
        height: bounds.height,
        x: startOffset.x,
        y: startOffset.y,
      };
      document.body.classList.add("is-resizing-layout");

      const onMove = (moveEvent) => {
        let nextWidth = bounds.width;
        let nextHeight = bounds.height;
        const nextOffset = { ...startOffset };
        if (affectsWidth) {
          const horizontalSign = direction.includes("w") ? -1 : 1;
          nextWidth = snapLayoutDimension(bounds.width + (moveEvent.clientX - startX) * horizontalSign, "width");
          panel.style.width = `${nextWidth}px`;
          if (direction.includes("w")) {
            nextOffset.x = snapLayoutOffset(startOffset.x + bounds.width - nextWidth, "x");
          }
        }
        if (affectsHeight) {
          const verticalSign = direction.includes("n") ? -1 : 1;
          nextHeight = snapLayoutDimension(bounds.height + (moveEvent.clientY - startY) * verticalSign, "height");
          panel.style.height = `${nextHeight}px`;
          if (direction.includes("n")) {
            nextOffset.y = snapLayoutOffset(startOffset.y + bounds.height - nextHeight, "y");
          }
        }
        applyPanelOffset(panel, nextOffset);
        if (panelOverlapsPeers(panel)) {
          panel.style.width = `${lastValidLayout.width}px`;
          panel.style.height = `${lastValidLayout.height}px`;
          applyPanelOffset(panel, { x: lastValidLayout.x, y: lastValidLayout.y });
          return;
        }
        const rect = panel.getBoundingClientRect();
        lastValidLayout = {
          width: rect.width,
          height: rect.height,
          ...currentPanelOffset(panel, positions[key]),
        };
      };

      const onUp = () => {
        const stored = readStoredMap(PANEL_SIZE_STORAGE_KEY);
        stored[key] = {
          width: Math.round(panel.getBoundingClientRect().width * 100) / 100,
          height: Math.round(panel.getBoundingClientRect().height * 100) / 100,
        };
        frozenPeerSlots.forEach((slot) => {
          stored[panelLayoutKey(slot.element)] = { width: slot.width, height: slot.height };
        });
        writeStoredMap(PANEL_SIZE_STORAGE_KEY, stored);
        const nextOffset = currentPanelOffset(panel, positions[key]);
        const positionStore = readStoredMap(PANEL_POSITION_STORAGE_KEY);
        if (Math.abs(nextOffset.x) > 0 || Math.abs(nextOffset.y) > 0) {
          positionStore[key] = nextOffset;
        } else {
          delete positionStore[key];
        }
        writeStoredMap(PANEL_POSITION_STORAGE_KEY, positionStore);
        document.body.classList.remove("is-resizing-layout");
        document.removeEventListener("pointermove", onMove);
        document.removeEventListener("pointerup", onUp);
        document.removeEventListener("pointercancel", onUp);
      };

      document.addEventListener("pointermove", onMove);
      document.addEventListener("pointerup", onUp, { once: true });
      document.addEventListener("pointercancel", onUp, { once: true });
    };

    const startMove = (event, panel) => {
      if (document.documentElement.dataset.layoutMode !== "edit" || event.button !== 0) return;
      if (isInteractiveLayoutTarget(event.target)) return;
      const header = event.target.closest(".panel-header");
      if (!header || !panel.contains(header)) return;
      event.preventDefault();
      event.stopPropagation();

      const key = panelLayoutKey(panel);
      const positions = readStoredMap(PANEL_POSITION_STORAGE_KEY);
      const startOffset = currentPanelOffset(panel, positions[key]);
      const startRect = panel.getBoundingClientRect();
      const startX = event.clientX;
      const startY = event.clientY;
      let lastValidOffset = startOffset;
      let swapTarget = null;
      panel.classList.add("dragging-panel");
      document.body.classList.add("is-moving-layout");

      const onMove = (moveEvent) => {
        const nextX = snapLayoutOffset(startOffset.x + moveEvent.clientX - startX, "x");
        const nextY = snapLayoutOffset(startOffset.y + moveEvent.clientY - startY, "y");
        applyPanelOffset(panel, { x: nextX, y: nextY });
        const nextSwapTarget = layoutDropTarget(panel, ".panel", { x: moveEvent.clientX, y: moveEvent.clientY });
        if (swapTarget !== nextSwapTarget) {
          swapTarget?.classList.remove("layout-swap-target");
          swapTarget = nextSwapTarget;
          swapTarget?.classList.add("layout-swap-target");
        }
        if (swapTarget) return;
        if (panelOverlapsPeers(panel)) {
          applyPanelOffset(panel, lastValidOffset);
          return;
        }
        lastValidOffset = currentPanelOffset(panel, positions[key]);
      };

      const onUp = () => {
        const stored = readStoredMap(PANEL_POSITION_STORAGE_KEY);
        const saveOffset = (storageKey, offset) => {
          if (Math.abs(offset.x) > 0 || Math.abs(offset.y) > 0) stored[storageKey] = offset;
          else delete stored[storageKey];
        };
        let nextOffset = lastValidOffset;
        if (swapTarget?.isConnected) {
          const targetKey = panelLayoutKey(swapTarget);
          const targetOffset = currentPanelOffset(swapTarget, stored[targetKey]);
          const targetRect = swapTarget.getBoundingClientRect();
          const swap = layoutSwapOffsets({
            activeOffset: startOffset,
            activeRect: startRect,
            max: LAYOUT_MAX_OFFSET,
            snap: LAYOUT_SNAP_SIZE,
            targetOffset,
            targetRect,
          });
          const swappedDimensions = layoutSwapDimensions({
            activeRect: startRect,
            max: LAYOUT_MAX_DIMENSION,
            minHeight: MIN_PANEL_HEIGHT,
            minWidth: MIN_PANEL_WIDTH,
            snap: LAYOUT_SNAP_SIZE,
            targetRect,
          });
          if (swap && swappedDimensions) {
            panel.style.width = `${swappedDimensions.active.width}px`;
            panel.style.height = `${swappedDimensions.active.height}px`;
            swapTarget.style.width = `${swappedDimensions.target.width}px`;
            swapTarget.style.height = `${swappedDimensions.target.height}px`;
            applyPanelOffset(panel, swap.active);
            applyPanelOffset(swapTarget, swap.target);
            const alignedActiveOffset = layoutAlignedOffset(panel, swap.active, targetRect, {
              max: LAYOUT_MAX_OFFSET,
              snap: 1,
            });
            const alignedTargetOffset = layoutAlignedOffset(swapTarget, swap.target, startRect, {
              max: LAYOUT_MAX_OFFSET,
              snap: 1,
            });
            applyPanelOffset(panel, alignedActiveOffset);
            applyPanelOffset(swapTarget, alignedTargetOffset);
            const collisionFreeActiveOffset = resolvePanelCollision(panel);
            const collisionFreeTargetOffset = resolvePanelCollision(swapTarget);
            if (
              collisionFreeActiveOffset
              && collisionFreeTargetOffset
              && !panelOverlapsPeers(panel)
              && !panelOverlapsPeers(swapTarget)
            ) {
              nextOffset = collisionFreeActiveOffset;
              saveOffset(targetKey, collisionFreeTargetOffset);
              const sizeStore = readStoredMap(PANEL_SIZE_STORAGE_KEY);
              sizeStore[key] = swappedDimensions.active;
              sizeStore[targetKey] = swappedDimensions.target;
              writeStoredMap(PANEL_SIZE_STORAGE_KEY, sizeStore);
            } else {
              panel.style.width = `${Math.round(startRect.width)}px`;
              panel.style.height = `${Math.round(startRect.height)}px`;
              swapTarget.style.width = `${Math.round(targetRect.width)}px`;
              swapTarget.style.height = `${Math.round(targetRect.height)}px`;
              applyPanelOffset(swapTarget, targetOffset);
              applyPanelOffset(panel, lastValidOffset);
            }
          }
        } else {
          applyPanelOffset(panel, lastValidOffset);
        }
        saveOffset(key, nextOffset);
        writeStoredMap(PANEL_POSITION_STORAGE_KEY, stored);
        swapTarget?.classList.remove("layout-swap-target");
        panel.classList.remove("dragging-panel");
        document.body.classList.remove("is-moving-layout");
        document.removeEventListener("pointermove", onMove, true);
        document.removeEventListener("pointerup", onUp, true);
        document.removeEventListener("pointercancel", onUp, true);
      };

      document.addEventListener("pointermove", onMove, true);
      document.addEventListener("pointerup", onUp, { once: true, capture: true });
      document.addEventListener("pointercancel", onUp, { once: true, capture: true });
    };

    const onPointerDown = (event) => {
      const resizeHandle = event.target.closest(".panel-resize-edge, .panel-resize-corner");
      const panel = event.target.closest(".panel");
      if (!panel || !root.contains(panel)) return;

      if (resizeHandle) {
        startResize(event, panel, resizeHandle.dataset.resizeDirection || "se");
        return;
      }
      startMove(event, panel);
    };

    const observer = new MutationObserver(enhancePanels);
    enhancePanels();
    observer.observe(root, { childList: true, subtree: true });
    root.addEventListener("pointerdown", onPointerDown);
    window.addEventListener(LAYOUT_RESET_EVENT, resetLayout);
    window.addEventListener(LAYOUT_RESTORE_EVENT, enhancePanels);
    window.addEventListener(LAYOUT_BASELINE_EVENT, capturePanelBaselines);

    return () => {
      observer.disconnect();
      root.removeEventListener("pointerdown", onPointerDown);
      window.removeEventListener(LAYOUT_RESET_EVENT, resetLayout);
      window.removeEventListener(LAYOUT_RESTORE_EVENT, enhancePanels);
      window.removeEventListener(LAYOUT_BASELINE_EVENT, capturePanelBaselines);
    };
  }, [rootRef]);
}

applyAppearance(readAppearance());
applyLayoutMode(readLayoutMode());

const LIVE_FLOW_STORAGE_KEY = "live_trader.guidedFlow.v1";
const LIVE_FLOW_IDS = ["overview", "gate", "functional-test", "accounts", "orders", "risk", "automation", "incidents", "audit", "settings"];
const DEPLOYMENT_CONTEXT_STORAGE_KEY = "live_trader.deploymentContext.v1";
function strategyDeploymentContext(strategy = null) {
  if (!strategy) {
    return {
      id: "",
      strategyId: "",
      name: "Deployment 미선택",
      portfolioId: "",
      portfolioName: "Portfolio 미확인",
      brokerId: "-",
      accountId: "미확인",
      symbol: "-",
      timeframe: "-",
    };
  }
  const gate = strategy.portfolio_gate && typeof strategy.portfolio_gate === "object" ? strategy.portfolio_gate : {};
  const brokerId = String(strategy.broker_id || strategy.provider || strategy.route || strategy.asset || "미확인");
  return {
    id: strategyDeploymentIdentity(strategy),
    strategyId: String(strategy.strategy_id || ""),
    name: String(strategy.name || strategy.strategy_id || "Deployment"),
    portfolioId: String(gate.portfolioId || gate.portfolio_id || ""),
    portfolioName: String(gate.portfolioName || gate.portfolio_name || gate.portfolioId || "Standalone (검토 필요)"),
    brokerId,
    accountId: String(strategy.account_id || strategy.accountId || "미확인"),
    symbol: String(strategy.symbol || "-"),
    timeframe: String(strategy.timeframe || "-"),
  };
}

function App() {
  const [snapshot, setSnapshot] = useState(fallbackSnapshot);
  const [selectedNav, setSelectedNav] = useState(() => readGuidedFlowStep(LIVE_FLOW_STORAGE_KEY, LIVE_FLOW_IDS, "overview"));
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [emergencyFeedback, setEmergencyFeedback] = useState("");
  const [emergencyAction, setEmergencyAction] = useState("");
  const [safetyConfirmation, setSafetyConfirmation] = useState(null);
  const [appearance, setAppearance] = useState(readAppearance);
  const [layoutMode, setLayoutMode] = useState(readLayoutMode);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(readSidebarCollapsed);
  const [searchQuery, setSearchQuery] = useState("");
  const [notificationsOpen, setNotificationsOpen] = useState(false);
  const [acknowledgedNotifications, setAcknowledgedNotifications] = useState(() => readStoredValue(NOTIFICATION_ACK_STORAGE_KEY, ""));
  const [selectedDeploymentId, setSelectedDeploymentId] = useState(() => readStoredValue(DEPLOYMENT_CONTEXT_STORAGE_KEY, ""));
  const workspaceRef = useRef(null);
  const notificationRef = useRef(null);
  const snapshotRequestInFlightRef = useRef(false);
  const snapshotSafetyEpochRef = useRef(0);
  const emergencyActionInFlightRef = useRef("");
  const safetyConfirmationResolverRef = useRef(null);
  const accountRefreshCoordinatorRef = useRef(null);
  if (!accountRefreshCoordinatorRef.current) {
    accountRefreshCoordinatorRef.current = createAccountRefreshCoordinator({
      syncExecutionEvents,
      runReconciliation,
    });
  }

  useEditablePanels(workspaceRef);

  useEffect(() => {
    const unregister = registerSafetyConfirmationPresenter((challenge) => new Promise((resolve) => {
      safetyConfirmationResolverRef.current = {
        challengeId: challenge.challengeId,
        resolve,
      };
      setSafetyConfirmation(challenge);
    }));
    return () => {
      unregister();
      const pending = safetyConfirmationResolverRef.current;
      safetyConfirmationResolverRef.current = null;
      if (pending) pending.resolve({ confirmed: false });
    };
  }, []);

  function resolveSafetyConfirmation(challengeId, decision) {
    const pending = safetyConfirmationResolverRef.current;
    if (!pending || pending.challengeId !== challengeId) return;
    safetyConfirmationResolverRef.current = null;
    setSafetyConfirmation(null);
    pending.resolve(decision);
  }

  async function refresh() {
    if (snapshotRequestInFlightRef.current || emergencyActionInFlightRef.current) return;
    const safetyEpoch = snapshotSafetyEpochRef.current;
    snapshotRequestInFlightRef.current = true;
    try {
      const next = await getSnapshot();
      if (safetyEpoch !== snapshotSafetyEpochRef.current) return;
      setSnapshot({ ...next, api_connected: true });
      setError("");
    } catch (err) {
      if (safetyEpoch !== snapshotSafetyEpochRef.current) return;
      const nativeEmergency = await getNativeEmergencyStopStatus();
      if (safetyEpoch !== snapshotSafetyEpochRef.current) return;
      setSnapshot((current) => disconnectedSnapshot(nativeEmergency, current));
      setError(err instanceof Error ? err.message : "API 연결 실패");
    } finally {
      snapshotRequestInFlightRef.current = false;
      setLoading(false);
    }
  }

  const liveRuntimeActive = Boolean(
    snapshot.continuous_runtime?.running || snapshot.execution_streams?.running,
  );
  const pollingIntervals = livePollingIntervals(liveRuntimeActive);

  useEffect(() => {
    refresh();
    const refreshWhenVisible = () => {
      if (document.visibilityState !== "hidden") refresh();
    };
    const timer = window.setInterval(refreshWhenVisible, pollingIntervals.snapshotMs);
    return () => window.clearInterval(timer);
  }, [pollingIntervals.snapshotMs]);

  useEffect(() => {
    if (selectedNav !== "accounts" || snapshot.api_connected !== true) return undefined;
    let stopped = false;
    const refreshBrokerAccounts = async () => {
      if (
        stopped
        || emergencyActionInFlightRef.current
        || accountRefreshCoordinatorRef.current.isRunning()
      ) return;
      const safetyEpoch = snapshotSafetyEpochRef.current;
      try {
        const result = await accountRefreshCoordinatorRef.current.run();
        if (!stopped && safetyEpoch === snapshotSafetyEpochRef.current && result?.snapshot) {
          setSnapshot({ ...result.snapshot, api_connected: true });
        }
      } catch {
        // Background account refresh stays quiet; the normal snapshot poll keeps connection status visible.
      }
    };
    void refreshBrokerAccounts();
    const timer = window.setInterval(refreshBrokerAccounts, ACCOUNT_REFRESH_INTERVAL_MS);
    return () => {
      stopped = true;
      window.clearInterval(timer);
    };
  }, [selectedNav, snapshot.api_connected]);

  useEffect(() => {
    applyAppearance(appearance);
  }, [appearance]);

  useEffect(() => {
    const exitLayoutEditing = (event) => {
      if (layoutMode !== "edit") return;
      if (event.detail) event.detail.handled = true;
      repairLivePanelOverlaps(workspaceRef.current);
      changeLayoutMode("locked");
    };
    window.addEventListener(LAYOUT_EDIT_EXIT_EVENT, exitLayoutEditing);
    return () => window.removeEventListener(LAYOUT_EDIT_EXIT_EVENT, exitLayoutEditing);
  }, [layoutMode]);

  useEffect(() => {
    if (layoutMode === "edit") return undefined;
    const frame = window.requestAnimationFrame(() => repairLivePanelOverlaps(workspaceRef.current));
    return () => window.cancelAnimationFrame(frame);
  }, [layoutMode, selectedNav]);

  useEffect(() => {
    let cancelled = false;
    getUiSettings()
      .then((result) => {
        if (cancelled || !result?.settings) return;
        const applyStockAccentBaseline = stockAccentBaselinePending();
        restoreUiSettings(result.settings);
        const savedAppearance = result.settings.appearance ?? readAppearance();
        const restoredAppearance = applyAppearance(
          applyStockAccentBaseline
            ? { ...savedAppearance, accent: "custom", customAccent: STOCK_CURRENT_ACCENT }
            : savedAppearance,
        );
        if (applyStockAccentBaseline) {
          persistUiSettings({ appearance: restoredAppearance }).then((saved) => {
            if (saved) markStockAccentBaselineApplied();
          });
        }
        setAppearance(restoredAppearance);
        setLayoutMode(readLayoutMode());
        setSidebarCollapsed(readSidebarCollapsed());
        window.dispatchEvent(new Event(LAYOUT_RESTORE_EVENT));
      })
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    applyLayoutMode(layoutMode);
    window.dispatchEvent(new Event(LAYOUT_RESTORE_EVENT));
  }, [layoutMode]);

  useEffect(() => {
    document.title = "[LIVE] Live Trader";
  }, []);

  const runningDeploymentId = snapshot.live_governance?.activeSession?.deploymentId
    || (snapshot.continuous_runtime?.running ? snapshot.continuous_runtime?.deploymentId : "")
    || "";
  const deploymentOptions = useMemo(
    () => buildCurrentDeploymentOptions(snapshot.strategies ?? [], {
      pinnedDeploymentIds: [runningDeploymentId],
    }),
    [runningDeploymentId, snapshot.strategies],
  );

  useEffect(() => {
    if (!deploymentOptions.length) {
      if (selectedDeploymentId) setSelectedDeploymentId("");
      return;
    }
    const stillExists = deploymentOptions.some((option) => option.id === selectedDeploymentId);
    if (stillExists) return;
    const nextId = deploymentOptions[0].id;
    setSelectedDeploymentId(nextId);
    try {
      window.localStorage.setItem(DEPLOYMENT_CONTEXT_STORAGE_KEY, JSON.stringify(nextId));
    } catch {
      // Storage-restricted runtimes keep the context for this session only.
    }
  }, [deploymentOptions, selectedDeploymentId]);

  useEffect(() => {
    if (!notificationsOpen) return undefined;
    const closeOnKey = (event) => {
      if (event.key === "Escape") setNotificationsOpen(false);
    };
    const closeOnPointer = (event) => {
      if (!notificationRef.current?.contains(event.target)) setNotificationsOpen(false);
    };
    window.addEventListener("keydown", closeOnKey);
    window.addEventListener("pointerdown", closeOnPointer);
    return () => {
      window.removeEventListener("keydown", closeOnKey);
      window.removeEventListener("pointerdown", closeOnPointer);
    };
  }, [notificationsOpen]);

  async function runAction(action, { allowDuringEmergency = false } = {}) {
    if (emergencyActionInFlightRef.current && !allowDuringEmergency) {
      return { ok: false, busy: true, reason: "emergency-action-in-progress" };
    }
    const safetyEpoch = snapshotSafetyEpochRef.current;
    setLoading(true);
    try {
      const result = await action();
      if (safetyEpoch === snapshotSafetyEpochRef.current) {
        setSnapshot({ ...(result.snapshot ?? result), api_connected: true });
        setError(result.ok === false ? result.reason : "");
      }
      return result;
    } catch (err) {
      const reason = err instanceof Error ? err.message : "요청 실패";
      if (
        safetyEpoch === snapshotSafetyEpochRef.current
        && (isApiConnectionFailure(err) || isApiRequestTimeout(err))
      ) {
        const nativeEmergency = await getNativeEmergencyStopStatus();
        if (safetyEpoch === snapshotSafetyEpochRef.current) {
          setSnapshot((current) => disconnectedSnapshot(nativeEmergency, current));
        }
      }
      if (safetyEpoch === snapshotSafetyEpochRef.current) setError(reason);
      return { ok: false, reason };
    } finally {
      setLoading(false);
    }
  }

  function updateAppearance(partial) {
    setAppearance((current) => {
      const next = applyAppearance({ ...current, ...partial });
      markStockAccentBaselineApplied();
      persistUiSettings({ appearance: next });
      return next;
    });
  }

  function changeLayoutMode(mode) {
    const next = applyLayoutMode(mode);
    setLayoutMode(next);
    persistUiSettings({ layoutMode: next });
    if (next === "edit") window.dispatchEvent(new Event(LAYOUT_BASELINE_EVENT));
  }

  function toggleSidebarCollapsed() {
    setSidebarCollapsed((current) => {
      const next = !current;
      saveSidebarCollapsed(next);
      persistUiSettings({ sidebarCollapsed: next });
      return next;
    });
  }

  function resetWorkspaceLayout() {
    try {
      window.localStorage.removeItem(PANEL_SIZE_STORAGE_KEY);
      window.localStorage.removeItem(PANEL_POSITION_STORAGE_KEY);
      persistUiSettings({ panelSizes: {}, panelPositions: {} });
    } catch {
      // The event below still resets the visible layout.
    }
    window.dispatchEvent(new Event(LAYOUT_RESET_EVENT));
  }

  function navigateWorkspace(navId) {
    if (LIVE_FLOW_IDS.includes(navId)) {
      writeGuidedFlowStep(LIVE_FLOW_STORAGE_KEY, navId);
    }
    setSelectedNav(navId);
    setNotificationsOpen(false);
  }

  function acknowledgeNotifications(key) {
    setAcknowledgedNotifications(key);
    try {
      window.localStorage.setItem(NOTIFICATION_ACK_STORAGE_KEY, JSON.stringify(key));
    } catch {
      // The badge can reset in storage-restricted runtimes, but the session state still updates.
    }
  }

  function confirmSafetyChange(message, action) {
    if (!window.confirm(message)) return Promise.resolve({ ok: false, cancelled: true });
    return runAction(action);
  }

  function beginEmergencyAction(action) {
    if (emergencyActionInFlightRef.current) return false;
    emergencyActionInFlightRef.current = action;
    snapshotSafetyEpochRef.current += 1;
    setEmergencyAction(action);
    return true;
  }

  function finishEmergencyAction(action) {
    if (emergencyActionInFlightRef.current === action) {
      emergencyActionInFlightRef.current = "";
    }
    setEmergencyAction((current) => (current === action ? "" : current));
  }

  async function engageGlobalKill() {
    if (emergencyActionInFlightRef.current) {
      return { ok: false, busy: true, reason: "emergency-action-in-progress" };
    }
    if (!beginEmergencyAction("engage")) {
      return { ok: false, busy: true, reason: "emergency-action-in-progress" };
    }
    const apiWasConnected = snapshot.api_connected === true;
    setLoading(true);
    setEmergencyFeedback("독립 긴급정지 래치를 기록하는 중입니다.");
    let nativeResult = null;
    let nativeFailClosed = null;
    try {
      try {
        nativeResult = await engageNativeEmergencyStop("operator global Kill Switch");
        setSnapshot((current) => ({
          ...current,
          kill_switch: true,
          new_entries_blocked: true,
          emergency_stop: { ...nativeResult, available: true, status_available: true },
        }));
        setEmergencyFeedback("독립 Kill 래치가 저장되었습니다. 이후 Broker POST는 차단됩니다.");
      } catch (nativeError) {
        const details = nativeError?.details;
        if (details?.active === true) {
          nativeFailClosed = details;
          setSnapshot((current) => ({
            ...current,
            kill_switch: true,
            new_entries_blocked: true,
            emergency_stop: { ...details, available: true, status_available: true },
          }));
        }
        if (!apiWasConnected) {
          const reason = nativeError instanceof Error ? nativeError.message : "독립 긴급정지 실패";
          setEmergencyFeedback(
            nativeFailClosed
              ? `Kill 내구 저장 실패 · 현재 프로세스는 Fail-Closed입니다: ${reason}`
              : `Kill 래치 저장 실패: ${reason}`,
          );
          setError(reason);
          return { ok: false, reason, active: nativeFailClosed?.active === true };
        }
      }

      if (apiWasConnected) {
        const result = await setFlag("kill_switch", true, true);
        const nextSnapshot = result.snapshot ?? result;
        const effectiveEmergency = nativeResult || nativeFailClosed;
        setSnapshot((current) => ({
          ...current,
          ...nextSnapshot,
          api_connected: true,
          ...(effectiveEmergency?.active === true
            ? {
                kill_switch: true,
                new_entries_blocked: true,
                emergency_stop: {
                  ...effectiveEmergency,
                  available: true,
                  status_available: true,
                },
              }
            : {}),
        }));
        if (result.ok === false) {
          setError(result.reason || "Kill 후속 정리 실패");
          setEmergencyFeedback(
            nativeResult?.ok === true
              ? "독립 Kill은 유지되지만 Runtime 정리 결과를 확인해야 합니다."
              : `Kill 실패: ${result.reason || "원인 미확인"}`,
          );
        } else {
          setError("");
          setEmergencyFeedback("Kill 고정 완료 · 신규 주문 차단 및 Runtime 정리를 요청했습니다.");
        }
        return result;
      }
      return nativeResult || { ok: false, reason: "독립 긴급정지 실패" };
    } catch (apiError) {
      const reason = apiError instanceof Error ? apiError.message : "Kill 후속 정리 실패";
      const effectiveEmergency = nativeResult || nativeFailClosed;
      setSnapshot((current) => ({
        ...current,
        api_connected: false,
        kill_switch: effectiveEmergency?.active === true || current.kill_switch === true,
        new_entries_blocked: effectiveEmergency?.active === true ? true : current.new_entries_blocked,
        emergency_stop: effectiveEmergency
          ? { ...effectiveEmergency, available: true, status_available: true }
          : current.emergency_stop,
      }));
      setError(reason);
      setEmergencyFeedback(
        nativeResult?.ok === true
          ? "API 정리는 실패했지만 독립 Kill 래치는 유지됩니다. API 복구 전에는 해제할 수 없습니다."
          : nativeFailClosed
            ? "API 정리는 실패했고 내구 저장도 확인되지 않았습니다. 현재 프로세스는 Fail-Closed입니다."
            : `Kill 실패: ${reason}`,
      );
      return { ok: nativeResult?.ok === true, reason };
    } finally {
      setLoading(false);
      finishEmergencyAction("engage");
    }
  }

  async function releaseGlobalKill() {
    if (emergencyActionInFlightRef.current) {
      return { ok: false, busy: true, reason: "emergency-action-in-progress" };
    }
    if (snapshot.api_connected !== true) {
      setEmergencyFeedback("Kill 해제는 복구된 HTTP API의 안전 확인을 거쳐야 합니다.");
      return { ok: false, reason: "api-confirmed-release-required" };
    }
    if (!beginEmergencyAction("release")) {
      return { ok: false, busy: true, reason: "emergency-action-in-progress" };
    }
    setEmergencyFeedback("Kill 해제 안전 경계를 확인하는 중입니다.");
    try {
      const result = await runAction(
        () => setFlag("kill_switch", false, true),
        { allowDuringEmergency: true },
      );
      if (result?.ok === true) {
        setEmergencyFeedback("Kill 해제 완료 · 재무장 전까지 다른 LIVE 게이트는 계속 차단합니다.");
      } else {
        setEmergencyFeedback(`Kill 해제 실패: ${result?.reason || "원인 미확인"}`);
      }
      return result;
    } finally {
      finishEmergencyAction("release");
    }
  }

  function emergencyKillFromSafetyConfirmation() {
    if (emergencyActionInFlightRef.current === "release") {
      setEmergencyFeedback("Kill 해제 확인을 취소했습니다. 현재 Kill은 계속 유지됩니다.");
      return;
    }
    void engageGlobalKill();
  }

  function selectDeploymentContext(deploymentId) {
    setSelectedDeploymentId(deploymentId);
    try {
      window.localStorage.setItem(DEPLOYMENT_CONTEXT_STORAGE_KEY, JSON.stringify(deploymentId));
    } catch {
      // Storage-restricted runtimes keep the context for this session only.
    }
  }

  const title = navItems.find((item) => item.id === selectedNav)?.label ?? "운영 현황";
  const selectedStrategy = deploymentOptions.find((option) => option.id === selectedDeploymentId)?.strategy
    || deploymentOptions[0]?.strategy
    || null;
  const deploymentContext = strategyDeploymentContext(selectedStrategy);
  const notifications = buildNotificationItems(snapshot, error);
  const notificationKey = notificationFingerprint(notifications);
  const unreadNotificationCount = notificationKey && notificationKey !== acknowledgedNotifications ? notifications.filter((item) => item.id !== "clear").length : 0;

  return (
    <div className={`app-shell ${sidebarCollapsed ? "sidebar-collapsed" : ""}`}>
      <aside className="sidebar">
        <div className="brand-block" aria-label="Live Trader">
          <div className="brand-mark">
            <Radio size={19} />
          </div>
          <div>
            <strong>Live Trader</strong>
            <span className="brand-environment">LIVE</span>
          </div>
        </div>
        <nav className="nav-list" aria-label="주요 메뉴">
          {navGroups.map((group) => (
            <div className="nav-group" key={group.id}>
              <span className="nav-group-label">{group.label}</span>
              {group.itemIds.map((itemId) => {
                const item = navItems.find((candidate) => candidate.id === itemId);
                const Icon = item.icon;
                return (
                  <button
                    className={`nav-item ${selectedNav === item.id ? "active" : ""}`}
                    type="button"
                    key={item.id}
                    onClick={() => navigateWorkspace(item.id)}
                  >
                    <Icon size={17} />
                    <span>{item.label}</span>
                  </button>
                );
              })}
            </div>
          ))}
        </nav>
        <div className="sidebar-footer">
          <span>전역 Kill</span>
          <StatusPill tone={snapshot.kill_switch ? "danger" : snapshot.api_connected ? "success" : "warning"}>
            {snapshot.kill_switch ? "KILLED" : snapshot.api_connected ? "NORMAL" : snapshot.emergency_stop?.available ? "Kill 사용 가능" : "API 끊김"}
          </StatusPill>
        </div>
      </aside>

      <main className={`workspace layout-${layoutMode}`} ref={workspaceRef} data-layout-mode={layoutMode}>
        <header className="topbar">
          <div className="topbar-left">
            <IconButton
              className="sidebar-toggle"
              active={sidebarCollapsed}
              aria-label={sidebarCollapsed ? "탭 화면 펼치기" : "탭 화면 접기"}
              pressed={sidebarCollapsed}
              title={sidebarCollapsed ? "탭 화면 펼치기" : "탭 화면 접기"}
              onClick={toggleSidebarCollapsed}
            >
              <PanelLeft size={18} />
            </IconButton>
            <div className="topbar-title-block">
              <span>{pageProfiles[selectedNav]?.eyebrow || "LIVE"}</span>
              <strong>{title}</strong>
            </div>
          </div>
          <div className="topbar-actions">
            <StatusPill tone={snapshot.mode === "FULL_LIVE" ? "danger" : snapshot.mode === "SMALL_LIVE" ? "warning" : "info"}>
              {snapshot.mode === "FULL_LIVE" ? "FULL LIVE" : snapshot.mode === "SMALL_LIVE" ? "LIMITED LIVE" : "MONITOR"}
            </StatusPill>
            <StatusPill tone={snapshot.new_entries_blocked ? "warning" : "success"}>
              신규 진입 {snapshot.new_entries_blocked ? "차단" : "허용"}
            </StatusPill>
            <StatusPill tone={(snapshot.order_queue?.active || 0) ? "warning" : "neutral"}>
              미결 주문 {snapshot.order_queue?.active || 0}
            </StatusPill>
            <div className="notification-wrap" ref={notificationRef}>
              <IconButton
                className="notification-button"
                active={notificationsOpen}
                aria-label="알림"
                aria-expanded={notificationsOpen}
                onClick={() => {
                  if (!notificationsOpen && notificationKey) acknowledgeNotifications(notificationKey);
                  setNotificationsOpen((open) => !open);
                }}
              >
                <Bell size={17} />
                {unreadNotificationCount > 0 && <span className="notification-badge">{Math.min(unreadNotificationCount, 9)}</span>}
              </IconButton>
              {notificationsOpen && <NotificationPanel items={notifications} onNavigate={navigateWorkspace} />}
            </div>
            <button
              className={`danger-button emergency-stop-button ${snapshot.kill_switch ? "active" : ""}`}
              aria-busy={emergencyAction ? "true" : "false"}
              disabled={Boolean(emergencyAction)}
              type="button"
              onClick={() =>
                snapshot.kill_switch && snapshot.api_connected
                  ? releaseGlobalKill()
                  : engageGlobalKill()
              }
            >
              <CircleStop size={17} />
              {emergencyAction === "engage"
                ? "Kill 고정 중"
                : emergencyAction === "release"
                  ? "Kill 해제 확인 중"
                  : snapshot.kill_switch && snapshot.api_connected
                    ? "Kill 해제"
                    : snapshot.kill_switch
                      ? "Kill 재고정"
                      : "전역 Kill"}
            </button>
            {emergencyFeedback && (
              <span
                className={`emergency-stop-feedback ${snapshot.kill_switch ? "active" : ""}`}
                role="status"
                title={emergencyFeedback}
              >
                {emergencyFeedback}
              </span>
            )}
          </div>
        </header>

        {selectedNav === "functional-test" ? null : (
          <LiveEnvironmentBar
            context={deploymentContext}
            deploymentOptions={deploymentOptions}
            onSelect={selectDeploymentContext}
            snapshot={snapshot}
          />
        )}

        {error && snapshot.api_connected === false && (
          <section className="api-connection-banner" role="alert">
            <Network size={18} />
            <div>
              <strong>HTTP API 상태를 확인할 수 없어 거래 Snapshot을 안전 차단 상태로 전환했습니다.</strong>
              <span>
                {error} · {snapshot.emergency_stop?.active
                  ? snapshot.emergency_stop?.durable
                    ? "독립 Kill 래치는 내구 저장된 상태입니다."
                    : "독립 Kill은 Fail-Closed지만 내구 저장을 확인해야 합니다."
                  : snapshot.emergency_stop?.available
                    ? snapshot.emergency_stop?.status_available
                      ? "독립 Kill 래치는 현재 해제 상태이며 Kill 실행 경로는 사용 가능합니다."
                      : "독립 Kill 상태는 확인하지 못했지만 Kill 실행 경로는 사용 가능합니다."
                    : "독립 Kill은 데스크톱 앱에서만 실행할 수 있습니다."}
              </span>
            </div>
            <button className="mini-button" type="button" disabled={loading} onClick={refresh}>
              <RefreshCcw size={14} />
              다시 연결
            </button>
          </section>
        )}

        <WorkspaceContent
          selectedNav={selectedNav}
          onNavigate={navigateWorkspace}
          snapshot={snapshot}
          deploymentContext={deploymentContext}
          selectedStrategy={selectedStrategy}
          onDeploymentSelect={selectDeploymentContext}
          searchQuery={searchQuery}
          onConfirm={() => runAction(() => setFlag("operator_confirmed", !snapshot.operator_confirmed))}
          onDryRun={() => runAction(() => setFlag("dry_run", !snapshot.dry_run, snapshot.dry_run))}
          onEntryBlock={() =>
            snapshot.new_entries_blocked
              ? runAction(() => setFlag("new_entries_blocked", false, true))
              : runAction(() => setFlag("new_entries_blocked", true))
          }
          onAutomation={(profileId, enabled, provider, mode) => runAction(() => setAutomationProfile(profileId, enabled, provider, mode))}
          onStrategyCycle={(profileId) => runAction(() => runStrategyCycle(profileId))}
          onValidationEvaluate={(candidateId) => runAction(() => evaluateValidationSmallLive(candidateId))}
          onRuntimeStart={(profileId, mode) => runAction(() => startContinuousRuntime(
            profileId,
            mode,
            deploymentContext.portfolioId,
            deploymentContext.id,
            deploymentContext.strategyId,
          ))}
          onRuntimeStop={(profileId) => runAction(() => stopContinuousRuntime(profileId))}
          onPromoteLive={(strategyId) => runAction(() => promoteStrategyToLive(strategyId))}
          onStrategyLifecycle={(strategyId, action) => runAction(() => setStrategyLifecycle(strategyId, action))}
          onWatchdog={() => runAction(runWatchdog)}
          onTestIntent={() => runAction(submitTestIntent)}
          onRiskSetting={(name, value) => runAction(() => setRiskSetting(name, value))}
          onRetryPolicy={(name, value) => runAction(() => setRetryPolicy(name, value))}
          onRetryOrder={(orderId) => runAction(() => retryOrder(orderId))}
          onCancelOrder={(orderId) => runAction(() => cancelOrder(orderId))}
          onReconcile={() => runAction(runReconciliation)}
          onPreflight={() => runAction(() => runFinalPreflight(deploymentContext.id, deploymentContext.strategyId))}
          onAccountRefresh={() => runAction(() => accountRefreshCoordinatorRef.current.run())}
          onProgramLedgerBaseline={() =>
            confirmSafetyChange(
              "현재 브로커 잔고와 포지션을 프로그램 원장의 새 기준으로 승인하시겠습니까? 외부 주문·입출금 차이가 모두 현재 값으로 재설정됩니다.",
              () => seedProgramLedgerBaseline(true),
            )
          }
          onEnvSettings={(values, confirmed) => runAction(() => saveEnvSettings(values, confirmed))}
          appearance={appearance}
          updateAppearance={updateAppearance}
          layoutMode={layoutMode}
          changeLayoutMode={changeLayoutMode}
          resetWorkspaceLayout={resetWorkspaceLayout}
        />
      </main>
      {safetyConfirmation && (
        <SafetyConfirmationModal
          challenge={safetyConfirmation}
          key={safetyConfirmation.challengeId}
          onEmergencyKill={emergencyKillFromSafetyConfirmation}
          onResolve={(decision) => resolveSafetyConfirmation(safetyConfirmation.challengeId, decision)}
        />
      )}
    </div>
  );
}

function LiveEnvironmentBar({ context, deploymentOptions, onSelect, snapshot }) {
  const governedDeploymentId = governedDeploymentIdentity(snapshot.live_governance);
  const contextMatchesPreflight = deploymentContextMatchesPreflight(
    context.id,
    snapshot.live_governance,
  );
  const preflightValid = snapshot.live_governance?.preflightValidity?.valid === true;
  const launchLocked = snapshot.launch_report?.real_order_lock !== "ready"
    || !contextMatchesPreflight
    || !preflightValid;
  const armed = snapshot.api_connected === true
    && !snapshot.kill_switch
    && snapshot.operator_confirmed
    && !launchLocked;
  const orderRouteEnabled = armed
    && !snapshot.dry_run
    && ["SMALL_LIVE", "FULL_LIVE"].includes(String(snapshot.mode || "").toUpperCase());
  const sessionId = snapshot.live_governance?.activeSession?.sessionId
    || snapshot.runtime_session?.sessionId
    || snapshot.continuous_runtime?.sessionId
    || "세션 없음";
  const safety = [
    { label: "실거래 잠금", value: armed ? "ARMED" : "LOCKED", tone: armed ? "warning" : "neutral" },
    { label: "신규 진입", value: snapshot.new_entries_blocked ? "BLOCKED" : "ALLOWED", tone: snapshot.new_entries_blocked ? "warning" : "success" },
    { label: "위험 증가 주문", value: snapshot.new_entries_blocked ? "REDUCE-ONLY" : "ALLOWED", tone: snapshot.new_entries_blocked ? "warning" : "success" },
    { label: "Broker 전송", value: orderRouteEnabled ? "ENABLED" : "DISABLED", tone: orderRouteEnabled ? "danger" : "neutral" },
    { label: "전역 Kill", value: snapshot.kill_switch ? "KILLED" : "NORMAL", tone: snapshot.kill_switch ? "danger" : "success" },
  ];

  return (
    <section className="live-environment-bar" aria-label="LIVE 환경 및 안전 상태">
      <div className="live-environment-identity">
        <span className="live-environment-badge">LIVE · 실계좌</span>
        <label>
          <span>현재 Deployment</span>
          <select value={context.id} onChange={(event) => onSelect(event.target.value)}>
            {(deploymentOptions || []).map((option) => (
              <option key={option.id} value={option.id}>{option.label}</option>
            ))}
            {!deploymentOptions?.length && <option value="">실행 가능한 Deployment 없음</option>}
          </select>
        </label>
        <div className="live-context-meta">
          <strong>{context.portfolioName}</strong>
          <span>{context.brokerId} · 계정 {context.accountId} · {context.symbol} {context.timeframe}</span>
          <span>Session · {sessionId}</span>
          {(!contextMatchesPreflight || !preflightValid) && <span>선택 변경 또는 만료 · 이 Deployment의 Preflight를 다시 실행하세요.</span>}
        </div>
      </div>
      <div className="live-safety-hierarchy">
        {safety.map((item) => (
          <div key={item.label}>
            <span>{item.label}</span>
            <StatusPill tone={item.tone}>{item.value}</StatusPill>
          </div>
        ))}
      </div>
    </section>
  );
}

function dateStamp(date = new Date()) {
  return date.toISOString().replace(/[-:]/g, "").slice(0, 15);
}

function formatAuditTime(item = {}) {
  const value = item.timestamp || item.datetime || item.occurred_at || item.occurredAt || item.created_at || item.createdAt || item.time || "";
  const text = String(value || "").trim();
  const fullMatch = text.match(/(\d{4})[-.](\d{2})[-.](\d{2})[T\s]+(\d{2}:\d{2}:\d{2})/);
  if (fullMatch) return `${fullMatch[1]}-${fullMatch[2]}-${fullMatch[3]} ${fullMatch[4]}`;
  const parsed = new Date(text);
  if (!Number.isNaN(parsed.getTime())) {
    const pad = (number) => String(number).padStart(2, "0");
    return `${parsed.getFullYear()}-${pad(parsed.getMonth() + 1)}-${pad(parsed.getDate())} ${pad(parsed.getHours())}:${pad(parsed.getMinutes())}:${pad(parsed.getSeconds())}`;
  }
  const timeMatch = text.match(/(\d{2}:\d{2}:\d{2})/);
  if (timeMatch) {
    const now = new Date();
    const pad = (number) => String(number).padStart(2, "0");
    return `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())} ${timeMatch[1]}`;
  }
  return text || "-";
}

function csvCell(value) {
  if (value === null || value === undefined) return "";
  const raw = String(value);
  const formulaSafe = typeof value === "string" && /^[=+\-@]/.test(raw) ? `'${raw}` : raw;
  const text = formulaSafe.replaceAll('"', '""');
  return /[",\n]/.test(text) ? `"${text}"` : text;
}

function downloadCsv(columns, rows, filename) {
  const csv = [columns.join(","), ...rows.map((row) => columns.map((column) => csvCell(row[column])).join(","))].join("\n");
  const blob = new Blob([`\uFEFF${csv}`], { type: "text/csv;charset=utf-8" });
  const url = window.URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  window.URL.revokeObjectURL(url);
}

function WorkspaceContent({
  selectedNav,
  onNavigate,
  snapshot,
  deploymentContext,
  selectedStrategy,
  onDeploymentSelect,
  searchQuery,
  onConfirm,
  onDryRun,
  onEntryBlock,
  onAutomation,
  onStrategyCycle,
  onValidationEvaluate,
  onRuntimeStart,
  onRuntimeStop,
  onPromoteLive,
  onStrategyLifecycle,
  onWatchdog,
  onTestIntent,
  onRiskSetting,
  onRetryPolicy,
  onRetryOrder,
  onCancelOrder,
  onReconcile,
  onPreflight,
  onAccountRefresh,
  onProgramLedgerBaseline,
  onEnvSettings,
  appearance,
  updateAppearance,
  layoutMode,
  changeLayoutMode,
  resetWorkspaceLayout,
}) {
  const renderPage = (content) => (
    <PageView selectedNav={selectedNav} onNavigate={onNavigate} snapshot={snapshot} searchQuery={searchQuery}>
      {content}
    </PageView>
  );

  if (selectedNav === "automation") {
    return renderPage(
      <section className="ts-panel-grid ts-panel-grid--two automation-page-layout">
        <DeploymentContextPanel className="live-grid-full" context={deploymentContext} />
        <AutomationLauncherPanel
          className="live-grid-full"
          deploymentContext={deploymentContext}
          profiles={snapshot.automation_profiles}
          strategies={snapshot.strategies}
          runnerState={snapshot.strategy_runner}
          onAutomation={onAutomation}
          onStrategyCycle={onStrategyCycle}
          onValidationEvaluate={onValidationEvaluate}
          onRuntimeStart={onRuntimeStart}
          onRuntimeStop={onRuntimeStop}
          runtime={snapshot.continuous_runtime}
          soakReport={snapshot.soakReport || snapshot.soak_report}
        />
        <RuntimeComponentStatusPanel snapshot={snapshot} />
        <WatchdogPanel watchdog={snapshot.watchdog} onWatchdog={onWatchdog} />
        <FuturesSettingsPanel snapshot={snapshot.binance_futures_settings} selectedSymbol={deploymentContext?.symbol} />
        <FuturesFillSoakPanel snapshot={snapshot.binance_futures_fill_soak} selectedSymbol={deploymentContext?.symbol} />
        <CapitalRolloutPanel
          className="live-grid-full"
          snapshot={snapshot.capital_rollout}
          selectedStrategyId={selectedStrategy?.strategy_id}
        />
      </section>,
    );
  }

  if (selectedNav === "functional-test") {
    return renderPage(<FunctionalTestWorkspace />);
  }

  if (selectedNav === "gate") {
    return renderPage(
      <section className="deployment-promotion-layout ts-layout-stack">
        <DeploymentContextPanel context={deploymentContext} />
        <DeploymentManifestPanel governance={snapshot.live_governance} />
        <LivePreparationPanel
          snapshot={snapshot}
          deploymentOnly
          selectedStrategyId={selectedStrategy?.strategy_id}
          onSelectedStrategyIdChange={(strategyId) => {
            const strategy = (snapshot.strategies ?? []).find((item) => item.strategy_id === strategyId);
            if (strategy) onDeploymentSelect(strategyDeploymentIdentity(strategy));
          }}
          onConfirm={onConfirm}
          onDryRun={onDryRun}
          onEntryBlock={onEntryBlock}
          onTestIntent={onTestIntent}
          onRiskSetting={onRiskSetting}
          onRetryPolicy={onRetryPolicy}
          onRetryOrder={onRetryOrder}
          onCancelOrder={onCancelOrder}
          onPromoteLive={onPromoteLive}
          onStrategyLifecycle={onStrategyLifecycle}
        />
      </section>,
    );
  }

  if (selectedNav === "accounts") {
    return renderPage(
      <section className="accounts-page-layout ts-layout-stack">
        <ThreeWayReconciliationPanel snapshot={snapshot} />
        <UnifiedBrokerAccountPanel
          accounts={snapshot.accounts ?? []}
          executionEvents={snapshot.execution_events}
          onBaseline={onProgramLedgerBaseline}
          onRefresh={onAccountRefresh}
          positions={snapshot.positions ?? []}
          reconciledAt={snapshot.reconciliation?.summary?.last_run}
          refreshDisabled={snapshot.api_connected !== true}
        />
      </section>,
    );
  }

  if (selectedNav === "orders") {
    return renderPage(
      <OrderExecutionWorkspace
        context={deploymentContext}
        snapshot={snapshot}
        onRetryOrder={onRetryOrder}
        onCancelOrder={onCancelOrder}
      />,
    );
  }

  if (selectedNav === "risk") {
    return renderPage(
      <section className="ts-panel-grid ts-panel-grid--two risk-page-layout">
        <DeploymentContextPanel className="live-grid-full" context={deploymentContext} />
        <RiskUsagePanel snapshot={snapshot} context={deploymentContext} />
        <OperationalSafeguardsPanel
          apiConnected={snapshot.api_connected === true}
          dryRun={snapshot.dry_run}
          newEntriesBlocked={snapshot.new_entries_blocked}
          killSwitch={snapshot.kill_switch}
          operatorConfirmed={snapshot.operator_confirmed}
          onConfirm={onConfirm}
          onDryRun={onDryRun}
          onEntryBlock={onEntryBlock}
          onTestIntent={onTestIntent}
        />
        <RiskSettingsPanel settings={snapshot.risk_settings} onRiskSetting={onRiskSetting} />
        <RetryPolicyPanel policy={snapshot.retry_policy} onRetryPolicy={onRetryPolicy} />
        <FuturesRiskSimulatorPanel strategies={selectedStrategy ? [selectedStrategy] : []} />
        <RetryDecisionMatrixPanel matrix={snapshot.retry_policy_matrix} />
        <WatchdogPanel className="live-grid-full" watchdog={snapshot.watchdog} onWatchdog={onWatchdog} />
      </section>,
    );
  }

  if (selectedNav === "incidents") {
    return renderPage(
      <IncidentAuditWorkspace snapshot={snapshot} />,
    );
  }

  if (selectedNav === "audit") {
    return renderPage(
      <section className="audit-page-layout ts-layout-stack">
        <AuditPanel audit={snapshot.technical_logs || snapshot.audit} title="기술 로그" />
      </section>,
    );
  }

  if (selectedNav === "settings") {
    return renderPage(
      <section className="settings-page-layout ts-layout-stack">
        <div className="settings-summary-grid ts-panel-grid ts-panel-grid--two">
          <AppearanceControlPanel
            appearance={appearance}
            updateAppearance={updateAppearance}
            layoutMode={layoutMode}
            changeLayoutMode={changeLayoutMode}
            resetWorkspaceLayout={resetWorkspaceLayout}
          />
          <TelegramConnectionPanel />
        </div>
        <BrokerConnectionAssistant brokers={snapshot.brokers} diagnostics={snapshot.broker_diagnostics} onSave={onEnvSettings} />
        <DoctorHistoryPanel diagnostics={snapshot.doctor_diagnostics} onNavigate={onNavigate} />
      </section>,
    );
  }

  return renderPage(
    <OperationsOverviewPage
      context={deploymentContext}
      snapshot={snapshot}
      onNavigate={onNavigate}
      onReconcile={onReconcile}
      onPreflight={onPreflight}
      onWatchdog={onWatchdog}
    />,
  );
}

function DeploymentContextPanel({ context = {}, className = "" }) {
  return (
    <section className={`panel deployment-context-panel ${className}`.trim()}>
      <PanelHeader
        title="현재 Deployment"
        subtitle="이 화면의 설정·Preview·Runtime은 상단에서 선택한 동일한 배포 컨텍스트를 사용합니다."
        suffix={<StatusPill tone={context.portfolioId ? "info" : "warning"}>{context.portfolioId ? "PORTFOLIO" : "STANDALONE"}</StatusPill>}
      />
      <div className="deployment-context-grid">
        <div><span>Portfolio</span><strong>{context.portfolioName || "미확인"}</strong><small>{context.portfolioId || "검증된 Portfolio 연결 필요"}</small></div>
        <div><span>Strategy</span><strong>{context.name || "미선택"}</strong><small>{context.strategyId || "-"}</small></div>
        <div><span>Broker · 계정</span><strong>{context.brokerId || "미확인"}</strong><small>{context.accountId || "미확인"}</small></div>
        <div><span>실행 대상</span><strong>{context.symbol || "-"}</strong><small>{context.timeframe || "-"}</small></div>
      </div>
    </section>
  );
}

function compactHash(value = "") {
  const text = String(value || "").trim();
  if (!text) return "미생성";
  return text.length > 18 ? `${text.slice(0, 10)}…${text.slice(-6)}` : text;
}

function DeploymentManifestPanel({ governance = {} }) {
  const manifest = governance?.manifest || null;
  const preflight = governance?.latestPreflight || governance?.latest_preflight || null;
  const session = governance?.activeSession || governance?.active_session || null;
  const integrity = governance?.integrity || {};
  const integrityOk = integrity.ok === true || integrity.valid === true;
  return (
    <section className="panel deployment-manifest-panel">
      <PanelHeader
        title="Deployment Manifest · Runtime 고정값"
        subtitle="전략·Portfolio·계좌 fingerprint·Risk·Runtime 버전을 한 revision으로 봉인합니다. 변경 시 새 Manifest와 Preflight가 필요합니다."
        suffix={<StatusPill tone={manifest ? (integrityOk ? "success" : "danger") : "neutral"}>{manifest ? (integrityOk ? "무결성 확인" : "무결성 점검") : "Preflight 전"}</StatusPill>}
      />
      {manifest ? (
        <div className="deployment-manifest-grid">
          <div><span>Manifest</span><strong>rev {manifest.revision} · {compactHash(manifest.manifestHash)}</strong><small>{manifest.deploymentId}</small></div>
          <div><span>Artifact Hash</span><strong>{compactHash(manifest.portfolioArtifactHash || manifest.strategyArtifactHash)}</strong><small>Portfolio 우선 · Strategy 봉인</small></div>
          <div><span>Broker · 계좌</span><strong>{manifest.brokerRoute || "미확인"}</strong><small>fingerprint {compactHash(manifest.accountFingerprint)}</small></div>
          <div><span>Risk · Config</span><strong>R{manifest.riskPolicyRevision} · C{manifest.configRevision}</strong><small>{compactHash(manifest.riskPolicyHash)} · {compactHash(manifest.configHash)}</small></div>
          <div><span>Preflight Snapshot</span><strong>{preflight?.status || "미생성"}</strong><small>{preflight?.snapshotId || "현재 Deployment 점검 필요"}</small></div>
          <div><span>Runtime Session</span><strong>{session?.lifecycle || "중지"} · {session?.mode || "MONITOR"}</strong><small>{session?.sessionId || "세션 없음"}</small></div>
        </div>
      ) : <EmptyRow text="현재 Deployment의 Preflight를 실행하면 immutable Manifest가 생성됩니다." />}
    </section>
  );
}

function PreflightScopePanel({ snapshot = {}, onPreflight }) {
  const checks = snapshot.final_preflight ?? [];
  const deploymentLabels = new Set(["포지션·계좌 대조", "대조 증거 신선도", "전략 승인", "필수 운영 체크리스트", "운용자 확인", "신규 진입 차단"]);
  const deploymentChecks = checks.filter((check) => deploymentLabels.has(check.label));
  const globalChecks = checks.filter((check) => !deploymentLabels.has(check.label) && check.label !== "전략 승인");
  const blockedInventory = (snapshot.strategies ?? []).filter((strategy) => strategy.live_allowed !== true).length;
  const governance = snapshot.live_governance ?? {};
  const latest = governance.latestPreflight || governance.latest_preflight || {};
  const validity = governance.preflightValidity || governance.preflight_validity || {};
  const expiresAt = latest.expiresAt || latest.expires_at || "";
  const remainingSeconds = expiresAt ? Math.max(0, Math.floor((new Date(expiresAt).getTime() - Date.now()) / 1000)) : null;
  const snapshotValid = validity.valid === true
    && String(latest.status || latest.result || "").toUpperCase() === "PASS"
    && remainingSeconds > 0;
  const snapshotTone = snapshotValid ? "success" : latest.snapshotId ? "warning" : "neutral";
  return (
    <section className="panel preflight-scope-panel">
      <PanelHeader
        title="Preflight 범위·유효성"
        subtitle="전역 시스템, 현재 Deployment, 저장 Artifact 재고를 분리합니다. 재고 경고는 현재 배포의 Hard Stop으로 계산하지 않습니다."
        suffix={<StatusPill tone={snapshotTone}>{latest.snapshotId ? (snapshotValid ? `${remainingSeconds}초 남음` : "무효·만료") : "스냅샷 없음"}</StatusPill>}
      />
      <div className="preflight-scope-grid">
        <article>
          <header><strong>전역 시스템 검사</strong><StatusPill tone={globalChecks.some((item) => item.status === "fail") ? "danger" : "success"}>{globalChecks.filter((item) => item.status === "fail").length} HARD</StatusPill></header>
          {globalChecks.slice(0, 5).map((item) => <StatusRow key={item.label} label={item.label} status={item.status} detail={item.detail} />)}
          {!globalChecks.length && <EmptyRow text="전역 검사 결과가 없습니다." />}
        </article>
        <article>
          <header><strong>현재 Deployment 검사</strong><StatusPill tone={deploymentChecks.some((item) => item.status === "fail") ? "danger" : "success"}>{deploymentChecks.filter((item) => item.status === "fail").length} HARD</StatusPill></header>
          {deploymentChecks.slice(0, 5).map((item) => <StatusRow key={item.label} label={item.label} status={item.status} detail={item.detail} />)}
          {!deploymentChecks.length && <EmptyRow text="Deployment 검사 결과가 없습니다." />}
        </article>
        <article className="inventory-scope-card">
          <header><strong>재고·관리 경고</strong><StatusPill tone={blockedInventory ? "warning" : "neutral"}>{blockedInventory}건</StatusPill></header>
          <p>승급 불가 저장 Artifact {blockedInventory}개</p>
          <span>현재 선택한 Deployment와 무관한 과거 Artifact는 관리 경고로만 표시합니다.</span>
        </article>
      </div>
      <div className="panel-action-line">
        <span>{latest.snapshotId ? `Snapshot ${latest.snapshotId}` : "실거래 테스트: 운용자 확인 → Dry Run 해제 → 신규 진입 허용 → 새 Preflight → Canary"}</span>
        <button className="primary-button" type="button" onClick={onPreflight}><BadgeCheck size={15} />새 Preflight 실행</button>
      </div>
    </section>
  );
}

function OperationsOverviewPage({ context, snapshot, onNavigate, onReconcile, onPreflight, onWatchdog }) {
  return (
    <section className="operations-overview-layout ts-layout-stack">
      <DeploymentContextPanel context={context} />
      <PreflightScopePanel snapshot={snapshot} onPreflight={onPreflight} />
      <section className="content-grid operations-overview-grid ts-panel-grid ts-panel-grid--two">
        <div className="content-column ts-scroll-panel">
          <PreTradeDoctorPanel
            snapshot={snapshot}
            onNavigate={onNavigate}
            onReconcile={onReconcile}
            onPreflight={onPreflight}
            onWatchdog={onWatchdog}
          />
        </div>
        <div className="content-column ts-scroll-panel">
          <LaunchReportPanel report={snapshot.launch_report ?? {}} />
          {snapshot.operation_report?.sections && <OperationsReportPanel report={snapshot.operation_report} />}
          <RuntimeComponentStatusPanel snapshot={snapshot} />
          <OrderQueueSummaryPanel summary={snapshot.order_queue ?? {}} />
        </div>
      </section>
    </section>
  );
}

function RuntimeComponentStatusPanel({ snapshot = {} }) {
  const runtime = snapshot.continuous_runtime ?? {};
  const profiles = Object.values(runtime.profiles ?? {});
  const runtimeRunning = profiles.some((profile) => profile?.running === true) || runtime.running === true;
  const activeLive = ["SMALL_LIVE", "FULL_LIVE"].includes(String(snapshot.mode || "").toUpperCase());
  const streams = snapshot.execution_streams?.brokers ?? {};
  const streamRows = Object.values(streams);
  const connectedStreams = streamRows.filter((item) => item?.running && item?.connected).length;
  const orderRoute = activeLive && !snapshot.kill_switch && !snapshot.dry_run;
  const rows = [
    { label: "시장 데이터", status: runtimeRunning ? "pass" : activeLive ? "fail" : "na", value: runtimeRunning ? "RUNNING" : activeLive ? "STOPPED" : "해당 없음", detail: runtimeRunning ? "선택 배포의 feed heartbeat를 감시합니다." : "MONITOR 대기 중에는 활성 feed 경로가 없습니다." },
    { label: "Bar Builder", status: runtimeRunning ? "pass" : activeLive ? "fail" : "na", value: runtimeRunning ? "RUNNING" : activeLive ? "STOPPED" : "해당 없음", detail: "완료 봉 경계와 중복 bar 처리를 런타임별로 추적합니다." },
    { label: "전략 평가", status: runtimeRunning ? "pass" : "na", value: runtimeRunning ? "RUNNING" : "대기", detail: `마지막 평가 ${snapshot.strategy_runner?.last_run || "미실행"}` },
    { label: "Portfolio Engine", status: runtimeRunning ? "pass" : "na", value: runtimeRunning ? "RUNNING" : "대기", detail: "전략 Sleeve를 계좌 Net Target으로 합산합니다." },
    { label: "Risk Gateway", status: snapshot.api_connected ? "pass" : "fail", value: snapshot.api_connected ? "ENFORCING" : "확인 불가", detail: "주문 전 최종 위험 증가·Reduce-only 정책을 적용합니다." },
    { label: "주문 전송", status: orderRoute ? "pass" : "na", value: orderRoute ? "ENABLED" : "BLOCKED", detail: orderRoute ? "LIVE route가 활성화되어 있습니다." : "전송 차단 상태에서도 시세·전략·대조는 독립적으로 동작할 수 있습니다." },
    { label: "Broker Event", status: connectedStreams ? "pass" : activeLive ? "fail" : "na", value: connectedStreams ? `${connectedStreams} CONNECTED` : activeLive ? "DISCONNECTED" : "해당 없음", detail: "사설 주문·체결 이벤트와 REST Snapshot을 함께 대조합니다." },
  ];
  return (
    <section className="panel runtime-component-panel">
      <PanelHeader title="Runtime 구성 요소" subtitle="단일 STOPPED 대신 데이터·전략·리스크·주문·이벤트 상태를 각각 표시합니다." />
      <div className="runtime-component-list">
        {rows.map((row) => <StatusRow key={row.label} label={row.label} status={row.status} value={row.value} detail={row.detail} />)}
      </div>
    </section>
  );
}

function ThreeWayReconciliationPanel({ snapshot = {} }) {
  const summary = snapshot.reconciliation?.summary ?? {};
  const execution = snapshot.execution_events ?? {};
  const ledger = snapshot.program_ledger ?? {};
  const streams = execution.streams?.brokers ?? snapshot.execution_streams?.brokers ?? {};
  const streamConnected = Object.values(streams).some((item) => item?.connected === true);
  const brokerKnown = Number(summary.api_required_count || 0) === 0 && Number(summary.error_count || 0) === 0;
  const ledgerKnown = Number(ledger.cash_count || ledger.cashCount || (ledger.cash || []).length || 0) > 0
    || Number(ledger.position_count || ledger.positionCount || (ledger.positions || []).length || 0) > 0;
  const projected = projectAccountReconciliation(snapshot);
  const rows = [
    { label: "Broker REST Snapshot", status: brokerKnown ? "pass" : "warn", value: brokerKnown ? "조회됨" : "미확인", detail: `마지막 대조 ${summary.last_run || "미실행"}` },
    { label: "실시간 주문·체결 Event", status: streamConnected ? "pass" : execution.last_poll ? "warn" : "na", value: streamConnected ? "연결" : execution.last_poll ? "폴링 보조" : "해당 없음", detail: `마지막 동기화 ${execution.last_poll || "미실행"}` },
    { label: "Local Event Ledger", status: ledgerKnown ? "pass" : "warn", value: ledgerKnown ? "원장 있음" : "기준 없음", detail: `체결 이벤트 ${(ledger.execution_events || []).length}건 · 기준 ${ledger.state?.last_baseline || "미승인"}` },
    { label: "3자 최종 대조", status: summary.status || "warn", value: summary.status_label || "미확인", detail: `불일치 ${summary.mismatch_count || 0} · API/원장 필요 ${summary.api_required_count || 0}` },
  ];
  return (
    <section className="panel three-way-reconciliation-panel">
      <PanelHeader title="계좌·포지션 3자 대조" subtitle={`조회 실패는 0으로 간주하지 않습니다. 확인된 계좌 ${projected.knownAccountCount}개 · 미확인 ${projected.unknownAccountCount}개`} />
      <div className="three-way-flow">
        {rows.map((row, index) => (
          <React.Fragment key={row.label}>
            <StatusCard title={row.label} value={row.value} detail={row.detail} tone={statusTone(row.status)} />
            {index < rows.length - 1 && <span>→</span>}
          </React.Fragment>
        ))}
      </div>
    </section>
  );
}

function orderStateLabel(value) {
  const key = String(value || "").toUpperCase();
  const labels = {
    CREATED: "생성됨", RISK_CHECKING: "위험 점검", RISK_REJECTED: "위험 차단", SUBMITTING: "전송 중",
    UNKNOWN: "접수 결과 불확실", UNKNOWN_SUBMIT_RESULT: "접수 결과 불확실", ACKNOWLEDGED: "접수됨",
    PARTIALLY_FILLED: "부분 체결", FILLED: "완전 체결", CANCEL_PENDING: "취소 확인 중", CANCELED: "취소됨",
    UNKNOWN_CANCEL_RESULT: "취소 결과 불확실",
    REJECTED: "거부됨", EXPIRED: "만료", FAILED: "실패", RISK_BLOCKED: "위험 차단", DRY_RUN: "Dry Run", SENT: "전송됨",
  };
  return labels[key] || key || "미확인";
}

function orderStateTone(value) {
  const key = String(value || "").toUpperCase();
  if (["FILLED", "ACKNOWLEDGED", "SENT", "DRY_RUN"].includes(key)) return "success";
  if (["UNKNOWN", "UNKNOWN_SUBMIT_RESULT", "UNKNOWN_CANCEL_RESULT", "PARTIALLY_FILLED", "SUBMITTING", "CANCEL_PENDING"].includes(key)) return "warning";
  if (["RISK_BLOCKED", "RISK_REJECTED", "REJECTED", "FAILED"].includes(key)) return "danger";
  return "neutral";
}

function ExecutionQualitySummary({ quality }) {
  return (
    <MetricGrid columns={3}>
      <MetricCard label="표본" value={String(quality.sampleCount) + "건"} detail={quality.submitted ? "Broker 제출 " + quality.submitted + "건" : "Paper/Shadow/Live 표본 필요"} />
      <MetricCard label="평균 Slippage" value={quality.averageSlippageBps == null ? "해당 없음" : quality.averageSlippageBps.toFixed(1) + " bps"} detail="기대 가격 대비" />
      <MetricCard label="평균 Broker 지연" value={quality.averageLatencyMs == null ? "해당 없음" : quality.averageLatencyMs.toFixed(0) + " ms"} detail={"불확실 주문 " + quality.unknownSubmitResult + "건"} />
    </MetricGrid>
  );
}

function OrderExecutionWorkspace({ context = {}, snapshot = {}, onRetryOrder, onCancelOrder }) {
  const orders = snapshot.orders ?? [];
  const events = snapshot.execution_events?.recent ?? snapshot.program_ledger?.execution_events ?? [];
  const [query, setQuery] = useState("");
  const [stateFilter, setStateFilter] = useState("all");
  const [brokerFilter, setBrokerFilter] = useState("all");
  const [selectedOrderKey, setSelectedOrderKey] = useState("");
  const calibration = snapshot.execution_calibration ?? {};
  const quality = projectExecutionQuality(orders, events, calibration);
  const orderRows = useMemo(
    () => orders.map((order, index) => ({
      key: String(
        order.order_id
        || order.client_order_id
        || order.idempotency_key
        || order.broker_order_id
        || "order-" + index,
      ),
      order,
    })),
    [orders],
  );
  const stateOptions = useMemo(
    () => [...new Set(orders.map((order) => String(order.state || "").toUpperCase()).filter(Boolean))].sort(),
    [orders],
  );
  const brokerOptions = useMemo(
    () => [...new Set(orders.map((order) => String(order.broker_id || "")).filter(Boolean))].sort(),
    [orders],
  );
  const visibleOrderRows = useMemo(() => {
    const needle = query.trim().toLocaleLowerCase();
    return orderRows.filter(({ order }) => {
      const state = String(order.state || "").toUpperCase();
      const broker = String(order.broker_id || "");
      if (stateFilter !== "all" && state !== stateFilter) return false;
      if (brokerFilter !== "all" && broker !== brokerFilter) return false;
      if (!needle) return true;
      return [
        order.order_id,
        order.client_order_id,
        order.idempotency_key,
        order.broker_order_id,
        order.deployment_id || context.id,
        order.symbol,
        order.state,
        orderStateLabel(order.state),
      ].some((value) => String(value || "").toLocaleLowerCase().includes(needle));
    });
  }, [brokerFilter, context.id, orderRows, query, stateFilter]);
  const selectedRow = visibleOrderRows.find((row) => row.key === selectedOrderKey) || visibleOrderRows[0] || null;
  const selected = selectedRow?.order || null;
  const selectedEvents = useMemo(() => {
    if (!selected) return [];
    const selectedIds = new Set(
      [selected.order_id, selected.broker_order_id, selected.client_order_id]
        .map((value) => String(value || ""))
        .filter(Boolean),
    );
    return events.filter((event) => (
      [event.order_id, event.broker_order_id, event.client_order_id]
        .map((value) => String(value || ""))
        .some((value) => value && selectedIds.has(value))
    ));
  }, [events, selected]);
  const handleExportOrders = () => {
    const exportRows = buildOrderCsvRows(visibleOrderRows, context.id, formatAuditTime);
    downloadCsv(ORDER_CSV_COLUMNS, exportRows, `live-trader-orders-${dateStamp()}.csv`);
  };
  return (
    <section className="order-execution-layout ts-layout-stack">
      <DeploymentContextPanel context={context} />
      <OrderQueueSummaryPanel summary={snapshot.order_queue ?? {}} />
      <section className="panel order-ledger-panel">
        <PanelHeader title="주문 상태 원장" subtitle="불확실한 주문 결과는 실패와 분리하며, 같은 Client Order ID의 존재를 대조하기 전 재전송하지 않습니다." />
        <MasterDetailLog
          className="order-ledger-workspace"
          classes={{
            detail: "order-workspace-detail",
            detailPane: "order-ledger-detail-pane",
            list: "table-scroll order-ledger-table-wrap",
          }}
          detailAriaLabel="선택한 주문 상세"
          detailHeader={selected ? (
            <span>{selected.order_id || selected.client_order_id || "주문"} · {selected.symbol || "-"}</span>
          ) : "주문 상세"}
          emptyDetail={(
            <div className="order-empty-detail">
              <section className="order-detail-section order-timeline-section">
                <header><h4>주문 타임라인</h4><span>0건</span></header>
                <EmptyRow text="상세 타임라인을 볼 주문이 없습니다." />
              </section>
              <section className="order-detail-section fill-ledger-panel">
                <header><h4>체결 원장</h4><span>{events.length}건</span></header>
                <div className="compact-list">
                  {events.length ? events.slice(0, 30).map((event, index) => (
                    <div className="compact-row" key={event.event_id || "unmatched-event-" + index}>
                      <strong>{event.symbol || "-"} · {event.side || "-"}</strong>
                      <span>{event.quantity ?? "-"} @ {event.price ?? "-"} · {formatAuditTime({ timestamp: event.occurred_at })}</span>
                      <StatusPill tone={orderStateTone(event.state)}>{orderStateLabel(event.state)}</StatusPill>
                    </div>
                  )) : <EmptyRow text="기록된 체결 Event가 없습니다." />}
                </div>
              </section>
              <section className="order-detail-section execution-quality-panel">
                <header><h4>실행 품질</h4><span>전체 주문 기준</span></header>
                <ExecutionQualitySummary quality={quality} />
              </section>
            </div>
          )}
          emptyList={(
            <table aria-label="주문 상태 원장 목록" className="data-table order-ledger-table" role="grid">
              <thead><tr><th>시각</th><th>Broker</th><th>주문 / <span>Client Order ID</span></th><th>Deployment</th><th>심볼</th><th>상태</th></tr></thead>
              <tbody><tr><td colSpan="6"><EmptyRow text="검색 조건에 맞는 주문이 없습니다." /></td></tr></tbody>
            </table>
          )}
          getItemKey={(row) => row.key}
          itemRole="row"
          items={visibleOrderRows}
          listAriaLabel="주문 상태 원장 목록"
          onSelectedKeyChange={(key) => setSelectedOrderKey(String(key))}
          renderDetail={({ order }) => {
            const timelineProjection = projectOrderTimeline(order, events);
            const orderEvents = selectedEvents;
            return (
              <>
                <section className="order-detail-summary" aria-label="주문 식별 정보">
                  <dl>
                    <div><dt>Broker</dt><dd>{order.broker_id || "-"}</dd></div>
                    <div><dt>Order ID</dt><dd>{order.order_id || "-"}</dd></div>
                    <div><dt>Client Order ID</dt><dd>{order.client_order_id || order.idempotency_key || "-"}</dd></div>
                    <div><dt>Deployment</dt><dd>{order.deployment_id || context.id || "-"}</dd></div>
                    <div><dt>주문</dt><dd>{order.symbol || "-"} · {order.side || "-"} · {order.quantity ?? order.qty ?? "-"}</dd></div>
                    <div><dt>체결</dt><dd>{order.executed_quantity ?? order.executed_volume ?? "-"}</dd></div>
                    <div><dt>상태</dt><dd><StatusPill tone={orderStateTone(order.state)}>{orderStateLabel(order.state)}</StatusPill></dd></div>
                    <div><dt>Risk</dt><dd>{order.risk_report?.can_submit === true ? "승인" : order.risk_report?.can_submit === false ? "차단" : "미확인"}</dd></div>
                  </dl>
                </section>
                <section className="order-detail-section order-timeline-section">
                  <header><h4>주문 타임라인</h4><span>{timelineProjection.timeline.length}건</span></header>
                  <div className="order-timeline">
                    {timelineProjection.timeline.map((event) => <div key={event.id}><span>{formatAuditTime({ timestamp: event.time })}</span><strong>{event.label}</strong><small>{event.detail || event.source || "상태 기록"}</small></div>)}
                    {!timelineProjection.timeline.length && <div><span>-</span><strong>Broker Event 대기</strong><small>체결 스트림 또는 REST 대조 결과가 아직 없습니다.</small></div>}
                  </div>
                  <div className="operator-actions">
                    <button className="secondary-button" type="button" disabled={!order.order_id || !timelineProjection.directRetryAllowed} onClick={() => onRetryOrder(order.order_id)}>상태 대조 후 재시도</button>
                    <button className="secondary-button" type="button" disabled={!order.order_id || Boolean(order.cancel_request_id) || ["filled", "canceled"].includes(String(order.state || "").toLowerCase())} onClick={() => onCancelOrder(order.order_id)}>{order.cancel_request_id ? "취소 대조 중" : "취소 요청"}</button>
                  </div>
                </section>
                <section className="order-detail-section fill-ledger-panel">
                  <header><h4>체결 원장</h4><span>{orderEvents.length}건</span></header>
                  <div className="compact-list">
                    {orderEvents.length ? orderEvents.slice(0, 30).map((event, index) => (
                      <div className="compact-row" key={event.event_id || "event-" + index}>
                        <strong>{event.symbol || "-"} · {event.side || "-"}</strong>
                        <span>{event.quantity ?? "-"} @ {event.price ?? "-"} · {formatAuditTime({ timestamp: event.occurred_at })}</span>
                        <StatusPill tone={orderStateTone(event.state)}>{orderStateLabel(event.state)}</StatusPill>
                      </div>
                    )) : <EmptyRow text="선택 주문에 연결된 체결 Event가 없습니다." />}
                  </div>
                </section>
                <section className="order-detail-section execution-quality-panel">
                  <header><h4>실행 품질</h4><span>전체 주문 기준</span></header>
                  <ExecutionQualitySummary quality={quality} />
                </section>
              </>
            );
          }}
          renderList={({ items, selectedKey, getItemProps }) => (
            <table aria-label="주문 상태 원장 목록" className="data-table order-ledger-table" role="grid">
              <thead><tr><th>시각</th><th>Broker</th><th>주문 / <span>Client Order ID</span></th><th>Deployment</th><th>심볼</th><th>상태</th></tr></thead>
              <tbody>
                {items.map((row, index) => (
                  <tr {...getItemProps(row, index, { className: selectedKey === row.key ? "is-selected" : "" })} key={row.key}>
                    <td>{formatAuditTime(row.order)}</td>
                    <td>{row.order.broker_id || "-"}</td>
                    <td><strong>{row.order.order_id || "-"}</strong><small>{row.order.client_order_id || row.order.idempotency_key || "-"}</small></td>
                    <td>{row.order.deployment_id || context.id || "-"}</td>
                    <td>{row.order.symbol || "-"}</td>
                    <td><StatusPill tone={orderStateTone(row.order.state)}>{orderStateLabel(row.order.state)}</StatusPill></td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
          selectedKey={selectedOrderKey}
          toolbar={(
            <div className="order-ledger-toolbar">
              <label className="search-box order-ledger-search">
                <input
                  aria-label="주문 검색"
                  value={query}
                  onChange={(event) => setQuery(event.currentTarget.value)}
                  placeholder="주문/Client Order ID, deployment, 심볼, 상태 검색"
                />
                <Search size={18} />
              </label>
              <select aria-label="주문 상태 필터" value={stateFilter} onChange={(event) => setStateFilter(event.currentTarget.value)}>
                <option value="all">전체 상태</option>
                {stateOptions.map((state) => <option value={state} key={state}>{orderStateLabel(state)}</option>)}
              </select>
              <select aria-label="브로커 필터" value={brokerFilter} onChange={(event) => setBrokerFilter(event.currentTarget.value)}>
                <option value="all">전체 브로커</option>
                {brokerOptions.map((broker) => <option value={broker} key={broker}>{broker}</option>)}
              </select>
              <button className="logs-export-button" type="button" onClick={handleExportOrders} disabled={!visibleOrderRows.length}>
                <Download size={16} />
                CSV
              </button>
              <span>{visibleOrderRows.length.toLocaleString()} / {orders.length.toLocaleString()}건</span>
            </div>
          )}
          toolbarAriaLabel="주문 검색 및 필터"
        />
      </section>
    </section>
  );
}

function RiskUsagePanel({ snapshot = {}, context = {} }) {
  const settings = Object.fromEntries((snapshot.risk_settings ?? []).map((item) => [item.key || item.name, item]));
  const openLimit = Number(settings.max_open_orders?.value ?? settings.max_open_orders?.current ?? 0);
  const openOrders = Number(snapshot.order_queue?.active || 0);
  const exposureLimit = Number(settings.max_symbol_exposure_pct?.value ?? settings.max_symbol_exposure_pct?.current ?? 0);
  const symbolPosition = (snapshot.positions ?? []).find((item) => item.symbol === context.symbol && item.broker_value != null);
  const account = (snapshot.accounts ?? []).find((item) => item.broker_id === symbolPosition?.broker_id && item.broker_equity_value != null);
  const exposure = symbolPosition && account && Number(account.broker_equity_value) > 0
    ? Math.abs(Number(symbolPosition.broker_value || 0)) / Number(account.broker_equity_value) * 100
    : null;
  const rows = [
    { label: "일일 손실", current: snapshot.account_risk?.daily_loss_pct, warning: snapshot.account_risk?.warning_loss_pct, hard: settings.daily_loss_limit_pct?.value, unit: "%" },
    { label: `${context.symbol || "선택 심볼"} 노출`, current: exposure, warning: exposureLimit ? exposureLimit * 0.8 : null, hard: exposureLimit || null, unit: "%" },
    { label: "미체결·대기 주문", current: openOrders, warning: openLimit ? Math.max(1, Math.floor(openLimit * 0.8)) : null, hard: openLimit || null, unit: "건" },
  ];
  return (
    <section className="panel risk-usage-panel">
      <PanelHeader title="현재 리스크 사용량" subtitle="설정값만 보여주지 않고 현재·주의·Hard Block을 함께 표시합니다. 관측값이 없으면 0이 아닌 미확인입니다." />
      <div className="risk-usage-list">
        {rows.map((row) => {
          const known = row.current !== null && row.current !== undefined && Number.isFinite(Number(row.current));
          const hard = row.hard !== null && row.hard !== undefined ? Number(row.hard) : null;
          const used = known && hard ? Math.min(100, Math.abs(Number(row.current) / hard) * 100) : 0;
          return <article key={row.label}><header><strong>{row.label}</strong><span>{known ? `${Number(row.current).toFixed(2)}${row.unit}` : "미확인"} / {hard !== null ? `${hard}${row.unit}` : "정책 없음"}</span></header><div className="risk-usage-track"><span style={{ width: `${used}%` }} /></div><small>주의 {row.warning != null ? `${row.warning}${row.unit}` : "미설정"} · Hard Block {hard != null ? `${hard}${row.unit}` : "미설정"}</small></article>;
        })}
      </div>
    </section>
  );
}

function RetryDecisionMatrixPanel({ matrix }) {
  const rows = Array.isArray(matrix) && matrix.length ? matrix : buildRetryMatrix();
  const legacyRows = rows[0]?.request_kind
    ? rows.map((row) => ({ request: row.request_kind === "read" ? "조회 API" : "주문 POST", result: row.outcome, action: row.next_action, automatic: row.automatic_retry === true }))
    : rows[0]?.request || rows[0]?.request_type
      ? rows
      : rows.map((row) => ({ request: row.label, result: row.directRetryAllowed ? "접수 전 확정" : "접수 여부 불확실/거부", action: row.nextAction, automatic: row.autoRetry }));
  /* Backend policy rows remain authoritative when provided; the shared model
     is the safe fallback used while the API is disconnected. */
  const displayRows = legacyRows.length ? legacyRows : [
    { request: "시세·잔고·공개 메타데이터 조회", result: "일시 오류·429", action: "지연 후 자동 재조회", automatic: true },
    { request: "주문 접수 전 로컬 검증", result: "명확한 로컬 실패", action: "원인 수정 후 새 Intent", automatic: false },
    { request: "주문 제출", result: "접수 결과 불확실", action: "재전송 금지 · Client Order ID로 먼저 대조", automatic: false },
    { request: "주문 취소", result: "취소 결과 불확실", action: "주문 상태 조회 후 결정", automatic: false },
    { request: "Broker 주문", result: "명시적 거부", action: "원인 수정 전 재전송 금지", automatic: false },
  ];
  return (
    <section className="panel retry-matrix-panel">
      <PanelHeader title="요청별 재시도 원칙" subtitle="조회 재시도와 주문 POST 재전송을 분리합니다." />
      <div className="table-scroll ts-scroll-panel"><table className="data-table"><thead><tr><th>요청</th><th>결과</th><th>동작</th><th>자동</th></tr></thead><tbody>{displayRows.map((row, index) => <tr key={`${row.request || row.request_type}-${index}`}><td>{row.request || row.request_type}</td><td>{row.result || row.outcome}</td><td>{row.action || row.policy}</td><td><StatusPill tone={row.automatic ? "warning" : "neutral"}>{row.automatic ? "허용" : "금지"}</StatusPill></td></tr>)}</tbody></table></div>
    </section>
  );
}

function IncidentAuditWorkspace({ snapshot = {} }) {
  const audit = snapshot.durable_audit ?? snapshot.audit_events ?? snapshot.audit ?? [];
  return (
    <section className="incident-audit-layout ts-layout-stack">
      <AuditPanel
        audit={audit}
        detailLabel="감사 기록 상세"
        emptyText="표시할 감사 이벤트가 없습니다."
        subtitle="잠금·배포·Preflight·모드·Risk·주문·Kill·Secret 변경을 append-only 원장에서 검색합니다."
        title="감사 이벤트"
      />
    </section>
  );
}

function DoctorHistoryPanel({ diagnostics = {}, onNavigate }) {
  const latest = diagnostics?.latest ?? {};
  const issues = latest.issues ?? [];
  return (
    <section className="panel doctor-history-panel">
      <PanelHeader title="설정·Runtime 자체 검사" subtitle={`최근 진단 ${latest.generated_at || "미실행"}`} suffix={<StatusPill tone={latest.summary?.hard_stop_count ? "danger" : latest.summary?.warning_count ? "warning" : latest.run_id ? "success" : "neutral"}>{latest.summary?.status || "미실행"}</StatusPill>} />
      <div className="compact-list">
        {issues.slice(0, 12).map((issue) => <button className="compact-row doctor-history-row" type="button" key={issue.issue_code} onClick={() => onNavigate(issue.related_tab || "overview")}><strong>{issue.problem}</strong><span>{issue.remediation}</span><StatusPill tone={issue.severity === "hard_stop" ? "danger" : "warning"}>{issue.severity === "hard_stop" ? "차단" : "주의"}</StatusPill></button>)}
        {!issues.length && <EmptyRow text="저장된 진단 이슈가 없습니다." />}
      </div>
    </section>
  );
}

function LivePreparationPanel({
  snapshot,
  deploymentOnly = false,
  selectedStrategyId: controlledSelectedStrategyId,
  onSelectedStrategyIdChange,
  onConfirm,
  onDryRun,
  onEntryBlock,
  onTestIntent,
  onRiskSetting,
  onRetryPolicy,
  onPromoteLive,
  onStrategyLifecycle,
}) {
  const [assetTab, setAssetTab] = useState("stock");
  const [internalSelectedStrategyId, setInternalSelectedStrategyId] = useState("");
  const selectedStrategyId = controlledSelectedStrategyId ?? internalSelectedStrategyId;
  const [discoveryFilters, setDiscoveryFilters] = useState(DEFAULT_STRATEGY_DISCOVERY_FILTERS);
  const [savedSearches, setSavedSearches] = useState(() => {
    const stored = readStoredValue(STRATEGY_SAVED_SEARCHES_KEY, []);
    return normalizeStrategySearchPresetDocument(stored, "live_trader").presets;
  });
  const [savedSearchId, setSavedSearchId] = useState("");
  const [savedSearchName, setSavedSearchName] = useState("");
  const [artifactMetadata, setArtifactMetadata] = useState({});
  const initialSavedSearches = useRef(savedSearches);
  const isStock = assetTab === "stock";
  const assetStrategies = useMemo(
    () => (snapshot.strategies ?? []).filter((strategy) => (isStock ? !isCryptoStrategy(strategy) : isCryptoStrategy(strategy))),
    [isStock, snapshot.strategies],
  );
  const stageOptions = useMemo(
    () => uniqueStrategyDiscoveryValues(assetStrategies.map(liveStrategyStageId)),
    [assetStrategies],
  );
  const timeframeOptions = useMemo(
    () => uniqueStrategyDiscoveryValues(assetStrategies.map((strategy) => strategy.timeframe)),
    [assetStrategies],
  );
  const pluginOptions = useMemo(
    () => uniqueStrategyDiscoveryValues(assetStrategies.map((strategy) => strategy.plugin_label || strategy.plugin)),
    [assetStrategies],
  );
  const failureOptions = useMemo(
    () => uniqueStrategyDiscoveryValues(assetStrategies.flatMap((strategy) => liveArtifactFailureReasons(strategy))),
    [assetStrategies],
  );
  const filteredStrategies = useMemo(
    () => sortLiveStrategies(
      assetStrategies.filter((strategy) => liveStrategyMatchesDiscovery(
        strategy,
        discoveryFilters,
        artifactMetadata[artifactMetadataKey(strategy.strategy_id, "strategy")],
        liveArtifactRunning(snapshot.continuous_runtime, strategy.strategy_id),
      )),
      discoveryFilters.sort,
    ),
    [artifactMetadata, assetStrategies, discoveryFilters, snapshot.continuous_runtime],
  );
  const controlledSelectedStrategy = controlledSelectedStrategyId
    ? (snapshot.strategies ?? []).find((strategy) => strategy.strategy_id === controlledSelectedStrategyId) ?? null
    : null;
  const controlledAssetTab = controlledSelectedStrategy
    ? (isCryptoStrategy(controlledSelectedStrategy) ? "crypto" : "stock")
    : "";
  const selectedStrategy = controlledSelectedStrategy
    ?? filteredStrategies.find((strategy) => strategy.strategy_id === selectedStrategyId)
    ?? filteredStrategies[0]
    ?? null;
  const tabItems = [
    { id: "stock", label: "주식/ETF", detail: "한국투자증권 KIS", count: (snapshot.strategies ?? []).filter((strategy) => !isCryptoStrategy(strategy)).length },
    { id: "crypto", label: "코인", detail: "Binance / Upbit", count: (snapshot.strategies ?? []).filter(isCryptoStrategy).length },
  ];
  const strategySearchPresets = savedSearches.filter((item) => item.entity === "strategy");

  function selectStrategyId(strategyId) {
    setInternalSelectedStrategyId(strategyId);
    onSelectedStrategyIdChange?.(strategyId);
  }

  useEffect(() => {
    if (controlledAssetTab) setAssetTab(controlledAssetTab);
  }, [controlledAssetTab, controlledSelectedStrategyId]);

  useEffect(() => {
    // A controlled Deployment is the source of truth. Asset tabs and discovery
    // filters may temporarily hide it, but must never replace it implicitly.
    // The operator can still change Deployment explicitly from the selector or
    // by choosing a strategy row.
    if (controlledSelectedStrategyId) return;
    const firstId = filteredStrategies[0]?.strategy_id ?? "";
    const stillVisible = filteredStrategies.some((strategy) => strategy.strategy_id === selectedStrategyId);
    if (!stillVisible && selectedStrategyId !== firstId) {
      selectStrategyId(firstId);
    }
  }, [controlledSelectedStrategyId, filteredStrategies, selectedStrategyId]);

  useEffect(() => {
    let active = true;
    loadSharedSearchPresets()
      .then((document) => {
        if (!active) return;
        const shared = normalizeStrategySearchPresetDocument(document, "shared").presets;
        const merged = mergeStrategySearchPresets(shared, initialSavedSearches.current);
        setSavedSearches(merged);
        window.localStorage.setItem(STRATEGY_SAVED_SEARCHES_KEY, JSON.stringify(merged));
        if (merged.length !== shared.length) {
          return saveSharedSearchPresets(merged);
        }
        return null;
      })
      .catch(() => undefined);
    return () => {
      active = false;
    };
  }, []);
  useEffect(() => {
    let active = true;
    loadArtifactMetadata()
      .then((document) => {
        if (active && document?.items) setArtifactMetadata(document.items);
      })
      .catch(() => undefined);
    return () => {
      active = false;
    };
  }, []);

  async function saveArtifactMetadata(artifactId, artifactType, changes) {
    const response = await updateArtifactMetadata(artifactId, artifactType, changes);
    if (response?.document?.items) setArtifactMetadata(response.document.items);
  }

  function updateDiscoveryFilter(key, value) {
    setDiscoveryFilters((current) => ({ ...current, [key]: value }));
    setSavedSearchId("");
  }

  function changeAssetTab(value) {
    setAssetTab(value);
    setDiscoveryFilters(DEFAULT_STRATEGY_DISCOVERY_FILTERS);
    setSavedSearchId("");
  }

  function saveCurrentSearch() {
    const name = savedSearchName.trim();
    if (!name) return;
    const existing = savedSearches.find((item) => item.entity === "strategy" && String(item.name).toLocaleLowerCase() === name.toLocaleLowerCase());
    const saved = createStrategySearchPreset({
      id: existing?.id,
      name,
      entity: "strategy",
      sourceApp: "live_trader",
      assetTab,
      filters: {
        query: discoveryFilters.query,
        lifecycle: discoveryFilters.stage,
        asset: discoveryFilters.asset || "all",
        timeframe: discoveryFilters.timeframe,
        strategyType: discoveryFilters.plugin,
        sort: discoveryFilters.sort,
      },
    });
    saved.filters.failure = discoveryFilters.failure;
    saved.filters.quick = discoveryFilters.quick;
    const next = mergeStrategySearchPresets(savedSearches.filter((item) => item.id !== existing?.id), saved);
    setSavedSearches(next);
    setSavedSearchId(saved.id);
    window.localStorage.setItem(STRATEGY_SAVED_SEARCHES_KEY, JSON.stringify(next.slice(-30)));
    saveSharedSearchPresets(next).catch(() => undefined);
  }

  function applySavedSearch(id) {
    setSavedSearchId(id);
    const saved = savedSearches.find((item) => item.id === id);
    if (!saved) return;
    const assetValue = String(saved.filters?.asset || "").toLocaleLowerCase();
    setAssetTab(saved.context?.assetTab || (/(crypto|coin|코인)/.test(assetValue) ? "crypto" : "stock"));
    setDiscoveryFilters({
      ...DEFAULT_STRATEGY_DISCOVERY_FILTERS,
      query: saved.filters?.query ?? "",
      stage: saved.filters?.lifecycle ?? "all",
      timeframe: saved.filters?.timeframe ?? "all",
      plugin: saved.filters?.strategyType ?? "all",
      failure: saved.filters?.failure ?? "all",
      quick: saved.filters?.quick ?? "all",
      sort: saved.filters?.sort ?? "updated-desc",
    });
    setSavedSearchName(saved.name);
  }

  function deleteCurrentSavedSearch() {
    if (!savedSearchId) return;
    const next = savedSearches.filter((item) => item.id !== savedSearchId);
    setSavedSearches(next);
    setSavedSearchId("");
    window.localStorage.setItem(STRATEGY_SAVED_SEARCHES_KEY, JSON.stringify(next));
    saveSharedSearchPresets(next).catch(() => undefined);
  }

  return (
    <section className="live-prep-shell">
      <NestedTabs
        ariaLabel="실거래 준비 자산군"
        className="internal-tabs prep-tabs"
        onChange={changeAssetTab}
        options={tabItems.map((item) => ({ id: item.id, label: item.label, detail: `전략 ${item.count}개`, title: item.detail }))}
        variant="cards"
        value={assetTab}
      />
      <section className="content-grid ts-scroll-panel">
        <div className="content-column">
          <StrategyDiscoveryToolbar
            filters={discoveryFilters}
            onFilterChange={updateDiscoveryFilter}
            stageOptions={stageOptions}
            timeframeOptions={timeframeOptions}
            pluginOptions={pluginOptions}
            failureOptions={failureOptions}
            visibleCount={filteredStrategies.length}
            totalCount={assetStrategies.length}
            savedSearches={strategySearchPresets}
            savedSearchId={savedSearchId}
            savedSearchName={savedSearchName}
            onSavedSearchNameChange={setSavedSearchName}
            onSavedSearchApply={applySavedSearch}
            onSavedSearchSave={saveCurrentSearch}
            onSavedSearchDelete={deleteCurrentSavedSearch}
            onReset={() => {
              setDiscoveryFilters(DEFAULT_STRATEGY_DISCOVERY_FILTERS);
              setSavedSearchId("");
            }}
          />
          <LivePromotionReadinessQueue
            onSelect={(strategyId) => {
              selectStrategyId(strategyId);
              saveArtifactMetadata(strategyId, "strategy", { markUsed: true }).catch(() => undefined);
            }}
            operatorConfirmed={Boolean(snapshot.operator_confirmed)}
            orders={snapshot.orders ?? []}
            runtime={snapshot.continuous_runtime}
            strategies={filteredStrategies}
            summary={snapshot.summary ?? {}}
          />
          <LineageFlowPanel
            snapshot={snapshot.lineage_flow}
            selectedStrategyId={selectedStrategy?.strategy_id}
          />
          <LiveStrategySelectorPanel
            automaticPromotion={snapshot.automatic_promotion}
            strategies={filteredStrategies}
            selectedStrategy={selectedStrategy}
            onSelect={(strategyId) => {
              selectStrategyId(strategyId);
              saveArtifactMetadata(strategyId, "strategy", { markUsed: true }).catch(() => undefined);
            }}
            metadata={selectedStrategy ? artifactMetadata[artifactMetadataKey(selectedStrategy.strategy_id, "strategy")] : null}
            onMetadataSave={saveArtifactMetadata}
            onPromoteLive={onPromoteLive}
            onStrategyLifecycle={onStrategyLifecycle}
            orders={snapshot.orders ?? []}
            summary={snapshot.summary ?? {}}
            operatorConfirmed={Boolean(snapshot.operator_confirmed)}
          />
          {selectedStrategy && (
            <PortfolioArtifactPanel
              portfolios={snapshot.portfolios ?? []}
              selectedStrategy={selectedStrategy}
              operationalReadiness={snapshot.operational_readiness}
              runtimeRecovery={snapshot.runtime_recovery}
              shadowLive={snapshot.shadow_live}
              multiStrategy={snapshot.multi_strategy}
              executionCalibration={snapshot.execution_calibration}
              metadataItems={artifactMetadata}
              onMetadataSave={saveArtifactMetadata}
              continuousRuntime={snapshot.continuous_runtime}
            />
          )}
          {!deploymentOnly && (
            <OperationalSafeguardsPanel
              apiConnected={snapshot.api_connected === true}
              dryRun={snapshot.dry_run}
              newEntriesBlocked={snapshot.new_entries_blocked}
              killSwitch={snapshot.kill_switch}
              operatorConfirmed={snapshot.operator_confirmed}
              onConfirm={onConfirm}
              onDryRun={onDryRun}
              onEntryBlock={onEntryBlock}
              onTestIntent={onTestIntent}
            />
          )}
        </div>
        {!deploymentOnly && (
          <div className="content-column">
            <FuturesSettingsPanel snapshot={snapshot.binance_futures_settings} selectedSymbol={selectedStrategy?.symbol} />
            <FuturesRiskSimulatorPanel strategies={selectedStrategy ? [selectedStrategy] : []} />
            <CapitalRolloutPanel snapshot={snapshot.capital_rollout} selectedStrategyId={selectedStrategy?.strategy_id} />
            <FuturesFillSoakPanel snapshot={snapshot.binance_futures_fill_soak} selectedSymbol={selectedStrategy?.symbol} />
            <RiskSettingsPanel settings={snapshot.risk_settings} onRiskSetting={onRiskSetting} />
            <RetryPolicyPanel policy={snapshot.retry_policy} onRetryPolicy={onRetryPolicy} />
          </div>
        )}
      </section>
    </section>
  );
}

const FUTURES_FILL_SOAK_BLOCKER_LABELS = {
  "account-cannot-trade": "선물 계정 거래 권한이 없습니다.",
  "position-mode-not-hedge": "포지션 모드를 Hedge Mode로 설정해야 합니다.",
  "margin-type-not-isolated": "검증 종목을 격리(ISOLATED) 마진으로 설정해야 합니다.",
  "leverage-not-1x": "검증 종목 레버리지를 1배로 설정해야 합니다.",
  "preflight-position-not-flat": "기존 선물 포지션을 먼저 평탄화해야 합니다.",
  "preflight-open-orders-present": "기존 미체결 선물 주문을 먼저 정리해야 합니다.",
  "available-usdt-invalid": "USD-M Futures 지갑에 주문 가능한 USDT가 없습니다.",
  "initial-equity-invalid": "USD-M Futures 계좌 equity를 확인할 수 없습니다.",
  "minimum-order-exceeds-available-usdt": "거래소 최소 주문 금액보다 Futures 가용 USDT가 적습니다.",
  "minimum-order-derivation-failed": "현재 종목은 5~10 USDT 안전 주문 범위로 수량을 만들 수 없습니다.",
  "real-orders-disabled": "실주문 환경 잠금이 비활성화되어 있습니다.",
  "immutable-report-path-unavailable": "덮어쓰기 방지 리포트 경로를 준비할 수 없습니다.",
  "preview-observation-failed": "실계좌 읽기 전용 조회에 실패했습니다.",
};

const FUTURES_RISK_BLOCKER_LABELS = {
  "account-equity-invalid": "계좌 equity를 확인할 수 없습니다.",
  "available-usdt-invalid": "주문 가능한 USDT가 없습니다.",
  "broker-risk-inputs-unavailable": "Binance 위험 입력 조회에 실패했습니다.",
  "entry-price-invalid": "현재 mark price를 확인할 수 없습니다.",
  "futures-margin-mode-drift": "현재 마진 방식이 Artifact 정책과 다릅니다.",
  "futures-margin-mode-crossed-not-allowed": "공유 증거금(CROSSED)은 신규 자동 진입 정책에서 허용하지 않습니다.",
  "futures-leverage-limit-drift": "현재 레버리지가 Artifact 상한을 초과합니다.",
  "leverage-policy-drift": "입력 레버리지가 Artifact 상한을 초과합니다.",
  "margin-type-policy-drift": "현재 마진 방식이 전략 정책과 다릅니다.",
  "max-notional-policy-exceeded": "주문 명목금액이 계좌 대비 Artifact 상한을 초과합니다.",
  "notional-invalid": "주문 명목금액을 0보다 크게 입력하세요.",
  "per-trade-risk-policy-exceeded": "손절 시 예상 손실이 거래당 위험 한도를 초과합니다.",
  "protective-stop-direction-invalid": "LONG 손절은 진입가 아래, SHORT 손절은 진입가 위여야 합니다.",
  "protective-stop-missing": "보호 손절가가 필요합니다.",
  "requested-leverage-broker-mismatch": "계산 레버리지와 현재 Binance 설정이 다릅니다.",
  "required-margin-exceeds-available": "초기 증거금과 진입 수수료가 가용 USDT를 초과합니다.",
  "strategy-artifact-missing": "선택한 전략 Artifact를 찾을 수 없습니다.",
  "strategy-route-not-binance-futures": "선택 전략은 Binance USD-M 실행 경로가 아닙니다.",
  "strategy-symbol-mismatch": "전략 종목과 계산 종목이 다릅니다.",
};

const CAPITAL_ROLLOUT_BLOCKER_LABELS = {
  "account-equity-invalid": "계좌 equity 확인 필요",
  "available-cash-invalid": "가용 현금 확인 필요",
  "blocked-orders-present": "차단·실패 주문 해소 필요",
  "canary-cap-unavailable": "Canary 주문 한도 없음",
  "full-live-requires-clean-soak-pass": "경고 없는 clean PASS Soak 필요",
  "futures-policy-invalid": "Futures 정책 무결성 실패",
  "lineage-invalid": "데이터·검증 계보 불완전",
  "minimum-canary-fills-not-met": "동일 scope 실제 체결 3건 필요",
  "minimum-full-live-fills-not-met": "Full Live 실제 체결 20건 필요",
  "minimum-full-live-observation-not-met": "무인 관찰 7일 필요",
  "reconciliation-not-fresh": "최신 계좌·포지션 대조 필요",
  "small-live-cap-unavailable": "Small Live 주문 한도 없음",
  "soak-not-accepted": "PASS 또는 PASS_WITH_WARNING Soak 필요",
};

function displayNumber(value, digits = 2) {
  const number = Number(value);
  return Number.isFinite(number) ? number.toLocaleString("ko-KR", { maximumFractionDigits: digits }) : "-";
}

function LineageFlowPanel({ snapshot = {}, selectedStrategyId = "" }) {
  const flows = Array.isArray(snapshot?.flows) ? snapshot.flows : [];
  const flow = flows.find((item) => item.strategyId === selectedStrategyId) || flows[0];
  return (
    <section className="panel lineage-flow-panel">
      <PanelHeader
        title="데이터·전략 계보"
        subtitle="Scraper 원천부터 Backtest, Paper, Live 증거까지 같은 전략 체인을 읽기 전용으로 추적합니다."
        suffix={(
          <StatusPill tone={snapshot?.brokenCount ? "warning" : flows.length ? "success" : "neutral"}>
            {snapshot?.completeCount || 0} COMPLETE · {snapshot?.brokenCount || 0} BROKEN
          </StatusPill>
        )}
      />
      {flow ? (
        <>
          <div className="lineage-flow-identity">
            <div>
              <strong>{flow.name}</strong>
              <span>{flow.strategyId} · {flow.symbol} · {flow.timeframe}</span>
            </div>
            <StatusPill tone={flow.complete ? "success" : "danger"}>
              {flow.complete ? "CHAIN OK" : "CHAIN BLOCKED"}
            </StatusPill>
          </div>
          <div className="lineage-stage-grid">
            {(flow.stages || []).map((stage, index) => (
              <React.Fragment key={stage.id}>
                <article {...semanticSurfaceProps(stage.status === "PASS" ? "success" : stage.status === "BLOCK" ? "danger" : "neutral", "lineage-stage-card")}>
                  <span>{stage.label}</span>
                  <strong>{stage.status}</strong>
                  <div className="lineage-stage-details">
                    {stage.id === "dataset" && stage.metadata?.datasetId && <small>dataset · {stage.metadata.datasetId}</small>}
                    {stage.id === "dataset" && stage.metadata?.lineageRunId && <small>run · {stage.metadata.lineageRunId}</small>}
                    {stage.id === "dataset" && stage.metadata?.sourceStage && <small>stage · {stage.metadata.sourceStage}</small>}
                    {stage.id === "dataset" && stage.metadata?.stageRevisionId && <small>revision · {stage.metadata.stageRevisionId}</small>}
                    {stage.id === "dataset" && stage.metadata?.transformationId && <small>transform · {stage.metadata.transformationId}</small>}
                    <small>
                      {stage.contentHash
                        ? `hash · ${stage.contentHash.slice(0, 12)}…`
                        : stage.required
                          ? "증거 없음"
                          : "아직 미진행"}
                    </small>
                  </div>
                </article>
                {index < (flow.stages || []).length - 1 && <span className="lineage-stage-arrow">→</span>}
              </React.Fragment>
            ))}
          </div>
          {!flow.complete && (
            <div {...semanticSurfaceProps("danger", "lineage-flow-warning")}>
              <ShieldAlert size={15} />
              <span>{(flow.brokenLinks || [])[0] || "계보 연결을 확인하세요."}</span>
            </div>
          )}
        </>
      ) : <EmptyRow text="표시할 전략 계보가 없습니다." />}
    </section>
  );
}

function CapitalRolloutPanel({ snapshot = {}, selectedStrategyId = "", className = "" }) {
  const rows = Array.isArray(snapshot?.strategies) ? snapshot.strategies : [];
  const rollout = rows.find((item) => item.strategyId === selectedStrategyId) || rows[0];
  return (
    <section className={`panel capital-rollout-panel ${className}`.trim()}>
      <PanelHeader
        title="단계별 자본 확대"
        subtitle="최소 Canary → Small Live → Full Live 순서로만 상한이 커집니다. 실제 주문 게이트에도 같은 한도를 적용합니다."
        suffix={(
          <StatusPill tone={snapshot?.accountFresh && snapshot?.reconciliationFresh ? "success" : "warning"}>
            {snapshot?.accountFresh && snapshot?.reconciliationFresh ? "ACCOUNT FRESH" : "REFRESH NEEDED"}
          </StatusPill>
        )}
      />
      {rollout ? (
        <>
          <div className="capital-rollout-summary">
            <div><span>전략</span><strong>{rollout.strategyName}</strong></div>
            <div><span>계좌 equity</span><strong>{displayNumber(rollout.accountEquity)} USDT</strong></div>
            <div><span>정책 상한</span><strong>{displayNumber(rollout.policyMaxNotionalPercent)}%</strong></div>
            <div><span>Canary 체결</span><strong>{rollout.canaryFills || 0}건</strong></div>
          </div>
          <div className="capital-rollout-stages">
            {(rollout.stages || []).map((stage, index) => (
              <React.Fragment key={stage.id}>
                <article {...semanticSurfaceProps(stage.ready ? "success" : "warning", "capital-rollout-stage")}>
                  <span>{stage.label}</span>
                  <strong>≤ {displayNumber(stage.maxNotional)} USDT</strong>
                  <small>{stage.ready ? "현재 증거 충족" : CAPITAL_ROLLOUT_BLOCKER_LABELS[stage.blockers?.[0]] || stage.blockers?.[0] || "확인 필요"}</small>
                </article>
                {index < (rollout.stages || []).length - 1 && <span className="capital-rollout-arrow">→</span>}
              </React.Fragment>
            ))}
          </div>
        </>
      ) : <EmptyRow text="Binance USD-M 전략이 없어 자본 확대 단계를 계산하지 않았습니다." />}
    </section>
  );
}

function FuturesRiskSimulatorPanel({ strategies = [] }) {
  const futuresStrategies = useMemo(
    () => strategies.filter(isFuturesRiskStrategy),
    [strategies],
  );
  const [strategyId, setStrategyId] = useState("");
  const [symbol, setSymbol] = useState("ETHUSDT");
  const [direction, setDirection] = useState("SHORT");
  const [notional, setNotional] = useState("5");
  const [leverage, setLeverage] = useState("1");
  const [stopPrice, setStopPrice] = useState("");
  const [fundingIntervals, setFundingIntervals] = useState("3");
  const [result, setResult] = useState(null);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const hydratedStrategyIdRef = useRef(null);
  const initialStrategySelectedRef = useRef(false);

  useEffect(() => {
    const selectedStillExists = futuresStrategies.some(
      (strategy) => strategy.strategy_id === strategyId,
    );
    if (strategyId && !selectedStillExists) {
      const replacementId = futuresStrategies[0]?.strategy_id || "";
      initialStrategySelectedRef.current = Boolean(replacementId);
      setStrategyId(replacementId);
      return;
    }
    if (
      !strategyId
      && !initialStrategySelectedRef.current
      && futuresStrategies[0]?.strategy_id
    ) {
      initialStrategySelectedRef.current = true;
      setStrategyId(futuresStrategies[0].strategy_id);
    }
  }, [futuresStrategies, strategyId]);

  useEffect(() => {
    if (!shouldHydrateRiskStrategy(hydratedStrategyIdRef.current, strategyId)) {
      return;
    }
    hydratedStrategyIdRef.current = strategyId;
    if (!strategyId) {
      setResult(null);
      return;
    }
    const selected = futuresStrategies.find((item) => item.strategy_id === strategyId);
    if (!selected) return;
    const defaults = futuresRiskStrategyDefaults(selected);
    setSymbol(defaults.symbol);
    setDirection(defaults.direction);
    setLeverage(defaults.leverage);
    setResult(null);
  }, [futuresStrategies, strategyId]);

  const selectedStrategy = futuresStrategies.find(
    (item) => item.strategy_id === strategyId,
  );
  const leverageOptions = futuresRiskLeverageOptions(
    selectedStrategy?.futures_execution_policy?.maxLeverageMultiplier,
    leverage,
  );

  async function calculateRisk() {
    setBusy(true);
    setMessage("");
    try {
      const response = await previewBinanceFuturesOrderRisk({
        strategy_id: strategyId,
        symbol,
        direction,
        notional_usdt: Number(notional),
        leverage: Number(leverage),
        stop_price: Number(stopPrice),
        funding_intervals: Number(fundingIntervals),
      });
      setResult(response.risk || null);
      setMessage(response.reason || "");
    } catch (error) {
      setResult(null);
      setMessage(error?.message || "위험 계산에 실패했습니다.");
    } finally {
      setBusy(false);
    }
  }

  const estimate = result?.estimate || {};
  const blockers = Array.isArray(result?.blockers) ? result.blockers : [];
  const tone = result?.status === "READY" ? "success" : result?.status === "WARNING" ? "warning" : result ? "danger" : "neutral";
  return (
    <section className="panel futures-risk-panel">
      <PanelHeader
        title="주문 직전 위험 시뮬레이터"
        subtitle="현재 Binance mark·funding·maintenance bracket·수수료와 Artifact 정책으로 계산합니다. 주문이나 계정 설정은 변경하지 않습니다."
        suffix={<StatusPill tone={tone}>{result?.status || "READ ONLY"}</StatusPill>}
      />
      <div className="futures-risk-controls">
        <label><span>전략 Artifact</span><select value={strategyId} onChange={(event) => setStrategyId(event.target.value)}><option value="">안전 기본 정책</option>{futuresStrategies.map((item) => <option key={item.strategy_id} value={item.strategy_id}>{item.name} · {item.symbol}</option>)}</select></label>
        <label><span>종목</span><input value={symbol} onChange={(event) => setSymbol(event.target.value.toUpperCase().replace(/[^A-Z0-9]/g, ""))} /></label>
        <label><span>방향</span><select value={direction} onChange={(event) => setDirection(event.target.value)}><option value="LONG">LONG</option><option value="SHORT">SHORT</option></select></label>
        <label><span>명목금액 (USDT)</span><input type="number" min="0" step="0.1" value={notional} onChange={(event) => setNotional(event.target.value)} /></label>
        <label><span>계산 레버리지 (x)</span><select value={leverage} onChange={(event) => setLeverage(event.target.value)}>{leverageOptions.map((value) => <option key={value} value={value}>{value}x</option>)}</select></label>
        <label><span>보호 손절가</span><input type="number" min="0" step="any" value={stopPrice} onChange={(event) => setStopPrice(event.target.value)} placeholder="필수" /></label>
        <label><span>예상 펀딩 횟수</span><input type="number" min="0" max="90" value={fundingIntervals} onChange={(event) => setFundingIntervals(event.target.value)} /></label>
      </div>
      <div className="operator-actions">
        <ActionButton className="secondary-button" disabled={busy} label={busy ? "조회·계산 중" : "현재값으로 계산"} onClick={calculateRisk} status={busy ? "pending" : undefined} />
        {message && <span className="inline-state">{message}</span>}
      </div>
      {result && (
        <>
          <div className="futures-risk-metrics">
            <div><span>Mark price</span><strong>{displayNumber(result.market?.mark_price, 6)}</strong></div>
            <div><span>초기 증거금</span><strong>{displayNumber(estimate.initial_margin_usdt, 4)} USDT</strong></div>
            <div><span>추정 청산가</span><strong>{estimate.liquidation_price == null ? "CROSS 산출 불가" : displayNumber(estimate.liquidation_price, 6)}</strong></div>
            <div><span>청산 여유</span><strong>{estimate.liquidation_buffer_pct == null ? "-" : `${displayNumber(estimate.liquidation_buffer_pct)}%`}</strong></div>
            <div><span>왕복 수수료</span><strong>{displayNumber(estimate.round_trip_fee_usdt, 6)} USDT</strong></div>
            <div><span>예상 펀딩 비용</span><strong>{displayNumber(estimate.estimated_funding_cost_usdt, 6)} USDT</strong></div>
            <div><span>손절 예상 손실</span><strong>{displayNumber(estimate.estimated_loss_at_stop_usdt, 6)} USDT</strong></div>
            <div><span>계좌 위험률</span><strong>{displayNumber(estimate.risk_pct_of_equity, 3)}%</strong></div>
          </div>
          {blockers.length > 0 && (
            <div className="futures-risk-blockers">
              {blockers.map((blocker) => (
                <div {...semanticSurfaceProps("danger", "futures-risk-blocker")} key={blocker}>
                  <ShieldAlert size={14} />
                  <span>{FUTURES_RISK_BLOCKER_LABELS[blocker] || blocker}</span>
                </div>
              ))}
            </div>
          )}
          <p className="futures-risk-disclaimer">{result.disclaimer}</p>
        </>
      )}
    </section>
  );
}

const FUTURES_SETTINGS_BLOCKER_LABELS = {
  "account-cannot-trade": "선물 계정 거래 권한을 확인할 수 없습니다.",
  "positions-present": "열린 선물 포지션이 있어 계정 설정 변경을 차단했습니다.",
  "open-orders-present": "미체결 선물 주문이 있어 계정 설정 변경을 차단했습니다.",
  "margin-type-unknown": "현재 마진 방식을 확인할 수 없습니다.",
  "leverage-unknown": "현재 레버리지를 확인할 수 없습니다.",
  "symbol-invalid": "유효한 USD-M 선물 종목을 입력하세요.",
  "only-isolated-supported": "안전 설정 화면에서는 격리(ISOLATED) 마진만 지원합니다.",
  "leverage-outside-safe-presets": "초기 안전 프리셋 1x·2x·3x·5x 중 하나를 선택하세요.",
  "observation-failed": "Binance 실계좌 설정 조회에 실패했습니다.",
};

function futuresDeploymentSymbol(value) {
  const normalized = String(value || "").toUpperCase().replace(/[^A-Z0-9]/g, "");
  return /^[A-Z0-9]{3,18}USDT$/.test(normalized) ? normalized : "";
}

function FuturesSettingsPanel({ snapshot = EMPTY_FUTURES_PANEL_SNAPSHOT, selectedSymbol = "" }) {
  const [view, setView] = useState(snapshot || {});
  const [symbol, setSymbol] = useState(() => futuresDeploymentSymbol(selectedSymbol) || "ETHUSDT");
  const [leverage, setLeverage] = useState(1);
  const [authorizationToken, setAuthorizationToken] = useState("");
  const [busy, setBusy] = useState("");
  const [message, setMessage] = useState("");
  const current = view?.status ? view : snapshot || {};
  const preview = current.preview || {};
  const blockers = Array.isArray(preview.blockers) ? preview.blockers : [];
  const canApply = current.status === "READY" && Boolean(authorizationToken);

  useEffect(() => {
    if (!snapshot || typeof snapshot !== "object") return;
    if (!view?.status || snapshot.status === "APPLIED" || snapshot.status === "FAILED") {
      setView(snapshot);
    }
  }, [snapshot, view?.status]);

  useEffect(() => {
    const next = futuresDeploymentSymbol(selectedSymbol);
    if (!next || next === symbol) return;
    setSymbol(next);
    setAuthorizationToken("");
  }, [selectedSymbol, symbol]);

  async function previewSettings() {
    setBusy("preview");
    setMessage("");
    setAuthorizationToken("");
    try {
      const response = await previewBinanceFuturesSettings(
        symbol,
        "ISOLATED",
        Number(leverage),
      );
      setView(response.settings || {});
      setAuthorizationToken(response.authorization?.confirmation_token || "");
      setMessage(response.reason || "");
    } catch (error) {
      setMessage(error?.message || "선물 설정 사전점검에 실패했습니다.");
    } finally {
      setBusy("");
    }
  }

  async function applySettings() {
    if (!authorizationToken) return;
    const confirmed = window.confirm(
      `${symbol}의 마진 방식을 ISOLATED, 레버리지를 ${leverage}x로 변경합니다.\n\n`
      + "레버리지 배수는 손실 위험률(%)이 아닙니다. 이 작업은 주문을 만들지 않지만 "
      + "향후 주문의 증거금·청산 위험을 바꿉니다.\n\n계속하시겠습니까?",
    );
    if (!confirmed) return;
    setBusy("apply");
    setMessage("");
    try {
      const response = await applyBinanceFuturesSettings(
        authorizationToken,
        true,
      );
      setAuthorizationToken("");
      setView(response.settings || {});
      setMessage(response.reason || "");
    } catch (error) {
      setAuthorizationToken("");
      setMessage(error?.message || "선물 설정 적용에 실패했습니다.");
    } finally {
      setBusy("");
    }
  }

  const statusTone = (
    current.status === "APPLIED" || current.status === "CONFIGURED"
      ? "success"
      : current.status === "FAILED" || current.status === "BLOCKED"
        ? "danger"
        : current.status === "READY" || current.status === "APPLYING"
          ? "warning"
          : "info"
  );

  return (
    <section className="panel futures-settings-panel">
      <PanelHeader
        title="Binance USD-M 종목별 증거금 설정"
        subtitle="배율(x)과 위험률(%)을 분리하고, 포지션·미체결 주문이 없을 때만 명시 확인 후 적용합니다."
        suffix={<StatusPill tone={statusTone}>{current.status || "IDLE"}</StatusPill>}
      />
      <div className="futures-settings-controls">
        <label>
          <span>종목</span>
          <input
            value={symbol}
            onChange={(event) => {
              setSymbol(event.target.value.toUpperCase().replace(/[^A-Z0-9]/g, ""));
              setAuthorizationToken("");
            }}
            maxLength={20}
          />
        </label>
        <label>
          <span>마진 방식</span>
          <select value="ISOLATED" disabled>
            <option value="ISOLATED">ISOLATED · 격리</option>
          </select>
        </label>
        <label>
          <span>초기 레버리지</span>
          <select
            value={leverage}
            onChange={(event) => {
              setLeverage(Number(event.target.value));
              setAuthorizationToken("");
            }}
          >
            {[1, 2, 3, 5].map((value) => (
              <option key={value} value={value}>{value}x</option>
            ))}
          </select>
        </label>
      </div>
      <div className="futures-settings-explainer">
        <strong>레버리지 {leverage}x</strong>
        <span>
          10 USDT 증거금 기준 최대 명목 노출은 약 {10 * Number(leverage)} USDT입니다.
          거래당 위험률은 손절 거리와 주문 크기로 별도 제한해야 합니다.
        </span>
      </div>
      {preview.symbol && (
        <div className="futures-fill-soak-observation">
          <span>현재 <strong>{preview.margin_type || "-"} · {preview.leverage ?? "-"}x</strong></span>
          <span>목표 <strong>{preview.target_margin_type || "ISOLATED"} · {preview.target_leverage ?? leverage}x</strong></span>
          <span>가용 <strong>{preview.available_usdt ?? "-"} USDT</strong></span>
          <span>포지션/미체결 <strong>{preview.position_count ?? 0}/{preview.open_order_count ?? 0}</strong></span>
        </div>
      )}
      {blockers.length > 0 && (
        <div className="futures-fill-soak-blockers" role="status">
          {blockers.map((blocker) => (
            <div {...semanticSurfaceProps("danger", "futures-fill-soak-blocker")} key={blocker}>
              <ShieldAlert size={15} />
              <span>{FUTURES_SETTINGS_BLOCKER_LABELS[blocker] || blocker}</span>
            </div>
          ))}
        </div>
      )}
      <div className="operator-actions">
        <ActionButton
          className="secondary-button"
          disabled={Boolean(busy)}
          label={busy === "preview" ? "점검 중" : "현재값·안전조건 점검"}
          onClick={previewSettings}
          status={busy === "preview" ? "pending" : undefined}
        />
        <ActionButton
          className="danger-button"
          disabled={!canApply || Boolean(busy)}
          label={busy === "apply" ? "적용 중" : "확인 후 설정 적용"}
          onClick={applySettings}
          status={busy === "apply" ? "pending" : undefined}
        />
        {message && <span className="inline-state">{message}</span>}
      </div>
    </section>
  );
}

function FuturesFillSoakPanel({ snapshot = EMPTY_FUTURES_PANEL_SNAPSHOT, selectedSymbol = "" }) {
  const [view, setView] = useState(snapshot || {});
  const [authorizationToken, setAuthorizationToken] = useState("");
  const [busy, setBusy] = useState("");
  const [message, setMessage] = useState("");
  const current = view?.session_id ? view : snapshot || {};
  const preview = current.preview || {};
  const blockers = Array.isArray(preview.blockers) ? preview.blockers : [];
  const active = current.active === true;
  const ready = current.status === "READY" && Boolean(authorizationToken);
  const finalReport = current.final_report || current.latest_durable_report || {};
  const soakSymbol = futuresDeploymentSymbol(selectedSymbol) || futuresDeploymentSymbol(current.symbol) || "ETHUSDT";

  useEffect(() => {
    if (!snapshot || typeof snapshot !== "object") return;
    if (
      snapshot.active
      || snapshot.status === "PASS"
      || snapshot.status === "FAIL"
      || !view?.session_id
      || snapshot.session_id === view.session_id
    ) {
      setView(snapshot);
    }
  }, [snapshot, view?.session_id]);

  async function previewSession() {
    setBusy("preview");
    setMessage("");
    try {
      const response = await previewBinanceFuturesFillSoak(soakSymbol);
      setView(response.fill_soak || {});
      setAuthorizationToken(response.authorization?.confirmation_token || "");
      setMessage(response.reason || "");
    } catch (error) {
      setMessage(error?.message || "사전점검 요청에 실패했습니다.");
    } finally {
      setBusy("");
    }
  }

  async function startSession() {
    if (!authorizationToken) return;
    setBusy("start");
    setMessage("");
    try {
      const response = await startBinanceFuturesFillSoak(
        authorizationToken,
        true,
        { symbol: soakSymbol },
      );
      setAuthorizationToken("");
      setView(response.fill_soak || {});
      setMessage(response.reason || "");
    } catch (error) {
      setAuthorizationToken("");
      setMessage(error?.message || "실체결 soak 시작에 실패했습니다.");
    } finally {
      setBusy("");
    }
  }

  async function stopSession() {
    if (!window.confirm("신규 주문을 중단하고 세션 소유 포지션의 안전 평탄화를 요청하시겠습니까?")) return;
    setBusy("stop");
    setMessage("");
    try {
      const response = await stopBinanceFuturesFillSoak();
      setView(response.fill_soak || {});
      setMessage(response.reason || "");
    } catch (error) {
      setMessage(error?.message || "안전 중단 요청에 실패했습니다.");
    } finally {
      setBusy("");
    }
  }

  const statusTone = (
    current.status === "PASS"
      ? "success"
      : current.status === "FAIL" || current.status === "BLOCKED"
        ? "danger"
        : active || current.status === "READY"
          ? "warning"
          : "info"
  );

  return (
    <section className="panel futures-fill-soak-panel">
      <PanelHeader
        title="Binance USD-M 실체결 Soak"
        subtitle="전략 승급과 분리된 브로커 경로 검증입니다. 결과는 자동 승급 증거로 사용하지 않습니다."
        suffix={<StatusPill tone={statusTone}>{current.status || "IDLE"}</StatusPill>}
      />
      <MetricGrid columns={4}>
        <MetricCard label="검증 종목" value={current.symbol || soakSymbol} detail="HEDGE · ISOLATED · 1x" />
        <MetricCard label="실체결 목표" value="왕복 3회" detail="진입·청산 총 6 FILLED" />
        <MetricCard label="주문·자본 상한" value="5~10 USDT" detail="최초 가용 USDT 100% 이내" />
        <MetricCard label="손실·시간" value="10% · 5시간" detail="손실 즉시 중단 · 최종 flat" />
      </MetricGrid>
      {preview.minimum_order && (
        <div className="futures-fill-soak-observation">
          <span>가용 <strong>{preview.available_usdt ?? "-"} USDT</strong></span>
          <span>예상 주문 <strong>{preview.minimum_order.estimated_notional_usdt ?? "-"} USDT</strong></span>
          <span>마진 <strong>{preview.margin_type || "-"} · {preview.leverage ?? "-"}x</strong></span>
          <span>포지션/미체결 <strong>{preview.positions?.length ?? 0}/{preview.open_orders?.length ?? 0}</strong></span>
        </div>
      )}
      {blockers.length > 0 && (
        <div className="futures-fill-soak-blockers" role="status">
          {blockers.map((blocker) => (
            <div {...semanticSurfaceProps("danger", "futures-fill-soak-blocker")} key={blocker}>
              <ShieldAlert size={15} />
              <span>{FUTURES_FILL_SOAK_BLOCKER_LABELS[blocker] || blocker}</span>
            </div>
          ))}
        </div>
      )}
      {finalReport?.session_id && (
        <div {...semanticSurfaceProps(finalReport.status === "PASS" ? "success" : "danger", "futures-fill-soak-final")}>
          <strong>{finalReport.status}</strong>
          <span>체결 {finalReport.fill_count ?? 0} · 왕복 {finalReport.round_trips_completed ?? 0} · flat {finalReport.flat === true ? "확인" : "미확인"}</span>
          {finalReport.report_path && <small>{finalReport.report_path}</small>}
        </div>
      )}
      <div className="operator-actions">
        <ActionButton
          className="secondary-button"
          disabled={Boolean(busy) || active}
          label={busy === "preview" ? "점검 중" : "읽기 전용 사전점검"}
          onClick={previewSession}
          status={busy === "preview" ? "pending" : undefined}
        />
        <ActionButton
          className="danger-button"
          disabled={!ready || Boolean(busy) || active}
          label={busy === "start" ? "시작 중" : "확인 후 실체결 시작"}
          onClick={startSession}
          status={busy === "start" ? "pending" : undefined}
        />
        <ActionButton
          className="secondary-button"
          disabled={!active || Boolean(busy)}
          label={busy === "stop" ? "중단 중" : "안전 중단"}
          onClick={stopSession}
          status={busy === "stop" ? "pending" : undefined}
        />
        {message && <span className="inline-state">{message}</span>}
      </div>
    </section>
  );
}

function PortfolioArtifactPanel({
  portfolios = [],
  selectedStrategy,
  operationalReadiness = {},
  runtimeRecovery = {},
  shadowLive = {},
  multiStrategy = {},
  executionCalibration = {},
  metadataItems = {},
  onMetadataSave,
  continuousRuntime = {},
}) {
  const gate = selectedStrategy?.portfolio_gate ?? {};
  const [replay, setReplay] = useState(null);
  const [replayRunning, setReplayRunning] = useState(false);
  const [operationResult, setOperationResult] = useState(null);
  const [filters, setFilters] = useState({ query: "", failure: "all", quick: "all" });
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);
  const failureOptions = uniqueStrategyDiscoveryValues(portfolios.flatMap((portfolio) => liveArtifactFailureReasons(portfolio)));
  const visiblePortfolios = portfolios.filter((portfolio) => {
    const artifactId = portfolio.id || portfolio.source_path;
    const metadata = metadataItems[artifactMetadataKey(artifactId, "portfolio")];
    const query = filters.query.trim().toLocaleLowerCase();
    const matchesQuery = !query || [
      portfolio.name,
      portfolio.id,
      portfolio.lifecycle_status,
      JSON.stringify(portfolio.strategy_instances || []),
      JSON.stringify(metadata?.tags || []),
      metadata?.note,
    ].some((value) => String(value || "").toLocaleLowerCase().includes(query));
    return matchesQuery
      && (filters.failure === "all" || liveArtifactFailureReasons(portfolio).includes(filters.failure))
      && artifactMatchesQuickFilter(metadata, filters.quick, liveArtifactRunning(continuousRuntime, artifactId));
  });
  const totalPages = Math.max(1, Math.ceil(visiblePortfolios.length / pageSize));
  const currentPage = Math.min(Math.max(page, 1), totalPages);
  const pageStart = (currentPage - 1) * pageSize;
  const pageEnd = Math.min(visiblePortfolios.length, pageStart + pageSize);
  const pagedPortfolios = visiblePortfolios.slice(pageStart, pageEnd);
  const pageNumbers = paginationNumbers(currentPage, totalPages);

  useEffect(() => {
    setPage(1);
  }, [filters.query, filters.failure, filters.quick, pageSize]);

  useEffect(() => {
    setPage((current) => Math.min(Math.max(current, 1), totalPages));
  }, [totalPages]);

  async function replayPolicy() {
    setReplayRunning(true);
    try {
      const response = await runPolicyReplay({
        side: "BUY",
        currentWeight: Number(gate.targetWeight || 0) * 0.5,
        portfolioEquity: 10000000,
        expectedAlphaBps: 12,
        expectedCostBps: 6,
        alternative: { policyVersion: "live-conservative-v1", deadbandWeight: 0.005, costBufferBps: 2, capitalMultiplier: Number(gate.capitalMultiplier ?? 1) },
      });
      setReplay(response.bundle ?? null);
    } finally {
      setReplayRunning(false);
    }
  }
  async function observeShadow() {
    const response = await runShadowLive({ side: "BUY", expected_cost_bps: 6, latency_ms: 100 });
    setOperationResult(response.evidence ? `Shadow evidence ${response.evidence.contentHash.slice(0, 12)}` : "Shadow 실행 완료");
  }
  async function verifyRecovery() {
    const response = await runRecoveryDrill();
    setOperationResult(response.ok ? `Recovery generation ${response.recovery.generation} 검증 완료` : "Recovery 안전 모드 진입");
  }
  return (
    <section className="panel portfolio-artifact-panel">
      <PanelHeader title="포트폴리오 Artifact" subtitle="Live 주문은 선택 전략이 포함된 포트폴리오 universe와 target weight를 기준으로 제한됩니다." />
      {gate.active && (
        <div className="portfolio-gate-metrics">
          <MetricCard className="metric-card" label="Portfolio" value={gate.portfolioName || gate.portfolioId || "-"} detail={gate.lifecycleStatus || "-"} />
          <MetricCard className="metric-card" label="실효 목표 비중" value={`${formatPercentValue(gate.targetWeight)}%`} detail={`설정 ${formatPercentValue(gate.configuredTargetWeight)}% × 전략 ${formatPercentValue(gate.positionSizeFraction)}% · Symbol max ${formatPercentValue((gate.maxSymbolWeightPct || 0) / 100)}%`} />
          <MetricCard className="metric-card" label="FX 기준시각" value={gate.fxFreshness?.source === "same-currency" ? "동일 통화" : gate.fxFreshness?.asOf || "없음"} detail={`${gate.fxFreshness?.currency || "-"}/${gate.fxFreshness?.baseCurrency || "-"} ×${Number(gate.fxFreshness?.rate ?? 0).toLocaleString("ko-KR")} · ${gate.fxFreshness?.fresh ? `신선 ${gate.fxFreshness?.ageDays ?? 0}일` : `STALE ${gate.fxFreshness?.ageDays ?? "?"}일`}`} />
          <MetricCard className="metric-card" label="Mandate" value={gate.mandateCompliant === false ? "BLOCK" : "PASS"} detail={(gate.mandateBreaches ?? []).join(" · ") || "위반 없음"} />
          <MetricCard className="metric-card" label="Auto De-risk" value={`${gate.automaticDeRiskAction || "KEEP"} ×${Number(gate.capitalMultiplier ?? 1).toFixed(2)}`} detail={`Stress ${gate.stressPassed === false ? "BLOCK" : "PASS"}`} />
        </div>
      )}
      {gate.active && (
        <div className="operator-actions">
          <ActionButton className="secondary-button" label="정책 Replay" onClick={replayPolicy} status={replayRunning ? "pending" : replay ? "success" : undefined} disabled={replayRunning} />
          <ActionButton className="secondary-button" label="Shadow 관찰" onClick={observeShadow} />
          <ActionButton className="secondary-button" label="복구 훈련" onClick={verifyRecovery} />
          {replay && <span className="inline-state success">{replay.eventCount}건 · 결정 변경 {replay.changedDecisionCount} · 원본 불변 {replay.sourceEventsImmutable ? "PASS" : "FAIL"}</span>}
          {operationResult && <span className="inline-state success">{operationResult}</span>}
        </div>
      )}
      {gate.active && (
        <div className="portfolio-gate-metrics">
          <MetricCard className="metric-card" label="Operational Readiness" value={`${operationalReadiness.score ?? 0}/${operationalReadiness.threshold ?? 85}`} detail={operationalReadiness.liveEligible ? "LIVE ELIGIBLE" : "신규 위험 차단"} />
          <MetricCard className="metric-card" label="Recovery" value={runtimeRecovery.verified && !runtimeRecovery.safeMode ? "VERIFIED" : "SAFE MODE"} detail={runtimeRecovery.detail || "복구 훈련 대기"} />
          <MetricCard className="metric-card" label="Shadow Live" value={`${shadowLive.count ?? 0} observations`} detail="브로커 전송은 항상 차단" />
          <MetricCard className="metric-card" label="Execution Calibration" value={`${executionCalibration.sampleCount ?? 0} samples`} detail={executionCalibration.status === "pass" ? `모델오차 ${Number(executionCalibration.meanAbsoluteModelErrorBps ?? 0).toFixed(1)} bps` : "Paper/Shadow 체결 표본 필요"} />
          <MetricCard className="metric-card" label="Strategy Sleeves" value={`${multiStrategy.sleeveCount ?? 0} active`} detail="종목별 순주문 1건 · Short는 명시 상품만" />
        </div>
      )}
      <div className="portfolio-artifact-discovery">
        <label><Search size={14} /><input value={filters.query} onChange={(event) => setFilters((current) => ({ ...current, query: event.target.value }))} placeholder="포트폴리오, 구성 전략, 태그, 메모 검색" /></label>
        <select value={filters.failure} onChange={(event) => setFilters((current) => ({ ...current, failure: event.target.value }))}>
          <option value="all">모든 실패 이유</option>
          {failureOptions.map((value) => <option key={value} value={value}>{value}</option>)}
        </select>
        {[
          ["all", "전체"],
          ["favorite", "즐겨찾기"],
          ["recent-used", "최근 사용"],
          ["recent-promoted", "최근 승급"],
          ["running", "현재 실행 중"],
        ].map(([value, label]) => <button className={filters.quick === value ? "active" : ""} key={value} type="button" onClick={() => setFilters((current) => ({ ...current, quick: value }))}>{label}</button>)}
      </div>
      <div className="portfolio-artifact-list">
        {visiblePortfolios.length ? (
          pagedPortfolios.map((portfolio) => {
            const permissions = portfolio.permissions ?? {};
            const targetCount = portfolio.target_portfolio?.length ?? 0;
            const strategyCount = portfolio.strategy_instances?.length ?? 0;
            const liveReady = permissions.live_allowed || permissions.live_export_allowed || permissions.live_small_allowed;
            const artifactId = portfolio.id || portfolio.source_path;
            const metadata = metadataItems[artifactMetadataKey(artifactId, "portfolio")];
            return (
              <article
                {...semanticSurfaceProps(liveReady ? "success" : "danger", "portfolio-artifact-item")}
                key={portfolio.id || portfolio.source_path}
              >
                <div>
                  <strong>{metadata?.favorite && <Star size={13} fill="currentColor" />}{portfolio.name || portfolio.id}</strong>
                  <span>{strategyCount} 전략 · {targetCount} target · {portfolio.lifecycle_status}</span>
                  {metadata?.tags?.length > 0 && <small>{metadata.tags.map((tag) => `#${tag}`).join(" ")}</small>}
                </div>
                <ArtifactMetadataEditor artifactId={artifactId} artifactType="portfolio" metadata={metadata} onSave={onMetadataSave} compact />
                <StatusPill tone={liveReady ? "success" : "danger"}>{liveReady ? "LIVE READY" : "LIVE BLOCKED"}</StatusPill>
              </article>
            );
          })
        ) : (
          <EmptyRow text={portfolios.length ? "검색 조건에 맞는 portfolio artifact가 없습니다." : "저장된 portfolio artifact가 없습니다. 없을 때는 기존 단일 전략 게이트를 사용합니다."} />
        )}
      </div>
      <div className="portfolio-pagination-footer">
        <span>
          검색 결과 {visiblePortfolios.length.toLocaleString()}개
          {visiblePortfolios.length ? ` · ${pageStart + 1}-${pageEnd}` : ""}
        </span>
        <div className="portfolio-pagination-controls" aria-label="포트폴리오 Artifact 페이지">
          <label>
            <span>페이지당</span>
            <select value={pageSize} onChange={(event) => setPageSize(Number(event.target.value))}>
              {[10, 20, 50].map((size) => <option key={size} value={size}>{size}개</option>)}
            </select>
          </label>
          <button disabled={currentPage <= 1} onClick={() => setPage(currentPage - 1)} type="button">이전</button>
          {pageNumbers.map((pageNumber, index) => (
            pageNumber === "gap" ? (
              <span className="portfolio-pagination-gap" key={`gap-${index}`}>...</span>
            ) : (
              <button
                className={pageNumber === currentPage ? "active" : ""}
                key={pageNumber}
                onClick={() => setPage(pageNumber)}
                type="button"
              >
                {pageNumber}
              </button>
            )
          ))}
          <button disabled={currentPage >= totalPages} onClick={() => setPage(currentPage + 1)} type="button">다음</button>
        </div>
      </div>
    </section>
  );
}

function OperationalSafeguardsPanel({ apiConnected, dryRun, newEntriesBlocked, killSwitch, operatorConfirmed, onConfirm, onDryRun, onEntryBlock, onTestIntent }) {
  return (
    <section className="panel operational-safeguards-panel">
      <PanelHeader title="운영 차단 설정" subtitle="자동화 모드 전환 전에 공통 보호 장치를 확인합니다." />
      <div className="operator-actions">
        <ActionButton
          active={operatorConfirmed}
          className="secondary-button"
          disabled={!apiConnected}
          icon={<BadgeCheck size={16} />}
          label="운용자 확인"
          onClick={onConfirm}
          status={operatorConfirmed ? "success" : undefined}
        />
        <ActionButton
          className={`secondary-button ${dryRun ? "safe-active" : "danger-active"}`}
          disabled={!apiConnected}
          icon={<ShieldCheck size={16} />}
          label="Dry Run"
          onClick={onDryRun}
          status={apiConnected ? (dryRun ? "success" : "error") : undefined}
        />
        <ActionButton
          active={newEntriesBlocked}
          className="secondary-button"
          disabled={!apiConnected}
          icon={<ShieldCheck size={16} />}
          label="신규 진입 차단"
          onClick={onEntryBlock}
          status={apiConnected && newEntriesBlocked ? "success" : undefined}
        />
        <ActionButton
          className="primary-button"
          disabled={!apiConnected}
          icon={<TerminalSquare size={16} />}
          label="테스트 주문 게이트"
          onClick={onTestIntent}
          pendingLabel="확인 중"
          variant="primary"
        />
        <span className={`inline-state ${apiConnected ? (killSwitch ? "danger" : "success") : "warning"}`}>
          {apiConnected ? (killSwitch ? "긴급 차단 켜짐" : "긴급 차단 꺼짐") : "긴급 차단 확인 불가"}
        </span>
      </div>
    </section>
  );
}

function PreTradeDoctorPanel({ snapshot, onNavigate, onReconcile, onPreflight, onWatchdog }) {
  const [running, setRunning] = useState(false);
  const [hasRun, setHasRun] = useState(false);
  const [selectedDoctorId, setSelectedDoctorId] = useState(null);
  const items = buildDoctorItems(snapshot);
  const selectedItem = items.find((item) => item.id === selectedDoctorId) ?? items[0];
  const problemCount = items.filter((item) => item.tone !== "success").length;
  const failCount = items.filter((item) => item.tone === "danger").length;
  const warnCount = items.filter((item) => item.tone === "warning").length;

  async function runDoctor() {
    setRunning(true);
    setHasRun(true);
    try {
      await onReconcile();
      await onWatchdog();
      await onPreflight();
    } finally {
      setRunning(false);
    }
  }

  return (
    <section className="panel doctor-panel">
      <PanelHeader title="실거래 Doctor" subtitle="실계좌 주문 전에 꼭 필요한 항목만 압축해서 점검합니다." />
      <div className="doctor-hero">
        <div>
          <span>점검 결과</span>
          <strong>{hasRun ? (problemCount ? "조치 필요" : "통과") : "대기"}</strong>
          <p>{hasRun ? `hard stop ${failCount}개, warning ${warnCount}개를 확인했습니다.` : "점검 실행 버튼을 눌러 API, 리스크, 체크리스트, 대조 상태를 검사하세요."}</p>
        </div>
        <ActionButton
          className="primary-button doctor-run-button"
          disabled={running || snapshot.api_connected === false}
          icon={<Play size={16} />}
          label="점검 실행"
          onClick={runDoctor}
          pending={running}
          pendingLabel="점검 중"
          status={running ? "pending" : hasRun ? (problemCount ? "error" : "success") : undefined}
          variant="primary"
        />
      </div>
      <div className="doctor-grid">
        {items.map((item) => (
          <StatusCard
            className={`doctor-card ${item.tone} ${selectedItem?.id === item.id ? "selected" : ""}`}
            key={item.id}
            type="button"
            as="button"
            tone={item.tone}
            leading={<span className="doctor-step">{item.index}</span>}
            title={item.title}
            detail={item.detail}
            onClick={() => setSelectedDoctorId(item.id)}
          />
        ))}
      </div>
      {selectedItem && (
        <div className="doctor-detail-panel">
          <div className="doctor-detail-head">
            <div>
              <span>상세 점검</span>
              <strong>{selectedItem.title}</strong>
            </div>
            <button className="mini-button" type="button" onClick={() => onNavigate(selectedItem.targetNav)}>
              관련 탭 열기
            </button>
          </div>
          <div className="doctor-detail-list">
            {selectedItem.details.map((detail) => (
              <SharedStatusRow
                className={`doctor-detail-row ${detail.tone}`}
                tone={detail.tone}
                key={`${selectedItem.id}-${detail.label}-${detail.value}`}
                title={detail.label}
                detail={detail.value}
              />
            ))}
          </div>
        </div>
      )}
    </section>
  );
}

function detailTone(status) {
  return statusTone(status === true ? "pass" : status === false ? "fail" : status);
}

function makeDetail(label, value, status = "neutral") {
  return {
    label,
    value: String(value ?? "-"),
    tone: detailTone(status),
    status: status === "pass" || status === true ? "통과" : status === "warn" ? "주의" : status === "fail" || status === false ? "조치" : "확인",
  };
}

function buildDoctorItems(snapshot) {
  if (snapshot.api_connected === false) {
    const disconnectedItems = [
      ["API / 브로커 연결", "Python API 연결이 필요합니다.", "settings"],
      ["운영 체크리스트", "API 연결 후 현재 체크 상태를 확인합니다.", "gate"],
      ["리스크 한도", "API 연결 후 현재 리스크 한도를 확인합니다.", "gate"],
      ["전략 lifecycle eligibility", "API 연결 후 전략 승인 상태를 확인합니다.", "gate"],
      ["포지션·계좌 대조", "API 연결 후 브로커 대조 상태를 확인합니다.", "accounts"],
      ["Live Watchdog", "API 연결 후 Watchdog 상태를 확인합니다.", "automation"],
      ["최종 Preflight", "API 연결 후 최종 점검을 실행합니다.", "overview"],
    ];
    return disconnectedItems.map(([title, detail, targetNav], index) => ({
      id: `doctor-disconnected-${index + 1}`,
      index: index + 1,
      title,
      detail,
      tone: index === 0 ? "danger" : "warning",
      status: index === 0 ? "연결 필요" : "확인 불가",
      targetNav,
      details: [makeDetail("API 연결", detail, index === 0 ? "fail" : "warn")],
    }));
  }
  const brokerDiagnostics = snapshot.broker_diagnostics ?? [];
  const missingBrokerEnvCount =
    brokerDiagnostics.reduce((count, broker) => count + (broker.env ?? []).filter((item) => !item.present).length, 0) ||
    (snapshot.brokers ?? []).reduce((count, broker) => count + (broker.missing_env?.length ?? 0), 0);
  const brokerConnectionErrors = snapshot.reconciliation?.errors ?? [];
  const configuredBrokerCount = (snapshot.brokers ?? []).filter((broker) => !(broker.missing_env?.length)).length;
  const missingChecklist = (snapshot.checklist ?? []).filter((item) => item.required && !item.checked);
  const riskFailures = (snapshot.risk_checks ?? []).filter((check) => check.status === "fail");
  const riskWarnings = (snapshot.risk_checks ?? []).filter((check) => check.status === "warn");
  const reconciliation = snapshot.reconciliation?.summary ?? {};
  const finalFailures = (snapshot.final_preflight ?? []).filter((check) => check.status === "fail");
  const finalWarnings = (snapshot.final_preflight ?? []).filter((check) => check.status === "warn");
  const strategyBlocked = (snapshot.strategies ?? []).filter((strategy) => !strategy.live_allowed);
  const apiDetails = [
    ...(snapshot.brokers ?? []).map((broker) =>
      makeDetail(
        broker.name,
        broker.missing_env?.length
          ? `환경 변수 ${broker.missing_env.length}개 누락 · ${broker.missing_env.join(", ")}`
          : "API 인증정보가 설정되어 있습니다. 실주문 승격 조건은 최종 Preflight에서 별도로 확인합니다.",
        broker.missing_env?.length ? "fail" : "pass",
      ),
    ),
    ...brokerConnectionErrors.map((error) => makeDetail(`${error.broker_id ?? "브로커"} 계좌 조회`, error.detail ?? "계좌 조회 오류가 발생했습니다.", "fail")),
  ];
  const checklistDetails = (snapshot.checklist ?? []).map((item) => makeDetail(item.label, item.detail, item.checked ? "pass" : item.required ? "fail" : "warn"));
  const riskDetails = (snapshot.risk_checks ?? []).map((check) => makeDetail(check.label, `${check.detail} · ${check.value}`, check.status));
  const strategyDetails = (snapshot.strategies ?? []).map((strategy) =>
    makeDetail(strategy.name, `${strategy.symbol} · ${strategy.block_reason || strategy.lifecycle_status}`, strategy.live_allowed ? "pass" : "warn"),
  );
  const reconciliationDetails = [
    makeDetail("대조 상태", `${reconciliation.status_label ?? reconciliation.status ?? "-"} · 마지막 대조 ${reconciliation.last_run ?? "-"}`, reconciliation.status ?? "neutral"),
    makeDetail("포지션", `${reconciliation.position_count ?? 0}개 · 불일치 ${reconciliation.mismatch_count ?? 0}개`, reconciliation.mismatch_count ? "fail" : "pass"),
    makeDetail("계좌", `${reconciliation.account_count ?? 0}개 · API 필요 ${reconciliation.api_required_count ?? 0}건`, reconciliation.api_required_count ? "warn" : "pass"),
    ...(snapshot.positions ?? []).map((position) => makeDetail(position.symbol, `${position.broker_name} · ${position.status_label} · ${position.detail}`, position.status)),
    ...(snapshot.accounts ?? []).map((account) => makeDetail(account.account, `${account.broker_name} · ${account.currency} · ${account.detail}`, account.status)),
  ];
  const watchdog = snapshot.watchdog ?? fallbackSnapshot.watchdog;
  const watchdogDetails = [
    makeDetail("Watchdog 상태", `${watchdog.status_label ?? watchdog.status} · 마지막 점검 ${watchdog.last_run ?? "-"}`, watchdog.status),
    makeDetail("Fail-closed", `${watchdog.trip_count ?? 0}회 · 마지막 ${watchdog.last_trip ?? "-"}`, watchdog.trip_count ? "warn" : "pass"),
    ...(watchdog.checks ?? []).map((check) => makeDetail(check.label, `${check.detail} · ${check.value}`, check.status)),
  ];
  const finalDetails = [
    ...(snapshot.final_preflight ?? []).map((check) => makeDetail(check.label, check.detail, check.status)),
    ...((snapshot.launch_report?.hard_stops ?? []).map((item) => makeDetail(`Hard stop · ${item.label}`, item.detail, "fail"))),
    ...((snapshot.launch_report?.warnings ?? []).map((item) => makeDetail(`Warning · ${item.label}`, item.detail, "warn"))),
    ...((snapshot.operation_report?.next_actions ?? []).map((action) => makeDetail("다음 조치", action, "warn"))),
  ];

  return [
    {
      id: "doctor-api",
      index: 1,
      title: "API / 브로커 연결",
      detail: missingBrokerEnvCount
        ? `API 환경 변수 ${missingBrokerEnvCount}개가 비어 있습니다.`
        : brokerConnectionErrors.length
          ? `브로커 계좌 조회 오류 ${brokerConnectionErrors.length}개를 확인하세요.`
          : `${configuredBrokerCount}개 브로커의 API 인증정보와 계좌 조회 상태를 확인했습니다.`,
      tone: missingBrokerEnvCount || brokerConnectionErrors.length ? "danger" : "success",
      status: missingBrokerEnvCount || brokerConnectionErrors.length ? "조치" : "통과",
      targetNav: "settings",
      details: apiDetails.length ? apiDetails : [makeDetail("API / 브로커", "점검할 API 문제가 없습니다.", "pass")],
    },
    {
      id: "doctor-checklist",
      index: 2,
      title: "운영 체크리스트",
      detail: missingChecklist.length ? `필수 확인 ${missingChecklist.length}개가 남아 있습니다.` : "필수 운영 확인이 완료되었습니다.",
      tone: missingChecklist.length ? "danger" : "success",
      status: missingChecklist.length ? "조치" : "통과",
      targetNav: "gate",
      details: checklistDetails.length ? checklistDetails : [makeDetail("운영 체크리스트", "점검할 체크리스트가 없습니다.", "pass")],
    },
    {
      id: "doctor-risk",
      index: 3,
      title: "리스크 한도",
      detail: riskFailures.length ? `리스크 차단 ${riskFailures.length}개가 있습니다.` : riskWarnings.length ? `warning ${riskWarnings.length}개를 검토하세요.` : "손실/노출/슬리피지 규칙이 정상입니다.",
      tone: riskFailures.length ? "danger" : riskWarnings.length ? "warning" : "success",
      status: riskFailures.length ? "차단" : riskWarnings.length ? "주의" : "통과",
      targetNav: "risk",
      details: riskDetails.length ? riskDetails : [makeDetail("리스크 한도", "점검할 리스크 항목이 없습니다.", "pass")],
    },
    {
      id: "doctor-strategy",
      index: 4,
      title: "전략 lifecycle eligibility",
      detail: strategyBlocked.length ? `Live-Small/Live 전 전략 ${strategyBlocked.length}개를 검토해야 합니다.` : "Live-Small 이상 전략을 확인했습니다.",
      tone: strategyBlocked.length ? "warning" : "success",
      status: strategyBlocked.length ? "검토" : "통과",
      targetNav: "gate",
      details: strategyDetails.length ? strategyDetails : [makeDetail("전략", "점검할 전략이 없습니다.", "warn")],
    },
    {
      id: "doctor-reconciliation",
      index: 5,
      title: "포지션·계좌 대조",
      detail: reconciliation.mismatch_count ? `불일치 ${reconciliation.mismatch_count}개가 있습니다.` : reconciliation.api_required_count ? `API 조회 필요 ${reconciliation.api_required_count}건이 있습니다.` : "계좌/포지션 대조가 정상입니다.",
      tone: reconciliation.mismatch_count ? "danger" : reconciliation.api_required_count ? "warning" : "success",
      status: reconciliation.mismatch_count ? "조치" : reconciliation.api_required_count ? "API" : "통과",
      targetNav: "accounts",
      details: reconciliationDetails,
    },
    {
      id: "doctor-watchdog",
      index: 6,
      title: "Live Watchdog",
      detail: watchdog.critical_count ? `critical ${watchdog.critical_count}개로 자동화가 차단됩니다.` : watchdog.warning_count ? `warning ${watchdog.warning_count}개를 확인하세요.` : "Watchdog 감시 상태가 정상입니다.",
      tone: watchdog.critical_count ? "danger" : watchdog.warning_count ? "warning" : "success",
      status: watchdog.critical_count ? "차단" : watchdog.warning_count ? "주의" : "통과",
      targetNav: "automation",
      details: watchdogDetails,
    },
    {
      id: "doctor-final",
      index: 7,
      title: "최종 Preflight",
      detail: finalFailures.length ? `hard stop ${finalFailures.length}개가 남아 있습니다.` : finalWarnings.length ? `warning ${finalWarnings.length}개를 확인하세요.` : "최종 점검을 통과했습니다.",
      tone: finalFailures.length ? "danger" : finalWarnings.length ? "warning" : "success",
      status: finalFailures.length ? "차단" : finalWarnings.length ? "주의" : "통과",
      targetNav: "overview",
      details: finalDetails.length ? finalDetails : [makeDetail("최종 Preflight", "점검할 hard stop이 없습니다.", "pass")],
    },
  ];
}

function PageView({ selectedNav, onNavigate, snapshot, searchQuery, children }) {
  const searchResults = buildSearchResults(snapshot, searchQuery);

  return (
    <section className={`page-view ${selectedNav}-view`}>
      <SearchResultsPanel query={searchQuery} results={searchResults} onNavigate={onNavigate} />

      {children}
    </section>
  );
}

function SearchResultsPanel({ query, results, onNavigate }) {
  if (!normalizeSearchText(query)) return null;
  return (
    <section className="search-results-panel" data-testid="search-results" aria-label="검색 결과">
      <div className="search-results-head">
        <strong>검색 결과</strong>
        <span>{results.length}건</span>
      </div>
      <div className="search-result-list">
        {results.length === 0 ? (
          <div className="search-empty-row">
            <TerminalSquare size={15} />
            <span>검색 결과 없음</span>
          </div>
        ) : (
          results.map((item) => (
            <button className="search-result-row" type="button" key={item.id} onClick={() => onNavigate(item.targetNav)}>
              <StatusPill tone={item.tone}>{item.type}</StatusPill>
              <div>
                <strong>{item.label}</strong>
                <span>{item.detail}</span>
              </div>
              <em>{pageProfiles[item.targetNav]?.title ?? item.targetNav}</em>
            </button>
          ))
        )}
      </div>
    </section>
  );
}

function toneLabel(tone) {
  if (tone === "danger") return "위험";
  if (tone === "warning") return "주의";
  if (tone === "success") return "정상";
  if (tone === "info") return "정보";
  return "참고";
}

function NotificationPanel({ items, onNavigate }) {
  return (
    <section className="notification-panel" data-testid="notification-panel" aria-label="알림 목록">
      <div className="notification-head">
        <strong>알림</strong>
        <span>{items.length}건</span>
      </div>
      <div className="notification-list">
        {items.map((item) => (
          <button className="notification-row" type="button" key={item.id} onClick={() => onNavigate(item.targetNav)}>
            <StatusPill tone={item.tone}>{toneLabel(item.tone)}</StatusPill>
            <div>
              <strong>{item.title}</strong>
              <span>{item.detail}</span>
            </div>
          </button>
        ))}
      </div>
    </section>
  );
}

function normalizePromotionStage(stage = "") {
  const normalized = String(stage || "").toLowerCase().replaceAll("_", "-");
  const aliases = {
    "live-small": "before-live-small",
    "live-canary": "before-live-small",
    "live-candidate": "before-live-small",
    "live-active": "live",
    "paper-candidate": "paper",
    "final-tested": "backtested",
  };
  return aliases[normalized] || normalized;
}

function promotionLabel(stage = "") {
  const normalized = normalizePromotionStage(stage);
  const labels = {
    draft: "Draft",
    backtested: "Backtested",
    "before-shadow": "Before Shadow",
    shadowed: "Shadowed",
    papered: "Papered",
    "before-live-small": "Before Live-Small",
    live: "Live",
    paused: "Paused",
    retired: "Retired",
    paper: "Paper",
    approved: "Approved",
  };
  return labels[normalized] || (normalized ? normalized.toUpperCase() : "UNKNOWN");
}

function promotionTone(stage = "") {
  const normalized = normalizePromotionStage(stage);
  if (normalized === "live") return "success";
  if (["before-live-small", "papered", "shadowed", "paper"].includes(normalized)) return "info";
  if (["backtested", "before-shadow", "approved"].includes(normalized)) return "warning";
  if (["retired", "rejected"].includes(normalized)) return "danger";
  if (normalized === "paused") return "warning";
  return "neutral";
}

const STRATEGY_LIFECYCLE_STEPS = [
  { id: "draft", label: "Draft" },
  { id: "backtested", label: "Backtested" },
  { id: "before-shadow", label: "Before Shadow" },
  { id: "shadowed", label: "Shadowed" },
  { id: "papered", label: "Papered" },
  { id: "before-live-small", label: "Before Live-Small" },
  { id: "live", label: "Live" },
];

const navGroups = [
  { id: "operate", label: "주 운영 흐름", itemIds: ["overview", "gate", "functional-test", "accounts", "orders", "risk", "automation"] },
  { id: "records", label: "기록·대응", itemIds: ["incidents", "audit"] },
  { id: "system", label: "시스템", itemIds: ["settings"] },
];

function strategyLifecycleRank(stage = "") {
  const normalized = normalizePromotionStage(stage);
  return STRATEGY_LIFECYCLE_STEPS.findIndex((item) => item.id === normalized);
}

function buildLiveLifecycleTimeline(strategy) {
  const rawStage = strategy?.lifecycle?.status || strategy?.promotion?.stage || strategy?.promotion_stage || strategy?.lifecycle_status || "draft";
  const currentStage = normalizePromotionStage(rawStage);
  const currentRank = strategyLifecycleRank(currentStage);
  const pausedFromRank = strategyLifecycleRank(strategy?.lifecycle?.pausedFrom || strategy?.pausedFrom);
  const effectiveRank = currentStage === "paused" || currentStage === "retired" ? Math.max(pausedFromRank, 0) : currentRank;
  const history = [
    ...(Array.isArray(strategy?.lifecycle?.history) ? strategy.lifecycle.history : []),
    ...(Array.isArray(strategy?.promotion?.history) ? strategy.promotion.history : []),
  ];
  const base = STRATEGY_LIFECYCLE_STEPS.map((step, index) => {
    const stepRank = strategyLifecycleRank(step.id);
    const matchedEvent = [...history].reverse().find((event) => normalizePromotionStage(event?.to) === step.id);
    const state = currentStage === "paused" || currentStage === "retired"
      ? (stepRank <= effectiveRank ? "done" : "pending")
      : stepRank < currentRank
        ? "done"
        : step.id === currentStage
          ? "current"
          : "pending";
    return {
      id: step.id,
      index: index + 1,
      label: step.label,
      state,
      statusLabel: state === "done" ? "완료" : state === "current" ? "현재 단계" : "대기",
      time: formatShortTimelineTime(matchedEvent?.at),
    };
  });
  if (currentStage === "paused" || currentStage === "retired") {
    base.push({
      id: currentStage,
      index: base.length + 1,
      label: promotionLabel(currentStage),
      state: currentStage,
      statusLabel: currentStage === "paused" ? "일시중지" : "폐기/보관",
      time: formatShortTimelineTime(strategy?.lifecycle?.updatedAt || strategy?.promotion?.promotedAt),
    });
  }
  return base;
}

function formatShortTimelineTime(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  const date = new Date(text);
  if (!Number.isNaN(date.getTime())) {
    return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")} ${String(date.getHours()).padStart(2, "0")}:${String(date.getMinutes()).padStart(2, "0")}`;
  }
  return text.replace("T", " ").slice(0, 16);
}

function liveSmallExecutionSummaryForStrategy(strategyId, orders) {
  let successful = 0;
  let blocked = 0;
  (orders || []).forEach((order) => {
    if (!strategyId || String(order.strategy_id) !== String(strategyId)) return;
    if (order.dry_run) return;
    if (String(order.mode || "").toUpperCase() !== "SMALL_LIVE") return;
    if (!order.canary_scope || typeof order.canary_scope !== "object") return;
    if (!String(order.broker_order_id || "").trim() || String(order.broker_order_id) === "-") return;
    const state = String(order.state || "").toLowerCase();
    const queueState = String(order.queue_state || "").toLowerCase();
    if (state === "filled" || queueState === "filled") successful += 1;
    if (["risk_blocked", "adapter_blocked", "failed", "rejected"].includes(state) || ["blocked", "risk_blocked", "failed", "rejected"].includes(queueState)) blocked += 1;
  });
  return { successful, blocked };
}

function buildLivePromotionChecklist(strategy, normalizedStage, execution, summary, operatorConfirmed) {
  const blockerCount = Number(summary?.blocker_count || 0);
  const liveSmallEligible = Boolean(strategy?.live_small_eligible);
  const evidenceGate = strategy?.paper_portfolio_evidence_gate ?? {};
  const evidenceItem = evidenceGate.required
    ? [{
      label: "Portfolio evidence",
      detail: evidenceGate.detail || "Paper Trader의 portfolio rebalance 실행 evidence가 필요합니다.",
      status: evidenceGate.ready ? "PASS" : "BLOCK",
      tone: evidenceGate.ready ? "success" : "danger",
    }]
    : [];
  return [
    {
      label: "승급 단계",
      detail: normalizedStage === "before-live-small" ? "Paper Trader에서 Live-Small 전 단계까지 승급되었습니다." : `${promotionLabel(normalizedStage)} 단계입니다.`,
      status: normalizedStage === "before-live-small" || normalizedStage === "live" ? "PASS" : "WAIT",
      tone: normalizedStage === "before-live-small" || normalizedStage === "live" ? "success" : "warning",
    },
    {
      label: "정적 권한",
      detail: liveSmallEligible ? "live_small_eligible evidence가 있습니다." : "live_small_eligible evidence가 부족합니다.",
      status: liveSmallEligible ? "PASS" : "WAIT",
      tone: liveSmallEligible ? "success" : "warning",
    },
    ...evidenceItem,
    {
      label: "소액 실거래",
      detail: execution.successful >= MIN_LIVE_CANARY_FILLS
        ? `브로커 체결 원장 ${execution.successful}건을 확인했습니다.`
        : `SMALL_LIVE broker-confirmed FILLED ${execution.successful}/${MIN_LIVE_CANARY_FILLS}건`,
      status: execution.successful >= MIN_LIVE_CANARY_FILLS ? "PASS" : "WAIT",
      tone: execution.successful >= MIN_LIVE_CANARY_FILLS ? "success" : "warning",
    },
    {
      label: "차단 주문",
      detail: execution.blocked === 0 ? "소액 실거래 중 차단/실패 주문이 없습니다." : `차단/실패 주문 ${execution.blocked}건이 있습니다.`,
      status: execution.blocked === 0 ? "PASS" : "BLOCK",
      tone: execution.blocked === 0 ? "success" : "danger",
    },
    {
      label: "운용자 확인",
      detail: operatorConfirmed ? "운용자 확인이 켜져 있습니다." : "실거래 전 운용자 확인이 필요합니다.",
      status: operatorConfirmed ? "PASS" : "WAIT",
      tone: operatorConfirmed ? "success" : "warning",
    },
    {
      label: "Readiness blocker",
      detail: blockerCount === 0 ? "현재 hard blocker가 없습니다." : `hard blocker ${blockerCount}개가 남아 있습니다.`,
      status: blockerCount === 0 ? "PASS" : "BLOCK",
      tone: blockerCount === 0 ? "success" : "danger",
    },
  ];
}

function formatKeyValueMap(values = {}) {
  if (!values || !Object.keys(values).length) return "";
  return Object.entries(values)
    .map(([key, value]) => `${key}: ${typeof value === "object" ? JSON.stringify(value) : value}`)
    .join("\n");
}

function formatPercentValue(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "0.00";
  return (numeric * 100).toFixed(2);
}

function statusTone(status) {
  if (
    status === "pass" ||
    status === "connected" ||
    status === "open" ||
    status === "dry_run" ||
    status === "interface_ready" ||
    status === "ready"
  ) {
    return "success";
  }
  if (
    status === "warn" ||
    status === "watch" ||
    status === "adapter_required" ||
    status === "adapter_blocked" ||
    status === "blocked_stub" ||
    status === "api_required" ||
    status === "review_required"
  ) {
    return "warning";
  }
  if (
    status === "fail" ||
    status === "blocked" ||
    status === "missing_credentials" ||
    status === "risk_blocked" ||
    status === "retry_exhausted" ||
    status === "mismatch" ||
    status === "locked"
  ) {
    return "danger";
  }
  if (status === "canceled") return "neutral";
  return "neutral";
}

function AppearanceControlPanel({ appearance, updateAppearance, layoutMode, changeLayoutMode, resetWorkspaceLayout }) {
  const isLayoutEditing = layoutMode === "edit";

  return (
    <AppearanceSettingsPanel
      accent={appearance.accent}
      className="appearance-panel"
      customAccent={appearance.customAccent}
      headerIcon={Palette}
      layoutEditIcon={Unlock}
      layoutEditing={isLayoutEditing}
      layoutEditingIcon={Lock}
      mode={appearance.theme}
      modeOptions={appearanceThemeOptions.map((option) => ({
        icon: option.icon,
        label: option.label,
        value: option.id,
      }))}
      onAccentChange={(accent) => updateAppearance({ accent })}
      onCustomAccentChange={(customAccent) => updateAppearance({ accent: "custom", customAccent })}
      onLayoutEditingChange={(editing) => changeLayoutMode(editing ? "edit" : "locked")}
      onModeChange={(theme) => updateAppearance({ theme })}
      onResetLayout={resetWorkspaceLayout}
      resetIcon={RefreshCw}
    />
  );
}

function BrokerConnectionAssistant({ brokers = [], diagnostics = [], onSave }) {
  const [settings, setSettings] = useState(null);
  const [activeGroup, setActiveGroup] = useState("kis");
  const [draft, setDraft] = useState({});
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState("");

  useEffect(() => {
    let cancelled = false;
    getEnvSettings()
      .then((result) => {
        if (!cancelled) setSettings(result.settings ?? null);
      })
      .catch((error) => {
        if (!cancelled) setMessage(error instanceof Error ? error.message : "env 설정을 읽지 못했습니다.");
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!settings?.fields) return;
    setDraft((current) => {
      const next = { ...current };
      settings.fields.forEach((field) => {
        if (next[field.key] !== undefined) return;
        next[field.key] = field.kind === "secret" ? "" : field.value ?? field.default ?? "";
      });
      return next;
    });
  }, [settings]);

  const groups = settings?.groups?.length ? settings.groups : [
    { id: "kis", label: "주식/ETF", detail: "한국투자증권 Open API" },
    { id: "binance", label: "Binance", detail: "코인 현물" },
    { id: "upbit", label: "Upbit", detail: "KRW 코인" },
    { id: "live-lock", label: "실거래 잠금", detail: "실전 주문 라우트" },
  ];
  const fields = settings?.fields ?? [];
  const visibleFields = fields.filter((field) => field.group === activeGroup);
  const broker = brokers.find((item) => item.broker_id === activeGroup);
  const diagnostic = diagnostics.find((item) => item.broker_id === activeGroup);
  const checkCount = visibleFields.filter((field) => assistantFieldStatus(field, draft[field.key]) !== "done").length;

  function updateDraft(key, value) {
    setDraft((current) => ({ ...current, [key]: value }));
  }

  async function handleSave() {
    const values = {};
    visibleFields.forEach((field) => {
      const value = draft[field.key];
      if (field.kind === "secret" && !String(value || "").trim()) return;
      values[field.key] = value ?? "";
    });
    if (!Object.keys(values).length) return;
    const enablingRealOrders = String(values.LIVE_TRADER_ENABLE_REAL_ORDERS || "").toLowerCase() === "true";
    setSaving(true);
    setMessage("");
    const result = await onSave(values, enablingRealOrders);
    if (result?.settings) setSettings(result.settings);
    setDraft((current) => clearSecretDrafts(visibleFields, current));
    setMessage(result?.ok === false ? result.reason || "저장 실패" : `${Object.keys(values).length}개 설정을 저장했습니다.`);
    setSaving(false);
  }

  return (
    <section className="panel live-connection-assistant-panel">
      <PanelHeader
        title="연결 설정 Assistant"
        subtitle=".env에 저장되는 실거래 브로커 설정을 탭별로 점검합니다."
        suffix={<StatusPill tone={checkCount ? "warning" : "success"}>{checkCount ? `${checkCount} CHECK` : "READY"}</StatusPill>}
      />
      <NestedTabs
        ariaLabel="실거래 연결 설정"
        className="internal-tabs live-assistant-tabs"
        onChange={setActiveGroup}
        options={groups.map((group) => ({ id: group.id, label: group.label, title: group.detail }))}
        variant="cards"
        value={activeGroup}
      />
      {broker && (
        <div
          {...semanticSurfaceProps(
            broker.order_ready ? "success" : broker.status === "missing_credentials" ? "danger" : "warning",
            "live-assistant-summary",
          )}
        >
          <strong>{broker.name}</strong>
          <span>{broker.detail}</span>
          <StatusPill tone={broker.order_ready ? "success" : broker.status === "missing_credentials" ? "danger" : "warning"}>{broker.order_ready ? "READY" : broker.status}</StatusPill>
        </div>
      )}
      {diagnostic && (
        <div className="live-assistant-steps">
          {(diagnostic.steps ?? []).map((step) => (
            <span className={step.status} key={step.key}>{step.label}</span>
          ))}
        </div>
      )}
      <div className="live-assistant-field-grid">
        {visibleFields.map((field) => {
          const value = draft[field.key] ?? "";
          const status = assistantFieldStatus(field, value);
          const fieldTone = status === "done" ? "success" : status === "block" ? "danger" : "neutral";
          const boolChecked = field.kind === "bool" && settingsBooleanValue(value);
          return (
            <label
              {...semanticSurfaceProps(
                fieldTone,
                `live-assistant-field ${field.kind} ${boolChecked ? "active" : ""}`,
              )}
              key={field.key}
            >
              <div>
                <strong>{field.label}</strong>
                <span>{field.detail}</span>
                <em>{field.key}</em>
              </div>
              {field.kind === "bool" ? (
                <ToggleSwitch
                  checked={boolChecked}
                  label={field.label}
                  onChange={(checked) => updateDraft(field.key, checked ? "true" : "false")}
                />
              ) : (
                <input
                  autoComplete={field.kind === "secret" ? "new-password" : undefined}
                  onChange={(event) => updateDraft(field.key, event.target.value)}
                  placeholder={field.kind === "secret" && field.configured ? "저장됨 · 새 값 입력 시 교체" : field.default || field.key}
                  type={field.kind === "secret" ? "password" : "text"}
                  value={String(value)}
                />
              )}
              <StatusPill tone={fieldTone}>{status === "done" ? "DONE" : status === "block" ? "BLOCK" : "WAIT"}</StatusPill>
            </label>
          );
        })}
      </div>
      <div className="live-assistant-save-row">
        <span>{settings?.envPath || ".env"} · secret은 저장 후 화면에 다시 표시하지 않습니다.</span>
        {message && <em>{message}</em>}
        <button className="primary-button compact-button" disabled={saving || !visibleFields.length} onClick={handleSave} type="button">
          <Save size={16} />
          연결 설정 저장
        </button>
      </div>
    </section>
  );
}

function TelegramConnectionPanel() {
  const [connection, setConnection] = useState({
    status: "idle",
    connected: false,
    detail: "Bot API와 채팅 접근 권한을 확인합니다.",
  });

  const refreshConnection = React.useCallback(async () => {
    setConnection((current) => ({ ...current, status: "checking", connected: false }));
    try {
      const payload = await getTelegramConnection();
      setConnection(payload.connection ?? {
        status: "error",
        connected: false,
        detail: "Telegram 연결 응답이 비어 있습니다.",
      });
    } catch (error) {
      setConnection({
        status: "error",
        connected: false,
        checkedAt: new Date().toISOString(),
        detail: error instanceof Error ? error.message : String(error),
      });
    }
  }, []);

  useEffect(() => {
    void refreshConnection();
  }, [refreshConnection]);

  const notificationsMuted = connection.connected && connection.notificationsEnabled === false;
  const statusLabel = connection.connected
    ? notificationsMuted ? "MUTED" : "CONNECTED"
    : connection.status === "checking" ? "CHECKING" : "CHECK";
  const statusTone = connection.connected && !notificationsMuted
    ? "success"
    : connection.status === "error" ? "danger" : "warning";

  return (
    <section className="panel telegram-settings-panel">
      <PanelHeader
        title="Telegram 공통 알림"
        subtitle="메시지를 보내지 않고 Bot API와 채팅 접근 권한을 확인합니다."
        suffix={(
          <StatusPill tone={statusTone}>
            {statusLabel}
          </StatusPill>
        )}
      />
      <TelegramConnectionStatus connection={connection} onRefresh={refreshConnection} />
    </section>
  );
}

function assistantFieldStatus(field, value) {
  const hasDraftValue = String(value ?? "").trim().length > 0;
  const configured = field.kind === "secret" ? Boolean(field.configured || hasDraftValue) : hasDraftValue;
  if (field.key === "LIVE_TRADER_ENABLE_REAL_ORDERS" && String(value).toLowerCase() !== "true") return "wait";
  if (field.required && !configured) return "block";
  if (!configured) return "wait";
  return "done";
}

function clearSecretDrafts(fields, draft) {
  const next = { ...draft };
  fields.forEach((field) => {
    if (field.kind === "secret") next[field.key] = "";
  });
  return next;
}

function formatProcessMemory(bytes) {
  const value = Number(bytes || 0);
  if (!Number.isFinite(value) || value <= 0) return "-";
  if (value >= 1024 ** 3) return `${(value / 1024 ** 3).toFixed(2)} GB`;
  return `${(value / 1024 ** 2).toFixed(1)} MB`;
}

function UnattendedSoakReportCard({ report }) {
  const status = String(report?.status || "IDLE").toUpperCase();
  const verdict = String(report?.verdict || "IDLE").toUpperCase();
  const running = ["RUNNING", "STARTING"].includes(status);
  const failed = ["FAIL", "FAILED", "ERROR", "ABORTED"].includes(verdict)
    || ["FAILED", "ERROR", "CRASHED"].includes(status);
  const tone = failed ? "danger" : running ? "info" : verdict === "PASS" ? "success" : "neutral";
  const progress = Math.max(0, Math.min(100, Number(report?.progressPct || 0)));
  const heartbeat = report?.heartbeat || {};
  const counts = report?.counts || {};
  const resources = report?.resources || {};
  const criteria = Array.isArray(report?.criteria) ? report.criteria : [];
  const failedCriteria = criteria.filter((item) => item?.passed === false);
  const durationSeconds = Math.max(0, Number(report?.durationSeconds || 0));
  const targetDurationSeconds = Math.max(0, Number(report?.targetDurationSeconds || 0));
  return (
    <section {...semanticSurfaceProps(tone, "unattended-soak-card")}>
      <div className="unattended-soak-heading">
        <div>
          <strong>무인 모니터 Soak</strong>
          <span>장시간 실행의 heartbeat, 재연결, 오류와 프로세스 자원을 한 리포트로 확인합니다.</span>
        </div>
        <StatusPill tone={tone}>{running ? status : verdict}</StatusPill>
      </div>
      <div className="unattended-soak-progress" aria-label={`Soak 진행률 ${progress.toFixed(0)}%`}>
        <i style={{ width: `${progress}%` }} />
      </div>
      <div className="unattended-soak-metrics">
        <div><span>진행</span><strong>{progress.toFixed(0)}%</strong><em>{Math.round(durationSeconds / 60)} / {Math.round(targetDurationSeconds / 60) || "-"}분</em></div>
        <div><span>Heartbeat gap</span><strong>{Number(heartbeat.maxGapSeconds || 0).toFixed(1)}초</strong><em>{Number(heartbeat.gapCount || 0)}건 · 기준 {Number(heartbeat.limitSeconds || 0)}초</em></div>
        <div><span>재연결 / 오류</span><strong>{Number(counts.reconnectCount || 0)} / {Number(counts.errorCount || 0)}</strong><em>차단 {Number(counts.blockCount || 0)}건</em></div>
        <div><span>봉 / 판단 / 체결</span><strong>{Number(counts.barCount || 0)} / {Number(counts.decisionCount || 0)} / {Number(counts.fillCount || 0)}</strong><em>실주문 {Number(counts.realOrderCount || 0)}건</em></div>
        <div><span>Peak CPU</span><strong>{Number(resources.peakProcessCpuPercent || 0).toFixed(1)}%</strong><em>프로세스 기준</em></div>
        <div><span>Peak 메모리</span><strong>{formatProcessMemory(resources.peakProcessMemoryBytes)}</strong><em>프로세스 RSS</em></div>
      </div>
      <div className="unattended-soak-footer">
        <span>
          {status === "IDLE"
            ? "아직 생성된 soak run이 없습니다."
            : failedCriteria.length
              ? `${failedCriteria[0].label || failedCriteria[0].id || "기준"} 실패 · ${failedCriteria.length}개 기준 확인 필요`
              : `${criteria.filter((item) => item?.passed === true).length}/${criteria.length || 0} 기준 통과`}
        </span>
        <code title={report?.viewPath || report?.exportPath || ""}>
          {report?.viewPath || report?.exportPath || "리포트 경로 대기"}
        </code>
      </div>
    </section>
  );
}

function AutomationLauncherPanel({
  className = "",
  deploymentContext,
  profiles,
  strategies,
  runnerState,
  onAutomation,
  onStrategyCycle,
  onValidationEvaluate,
  onRuntimeStart,
  onRuntimeStop,
  runtime,
  soakReport,
}) {
  const rows = profiles?.length ? profiles : fallbackSnapshot.automation_profiles;
  const [assetTab, setAssetTab] = useState(rows[0]?.id ?? "stock");
  const [validation, setValidation] = useState(null);
  const [validationLoading, setValidationLoading] = useState(false);
  const [selectedValidationId, setSelectedValidationId] = useState("");
  const [lastValidationResult, setLastValidationResult] = useState(null);
  const [requestedModes, setRequestedModes] = useState({
    stock: "MONITOR",
    crypto: "MONITOR",
  });
  const activeProfile = rows.find((profile) => profile.id === assetTab) ?? rows[0];
  const modes = [
    { id: "MONITOR", label: "MONITOR", icon: Power },
    { id: "SMALL_LIVE", label: "SMALL LIVE", icon: Play },
    { id: "FULL_LIVE", label: "FULL LIVE", icon: LockKeyhole },
  ];
  const tabs = rows.map((profile) => ({
    id: profile.id,
    label: profile.id === "stock" ? "주식/ETF" : "코인",
    detail: `${profile.provider_label} · 전략 ${profile.strategy_count}개`,
  }));
  const validationCandidates = (validation?.candidates ?? []).filter((candidate) => {
    const broker = String(candidate.brokerHint ?? "").toLowerCase();
    const symbol = String(candidate.symbol ?? "").toUpperCase();
    const crypto = ["binance", "binance-futures", "upbit"].includes(broker)
      || symbol.startsWith("KRW-")
      || symbol.endsWith("USDT")
      || symbol.endsWith("USDC");
    return assetTab === "crypto" ? crypto : !crypto;
  });
  const selectedValidation = validationCandidates.find(
    (candidate) => candidate.validationStrategyInstanceId === selectedValidationId,
  ) ?? validationCandidates[0] ?? null;
  const researchShort = validation?.researchShortBundle ?? null;

  async function refreshValidationPlan() {
    setValidationLoading(true);
    try {
      const response = await getValidationSmallLive();
      setValidation(response?.validation ?? null);
    } catch (error) {
      setValidation({
        ok: false,
        reason: error instanceof Error ? error.message : "검증 plan 조회 실패",
        candidates: [],
      });
    } finally {
      setValidationLoading(false);
    }
  }

  useEffect(() => {
    void refreshValidationPlan();
  }, []);

  useEffect(() => {
    const nextId = validationCandidates[0]?.validationStrategyInstanceId ?? "";
    if (!validationCandidates.some((item) => item.validationStrategyInstanceId === selectedValidationId)) {
      setSelectedValidationId(nextId);
      setLastValidationResult(null);
    }
  }, [assetTab, validation, selectedValidationId]);

  async function evaluateSelectedValidation() {
    if (!selectedValidation?.runtimeEvaluationReady || validationLoading) return;
    setValidationLoading(true);
    try {
      const result = await onValidationEvaluate(selectedValidation.validationStrategyInstanceId);
      setLastValidationResult(result);
      if (result?.validation) setValidation(result.validation);
    } finally {
      setValidationLoading(false);
    }
  }

  if (!activeProfile) {
    return (
      <section className={`panel automation-panel ${className}`.trim()}>
        <PanelHeader title="브로커별 자동화" subtitle="실거래 자동화는 자산군과 브로커별로 분리해서 시작합니다." />
        <EmptyRow text="사용 가능한 자동화 프로필이 없습니다." />
      </section>
    );
  }

  const routeStrategies = strategies.filter((strategy) => (activeProfile.id === "stock" ? !isCryptoStrategy(strategy) : isCryptoStrategy(strategy)));
  const routeSchedules = routeStrategies
    .map((strategy) => liveStrategyBarSchedule(strategy, activeProfile.provider))
    .filter(Boolean);
  const runnerText = runnerState?.last_profile === activeProfile.id ? runnerState.last_action : activeProfile.last_action;
  const profileRuntime = runtime?.profiles?.[activeProfile.id] || runtime;
  const runtimeForProfile = profileRuntime?.profileId === activeProfile.id;
  const runtimeRunning = Boolean(profileRuntime?.running && runtimeForProfile);
  const runtimeTone = runtimeRunning
    ? profileRuntime.phase === "DEGRADED" ? "warning" : "success"
    : profileRuntime?.phase === "FAILED" ? "danger" : "neutral";
  const requestedMode = runtimeRunning
    ? String(profileRuntime.mode || "MONITOR").toUpperCase()
    : requestedModes[activeProfile.id] || "MONITOR";
  const expectedRuntimeProfile = deploymentRuntimeProfile(deploymentContext);
  const runtimeBindingBlocked = !deploymentContext?.id
    || !expectedRuntimeProfile
    || expectedRuntimeProfile !== activeProfile.id;
  const runtimeBindingDetail = !deploymentContext?.id
    ? "상단에서 실행할 Deployment를 먼저 선택하세요."
    : !expectedRuntimeProfile
      ? `선택한 Deployment의 broker(${deploymentContext.brokerId || "미확인"})를 runtime profile에 매핑할 수 없습니다.`
      : expectedRuntimeProfile !== activeProfile.id
        ? `선택한 Deployment는 ${expectedRuntimeProfile === "stock" ? "주식/ETF" : "코인"} profile입니다. 같은 자산군 탭에서만 Run할 수 있습니다.`
        : `${deploymentContext.name} · ${deploymentContext.symbol} · ${deploymentContext.id}`;

  return (
    <section className={`panel automation-panel ${className}`.trim()}>
      <PanelHeader title="브로커별 자동화" subtitle="실거래 자동화는 자산군과 브로커별로 분리해서 시작합니다." />
      <NestedTabs
        ariaLabel="자동화 자산군"
        className="internal-tabs automation-profile-tabs"
        onChange={setAssetTab}
        options={tabs.map((tab) => ({ id: tab.id, label: tab.label, title: tab.detail }))}
        variant="cards"
        value={assetTab}
      />
      <div
        {...semanticSurfaceProps(
          runtimeRunning ? "success" : activeProfile.ready ? "info" : "danger",
          `automation-card ${runtimeRunning ? "running" : ""}`,
        )}
      >
        <div className="automation-card-head">
          <div>
            <strong>{activeProfile.title}</strong>
            <span>{activeProfile.provider_label}</span>
          </div>
          <StatusPill tone={runtimeRunning ? "success" : activeProfile.ready ? "info" : "danger"}>
            {runtimeRunning ? "실행 중" : activeProfile.ready ? "대기" : "차단"}
          </StatusPill>
        </div>
        <p>{activeProfile.detail}</p>
        <div className="automation-scope">
          {activeProfile.asset_scope.map((item) => (
            <span key={item}>{item}</span>
          ))}
        </div>
        {activeProfile.id === "crypto" && (
          <div className="provider-toggle" aria-label="코인 거래소 선택">
            {["binance", "upbit"].map((provider) => (
              <button
                className={activeProfile.provider === provider ? "active" : ""}
                type="button"
                key={provider}
                onClick={() => onAutomation(activeProfile.id, activeProfile.enabled, provider, activeProfile.mode)}
              >
                {provider === "binance" ? "Binance" : "Upbit"}
              </button>
            ))}
          </div>
        )}
        <div className="automation-mode-grid">
          {modes.map((mode) => {
            const Icon = mode.icon;
            const active = requestedMode === mode.id;
            return (
              <button
                className={`mode-button ts-action-button ${active ? "active" : ""}`}
                data-action-status={active ? "success" : undefined}
                aria-pressed={active}
                disabled={runtimeRunning}
                type="button"
                key={mode.id}
                onClick={() => setRequestedModes((current) => ({
                  ...current,
                  [activeProfile.id]: mode.id,
                }))}
              >
                <Icon size={16} />
                <span>{mode.label}</span>
                {mode.id !== "MONITOR" && <LockKeyhole size={13} />}
              </button>
            );
          })}
        </div>
        <div {...semanticSurfaceProps(runtimeBindingBlocked ? "danger" : "info", "runtime-deployment-binding")}>
          <StatusPill tone={runtimeBindingBlocked ? "danger" : "info"}>
            {runtimeBindingBlocked ? "RUN BLOCK" : "DEPLOYMENT BOUND"}
          </StatusPill>
          <span>{runtimeBindingDetail}</span>
          <small>모드 버튼은 화면의 실행 요청만 선택하며, Run을 누르기 전에는 runtime 설정을 변경하지 않습니다.</small>
        </div>
        <div className="automation-metrics">
          <div>
            <span>연결 브로커</span>
            <strong>{activeProfile.broker_ids.join(", ")}</strong>
          </div>
          <div>
            <span>전략</span>
            <strong>{activeProfile.strategy_count}개</strong>
          </div>
          <div>
            <span>LIVE 허용</span>
            <strong>{activeProfile.live_strategy_count}개</strong>
          </div>
        </div>
        <div className="adapter-preview">
          <strong>API 요청 미리보기</strong>
          <span>{activeProfile.sample_request?.method} {activeProfile.sample_request?.endpoint}</span>
          <code>{activeProfile.sample_request?.provider} · {activeProfile.sample_request?.can_send ? "전송 가능" : "키/게이트 필요"}</code>
        </div>
        <div className="automation-strategies">
          {routeStrategies.slice(0, 4).map((strategy) => (
            <span key={strategy.strategy_id}>{strategy.name} · {strategy.symbol}</span>
          ))}
          {!routeStrategies.length && <span>연결 가능한 전략 artifact가 없습니다.</span>}
        </div>
        <div className="automation-actions">
          <span>{runtimeRunning ? `${profileRuntime.mode} · 확정 봉 ${profileRuntime.engine?.barCount ?? 0} · 판단 ${profileRuntime.engine?.decisionCount ?? 0} · ${profileRuntime.engine?.lastCycleAt || "새 봉 대기"}` : runnerText}</span>
          <button
            className="ts-action-button"
            type="button"
            disabled={profileRuntime?.running || runtimeBindingBlocked}
            onClick={() => onRuntimeStart(activeProfile.id, requestedMode)}
          >
            <Play size={16} />
            <span>{requestedMode} Run</span>
          </button>
          <button
            className="ts-action-button"
            type="button"
            disabled={!profileRuntime?.running}
            onClick={() => onRuntimeStop(activeProfile.id)}
          >
            <CircleStop size={16} />
            <span>Stop</span>
          </button>
          <button
            className="ts-action-button"
            type="button"
            onClick={() => onStrategyCycle(activeProfile.id)}
          >
            <Play size={16} />
            <span>1회 진단</span>
          </button>
        </div>
        <LiveClosedBarCountdown schedules={routeSchedules} />
        <div {...semanticSurfaceProps(runtimeTone, "continuous-runtime-status")}>
          <StatusPill tone={runtimeTone}>
            {runtimeRunning ? `${profileRuntime.mode} RUNNING` : profileRuntime?.phase || "STOPPED"}
          </StatusPill>
          <span>시세는 계속 수신하고 전략은 확정 봉마다 1회만 평가합니다. HOLD 후에도 다음 봉에서 자동으로 다시 판단합니다.</span>
          {profileRuntime?.lastError && <small data-ts-semantic-preserve="true">{profileRuntime.lastError}</small>}
        </div>
      </div>
      <UnattendedSoakReportCard report={soakReport} />
      <section className="validation-monitor-card">
        <PanelHeader
          title="검증 전용 MONITOR"
          subtitle="Backtest 후보를 Portfolio 또는 명시적 standalone Strategy로 정확히 바인딩해, lifecycle 승급 없이 실제 지표 코드로 1회 평가합니다. 이 경로에는 OrderIntent 생성과 브로커 주문 전송이 없습니다."
          suffix={(
            <button className="mini-button" type="button" disabled={validationLoading} onClick={refreshValidationPlan}>
              <RefreshCcw size={14} />
              새로고침
            </button>
          )}
        />
        <div className="validation-monitor-metrics">
          <MetricCard label="일반·Standalone Smoke" value={`${validation?.generalSmokeCandidateCount ?? 0}개`} tone="info" />
          <MetricCard label="Futures SHORT 정식 후보" value={`${validation?.futuresShortCandidateCount ?? 0}개`} tone={(validation?.futuresShortCandidateCount ?? 0) > 0 ? "success" : "warning"} />
          <MetricCard label="승급 차단 SHORT" value={`${validation?.blockedFuturesShortCount ?? 0}개`} tone={(validation?.blockedFuturesShortCount ?? 0) > 0 ? "warning" : "success"} />
          <MetricCard label="실제 평가 가능" value={`${validation?.runtimeEvaluationReadyCount ?? 0}개`} tone="success" />
          <MetricCard label="주문 가능" value="0개" tone="success" />
        </div>
        <div className="validation-candidate-selector">
          <label>
            <span>{assetTab === "crypto" ? "코인" : "주식/ETF"} 검증 후보</span>
            <select
              value={selectedValidation?.validationStrategyInstanceId ?? ""}
              onChange={(event) => {
                setSelectedValidationId(event.currentTarget.value);
                setLastValidationResult(null);
              }}
            >
              {validationCandidates.map((candidate) => (
                <option key={candidate.validationStrategyInstanceId} value={candidate.validationStrategyInstanceId}>
                  {candidate.symbol} · {candidate.timeframe} · {candidate.positionDirection === "short" ? "SHORT" : "LONG"} · {candidate.runtimeEvaluationReady ? "평가 가능" : "canonical 재발행 필요"}
                </option>
              ))}
            </select>
          </label>
          <button
            className="ts-action-button"
            type="button"
            disabled={!selectedValidation?.runtimeEvaluationReady || validationLoading}
            onClick={evaluateSelectedValidation}
          >
            <Play size={16} />
            <span>{validationLoading ? "확정 봉 로딩 중" : "1회 MONITOR 평가"}</span>
          </button>
        </div>
        {selectedValidation ? (
          <div
            {...semanticSurfaceProps(
              selectedValidation.runtimeEvaluationReady ? "success" : "warning",
              "validation-candidate-detail",
            )}
          >
            <div>
              <strong>{selectedValidation.strategyName || selectedValidation.strategyId}</strong>
              <span>
                {selectedValidation.brokerHint} · {selectedValidation.marketType} · {selectedValidation.plugin}
                {selectedValidation.standaloneStrategy ? " · Standalone Strategy" : ` · ${selectedValidation.portfolioName || selectedValidation.portfolioId}`}
              </span>
              <small>
                {selectedValidation.strategyId} · artifact {selectedValidation.strategyArtifactHash?.slice(0, 12) || "N/A"} · file {selectedValidation.strategyFileSha256?.slice(0, 12) || "N/A"}
              </small>
            </div>
            <StatusPill tone={selectedValidation.runtimeEvaluationReady ? "success" : "warning"}>
              {selectedValidation.runtimeEvaluationReady ? "CANONICAL · MONITOR ONLY" : "LEGACY · 목록 전용"}
            </StatusPill>
          </div>
        ) : (
          <EmptyRow text="이 자산군에 검증 plan 후보가 없습니다." />
        )}
        {lastValidationResult?.ok && (
          <div
            {...semanticSurfaceProps(
              lastValidationResult.decision?.signal === "HOLD" ? "neutral" : "info",
              "validation-evaluation-result",
            )}
          >
            <StatusPill tone={lastValidationResult.decision?.signal === "HOLD" ? "neutral" : "info"}>
              {lastValidationResult.decision?.signal ?? "HOLD"}
            </StatusPill>
            <div>
              <strong>{lastValidationResult.symbol} · {lastValidationResult.timeframe} 확정 봉 평가 완료</strong>
              <span>{lastValidationResult.decision?.reason}</span>
            </div>
            <small>MONITOR · 주문 경로 없음 · 최대 주문금액 0</small>
          </div>
        )}
        {lastValidationResult?.ok === false && (
          <div {...semanticSurfaceProps("danger", "validation-evaluation-error")}>{lastValidationResult.reason}</div>
        )}
        <p className="validation-monitor-note">
          이 검증 plan은 지속 감시 runner와 연결하지 않습니다. 후보 plan을 우회해 장시간 runtime을 시작하지 않으며, Portfolio를 합성하지 않고 표준 SMALL/FULL LIVE 권한도 변경하지 않습니다.
        </p>
        {researchShort && (
          <div
            {...semanticSurfaceProps(
              researchShort.functionalPass ? "success" : "warning",
              "research-short-summary",
            )}
          >
            <div>
              <strong>Binance Futures SHORT 연구 bundle</strong>
              <span>
                {researchShort.strategyCount ?? 0}개 전략 · Portfolio 실행 계약 {researchShort.portfolioExecutionPassed ? "PASS" : "CHECK"} · Shadow/Paper {researchShort.paperPassedStrategyCount ?? 0}/{researchShort.strategyCount ?? 0}
              </span>
            </div>
            <StatusPill tone={researchShort.functionalPass ? "success" : "warning"}>
              {researchShort.functionalPass ? "기능 검증 PASS · 승급 불가" : "검증 확인 필요"}
            </StatusPill>
            <div className="research-short-strategies">
              {(researchShort.strategies ?? []).map((strategy) => (
                <span key={strategy.strategyId}>
                  {strategy.symbol} {strategy.timeframe} · SELL 진입 / BUY reduce-only 청산
                </span>
              ))}
            </div>
            <small>
              researchOnly · 실제 주문 0 · 기존 holdout 재사용으로 production 승급과 Live 권한 부여는 차단됩니다.
            </small>
          </div>
        )}
      </section>
    </section>
  );
}

function isCryptoStrategy(strategy) {
  const text = `${strategy.asset ?? ""} ${strategy.symbol ?? ""}`.toLowerCase();
  return ["crypto", "coin", "btc", "eth", "usdt", "코인"].some((token) => text.includes(token));
}


function RiskSettingsPanel({ settings, onRiskSetting }) {
  function commitChange(event, setting) {
    const nextValue = event.currentTarget.value;
    if (Number(nextValue) !== Number(setting.value)) {
      onRiskSetting(setting.key, nextValue);
    }
  }

  return (
    <section className="panel risk-settings-panel">
      <PanelHeader title="리스크 한도 설정" subtitle="주문 전 게이트에서 사용하는 기본 안전 한도입니다." />
      <div className="settings-list">
        {settings.map((setting) => (
          <div className="setting-row" key={setting.key}>
            <SlidersHorizontal size={16} />
            <div>
              <strong>{setting.label}</strong>
              <span>{setting.detail}</span>
            </div>
            <label>
              <input
                key={`${setting.key}-${setting.value}`}
                type="number"
                defaultValue={setting.value}
                min={setting.min}
                max={setting.max}
                step={setting.step}
                onBlur={(event) => commitChange(event, setting)}
                onKeyDown={(event) => {
                  if (event.key === "Enter") event.currentTarget.blur();
                }}
              />
              <span>{setting.unit}</span>
            </label>
          </div>
        ))}
      </div>
    </section>
  );
}

function WatchdogPanel({ watchdog, onWatchdog, className = "" }) {
  const data = watchdog ?? fallbackSnapshot.watchdog;
  const tone = statusTone(data.status);
  const statusText = data.status_label ?? data.status ?? "대기";
  const checks = data.checks ?? [];
  const metrics = [
    { label: "상태", value: statusText, tone },
    { label: "Critical", value: data.critical_count ?? 0, tone: data.critical_count ? "danger" : "success" },
    { label: "Warning", value: data.warning_count ?? 0, tone: data.warning_count ? "warning" : "success" },
    { label: "Fail-closed", value: data.trip_count ?? 0, tone: data.trip_count ? "warning" : "neutral" },
  ];

  return (
    <section className={`panel watchdog-panel ${className}`.trim()}>
      <PanelHeader title="Live Watchdog" subtitle={`마지막 점검 ${data.last_run ?? "미실행"}`} />
      <div className="panel-action-line">
        <div>
          <strong>{data.last_action ?? "대기"}</strong>
          <span>{data.active_live ? `활성 브로커 ${data.active_brokers?.join(", ") || "-"}` : "자동화 라우트 비활성"}</span>
        </div>
        <ActionButton
          className="secondary-button"
          icon={<Radio size={16} />}
          label="Watchdog 점검"
          onClick={onWatchdog}
          status={tone === "danger" ? "error" : tone === "success" ? "success" : undefined}
        />
      </div>
      <div className="queue-grid watchdog-metrics">
        {metrics.map((item) => (
          <div {...semanticSurfaceProps(item.tone, "queue-card")} key={item.label}>
            <span>{item.label}</span>
            <strong>{item.value}</strong>
            <span className={`summary-state ${item.tone}`}>{item.tone === "success" ? "정상" : "확인"}</span>
          </div>
        ))}
      </div>
      <div className="risk-grid watchdog-grid">
        {checks.map((check) => (
          <SharedStatusRow
            className={`risk-rule ${check.status}`}
            tone={statusTone(check.status)}
            key={check.label}
            title={check.label}
            detail={check.detail}
            value={check.value}
          />
        ))}
        {!checks.length && <EmptyRow text="아직 Watchdog 점검 결과가 없습니다." />}
      </div>
    </section>
  );
}

function OrderQueueSummaryPanel({ summary }) {
  const items = [
    { label: "전체 주문", value: summary.total, tone: summary.total ? "info" : "neutral" },
    { label: "차단", value: summary.blocked, tone: summary.blocked ? "danger" : "success" },
    { label: "Dry Run", value: summary.dry_run, tone: summary.dry_run ? "success" : "neutral" },
    { label: "재시도 가능", value: summary.retryable, tone: summary.retryable ? "warning" : "neutral" },
    { label: "취소", value: summary.canceled, tone: summary.canceled ? "neutral" : "success" },
  ];
  return (
    <section className="panel order-queue-panel">
      <PanelHeader title="주문 큐 요약" subtitle="주문 의도의 현재 생명주기 상태입니다." />
      <div className="queue-grid">
        {items.map((item) => (
          <div {...semanticSurfaceProps(item.tone, "queue-card")} key={item.label}>
            <span>{item.label}</span>
            <strong>{item.value}</strong>
            <span className={`summary-state ${item.tone}`}>{item.tone === "success" ? "정상" : "확인"}</span>
          </div>
        ))}
      </div>
    </section>
  );
}

function RetryPolicyPanel({ policy, onRetryPolicy }) {
  function commitNumber(event, setting) {
    const nextValue = event.currentTarget.value;
    if (Number(nextValue) !== Number(setting.value)) {
      onRetryPolicy(setting.key, nextValue);
    }
  }

  return (
    <section className="panel retry-policy-panel">
      <PanelHeader title="재시도 정책" subtitle="브로커 전송 전 단계에서 사용할 재시도 기준입니다." />
      <div className="settings-list">
        {policy.map((setting) => (
          <div className={`setting-row ${setting.type === "boolean" && settingsBooleanValue(setting.value) ? "active" : ""}`} key={setting.key}>
            <Clock3 size={16} />
            <div>
              <strong>{setting.label}</strong>
              <span>{setting.detail}</span>
            </div>
            {setting.type === "boolean" ? (
              <ToggleSwitch
                checked={settingsBooleanValue(setting.value)}
                className="switch-label"
                label={setting.label}
                onChange={(checked) => onRetryPolicy(setting.key, checked)}
              />
            ) : (
              <label>
                <input
                  key={`${setting.key}-${setting.value}`}
                  type="number"
                  defaultValue={setting.value}
                  min={setting.min}
                  max={setting.max}
                  step={setting.step}
                  onBlur={(event) => commitNumber(event, setting)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter") event.currentTarget.blur();
                  }}
                />
                <span>{setting.unit}</span>
              </label>
            )}
          </div>
        ))}
      </div>
    </section>
  );
}

function numericDisplayValue(value) {
  const parsed = Number(String(value ?? "").replaceAll(",", "").replace(/[^0-9.+-]/g, ""));
  return Number.isFinite(parsed) ? parsed : 0;
}

function compactAllocationValue(value, currency) {
  const numeric = Number(value || 0);
  const unit = String(currency || "").toUpperCase();
  if (unit === "KRW") {
    if (numeric >= 100_000_000) return `${(numeric / 100_000_000).toFixed(1)}억`;
    if (numeric >= 10_000) return `${(numeric / 10_000).toFixed(numeric >= 1_000_000 ? 0 : 1)}만`;
    return Math.round(numeric).toLocaleString("ko-KR");
  }
  if (numeric >= 1_000_000) return `${(numeric / 1_000_000).toFixed(1)}M`;
  if (numeric >= 1_000) return `${(numeric / 1_000).toFixed(1)}K`;
  return numeric.toLocaleString("ko-KR", { maximumFractionDigits: 2 });
}

function AllocationDonut({ group }) {
  const visibleItems = group.items.filter((item) => item.ratio > 0);
  return (
    <div className="account-allocation-donut">
      <svg aria-label={`${group.currency} 계좌 자본 배분`} role="img" viewBox="0 0 120 120">
        <circle className="account-allocation-donut__track" cx="60" cy="60" r="46" pathLength="100" />
        {visibleItems.map((item) => (
          <circle
            className="account-allocation-donut__segment"
            cx="60"
            cy="60"
            key={item.id}
            pathLength="100"
            r="46"
            stroke={item.color}
            strokeDasharray={`${item.ratio * 100} ${100 - item.ratio * 100}`}
            strokeDashoffset={-item.offset * 100}
          />
        ))}
      </svg>
      <div className="account-allocation-donut__center">
        <strong>{compactAllocationValue(group.total, group.currency)}</strong>
        <span>{group.currency}</span>
      </div>
    </div>
  );
}

function AllocationLegend({ currency, items }) {
  return (
    <div className="account-allocation-legend">
      {items.map((item) => (
        <div key={item.id}>
          <span className="account-allocation-legend__marker" style={{ backgroundColor: item.color }} />
          <span className="account-allocation-legend__name">
            <strong>{item.label}</strong>
            <small>{item.basis}</small>
          </span>
          <span className="account-allocation-legend__value">
            <strong>{(item.ratio * 100).toFixed(item.ratio >= 0.1 ? 1 : 2)}%</strong>
            <small>{formatAllocationValue(item.value, currency)}</small>
          </span>
        </div>
      ))}
    </div>
  );
}

function PositionExposureCard({ group }) {
  return (
    <article className="position-exposure-card">
      <header>
        <div>
          <strong>{group.brokerLabel}</strong>
          <span>{group.currency} 기준</span>
        </div>
        <strong>{formatAllocationValue(group.total, group.currency)}</strong>
      </header>
      {group.items.length ? (
        <>
          <div className="position-exposure-bar" aria-label={`${group.brokerLabel} 포지션 비중`}>
            {group.items.map((item) => (
              <span
                key={item.id}
                style={{ backgroundColor: item.color, width: `${Math.max(item.ratio * 100, 0.7)}%` }}
                title={`${item.label} ${(item.ratio * 100).toFixed(1)}%`}
              />
            ))}
          </div>
          <AllocationLegend currency={group.currency} items={group.items} />
        </>
      ) : (
        <div className="account-visual-empty">평가 가능한 포지션 금액이 없습니다.</div>
      )}
      {group.pendingCount ? (
        <p className="position-exposure-card__pending">현재가가 없어 평가 대기 중인 포지션 {group.pendingCount}개는 비중 계산에서 제외했습니다.</p>
      ) : null}
    </article>
  );
}

function AccountAllocationOverview({ accounts, positions }) {
  const model = useMemo(
    () => buildAccountVisualization(accounts, positions),
    [accounts, positions],
  );
  return (
    <section className="panel account-visual-overview">
      <PanelHeader
        title="계좌 자본·포지션 노출"
        subtitle="환율을 임의 추정하지 않고 통화별 자본 배분과 계좌별 포지션 집중도를 분리해 표시합니다."
      />
      <div className="account-visual-summary">
        <div><span>연결 계좌</span><strong>{model.accountCount}개</strong></div>
        <div><span>보유 포지션</span><strong>{model.positionCount}개</strong></div>
        <div><span>기준 통화</span><strong>{model.capitalGroups.length}개</strong></div>
        <div className={model.missingValuationCount ? "is-warning" : ""}>
          <span>평가 대기</span>
          <strong>{model.missingValuationCount}개</strong>
        </div>
      </div>
      <div className="account-visual-section">
        <div className="account-visual-section__heading">
          <div>
            <h3>계좌 자본 배분</h3>
            <p>KRW·USD·USDT를 섞지 않고 브로커가 제공한 총 평가 또는 현금성 잔고를 사용합니다.</p>
          </div>
        </div>
        {model.capitalGroups.length ? (
          <div className="account-allocation-grid">
            {model.capitalGroups.map((group) => (
              <article className="account-allocation-card" key={group.currency}>
                <AllocationDonut group={group} />
                <AllocationLegend currency={group.currency} items={group.items} />
              </article>
            ))}
          </div>
        ) : (
          <div className="account-visual-empty">계좌를 갱신하면 자본 배분을 표시합니다.</div>
        )}
      </div>
      <div className="account-visual-section">
        <div className="account-visual-section__heading">
          <div>
            <h3>계좌별 포지션 집중도</h3>
            <p>현물은 평가금액, 선물은 지갑 자본과 분리된 명목 노출 기준입니다.</p>
          </div>
        </div>
        {model.exposureGroups.length ? (
          <div className="position-exposure-grid">
            {model.exposureGroups.map((group) => (
              <PositionExposureCard group={group} key={`${group.brokerId}:${group.currency}`} />
            ))}
          </div>
        ) : (
          <div className="account-visual-empty">평가 가능한 보유 포지션이 없습니다.</div>
        )}
      </div>
    </section>
  );
}

function UnifiedBrokerAccountPanel({
  accounts,
  executionEvents,
  onBaseline,
  onRefresh,
  positions,
  reconciledAt,
  refreshDisabled,
}) {
  const accountRows = accounts.map((account) => ({
    id: account.broker_id,
    provider: account.broker_id,
    providerLabel: account.broker_name,
    accountLabel: `${account.account} · ${account.currency}`,
    balance: account.broker_equity ?? account.broker_cash,
    available: account.broker_cash,
    total: account.broker_equity ?? account.broker_cash,
    detail: account.detail,
    statusLabel: account.status_label,
    tone: statusTone(account.status),
  }));
  const positionRows = positions
    .filter((position) => Math.abs(numericDisplayValue(position.broker_qty)) > 0 || Math.abs(numericDisplayValue(position.program_qty)) > 0)
    .map((position) => {
      const positionSide = String(position.position_side || position.positionSide || "").toUpperCase();
      return {
        id: `${position.broker_id}:${position.symbol}:${positionSide || "NET"}`,
        provider: position.broker_id,
        providerLabel: position.broker_name,
        symbol: position.symbol,
        name: `${position.asset}${positionSide ? ` · ${positionSide}` : ""}`,
        quantity: position.broker_qty,
        averagePrice: position.average_price_display || "조회 정보 없음",
        currentPrice: position.current_price_display || "조회 정보 없음",
        evaluation: position.broker_value_display || "평가 대기",
        profitLoss: position.status === "pass" ? "원장 일치" : `대조 Δ ${position.delta_qty}`,
        profitLossTone: position.status === "pass" ? "success" : "danger",
        brokerValue: position.broker_value,
        brokerQuantity: position.broker_qty_value,
        currency: position.currency,
        positionSide,
        valuationBasis: position.valuation_basis,
      };
    });
  return (
    <>
      <AccountAllocationOverview accounts={accounts} positions={positions} />
      <BrokerAccountWorkspace
        accounts={accountRows}
        autoRefreshLabel="10초 자동 갱신·대조"
        className="live-unified-account-panel"
        emptyMessage="KIS·Binance Spot/Futures·Upbit에 현재 보유 포지션이 없습니다."
        onRefresh={onRefresh}
        positions={positionRows}
        refreshDisabled={refreshDisabled}
        subtitle="KIS·Binance Spot/Futures·Upbit의 실제 계좌 잔고와 보유 포지션을 같은 형식으로 표시합니다."
        title="내 계좌·보유 포지션"
        updatedAt={reconciledAt ?? executionEvents?.last_poll ?? "미조회"}
      />
      <div className="account-baseline-toolbar">
        <div>
          <strong>프로그램 기준 원장</strong>
          <span>자동 갱신은 원장을 덮어쓰지 않습니다. 확인한 현재 계좌를 새 기준으로 삼을 때만 승인하세요.</span>
        </div>
        <button className="secondary-button" disabled={refreshDisabled} onClick={onBaseline} type="button">
          <ShieldCheck size={15} />
          현재 계좌를 기준 원장으로 승인
        </button>
      </div>
    </>
  );
}

function OperationsReportPanel({ report }) {
  return (
    <section className="panel operations-report-panel">
      <PanelHeader title="운용 리포트" subtitle={`생성 시각 ${report.generated_at}`} />
      <div className="compact-list">
        {report.sections.map((section) => (
          <div {...semanticSurfaceProps(statusTone(section.status), "compact-row report-row")} key={section.label}>
            <strong>{section.label}</strong>
            <span>{section.value} · {section.detail}</span>
            <StatusPill tone={statusTone(section.status)}>{section.status}</StatusPill>
          </div>
        ))}
      </div>
    </section>
  );
}

function LaunchReportPanel({ report }) {
  const items = [
    { label: "실주문 잠금", value: report.real_order_lock, tone: statusTone(report.real_order_lock) },
    { label: "SMALL LIVE", value: report.small_live_status, tone: statusTone(report.small_live_status) },
    { label: "FULL LIVE", value: report.full_live_status, tone: statusTone(report.full_live_status) },
    { label: "Hard Stop", value: report.hard_stop_count, tone: report.hard_stop_count ? "danger" : "success" },
    { label: "Warning", value: report.warning_count, tone: report.warning_count ? "warning" : "success" },
  ];
  return (
    <section className="panel launch-report-panel">
      <PanelHeader title="실거래 승인 패키지" subtitle={`마지막 최종 점검 ${report.last_run}`} />
      <div className="launch-lock">
        <ShieldAlert size={18} />
        <div>
          <strong>{report.lock_reason}</strong>
          <span>실제 주문 전송은 모든 hard stop과 warning 해소 후에만 검토합니다.</span>
        </div>
      </div>
      <MetricGrid className="metric-grid">
        {items.map((item) => (
          <MetricCard
            className="metric-card"
            detail={item.tone === "success" ? "해제 가능" : "차단"}
            detailClassName={`summary-state ${item.tone}`}
            key={item.label}
            label={item.label}
            tone={item.tone}
            value={item.value}
          />
        ))}
      </MetricGrid>
      <div className="next-actions">
        {report.next_actions.map((action) => (
          <span key={action}>{action}</span>
        ))}
      </div>
    </section>
  );
}

function LivePromotionReadinessQueue({
  operatorConfirmed,
  onSelect,
  orders = [],
  runtime = {},
  strategies = [],
  summary = {},
}) {
  const [filter, setFilter] = useState("all");
  const rows = useMemo(
    () => buildPromotionReadinessQueue(strategies.map((strategy) => {
      const stage = normalizePromotionLifecycle(
        strategy?.lifecycle?.status
        || strategy?.promotion?.stage
        || strategy?.promotion_stage
        || strategy?.lifecycle_status,
      );
      const execution = liveSmallExecutionSummaryForStrategy(strategy.strategy_id, orders);
      const rawFailureReasons = liveArtifactFailureReasons(strategy);
      const expectedCanaryReasons = stage === "before-live-small"
        ? rawFailureReasons.filter((reason) => /live-activation-required|소액 실거래|canary/i.test(reason))
        : [];
      const blockers = rawFailureReasons.filter((reason) => !expectedCanaryReasons.includes(reason));
      const warnings = [];
      warnings.push(...expectedCanaryReasons);
      let nextAction = "";
      if (stage === "before-live-small") {
        if (strategy.live_small_eligible !== true) {
          blockers.push("live_small_eligible 권한이 없습니다.");
        }
        if (execution.blocked > 0) {
          blockers.push(`현재 canary scope에서 차단·실패 주문 ${execution.blocked}건을 해소해야 합니다.`);
        }
        if (Number(summary?.blocker_count || 0) > 0) {
          blockers.push(`운영 readiness blocker ${Number(summary.blocker_count)}개가 남아 있습니다.`);
        }
        if (execution.successful < MIN_LIVE_CANARY_FILLS) {
          warnings.push(`현재 hash·deployment scope의 실제 체결 ${execution.successful}/${MIN_LIVE_CANARY_FILLS}건`);
        }
        if (!operatorConfirmed) {
          warnings.push("운용자 확인이 필요합니다.");
        }
        nextAction = execution.successful < MIN_LIVE_CANARY_FILLS
          ? `SMALL LIVE canary 체결을 ${MIN_LIVE_CANARY_FILLS - execution.successful}건 더 확인하세요.`
          : "운용자 확인과 최종 preflight 후 정식 Live 승급을 검토하세요.";
      } else if (stage === "papered") {
        nextAction = "Paper Trader에서 current hash의 Live-Small 전 승급 evidence를 확정하세요.";
      } else if (stage === "live") {
        nextAction = "승급 완료 상태입니다. 무인 모니터와 정기 재검증을 계속하세요.";
      }
      return {
        blockers,
        detail: `${strategy.symbol || "-"} · ${strategy.timeframe || "-"} · ${strategy.plugin_label || strategy.plugin || "전략"}`,
        id: strategy.strategy_id,
        name: strategy.name || strategy.strategy_id,
        nextAction,
        running: liveArtifactRunning(runtime, strategy.strategy_id),
        stage,
        warnings,
      };
    })),
    [operatorConfirmed, orders, runtime, strategies, summary],
  );
  const queueSummary = promotionQueueSummary(rows);
  const visibleRows = rows
    .filter((row) => {
      if (filter === "ready") return ["READY", "OBSERVING"].includes(row.status);
      if (filter === "check") return row.status === "CHECK";
      if (filter === "block") return row.status === "BLOCK";
      return true;
    })
    .slice(0, 10);

  return (
    <section className="panel promotion-readiness-queue">
      <PanelHeader
        title="승급 준비 큐"
        subtitle="후보·차단 근거·다음 작업을 모아 봅니다. 표준 lifecycle, preflight와 current-hash evidence를 건너뛰지 않습니다."
        suffix={(
          <StatusPill tone={queueSummary.block ? "warning" : queueSummary.total ? "success" : "neutral"}>
            {queueSummary.ready + queueSummary.observing} READY · {queueSummary.block} BLOCK
          </StatusPill>
        )}
      />
      <div className="promotion-readiness-toolbar" role="group" aria-label="승급 준비 큐 필터">
        {[
          ["all", `전체 ${queueSummary.total}`],
          ["ready", `준비 ${queueSummary.ready + queueSummary.observing}`],
          ["check", `확인 ${queueSummary.check}`],
          ["block", `차단 ${queueSummary.block}`],
        ].map(([value, label]) => (
          <button
            aria-pressed={filter === value}
            className={filter === value ? "active" : ""}
            key={value}
            onClick={() => setFilter(value)}
            type="button"
          >
            {label}
          </button>
        ))}
      </div>
      <div className="promotion-readiness-list">
        {visibleRows.map((row) => {
          const issues = [...row.blockers, ...row.warnings];
          return (
            <article
              {...semanticSurfaceProps(row.tone, `promotion-readiness-row ${row.status.toLowerCase()}`)}
              key={row.id}
            >
              <div className="promotion-readiness-identity">
                <StatusPill tone={row.tone}>{row.status}</StatusPill>
                <div>
                  <strong>{row.name}</strong>
                  <span>{row.id} · {row.detail}</span>
                </div>
              </div>
              <div className="promotion-readiness-stage">
                <span>{row.stageLabel}</span>
                <strong>→ {row.nextStageLabel}</strong>
              </div>
              <div className="promotion-readiness-blockers">
                <span>차단·확인 근거</span>
                <strong>{issues[0] || "현재 확인된 차단 근거 없음"}</strong>
                {issues.length > 1 && <em>외 {issues.length - 1}건</em>}
              </div>
              <div className="promotion-readiness-action">
                <span>다음 작업</span>
                <strong>{row.nextAction}</strong>
              </div>
              <button
                className="secondary-button compact-button"
                onClick={() => onSelect(row.id)}
                type="button"
              >
                Artifact 보기
              </button>
            </article>
          );
        })}
        {!visibleRows.length && <EmptyRow text="이 상태에 해당하는 승급 후보가 없습니다." />}
      </div>
    </section>
  );
}

function LiveStrategySelectorPanel({
  automaticPromotion,
  strategies,
  selectedStrategy,
  onSelect,
  onPromoteLive,
  onStrategyLifecycle,
  orders = [],
  summary = {},
  operatorConfirmed = false,
  metadata,
  onMetadataSave,
}) {
  const parametersText = formatKeyValueMap(selectedStrategy?.parameters);
  const promotionStage = selectedStrategy?.promotion?.stage || selectedStrategy?.promotion_stage || selectedStrategy?.lifecycle_status || "unknown";
  const normalizedStage = normalizePromotionStage(promotionStage);
  const execution = liveSmallExecutionSummaryForStrategy(selectedStrategy?.strategy_id, orders);
  const automaticResult = (automaticPromotion?.results ?? []).find(
    (item) => item.strategyId === selectedStrategy?.strategy_id,
  );
  const lifecycleTimeline = buildLiveLifecycleTimeline(selectedStrategy);
  const canPromoteLive = Boolean(
    selectedStrategy
      && normalizedStage === "before-live-small"
      && selectedStrategy.live_small_eligible
      && execution.successful >= MIN_LIVE_CANARY_FILLS
      && execution.blocked === 0
      && operatorConfirmed
      && Number(summary?.blocker_count || 0) === 0
      && onPromoteLive,
  );
  const isPaused = normalizedStage === "paused";
  const isRetired = normalizedStage === "retired";
  const pausedFromLiveStage = strategyLifecycleRank(
    selectedStrategy?.lifecycle?.pausedFrom || selectedStrategy?.pausedFrom,
  ) >= strategyLifecycleRank("before-live-small");
  const resumeRevalidationRequired = liveArtifactFailureReasons(selectedStrategy).some(
    (reason) => reason.includes("resume-current-paper-live-forward-repromotion-required"),
  );
  return (
    <section className="panel live-strategy-selector-panel">
      <PanelHeader title="활성 전략 선택" subtitle="Backtester/Paper Trader에서 검증된 artifact를 읽기 전용으로 확인합니다." />
      <div className="live-strategy-selector-grid">
        <label>
          <span>전략 artifact</span>
          <select value={selectedStrategy?.strategy_id || ""} onChange={(event) => onSelect(event.target.value)} disabled={!strategies.length}>
            {!strategies.length && <option value="">전략 없음</option>}
            {strategies.map((strategy) => (
              <option key={strategy.strategy_id} value={strategy.strategy_id}>
                {strategy.name} / {strategy.symbol} / {strategy.timeframe}
              </option>
            ))}
          </select>
        </label>
      </div>
      {selectedStrategy ? (
        <>
          <div className="live-strategy-summary-grid">
            <MetricCard className="metric-card" label="전략" value={selectedStrategy.plugin_label || selectedStrategy.plugin} detail={selectedStrategy.strategy_id} />
            <MetricCard className="metric-card" label="대상" value={`${selectedStrategy.symbol} · ${selectedStrategy.timeframe}`} detail={selectedStrategy.asset} />
            <MetricCard className="metric-card" label="Release" value={selectedStrategy.release?.release_id || selectedStrategy.release_id || "-"} detail={selectedStrategy.release?.parameter_hash || "parameter hash 없음"} />
          </div>
          <div className="live-strategy-promotion-line">
            {automaticResult && (
              <StatusPill tone={automaticResult.action === "PROMOTE" ? "success" : automaticResult.action === "PAUSE" ? "danger" : "warning"}>
                AUTO {automaticResult.action}
              </StatusPill>
            )}
            <ActionButton
              className="secondary-button"
              disabled={!canPromoteLive}
              icon={<BadgeCheck size={16} />}
              label="정식 Live 승급"
              onClick={() => onPromoteLive?.(selectedStrategy.strategy_id)}
              status={canPromoteLive ? "success" : undefined}
            />
          </div>
          <div className="strategy-lifecycle-timeline live-lifecycle-timeline" aria-label="전략 승급 타임라인">
            {lifecycleTimeline.map((item) => (
              <article className={item.state} key={item.id}>
                <span>{item.index}</span>
                <div>
                  <strong>{item.label}</strong>
                  <em>{item.time || item.statusLabel}</em>
                </div>
              </article>
            ))}
          </div>
          <div className="live-strategy-control-line">
            {isPaused ? (
              <ActionButton
                className="secondary-button"
                disabled={!selectedStrategy || isRetired}
                icon={<Play size={16} />}
                label="재개"
                onClick={() => onStrategyLifecycle?.(selectedStrategy.strategy_id, "resume")}
              />
            ) : (
              <ActionButton
                className="secondary-button"
                disabled={!selectedStrategy || isRetired}
                icon={<Pause size={16} />}
                label="일시중지"
                onClick={() => onStrategyLifecycle?.(selectedStrategy.strategy_id, "pause")}
              />
            )}
            <ActionButton
              className="danger-button"
              disabled={!selectedStrategy || isRetired}
              icon={<Trash2 size={16} />}
              label="폐기/보관"
              onClick={() => onStrategyLifecycle?.(selectedStrategy.strategy_id, "retire")}
            />
            <span>
              {isRetired
                ? "retired 상태라 신규 주문과 승급이 차단됩니다."
                : isPaused && pausedFromLiveStage
                  ? "Live 단계 재개는 current-hash Paper live-forward 증거와 현재 Portfolio gate를 다시 통과해야 합니다."
                  : isPaused
                    ? "비-Live 단계로 안전 재개하며 실거래 권한은 복구하지 않습니다."
                  : resumeRevalidationRequired
                    ? "이전 Live 권한은 복구되지 않았습니다. Paper Trader에서 연속 관찰 증거로 다시 승급하세요."
                    : "상태 변경은 공유 전략 artifact lifecycle에 기록됩니다."}
            </span>
          </div>
          <div className="live-strategy-parameter-panel">
            <strong>Parameters</strong>
            <pre>{parametersText || "-"}</pre>
          </div>
          <ArtifactMetadataEditor artifactId={selectedStrategy.strategy_id} artifactType="strategy" metadata={metadata} onSave={onMetadataSave} />
        </>
      ) : (
        <EmptyRow text="이 자산군에 표시할 전략 artifact가 없습니다." />
      )}
    </section>
  );
}

function StrategyDiscoveryToolbar({
  filters,
  onFilterChange,
  stageOptions,
  timeframeOptions,
  pluginOptions,
  failureOptions,
  visibleCount,
  totalCount,
  savedSearches,
  savedSearchId,
  savedSearchName,
  onSavedSearchNameChange,
  onSavedSearchApply,
  onSavedSearchSave,
  onSavedSearchDelete,
  onReset,
}) {
  const activeLabels = [
    filters.query && `검색: ${filters.query}`,
    filters.stage !== "all" && `단계: ${promotionLabel(filters.stage)}`,
    filters.timeframe !== "all" && `주기: ${filters.timeframe}`,
    filters.plugin !== "all" && `전략 유형: ${filters.plugin}`,
    filters.failure !== "all" && `실패 이유: ${filters.failure}`,
    filters.quick !== "all" && `빠른 필터: ${artifactQuickFilterLabel(filters.quick)}`,
  ].filter(Boolean);
  return (
    <section className="panel strategy-discovery-panel">
      <PanelHeader title="전략 찾기" subtitle="이름·ID·종목·파라미터를 검색하고, 자주 쓰는 조건은 저장해서 다시 불러옵니다." />
      <div className="strategy-discovery-primary">
        <label className="strategy-discovery-search">
          <Search size={16} />
          <input
            type="search"
            value={filters.query}
            onChange={(event) => onFilterChange("query", event.target.value)}
            placeholder="이름, ID, 종목, 파라미터, 차단 사유 검색"
          />
        </label>
        <label><span>현재 단계</span><select value={filters.stage} onChange={(event) => onFilterChange("stage", event.target.value)}><option value="all">전체 단계</option>{stageOptions.map((value) => <option key={value} value={value}>{promotionLabel(value)}</option>)}</select></label>
        <label><span>주기</span><select value={filters.timeframe} onChange={(event) => onFilterChange("timeframe", event.target.value)}><option value="all">전체 주기</option>{timeframeOptions.map((value) => <option key={value} value={value}>{value}</option>)}</select></label>
        <label><span>전략 유형</span><select value={filters.plugin} onChange={(event) => onFilterChange("plugin", event.target.value)}><option value="all">전체 유형</option>{pluginOptions.map((value) => <option key={value} value={value}>{value}</option>)}</select></label>
        <label><span>정렬</span><select value={filters.sort} onChange={(event) => onFilterChange("sort", event.target.value)}><option value="updated-desc">최근 갱신순</option><option value="name-asc">이름순</option><option value="stage-desc">단계 높은순</option><option value="stage-asc">단계 낮은순</option></select></label>
      </div>
      <div className="strategy-discovery-saved">
        <select aria-label="저장된 전략 검색" value={savedSearchId} onChange={(event) => onSavedSearchApply(event.target.value)}>
          <option value="">저장된 검색 불러오기</option>
          {savedSearches.map((item) => <option key={item.id} value={item.id}>{item.name} · {item.sourceApp}</option>)}
        </select>
        <input aria-label="검색 조건 이름" value={savedSearchName} onChange={(event) => onSavedSearchNameChange(event.target.value)} placeholder="검색 조건 이름" />
        <button className="secondary-button compact-button" type="button" disabled={!savedSearchName.trim()} onClick={onSavedSearchSave}><Save size={15} />조건 저장</button>
        <button className="secondary-button compact-button icon-only trash-icon-button" type="button" disabled={!savedSearchId} onClick={onSavedSearchDelete} aria-label="저장 검색 삭제"><Trash2 size={15} /></button>
      </div>
      <div className="artifact-quick-filters">
        {[
          ["all", "전체"],
          ["favorite", "즐겨찾기"],
          ["recent-used", "최근 사용"],
          ["recent-promoted", "최근 승급"],
          ["running", "현재 실행 중"],
        ].map(([value, label]) => (
          <button className={filters.quick === value ? "active" : ""} key={value} type="button" onClick={() => onFilterChange("quick", value)}>
            {value === "favorite" && <Star size={13} />}
            {label}
          </button>
        ))}
        <select value={filters.failure} onChange={(event) => onFilterChange("failure", event.target.value)} aria-label="실패 이유 필터">
          <option value="all">모든 실패 이유</option>
          {failureOptions.map((value) => <option key={value} value={value}>{value}</option>)}
        </select>
      </div>
      <div className="strategy-discovery-summary">
        <span><strong>{visibleCount}</strong> / {totalCount}개 표시</span>
        <div>{activeLabels.map((label) => <em key={label}>{label}</em>)}</div>
        <button className="secondary-button compact-button" type="button" onClick={onReset} disabled={!activeLabels.length && filters.sort === DEFAULT_STRATEGY_DISCOVERY_FILTERS.sort}>조건 초기화</button>
      </div>
    </section>
  );
}

function artifactMetadataKey(artifactId, artifactType = "strategy") {
  return `${artifactType === "portfolio" ? "portfolio" : "strategy"}:${String(artifactId || "").trim()}`;
}

function paginationNumbers(currentPage, totalPages) {
  if (totalPages <= 7) {
    return Array.from({ length: totalPages }, (_, index) => index + 1);
  }
  const pages = [1];
  const start = Math.max(2, currentPage - 1);
  const end = Math.min(totalPages - 1, currentPage + 1);
  if (start > 2) pages.push("gap");
  for (let page = start; page <= end; page += 1) {
    pages.push(page);
  }
  if (end < totalPages - 1) pages.push("gap");
  pages.push(totalPages);
  return pages;
}

function liveArtifactFailureReasons(artifact) {
  const permissions = artifact?.permissions && typeof artifact.permissions === "object" ? artifact.permissions : {};
  const policy = artifact?.strategy_policy && typeof artifact.strategy_policy === "object"
    ? artifact.strategy_policy
    : artifact?.strategyPolicy && typeof artifact.strategyPolicy === "object"
      ? artifact.strategyPolicy
      : {};
  return [...new Set([
    artifact?.failure_reasons,
    artifact?.failureReasons,
    artifact?.fail_reasons,
    artifact?.failure_modes,
    artifact?.failureModes,
    artifact?.blockers,
    artifact?.block_reason,
    permissions.fail_reasons,
    permissions.failReasons,
    policy.failureModes,
  ].flatMap((value) => Array.isArray(value) ? value : value ? [value] : [])
    .map((value) => String(value || "").trim())
    .filter(Boolean))];
}

function artifactMatchesQuickFilter(metadata, quick, running = false) {
  if (quick === "favorite") return metadata?.favorite === true;
  if (quick === "running") return running;
  const cutoff = Date.now() - 14 * 24 * 60 * 60 * 1000;
  if (quick === "recent-used") return Date.parse(metadata?.lastUsedAt || "") >= cutoff;
  if (quick === "recent-promoted") return Date.parse(metadata?.lastPromotedAt || "") >= cutoff;
  return true;
}

function artifactQuickFilterLabel(value) {
  return {
    favorite: "즐겨찾기",
    "recent-used": "최근 사용",
    "recent-promoted": "최근 승급",
    running: "현재 실행 중",
  }[value] || "전체";
}

function liveArtifactRunning(runtime, artifactId) {
  if (!runtime?.running || !artifactId) return false;
  return JSON.stringify(runtime).includes(String(artifactId));
}

function ArtifactMetadataEditor({ artifactId, artifactType, metadata, onSave, compact = false }) {
  const [tags, setTags] = useState((metadata?.tags || []).join(", "));
  const [note, setNote] = useState(metadata?.note || "");
  useEffect(() => {
    setTags((metadata?.tags || []).join(", "));
    setNote(metadata?.note || "");
  }, [artifactId, metadata?.note, metadata?.tags]);
  if (!artifactId || !onSave) return null;
  return (
    <details className={`artifact-metadata-editor ${compact ? "compact" : ""}`}>
      <summary>
        <Star size={14} fill={metadata?.favorite ? "currentColor" : "none"} />
        {metadata?.favorite ? "즐겨찾기 · 태그·메모" : "즐겨찾기·태그·메모"}
      </summary>
      <div>
        <button type="button" className="secondary-button compact-button" onClick={() => onSave(artifactId, artifactType, { favorite: !metadata?.favorite })}>
          <Star size={13} fill={metadata?.favorite ? "currentColor" : "none"} />{metadata?.favorite ? "즐겨찾기 해제" : "즐겨찾기"}
        </button>
        <label><span>태그</span><input value={tags} onChange={(event) => setTags(event.target.value)} placeholder="추세, Binance, 소액" /></label>
        <label><span>메모</span><textarea value={note} onChange={(event) => setNote(event.target.value)} maxLength={4000} /></label>
        <button type="button" className="secondary-button compact-button" onClick={() => onSave(artifactId, artifactType, {
          tags: tags.split(",").map((tag) => tag.trim()).filter(Boolean),
          note,
        })}><Save size={13} />저장</button>
      </div>
    </details>
  );
}

function liveStrategyStageId(strategy) {
  const raw = strategy?.lifecycle?.status || strategy?.promotion?.stage || strategy?.promotion_stage || strategy?.lifecycle_status || "draft";
  const normalized = normalizePromotionStage(raw);
  return STRATEGY_LIFECYCLE_STEPS.some((step) => step.id === normalized) ? normalized : normalized || "draft";
}

function uniqueStrategyDiscoveryValues(values) {
  return [...new Set(values.map((value) => String(value || "").trim()).filter(Boolean))]
    .sort((left, right) => left.localeCompare(right, "ko"));
}

function liveStrategyMatchesDiscovery(strategy, filters, metadata, running = false) {
  const query = String(filters.query || "").trim().toLocaleLowerCase();
  const stage = liveStrategyStageId(strategy);
  const plugin = strategy.plugin_label || strategy.plugin || "";
  const queryMatches = !query || [
    strategy.name,
    strategy.strategy_id,
    strategy.symbol,
    strategy.asset,
    strategy.timeframe,
    plugin,
    strategy.block_reason,
    strategy.permission_label,
    metadata?.note,
    ...(metadata?.tags || []),
    promotionLabel(stage),
    JSON.stringify(strategy.parameters || {}),
    JSON.stringify(strategy.release || {}),
  ].some((value) => String(value || "").toLocaleLowerCase().includes(query));
  return queryMatches
    && (filters.stage === "all" || stage === filters.stage)
    && (filters.timeframe === "all" || strategy.timeframe === filters.timeframe)
    && (filters.plugin === "all" || plugin === filters.plugin)
    && (filters.failure === "all" || liveArtifactFailureReasons(strategy).includes(filters.failure))
    && artifactMatchesQuickFilter(metadata, filters.quick, running);
}

function sortLiveStrategies(strategies, sort) {
  return [...strategies].sort((left, right) => {
    const leftName = left.name || left.strategy_id || "";
    const rightName = right.name || right.strategy_id || "";
    if (sort === "name-asc") return leftName.localeCompare(rightName, "ko");
    if (sort === "stage-desc") return strategyLifecycleRank(liveStrategyStageId(right)) - strategyLifecycleRank(liveStrategyStageId(left)) || leftName.localeCompare(rightName, "ko");
    if (sort === "stage-asc") return strategyLifecycleRank(liveStrategyStageId(left)) - strategyLifecycleRank(liveStrategyStageId(right)) || leftName.localeCompare(rightName, "ko");
    const leftDate = left.updated_at || left.updatedAt || left.release?.created_at || "";
    const rightDate = right.updated_at || right.updatedAt || right.release?.created_at || "";
    return String(rightDate).localeCompare(String(leftDate)) || leftName.localeCompare(rightName, "ko");
  });
}

function AuditPanel({
  audit = [],
  detailLabel = "기술 로그 상세",
  emptyText = "검색 조건에 맞는 로그가 없습니다.",
  subtitle = "사용자용 감사 기록과 분리된 개발·운영 진단 로그입니다.",
  title = "기술 로그",
}) {
  const [query, setQuery] = useState("");
  const [channel, setChannel] = useState("all");
  const [level, setLevel] = useState("all");
  const [sort, setSort] = useState("latest");
  const [selectedLogId, setSelectedLogId] = useState("");
  const rows = audit.map((item, index) => {
    const logChannel = inferLogChannel(item);
    const rawLevel = String(item.level || "INFO").toUpperCase();
    const normalizedLevel = item.level === "danger" || ["ERROR", "CRITICAL", "FAIL", "FAILED"].includes(rawLevel) ? "ERROR" : item.level === "warn" || ["WARN", "WARNING"].includes(rawLevel) ? "WARN" : "INFO";
    const displayTime = formatAuditTime(item);
    const source = item.source || item.event || item.category || "SYSTEM";
    const message = item.message || item.detail || item.reason || "-";
    return {
      id: item.event_id || `${item.timestamp || item.occurred_at || item.occurredAt || item.created_at || item.createdAt || item.time}-${index}`,
      time: displayTime,
      level: normalizedLevel,
      channel: logChannel,
      scope: logChannel,
      module: source,
      source,
      message,
      item,
      raw: `${displayTime} ${normalizedLevel} ${logChannel} ${source} ${message} ${item.trace_id || ""}`.toLowerCase(),
    };
  });
  const visibleRows = rows
    .filter((row) => channel === "all" || row.channel === channel)
    .filter((row) => level === "all" || row.level === level)
    .filter((row) => !query.trim() || row.raw.includes(query.trim().toLowerCase()))
    .sort((a, b) => (sort === "latest" ? rows.indexOf(a) - rows.indexOf(b) : rows.indexOf(b) - rows.indexOf(a)));
  const channels = ["all", ...Array.from(new Set(rows.map((row) => row.channel)))];
  const selectedLog = visibleRows.find((row) => row.id === selectedLogId) || visibleRows[0] || null;
  const handleExportLogs = () => {
    const exportRows = visibleRows.map((row) => ({
      time: row.time || "",
      scope: row.scope || "",
      level: row.level || "",
      source: row.source || "",
      message: row.message || "",
    }));
    downloadCsv(["time", "scope", "level", "source", "message"], exportRows, `live-trader-logs-${dateStamp()}.csv`);
  };

  return (
    <section className="panel audit-panel">
      <PanelHeader title={title} subtitle={subtitle} />
      <MasterDetailLog
        className="logs-workbench audit-log-workspace"
        classes={{
          detailPane: "log-detail-panel audit-log-detail",
          list: "table-scroll compact-table logs-table",
        }}
        detailAriaLabel={`선택한 ${detailLabel}`}
        detailHeader={<h3>{detailLabel}</h3>}
        emptyDetail={<EmptyRow text="상세를 볼 로그가 없습니다." />}
        emptyList={(
          <table aria-label={`${title} 목록`} role="grid">
            <thead>
              <tr>
                <th>시간</th>
                <th>Scope</th>
                <th>Level</th>
                <th>Source</th>
                <th>메시지</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td colSpan={5}>
                  <EmptyRow text={emptyText} />
                </td>
              </tr>
            </tbody>
          </table>
        )}
        getItemKey={(row) => row.id}
        itemRole="row"
        items={visibleRows}
        listAriaLabel={`${title} 목록`}
        onSelectedKeyChange={(key) => setSelectedLogId(key)}
        renderDetail={(row) => {
          const detailFields = auditDetailFields(row.item);
          const payload = formatAuditPayload(row.item);
          return (
            <>
              <p>{row.message}</p>
              <dl>
                <div><dt>시각</dt><dd>{row.time}</dd></div>
                <div><dt>Scope · Level</dt><dd>{row.scope} · {row.level}</dd></div>
                <div><dt>Source</dt><dd>{row.source}</dd></div>
                <div><dt>Session</dt><dd>{row.item.session_id || row.item.sessionId || "-"}</dd></div>
                <div><dt>Deployment</dt><dd>{row.item.deployment_id || row.item.deploymentId || "-"}</dd></div>
                <div><dt>Strategy · Symbol</dt><dd>{row.item.strategy_id || "-"} · {row.item.symbol || "-"}</dd></div>
                <div><dt>Order</dt><dd>{row.item.order_id || row.item.orderId || "-"}</dd></div>
                <div><dt>Correlation</dt><dd>{row.item.correlation_id || row.item.trace_id || "-"}</dd></div>
                {detailFields.map((field) => <div key={field.label}><dt>{field.label}</dt><dd>{field.value}</dd></div>)}
              </dl>
              {(row.item.stack_trace || row.item.stackTrace) && <pre>{row.item.stack_trace || row.item.stackTrace}</pre>}
              {payload ? <><h4>Payload</h4><pre>{payload}</pre></> : null}
            </>
          );
        }}
        renderList={({ items, selectedKey, getItemProps }) => (
          <table aria-label={`${title} 목록`} role="grid">
            <thead>
              <tr>
                <th>시간</th>
                <th>Scope</th>
                <th>Level</th>
                <th>Source</th>
                <th>메시지</th>
              </tr>
            </thead>
            <tbody>
              {items.map((row, index) => (
                  <tr
                    {...getItemProps(row, index, { className: selectedKey === row.id ? "is-selected" : "" })}
                    key={row.id}
                  >
                    <td>{row.time}</td>
                    <td><span className={`scope-pill scope-${logToken(row.scope)}`}>{row.scope}</span></td>
                    <td><span className={`level-pill level-${logToken(row.level)}`}>{row.level}</span></td>
                    <td>{row.source}</td>
                    <td className="log-message-cell" title={row.message}>{row.message}</td>
                  </tr>
              ))}
            </tbody>
          </table>
        )}
        selectedKey={selectedLog?.id || ""}
        toolbar={(
          <div className="logs-toolbar">
            <label className="search-box logs-search">
              <input aria-label={`${title} 검색`} value={query} onChange={(event) => setQuery(event.currentTarget.value)} placeholder="메시지, scope, 작업명 검색" />
              <Search size={18} />
            </label>
            <select aria-label={`${title} 채널 필터`} value={channel} onChange={(event) => setChannel(event.currentTarget.value)}>
              {channels.map((item) => (
                <option value={item} key={item}>
                  {item === "all" ? "전체" : item}
                </option>
              ))}
            </select>
            <select aria-label={`${title} 레벨 필터`} value={level} onChange={(event) => setLevel(event.currentTarget.value)}>
              <option value="all">전체</option>
              <option value="INFO">INFO</option>
              <option value="WARN">WARN</option>
              <option value="ERROR">ERROR</option>
            </select>
            <select aria-label={`${title} 정렬`} value={sort} onChange={(event) => setSort(event.currentTarget.value)}>
              <option value="latest">최신순</option>
              <option value="oldest">오래된순</option>
            </select>
            <button className="logs-export-button" type="button" onClick={handleExportLogs} disabled={!visibleRows.length}>
              <Download size={16} />
              CSV
            </button>
            <span>{visibleRows.length.toLocaleString()} / {rows.length.toLocaleString()}개</span>
          </div>
        )}
      />
    </section>
  );
}

const AUDIT_PAYLOAD_SECRET_KEY = /(authorization|api[-_]?key|secret|password|passwd|token|credential|private[-_]?key|chat[-_]?id)/i;

function firstAuditValue(item, ...keys) {
  for (const key of keys) {
    const value = item?.[key];
    if (value !== undefined && value !== null && String(value).trim() !== "") return String(value);
  }
  return "";
}

function auditDetailFields(item = {}) {
  return [
    { label: "Decision", value: firstAuditValue(item, "decision") },
    { label: "State", value: firstAuditValue(item, "state") },
    { label: "Reason", value: firstAuditValue(item, "reason") },
    { label: "Risk gate", value: firstAuditValue(item, "risk_gate", "riskGate") },
    { label: "Run ID", value: firstAuditValue(item, "run_id", "runId") },
    { label: "Passport ID", value: firstAuditValue(item, "passport_id", "passportId") },
  ].filter((field) => field.value);
}

function sanitizeAuditPayload(value, depth = 0) {
  if (depth >= 5) return "[depth limited]";
  if (Array.isArray(value)) return value.slice(0, 30).map((item) => sanitizeAuditPayload(item, depth + 1));
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).slice(0, 60).map(([key, item]) => [
        key,
        AUDIT_PAYLOAD_SECRET_KEY.test(key) ? "[redacted]" : sanitizeAuditPayload(item, depth + 1),
      ]),
    );
  }
  if (typeof value === "string") return value.length > 600 ? `${value.slice(0, 600)}…` : value;
  return value;
}

function formatAuditPayload(item = {}) {
  let payload = item.payload ?? item.payload_json ?? item.payloadJson;
  if (payload === undefined || payload === null || payload === "") return "";
  if (typeof payload === "string") {
    try {
      payload = JSON.parse(payload);
    } catch {
      payload = payload.length > 2000 ? `${payload.slice(0, 2000)}…` : payload;
    }
  }
  try {
    const text = JSON.stringify(sanitizeAuditPayload(payload), null, 2);
    return text.length > 5000 ? `${text.slice(0, 5000)}\n…` : text;
  } catch {
    return "[payload를 표시할 수 없습니다.]";
  }
}

function logToken(value = "") {
  return String(value || "unknown").trim().toLowerCase().replace(/[^a-z0-9가-힣]+/g, "-").replace(/^-+|-+$/g, "") || "unknown";
}

function inferLogChannel(item) {
  const text = `${item.source ?? item.event ?? ""} ${item.message ?? item.detail ?? ""} ${item.category ?? ""}`.toLowerCase();
  if (text.includes("주문") || text.includes("order")) return "ORDER";
  if (text.includes("api") || text.includes("broker") || text.includes("kis") || text.includes("binance")) return "API";
  if (text.includes("전략") || text.includes("contract") || text.includes("artifact")) return "STRATEGY";
  if (text.includes("risk") || text.includes("리스크") || item.level === "danger") return "RISK";
  return "SYSTEM";
}

function StatusRow({ label, status, detail, value }) {
  const tone = statusTone(status);
  const statusLabel = status === "pass" ? "통과" : status === "warn" ? "주의" : status === "fail" ? "조치" : status === "na" ? "해당 없음" : status;
  const pillLabel = status === "pass" ? "pass" : status === "warn" ? "warn" : status === "fail" ? "fail" : status === "na" ? "wait" : status;
  return (
    <SharedStatusRow
      className={`status-row ${status}`}
      tone={tone}
      title={label}
      detail={value ? `${value} · ${detail}` : detail}
      badge={<StatusPill aria-label={`상태: ${statusLabel}`} tone={tone}>{pillLabel}</StatusPill>}
    />
  );
}

function EmptyRow({ text }) {
  return (
    <EmptyState className="empty-row" icon={<TerminalSquare size={16} />} message={text} />
  );
}

export default App;
