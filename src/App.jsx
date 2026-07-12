import * as React from "react";
import { useEffect, useRef, useState } from "react";
import {
  BadgeCheck,
  Bell,
  CircleStop,
  Clock3,
  DatabaseZap,
  Download,
  FileClock,
  KeyRound,
  LayoutDashboard,
  ListChecks,
  Lock,
  LockKeyhole,
  Moon,
  Network,
  PanelLeft,
  Pause,
  Play,
  Power,
  Radio,
  RefreshCcw,
  RotateCcw,
  Save,
  Search,
  Settings,
  ShieldAlert,
  ShieldCheck,
  SlidersHorizontal,
  Sun,
  TerminalSquare,
  Trash2,
  Unlock,
  WalletCards,
} from "lucide-react";
import {
  cancelOrder,
  getEnvSettings,
  getSnapshot,
  getUiSettings,
  runFinalPreflight,
  runReconciliation,
  runStrategyCycle,
  runWatchdog,
  promoteStrategyToLive,
  seedProgramLedgerBaseline,
  retryOrder,
  setFlag,
  setStrategyLifecycle,
  setAutomationProfile,
  setMode,
  syncExecutionEvents,
  setRetryPolicy,
  setRiskSetting,
  saveUiSettings,
  saveEnvSettings,
  submitTestIntent,
  runPolicyReplay,
} from "./api";
import { createActionButton } from "../../../packages/design/action-button.js";
import { createStatusPill } from "../../../packages/design/status-pill.js";
import {
  createEmptyState,
  createFormField,
  createIconButton,
  createMetricCard,
  createMetricGrid,
  createPanelHeader,
  createSegmentedControl,
  createStatusCard,
  createStatusRow,
  createToggleSwitch,
} from "../../../packages/design/ui-primitives.js";
import designTokens from "../../../packages/design/design_tokens.json";

const ActionButton = createActionButton(React);
const StatusPill = createStatusPill(React);
const EmptyState = createEmptyState(React);
const FormField = createFormField(React);
const IconButton = createIconButton(React);
const MetricCard = createMetricCard(React);
const MetricGrid = createMetricGrid(React);
const PanelHeader = createPanelHeader(React);
const SegmentedControl = createSegmentedControl(React);
const StatusCard = createStatusCard(React);
const SharedStatusRow = createStatusRow(React);
const ToggleSwitch = createToggleSwitch(React);

const navItems = [
  { id: "overview", label: "사전점검", icon: LayoutDashboard },
  { id: "gate", label: "실거래 준비", icon: ListChecks },
  { id: "automation", label: "자동화", icon: Power },
  { id: "audit", label: "로그", icon: FileClock },
  { id: "brokers", label: "API", icon: Network },
  { id: "settings", label: "설정", icon: Settings },
];

const pageProfiles = {
  overview: {
    title: "사전점검",
    eyebrow: "Live Doctor",
    summary: "실거래 전 API, 리스크, 체크리스트, 대조 상태를 한 번에 점검합니다.",
  },
  gate: {
    title: "실거래 준비",
    eyebrow: "자산군별 준비",
    summary: "주식/ETF와 코인의 전략, 리스크 한도, 운영 차단 설정을 분리해서 준비합니다.",
  },
  automation: {
    title: "자동화",
    eyebrow: "브로커별 실행",
    summary: "주식/ETF는 한국투자증권, 코인은 Binance 또는 Upbit로 분리해 자동화를 시작합니다.",
  },
  brokers: {
    title: "API",
    eyebrow: "브로커/API 관리",
    summary: "KIS/Binance 연결, 환경 변수, 주문 어댑터, 인터페이스 계약을 한곳에서 관리합니다.",
  },
  audit: {
    title: "로그",
    eyebrow: "운영 기록",
    summary: "모드 전환, 주문 차단, 설정 변경 이력을 추적합니다.",
  },
  settings: {
    title: "설정",
    eyebrow: "화면/운영 환경",
    summary: "테마, 강조 색상, 레이아웃 편집 같은 화면 환경만 관리합니다.",
  },
};

const fallbackSnapshot = {
  generated_at: "-",
  mode: "MONITOR",
  dry_run: true,
  kill_switch: false,
  new_entries_blocked: true,
  operator_confirmed: false,
  summary: { status: "blocked", blocker_count: 1, warning_count: 0, live_strategy_count: 0, broker_ready_count: 0 },
  sessions: [],
  readiness: [{ label: "Python API", status: "fail", detail: "Python server connection is required." }],
  risk_checks: [],
  risk_settings: [],
  checklist: [],
  retry_policy: [],
  order_queue: { total: 0, blocked: 0, dry_run: 0, retryable: 0, canceled: 0 },
  watchdog: {
    last_run: "미실행",
    status: "idle",
    status_label: "대기",
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
      status: "warn",
      status_label: "API 필요",
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
const NOTIFICATION_ACK_STORAGE_KEY = "live-trader.notifications.ack.v1";
const LEGACY_THEME_STORAGE_KEY = "live-trader.ui-theme.v1";
const LAYOUT_RESET_EVENT = "live-trader-layout-reset";
const LAYOUT_RESTORE_EVENT = "live-trader-layout-restore";
const LAYOUT_SNAP_SIZE = 8;
const LAYOUT_COLLISION_GAP = 8;
const LAYOUT_MAX_DIMENSION = 100000;
const LAYOUT_MAX_OFFSET = 100000;
const MIN_PANEL_WIDTH = 260;
const MIN_PANEL_HEIGHT = 150;

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
  saveUiSettings(payload).catch(() => {});
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
  const whiteContrast = 1.05 / (luminance + 0.05);
  const darkContrast = (luminance + 0.05) / 0.05;
  return whiteContrast >= darkContrast ? "#ffffff" : "#0f172a";
}

function accentColorForContrast(accent, customAccent) {
  if (accent === "custom") return normalizeHexColor(customAccent);
  return normalizeHexColor(accentPalettes[accent]?.swatch ?? fallbackAccentSwatch, fallbackAccentSwatch);
}

function customAccentVars(color) {
  const primary = normalizeHexColor(color);
  const { r, g, b } = hexToRgb(primary);
  return {
    "--primary": primary,
    "--primary-hover": `color-mix(in srgb, ${primary} 70%, #000000)`,
    "--primary-border": `color-mix(in srgb, ${primary} 76%, #ffffff)`,
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
  accent: fallbackAccentId,
  customAccent: fallbackAccentSwatch,
};

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
  root.style.setProperty("--accent-contrast-text", accentContrastText(accentColorForContrast(appearance.accent, appearance.customAccent)));
}

function readAppearance() {
  try {
    const raw = window.localStorage.getItem(APPEARANCE_STORAGE_KEY);
    if (raw) return normalizeAppearance(JSON.parse(raw));
    return normalizeAppearance({ theme: window.localStorage.getItem(LEGACY_THEME_STORAGE_KEY) });
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
      targetNav: "gate",
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
      targetNav: "brokers",
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
    addSearchResult(results, query, {
      id: `position-${position.symbol}`,
      type: "포지션",
      label: position.symbol,
      detail: `${position.status_label} · ${position.broker_name}`,
      meta: [position.asset, position.currency, position.detail].join(" "),
      targetNav: "overview",
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
      targetNav: "overview",
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

  return results;
}

function buildNotificationItems(snapshot, error) {
  const items = [];
  const push = (item) => {
    if (items.length < 14) items.push(item);
  };

  if (error) {
    push({ id: "api-error", tone: "danger", title: "API 연결 오류", detail: error, targetNav: "overview" });
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
        targetNav: "brokers",
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
      targetNav: "overview",
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

function snapLayoutOffset(value, axis) {
  const snapped = Math.round(Number(value) / LAYOUT_SNAP_SIZE) * LAYOUT_SNAP_SIZE;
  return clampNumber(snapped, -LAYOUT_MAX_OFFSET, LAYOUT_MAX_OFFSET, 0);
}

function panelRectsOverlap(left, right, gap = LAYOUT_COLLISION_GAP) {
  return (
    left.left < right.right + gap &&
    left.right + gap > right.left &&
    left.top < right.bottom + gap &&
    left.bottom + gap > right.top
  );
}

function panelOverlapsPeers(activePanel) {
  const scope = activePanel.closest(".page-view") || activePanel.parentElement;
  if (!scope) return false;
  const activeRect = activePanel.getBoundingClientRect();
  return Array.from(scope.querySelectorAll(".panel")).some((panel) => {
    if (panel === activePanel || !(panel instanceof HTMLElement)) return false;
    const rect = panel.getBoundingClientRect();
    return rect.width > 0 && rect.height > 0 && panelRectsOverlap(activeRect, rect);
  });
}

function applyPanelOffset(panel, position = {}) {
  const nextX = snapLayoutOffset(position.x, "x");
  const nextY = snapLayoutOffset(position.y, "y");
  panel.style.removeProperty("transform");
  if (Math.abs(nextX) > 0) {
    panel.dataset.layoutOffsetX = String(nextX);
    panel.style.marginLeft = `${nextX}px`;
  } else {
    delete panel.dataset.layoutOffsetX;
    panel.style.removeProperty("margin-left");
  }
  if (Math.abs(nextY) > 0) {
    panel.dataset.layoutOffsetY = String(nextY);
    panel.style.marginTop = `${nextY}px`;
  } else {
    delete panel.dataset.layoutOffsetY;
    panel.style.removeProperty("margin-top");
  }
}

function clearPanelOffset(panel) {
  delete panel.dataset.layoutOffsetX;
  delete panel.dataset.layoutOffsetY;
  panel.style.removeProperty("transform");
  panel.style.removeProperty("margin-left");
  panel.style.removeProperty("margin-top");
}

function currentPanelOffset(panel, storedPosition = {}) {
  const inlineX = Number(String(panel.style.marginLeft || "0").replace("px", ""));
  const inlineY = Number(String(panel.style.marginTop || "0").replace("px", ""));
  const storedX = Number(storedPosition?.x);
  const storedY = Number(storedPosition?.y);
  return {
    x: snapLayoutOffset(Number.isFinite(inlineX) && Math.abs(inlineX) > 0 ? inlineX : storedX, "x"),
    y: snapLayoutOffset(Number.isFinite(inlineY) && Math.abs(inlineY) > 0 ? inlineY : storedY, "y"),
  };
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

  panel.style.transform = "";
  if (size && Number.isFinite(size.width)) {
    panel.style.width = `${snapLayoutDimension(size.width, "width")}px`;
  }
  if (size && Number.isFinite(size.height)) {
    panel.style.height = `${snapLayoutDimension(size.height, "height")}px`;
  }
  applyPanelOffset(panel, position);
  window.requestAnimationFrame(() => {
    if (!panel.isConnected || !panelOverlapsPeers(panel)) return;
    panel.style.removeProperty("width");
    clearPanelOffset(panel);
  });

  if (panel.querySelector(":scope > .panel-resize-north-west")) return;
  panel.querySelectorAll(":scope > .panel-resize-edge, :scope > .panel-resize-corner").forEach((handle) => handle.remove());

  [
    ["panel-resize-corner panel-resize-north-west", "vertical", "nw"],
    ["panel-resize-corner panel-resize-north-east", "vertical", "ne"],
    ["panel-resize-corner panel-resize-south-east", "vertical", "se"],
    ["panel-resize-corner panel-resize-south-west", "vertical", "sw"],
  ].forEach(([className, orientation, direction]) => {
    const handle = document.createElement("span");
    handle.className = className;
    handle.dataset.resizeDirection = direction;
    handle.setAttribute("role", "separator");
    handle.setAttribute("aria-orientation", orientation);
    handle.setAttribute("aria-label", "패널 크기 조절");
    panel.appendChild(handle);
  });
}

function useEditablePanels(rootRef) {
  useEffect(() => {
    const root = rootRef.current;
    if (!root) return undefined;

    const enhancePanels = () => {
      root.querySelectorAll(".panel").forEach((panel) => ensurePanelHandles(panel));
    };

    const resetLayout = () => {
      root.querySelectorAll(".panel").forEach((panel) => {
        panel.style.width = "";
        panel.style.height = "";
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
          width: Math.round(panel.getBoundingClientRect().width),
          height: Math.round(panel.getBoundingClientRect().height),
        };
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
      const startX = event.clientX;
      const startY = event.clientY;
      let lastValidOffset = startOffset;
      panel.classList.add("dragging-panel");
      document.body.classList.add("is-moving-layout");

      const onMove = (moveEvent) => {
        const nextX = snapLayoutOffset(startOffset.x + moveEvent.clientX - startX, "x");
        const nextY = snapLayoutOffset(startOffset.y + moveEvent.clientY - startY, "y");
        applyPanelOffset(panel, { x: nextX, y: nextY });
        if (panelOverlapsPeers(panel)) {
          applyPanelOffset(panel, lastValidOffset);
          return;
        }
        lastValidOffset = currentPanelOffset(panel, positions[key]);
      };

      const onUp = () => {
        const nextOffset = currentPanelOffset(panel, positions[key]);
        const stored = readStoredMap(PANEL_POSITION_STORAGE_KEY);
        if (Math.abs(nextOffset.x) > 0 || Math.abs(nextOffset.y) > 0) {
          stored[key] = nextOffset;
        } else {
          delete stored[key];
        }
        writeStoredMap(PANEL_POSITION_STORAGE_KEY, stored);
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

    return () => {
      observer.disconnect();
      root.removeEventListener("pointerdown", onPointerDown);
      window.removeEventListener(LAYOUT_RESET_EVENT, resetLayout);
      window.removeEventListener(LAYOUT_RESTORE_EVENT, enhancePanels);
    };
  }, [rootRef]);
}

applyAppearance(readAppearance());
applyLayoutMode(readLayoutMode());

function App() {
  const [snapshot, setSnapshot] = useState(fallbackSnapshot);
  const [selectedNav, setSelectedNav] = useState("overview");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [appearance, setAppearance] = useState(readAppearance);
  const [layoutMode, setLayoutMode] = useState(readLayoutMode);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(readSidebarCollapsed);
  const [searchQuery, setSearchQuery] = useState("");
  const [notificationsOpen, setNotificationsOpen] = useState(false);
  const [acknowledgedNotifications, setAcknowledgedNotifications] = useState(() => readStoredValue(NOTIFICATION_ACK_STORAGE_KEY, ""));
  const workspaceRef = useRef(null);
  const notificationRef = useRef(null);

  useEditablePanels(workspaceRef);

  async function refresh() {
    try {
      const next = await getSnapshot();
      setSnapshot(next);
      setError("");
    } catch (err) {
      setError(err instanceof Error ? err.message : "API 연결 실패");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    refresh();
    const timer = window.setInterval(refresh, 5000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    applyAppearance(appearance);
  }, [appearance]);

  useEffect(() => {
    let cancelled = false;
    getUiSettings()
      .then((result) => {
        if (cancelled || !result?.settings) return;
        restoreUiSettings(result.settings);
        const restoredAppearance = result.settings.appearance ? applyAppearance(result.settings.appearance) : readAppearance();
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
  }, [layoutMode]);

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

  async function runAction(action) {
    setLoading(true);
    try {
      const result = await action();
      setSnapshot(result.snapshot ?? result);
      setError(result.ok === false ? result.reason : "");
      return result;
    } catch (err) {
      const reason = err instanceof Error ? err.message : "요청 실패";
      setError(reason);
      return { ok: false, reason };
    } finally {
      setLoading(false);
    }
  }

  function updateAppearance(partial) {
    setAppearance((current) => {
      const next = applyAppearance({ ...current, ...partial });
      persistUiSettings({ appearance: next });
      return next;
    });
  }

  function changeLayoutMode(mode) {
    const next = applyLayoutMode(mode);
    setLayoutMode(next);
    persistUiSettings({ layoutMode: next });
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

  const title = navItems.find((item) => item.id === selectedNav)?.label ?? "사전점검";
  const canLive = snapshot.summary.blocker_count === 0;
  const canFullLive = canLive && snapshot.summary.warning_count === 0;
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
            <strong>실시간거래소</strong>
          </div>
        </div>
        <nav className="nav-list" aria-label="주요 메뉴">
          {navItems.map((item) => {
            const Icon = item.icon;
            return (
              <button
                className={`nav-item ${selectedNav === item.id ? "active" : ""}`}
                type="button"
                key={item.id}
                onClick={() => setSelectedNav(item.id)}
              >
                <Icon size={17} />
                <span>{item.label}</span>
              </button>
            );
          })}
        </nav>
        <div className="sidebar-footer">
          <span>전체 차단</span>
          <StatusPill tone={snapshot.kill_switch ? "danger" : "success"}>{snapshot.kill_switch ? "ON" : "OFF"}</StatusPill>
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
          </div>
          <div className="topbar-actions">
            <button
              className={`layout-mode-button ${layoutMode === "edit" ? "active" : ""}`}
              type="button"
              aria-pressed={layoutMode === "edit"}
              onClick={() => changeLayoutMode(layoutMode === "edit" ? "locked" : "edit")}
              title={layoutMode === "edit" ? "레이아웃 편집 모드를 끄고 조작을 잠급니다." : "레이아웃 편집 모드를 켜서 패널 크기 조절을 활성화합니다."}
            >
              {layoutMode === "edit" ? <Unlock size={16} /> : <Lock size={16} />}
              <span>{layoutMode === "edit" ? "레이아웃 편집" : "레이아웃 잠금"}</span>
            </button>
            <button className="layout-reset-button" type="button" onClick={resetWorkspaceLayout} title="패널 크기와 위치를 기본값으로 되돌립니다.">
              <RotateCcw size={15} />
              <span>레이아웃 초기화</span>
            </button>
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
              className={`danger-button ${snapshot.kill_switch ? "active" : ""}`}
              type="button"
              onClick={() => runAction(() => setFlag("kill_switch", !snapshot.kill_switch))}
            >
              <CircleStop size={17} />
              긴급 차단
            </button>
          </div>
        </header>

        <WorkspaceContent
          selectedNav={selectedNav}
          onNavigate={navigateWorkspace}
          snapshot={snapshot}
          searchQuery={searchQuery}
          canLive={canLive}
          canFullLive={canFullLive}
          onMode={(mode) => runAction(() => setMode(mode))}
          onConfirm={() => runAction(() => setFlag("operator_confirmed", !snapshot.operator_confirmed))}
          onDryRun={() => runAction(() => setFlag("dry_run", !snapshot.dry_run))}
          onEntryBlock={() => runAction(() => setFlag("new_entries_blocked", !snapshot.new_entries_blocked))}
          onAutomation={(profileId, enabled, provider, mode) => runAction(() => setAutomationProfile(profileId, enabled, provider, mode))}
          onStrategyCycle={(profileId) => runAction(() => runStrategyCycle(profileId))}
          onPromoteLive={(strategyId) => runAction(() => promoteStrategyToLive(strategyId))}
          onStrategyLifecycle={(strategyId, action) => runAction(() => setStrategyLifecycle(strategyId, action))}
          onWatchdog={() => runAction(runWatchdog)}
          onTestIntent={() => runAction(submitTestIntent)}
          onRiskSetting={(name, value) => runAction(() => setRiskSetting(name, value))}
          onRetryPolicy={(name, value) => runAction(() => setRetryPolicy(name, value))}
          onRetryOrder={(orderId) => runAction(() => retryOrder(orderId))}
          onCancelOrder={(orderId) => runAction(() => cancelOrder(orderId))}
          onReconcile={() => runAction(runReconciliation)}
          onPreflight={() => runAction(runFinalPreflight)}
          onProgramLedgerBaseline={() => runAction(seedProgramLedgerBaseline)}
          onExecutionEvents={(brokerId) => runAction(() => syncExecutionEvents(brokerId))}
          onEnvSettings={(values) => runAction(() => saveEnvSettings(values))}
          appearance={appearance}
          updateAppearance={updateAppearance}
          layoutMode={layoutMode}
          changeLayoutMode={changeLayoutMode}
          resetWorkspaceLayout={resetWorkspaceLayout}
        />
      </main>
    </div>
  );
}

function dateStamp(date = new Date()) {
  return date.toISOString().replace(/[-:]/g, "").slice(0, 15);
}

function formatAuditTime(item = {}) {
  const value = item.timestamp || item.datetime || item.createdAt || item.time || "";
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
  const text = String(value).replaceAll('"', '""');
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
  searchQuery,
  canLive,
  canFullLive,
  onMode,
  onConfirm,
  onDryRun,
  onEntryBlock,
  onAutomation,
  onStrategyCycle,
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
  onProgramLedgerBaseline,
  onExecutionEvents,
  onEnvSettings,
  appearance,
  updateAppearance,
  layoutMode,
  changeLayoutMode,
  resetWorkspaceLayout,
}) {
  const modeConsole = (
    <ModeConsole
      mode={snapshot.mode}
      canLive={canLive}
      canFullLive={canFullLive}
      onMode={onMode}
      onConfirm={onConfirm}
      dryRun={snapshot.dry_run}
      onDryRun={onDryRun}
      operatorConfirmed={snapshot.operator_confirmed}
      newEntriesBlocked={snapshot.new_entries_blocked}
      onEntryBlock={onEntryBlock}
      onTestIntent={onTestIntent}
    />
  );
  const renderPage = (content) => (
    <PageView selectedNav={selectedNav} onNavigate={onNavigate} snapshot={snapshot} searchQuery={searchQuery}>
      {content}
    </PageView>
  );

  if (selectedNav === "automation") {
    return renderPage(
      <section className="content-grid">
        <div className="content-column">
          <AutomationLauncherPanel
            profiles={snapshot.automation_profiles}
            strategies={snapshot.strategies}
            runnerState={snapshot.strategy_runner}
            onAutomation={onAutomation}
            onStrategyCycle={onStrategyCycle}
          />
        </div>
        <div className="content-column">
          <WatchdogPanel watchdog={snapshot.watchdog} onWatchdog={onWatchdog} />
          <OrderQueueSummaryPanel summary={snapshot.order_queue} />
          <OrderPanel orders={snapshot.orders} onRetryOrder={onRetryOrder} onCancelOrder={onCancelOrder} />
        </div>
      </section>,
    );
  }

  if (selectedNav === "gate") {
    return renderPage(
      <LivePreparationPanel
        snapshot={snapshot}
        onConfirm={onConfirm}
        onDryRun={onDryRun}
        onEntryBlock={onEntryBlock}
        onTestIntent={onTestIntent}
        onWatchdog={onWatchdog}
        onRiskSetting={onRiskSetting}
        onRetryPolicy={onRetryPolicy}
        onRetryOrder={onRetryOrder}
        onCancelOrder={onCancelOrder}
        onPromoteLive={onPromoteLive}
        onStrategyLifecycle={onStrategyLifecycle}
      />,
    );
  }

  if (selectedNav === "brokers") {
    return renderPage(
      <section className="content-grid">
        <div className="content-column">
          <BrokerPanel brokers={snapshot.brokers} />
        </div>
        <div className="content-column">
          <BrokerCapabilityPanel diagnostics={snapshot.broker_diagnostics} />
        </div>
      </section>,
    );
  }

  if (selectedNav === "audit") {
    return renderPage(
      <section className="audit-page-layout">
        <AuditPanel audit={snapshot.audit} />
      </section>,
    );
  }

  if (selectedNav === "settings") {
    return renderPage(
      <section className="content-grid settings-content-grid">
        <div className="content-column">
          <AppearanceControlPanel
            appearance={appearance}
            updateAppearance={updateAppearance}
            layoutMode={layoutMode}
            changeLayoutMode={changeLayoutMode}
            resetWorkspaceLayout={resetWorkspaceLayout}
          />
          <BrokerConnectionAssistant brokers={snapshot.brokers} diagnostics={snapshot.broker_diagnostics} onSave={onEnvSettings} />
        </div>
      </section>,
    );
  }

  return renderPage(
    <section className="content-grid">
      <div className="content-column">
        <PreTradeDoctorPanel
          snapshot={snapshot}
          onNavigate={onNavigate}
          onReconcile={onReconcile}
          onPreflight={onPreflight}
          onWatchdog={onWatchdog}
        />
      </div>
      <div className="content-column">
        <ReconciliationSummaryPanel
          reconciliation={snapshot.reconciliation}
          programLedger={snapshot.program_ledger}
          executionEvents={snapshot.execution_events}
          onReconcile={onReconcile}
          onProgramLedgerBaseline={onProgramLedgerBaseline}
          onExecutionEvents={onExecutionEvents}
        />
        <AccountReconciliationPanel accounts={snapshot.accounts ?? []} />
      </div>
    </section>,
  );
}

function LivePreparationPanel({
  snapshot,
  onConfirm,
  onDryRun,
  onEntryBlock,
  onTestIntent,
  onWatchdog,
  onRiskSetting,
  onRetryPolicy,
  onPromoteLive,
  onStrategyLifecycle,
}) {
  const [assetTab, setAssetTab] = useState("stock");
  const [selectedStrategyId, setSelectedStrategyId] = useState("");
  const isStock = assetTab === "stock";
  const filteredStrategies = (snapshot.strategies ?? []).filter((strategy) => (isStock ? !isCryptoStrategy(strategy) : isCryptoStrategy(strategy)));
  const selectedStrategy = filteredStrategies.find((strategy) => strategy.strategy_id === selectedStrategyId) ?? filteredStrategies[0] ?? null;
  const tabItems = [
    { id: "stock", label: "주식/ETF", detail: "한국투자증권 KIS", count: (snapshot.strategies ?? []).filter((strategy) => !isCryptoStrategy(strategy)).length },
    { id: "crypto", label: "코인", detail: "Binance / Upbit", count: (snapshot.strategies ?? []).filter(isCryptoStrategy).length },
  ];

  useEffect(() => {
    const firstId = filteredStrategies[0]?.strategy_id ?? "";
    const stillVisible = filteredStrategies.some((strategy) => strategy.strategy_id === selectedStrategyId);
    if (!stillVisible && selectedStrategyId !== firstId) {
      setSelectedStrategyId(firstId);
    }
  }, [filteredStrategies, selectedStrategyId]);

  return (
    <section className="live-prep-shell">
      <div className="internal-tabs prep-tabs" role="tablist" aria-label="실거래 준비 자산군">
        {tabItems.map((item) => (
          <button
            className={assetTab === item.id ? "active" : ""}
            type="button"
            key={item.id}
            onClick={() => setAssetTab(item.id)}
          >
            <strong>{item.label}</strong>
            <span>{item.detail} · 전략 {item.count}개</span>
          </button>
        ))}
      </div>
      <section className="content-grid">
        <div className="content-column">
          <LiveStrategySelectorPanel
            strategies={filteredStrategies}
            selectedStrategy={selectedStrategy}
            onSelect={setSelectedStrategyId}
            onPromoteLive={onPromoteLive}
            onStrategyLifecycle={onStrategyLifecycle}
            orders={snapshot.orders ?? []}
            summary={snapshot.summary ?? {}}
            operatorConfirmed={Boolean(snapshot.operator_confirmed)}
          />
          <PortfolioArtifactPanel portfolios={snapshot.portfolios ?? []} selectedStrategy={selectedStrategy} />
          <StrategyPanel strategies={filteredStrategies} selectedStrategyId={selectedStrategy?.strategy_id} onSelect={setSelectedStrategyId} />
          <OperationalSafeguardsPanel
            dryRun={snapshot.dry_run}
            newEntriesBlocked={snapshot.new_entries_blocked}
            killSwitch={snapshot.kill_switch}
            onDryRun={onDryRun}
            onEntryBlock={onEntryBlock}
            onTestIntent={onTestIntent}
          />
          <WatchdogPanel watchdog={snapshot.watchdog} onWatchdog={onWatchdog} />
        </div>
        <div className="content-column">
          <RiskSettingsPanel settings={snapshot.risk_settings} onRiskSetting={onRiskSetting} />
          <RetryPolicyPanel policy={snapshot.retry_policy} onRetryPolicy={onRetryPolicy} />
        </div>
      </section>
    </section>
  );
}

function PortfolioArtifactPanel({ portfolios = [], selectedStrategy }) {
  const gate = selectedStrategy?.portfolio_gate ?? {};
  const evidenceGate = selectedStrategy?.paper_portfolio_evidence_gate ?? {};
  const [replay, setReplay] = useState(null);
  const [replayRunning, setReplayRunning] = useState(false);
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
  return (
    <section className="panel portfolio-artifact-panel">
      <PanelHeader title="포트폴리오 Artifact" subtitle="Live 주문은 선택 전략이 포함된 포트폴리오 universe와 target weight를 기준으로 제한됩니다." />
      <div className="portfolio-gate-summary">
        <div>
          <span>선택 전략 Gate</span>
          <strong>{gate.active ? gate.detail : "단일 전략 기준"}</strong>
        </div>
        <StatusPill tone={!gate.active ? "info" : gate.allowed ? "success" : "danger"}>
          {!gate.active ? "LEGACY" : gate.allowed ? "PASS" : "BLOCK"}
        </StatusPill>
      </div>
      {evidenceGate.required && (
        <div className="portfolio-gate-summary portfolio-evidence-summary">
          <div>
            <span>Paper Portfolio Evidence</span>
            <strong>{evidenceGate.detail || "Paper Trader 리밸런싱 실행 증거를 확인합니다."}</strong>
          </div>
          <StatusPill tone={evidenceGate.ready ? "success" : "danger"}>
            {evidenceGate.ready ? "PASS" : "BLOCK"}
          </StatusPill>
        </div>
      )}
      {gate.active && (
        <div className="portfolio-gate-metrics">
          <MetricCard className="metric-card" label="Portfolio" value={gate.portfolioName || gate.portfolioId || "-"} detail={gate.lifecycleStatus || "-"} />
          <MetricCard className="metric-card" label="Target Weight" value={`${formatPercentValue(gate.targetWeight)}%`} detail={`Symbol max ${formatPercentValue((gate.maxSymbolWeightPct || 0) / 100)}%`} />
          <MetricCard className="metric-card" label="Mandate" value={gate.mandateCompliant === false ? "BLOCK" : "PASS"} detail={(gate.mandateBreaches ?? []).join(" · ") || "위반 없음"} />
          <MetricCard className="metric-card" label="Auto De-risk" value={`${gate.automaticDeRiskAction || "KEEP"} ×${Number(gate.capitalMultiplier ?? 1).toFixed(2)}`} detail={`Stress ${gate.stressPassed === false ? "BLOCK" : "PASS"}`} />
        </div>
      )}
      {gate.active && (
        <div className="operator-actions">
          <ActionButton className="secondary-button" label="정책 Replay" onClick={replayPolicy} status={replayRunning ? "pending" : replay ? "success" : undefined} disabled={replayRunning} />
          {replay && <span className="inline-state success">{replay.eventCount}건 · 결정 변경 {replay.changedDecisionCount} · 원본 불변 {replay.sourceEventsImmutable ? "PASS" : "FAIL"}</span>}
        </div>
      )}
      <div className="portfolio-artifact-list">
        {portfolios.length ? (
          portfolios.map((portfolio) => {
            const permissions = portfolio.permissions ?? {};
            const targetCount = portfolio.target_portfolio?.length ?? 0;
            const strategyCount = portfolio.strategy_instances?.length ?? 0;
            const liveReady = permissions.live_allowed || permissions.live_export_allowed || permissions.live_small_allowed;
            return (
              <article className="portfolio-artifact-item" key={portfolio.id || portfolio.source_path}>
                <div>
                  <strong>{portfolio.name || portfolio.id}</strong>
                  <span>{strategyCount} 전략 · {targetCount} target · {portfolio.lifecycle_status}</span>
                </div>
                <StatusPill tone={liveReady ? "success" : "danger"}>{liveReady ? "LIVE READY" : "LIVE BLOCKED"}</StatusPill>
              </article>
            );
          })
        ) : (
          <EmptyRow text="저장된 portfolio artifact가 없습니다. 없을 때는 기존 단일 전략 게이트를 사용합니다." />
        )}
      </div>
    </section>
  );
}

function OperationalSafeguardsPanel({ dryRun, newEntriesBlocked, killSwitch, onDryRun, onEntryBlock, onTestIntent }) {
  return (
    <section className="panel operational-safeguards-panel">
      <PanelHeader title="운영 차단 설정" subtitle="자동화 모드 전환 전에 공통 보호 장치를 확인합니다." />
      <div className="operator-actions">
        <ActionButton
          className={`secondary-button ${dryRun ? "safe-active" : "danger-active"}`}
          icon={<ShieldCheck size={16} />}
          label="Dry Run"
          onClick={onDryRun}
          status={dryRun ? "success" : "error"}
        />
        <ActionButton
          active={newEntriesBlocked}
          className="secondary-button"
          icon={<ShieldCheck size={16} />}
          label="신규 진입 차단"
          onClick={onEntryBlock}
          status={newEntriesBlocked ? "success" : undefined}
        />
        <ActionButton
          className="primary-button"
          icon={<TerminalSquare size={16} />}
          label="테스트 주문 게이트"
          onClick={onTestIntent}
          pendingLabel="확인 중"
          variant="primary"
        />
        <span className={`inline-state ${killSwitch ? "danger" : "success"}`}>{killSwitch ? "긴급 차단 켜짐" : "긴급 차단 꺼짐"}</span>
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
      await onPreflight();
      await onWatchdog();
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
          disabled={running}
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
            badge={<span className="doctor-status">{item.status}</span>}
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
                leading={<StatusPill tone={detail.tone}>{detail.status}</StatusPill>}
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
  const readinessFails = (snapshot.readiness ?? []).filter((check) => check.status === "fail");
  const readinessWarns = (snapshot.readiness ?? []).filter((check) => check.status === "warn");
  const brokerDiagnostics = snapshot.broker_diagnostics ?? [];
  const missingBrokerEnvCount =
    brokerDiagnostics.reduce((count, broker) => count + (broker.env ?? []).filter((item) => !item.present).length, 0) ||
    (snapshot.brokers ?? []).reduce((count, broker) => count + (broker.missing_env?.length ?? 0), 0);
  const diagnosticFailures = brokerDiagnostics.reduce((count, broker) => count + (broker.steps ?? []).filter((step) => step.status === "fail").length, 0);
  const diagnosticWarnings = brokerDiagnostics.reduce((count, broker) => count + (broker.steps ?? []).filter((step) => step.status === "warn").length, 0);
  const missingCapabilities = brokerDiagnostics.reduce(
    (count, broker) => count + (broker.capabilities ?? []).filter((capability) => !capability.implemented).length,
    0,
  );
  const missingChecklist = (snapshot.checklist ?? []).filter((item) => item.required && !item.checked);
  const riskFailures = (snapshot.risk_checks ?? []).filter((check) => check.status === "fail");
  const riskWarnings = (snapshot.risk_checks ?? []).filter((check) => check.status === "warn");
  const reconciliation = snapshot.reconciliation?.summary ?? {};
  const finalFailures = (snapshot.final_preflight ?? []).filter((check) => check.status === "fail");
  const finalWarnings = (snapshot.final_preflight ?? []).filter((check) => check.status === "warn");
  const strategyBlocked = (snapshot.strategies ?? []).filter((strategy) => !strategy.live_allowed);
  const apiDetails = [
    ...(snapshot.brokers ?? []).flatMap((broker) =>
      (broker.missing_env?.length ? broker.missing_env : broker.required_env ?? []).map((name) =>
        makeDetail(`${broker.name} · ${name}`, broker.missing_env?.includes(name) ? "환경 변수가 비어 있습니다." : "환경 변수가 입력되어 있습니다.", broker.missing_env?.includes(name) ? "fail" : "pass"),
      ),
    ),
    ...brokerDiagnostics.flatMap((broker) => [
      ...(broker.steps ?? []).map((step) => makeDetail(`${broker.name} · ${step.label}`, step.detail, step.status)),
      ...(broker.capabilities ?? []).map((capability) => makeDetail(`${broker.name} · ${capability.label}`, capability.detail, capability.implemented ? "pass" : "warn")),
    ]),
    ...readinessFails.map((check) => makeDetail(check.label, check.detail, check.status)),
    ...readinessWarns.map((check) => makeDetail(check.label, check.detail, check.status)),
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
        : diagnosticFailures || readinessFails.length
          ? `브로커/API hard stop ${diagnosticFailures + readinessFails.length}개가 남아 있습니다.`
          : missingCapabilities
            ? `주문 capability ${missingCapabilities}개 구현 확인이 필요합니다.`
            : diagnosticWarnings || readinessWarns.length
              ? `API warning ${diagnosticWarnings + readinessWarns.length}개를 검토하세요.`
              : "실거래 API 키와 브로커 어댑터 상태를 확인했습니다.",
      tone: missingBrokerEnvCount || diagnosticFailures || readinessFails.length ? "danger" : missingCapabilities || diagnosticWarnings || readinessWarns.length ? "warning" : "success",
      status: missingBrokerEnvCount || diagnosticFailures || readinessFails.length ? "조치" : missingCapabilities || diagnosticWarnings || readinessWarns.length ? "주의" : "통과",
      targetNav: "brokers",
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
      targetNav: "gate",
      details: riskDetails.length ? riskDetails : [makeDetail("리스크 한도", "점검할 리스크 항목이 없습니다.", "pass")],
    },
    {
      id: "doctor-strategy",
      index: 4,
      title: "전략 lifecycle eligibility",
      detail: strategyBlocked.length ? `Live-Small/Live 전 전략 ${strategyBlocked.length}개를 검토해야 합니다.` : "Live-Small 이상 전략을 확인했습니다.",
      tone: strategyBlocked.length ? "warning" : "success",
      status: strategyBlocked.length ? "검토" : "통과",
      targetNav: "strategies",
      details: strategyDetails.length ? strategyDetails : [makeDetail("전략", "점검할 전략이 없습니다.", "warn")],
    },
    {
      id: "doctor-reconciliation",
      index: 5,
      title: "포지션·계좌 대조",
      detail: reconciliation.mismatch_count ? `불일치 ${reconciliation.mismatch_count}개가 있습니다.` : reconciliation.api_required_count ? `API 조회 필요 ${reconciliation.api_required_count}건이 있습니다.` : "계좌/포지션 대조가 정상입니다.",
      tone: reconciliation.mismatch_count ? "danger" : reconciliation.api_required_count ? "warning" : "success",
      status: reconciliation.mismatch_count ? "조치" : reconciliation.api_required_count ? "API" : "통과",
      targetNav: "overview",
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

function verificationTone(status) {
  const normalized = String(status || "").toLowerCase();
  if (["pass", "passed", "ready", "valid", "ok"].includes(normalized)) return "success";
  if (["fail", "failed", "rejected", "blocked"].includes(normalized)) return "danger";
  if (["watch", "warn", "warning", "unknown"].includes(normalized)) return "warning";
  if (["wait", "empty", "missing"].includes(normalized)) return "neutral";
  return "info";
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
    const state = String(order.state || "").toLowerCase();
    const queueState = String(order.queue_state || "").toLowerCase();
    if (["sent", "filled"].includes(state) || ["sent", "filled"].includes(queueState)) successful += 1;
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
      detail: execution.successful > 0 ? `실제 성공 주문 ${execution.successful}건을 확인했습니다.` : "SMALL_LIVE 실제 성공 주문 1건 이상이 필요합니다.",
      status: execution.successful > 0 ? "PASS" : "WAIT",
      tone: execution.successful > 0 ? "success" : "warning",
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

function portfolioEvidenceLabel(gate = {}) {
  if (!gate.required) return "LEGACY";
  return gate.ready ? "PASS" : "BLOCK";
}

function portfolioEvidenceTone(gate = {}) {
  if (!gate.required) return "info";
  return gate.ready ? "success" : "danger";
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
  const handleCustomAccentChange = (event) => {
    updateAppearance({ accent: "custom", customAccent: event.target.value });
  };

  return (
    <section className="panel appearance-panel">
      <PanelHeader title="화면 / 레이아웃" subtitle="백테스터와 같은 UI 토큰, 강조 색상, 패널 편집 모드를 사용합니다." />
      <div className="settings-control-group">
        <div>
          <strong>모드</strong>
          <span>실거래 콘솔의 밝기와 텍스트 대비를 바꿉니다.</span>
        </div>
        <SegmentedControl
          activeClassName="selected"
          buttonClassName="theme-mode-button"
          className="theme-mode-row"
          onChange={(theme) => updateAppearance({ theme })}
          options={appearanceThemeOptions.map((option) => ({
            icon: option.icon,
            label: option.label,
            value: option.id,
          }))}
          value={appearance.theme}
        />
      </div>

      <div className="settings-control-group">
        <div>
          <strong>강조 색상</strong>
          <span>선택된 메뉴, 주요 버튼, 진행 상태의 기준 색을 정합니다.</span>
        </div>
        <div className="custom-accent-row">
          <label
            className={`custom-accent-picker ${appearance.accent === "custom" ? "selected" : ""}`}
            style={{ "--custom-accent": appearance.customAccent }}
            onClick={() => updateAppearance({ accent: "custom" })}
          >
            <span className="custom-accent-wheel" aria-hidden="true">
              <i />
            </span>
            <span className="custom-accent-label">
              <strong>사용자 색상</strong>
              <em>{appearance.customAccent}</em>
            </span>
            <input
              type="color"
              value={appearance.customAccent}
              aria-label="사용자 강조 색상"
              onInput={handleCustomAccentChange}
              onChange={handleCustomAccentChange}
            />
          </label>
        </div>
      </div>

      <div className="layout-settings-card">
        <div>
          <strong>{isLayoutEditing ? "레이아웃 편집 중" : "레이아웃 잠금 중"}</strong>
          <span>{isLayoutEditing ? "패널 헤더를 끌고 오른쪽 아래 핸들로 크기를 조절할 수 있습니다." : "패널 위치와 크기가 고정되어 실수로 바뀌지 않습니다."}</span>
        </div>
        <div className="layout-settings-actions">
          <button className={`ghost-button ${isLayoutEditing ? "active" : ""}`} type="button" onClick={() => changeLayoutMode(isLayoutEditing ? "locked" : "edit")}>
            {isLayoutEditing ? <Unlock size={16} /> : <Lock size={16} />}
            {isLayoutEditing ? "편집 종료" : "편집 모드"}
          </button>
          <button className="ghost-button" type="button" onClick={resetWorkspaceLayout}>
            <PanelLeft size={16} />
            초기화
          </button>
        </div>
      </div>
    </section>
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
    setSaving(true);
    setMessage("");
    const result = await onSave(values);
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
      <div className="internal-tabs live-assistant-tabs" role="tablist" aria-label="실거래 연결 설정">
        {groups.map((group) => (
          <button
            aria-selected={activeGroup === group.id}
            className={activeGroup === group.id ? "active" : ""}
            key={group.id}
            onClick={() => setActiveGroup(group.id)}
            role="tab"
            type="button"
          >
            <strong>{group.label}</strong>
            <span>{group.detail}</span>
          </button>
        ))}
      </div>
      {broker && (
        <div className="live-assistant-summary">
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
          const boolChecked = field.kind === "bool" && settingsBooleanValue(value);
          return (
            <label className={`live-assistant-field ${field.kind} ${boolChecked ? "active" : ""}`} key={field.key}>
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
              <StatusPill tone={status === "done" ? "success" : status === "block" ? "danger" : "info"}>{status === "done" ? "DONE" : status === "block" ? "BLOCK" : "WAIT"}</StatusPill>
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

function ModeConsole({
  mode,
  canLive,
  canFullLive,
  onMode,
  dryRun,
  onDryRun,
  operatorConfirmed,
  onConfirm,
  newEntriesBlocked,
  onEntryBlock,
  onTestIntent,
}) {
  const modes = [
    { id: "MONITOR", icon: Power, locked: false },
    { id: "SMALL_LIVE", icon: Play, locked: !canLive },
    { id: "FULL_LIVE", icon: LockKeyhole, locked: !canFullLive },
  ];
  return (
    <section className="panel mode-console">
      <PanelHeader title="실거래 모드" subtitle="실계좌 주문은 모든 게이트 통과 후에만 열립니다." />
      <div className="mode-selector">
        {modes.map((item) => {
          const Icon = item.icon;
          return (
            <button
              type="button"
              key={item.id}
              className={`mode-button ts-action-button ${mode === item.id ? "active" : ""}`}
              data-action-status={mode === item.id ? "success" : undefined}
              aria-pressed={mode === item.id}
              onClick={() => onMode(item.id)}
            >
              <Icon size={16} />
              <span>{item.id}</span>
              {item.locked && <LockKeyhole size={13} />}
            </button>
          );
        })}
      </div>
      <div className="operator-actions">
        <ActionButton
          active={operatorConfirmed}
          className="secondary-button"
          icon={<BadgeCheck size={16} />}
          label="운용자 확인"
          onClick={onConfirm}
          status={operatorConfirmed ? "success" : undefined}
        />
        <ActionButton
          className={`secondary-button ${dryRun ? "safe-active" : "danger-active"}`}
          icon={<ShieldCheck size={16} />}
          label="Dry Run"
          onClick={onDryRun}
          status={dryRun ? "success" : "error"}
        />
        <ActionButton
          active={newEntriesBlocked}
          className="secondary-button"
          icon={<ShieldCheck size={16} />}
          label="신규 진입 차단"
          onClick={onEntryBlock}
          status={newEntriesBlocked ? "success" : undefined}
        />
        <ActionButton
          className="primary-button"
          icon={<TerminalSquare size={16} />}
          label="테스트 주문 게이트"
          onClick={onTestIntent}
          pendingLabel="확인 중"
          variant="primary"
        />
      </div>
    </section>
  );
}

function BrokerPanel({ brokers }) {
  return (
    <section className="panel broker-panel">
      <PanelHeader title="브로커/API 연결" subtitle="실거래 API 키와 주문 어댑터 준비 상태입니다." />
      <div className="broker-list">
        {brokers.map((broker) => (
          <div className="broker-row" key={broker.broker_id}>
            <div className="broker-title">
              <KeyRound size={17} />
              <div>
                <strong>{broker.name}</strong>
                <span>{broker.role}</span>
              </div>
            </div>
            <p>{broker.detail}</p>
            <div className="env-list">
              {broker.required_env.map((name) => (
                <span className={broker.missing_env.includes(name) ? "missing" : ""} key={name}>
                  {name}
                </span>
              ))}
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

function AutomationLauncherPanel({ profiles, strategies, runnerState, onAutomation, onStrategyCycle }) {
  const rows = profiles?.length ? profiles : fallbackSnapshot.automation_profiles;
  const [assetTab, setAssetTab] = useState(rows[0]?.id ?? "stock");
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

  if (!activeProfile) {
    return (
      <section className="panel automation-panel">
        <PanelHeader title="브로커별 자동화" subtitle="실거래 자동화는 자산군과 브로커별로 분리해서 시작합니다." />
        <EmptyRow text="사용 가능한 자동화 프로필이 없습니다." />
      </section>
    );
  }

  const routeStrategies = strategies.filter((strategy) => (activeProfile.id === "stock" ? !isCryptoStrategy(strategy) : isCryptoStrategy(strategy)));
  const runnerText = runnerState?.last_profile === activeProfile.id ? runnerState.last_action : activeProfile.last_action;

  return (
    <section className="panel automation-panel">
      <PanelHeader title="브로커별 자동화" subtitle="실거래 자동화는 자산군과 브로커별로 분리해서 시작합니다." />
      <div className="internal-tabs automation-profile-tabs" role="tablist" aria-label="자동화 자산군">
        {tabs.map((tab) => (
          <button
            className={assetTab === tab.id ? "active" : ""}
            type="button"
            key={tab.id}
            onClick={() => setAssetTab(tab.id)}
          >
            <strong>{tab.label}</strong>
            <span>{tab.detail}</span>
          </button>
        ))}
      </div>
      <div className={`automation-card ${activeProfile.enabled ? "running" : ""}`}>
        <div className="automation-card-head">
          <div>
            <strong>{activeProfile.title}</strong>
            <span>{activeProfile.provider_label}</span>
          </div>
          <StatusPill tone={activeProfile.enabled ? "success" : activeProfile.ready ? "info" : "danger"}>
            {activeProfile.enabled ? "실행 중" : activeProfile.ready ? "대기" : "차단"}
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
            const active = activeProfile.mode === mode.id;
            return (
              <button
                className={`mode-button ts-action-button ${active ? "active" : ""}`}
                data-action-status={active ? "success" : undefined}
                aria-pressed={active}
                type="button"
                key={mode.id}
                onClick={() => onAutomation(activeProfile.id, mode.id !== "MONITOR", activeProfile.provider, mode.id)}
              >
                <Icon size={16} />
                <span>{mode.label}</span>
                {mode.id !== "MONITOR" && <LockKeyhole size={13} />}
              </button>
            );
          })}
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
          <span>{runnerText}</span>
          <button
            className="ts-action-button"
            type="button"
            onClick={() => onStrategyCycle(activeProfile.id)}
          >
            <Play size={16} />
            <span>전략 사이클 점검</span>
          </button>
        </div>
      </div>
    </section>
  );
}

function isCryptoStrategy(strategy) {
  const text = `${strategy.asset ?? ""} ${strategy.symbol ?? ""}`.toLowerCase();
  return ["crypto", "coin", "btc", "eth", "usdt", "코인"].some((token) => text.includes(token));
}

function BrokerCapabilityPanel({ diagnostics }) {
  return (
    <section className="panel broker-capability-panel">
      <PanelHeader title="브로커 Capability" subtitle="실계좌 연결에 필요한 기능별 구현/차단 상태입니다." />
      <div className="capability-list">
        {diagnostics.map((broker) => (
          <div className="capability-broker" key={broker.broker_id}>
            <div className="capability-broker-title">
              <Network size={16} />
              <strong>{broker.name}</strong>
            </div>
            <div className="capability-grid">
              {broker.capabilities.map((capability) => (
                <div className={`capability-item ${capability.status}`} key={capability.key}>
                  <strong>{capability.label}</strong>
                  <span>{capability.detail}</span>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

function BrokerAdapterContractPanel({ contract }) {
  return (
    <section className="panel broker-contract-panel">
      <PanelHeader title="어댑터 인터페이스 계약" subtitle="KIS/Binance 어댑터가 공통으로 구현해야 할 메서드입니다." />
      <div className="compact-list">
        {contract.map((item) => (
          <div className="compact-row" key={item.method}>
            <strong>{item.method}</strong>
            <span>{item.purpose}</span>
            <StatusPill tone={statusTone(item.status)}>{item.status}</StatusPill>
          </div>
        ))}
      </div>
    </section>
  );
}

function RiskPanel({ checks }) {
  return (
    <section className="panel risk-panel">
      <PanelHeader title="Pre-Trade Risk Gate" subtitle="주문 전 차단 규칙은 항상 브로커 전송보다 먼저 실행됩니다." />
      <div className="risk-grid">
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
      </div>
    </section>
  );
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

function OrderCommandPanel({ newEntriesBlocked, dryRun, killSwitch, onDryRun, onEntryBlock, onTestIntent }) {
  return (
    <section className="panel">
      <PanelHeader title="주문 제어" subtitle="실주문 전송 전 차단 상태와 테스트 게이트를 관리합니다." />
      <div className="operator-actions">
        <ActionButton
          className={`secondary-button ${dryRun ? "safe-active" : "danger-active"}`}
          icon={<ShieldCheck size={16} />}
          label="Dry Run"
          onClick={onDryRun}
          status={dryRun ? "success" : "error"}
        />
        <ActionButton
          active={newEntriesBlocked}
          className="secondary-button"
          icon={<ShieldCheck size={16} />}
          label="신규 진입 차단"
          onClick={onEntryBlock}
          status={newEntriesBlocked ? "success" : undefined}
        />
        <ActionButton
          className="primary-button"
          icon={<TerminalSquare size={16} />}
          label="테스트 주문 게이트"
          onClick={onTestIntent}
          pendingLabel="확인 중"
          variant="primary"
        />
        <span className={`inline-state ${killSwitch ? "danger" : "success"}`}>{killSwitch ? "긴급 차단 켜짐" : "긴급 차단 꺼짐"}</span>
      </div>
    </section>
  );
}

function WatchdogPanel({ watchdog, onWatchdog }) {
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
    <section className="panel watchdog-panel">
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
          <div className="queue-card" key={item.label}>
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
          <div className="queue-card" key={item.label}>
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

function BrokerRequirementsPanel({ brokers }) {
  return (
    <section className="panel">
      <PanelHeader title="브로커 준비 항목" subtitle="실제 주문 연결 전에 비어 있는 환경 값을 확인합니다." />
      <div className="compact-list">
        {brokers.map((broker) => (
          <div className="compact-row" key={broker.broker_id}>
            <strong>{broker.name}</strong>
            <span>{broker.missing_env.length ? `${broker.missing_env.length}개 값 필요` : "환경 값 입력됨"}</span>
            <StatusPill tone={broker.order_ready ? "success" : "danger"}>{broker.order_ready ? "ready" : "blocked"}</StatusPill>
          </div>
        ))}
      </div>
    </section>
  );
}

function ReconciliationSummaryPanel({ reconciliation, programLedger, executionEvents, onReconcile, onProgramLedgerBaseline, onExecutionEvents }) {
  const summary = reconciliation?.summary ?? fallbackSnapshot.reconciliation.summary;
  const actions = reconciliation?.next_actions ?? [];
  const items = [
    { label: "상태", value: summary.status_label, tone: statusTone(summary.status) },
    { label: "포지션", value: summary.position_count, tone: "info" },
    { label: "계좌", value: summary.account_count, tone: "info" },
    { label: "API 필요", value: summary.api_required_count, tone: summary.api_required_count ? "warning" : "success" },
    { label: "불일치", value: summary.mismatch_count, tone: summary.mismatch_count ? "danger" : "success" },
    { label: "조회 오류", value: summary.error_count ?? 0, tone: summary.error_count ? "danger" : "success" },
  ];
  const ledger = programLedger ?? fallbackSnapshot.program_ledger;
  const events = executionEvents ?? fallbackSnapshot.execution_events;
  const ledgerItems = [
    { label: "원장 현금", value: ledger.cash_count ?? 0, tone: ledger.cash_count ? "success" : "warning" },
    { label: "원장 포지션", value: ledger.position_count ?? 0, tone: ledger.position_count ? "success" : "warning" },
    { label: "체결 이벤트", value: ledger.execution_event_count ?? 0, tone: events.errors?.length ? "warning" : "info" },
  ];
  return (
    <section className="panel reconciliation-panel">
      <PanelHeader title="포지션·계좌 대조 요약" subtitle={`마지막 대조 ${summary.last_run}`} />
      <div className="panel-action-line">
        <StatusPill tone={statusTone(summary.status)}>{summary.status_label}</StatusPill>
        <button className="mini-button" type="button" onClick={onReconcile}>
          <RefreshCcw size={14} />
          대조 실행
        </button>
        <button className="mini-button" type="button" onClick={onProgramLedgerBaseline}>
          <DatabaseZap size={14} />
          원장 기준 저장
        </button>
        <button className="mini-button" type="button" onClick={() => onExecutionEvents("all")}>
          <FileClock size={14} />
          체결 동기화
        </button>
      </div>
      <MetricGrid className="metric-grid">
        {[...items, ...ledgerItems].map((item) => (
          <MetricCard
            className="metric-card"
            detail={item.tone === "success" ? "정상" : "확인"}
            detailClassName={`summary-state ${item.tone}`}
            key={item.label}
            label={item.label}
            tone={item.tone}
            value={item.value}
          />
        ))}
      </MetricGrid>
      <div className="next-actions">
        {actions.map((action) => (
          <span key={action}>{action}</span>
        ))}
      </div>
      <div className="ledger-footnote">
        <span>프로그램 원장: {ledger.path ?? "-"}</span>
        <span>마지막 체결 동기화: {events.last_poll ?? "미실행"}</span>
        {events.errors?.length ? <span>이벤트 어댑터 확인 필요 {events.errors.length}건</span> : null}
      </div>
    </section>
  );
}

function AccountReconciliationPanel({ accounts }) {
  return (
    <section className="panel account-panel">
      <PanelHeader title="계좌 현금 대조" subtitle="프로그램 원장과 브로커 계좌 현금성 잔고를 비교합니다." />
      <div className="account-list">
        {accounts.map((account) => (
          <div className="account-row" key={account.broker_id}>
            <WalletCards size={16} />
            <div>
              <strong>{account.account}</strong>
              <span>{account.broker_name} · {account.currency}</span>
            </div>
            <em>{account.program_cash} / {account.broker_cash}</em>
            <StatusPill tone={statusTone(account.status)}>{account.status_label}</StatusPill>
          </div>
        ))}
      </div>
    </section>
  );
}

function OperationsReportPanel({ report }) {
  return (
    <section className="panel operations-report-panel">
      <PanelHeader title="운용 리포트" subtitle={`생성 시각 ${report.generated_at}`} />
      <div className="compact-list">
        {report.sections.map((section) => (
          <div className="compact-row report-row" key={section.label}>
            <strong>{section.label}</strong>
            <span>{section.value} · {section.detail}</span>
            <StatusPill tone={statusTone(section.status)}>{section.status}</StatusPill>
          </div>
        ))}
      </div>
    </section>
  );
}

function FinalPreflightPanel({ checks, onPreflight, compact = false }) {
  const visibleChecks = compact ? checks.slice(0, 6) : checks;
  return (
    <section className="panel final-preflight-panel">
      <PanelHeader title="최종 점검" subtitle="실브로커 어댑터 연결 직전의 hard stop과 warning을 점검합니다." />
      <div className="panel-action-line">
        <StatusPill tone={checks.some((check) => check.status === "fail") ? "danger" : checks.some((check) => check.status === "warn") ? "warning" : "success"}>
          {checks.filter((check) => check.status === "fail").length} hard stop
        </StatusPill>
        <button className="mini-button" type="button" onClick={onPreflight}>
          <BadgeCheck size={14} />
          최종 점검
        </button>
      </div>
      <div className="check-list">
        {visibleChecks.map((check) => (
          <StatusRow key={check.label} label={check.label} status={check.status} detail={check.detail} />
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

function LiveStrategySelectorPanel({
  strategies,
  selectedStrategy,
  onSelect,
  onPromoteLive,
  onStrategyLifecycle,
  orders = [],
  summary = {},
  operatorConfirmed = false,
}) {
  const parametersText = formatKeyValueMap(selectedStrategy?.parameters);
  const promotionStage = selectedStrategy?.promotion?.stage || selectedStrategy?.promotion_stage || selectedStrategy?.lifecycle_status || "unknown";
  const normalizedStage = normalizePromotionStage(promotionStage);
  const execution = liveSmallExecutionSummaryForStrategy(selectedStrategy?.strategy_id, orders);
  const checklist = buildLivePromotionChecklist(selectedStrategy, normalizedStage, execution, summary, operatorConfirmed);
  const lifecycleTimeline = buildLiveLifecycleTimeline(selectedStrategy);
  const evidenceGate = selectedStrategy?.paper_portfolio_evidence_gate ?? {};
  const canPromoteLive = Boolean(
    selectedStrategy
      && normalizedStage === "before-live-small"
      && selectedStrategy.live_small_eligible
      && execution.successful > 0
      && execution.blocked === 0
      && operatorConfirmed
      && Number(summary?.blocker_count || 0) === 0
      && onPromoteLive,
  );
  const isPaused = normalizedStage === "paused";
  const isRetired = normalizedStage === "retired";
  const livePromotionHint = !selectedStrategy
    ? "전략을 먼저 선택하세요."
    : normalizedStage === "live"
      ? "이미 정식 Live 상태입니다."
      : normalizedStage !== "before-live-small"
        ? `현재 ${promotionLabel(normalizedStage)} 단계입니다. Paper Trader에서 before-live-small까지 승급한 뒤 진행합니다.`
        : selectedStrategy.live_small_eligible
          ? "SMALL_LIVE 성공 주문과 현재 운용 게이트를 다시 확인한 뒤 live로 고정합니다."
          : "before-live-small 상태이지만 live_small_eligible 증거가 부족합니다.";
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
        <div className="live-strategy-status-strip">
          <StatusPill tone={promotionTone(promotionStage)}>{promotionLabel(promotionStage)}</StatusPill>
          <StatusPill tone={selectedStrategy?.live_allowed ? "success" : "danger"}>{selectedStrategy?.permission_label || "WAIT"}</StatusPill>
          <StatusPill tone={portfolioEvidenceTone(evidenceGate)}>PORT {portfolioEvidenceLabel(evidenceGate)}</StatusPill>
          <StatusPill tone="info">READ ONLY</StatusPill>
        </div>
      </div>
      {selectedStrategy ? (
        <>
          <div className="live-strategy-summary-grid">
            <MetricCard className="metric-card" label="전략" value={selectedStrategy.plugin_label || selectedStrategy.plugin} detail={selectedStrategy.strategy_id} />
            <MetricCard className="metric-card" label="대상" value={`${selectedStrategy.symbol} · ${selectedStrategy.timeframe}`} detail={selectedStrategy.asset} />
            <MetricCard className="metric-card" label="Release" value={selectedStrategy.release?.release_id || selectedStrategy.release_id || "-"} detail={selectedStrategy.release?.parameter_hash || "parameter hash 없음"} />
            <MetricCard className="metric-card" label="검증" value={selectedStrategy.verification?.paper_trader?.label || "Paper 상태 없음"} detail={selectedStrategy.promotion?.promoted_at || "승급 시간 없음"} />
            <MetricCard
              className="metric-card"
              label="Portfolio Evidence"
              value={portfolioEvidenceLabel(evidenceGate)}
              detail={evidenceGate.required ? evidenceGate.detail : "단일 전략 모드"}
            />
          </div>
          <div className="live-strategy-readonly-note">
            <strong>이 전략은 Backtester/Paper Trader에서 저장된 검증본입니다.</strong>
            <span>Live Trader에서는 파라미터를 직접 수정하지 않고, 새 파라미터는 Backtester에서 새 release로 다시 승급합니다.</span>
          </div>
          <div className="live-strategy-promotion-line">
            <ActionButton
              className="secondary-button"
              disabled={!canPromoteLive}
              icon={<BadgeCheck size={16} />}
              label="정식 Live 승급"
              onClick={() => onPromoteLive?.(selectedStrategy.strategy_id)}
              status={canPromoteLive ? "success" : undefined}
            />
            <span>{livePromotionHint}</span>
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
          <div className="promotion-checklist live-promotion-checklist">
            {checklist.map((item) => (
              <article className={item.tone} key={item.label}>
                <StatusPill tone={item.tone}>{item.status}</StatusPill>
                <div>
                  <strong>{item.label}</strong>
                  <span>{item.detail}</span>
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
            <span>{isRetired ? "retired 상태라 신규 주문과 승급이 차단됩니다." : "상태 변경은 공유 전략 artifact lifecycle에 기록됩니다."}</span>
          </div>
          <div className="live-strategy-parameter-panel">
            <strong>Parameters</strong>
            <pre>{parametersText || "-"}</pre>
          </div>
        </>
      ) : (
        <EmptyRow text="이 자산군에 표시할 전략 artifact가 없습니다." />
      )}
    </section>
  );
}

function StrategyPanel({ strategies, selectedStrategyId, onSelect }) {
  return (
    <section className="panel strategy-panel">
      <PanelHeader title="전략 Artifact" subtitle="Backtester/Paper 승인 결과와 lifecycle/evidence 계약을 확인합니다." />
      <div className="data-table strategy-table">
        <div className="table-head">
          <span>전략</span>
          <span>심볼</span>
          <span>상태</span>
          <span>Score</span>
          <span>검증</span>
          <span>권한</span>
          <span>Evidence</span>
          <span>차단 사유</span>
        </div>
        {strategies.map((strategy) => {
          const backtester = strategy.verification?.backtester || {
            label: strategy.backtester_label || "Backtester 정보 없음",
            status: strategy.backtester_verified ? "pass" : "unknown",
          };
          const paper = strategy.verification?.paper_trader || {
            label: strategy.paper_trader_label || "Paper 미검증",
            status: strategy.paper_trader_verified ? "pass" : "wait",
          };
          return (
            <div
              className={`table-row ${strategy.strategy_id === selectedStrategyId ? "selected" : ""}`}
              key={strategy.strategy_id}
              onClick={() => onSelect(strategy.strategy_id)}
              role="button"
              tabIndex={0}
              onKeyDown={(event) => {
                if (event.key === "Enter" || event.key === " ") {
                  event.preventDefault();
                  onSelect(strategy.strategy_id);
                }
              }}
            >
              <strong>{strategy.name}</strong>
              <span>{strategy.symbol}</span>
              <span>{strategy.lifecycle_status}</span>
              <span>{strategy.score}</span>
              <span className="strategy-verification-pills">
                <StatusPill tone={verificationTone(backtester.status)}>{backtester.label}</StatusPill>
                <StatusPill tone={verificationTone(paper.status)}>{paper.label}</StatusPill>
              </span>
              <StatusPill tone={strategy.live_allowed ? "success" : "danger"}>{strategy.permission_label}</StatusPill>
              <StatusPill tone={portfolioEvidenceTone(strategy.paper_portfolio_evidence_gate)}>
                {portfolioEvidenceLabel(strategy.paper_portfolio_evidence_gate)}
              </StatusPill>
              <em>{strategy.block_reason}</em>
            </div>
          );
        })}
      </div>
    </section>
  );
}

function OrderPanel({ orders, onRetryOrder, onCancelOrder }) {
  return (
    <section className="panel order-panel">
      <PanelHeader title="주문 기록" subtitle="차단, Dry Run, 재시도, 취소 이벤트를 감사 추적합니다." />
      <div className="order-ledger-list">
        {orders.length === 0 ? (
          <EmptyRow text="아직 주문 이벤트가 없습니다. 테스트 주문 게이트를 누르면 차단 이벤트가 생성됩니다." />
        ) : (
          orders.map((order) => (
            <div className="order-ledger-row" key={order.order_id}>
              <div className="order-ledger-head">
                <div>
                  <strong>{order.order_id}</strong>
                  <span>{order.time} · {order.strategy_id}</span>
                </div>
                <StatusPill tone={statusTone(order.state)}>{order.state}</StatusPill>
              </div>
              <div className="order-ledger-meta">
                <span>{order.symbol}</span>
                <span className="side-buy">{order.side}</span>
                <span>큐 {order.queue_state}</span>
                <span>시도 {order.attempts}/{order.max_attempts}</span>
                <span>다음 {order.next_retry_at}</span>
              </div>
              <div className="order-ledger-foot">
                <em>{order.reason}</em>
                <div className="order-actions">
                  <IconButton
                    className="mini-icon-button"
                    title="재시도"
                    aria-label={`${order.order_id} 재시도`}
                    disabled={!order.retryable}
                    onClick={() => onRetryOrder(order.order_id)}
                  >
                    <RotateCcw size={13} />
                  </IconButton>
                  <IconButton
                    className="mini-icon-button"
                    title="취소"
                    aria-label={`${order.order_id} 취소`}
                    disabled={order.state === "canceled"}
                    onClick={() => onCancelOrder(order.order_id)}
                  >
                    <CircleStop size={13} />
                  </IconButton>
                </div>
              </div>
            </div>
          ))
        )}
      </div>
    </section>
  );
}

function PositionPanel({ positions }) {
  return (
    <section className="panel position-panel">
      <PanelHeader title="포지션 대조" subtitle="프로그램 포지션과 브로커 계좌 포지션 비교가 필요합니다." />
      <div className="position-list">
        {positions.map((position) => (
          <div className="position-row" key={position.symbol}>
            <WalletCards size={16} />
            <div>
              <strong>{position.symbol}</strong>
              <span>{position.asset} · {position.broker_name}</span>
            </div>
            <em>{position.program_qty} / {position.broker_qty} · Δ {position.delta_qty}</em>
            <StatusPill tone={statusTone(position.status)}>{position.status_label}</StatusPill>
          </div>
        ))}
      </div>
    </section>
  );
}

function AuditPanel({ audit }) {
  const [query, setQuery] = useState("");
  const [channel, setChannel] = useState("all");
  const [level, setLevel] = useState("all");
  const [sort, setSort] = useState("latest");
  const rows = audit.map((item, index) => {
    const logChannel = inferLogChannel(item);
    const normalizedLevel = item.level === "danger" ? "ERROR" : item.level === "warn" ? "WARN" : "INFO";
    const displayTime = formatAuditTime(item);
    return {
      id: `${item.timestamp || item.time}-${index}`,
      time: displayTime,
      level: normalizedLevel,
      channel: logChannel,
      scope: logChannel,
      module: item.event,
      source: item.event,
      message: item.detail,
      raw: `${displayTime} ${normalizedLevel} ${logChannel} ${item.event} ${item.detail}`.toLowerCase(),
    };
  });
  const visibleRows = rows
    .filter((row) => channel === "all" || row.channel === channel)
    .filter((row) => level === "all" || row.level === level)
    .filter((row) => !query.trim() || row.raw.includes(query.trim().toLowerCase()))
    .sort((a, b) => (sort === "latest" ? rows.indexOf(a) - rows.indexOf(b) : rows.indexOf(b) - rows.indexOf(a)));
  const channels = ["all", ...Array.from(new Set(rows.map((row) => row.channel)))];
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
      <div className="logs-toolbar">
        <label className="search-box logs-search">
          <input value={query} onChange={(event) => setQuery(event.currentTarget.value)} placeholder="메시지, scope, 작업명 검색" />
          <Search size={18} />
        </label>
        <select value={channel} onChange={(event) => setChannel(event.currentTarget.value)}>
          {channels.map((item) => (
            <option value={item} key={item}>
              {item === "all" ? "전체" : item}
            </option>
          ))}
        </select>
        <select value={level} onChange={(event) => setLevel(event.currentTarget.value)}>
          <option value="all">전체</option>
          <option value="INFO">INFO</option>
          <option value="WARN">WARN</option>
          <option value="ERROR">ERROR</option>
        </select>
        <select value={sort} onChange={(event) => setSort(event.currentTarget.value)}>
          <option value="latest">최신순</option>
          <option value="oldest">오래된순</option>
        </select>
        <button className="logs-export-button" type="button" onClick={handleExportLogs} disabled={!visibleRows.length}>
          <Download size={16} />
          CSV
        </button>
        <span>{visibleRows.length.toLocaleString()} / {rows.length.toLocaleString()}개</span>
      </div>
      <div className="table-scroll compact-table logs-table">
        <table>
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
            {visibleRows.length ? (
              visibleRows.map((row) => (
                <tr key={row.id}>
                  <td>{row.time}</td>
                  <td><span className={`scope-pill scope-${logToken(row.scope)}`}>{row.scope}</span></td>
                  <td><span className={`level-pill level-${logToken(row.level)}`}>{row.level}</span></td>
                  <td>{row.source}</td>
                  <td className="log-message-cell" title={row.message}>{row.message}</td>
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan={5}>
                  <EmptyRow text="검색 조건에 맞는 로그가 없습니다." />
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function logToken(value = "") {
  return String(value || "unknown").trim().toLowerCase().replace(/[^a-z0-9가-힣]+/g, "-").replace(/^-+|-+$/g, "") || "unknown";
}

function inferLogChannel(item) {
  const text = `${item.event ?? ""} ${item.detail ?? ""}`.toLowerCase();
  if (text.includes("주문") || text.includes("order")) return "ORDER";
  if (text.includes("api") || text.includes("broker") || text.includes("kis") || text.includes("binance")) return "API";
  if (text.includes("전략") || text.includes("contract") || text.includes("artifact")) return "STRATEGY";
  if (text.includes("risk") || text.includes("리스크") || item.level === "danger") return "RISK";
  return "SYSTEM";
}

function StatusRow({ label, status, detail }) {
  const tone = statusTone(status);
  const statusLabel = status === "pass" ? "통과" : status === "warn" ? "주의" : status === "fail" ? "조치" : status;
  return (
    <SharedStatusRow
      className={`status-row ${status}`}
      tone={tone}
      title={label}
      detail={detail}
      badge={<span className="status-row-state">{statusLabel}</span>}
    />
  );
}

function EmptyRow({ text }) {
  return (
    <EmptyState className="empty-row" icon={<TerminalSquare size={16} />} message={text} />
  );
}

export default App;
