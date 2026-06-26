import { useEffect, useRef, useState } from "react";
import {
  AlertTriangle,
  BadgeCheck,
  Bell,
  CircleStop,
  ClipboardCheck,
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
  Play,
  Power,
  Radio,
  RefreshCcw,
  RotateCcw,
  Route,
  Settings,
  ShieldAlert,
  ShieldCheck,
  SlidersHorizontal,
  Siren,
  Sun,
  TerminalSquare,
  Unlock,
  WalletCards,
} from "lucide-react";
import {
  cancelOrder,
  exportAudit,
  getSnapshot,
  runBrokerCheck,
  runFinalPreflight,
  runReconciliation,
  retryOrder,
  setChecklistItem,
  setFlag,
  setMode,
  setRetryPolicy,
  setRiskSetting,
  submitTestIntent,
} from "./api";
import designTokens from "../../../packages/design/design_tokens.json";

const navItems = [
  { id: "overview", label: "대시보드", icon: LayoutDashboard },
  { id: "gate", label: "실거래 게이트", icon: ListChecks },
  { id: "orders", label: "주문", icon: Route },
  { id: "brokers", label: "API", icon: Network },
  { id: "strategies", label: "전략", icon: DatabaseZap },
  { id: "audit", label: "감사 로그", icon: FileClock },
  { id: "preflight", label: "최종 점검", icon: ShieldCheck },
  { id: "settings", label: "설정", icon: Settings },
];

const pageProfiles = {
  overview: {
    title: "대시보드",
    eyebrow: "실거래 관제",
    summary: "실거래 게이트, 주문, 브로커, 대조 상태를 한 화면에서 확인합니다.",
  },
  gate: {
    title: "실거래 게이트",
    eyebrow: "승인/차단",
    summary: "실주문 전환 조건과 운영 체크리스트를 점검합니다.",
  },
  orders: {
    title: "주문",
    eyebrow: "주문 큐",
    summary: "주문 의도, Dry Run 원장, 재시도 정책을 관리합니다.",
  },
  brokers: {
    title: "API",
    eyebrow: "브로커/API 관리",
    summary: "KIS/Binance 연결, 환경 변수, 주문 어댑터, 인터페이스 계약을 한곳에서 관리합니다.",
  },
  strategies: {
    title: "전략",
    eyebrow: "전략 산출물",
    summary: "Backtester/Paper 승인 전략과 live_allowed 권한을 검토합니다.",
  },
  audit: {
    title: "감사 로그",
    eyebrow: "운영 기록",
    summary: "모드 전환, 주문 차단, 설정 변경 이력을 추적합니다.",
  },
  preflight: {
    title: "최종 점검",
    eyebrow: "출시 전 검사",
    summary: "실브로커 연결 직전 hard stop과 warning을 최종 확인합니다.",
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
  dry_run_ledger: [],
  brokers: [],
  broker_diagnostics: [],
  broker_adapter_contract: [],
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
    },
    positions: [],
    accounts: [],
    next_actions: [],
  },
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
const LEGACY_THEME_STORAGE_KEY = "live-trader.ui-theme.v1";
const LAYOUT_RESET_EVENT = "live-trader-layout-reset";
const LAYOUT_SNAP_SIZE = 8;
const LAYOUT_COLLISION_GAP = 8;
const LAYOUT_MAX_DIMENSION = 100000;
const LAYOUT_MAX_OFFSET = 100000;
const MIN_PANEL_WIDTH = 260;
const MIN_PANEL_HEIGHT = 150;

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

function applyCustomAccent(root, appearance) {
  if (appearance.accent !== "custom") {
    customAccentVarNames.forEach((name) => root.style.removeProperty(name));
    return;
  }
  Object.entries(customAccentVars(appearance.customAccent)).forEach(([name, value]) => {
    root.style.setProperty(name, value);
  });
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
      targetNav: "strategies",
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

  (snapshot.positions ?? []).forEach((position) => {
    addSearchResult(results, query, {
      id: `position-${position.symbol}`,
      type: "포지션",
      label: position.symbol,
      detail: `${position.status_label} · ${position.broker_name}`,
      meta: [position.asset, position.currency, position.detail].join(" "),
      targetNav: "preflight",
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
      targetNav: "preflight",
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
      targetNav: "preflight",
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
      targetNav: "orders",
    });
  }

  if (snapshot.reconciliation?.summary?.status && snapshot.reconciliation.summary.status !== "pass") {
    push({
      id: "reconciliation",
      tone: statusTone(snapshot.reconciliation.summary.status),
      title: `포지션·계좌 대조 ${snapshot.reconciliation.summary.status_label}`,
      detail: `API 필요 ${snapshot.reconciliation.summary.api_required_count}개, 불일치 ${snapshot.reconciliation.summary.mismatch_count}개`,
      targetNav: "preflight",
    });
  }

  (snapshot.final_preflight ?? [])
    .filter((check) => check.status !== "pass")
    .slice(0, 3)
    .forEach((check) => {
      push({ id: `preflight-${check.label}`, tone: statusTone(check.status), title: check.label, detail: check.detail, targetNav: "preflight" });
    });

  if (!items.length) {
    push({ id: "clear", tone: "success", title: "주요 알림 없음", detail: "현재 표시할 blocker 또는 warning이 없습니다.", targetNav: "overview" });
  }
  return items;
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

    return () => {
      observer.disconnect();
      root.removeEventListener("pointerdown", onPointerDown);
      window.removeEventListener(LAYOUT_RESET_EVENT, resetLayout);
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
  const [exportResult, setExportResult] = useState(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [notificationsOpen, setNotificationsOpen] = useState(false);
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
    } catch (err) {
      setError(err instanceof Error ? err.message : "요청 실패");
    } finally {
      setLoading(false);
    }
  }

  async function runAuditExport(format) {
    setLoading(true);
    try {
      const result = await exportAudit(format);
      if (result.snapshot) setSnapshot(result.snapshot);
      setExportResult(result);
      setError(result.ok === false ? result.reason : "");
      if (result.ok !== false && result.content) {
        downloadExport(result);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "내보내기 실패");
    } finally {
      setLoading(false);
    }
  }

  function updateAppearance(partial) {
    setAppearance((current) => applyAppearance({ ...current, ...partial }));
  }

  function changeLayoutMode(mode) {
    setLayoutMode(applyLayoutMode(mode));
  }

  function toggleSidebarCollapsed() {
    setSidebarCollapsed((current) => {
      const next = !current;
      saveSidebarCollapsed(next);
      return next;
    });
  }

  function resetWorkspaceLayout() {
    try {
      window.localStorage.removeItem(PANEL_SIZE_STORAGE_KEY);
      window.localStorage.removeItem(PANEL_POSITION_STORAGE_KEY);
    } catch {
      // The event below still resets the visible layout.
    }
    window.dispatchEvent(new Event(LAYOUT_RESET_EVENT));
  }

  function navigateWorkspace(navId) {
    setSelectedNav(navId);
    setNotificationsOpen(false);
  }

  const title = navItems.find((item) => item.id === selectedNav)?.label ?? "대시보드";
  const canLive = snapshot.summary.blocker_count === 0;
  const canFullLive = canLive && snapshot.summary.warning_count === 0;
  const notifications = buildNotificationItems(snapshot, error);

  return (
    <div className={`app-shell ${sidebarCollapsed ? "sidebar-collapsed" : ""}`}>
      <aside className="sidebar">
        <div className="brand-block" aria-label="Live Trader">
          <div className="brand-mark">
            <Radio size={19} />
          </div>
          <div>
            <strong>실거래 콘솔</strong>
            <span>주문 운영 데스크</span>
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
          <span>운용 모드</span>
          <StatusPill tone={snapshot.mode === "MONITOR" ? "info" : "danger"}>{snapshot.mode}</StatusPill>
        </div>
      </aside>

      <main className={`workspace layout-${layoutMode}`} ref={workspaceRef} data-layout-mode={layoutMode}>
        <header className="topbar">
          <div className="topbar-left">
            <button
              className={`icon-button sidebar-toggle ${sidebarCollapsed ? "active" : ""}`}
              type="button"
              aria-label={sidebarCollapsed ? "탭 화면 펼치기" : "탭 화면 접기"}
              aria-pressed={sidebarCollapsed}
              title={sidebarCollapsed ? "탭 화면 펼치기" : "탭 화면 접기"}
              onClick={toggleSidebarCollapsed}
            >
              <PanelLeft size={18} />
            </button>
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
              <button
                className={`icon-button notification-button ${notificationsOpen ? "active" : ""}`}
                type="button"
                aria-label="알림"
                aria-expanded={notificationsOpen}
                onClick={() => setNotificationsOpen((open) => !open)}
              >
                <Bell size={17} />
                {notifications.length > 0 && <span className="notification-badge">{Math.min(notifications.length, 9)}</span>}
              </button>
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

        <MarketStrip sessions={snapshot.sessions} />

        {(error || snapshot.summary.blocker_count > 0) && (
          <section className="alert-band" aria-live="polite">
            <ShieldAlert size={20} />
            <div>
              <strong>실거래 주문 차단 상태</strong>
              <span>{error || `readiness blocker ${snapshot.summary.blocker_count}개가 남아 있습니다. API 키, 주문 어댑터, live_allowed 권한을 확인하세요.`}</span>
            </div>
          </section>
        )}

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
          onTestIntent={() => runAction(submitTestIntent)}
          onRiskSetting={(name, value) => runAction(() => setRiskSetting(name, value))}
          onChecklist={(name, value) => runAction(() => setChecklistItem(name, value))}
          onRetryPolicy={(name, value) => runAction(() => setRetryPolicy(name, value))}
          onRetryOrder={(orderId) => runAction(() => retryOrder(orderId))}
          onCancelOrder={(orderId) => runAction(() => cancelOrder(orderId))}
          onBrokerCheck={(brokerId) => runAction(() => runBrokerCheck(brokerId))}
          onReconcile={() => runAction(runReconciliation)}
          onPreflight={() => runAction(runFinalPreflight)}
          onAuditExport={runAuditExport}
          exportResult={exportResult}
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

function downloadExport(result) {
  const blob = new Blob([result.content], { type: result.mime || "text/plain;charset=utf-8" });
  const url = window.URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = result.filename || `live-trader-audit.${result.format || "txt"}`;
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
  onTestIntent,
  onRiskSetting,
  onChecklist,
  onRetryPolicy,
  onRetryOrder,
  onCancelOrder,
  onBrokerCheck,
  onReconcile,
  onPreflight,
  onAuditExport,
  exportResult,
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

  if (selectedNav === "gate") {
    return renderPage(
      <section className="content-grid">
        <div className="content-column">
          {modeConsole}
          <RunbookChecklistPanel checklist={snapshot.checklist} onChecklist={onChecklist} />
          <ReadinessPanel checks={snapshot.readiness} />
        </div>
        <div className="content-column">
          <SummaryPanel summary={snapshot.summary} generatedAt={snapshot.generated_at} />
          <GateRunbookPanel />
          <RiskSettingsPanel settings={snapshot.risk_settings} onRiskSetting={onRiskSetting} />
          <RiskPanel checks={snapshot.risk_checks} />
          <ReconciliationSummaryPanel reconciliation={snapshot.reconciliation} onReconcile={onReconcile} />
          <PositionPanel positions={snapshot.positions} />
        </div>
      </section>,
    );
  }

  if (selectedNav === "orders") {
    return renderPage(
      <section className="content-grid">
        <div className="content-column">
          <OrderCommandPanel
            newEntriesBlocked={snapshot.new_entries_blocked}
            dryRun={snapshot.dry_run}
            killSwitch={snapshot.kill_switch}
            onDryRun={onDryRun}
            onEntryBlock={onEntryBlock}
            onTestIntent={onTestIntent}
          />
          <OrderQueueSummaryPanel summary={snapshot.order_queue} />
          <OrderPanel orders={snapshot.orders} onRetryOrder={onRetryOrder} onCancelOrder={onCancelOrder} />
          <RiskPanel checks={snapshot.risk_checks} />
        </div>
        <div className="content-column">
          <SummaryPanel summary={snapshot.summary} generatedAt={snapshot.generated_at} />
          <RetryPolicyPanel policy={snapshot.retry_policy} onRetryPolicy={onRetryPolicy} />
          <DryRunLedgerPanel ledger={snapshot.dry_run_ledger} />
          <ReconciliationSummaryPanel reconciliation={snapshot.reconciliation} onReconcile={onReconcile} />
        </div>
      </section>,
    );
  }

  if (selectedNav === "brokers") {
    return renderPage(
      <section className="content-grid">
        <div className="content-column">
          <BrokerPanel brokers={snapshot.brokers} />
          <BrokerConnectionWizardPanel diagnostics={snapshot.broker_diagnostics} onBrokerCheck={onBrokerCheck} />
          <BrokerCapabilityPanel diagnostics={snapshot.broker_diagnostics} />
        </div>
        <div className="content-column">
          <ReadinessPanel checks={snapshot.readiness} />
        </div>
      </section>,
    );
  }

  if (selectedNav === "strategies") {
    return renderPage(
      <section className="content-grid">
        <div className="content-column">
          <StrategyPanel strategies={snapshot.strategies} />
          <StrategyWorkflowPanel />
        </div>
        <div className="content-column">
          <SummaryPanel summary={snapshot.summary} generatedAt={snapshot.generated_at} />
          <ReadinessPanel checks={snapshot.readiness} />
        </div>
      </section>,
    );
  }

  if (selectedNav === "audit") {
    return renderPage(
      <section className="content-grid">
        <div className="content-column">
          <AuditPanel audit={snapshot.audit} />
          <AuditExportPanel onExport={onAuditExport} exportResult={exportResult} />
        </div>
        <div className="content-column">
          <SummaryPanel summary={snapshot.summary} generatedAt={snapshot.generated_at} />
          <OperationsReportPanel report={snapshot.operation_report} />
          <RiskPanel checks={snapshot.risk_checks} />
        </div>
      </section>,
    );
  }

  if (selectedNav === "preflight") {
    return renderPage(
      <section className="content-grid">
        <div className="content-column">
          <FinalPreflightPanel checks={snapshot.final_preflight} onPreflight={onPreflight} />
          <LaunchReportPanel report={snapshot.launch_report} />
          <ReadinessPanel checks={snapshot.readiness} />
        </div>
        <div className="content-column">
          <ReconciliationSummaryPanel reconciliation={snapshot.reconciliation} onReconcile={onReconcile} />
          <AccountReconciliationPanel accounts={snapshot.accounts} />
          <PositionPanel positions={snapshot.positions} />
          <OperationsReportPanel report={snapshot.operation_report} />
        </div>
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
        </div>
      </section>,
    );
  }

  return renderPage(
    <>
      <section className="command-grid">
        {modeConsole}
        <SummaryPanel summary={snapshot.summary} generatedAt={snapshot.generated_at} />
      </section>

      <section className="content-grid">
        <div className="content-column">
          <ReadinessPanel checks={snapshot.readiness} />
          <RunbookChecklistPanel checklist={snapshot.checklist} onChecklist={onChecklist} />
          <RiskPanel checks={snapshot.risk_checks} />
          <FinalPreflightPanel checks={snapshot.final_preflight} onPreflight={onPreflight} compact />
          <StrategyPanel strategies={snapshot.strategies} />
          <OrderPanel orders={snapshot.orders} onRetryOrder={onRetryOrder} onCancelOrder={onCancelOrder} />
          <AuditPanel audit={snapshot.audit} />
        </div>
        <div className="content-column">
          <ReconciliationSummaryPanel reconciliation={snapshot.reconciliation} onReconcile={onReconcile} />
          <AccountReconciliationPanel accounts={snapshot.accounts} />
          <PositionPanel positions={snapshot.positions} />
        </div>
      </section>
    </>,
  );
}

function PageView({ selectedNav, onNavigate, snapshot, searchQuery, children }) {
  const profile = pageProfiles[selectedNav] ?? pageProfiles.overview;
  const blockerCount = snapshot.summary?.blocker_count ?? 0;
  const warningCount = snapshot.summary?.warning_count ?? 0;
  const searchResults = buildSearchResults(snapshot, searchQuery);

  return (
    <section className={`page-view ${selectedNav}-view`}>
      <div className="page-heading">
        <div>
          <span>{profile.eyebrow}</span>
          <h1>{profile.title}</h1>
          <p>{profile.summary}</p>
        </div>
        <div className="page-heading-actions">
          <StatusPill tone={statusTone(snapshot.summary?.status)}>{snapshot.summary?.status ?? "unknown"}</StatusPill>
          <StatusPill tone={blockerCount ? "danger" : "success"}>{blockerCount} blocker</StatusPill>
          <StatusPill tone={warningCount ? "warning" : "success"}>{warningCount} warn</StatusPill>
          <span>{snapshot.generated_at}</span>
        </div>
      </div>

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

function StatusPill({ children, tone = "neutral" }) {
  return (
    <span className={`status-pill ${tone}`}>
      <span className="status-pill-text">{children}</span>
    </span>
  );
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
        <div className="theme-mode-row">
          {appearanceThemeOptions.map((option) => {
            const Icon = option.icon;
            return (
              <button
                key={option.id}
                className={`theme-mode-button ${appearance.theme === option.id ? "selected" : ""}`}
                type="button"
                onClick={() => updateAppearance({ theme: option.id })}
              >
                <Icon size={16} />
                {option.label}
              </button>
            );
          })}
        </div>
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

function MarketStrip({ sessions }) {
  return (
    <section className="market-strip">
      {sessions.map((session) => (
        <div className="market-item" key={session.label}>
          <Radio size={16} />
          <div>
            <strong>{session.label}</strong>
            <span>{session.detail}</span>
          </div>
          <span className={`market-time ${statusTone(session.state)}`}>{session.time}</span>
        </div>
      ))}
    </section>
  );
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
              className={`mode-button ${mode === item.id ? "active" : ""}`}
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
        <button className={`secondary-button ${operatorConfirmed ? "active" : ""}`} type="button" onClick={onConfirm}>
          <BadgeCheck size={16} />
          운용자 확인
        </button>
        <button className={`secondary-button ${dryRun ? "safe-active" : "danger-active"}`} type="button" onClick={onDryRun}>
          <ShieldCheck size={16} />
          Dry Run
        </button>
        <button className={`secondary-button ${newEntriesBlocked ? "active" : ""}`} type="button" onClick={onEntryBlock}>
          <ShieldCheck size={16} />
          신규 진입 차단
        </button>
        <button className="primary-button" type="button" onClick={onTestIntent}>
          <TerminalSquare size={16} />
          테스트 주문 게이트
        </button>
      </div>
    </section>
  );
}

function SummaryPanel({ summary, generatedAt }) {
  const items = [
    { label: "상태", value: summary.status, tone: statusTone(summary.status) },
    { label: "Blocker", value: summary.blocker_count, tone: summary.blocker_count ? "danger" : "success" },
    { label: "Warning", value: summary.warning_count, tone: summary.warning_count ? "warning" : "success" },
    { label: "Live 전략", value: summary.live_strategy_count, tone: summary.live_strategy_count ? "success" : "danger" },
    { label: "브로커 준비", value: summary.broker_ready_count, tone: summary.broker_ready_count ? "success" : "danger" },
  ];
  return (
    <section className="panel summary-panel">
      <PanelHeader title="운용 요약" subtitle={`마지막 갱신 ${generatedAt}`} />
      <div className="summary-grid">
        {items.map((item) => (
          <div key={item.label}>
            <span>{item.label}</span>
            <strong>{item.value}</strong>
            <span className={`summary-state ${item.tone}`}>{item.tone === "success" ? "정상" : "확인 필요"}</span>
          </div>
        ))}
      </div>
    </section>
  );
}

function ReadinessPanel({ checks }) {
  return (
    <section className="panel readiness-panel">
      <PanelHeader title="Live Readiness" subtitle="API, 계약, 권한, 운용자 확인을 동시에 검사합니다." />
      <div className="check-list">
        {checks.map((check) => (
          <StatusRow key={check.label} label={check.label} status={check.status} detail={check.detail} />
        ))}
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
              <StatusPill tone={statusTone(broker.status)}>{broker.status}</StatusPill>
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

function BrokerConnectionWizardPanel({ diagnostics, onBrokerCheck }) {
  return (
    <section className="panel broker-wizard-panel">
      <PanelHeader title="API 키 점검 마법사" subtitle="실제 키 원문은 표시하지 않고 존재 여부와 어댑터 준비 상태만 점검합니다." />
      <div className="wizard-list">
        {diagnostics.map((broker) => (
          <div className="wizard-card" key={broker.broker_id}>
            <div className="wizard-head">
              <div>
                <strong>{broker.name}</strong>
                <span>{broker.docs}</span>
              </div>
              <button className="mini-button" type="button" onClick={() => onBrokerCheck(broker.broker_id)}>
                <RefreshCcw size={14} />
                점검
              </button>
            </div>
            <div className="wizard-steps">
              {broker.steps.map((step) => (
                <div className={`wizard-step ${step.status}`} key={step.key}>
                  {step.status === "pass" ? <ShieldCheck size={15} /> : <ShieldAlert size={15} />}
                  <div>
                    <strong>{step.label}</strong>
                    <span>{step.detail}</span>
                  </div>
                  <StatusPill tone={statusTone(step.status)}>{step.status}</StatusPill>
                </div>
              ))}
            </div>
            <div className="env-check-grid">
              {broker.env.map((item) => (
                <div className={`env-check ${item.present ? "present" : "missing"}`} key={item.name}>
                  <strong>{item.name}</strong>
                  <span>{item.present ? item.masked || "입력됨" : "missing"}</span>
                </div>
              ))}
            </div>
            <div className="next-actions">
              {broker.next_actions.map((action) => (
                <span key={action}>{action}</span>
              ))}
            </div>
          </div>
        ))}
      </div>
    </section>
  );
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
              <StatusPill tone={statusTone(broker.status)}>{broker.status}</StatusPill>
            </div>
            <div className="capability-grid">
              {broker.capabilities.map((capability) => (
                <div className={`capability-item ${capability.status}`} key={capability.key}>
                  <strong>{capability.label}</strong>
                  <span>{capability.detail}</span>
                  <StatusPill tone={statusTone(capability.status)}>{capability.implemented ? "ready" : "필요"}</StatusPill>
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
          <div className={`risk-rule ${check.status}`} key={check.label}>
            <AlertTriangle size={16} />
            <div>
              <strong>{check.label}</strong>
              <span>{check.detail}</span>
            </div>
            <em>{check.value}</em>
          </div>
        ))}
      </div>
    </section>
  );
}

function GateRunbookPanel() {
  const items = [
    ["API", "실계좌 키와 주문 어댑터 확인"],
    ["권한", "live_allowed 전략만 통과"],
    ["대조", "브로커 포지션과 프로그램 포지션 비교"],
    ["승인", "운용자 확인 후 SMALL_LIVE부터 시작"],
  ];
  return (
    <section className="panel">
      <PanelHeader title="실거래 게이트 체크라인" subtitle="실거래 전환 전 필요한 운영 조건입니다." />
      <div className="compact-list">
        {items.map(([label, detail]) => (
          <div className="compact-row" key={label}>
            <strong>{label}</strong>
            <span>{detail}</span>
            <StatusPill tone="neutral">필수</StatusPill>
          </div>
        ))}
      </div>
    </section>
  );
}

function RunbookChecklistPanel({ checklist, onChecklist }) {
  return (
    <section className="panel checklist-panel">
      <PanelHeader title="운영 체크리스트" subtitle="필수 항목이 모두 확인되어야 실거래 게이트를 통과할 수 있습니다." />
      <div className="checklist-list">
        {checklist.map((item) => (
          <label className={`checklist-row ${item.checked ? "checked" : ""}`} key={item.key}>
            <input
              type="checkbox"
              checked={item.checked}
              onChange={(event) => onChecklist(item.key, event.currentTarget.checked)}
            />
            <ClipboardCheck size={16} />
            <div>
              <strong>{item.label}</strong>
              <span>{item.detail}</span>
            </div>
            <StatusPill tone={item.checked ? "success" : item.required ? "warning" : "neutral"}>
              {item.checked ? "완료" : item.required ? "필수" : "권장"}
            </StatusPill>
          </label>
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
        <button className={`secondary-button ${dryRun ? "safe-active" : "danger-active"}`} type="button" onClick={onDryRun}>
          <ShieldCheck size={16} />
          Dry Run
        </button>
        <button className={`secondary-button ${newEntriesBlocked ? "active" : ""}`} type="button" onClick={onEntryBlock}>
          <ShieldCheck size={16} />
          신규 진입 차단
        </button>
        <button className="primary-button" type="button" onClick={onTestIntent}>
          <TerminalSquare size={16} />
          테스트 주문 게이트
        </button>
        <span className={`inline-state ${killSwitch ? "danger" : "success"}`}>{killSwitch ? "긴급 차단 켜짐" : "긴급 차단 꺼짐"}</span>
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
          <div className="setting-row" key={setting.key}>
            <Clock3 size={16} />
            <div>
              <strong>{setting.label}</strong>
              <span>{setting.detail}</span>
            </div>
            {setting.type === "boolean" ? (
              <label className="switch-label">
                <input
                  type="checkbox"
                  checked={setting.value}
                  onChange={(event) => onRetryPolicy(setting.key, event.currentTarget.checked)}
                />
                <span>{setting.value ? "ON" : "OFF"}</span>
              </label>
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

function DryRunLedgerPanel({ ledger }) {
  return (
    <section className="panel dry-ledger-panel">
      <PanelHeader title="Dry Run 주문 원장" subtitle="브로커 전송 없이 기록된 주문 의도와 차단 결과입니다." />
      <div className="compact-list">
        {ledger.length === 0 ? (
          <EmptyRow text="아직 Dry Run 주문 의도가 없습니다." />
        ) : (
          ledger.map((order) => (
            <div className="compact-row ledger-row" key={order.order_id}>
              <strong>{order.symbol}</strong>
              <span>{order.order_id} · {order.attempts}/{order.max_attempts}회 · {order.reason}</span>
              <StatusPill tone={statusTone(order.state)}>{order.state}</StatusPill>
            </div>
          ))
        )}
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

function StrategyWorkflowPanel() {
  const steps = [
    ["BACKTEST", "최종 테스트 통과"],
    ["SHADOW", "실시간 신호 기록"],
    ["PAPER", "모의 체결 검증"],
    ["LIVE", "live_allowed 승인"],
  ];
  return (
    <section className="panel">
      <PanelHeader title="전략 승급 흐름" subtitle="실거래 전략은 승인 단계와 계약 권한을 모두 통과해야 합니다." />
      <div className="workflow-strip">
        {steps.map(([label, detail], index) => (
          <div className="workflow-step" key={label}>
            <span>{index + 1}</span>
            <strong>{label}</strong>
            <em>{detail}</em>
          </div>
        ))}
      </div>
    </section>
  );
}

function AuditExportPanel({ onExport, exportResult }) {
  return (
    <section className="panel">
      <PanelHeader title="감사 로그 내보내기" subtitle="주문 차단, 모드 변경, 설정 변경을 CSV/HTML로 저장합니다." />
      <div className="compact-list">
        <div className="compact-row">
          <strong>CSV</strong>
          <span>운영 이벤트 원장</span>
          <button className="mini-button" type="button" onClick={() => onExport("csv")}>
            <Download size={14} />
            저장
          </button>
        </div>
        <div className="compact-row">
          <strong>HTML</strong>
          <span>인쇄용 운용 리포트</span>
          <button className="mini-button" type="button" onClick={() => onExport("html")}>
            <Download size={14} />
            저장
          </button>
        </div>
      </div>
      {exportResult?.ok !== false && exportResult?.filename && (
        <div className="export-result">
          <Download size={15} />
          <span>{exportResult.filename} 생성 완료</span>
        </div>
      )}
    </section>
  );
}

function ReconciliationSummaryPanel({ reconciliation, onReconcile }) {
  const summary = reconciliation?.summary ?? fallbackSnapshot.reconciliation.summary;
  const actions = reconciliation?.next_actions ?? [];
  const items = [
    { label: "상태", value: summary.status_label, tone: statusTone(summary.status) },
    { label: "포지션", value: summary.position_count, tone: "info" },
    { label: "계좌", value: summary.account_count, tone: "info" },
    { label: "API 필요", value: summary.api_required_count, tone: summary.api_required_count ? "warning" : "success" },
    { label: "불일치", value: summary.mismatch_count, tone: summary.mismatch_count ? "danger" : "success" },
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
      </div>
      <div className="metric-grid">
        {items.map((item) => (
          <div className="metric-card" key={item.label}>
            <span>{item.label}</span>
            <strong>{item.value}</strong>
            <span className={`summary-state ${item.tone}`}>{item.tone === "success" ? "정상" : "확인"}</span>
          </div>
        ))}
      </div>
      <div className="next-actions">
        {actions.map((action) => (
          <span key={action}>{action}</span>
        ))}
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
      <div className="metric-grid">
        {items.map((item) => (
          <div className="metric-card" key={item.label}>
            <span>{item.label}</span>
            <strong>{item.value}</strong>
            <span className={`summary-state ${item.tone}`}>{item.tone === "success" ? "해제 가능" : "차단"}</span>
          </div>
        ))}
      </div>
      <div className="next-actions">
        {report.next_actions.map((action) => (
          <span key={action}>{action}</span>
        ))}
      </div>
    </section>
  );
}

function StrategyPanel({ strategies }) {
  return (
    <section className="panel strategy-panel">
      <PanelHeader title="전략 Artifact" subtitle="Backtester/Paper 승인 결과와 live_allowed 계약을 확인합니다." />
      <div className="data-table strategy-table">
        <div className="table-head">
          <span>전략</span>
          <span>심볼</span>
          <span>상태</span>
          <span>Score</span>
          <span>권한</span>
          <span>차단 사유</span>
        </div>
        {strategies.map((strategy) => (
          <div className="table-row" key={strategy.strategy_id}>
            <strong>{strategy.name}</strong>
            <span>{strategy.symbol}</span>
            <span>{strategy.lifecycle_status}</span>
            <span>{strategy.score}</span>
            <StatusPill tone={strategy.live_allowed ? "success" : "danger"}>{strategy.permission_label}</StatusPill>
            <em>{strategy.block_reason}</em>
          </div>
        ))}
      </div>
    </section>
  );
}

function OrderPanel({ orders, onRetryOrder, onCancelOrder }) {
  return (
    <section className="panel order-panel">
      <PanelHeader title="Order Blotter" subtitle="차단, Dry Run, 재시도, 취소 이벤트를 감사 추적합니다." />
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
                  <button
                    className="mini-icon-button"
                    type="button"
                    title="재시도"
                    aria-label={`${order.order_id} 재시도`}
                    disabled={!order.retryable}
                    onClick={() => onRetryOrder(order.order_id)}
                  >
                    <RotateCcw size={13} />
                  </button>
                  <button
                    className="mini-icon-button"
                    type="button"
                    title="취소"
                    aria-label={`${order.order_id} 취소`}
                    disabled={order.state === "canceled"}
                    onClick={() => onCancelOrder(order.order_id)}
                  >
                    <CircleStop size={13} />
                  </button>
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
  return (
    <section className="panel audit-panel">
      <PanelHeader title="감사 스트림" subtitle="모드 전환, 주문 차단, 설정 변경을 시간순으로 추적합니다." />
      <div className="audit-list">
        {audit.map((item, index) => (
          <div className={`audit-row ${item.level}`} key={`${item.time}-${index}`}>
            <Siren size={15} />
            <span>{item.time}</span>
            <strong>{item.event}</strong>
            <em>{item.detail}</em>
          </div>
        ))}
      </div>
    </section>
  );
}

function StatusRow({ label, status, detail }) {
  return (
    <div className={`status-row ${status}`}>
      {status === "pass" ? <ShieldCheck size={16} /> : <ShieldAlert size={16} />}
      <div>
        <strong>{label}</strong>
        <span>{detail}</span>
      </div>
      <StatusPill tone={statusTone(status)}>{status}</StatusPill>
    </div>
  );
}

function EmptyRow({ text }) {
  return (
    <div className="empty-row">
      <TerminalSquare size={16} />
      <span>{text}</span>
    </div>
  );
}

function PanelHeader({ title, subtitle }) {
  return (
    <div className="panel-header">
      <div>
        <h2>{title}</h2>
        <p>{subtitle}</p>
      </div>
    </div>
  );
}

export default App;
