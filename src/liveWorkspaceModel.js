const STATUS_DEFINITIONS = {
  normal: { label: "정상", tone: "success", known: true, blocking: false },
  ready: { label: "준비됨", tone: "success", known: true, blocking: false },
  connected: { label: "연결됨", tone: "success", known: true, blocking: false },
  running: { label: "실행 중", tone: "success", known: true, blocking: false },
  monitoring: { label: "모니터링", tone: "info", known: true, blocking: false },
  filled: { label: "체결 완료", tone: "success", known: true, blocking: false },
  partially_filled: { label: "부분 체결", tone: "info", known: true, blocking: false },
  submitted: { label: "접수 확인", tone: "info", known: true, blocking: false },
  pending: { label: "진행 중", tone: "info", known: true, blocking: false },
  review: { label: "확인 필요", tone: "warning", known: true, blocking: false },
  warning: { label: "주의", tone: "warning", known: true, blocking: false },
  degraded: { label: "성능 저하", tone: "warning", known: true, blocking: false },
  blocked: { label: "차단", tone: "danger", known: true, blocking: true },
  rejected: { label: "거절", tone: "danger", known: true, blocking: true },
  failed: { label: "오류", tone: "danger", known: true, blocking: true },
  critical: { label: "긴급", tone: "danger", known: true, blocking: true },
  stopped: { label: "중지", tone: "neutral", known: true, blocking: false },
  canceled: { label: "취소", tone: "neutral", known: true, blocking: false },
  not_applicable: { label: "해당 없음", tone: "neutral", known: true, blocking: false },
  unknown: { label: "확인 불가", tone: "warning", known: false, blocking: true },
};

const STATUS_ALIASES = {
  pass: "normal",
  passed: "normal",
  success: "normal",
  successful: "normal",
  ok: "normal",
  healthy: "normal",
  정상: "normal",
  통과: "normal",
  ready: "ready",
  configured: "ready",
  applied: "ready",
  complete: "ready",
  completed: "ready",
  준비: "ready",
  준비됨: "ready",
  connected: "connected",
  연결됨: "connected",
  running: "running",
  active: "running",
  started: "running",
  실행_중: "running",
  monitor: "monitoring",
  monitoring: "monitoring",
  관찰: "monitoring",
  filled: "filled",
  fill: "filled",
  체결: "filled",
  체결_완료: "filled",
  partially_filled: "partially_filled",
  partial_fill: "partially_filled",
  부분_체결: "partially_filled",
  acknowledged: "submitted",
  accepted: "submitted",
  submitted: "submitted",
  broker_submitted: "submitted",
  pending: "pending",
  starting: "pending",
  applying: "pending",
  checking: "pending",
  retrying: "pending",
  in_progress: "pending",
  wait: "review",
  waiting: "review",
  check: "review",
  review: "review",
  확인: "review",
  확인_필요: "review",
  warn: "warning",
  warning: "warning",
  watch: "warning",
  stale: "warning",
  주의: "warning",
  경고: "warning",
  degraded: "degraded",
  blocked: "blocked",
  block: "blocked",
  risk_blocked: "blocked",
  adapter_blocked: "blocked",
  retry_exhausted: "blocked",
  mismatch: "blocked",
  locked: "blocked",
  차단: "blocked",
  rejected: "rejected",
  reject: "rejected",
  거절: "rejected",
  fail: "failed",
  failed: "failed",
  error: "failed",
  crashed: "failed",
  오류: "failed",
  실패: "failed",
  critical: "critical",
  emergency: "critical",
  긴급: "critical",
  idle: "stopped",
  stopped: "stopped",
  disabled: "stopped",
  off: "stopped",
  중지: "stopped",
  canceled: "canceled",
  cancelled: "canceled",
  취소: "canceled",
  na: "not_applicable",
  "n/a": "not_applicable",
  not_applicable: "not_applicable",
  not_required: "not_applicable",
  해당_없음: "not_applicable",
  unknown: "unknown",
  unavailable: "unknown",
  unverified: "unknown",
  missing: "unknown",
  미확인: "unknown",
  확인_불가: "unknown",
};

const UNKNOWN_VALUE_TOKENS = new Set([
  "",
  "-",
  "--",
  "unknown",
  "unavailable",
  "undefined",
  "null",
  "none",
  "na",
  "n/a",
  "미확인",
  "확인 불가",
  "조회 정보 없음",
  "미조회",
  "평가 대기",
]);

const MODE_LABELS = {
  MONITOR: "모니터링",
  SMALL_LIVE: "소액 실거래",
  FULL_LIVE: "전체 실거래",
  LIVE: "실거래",
};

const TIMELINE_LABELS = {
  BAR_CLOSED: "확정 봉 수신",
  SIGNAL_DECIDED: "전략 신호 결정",
  TARGET_ALLOCATED: "목표 포지션 계산",
  RISK_DECIDED: "주문 전 리스크 판정",
  ORDER_CREATED: "주문 의도 생성",
  BROKER_SUBMIT: "브로커 전송",
  BROKER_SUBMITTED: "브로커 전송",
  ACKNOWLEDGED: "브로커 접수 확인",
  PARTIALLY_FILLED: "부분 체결",
  FILLED: "체결 완료",
  CANCELED: "주문 취소",
  CANCELLED: "주문 취소",
  REJECTED: "주문 거절",
  BLOCKED: "주문 차단",
  UNKNOWN_SUBMIT_RESULT: "접수 결과 확인 불가",
};

const TIMELINE_ORDER = Object.keys(TIMELINE_LABELS).reduce((result, key, index) => {
  result[key] = index;
  return result;
}, {});

const RETRY_MATRIX = [
  {
    id: "market-data-read",
    label: "시세 조회",
    autoRetry: true,
    directRetryAllowed: true,
    idempotencyRequired: false,
    nextAction: "지수 백오프와 레이트 리밋을 적용해 자동 재조회",
    tone: "success",
  },
  {
    id: "account-position-read",
    label: "잔고·포지션 조회",
    autoRetry: true,
    directRetryAllowed: true,
    idempotencyRequired: false,
    nextAction: "자동 재조회하되 실패 중에는 신규 위험 주문 차단",
    tone: "success",
  },
  {
    id: "public-metadata-read",
    label: "공개 메타데이터 조회",
    autoRetry: true,
    directRetryAllowed: true,
    idempotencyRequired: false,
    nextAction: "캐시를 유지하며 자동 재조회",
    tone: "success",
  },
  {
    id: "order-local-pre-submit",
    label: "주문 전 로컬 실패",
    autoRetry: true,
    directRetryAllowed: true,
    idempotencyRequired: true,
    nextAction: "동일 idempotency key로 브로커 전송 전에만 재시도",
    tone: "warning",
  },
  {
    id: "order-rate-limit-pre-acceptance",
    label: "주문 접수 전 레이트 리밋",
    autoRetry: false,
    directRetryAllowed: true,
    idempotencyRequired: true,
    nextAction: "브로커 미접수 증거가 있을 때만 제한적으로 재시도",
    tone: "warning",
  },
  {
    id: "order-submit-unknown",
    label: "주문 접수 결과 확인 불가",
    autoRetry: false,
    directRetryAllowed: false,
    idempotencyRequired: true,
    nextAction: "주문 ID·idempotency key로 브로커 조회 후 대조",
    tone: "danger",
  },
  {
    id: "order-rejected",
    label: "브로커 주문 거절",
    autoRetry: false,
    directRetryAllowed: false,
    idempotencyRequired: true,
    nextAction: "거절 사유와 주문 제약을 수정한 새 의도로 재평가",
    tone: "danger",
  },
  {
    id: "order-cancel-unknown",
    label: "취소 결과 확인 불가",
    autoRetry: false,
    directRetryAllowed: false,
    idempotencyRequired: true,
    nextAction: "브로커 미체결 주문을 조회하고 잔존 주문만 다시 취소",
    tone: "danger",
  },
  {
    id: "manual-review",
    label: "분류되지 않은 요청",
    autoRetry: false,
    directRetryAllowed: false,
    idempotencyRequired: false,
    nextAction: "요청 종류와 브로커 접수 여부를 확인",
    tone: "warning",
  },
];

function normalizedToken(value) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/[\s-]+/g, "_");
}

function firstValue(object, keys) {
  if (!object || typeof object !== "object") return undefined;
  for (const key of keys) {
    if (Object.prototype.hasOwnProperty.call(object, key)) return object[key];
  }
  return undefined;
}

function firstText(object, keys, fallback = "") {
  for (const key of keys) {
    const value = object?.[key];
    if (value == null) continue;
    const text = String(value).trim();
    if (text) return text;
  }
  return fallback;
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function safeObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function finiteNumber(value) {
  if (value == null) return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  const text = String(value).trim();
  if (UNKNOWN_VALUE_TOKENS.has(text.toLowerCase())) return null;
  const normalized = text.replaceAll(",", "").replace(/[^0-9.+-]/g, "");
  if (!/[0-9]/.test(normalized)) return null;
  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

function numericFrom(object, keys) {
  for (const key of keys) {
    const value = finiteNumber(object?.[key]);
    if (value != null) return { raw: object[key], value };
  }
  return { raw: firstValue(object, keys), value: null };
}

function formatNumber(value, unit = "") {
  if (value == null) return "확인 불가";
  const digits = Math.abs(value) >= 100 ? 2 : 6;
  const text = value.toLocaleString("ko-KR", { maximumFractionDigits: digits });
  return unit ? `${text} ${unit}` : text;
}

function statusSeverity(status) {
  return {
    critical: 6,
    failed: 5,
    blocked: 5,
    rejected: 5,
    unknown: 4,
    degraded: 3,
    warning: 3,
    review: 2,
    pending: 1,
  }[status?.code] ?? 0;
}

export function normalizeKoreanStatus(value) {
  if (value === true) value = "pass";
  if (value === false) value = "blocked";
  const raw = String(value ?? "").trim();
  const token = normalizedToken(raw);
  const code = STATUS_ALIASES[token] || "unknown";
  return { raw, code, ...STATUS_DEFINITIONS[code] };
}

function normalizeMode(value) {
  const code = String(value || "MONITOR").trim().toUpperCase().replace(/[\s-]+/g, "_");
  return {
    code,
    label: MODE_LABELS[code] || "확인 불가",
    tone: code === "FULL_LIVE" || code === "LIVE" ? "danger" : code === "SMALL_LIVE" ? "warning" : "info",
  };
}

function strategyIdsFromDeployment(deployment) {
  const values = [
    ...asArray(deployment?.strategyIds),
    ...asArray(deployment?.strategy_ids),
    ...asArray(deployment?.strategy_instances),
    ...asArray(deployment?.strategyInstances),
  ];
  return [...new Set(values.map((item) => (
    typeof item === "string"
      ? item
      : firstText(item, ["strategyId", "strategy_id", "id", "instanceId", "instance_id"])
  )).filter(Boolean))];
}

function normalizeDeployment(deployment, source, index) {
  const portfolioArtifact = safeObject(deployment?.portfolioArtifact || deployment?.portfolio_artifact);
  const strategyArtifact = safeObject(deployment?.strategyArtifact || deployment?.strategy_artifact);
  const deploymentId = firstText(deployment, ["deploymentId", "deployment_id", "id"], `deployment-${index + 1}`);
  const portfolioId = firstText(deployment, ["portfolioId", "portfolio_id"], firstText(portfolioArtifact, ["portfolioId", "portfolio_id", "id"]));
  const strategyIds = strategyIdsFromDeployment(deployment);
  const directStrategyId = firstText(deployment, ["strategyId", "strategy_id"], firstText(strategyArtifact, ["strategyId", "strategy_id", "id"]));
  if (directStrategyId && !strategyIds.includes(directStrategyId)) strategyIds.push(directStrategyId);
  return {
    deploymentId,
    portfolioId,
    strategyId: directStrategyId || strategyIds[0] || "",
    strategyIds,
    name: firstText(deployment, ["name", "label", "title"], portfolioId || deploymentId),
    environment: firstText(deployment, ["environment", "mode"], "LIVE").toUpperCase(),
    mode: firstText(deployment, ["mode", "environment"], "MONITOR").toUpperCase(),
    brokerId: firstText(deployment, ["brokerId", "broker_id", "provider"]),
    accountId: firstText(deployment, ["accountId", "account_id", "account"]),
    symbol: firstText(deployment, ["symbol", "instrumentId", "instrument_id"]),
    timeframe: firstText(deployment, ["timeframe", "interval"]),
    revision: firstValue(deployment, ["revision", "deploymentRevision", "deployment_revision"]),
    contentHash: firstText(deployment, ["contentHash", "content_hash", "artifactHash", "artifact_hash"]),
    active: deployment?.active === true || deployment?.running === true || ["LIVE", "LIVE_ROLLOUT", "RUNNING"].includes(String(deployment?.lifecycle || deployment?.status || "").toUpperCase()),
    source,
    raw: deployment,
  };
}

function deploymentCandidates(snapshot) {
  const result = [];
  const byId = new Map();
  const add = (candidate) => {
    const existing = byId.get(candidate.deploymentId);
    if (existing) {
      existing.strategyIds = [...new Set([...existing.strategyIds, ...candidate.strategyIds])];
      existing.strategyId ||= candidate.strategyId;
      existing.portfolioId ||= candidate.portfolioId;
      existing.brokerId ||= candidate.brokerId;
      existing.accountId ||= candidate.accountId;
      existing.symbol ||= candidate.symbol;
      existing.timeframe ||= candidate.timeframe;
      existing.contentHash ||= candidate.contentHash;
      existing.active ||= candidate.active;
      return existing;
    }
    byId.set(candidate.deploymentId, candidate);
    result.push(candidate);
    return candidate;
  };

  asArray(snapshot?.deployments).forEach((item, index) => add(normalizeDeployment(item, "deployment", index)));
  asArray(snapshot?.portfolios).forEach((portfolio, index) => {
    const portfolioId = firstText(portfolio, ["portfolioId", "portfolio_id", "id", "source_path"], `portfolio-${index + 1}`);
    const candidate = normalizeDeployment({
      ...portfolio,
      deploymentId: firstText(portfolio, ["deploymentId", "deployment_id"], portfolioId),
      portfolioId,
      strategyIds: strategyIdsFromDeployment(portfolio),
    }, "portfolio", index);
    add(candidate);
  });
  asArray(snapshot?.strategies).forEach((strategy, index) => {
    const strategyId = firstText(strategy, ["strategyId", "strategy_id", "id"], `strategy-${index + 1}`);
    const portfolioGate = safeObject(strategy?.portfolio_gate || strategy?.portfolioGate);
    const deploymentId = firstText(strategy, ["deploymentId", "deployment_id"], firstText(portfolioGate, ["deploymentId", "deployment_id"], strategyId));
    const candidate = add(normalizeDeployment({
      ...strategy,
      deploymentId,
      portfolioId: firstText(strategy, ["portfolioId", "portfolio_id"], firstText(portfolioGate, ["portfolioId", "portfolio_id"])),
      strategyId,
      strategyIds: [strategyId],
      active: strategy?.running === true || strategy?.live_allowed === true,
    }, "strategy", index));
    candidate.strategyIds = [...new Set([...candidate.strategyIds, strategyId])];
    candidate.strategyId ||= strategyId;
  });
  return result;
}

function activeRuntimeScope(snapshot) {
  const runtime = safeObject(snapshot?.continuous_runtime || snapshot?.continuousRuntime);
  const profiles = Object.values(safeObject(runtime.profiles));
  const active = profiles.find((profile) => profile?.running) || (runtime.running ? runtime : {}) || {};
  return {
    deploymentId: firstText(active, ["deploymentId", "deployment_id"], firstText(runtime, ["deploymentId", "deployment_id"])),
    portfolioId: firstText(active, ["portfolioId", "portfolio_id"], firstText(runtime, ["portfolioId", "portfolio_id"])),
    strategyId: firstText(active, ["strategyId", "strategy_id", "artifactId", "artifact_id"]),
    profileId: firstText(active, ["profileId", "profile_id"]),
    running: active?.running === true || runtime?.running === true,
    raw: active,
  };
}

function deploymentMatches(candidate, key, value) {
  if (!value) return false;
  if (key === "strategyId") return candidate.strategyIds.includes(value) || candidate.strategyId === value;
  return candidate[key] === value;
}

export function selectCurrentDeploymentContext(snapshot = {}, preference = {}) {
  const candidates = deploymentCandidates(snapshot);
  if (!candidates.length) {
    return {
      known: false,
      source: "none",
      deploymentId: "",
      portfolioId: "",
      strategyId: "",
      strategyIds: [],
      name: "선택된 Deployment 없음",
      contextKey: "deployment:unknown",
      requiresConfirmation: true,
      mismatches: [],
    };
  }
  const runtime = activeRuntimeScope(snapshot);
  const requested = {
    deploymentId: firstText(preference, ["deploymentId", "deployment_id"]),
    portfolioId: firstText(preference, ["portfolioId", "portfolio_id"]),
    strategyId: firstText(preference, ["strategyId", "strategy_id"]),
  };
  const selectors = [
    ["explicit-deployment", "deploymentId", requested.deploymentId],
    ["explicit-portfolio", "portfolioId", requested.portfolioId],
    ["explicit-strategy", "strategyId", requested.strategyId],
    ["runtime-deployment", "deploymentId", runtime.deploymentId],
    ["runtime-portfolio", "portfolioId", runtime.portfolioId],
    ["runtime-strategy", "strategyId", runtime.strategyId],
  ];
  let selected = null;
  let source = "default";
  for (const [candidateSource, key, value] of selectors) {
    if (!value) continue;
    const match = candidates.find((candidate) => deploymentMatches(candidate, key, value));
    if (match) {
      selected = match;
      source = candidateSource;
      break;
    }
  }
  if (!selected) {
    selected = candidates.find((candidate) => candidate.active) || candidates[0];
    source = selected.active ? "active-fallback" : "default";
  }
  const selectedStrategy = asArray(snapshot?.strategies).find((item) => selected.strategyIds.includes(firstText(item, ["strategyId", "strategy_id", "id"]))) || {};
  const selectedPortfolio = asArray(snapshot?.portfolios).find((item) => selected.portfolioId && selected.portfolioId === firstText(item, ["portfolioId", "portfolio_id", "id", "source_path"])) || {};
  const enriched = {
    ...selected,
    brokerId: selected.brokerId || firstText(selectedStrategy, ["brokerId", "broker_id", "provider"]),
    accountId: selected.accountId || firstText(selectedStrategy, ["accountId", "account_id", "account"]),
    symbol: selected.symbol || firstText(selectedStrategy, ["symbol", "instrumentId", "instrument_id"]),
    timeframe: selected.timeframe || firstText(selectedStrategy, ["timeframe", "interval"]),
    contentHash: selected.contentHash || firstText(selectedPortfolio, ["contentHash", "content_hash", "artifactHash", "artifact_hash"]) || firstText(selectedStrategy, ["contentHash", "content_hash", "artifactHash", "artifact_hash"]),
  };
  const mismatches = [];
  if (requested.deploymentId && requested.deploymentId !== enriched.deploymentId) mismatches.push("deployment-id-mismatch");
  if (requested.portfolioId && requested.portfolioId !== enriched.portfolioId) mismatches.push("portfolio-id-mismatch");
  if (requested.strategyId && !enriched.strategyIds.includes(requested.strategyId)) mismatches.push("strategy-id-mismatch");
  return {
    ...enriched,
    known: true,
    source,
    requiresConfirmation: source === "default" || source === "active-fallback",
    mismatches,
    contextKey: [enriched.deploymentId, enriched.revision ?? "-", enriched.contentHash || "-"].join(":"),
  };
}

function safetyModel(snapshot, mode) {
  const centralControl = safeObject(snapshot?.central_control || snapshot?.centralControl);
  const apiConnected = snapshot?.api_connected === true || snapshot?.apiConnected === true;
  const apiExplicitlyDisconnected = snapshot?.api_connected === false || snapshot?.apiConnected === false;
  const killSwitch = snapshot?.kill_switch === true || snapshot?.killSwitch === true || centralControl.globalKill === true;
  const halted = centralControl.halted === true;
  const entryBlocked = snapshot?.new_entries_blocked !== false && snapshot?.newEntriesBlocked !== false;
  const dryRun = snapshot?.dry_run !== false && snapshot?.dryRun !== false;
  let level = "live";
  let label = mode.code === "SMALL_LIVE" ? "소액 실거래 제한" : "실거래 허용";
  let tone = mode.code === "SMALL_LIVE" ? "warning" : "danger";
  const reasons = [];
  if (apiExplicitlyDisconnected || (!apiConnected && snapshot?.api_connected == null && snapshot?.apiConnected == null)) {
    level = "fail_closed";
    label = "상태 확인 불가 · 안전 차단";
    tone = "danger";
    reasons.push("API 연결 상태 확인 불가");
  } else if (killSwitch || halted) {
    level = "emergency";
    label = "긴급 차단";
    tone = "danger";
    if (killSwitch) reasons.push("전역 Kill Switch 활성화");
    if (halted) reasons.push("중앙 제어 중지 상태");
  } else if (entryBlocked) {
    level = "entry_blocked";
    label = "신규 진입 차단";
    tone = "warning";
    reasons.push("신규 위험 증가 주문 차단");
  } else if (dryRun) {
    level = "dry_run";
    label = "모의 전송만 허용";
    tone = "info";
    reasons.push("Dry Run 보호 활성화");
  } else if (mode.code === "MONITOR") {
    level = "monitor";
    label = "모니터링 전용";
    tone = "info";
    reasons.push("MONITOR 모드");
  }
  const realOrderAllowed = ["live"].includes(level) && ["SMALL_LIVE", "FULL_LIVE", "LIVE"].includes(mode.code);
  return {
    level,
    label,
    tone,
    reasons,
    realOrderAllowed,
    riskIncreasingOrdersAllowed: realOrderAllowed,
    simulatedOrdersAllowed: level === "dry_run" || level === "monitor",
    cancelAllowed: true,
    riskReductionAllowed: true,
  };
}

export function buildLiveEnvironmentModel(snapshot = {}, deploymentContext = null) {
  const context = deploymentContext?.contextKey
    ? deploymentContext
    : selectCurrentDeploymentContext(snapshot, deploymentContext || {});
  const mode = normalizeMode(snapshot?.mode || context?.mode || "MONITOR");
  const safety = safetyModel(snapshot, mode);
  const runtime = activeRuntimeScope(snapshot);
  const selectedBroker = asArray(snapshot?.brokers).find((broker) => firstText(broker, ["brokerId", "broker_id"]) === context.brokerId);
  const sessionId = firstText(runtime.raw, ["sessionId", "session_id", "runId", "run_id"], firstText(snapshot, ["sessionId", "session_id"]));
  const brokerLabel = firstText(selectedBroker, ["name", "label"], context.brokerId || "확인 불가");
  return {
    appEnvironment: "LIVE",
    appEnvironmentLabel: "실거래",
    titlePrefix: "[LIVE]",
    watermark: "LIVE",
    mode,
    safety,
    deployment: context,
    barItems: [
      { id: "environment", label: "환경", value: "LIVE", tone: "danger" },
      { id: "mode", label: "운영 모드", value: mode.label, tone: mode.tone },
      { id: "session", label: "세션", value: sessionId || "미실행", tone: sessionId ? "info" : "neutral" },
      { id: "deployment", label: "Deployment", value: context.known ? context.name : "선택 필요", tone: context.known && !context.requiresConfirmation ? "success" : "warning" },
      { id: "broker", label: "브로커", value: brokerLabel, tone: selectedBroker?.order_ready ? "success" : "warning" },
      { id: "account", label: "계좌", value: context.accountId || "확인 불가", tone: context.accountId ? "info" : "warning" },
      { id: "safety", label: "안전 상태", value: safety.label, tone: safety.tone },
    ],
  };
}

function componentRow(id, label, rawStatus, detail, lastSeenAt = "") {
  const status = normalizeKoreanStatus(rawStatus);
  return { id, label, status, detail: String(detail || ""), lastSeenAt: String(lastSeenAt || "") };
}

export function buildRuntimeComponentModel(snapshot = {}, deploymentContext = null) {
  const context = deploymentContext?.contextKey
    ? deploymentContext
    : selectCurrentDeploymentContext(snapshot, deploymentContext || {});
  const runtime = safeObject(snapshot?.continuous_runtime || snapshot?.continuousRuntime);
  const profiles = Object.values(safeObject(runtime.profiles));
  const profile = profiles.find((item) => (
    (context.portfolioId && firstText(item, ["portfolioId", "portfolio_id"]) === context.portfolioId)
    || (context.deploymentId && firstText(item, ["deploymentId", "deployment_id"]) === context.deploymentId)
  )) || profiles.find((item) => item?.running) || runtime;
  const engine = safeObject(profile.engine || runtime.engine);
  const market = safeObject(profile.marketData || profile.market_data || runtime.marketData || runtime.market_data || snapshot?.market_data);
  const streams = safeObject(snapshot?.execution_streams || snapshot?.executionStreams);
  const watchdog = safeObject(snapshot?.watchdog);
  const environment = buildLiveEnvironmentModel(snapshot, context);
  const selectedBroker = asArray(snapshot?.brokers).find((broker) => firstText(broker, ["brokerId", "broker_id"]) === context.brokerId);
  const runtimeRunning = profile?.running === true || runtime?.running === true;
  const marketStatus = firstText(market, ["status", "phase"])
    || (firstText(engine, ["lastBarAt", "last_bar_at", "lastCycleAt", "last_cycle_at"]) ? (runtimeRunning ? "running" : "stopped") : (runtimeRunning ? "unknown" : "stopped"));
  const schedulerStatus = firstText(profile, ["phase", "status"], runtimeRunning ? "running" : "stopped");
  const riskStatus = environment.safety.level === "live" ? "ready" : ["monitor", "dry_run"].includes(environment.safety.level) ? "monitoring" : "blocked";
  const streamErrors = asArray(streams.errors);
  const routerStatus = streamErrors.length
    ? "error"
    : streams.running === true
      ? "running"
      : environment.mode.code === "MONITOR"
        ? "stopped"
        : "unknown";
  const watchdogStatus = firstText(watchdog, ["status"], "unknown");
  const brokerStatus = selectedBroker
    ? (selectedBroker.order_ready === true ? "ready" : firstText(selectedBroker, ["status"], "blocked"))
    : "unknown";
  const components = [
    componentRow("market-data", "시장 데이터", marketStatus, firstText(market, ["detail", "message"], firstText(engine, ["lastCycleAt", "last_cycle_at"], "마지막 수신 시각 확인 필요")), firstText(market, ["lastSeenAt", "last_seen_at", "updatedAt", "updated_at"])),
    componentRow("strategy-scheduler", "전략 스케줄러", schedulerStatus, runtimeRunning ? "확정 봉마다 전략을 평가합니다." : "스케줄러가 실행 중이 아닙니다.", firstText(engine, ["lastCycleAt", "last_cycle_at"])),
    componentRow("risk-gateway", "리스크 게이트웨이", riskStatus, environment.safety.label),
    componentRow("order-router", "주문 라우터", routerStatus, streamErrors[0]?.detail || streamErrors[0]?.message || (streams.running ? "체결 스트림 감시 중" : "주문·체결 스트림 중지"), firstText(streams, ["lastPoll", "last_poll", "updatedAt", "updated_at"])),
    componentRow("watchdog", "Live Watchdog", watchdogStatus, firstText(watchdog, ["last_action", "lastAction", "detail"], "점검 이력 확인 필요"), firstText(watchdog, ["last_run", "lastRun"])),
    componentRow("broker", "브로커 연결", brokerStatus, firstText(selectedBroker, ["detail"], context.brokerId || "현재 Deployment의 브로커 선택 필요")),
  ];
  const overall = [...components].sort((left, right) => statusSeverity(right.status) - statusSeverity(left.status))[0]?.status || normalizeKoreanStatus("unknown");
  return { deployment: context, overall, components };
}

export function buildRetryMatrix() {
  return RETRY_MATRIX.map((row) => ({ ...row }));
}

export function classifyRetryRequest(request = {}) {
  const operation = normalizedToken(firstText(request, ["operation", "kind", "requestType", "request_type"]));
  const outcome = normalizedToken(firstText(request, ["outcome", "status", "result", "state"]));
  const phase = normalizedToken(firstText(request, ["phase", "stage"]));
  const method = String(request?.method || "").toUpperCase();
  const brokerExplicitlyNotAccepted = request?.brokerAccepted === false
    || request?.broker_accepted === false
    || normalizedToken(request?.acceptance) === "not_accepted";
  const safelyBeforeAcceptance = brokerExplicitlyNotAccepted
    || ["local", "pre_submit", "before_submit", "pre_acceptance", "before_acceptance"].includes(phase);
  let id = "manual-review";
  if (operation.includes("cancel") && ["unknown", "timeout", "network_error", "unknown_submit_result"].includes(outcome)) id = "order-cancel-unknown";
  else if (["unknown", "timeout", "network_error", "unknown_submit_result", "reconcile_required"].includes(outcome) && operation.includes("order")) id = "order-submit-unknown";
  else if (["rejected", "invalid", "validation_failed", "risk_blocked"].includes(outcome) && operation.includes("order")) id = "order-rejected";
  else if (["rate_limit", "rate_limited", "429"].includes(outcome) && operation.includes("order")) id = safelyBeforeAcceptance ? "order-rate-limit-pre-acceptance" : "order-submit-unknown";
  else if (operation.includes("order") && ["local", "pre_submit", "before_submit", "validation"].includes(phase)) id = "order-local-pre-submit";
  else if (operation.includes("account") || operation.includes("balance") || operation.includes("position")) id = "account-position-read";
  else if (operation.includes("metadata") || operation.includes("exchange_info") || operation.includes("instrument")) id = "public-metadata-read";
  else if (method === "GET" || operation.includes("market") || operation.includes("quote") || operation.includes("candle")) id = "market-data-read";
  return { ...RETRY_MATRIX.find((row) => row.id === id), request: { operation, outcome, phase, method } };
}

function identifierSet(value) {
  return new Set([
    firstText(value, ["orderId", "order_id"]),
    firstText(value, ["omsOrderId", "oms_order_id"]),
    firstText(value, ["brokerOrderId", "broker_order_id"]),
    firstText(value, ["traceId", "trace_id"]),
    firstText(value, ["idempotencyKey", "idempotency_key"]),
  ].filter(Boolean));
}

function timelineStage(value) {
  const stage = firstText(value, ["stage", "eventType", "event_type", "state", "status", "decision"], "UNKNOWN");
  return stage.toUpperCase().replace(/[\s-]+/g, "_");
}

function timelineStatus(stage, value) {
  if (stage === "UNKNOWN_SUBMIT_RESULT") return normalizeKoreanStatus("unknown");
  if (["REJECTED", "BLOCKED"].includes(stage)) return normalizeKoreanStatus("blocked");
  if (stage.includes("FILL")) return normalizeKoreanStatus(stage === "FILLED" ? "filled" : "partially_filled");
  if (stage.includes("CANCEL")) return normalizeKoreanStatus("canceled");
  return normalizeKoreanStatus(firstText(value, ["status", "state", "decision"], stage === "RISK_DECIDED" ? "review" : "pending"));
}

function timelineEntry(value, index, synthetic = false) {
  const stage = timelineStage(value);
  const time = firstText(value, ["occurredAt", "occurred_at", "createdAt", "created_at", "updatedAt", "updated_at", "timestamp", "time"]);
  return {
    id: firstText(value, ["eventId", "event_id", "id"], `${stage}-${index}`),
    stage,
    label: TIMELINE_LABELS[stage] || "주문 상태 변경",
    time,
    status: timelineStatus(stage, value),
    detail: firstText(value, ["detail", "message", "reason"], firstText(value, ["decision", "state", "status"])),
    source: firstText(value, ["source", "category"], synthetic ? "order" : "event"),
    synthetic,
    index,
  };
}

function eventMatchesOrder(event, orderIdentifiers) {
  if (!orderIdentifiers.size) return true;
  const eventIdentifiers = identifierSet(event);
  return [...eventIdentifiers].some((identifier) => orderIdentifiers.has(identifier));
}

function orderHasBrokerSubmission(order) {
  const brokerOrderId = firstText(order, ["brokerOrderId", "broker_order_id"]);
  const state = normalizedToken(firstText(order, ["state", "status", "oms_status"]));
  return Boolean(brokerOrderId && brokerOrderId !== "-") || ["submitted", "acknowledged", "partially_filled", "filled", "unknown", "unknown_submit_result"].includes(state);
}

function unknownSubmitResult(order) {
  const values = [order?.state, order?.status, order?.oms_status, order?.queue_state, order?.queueState].map(normalizedToken);
  return values.some((value) => ["unknown", "unknown_submit_result", "reconcile_required"].includes(value));
}

export function projectOrderTimeline(order = {}, events = []) {
  const identifiers = identifierSet(order);
  const rows = asArray(events)
    .filter((event) => eventMatchesOrder(event, identifiers))
    .map((event, index) => timelineEntry(event, index));
  const createdAt = firstText(order, ["createdAt", "created_at", "time"]);
  rows.push(timelineEntry({ stage: "ORDER_CREATED", time: createdAt, reason: firstText(order, ["reason"]) }, rows.length, true));
  if (order?.risk_report || order?.riskReport || ["risk_blocked", "adapter_blocked"].includes(normalizedToken(order?.state))) {
    rows.push(timelineEntry({ stage: "RISK_DECIDED", time: createdAt, state: String(order?.state || "review").includes("blocked") ? "blocked" : "pass", reason: firstText(order, ["reason"]) }, rows.length, true));
  }
  if (orderHasBrokerSubmission(order)) {
    rows.push(timelineEntry({ stage: "BROKER_SUBMIT", time: firstText(order, ["submittedAt", "submitted_at", "updatedAt", "updated_at", "time"]), state: unknownSubmitResult(order) ? "unknown" : "submitted", reason: firstText(order, ["broker_order_id", "brokerOrderId"]) }, rows.length, true));
  }
  const finalState = unknownSubmitResult(order) ? "UNKNOWN_SUBMIT_RESULT" : timelineStage({ state: firstText(order, ["state", "status", "oms_status"], "UNKNOWN") });
  rows.push(timelineEntry({ stage: finalState, time: firstText(order, ["updatedAt", "updated_at", "time"]), state: finalState, reason: firstText(order, ["reason"]) }, rows.length, true));
  const seen = new Set();
  const unique = rows.filter((row) => {
    const key = [row.stage, row.time, row.detail].join("|");
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
  unique.sort((left, right) => {
    const leftTime = Date.parse(left.time);
    const rightTime = Date.parse(right.time);
    if (Number.isFinite(leftTime) && Number.isFinite(rightTime) && leftTime !== rightTime) return leftTime - rightTime;
    return (TIMELINE_ORDER[left.stage] ?? 999) - (TIMELINE_ORDER[right.stage] ?? 999) || left.index - right.index;
  });
  return {
    orderId: firstText(order, ["orderId", "order_id", "omsOrderId", "oms_order_id"]),
    status: normalizeKoreanStatus(unknownSubmitResult(order) ? "unknown" : firstText(order, ["state", "status", "oms_status"])),
    unknownSubmitResult: unknownSubmitResult(order),
    directRetryAllowed: !unknownSubmitResult(order) && order?.retryable === true,
    timeline: unique.map(({ index, ...row }) => row),
  };
}

function executionSlippageBps(row) {
  const explicit = numericFrom(row, ["slippageBps", "slippage_bps", "executionSlippageBps", "execution_slippage_bps"]).value;
  if (explicit != null) return explicit;
  const fill = numericFrom(row, ["fillPrice", "fill_price", "averageFillPrice", "average_fill_price", "average_price"]).value;
  const reference = numericFrom(row, ["referencePrice", "reference_price", "expectedPrice", "expected_price"]).value;
  if (fill == null || reference == null || reference === 0) return null;
  const side = String(row?.side || "BUY").toUpperCase();
  return (side === "SELL" ? reference - fill : fill - reference) / reference * 10_000;
}

function average(values) {
  return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : null;
}

export function projectExecutionQuality(orders = [], events = [], calibration = {}) {
  const orderRows = asArray(orders);
  const states = orderRows.map((order) => normalizedToken(firstText(order, ["state", "status", "oms_status"])));
  const submitted = orderRows.filter(orderHasBrokerSubmission).length;
  const filled = states.filter((state) => state === "filled").length;
  const partiallyFilled = states.filter((state) => state === "partially_filled").length;
  const rejected = states.filter((state) => ["rejected", "risk_blocked", "adapter_blocked", "retry_exhausted"].includes(state)).length;
  const unknown = orderRows.filter(unknownSubmitResult).length;
  const canceled = states.filter((state) => ["canceled", "cancelled"].includes(state)).length;
  const samples = [...orderRows, ...asArray(events)];
  const slippage = samples.map(executionSlippageBps).filter((value) => value != null);
  const latency = samples.map((row) => numericFrom(row, ["latencyMs", "latency_ms", "executionLatencyMs", "execution_latency_ms"]).value).filter((value) => value != null);
  const feesByCurrency = {};
  samples.forEach((row) => {
    const fee = numericFrom(row, ["fee", "feeAmount", "fee_amount", "commission"]).value;
    if (fee == null) return;
    const currency = firstText(row, ["feeCurrency", "fee_currency", "commissionAsset", "commission_asset", "currency"], "기타").toUpperCase();
    feesByCurrency[currency] = (feesByCurrency[currency] || 0) + fee;
  });
  const modelError = finiteNumber(firstValue(calibration, ["meanAbsoluteModelErrorBps", "mean_absolute_model_error_bps"]));
  const p95Slippage = finiteNumber(firstValue(calibration, ["p95AbsoluteSlippageBps", "p95_absolute_slippage_bps"]));
  return {
    sampleCount: orderRows.length,
    submitted,
    filled,
    partiallyFilled,
    rejected,
    unknownSubmitResult: unknown,
    canceled,
    fillRatePct: submitted ? filled / submitted * 100 : null,
    averageSlippageBps: average(slippage),
    averageLatencyMs: average(latency),
    feesByCurrency,
    calibration: {
      sampleCount: finiteNumber(calibration?.sampleCount ?? calibration?.sample_count),
      meanAbsoluteModelErrorBps: modelError,
      p95AbsoluteSlippageBps: p95Slippage,
      status: normalizeKoreanStatus(calibration?.status),
    },
    availability: normalizeKoreanStatus(orderRows.length || asArray(events).length ? "ready" : "unknown"),
  };
}

export function projectAccountValue(value, unit = "") {
  const numeric = finiteNumber(value);
  return {
    raw: value,
    known: numeric != null,
    value: numeric,
    label: formatNumber(numeric, unit),
    status: normalizeKoreanStatus(numeric == null ? "unknown" : "pass"),
  };
}

function truthKey(row) {
  const brokerId = firstText(row, ["brokerId", "broker_id", "provider"], "unknown").toLowerCase();
  const symbol = firstText(row, ["symbol", "instrumentId", "instrument_id"], "unknown").toUpperCase();
  const side = firstText(row, ["positionSide", "position_side", "side"], "NET").toUpperCase();
  return `${brokerId}:${symbol}:${side}`;
}

function truthMap(rows, source) {
  const result = new Map();
  asArray(rows).forEach((row) => {
    const quantityKeys = source === "broker"
      ? ["brokerQtyValue", "broker_qty_value", "brokerQty", "broker_qty", "quantity", "qty"]
      : source === "stream"
        ? ["streamQty", "stream_qty", "positionQty", "position_qty", "netQuantity", "net_quantity", "quantity", "qty"]
        : ["programQtyValue", "program_qty_value", "programQty", "program_qty", "quantity", "qty"];
    const quantity = numericFrom(row, quantityKeys).value;
    const key = truthKey(row);
    result.set(key, {
      key,
      brokerId: key.split(":")[0],
      symbol: key.split(":")[1],
      positionSide: key.split(":")[2],
      quantity,
      known: quantity != null,
      updatedAt: firstText(row, ["updatedAt", "updated_at", "capturedAt", "captured_at", "time", "timestamp"]),
      raw: row,
    });
  });
  return result;
}

function quantitiesMatch(values, tolerance) {
  const scale = Math.max(1, ...values.map((value) => Math.abs(value)));
  return Math.max(...values) - Math.min(...values) <= tolerance * scale;
}

export function projectThreeWayReconciliation(input = {}, options = {}) {
  const brokerRows = input.brokerPositions || input.reconciliation?.positions || input.positions || [];
  const streamRows = input.streamPositions || input.executionEvents?.positions || input.execution_events?.positions || [];
  const ledgerRows = input.ledgerPositions || input.programLedger?.positions || input.program_ledger?.positions || [];
  const tolerance = Number.isFinite(Number(options.tolerance)) ? Math.max(0, Number(options.tolerance)) : 1e-8;
  const sources = {
    broker: truthMap(brokerRows, "broker"),
    stream: truthMap(streamRows, "stream"),
    ledger: truthMap(ledgerRows, "ledger"),
  };
  const keys = [...new Set(Object.values(sources).flatMap((source) => [...source.keys()]))].sort();
  const rows = keys.map((key) => {
    const broker = sources.broker.get(key) || { key, known: false, quantity: null, updatedAt: "" };
    const stream = sources.stream.get(key) || { key, known: false, quantity: null, updatedAt: "" };
    const ledger = sources.ledger.get(key) || { key, known: false, quantity: null, updatedAt: "" };
    const values = [broker.quantity, stream.quantity, ledger.quantity];
    const allKnown = [broker, stream, ledger].every((source) => source.known);
    const matched = allKnown && quantitiesMatch(values, tolerance);
    const status = normalizeKoreanStatus(!allKnown ? "unknown" : matched ? "pass" : "mismatch");
    const [brokerId, symbol, positionSide] = key.split(":");
    return {
      key,
      brokerId,
      symbol,
      positionSide,
      broker: { ...broker, label: formatNumber(broker.quantity) },
      stream: { ...stream, label: formatNumber(stream.quantity) },
      ledger: { ...ledger, label: formatNumber(ledger.quantity) },
      status,
      blocking: !matched,
    };
  });
  const matchedCount = rows.filter((row) => row.status.code === "normal").length;
  const mismatchCount = rows.filter((row) => row.status.code === "blocked").length;
  const unknownCount = rows.filter((row) => row.status.code === "unknown").length;
  return {
    rows,
    summary: {
      total: rows.length,
      matchedCount,
      mismatchCount,
      unknownCount,
      status: normalizeKoreanStatus(mismatchCount ? "mismatch" : unknownCount || !rows.length ? "unknown" : "pass"),
    },
    blocking: mismatchCount > 0 || unknownCount > 0 || !rows.length,
  };
}

export function projectAccountReconciliation(snapshot = {}) {
  const accounts = asArray(snapshot?.accounts).map((account) => {
    const currency = firstText(account, ["currency"], "");
    const cash = numericFrom(account, ["broker_cash_value", "brokerCashValue", "broker_cash", "brokerCash"]);
    const equity = numericFrom(account, ["broker_equity_value", "brokerEquityValue", "broker_equity", "brokerEquity"]);
    return {
      id: firstText(account, ["brokerId", "broker_id", "account", "id"]),
      brokerId: firstText(account, ["brokerId", "broker_id"]),
      brokerLabel: firstText(account, ["brokerName", "broker_name", "name"]),
      accountLabel: firstText(account, ["account", "accountId", "account_id"]),
      currency,
      cash: projectAccountValue(cash.value, currency),
      equity: projectAccountValue(equity.value, currency),
      status: normalizeKoreanStatus(account?.status),
      unknown: cash.value == null && equity.value == null,
      raw: account,
    };
  });
  const reconciliation = projectThreeWayReconciliation({
    positions: snapshot?.positions || snapshot?.reconciliation?.positions,
    streamPositions: snapshot?.stream_positions || snapshot?.execution_events?.positions,
    ledgerPositions: snapshot?.program_ledger?.positions,
  });
  return {
    accounts,
    knownAccountCount: accounts.filter((account) => !account.unknown).length,
    unknownAccountCount: accounts.filter((account) => account.unknown).length,
    reconciliation,
  };
}

function incidentState(value) {
  const token = normalizedToken(value);
  if (["resolved", "closed", "recovered", "해결"].includes(token)) return { code: "resolved", label: "해결됨", tone: "success" };
  if (["acknowledged", "ack", "확인됨"].includes(token)) return { code: "acknowledged", label: "확인됨", tone: "info" };
  return { code: "open", label: "발생", tone: "danger" };
}

function normalizedIncident(incident, index, derived = false) {
  const severity = normalizeKoreanStatus(firstText(incident, ["severity", "level"], "warning"));
  return {
    id: firstText(incident, ["incidentId", "incident_id", "id"], `incident-${index + 1}`),
    title: firstText(incident, ["title", "event", "name"], "운영 사고"),
    detail: firstText(incident, ["detail", "message", "reason"]),
    occurredAt: firstText(incident, ["occurredAt", "occurred_at", "createdAt", "created_at", "timestamp", "time"]),
    severity,
    state: incidentState(firstText(incident, ["state", "status"], "open")),
    source: firstText(incident, ["source", "category"], derived ? "fallback" : "incident-store"),
    deploymentId: firstText(incident, ["deploymentId", "deployment_id"]),
    accountId: firstText(incident, ["accountId", "account_id", "account"]),
    blocking: incident?.blocking !== false && severity.tone === "danger",
    derived,
    raw: incident,
  };
}

export function projectIncidents(snapshot = {}, deploymentContext = null) {
  const explicit = asArray(snapshot?.incidents);
  if (explicit.length) {
    return explicit.map((incident, index) => normalizedIncident(incident, index, false));
  }
  const context = deploymentContext?.contextKey
    ? deploymentContext
    : selectCurrentDeploymentContext(snapshot, deploymentContext || {});
  const fallback = [];
  const add = (incident) => {
    if (!fallback.some((item) => item.id === incident.id)) fallback.push(incident);
  };
  if (snapshot?.api_connected === false || snapshot?.apiConnected === false) {
    add({ id: "api-disconnected", title: "API 연결 끊김", detail: "상태를 확인할 수 없어 신규 위험 주문을 차단해야 합니다.", severity: "critical", state: "open" });
  }
  if (snapshot?.kill_switch === true || snapshot?.killSwitch === true || snapshot?.central_control?.globalKill === true) {
    add({ id: "kill-switch-active", title: "긴급 차단 활성화", detail: "전역 신규 주문 차단 상태입니다.", severity: "critical", state: "open" });
  }
  const watchdog = safeObject(snapshot?.watchdog);
  if (Number(watchdog.critical_count ?? watchdog.criticalCount ?? 0) > 0) {
    add({ id: "watchdog-critical", title: "Watchdog 긴급 상태", detail: firstText(watchdog, ["last_action", "lastAction", "detail"], `${watchdog.critical_count ?? watchdog.criticalCount}개 긴급 점검`), severity: "critical", state: "open", occurredAt: firstText(watchdog, ["last_run", "lastRun"]) });
  }
  const reconciliation = safeObject(snapshot?.reconciliation?.summary || snapshot?.reconciliationSummary);
  const mismatchCount = Number(reconciliation.mismatch_count ?? reconciliation.mismatchCount ?? 0);
  const requiredCount = Number(reconciliation.api_required_count ?? reconciliation.apiRequiredCount ?? 0);
  if (mismatchCount || requiredCount) {
    add({ id: "reconciliation-blocked", title: "계좌·포지션 대조 실패", detail: `불일치 ${mismatchCount}건 · 확인 불가 ${requiredCount}건`, severity: "critical", state: "open", occurredAt: firstText(reconciliation, ["last_run", "lastRun"]) });
  }
  const unknownOrders = asArray(snapshot?.orders).filter(unknownSubmitResult);
  if (unknownOrders.length) {
    add({ id: "unknown-submit-result", title: "주문 접수 결과 확인 불가", detail: `${unknownOrders.length}건을 브로커 원장과 대조해야 합니다.`, severity: "critical", state: "open", occurredAt: firstText(unknownOrders[0], ["updated_at", "updatedAt", "time"]) });
  }
  const runtime = safeObject(snapshot?.continuous_runtime || snapshot?.continuousRuntime);
  if (["FAILED", "ERROR", "CRASHED"].includes(String(runtime.phase || runtime.status || "").toUpperCase())) {
    add({ id: "runtime-failed", title: "실거래 런타임 오류", detail: firstText(runtime, ["lastError", "last_error", "detail"]), severity: "critical", state: "open" });
  }
  asArray(snapshot?.audit).filter((item) => ["danger", "error", "critical"].includes(normalizedToken(item?.level))).slice(0, 5).forEach((item, index) => {
    add({ id: `audit-${firstText(item, ["timestamp", "time"], String(index))}-${firstText(item, ["event"], "error")}`, title: firstText(item, ["event"], "운영 오류"), detail: firstText(item, ["detail", "message"]), severity: item.level, state: "open", occurredAt: firstText(item, ["timestamp", "time"]), source: "audit-fallback" });
  });
  return fallback.map((incident, index) => normalizedIncident({
    ...incident,
    deploymentId: incident.deploymentId || context.deploymentId,
    accountId: incident.accountId || context.accountId,
  }, index, true)).sort((left, right) => statusSeverity(right.severity) - statusSeverity(left.severity) || String(right.occurredAt).localeCompare(String(left.occurredAt)));
}
