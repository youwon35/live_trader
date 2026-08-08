export const EMPTY_FUNCTIONAL_TEST_WORKSPACE = Object.freeze({
  ok: true,
  environment: "KIS_LIVE",
  status: "STOPPED",
  readinessOnly: true,
  brokerSubmissionAllowed: false,
  promotionEligible: false,
  durationLimits: { units: ["HOURS", "DAYS"], maxDays: 90, dailyActivationMaxHours: 6 },
  caps: {
    maxOrderQuantity: 1,
    maxOrderNotional: 100000,
    maxGrossExposure: 300000,
    maxOrders: 20,
    maxOpenPositions: 3,
    maxLoss: 20000,
  },
  effectiveCaps: {
    available: false,
    reason: "fresh-kis-account-risk-required",
    observedAt: "",
    values: {
      maxOrderQuantity: 0,
      maxOrderNotional: 0,
      maxGrossExposure: 0,
      maxOrders: 0,
      maxOpenPositions: 0,
      maxLoss: 0,
    },
  },
  account: {
    label: "KIS 계좌 확인 중",
    bindingId: "",
    credentialsReady: false,
    realOrderAdapterEnabled: false,
    missingSettings: [],
  },
  candidates: [],
  current: {
    permit: null,
    activation: null,
    selectedTargetKey: "",
    blockers: ["functional-test-api-unavailable"],
    ready: false,
    authorityReferencePresent: false,
    pausedAt: "",
    pauseRequestedAt: "",
    stoppedAt: "",
  },
  runtime: {
    running: false,
    mode: "MONITOR",
    executionPurpose: "",
    functionalTestRunning: false,
    strategyIds: [],
    allowedSymbols: [],
  },
  authorityMutation: {
    allowed: false,
    blockers: ["functional-test-api-unavailable"],
    workingOrderCount: 0,
    kisReconciled: false,
  },
  notice: "기능시험 준비 상태를 불러오는 중입니다.",
});

function finiteNumber(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

export function normalizeFunctionalTestWorkspace(value = {}) {
  const source = value && typeof value === "object" ? value : {};
  const current = source.current && typeof source.current === "object" ? source.current : {};
  const account = source.account && typeof source.account === "object" ? source.account : {};
  const limits = source.durationLimits && typeof source.durationLimits === "object" ? source.durationLimits : {};
  const caps = source.caps && typeof source.caps === "object" ? source.caps : {};
  const effectiveCaps = source.effectiveCaps && typeof source.effectiveCaps === "object"
    ? source.effectiveCaps
    : {};
  const effectiveValues = effectiveCaps.values && typeof effectiveCaps.values === "object"
    ? effectiveCaps.values
    : {};
  const runtime = source.runtime && typeof source.runtime === "object" ? source.runtime : {};
  const authorityMutation = source.authorityMutation && typeof source.authorityMutation === "object"
    ? source.authorityMutation
    : {};
  const candidates = Array.isArray(source.candidates)
    ? source.candidates.filter((candidate) => candidate && typeof candidate === "object")
    : [];
  return {
    ...EMPTY_FUNCTIONAL_TEST_WORKSPACE,
    ...source,
    environment: "KIS_LIVE",
    readinessOnly: true,
    brokerSubmissionAllowed: false,
    promotionEligible: false,
    durationLimits: {
      ...EMPTY_FUNCTIONAL_TEST_WORKSPACE.durationLimits,
      ...limits,
      maxDays: Math.max(1, finiteNumber(limits.maxDays, 90)),
      dailyActivationMaxHours: Math.max(1, finiteNumber(limits.dailyActivationMaxHours, 6)),
    },
    caps: { ...EMPTY_FUNCTIONAL_TEST_WORKSPACE.caps, ...caps },
    effectiveCaps: {
      ...EMPTY_FUNCTIONAL_TEST_WORKSPACE.effectiveCaps,
      ...effectiveCaps,
      values: {
        ...EMPTY_FUNCTIONAL_TEST_WORKSPACE.effectiveCaps.values,
        ...effectiveValues,
      },
    },
    account: { ...EMPTY_FUNCTIONAL_TEST_WORKSPACE.account, ...account },
    candidates,
    current: {
      ...EMPTY_FUNCTIONAL_TEST_WORKSPACE.current,
      ...current,
      blockers: Array.isArray(current.blockers)
        ? current.blockers.map(String)
        : [...EMPTY_FUNCTIONAL_TEST_WORKSPACE.current.blockers],
    },
    runtime: {
      ...EMPTY_FUNCTIONAL_TEST_WORKSPACE.runtime,
      ...runtime,
      strategyIds: Array.isArray(runtime.strategyIds) ? runtime.strategyIds.map(String) : [],
      allowedSymbols: Array.isArray(runtime.allowedSymbols) ? runtime.allowedSymbols.map(String) : [],
    },
    authorityMutation: {
      ...EMPTY_FUNCTIONAL_TEST_WORKSPACE.authorityMutation,
      ...authorityMutation,
      blockers: Array.isArray(authorityMutation.blockers)
        ? authorityMutation.blockers.map(String)
        : [...EMPTY_FUNCTIONAL_TEST_WORKSPACE.authorityMutation.blockers],
    },
  };
}

export function preferredFunctionalTestCandidate(workspace, selectedKey = "") {
  const candidates = Array.isArray(workspace?.candidates) ? workspace.candidates : [];
  const selected = candidates.find((candidate) => candidate.key === selectedKey);
  if (selected) return selected;
  const current = candidates.find(
    (candidate) => candidate.key === workspace?.current?.selectedTargetKey,
  );
  if (current) return current;
  return candidates.find((candidate) => candidate.available === true) || candidates[0] || null;
}

export function functionalTestDurationBounds(unit = "HOURS", maxDays = 90) {
  const normalizedUnit = String(unit || "HOURS").toUpperCase();
  return {
    min: 1,
    max: normalizedUnit === "DAYS" ? maxDays : maxDays * 24,
  };
}

export function functionalTestProgress(permit, now = new Date()) {
  if (!permit) return { ratio: 0, remainingMs: 0, elapsedMs: 0 };
  const startsAt = new Date(permit.startsAt).getTime();
  const endsAt = new Date(permit.endsAt).getTime();
  const current = now instanceof Date ? now.getTime() : new Date(now).getTime();
  if (![startsAt, endsAt, current].every(Number.isFinite) || endsAt <= startsAt) {
    return { ratio: 0, remainingMs: 0, elapsedMs: 0 };
  }
  const elapsedMs = Math.max(0, Math.min(endsAt - startsAt, current - startsAt));
  return {
    ratio: elapsedMs / (endsAt - startsAt),
    remainingMs: Math.max(0, endsAt - current),
    elapsedMs,
  };
}

export function formatFunctionalTestRemaining(milliseconds) {
  const totalMinutes = Math.max(0, Math.ceil(finiteNumber(milliseconds) / 60_000));
  const days = Math.floor(totalMinutes / (24 * 60));
  const hours = Math.floor((totalMinutes % (24 * 60)) / 60);
  const minutes = totalMinutes % 60;
  if (days > 0) return `${days}일 ${hours}시간`;
  if (hours > 0) return `${hours}시간 ${minutes}분`;
  return `${minutes}분`;
}

export function functionalTestStatusTone(status = "") {
  const normalized = String(status || "").toUpperCase();
  if (normalized === "ACTIVE") return "warning";
  if (["PERMIT_READY", "PAUSED"].includes(normalized)) return "info";
  if (["PAUSING", "PAUSE_REQUIRED"].includes(normalized)) return "warning";
  if (normalized === "STOP_FAILED") return "warning";
  return "neutral";
}
