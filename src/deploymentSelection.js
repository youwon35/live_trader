const TERMINAL_DEPLOYMENT_STAGES = new Set([
  "archived",
  "paused",
  "rejected",
  "retired",
]);

const LIVE_WORKSPACE_STAGES = new Set([
  "papered",
  "before-live-small",
  "live",
]);

export function strategyDeploymentIdentity(strategy = {}) {
  return String(
    strategy.deployment_id
      || strategy.deploymentId
      || strategy.strategy_id
      || "",
  ).trim();
}

export function strategyLifecycleStage(strategy = {}) {
  const lifecycle = strategy.lifecycle && typeof strategy.lifecycle === "object"
    ? strategy.lifecycle
    : {};
  const promotion = strategy.promotion && typeof strategy.promotion === "object"
    ? strategy.promotion
    : {};
  return String(
    lifecycle.status
      || promotion.stage
      || strategy.promotion_stage
      || strategy.lifecycle_status
      || strategy.status
      || "unknown",
  ).trim().toLowerCase();
}

export function deploymentRuntimeProfile(context = {}) {
  const broker = String(
    context.brokerId
      || context.broker_id
      || context.provider
      || context.route
      || context.asset
      || "",
  ).trim().toLowerCase();
  if (!broker) return "";
  if (broker === "kis" || broker.includes("stock") || broker.includes("주식")) {
    return "stock";
  }
  if (
    broker.includes("binance")
    || broker.includes("upbit")
    || broker.includes("crypto")
    || broker.includes("coin")
    || broker.includes("코인")
  ) {
    return "crypto";
  }
  return "";
}

export function governedDeploymentIdentity(liveGovernance = {}) {
  const governance = liveGovernance && typeof liveGovernance === "object"
    ? liveGovernance
    : {};
  const manifest = governance.manifest && typeof governance.manifest === "object"
    ? governance.manifest
    : {};
  const preflight = governance.latestPreflight && typeof governance.latestPreflight === "object"
    ? governance.latestPreflight
    : governance.latest_preflight && typeof governance.latest_preflight === "object"
      ? governance.latest_preflight
      : {};
  return String(
    governance.deploymentId
      || governance.deployment_id
      || manifest.deploymentId
      || manifest.deployment_id
      || preflight.deploymentId
      || preflight.deployment_id
      || "",
  ).trim();
}

export function deploymentContextMatchesPreflight(
  selectedDeploymentId,
  liveGovernance = {},
) {
  const selected = String(selectedDeploymentId || "").trim();
  const governed = governedDeploymentIdentity(liveGovernance);
  return Boolean(selected) && Boolean(governed) && selected === governed;
}

function isTerminalDeployment(strategy) {
  return strategy.archived === true
    || strategy.is_archived === true
    || strategy.retired === true
    || TERMINAL_DEPLOYMENT_STAGES.has(strategyLifecycleStage(strategy));
}

function isLiveWorkspaceCandidate(strategy) {
  const permissions = strategy.permissions && typeof strategy.permissions === "object"
    ? strategy.permissions
    : {};
  return strategy.live_allowed === true
    || strategy.live_small_eligible === true
    || strategy.live_eligible === true
    || permissions.live_allowed === true
    || permissions.live_small_eligible === true
    || permissions.live_eligible === true
    || LIVE_WORKSPACE_STAGES.has(strategyLifecycleStage(strategy));
}

function candidateScore(strategy, pinned) {
  const stage = strategyLifecycleStage(strategy);
  const stageScore = stage === "live" ? 30 : stage === "before-live-small" ? 20 : stage === "papered" ? 10 : 0;
  return (pinned ? 1000 : 0)
    + (strategy.live_eligible === true ? 300 : 0)
    + (strategy.live_small_eligible === true || strategy.live_allowed === true ? 200 : 0)
    + (strategy.portfolio_gate?.active === true ? 20 : 0)
    + stageScore;
}

function compactDeploymentToken(value) {
  let hash = 2166136261;
  for (const character of String(value || "")) {
    hash ^= character.codePointAt(0);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0).toString(36).toUpperCase().padStart(7, "0").slice(-7);
}

function optionBaseLabel(strategy) {
  const gate = strategy.portfolio_gate && typeof strategy.portfolio_gate === "object"
    ? strategy.portfolio_gate
    : {};
  const portfolio = gate.portfolioName || gate.portfolioId || "Standalone";
  const name = strategy.name || strategy.strategy_id || "이름 없음";
  const symbol = strategy.symbol || "-";
  const timeframe = strategy.timeframe || "-";
  const stage = strategyLifecycleStage(strategy);
  return `${portfolio} · ${symbol} ${timeframe} · ${name} · ${stage}`;
}

export function buildCurrentDeploymentOptions(strategies = [], { pinnedDeploymentIds = [] } = {}) {
  const pinned = new Set((pinnedDeploymentIds || []).map((item) => String(item || "").trim()).filter(Boolean));
  const unique = new Map();

  for (const strategy of strategies || []) {
    if (!strategy || typeof strategy !== "object") continue;
    const id = strategyDeploymentIdentity(strategy);
    if (!id) continue;
    const isPinned = pinned.has(id);
    if (!isPinned && (isTerminalDeployment(strategy) || !isLiveWorkspaceCandidate(strategy))) continue;
    const current = unique.get(id);
    if (!current || candidateScore(strategy, isPinned) > candidateScore(current.strategy, current.pinned)) {
      unique.set(id, { id, strategy, pinned: isPinned });
    }
  }

  return [...unique.values()]
    .sort((left, right) => {
      const score = candidateScore(right.strategy, right.pinned) - candidateScore(left.strategy, left.pinned);
      if (score) return score;
      return optionBaseLabel(left.strategy).localeCompare(optionBaseLabel(right.strategy), "ko");
    })
    .map((option) => ({
      ...option,
      label: `${option.pinned ? "[현재 세션] " : ""}${optionBaseLabel(option.strategy)} · #${compactDeploymentToken(option.id)}`,
    }));
}
