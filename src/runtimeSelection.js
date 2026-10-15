// Real account/deployment runtimes take precedence over profile aggregates.
export function runtimeRows(runtime = {}) {
  if (runtime.runtimes && Object.keys(runtime.runtimes).length) {
    return Object.entries(runtime.runtimes).map(([id, value]) => ({ ...value, runtimeId: id }));
  }
  if (runtime.profiles) return Object.values(runtime.profiles).filter((row) => row && row.ambiguous !== true);
  return runtime.profileId ? [runtime] : [];
}

export function runtimeForDeployment(runtime = {}, context = {}) {
  const deploymentId = String(context.id || context.deploymentId || "");
  if (!deploymentId) return null;
  const broker = String(context.brokerId || context.broker_id || "").toLowerCase();
  const matches = runtimeRows(runtime).filter((row) => row.deploymentId === deploymentId
    && (!broker || !row.brokerId || row.brokerId === broker));
  return matches.length === 1 ? matches[0] : null;
}

export function runningDeploymentIds(runtime = {}) {
  return [...new Set(runtimeRows(runtime).filter((row) => row.running === true)
    .map((row) => row.deploymentId).filter(Boolean))];
}

export function artifactIsRunning(runtime = {}, artifactId = "") {
  if (!artifactId) return false;
  return runtimeRows(runtime).some((row) => row.running === true
    && (row.requestedStrategyId === artifactId || row.portfolioId === artifactId
      || (Array.isArray(row.strategyIds) && row.strategyIds.includes(artifactId))));
}

// An aggregate maximum is never the selected account's mode.
export function runtimeModeForDeployment(snapshot = {}, context = {}) {
  if (String(snapshot.mode || "MONITOR").toUpperCase() === "MONITOR") return "MONITOR";
  const runtime = snapshot.continuous_runtime || {};
  const selected = runtimeForDeployment(runtime, context);
  if (runtime.runtimes && Object.keys(runtime.runtimes).length) {
    return selected?.running === true ? String(selected.mode || "MONITOR").toUpperCase() : "MONITOR";
  }
  return String(selected?.mode || snapshot.mode || "MONITOR").toUpperCase();
}
