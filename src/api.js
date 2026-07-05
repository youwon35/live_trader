export async function getSnapshot() {
  return request("/api/snapshot");
}

export async function getUiSettings() {
  return request("/api/ui-settings");
}

export async function saveUiSettings(settings) {
  return request("/api/ui-settings", { method: "POST", body: settings });
}

export async function getEnvSettings() {
  return request("/api/env-settings");
}

export async function saveEnvSettings(values) {
  return request("/api/env-settings", { method: "POST", body: { values } });
}

export async function setMode(mode) {
  return request("/api/mode", { method: "POST", body: { mode } });
}

export async function setFlag(name, value) {
  return request("/api/flag", { method: "POST", body: { name, value } });
}

export async function setAutomationProfile(profileId, enabled, provider, mode) {
  return request("/api/automation", { method: "POST", body: { profile_id: profileId, enabled, provider, mode } });
}

export async function setRiskSetting(name, value) {
  return request("/api/risk-setting", { method: "POST", body: { name, value } });
}

export async function setChecklistItem(name, value) {
  return request("/api/checklist", { method: "POST", body: { name, value } });
}

export async function setRetryPolicy(name, value) {
  return request("/api/retry-policy", { method: "POST", body: { name, value } });
}

export async function retryOrder(orderId) {
  return request("/api/order-retry", { method: "POST", body: { order_id: orderId } });
}

export async function cancelOrder(orderId) {
  return request("/api/order-cancel", { method: "POST", body: { order_id: orderId } });
}

export async function runBrokerCheck(brokerId) {
  return request("/api/broker-check", { method: "POST", body: { broker_id: brokerId } });
}

export async function runReconciliation() {
  return request("/api/reconcile", { method: "POST", body: {} });
}

export async function seedProgramLedgerBaseline() {
  return request("/api/program-ledger-baseline", { method: "POST", body: {} });
}

export async function syncExecutionEvents(brokerId = "all") {
  return request("/api/execution-events", { method: "POST", body: { broker_id: brokerId } });
}

export async function runFinalPreflight() {
  return request("/api/preflight", { method: "POST", body: {} });
}

export async function exportAudit(format) {
  return request("/api/audit-export", { method: "POST", body: { format } });
}

export async function submitTestIntent() {
  return request("/api/test-intent", { method: "POST", body: {} });
}

export async function runStrategyCycle(profileId) {
  return request("/api/strategy-cycle", { method: "POST", body: { profile_id: profileId } });
}

export async function promoteStrategyToLive(strategyId) {
  return request("/api/strategy-live-promotion", { method: "POST", body: { strategy_id: strategyId } });
}

export async function setStrategyLifecycle(strategyId, action) {
  return request("/api/strategy-lifecycle", { method: "POST", body: { strategy_id: strategyId, action } });
}

export async function runWatchdog() {
  return request("/api/watchdog", { method: "POST", body: {} });
}

async function request(path, options = {}) {
  const response = await fetch(path, {
    method: options.method ?? "GET",
    headers: { "Content-Type": "application/json" },
    body: options.body ? JSON.stringify(options.body) : undefined,
  });
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json();
}
