export const API_DEFAULT_TIMEOUT_MS = 15_000;
export const BROKER_REFRESH_TIMEOUT_MS = 45_000;

export class ApiRequestError extends Error {
  constructor(message, code, options = {}) {
    super(message, options);
    this.name = "ApiRequestError";
    this.code = code;
  }
}

export function isApiRequestTimeout(error) {
  return error?.code === "TIMEOUT";
}

export function isApiConnectionFailure(error) {
  return error?.code === "NETWORK";
}

export async function getSnapshot() {
  return request("/api/snapshot");
}

export async function getTelegramConnection() {
  return request("/api/telegram/connection");
}

export async function getUiSettings() {
  return request("/api/ui-settings");
}

export async function saveUiSettings(settings) {
  return request("/api/ui-settings", { method: "POST", body: settings });
}

export async function loadSharedSearchPresets() {
  return request("/api/search-presets");
}

export async function saveSharedSearchPresets(presets) {
  return request("/api/search-presets", { method: "POST", body: { presets } });
}

export async function loadArtifactMetadata() {
  return request("/api/artifact-metadata");
}

export async function updateArtifactMetadata(artifactId, artifactType, changes) {
  return request("/api/artifact-metadata", { method: "POST", body: { artifactId, artifactType, changes } });
}

export async function getEnvSettings() {
  return request("/api/env-settings");
}

export async function saveEnvSettings(values, confirmed = false) {
  return request("/api/env-settings", { method: "POST", body: { values, confirmed } });
}

export async function setMode(mode) {
  return request("/api/mode", { method: "POST", body: { mode } });
}

export async function setFlag(name, value, confirmed = false) {
  return request("/api/flag", { method: "POST", body: { name, value, confirmed } });
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

export async function runReconciliation(options = {}) {
  return request("/api/reconcile", {
    method: "POST",
    body: {
      refresh_brokers: options.refreshBrokers !== false,
      include_snapshot: options.includeSnapshot !== false,
    },
    timeoutMs: BROKER_REFRESH_TIMEOUT_MS,
  });
}

export async function seedProgramLedgerBaseline(confirmed = false) {
  return request("/api/program-ledger-baseline", { method: "POST", body: { confirmed } });
}

export async function syncExecutionEvents(brokerId = "all", options = {}) {
  return request("/api/execution-events", {
    method: "POST",
    body: {
      broker_id: brokerId,
      force_snapshot: options.forceSnapshot,
      include_snapshot: options.includeSnapshot !== false,
    },
    timeoutMs: BROKER_REFRESH_TIMEOUT_MS,
  });
}

export async function runFinalPreflight() {
  return request("/api/preflight", { method: "POST", body: {} });
}

export async function previewBinanceFuturesFillSoak(symbol = "ETHUSDT") {
  return request("/api/binance-futures-fill-soak/preview", {
    method: "POST",
    body: { symbol },
    timeoutMs: BROKER_REFRESH_TIMEOUT_MS,
  });
}

export async function startBinanceFuturesFillSoak(confirmationToken, confirmed = false) {
  return request("/api/binance-futures-fill-soak/start", {
    method: "POST",
    body: { confirmation_token: confirmationToken, confirmed },
    timeoutMs: BROKER_REFRESH_TIMEOUT_MS,
  });
}

export async function stopBinanceFuturesFillSoak() {
  return request("/api/binance-futures-fill-soak/stop", {
    method: "POST",
    body: {},
    timeoutMs: 15000,
  });
}

export async function previewUpbitSmokeOrder(strategyId, notionalKrw = 5000) {
  return request("/api/upbit-smoke-preview", {
    method: "POST",
    body: { strategy_id: strategyId, notional_krw: notionalKrw },
  });
}

export async function submitUpbitSmokeOrder(confirmationToken, confirmed = false) {
  return request("/api/upbit-smoke-submit", {
    method: "POST",
    body: { confirmation_token: confirmationToken, confirmed },
  });
}

export async function refreshUpbitSmokeOrder() {
  return request("/api/upbit-smoke-refresh", { method: "POST", body: {} });
}

export async function exportAudit(format) {
  return request("/api/audit-export", { method: "POST", body: { format } });
}

export async function submitTestIntent() {
  return request("/api/test-intent", { method: "POST", body: {} });
}

export async function runPolicyReplay(payload = {}) {
  return request("/api/policy-replay", { method: "POST", body: payload });
}

export async function runShadowLive(payload = {}) {
  return request("/api/shadow-live", { method: "POST", body: payload });
}

export async function runRecoveryDrill() {
  return request("/api/recovery-drill", { method: "POST", body: {} });
}

export async function getValidationSmallLive() {
  return request("/api/validation-small-live", { timeoutMs: 15000 });
}

export async function evaluateValidationSmallLive(validationStrategyInstanceId) {
  return request("/api/validation-small-live/evaluate", {
    method: "POST",
    body: {
      validation_strategy_instance_id: validationStrategyInstanceId,
    },
    timeoutMs: 30000,
  });
}

export async function runStrategyCycle(profileId) {
  return request("/api/strategy-cycle", { method: "POST", body: { profile_id: profileId } });
}

export async function startContinuousRuntime(profileId, mode, portfolioId = "") {
  return request("/api/runtime/start", { method: "POST", body: { profile_id: profileId, mode, portfolio_id: portfolioId }, timeoutMs: 30000 });
}

export async function stopContinuousRuntime(profileId = "") {
  return request("/api/runtime/stop", { method: "POST", body: { profile_id: profileId }, timeoutMs: 15000 });
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

export async function request(path, options = {}) {
  const controller = new AbortController();
  const timeoutMs = options.timeoutMs ?? API_DEFAULT_TIMEOUT_MS;
  const timeout = window.setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(path, {
      method: options.method ?? "GET",
      headers: { "Content-Type": "application/json" },
      body: options.body ? JSON.stringify(options.body) : undefined,
      signal: controller.signal,
    });
    const contentType = response.headers.get("content-type") ?? "";
    const result = contentType.includes("application/json") ? await response.json() : null;
    if (!response.ok) {
      throw new ApiRequestError(
        result?.reason || result?.message || `요청 실패 (${response.status})`,
        "HTTP",
      );
    }
    if (!result) {
      throw new ApiRequestError("API가 올바른 JSON 응답을 반환하지 않았습니다.", "INVALID_RESPONSE");
    }
    return result;
  } catch (error) {
    if (error?.name === "AbortError") {
      const seconds = Math.max(1, Math.round(timeoutMs / 1000));
      throw new ApiRequestError(`API 응답 시간이 ${seconds}초를 초과했습니다.`, "TIMEOUT", { cause: error });
    }
    if (error instanceof ApiRequestError) throw error;
    if (error instanceof TypeError) {
      throw new ApiRequestError("API 서버에 연결할 수 없습니다.", "NETWORK", { cause: error });
    }
    throw error;
  } finally {
    window.clearTimeout(timeout);
  }
}
