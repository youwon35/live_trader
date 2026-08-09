export const API_DEFAULT_TIMEOUT_MS = 15_000;
export const BROKER_REFRESH_TIMEOUT_MS = 45_000;

export const SAFETY_CONFIRMATION_ACTIONS = Object.freeze({
  KILL_SWITCH_OFF: "KILL_SWITCH_OFF",
  DRY_RUN_OFF: "DRY_RUN_OFF",
  NEW_ENTRIES_BLOCKED_OFF: "NEW_ENTRIES_BLOCKED_OFF",
  REAL_ORDERS_ENABLE: "REAL_ORDERS_ENABLE",
  FUNCTIONAL_TEST_START: "FUNCTIONAL_TEST_START",
  BINANCE_FUTURES_FILL_SOAK_START: "BINANCE_FUTURES_FILL_SOAK_START",
});

let safetyConfirmationPresenter = null;
let activeSafetyConfirmationFlow = null;

export class ApiRequestError extends Error {
  constructor(message, code, options = {}) {
    super(message, options);
    this.name = "ApiRequestError";
    this.code = code;
    this.details = options.details;
  }
}

export function isApiRequestTimeout(error) {
  return error?.code === "TIMEOUT";
}

export function isApiConnectionFailure(error) {
  return error?.code === "NETWORK";
}

export function isSafetyConfirmationCancelled(error) {
  return [
    "SAFETY_CONFIRMATION_CANCELLED",
    "SAFETY_CONFIRMATION_EXPIRED",
  ].includes(error?.code);
}

export function registerSafetyConfirmationPresenter(presenter) {
  if (typeof presenter !== "function") {
    throw new TypeError("safety confirmation presenter must be a function");
  }
  safetyConfirmationPresenter = presenter;
  return () => {
    if (safetyConfirmationPresenter === presenter) safetyConfirmationPresenter = null;
  };
}

function normalizedSafetyChallenge(response, action) {
  const raw = response?.challenge && typeof response.challenge === "object"
    ? response.challenge
    : response;
  const challengeId = String(raw?.challengeId || "");
  const token = String(raw?.token || "");
  const expectedPhrase = String(raw?.expectedPhrase || "");
  const expiresAt = String(raw?.expiresAt || "");
  const expiresAtMs = Date.parse(expiresAt);
  if (!challengeId || !token || !expectedPhrase || !Number.isFinite(expiresAtMs)) {
    throw new ApiRequestError(
      "2단계 안전 확인 challenge 응답이 올바르지 않습니다.",
      "SAFETY_CONFIRMATION_INVALID",
    );
  }
  return {
    action,
    challengeId,
    token,
    expectedPhrase,
    expiresAt,
    expiresAtMs,
    displayContext: raw?.displayContext && typeof raw.displayContext === "object"
      ? raw.displayContext
      : {},
  };
}

async function runWithSafetyConfirmation(action, context, mutation) {
  if (typeof safetyConfirmationPresenter !== "function") {
    throw new ApiRequestError(
      "2단계 안전 확인 화면을 사용할 수 없습니다.",
      "SAFETY_CONFIRMATION_UI_UNAVAILABLE",
    );
  }
  if (activeSafetyConfirmationFlow) {
    throw new ApiRequestError(
      "다른 2단계 안전 확인이 진행 중입니다.",
      "SAFETY_CONFIRMATION_BUSY",
    );
  }

  const flow = (async () => {
    const response = await request("/api/safety-confirmation/challenge", {
      method: "POST",
      body: { action, context },
    });
    if (response?.ok === false) {
      throw new ApiRequestError(
        response.reason || "서버가 2단계 안전 확인 발급을 거부했습니다.",
        "SAFETY_CONFIRMATION_REJECTED",
        { details: response },
      );
    }
    const challenge = normalizedSafetyChallenge(response, action);
    if (Date.now() >= challenge.expiresAtMs) {
      throw new ApiRequestError(
        "2단계 안전 확인이 만료되었습니다. 다시 시작하세요.",
        "SAFETY_CONFIRMATION_EXPIRED",
      );
    }
    const decision = await safetyConfirmationPresenter({
      action: challenge.action,
      challengeId: challenge.challengeId,
      expectedPhrase: challenge.expectedPhrase,
      expiresAt: challenge.expiresAt,
      displayContext: challenge.displayContext,
    });
    if (decision?.confirmed !== true) {
      throw new ApiRequestError(
        decision?.expired
          ? "2단계 안전 확인이 만료되었습니다. 다시 시작하세요."
          : "2단계 안전 확인이 취소되었습니다.",
        decision?.expired
          ? "SAFETY_CONFIRMATION_EXPIRED"
          : "SAFETY_CONFIRMATION_CANCELLED",
      );
    }
    if (Date.now() >= challenge.expiresAtMs) {
      throw new ApiRequestError(
        "2단계 안전 확인이 만료되었습니다. 다시 시작하세요.",
        "SAFETY_CONFIRMATION_EXPIRED",
      );
    }
    const typedPhrase = String(decision.typedPhrase || "");
    if (typedPhrase !== challenge.expectedPhrase) {
      throw new ApiRequestError(
        "서버가 요구한 확인 문구와 일치하지 않습니다.",
        "SAFETY_CONFIRMATION_PHRASE_MISMATCH",
      );
    }
    return mutation({
      challengeId: challenge.challengeId,
      token: challenge.token,
      typedPhrase,
    });
  })();

  activeSafetyConfirmationFlow = flow;
  try {
    return await flow;
  } finally {
    if (activeSafetyConfirmationFlow === flow) activeSafetyConfirmationFlow = null;
  }
}

function safetyConfirmationPayload(confirmation) {
  return confirmation
    ? {
        safety_confirmation: {
          challengeId: confirmation.challengeId,
          token: confirmation.token,
          typedPhrase: confirmation.typedPhrase,
        },
      }
    : {};
}

export async function safetyConfirmationValuesDigest(values) {
  const source = values && typeof values === "object" && !Array.isArray(values) ? values : {};
  const sortedValues = Object.fromEntries(
    Object.keys(source).sort().map((key) => [key, source[key]]),
  );
  const canonicalJson = JSON.stringify(sortedValues);
  const subtle = globalThis.crypto?.subtle;
  if (typeof globalThis.TextEncoder !== "function" || typeof subtle?.digest !== "function") {
    throw new ApiRequestError(
      "실전 주문 설정을 안전하게 봉인할 SHA-256 기능을 사용할 수 없습니다.",
      "SAFETY_CONFIRMATION_DIGEST_UNAVAILABLE",
    );
  }
  const digest = await subtle.digest("SHA-256", new TextEncoder().encode(canonicalJson));
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
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
  const mutation = (confirmation) => request("/api/env-settings", {
    method: "POST",
    body: { values, confirmed, ...safetyConfirmationPayload(confirmation) },
  });
  const enableRealOrders = String(values?.LIVE_TRADER_ENABLE_REAL_ORDERS || "").toLowerCase() === "true";
  if (!enableRealOrders) return mutation(null);
  const settingKeys = Object.keys(values || {}).sort();
  const valuesDigest = await safetyConfirmationValuesDigest(values);
  return runWithSafetyConfirmation(
    SAFETY_CONFIRMATION_ACTIONS.REAL_ORDERS_ENABLE,
    {
      settingKeys,
      enableRealOrders: true,
      valuesDigest,
    },
    mutation,
  );
}

export async function setMode(mode) {
  return request("/api/mode", { method: "POST", body: { mode } });
}

export async function setFlag(name, value, confirmed = false) {
  const mutation = (confirmation) => request("/api/flag", {
    method: "POST",
    body: { name, value, confirmed, ...safetyConfirmationPayload(confirmation) },
  });
  const safetyAction = value === false
    ? {
        kill_switch: SAFETY_CONFIRMATION_ACTIONS.KILL_SWITCH_OFF,
        dry_run: SAFETY_CONFIRMATION_ACTIONS.DRY_RUN_OFF,
        new_entries_blocked: SAFETY_CONFIRMATION_ACTIONS.NEW_ENTRIES_BLOCKED_OFF,
      }[name]
    : null;
  return safetyAction
    ? runWithSafetyConfirmation(safetyAction, { name, value: false }, mutation)
    : mutation(null);
}

export function nativeEmergencyStopAvailable() {
  return typeof globalThis.window?.pywebview?.api?.engage_emergency_stop === "function";
}

export async function engageNativeEmergencyStop(reason = "operator global Kill Switch") {
  const bridge = globalThis.window?.pywebview?.api;
  if (typeof bridge?.engage_emergency_stop !== "function") {
    throw new ApiRequestError(
      "독립 긴급정지 브리지를 사용할 수 없습니다. Live Trader 데스크톱에서 다시 시도하세요.",
      "EMERGENCY_BRIDGE_UNAVAILABLE",
    );
  }
  let result;
  try {
    result = await bridge.engage_emergency_stop(reason);
  } catch (error) {
    throw new ApiRequestError(
      "독립 긴급정지 브리지 호출에 실패했습니다.",
      "EMERGENCY_BRIDGE_FAILED",
      { cause: error },
    );
  }
  if (result?.ok !== true || result?.active !== true || result?.durable !== true) {
    throw new ApiRequestError(
      result?.reason || "독립 긴급정지 래치를 기록하지 못했습니다.",
      "EMERGENCY_LATCH_FAILED",
      { details: result && typeof result === "object" ? result : undefined },
    );
  }
  return result;
}

export async function getNativeEmergencyStopStatus() {
  const bridge = globalThis.window?.pywebview?.api;
  const available = nativeEmergencyStopAvailable();
  if (typeof bridge?.emergency_stop_status !== "function") {
    return {
      available,
      status_available: false,
      active: false,
      durable: false,
      status: "unavailable",
    };
  }
  try {
    const result = await bridge.emergency_stop_status();
    if (!result || typeof result !== "object") {
      return {
        available,
        status_available: false,
        active: false,
        durable: false,
        status: "invalid-response",
      };
    }
    return {
      ...result,
      available,
      status_available: true,
      active: result.active === true,
      durable: result.durable === true,
      status: String(result.status || "unknown"),
    };
  } catch (error) {
    return {
      available,
      status_available: false,
      active: false,
      durable: false,
      status: "read-failed",
      reason: error instanceof Error ? error.message : "native emergency status read failed",
    };
  }
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

export async function runFinalPreflight(deploymentId = "", strategyId = "") {
  return request("/api/preflight", {
    method: "POST",
    body: { deployment_id: deploymentId, strategy_id: strategyId },
    timeoutMs: BROKER_REFRESH_TIMEOUT_MS,
  });
}

export async function previewBinanceFuturesSettings(
  symbol = "ETHUSDT",
  marginType = "ISOLATED",
  leverage = 1,
) {
  return request("/api/binance-futures-settings/preview", {
    method: "POST",
    body: { symbol, margin_type: marginType, leverage },
    timeoutMs: BROKER_REFRESH_TIMEOUT_MS,
  });
}

export async function applyBinanceFuturesSettings(
  confirmationToken,
  confirmed = false,
) {
  return request("/api/binance-futures-settings/apply", {
    method: "POST",
    body: { confirmation_token: confirmationToken, confirmed },
    timeoutMs: BROKER_REFRESH_TIMEOUT_MS,
  });
}

export async function previewBinanceFuturesOrderRisk(payload = {}) {
  return request("/api/binance-futures-risk/preview", {
    method: "POST",
    body: payload,
    timeoutMs: BROKER_REFRESH_TIMEOUT_MS,
  });
}

export async function previewBinanceFuturesFillSoak(symbol = "ETHUSDT") {
  return request("/api/binance-futures-fill-soak/preview", {
    method: "POST",
    body: { symbol },
    timeoutMs: BROKER_REFRESH_TIMEOUT_MS,
  });
}

export async function startBinanceFuturesFillSoak(
  confirmationToken,
  confirmed = false,
  context = {},
) {
  return runWithSafetyConfirmation(
    SAFETY_CONFIRMATION_ACTIONS.BINANCE_FUTURES_FILL_SOAK_START,
    { symbol: String(context.symbol || "") },
    (confirmation) => request("/api/binance-futures-fill-soak/start", {
      method: "POST",
      body: {
        confirmation_token: confirmationToken,
        confirmed,
        ...safetyConfirmationPayload(confirmation),
      },
      timeoutMs: BROKER_REFRESH_TIMEOUT_MS,
    }),
  );
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

export async function getFunctionalTestWorkspace() {
  return request("/api/functional-test", { timeoutMs: 30_000 });
}

export async function createFunctionalTestPermit(targetKey, durationValue, durationUnit) {
  return request("/api/functional-test/permit", {
    method: "POST",
    body: {
      targetKey,
      durationValue,
      durationUnit,
    },
    timeoutMs: 30_000,
  });
}

export async function activateFunctionalTestToday(authorizedBy, confirmed = false) {
  return request("/api/functional-test/activate", {
    method: "POST",
    body: { authorizedBy, confirmed },
    timeoutMs: 30_000,
  });
}

export async function startFunctionalTest(targetKey, confirmed = false) {
  return runWithSafetyConfirmation(
    SAFETY_CONFIRMATION_ACTIONS.FUNCTIONAL_TEST_START,
    { targetKey },
    (confirmation) => request("/api/functional-test/start", {
      method: "POST",
      body: {
        targetKey,
        confirmed,
        ...safetyConfirmationPayload(confirmation),
      },
      timeoutMs: 180_000,
    }),
  );
}

export async function pauseFunctionalTestToday(confirmed = false) {
  return request("/api/functional-test/pause", {
    method: "POST",
    body: { confirmed },
    timeoutMs: 180_000,
  });
}

export async function stopFunctionalTest(confirmed = false) {
  return request("/api/functional-test/stop", {
    method: "POST",
    body: { confirmed },
    timeoutMs: 180_000,
  });
}

export async function runStrategyCycle(profileId) {
  return request("/api/strategy-cycle", { method: "POST", body: { profile_id: profileId } });
}

export async function startContinuousRuntime(
  profileId,
  mode,
  portfolioId = "",
  deploymentId = "",
  strategyId = "",
) {
  return request("/api/runtime/start", {
    method: "POST",
    body: {
      profile_id: profileId,
      mode,
      portfolio_id: portfolioId,
      deployment_id: deploymentId,
      strategy_id: strategyId,
    },
    timeoutMs: 30000,
  });
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

export async function transitionIncident(incidentId, action, note = "") {
  return request("/api/incidents/transition", {
    method: "POST",
    body: { incident_id: incidentId, action, note },
  });
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
