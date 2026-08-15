import assert from "node:assert/strict";
import { createHash } from "node:crypto";

import {
  API_DEFAULT_TIMEOUT_MS,
  BROKER_REFRESH_TIMEOUT_MS,
  ApiRequestError,
  engageNativeEmergencyStop,
  getNativeEmergencyStopStatus,
  isApiConnectionFailure,
  isApiRequestTimeout,
  isSafetyConfirmationCancelled,
  nativeEmergencyStopAvailable,
  registerSafetyConfirmationPresenter,
  reprepareCryptoFirstLive,
  recoverBinanceSpotFunctional,
  recoverUpbitFunctional,
  request,
  runFinalPreflight,
  safetyConfirmationValuesDigest,
  saveEnvSettings,
  setFlag,
  startBinanceSpotFunctional,
  startBinanceFuturesFillSoak,
  startContinuousRuntime,
  startFunctionalTest,
  startUpbitFunctional,
  stopBinanceSpotFunctional,
  stopUpbitFunctional,
  transitionIncident,
} from "../src/api.js";

assert.ok(API_DEFAULT_TIMEOUT_MS > 10_000);
assert.ok(BROKER_REFRESH_TIMEOUT_MS > API_DEFAULT_TIMEOUT_MS);

const timeoutError = new ApiRequestError("slow read", "TIMEOUT");
assert.equal(isApiRequestTimeout(timeoutError), true);
assert.equal(isApiConnectionFailure(timeoutError), false);

const networkError = new ApiRequestError("offline", "NETWORK");
assert.equal(isApiConnectionFailure(networkError), true);
assert.equal(isApiRequestTimeout(networkError), false);

const previousWindow = globalThis.window;
const previousFetch = globalThis.fetch;
let unregisterSafetyPresenter = null;
globalThis.window = globalThis;

try {
  let nativeEngagements = 0;
  globalThis.pywebview = {
    api: {
      engage_emergency_stop: async () => {
        nativeEngagements += 1;
        return { ok: true, active: true, durable: true, status: "engaged" };
      },
      emergency_stop_status: async () => ({ active: true, durable: true, status: "engaged" }),
    },
  };
  assert.equal(nativeEmergencyStopAvailable(), true);
  assert.equal((await engageNativeEmergencyStop()).active, true);
  assert.deepEqual(await getNativeEmergencyStopStatus(), {
    active: true,
    durable: true,
    status: "engaged",
    available: true,
    status_available: true,
  });
  assert.equal(nativeEngagements, 1);

  globalThis.pywebview.api.engage_emergency_stop = async () => ({
    ok: false,
    active: true,
    durable: false,
    status: "write-failed-local-fail-closed",
    reason: "disk-write-failed",
  });
  await assert.rejects(
    engageNativeEmergencyStop(),
    (error) => error.code === "EMERGENCY_LATCH_FAILED"
      && error.details?.active === true
      && error.details?.durable === false,
  );

  globalThis.pywebview.api.engage_emergency_stop = async () => {
    throw new Error("native transport unavailable");
  };
  await assert.rejects(
    engageNativeEmergencyStop(),
    (error) => error.code === "EMERGENCY_BRIDGE_FAILED" && error.cause instanceof Error,
  );

  globalThis.pywebview.api.engage_emergency_stop = async () => ({ ok: true, active: true, durable: true });
  globalThis.pywebview.api.emergency_stop_status = async () => {
    throw new Error("status unavailable");
  };
  assert.deepEqual(await getNativeEmergencyStopStatus(), {
    available: true,
    status_available: false,
    active: false,
    durable: false,
    status: "read-failed",
    reason: "status unavailable",
  });

  delete globalThis.pywebview.api.engage_emergency_stop;
  globalThis.pywebview.api.emergency_stop_status = async () => ({ active: true, durable: true, status: "engaged" });
  assert.equal(nativeEmergencyStopAvailable(), false);
  assert.deepEqual(await getNativeEmergencyStopStatus(), {
    active: true,
    durable: true,
    status: "engaged",
    available: false,
    status_available: true,
  });

  delete globalThis.pywebview;
  assert.equal(nativeEmergencyStopAvailable(), false);
  await assert.rejects(
    engageNativeEmergencyStop(),
    (error) => error.code === "EMERGENCY_BRIDGE_UNAVAILABLE",
  );
  assert.deepEqual(await getNativeEmergencyStopStatus(), {
    available: false,
    status_available: false,
    active: false,
    durable: false,
    status: "unavailable",
  });

  const jsonResponse = (result) => ({
    ok: true,
    headers: { get: () => "application/json" },
    json: async () => result,
  });
  const challengeExpiry = () => new Date(Date.now() + 60_000).toISOString();

  const functionalRequests = [];
  globalThis.fetch = async (path, options) => {
    functionalRequests.push({ path, options });
    return jsonResponse({ ok: true });
  };
  await request("/api/binance-spot-functional/status");
  assert.equal(
    functionalRequests.length,
    1,
    "protected GET must rely on the HttpOnly cookie without touching the native bridge",
  );
  await assert.rejects(
    request("/api/binance-spot-functional/stop", { method: "POST", body: {} }),
    (error) => error.code === "TRUSTED_APP_SESSION_UNAVAILABLE",
  );
  assert.equal(
    functionalRequests.length,
    1,
    "missing native CSRF bridge must fail before network mutation",
  );
  let functionalBridgeCalls = 0;
  globalThis.pywebview = {
    api: {
      functional_http_session: async () => {
        functionalBridgeCalls += 1;
        return {
          ok: true,
          available: true,
          csrfHeader: "X-LiveTrader-CSRF",
          csrfToken: "csrf-token-that-is-long-enough-for-the-native-boundary",
        };
      },
    },
  };
  await request("/api/binance-spot-functional/stop", { method: "POST", body: {} });
  assert.equal(
    functionalBridgeCalls,
    1,
    "a later native bridge must recover after an unavailable first attempt",
  );
  assert.equal(
    functionalRequests.at(-1).options.headers["X-LiveTrader-CSRF"],
    "csrf-token-that-is-long-enough-for-the-native-boundary",
  );

  let presentedChallenge = null;
  let safetyRequests = [];
  unregisterSafetyPresenter = registerSafetyConfirmationPresenter(async (challenge) => {
    presentedChallenge = challenge;
    return { confirmed: true, typedPhrase: challenge.expectedPhrase };
  });
  globalThis.fetch = async (path, options) => {
    safetyRequests.push({ path, body: JSON.parse(options.body) });
    if (path === "/api/safety-confirmation/challenge") {
      return jsonResponse({
        ok: true,
        challenge: {
          challengeId: "challenge-kill-off",
          token: "secret-one-time-token",
          expectedPhrase: "LIVE 1234",
          expiresAt: challengeExpiry(),
          displayContext: { accountLast4: "1234", target: "전역 Kill 해제" },
        },
      });
    }
    return jsonResponse({ ok: true, snapshot: { kill_switch: false } });
  };
  await setFlag("kill_switch", false, true);
  assert.deepEqual(safetyRequests.map((item) => item.path), [
    "/api/safety-confirmation/challenge",
    "/api/flag",
  ]);
  assert.deepEqual(safetyRequests[0].body, {
    action: "KILL_SWITCH_OFF",
    context: { name: "kill_switch", value: false },
  });
  assert.equal("token" in presentedChallenge, false, "presenter/UI must never receive the bearer token");
  assert.deepEqual(presentedChallenge.displayContext, {
    accountLast4: "1234",
    target: "전역 Kill 해제",
  });
  assert.deepEqual(safetyRequests[1].body, {
    name: "kill_switch",
    value: false,
    confirmed: true,
    safety_confirmation: {
      challengeId: "challenge-kill-off",
      token: "secret-one-time-token",
      typedPhrase: "LIVE 1234",
    },
  });
  unregisterSafetyPresenter();

  let mutationCalls = 0;
  unregisterSafetyPresenter = registerSafetyConfirmationPresenter(async () => ({ confirmed: false }));
  globalThis.fetch = async (path) => {
    if (path === "/api/safety-confirmation/challenge") {
      return jsonResponse({
        ok: true,
        challengeId: "challenge-cancel",
        token: "cancel-token",
        expectedPhrase: "LIVE 1234",
        expiresAt: challengeExpiry(),
        displayContext: {},
      });
    }
    mutationCalls += 1;
    return jsonResponse({ ok: true });
  };
  await assert.rejects(
    setFlag("dry_run", false, true),
    (error) => isSafetyConfirmationCancelled(error),
  );
  assert.equal(mutationCalls, 0, "cancelled confirmation must not mutate state");
  unregisterSafetyPresenter();

  let expiredPresenterCalls = 0;
  unregisterSafetyPresenter = registerSafetyConfirmationPresenter(async () => {
    expiredPresenterCalls += 1;
    return { confirmed: true, typedPhrase: "LIVE 1234" };
  });
  globalThis.fetch = async (path) => {
    if (path === "/api/safety-confirmation/challenge") {
      return jsonResponse({
        ok: true,
        challengeId: "challenge-expired",
        token: "expired-token",
        expectedPhrase: "LIVE 1234",
        expiresAt: new Date(Date.now() - 1_000).toISOString(),
        displayContext: {},
      });
    }
    mutationCalls += 1;
    return jsonResponse({ ok: true });
  };
  await assert.rejects(
    setFlag("dry_run", false, true),
    (error) => error.code === "SAFETY_CONFIRMATION_EXPIRED",
  );
  assert.equal(expiredPresenterCalls, 0, "expired challenge must not reopen the confirmation UI");
  assert.equal(mutationCalls, 0, "expired challenge must not mutate state");
  unregisterSafetyPresenter();

  let presenterResolve;
  let presenterStartedResolve;
  const presenterStarted = new Promise((resolve) => { presenterStartedResolve = resolve; });
  let challengeIssueCalls = 0;
  mutationCalls = 0;
  unregisterSafetyPresenter = registerSafetyConfirmationPresenter((challenge) => {
    presenterStartedResolve(challenge);
    return new Promise((resolve) => { presenterResolve = resolve; });
  });
  globalThis.fetch = async (path) => {
    if (path === "/api/safety-confirmation/challenge") {
      challengeIssueCalls += 1;
      return jsonResponse({
        ok: true,
        challengeId: "challenge-concurrent",
        token: "concurrent-token",
        expectedPhrase: "LIVE 5678",
        expiresAt: challengeExpiry(),
        displayContext: { accountLast4: "5678" },
      });
    }
    mutationCalls += 1;
    return jsonResponse({ ok: true });
  };
  const firstProtectedMutation = setFlag("new_entries_blocked", false, true);
  await presenterStarted;
  await assert.rejects(
    setFlag("new_entries_blocked", false, true),
    (error) => error.code === "SAFETY_CONFIRMATION_BUSY",
  );
  assert.equal(challengeIssueCalls, 1, "double click must issue only one challenge");
  presenterResolve({ confirmed: true, typedPhrase: "LIVE 5678" });
  await firstProtectedMutation;
  assert.equal(mutationCalls, 1, "confirmed challenge must be submitted once");
  unregisterSafetyPresenter();

  const realOrderValues = {
    LIVE_TRADER_ENABLE_REAL_ORDERS: "true",
    KIS_APP_KEY: "never-send-this-secret-to-challenge",
    KIS_ACCOUNT_NUMBER: "12345678",
  };
  const canonicalValues = Object.fromEntries(
    Object.keys(realOrderValues).sort().map((key) => [key, realOrderValues[key]]),
  );
  const expectedValuesDigest = createHash("sha256")
    .update(JSON.stringify(canonicalValues), "utf8")
    .digest("hex");
  assert.equal(await safetyConfirmationValuesDigest(realOrderValues), expectedValuesDigest);
  safetyRequests = [];
  unregisterSafetyPresenter = registerSafetyConfirmationPresenter(async (challenge) => ({
    confirmed: true,
    typedPhrase: challenge.expectedPhrase,
  }));
  globalThis.fetch = async (path, options) => {
    safetyRequests.push({ path, body: JSON.parse(options.body) });
    if (path === "/api/safety-confirmation/challenge") {
      return jsonResponse({
        ok: true,
        challengeId: "challenge-real-orders",
        token: "real-orders-token",
        expectedPhrase: "LIVE 1234",
        expiresAt: challengeExpiry(),
        displayContext: { accountLast4: "1234", maxAmount: "1,000,000 KRW" },
      });
    }
    return jsonResponse({ ok: true, settings: {} });
  };
  await saveEnvSettings(realOrderValues, true);
  assert.deepEqual(safetyRequests[0].body, {
    action: "REAL_ORDERS_ENABLE",
    context: {
      settingKeys: Object.keys(realOrderValues).sort(),
      enableRealOrders: true,
      valuesDigest: expectedValuesDigest,
    },
  });
  assert.equal(
    JSON.stringify(safetyRequests[0].body).includes(realOrderValues.KIS_APP_KEY),
    false,
    "challenge request must not contain raw credentials",
  );
  assert.deepEqual(safetyRequests[1].body.values, realOrderValues);
  assert.equal(safetyRequests[1].body.safety_confirmation.challengeId, "challenge-real-orders");

  const postSafetyFetch = globalThis.fetch;
  const cryptoRequests = [];
  globalThis.fetch = async (path, options) => {
    const body = options.body ? JSON.parse(options.body) : undefined;
    cryptoRequests.push({ path, body, headers: options.headers });
    if (path === "/api/safety-confirmation/challenge") {
      const action = body.action;
      return jsonResponse({
        ok: true,
        challengeId: `challenge-${action.toLowerCase()}`,
        token: `token-${action.toLowerCase()}`,
        expectedPhrase: "LIVE C0DE",
        expiresAt: challengeExpiry(),
        displayContext: { sessionId: body.context.sessionId || "" },
        ...(action === "UPBIT_FUNCTIONAL_START"
          ? { approvalId: "upbit-preissued-approval", candidateHash: "a".repeat(64) }
          : {}),
        ...(action === "UPBIT_FUNCTIONAL_RECOVER"
          ? { recoveryId: "upbit-preissued-recovery", sessionId: "upbit-session-1" }
          : {}),
        ...(action === "BINANCE_SPOT_FUNCTIONAL_START"
          ? {
              approvalId: "binance-preissued-approval",
              bootstrapId: "bootstrap-1",
              bootstrapHash: "b".repeat(64),
              sessionNonceHash: "c".repeat(64),
              codeHash: "d".repeat(64),
              accountFingerprint: "e".repeat(64),
              bindingHash: "f".repeat(64),
            }
          : {}),
      });
    }
    return jsonResponse({ ok: true });
  };

  await startUpbitFunctional();
  await stopUpbitFunctional("upbit-session-1");
  await recoverUpbitFunctional("", "upbit-session-1");
  await startBinanceSpotFunctional();
  await stopBinanceSpotFunctional("binance-session-1");
  await recoverBinanceSpotFunctional("binance-session-1");
  const cryptoMutations = cryptoRequests.filter(
    (item) => item.path !== "/api/safety-confirmation/challenge",
  );
  assert.deepEqual(cryptoMutations.map((item) => item.path), [
    "/api/upbit-functional/start",
    "/api/upbit-functional/stop",
    "/api/upbit-functional/recover",
    "/api/binance-spot-functional/start",
    "/api/binance-spot-functional/stop",
    "/api/binance-spot-functional/recover",
  ]);
  assert.deepEqual(cryptoMutations[0].body, {
    approvalId: "upbit-preissued-approval",
    operatorConfirmation: {
      challengeId: "challenge-upbit_functional_start",
      token: "token-upbit_functional_start",
      typedPhrase: "LIVE C0DE",
    },
  });
  assert.equal(cryptoMutations[2].body.recoveryId, "upbit-preissued-recovery");
  assert.equal(cryptoMutations[3].body.approvalId, "binance-preissued-approval");
  for (const item of cryptoMutations) {
    assert.equal(
      item.headers["X-LiveTrader-CSRF"],
      "csrf-token-that-is-long-enough-for-the-native-boundary",
    );
  }
  const presentedText = JSON.stringify(presentedChallenge || {});
  assert.equal(presentedText.includes("token-upbit"), false, "UI must not receive bearer tokens");

  cryptoRequests.length = 0;
  await reprepareCryptoFirstLive();
  assert.equal(cryptoRequests[0].path, "/api/crypto-first-live/reprepare");
  assert.deepEqual(cryptoRequests[0].body, {});
  assert.equal(
    cryptoRequests[0].headers["X-LiveTrader-CSRF"],
    "csrf-token-that-is-long-enough-for-the-native-boundary",
  );
  globalThis.fetch = postSafetyFetch;

  safetyRequests = [];
  await startFunctionalTest("kis:portfolio:alpha", true);
  assert.deepEqual(safetyRequests[0].body, {
    action: "FUNCTIONAL_TEST_START",
    context: { targetKey: "kis:portfolio:alpha" },
  });
  assert.equal(safetyRequests[1].path, "/api/functional-test/start");
  assert.equal(safetyRequests[1].body.safety_confirmation.typedPhrase, "LIVE 1234");

  safetyRequests = [];
  await startBinanceFuturesFillSoak("preview-confirmation-token", true, { symbol: "ETHUSDT" });
  assert.deepEqual(safetyRequests[0].body, {
    action: "BINANCE_FUTURES_FILL_SOAK_START",
    context: { symbol: "ETHUSDT" },
  });
  assert.equal(safetyRequests[1].path, "/api/binance-futures-fill-soak/start");
  assert.equal(safetyRequests[1].body.confirmation_token, "preview-confirmation-token");
  assert.equal(safetyRequests[1].body.safety_confirmation.challengeId, "challenge-real-orders");

  safetyRequests = [];
  await setFlag("kill_switch", true, true);
  assert.deepEqual(
    safetyRequests.map((item) => item.path),
    ["/api/flag"],
    "Kill ON must bypass the release challenge and execute immediately",
  );
  assert.equal(safetyRequests[0].body.safety_confirmation, undefined);
  unregisterSafetyPresenter();

  let preflightRequest = null;
  globalThis.fetch = async (path, options) => {
    preflightRequest = { path, options };
    return {
      ok: true,
      headers: { get: () => "application/json" },
      json: async () => ({ ok: true, preflight_snapshot: { deployment_id: "DEPLOY-20260801-01" } }),
    };
  };
  const preflightResult = await runFinalPreflight("DEPLOY-20260801-01", "STRATEGY-BTC-1H");
  assert.equal(preflightRequest.path, "/api/preflight");
  assert.equal(preflightRequest.options.method, "POST");
  assert.deepEqual(JSON.parse(preflightRequest.options.body), {
    deployment_id: "DEPLOY-20260801-01",
    strategy_id: "STRATEGY-BTC-1H",
  });
  assert.equal(preflightResult.preflight_snapshot.deployment_id, "DEPLOY-20260801-01");

  let runtimeRequest = null;
  globalThis.fetch = async (path, options) => {
    runtimeRequest = { path, options };
    return {
      ok: true,
      headers: { get: () => "application/json" },
      json: async () => ({ ok: true, runtime_session_id: "SESSION-01" }),
    };
  };
  await startContinuousRuntime("crypto", "SMALL_LIVE", "PORTFOLIO-01", "DEPLOYMENT-01", "STRATEGY-01");
  assert.equal(runtimeRequest.path, "/api/runtime/start");
  assert.deepEqual(JSON.parse(runtimeRequest.options.body), {
    profile_id: "crypto",
    mode: "SMALL_LIVE",
    portfolio_id: "PORTFOLIO-01",
    deployment_id: "DEPLOYMENT-01",
    strategy_id: "STRATEGY-01",
  });

  let incidentRequest = null;
  globalThis.fetch = async (path, options) => {
    incidentRequest = { path, options };
    return {
      ok: true,
      headers: { get: () => "application/json" },
      json: async () => ({ ok: true, incident: { state: "ACKNOWLEDGED" } }),
    };
  };
  await transitionIncident("incident-01", "acknowledge", "원인 확인 중");
  assert.equal(incidentRequest.path, "/api/incidents/transition");
  assert.deepEqual(JSON.parse(incidentRequest.options.body), {
    incident_id: "incident-01",
    action: "acknowledge",
    note: "원인 확인 중",
  });

  globalThis.fetch = async (_path, { signal }) =>
    new Promise((_resolve, reject) => {
      signal.addEventListener("abort", () => {
        const error = new Error("aborted");
        error.name = "AbortError";
        reject(error);
      }, { once: true });
    });

  await assert.rejects(
    request("/api/slow", { timeoutMs: 5 }),
    (error) => isApiRequestTimeout(error) && /1초/.test(error.message),
  );

  globalThis.fetch = async () => {
    throw new TypeError("fetch failed");
  };
  await assert.rejects(
    request("/api/offline"),
    (error) => isApiConnectionFailure(error) && /연결할 수 없습니다/.test(error.message),
  );
} finally {
  try {
    unregisterSafetyPresenter?.();
  } catch {
    // Test cleanup only.
  }
  delete globalThis.pywebview;
  globalThis.fetch = previousFetch;
  if (previousWindow === undefined) {
    delete globalThis.window;
  } else {
    globalThis.window = previousWindow;
  }
}

console.log("live API timeout/connection classification regression checks passed");
