import assert from "node:assert/strict";

import {
  API_DEFAULT_TIMEOUT_MS,
  BROKER_REFRESH_TIMEOUT_MS,
  ApiRequestError,
  isApiConnectionFailure,
  isApiRequestTimeout,
  request,
  runFinalPreflight,
  startContinuousRuntime,
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
globalThis.window = globalThis;

try {
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
  globalThis.fetch = previousFetch;
  if (previousWindow === undefined) {
    delete globalThis.window;
  } else {
    globalThis.window = previousWindow;
  }
}

console.log("live API timeout/connection classification regression checks passed");
