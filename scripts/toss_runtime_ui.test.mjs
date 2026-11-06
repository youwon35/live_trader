import test from "node:test";
import assert from "node:assert/strict";
import { startContinuousRuntime, stopContinuousRuntime, registerSafetyConfirmationPresenter } from "../src/api.js";
import { deploymentRuntimeProfile } from "../src/deploymentSelection.js";

const originalWindow = globalThis.window;
const originalFetch = globalThis.fetch;
const response = value => ({ok: true, headers: {get: () => "application/json"}, json: async () => value});

test("Toss uses exact deployment, existing consent, and ordinary server guards without KIS native proof", async () => {
  const calls = [];
  globalThis.window = globalThis;
  globalThis.pywebview = {api: {
    functional_http_session: async () => ({ok:true, available:true, csrfHeader:"X-LiveTrader-CSRF", csrfToken:"fixture-only-token-long-enough-for-native-csrf-session"}),
    request_kis_operational_start_confirmation: () => {throw new Error("Toss must never use KIS native proof");},
  }};
  globalThis.fetch = async (path, options) => {
    calls.push({path, body: options.body ? JSON.parse(options.body) : null});
    return response(path === "/api/safety-confirmation/challenge"
      ? {ok:true, challengeId:"fixture", token:"confirmation-token", expectedPhrase:"LIVE TEST", expiresAt:new Date(Date.now()+60000).toISOString()}
      : {ok:false, reason:"legitimate Toss evidence still required"});
  };
  const unregister = registerSafetyConfirmationPresenter(async c => ({confirmed:true, typedPhrase:c.expectedPhrase}));
  try {
    const result = await startContinuousRuntime("stock", "SMALL_LIVE", "p", "toss-reviewed-deployment", "s", {brokerId:"toss"});
    assert.equal(result.ok, false);
    assert.deepEqual(calls.map(r => r.path), ["/api/safety-confirmation/challenge", "/api/runtime/start"]);
    assert.equal(calls[1].body.deployment_id, "toss-reviewed-deployment");
    assert.ok(calls[1].body.safety_confirmation);
    assert.equal(calls[1].body.initializationOptions, undefined);
  } finally {unregister();}
});

test("Toss stop never enters KIS cancel-and-reconcile and keeps exact runtime", async () => {
  const calls = [];
  globalThis.fetch = async (path, options) => {calls.push({path,body:JSON.parse(options.body)});return response({ok:true});};
  await stopContinuousRuntime("stock", {brokerId:"toss",runtimeId:"runtime-toss",deploymentId:"deployment-toss"});
  assert.equal(calls[0].path, "/api/runtime/stop");
  assert.equal(calls[0].body.runtime_id, "runtime-toss");
  assert.equal(deploymentRuntimeProfile({brokerId:"toss"}), "stock");
  assert.equal(deploymentRuntimeProfile({brokerId:"kis"}), "stock");
});

test.after(() => {globalThis.window=originalWindow;globalThis.fetch=originalFetch;delete globalThis.pywebview;});
