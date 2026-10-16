import test from "node:test";
import assert from "node:assert/strict";
import { startContinuousRuntime, stopContinuousRuntime, saveEnvSettings,
  cancelAllKisStartPreparations, registerKisPreparationPresenter } from "../src/api.js";

const previousWindow = globalThis.window; const previousFetch = globalThis.fetch;
const deferred = () => { let resolve; const promise = new Promise((yes) => { resolve = yes; }); return { promise, resolve }; };
const turn = () => new Promise((resolve) => setImmediate(resolve));
const csrf = "test-native-session-csrf-value-with-at-least-forty-characters";
const body = { profile_id: "stock", mode: "SMALL_LIVE", portfolio_id: "portfolio-a", deployment_id: "deployment-a", strategy_id: "strategy-a" };
const begin = { ok: true, preparationId: "job-a", status: "PREPARING_PUBLIC", maximumPreparationSeconds: 660, runtimeStarted: false };
function ready(changes = {}) {
  return { ok: true, preparationId: "job-a", status: "READY", nativeNonce: "opaque-native-nonce",
    expiresAt: new Date(Date.now() + 45000).toISOString(),
    context: { schemaVersion: "kis-scoped-manual-start-context/v1", profileId: "stock", mode: "SMALL_LIVE",
      portfolioId: "portfolio-a", deploymentId: "deployment-a", strategyId: "strategy-a" }, ...changes };
}
const json = (value) => ({ ok: true, headers: { get: () => "application/json" }, json: async () => value });
function setup({ status, native } = {}) {
  globalThis.window = globalThis; const calls = []; let nativeCalls = 0;
  globalThis.pywebview = { api: {
    functional_http_session: async () => ({ ok: true, available: true, csrfHeader: "X-LiveTrader-CSRF", csrfToken: csrf }),
    request_kis_operational_start_confirmation: async (nonce) => { nativeCalls += 1; assert.equal(nonce, "opaque-native-nonce"); return native ? native() : { ok: true, attestation: { opaque: "native-only" } }; },
  } };
  globalThis.fetch = async (path, options) => {
    calls.push({ path, method: options.method, body: options.body ? JSON.parse(options.body) : undefined, headers: options.headers });
    if (path.endsWith("-challenge")) return json(begin);
    if (path.includes("prepare-status?")) return json(status ? await status() : ready());
    if (path.endsWith("prepare-cancel")) return json({ ok: true, preparationId: "job-a", status: "CANCELLED" });
    return json({ ok: true, snapshot: { fixtureOnly: true } });
  };
  return { calls, nativeCalls: () => nativeCalls };
}
const start = () => startContinuousRuntime("stock", "SMALL_LIVE", "portfolio-a", "deployment-a", "strategy-a");

test("async status creates no authority; native direct confirmation precedes exactly one normal Start", async () => {
  const f = setup(); const progress = []; const unregister = registerKisPreparationPresenter((v) => progress.push(v));
  try {
    assert.equal((await start()).ok, true);
    assert.deepEqual(f.calls.map((r) => r.path), ["/api/runtime/kis-manual-start-challenge", "/api/runtime/kis-native-prepare-status?preparationId=job-a", "/api/runtime/start"]);
    assert.deepEqual(f.calls[0].body, body); assert.equal(f.calls[0].headers["X-LiveTrader-CSRF"], csrf);
    assert.equal(f.calls[1].method, "GET"); assert.equal(f.calls[1].headers["X-LiveTrader-CSRF"], undefined);
    assert.deepEqual(f.calls[2].body.safety_confirmation, { nativeAttestation: { opaque: "native-only" } });
    assert.equal(f.nativeCalls(), 1); assert.equal(progress.at(-1).status, "COMPLETE");
  } finally { unregister(); }
});

test("native cancel or changed exact target sends zero normal Start", async () => {
  for (const options of [{ native: () => ({ ok: false }) }, { status: () => ready({ context: { ...ready().context, deploymentId: "foreign" } }) }]) {
    const f = setup(options); await assert.rejects(start());
    assert.equal(f.calls.filter((r) => r.path === "/api/runtime/start").length, 0);
    assert.equal(f.calls.at(-1).path, "/api/runtime/kis-native-prepare-cancel");
    assert.equal(f.calls.at(-1).headers["X-LiveTrader-CSRF"], csrf);
  }
});

test("public KIS preparation does not block independent crypto normal Start", async () => {
  const hold = deferred(); const f = setup({ status: () => hold.promise });
  const pending = start().catch((e) => e); await turn();
  const crypto = await startContinuousRuntime("crypto", "SMALL_LIVE", "crypto-portfolio", "crypto-deployment", "crypto-source");
  assert.equal(crypto.ok, true); assert.equal(f.nativeCalls(), 0);
  const submitted = f.calls.filter((r) => r.path === "/api/runtime/start");
  assert.equal(submitted.length, 1); assert.equal(submitted[0].body.profile_id, "crypto");
  await cancelAllKisStartPreparations(); hold.resolve(ready());
  assert.equal((await pending).code, "KIS_PREPARATION_CANCELLED");
});

test("STOP proceeds while KIS poll is pending and old READY can never start", async () => {
  const hold = deferred(); const f = setup({ status: () => hold.promise });
  const pending = start().catch((e) => e); await turn();
  await stopContinuousRuntime("stock", { runtimeId: "runtime-a", deploymentId: "deployment-a" });
  assert.ok(f.calls.some((r) => r.path === "/api/runtime/kis-stop-and-reconcile"));
  assert.ok(f.calls.some((r) => r.path === "/api/runtime/kis-native-prepare-cancel"));
  hold.resolve(ready()); assert.equal((await pending).code, "KIS_PREPARATION_CANCELLED");
  assert.equal(f.nativeCalls(), 0); assert.equal(f.calls.filter((r) => r.path === "/api/runtime/start").length, 0);
});

test("settings mutation invalidates native-confirmation intent before it can submit", async () => {
  const hold = deferred(); const f = setup({ native: () => hold.promise });
  const pending = start().catch((e) => e); await turn(); assert.equal(f.nativeCalls(), 1);
  await saveEnvSettings({ KIS_ACCOUNT_PRODUCT_CODE: "synthetic-settings-change" });
  hold.resolve({ ok: true, attestation: { opaque: "late-native" } });
  assert.equal((await pending).code, "KIS_PREPARATION_CANCELLED");
  assert.equal(f.calls.filter((r) => r.path === "/api/runtime/start").length, 0);
});

test("native CSRF session timeout releases request and cannot send a late begin POST", async () => {
  const hold = deferred(); let sent = 0;
  globalThis.window = { setTimeout, clearTimeout, pywebview: { api: { functional_http_session: () => hold.promise } } };
  globalThis.fetch = async () => { sent += 1; throw new Error("A timed-out request must never be sent"); };
  const freshApi = await import("../src/api.js?isolated-native-session-timeout-fixture");
  await assert.rejects(freshApi.request("/api/runtime/kis-manual-start-challenge", { method: "POST", body, timeoutMs: 5 }), (e) => e.code === "TIMEOUT");
  hold.resolve({ ok: true, available: true, csrfHeader: "X-LiveTrader-CSRF", csrfToken: csrf });
  await turn(); assert.equal(sent, 0);
});

test("a never-resolving first CSRF bridge cannot poison a later valid retry", async () => {
  let bridgeCalls = 0; const sent = [];
  const fresh = await import("../src/api.js?csrf-never-resolving-recovery");
  globalThis.window = { setTimeout, clearTimeout, pywebview: { api: { functional_http_session: () => { bridgeCalls += 1; return new Promise(() => {}); } } } };
  globalThis.fetch = async (path, options) => { sent.push(options.headers["X-LiveTrader-CSRF"]); return json({ ok: true }); };
  await assert.rejects(fresh.request("/api/runtime/kis-manual-start-challenge", { method: "POST", body, timeoutMs: 5 }), (e) => e.code === "TIMEOUT");
  const recoveredToken = "recovered-native-csrf-token-".repeat(2);
  globalThis.window.pywebview.api.functional_http_session = async () => { bridgeCalls += 1; return { ok: true, available: true, csrfHeader: "X-LiveTrader-CSRF", csrfToken: recoveredToken }; };
  assert.equal((await fresh.request("/api/runtime/kis-manual-start-challenge", { method: "POST", body, timeoutMs: 100 })).ok, true);
  assert.equal(bridgeCalls, 2); assert.deepEqual(sent, [recoveredToken]);
});

test("old concurrent CSRF failure cannot clear a newer valid generation or send an old POST", async () => {
  let oldReject;
  const oldPromise = new Promise((_, reject) => { oldReject = reject; });
  let bridgeCalls = 0; const tokens = [];
  const fresh = await import("../src/api.js?csrf-generation-concurrent-failure");
  globalThis.window = { setTimeout, clearTimeout, pywebview: { api: { functional_http_session: () => { bridgeCalls += 1; return oldPromise; } } } };
  globalThis.fetch = async (path, options) => { tokens.push(options.headers["X-LiveTrader-CSRF"]); return json({ ok: true }); };
  const first = fresh.request("/api/runtime/kis-manual-start-challenge", { method: "POST", body, timeoutMs: 5 }).catch((e) => e);
  const oldWaiter = fresh.request("/api/runtime/kis-native-prepare-cancel", { method: "POST", body: { preparationId: "old" }, timeoutMs: 1000 }).catch((e) => e);
  assert.equal((await first).code, "TIMEOUT");
  const recoveredToken = "new-generation-native-csrf-token-".repeat(2);
  globalThis.window.pywebview.api.functional_http_session = async () => { bridgeCalls += 1; return { ok: true, available: true, csrfHeader: "X-LiveTrader-CSRF", csrfToken: recoveredToken }; };
  await fresh.request("/api/runtime/kis-manual-start-challenge", { method: "POST", body, timeoutMs: 100 });
  oldReject(new Error("old bridge failed")); await oldWaiter; await turn();
  await fresh.request("/api/runtime/kis-manual-start-challenge", { method: "POST", body, timeoutMs: 100 });
  assert.equal(bridgeCalls, 2); assert.deepEqual(tokens, [recoveredToken, recoveredToken]);
});

test("a timed-out fetch after valid CSRF resolution keeps the valid session cached", async () => {
  const fresh = await import("../src/api.js?csrf-valid-fetch-timeout");
  let bridgeCalls = 0; let fetchCalls = 0;
  globalThis.window = { setTimeout, clearTimeout, pywebview: { api: { functional_http_session: async () => { bridgeCalls += 1; return { ok: true, available: true, csrfHeader: "X-LiveTrader-CSRF", csrfToken: csrf }; } } } };
  globalThis.fetch = async (_, options) => {
    fetchCalls += 1;
    if (fetchCalls === 1) return new Promise((_, reject) => options.signal.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")), { once: true }));
    return json({ ok: true });
  };
  await assert.rejects(fresh.request("/api/runtime/kis-manual-start-challenge", { method: "POST", body, timeoutMs: 5 }), (e) => e.code === "TIMEOUT");
  await fresh.request("/api/runtime/kis-manual-start-challenge", { method: "POST", body, timeoutMs: 100 });
  assert.equal(bridgeCalls, 1); assert.equal(fetchCalls, 2);
});

test.after(async () => {
  await cancelAllKisStartPreparations(); globalThis.fetch = previousFetch; delete globalThis.pywebview;
  if (previousWindow === undefined) delete globalThis.window; else globalThis.window = previousWindow;
});
