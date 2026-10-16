import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

const source = process.argv[2] || new URL("../src/api.js", import.meta.url);
const api = await import("data:text/javascript;base64," + Buffer.from(await readFile(source, "utf8")).toString("base64"));
const saved = { fetch: globalThis.fetch, window: globalThis.window };
const context = { schemaVersion: "kis-scoped-manual-start-context/v1", profileId: "stock",
  mode: "SMALL_LIVE", portfolioId: "p1", deploymentId: "d1", strategyId: "s1" };
let calls = [], nativeCalls = [], nativeResult = { ok: true, attestation: { oneUseNonce: "test-only" } };
let challenge = () => ({ ok: true, nativeNonce: "n".repeat(43), expiresAt: new Date(Date.now()+40000).toISOString(), context });
function setup() {
  calls = []; nativeCalls = [];
  globalThis.window = { setTimeout, clearTimeout, pywebview: { api: {
    functional_http_session: async () => ({ ok: true, available: true, csrfHeader: "X-LiveTrader-CSRF", csrfToken: "c".repeat(48) }),
    request_kis_operational_start_confirmation: async (nonce) => { nativeCalls.push(nonce); return nativeResult; },
  } } };
  globalThis.fetch = async (path, options) => {
    calls.push({ path, body: JSON.parse(options.body), headers: options.headers });
    return { ok: true, headers: { get: () => "application/json" },
      json: async () => path.endsWith("kis-manual-start-challenge") ? challenge() : { ok: true, runtime_session_id: "mock-session" } };
  };
}
try {
  setup();
  await api.startContinuousRuntime("stock","SMALL_LIVE","p1","d1","s1");
  assert.deepEqual(calls.map(x=>x.path),["/api/runtime/kis-manual-start-challenge","/api/runtime/start"]);
  assert.equal(calls[0].headers["X-LiveTrader-CSRF"],"c".repeat(48));
  assert.deepEqual(nativeCalls,["n".repeat(43)]);
  assert.deepEqual(calls[1].body.safety_confirmation,{nativeAttestation:{oneUseNonce:"test-only"}});
  assert.equal(Object.hasOwn(calls[0].body,"safety_confirmation"),false);

  setup(); nativeResult={ok:false,code:"KIS_NATIVE_CANCELLED"};
  await assert.rejects(api.startContinuousRuntime("stock","SMALL_LIVE","p1","d1","s1"),{code:"SAFETY_CONFIRMATION_CANCELLED"});
  assert.equal(calls.length,1);
  nativeResult={ok:true,attestation:{oneUseNonce:"test-only"}};

  setup();
  delete globalThis.window.pywebview.api.request_kis_operational_start_confirmation;
  await assert.rejects(api.startContinuousRuntime("stock","SMALL_LIVE","p1","d1","s1"),{code:"KIS_NATIVE_UNAVAILABLE"});
  assert.equal(calls.length,0);

  for (const changed of [{...context,deploymentId:"other"},{...context,mode:"FULL_LIVE"},{...context,profileId:"crypto"}]) {
    setup(); challenge=()=>({ok:true,nativeNonce:"n".repeat(43),expiresAt:new Date(Date.now()+40000).toISOString(),context:changed});
    await assert.rejects(api.startContinuousRuntime("stock","SMALL_LIVE","p1","d1","s1"),{code:"KIS_NATIVE_CONTEXT_INVALID"});
    assert.equal(nativeCalls.length,0); assert.equal(calls.length,1);
  }

  setup();challenge=()=>({ok:true,nativeNonce:"n".repeat(43),expiresAt:new Date(Date.now()-1).toISOString(),context});
  await assert.rejects(api.startContinuousRuntime("stock","SMALL_LIVE","p1","d1","s1"),{code:"KIS_NATIVE_CONTEXT_INVALID"});
  assert.equal(nativeCalls.length,0);

  setup();challenge=()=>({ok:true,nativeNonce:"n".repeat(43),expiresAt:new Date(Date.now()+40000).toISOString(),context});
  let release,entered;
  const waiting = new Promise(r=>{entered=r;});
  globalThis.window.pywebview.api.request_kis_operational_start_confirmation=async()=>{
    entered();return new Promise(r=>{release=r;});
  };
  const first=api.startContinuousRuntime("stock","SMALL_LIVE","p1","d1","s1");
  await waiting;
  await assert.rejects(api.startContinuousRuntime("stock","SMALL_LIVE","p1","d1","s1"),{code:"KIS_START_IN_PROGRESS"});
  release(nativeResult);await first;
  assert.equal(calls.filter(x=>x.path==="/api/runtime/start").length,1);

  setup();globalThis.window = { setTimeout, clearTimeout };
  await api.startContinuousRuntime("stock","MONITOR","p1","d1","s1");
  assert.deepEqual(calls.map(x=>x.path),["/api/runtime/start"]);
  assert.equal(Object.hasOwn(calls[0].body,"safety_confirmation"),false);

  setup();globalThis.window = { setTimeout, clearTimeout };
  await api.startContinuousRuntime("crypto","SMALL_LIVE","p1","d1","s1");
  assert.deepEqual(calls.map(x=>x.path),["/api/runtime/start"]);
  assert.equal(Object.hasOwn(calls[0].body,"safety_confirmation"),false);

  setup();
  await api.stopContinuousRuntime("stock",{runtimeId:"stock-scope",deploymentId:"stock-deployment"});
  assert.deepEqual(calls.map(x=>x.path),["/api/runtime/kis-stop-and-reconcile"]);
  assert.equal(calls[0].headers["X-LiveTrader-CSRF"],"c".repeat(48));
  assert.deepEqual(calls[0].body,{profile_id:"stock",runtime_id:"stock-scope",deployment_id:"stock-deployment"});

  setup();globalThis.window={setTimeout,clearTimeout};
  await api.stopContinuousRuntime("stock");
  assert.deepEqual(calls.map(x=>x.path),["/api/runtime/stop"]);
  assert.deepEqual(calls[0].body,{profile_id:"stock",runtime_id:"",deployment_id:""});
  assert.equal(Object.hasOwn(calls[0].body,"cleanup_requested"),false);

  setup();globalThis.window={setTimeout,clearTimeout};
  await api.stopContinuousRuntime("stock",{runtimeId:"stock-monitor"});
  assert.deepEqual(calls.map(x=>x.path),["/api/runtime/stop"]);
  assert.deepEqual(calls[0].body,{profile_id:"stock",runtime_id:"stock-monitor",deployment_id:""});
  assert.equal(Object.hasOwn(calls[0].body,"cleanup_requested"),false);

  setup();globalThis.window={setTimeout,clearTimeout};
  await api.stopContinuousRuntime("crypto",{runtimeId:"crypto-scope",deploymentId:"crypto-deployment"});
  assert.deepEqual(calls.map(x=>x.path),["/api/runtime/stop"]);
  assert.equal(Object.hasOwn(calls[0].headers,"X-LiveTrader-CSRF"),false);
  console.log("KIS manual-start and exact STOP UI workflow: 14 cases PASS; mocked transports only.");
} catch (error) {
  console.error(error.name + ": " + error.message);
  process.exitCode = 1;
} finally {
  globalThis.fetch=saved.fetch;
  globalThis.window=saved.window;
}

