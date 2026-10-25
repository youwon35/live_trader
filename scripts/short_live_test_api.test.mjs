// Bundles the real short-live API wrappers with an in-memory transport.
// No browser bridge, account, HTTP server, or broker API is opened.
import assert from "node:assert/strict";
import test from "node:test";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { join } from "node:path";

const root = fileURLToPath(new URL("../", import.meta.url));
const require = createRequire(join(root, "node_modules", "fixture-resolver.cjs"));
const { build } = require("esbuild");
const bundle = await build({ entryPoints: [join(root, "src", "shortLiveTestApi.js")], bundle: true,
  write: false, format: "esm", platform: "node", logLevel: "silent", plugins: [{ name: "no-network-transport", setup(build) {
    build.onResolve({ filter: /^\.\/api\.js$/ }, () => ({ path: "fixture-transport", namespace: "offline" }));
    build.onLoad({ filter: /.*/, namespace: "offline" }, () => ({ contents: "export const request = (...args) => globalThis.__shortLiveApiTransport(...args); export const setFlag = (...args) => globalThis.__shortLiveFlagTransport(...args);", loader: "js" }));
  } }] });
const api = await import(`data:text/javascript;base64,${Buffer.from(bundle.outputFiles[0].text).toString("base64")}`);
const good = () => ({ ok: true, status: "IDLE", catalog: [], prepared: null, runtime: null });
const cases = [["snapshot", api.getShortLiveTestSnapshot], ["prepare", api.prepareShortLiveTest], ["start", api.startShortLiveTest], ["stop", api.stopShortLiveTest]];

test("all four actual wrappers reject HTTP-success application failure", async () => {
  for (const [action, call] of cases) {
    const requests = []; globalThis.__shortLiveApiTransport = async (...args) => { requests.push(args); return { ok: false, reasonCode: "OFFICIAL_READ_REJECTED" }; };
    await assert.rejects(call({ fixtureOnly: true }), /OFFICIAL_READ_REJECTED/);
    assert.equal(requests.length, 1); assert.equal(requests[0][0], `/api/short-live-tests/${action}`);
    assert.equal(requests[0][1].method || "GET", action === "snapshot" ? "GET" : "POST");
  }
});
test("only demonstrably pre-mutation server branches are labeled known rejection", async () => {
  for (const code of ["PREPARATION_EXPIRED", "PREPARATION_SCOPE_MISMATCH", "START_BLOCKED:REAL_ORDERS_DISABLED"]) {
    globalThis.__shortLiveApiTransport = async () => ({ ok: false, reasonCode: code });
    await assert.rejects(api.startShortLiveTest({}), (error) => error.knownPreMutationRejection === true);
  }
  for (const code of ["RuntimeError", "STARTUP_HISTORY_NATIVE_CLOSED_IDENTITY_REQUIRED", "UNKNOWN_FAILURE"]) {
    globalThis.__shortLiveApiTransport = async () => ({ ok: false, reasonCode: code });
    await assert.rejects(api.startShortLiveTest({}), (error) => error.knownPreMutationRejection === false);
    await assert.rejects(api.stopShortLiveTest({}), (error) => error.knownPreMutationRejection === false);
  }
  globalThis.__shortLiveApiTransport = async () => ({ ok: false, reasonCode: "STOP_SCOPE_MISMATCH" });
  await assert.rejects(api.stopShortLiveTest({}), (error) => error.knownPreMutationRejection === true);
});
test("malformed and nonboolean success never become usable snapshots", async () => {
  for (const response of [null, {}, { ok: "true" }, { ok: true, status: "PREPARED" }]) {
    globalThis.__shortLiveApiTransport = async () => response;
    await assert.rejects(api.prepareShortLiveTest({}), /SHORT_LIVE_RESPONSE_UNVERIFIED/);
  }
  const response = good(); globalThis.__shortLiveApiTransport = async () => response;
  assert.deepEqual(await api.getShortLiveTestSnapshot(), response);
});
test("network uncertainty is propagated once without automatic retry", async () => {
  let requests = 0; globalThis.__shortLiveApiTransport = async () => { requests += 1; throw new Error("FIXTURE_LOST_REPLY"); };
  await assert.rejects(api.startShortLiveTest({}), (error) => error.message === "FIXTURE_LOST_REPLY" && error.knownPreMutationRejection !== true);
  assert.equal(requests, 1);
});
test("catalog discovery is a separate authenticated read-only shape and bounded request", async () => {
  const response = { ok: true, schemaVersion: "short-live-test-catalog-v1", catalog: [], readOnly: true, authorityGranted: false };
  const requests = [];
  globalThis.__shortLiveApiTransport = async (...args) => { requests.push(args); return response; };
  assert.deepEqual(await api.getShortLiveTestCatalog(), response);
  assert.equal(requests[0][0], "/api/short-live-tests/catalog");
  assert.equal(requests[0][1].method || "GET", "GET");
  assert.equal(requests[0][1].timeoutMs, 45000);
  for (const value of [good(), { ...response, authorityGranted: true }, { ...response, readOnly: false }, { ...response, catalog: null }]) {
    globalThis.__shortLiveApiTransport = async () => value;
    await assert.rejects(api.getShortLiveTestCatalog(), /SHORT_LIVE_CATALOG_UNVERIFIED/);
  }
});

test("dry-run shortcut uses the existing confirmed flag flow and verifies actual false", async () => {
  const calls = [];
  globalThis.__shortLiveFlagTransport = async (...args) => { calls.push(args); return { ok: true, snapshot: { dry_run: false } }; };
  globalThis.__shortLiveApiTransport = async () => { throw new Error("must not prepare or start"); };
  await api.releaseShortLiveDryRun();
  assert.deepEqual(calls, [["dry_run", false, true]]);
  for (const result of [{ok:false}, {ok:true}, {ok:true,snapshot:{dry_run:true}}, {ok:true,snapshot:{dry_run:"false"}}]) {
    globalThis.__shortLiveFlagTransport = async () => result;
    await assert.rejects(api.releaseShortLiveDryRun(), /해제 결과를 확인하지 못/);
  }
  let count = 0;
  globalThis.__shortLiveFlagTransport = async () => { count++; throw new Error("cancelled or lost reply"); };
  await assert.rejects(api.releaseShortLiveDryRun(), /cancelled or lost reply/);
  assert.equal(count, 1);
});
