import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

const source = process.argv[2] || new URL("../src/kisStopCleanup.js", import.meta.url);
const { kisStopCleanupView, kisCleanupBlocksStart, kisManualStartPathSupported } = await import(
  "data:text/javascript;base64," + Buffer.from(await readFile(source, "utf8")).toString("base64"));
const clean = () => ({ kis_scoped_cleanup: { schemaVersion: "kis-scoped-owned-stop-cleanup/v1", status: "CLEAN", scopes: [] } });
const row = () => ({ runtimeScopeId: "stock:account:usd", sessionId: "session", deploymentId: "deployment",
  status: "RECONCILIATION_REQUIRED", requiresReconciliation: true, placementBlocked: true,
  remainingOrderCount: 2, reason: "terminal confirmation pending" });
const dirty = (value = row()) => ({ kis_scoped_cleanup: { schemaVersion: "kis-scoped-owned-stop-cleanup/v1",
  status: "RECONCILIATION_REQUIRED", scopes: [value] } });

assert.equal(kisStopCleanupView(clean()).blocked, false);
assert.equal(kisStopCleanupView({}).blocked, true);
assert.equal(kisStopCleanupView({ kis_scoped_cleanup: { ...clean().kis_scoped_cleanup, schemaVersion: "other" } }).blocked, true);
assert.equal(kisStopCleanupView(dirty()).scopes[0].remainingOrderCount, 2);
assert.equal(kisStopCleanupView(dirty({ ...row(), remainingOrderCount: 0 })).scopes[0].remainingOrderCount, null);
assert.equal(kisStopCleanupView(dirty({ ...row(), remainingOrderCount: NaN })).scopes[0].remainingOrderCount, null);
assert.equal(kisStopCleanupView(dirty({ ...row(), deploymentId: "" })).verified, false);
assert.equal(kisStopCleanupView(dirty({ ...row(), runtimeScopeId: "bad\ncontext" })).scopes.length, 0);
assert.equal(kisStopCleanupView(dirty({ ...row(), status: "CLEAN" })).blocked, true);
assert.equal(kisStopCleanupView(dirty({ ...row(), status: "CLEAN", requiresReconciliation: false, placementBlocked: false })).blocked, true);
assert.equal(kisStopCleanupView({ kis_scoped_cleanup: { ...clean().kis_scoped_cleanup, scopes: [row()] } }).blocked, true);
assert.equal(kisStopCleanupView(clean()).verified, true);
const pending = kisStopCleanupView(dirty());
assert.equal(kisCleanupBlocksStart(pending, "stock", "SMALL_LIVE"), true);
assert.equal(kisCleanupBlocksStart(pending, "stock", "FULL_LIVE"), true);
assert.equal(kisCleanupBlocksStart(pending, "stock", "MONITOR"), false);
assert.equal(kisCleanupBlocksStart(pending, "crypto", "SMALL_LIVE"), false);
assert.equal(kisCleanupBlocksStart(pending, "crypto", "FULL_LIVE"), false);
assert.equal(kisCleanupBlocksStart(kisStopCleanupView(clean()), "stock", "FULL_LIVE"), false);
const support = { kis_manual_start_support: { schemaVersion: "kis-scoped-manual-start-support/v1",
  supported: true, requiresNativeConfirmation: true, ordinaryRoutesRemainClosed: true, orderAuthorityGranted: false } };
assert.equal(kisManualStartPathSupported(support, "stock", "kis", "SMALL_LIVE"), true);
assert.equal(kisManualStartPathSupported(support, "stock", "kis", "FULL_LIVE"), true);
assert.equal(kisManualStartPathSupported({}, "stock", "kis", "SMALL_LIVE"), false);
assert.equal(kisManualStartPathSupported(support, "crypto", "kis", "SMALL_LIVE"), false);
assert.equal(kisManualStartPathSupported(support, "stock", "upbit", "SMALL_LIVE"), false);
assert.equal(kisManualStartPathSupported(support, "stock", "kis", "MONITOR"), false);
for (const key of ["requiresNativeConfirmation", "ordinaryRoutesRemainClosed"]) {
  assert.equal(kisManualStartPathSupported({ kis_manual_start_support: { ...support.kis_manual_start_support, [key]: false } }, "stock", "kis", "SMALL_LIVE"), false);
}
assert.equal(kisManualStartPathSupported({ kis_manual_start_support: { ...support.kis_manual_start_support, orderAuthorityGranted: true } }, "stock", "kis", "SMALL_LIVE"), false);
console.log("KIS STOP cleanup and protected manual path UI: 27 cases PASS; no transport or native confirmation.");
