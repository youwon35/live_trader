import assert from "node:assert/strict";

import {
  ACCOUNT_REFRESH_INTERVAL_MS,
  createAccountRefreshCoordinator,
} from "../src/accountRefresh.js";

assert.equal(ACCOUNT_REFRESH_INTERVAL_MS, 10_000);

const order = [];
let releaseSync;
const syncGate = new Promise((resolve) => {
  releaseSync = resolve;
});
let syncCalls = 0;
let reconciliationCalls = 0;
const coordinator = createAccountRefreshCoordinator({
  syncExecutionEvents: async (brokerId) => {
    syncCalls += 1;
    order.push(`sync:${brokerId}`);
    await syncGate;
  },
  runReconciliation: async () => {
    reconciliationCalls += 1;
    order.push("reconcile");
    return { ok: true, snapshot: { reconciled: true } };
  },
});

// A 10-second timer tick and a simultaneous manual click must share one request.
const automaticTimerTick = coordinator.run();
const manualRefreshDuringTimerTick = coordinator.run();
assert.strictEqual(automaticTimerTick, manualRefreshDuringTimerTick);
assert.equal(coordinator.isRunning(), true);
releaseSync();
assert.deepEqual(await automaticTimerTick, { ok: true, snapshot: { reconciled: true } });
assert.deepEqual(order, ["sync:all", "reconcile"]);
assert.equal(syncCalls, 1);
assert.equal(reconciliationCalls, 1);
assert.equal(coordinator.isRunning(), false);

// A later manual refresh starts one new sync and one new reconciliation.
await coordinator.run();
assert.equal(syncCalls, 2);
assert.equal(reconciliationCalls, 2);

let failedSyncReconciliationCalls = 0;
const syncFailureStillReconciles = createAccountRefreshCoordinator({
  syncExecutionEvents: async () => {
    throw new Error("temporary sync failure");
  },
  runReconciliation: async () => {
    failedSyncReconciliationCalls += 1;
    return { ok: true, snapshot: { reconciled: true } };
  },
});
const syncFailureResult = await syncFailureStillReconciles.run();
assert.equal(failedSyncReconciliationCalls, 1);
assert.equal(syncFailureResult.ok, true);
assert.equal(syncFailureResult.snapshot.reconciled, true);
assert.equal(syncFailureResult.syncWarning, "체결 동기화에 실패했지만 계좌·포지션 대조는 실행했습니다.");

let reconciliationShouldFail = true;
const recoverable = createAccountRefreshCoordinator({
  syncExecutionEvents: async () => {},
  runReconciliation: async () => {
    if (reconciliationShouldFail) throw new Error("temporary reconciliation failure");
    return { ok: true };
  },
});
await assert.rejects(recoverable.run(), /temporary reconciliation failure/);
assert.equal(recoverable.isRunning(), false);
reconciliationShouldFail = false;
assert.deepEqual(await recoverable.run(), { ok: true });

console.log("live account refresh/reconciliation regression checks passed");
