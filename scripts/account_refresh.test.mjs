import assert from "node:assert/strict";

import {
  ACCOUNT_REFRESH_INTERVAL_MS,
  createAccountRefreshCoordinator,
} from "../src/accountRefresh.js";

assert.equal(ACCOUNT_REFRESH_INTERVAL_MS, 10_000);

function successfulResult(extraSnapshot = {}) {
  return {
    ok: true,
    snapshot: {
      reconciliation: {
        summary: {
          status: "pass", status_label: "정상", account_count: 1,
          api_required_count: 0, error_count: 0, mismatch_count: 0,
          last_run: "2026-09-06 10:00:00",
        },
      },
      ...extraSnapshot,
    },
  };
}

const order = [];
const syncOptions = [];
const reconciliationOptions = [];
let releaseSync;
const syncGate = new Promise((resolve) => {
  releaseSync = resolve;
});
let syncCalls = 0;
let reconciliationCalls = 0;
const coordinator = createAccountRefreshCoordinator({
  syncExecutionEvents: async (brokerId, options) => {
    syncCalls += 1;
    syncOptions.push(options);
    order.push(`sync:${brokerId}`);
    await syncGate;
    return { ok: true };
  },
  runReconciliation: async (options) => {
    reconciliationCalls += 1;
    reconciliationOptions.push(options);
    order.push("reconcile");
    return successfulResult({ reconciled: true });
  },
});

// A timer tick and simultaneous manual click share one completed refresh.
const automaticTimerTick = coordinator.run();
const manualRefreshDuringTimerTick = coordinator.run();
assert.strictEqual(automaticTimerTick, manualRefreshDuringTimerTick);
assert.equal(coordinator.isRunning(), true);
releaseSync();
assert.deepEqual(await automaticTimerTick, successfulResult({ reconciled: true }));
assert.deepEqual(order, ["sync:all", "reconcile"]);
assert.equal(syncCalls, 1);
assert.equal(reconciliationCalls, 1);
assert.deepEqual(syncOptions[0], { forceSnapshot: true, includeSnapshot: false });
assert.deepEqual(reconciliationOptions[0], { refreshBrokers: false, includeSnapshot: true });
assert.equal(coordinator.isRunning(), false);

await coordinator.run();
assert.equal(syncCalls, 2);
assert.equal(reconciliationCalls, 2);

// Transport failures, HTTP-200 failures, and invalid responses cannot reuse
// the old broker cache while giving its reconciliation a fresh timestamp.
const failedSynchronizations = [
  async () => { throw new Error("temporary sync failure"); },
  async () => ({ ok: false, errors: [{ broker_id: "kis" }] }),
  async () => undefined,
  async () => null,
  async () => [],
  async () => ({ ok: "true" }),
];
for (const syncExecutionEvents of failedSynchronizations) {
  const requests = [];
  const refreshing = createAccountRefreshCoordinator({
    syncExecutionEvents,
    runReconciliation: async (options) => {
      requests.push(options);
      assert.equal(options.refreshBrokers, true, "a failed sync cannot republish the stale PASS cache");
      return successfulResult({ observation: "fresh-broker-read" });
    },
  });
  const result = await refreshing.run();
  assert.deepEqual(requests, [{ refreshBrokers: true, includeSnapshot: true }]);
  assert.equal(result.snapshot.observation, "fresh-broker-read");
  assert.match(result.syncWarning, /새로 조회해 대조/);
  assert.equal(refreshing.isRunning(), false);
}

// A failed fallback never turns the last cached account state into success.
const failedFallback = createAccountRefreshCoordinator({
  syncExecutionEvents: async () => { throw new Error("sync unavailable"); },
  runReconciliation: async (options) => {
    assert.equal(options.refreshBrokers, true);
    throw new Error("broker refresh unavailable");
  },
});
await assert.rejects(failedFallback.run(), /broker refresh unavailable/);
assert.equal(failedFallback.isRunning(), false);

// Explicit backend failure retains its reason and diagnostic snapshot.
const rejectedResult = {
  ok: false,
  reason: "현재 계좌 대조 권한을 확인하지 못했습니다.",
  snapshot: { reconciliation: { summary: { status: "blocked", error_count: 1 } } },
};
for (const syncOk of [true, false]) {
  const rejected = createAccountRefreshCoordinator({
    syncExecutionEvents: async () => ({ ok: syncOk }),
    runReconciliation: async (options) => {
      assert.equal(options.refreshBrokers, !syncOk);
      return rejectedResult;
    },
  });
  const result = await rejected.run();
  assert.equal(result.ok, false);
  assert.equal(result.reason, rejectedResult.reason);
  assert.strictEqual(result.snapshot, rejectedResult.snapshot);
  if (!syncOk) {
    assert.match(result.syncWarning, /완료하지 못했습니다/);
    assert.doesNotMatch(result.syncWarning, /조회해 대조했습니다/);
  } else {
    assert.strictEqual(result, rejectedResult);
  }
  assert.equal(rejected.isRunning(), false);
}

// A missing/malformed final snapshot must not reach either UI refresh path.
// The local guard must unlock, allowing the next valid response to recover.
const malformedReconciliations = [
  undefined, null, [], {}, { ok: true },
  { ok: "true", snapshot: successfulResult().snapshot },
  { ok: false, reason: "failure without snapshot" },
  { ok: true, snapshot: null },
  { ok: true, snapshot: [] },
  { ok: true, snapshot: {} },
  { ok: true, snapshot: { reconciliation: null } },
  { ok: true, snapshot: { reconciliation: [] } },
  { ok: true, snapshot: { reconciliation: {} } },
  { ok: true, snapshot: { reconciliation: { summary: null } } },
  { ok: true, snapshot: { reconciliation: { summary: [] } } },
];
for (const invalid of malformedReconciliations) {
  for (const syncOk of [true, false]) {
    let response = invalid;
    const retryable = createAccountRefreshCoordinator({
      syncExecutionEvents: async () => ({ ok: syncOk }),
      runReconciliation: async () => response,
    });
    await assert.rejects(retryable.run(), /대조 응답을 확인하지 못했습니다/);
    assert.equal(retryable.isRunning(), false);
    response = successfulResult();
    assert.deepEqual((await retryable.run()).snapshot, response.snapshot);
    assert.equal(retryable.isRunning(), false);
  }
}

// A coalesced server response means the first poll has not completed yet.
// Do not launch a duplicate read or mark its cached reconciliation fresh.
let coalesced = true;
let completedReconciliations = 0;
const sharedServerPoll = createAccountRefreshCoordinator({
  syncExecutionEvents: async () => ({ ok: true, coalesced }),
  runReconciliation: async (options) => {
    completedReconciliations += 1;
    assert.equal(options.refreshBrokers, false);
    return successfulResult();
  },
});
await assert.rejects(sharedServerPoll.run(), /동기화가 진행 중/);
assert.equal(completedReconciliations, 0);
assert.equal(sharedServerPoll.isRunning(), false);
coalesced = false;
assert.deepEqual(await sharedServerPoll.run(), successfulResult());
assert.equal(completedReconciliations, 1);

// Reconciliation failure releases the local guard so a later retry can run.
let reconciliationShouldFail = true;
const recoverable = createAccountRefreshCoordinator({
  syncExecutionEvents: async () => ({ ok: true }),
  runReconciliation: async () => {
    if (reconciliationShouldFail) throw new Error("temporary reconciliation failure");
    return successfulResult();
  },
});
await assert.rejects(recoverable.run(), /temporary reconciliation failure/);
assert.equal(recoverable.isRunning(), false);
reconciliationShouldFail = false;
assert.deepEqual(await recoverable.run(), successfulResult());

console.log("live account refresh/reconciliation regression checks passed");