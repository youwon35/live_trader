import assert from "node:assert/strict";
import test from "node:test";
import {
  ordinaryExecutionView,
  recordedReconciliation,
  verifiedCanaryExecution,
} from "../src/executionAvailability.js";

// Pure display-model tests: no server, account, broker or filesystem mutation.
const executionSnapshot = (dispatch = false) => ({
  api_connected: true,
  mode: "MONITOR",
  dry_run: true,
  kill_switch: true,
  new_entries_blocked: true,
  execution_availability: {
    schemaVersion: "live-execution-availability-v1",
    authorizationGranted: false,
    ordinaryContinuous: {
      monitorSupported: true,
      liveDispatchAvailable: dispatch,
      detail: "일반 연속 운용의 실주문 전송 경로가 잠겨 있습니다.",
      nextAction: "관찰 모드에서 상태를 점검하세요.",
    },
  },
});
const summary = {
  last_run: "2026-09-06T01:00:00.000Z",
  account_count: 1,
  api_required_count: 0,
  error_count: 0,
  mismatch_count: 0,
  status: "pass",
};
const reconciliationSnapshot = (changes = {}, errors = []) => ({
  api_connected: true,
  reconciliation: { summary: { ...summary, ...changes }, errors },
});
const canaryStrategy = (changes = {}) => ({
  canary_execution: {
    verified: true,
    scope: { eligible: true },
    successful: 3,
    blocked: 0,
    ...changes,
  },
});

test("mode, dry-run and safety switches cannot manufacture execution capability on an old server", () => {
  for (const mode of ["MONITOR", "SMALL_LIVE", "FULL_LIVE"]) {
    const view = ordinaryExecutionView({
      api_connected: true, mode, dry_run: false, kill_switch: false,
      new_entries_blocked: false, operator_confirmed: true,
      launch_report: { launch_locked: false }, real_orders_enabled: true,
    });
    assert.equal(view.known, false, mode);
    assert.equal(view.monitorSupported, false, mode);
    assert.equal(view.liveDispatchAvailable, false, mode);
    assert.match(view.detail, /확인하지 못했습니다/);
  }
});

test("missing, unknown-version or authorization-granting metadata cannot unlock the display", () => {
  const reference = executionSnapshot().execution_availability;
  for (const availability of [
    undefined,
    null,
    {},
    { ...reference, schemaVersion: "live-execution-availability-v2" },
    { ...reference, authorizationGranted: undefined },
    { ...reference, authorizationGranted: true },
    { ...reference, ordinaryContinuous: {} },
    { ...reference, ordinaryContinuous: { monitorSupported: "true", liveDispatchAvailable: true } },
    { ...reference, ordinaryContinuous: { monitorSupported: true, liveDispatchAvailable: 1 } },
  ]) {
    const view = ordinaryExecutionView({ api_connected: true, execution_availability: availability });
    assert.equal(view.known, false);
    assert.equal(view.liveDispatchAvailable, false);
  }
});

test("disconnect discards otherwise valid advertised capability", () => {
  for (const connected of [false, undefined, "true", 1]) {
    const view = ordinaryExecutionView({ ...executionSnapshot(true), api_connected: connected });
    assert.equal(view.known, false);
    assert.equal(view.monitorSupported, false);
    assert.equal(view.liveDispatchAvailable, false);
  }
});

test("current locked runtime allows monitoring and preserves the server's next action", () => {
  const snapshot = executionSnapshot();
  const view = ordinaryExecutionView(snapshot);
  assert.equal(view.known, true);
  assert.equal(view.monitorSupported, true);
  assert.equal(view.liveDispatchAvailable, false);
  assert.equal(view.detail, snapshot.execution_availability.ordinaryContinuous.detail);
  assert.equal(view.nextAction, snapshot.execution_availability.ordinaryContinuous.nextAction);
});

test("static capability is independent of selected mode and grants no per-order authorization", () => {
  const snapshot = executionSnapshot(true);
  const locked = ordinaryExecutionView(snapshot);
  const requestedLive = ordinaryExecutionView({
    ...snapshot, mode: "FULL_LIVE", dry_run: false,
    kill_switch: false, new_entries_blocked: false,
  });
  assert.equal(locked.known, true);
  assert.equal(locked.liveDispatchAvailable, true);
  assert.deepEqual(requestedLive, locked, "display capability cannot change when an operator merely selects a mode");
  assert.notEqual(locked.authorizationGranted, true);
  assert.notEqual(locked.canSubmit, true);
});

test("canary display accepts only an explicitly verified eligible current-scope summary", () => {
  assert.deepEqual(verifiedCanaryExecution(canaryStrategy()), { successful: 3, blocked: 0, verified: true });
  for (const strategy of [
    {},
    { canary_execution: null },
    canaryStrategy({ verified: false }),
    canaryStrategy({ verified: "true" }),
    canaryStrategy({ scope: undefined }),
    canaryStrategy({ scope: {} }),
    canaryStrategy({ scope: { eligible: false } }),
    canaryStrategy({ scope: { eligible: "true" } }),
    { orders: [{ mode: "SMALL_LIVE", state: "filled", canary_scope: { eligible: true } }] },
  ]) {
    assert.deepEqual(verifiedCanaryExecution(strategy), { successful: 0, blocked: 0, verified: false });
  }
});

test("canary missing, negative, fractional, non-finite and string counts are never verified", () => {
  for (const field of ["successful", "blocked"]) {
    for (const value of [undefined, null, NaN, Infinity, -Infinity, -1, 0.5, "3", "", true, Number.MAX_SAFE_INTEGER + 1]) {
      const result = verifiedCanaryExecution(canaryStrategy({ [field]: value }));
      assert.equal(result.verified, false, `${field}=${String(value)}`);
      assert.equal(result.successful, 0);
      assert.equal(result.blocked, 0);
    }
  }
});

test("reconciliation requires a recorded account observation and connection", () => {
  assert.deepEqual(recordedReconciliation(reconciliationSnapshot()), {
    observed: true, blocked: false, verified: true,
  });
  for (const snapshot of [
    {},
    { api_connected: true },
    reconciliationSnapshot({ last_run: undefined }),
    reconciliationSnapshot({ last_run: "" }),
    reconciliationSnapshot({ last_run: "미실행" }),
    reconciliationSnapshot({ last_run: "not-a-date" }),
    reconciliationSnapshot({ account_count: 0 }),
    { ...reconciliationSnapshot(), api_connected: false },
  ]) {
    assert.equal(recordedReconciliation(snapshot).verified, false);
  }
});

test("reconciliation known failures and absent final PASS cannot look verified", () => {
  for (const snapshot of [
    reconciliationSnapshot({ error_count: 1 }),
    reconciliationSnapshot({ mismatch_count: 1 }),
    reconciliationSnapshot({ api_required_count: 1 }),
    reconciliationSnapshot({}, [{ detail: "계좌 조회 실패" }]),
    reconciliationSnapshot({ status: "error" }),
    reconciliationSnapshot({ status: "blocked" }),
    reconciliationSnapshot({ status: "unknown" }),
    reconciliationSnapshot({ status: undefined }),
  ]) {
    assert.equal(recordedReconciliation(snapshot).verified, false);
  }
});

for (const field of ["account_count", "api_required_count", "error_count", "mismatch_count"]) {
  test(`reconciliation invalid or missing ${field} cannot be treated as zero errors`, () => {
    for (const value of [undefined, null, NaN, Infinity, -Infinity, -1, 0.5, "", "unknown", true, Number.MAX_SAFE_INTEGER + 1]) {
      assert.equal(
        recordedReconciliation(reconciliationSnapshot({ [field]: value })).verified,
        false,
        `${field}=${String(value)}`,
      );
    }
  });
}

test("malformed reconciliation errors cannot erase evidence of failure", () => {
  for (const errors of [{}, { detail: "계좌 오류" }, "error", 1]) {
    assert.equal(recordedReconciliation(reconciliationSnapshot({}, errors)).verified, false);
  }
});
