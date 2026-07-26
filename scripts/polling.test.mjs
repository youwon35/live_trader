import assert from "node:assert/strict";

import {
  EXECUTION_POLL_ACTIVE_MS,
  EXECUTION_POLL_IDLE_MS,
  SNAPSHOT_POLL_ACTIVE_MS,
  SNAPSHOT_POLL_IDLE_MS,
  livePollingIntervals,
} from "../src/polling.js";

assert.deepEqual(livePollingIntervals(true), {
  snapshotMs: SNAPSHOT_POLL_ACTIVE_MS,
  executionMs: EXECUTION_POLL_ACTIVE_MS,
});
assert.deepEqual(livePollingIntervals(false), {
  snapshotMs: SNAPSHOT_POLL_IDLE_MS,
  executionMs: EXECUTION_POLL_IDLE_MS,
});
assert.ok(SNAPSHOT_POLL_IDLE_MS > SNAPSHOT_POLL_ACTIVE_MS);
assert.ok(EXECUTION_POLL_IDLE_MS > EXECUTION_POLL_ACTIVE_MS);
assert.equal(EXECUTION_POLL_ACTIVE_MS, 30_000);
assert.equal(EXECUTION_POLL_IDLE_MS, 300_000);

console.log("live polling interval regression checks passed");
