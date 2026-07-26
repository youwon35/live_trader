export const SNAPSHOT_POLL_ACTIVE_MS = 15_000;
export const SNAPSHOT_POLL_IDLE_MS = 60_000;
export const EXECUTION_POLL_ACTIVE_MS = 30_000;
export const EXECUTION_POLL_IDLE_MS = 300_000;

export function livePollingIntervals(runtimeActive) {
  return runtimeActive
    ? {
        snapshotMs: SNAPSHOT_POLL_ACTIVE_MS,
        executionMs: EXECUTION_POLL_ACTIVE_MS,
      }
    : {
        snapshotMs: SNAPSHOT_POLL_IDLE_MS,
        executionMs: EXECUTION_POLL_IDLE_MS,
      };
}
