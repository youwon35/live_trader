export const ACCOUNT_REFRESH_INTERVAL_MS = 10_000;

export function createAccountRefreshCoordinator({
  syncExecutionEvents,
  runReconciliation,
}) {
  if (typeof syncExecutionEvents !== "function" || typeof runReconciliation !== "function") {
    throw new TypeError("계좌 갱신 coordinator에는 동기화와 대조 함수가 필요합니다.");
  }

  let inFlight = null;

  return {
    isRunning() {
      return inFlight !== null;
    },
    run() {
      if (inFlight) return inFlight;

      inFlight = Promise.resolve()
        .then(async () => {
          let syncWarning = "";
          let synchronization = null;
          try {
            synchronization = await syncExecutionEvents("all", {
              forceSnapshot: true,
              includeSnapshot: false,
            });
            if (!synchronization || typeof synchronization !== "object"
              || Array.isArray(synchronization) || synchronization.ok !== true) {
              throw new Error("체결 동기화 결과를 확인하지 못했습니다.");
            }
          } catch {
            syncWarning = "체결 동기화를 확인하지 못했습니다.";
          }
          if (!syncWarning && synchronization.coalesced === true) {
            // Another poll is still fetching. Its cached response is not a
            // completed broker observation and must not refresh the PASS time.
            throw new Error("계좌·체결 동기화가 진행 중입니다. 완료 후 다시 확인하세요.");
          }
          // Reuse the cache only after a confirmed successful refresh. A
          // failed sync must fetch broker truth before publishing a new result.
          const reconciled = await runReconciliation({
            refreshBrokers: Boolean(syncWarning),
            includeSnapshot: true,
          });
          const record = (value) => value !== null && typeof value === "object" && !Array.isArray(value);
          if (!record(reconciled) || typeof reconciled.ok !== "boolean"
            || !record(reconciled.snapshot)
            || !record(reconciled.snapshot.reconciliation)
            || !record(reconciled.snapshot.reconciliation.summary)) {
            throw new Error("계좌·포지션 대조 응답을 확인하지 못했습니다. 다시 조회하세요.");
          }
          if (syncWarning) {
            syncWarning = reconciled.ok
              ? "체결 동기화를 확인하지 못해 계좌·포지션을 새로 조회해 대조했습니다."
              : "체결 동기화를 확인하지 못했고 계좌·포지션 대조도 완료하지 못했습니다.";
          }
          return syncWarning ? { ...reconciled, syncWarning } : reconciled;
        })
        .finally(() => {
          inFlight = null;
        });
      return inFlight;
    },
  };
}