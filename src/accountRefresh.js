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
          try {
            await syncExecutionEvents("all");
          } catch {
            syncWarning = "체결 동기화에 실패했지만 계좌·포지션 대조는 실행했습니다.";
          }
          const reconciled = await runReconciliation();
          return syncWarning && reconciled && typeof reconciled === "object"
            ? { ...reconciled, syncWarning }
            : reconciled;
        })
        .finally(() => {
          inFlight = null;
        });
      return inFlight;
    },
  };
}
