// This is a display contract, never an order authorization.
export function ordinaryExecutionView(snapshot = {}) {
  const availability = snapshot.execution_availability;
  const route = availability?.ordinaryContinuous;
  const known = snapshot.api_connected === true
    && availability?.schemaVersion === "live-execution-availability-v1"
    && availability?.authorizationGranted === false
    && typeof route?.monitorSupported === "boolean"
    && typeof route?.liveDispatchAvailable === "boolean";
  return {
    known,
    monitorSupported: known && route.monitorSupported === true,
    liveDispatchAvailable: known && route.liveDispatchAvailable === true,
    detail: known && typeof route.detail === "string"
      ? route.detail : "현재 버전의 실행 가능 범위를 확인하지 못했습니다. 서버 연결과 버전을 확인하세요.",
    nextAction: known && typeof route.nextAction === "string" ? route.nextAction : "",
  };
}

export function verifiedCanaryExecution(strategy = {}) {
  const evidence = strategy?.canary_execution;
  const validCount = (value) => Number.isSafeInteger(value) && value >= 0;
  const verified = evidence?.verified === true
    && evidence?.scope?.eligible === true
    && validCount(evidence.successful) && validCount(evidence.blocked);
  return verified
    ? { successful: evidence.successful, blocked: evidence.blocked, verified: true }
    : { successful: 0, blocked: 0, verified: false };
}

export function recordedReconciliation(snapshot = {}) {
  const summary = snapshot.reconciliation?.summary ?? {};
  const time = Date.parse(summary.last_run || "");
  const countsKnown = ["account_count", "error_count", "mismatch_count", "api_required_count"]
    .every((key) => Number.isSafeInteger(summary[key]) && summary[key] >= 0);
  const errors = snapshot.reconciliation?.errors;
  const observed = countsKnown && Array.isArray(errors)
    && Number.isFinite(time) && summary.account_count > 0;
  const blocked = (Array.isArray(errors) && errors.length > 0)
    || summary.error_count > 0 || summary.mismatch_count > 0;
  const verified = snapshot.api_connected === true && observed && !blocked
    && summary.api_required_count === 0
    && ["pass", "matched", "ready"].includes(String(summary.status || "").toLowerCase());
  return { observed, blocked, verified };
}
