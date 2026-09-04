// Presentation only: an empty/error-free summary is not proof of a broker read.
export function liveReconciliationDisplay(snapshot = {}) {
  const summary = snapshot.reconciliation?.summary ?? {};
  const lastRun = String(summary.last_run ?? "").trim();
  const observed = snapshot.api_connected === true
    && Number.isFinite(Date.parse(lastRun))
    && Number(summary.account_count) > 0;
  const brokerKnown = observed
    && Number(summary.api_required_count) === 0
    && Number(summary.error_count) === 0;
  const failed = Number(summary.mismatch_count) > 0
    || Number(summary.error_count) > 0
    || ["fail", "failed", "blocked", "error"].includes(String(summary.status ?? "").toLowerCase());
  return {
    brokerKnown,
    status: failed ? "fail" : brokerKnown ? summary.status || "warn" : "warn",
    label: failed ? summary.status_label || "대조 실패" : brokerKnown ? summary.status_label || "확인 필요" : "대조 미확인",
  };
}
