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

const rows = (value) => Array.isArray(value) ? value : [];
const record = (value) => value && typeof value === "object" && !Array.isArray(value) ? value : {};
const validTime = (value) => typeof value === "string" && Number.isFinite(Date.parse(value));
const nonnegativeCount = (value) => typeof value === "number" && Number.isInteger(value) && value >= 0 ? value : null;

// Poll loop timestamps are attempts, including skipped and failed reads.
// Only the backend's successful broker observation time proves a recent read.
export function liveExecutionDisplay(snapshot = {}, { nowMs = Date.now(), maxAgeMs = 120_000 } = {}) {
  const execution = record(snapshot.execution_events);
  const streams = Object.values(record(execution.streams?.brokers ?? snapshot.execution_streams?.brokers));
  const connected = streams.filter((item) => item?.connected === true).length;
  const running = streams.some((item) => item?.running === true) || execution.streams?.running === true || snapshot.execution_streams?.running === true;
  const polls = Object.values(record(execution.snapshot_poll?.brokers));
  const failures = polls.filter((item) => item?.last_status === "backoff" || item?.last_error).length + rows(execution.errors).length;
  const recentSuccesses = polls.filter((item) => {
    if (!["changed", "unchanged"].includes(item?.last_status) || !validTime(item?.last_success_at)) return false;
    const age = nowMs - Date.parse(item.last_success_at);
    return Number.isFinite(nowMs) && age >= 0 && age <= maxAgeMs;
  }).length;
  const attempt = validTime(execution.last_poll) ? execution.last_poll : null;
  if (connected) return {
    status: connected === streams.length && !failures ? "pass" : "warn",
    value: connected === streams.length ? "실시간 연결" : "일부 실시간 연결",
    detail: `실시간 연결 ${connected}/${streams.length}곳 · 최근 REST 조회 성공 ${recentSuccesses}곳${failures ? " · 조회 오류 있음" : ""}`,
  };
  if (failures) return {
    status: "warn", value: "조회 오류 확인",
    detail: `실시간 연결 없음 · 최근 REST 조회 성공 ${recentSuccesses}곳 · 마지막 동기화 시도 ${attempt || "미실행"}`,
  };
  if (recentSuccesses) return {
    status: "warn", value: "REST 조회 완료",
    detail: `최근 REST 조회 성공 ${recentSuccesses}곳 · 실시간 체결 감시 ${running ? "연결 대기" : "미시작"}`,
  };
  return {
    status: "warn", value: attempt ? "최근 조회 미확인" : "체결 감시 미시작",
    detail: `실시간 연결 없음 · 마지막 동기화 시도 ${attempt || "미실행"} · 성공 시각 미확인 또는 오래됨`,
  };
}

export function liveLedgerDisplay(snapshot = {}) {
  const ledger = record(snapshot.program_ledger);
  const records = [...rows(ledger.cash), ...rows(ledger.positions)];
  const exists = records.length > 0 || Number(ledger.cash_count) > 0 || Number(ledger.position_count) > 0;
  const legacy = records.some((item) => item?.source === "event_poll");
  const baseline = record(ledger.state).last_baseline;
  const baselineRecorded = validTime(baseline);
  const fillCount = nonnegativeCount(ledger.fill_event_count);
  const snapshotCount = nonnegativeCount(ledger.snapshot_event_count);
  const otherCount = nonnegativeCount(ledger.other_event_count);
  const countText = fillCount === null ? "실제 체결 건수 미확인" : `실제 체결 기록 ${fillCount}건`;
  const historyText = snapshotCount === null ? "" : ` · 과거 잔고 기록 ${snapshotCount}건`;
  const otherText = otherCount ? ` · 기타 상태 기록 ${otherCount}건` : "";
  const updated = [ledger.last_cash_update, ledger.last_position_update].filter(validTime).sort().at(-1);
  return {
    status: "warn",
    value: !exists ? "원장 없음" : legacy ? "과거 잔고 기준" : baselineRecorded ? "기준 기록 있음" : "기준 미확인",
    detail: `${countText}${historyText}${otherText} · 원장 변경 ${updated || "미확인"} · ${baselineRecorded ? `기준 기록 ${baseline}` : "기준 승인 근거 미확인"}`,
    baselineRecorded, legacy, fillCount, snapshotCount, otherCount,
  };
}

// Explanations use only observed mismatches. They never reconcile a delta,
// approve a baseline, or attribute it to fees/transfers without evidence.
export function liveReconciliationIssues(snapshot = {}) {
  const reconciliation = record(snapshot.reconciliation);
  const ledger = record(snapshot.program_ledger);
  const findLedger = (row, kind) => rows(kind === "cash" ? ledger.cash : ledger.positions).find((item) =>
    item?.broker_id === row.broker_id && (kind === "cash" ? item.currency === row.currency : item.symbol === row.symbol && String(item.position_side || "") === String(row.position_side || "")));
  const issues = [];
  for (const [kind, items] of [["cash", reconciliation.accounts], ["position", reconciliation.positions]]) {
    for (const row of rows(items)) {
      if (!["mismatch", "api_required"].includes(row?.status)) continue;
      const source = row.program_source || findLedger(row, kind)?.source;
      const updatedAt = findLedger(row, kind)?.updated_at;
      let reason = row.status === "api_required" ? "조회 또는 원장 근거 미확인" : kind === "cash" ? "현금 원장과 조회 잔고 차이" : "수량 원장과 조회 보유량 차이";
      let detail = "수수료·입출금·수동 거래 여부는 이 대조 결과만으로 확정할 수 없습니다.";
      if (source === "missing") {
        reason = "프로그램 원장에 없는 보유 자산";
        detail = "기존 보유 또는 외부 거래 여부를 확인해야 합니다. 앱이 매수한 자산으로 간주하지 않습니다.";
        if (kind === "cash") { reason = "현금 원장 기준 없음"; detail = "조회 잔고와 비교할 현금 원장 근거가 없습니다."; }
      } else if (source === "event_poll") {
        detail = `과거 조회값으로 만든 원장${validTime(updatedAt) ? ` (${updatedAt})` : ""}과 현재 관측값을 비교합니다. 기준 승인·실제 체결 근거는 별도 확인이 필요합니다.`;
      }
      issues.push({ kind, brokerId: row.broker_id || "unknown", symbol: row.symbol || "", currency: row.currency || "", reason, detail,
        delta: kind === "cash" ? row.delta_cash : row.delta_qty, status: row.status, source: source || "unknown" });
    }
  }
  return issues;
}
