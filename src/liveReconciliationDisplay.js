import { reconciliationReferenceView, reconciliationReferenceKind } from './reconciliationReferenceView.js';
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
  const reference = reconciliationReferenceView(snapshot);
  const rawStatus = failed ? "fail" : brokerKnown ? summary.status || "warn" : "warn";
  return {
    brokerKnown,
    status: reference.referenceOnly ? "info" : rawStatus,
    label: reference.referenceOnly ? "과거 기준 차이 · 참고" : failed ? summary.status_label || "대조 실패" : brokerKnown ? summary.status_label || "확인 필요" : "대조 미확인",
    rawStatus,
    referenceCount: reference.classificationEnabled ? reference.referenceCount : 0,
    actionableCount: reference.classificationEnabled ? reference.actionableCount : null,
    referenceOnly: reference.referenceOnly,
    detail: reference.referenceOnly ? "과거 조회 기준과 현재 잔고의 변화입니다. 현재 운용 손익·주문·포지션 대조 통과를 뜻하지 않습니다." : "",
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
  const referenceOnly = reconciliationReferenceView(snapshot).referenceOnly;
  return {
    status: referenceOnly ? "info" : "warn",
    value: !exists ? "원장 없음" : legacy && referenceOnly ? "과거 조회 참고" : legacy ? "과거 잔고 기준" : baselineRecorded ? "기준 기록 있음" : "기준 미확인",
    detail: `${countText}${historyText}${otherText} · 원장 변경 ${updated || "미확인"} · ${referenceOnly ? "과거 조회 참고 · 현재 운용 원장과 별도" : baselineRecorded ? `기준 기록 ${baseline}` : "기준 승인 근거 미확인"}`,
    baselineRecorded, legacy, fillCount, snapshotCount, otherCount,
  };
}

// Explanations use only observed mismatches. They never reconcile a delta,
// approve a baseline, or attribute it to fees/transfers without evidence.
export function liveReconciliationIssues(snapshot = {}) {
  const reconciliation = record(snapshot.reconciliation);
  const ledger = record(snapshot.program_ledger);
  const findLedger = (row, kind) => {
    const matches = rows(kind === "cash" ? ledger.cash : ledger.positions).filter((item) =>
      item?.broker_id === row.broker_id && (kind === "cash" ? item.currency === row.currency : item.symbol === row.symbol && String(item.position_side || "") === String(row.position_side || "")));
    return matches.length === 1 ? matches[0] : undefined;
  };
  const referenceView = reconciliationReferenceView(snapshot);
  const issues = [];
  for (const [kind, items] of [["cash", reconciliation.accounts], ["position", reconciliation.positions]]) {
    for (const row of rows(items)) {
      if (!["mismatch", "api_required"].includes(row?.status)) continue;
      const observedCurrency = String(row.currency || "").trim().toUpperCase();
      const programCurrency = String(row.program_currency || "").trim().toUpperCase();
      const currencyChanged = kind === "cash" && observedCurrency && programCurrency && observedCurrency !== programCurrency;
      const reportedIssue = typeof row.comparison_issue === "string" ? row.comparison_issue.trim() : "";
      const comparisonIssue = reportedIssue || (currencyChanged ? "account_scope_changed" : "");
      const ambiguousScope = comparisonIssue === "ambiguous_account_scope";
      const ledgerRow = comparisonIssue ? undefined : findLedger(row, kind);
      const source = row.program_source || ledgerRow?.source;
      const updatedAt = ambiguousScope ? undefined : row.program_updated_at || ledgerRow?.updated_at;
      let reason = row.status === "api_required" ? "조회 또는 원장 근거 미확인" : kind === "cash" ? "현금 원장과 조회 잔고 차이" : "수량 원장과 조회 보유량 차이";
      let detail = "수수료·입출금·수동 거래 여부는 이 대조 결과만으로 확정할 수 없습니다.";
      if (comparisonIssue) {
        reason = ({ ambiguous_account_scope: "계좌·통화 비교 대상 중복", account_scope_unverified: "계좌·통화 비교 범위 미확인", account_scope_changed: "계좌 또는 통화 범위 변경" })[comparisonIssue] || "계좌·통화 비교 범위 확인";
        const fallbackDetail = ambiguousScope ? "여러 계좌 또는 통화 행이 있어 비교 대상을 확정할 수 없습니다. 원본 범위를 확인해야 합니다."
          : comparisonIssue === "account_scope_changed" ? "이전 기준과 현재 조회의 계좌 또는 통화가 다릅니다. 서로 다른 범위의 금액 차이는 계산하지 않습니다."
            : "이전 기준과 현재 조회가 같은 계좌·통화 범위인지 확인되지 않아 금액 차이를 표시하지 않습니다.";
        detail = reportedIssue && typeof row.detail === "string" && row.detail.trim() ? row.detail.trim() : fallbackDetail;
      } else if (source === "missing") {
        reason = "프로그램 원장에 없는 보유 자산";
        detail = "기존 보유 또는 외부 거래 여부를 확인해야 합니다. 앱이 매수한 자산으로 간주하지 않습니다.";
        if (kind === "cash") { reason = "현금 원장 기준 없음"; detail = "조회 잔고와 비교할 현금 원장 근거가 없습니다."; }
      } else if (source === "event_poll") {
        if (row.status === "mismatch") reason = "과거 잔고 기준과 차이";
        detail = `과거 조회값으로 만든 원장${validTime(updatedAt) ? ` (${updatedAt})` : ""}과 현재 관측값을 비교합니다. 기준 승인·실제 체결 근거는 별도 확인이 필요합니다.`;
      }
      const amount = (value) => typeof value === "number" && Number.isFinite(value) ? value.toLocaleString("ko-KR", { maximumFractionDigits: 12 }) : "미확인";
      const basisValue = source === "missing" && !comparisonIssue ? "없음" : amount(kind === "cash" ? row.program_cash_value : row.program_qty_value);
      const observedValue = amount(kind === "cash" ? row.broker_cash_value : row.broker_qty_value);
      const unit = kind === "cash" ? observedCurrency : row.asset || row.symbol || "";
      const comparison = ambiguousScope ? "이전 기준 미확정 → 거래소 관측 미확정 · 비교 대상 확인 필요"
        : kind === "cash" && comparisonIssue ? `이전 기준 ${basisValue} ${programCurrency || "통화 미확인"} → 거래소 관측 ${observedValue} ${observedCurrency || "통화 미확인"} · ${currencyChanged ? "통화가 달라 금액 차이를 계산하지 않습니다." : "비교 범위 확인 필요"}`
          : `이전 기준 ${basisValue} → 거래소 관측 ${observedValue}${unit ? ` ${unit}` : ""}`;
      const referenceKind = reconciliationReferenceKind(row, kind, snapshot);
      const presentationOnlyReference = referenceView.classificationEnabled && referenceKind !== null;
      if (presentationOnlyReference) {
        reason = referenceKind === 'HISTORICAL_BALANCE_REFERENCE' ? '과거 조회와 잔고 변화 · 참고' : '앱 원장에 없는 기존 보유 · 참고';
        detail = referenceKind === 'HISTORICAL_BALANCE_REFERENCE'
          ? '입출금·수동 거래·수수료 등으로 잔고가 달라질 수 있습니다. 이 차이만으로 원인을 확정하거나 현재 운용 손실로 계산하지 않습니다. 실제 운용의 주문·포지션 대조와 구분합니다.'
          : '조회된 보유분을 앱이 매수한 자산이나 수익으로 간주하지 않습니다. 현재 운용에 자동 편입하지 않으며, 실제 주문·포지션 원장과 구분해 보존합니다.';
      }
      issues.push({ referenceKind, presentationOnlyReference, displayTone: presentationOnlyReference ? 'info' : 'danger', cashFlowCauseVerified: false, kind, brokerId: row.broker_id || "unknown", symbol: row.symbol || "", currency: row.currency || "", reason, detail, comparison,
        baselineAt: validTime(updatedAt) ? updatedAt : null,
        delta: comparisonIssue ? null : kind === "cash" ? row.delta_cash : row.delta_qty, status: row.status, source: source || "unknown" });
    }
  }
  return issues;
}
