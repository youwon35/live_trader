// Display classification only. Raw reconciliation, ledger and execution guards
// remain authoritative and unchanged. A balance delta is never a cash-flow receipt.
import { ordinaryRuntimeActive, ordinaryRuntimeKnown } from './statusTruth.js';

const rows = value => Array.isArray(value) ? value : [];
const record = value => value && typeof value === 'object' && !Array.isArray(value) ? value : {};
const number = value => typeof value === 'number' && Number.isFinite(value);
const count = value => Number.isSafeInteger(value) && value >= 0;
const time = value => {
  if (typeof value !== 'string') return null;
  const fields=value.match(/^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})?$/);
  if (!fields) return null;
  const calendar=new Date(Date.UTC(...fields.slice(1).map(Number).map((part,index)=>index===1 ? part-1 : part)));
  const valueHead=value.slice(0,19).replace(' ','T');
  return Number.isFinite(calendar.getTime()) && calendar.toISOString().slice(0,19) === valueHead && Number.isFinite(Date.parse(value)) ? Date.parse(value) : null;
};
const terminalOrders = new Set(['FILLED','CANCELED','CANCELLED','REJECTED','EXPIRED','FAILED','RISK_REJECTED','RISK_BLOCKED','DRY_RUN']);

export function reconciliationDisplayIdle(snapshot = {}) {
  if (!ordinaryRuntimeKnown(snapshot) || ordinaryRuntimeActive(snapshot)) return false;
  const runtime = record(snapshot.continuous_runtime);
  const runtimeRows = [...Object.values(record(runtime.runtimes)), ...Object.values(record(runtime.profiles))];
  if (runtimeRows.some(row => !row || row.ambiguous === true || typeof row.running !== 'boolean' || row.running)) return false;
  const short = snapshot.short_live_test_status;
  if (short != null && (typeof short !== 'object' || short.active !== false)) return false;
  if (!Array.isArray(snapshot.orders) || !count(snapshot.order_queue?.active) || snapshot.order_queue.active !== 0) return false;
  return snapshot.orders.every(row => terminalOrders.has(String(row?.state || row?.status || '').toUpperCase()));
}

export function reconciliationReferenceKind(row = {}, kind, snapshot = {}) {
  if (row.status !== 'mismatch' || String(row.comparison_issue || '').trim()) return null;
  if (kind === 'cash') {
    const ledger=record(snapshot.program_ledger),baseline=record(ledger.state);
    if (!ledger.state || typeof ledger.state !== 'object' || Array.isArray(ledger.state)
        || !Object.hasOwn(baseline,'last_baseline') || !Object.hasOwn(baseline,'last_baseline_source')) return null;
    if (baseline.last_baseline != null && baseline.last_baseline !== '' || baseline.last_baseline_source != null && baseline.last_baseline_source !== '') return null;
    const currency=String(row.currency || '').trim().toUpperCase();
    const programCurrency=String(row.program_currency || '').trim().toUpperCase();
    if (row.program_source === 'event_poll' && currency && currency === programCurrency
        && number(row.program_cash_value) && number(row.broker_cash_value)
        && time(row.program_updated_at) !== null && time(snapshot.reconciliation?.summary?.last_run) !== null
        && time(row.program_updated_at) <= time(snapshot.reconciliation.summary.last_run)) return 'HISTORICAL_BALANCE_REFERENCE';
    return null;
  }
  if (kind !== 'position' || !['binance','upbit'].includes(row.broker_id) || row.program_source !== 'missing'
      || row.program_qty_value !== 0 || !number(row.broker_qty_value) || row.broker_qty_value <= 0) return null;
  const ledger=record(snapshot.program_ledger);
  // A fill-bearing or incompletely classified ledger cannot establish that a
  // broker-only holding is outside this app's managed position scope.
  if (ledger.fill_event_count !== 0 || ledger.other_event_count !== 0
      || !count(ledger.snapshot_event_count) || ledger.snapshot_event_count === 0) return null;
  const positions=rows(ledger.positions);
  if (positions.some(item => item?.broker_id === row.broker_id && item?.symbol === row.symbol)) return null;
  if (positions.some(item => item?.source !== 'event_poll')) return null;
  return 'UNMANAGED_SPOT_INVENTORY';
}

export function reconciliationReferenceView(snapshot = {}) {
  const reconciliation=record(snapshot.reconciliation),summary=record(reconciliation.summary);
  const input=[...rows(reconciliation.accounts).map(row=>({row,kind:'cash'})),...rows(reconciliation.positions).map(row=>({row,kind:'position'}))];
  const rawMismatchCount=input.filter(({row})=>row?.status === 'mismatch').length;
  const rawApiRequired=input.filter(({row})=>row?.status === 'api_required').length;
  const rawCapabilityGap=input.filter(({row})=>row?.status === 'capability_unavailable').length;
  const errors=rows(reconciliation.errors);
  const countsVerified=Array.isArray(reconciliation.accounts) && Array.isArray(reconciliation.positions)
    && Array.isArray(reconciliation.errors) && count(summary.account_count) && count(summary.position_count)
    && summary.account_count === reconciliation.accounts.length && summary.position_count === reconciliation.positions.length
    && count(summary.mismatch_count) && summary.mismatch_count === rawMismatchCount
    && count(summary.api_required_count) && summary.api_required_count === rawApiRequired
    && count(summary.capability_gap_count) && summary.capability_gap_count === rawCapabilityGap
    && count(summary.error_count) && summary.error_count === errors.length
    && count(summary.blocking_count) && summary.blocking_count === rawMismatchCount + rawApiRequired
    && input.every(({row})=>['pass','mismatch','api_required','capability_unavailable'].includes(row?.status));
  const classified=input.map(({row,kind})=>({row,kind,referenceKind:reconciliationReferenceKind(row,kind,snapshot)}));
  const referenceRows=classified.filter(item=>item.referenceKind);
  const actionableRows=classified.filter(({row,referenceKind})=>!referenceKind && ['mismatch','api_required'].includes(row?.status));
  const idle=reconciliationDisplayIdle(snapshot);
  const referenceOnly=countsVerified && snapshot.api_connected === true && idle && errors.length === 0
    && summary.status === 'fail' && rawApiRequired === 0 && rawCapabilityGap === 0 && rawMismatchCount > 0 && referenceRows.length === rawMismatchCount;
  return { countsVerified, idle, classificationEnabled: countsVerified && snapshot.api_connected === true && idle, referenceOnly, rawMismatchCount,
    referenceCount:referenceRows.length, actionableCount:actionableRows.length,
    referenceRows, actionableRows, executionAuthorityGranted:false, cashFlowCauseVerified:false,
    detail:'입출금·수동 거래·수수료 등은 잔고 차이만으로 구분할 수 없습니다. 과거 조회 기준은 현재 운용 손익·체결 원장과 구분하며, 실제 실행 검사는 해당 회차의 원래 근거를 사용합니다.' };
}
