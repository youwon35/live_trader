import assert from 'node:assert/strict';
import test from 'node:test';
import { liveReconciliationDisplay, liveExecutionDisplay, liveLedgerDisplay, liveReconciliationIssues } from '../src/liveReconciliationDisplay.js';

const summary = { last_run: '2026-09-05 09:00:00', account_count: 1, api_required_count: 0, error_count: 0, mismatch_count: 0, status: 'pass', status_label: '정상' };
test('empty or never-run reconciliation cannot appear as a successful account read', () => {
  for (const snapshot of [{}, { api_connected: true }, { api_connected: true, reconciliation: { summary: { ...summary, last_run: '미실행' } } }, { api_connected: true, reconciliation: { summary: { ...summary, account_count: 0 } } }]) {
    assert.equal(liveReconciliationDisplay(snapshot).brokerKnown, false);
    assert.equal(liveReconciliationDisplay(snapshot).label, '대조 미확인');
  }
});
test('explicit account observation is required and disconnect invalidates the display', () => {
  assert.equal(liveReconciliationDisplay({ api_connected: true, reconciliation: { summary } }).brokerKnown, true);
  assert.equal(liveReconciliationDisplay({ api_connected: false, reconciliation: { summary } }).brokerKnown, false);
  assert.equal(liveReconciliationDisplay({ api_connected: true, reconciliation: { summary: { ...summary, error_count: undefined } } }).brokerKnown, false);
});
test('known failures remain visible even when broker observation is incomplete', () => {
  const failed = liveReconciliationDisplay({ reconciliation: { summary: { ...summary, last_run: '미실행', mismatch_count: 1, status: 'fail', status_label: '불일치' } } });
  assert.equal(failed.brokerKnown, false);
  assert.equal(failed.status, 'fail');
  assert.equal(failed.label, '불일치');
});

test('a poll attempt alone never claims successful REST or real-time observation', () => {
  const view = liveExecutionDisplay({ execution_events: { last_poll: '2026-10-28T09:00:00Z' } }, { nowMs: Date.parse('2026-10-28T09:00:10Z') });
  assert.equal(view.status, 'warn');
  assert.equal(view.value, '최근 조회 미확인');
});
test('actual recent REST success is distinct from a connected private stream', () => {
  const snapshot = { execution_events: { snapshot_poll: { brokers: { upbit: { last_status: 'unchanged', last_success_at: '2026-10-28T09:00:00Z' } } } } };
  assert.equal(liveExecutionDisplay(snapshot, { nowMs: Date.parse('2026-10-28T09:00:10Z') }).value, 'REST 조회 완료');
  assert.equal(liveExecutionDisplay(snapshot, { nowMs: Date.parse('2026-10-28T09:10:00Z') }).value, '체결 감시 미시작');
  assert.equal(liveExecutionDisplay(snapshot, { nowMs: Date.parse('2026-10-28T08:00:00Z') }).value, '체결 감시 미시작');
  snapshot.execution_events.errors = [{ broker_id: 'upbit' }];
  assert.equal(liveExecutionDisplay(snapshot, { nowMs: Date.parse('2026-10-28T09:00:10Z') }).value, '조회 오류 확인');
});
test('one connected stream does not imply every broker is connected', () => {
  const view = liveExecutionDisplay({ execution_streams: { brokers: { upbit: { connected: true }, kis: { connected: false } } } });
  assert.equal(view.value, '일부 실시간 연결');
  assert.equal(view.status, 'warn');
});
test('legacy balance observations cannot become fills or baseline approval', () => {
  const view = liveLedgerDisplay({ program_ledger: { cash: [{ source: 'event_poll' }], fill_event_count: 0, snapshot_event_count: 91468, other_event_count: 0, last_cash_update: '2026-07-30 02:30:36', execution_events: Array(20).fill({ state: 'UNKNOWN' }) } });
  assert.equal(view.value, '과거 잔고 기준');
  assert.equal(view.fillCount, 0);
  assert.equal(view.baselineRecorded, false);
  assert.equal(view.status, 'warn');
  assert.match(view.detail, /실제 체결 기록 0건/);
  assert.match(view.detail, /기준 승인 근거 미확인/);
  assert.match(liveLedgerDisplay({ program_ledger: { execution_event_count: 91468 } }).detail, /실제 체결 건수 미확인/);
});
test('mismatch explanations retain failure and do not invent fee or ownership causes', () => {
  const snapshot = { reconciliation: { accounts: [{ broker_id: 'upbit', currency: 'KRW', program_source: 'event_poll', status: 'mismatch', delta_cash: '-10' }], positions: [{ broker_id: 'binance', symbol: 'BTCUSDT', program_source: 'missing', status: 'mismatch', delta_qty: '-1' }] }, program_ledger: { cash: [{ broker_id: 'upbit', currency: 'KRW', source: 'event_poll', updated_at: '2026-07-30 02:30:36' }] } };
  const issues = liveReconciliationIssues(snapshot);
  assert.equal(issues.length, 2);
  assert.equal(issues[0].status, 'mismatch');
  assert.match(issues[0].detail, /과거 조회값/);
  assert.match(issues[1].detail, /기존 보유 또는 외부 거래 여부를 확인/);
});
