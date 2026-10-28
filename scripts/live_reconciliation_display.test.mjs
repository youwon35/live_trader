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

test('old snapshot differences expose exact baseline and observation without inventing transfers', () => {
  const issues = liveReconciliationIssues({ reconciliation: { accounts: [{ broker_id: 'binance', currency: 'USDT', program_source: 'event_poll', program_updated_at: '2026-07-30 02:30:36', status: 'mismatch', program_cash_value: 42.25, broker_cash_value: 32.25 }] } });
  assert.equal(issues[0].reason, '과거 잔고 기준과 차이');
  assert.equal(issues[0].comparison, '이전 기준 42.25 → 거래소 관측 32.25 USDT');
  assert.equal(issues[0].baselineAt, '2026-07-30 02:30:36');
  assert.equal(issues[0].status, 'mismatch');
  assert.doesNotMatch(issues[0].detail, /이체 완료|정상 처리/);
});
test('missing asset baseline is not presented as verified zero and unknown observations remain unknown', () => {
  const issues = liveReconciliationIssues({ reconciliation: { positions: [{ broker_id: 'binance', symbol: 'FDUSD', asset: 'FDUSD', program_source: 'missing', status: 'mismatch', program_qty_value: 0, broker_qty_value: 0.25 }], accounts: [{ broker_id: 'kis', currency: 'KRW', status: 'api_required', program_cash_value: 0, broker_cash_value: null }] } });
  assert.match(issues[1].comparison, /이전 기준 없음 → 거래소 관측 0.25 FDUSD/);
  assert.equal(issues[0].comparison, '이전 기준 0 → 거래소 관측 미확인 KRW');
  assert.equal(issues[1].status, 'mismatch');
});

test('account scope errors take priority over legacy or missing baseline explanations', () => {
  for (const program_source of ['event_poll', 'missing']) {
    const [issue] = liveReconciliationIssues({ reconciliation: { accounts: [{ broker_id: 'binance', currency: 'USDT', program_currency: 'USDT', program_source, status: 'api_required', comparison_issue: 'account_scope_changed', detail: '현재 조회와 과거 원장의 계좌가 다릅니다.', program_cash_value: 23, broker_cash_value: 13, delta_cash: '10 USDT' }] } });
    assert.equal(issue.reason, '계좌 또는 통화 범위 변경');
    assert.equal(issue.detail, '현재 조회와 과거 원장의 계좌가 다릅니다.');
    assert.equal(issue.delta, null);
    assert.doesNotMatch(issue.detail, /과거 조회값으로 만든|현금 원장 근거가 없습니다/);
    assert.equal(issue.status, 'api_required');
  }
});

test('different currencies retain their own units and never expose a numeric delta', () => {
  for (const comparison_issue of ['account_scope_changed', '']) {
    const [issue] = liveReconciliationIssues({ reconciliation: { accounts: [{ broker_id: 'binance', currency: 'KRW', program_currency: 'USDT', program_source: 'event_poll', status: 'api_required', comparison_issue, detail: comparison_issue ? '현재 조회와 과거 원장의 통화가 다릅니다.' : '', program_cash_value: 42.25, broker_cash_value: 100346, delta_cash: '-100322.1950573 KRW' }] } });
    assert.equal(issue.comparison, '이전 기준 42.25 USDT → 거래소 관측 100,346 KRW · 통화가 달라 금액 차이를 계산하지 않습니다.');
    assert.equal(issue.delta, null);
    assert.match(issue.detail, /통화가 다릅니다/);
  }
});

test('ambiguous account scopes never present arbitrary amounts or a fallback time as confirmed', () => {
  const [issue] = liveReconciliationIssues({ reconciliation: { accounts: [{ broker_id: 'binance', currency: 'USDT', program_currency: 'USDT', program_source: 'event_poll', program_updated_at: '2026-07-30 02:30:36', status: 'api_required', comparison_issue: 'ambiguous_account_scope', detail: '같은 거래소에 여러 계좌 또는 통화 행이 있어 비교 대상을 하나로 확정할 수 없습니다.', program_cash_value: 42.25, broker_cash_value: 32.25, delta_cash: '10 USDT' }] }, program_ledger: { cash: [{ broker_id: 'binance', currency: 'USDT', source: 'event_poll', updated_at: '2026-07-30 02:30:36' }] } });
  assert.equal(issue.reason, '계좌·통화 비교 대상 중복');
  assert.equal(issue.comparison, '이전 기준 미확정 → 거래소 관측 미확정 · 비교 대상 확인 필요');
  assert.equal(issue.baselineAt, null);
  assert.equal(issue.delta, null);
  assert.doesNotMatch(issue.comparison, /42.25|32.25|10 USDT/);
  assert.match(issue.detail, /여러 계좌 또는 통화 행/);
});

test('unverified or future scope issues preserve backend detail without guessing missing currency', () => {
  for (const comparison_issue of ['account_scope_unverified', 'future_scope_issue']) {
    const [issue] = liveReconciliationIssues({ reconciliation: { accounts: [{ broker_id: 'kis', currency: 'KRW', program_currency: '', program_source: 'missing', status: 'api_required', comparison_issue, detail: '현재 조회 또는 과거 원장에 계좌·통화 식별 정보가 없습니다.', program_cash_value: 100346, broker_cash_value: 100456, delta_cash: '-110 KRW' }] } });
    assert.match(issue.detail, /계좌·통화 식별 정보가 없습니다/);
    assert.match(issue.comparison, /이전 기준 100,346 통화 미확인 → 거래소 관측 100,456 KRW/);
    assert.equal(issue.delta, null);
    assert.doesNotMatch(issue.detail, /조회 잔고와 비교할 현금 원장/);
  }
});
