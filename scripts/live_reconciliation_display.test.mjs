import assert from 'node:assert/strict';
import test from 'node:test';
import { liveReconciliationDisplay } from '../src/liveReconciliationDisplay.js';

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
