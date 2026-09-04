import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';
import { buildLiveStrategyProgress, liveStrategyProgressLabel, liveRuntimeModeLabel } from '../src/strategyProgressDisplay.js';

test('live lifecycle heading and four-step display use canonical evidence, not approval', () => {
  const strategy = { lifecycle: { status: 'backtested' }, promotion: { stage: 'SHADOW' } };
  assert.equal(liveStrategyProgressLabel(strategy), '백테스트 완료');
  const steps = buildLiveStrategyProgress(strategy);
  assert.equal(steps.length, 4);
  assert.equal(steps[0].state, 'current');
  assert.equal(steps[1].state, 'pending');
});

test('missing lifecycle does not infer passed phases from PAPER or LIVE approvals', () => {
  for (const stage of ['PAPER', 'LIVE', 'LIVE_SMALL']) {
    const strategy = { promotion: { stage } };
    assert.equal(liveStrategyProgressLabel(strategy), '검증 상태 미확인');
    assert.ok(buildLiveStrategyProgress(strategy).every(({ state }) => state === 'pending'));
  }
});

test('UI exposes validation progress separately from execution scope without changing order guards', () => {
  const source = readFileSync(new URL('../src/App.jsx', import.meta.url), 'utf8');
  assert.match(source, /label="검증 진행" value=\{liveStrategyProgressLabel\(selectedStrategy\)\}/);
  assert.match(source, /실행 허용 범위: \$\{executionApprovalLabel\(promotionStage\)\}/);
  assert.match(source, /normalizedStage === "before-live-small"[\s\S]*?selectedStrategy\.live_small_eligible[\s\S]*?operatorConfirmed/);
});

test('runtime mode labels simplify display without treating missing or unknown modes as monitor', () => {
  assert.equal(liveRuntimeModeLabel('SMALL_LIVE'), '제한 실거래');
  assert.equal(liveRuntimeModeLabel('FULL_LIVE'), '실전 운용');
  assert.equal(liveRuntimeModeLabel('MONITOR'), '관찰 (주문 없음)');
  assert.equal(liveRuntimeModeLabel(), '모드 미확인');
  assert.match(liveRuntimeModeLabel('other'), /확인 필요/);
});
