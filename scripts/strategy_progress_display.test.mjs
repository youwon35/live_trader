import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';
import React from 'react';
import { transformSync } from 'esbuild';
import { executionApprovalLabel, strategyLifecycleRank } from '../../../packages/design/strategy-progress.js';
import { verifiedCanaryExecution } from '../src/executionAvailability.js';
import {
  buildLiveStrategyProgress, liveStrategyLifecycleStage, liveStrategyPhaseFilter,
  liveStrategyPhaseId, liveStrategyPhaseLabel, liveStrategyPhaseOptions, liveStrategyProgressLabel, liveRuntimeModeLabel,
} from '../src/strategyProgressDisplay.js';

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

test('discovery groups the seven saved validation states into four shared phases', () => {
  const states = ['draft', 'backtested', 'before-shadow', 'shadowed', 'papered', 'before-live-small', 'live'];
  const phases = states.map((status) => liveStrategyPhaseId({ lifecycle_status: status }));
  assert.deepEqual(phases, ['backtest', 'backtest', 'paper', 'paper', 'paper', 'live-check', 'live']);
  assert.deepEqual([...new Set(phases)].map(liveStrategyPhaseLabel), ['백테스트', '모의 검증', '제한 실거래', '실전 운용']);
  assert.equal(liveStrategyPhaseFilter('shadowed'), 'paper');
  assert.equal(liveStrategyPhaseFilter('before-live-small'), 'live-check');
  assert.equal(liveStrategyPhaseFilter('all'), 'all');
  assert.equal(liveStrategyPhaseId({ promotion: { stage: 'LIVE' } }), 'unknown');
  assert.deepEqual(liveStrategyPhaseOptions([...states, 'retired', 'paused', ''].reverse()
    .map((status) => ({ lifecycle_status: status }))),
  ['backtest', 'paper', 'live-check', 'live', 'paused', 'retired', 'unknown']);
});

// Compile only the actual passive selector function. App module initialization,
// native bridges, backend imports and network requests are never executed.
const appSource = readFileSync(new URL('../src/App.jsx', import.meta.url), 'utf8');
const selectorSource = appSource.slice(appSource.indexOf('function LiveStrategySelectorPanel('), appSource.indexOf('function StrategyDiscoveryToolbar('));
const compiledSelector = transformSync(selectorSource, { loader: 'jsx', jsx: 'transform', jsxFactory: 'React.createElement', jsxFragment: 'React.Fragment' }).code;
const selectorDependencies = {
  React, executionApprovalLabel, strategyLifecycleRank, verifiedCanaryExecution,
  liveStrategyLifecycleStage, buildLiveStrategyProgress, liveStrategyProgressLabel,
  MIN_LIVE_CANARY_FILLS: 3, formatKeyValueMap: () => '', liveArtifactFailureReasons: () => [],
  PanelHeader: 'header', MetricCard: 'metric', StatusPill: 'status', ActionButton: 'action',
  CompactDisclosure: 'details', BadgeCheck: 'icon', Play: 'icon', Pause: 'icon', Trash2: 'icon', EmptyRow: 'empty',
};
const Selector = new Function(...Object.keys(selectorDependencies), `${compiledSelector}; return LiveStrategySelectorPanel;`)(...Object.values(selectorDependencies));
function actions(node) {
  if (!node || typeof node !== 'object') return [];
  if (Array.isArray(node)) return node.flatMap(actions);
  return [...(node.type === 'action' ? [node.props] : []), ...actions(node.props?.children)];
}
const eligible = {
  strategy_id: 'current-strategy', lifecycle_status: 'before-live-small', live_small_eligible: true,
  canary_execution: { verified: true, scope: { eligible: true }, successful: 3, blocked: 0 },
};
function selectorActions(strategy, overrides = {}) {
  return actions(Selector({ strategies: [strategy], selectedStrategy: strategy,
    operatorConfirmed: true, onPromoteLive: () => {}, ...overrides }));
}

test('rendered selector blocks stale approvals and preserves real promotion prerequisites', () => {
  const promotion = (strategy, overrides) => selectorActions(strategy, overrides).find((action) => action.label === '실전 운용 단계로 승인');
  assert.equal(promotion(eligible).disabled, false);
  assert.equal(promotion(eligible, { operatorConfirmed: false }).disabled, true);
  assert.equal(promotion(eligible, { summary: { blocker_count: 1 } }).disabled, true);
  assert.equal(promotion({ ...eligible, canary_execution: { verified: false } }).disabled, true);
  assert.equal(promotion({ ...eligible, lifecycle_status: undefined, promotion: { stage: 'before-live-small' } }).disabled, true);
  for (const status of ['paused', 'retired']) {
    const current = { ...eligible, lifecycle: { status: 'before-live-small' }, lifecycle_status: status, promotion: { stage: 'LIVE' } };
    assert.equal(promotion(current).disabled, true);
    const controls = selectorActions(current);
    assert.equal(controls.some((action) => action.label === '재개'), status === 'paused');
    if (status === 'retired') assert.ok(controls.every((action) => action.disabled));
  }
});
