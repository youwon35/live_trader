import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';
import React from 'react';
import { transformSync } from 'esbuild';
import { executionApprovalLabel, strategyLifecycleRank } from '../../../packages/design/strategy-progress.js';
import { readArtifactLifecycle } from '../../../packages/design/artifact-lifecycle.js';
import { verifiedCanaryExecution } from '../src/executionAvailability.js';
import {
  buildLiveStrategyProgress, liveDeploymentLifecycleLabel, liveStrategyLifecycleStage, liveStrategyValidationStage, liveStrategyPhaseFilter,
  liveStrategyPhaseId, liveStrategyPhaseLabel, liveStrategyPhaseOptions, liveStrategyProgressLabel, liveRuntimeModeLabel,
} from '../src/strategyProgressDisplay.js';

const projection = (status) => ({ status: status || 'unknown', source: status ? 'lifecycle.status' : 'missing', conflicts: [] });

test('live lifecycle heading and four-step display use canonical evidence, not approval', () => {
  const strategy = { artifactLifecycle: projection('backtested'), lifecycle: { status: 'live' }, promotion: { stage: 'LIVE' } };
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
  assert.match(source, /label="저장본 검증 단계" value=\{liveStrategyProgressLabel\(selectedStrategy\)\}/);
  assert.match(source, /label="배포 운용 상태" value=\{liveDeploymentLifecycleLabel\(selectedStrategy\)\}/);
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
  const phases = states.map((status) => liveStrategyPhaseId({ artifactLifecycle: projection(status), lifecycle_status: 'live' }));
  assert.deepEqual(phases, ['backtest', 'backtest', 'paper', 'paper', 'paper', 'live-check', 'live']);
  assert.deepEqual([...new Set(phases)].map(liveStrategyPhaseLabel), ['백테스트', '모의 검증', '제한 실거래', '실전 운용']);
  assert.equal(liveStrategyPhaseFilter('shadowed'), 'paper');
  assert.equal(liveStrategyPhaseFilter('before-live-small'), 'live-check');
  assert.equal(liveStrategyPhaseFilter('all'), 'all');
  assert.equal(liveStrategyPhaseId({ promotion: { stage: 'LIVE' } }), 'unknown');
  assert.deepEqual(liveStrategyPhaseOptions([...states, 'retired', 'paused', ''].reverse()
    .map((status) => ({ artifactLifecycle: projection(status), lifecycle_status: 'live' }))),
  ['backtest', 'paper', 'live-check', 'live', 'paused', 'retired', 'unknown']);
});

test('runtime-only or malformed projected rows cannot supply stored validation metadata', () => {
  for (const row of [
    { lifecycle: { status: 'live' }, lifecycle_status: 'live' },
    { artifactLifecycle: null, lifecycle_status: 'live' },
    { artifactLifecycle: { ...projection('live'), source: 'deployment.lifecycle' }, lifecycle_status: 'live' },
    { artifactLifecycle: { ...projection('live'), conflicts: {} }, lifecycle_status: 'live' },
  ]) {
    assert.equal(liveStrategyValidationStage(row), 'unknown');
    assert.equal(liveStrategyProgressLabel(row), '검증 상태 미확인');
    assert.equal(liveStrategyPhaseId(row), 'unknown');
    assert.equal(liveStrategyPhaseLabel(liveStrategyPhaseId(row)), '검증 상태 미확인');
    assert.ok(buildLiveStrategyProgress(row).every(({ state }) => state === 'pending'));
    assert.equal(liveStrategyLifecycleStage(row), 'live');
  }
  const incompleteDeployment = { artifactLifecycle: projection('live'), lifecycle_status: 'live', deploymentLifecycle: { status: 'unknown', source: 'deployment-registry' } };
  assert.equal(liveStrategyProgressLabel(incompleteDeployment), '실전 운용 단계');
  assert.equal(liveDeploymentLifecycleLabel(incompleteDeployment), '배포 상태 미확인');
  assert.equal(liveStrategyLifecycleStage(incompleteDeployment), 'live');
});

// Compile only the actual passive selector function. App module initialization,
// native bridges, backend imports and network requests are never executed.
const appSource = readFileSync(new URL('../src/App.jsx', import.meta.url), 'utf8');
const selectorSource = appSource.slice(appSource.indexOf('function LiveStrategySelectorPanel('), appSource.indexOf('function StrategyDiscoveryToolbar('));
const compiledSelector = transformSync(selectorSource, { loader: 'jsx', jsx: 'transform', jsxFactory: 'React.createElement', jsxFragment: 'React.Fragment' }).code;
const selectorDependencies = {
  React, executionApprovalLabel, strategyLifecycleRank, verifiedCanaryExecution, readArtifactLifecycle,
  liveStrategyLifecycleStage, buildLiveStrategyProgress, liveStrategyProgressLabel, liveDeploymentLifecycleLabel,
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
  artifactLifecycle: projection('backtested'),
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

test('rendered metadata and deployment cards remain distinct for the same artifact identity', () => {
  const metrics = (node) => {
    if (!node || typeof node !== 'object') return [];
    if (Array.isArray(node)) return node.flatMap(metrics);
    return [...(node.type === 'metric' ? [node.props] : []), ...metrics(node.props?.children)];
  };
  for (const status of ['live', 'paused', 'retired']) {
    const strategy = { ...eligible, lifecycle_status: status,
      artifact_reference: { artifactId: 'same-artifact', artifactHash: 'a'.repeat(64) } };
    const cards = metrics(Selector({ strategies: [strategy], selectedStrategy: strategy }));
    assert.equal(cards.find((card) => card.label === '저장본 검증 단계').value, '백테스트 완료');
    assert.equal(cards.find((card) => card.label === '배포 운용 상태').value, liveDeploymentLifecycleLabel(strategy));
  }
  assert.doesNotThrow(() => Selector({ strategies: [eligible], selectedStrategy: { ...eligible,
    artifactLifecycle: { ...projection('live'), conflicts: {} } } }));
});
