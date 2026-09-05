import {
  PIPELINE_PHASES, canonicalStrategyLifecycleStage, strategyLifecycleLabel,
  strategyLifecyclePhase, strategyLifecycleSteps,
} from '../../../packages/design/strategy-progress.js';

export function liveRuntimeModeLabel(mode) {
  const normalized = String(mode ?? '').trim().toUpperCase();
  return { MONITOR: '관찰 (주문 없음)', SMALL_LIVE: '제한 실거래', FULL_LIVE: '실전 운용' }[normalized]
    || (normalized ? `모드 확인 필요 (${normalized})` : '모드 미확인');
}

export function liveStrategyLifecycleStage(strategy) {
  // promotion.stage is execution authorization, not completed validation evidence.
  const stages = [strategy?.lifecycle?.status, strategy?.lifecycle_status]
    .filter((stage) => String(stage ?? '').trim())
    .map(canonicalStrategyLifecycleStage);
  // A stale active projection must never hide a recorded pause or retirement.
  if (stages.includes('retired')) return 'retired';
  if (stages.includes('paused')) return 'paused';
  return stages[0] || '';
}

export function liveStrategyPhaseFilter(value) {
  if (value === 'all') return 'all';
  if (PIPELINE_PHASES.some((phase) => phase.key === value)) return value;
  const stage = canonicalStrategyLifecycleStage(value);
  return strategyLifecyclePhase(stage) || stage || 'unknown';
}

export function liveStrategyPhaseId(strategy) {
  return liveStrategyPhaseFilter(liveStrategyLifecycleStage(strategy));
}

export function liveStrategyPhaseOptions(strategies) {
  const present = new Set(strategies.map(liveStrategyPhaseId));
  return [...PIPELINE_PHASES.map((phase) => phase.key), 'paused', 'retired', 'unknown']
    .filter((phase) => present.has(phase));
}

export function liveStrategyPhaseLabel(value) {
  const phase = PIPELINE_PHASES.find((item) => item.key === value);
  return phase?.label || strategyLifecycleLabel(value === 'unknown' ? '' : value);
}

export function liveStrategyProgressLabel(strategy) {
  return strategyLifecycleLabel(liveStrategyLifecycleStage(strategy));
}

export function buildLiveStrategyProgress(strategy) {
  return strategyLifecycleSteps(liveStrategyLifecycleStage(strategy)).map((step, index) => ({
    ...step,
    id: step.key,
    index: index + 1,
    state: step.state === 'complete' ? 'done' : step.state,
  }));
}
