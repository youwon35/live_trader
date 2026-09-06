import {
  PIPELINE_PHASES, canonicalStrategyLifecycleStage, strategyLifecycleLabel,
  strategyLifecyclePhase, strategyLifecycleSteps,
} from '../../../packages/design/strategy-progress.js';
import { readArtifactLifecycle } from '../../../packages/design/artifact-lifecycle.js';

export function liveRuntimeModeLabel(mode) {
  const normalized = String(mode ?? '').trim().toUpperCase();
  return { MONITOR: '관찰 (주문 없음)', SMALL_LIVE: '제한 실거래', FULL_LIVE: '실전 운용' }[normalized]
    || (normalized ? `모드 확인 필요 (${normalized})` : '모드 미확인');
}

export function liveStrategyLifecycleStage(strategy) {
  // Operational compatibility contract used by deployment selection and controls.
  // Stored-artifact validation is read separately below.
  const stages = [strategy?.lifecycle?.status, strategy?.lifecycle_status]
    .filter((stage) => String(stage ?? '').trim())
    .map(canonicalStrategyLifecycleStage);
  // A stale active projection must never hide a recorded pause or retirement.
  if (stages.includes('retired')) return 'retired';
  if (stages.includes('paused')) return 'paused';
  return stages[0] || '';
}

export function liveStrategyValidationStage(strategy) {
  // Normalized Live rows may contain a deployment lifecycle. An older API without
  // the explicit projection cannot prove the stored artifact's validation stage.
  return readArtifactLifecycle(strategy, { allowRawFallback: false }).status;
}

export function liveDeploymentLifecycleLabel(strategy) {
  if (strategy?.deployment_source === 'legacy-artifact') return '배포 미등록';
  const stage = strategy?.deploymentLifecycle?.status ?? liveStrategyLifecycleStage(strategy);
  return canonicalStrategyLifecycleStage(stage) ? strategyLifecycleLabel(stage) : '배포 상태 미확인';
}

export function liveStrategyPhaseFilter(value) {
  if (value === 'all') return 'all';
  if (PIPELINE_PHASES.some((phase) => phase.key === value)) return value;
  const stage = canonicalStrategyLifecycleStage(value);
  return strategyLifecyclePhase(stage) || stage || 'unknown';
}

export function liveStrategyPhaseId(strategy) {
  return liveStrategyPhaseFilter(liveStrategyValidationStage(strategy));
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
  const stage = liveStrategyValidationStage(strategy);
  return strategyLifecycleLabel(stage === 'unknown' ? '' : stage);
}

export function buildLiveStrategyProgress(strategy) {
  return strategyLifecycleSteps(liveStrategyValidationStage(strategy)).map((step, index) => ({
    ...step,
    id: step.key,
    index: index + 1,
    state: step.state === 'complete' ? 'done' : step.state,
  }));
}
