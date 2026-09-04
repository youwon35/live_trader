import { strategyLifecycleLabel, strategyLifecycleSteps } from '../../../packages/design/strategy-progress.js';

export function liveRuntimeModeLabel(mode) {
  const normalized = String(mode ?? '').trim().toUpperCase();
  return { MONITOR: '관찰 (주문 없음)', SMALL_LIVE: '제한 실거래', FULL_LIVE: '실전 운용' }[normalized]
    || (normalized ? `모드 확인 필요 (${normalized})` : '모드 미확인');
}

export function liveStrategyLifecycleStage(strategy) {
  // promotion.stage is execution authorization, not completed validation evidence.
  return strategy?.lifecycle?.status ?? strategy?.lifecycle_status ?? '';
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
