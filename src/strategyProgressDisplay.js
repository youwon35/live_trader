import { strategyLifecycleLabel, strategyLifecycleSteps } from '../../../packages/design/strategy-progress.js';

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
