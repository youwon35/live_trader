// A public observation is a warning, never a Start permit or Paper evidence.
const FINAL = new Set(['OBSERVED', 'WARNING', 'UNKNOWN']);
export function createMarketGapCoordinator({ begin, status, cancelRemote, onView = () => {},
  now = Date.now, sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms)), pollMs = 500 }) {
  const active = new Map(); let sequence = 0;
  const current = (flow) => active.get(flow.key) === flow && !flow.cancelled;
  const cancelledError = () => Object.assign(Error('최근 체결 공백 확인이 취소되거나 구성이 변경되었습니다.'), { code: 'MARKET_GAP_CANCELLED' });
  const publish = (flow, phase, extra = {}) => {
    if (active.get(flow.key) === flow) onView({ scopeKey: flow.key, label: flow.label,
      flowId: flow.sequence, phase, result: flow.result, ...extra });
  };
  async function remoteCancel(flow) {
    if (!flow.id || flow.cancelSent) return;
    flow.cancelSent = true;
    try { await cancelRemote(flow.id); } catch { /* Code-owned server deadline still bounds the diagnostic. */ }
  }
  async function cancel(key) {
    const flow = active.get(key); if (!flow) return false;
    flow.cancelled = true; publish(flow, 'CANCELLED'); active.delete(key);
    flow.rejectReview?.(cancelledError()); await remoteCancel(flow); return true;
  }
  const cancelAll = () => Promise.all([...active.keys()].map(cancel));
  function accept(key) {
    const flow = active.get(key);
    if (!flow || flow.phase !== 'REVIEW' || !current(flow)) return false;
    flow.phase = 'CONTINUING'; flow.resolveReview(); return true;
  }
  async function run({ scopeKey, label, body, continueStart }) {
    if (active.has(scopeKey)) throw Object.assign(Error('이 구성의 체결 공백 확인이 이미 진행 중입니다.'), { code: 'MARKET_GAP_IN_PROGRESS' });
    const flow = { key: scopeKey, label, sequence: ++sequence, id: '', cancelled: false,
      result: null, phase: 'PREPARING', cancelSent: false };
    active.set(scopeKey, flow); publish(flow, 'PREPARING');
    try {
      try {
        const started = now(); const initial = await begin(body);
        if (typeof initial?.preparationId === 'string') flow.id = initial.preparationId;
        if (!current(flow)) { await remoteCancel(flow); throw cancelledError(); }
        if (initial?.ok !== true || !flow.id) throw Error('Observation unavailable');
        let value = initial;
        while (value.status === 'PREPARING') {
          if (now() - started >= 65000) throw Error('Observation timeout');
          await sleep(pollMs);
          if (!current(flow)) throw cancelledError();
          value = await status(flow.id);
          if (!current(flow)) throw cancelledError();
          if (value?.preparationId !== flow.id || value?.ok !== true) throw Error('Observation unavailable');
          publish(flow, 'PREPARING', { completedMembers: value.completedMembers, memberCount: value.memberCount });
        }
        if (['CANCELLED', 'STALE', 'EXPIRED'].includes(value.status)) throw cancelledError();
        if (!FINAL.has(value.status)) throw Error('Observation unavailable');
        if (value.result && (value.result.authorityGranted !== false || value.result.qualificationGranted !== false
            || value.result.runtimeStarted !== false || value.result.brokerSubmissionPerformed !== false)) throw Error('Invalid observation');
        flow.result = value.result || null;
      } catch (error) {
        if (!current(flow) || error?.code === 'MARKET_GAP_CANCELLED') throw cancelledError();
        await remoteCancel(flow); flow.id = ''; flow.result = null;
        // Public-data unavailability does not invent a new financial gate.
      }
      if (!current(flow)) throw cancelledError();
      flow.phase = 'REVIEW';
      const reviewed = new Promise((resolve, reject) => { flow.resolveReview = resolve; flow.rejectReview = reject; });
      publish(flow, 'REVIEW'); await reviewed;
      if (!current(flow)) throw cancelledError();
      if (flow.result && Date.parse(flow.result.expiresAtUtc) <= now()) throw Object.assign(Error('최근 체결 공백 관측이 만료되었습니다. 다시 확인해 주세요.'), { code: 'MARKET_GAP_EXPIRED' });
      publish(flow, 'CONTINUING');
      // Only the original user intent continues; all original Start gates run.
      const result = await continueStart(flow.result ? flow.id : '');
      if (!current(flow)) throw cancelledError();
      publish(flow, 'COMPLETE'); return result;
    } catch (error) {
      if (current(flow)) publish(flow, error?.code === 'MARKET_GAP_CANCELLED' ? 'CANCELLED' : 'FAILED');
      throw error;
    } finally {
      if (active.get(scopeKey) === flow) active.delete(scopeKey);
      await remoteCancel(flow);
    }
  }
  return { run, accept, cancel, cancelAll, activeCount: () => active.size };
}
