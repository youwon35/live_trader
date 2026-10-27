import { runtimeRows } from './runtimeSelection.js';

const number = value => typeof value === 'number' && Number.isFinite(value) ? value : null;
const stamp = value => typeof value === 'string' && Number.isFinite(Date.parse(value)) ? Date.parse(value) : null;
export const routeName = id => ({binance:'Binance 현물','binance-futures':'Binance 선물',upbit:'Upbit',kis:'한국투자증권'})[id] || id || '경로 미확인';

// A curve consists of actual account samples, never of browser polling times.
export function equitySeries(history = [], route, now = Date.now()) {
  const samples = new Map();
  for (const row of history) {
    const at = stamp(row.observedAt);
    if (row.route !== route || at === null || at > now + 5000 || row.basis !== 'OBSERVED_NATIVE_EQUITY_CHANGE') continue;
    const baseline = number(row.baselineNative), current = number(row.currentNative);
    if (baseline === null || current === null || !row.quoteCurrency) continue;
    samples.set(at, {at, value:current-baseline, currency:row.quoteCurrency, baseline});
  }
  const sorted = [...samples.values()].sort((a,b) => a.at-b.at);
  const last = sorted.at(-1);
  // A changed currency or baseline is a different series, not continuous profit.
  if (!last) return [];
  let first = sorted.length-1;
  while (first>0 && sorted[first-1].currency===last.currency && sorted[first-1].baseline===last.baseline) first--;
  return sorted.slice(first).slice(-720);
}

export function projectContinuousDashboard(snapshot = {}, now = Date.now()) {
  const connected = snapshot.api_connected === true;
  const rows = runtimeRows(snapshot.continuous_runtime).map(row => {
    const running = row.running === true;
    const heartbeat = stamp(row.lastHeartbeat), dataAt = stamp(row.lastDataAt);
    const heartbeatCurrent = connected && heartbeat !== null && now-heartbeat <= 30000 && heartbeat-now <= 5000;
    // lastDataAt is a closed-bar/open-boundary receipt, not a wire heartbeat.
    // A 1h strategy legitimately has no new boundary for almost one hour.
    const dataObserved = dataAt !== null && dataAt-now <= 5000;
    const feedErrors = Object.values(row.feedErrors || {}).filter(Boolean);
    const integrityFailures = [row.marketClockIntegrity, row.sourceIntegrity]
      .flatMap(integrity=>Array.isArray(integrity?.failureCodes)?integrity.failureCodes:[]).filter(Boolean);
    const issue = row.lastError || feedErrors[0] || integrityFailures.join(' · ') || '';
    const phase = String(row.phase || '').toUpperCase();
    const degraded = ['DEGRADED','FAILED','ERROR','UNKNOWN'].includes(phase);
    const feedReady = row.allFeedsReady === true && row.deliveryReadinessSatisfied !== false;
    const tone = issue || degraded ? 'danger' : !connected || (running && (!heartbeatCurrent || !feedReady)) ? 'warning' : running ? 'success' : 'neutral';
    const state = issue || degraded ? '운용 검사 확인 필요' : !connected ? '현재 상태 미확인' : running && !heartbeatCurrent ? '엔진 응답 확인 필요' : running && !feedReady ? '수신 준비 확인 필요' : running ? '엔진 응답 정상' : row.phase || '대기';
    const bars = Object.values(row.engine?.latestBars || {});
    const cycles = Array.isArray(row.engine?.recentCycles) ? row.engine.recentCycles : [];
    const strategies = (Array.isArray(row.engine?.strategies) ? row.engine.strategies : []).map(spec => {
      const matches = bars.filter(bar => bar.symbol === spec.symbol && bar.provider === spec.provider && bar.timeframe === spec.timeframe);
      const bar = matches.length === 1 ? matches[0] : null;
      const decisions = cycles.flatMap(cycle => (cycle.decisions || []).filter(decision => spec.strategyInstanceId && decision.strategyInstanceId === spec.strategyInstanceId)
        .map(decision => ({...decision, occurredAt:cycle.occurredAt}))).filter(decision=>stamp(decision.occurredAt)!==null)
        .sort((a,b)=>stamp(b.occurredAt)-stamp(a.occurredAt));
      const decision = decisions[0];
      return {id:spec.strategyInstanceId, symbol:spec.symbol, timeframe:spec.timeframe, plugin:spec.pluginId,
        lastPrice:number(bar?.close), barAt:bar?.endTime, decisionAt:decision?.occurredAt,
        signal:decision?.signal || '', reason:typeof decision?.reason === 'string' ? decision.reason.slice(0,300) : ''};
    });
    return {...row, issue, tone, state, strategies, heartbeatCurrent, dataObserved, feedReady, brokerLabel:routeName(row.brokerId || row.runtimeScope?.brokerId),
      decisions:number(row.engine?.decisionCount), bars:number(row.engine?.barCount),
      symbols:Array.isArray(row.allowedSymbols)?row.allowedSymbols:[], lastCycleAt:row.engine?.lastCycleAt || ''};
  });
  return {rows, connected, active:rows.filter(row=>row.running===true).length,
    issueCount:rows.filter(row=>['warning','danger'].includes(row.tone)).length};
}
