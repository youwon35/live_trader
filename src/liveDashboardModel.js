import { runtimeRows } from './runtimeSelection.js';
import { signalDiagnostics } from './signalDiagnostics.js';

const number = value => typeof value === 'number' && Number.isFinite(value) ? value : null;
const stamp = value => typeof value === 'string' && Number.isFinite(Date.parse(value)) ? Date.parse(value) : null;
export const routeName = id => ({binance:'Binance 현물','binance-futures':'Binance 선물',upbit:'Upbit',kis:'한국투자증권',toss:'토스증권'})[id] || id || '경로 미확인';
export const watchSignal = value => ({BUY:'매수 신호',SELL:'매도 신호',HOLD:'관망',NONE:'신호 없음'})[value] || '미관측';
const array = value => Array.isArray(value) ? value : [];
const note = value => typeof value === 'string' ? value.slice(0, 500) : '';
const latestFirst = (a,b) => stamp(b.at)-stamp(a.at);
const localDay = new Intl.DateTimeFormat('sv-SE',{timeZone:'Asia/Seoul',year:'numeric',month:'2-digit',day:'2-digit'});

function observedAccounts(snapshot, routes, now) {
  const budgets = Object.values(snapshot.account_risk?.budgets || {});
  return budgets.flatMap(row => {
    if (!row || !routes.has(row.broker_id) || !row.currency) return [];
    const at = stamp(row.observed_at);
    const start = number(row.starting_equity), equity = number(row.current_equity);
    if (at === null || at > now+5000 || start === null || start < 0 || equity === null || equity < 0) return [];
    return [{broker:row.broker_id,currency:row.currency,at:row.observed_at,start,equity,day:row.day,
      change:equity-start,percent:start>0?(equity-start)/start*100:null,
      current:snapshot.api_connected===true && row.day===localDay.format(now) && now-at<=120000}];
  }).sort((a,b)=>Number(b.current)-Number(a.current)||latestFirst(a,b));
}

// Keep only dated account observations actually received by this window.
export function appendAccountObservations(previous, accounts) {
  let next = previous;
  for (const account of accounts) {
    if (!account.current || account.percent === null) continue;
    const key = account.broker+':'+account.currency;
    const old = next[key] || [];
    const last = old.at(-1);
    if (last && stamp(account.at) <= stamp(last.at)) continue;
    const sameBasis = last?.day===account.day && last.start===account.start;
    if (next === previous) next = {...previous};
    next[key] = [...(sameBasis?old:[]),account].slice(-120);
  }
  return next;
}

const orderLabels = {
  submitted:'주문 접수', accepted:'주문 접수', open:'미결 주문', partial:'부분 체결', partially_filled:'부분 체결',
  filled:'체결 완료', risk_blocked:'위험 검사 차단', risk_rejected:'위험 검사 차단', rejected:'주문 거절',
  failed:'주문 실패', unknown:'주문 결과 미확인', timeout:'주문 결과 미확인', canceled:'주문 취소',
  cancelled:'주문 취소', expired:'주문 만료', dry_run:'가상 전송 기록',
};
const orderIssues = new Set(['risk_blocked','risk_rejected','rejected','failed','unknown','timeout']);

function scopedOrders(snapshot, rows, now) {
  const scope = new Map();
  for (const row of rows) {
    if (!row.broker || !row.deploymentId) continue;
    const key = row.broker + ':' + row.deploymentId;
    const matches = scope.get(key) || [];
    matches.push(row);
    scope.set(key,matches);
  }
  return array(snapshot.orders).flatMap(order => {
    const matching = scope.get(order.broker_id + ':' + order.deployment_id);
    if (matching?.length !== 1) return [];
    const [runtime] = matching;
    const at = [order.updated_at,order.time,order.created_at].find(value=>stamp(value)!==null);
    const status = String(order.state || '').toLowerCase();
    const label = orderLabels[status];
    const dayStart = stamp(runtime.startedAt);
    if (!label || !at || stamp(at)>now+5000 || (dayStart!==null && stamp(at)<dayStart)) return [];
    return [{id:order.order_id,at,symbol:order.symbol,broker:order.broker_id,kind:'order',
      label:order.dry_run === true ? '가상 전송 기록' : label,
      side:order.side,reason:note(order.reason),issue:orderIssues.has(status)}];
  });
}

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
  const rows = runtimeRows(snapshot.continuous_runtime || {}).filter(row=>row && !row.ambiguous).map(row => {
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
    const broker = row.brokerId || row.runtimeScope?.brokerId;
    const bars = Object.values(row.engine?.latestBars || {});
    const specs = array(row.engine?.strategies);
    const specById = new Map(specs.filter(spec=>spec.strategyInstanceId).map(spec=>[spec.strategyInstanceId,spec]));
    const started = stamp(row.startedAt);
    const decisions = array(row.engine?.recentCycles).flatMap(cycle => {
      const time = stamp(cycle.occurredAt);
      if (time === null || time > now+5000 || (running && started!==null && time<started)) return [];
      const results = array(cycle.handlerResult?.results);
      const cycleDecisions = array(cycle.decisions);
      return cycleDecisions.flatMap(decision => {
        const spec = specById.get(decision.strategyInstanceId);
        if (!spec) return [];
        const scoped = results.filter(result => result.strategyInstanceId === spec.strategyInstanceId);
        const legacy = cycleDecisions.filter(item=>item.strategyId===decision.strategyId).length === 1
          ? results.filter(result=>!result.strategyInstanceId && decision.strategyId && result.strategyId===decision.strategyId) : [];
        const result = scoped.length===1 ? scoped[0] : scoped.length===0 && legacy.length===1 ? legacy[0] : null;
        const blocked = result?.ok===false || decision.metadata?.dataSafetyBlocked===true;
        return [{id:decision.evaluationKey || cycle.cycleId,instance:spec.strategyInstanceId,at:cycle.occurredAt,
          broker,kind:'signal',symbol:spec.symbol,timeframe:spec.timeframe,signal:decision.signal,
          label:blocked && decision.signal==='HOLD' ? '판단 보류' : watchSignal(decision.signal),
          price:number(decision.bar?.close),barAt:decision.bar?.endTime,
          reason:note(decision.reason),action:note(result?.action),
          outcome:note(result?.reason),issue:blocked,diagnostics:signalDiagnostics(decision.metadata)}];
      });
    }).sort(latestFirst);
    const latestByInstance = new Map();
    for (const decision of decisions) if (!latestByInstance.has(decision.instance)) latestByInstance.set(decision.instance,decision);
    const strategies = specs.map(spec => {
      const matches = bars.filter(bar => bar.symbol === spec.symbol && bar.provider === spec.provider && bar.timeframe === spec.timeframe);
      const bar = matches.length === 1 ? matches[0] : null;
      const decision = latestByInstance.get(spec.strategyInstanceId);
      return {id:spec.strategyInstanceId, symbol:spec.symbol, timeframe:spec.timeframe, plugin:spec.pluginId,
        lastPrice:number(bar?.close), barAt:bar?.endTime, decisionAt:decision?.at,
        signal:decision?.signal || '', reason:decision?.reason || '',issue:decision?.issue===true};
    });
    return {...row, issue, tone, state, strategies, events:decisions, heartbeatCurrent, dataObserved, feedReady,
      broker,brokerLabel:routeName(broker),
      decisions:number(row.engine?.decisionCount), bars:number(row.engine?.barCount),
      symbols:Array.isArray(row.allowedSymbols)?row.allowedSymbols:[], lastCycleAt:row.engine?.lastCycleAt || ''};
  });
  const activeRows = rows.filter(row=>row.running===true);
  const relevant = activeRows.length ? activeRows : rows.filter(row=>row.strategies.length || row.issue);
  const watch = relevant.flatMap(row=>{
    const scope = row.runtimeId || row.deploymentId || row.profileId;
    const strategies = row.strategies.length ? row.strategies : row.symbols.map(symbol=>({symbol}));
    return strategies.map((strategy,index)=>({...strategy,key:[scope,row.broker,strategy.id || index].join(':'),
      broker:row.broker,brokerLabel:row.brokerLabel,runtimeState:row.state,tone:row.tone}));
  });
  const routes = new Set(relevant.map(row=>row.broker).filter(Boolean));
  const accounts = observedAccounts(snapshot,routes,now);
  const events = [...relevant.flatMap(row=>row.events),...scopedOrders(snapshot,relevant,now)].sort(latestFirst);
  const signals = events.filter(event=>event.kind==='order' || event.issue || ['BUY','SELL'].includes(event.signal));
  return {rows,visibleRows:relevant,connected,active:activeRows.length,watch,accounts,
    symbols:new Set(watch.filter(row=>row.symbol).map(row=>[row.broker,row.symbol].join(':'))).size,
    recent:signals.slice(0,12),details:events.slice(0,40),
    pending:connected?number(snapshot.order_queue?.active):null,
    evaluations:relevant.length && relevant.every(row=>row.decisions!==null) ? relevant.reduce((sum,row)=>sum+row.decisions,0) : null,
    issueCount:activeRows.filter(row=>['warning','danger'].includes(row.tone)).length};
}
