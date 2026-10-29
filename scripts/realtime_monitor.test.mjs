import assert from 'node:assert/strict';
import test from 'node:test';
import { projectRealtimeMonitor } from '../src/realtimeMonitorModel.js';
import { normalizeShortLiveSnapshot } from '../src/shortLiveTestModel.js';
const now = Date.parse('2026-10-27T03:00:00Z');
const iso = delta => new Date(now + delta).toISOString();
const opts = { fresh:true, receivedAt:now, now };
const raw = changes => ({ observedAt:iso(0), runtime:{ sessionId:'one', status:'RUNNING', ownerAlive:true, workerAlive:true,
  newEntriesBlocked:false, unknownOrderCount:0, startedAt:iso(-3600000), endsAt:iso(18000000),
  fillCount:0, strategyRows:[{ instanceId:'one', route:'BINANCE_SPOT', symbol:'BTCUSDT', timeframe:'1h', decisionCount:0, orderAttemptCount:0, fillObservationCount:0 }],
  brokerRows:[{ route:'BINANCE_SPOT', effectiveRouteBudgetKrw:100000, realizedLossKrw:0, unrealizedLossKrw:0 }], ...changes }});
const project = changes => projectRealtimeMonitor(normalizeShortLiveSnapshot(raw(changes)), opts);
test('running alone is not healthy; unknown orders and dead worker surface ahead of normal waiting',()=>{
  assert.match(project({}).state.title,/완성봉/);
  assert.match(project({ unknownOrderCount:1 }).state.title,/주문 결과/);
  assert.match(project({ workerAlive:null }).state.title,/프로세스/);
  assert.match(project({ blockers:['NETWORK_TIMEOUT'] }).state.title,/문제/);
});
test('fresh poll success cannot hide stale/missing/future server observation',()=>{
  for(const stamp of [undefined,iso(-21000),iso(6000)]) {
    const value=normalizeShortLiveSnapshot({...raw({}),observedAt:stamp});
    assert.equal(projectRealtimeMonitor(value,opts).responseCurrent,false);
  }
  assert.equal(projectRealtimeMonitor(normalizeShortLiveSnapshot(raw({})),{...opts,now:now+21000}).responseCurrent,false);
});
test('historical verified terminal remains a historical result without being labelled an active outage',()=>{
  const value=normalizeShortLiveSnapshot(raw({status:'TERMINATED',terminalVerified:true,naturalExitObserved:true}));
  const result=projectRealtimeMonitor(value,{...opts,now:now+7200000,fresh:false});
  assert.equal(result.terminal,true);assert.equal(result.state.tone,'neutral');assert.match(result.state.title,/종료 기록/);
  assert.equal(project({status:'TERMINATED',terminalVerified:true,naturalExitObserved:false}).terminal,false);
});
test('terminal duration freezes at observed retirement instead of advancing with display polling',()=>{
  const value=normalizeShortLiveSnapshot(raw({status:'TERMINATED',terminalVerified:true,naturalExitObserved:true,startedAt:iso(-70000),endsAt:iso(21530000),terminatedAt:iso(0)}));
  for (const later of [0,1800000,86400000]) {
    const result=projectRealtimeMonitor(value,{...opts,now:now+later});
    assert.equal(result.elapsedSeconds,70);assert.equal(result.terminatedAt,iso(0));
    assert.ok(Math.abs(result.progress-70/21600*100)<1e-10);
  }
});

test('terminal endpoint requires actual valid retirement time, with only real legacy retirement events as fallback',()=>{
  const retired={status:'TERMINATED',terminalVerified:true,naturalExitObserved:true};
  assert.equal(project(retired).progress,null);
  assert.equal(project({...retired,terminatedAt:iso(6000)}).progress,null);
  assert.equal(project({...retired,terminatedAt:iso(-3601000)}).progress,null);
  assert.equal(project({...retired,terminalVerified:false,terminatedAt:iso(0)}).progress,null);
  assert.equal(project({...retired,recentActivity:[{kind:'WORKER_EXIT_INTENT',observedAt:iso(0)}]}).progress,null);
  assert.equal(project({...retired,recentActivity:[{kind:'WORKER_DEATH_OBSERVED',observedAt:iso(-3530000)}]}).elapsedSeconds,70);
});

test('clamped zero risk loss is never promoted to zero profit; all routes required for aggregate',()=>{
  assert.equal(project({}).totalChangeKrw,null);
  const row={route:'BINANCE_SPOT',accountObservedAt:iso(0),monitorEquity:{basis:'OBSERVED_NATIVE_EQUITY_CHANGE',baselineNative:'100',currentNative:'105',krwPerQuote:'1400',quoteCurrency:'USDT'}};
  const result=project({brokerRows:[row]});
  assert.equal(result.totalChangeKrw,7000);assert.ok(Math.abs(result.routes[0].changePercent-5)<1e-10);
  assert.equal(project({brokerRows:[row,{route:'UPBIT_KRW_SPOT'}]}).totalChangeKrw,null);
  assert.equal(project({brokerRows:[{...row,monitorEquity:{...row.monitorEquity,baselineNative:'0'}}]}).routes[0].changePercent,null);
});
test('account freshness is separate from latest UI poll; absent counts remain unknown',()=>{
  const result=project({brokerRows:[{route:'BINANCE_SPOT',accountObservedAt:iso(-21000)}],strategyRows:[{instanceId:'one'}],fillCount:undefined});
  assert.equal(result.responseCurrent,true);assert.equal(result.routes[0].accountCurrent,false);
  assert.equal(result.counts.decisions,null);assert.equal(result.counts.attempts,null);assert.equal(result.counts.fills,null);
});
test('only allowed real events with valid time reach the public timeline in stable order',()=>{
  const result=project({recentActivity:[{sequence:2,kind:'ORDER_REPLY',observedAt:iso(0),status:'PARTIAL'},
    {sequence:1,kind:'ORDER_INTENT_BEFORE_SUBMIT',observedAt:iso(0)},
    {sequence:3,kind:'ACCOUNT_SECRET',observedAt:iso(0)},{kind:'ACTUAL_FILL_INCREMENT'}]});
  assert.deepEqual(result.activity.map(row=>row.sequence),[2,1]);
});

test('future local receipt and future activity are unverified, not current',()=>{
  const value=normalizeShortLiveSnapshot(raw({recentActivity:[{kind:'ACTUAL_FILL_INCREMENT',observedAt:iso(6000)}]}));
  assert.equal(projectRealtimeMonitor(value,{...opts,receivedAt:now+6000}).responseCurrent,false);
  assert.equal(projectRealtimeMonitor(value,opts).activity.length,0);
});
test('feed receipt is independent of display polling and timeframes remain honest',()=>{
  assert.match(project({feedObservations:[{route:'BINANCE_SPOT',streams:[{socketPresent:true,lastFrameAt:iso(-61000)}]}]}).state.title,/시세 수신/);
  assert.match(project({feedObservations:[{route:'BINANCE_SPOT',streams:[{socketPresent:false,lastFrameAt:iso(0)}]}]}).state.title,/시세 수신/);
  const unknown=project({strategyRows:[{timeframe:'5m',decisionCount:0,orderAttemptCount:0}]});
  assert.equal(unknown.state.tone,'neutral');assert.doesNotMatch(unknown.state.detail,/1~2시간/);
  const current=project({feedObservations:[{route:'BINANCE_SPOT',streams:[{socketPresent:true,lastFrameAt:iso(-1000)}]}]});
  assert.equal(current.feedsRecent,true);assert.equal(current.state.tone,'active');
});


test('terminal failure keeps its warning marker even when viewing a historical result',()=>{
  const value=normalizeShortLiveSnapshot(raw({status:'TERMINATED',terminalVerified:true,naturalExitObserved:true,firstFailure:'NETWORK_TIMEOUT'}));
  assert.equal(projectRealtimeMonitor(value,opts).resultHasIssue,true);
  assert.equal(projectRealtimeMonitor(value,opts).state.tone,'warning');
  assert.equal(projectRealtimeMonitor(value,opts).allAccountsCurrent,false);
  assert.equal(projectRealtimeMonitor(value,{...opts,fresh:false}).resultHasIssue,true);
});

test('first failure keeps safe API diagnostics independently of later cleanup success',()=>{
  const result=project({status:'TERMINATED',terminalVerified:true,naturalExitObserved:true,firstFailure:'OFFICIAL_READ_REJECTED',
    stopReason:'CLEANUP_COMPLETED',firstFailureDiagnostics:[{provider:'binance-futures',path:'/fapi/v1/exchangeInfo',elapsedMs:8001,
      status:'UNAVAILABLE',failureKind:'TIMEOUT',httpStatus:429,brokerCode:-1003,body:'private raw response',headers:{secret:'private'}}]});
  assert.equal(result.resultHasIssue,true);assert.match(result.firstFailure,/거절/);
  assert.equal(result.failureDiagnostics[0].providerLabel,'Binance USDT 선물');
  assert.match(result.failureDiagnostics[0].resultLabel,/HTTP 429.*응답 시간 초과/);
  assert.equal(result.failureDiagnostics[0].elapsedLabel,'8.0초');
  assert.equal(result.failureDiagnostics[0].brokerCode,-1003);
  assert.doesNotMatch(JSON.stringify(result.failureDiagnostics),/private|headers|body/);
  assert.deepEqual(project({firstFailure:'OFFICIAL_READ_REJECTED'}).failureDiagnostics,[]);
  assert.deepEqual(project({firstFailureDiagnostics:[{status:'ERROR'}]}).failureDiagnostics,[]);
});

test('API diagnostic fields reject URLs, queries, raw messages and invalid numbers',()=>{
  const result=project({firstFailure:'OFFICIAL_READ_REJECTED',firstFailureDiagnostics:[null,{
    provider:'private provider',path:'/fapi/v1/account?signature=private',elapsedMs:'8000',
    status:'Authorization: private',failureKind:'HTTP error private',httpStatus:0,brokerCode:'message: private'}]});
  assert.deepEqual(result.failureDiagnostics,[]);
  const valid={provider:'binance-futures',path:'/fapi/v1/exchangeInfo',elapsedMs:10,status:'UNAVAILABLE'};
  const invalid=project({firstFailure:'OFFICIAL_READ_REJECTED',firstFailureDiagnostics:[{...valid,brokerCode:'-1003',httpStatus:true,failureKind:'private-message'}]}).failureDiagnostics[0];
  assert.equal(invalid.brokerCode,null);assert.equal(invalid.httpStatus,null);assert.equal(invalid.failureKind,'');
  for (const change of [{provider:'binance'},{path:'/fapi/v1/private-identity'},{elapsedMs:-1},{elapsedMs:true},{status:'ERROR'}]) {
    assert.deepEqual(project({firstFailure:'OFFICIAL_READ_REJECTED',firstFailureDiagnostics:[{...valid,...change}]}).failureDiagnostics,[]);
  }
  assert.equal(project({firstFailure:'OFFICIAL_READ_REJECTED',firstFailureDiagnostics:Array.from({length:40},()=>valid)}).failureDiagnostics.length,32);
  const context=project({firstFailure:'OFFICIAL_READ_REJECTED',firstFailureDiagnostics:[{...valid,status:'AVAILABLE'}]}).failureDiagnostics;assert.equal(context.length,1);assert.equal(context[0].status,'AVAILABLE');assert.match(context[0].resultLabel,/HTTP 조회 완료/);
});

test('missing or duplicated selected route never becomes a complete equity or feed aggregate',()=>{
  const row={route:'BINANCE_SPOT',accountObservedAt:iso(0),monitorEquity:{basis:'OBSERVED_NATIVE_EQUITY_CHANGE',baselineNative:'100',currentNative:'105',krwPerQuote:'1400'}};
  const feed={route:'BINANCE_SPOT',streams:[{socketPresent:true,lastFrameAt:iso(0)}]};
  const missing=project({strategyRows:[{route:'BINANCE_SPOT'},{route:'UPBIT_KRW_SPOT'}],brokerRows:[row],feedObservations:[feed]});
  assert.equal(missing.totalChangeKrw,null);assert.equal(missing.allAccountsCurrent,false);assert.equal(missing.feedsRecent,false);
  const duplicate=project({brokerRows:[row,row],feedObservations:[feed,feed]});
  assert.equal(duplicate.totalChangeKrw,null);assert.equal(duplicate.feedsRecent,false);
});
