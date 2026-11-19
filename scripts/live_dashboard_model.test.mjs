import test from 'node:test';
import assert from 'node:assert/strict';
import {appendAccountObservations,equitySeries,projectContinuousDashboard} from '../src/liveDashboardModel.js';
const now=Date.parse('2026-10-27T03:00:00Z'),at=ms=>new Date(now+ms).toISOString();
const sample=(change={})=>({route:'BINANCE_SPOT',observedAt:at(-1000),baselineNative:20,currentNative:21,quoteCurrency:'USDT',basis:'OBSERVED_NATIVE_EQUITY_CHANGE',...change});
test('curves use real same-route samples, preserving negative and zero without fake gaps',()=>{
  const points=equitySeries([sample({observedAt:at(-180000),currentNative:19}),sample({observedAt:at(-3000),currentNative:20}),sample(),sample({route:'UPBIT_KRW_SPOT',currentNative:999})],'BINANCE_SPOT',now);
  assert.deepEqual(points.map(p=>p.value),[-1,0,1]);assert.equal(points[1].at-points[0].at,177000);
});
test('missing, numeric-string, future and unsupported profit sources do not invent equity',()=>{
  assert.deepEqual(equitySeries([sample({currentNative:null}),sample({currentNative:'21'}),sample({observedAt:at(10000)}),sample({basis:'ASSUMED'})],'BINANCE_SPOT',now),[]);
});
test('changed baseline and currency form independent observations and duplicate timestamps stay one',()=>{
  const points=equitySeries([sample({observedAt:at(-6000)}),sample({observedAt:at(-5000),quoteCurrency:'KRW'}),sample({baselineNative:25,currentNative:26}),sample({baselineNative:25,currentNative:27})],'BINANCE_SPOT',now);
  assert.equal(points.length,1);assert.equal(points[0].value,2);
});
test('RUNNING is not fresh feed health and a disconnected response never claims healthy',()=>{
  const row={running:true,brokerId:'binance',lastHeartbeat:at(-1000),lastDataAt:at(-1000),allFeedsReady:true,engine:{decisionCount:0,barCount:0}};
  let view=projectContinuousDashboard({api_connected:true,continuous_runtime:{runtimes:{one:row}}},now);
  assert.equal(view.rows[0].state,'엔진 응답 정상');assert.equal(view.rows[0].decisions,0);
  view=projectContinuousDashboard({api_connected:false,continuous_runtime:{runtimes:{one:row}}},now);
  assert.equal(view.rows[0].state,'현재 상태 미확인');assert.equal(view.rows[0].heartbeatCurrent,false);
  view=projectContinuousDashboard({api_connected:true,continuous_runtime:{runtimes:{one:{...row,lastDataAt:at(-90000)},two:{...row,lastHeartbeat:at(10000)}}}},now);
  assert.equal(view.rows[0].state,'엔진 응답 정상');assert.equal(view.rows[1].state,'엔진 응답 확인 필요');
});
test('separate deployment identity and missing metrics remain separate and unknown',()=>{
  const view=projectContinuousDashboard({api_connected:true,continuous_runtime:{runtimes:{a:{deploymentId:'btc',brokerId:'binance',running:true},b:{deploymentId:'samsung',brokerId:'kis',running:false}}}},now);
  assert.equal(view.rows.length,2);assert.equal(view.rows[0].brokerLabel,'Binance 현물');assert.equal(view.rows[1].brokerLabel,'한국투자증권');assert.equal(view.rows[1].decisions,null);
});

test('continuous strategy observation joins exact provider, symbol, timeframe and instance',()=>{
 const strategy={strategyInstanceId:'exact',symbol:'BTCUSDT',provider:'binance',timeframe:'1h',pluginId:'ma'};
 const engine={strategies:[strategy],latestBars:{valid:{symbol:'BTCUSDT',provider:'binance',timeframe:'1h',close:100,endTime:at(-1000)},foreign:{symbol:'BTCUSDT',provider:'binance-futures',timeframe:'1h',close:999}},recentCycles:[{occurredAt:at(-1000),decisions:[{strategyInstanceId:'foreign',signal:'SELL'},{strategyInstanceId:'exact',signal:'BUY'}]}]};
 const snapshot={api_connected:true,continuous_runtime:{runtimes:{a:{engine}}}};
 let result=projectContinuousDashboard(snapshot,now).rows[0].strategies[0];
 assert.equal(result.lastPrice,100);assert.equal(result.signal,'BUY');
 engine.latestBars.duplicate={...engine.latestBars.valid,close:101};
 result=projectContinuousDashboard(snapshot,now).rows[0].strategies[0];assert.equal(result.lastPrice,null);
});

test('guard failures and incomplete feed readiness cannot look healthy',()=>{
 const row={running:true,lastHeartbeat:at(-1000),lastDataAt:at(-3500000),allFeedsReady:true};
 for (const patch of [{phase:'DEGRADED'},{marketClockIntegrity:{failureCodes:['CLOCK']}},{sourceIntegrity:{failureCodes:['SOURCE']}},{allFeedsReady:false},{deliveryReadinessSatisfied:false},{allFeedsReady:undefined}]) {
  const view=projectContinuousDashboard({api_connected:true,continuous_runtime:{runtimes:{one:{...row,...patch}}}},now);
  assert.notEqual(view.rows[0].tone,'success');assert.equal(view.issueCount,1);
 }
 const view=projectContinuousDashboard({api_connected:true,continuous_runtime:{runtimes:{one:row}}},now);
 assert.equal(view.rows[0].tone,'success');assert.equal(view.rows[0].state,'엔진 응답 정상');
});
test('a returning baseline cannot stitch across an intervening different account series',()=>{
 const points=equitySeries([sample({observedAt:at(-5000)}),sample({observedAt:at(-3000),baselineNative:25}),sample()],'BINANCE_SPOT',now);
 assert.equal(points.length,1);assert.equal(points[0].at,now-1000);
});

test('control tower separates watch scope, account equity and exact decision outcome',()=>{
 const spec={strategyInstanceId:'toss-one',symbol:'005930',provider:'toss',timeframe:'5m'};
 const runtime={running:true,brokerId:'toss',deploymentId:'owned',startedAt:at(-20000),lastHeartbeat:at(-300),allFeedsReady:true,
  engine:{strategies:[spec],decisionCount:4,recentCycles:[
   {occurredAt:at(-4000),decisions:[{strategyInstanceId:'toss-one',strategyId:'same',signal:'BUY',reason:'이동평균 교차',
    bar:{close:141500,endTime:at(-5000)}}],handlerResult:{results:[{strategyInstanceId:'toss-one',action:'BLOCKED',ok:false,reason:'주문 한도 확인'}]}},
   {occurredAt:at(-30000),decisions:[{strategyInstanceId:'toss-one',strategyId:'same',signal:'SELL'}]},
  ]}};
 const view=projectContinuousDashboard({api_connected:true,continuous_runtime:{runtimes:{a:runtime,other:{brokerId:'kis',running:false,engine:runtime.engine}}},
  order_queue:{active:2},account_risk:{budgets:{
   readable:{broker_id:'toss',currency:'KRW',day:'2026-10-27',observed_at:at(-500),starting_equity:200000,current_equity:210000},
   unrelated:{broker_id:'kis',currency:'KRW',day:'2026-10-27',observed_at:at(-500),starting_equity:1,current_equity:10},
   stale:{broker_id:'toss',currency:'USD',day:'2026-10-26',observed_at:at(-200000),starting_equity:0,current_equity:1},
  }},orders:[
   {order_id:'owned-order',broker_id:'toss',deployment_id:'owned',symbol:'005930',state:'rejected',reason:'승인 거절',updated_at:at(-3000)},
   {order_id:'other-order',broker_id:'toss',deployment_id:'other',symbol:'005930',state:'filled',updated_at:at(-1000)},
  ]},now);
 assert.equal(view.active,1);assert.equal(view.symbols,1);
 assert.equal(view.watch[0].brokerLabel,'토스증권');assert.equal(view.pending,2);
 assert.deepEqual(view.accounts.map(row=>[row.currency,row.percent,row.current]),[['KRW',5,true],['USD',null,false]]);
 assert.deepEqual(view.recent.map(row=>row.label),['주문 거절','매수 신호']);
 assert.equal(view.details[1].price,141500);assert.equal(view.details[1].outcome,'주문 한도 확인');
 assert.equal(view.details[1].issue,true);
});

test('a shared artifact cannot cross-assign a legacy decision outcome',()=>{
 const engine={strategies:[{strategyInstanceId:'a',symbol:'A'},{strategyInstanceId:'b',symbol:'B'}],
  recentCycles:[{occurredAt:at(-100),decisions:[{strategyInstanceId:'a',strategyId:'shared',signal:'HOLD'},{strategyInstanceId:'b',strategyId:'shared',signal:'SELL'}],
   handlerResult:{results:[{strategyId:'shared',ok:false,reason:'다른 인스턴스에 속할 수 있음'}]}}]};
 const view=projectContinuousDashboard({api_connected:false,order_queue:{active:3},continuous_runtime:{runtimes:{s:{running:true,engine}}}},now);
 assert.equal(view.pending,null);assert.equal(view.details.length,2);
 assert.equal(view.details.every(row=>!row.outcome && !row.issue),true);
});

test('account display history deduplicates observations and resets when baseline changes',()=>{
 const first={broker:'kis',currency:'KRW',start:100,day:'2026-10-27',at:at(-900),percent:1,current:true};
 const history=appendAccountObservations({},[first]);
 assert.equal(appendAccountObservations(history,[first,{...first,at:at(-50),current:false}]),history);
 const second={...first,at:at(-500),percent:-1};
 const two=appendAccountObservations(history,[second]);
 assert.deepEqual(two['kis:KRW'].map(point=>point.percent),[1,-1]);
 const revised=appendAccountObservations(two,[{...first,start:200,at:at(0),percent:2}]);
 assert.deepEqual(revised['kis:KRW'].map(point=>point.percent),[2]);
});
