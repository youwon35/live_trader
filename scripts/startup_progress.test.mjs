import test from 'node:test';import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';import {createRequire} from 'node:module';import {runInNewContext} from 'node:vm';
import React from 'react';import {renderToStaticMarkup} from 'react-dom/server';import {transformSync} from 'esbuild';
import {startupInitial,startupTransition,startupView,startupAccountObservation,startupStrategyObservation} from '../src/startupProgressModel.js';
const done=(state,id,at=100)=>startupTransition(state,{type:'done',id,at});
test('elapsed time never manufactures startup progress, and independent completed reads count once',()=>{
 let state=startupInitial();assert.equal(startupView(state).percent,0);
 state=startupTransition(state,{type:'begin',id:'connection',at:600000});assert.equal(startupView(state).percent,0);
 state=done(state,'preferences');assert.equal(startupView(state).percent,25);
 assert.equal(startupView(done(state,'preferences')).percent,25);
 state=done(done(state,'connection'),'strategies');assert.equal(startupView(state).ready,false);
 state=done(state,'accounts',700000);assert.equal(startupView(state).percent,100);assert.equal(state.completedAt,700000);
});
test('a failed read remains incomplete until its own successful retry; invalid completion time is rejected',()=>{
 let state=done(done(done(startupInitial(),'connection'),'preferences'),'strategies');
 state=startupTransition(state,{type:'fail',id:'accounts',at:200,detail:'safe'});
 assert.equal(startupView(state).percent,75);assert.equal(startupView(state).failed,true);assert.equal(startupView(state).ready,false);
 for(const at of [undefined,NaN,Infinity,-1])assert.equal(startupView(startupTransition(state,{type:'done',id:'accounts',at})).ready,false);
 state=done(state,'accounts',300);assert.equal(startupView(state).ready,true);
 assert.equal(startupTransition(state,{type:'fail',id:'connection',at:500}),state);
});
test('completed account reads do not grant reconciliation approval or infer deposits from historical differences',()=>{
 const summary={account_count:4,error_count:0,api_required_count:0,last_run:'2026-10-30T00:00:00Z',mismatch_count:5,status:'mismatch'};
 const accounts=Array.from({length:4},()=>({status:"mismatch"}));
 const result={ok:false,snapshot:{reconciliation:{summary,accounts}}};assert.equal(startupAccountObservation(result).ok,true);
 assert.equal(result.ok,false);assert.equal(summary.mismatch_count,5);
 assert.equal(startupAccountObservation({ok:true,snapshot:{reconciliation:{summary,accounts:[]}}}).ok,false);
 for(const invalid of [null,{}, {...summary,account_count:0},{...summary,account_count:'4'},{...summary,error_count:1},{...summary,api_required_count:1},{...summary,last_run:'invalid'},{...summary,error_count:undefined}]){
   assert.equal(startupAccountObservation({ok:true,snapshot:{reconciliation:{summary:invalid,accounts}}}).ok,false);
 }
});
const require=createRequire(import.meta.url);
const jsx=readFileSync(new URL('../src/StartupProgress.jsx',import.meta.url),'utf8');
const compiled=transformSync(jsx.replace(/^import '\.\/StartupProgress.css';\s*$/m,''),{loader:'jsx',format:'cjs',jsx:'automatic'}).code;
const box={exports:{}};const localRequire=name=>name==='./startupProgressModel.js'?{startupInitial,startupTransition,startupView}:require(name);
runInNewContext(compiled,{module:box,exports:box.exports,require:localRequire,performance:{now:()=>1000},setInterval,clearInterval});
const Component=box.exports.default;
test('actual startup component exposes truthful progress and explicit execution boundary',()=>{
 const view={...startupView(startupInitial()),elapsedSeconds:12};
 const html=renderToStaticMarkup(React.createElement(Component,{view}));
 assert.match(html,/value="0"/);assert.match(html,/0\/4단계 완료/);assert.match(html,/12초 경과/);assert.match(html,/아래 실거래 시작 점검을 실행하세요/);
 let state=done(done(done(done(startupInitial(),'accounts'),'connection'),'preferences'),'strategies');
 const ready=renderToStaticMarkup(React.createElement(Component,{view:{...startupView(state),elapsedSeconds:15}}));
 assert.match(ready,/초기 점검 완료/);assert.match(ready,/value="100"/);assert.match(ready,/4\/4단계 완료/);assert.doesNotMatch(ready,/100%/);assert.match(ready,/aria-expanded="false"/);assert.doesNotMatch(ready,/<ol>/);
});
test('actual App wiring observes existing reads without adding broker polling or mutating authority',()=>{
 const app=readFileSync(new URL('../src/App.jsx',import.meta.url),'utf8');
 assert.equal(app.match(/await getSnapshot\(\)/g).length,1);
 assert.equal(app.match(/accountRefreshCoordinatorRef\.current\.run\(\)/g).length,1);
 assert.match(app,/startup\.finish\("connection", true/);
 assert.match(app,/startup\.finish\("accounts", startupRead\.ok, startupRead\.detail, startupRead\)/);
 assert.match(app,/startup\.finish\("preferences", true/);
 assert.match(app,/startupStrategyObservation\(next\)/);
 assert.match(app,/selectedNav === "overview" && <StartupProgress[^\n]+onNavigate=\{navigateWorkspace\}/);
 assert.match(app,/onRetryAccounts=\{snapshot\.api_connected \? refreshBrokerAccounts : undefined\}/);
 assert.match(app,/startupAccountReadRef\.current\(snapshot\.api_connected === true, refreshBrokerAccounts\)/);
 assert.doesNotMatch(jsx,/\/api\/|fetch\(|startShortLive|onStart|onConfirm|killSwitch/);
});


test('an empty strategy list is checked, stays visible, and links to the strategy workspace', () => {
 const evidence = startupStrategyObservation({ strategies: [] });
 assert.equal(evidence.review, true);
 assert.equal(startupStrategyObservation({ strategies: [{ strategy_id: 'draft' }] }).detail, '저장된 전략 목록 1개를 불러왔습니다.');
 const otherSteps = done(done(done(startupInitial(), 'connection'), 'preferences'), 'accounts');
 const checked = startupTransition(otherSteps, { type: 'review', id: 'strategies', at: 120, detail: evidence.detail });
 const view = startupView(checked);
 assert.equal(view.percent, 100);
 assert.equal(view.review, true);
 const html = renderToStaticMarkup(React.createElement(Component, { view: { ...view, elapsedSeconds: 1 }, onNavigate() {} }));
 assert.match(html, /등록된 운용 전략이 없습니다/);
 assert.match(html, /운용 전략 열기/);
 assert.match(html, /data-status="review"/);
 const updated = done(checked, 'strategies', 140);
 assert.equal(startupView(updated).review, false);
 assert.equal(updated.completedAt, 120);
});

test('missing account credentials link to connection settings and incomplete results link to account details', () => {
 const summary = { account_count: 1, error_count: 1, api_required_count: 1, last_run: '2026-10-30T00:00:00Z' };
 const result = { ok: false, snapshot: { reconciliation: { summary, accounts: [{ status: 'unchecked' }] } } };
 const observation = startupAccountObservation(result);
 assert.equal(observation.route, 'settings');
 const state = startupTransition(startupInitial(), { type: 'fail', id: 'accounts', at: 90, ...observation });
 const html = renderToStaticMarkup(React.createElement(Component, {
  view: startupView(state), onNavigate() {}, onRetryAccounts() {},
 }));
 assert.match(html, /설정 열기/);
 assert.match(html, /초기 계좌·잔고 조회 다시 조회/);
 const brokerError = startupAccountObservation({ ...result, snapshot: {
  reconciliation: { summary: { ...summary, api_required_count: 0 }, accounts: [{}] },
 } });
 assert.equal(brokerError.ok, false);
 assert.equal(brokerError.route, undefined);
 const syncWarning = startupAccountObservation({ ...result, syncWarning: 'backend error text', snapshot: {
  reconciliation: { summary: { ...summary, error_count: 0, api_required_count: 0 }, accounts: [{}] },
 } });
 assert.equal(syncWarning.ok, true);
 assert.equal(syncWarning.review, true);
 assert.doesNotMatch(syncWarning.detail, /backend error text/);
});
