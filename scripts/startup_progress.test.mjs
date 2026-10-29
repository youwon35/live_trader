import test from 'node:test';import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';import {createRequire} from 'node:module';import {runInNewContext} from 'node:vm';
import React from 'react';import {renderToStaticMarkup} from 'react-dom/server';import {transformSync} from 'esbuild';
import {startupInitial,startupTransition,startupView,startupAccountObservation} from '../src/startupProgressModel.js';
const done=(state,id,at=100)=>startupTransition(state,{type:'done',id,at});
test('elapsed time never manufactures startup progress, and independent completed reads count once',()=>{
 let state=startupInitial();assert.equal(startupView(state).percent,0);
 state=startupTransition(state,{type:'begin',id:'connection',at:600000});assert.equal(startupView(state).percent,0);
 state=done(state,'preferences');assert.equal(startupView(state).percent,33);
 assert.equal(startupView(done(state,'preferences')).percent,33);
 state=done(state,'connection');assert.equal(startupView(state).ready,false);
 state=done(state,'accounts',700000);assert.equal(startupView(state).percent,100);assert.equal(state.completedAt,700000);
});
test('a failed read remains incomplete until its own successful retry; invalid completion time is rejected',()=>{
 let state=done(done(startupInitial(),'connection'),'preferences');
 state=startupTransition(state,{type:'fail',id:'accounts',at:200,detail:'safe'});
 assert.equal(startupView(state).percent,66);assert.equal(startupView(state).failed,true);assert.equal(startupView(state).ready,false);
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
 assert.match(html,/value="0"/);assert.match(html,/0\/3단계 완료/);assert.match(html,/12초 경과/);assert.match(html,/주문 시작·계좌 대조 승인·시장 데이터 준비를 뜻하지 않습니다/);
 let state=done(done(done(startupInitial(),'accounts'),'connection'),'preferences');
 const ready=renderToStaticMarkup(React.createElement(Component,{view:{...startupView(state),elapsedSeconds:15}}));
 assert.match(ready,/초기 조회 완료/);assert.match(ready,/aria-expanded="false"/);assert.doesNotMatch(ready,/<ol>/);
});
test('actual App wiring observes existing reads without adding broker polling or mutating authority',()=>{
 const app=readFileSync(new URL('../src/App.jsx',import.meta.url),'utf8');
 assert.equal(app.match(/await getSnapshot\(\)/g).length,1);
 assert.equal(app.match(/accountRefreshCoordinatorRef\.current\.run\(\)/g).length,1);
 assert.match(app,/startup\.finish\("connection", true/);
 assert.match(app,/startup\.finish\("accounts", startupRead\.ok, startupRead\.detail\)/);
 assert.match(app,/startup\.finish\("preferences", true/);
 assert.match(app,/onRetryAccounts=\{refreshBrokerAccounts\}/);
 assert.match(app,/startupAccountReadRef\.current\(snapshot\.api_connected === true, refreshBrokerAccounts\)/);
 assert.doesNotMatch(jsx,/\/api\/|fetch\(|startShortLive|onStart|onConfirm|killSwitch/);
});
