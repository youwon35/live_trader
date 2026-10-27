import test from 'node:test';
import assert from 'node:assert/strict';
import { createSnapshotResponseGate, createSnapshotReadCoordinator, initialAccountRefreshState, accountRefreshResultState, accountObservationView } from '../src/snapshotRefreshState.js';

const now=Date.parse('2026-10-28T04:00:00Z');
const snap=(delta=0)=>({api_connected:true,generated_at:new Date(now+delta).toISOString(),reconciliation:{summary:{last_run:new Date(now+delta).toISOString(),account_count:2,error_count:0,api_required_count:0}}});
test('slow older snapshot cannot overwrite a newer account observation',()=>{
 const gate=createSnapshotResponseGate(),poll=gate.begin(),account=gate.begin();
 assert.equal(gate.accept(account,snap(1000)),true);
 assert.equal(gate.accept(poll,snap()),false);
 assert.equal(gate.acceptFailure(poll),false);
});
test('slow account request can publish newer actual snapshot after a later cached poll',()=>{
 const gate=createSnapshotResponseGate(),account=gate.begin(),poll=gate.begin();
 assert.equal(gate.accept(poll,snap()),true);
 assert.equal(gate.accept(account,snap(1000)),true);
 assert.equal(gate.accept(poll,snap()),false);
});
test('same-second responses use request order and old timeout cannot undo recovery',()=>{
 const gate=createSnapshotResponseGate(),old=gate.begin(),recovery=gate.begin();
 assert.equal(gate.accept(recovery,snap()),true);
 assert.equal(gate.accept(old,snap()),false);
 assert.equal(gate.acceptFailure(old),false);
 const failed=gate.begin();assert.equal(gate.acceptFailure(failed),true);
 assert.equal(gate.accept(recovery,snap()),false);
 assert.equal(gate.accept(gate.begin(),snap(1)),true);
});
test('newer request with older backend generated time cannot roll state back',()=>{
 const gate=createSnapshotResponseGate();assert.equal(gate.accept(gate.begin(),snap(1000)),true);
 assert.equal(gate.accept(gate.begin(),snap()),false);
});
test('missing timestamp never makes an older reply current',()=>{
 const gate=createSnapshotResponseGate(),a=gate.begin(),b=gate.begin();
 assert.equal(gate.accept(b,snap()),true);assert.equal(gate.accept(a,{}),false);
});
test('only completed full account query records success; partial or errors preserve prior success',()=>{
 const success=accountRefreshResultState(initialAccountRefreshState,{ok:true,snapshot:snap()});
 assert.equal(success.lastSuccessAt,snap().reconciliation.summary.last_run);
 for(const result of [{ok:false,reason:'잔고 조회 실패',snapshot:snap(1)},{ok:true,snapshot:{...snap(1),reconciliation:{summary:{...snap(1).reconciliation.summary,error_count:1}}}},{ok:true,snapshot:{...snap(1),reconciliation:{summary:{...snap(1).reconciliation.summary,account_count:0}}}}]){
   assert.equal(accountRefreshResultState(success,result).lastSuccessAt,success.lastSuccessAt);
 }
});
test('sync warning survives the separate successful account reconciliation',()=>{
 const result=accountRefreshResultState(initialAccountRefreshState,{ok:true,snapshot:snap(),syncWarning:'체결 동기화 실패 후 새 계좌 대조'});
 assert.match(result.warning,/체결 동기화 실패/);assert.equal(result.error,'');
});
test('account failure stays visible across unrelated successful snapshot polls',()=>{
 const state={...initialAccountRefreshState,error:'계좌 API 응답 시간이 45초를 초과했습니다.'};
 const view=accountObservationView(snap(),state,now);
 assert.equal(view.connected,true);assert.equal(view.fresh,false);assert.equal(view.label,'갱신 실패');assert.equal(view.tone,'fail');
});
test('disconnected, missing, stale and future observations never display current assets',()=>{
 for(const value of [{...snap(),api_connected:false},{api_connected:true},snap(-300001),snap(5001)]){
  assert.equal(accountObservationView(value,initialAccountRefreshState,now).fresh,false);
 }
 assert.equal(accountObservationView(snap(-300000),initialAccountRefreshState,now).fresh,true);
});
test('pending is explicit without turning historical data into successful fresh observation',()=>{
 const view=accountObservationView(snap(-400000),{...initialAccountRefreshState,pending:true},now);
 assert.equal(view.fresh,false);assert.equal(view.label,'계좌·체결 읽는 중');assert.equal(view.tone,'info');
});

test('account/snapshot shared ownership keeps simultaneous full snapshot work at one', async()=>{
 const coordinator=createSnapshotReadCoordinator();let releaseSnapshot,releaseAccount,active=0,maximum=0;
 const snapshotWait=new Promise(resolve=>releaseSnapshot=resolve),accountWait=new Promise(resolve=>releaseAccount=resolve),calls=[];
 const work=async(name,gate)=>{active++;maximum=Math.max(maximum,active);calls.push(name);await gate;active--;return name;};
 const first=coordinator.runSnapshot(()=>work('snapshot',snapshotWait));
 assert.strictEqual(coordinator.runSnapshot(()=>work('duplicate snapshot',snapshotWait)),first);
 await Promise.resolve();assert.equal(active,1);
 const second=coordinator.runAccount(()=>work('account',accountWait));
 assert.strictEqual(coordinator.runAccount(()=>work('duplicate account',accountWait)),second);
 assert.equal(await coordinator.runSnapshot(()=>work('skipped poll',snapshotWait)),undefined);
 assert.deepEqual(calls,['snapshot']);
 releaseSnapshot();assert.equal(await first,'snapshot');await Promise.resolve();
 assert.deepEqual(calls,['snapshot','account']);
 assert.equal(await coordinator.runSnapshot(()=>work('skipped poll 2',snapshotWait)),undefined);
 releaseAccount();assert.equal(await second,'account');assert.equal(maximum,1);
 assert.equal(await coordinator.runSnapshot(async()=> 'recovered'),'recovered');
});
test('snapshot failure releases a waiting account read; account failure releases future polling',async()=>{
 const coordinator=createSnapshotReadCoordinator();
 const first=coordinator.runSnapshot(async()=>{throw new Error('15s timeout');});
 const waiting=coordinator.runAccount(async()=> 'account read after completed timeout');
 await assert.rejects(first,/15s timeout/);assert.equal(await waiting,'account read after completed timeout');
 await assert.rejects(coordinator.runAccount(async()=>{throw new Error('account failed');}),/account failed/);
 assert.equal(await coordinator.runSnapshot(async()=> 'fresh'),'fresh');
});
