import test from 'node:test';
import assert from 'node:assert/strict';
import {environmentStatus, ordinaryEntryStatus, currentOrderPermission, ordinaryRuntimeActive, ordinaryRuntimeKnown, activeOrderCountLabel, brokerAccountObservation} from '../src/statusTruth.js';
const now=Date.parse('2026-10-28T00:00:00Z');
const base=()=>({api_connected:true,kill_switch:false,new_entries_blocked:true,readiness:[{label:'실거래 라우트 환경',status:'pass'}],continuous_runtime:{profiles:{}},live_governance:{}});
test('allowed current settings are not mislabeled locked when no preflight or worker exists',()=>{
 const result=environmentStatus(base(),'a',now); assert.equal(result.items[0].value,'시작 시 한 번');assert.equal(result.items[0].label,'실거래 확인');
 assert.equal(result.items[1].value,'점검 필요');assert.equal(result.items.length,3);
 assert.equal(ordinaryRuntimeActive(base()),false);assert.equal(result.valid,false);
});
test('actual running ordinary guard is shown while idle guard is hidden',()=>{
 const s=base();s.continuous_runtime.profiles.crypto={running:true};
 assert.equal(environmentStatus(s,'a',now).items.find(x=>x.label.includes('신규')).value,'차단');
 s.new_entries_blocked=false; assert.equal(environmentStatus(s,'a',now).items.find(x=>x.label.includes('신규')).value,'허용');
});
test('disconnection cannot display cached settings or normal emergency status as current',()=>{
 const s={...base(),api_connected:false};assert.equal(currentOrderPermission(s),null);
 assert.ok(environmentStatus(s,'a',now).items.every(x=>x.value==='미확인'));
 s.kill_switch=true;assert.equal(environmentStatus(s,'a',now).items.at(-1).value,'작동 중');
});
test('preflight must belong to selected deployment and not be expired or non-PASS',()=>{
 const s=base();s.live_governance={deploymentId:'a',preflightValidity:{valid:true},latestPreflight:{deploymentId:'a',status:'PASS',expiresAt:new Date(now+1).toISOString()}};
 assert.equal(environmentStatus(s,'a',now).valid,true);assert.equal(environmentStatus(s,'b',now).valid,false);
 assert.equal(environmentStatus(s,'a',now+1).valid,false);s.live_governance.latestPreflight.status='FAIL';assert.equal(environmentStatus(s,'a',now).valid,false);
});
test('API key entry and another account observation never establish this account success',()=>{
 const s={...base(), accounts:[{broker_id:'upbit',broker_cash_value:0,lookup_status:'success',observed_at:new Date(now).toISOString(),account_observation_fresh:true,observation_max_age_seconds:60}],reconciliation:{summary:{last_run:new Date(now).toISOString()},errors:[]}};
 assert.equal(brokerAccountObservation(s,'upbit',now).status,'pass');
 assert.equal(brokerAccountObservation(s,'kis',now).status,'warn');
 s.reconciliation.errors=[{broker_id:'upbit'}];assert.equal(brokerAccountObservation(s,'upbit',now).status,'fail');
 s.reconciliation.errors=[];assert.equal(brokerAccountObservation(s,'upbit',now+300001).status,'warn');
 assert.equal(brokerAccountObservation({...s,api_connected:false},'upbit',now).status,'warn');
});

test('missing runtime and queue counters stay unknown, aggregate running is not idle',()=>{
 assert.equal(ordinaryRuntimeKnown(base()),false);
 const s={...base(),continuous_runtime:{running:true}};
 assert.equal(ordinaryRuntimeActive(s),true);assert.equal(ordinaryRuntimeKnown(s),true);
 for(const active of [undefined,null,-1,'0'])assert.equal(activeOrderCountLabel({...s,order_queue:{active}}),'미결 주문 미확인');
 assert.equal(activeOrderCountLabel({...s,order_queue:{active:0}}),'미결 주문 0');
});

test('running header never treats missing entry guard as permission',()=>{
 for(const value of [undefined,null,'false']){
  const result=ordinaryEntryStatus({...base(),continuous_runtime:{running:true},new_entries_blocked:value});
  assert.equal(result.label,'일반 운용 신규 진입 미확인');assert.equal(result.tone,'warning');
 }
 assert.equal(ordinaryEntryStatus({...base(),new_entries_blocked:false}).label,'일반 운용 신규 진입 허용');
 assert.equal(ordinaryEntryStatus({...base(),api_connected:false,new_entries_blocked:false}).tone,'warning');
});

test('another broker refresh never renews an old or undated account observation',()=>{
 const s={...base(),accounts:[{broker_id:'binance',broker_cash_value:10,lookup_status:'success',observed_at:new Date(now-61000).toISOString(),account_observation_fresh:true,observation_max_age_seconds:60}],reconciliation:{summary:{last_run:new Date(now).toISOString()},errors:[]}};
 assert.equal(brokerAccountObservation(s,'binance',now).status,'warn');
 delete s.accounts[0].observed_at;assert.equal(brokerAccountObservation(s,'binance',now).status,'warn');
 s.accounts[0].observed_at=new Date(now).toISOString();s.accounts[0].lookup_status='unknown';assert.equal(brokerAccountObservation(s,'binance',now).status,'warn');
});
test('a position-query failure does not mislabel a successful account lookup as failed API authentication',()=>{
 const account={broker_id:'upbit',broker_cash_value:0,lookup_status:'success',observed_at:new Date(now).toISOString(),account_observation_fresh:true,observation_max_age_seconds:60};
 const s={...base(),accounts:[account],reconciliation:{errors:[{broker_id:'upbit',scope:'positions'}]}};
 assert.equal(brokerAccountObservation(s,'upbit',now).status,'pass');
 s.accounts[0].lookup_status='error';assert.equal(brokerAccountObservation(s,'upbit',now).status,'fail');
});
