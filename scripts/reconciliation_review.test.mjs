import assert from 'node:assert/strict';
import test from 'node:test';
import {liveBrokerBalanceDisplay,liveReconciliationDisplay,liveReconciliationIssues,liveExecutionDisplay} from '../src/liveReconciliationDisplay.js';

const fixture=()=>({api_connected:true,reconciliation:{summary:{last_run:'2026-10-29T07:00:00Z',account_count:1,position_count:0,
 mismatch_count:1,api_required_count:0,error_count:0,status:'fail',status_label:'불일치'},accounts:[{broker_id:'upbit',
 currency:'KRW',symbol:'',position_side:'',program_source:'event_poll',program_updated_at:'2026-07-30 02:30:36',
 status:'mismatch',program_cash_value:100,broker_cash_value:120}],positions:[],errors:[]},
 reconciliation_review:{items:[{fingerprint:'a'.repeat(64),kind:'cash',brokerId:'upbit',currency:'KRW',symbol:'',side:'',
 programSource:'event_poll',baselineAt:'2026-07-30 02:30:36',before:100,after:120,classification:'HISTORICAL_BALANCE_REFERENCE',acknowledged:false}]}});

test('last manual account read remains a dated observation, never current authority',()=>{
 const view=liveBrokerBalanceDisplay(fixture());assert.equal(view.value,'마지막 조회값 보관');assert.equal(view.status,'info');
 assert.match(view.detail,/자동 반복 조회하지 않음/);assert.match(view.detail,/2026-10-29/);
 assert.equal(liveBrokerBalanceDisplay(fixture(),{fresh:true}).value,'조회됨');
 const failed=fixture();failed.reconciliation.summary.error_count=1;
 assert.equal(liveBrokerBalanceDisplay(failed).value,'조회 결과 확인 필요');
});

test('only exact annotated history moves to reviewed; raw mismatches preserved',()=>{
 const data=fixture(),before=structuredClone(data.reconciliation);
 assert.equal(liveReconciliationIssues(data)[0].reviewed,false);
 data.reconciliation_review.items[0].acknowledged=true;
 assert.equal(liveReconciliationIssues(data)[0].reviewed,true);
 const view=liveReconciliationDisplay(data);assert.equal(view.label,'과거 차이 · 사용자 확인');
 assert.equal(view.status,'info');assert.equal(view.rawStatus,'fail');assert.equal(view.reviewedCount,1);
 assert.deepEqual(data.reconciliation,before);
});

test('changed quantity/basis/source/side, duplicate, orphan and disconnected reviews do not hide differences',()=>{
 for(const [key,value] of [['broker_cash_value',121],['program_cash_value',99],['program_updated_at','2026-07-31 02:30:36'],
 ['program_source','execution_fill'],['position_side','LONG']]) {
  const data=fixture();data.reconciliation_review.items[0].acknowledged=true;data.reconciliation.accounts[0][key]=value;
  assert.equal(liveReconciliationIssues(data)[0].reviewed,undefined,key);assert.equal(liveReconciliationDisplay(data).status,'fail',key);
 }
 for(const change of [s=>s.reconciliation_review.items.push({...s.reconciliation_review.items[0]}),
 s=>s.reconciliation_review.items[0].brokerId='binance',s=>{s.api_connected=false;}]){
  const data=fixture();data.reconciliation_review.items[0].acknowledged=true;change(data);
  assert.equal(liveReconciliationIssues(data)[0].reviewed,undefined);assert.equal(liveReconciliationDisplay(data).status,'fail');
 }
});

test('successful old REST read differs from missing, failed and future read',()=>{
 const nowMs=Date.parse('2026-10-29T08:00:00Z');
 const data={execution_events:{snapshot_poll:{brokers:{upbit:{last_status:'unchanged',last_success_at:'2026-10-29T07:00:00Z'}}}}};
 const view=liveExecutionDisplay(data,{nowMs});assert.equal(view.value,'마지막 REST 조회 보관');assert.equal(view.status,'info');
 data.execution_events.snapshot_poll.brokers.upbit.last_error='failed';assert.equal(liveExecutionDisplay(data,{nowMs}).value,'조회 오류 확인');
 data.execution_events.snapshot_poll.brokers.upbit={last_status:'skipped',last_attempt_at:'2026-10-29T07:00:00Z'};
 assert.equal(liveExecutionDisplay(data,{nowMs}).value,'체결 감시 미시작');
 data.execution_events.snapshot_poll.brokers.upbit.last_success_at='2026-10-29T09:00:00Z';
 assert.equal(liveExecutionDisplay(data,{nowMs}).value,'체결 감시 미시작');
});
