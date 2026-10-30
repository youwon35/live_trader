import assert from 'node:assert/strict';
import test from 'node:test';
import {reconciliationReferenceView,reconciliationReferenceKind,reconciliationDisplayIdle} from '../src/reconciliationReferenceView.js';
import {liveReconciliationDisplay,liveReconciliationIssues,liveLedgerDisplay} from '../src/liveReconciliationDisplay.js';

function snapshot(){const cash=[['kis','KRW'],['binance','USDT'],['binance-futures','USDT'],['upbit','KRW']].map(([broker_id,currency])=>({broker_id,currency,program_currency:currency,program_source:'event_poll',program_updated_at:'2026-07-30 02:30:36',program_cash_value:100,broker_cash_value:110,status:'mismatch'}));const positions=[{broker_id:'binance',symbol:'FDUSD',program_source:'missing',program_qty_value:0,broker_qty_value:.25,status:'mismatch'}];return {api_connected:true,continuous_runtime:{running:false,profiles:{}},short_live_test_status:{active:false},orders:[],order_queue:{active:0},reconciliation:{accounts:cash,positions,errors:[],summary:{last_run:'2026-10-30 04:53:43',account_count:4,position_count:1,mismatch_count:5,blocking_count:5,api_required_count:0,capability_gap_count:0,error_count:0,status:'fail',status_label:'불일치'}},program_ledger:{state:{last_baseline:null,last_baseline_source:''},cash:cash.map(row=>({...row,source:'event_poll',updated_at:row.program_updated_at})),positions:[],fill_event_count:0,snapshot_event_count:91468,other_event_count:0}};}

test('the four legacy cash deltas and broker-only spot holding are reference changes, not observed trading loss',()=>{
 const s=snapshot(),raw=structuredClone(s),view=reconciliationReferenceView(s);
 assert.equal(view.referenceOnly,true);assert.equal(view.referenceCount,5);assert.equal(view.actionableCount,0);
 assert.equal(view.cashFlowCauseVerified,false);assert.equal(view.executionAuthorityGranted,false);assert.deepEqual(s,raw);
 const display=liveReconciliationDisplay(s);assert.equal(display.status,'info');assert.match(display.label,/과거 기준/);assert.notEqual(display.label,'정상');assert.equal(display.rawStatus,'fail');
 const issues=liveReconciliationIssues(s);assert.equal(issues.length,5);assert.ok(issues.every(row=>row.presentationOnlyReference===true));assert.ok(issues.every(row=>row.status==='mismatch'));
 assert.doesNotMatch(view.detail,/입금 완료|출금 완료|정상 처리/);
});
test('historical reference projection never rewrites raw mismatch counts, amounts or approval state',()=>{
 const s=snapshot();s.operator_confirmed=false;s.new_entries_blocked=true;const before=JSON.stringify(s);
 liveReconciliationDisplay(s);liveReconciliationIssues(s);reconciliationReferenceView(s);
 assert.equal(JSON.stringify(s),before);assert.equal(s.reconciliation.summary.mismatch_count,5);assert.equal(s.new_entries_blocked,true);
});
test('managed positions and genuine broker errors stay actionable alongside the historical cash notes',()=>{
 for(const change of ['managed','error','missing-cash','changed-scope']){const s=snapshot();
  if(change==='managed')s.reconciliation.positions[0].program_source='execution_event';
  if(change==='error'){s.reconciliation.errors=[{broker_id:'upbit',scope:'account'}];s.reconciliation.summary.error_count=1;}
  if(change==='missing-cash'){s.reconciliation.accounts[0].program_source='missing';}
  if(change==='changed-scope')s.reconciliation.accounts[0].comparison_issue='account_scope_changed';
  assert.equal(reconciliationReferenceView(s).referenceOnly,false,change);assert.equal(liveReconciliationDisplay(s).status,'fail',change);
 }
});
test('active or unknown execution and pending orders cannot be rendered as reference-only reconciliation',()=>{
 const mutations=[s=>s.continuous_runtime.running=true,s=>s.continuous_runtime.runtimes={one:{running:true}},s=>delete s.continuous_runtime.running,s=>s.continuous_runtime.profiles={one:{ambiguous:true,running:false}},s=>s.short_live_test_status.active=true,s=>s.short_live_test_status={},s=>s.order_queue.active=1,s=>s.orders=[{state:'UNKNOWN'}],s=>s.orders=[{state:'ACCEPTED'}],s=>delete s.orders,s=>delete s.order_queue.active];
 for(const mutate of mutations){const s=snapshot();mutate(s);assert.equal(reconciliationDisplayIdle(s),false);assert.equal(reconciliationReferenceView(s).referenceOnly,false);assert.equal(liveReconciliationDisplay(s).status,'fail');}
});
test('invalid counts or incomplete row collections cannot demote a raw failure',()=>{
 const mutations=[s=>s.reconciliation.summary.mismatch_count=6,s=>s.reconciliation.summary.api_required_count=undefined,s=>s.reconciliation.summary.error_count='0',s=>s.reconciliation.accounts.pop(),s=>delete s.reconciliation.positions,s=>s.reconciliation.positions[0].status='future-unknown-status',s=>delete s.reconciliation.errors];
 for(const mutate of mutations){const s=snapshot();mutate(s);assert.equal(reconciliationReferenceView(s).countsVerified,false);assert.equal(liveReconciliationDisplay(s).status,'fail');}
});
test('futures exposure is never classified as unmanaged spot inventory',()=>{
 const s=snapshot(),row={...s.reconciliation.positions[0],broker_id:'binance-futures',symbol:'SOLUSDT',position_side:'SHORT'};
 assert.equal(reconciliationReferenceKind(row,'position',s),null);
 for(const change of [s=>s.program_ledger.fill_event_count=1,s=>s.program_ledger.other_event_count=1,s=>delete s.program_ledger.fill_event_count,s=>s.program_ledger.snapshot_event_count=0,s=>s.program_ledger.positions=[{broker_id:'binance',symbol:'FDUSD',source:'event_poll'}]]){const value=snapshot();change(value);assert.equal(reconciliationReferenceKind(value.reconciliation.positions[0],'position',value),null);}
});
test('currency scope, malformed values and absent baseline time remain unverified',()=>{
 const row=snapshot().reconciliation.accounts[0];for(const change of [{program_currency:'USD'},{comparison_issue:'account_scope_unverified'},{program_cash_value:NaN},{broker_cash_value:null},{program_updated_at:''},{program_source:'broker_snapshot'}])assert.equal(reconciliationReferenceKind({...row,...change},'cash',snapshot()),null);
});
test('a disconnected view cannot become a current historical-reference confirmation',()=>{
 const s=snapshot();s.api_connected=false;assert.equal(reconciliationReferenceView(s).referenceOnly,false);assert.equal(liveReconciliationDisplay(s).status,'fail');
});

test('an established or malformed baseline never becomes an unapproved historical cash reference',()=>{
 for(const state of [undefined,{},[],{last_baseline:'2026-10-30 04:00:00',last_baseline_source:'broker_snapshot'},{last_baseline:null,last_baseline_source:'broker_snapshot'},{last_baseline:false,last_baseline_source:''},{last_baseline:0,last_baseline_source:''}]){const s=snapshot();s.program_ledger.state=state;assert.equal(reconciliationReferenceKind(s.reconciliation.accounts[0],'cash',s),null);assert.equal(reconciliationReferenceView(s).referenceOnly,false);assert.equal(liveReconciliationDisplay(s).status,'fail');}
});
test('future or impossible historical timestamps and inconsistent blocking counts preserve the failure',()=>{
 for(const stamp of ['2099-01-01 00:00:00','2026-02-30 01:00:00','2026-10-30 25:00:00']){const s=snapshot();s.reconciliation.accounts[0].program_updated_at=stamp;assert.equal(reconciliationReferenceKind(s.reconciliation.accounts[0],'cash',s),null);assert.equal(liveReconciliationDisplay(s).status,'fail');}
 for(const count of [undefined,-1,0,'5',6]){const s=snapshot();s.reconciliation.summary.blocking_count=count;assert.equal(reconciliationReferenceView(s).countsVerified,false);assert.equal(liveReconciliationDisplay(s).status,'fail');}
});
test('only verified reference-only history changes the ledger presentation, never its original fields',()=>{
 const s=snapshot(),before=structuredClone(s.program_ledger),display=liveLedgerDisplay(s);assert.equal(display.value,'과거 조회 참고');assert.equal(display.status,'info');assert.equal(display.baselineRecorded,false);assert.match(display.detail,/현재 운용 원장과 별도/);assert.deepEqual(s.program_ledger,before);
 s.continuous_runtime.running=true;assert.equal(liveLedgerDisplay(s).value,'과거 조회 기록');assert.equal(liveLedgerDisplay(s).status,'warn');
});
test('an unknown backend summary failure status is not reclassified as a healthy reference-only state',()=>{
 const s=snapshot();s.reconciliation.summary.status='error';assert.equal(reconciliationReferenceView(s).referenceOnly,false);assert.equal(liveReconciliationDisplay(s).status,'fail');
});
