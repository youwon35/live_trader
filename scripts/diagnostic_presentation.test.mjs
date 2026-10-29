import test from 'node:test';
import assert from 'node:assert/strict';
import {doctorCategorySummary,doctorHistoryPresentation,launchConditionPresentation,deploymentMetadataLabel,diagnosticSafetyContext,preflightCheckPresentation} from '../src/diagnosticPresentation.js';

test('category and individual condition counts are separate units',()=>{
  const categories=doctorCategorySummary([{tone:'danger'},{tone:'danger'},{tone:'warning'}]);
  const conditions=launchConditionPresentation({hard_stop_count:4,warning_count:0,hard_stops:Array.from({length:4},(_,i)=>({label:`condition ${i}`})),warnings:[]},true);
  assert.equal(categories.blocked,2);assert.match(categories.detail,/분야 2곳/);
  assert.equal(conditions.blocked,4);assert.match(conditions.label,/조건 4건/);
  assert.ok(!categories.detail.includes('hard stop'));
});
test('history is always recorded evidence, never current API state or execution consent',()=>{
  const view=doctorHistoryPresentation({latest:{run_id:'old',generated_at:'2026-01-01',summary:{status:'pass'},issues:[{severity:'hard_stop',problem:'old'}]}});
  assert.equal(view.exists,true);assert.equal(view.blocked,1);assert.match(view.label,/기록 당시/);
  assert.match(view.detail,/현재 API/);assert.equal(doctorHistoryPresentation().exists,false);
});
test('missing or contradictory conditions cannot render a false green zero',()=>{
  for(const report of [{},{hard_stop_count:0,warning_count:0},{hard_stop_count:0,warning_count:0,hard_stops:[{label:'failure'}],warnings:[]}]){
    const view=launchConditionPresentation(report,true);assert.equal(view.known,false);assert.notEqual(view.tone,'success');assert.equal(view.blocked,null);
  }
});
test('valid zero conditions stays diagnostic and disconnected snapshots stay unknown',()=>{
  const report={hard_stop_count:0,warning_count:0,hard_stops:[],warnings:[]};
  const view=launchConditionPresentation(report,true);assert.equal(view.tone,'success');assert.match(view.detail,/実行|실행 승인/);
  assert.equal(launchConditionPresentation(report,false).known,false);
});
test('unbound legacy deployment metadata is not an API connection error or fake account',()=>{
  const unknown=deploymentMetadataLabel({brokerId:'CRYPTO',accountId:'***'});
  assert.match(unknown,/배포 기록/);assert.ok(!unknown.includes('거래소 미확인'));assert.ok(!unknown.includes('***'));
  assert.match(deploymentMetadataLabel({brokerId:'upbit',accountId:'masked 12'}),/Upbit 현물 · 계정 masked 12/);
});

test('idle start defaults are explanatory information, active or incident entry holds stay visible',()=>{
 const labels=['운용자 확인','Dry Run 보호','신규 진입 차단'];
 const report={hard_stop_count:1,warning_count:3,hard_stops:[{label:'원장',status:'fail'}],warnings:labels.map(label=>({label,status:'warn'}))};
 const original=JSON.stringify(report);const context=diagnosticSafetyContext({api_connected:true,continuous_runtime:{running:false},kill_switch:false,central_control:{halted:false},watchdog:{critical_count:0},final_preflight:[]});
 const idle=launchConditionPresentation(report,true,context);assert.equal(idle.blocked,1);assert.equal(idle.warnings,1);assert.equal(idle.informationCount,2);assert.equal(JSON.stringify(report),original);
 for(const current of [{ordinaryActive:true,runtimeKnown:true,safetyHold:false},{ordinaryActive:false,runtimeKnown:true,safetyHold:true},{ordinaryActive:false,runtimeKnown:false,safetyHold:false}]){
  const row=preflightCheckPresentation({label:'신규 진입 차단',status:'warn'},current);assert.equal(row.status,'warn');assert.ok(!row.standbyOnly);
 }
});
test('guidance links use actual Live page destinations and never recommends a hidden unlock toggle',()=>{
 const destinations=['accounts','gate','risk','functional-test'];
 for(const label of ['포지션·계좌 대조','대조 증거 신선도','전략 승인','필수 운영 체크리스트']){
  const row=preflightCheckPresentation({label,status:'fail'});assert.ok(destinations.includes(row.destination));assert.ok(row.meaning&&row.action);assert.equal(row.status,'fail');
 }
});

test('missing safety observations stay unknown and real central or hard-stop holds cannot become default information',()=>{
 const base={api_connected:true,continuous_runtime:{running:false},kill_switch:false,central_control:{halted:false},watchdog:{critical_count:0},final_preflight:[]};
 assert.equal(diagnosticSafetyContext(base).safetyHold,false);
 for(const value of [{...base,kill_switch:undefined},{...base,watchdog:{}},{...base,watchdog:{critical_count:NaN}},{...base,central_control:{}}]) assert.equal(diagnosticSafetyContext(value).safetyHold,null);
 for(const value of [{...base,central_control:{halted:true}},{...base,final_preflight:[{status:'fail'}]},{...base,kill_switch:true}]) assert.equal(diagnosticSafetyContext(value).safetyHold,true);
 assert.equal(diagnosticSafetyContext({...base,continuous_runtime:{running:false,runtimes:{one:{running:true}}}}).ordinaryActive,true);
});
