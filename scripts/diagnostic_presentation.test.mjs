import test from 'node:test';
import assert from 'node:assert/strict';
import {doctorCategorySummary,doctorHistoryPresentation,launchConditionPresentation,deploymentMetadataLabel} from '../src/diagnosticPresentation.js';

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
