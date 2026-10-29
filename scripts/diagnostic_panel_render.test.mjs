import assert from 'node:assert/strict';
import test from 'node:test';
import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import {runInNewContext} from 'node:vm';
import React from 'react';
import {renderToStaticMarkup} from 'react-dom/server';
import {transformSync} from 'esbuild';
import {parse} from '@babel/parser';
import {doctorHistoryPresentation,launchConditionPresentation,diagnosticSafetyContext,preflightCheckPresentation,ordinarySafetyPresentation} from '../src/diagnosticPresentation.js';
import {deploymentContextMatchesPreflight} from '../src/deploymentSelection.js';

const require=createRequire(import.meta.url);
const source=readFileSync(new URL('../src/App.jsx',import.meta.url),'utf8');
const body=parse(source,{sourceType:'module',plugins:['jsx']}).program.body;
const names=['DoctorHistoryPanel','LaunchReportPanel','PreflightScopePanel','OperationalSafeguardsPanel','LegacyOperationalSafeguardsControls','statusTone'];
const functions=names.map(name=>{
 const node=body.find(row=>row.type==='FunctionDeclaration'&&row.id?.name===name);
 assert.ok(node,`production component missing: ${name}`);return source.slice(node.start,node.end);
}).join('\n');
const compiled=transformSync(functions+'\nmodule.exports={DoctorHistoryPanel,LaunchReportPanel,PreflightScopePanel,OperationalSafeguardsPanel,LegacyOperationalSafeguardsControls};',
 {loader:'jsx',jsx:'automatic',format:'cjs',target:'es2022'}).code;
const box=({children,...props})=>React.createElement('div',{'data-tone':props.tone},children);
const capturedActions=[];
const sandbox={module:{exports:{}},require,Date,doctorHistoryPresentation,launchConditionPresentation,diagnosticSafetyContext,preflightCheckPresentation,ordinarySafetyPresentation,
 deploymentContextMatchesPreflight,semanticSurfaceProps:(tone,className)=>({'data-tone':tone,className}),
 PanelHeader:({title,subtitle,suffix})=>React.createElement('header',null,title,subtitle,suffix),
 CompactDisclosure:({title,children})=>React.createElement('details',null,React.createElement('summary',null,title),children),
 ActionButton:({label,onClick,disabled})=>{capturedActions.push({label,onClick});return React.createElement('button',{onClick,disabled},label);},
 ShieldCheck:()=>React.createElement('svg'),TerminalSquare:()=>React.createElement('svg'),
 StatusPill:box,MetricGrid:box,MetricCard:({label,value,detail})=>React.createElement('article',null,label,value,detail),
 StatusRow:({label,status,detail})=>React.createElement('div',{'data-status':status},label,detail),
 EmptyRow:({text})=>React.createElement('p',null,text),ShieldAlert:()=>React.createElement('svg'),BadgeCheck:()=>React.createElement('svg')};
runInNewContext(compiled,sandbox);
const components=sandbox.module.exports;
const render=(name,props)=>renderToStaticMarkup(React.createElement(components[name],props));

test('actual advanced preflight component renders empty data as unknown without scope reference errors',()=>{
 const html=render('PreflightScopePanel',{snapshot:{},selectedDeploymentId:'selected',onPreflight:()=>{}});
 assert.match(html,/미확인/);assert.ok(!html.includes('0 HARD'));assert.ok(!html.includes('운용자 확인 →'));
});
test('actual recorded history stays collapsed and clearly separates old diagnosis from current API',()=>{
 const html=render('DoctorHistoryPanel',{diagnostics:{latest:{run_id:'record',generated_at:'2026-01-01',issues:[{severity:'hard_stop',issue_code:'old',problem:'옛 진단',remediation:'재확인'}]}},onNavigate:()=>{}});
 assert.match(html,/기록 당시 미충족 1건/);assert.match(html,/현재 API 연결/);assert.match(html,/<details/);assert.ok(!/<details[^>]*open/.test(html));
});
test('actual condition report uses one current fail list and retains diagnostic-only wording',()=>{
 const report={hard_stop_count:1,warning_count:0,hard_stops:[{label:'원장 대조',detail:'실제 증거 확인 필요'}],warnings:[]};
 const html=render('LaunchReportPanel',{report,observed:true,onNavigate:()=>{}});
 assert.match(html,/미충족 시작 조건 1건/);assert.match(html,/실행 승인이나 주문 연결 시험의 준비 결과가 아닙니다/);
 assert.match(html,/실제 증거 확인 필요/);assert.ok(!html.includes('blocked'));
});

test('actual safety panel keeps functional general-operation controls in a closed advanced disclosure',()=>{
 const confirm=()=>{},dry=()=>{},entry=()=>{},intent=()=>{};capturedActions.length=0;
 const html=render('OperationalSafeguardsPanel',{snapshot:{api_connected:true,continuous_runtime:{running:false},kill_switch:false,new_entries_blocked:true},onConfirm:confirm,onDryRun:dry,onEntryBlock:entry,onTestIntent:intent});
 assert.match(html,/시작 화면에서 한 번/);assert.match(html,/일반 운용 호환 제어 · 고급/);assert.ok(!/<details[^>]*open/.test(html));
 assert.equal(capturedActions.find(row=>row.label==='일반 운용 승인 확인').onClick,confirm);
 assert.equal(capturedActions.find(row=>row.label==='일반 운용 신규 주문 보류').onClick,entry);
 assert.equal(capturedActions.find(row=>row.label==='주문 전송 없이 점검').onClick,dry);
 assert.equal(capturedActions.find(row=>row.label==='일반 운용 시험 주문 요청').onClick,intent);
 assert.match(html,/Dry Run 해제 상태에서는 실제 주문을 전송할 수 있습니다/);
});
test('actual launch panel separates idle defaults while keeping real hard stops',()=>{
 const warnings=['운용자 확인','Dry Run 보호','신규 진입 차단'].map(label=>({label,status:'warn',detail:'legacy'}));
 const html=render('LaunchReportPanel',{report:{hard_stop_count:1,warning_count:3,hard_stops:[{label:'전략 승인',status:'fail',detail:'원본 자격 없음'}],warnings},observed:true,snapshot:{api_connected:true,continuous_runtime:{running:false},kill_switch:false,watchdog:{critical_count:0}},onNavigate:()=>{}});
 assert.match(html,/일반 운용 시작 전 기본 상태 2건/);assert.match(html,/원본 자격 없음/);assert.match(html,/운용 전략에서/);assert.match(html,/관련 화면 열기/);
});
