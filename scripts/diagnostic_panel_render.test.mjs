import assert from 'node:assert/strict';
import test from 'node:test';
import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import {runInNewContext} from 'node:vm';
import React from 'react';
import {renderToStaticMarkup} from 'react-dom/server';
import {transformSync} from 'esbuild';
import {parse} from '@babel/parser';
import {doctorHistoryPresentation,launchConditionPresentation} from '../src/diagnosticPresentation.js';
import {deploymentContextMatchesPreflight} from '../src/deploymentSelection.js';

const require=createRequire(import.meta.url);
const source=readFileSync(new URL('../src/App.jsx',import.meta.url),'utf8');
const body=parse(source,{sourceType:'module',plugins:['jsx']}).program.body;
const names=['DoctorHistoryPanel','LaunchReportPanel','PreflightScopePanel','statusTone'];
const functions=names.map(name=>{
 const node=body.find(row=>row.type==='FunctionDeclaration'&&row.id?.name===name);
 assert.ok(node,`production component missing: ${name}`);return source.slice(node.start,node.end);
}).join('\n');
const compiled=transformSync(functions+'\nmodule.exports={DoctorHistoryPanel,LaunchReportPanel,PreflightScopePanel};',
 {loader:'jsx',jsx:'automatic',format:'cjs',target:'es2022'}).code;
const box=({children,...props})=>React.createElement('div',{'data-tone':props.tone},children);
const sandbox={module:{exports:{}},require,Date,doctorHistoryPresentation,launchConditionPresentation,
 deploymentContextMatchesPreflight,semanticSurfaceProps:(tone,className)=>({'data-tone':tone,className}),
 PanelHeader:({title,subtitle,suffix})=>React.createElement('header',null,title,subtitle,suffix),
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
