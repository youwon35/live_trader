import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';
import Module, { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { renderToStaticMarkup } from 'react-dom/server';
import { transformSync } from 'esbuild';
const require = createRequire(import.meta.url);
const sourcePath = fileURLToPath(new URL('../src/MarketGapPreparationPanel.jsx', import.meta.url));
const module = new Module(sourcePath);module.filename=sourcePath;module.paths=Module._nodeModulePaths(fileURLToPath(new URL('../src',import.meta.url)));module.require=require;
module._compile(transformSync(readFileSync(sourcePath,'utf8'),{loader:'jsx',jsx:'automatic',format:'cjs',target:'es2022'}).code,sourcePath);
const Panel=module.exports.default;
function elements(node,type) { if (!node || typeof node!=='object')return []; if(Array.isArray(node))return node.flatMap((n)=>elements(n,type));return [...(node.type===type?[node]:[]),...elements(node.props?.children,type)]; }
const row=(phase='REVIEW',scopeKey='upbit')=>({scopeKey,phase,label:'선택한 포트폴리오',result:{members:[{strategyInstanceId:'i',symbol:'KRW-SOL',status:'WARNING',openingLimitSeconds:30,recentDeadlineMisses:4,
  latestTrade:{idleSeconds:31.5},sampleBoundaries:[{boundaryUtc:'2026-10-16T12:00:00Z'},{boundaryUtc:'2026-10-16T12:15:00Z'}],
  sixHourScenario:{upperBoundIfTheseMissesOccurred:68/72,coverageThreshold:.98}}]}});
test('passive mount neither fetches nor exposes a Start control',()=>{assert.equal(renderToStaticMarkup(Panel({views:{}})),'');});
test('real review panel shows symbol/recent windows/deadline/counterfactual98% and no implementation identity',()=>{
 const html=renderToStaticMarkup(Panel({views:{a:row()},onCancel:()=>{},onContinue:()=>{}}));
 for(const text of ['KRW-SOL','30초','31.5초','94.44%','98%','통과를 보장하지','12:00:00','12:15:00'])assert.ok(html.includes(text),text);
 assert.doesNotMatch(html,/sourceArtifactHash|schemaVersion|scopeHash|manifest|preparationId|주문 권한/);
});
test('continue and cancel actions belong to the exact visible account, and never run on render',()=>{
 const calls=[];const tree=Panel({views:{a:row(),b:row('REVIEW','binance')},onCancel:(key)=>calls.push(['cancel',key]),onContinue:(key)=>calls.push(['continue',key])});
 assert.deepEqual(calls,[]);const buttons=elements(tree,'button');buttons[2].props.onClick();buttons[1].props.onClick();
 assert.deepEqual(calls,[['continue','binance'],['cancel','upbit']]);
});
test('pending observations keep direct cancel while Source/stale/expiry failure never displays continue',()=>{
 for(const phase of ['PREPARING','FAILED','CONTINUING']){
   const tree=Panel({views:{a:row(phase)},onCancel:()=>{},onContinue:()=>assert.fail()});
   const html=renderToStaticMarkup(tree);assert.doesNotMatch(html,/기존 시작 절차 계속/);
   assert.equal(elements(tree,'button').length,phase==='PREPARING'?1:0);
 }
});
test('no-data is honest unknown and allows only original explicit user procedure',()=>{
 const html=renderToStaticMarkup(Panel({views:{a:{...row(),result:null}},onCancel:()=>{},onContinue:()=>{}}));
 assert.match(html,/자료를 확인하지 못했습니다/);assert.match(html,/기존 시작 절차 계속/);assert.doesNotMatch(html,/검증 통과 완료|시작 승인 완료/);
});
test('both actual App start paths integrate review without global busy during public preparation',()=>{
 const live=readFileSync(new URL('../src/App.jsx',import.meta.url),'utf8');
 const start=live.slice(live.indexOf('async function runRuntimeStart('),live.indexOf('function updateAppearance('));
 assert.match(start,/prepareMarketGapsForStart/);assert.match(start,/continueStart:.*runAction.*startContinuousRuntime/);assert.doesNotMatch(start,/setLoading\(/);
 const paper=readFileSync(new URL('../../paper_trader/src/App.tsx',import.meta.url),'utf8');
 assert.match(paper,/begin-market-gap-preparation/);assert.match(paper,/prepareMarketGapStart\(target, original\)/);assert.match(paper,/MarketGapPreparationPanel views=/);
 assert.match(paper,/gapTargetBindings/);assert.match(live,/cancelAllMarketGapPreparations.*selectedDeploymentId/s);
});
