// Actual components, synthetic public observations. No application backend.
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {readFile,mkdir,writeFile,readdir} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
import {resolve} from 'node:path';
import {build} from 'esbuild';
import {chromium} from 'playwright-core';
const app=fileURLToPath(new URL('../',import.meta.url));
const owner=resolve(app,'../../..');
const output=resolve(owner,'mock-preparation-ui-'+Date.now());await mkdir(output,{recursive:false});
const entry=`import React,{useState,useRef} from 'react';import{createRoot}from'react-dom/client';
import GapPanel from './src/MarketGapPreparationPanel.jsx';
import NativePanel from '../paper_trader/src/NativeTickPreparationPanel.tsx';
import KisPanel from './src/KisNativePreparationPanel.jsx';
import{createMarketGapCoordinator}from'./src/marketGapPreparation.js';
function Fixture(){const[views,setViews]=useState({});const[count,setCount]=useState(0);const[method,setMethod]=useState('PUBLIC_H0STCNT0');const[cutoff,setCutoff]=useState('2026-10-19T14:30');const[native,setNative]=useState({label:'국내 포트폴리오',status:'PREPARING_PUBLIC',canCancel:true,publicProgress:{phase:'CAPTURING_NATIVE_TICKS',frameCount:123456,elapsedSeconds:12500,cutoff:'2026-10-19T14:30:00+09:00'}});
const ref=useRef();if(!ref.current)ref.current=createMarketGapCoordinator({begin:async()=>({ok:true,preparationId:'mock',status:'WARNING',result:{status:'WARNING',authorityGranted:false,qualificationGranted:false,runtimeStarted:false,brokerSubmissionPerformed:false,expiresAtUtc:new Date(Date.now()+60000).toISOString(),members:[{strategyInstanceId:'i',symbol:'KRW-SOL',status:'WARNING',openingLimitSeconds:30,recentDeadlineMisses:4,latestTrade:{idleSeconds:45.7},sampleBoundaries:[{boundaryUtc:'2026-10-16T12:00:00Z'},{boundaryUtc:'2026-10-16T12:15:00Z'}],sixHourScenario:{upperBoundIfTheseMissesOccurred:68/72,coverageThreshold:.98}}]}}),cancelRemote:async()=>({ok:true}),status:async()=>{},onView:(view)=>setViews((old)=>({...old,[view.scopeKey]:view}))});
return <main><h1>준비 화면 오프라인 검사</h1><button onClick={()=>void ref.current.run({scopeKey:'upbit',label:'코인 포트폴리오',body:{},continueStart:async()=>{setCount((n)=>n+1);return{ok:true}}})}>최근 체결 공백 확인</button><output aria-label='모의 기존 절차 호출 횟수'>{count}</output><GapPanel views={views} onContinue={(key)=>ref.current.accept(key)} onCancel={(key)=>void ref.current.cancel(key)}/><NativePanel method={method} cutoff={cutoff} view={native} onMethod={setMethod} onCutoff={setCutoff} onCancel={()=>setNative((old)=>({...old,status:'CANCEL_REQUESTED',detail:'준비 취소 요청을 확인하는 중입니다.'}))}/><KisPanel views={{kr:{scopeKey:'kr',label:'국내 Live 준비',status:'PREPARING_PUBLIC',canCancel:true,publicProgress:{phase:'CAPTURING_NATIVE_TICKS',frameCount:123,elapsedSeconds:500,cutoff:'2026-10-19T14:30:00+09:00'}}}} onCancel={()=>{}}/></main>}
createRoot(document.getElementById('root')).render(<Fixture/>);`;
const built=await build({stdin:{contents:entry,resolveDir:app,sourcefile:'offline-preparation-fixture.jsx',loader:'jsx'},bundle:true,write:false,format:'iife',jsx:'automatic',target:'es2022'});
const script=built.outputFiles[0].text;await writeFile(resolve(output,'fixture.js'),script);
const styles=(await readdir(resolve(app,'dist/assets'))).find((name)=>name.endsWith('.css'));
const css=await readFile(resolve(app,'dist/assets',styles),'utf8');
const html=`<!doctype html><html lang="ko"><meta charset="utf-8"><title>Offline preparation</title><style>${css}\nbody{margin:0}main{max-width:1180px;margin:24px auto;padding:16px;display:grid;gap:20px}.panel{padding:20px}label{display:block;margin:12px 0}button{margin:6px}p{overflow-wrap:anywhere}</style><div id="root"></div><script src="/fixture.js"></script></html>`;
const server=createServer((request,response)=>{if(request.url==='/fixture.js')response.writeHead(200,{'Content-Type':'text/javascript'}).end(script);else if(request.url==='/')response.writeHead(200,{'Content-Type':'text/html'}).end(html);else response.writeHead(404).end();});
await new Promise((yes)=>server.listen(0,'127.0.0.1',yes));const port=server.address().port;let browser;const external=[];const checks=[];
try{
 browser=await chromium.launch({executablePath:'C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe',headless:true,args:['--disable-background-networking','--disable-component-update','--no-first-run']});
 for(const width of [1280,1920]){
  const context=await browser.newContext({viewport:{width,height:1080}});await context.route('**/*',(route)=>{const url=new URL(route.request().url());if(url.hostname==='127.0.0.1'&&url.port===String(port))return route.continue();external.push('BLOCKED');return route.abort();});
  const page=await context.newPage();const errors=[];page.on('pageerror',(error)=>errors.push(error.message));
  await page.goto('http://127.0.0.1:'+port);await page.getByRole('button',{name:'최근 체결 공백 확인',exact:true}).click();
  await page.getByRole('button',{name:'기존 시작 절차 계속',exact:true}).waitFor();assert.equal(await page.getByLabel('모의 기존 절차 호출 횟수').textContent(),'0');
  assert.ok(await page.getByText('KRW-SOL',{exact:false}).isVisible());
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>window.innerWidth),false);
  await page.screenshot({path:resolve(output,'preparation-'+width+'.png'),fullPage:true});
  await page.getByRole('button',{name:'기존 시작 절차 계속',exact:true}).click();await page.waitForFunction(()=>document.querySelector('output').textContent==='1');
  const native=page.getByRole('region',{name:'국내 Paper 시세 준비'});await native.getByRole('button',{name:'준비 취소',exact:true}).click();await page.getByText('준비 취소 요청을 확인하는 중입니다.').waitFor();
  assert.deepEqual(errors,[]);checks.push({width,explicitContinueRequired:true,normalMockCalls:1,cancelDisplayedAsRequest:true,noHorizontalOverflow:true,pageErrors:0});await context.close();
 }
}finally{await browser?.close();await new Promise((yes)=>server.close(yes));}
const result={checks,externalBrowserRequests:external.length,actualMarketOrAccountCalls:0,nativePrompts:0,applicationBackendsStarted:0,bundleSha256:createHash('sha256').update(script).digest('hex')};
assert.equal(external.length,0);await writeFile(resolve(output,'result.json'),JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify({output,...result}));
