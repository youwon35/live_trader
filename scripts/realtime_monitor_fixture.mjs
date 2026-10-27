// Synthetic, isolated rendering only. No backend, account or order API is loaded.
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {mkdir,readFile,writeFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {fileURLToPath} from 'node:url';
import {join,basename,resolve} from 'node:path';
const root=fileURLToPath(new URL('../',import.meta.url));
const require=createRequire(join(root,'node_modules/fixture-resolver.cjs'));
const {build}=require('esbuild');const {chromium}=require('playwright-core');
const output=process.env.LIVE_MONITOR_OUTPUT || join(root,'output/playwright/realtime-monitor');
await mkdir(output,{recursive:true});
const entry=`import React from 'react';import {createRoot} from 'react-dom/client';
import RealtimeMonitor from './src/RealtimeMonitor.jsx';import {normalizeShortLiveSnapshot} from './src/shortLiveTestModel.js';import './src/styles.css';import '../../packages/design/moa-theme.css';import './src/moa-surfaces.css';
const now=Date.now(),at=(delta)=>new Date(now+delta).toISOString();
const routeNames=['BINANCE_SPOT','BINANCE_USDM_FUTURES','UPBIT_KRW_SPOT'];
const raw={fixture:true,observedAt:at(0),runtime:{sessionId:'synthetic-session',status:'RUNNING',ownerAlive:true,workerAlive:true,newEntriesBlocked:false,unknownOrderCount:0,startedAt:at(-7200000),endsAt:at(14400000),fillCount:2,
strategyRows:routeNames.map((route,i)=>({instanceId:'synthetic-'+i,route,symbol:['BTCUSDT','SOLUSDT','KRW-BTC'][i],pluginId:'moving_average_cross',timeframe:'1h',lastSignal:i===0?'BUY':'HOLD',decisionCount:2,orderAttemptCount:i===0?1:0,fillObservationCount:i===0?2:0,lastPrice:[110000,185,160000000][i],lastBarAt:at(-60000),lastDecisionAt:at(-2000),positionQuantity:i===0?.0002:0,averageEntryPrice:i===0?109900:null,pendingOrderCount:0})),
brokerRows:routeNames.map((route,i)=>({route,effectiveRouteBudgetKrw:i===2?100000:50000,actualFeeKnown:i!==1,accountObservedAt:at(-2000),monitorEquity:{basis:'OBSERVED_NATIVE_EQUITY_CHANGE',baselineNative:i===2?100000:35,currentNative:i===2?100250:i===0?35.05:34.99,krwPerQuote:i===2?1:1400,quoteCurrency:i===2?'KRW':'USDT',preservedSpotExcluded:i!==1}})),
equityHistory:routeNames.flatMap((route,i)=>Array.from({length:30},(_,j)=>({route,basis:'OBSERVED_NATIVE_EQUITY_CHANGE',observedAt:at(-((29-j)*60000)),baselineNative:i===2?100000:35,currentNative:i===2?100000+j*12:35+Math.sin(j/3)*.04+j*.003,krwPerQuote:i===2?1:1400,quoteCurrency:i===2?'KRW':'USDT'}))),
feedObservations:routeNames.map(route=>({route,streams:[{socketPresent:true,lastFrameAt:at(-3000)}]})),
recentActivity:[{sequence:1,kind:'STARTED',observedAt:at(-7200000)},{sequence:2,kind:'BAR_EVALUATED',route:'BINANCE_SPOT',observedAt:at(-3600000)},{sequence:3,kind:'ORDER_INTENT_BEFORE_SUBMIT',route:'BINANCE_SPOT',symbol:'BTCUSDT',side:'BUY',observedAt:at(-3599000)},{sequence:4,kind:'ORDER_REPLY',route:'BINANCE_SPOT',symbol:'BTCUSDT',status:'PARTIAL',observedAt:at(-3598000)},{sequence:5,kind:'ACTUAL_FILL_INCREMENT',route:'BINANCE_SPOT',symbol:'BTCUSDT',side:'BUY',incrementalQuantity:.0002,observedAt:at(-3597000)},{sequence:6,kind:'BAR_EVALUATED',route:'UPBIT_KRW_SPOT',observedAt:at(-2000)}]}};
const renderer=createRoot(document.getElementById('root'));window.renderMonitor=(scenario='running')=>{const data=structuredClone(raw);let fresh=true,receivedAt=now;
if(scenario==='disconnected'){fresh=false;receivedAt=now-90000;}
if(scenario==='feed-lag'){data.runtime.feedObservations[1].streams[0].lastFrameAt=at(-90000);}
if(scenario==='waiting'){data.runtime.strategyRows.forEach(row=>{row.decisionCount=0;row.orderAttemptCount=0;row.fillObservationCount=0;row.lastSignal='NOT_OBSERVED';});data.runtime.fillCount=0;data.runtime.recentActivity=[];}
if(scenario==='unknown-pnl'){data.runtime.brokerRows.forEach(row=>delete row.monitorEquity);data.runtime.equityHistory=[];}
if(scenario==='terminal'){
  Object.assign(data.runtime,{terminatedAt:at(-7130000),status:'TERMINATED',terminalVerified:true,naturalExitObserved:true,ownerAlive:false,workerAlive:false,
    firstFailure:'OFFICIAL_READ_REJECTED',blockers:['OFFICIAL_READ_REJECTED'],fillCount:0,
    firstFailureDiagnostics:[{provider:'binance-futures',path:'/fapi/v1/exchangeInfo',status:'UNAVAILABLE',failureKind:'TIMEOUT',elapsedMs:8000}]});
  data.runtime.strategyRows.forEach(row=>{row.decisionCount=0;row.orderAttemptCount=0;row.fillObservationCount=0;row.lastSignal='NOT_OBSERVED';});
  data.runtime.brokerRows.forEach(row=>{row.accountObservedAt=at(-7131000);row.monitorEquity.currentNative=row.monitorEquity.baselineNative;});
  data.runtime.feedObservations.forEach(row=>{row.streams[0].lastFrameAt=at(-7134000);});
  data.runtime.recentActivity=[{sequence:1,kind:'STARTED',observedAt:at(-7200000)},{sequence:2,kind:'CLEANUP_INTENT_ONCE',observedAt:at(-7153000)},{sequence:3,kind:'WORKER_EXIT_INTENT',observedAt:at(-7131000)},{sequence:4,kind:'WORKER_DEATH_OBSERVED',observedAt:at(-7130000)}];
}
renderer.render(<main style={{maxWidth:1420,margin:'0 auto',padding:24}}><p style={{color:'#64766f',fontSize:12}}>오프라인 화면 검사 · 합성 자료 · 실계좌/실주문 없음</p><RealtimeMonitor snapshot={normalizeShortLiveSnapshot(data)} fresh={fresh} receivedAt={receivedAt}/></main>);};window.renderMonitor();`;
await build({stdin:{contents:entry,resolveDir:root,loader:'jsx'},bundle:true,format:'esm',outfile:join(output,'fixture.js'),loader:{'.css':'css','.woff2':'file','.woff':'file','.svg':'file'},define:{'process.env.NODE_ENV':'"test"'},logLevel:'silent'});
await writeFile(join(output,'index.html'),'<!doctype html><html lang="ko" data-ui-theme="light" data-program="live-trader"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Offline Live monitor</title><link rel="stylesheet" href="/fixture.css"><div id="root"></div><script type="module" src="/fixture.js"></script></html>');
let forbidden=0;
const server=createServer(async(req,res)=>{const path=new URL(req.url,'http://127.0.0.1').pathname;if(req.method!=='GET'||path.includes('api')||path.includes('..')){forbidden++;res.writeHead(403);res.end();return;}const file=path==='/'?'index.html':basename(path);try{res.setHeader('Content-Type',file.endsWith('.js')?'application/javascript':file.endsWith('.css')?'text/css':file.endsWith('.woff2')?'font/woff2':file.endsWith('.html')?'text/html':'application/octet-stream');res.end(await readFile(join(output,file)));}catch{res.writeHead(404);res.end();}});
await new Promise(done=>server.listen(0,'127.0.0.1',done));const origin='http://127.0.0.1:'+server.address().port;
const browser=await chromium.launch({channel:'msedge',headless:true});const report={fixtureOnly:true,externalRequests:[],pageErrors:[],screenshots:[],checks:[]};
try{const context=await browser.newContext({viewport:{width:1280,height:800},serviceWorkers:'block'});await context.route('**/*',async route=>{if(new URL(route.request().url()).origin!==origin){report.externalRequests.push(route.request().url());await route.abort();}else await route.continue();});const page=await context.newPage();page.on('pageerror',e=>report.pageErrors.push(e.message));await page.goto(origin);await page.getByRole('heading',{name:'주문 연결 시험 · 실시간 감시',exact:true}).waitFor();
for(const scenario of ['running','disconnected','feed-lag','waiting','unknown-pnl','terminal']){await page.evaluate(s=>window.renderMonitor(s),scenario);await page.waitForTimeout(100);if(scenario==='terminal'){assert.match(await page.locator('body').innerText(),/종료까지 관측 경과 1분 10초/);assert.equal(await page.getByRole('progressbar').getAttribute('value'),'0.32407407407407407');report.checks.push('terminal duration frozen at observed 70 seconds');await page.getByText('최초 오류 상세',{exact:false}).click();assert.match(await page.locator('body').innerText(),/응답 시간 초과.*8.0초/);assert.match(await page.locator('body').innerText(),/\/fapi\/v1\/exchangeInfo/);assert.doesNotMatch(await page.locator('body').innerText(),/최근 계좌 대조|첫 완성봉 평가를 기다립니다/);report.checks.push('safe first-failure diagnostics remain visible after terminal cleanup');}const filename=join(output,scenario+'.png');await page.screenshot({path:filename,fullPage:true});report.screenshots.push(filename);assert.equal(await page.getByRole('button').count(),0);report.checks.push(scenario+' has no mutation controls');}
for(const zoom of [1,1.25,1.5]){await page.evaluate(z=>{document.documentElement.style.zoom=z;window.renderMonitor('running');},zoom);await page.waitForTimeout(100);const overflow=await page.evaluate(()=>document.documentElement.scrollWidth>document.documentElement.clientWidth+2);assert.equal(overflow,false,'horizontal overflow at '+zoom);await page.screenshot({path:join(output,'scale-'+zoom+'.png'),fullPage:true});report.checks.push('1280x800 at '+zoom+' no horizontal overflow');}
await page.evaluate(()=>{document.documentElement.style.zoom=1;document.documentElement.dataset.uiTheme='dark';window.renderMonitor('running');});await page.waitForTimeout(100);await page.screenshot({path:join(output,'dark-running.png'),fullPage:true});report.checks.push('Moa dark theme with actual strategy fields rendered');
assert.deepEqual(report.externalRequests,[]);assert.deepEqual(report.pageErrors,[]);assert.equal(forbidden,0);report.ok=true;
}finally{await browser.close();await new Promise(done=>server.close(done));await writeFile(join(output,'report.json'),JSON.stringify(report,null,2));}console.log(JSON.stringify(report));
