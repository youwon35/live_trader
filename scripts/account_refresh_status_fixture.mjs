// Offline production UI only. Every API is intercepted; no trading server runs.
import assert from 'node:assert/strict';
import {readFile,writeFile,mkdir} from 'node:fs/promises';
import {createServer} from 'node:http';
import {resolve,extname,sep} from 'node:path';
import {fileURLToPath} from 'node:url';
import {runInNewContext} from 'node:vm';
import {chromium} from 'playwright-core';
import {EMPTY_FUNCTIONAL_TEST_WORKSPACE} from '../src/functionalTestModel.js';
const root=fileURLToPath(new URL('../',import.meta.url)),dist=resolve(root,'dist');
const output=process.env.LIVE_ACCOUNT_UI_OUTPUT||resolve(root,'output/playwright/account-refresh-status');
await mkdir(output,{recursive:true});
const source=await readFile(resolve(root,'src/App.jsx'),'utf8');
const literal=source.match(/const fallbackSnapshot = (\{[\s\S]*?\n\});\s*const PANEL_SIZE_STORAGE_KEY/);assert.ok(literal);
const base=JSON.parse(JSON.stringify(runInNewContext(`(${literal[1]})`,{},{timeout:500})));
const initial=Date.parse('2026-10-28T04:00:00Z');
const snapshot=(delta=0)=>({...structuredClone(base),api_connected:true,generated_at:new Date(initial+delta).toISOString(),continuous_runtime:{running:true,profiles:{}},
 accounts:[{broker_id:'upbit',broker_name:'Upbit',account:'오프라인 합성 계좌',currency:'KRW',broker_cash_value:12000+delta,broker_equity_value:null,valuation_basis:'cash_only'}],
 positions:[{broker_id:'upbit',symbol:'KRW-BTC',broker_qty_value:0.1,currency:'KRW',display_valuation:{status:'OBSERVED',current_price:100000,currency:'KRW',observed_at:new Date(initial).toISOString(),expires_at:new Date(initial+60000).toISOString(),order_authority_granted:false}}],
 reconciliation:{summary:{status:'pass',status_label:'정상',last_run:new Date(initial+delta).toISOString(),account_count:1,error_count:0,api_required_count:0,mismatch_count:0},positions:[],accounts:[],next_actions:[]},
 program_ledger:{cash_count:1,position_count:0,execution_events:[],state:{last_baseline:new Date(initial).toISOString()}}});
const server=createServer(async(req,res)=>{const path=new URL(req.url,'http://localhost').pathname,file=resolve(dist,`.${path==='/'?'/index.html':path}`);if(req.method!=='GET'||path.startsWith('/api/')||!file.startsWith(dist+sep)){res.writeHead(403).end();return;}try{res.setHeader('Content-Type',({'.html':'text/html','.js':'application/javascript','.css':'text/css'})[extname(file)]||'application/octet-stream');res.end(await readFile(file));}catch{res.writeHead(404).end();}});
await new Promise(done=>server.listen(0,'127.0.0.1',done));const origin=`http://127.0.0.1:${server.address().port}`;
const browser=await chromium.launch({channel:'msedge',headless:true});
const report={externalRequests:[],unexpectedMutations:[],pageErrors:[],checks:[]};
try{
 const context=await browser.newContext({viewport:{width:1440,height:1100},serviceWorkers:'block'}),page=await context.newPage();
 await page.clock.install({time:initial});await page.clock.pauseAt(initial);
 page.on('pageerror',e=>report.pageErrors.push(e.message));
 let snapshotCount=0,syncCount=0,reconcileCount=0;
 const heldSnapshots=new Map();let heldSync=null,reconcileFails=true;
 const fulfill=(route,body,status=200)=>route.fulfill({status,contentType:'application/json',body:JSON.stringify(body)});
 await context.route('**/*',async route=>{
  const request=route.request(),url=new URL(request.url());
  if(url.origin!==origin){report.externalRequests.push(url.origin);await route.abort();return;}
  if(!url.pathname.startsWith('/api/')){await route.continue();return;}
  if(url.pathname==='/api/snapshot'){
   snapshotCount++;if(snapshotCount===2||snapshotCount===3){heldSnapshots.set(snapshotCount,route);return;}
   await fulfill(route,snapshot(snapshotCount>=4?30000:0));return;
  }
  if(url.pathname==='/api/execution-events'){
   syncCount++;if(syncCount===1){heldSync=route;return;}await fulfill(route,{ok:true});return;
  }
  if(url.pathname==='/api/reconcile'){
   reconcileCount++;if(reconcileFails){await fulfill(route,{error:'오프라인 합성 계좌 응답 실패'},503);return;}
   await fulfill(route,{ok:true,snapshot:snapshot(30000)});return;
  }
  if(request.method()!=='GET'){report.unexpectedMutations.push(url.pathname);await route.abort();return;}
  await fulfill(route,url.pathname==='/api/functional-test'?EMPTY_FUNCTIONAL_TEST_WORKSPACE:{ok:true});
 });
 await page.goto(origin);
 await page.waitForFunction(()=>document.querySelector('.sidebar-footer')?.textContent.includes('정상'));
 await page.clock.runFor(15001);
 assert.equal(snapshotCount,2);
 await page.getByRole('button',{name:'계좌·잔고',exact:true}).click();
 await page.waitForFunction(()=>document.querySelector('.account-refresh-status')?.textContent.includes('전체 상태 조회 완료 대기'));
 assert.equal(syncCount,0,'account read must wait for the in-flight general snapshot');
 assert.equal(await page.getByRole('button',{name:'계좌 다시 읽기',exact:true}).isDisabled(),true);
 await page.clock.runFor(15000);await page.locator('.api-connection-banner').waitFor();
 await page.waitForFunction(()=>document.querySelector('.account-refresh-status')?.textContent.includes('계좌·체결 읽는 중'));
 assert.equal(syncCount,1);assert.equal(snapshotCount,2);
 assert.match(await page.locator('.api-connection-banner').innerText(),/15초/);
 assert.equal(await page.getByRole('button',{name:'계좌 조회 완료 대기…',exact:true}).isDisabled(),true);
 const unknown=await page.locator('.three-way-reconciliation-panel').innerText();
 assert.doesNotMatch(unknown,/0건|0개|기준 없음/);assert.match(unknown,/원장·체결 건수를 확인할 수 없습니다/);
 assert.equal(await page.locator('.ts-allocation-workspace').count(),0);
 await page.screenshot({path:resolve(output,'snapshot-timeout.png')});
 report.checks.push('account read waits for a 15s snapshot timeout, then owns the read; reconnect cannot overlap it');
 await fulfill(heldSync,{ok:true});
 await page.waitForFunction(()=>document.querySelector('.account-refresh-status')?.textContent.includes('갱신 실패'));
 await page.getByRole('button',{name:'다시 연결',exact:true}).click();
 await page.waitForFunction(()=>document.querySelector('.api-connection-banner button')?.textContent.includes('연결 확인 중'));
 assert.equal(await page.getByRole('button',{name:'연결 확인 중…',exact:true}).isDisabled(),true);
 await page.locator('.api-connection-banner button').evaluate(button=>button.click());
 assert.equal(snapshotCount,3);
 // Pause auto account refresh to inspect whether a general recovery erases its error.
 await page.evaluate(()=>Object.defineProperty(document,'visibilityState',{configurable:true,get:()=> 'hidden'}));
 await fulfill(heldSnapshots.get(3),snapshot(32000));
 await page.waitForFunction(()=>!document.querySelector('.api-connection-banner'));
 assert.equal(syncCount,1);
 assert.match(await page.locator('.account-refresh-status').innerText(),/갱신 실패/);
 assert.match(await page.locator('.ts-allocation-workspace').innerText(),/이전 관측값|현재 평가 미확인/);
 await page.screenshot({path:resolve(output,'account-error.png')});
 report.checks.push('reconnect is single-flight with progress; successful general polling does not erase account error');
 reconcileFails=false;
 await page.getByRole('button',{name:'계좌 다시 읽기',exact:true}).click();
 await page.waitForFunction(()=>document.querySelector('.account-refresh-status')?.textContent.includes('최근 전체 조회 성공 2026. 10. 28. 오후 1:00:30'));
 assert.equal(syncCount,2);assert.equal(reconcileCount,2);
 assert.doesNotMatch(await page.locator('.account-refresh-status').innerText(),/갱신 실패/);
 await page.screenshot({path:resolve(output,'account-recovered.png')});
 await page.clock.runFor(31000);assert.equal(syncCount,2,'hidden account page does not poll broker reads');
 assert.match(await page.locator('.ts-allocation-workspace').innerText(),/부분 조회·평가 미확인/);
 assert.equal(await page.locator('.ts-allocation-donut').count(),0,'expired display quote is invalidated by UI clock without a snapshot');
 await page.screenshot({path:resolve(output,'quote-expired.png')});
 report.checks.push('manual retry records observation time; hidden page does not read brokers; display quote expires without another snapshot');
 assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
 await context.close();
 assert.deepEqual(report.externalRequests,[]);assert.deepEqual(report.unexpectedMutations,[]);assert.deepEqual(report.pageErrors,[]);
}finally{await browser.close();await new Promise(done=>server.close(done));}
await writeFile(resolve(output,'report.json'),JSON.stringify(report,null,2));console.log(JSON.stringify(report,null,2));
