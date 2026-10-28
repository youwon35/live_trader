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
   assert.equal(request.postDataJSON().display_only,true);syncCount++;if(syncCount===1){heldSync=route;return;}await fulfill(route,{ok:true});return;
  }
  if(url.pathname==='/api/reconcile'){
   assert.equal(request.postDataJSON().display_only,true);reconcileCount++;if(reconcileFails){await fulfill(route,{error:'오프라인 합성 계좌 응답 실패'},503);return;}
   await fulfill(route,{ok:true,snapshot:snapshot(30000)});return;
  }
  if(request.method()!=='GET'){report.unexpectedMutations.push(url.pathname);await route.abort();return;}
  await fulfill(route,url.pathname==='/api/functional-test'?EMPTY_FUNCTIONAL_TEST_WORKSPACE:{ok:true});
 });
 await page.goto(origin);
 await page.waitForFunction(()=>document.querySelector('.sidebar-footer')?.textContent.includes('정상'));
 await page.getByRole('button',{name:'계좌·잔고',exact:true}).click();
 await page.waitForFunction(()=>document.querySelector('.account-refresh-status')?.textContent.includes('계좌·체결 읽는 중'));
 assert.equal(syncCount,1,'startup makes one account read');
 await page.clock.runFor(15001);
 assert.equal(snapshotCount,1,'regular status reads cannot overlap the startup account read');
 await fulfill(heldSync,{ok:true});
 await page.waitForFunction(()=>document.querySelector('.account-refresh-status')?.textContent.includes('갱신 실패'));
 await page.clock.runFor(15001);assert.equal(snapshotCount,2);
 await page.getByRole('button',{name:'계좌 다시 읽기',exact:true}).click();
 await page.waitForFunction(()=>document.querySelector('.account-refresh-status')?.textContent.includes('전체 상태 조회 완료 대기'));
 assert.equal(syncCount,1);
 await page.clock.runFor(15001);
 await page.locator('.api-connection-banner').waitFor();
 await page.waitForFunction(()=>document.querySelector('.account-refresh-status')?.textContent.includes('갱신 실패'));
 assert.equal(syncCount,2);
 const unknown=await page.locator('.three-way-reconciliation-panel').innerText();
 assert.doesNotMatch(unknown,/0건|0개|기준 없음/);
 assert.equal(await page.locator('.ts-allocation-workspace').count(),0);
 await page.screenshot({path:resolve(output,'snapshot-timeout.png')});
 report.checks.push('startup once, status/account reads serialized, failed read never auto retries');
 await page.getByRole('button',{name:'다시 연결',exact:true}).click();
 await page.waitForFunction(()=>document.querySelector('.api-connection-banner button')?.textContent.includes('연결 확인 중'));
 assert.equal(snapshotCount,3);
 await fulfill(heldSnapshots.get(3),snapshot(32000));
 await page.waitForFunction(()=>!document.querySelector('.api-connection-banner'));
 assert.equal(syncCount,2,'reconnect must not refresh accounts');
 assert.match(await page.locator('.account-refresh-status').innerText(),/갱신 실패/);
 reconcileFails=false;
 await page.getByRole('button',{name:'계좌 다시 읽기',exact:true}).click();
 await page.waitForFunction(()=>document.querySelector('.account-refresh-status')?.textContent.includes('최근 전체 조회 성공 2026. 10. 28. 오후 1:00:30'));
 assert.equal(syncCount,3);assert.equal(reconcileCount,3);
 assert.doesNotMatch(await page.locator('.account-refresh-status').innerText(),/갱신 실패/);
 await page.screenshot({path:resolve(output,'manual-account-recovered.png')});
 await page.clock.runFor(120001);
 assert.equal(syncCount,3,'visible account tab must not perform periodic broker reads');
 assert.match(await page.locator('.ts-allocation-workspace').innerText(),/실시간 평가가 아닙니다/);
 assert.ok(await page.locator('.ts-allocation-donut').count()>0,'last observed valuation remains visible with its historical label');
 await page.getByRole('button',{name:'실시간 감시',exact:true}).click();
 await page.getByRole('button',{name:'계좌·잔고',exact:true}).click();
 assert.equal(syncCount,3,'navigation must not read accounts again');
 await page.screenshot({path:resolve(output,'manual-observation-retained.png')});
 report.checks.push('manual retry records observation time; reconnect, navigation and two minutes visible do not repeat account reads');
 report.checks.push('past valuation remains visibly labelled historical, never silently becomes zero');
 assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
 await context.close();
 assert.deepEqual(report.externalRequests,[]);assert.deepEqual(report.unexpectedMutations,[]);assert.deepEqual(report.pageErrors,[]);
}finally{await browser.close();await new Promise(done=>server.close(done));}
await writeFile(resolve(output,'report.json'),JSON.stringify(report,null,2));console.log(JSON.stringify(report,null,2));
