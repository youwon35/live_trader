// Production UI with intercepted synthetic responses only; never starts Python.
import assert from 'node:assert/strict';
import {readFile,writeFile,mkdir} from 'node:fs/promises';
import {createServer} from 'node:http';
import {resolve,extname,sep} from 'node:path';
import {fileURLToPath} from 'node:url';
import {runInNewContext} from 'node:vm';
import {chromium} from 'playwright-core';
import {EMPTY_FUNCTIONAL_TEST_WORKSPACE} from '../src/functionalTestModel.js';
const root=fileURLToPath(new URL('../',import.meta.url)), dist=resolve(root,'dist');
const output=process.env.LIVE_STATUS_OUTPUT || resolve(root,'output/playwright/status-truth');
await mkdir(output,{recursive:true});
const appSource=await readFile(resolve(root,'src/App.jsx'),'utf8');
const literal=appSource.match(/const fallbackSnapshot = (\{[\s\S]*?\n\});\s*const PANEL_SIZE_STORAGE_KEY/);
assert.ok(literal);
const valid=JSON.parse(JSON.stringify(runInNewContext(`(${literal[1]})`,{},{timeout:500})));
valid.generated_at='2026-10-28 01:30:00';
const server=createServer(async(req,res)=>{
  const path=new URL(req.url,'http://localhost').pathname;
  const file=resolve(dist,`.${path==='/'?'/index.html':path}`);
  if(req.method!=='GET'||path.startsWith('/api/')||!file.startsWith(dist+sep)){res.writeHead(403).end();return;}
  try{res.setHeader('Content-Type',({'.html':'text/html','.js':'application/javascript','.css':'text/css'})[extname(file)]||'application/octet-stream');res.end(await readFile(file));}
  catch{res.writeHead(404).end();}
});
await new Promise(done=>server.listen(0,'127.0.0.1',done));
const origin=`http://127.0.0.1:${server.address().port}`;
const browser=await chromium.launch({channel:'msedge',headless:true});
const report={views:[],externalRequests:[],mutations:[]};
valid.generated_at=new Date().toISOString();
valid.readiness=[{label:'실거래 라우트 환경',status:'pass',detail:'synthetic permission'}];
valid.strategies=[{strategy_id:'selected',deployment_id:'selected',name:'선택한 전략',symbol:'BTCUSDT',timeframe:'1h',broker_id:'binance',asset:'CRYPTO',live_allowed:true,live_small_eligible:true,lifecycle_status:'live'},
 {strategy_id:'other',deployment_id:'other',name:'선택하지 않은 저장 전략',symbol:'KRW-BTC',timeframe:'1h',broker_id:'upbit',live_allowed:false,lifecycle_status:'research'}];
valid.brokers=[{broker_id:'kis',name:'한국투자증권',missing_env:[]},{broker_id:'upbit',name:'Upbit',missing_env:[]}];
valid.accounts=[{broker_id:'kis',currency:'KRW/USD',broker_cash_value:null,broker_equity_value:null},{broker_id:'upbit',currency:'KRW',broker_cash_value:10000,broker_equity_value:10000,valuation_basis:'cash_only'}];
valid.reconciliation={summary:{last_run:new Date().toISOString(),account_count:2,position_count:0,error_count:1,mismatch_count:0,api_required_count:1,status:'warn'},errors:[{broker_id:'kis',scope:'account',detail:'synthetic KIS read failure'}]};
valid.continuous_runtime={running:false,profiles:{}};
try {
 for (const [width,theme] of [[1280,'light'],[1920,'light'],[1280,'dark']]) {
  const context=await browser.newContext({viewport:{width,height:1000},serviceWorkers:'block'});
  const page=await context.newPage(), errors=[];
  page.on('pageerror',error=>errors.push(error.message));
  await context.route('**/*',async route=>{
   const request=route.request(),url=new URL(request.url());
   if(url.origin!==origin){report.externalRequests.push(url.origin);await route.abort();return;}
   if(!url.pathname.startsWith('/api/')){await route.continue();return;}
   if(request.method()!=='GET'){report.mutations.push(url.pathname);await route.abort();return;}
   const body=url.pathname==='/api/snapshot'?valid:url.pathname==='/api/functional-test'?EMPTY_FUNCTIONAL_TEST_WORKSPACE:{ok:true};
   await route.fulfill({status:200,contentType:'application/json',body:JSON.stringify(body)});
  });
  await page.goto(origin);await page.getByText('현재 앱 주문 허용',{exact:true}).waitFor();
  await page.evaluate(theme=>document.documentElement.dataset.uiTheme=theme,theme);
  assert.equal(await page.evaluate(()=>document.documentElement.dataset.uiTheme),theme);
  assert.match(await page.locator('.launch-report-panel').innerText(),/점검 결과 미확인/);
  assert.doesNotMatch(await page.locator('.launch-report-panel').innerText(),/해제 가능/);
  const bar=page.locator('.live-environment-bar');
  assert.match(await bar.innerText(),/현재 앱 주문 허용[\s\S]*허용/);
  assert.doesNotMatch(await bar.innerText(),/실거래 잠금|위험 증가 주문|신규 진입/);
  assert.match(await page.locator('.topbar').innerText(),/실행 대기/);
  assert.doesNotMatch(await page.locator('.pretrade-doctor-panel').innerText().catch(()=>''),/전 전략 2개/);
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
  await page.screenshot({path:resolve(output,`overview-${width}-${theme}.png`),fullPage:true});
  assert.deepEqual(errors,[]);report.views.push({width,theme,pageErrors:errors,scopeAndPermissionDistinct:true});
  await context.close();
 }
 assert.deepEqual(report.externalRequests,[]);assert.deepEqual(report.mutations,[]);
}finally{await browser.close();await new Promise(done=>server.close(done));}
await writeFile(resolve(output,'report.json'),JSON.stringify(report,null,2));console.log(JSON.stringify(report));
