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
const output=process.env.LIVE_SNAPSHOT_OUTPUT || resolve(root,'output/playwright/snapshot-response');
await mkdir(output,{recursive:true});
const appSource=await readFile(resolve(root,'src/App.jsx'),'utf8');
const literal=appSource.match(/const fallbackSnapshot = (\{[\s\S]*?\n\});\s*const PANEL_SIZE_STORAGE_KEY/);
assert.ok(literal);
const valid=JSON.parse(JSON.stringify(runInNewContext(`(${literal[1]})`,{},{timeout:500})));
valid.generated_at='2026-10-28 01:30:00';
valid.continuous_runtime={running:false,profiles:{}};
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
const report={scenarios:[],externalRequests:[],mutations:[]};
try{
  for(const [name,bad] of [['empty',{}],['error',{ok:false,reason:'snapshot unavailable'}],['invalid-flags',{...valid,dry_run:'false'}],['invalid-rows',{...valid,strategies:null}],['invalid-row-item',{...valid,brokers:[null]}]]){
    const context=await browser.newContext({viewport:{width:1280,height:900},serviceWorkers:'block'});
    const page=await context.newPage(),errors=[];
    await page.clock.install();
    page.on('pageerror',error=>errors.push(error.message));
    let response=bad;
    await context.route('**/*',async route=>{
      const req=route.request(),url=new URL(req.url());
      if(url.origin!==origin){report.externalRequests.push(url.origin);await route.abort();return;}
      if(!url.pathname.startsWith('/api/')){await route.continue();return;}
      if(req.method()!=='GET'){report.mutations.push({path:url.pathname,method:req.method()});await route.abort();return;}
      const body=url.pathname==='/api/snapshot'?response:url.pathname==='/api/functional-test'?EMPTY_FUNCTIONAL_TEST_WORKSPACE:{ok:true};
      await route.fulfill({status:200,contentType:'application/json',body:JSON.stringify(body)});
    });
    await page.goto(origin);
    await page.locator('.api-connection-banner').waitFor();
    assert.equal(await page.getByRole('heading',{name:'시작 점검',exact:true}).count(),1);
    assert.match(await page.locator('.topbar').innerText(),/실행 상태 미확인/);
    assert.match(await page.locator('.sidebar-footer').innerText(),/API 끊김/);
    assert.match(await page.locator('.topbar').innerText(),/미결 주문 미확인/);
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false);
    await page.screenshot({path:resolve(output,`${name}-blocked.png`)});
    response=valid;
    await page.getByRole('button',{name:'다시 연결',exact:true}).click();
    await page.waitForFunction(()=>!document.body.innerText.includes('현재 상태 응답이 불완전합니다.'));
    assert.match(await page.locator('.sidebar-footer').innerText(),/NORMAL|정상/);
    assert.match(await page.locator('.topbar').innerText(),/실행 대기/);
    assert.deepEqual(errors,[]);
    report.scenarios.push({name,rendered:true,invalidResponseBlocked:true,recoveredWithoutReload:true,pageErrors:errors});
    await context.close();
  }
  assert.deepEqual(report.externalRequests,[]);assert.deepEqual(report.mutations,[]);
}finally{await browser.close();await new Promise(done=>server.close(done));}
await writeFile(resolve(output,'report.json'),JSON.stringify(report,null,2));
console.log(JSON.stringify(report,null,2));
