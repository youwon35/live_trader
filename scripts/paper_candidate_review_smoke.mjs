// Actual component/api + production-generated synthetic responses. No broker/runtime server.
import assert from 'node:assert/strict';
import { build } from 'esbuild';
import { chromium } from 'playwright-core';
import { execFileSync } from 'node:child_process';
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { createServer } from 'node:http';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
const root=fileURLToPath(new URL('../',import.meta.url)),out=resolve(root,'output/sep11-functional-audit/candidate-ui');
await mkdir(out,{recursive:true});
execFileSync(resolve(root,'.venv/Scripts/python.exe'),[resolve(root,'scripts/paper_candidate_review_fixture.py')],{cwd:root,stdio:'pipe'});
const responses=JSON.parse(await readFile(resolve(out,'responses.json'),'utf8'));
await build({entryPoints:[resolve(root,'scripts/fixtures/paper_candidate_review.jsx')],bundle:true,format:'esm',jsx:'automatic',outfile:resolve(out,'bundle.js')});
const server=createServer(async(req,res)=>{
 try {const pathname=new URL(req.url,'http://localhost').pathname;
 if(req.method!=='GET'||!['/','/bundle.js','/bundle.css'].includes(pathname)){res.writeHead(403).end();return;}
 if(pathname==='/'){res.writeHead(200,{'content-type':'text/html'}).end('<html lang="ko"><head><meta charset="UTF-8"><link rel="stylesheet" href="/bundle.css"></head><body><div id="root"></div><script type="module" src="/bundle.js"></script></body></html>');return;}
 res.writeHead(200,{'content-type':pathname.endsWith('.css')?'text/css':'text/javascript'}).end(await readFile(resolve(out,pathname.slice(1))));
 }catch {res.writeHead(500).end();}
});
await new Promise(r=>server.listen(0,'127.0.0.1',r));
const base=`http://127.0.0.1:${server.address().port}`,browser=await chromium.launch({channel:'msedge',headless:true});
const reports=[];
try{
 for(const width of [1280,1920])for(const theme of ['light','dark']){
 const page=await browser.newPage({viewport:{width,height:1080}});let registered=false,posts=0,trialPosts=0,gets=0,external=0;const errors=[];
 page.on('pageerror',error=>errors.push(error.message));
 await page.addInitScript(({theme})=>{window.reviewTheme=theme;window.pywebview={api:{functional_http_session:async()=>({ok:true,available:true,csrfHeader:'X-LiveTrader-CSRF',csrfToken:'fixture-csrf-'.repeat(4)})}};},{theme});
 await page.route('**/*',async route=>{const request=route.request(),url=new URL(request.url());
 if(url.origin!==base){external++;await route.abort();return;}
 if(url.pathname==='/api/paper-candidates'){assert.equal(request.method(),'GET');gets++;await route.fulfill({json:registered?responses.after:responses.before});return;}
 if(url.pathname==='/api/paper-candidates/import'){assert.equal(request.method(),'POST');assert.equal(request.headers()['x-livetrader-csrf'],'fixture-csrf-'.repeat(4));assert.deepEqual(request.postDataJSON(),responses.request);posts++;registered=true;await route.fulfill({json:responses.result});return;}
 if(url.pathname==='/api/monitor-trial/run'){assert.equal(request.method(),'POST');assert.equal(request.headers()['x-livetrader-csrf'],'fixture-csrf-'.repeat(4));assert.deepEqual(request.postDataJSON(),responses.trialRequest);trialPosts++;await route.fulfill({json:responses.trialResult});return;}
 if(url.pathname.startsWith('/api/')){external++;await route.abort();return;}
 await route.continue();});
 await page.goto(base);await page.getByRole('heading',{name:'운용 전략 · 검토 대기 후보'}).waitFor();
 assert.equal(posts,0);assert.equal(gets,0);
 await page.getByText('모의거래 검증 근거 확인',{exact:true}).click();
 await page.getByRole('button',{name:'Paper 검증 근거 새로고침',exact:true}).click();
 await page.getByRole('button',{name:'검토 대기 후보 등록',exact:true}).waitFor();assert.equal(posts,0);assert.equal(gets,1);
 await page.getByRole('button',{name:'검토 대기 후보 등록',exact:true}).click();
 await page.getByRole('button',{name:'등록한 배포 보기',exact:true}).waitFor();
 await page.waitForFunction(id=>window.reviewSelected===id,responses.result.deploymentId);
 assert.equal(posts,1);assert.equal(gets,2);assert.equal(await page.getByRole('button',{name:'검토 대기 후보 등록',exact:true}).count(),0);
 await page.getByRole('button',{name:'등록한 배포 보기',exact:true}).click();assert.equal(posts,1);
 await page.getByText('봉인 정보 보기',{exact:true}).click();
 await page.getByText('현재 배포:',{exact:false}).waitFor();
 assert.match(await page.locator('body').innerText(),/MONITOR/);
 await page.getByText('봉인 정보 보기',{exact:true}).click();
 assert.equal(await page.getByRole('button',{name:'주문 없이 연결 시험',exact:true}).isDisabled(),true);
 await page.getByLabel('시험용 구성 선택').selectOption(`${responses.trialRequest.rootKey}:${responses.trialRequest.portfolioId}`);
 assert.equal(trialPosts,0);
 await page.getByRole('button',{name:'주문 없이 연결 시험',exact:true}).click();
 await page.getByText('입력 근거와 한계 보기',{exact:true}).waitFor();
 assert.equal(trialPosts,1);assert.equal(posts,1);assert.equal(await page.evaluate(()=>window.reviewSelected),responses.result.deploymentId);
 assert.match(await page.locator('body').innerText(),/주문 0건/);
 await page.getByText('입력 근거와 한계 보기',{exact:true}).click();
 assert.match(await page.locator('body').innerText(),/실제 전체 기간: 표본 시각이 없어 미확인/);
 const screenshot=resolve(out,`registered-${theme}-${width}.png`);await page.screenshot({path:screenshot,fullPage:true});
 const font=await page.locator('h1').evaluate(el=>getComputedStyle(el).fontFamily);assert.match(font,/Malgun Gothic/);
 assert.deepEqual(errors,[]);assert.equal(external,0);
 reports.push({width,theme,posts,trialPosts,gets,external,errors,selectedId:responses.result.deploymentId,font,screenshot});await page.close();
 }
 const result={ok:true,cases:reports,realOrders:0,brokerApiCalls:0,productionFixture:'temporary store: valid evidence -> importer -> registry -> inbox'};
 await writeFile(resolve(out,'report.json'),JSON.stringify(result,null,2));console.log(JSON.stringify({ok:true,cases:reports.length,external:0,orders:0,report:resolve(out,'report.json')}));
}finally{await browser.close();await new Promise(r=>server.close(r));}
