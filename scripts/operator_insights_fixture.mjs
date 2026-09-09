import { verifyAppearancePolish, verifySelectedCards } from './surface_polish_probe.mjs';
// Offline production-bundle regression. Never starts Python or reads account state.
// All API responses are intercepted in the browser; the static server rejects /api/.
import assert from 'node:assert/strict';
import { probeRestoredUi, inspectRestoredTabs } from './font_restore_probe.mjs';
const fontReview=process.env.FONT_RESTORE_REVIEW==='1';
import { readFile, mkdir, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { extname, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { runInNewContext } from 'node:vm';
import { chromium } from 'playwright-core';
import { EMPTY_FUNCTIONAL_TEST_WORKSPACE } from '../src/functionalTestModel.js';

const root = fileURLToPath(new URL('../', import.meta.url));
const dist = resolve(root, 'dist');
const output = resolve(root, 'output/playwright/operator-insights');
const appSource = await readFile(resolve(root, 'src/App.jsx'), 'utf8');
const fallbackLiteral = appSource.match(/const fallbackSnapshot = (\{[\s\S]*?\n\});\s*const PANEL_SIZE_STORAGE_KEY/);
assert.ok(fallbackLiteral, 'the fail-closed fixture base must be an explicit static literal');
const snapshot = JSON.parse(JSON.stringify(runInNewContext(`(${fallbackLiteral[1]})`, {}, { timeout: 500 })));
snapshot.api_connected = true;
if(fontReview) snapshot.technical_logs=['INFO','WARN','ERROR'].map((level,i)=>({level,event_id:`font-log-${i}`,timestamp:'2026-09-08T01:00:00Z',source:'오프라인 글꼴 검사',message:`${level} 의미색 검증` }));
snapshot.execution_availability = {
  schemaVersion: 'live-execution-availability-v1', authorizationGranted: false,
  ordinaryContinuous: { monitorSupported: true, liveDispatchAvailable: false,
    detail: '현재 일반 자동매매의 실주문 전송은 차단되어 있습니다.',
    nextAction: '관찰 모드에서 시세 수신과 전략 판단을 확인할 수 있습니다.' },
};
snapshot.checklist = [
  { key: 'api_keys_reviewed', label: 'API·계좌 자동 확인', source: 'automatic', checked: false, required: true },
  { key: 'risk_limits_reviewed', label: '위험 한도 검토', source: 'pending', checked: false, required: true },
  { key: 'notification_channel_reviewed', label: '알림 채널 확인', source: 'pending', checked: false, required: true },
  { key: 'operator_takeover_ready', label: '수동 개입 준비', source: 'pending', checked: false, required: true },
  { key: 'position_reconcile_reviewed', label: '포지션 대조 자동 확인', source: 'automatic', checked: false, required: true },
];
snapshot.generated_at = '2026-09-05T00:00:00Z';
snapshot.live_governance.deploymentId = 'fixture-deployment';
snapshot.live_governance.activeSession = { lifecycleState: 'DRAINING', healthStatus: 'TAINTED', sessionId: 'fixture-session' };
snapshot.strategies = [{ strategy_id: 'fixture-strategy', deployment_id: 'fixture-deployment', name: '화면 검증용 전략', asset: 'KR_STOCK', broker_id: 'kis', symbol: '005930', timeframe: '1d', plugin: 'moving_average_cross', lifecycle: { status: 'before-live-small' }, lifecycle_status: 'before-live-small', promotion: { stage: 'LIVE_SMALL' }, live_small_eligible: false, parameters: {} }];
snapshot.strategies[0].artifactLifecycle = { status: 'backtested', source: 'lifecycle.status', conflicts: [] };
snapshot.strategies[0].artifact_reference = { artifactId: 'fixture-artifact', artifactHash: 'a'.repeat(64) };
snapshot.strategies[0].deploymentLifecycle = { status: 'before-live-small', source: 'deployment-registry', updatedAt: '', history: [] };
snapshot.automation_profiles = [{ id: 'stock', title: '화면 검증용 프로필', provider: 'kis', provider_label: 'KIS fixture', asset_scope: ['KR_STOCK'], broker_ids: ['kis'], strategy_count: 1, live_strategy_count: 0, ready: false, mode: 'MONITOR', enabled: false, detail: '브로커 연결 없는 화면 테스트', last_action: '대기' }];
snapshot.continuous_runtime = { running: false, phase: 'STOPPED', profiles: {} };

snapshot.accounts = [
  { broker_id: 'kis', broker_name: 'KIS 국내', account: '합성 국내계좌', currency: 'KRW', broker_cash_value: 100000, broker_equity_value: 400000, valuation_basis: 'broker_equity' },
  { broker_id: 'upbit', broker_name: 'Upbit', account: '합성 현물계좌', currency: 'KRW', broker_cash_value: 50000, broker_equity_value: 50000, valuation_basis: 'cash_only' },
  { broker_id: 'binance-futures', broker_name: 'Binance 선물', account: '합성 선물계좌', currency: 'USDT', broker_cash_value: 80, broker_equity_value: 100, valuation_basis: 'margin_balance' },
];
snapshot.positions = [
  { broker_id: 'kis', broker_name: 'KIS', asset: '한국주식', symbol: '005930', broker_qty_value: 2, broker_qty: '2', program_qty: '2', broker_value: 200000, current_price: 100000, currency: 'KRW', valuation_basis: 'market_value' },
  { broker_id: 'kis', broker_name: 'KIS', asset: '한국주식', symbol: '035420', broker_qty_value: 1, broker_qty: '1', program_qty: '1', broker_value: 100000, current_price: 100000, currency: 'KRW', valuation_basis: 'market_value' },
  { broker_id: 'upbit', broker_name: 'Upbit', asset: '코인', symbol: 'KRW-BTC', broker_qty_value: .1, broker_qty: '.1', program_qty: '.1', broker_value: 50000, current_price: 500000, currency: 'KRW', valuation_basis: 'market_value' },
  { broker_id: 'binance-futures', broker_name: 'Binance 선물', asset: '코인', symbol: 'BTCUSDT', position_side: 'LONG', broker_qty_value: .01, broker_qty: '.01', program_qty: '.01', broker_value: 400, current_price: 40000, currency: 'USDT', valuation_basis: 'market_notional' },
];
const reviewKey = 'd'.repeat(64);
snapshot.operator_review_revision='fixture-1';
snapshot.operator_runtime_bindings=[{running:true,profile:'stock',deploymentId:'running-old',sessionId:'session-old',mode:'MONITOR',lifecycle:'RUNNING',manifest:{revision:2,manifestHash:'a'.repeat(64),strategyArtifactHash:'b'.repeat(64),brokerRoute:'kis',accountFingerprint:'c'.repeat(64),metadata:{strategyIds:['running-strategy']}}}];
snapshot.strategies.push({...snapshot.strategies[0],strategy_id:'running-strategy',deployment_id:'running-old',name:'실행 고정 전략'});
snapshot.continuous_runtime={running:true,profiles:{stock:{profileId:'stock',running:true,deploymentId:'running-old',phase:'RUNNING',mode:'MONITOR'}}};
snapshot.positions[0].program_qty_value=1;snapshot.positions[0].detail='거래소 2주, 원장 1주';
snapshot.reconciliation.positions=snapshot.positions;snapshot.reconciliation.summary={...snapshot.reconciliation.summary,last_run:'2026-09-10T02:00:00Z'};
snapshot.risk_settings=[{key:'strategy_capital_limit_krw',label:'전략별 자본 한도',value:300000,min:100000,max:10000000,step:1000,unit:'KRW',detail:'단일 전략 최대 명목금액'}];
snapshot.orders=[{order_id:'fixture-order',broker_id:'kis',deployment_id:'fixture-deployment',strategy_id:'fixture-artifact',symbol:'005930',side:'BUY',quantity:1,state:'UNKNOWN',queue_state:'reconcile_required',reason:'접수 결과 미확정',reconciliation_warning:'브로커 응답 누락 · 중복 전송 금지',updated_at:'2026-09-10T01:30:00Z',time:'2026-09-10T01:00:00Z'}];
snapshot.durable_audit=[{event_id:'fixture-event',operatorRecordKey:reviewKey,occurred_at:'2026-09-10T01:31:00Z',level:'WARN',source:'주문 상태 대조',message:'첫 번째 조회에서 주문 상태 미확정',order_id:'fixture-order',broker_id:'kis',category:'ORDER'}];
snapshot.technical_logs=snapshot.durable_audit;
snapshot.program_ledger.execution_events=[{event_id:'fill1',order_id:'fixture-order',broker_id:'kis',symbol:'005930',side:'BUY',state:'PARTIALLY_FILLED',occurred_at:'2026-09-10T01:30:00Z',quantity:1,price:100000,raw:{fee:0,quantity:1,price:100000,quantity_mode:'cumulative'}}];
snapshot.execution_events={recent:snapshot.program_ledger.execution_events,last_poll:'2026-09-10T01:31:00Z'};
const before={deploymentId:'fixture-deployment',metadata:{allowedSymbols:['005930']},strategyArtifactHash:'a'.repeat(64),brokerRoute:'kis',accountFingerprint:'c'.repeat(64),reviewWeights:{'s:005930':.5},reviewRisk:{strategy_capital_limit_krw:200000},riskPolicyHash:'one'};
const review={ok:true,scope:'fixture-deployment',comparisonBasis:'실행 중 고정 버전',before,after:{...before,reviewWeights:{'s:005930':.25},reviewRisk:{strategy_capital_limit_krw:300000},riskPolicyHash:'two'},history:[{recorded_at:'2026-09-10T02:00:00Z',checks:[{label:'새 권한 문제',status:'fail',detail:'인증 권한을 확인하세요'},{label:'시세',status:'pass'}]},{recorded_at:'2026-09-10T01:00:00Z',checks:[{label:'시세',status:'fail',detail:'시세 없음'}]}]};
const server=createServer(async(req,res)=>{try{const url=new URL(req.url,'http://127.0.0.1');if(req.method!=='GET'||url.pathname.startsWith('/api/')){res.writeHead(403);res.end('offline-only');return;}const file=resolve(dist,url.pathname==='/'?'index.html':url.pathname.slice(1));if(!file.startsWith(dist+sep)){res.writeHead(403);res.end();return;}const bytes=await readFile(file);res.writeHead(200,{'Content-Type':{'.html':'text/html','.js':'application/javascript','.css':'text/css','.svg':'image/svg+xml'}[extname(file)]||'application/octet-stream'});res.end(bytes);}catch{res.writeHead(404);res.end();}});
await new Promise(done=>server.listen(0,'127.0.0.1',done));const origin=`http://127.0.0.1:${server.address().port}`;
await mkdir(output,{recursive:true});
const browser=await chromium.launch({channel:'msedge',headless:true});
const report={ok:false,brokerIO:0,external:[],writes:[],views:[],errors:[]};
try{
for(const width of [1024,1920])for(const theme of ['light','dark']){
 const context=await browser.newContext({viewport:{width,height:1100},serviceWorkers:'block'});
 let note={record_key:reviewKey,text:'',revision:0,updated_at:'',history:[]};
 await context.addInitScript(()=>localStorage.setItem('live_trader.deploymentContext.v1',JSON.stringify('fixture-deployment')));
 await context.route('**/*',async route=>{
  const req=route.request(),url=new URL(req.url());if(url.origin!==origin){report.external.push(url.origin);await route.abort();return;}
  if(!url.pathname.startsWith('/api/')){await route.continue();return;}
  let body={ok:true};
  if(req.method()!=='GET' && ['/api/execution-events','/api/reconcile'].includes(url.pathname)){ report.writes.push(url.pathname);body={ok:true,snapshot}; } else if(req.method()!=='GET'){
   report.writes.push(url.pathname);assert.equal(url.pathname,'/api/operator-note','only explicit local memo save may POST');
   const payload=req.postDataJSON();assert.equal(payload.record_key,reviewKey);assert.equal(payload.revision,note.revision);
   note={...note,text:payload.text,revision:note.revision+1,updated_at:'2026-09-10T03:00:00Z',history:[{text:payload.text,revision:note.revision+1,updated_at:'2026-09-10T03:00:00Z'},...note.history]};body={ok:true,note};
  } else if(url.pathname==='/api/snapshot')body=snapshot;
  else if(url.pathname==='/api/operator-review')body=review;
  else if(url.pathname==='/api/operator-note')body={ok:true,note};
  else if(url.pathname==='/api/operator-order-history')body={ok:true,events:snapshot.durable_audit,total:1,nextOffset:null};
  else if(url.pathname==='/api/functional-test')body=EMPTY_FUNCTIONAL_TEST_WORKSPACE;
  else if(url.pathname==='/api/env-settings')body={settings:{fields:[],groups:[]}};
  else if(url.pathname==='/api/search-presets')body={schemaVersion:1,presets:[]};
  else if(url.pathname==='/api/artifact-metadata')body={items:{}};
  await route.fulfill({status:200,contentType:'application/json',body:JSON.stringify(body)});
 });
 const page=await context.newPage();page.on('pageerror',error=>report.errors.push(error.message));
 await page.goto(origin);await page.getByRole('heading',{name:'시작 점검',exact:true}).waitFor();await page.evaluate(theme=>document.documentElement.dataset.uiTheme=theme,theme);
 const nav=async label=>{await page.locator('.nav-list').getByRole('button',{name:label,exact:true}).click();await page.getByRole('heading',{name:label,exact:true}).waitFor();};
 const capture=async label=>{if(label==='capital')await page.getByLabel('금액 한도 미리보기').scrollIntoViewIfNeeded();await page.evaluate(()=>new Promise(done=>requestAnimationFrame(()=>requestAnimationFrame(done))));assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false,`${label} overflow`);report.views.push({width,theme,label});await page.screenshot({path:resolve(output,`${label}-${theme}-${width}.png`),fullPage:false});};
 await page.locator('.live-environment-identity select').selectOption('fixture-deployment');
 await page.getByLabel('직전 시작 점검과 비교').locator('.operator-inline-toggle').first().click();
 await page.getByText('새 권한 문제 · 인증 권한을 확인하세요',{exact:true}).waitFor();assert.ok((await page.getByLabel('직전 시작 점검과 비교').textContent()).includes('해결 1'));
 assert.ok((await page.getByLabel('실제 실행 고정 정보').textContent()).includes('조회 선택과 다름'));await page.getByLabel('실제 실행 고정 정보').locator('button').first().click();await capture('preflight');await page.getByLabel('실제 실행 고정 정보').locator('button').first().click();
 await nav('운용 전략');await page.getByText('기존 배포와 달라진 설정',{exact:true}).click();await page.getByText('목표 비중 s:005930',{exact:true}).waitFor();await capture('deployment');
 await nav('실거래 운용');await page.getByText('중지 방법별 영향 비교',{exact:true}).click();assert.ok((await page.getByText('새 주문이 없는 이유 · 005930',{exact:true}).locator('..').textContent()).includes('조회 중인 배포는 실행 중이 아닙니다'));await capture('runtime');
 await page.getByRole('tab',{name:'한도·안전장치',exact:true}).click();await page.getByRole('button',{name:/리스크 정책·재시도·선물 계산/}).click();
 const input=page.locator('.risk-settings-panel input');await input.fill('400000');await page.getByLabel('금액 한도 미리보기').getByText(/현재 사용 200,000원 · 보유 기준 잔여 200,000원/).waitFor();await input.fill('300000');await capture('capital');
 await nav('계좌·잔고');await page.getByText('종목·체결·수수료 대조 근거',{exact:true}).click();await page.getByText('거래소 2주, 원장 1주',{exact:true}).waitFor();await page.getByText(/통화 미기록 · 기준 미기록 · 확정 비용 미확인/).waitFor();await capture('reconciliation');
 await nav('주문·체결');await page.getByText('주문 조사 내역 · 마지막 대조',{exact:true}).waitFor();await page.getByText('첫 번째 조회에서 주문 상태 미확정',{exact:false}).first().waitFor();assert.equal(await page.getByRole('button',{name:'상태 대조 후 재시도',exact:true}).isDisabled(),true);await capture('order');
 await nav('실행 기록');const editor=page.getByLabel('실행 기록 메모 입력');await editor.waitFor();await editor.fill('확인 후 다음 대조에서 재확인');await page.getByRole('button',{name:'메모 저장',exact:true}).click();await page.getByText('메모 저장됨',{exact:true}).waitFor();await capture('note');await page.reload();await page.getByLabel('실행 기록 메모 입력').waitFor();await page.waitForFunction(()=>document.querySelector('[aria-label="실행 기록 메모 입력"]')?.value==='확인 후 다음 대조에서 재확인');
 assert.equal(report.errors.length,0);await context.close();
}
assert.equal(report.external.length,0);report.ok=true;
}finally{await writeFile(resolve(output,'report.json'),JSON.stringify(report,null,2));await browser.close();await new Promise(done=>server.close(done));}
console.log(JSON.stringify(report));
