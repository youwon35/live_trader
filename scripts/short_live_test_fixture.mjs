// Offline UI interaction fixture. No Python server, account, broker API or real orders.
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { createServer } from "node:http";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { resolve, join, basename } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const dependencyRoot = process.env.SHORT_LIVE_NODE_MODULES || join(root, "node_modules");
const designRoot = process.env.SHORT_LIVE_DESIGN_ROOT || resolve(root, "../../packages/design");
const require = createRequire(join(dependencyRoot, "fixture-resolver.cjs"));
const { build } = require("esbuild");
const { chromium } = require("playwright-core");
const output = join(root, "output", "short-live-fixture");
await mkdir(output, { recursive: true });
const entry = `
import * as React from 'react';
import {createRoot} from 'react-dom/client';
import ShortLiveTestPanel from './src/ShortLiveTestPanel.jsx';
import './src/styles.css';
const hash='a'.repeat(64);
const row={selectionKey:'STRATEGY_INSTANCE:fixture:one',kind:'STRATEGY_INSTANCE',id:'fixture',label:'화면검사 전용 전략',route:'KIS_KR_CASH',routes:['KIS_KR_CASH'],timeframe:'1d',timeframes:['1d'],artifactFileSha256:hash,instanceFileSha256:hash,memberCount:1,pluginIds:['moving_average_cross'],symbols:['005930'],eligibleForFunctionalPreparation:true,blockers:[]};
const hourly={...row,selectionKey:'STRATEGY_INSTANCE:fixture:hourly',id:'fixture-hourly',label:'소액 배정금 100% 기능시험 · BTCUSDT · 1h',route:'BINANCE_SPOT',routes:['BINANCE_SPOT'],timeframe:'1h',timeframes:['1h'],symbols:['BTCUSDT'],positionSizePercents:[100],functionalSizingProfiles:['BOUNDED_TEST_SLEEVE_100_PERCENT_V1']};
const originalHourly={...hourly,selectionKey:'STRATEGY_INSTANCE:fixture:original-hourly',id:'original-hourly',label:'기존 5% 1시간봉 전략',positionSizePercents:[5],functionalSizingProfiles:[]};
let state={schemaVersion:'fixture-only',fixture:true,status:'IDLE',observedAt:'2026-10-24T06:00:00Z',catalog:{candidates:[row,hourly,originalHourly]},blockers:[],prepared:null,runtime:null};
const status=()=>({...structuredClone(state),catalogIncluded:false,catalog:[]});let catalogReads=0;let failSnapshot=false;let holdCatalog=false;let releaseCatalog=null;const calls=[];let failStart=false;let failStop=false;let holdNext=false;let releaseSnapshot=null;
window.__shortLiveFixture={calls, state:()=>state,catalogReads:()=>catalogReads,failSnapshot:()=>{failSnapshot=true;},holdCatalog:()=>{holdCatalog=true;},catalogHeld:()=>Boolean(releaseCatalog),releaseCatalog:()=>{releaseCatalog?.();releaseCatalog=null;}, setBlocked:()=>{state={...state,blockers:['insufficient-balance']};}, reset:()=>{state={...state,blockers:[]};}, failStart:()=>{failStart=true;},failStop:()=>{failStop=true;},holdNextSnapshot:()=>{holdNext=true;},snapshotHeld:()=>Boolean(releaseSnapshot),releaseSnapshot:()=>{releaseSnapshot?.();releaseSnapshot=null;},expirePreparation:()=>{state={...state,prepared:{...state.prepared,expiresAt:new Date(Date.now()-1000).toISOString()}};}};
const api={catalog:async()=>{catalogReads++;if(holdCatalog){holdCatalog=false;await new Promise(resolve=>{releaseCatalog=resolve;});}return {schemaVersion:'short-live-test-catalog-v1',catalog:structuredClone(state.catalog.candidates),readOnly:true,authorityGranted:false};},snapshot:async()=>{if(failSnapshot){failSnapshot=false;throw new Error('검사용 상태 시간 초과');}if(holdNext){holdNext=false;const old=status();return new Promise(resolve=>{releaseSnapshot=()=>resolve(old);});}return status();},prepare:async(body)=>{calls.push({method:'prepare',body});state={...state,status:'PREPARED',prepared:{preparationId:'fixture-preparation',scopeHash:hash,readyToStart:true,targets:body.selections.map(target=>state.catalog.candidates.find(candidate=>candidate.selectionKey===target.selectionKey)),blockers:[],durationHours:body.durationHours,capitalKrwPerBroker:body.capitalKrwPerBroker,dailyLossStopKrwPerBroker:body.dailyLossStopKrwPerBroker,expiresAt:new Date(Date.now()+180000).toISOString()}};return {snapshot:status()};},start:async(body)=>{calls.push({method:'start',body});state={...state,status:'RUNNING',runtime:{sessionId:'fixture-session',status:'RUNNING',ownerAlive:true,workerAlive:true,newEntriesBlocked:false,naturalExitObserved:false,unknownOrderCount:0,terminalVerified:false,startedAt:'2026-10-24T06:05:00Z',endsAt:'2026-10-24T12:05:00Z',brokerRows:[{route:'BINANCE_SPOT',effectiveRouteBudgetKrw:23000,brokerBudgetKrw:100000,exposureKrw:0,realizedLossKrw:0,unrealizedLossKrw:0,dailyLossStopKrw:50000,lossBasis:'ACCOUNT_NATIVE_EQUITY_DECLINE_AT_FRESH_FX',actualFeeKnown:false,ready:true}],strategyRows:[{instanceId:'fixture-daily',pluginId:'moving_average_cross',symbol:'005930',route:'KIS_KR_CASH',timeframe:'1d',lastSignal:'NOT_OBSERVED',decisionCount:0,orderAttemptCount:0,fillObservationCount:0}]}};if(failStart)throw new Error('FIXTURE_LOST_REPLY');return {snapshot:status()};},stop:async(body)=>{calls.push({method:'stop',body});state={...state,status:'STOPPING',runtime:{...state.runtime,status:'STOPPING',newEntriesBlocked:true}};if(failStop)throw new Error('FIXTURE_LOST_STOP_REPLY');return {snapshot:status()};}};
function FixtureApp(){const [view,setView]=React.useState('setup');return <ShortLiveTestPanel api={api} pollMs={1000} viewMode={view} onOpenMonitor={()=>{if(state.status!=='RUNNING')setView('monitor');}} onOpenSetup={()=>setView('setup')}/>;}
createRoot(document.getElementById('root')).render(<FixtureApp/>);
`;
await build({ stdin: { contents: entry, resolveDir: root, loader: "jsx" }, bundle: true, format: "esm",
  nodePaths: [dependencyRoot], outfile: join(output, "fixture.js"), loader: { ".css": "css" },
  define: { "process.env.NODE_ENV": '"test"' }, logLevel: "silent",
  plugins: [{ name: "shared-design-location", setup(build) {
    build.onResolve({ filter: /packages\/design\/(design-tokens|interaction-standards)\.css$/ },
      (args) => ({ path: join(designRoot, basename(args.path)) }));
  } }] });
await writeFile(join(output, "index.html"), '<!doctype html><html lang="ko"><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Offline short live UI fixture</title><link rel="stylesheet" href="/fixture.css"><div id="root"></div><script type="module" src="/fixture.js"></script></html>');
let forbiddenRequests = 0;
const server = createServer(async (req, res) => {
  const url = new URL(req.url, "http://127.0.0.1");
  if (req.method !== "GET" || !["/", "/fixture.js", "/fixture.css"].includes(url.pathname)) {
    forbiddenRequests += 1; res.writeHead(403); res.end("offline fixture only"); return;
  }
  const filename = url.pathname === "/" ? "index.html" : url.pathname.slice(1);
  try { const bytes = await readFile(join(output, filename)); res.writeHead(200, { "Content-Type": filename.endsWith(".js") ? "application/javascript" : filename.endsWith(".css") ? "text/css" : "text/html" }); res.end(bytes); }
  catch { res.writeHead(404); res.end(); }
});
await new Promise((done) => server.listen(0, "127.0.0.1", done));
const origin = `http://127.0.0.1:${server.address().port}`;
const browser = await chromium.launch({ channel: "msedge", headless: true });
const report = { fixtureOnly: true, realAccountQueries: 0, realOrders: 0, screenshots: [], externalRequests: [], errors: [], checks: [] };
try {
  const context = await browser.newContext({ viewport: { width: 1280, height: 1000 }, serviceWorkers: "block" });
  await context.route("**/*", async (route) => {
    const url = new URL(route.request().url());
    if (url.origin !== origin || url.pathname.startsWith("/api/")) { report.externalRequests.push(url.href); await route.abort(); }
    else await route.continue();
  });
  const page = await context.newPage(); page.on("pageerror", (error) => report.errors.push(error.message));
  await page.goto(origin);
  await page.getByText("오프라인 화면 검사", { exact: false }).waitFor();
  await page.getByRole("heading", { name: "짧은 실거래 기능시험", exact: true }).waitFor();
  const start = page.getByRole("button", { name: "동의하고 실거래 시작", exact: true });
  assert.equal(await start.isDisabled(), true);
  assert.deepEqual(await page.evaluate(() => window.__shortLiveFixture.calls), []);
  report.checks.push("mount is GET-only with no implicit prepare/start");
  await page.waitForFunction(() => window.__shortLiveFixture.catalogReads() === 1);
  await page.getByRole("button", { name: "전략 목록 새로 확인", exact: true }).waitFor();
  await page.evaluate(() => window.__shortLiveFixture.failSnapshot());
  await page.getByRole("button", { name: "실행 상태 새로 확인", exact: true }).click();
  await page.getByText("상태 조회: 검사용 상태 시간 초과", { exact: true }).waitFor();
  await page.getByRole("button", { name: "실행 상태 새로 확인", exact: true }).click();
  await page.waitForFunction(() => !document.body.textContent.includes("상태 조회: 검사용 상태 시간 초과"));
  assert.equal(await page.evaluate(() => window.__shortLiveFixture.catalogReads()), 1);
  await page.evaluate(() => window.__shortLiveFixture.holdCatalog());
  await page.getByRole("button", { name: "전략 목록 새로 확인", exact: true }).click();
  await page.waitForFunction(() => window.__shortLiveFixture.catalogHeld());
  await page.getByRole("button", { name: "실행 상태 새로 확인", exact: true }).click();
  await page.getByText("최근 조회 성공", { exact: false }).waitFor();
  await page.getByText("전략 목록 미확인", { exact: false }).waitFor();
  assert.equal(await page.getByText("이 조건에 정확히 연결된 후보가 없습니다.", { exact: false }).count(), 0);
  assert.equal(await page.getByRole("button", { name: "자료·계좌 준비 검사 (주문 없음)", exact: true }).isDisabled(), true);
  await page.evaluate(() => window.__shortLiveFixture.releaseCatalog());
  await page.getByRole("button", { name: "전략 목록 새로 확인", exact: true }).waitFor();
  report.checks.push("catalog discovery is separate from fresh status; recovery clears only the polling error");
  await page.getByRole("button", { name: "1시간봉 소액 후보 보기", exact: true }).click();
  const hourly = page.getByRole("checkbox", { name: "시험 대상 소액 배정금 100% 기능시험 · BTCUSDT · 1h", exact: true });
  await hourly.waitFor(); assert.equal(await hourly.isChecked(), false);
  assert.equal(await page.getByRole("checkbox", { name: "시험 대상 기존 5% 1시간봉 전략", exact: true }).count(), 0);
  await hourly.check();
  const readsBeforeNavigation=await page.evaluate(() => window.__shortLiveFixture.catalogReads());
  await page.getByRole('button',{name:'실시간 감시 열기',exact:true}).click();
  await page.getByRole('button',{name:'시험 설정·시작·중지',exact:true}).click();
  assert.equal(await hourly.isChecked(),true);
  assert.equal(await page.evaluate(() => window.__shortLiveFixture.catalogReads()),readsBeforeNavigation);
  assert.deepEqual(await page.evaluate(() => window.__shortLiveFixture.calls),[]);
  report.checks.push('monitor navigation preserves selections with one catalog reader and no starts');
  await page.screenshot({ path: join(output, "hourly-desktop.png"), fullPage: true }); report.screenshots.push("hourly-desktop.png");
  await page.setViewportSize({ width: 390, height: 844 });
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), false);
  await page.screenshot({ path: join(output, "hourly-mobile.png"), fullPage: true }); report.screenshots.push("hourly-mobile.png");
  await page.setViewportSize({ width: 1280, height: 1000 });
  await page.getByLabel("짧은 시험 시간봉", { exact: true }).selectOption("1d");
  await page.getByLabel("짧은 시험 후보 구분", { exact: true }).selectOption("ALL");
  await page.getByText("필터 밖 선택 1개", { exact: false }).waitFor();
  await page.getByRole("button", { name: "선택 해제 소액 배정금 100% 기능시험 · BTCUSDT · 1h", exact: true }).click();
  assert.deepEqual(await page.evaluate(() => window.__shortLiveFixture.calls), []);
  report.checks.push("hourly bounded shortcut is filter-only; hidden selections remain visible and removable");
  await page.getByRole("checkbox", { name: "시험 대상 화면검사 전용 전략", exact: true }).check();
  await page.evaluate(() => window.__shortLiveFixture.holdNextSnapshot());
  await page.getByRole("button", { name: "실행 상태 새로 확인", exact: true }).click();
  await page.waitForFunction(() => window.__shortLiveFixture.snapshotHeld());
  await page.getByRole("button", { name: "자료·계좌 준비 검사 (주문 없음)", exact: true }).click();
  await page.waitForFunction(() => window.__shortLiveFixture.calls.some(row => row.method === "prepare"));
  assert.equal(await start.isDisabled(), true);
  await page.evaluate(() => window.__shortLiveFixture.releaseSnapshot());
  await page.getByText("서버의 현재 준비 조건 충족", { exact: false }).waitFor();
  assert.equal(await page.locator('.short-live-ack input').count(), 0);
  assert.equal(await page.getByLabel("실제 주문 시험 시작 확인 문구").count(), 0);
  assert.equal(await page.getByRole("button", {name:"모의 실행 해제", exact:true}).count(), 0);
  await page.waitForFunction(() => !document.querySelector('.short-live-real-start')?.disabled);
  report.checks.push("a pending stale GET cannot overwrite prepare or prevent binding its exact fresh authority");
  await page.evaluate(() => window.__shortLiveFixture.expirePreparation());
  await page.getByRole("button", { name: "실행 상태 새로 확인", exact: true }).click();
  await page.getByText("준비 유효시간 만료", { exact: false }).waitFor();
  assert.equal(await start.isDisabled(), true);
  report.checks.push("expired preparation immediately disables consent start");
  const callsBeforeRestore = await page.evaluate(() => window.__shortLiveFixture.calls.length);
  await page.getByRole("button", { name: "선택 모두 해제", exact: true }).click();
  await page.getByText("아래는 마지막 준비 범위의 결과입니다.", { exact: false }).waitFor();
  await page.getByRole("button", { name: "이전 시험 설정 다시 선택", exact: true }).click();
  assert.equal(await page.getByRole("checkbox", { name: "시험 대상 화면검사 전용 전략", exact: true }).isChecked(), true);
  assert.equal(await start.isDisabled(), true);
  assert.equal(await page.evaluate(() => window.__shortLiveFixture.calls.length), callsBeforeRestore);
  report.checks.push("previous exact selections restore without API mutation or expired start authority");
  await page.getByLabel("실거래 시험 시간").selectOption("7");
  assert.equal(await start.isDisabled(), true);
  report.checks.push("duration change invalidates prepared authority before consent");
  await page.getByRole("button", { name: "자료·계좌 준비 검사 (주문 없음)", exact: true }).click();
  await page.getByText("서버의 현재 준비 조건 충족", { exact: false }).waitFor();
  await page.waitForFunction(() => !document.querySelector('.short-live-real-start')?.disabled);
  await page.evaluate(() => window.__shortLiveFixture.setBlocked());
  await page.getByRole("button", { name: "실행 상태 새로 확인", exact: true }).click();
  await page.getByLabel("진행을 막는 사유", { exact: true }).getByText("사용 가능한 잔고 부족", { exact: true }).waitFor();
  assert.equal(await start.isDisabled(), true); report.checks.push("fresh server blocker disables real start");
  await page.evaluate(() => window.__shortLiveFixture.reset()); await page.getByRole("button", { name: "실행 상태 새로 확인", exact: true }).click();
  await page.waitForFunction(() => !document.querySelector('.short-live-real-start')?.disabled);
  assert.equal(await page.evaluate(() => window.__shortLiveFixture.calls.filter(row => row.method === "start").length), 0);
  await page.locator('.short-live-consent').screenshot({path:join(output,"single-consent.png")});
  await start.click();
  await page.getByText("일봉 신호 미관측", { exact: false }).waitFor();
  assert.equal(await page.getByRole("cell", { name: "0 / 0 / 0", exact: true }).count(), 1);
  assert.equal(await page.getByRole("cell").filter({ hasText: /^23,000원/ }).count(), 1);
  report.checks.push("actual public effective budget and attempt/fill-observation counters are rendered without inferred counts");
  assert.equal(await start.isDisabled(), true);
  await page.getByRole("button", { name: "정상 중지 요청", exact: true }).click();
  await page.getByText("실제 종료 확인 전입니다", { exact: false }).waitFor();
  const calls = await page.evaluate(() => window.__shortLiveFixture.calls);
  assert.deepEqual(calls.map((row) => row.method), ["prepare", "prepare", "start", "stop"]);
  assert.equal(calls[1].body.durationHours, 7); assert.equal(calls[2].body.confirmed, true); assert.equal(calls[2].body.confirmationText, "실제 주문 시험 시작");
  assert.equal(calls[2].body.preparationId, "fixture-preparation");
  assert.equal(calls[3].body.sessionId, "fixture-session");
  report.checks.push("only explicit prepare/start/stop calls; daily no-signal is not failure or PASS; STOPPING is not terminal");
  await page.screenshot({ path: join(output, "desktop.png"), fullPage: true }); report.screenshots.push("desktop.png");
  await page.setViewportSize({ width: 390, height: 844 });
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), false);
  await page.screenshot({ path: join(output, "mobile.png"), fullPage: true }); report.screenshots.push("mobile.png");
  await page.reload();
  await page.getByRole("checkbox", { name: "시험 대상 화면검사 전용 전략", exact: true }).check();
  await page.getByRole("button", { name: "자료·계좌 준비 검사 (주문 없음)", exact: true }).click();
  await page.getByText("서버의 현재 준비 조건 충족", { exact: false }).waitFor();
  await page.waitForFunction(() => !document.querySelector('.short-live-real-start')?.disabled);
  await page.evaluate(() => window.__shortLiveFixture.failStart()); await start.click();
  await page.getByText("시작 결과 미확정", { exact: false }).waitFor();
  await page.getByRole("button", { name: "실행 상태 새로 확인", exact: true }).click();
  assert.equal(await page.getByText("시작 결과 미확정", { exact: false }).count(), 1);
  assert.equal(await start.isDisabled(), true);
  assert.equal(await page.evaluate(() => window.__shortLiveFixture.calls.filter((row) => row.method === "start").length), 1);
  await page.evaluate(() => window.__shortLiveFixture.failStop());
  const stop = page.getByRole("button", { name: "정상 중지 요청", exact: true }); await stop.click();
  await page.getByText("중지 결과 미확정", { exact: false }).waitFor();
  assert.equal(await stop.isDisabled(), true);
  report.checks.push("lost start reply is not retried; exact session stop remains possible; lost stop reply disables repeat stop");
  assert.equal(report.errors.length, 0); assert.equal(report.externalRequests.length, 0); assert.equal(forbiddenRequests, 0);
  report.ok = true; await context.close();
} finally {
  await writeFile(join(output, "report.json"), JSON.stringify(report, null, 2));
  await browser.close(); await new Promise((done) => server.close(done));
}
console.log(JSON.stringify(report));
