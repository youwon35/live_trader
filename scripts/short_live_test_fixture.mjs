// Offline UI interaction fixture. No Python server, account, broker API or real orders.
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { createServer } from "node:http";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { resolve, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const dependencyRoot = process.env.SHORT_LIVE_NODE_MODULES || join(root, "node_modules");
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
let state={schemaVersion:'fixture-only',fixture:true,status:'IDLE',observedAt:'2026-10-22T06:00:00Z',catalog:{candidates:[row]},blockers:[],prepared:null,runtime:null};
const calls=[];let failStart=false;let failStop=false;
window.__shortLiveFixture={calls, state:()=>state, setBlocked:()=>{state={...state,blockers:['insufficient-balance']};}, reset:()=>{state={...state,blockers:[]};}, failStart:()=>{failStart=true;},failStop:()=>{failStop=true;}};
const api={snapshot:async()=>structuredClone(state),prepare:async(body)=>{calls.push({method:'prepare',body});state={...state,status:'PREPARED',prepared:{preparationId:'fixture-preparation',scopeHash:hash,readyToStart:true,targets:[row],blockers:[],durationHours:body.durationHours,capitalKrwPerBroker:body.capitalKrwPerBroker,dailyLossStopKrwPerBroker:body.dailyLossStopKrwPerBroker}};return {snapshot:structuredClone(state)};},start:async(body)=>{calls.push({method:'start',body});state={...state,status:'RUNNING',runtime:{sessionId:'fixture-session',status:'RUNNING',ownerAlive:true,newEntriesBlocked:false,naturalExitObserved:false,unknownOrderCount:0,terminalVerified:false,startedAt:'2026-10-22T06:05:00Z',endsAt:'2026-10-22T12:05:00Z',brokers:[{broker:'KIS',budgetKrw:100000,exposureKrw:0,dailyRealizedLossKrw:0,lossLimitKrw:50000,status:'FIXTURE'}],strategies:[{selectionKey:row.selectionKey,label:row.label,timeframe:'1d',signalCount:0,orderCount:0,fillCount:0}]}};if(failStart)throw new Error('FIXTURE_LOST_REPLY');return {snapshot:structuredClone(state)};},stop:async(body)=>{calls.push({method:'stop',body});state={...state,status:'STOPPING',runtime:{...state.runtime,status:'STOPPING',newEntriesBlocked:true}};if(failStop)throw new Error('FIXTURE_LOST_STOP_REPLY');return {snapshot:structuredClone(state)};}};
createRoot(document.getElementById('root')).render(<ShortLiveTestPanel api={api} pollMs={1000}/>);
`;
await build({ stdin: { contents: entry, resolveDir: root, loader: "jsx" }, bundle: true, format: "esm",
  nodePaths: [dependencyRoot], outfile: join(output, "fixture.js"), loader: { ".css": "css" },
  define: { "process.env.NODE_ENV": '"test"' }, logLevel: "silent" });
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
  const start = page.getByRole("button", { name: "선택한 범위로 실제 주문 시험 시작", exact: true });
  assert.equal(await start.isDisabled(), true);
  assert.deepEqual(await page.evaluate(() => window.__shortLiveFixture.calls), []);
  report.checks.push("mount is GET-only with no implicit prepare/start");
  await page.getByRole("checkbox", { name: "시험 대상 화면검사 전용 전략", exact: true }).check();
  await page.getByRole("button", { name: "무주문 준비 검사", exact: true }).click();
  await page.getByText("서버의 현재 준비 조건 충족", { exact: false }).waitFor();
  assert.equal(await start.isDisabled(), true);
  const acknowledgement = page.locator('.short-live-ack input');
  await acknowledgement.check();
  await page.getByLabel("실제 주문 시험 시작 확인 문구").fill("실제 주문 시험 시작");
  await page.waitForFunction(() => !document.querySelector('.short-live-real-start')?.disabled);
  await page.getByLabel("실거래 시험 시간").selectOption("7");
  assert.equal(await start.isDisabled(), true); assert.equal(await acknowledgement.isChecked(), false);
  report.checks.push("duration change invalidates prepared authority and manual acknowledgement");
  await page.getByRole("button", { name: "무주문 준비 검사", exact: true }).click();
  await page.getByText("서버의 현재 준비 조건 충족", { exact: false }).waitFor();
  await acknowledgement.check(); await page.getByLabel("실제 주문 시험 시작 확인 문구").fill("실제 주문 시험 시작");
  await page.waitForFunction(() => !document.querySelector('.short-live-real-start')?.disabled);
  await page.evaluate(() => window.__shortLiveFixture.setBlocked());
  await page.getByRole("button", { name: "상태 새로 확인", exact: true }).click();
  await page.getByText("사용 가능한 잔고 부족", { exact: true }).waitFor();
  assert.equal(await start.isDisabled(), true); report.checks.push("fresh server blocker disables real start");
  await page.evaluate(() => window.__shortLiveFixture.reset()); await page.getByRole("button", { name: "상태 새로 확인", exact: true }).click();
  await page.waitForFunction(() => !document.querySelector('.short-live-real-start')?.disabled);
  await start.click();
  await page.getByText("일봉 신호 미관측", { exact: false }).waitFor();
  assert.equal(await start.isDisabled(), true);
  await page.getByRole("button", { name: "정상 중지 요청", exact: true }).click();
  await page.getByText("실제 정상 종료 확인 전입니다", { exact: false }).waitFor();
  const calls = await page.evaluate(() => window.__shortLiveFixture.calls);
  assert.deepEqual(calls.map((row) => row.method), ["prepare", "prepare", "start", "stop"]);
  assert.equal(calls[1].body.durationHours, 7); assert.equal(calls[2].body.confirmed, true);
  assert.equal(calls[2].body.preparationId, "fixture-preparation");
  assert.equal(calls[3].body.sessionId, "fixture-session");
  report.checks.push("only explicit prepare/start/stop calls; daily no-signal is not failure or PASS; STOPPING is not terminal");
  await page.screenshot({ path: join(output, "desktop.png"), fullPage: true }); report.screenshots.push("desktop.png");
  await page.setViewportSize({ width: 390, height: 844 });
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), false);
  await page.screenshot({ path: join(output, "mobile.png"), fullPage: true }); report.screenshots.push("mobile.png");
  await page.reload();
  await page.getByRole("checkbox", { name: "시험 대상 화면검사 전용 전략", exact: true }).check();
  await page.getByRole("button", { name: "무주문 준비 검사", exact: true }).click();
  await page.getByText("서버의 현재 준비 조건 충족", { exact: false }).waitFor();
  await acknowledgement.check(); await page.getByLabel("실제 주문 시험 시작 확인 문구").fill("실제 주문 시험 시작");
  await page.waitForFunction(() => !document.querySelector('.short-live-real-start')?.disabled);
  await page.evaluate(() => window.__shortLiveFixture.failStart()); await start.click();
  await page.getByText("시작 결과 미확정", { exact: false }).waitFor();
  await page.getByRole("button", { name: "상태 새로 확인", exact: true }).click();
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
