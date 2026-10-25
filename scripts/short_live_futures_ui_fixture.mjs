// Local-only synthetic UI: no Python application, credential, or broker API.
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { createServer } from "node:http";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join, resolve, basename } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const dependencyRoot = process.env.SHORT_LIVE_NODE_MODULES || join(root, "node_modules");
const designRoot = process.env.SHORT_LIVE_DESIGN_ROOT || resolve(root, "../../packages/design");
const require = createRequire(join(dependencyRoot, "fixture-resolver.cjs"));
const { build } = require("esbuild");
const { chromium } = require("playwright-core");
const output = join(root, "output", "playwright", "futures-protection");
await mkdir(output, { recursive: true });
const entry = `
import * as React from 'react';
import {createRoot} from 'react-dom/client';
import ShortLiveTestPanel from './src/ShortLiveTestPanel.jsx';
import './src/styles.css';
const hash='a'.repeat(64);
const row={selectionKey:'STRATEGY_INSTANCE:fixture:future',kind:'STRATEGY_INSTANCE',id:'fixture-future',label:'선물 화면검사 전략',route:'BINANCE_USDM_FUTURES',routes:['BINANCE_USDM_FUTURES'],timeframe:'1h',timeframes:['1h'],artifactFileSha256:hash,instanceFileSha256:hash,memberCount:1,pluginIds:['moving_average_cross'],symbols:['SOLUSDT'],eligibleForFunctionalPreparation:true,blockers:[]};
let state={schemaVersion:'fixture-only',fixture:true,status:'IDLE',catalog:[row],blockers:[],prepared:null,runtime:null};
const calls=[];
window.__futuresFixture={calls,observe:()=>{state={...state,runtime:{sessionId:'fixture',status:'BLOCKED',newEntriesBlocked:true,ownerAlive:true,brokerRows:[{route:'BINANCE_USDM_FUTURES',effectiveRouteBudgetKrw:1000,brokerBudgetKrw:100000,committedCapitalKrw:1000,notionalExposureKrw:5000,protections:[{symbol:'SOLUSDT',positionSide:'LONG',status:'TRIGGERED',triggerPrice:98,verified:true,observedAt:'2026-10-25T12:00:00Z'}],futuresPositions:[{symbol:'SOLUSDT',positionSide:'LONG',markPrice:100,actualLiquidationPrice:80.5,leverage:5}],futuresIncidents:[{type:'LIQUIDATION',symbol:'SOLUSDT',positionSide:'LONG',quantity:0.5,verified:true,observedAt:'2026-10-25T12:00:01Z'}]}]}};}};
const api={snapshot:async()=>structuredClone(state),catalog:async()=>({catalog:state.catalog}),prepare:async(body)=>{calls.push({method:'prepare',body});state={...state,status:'PREPARED',prepared:{preparationId:'fixture-prep',scopeHash:hash,readyToStart:true,targets:[row],blockers:[],durationHours:body.durationHours,capitalKrwPerBroker:body.capitalKrwPerBroker,dailyLossStopKrwPerBroker:body.dailyLossStopKrwPerBroker,futuresPolicy:body.futuresPolicy,expiresAt:new Date(Date.now()+180000).toISOString()}};return structuredClone(state);},start:async()=>{throw new Error('START_MUST_NOT_BE_CALLED');},stop:async()=>{throw new Error('STOP_MUST_NOT_BE_CALLED');}};
createRoot(document.getElementById('root')).render(<ShortLiveTestPanel api={api} pollMs={1000}/>);
`;
await build({ stdin: { contents: entry, resolveDir: root, loader: "jsx" }, bundle: true,
  nodePaths: [dependencyRoot], format: "esm", outfile: join(output, "fixture.js"),
  loader: { ".css": "css" }, define: { "process.env.NODE_ENV": '"test"' }, logLevel: "silent",
  plugins: [{ name: "shared-design-location", setup(build) {
    build.onResolve({ filter: /packages\/design\/(design-tokens|interaction-standards)\.css$/ },
      (args) => ({ path: join(designRoot, basename(args.path)) }));
  } }] });
await writeFile(join(output, "index.html"), '<!doctype html><html lang="ko"><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><link rel="stylesheet" href="/fixture.css"><div id="root"></div><script type="module" src="/fixture.js"></script></html>');
let forbiddenRequests = 0;
const server = createServer(async (req, res) => {
  const pathname = new URL(req.url, "http://127.0.0.1").pathname;
  if (req.method !== "GET" || !["/", "/fixture.js", "/fixture.css"].includes(pathname)) {
    forbiddenRequests++; res.writeHead(403); res.end(); return;
  }
  try {
    const name = pathname === "/" ? "index.html" : pathname.slice(1);
    const bytes = await readFile(join(output, name));
    res.writeHead(200, { "Content-Type": name.endsWith(".js") ? "application/javascript" : name.endsWith(".css") ? "text/css" : "text/html" });
    res.end(bytes);
  } catch { res.writeHead(404); res.end(); }
});
await new Promise((done) => server.listen(0, "127.0.0.1", done));
const origin = `http://127.0.0.1:${server.address().port}`;
const browser = await chromium.launch({ channel: "msedge", headless: true });
const report = { fixtureOnly: true, realAccountQueries: 0, realOrders: 0, checks: [], externalRequests: [], errors: [] };
try {
  const context = await browser.newContext({ viewport: { width: 1280, height: 1000 }, serviceWorkers: "block" });
  await context.route("**/*", async (route) => {
    if (new URL(route.request().url()).origin !== origin) { report.externalRequests.push(route.request().url()); await route.abort(); }
    else await route.continue();
  });
  const page = await context.newPage();
  page.on("pageerror", (error) => report.errors.push(error.message));
  await page.goto(origin);
  await page.getByRole("checkbox", { name: "시험 대상 선물 화면검사 전략", exact: true }).check();
  const prepare = page.getByRole("button", { name: "무주문 준비 검사", exact: true });
  const start = page.getByRole("button", { name: "동의하고 실거래 시작", exact: true });
  const leverage = page.getByLabel("선물 기대 레버리지", { exact: true });
  const stop = page.getByLabel("선물 보호 손절 가격 거리 퍼센트", { exact: true });
  assert.equal(await leverage.inputValue(), "1"); assert.equal(await stop.inputValue(), "");
  assert.equal(await prepare.isDisabled(), true); assert.equal(await start.isDisabled(), true);
  assert.deepEqual(await page.evaluate(() => window.__futuresFixture.calls), []);
  await leverage.fill("5"); await stop.fill("20"); assert.equal(await prepare.isDisabled(), true);
  await stop.fill("2"); assert.equal(await prepare.isDisabled(), false);
  await prepare.click(); await page.getByText("확정 선물 범위: 5배", { exact: false }).waitFor();
  const calls = await page.evaluate(() => window.__futuresFixture.calls);
  assert.equal(calls.length, 1); assert.equal(calls[0].method, "prepare");
  assert.deepEqual(calls[0].body.futuresPolicy, { leverage: 5, marginMode: "ISOLATED", stopLossPercent: 2 });
  assert.equal(calls[0].body.capitalKrwPerBroker, 100000);
  report.checks.push("explicit valid 5x/2% policy is required; actual capital is unchanged and no settings mutation occurs");
  await page.waitForFunction(() => !document.querySelector('.short-live-real-start').disabled);
  await stop.fill("1.5");
  assert.equal(await start.isDisabled(), true);
  assert.equal((await page.evaluate(() => window.__futuresFixture.calls)).length, 1);
  report.checks.push("changing protection policy invalidates preparation without auto-preparing or starting");
  await page.setViewportSize({ width: 390, height: 844 });
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), false);
  const selectedText = await page.locator('.short-live-selection li > span').boundingBox();
  assert.ok(selectedText.width >= 120 && selectedText.height < 100, `selected target remains readable beside the remove button on mobile: ${JSON.stringify(selectedText)}`);
  await page.screenshot({ path: join(output, "policy-mobile.png"), fullPage: true });
  await page.setViewportSize({ width: 1280, height: 1000 });
  await page.evaluate(() => window.__futuresFixture.observe());
  await page.getByRole("button", { name: "상태 새로 확인", exact: true }).click();
  await page.getByText("거래소 강제청산 · SOLUSDT", { exact: true }).waitFor();
  await page.getByText("포지션 투입 증거금 / 명목 노출", { exact: true }).waitFor();
  await page.getByText("거래소 청산가 80.5 USDT", { exact: false }).waitFor();
  await page.getByRole("cell", { name: /보호 발동/ }).waitFor();
  assert.equal(await start.isDisabled(), true);
  assert.equal((await page.evaluate(() => window.__futuresFixture.calls)).length, 1);
  await page.screenshot({ path: join(output, "observation-desktop.png"), fullPage: true });
  report.checks.push("actual margin, notional, protection, exchange liquidation price and liquidation incident render separately");
  assert.deepEqual(report.errors, []); assert.deepEqual(report.externalRequests, []); assert.equal(forbiddenRequests, 0);
  report.ok = true; await context.close();
} finally {
  await writeFile(join(output, "report.json"), JSON.stringify(report, null, 2));
  await browser.close(); await new Promise((done) => server.close(done));
}
console.log(JSON.stringify(report));
