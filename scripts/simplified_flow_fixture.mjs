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
const output = resolve(root, 'output/playwright/simplified-flow');
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
const server = createServer(async (request, response) => {
  try {
    const pathname = decodeURIComponent(new URL(request.url, 'http://127.0.0.1').pathname);
    if (pathname.startsWith('/api/') || request.method !== 'GET') {
      response.writeHead(403).end('No backend in fixture server');
      return;
    }
    const target = resolve(dist, `.${pathname === '/' ? '/index.html' : pathname}`);
    if (!target.startsWith(`${dist}${sep}`)) {
      response.writeHead(403).end();
      return;
    }
    const content = await readFile(target);
    response.writeHead(200, { 'Content-Type': { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.svg': 'image/svg+xml' }[extname(target)] || 'application/octet-stream' });
    response.end(content);
  } catch {
    response.writeHead(404).end();
  }
});
await new Promise((resolveListen) => server.listen(0, '127.0.0.1', resolveListen));
const origin = `http://127.0.0.1:${server.address().port}`;
await mkdir(output, { recursive: true });
const browser = await chromium.launch({ channel: 'msedge', headless: true });
const report = { ok: false, brokerIO: 0, externalRequests: [], views: [], apiRequests: [],styles: [] };
try {
  const configurations=fontReview?[1024,1920].flatMap(width=>['light','dark'].map(theme=>({viewport:{width,height:1000},theme}))):[{viewport:{width:1280,height:800},theme:'light'},{viewport:{width:1920,height:1080},theme:'light'}];
  for (const {viewport,theme} of configurations) {
    const context = await browser.newContext({ viewport, serviceWorkers: 'block' });
    const page = await context.newPage();
    const errors = [];
    page.on('pageerror', (error) => errors.push(error.message));
    await context.route('**/*', async (route) => {
      const request = route.request();
      const url = new URL(request.url());
      if (url.origin !== origin) {
        report.externalRequests.push(url.origin);
        await route.abort();
        return;
      }
      if (!url.pathname.startsWith('/api/')) {
        await route.continue();
        return;
      }
      report.apiRequests.push({ path: url.pathname, method: request.method(), viewport: viewport.width });
      let body = { ok: true };
      if (url.pathname === '/api/snapshot') body = snapshot;
      else if (url.pathname === '/api/functional-test') body = EMPTY_FUNCTIONAL_TEST_WORKSPACE;
      else if (url.pathname === '/api/search-presets') body = { schemaVersion: 1, presets: [] };
      else if (url.pathname === '/api/artifact-metadata') body = { items: {} };
      else if (url.pathname === '/api/env-settings') body = { settings: { fields: [], groups: [] } };
      else if (url.pathname === '/api/checklist' && request.method() === 'POST') {
        const input = request.postDataJSON();
        const item = snapshot.checklist.find((row) => row.key === input.name);
        assert.ok(item && ['manual', 'pending'].includes(item.source), 'only an explicit operator acknowledgement can change');
        item.checked = input.value;
        item.source = 'manual';
        body = { ok: true, snapshot };
      }
      else if (url.pathname.includes('reconcil') || url.pathname.includes('execution-events')) body = { ok: true, snapshot };
      else if (url.pathname.includes('validation')) body = { ok: true, validation: { ok: true, candidates: [] } };
      await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(body) });
    });
    await page.goto(origin);
    await page.getByRole('heading', { name: '시작 점검', exact: true }).waitFor();
    const navLabels = ['시작 점검', '운용 전략', '실거래 운용', '계좌·잔고', '주문·체결', '실행 기록', '연결·설정', '주문 연결 시험'];
    assert.deepEqual(await page.locator('.nav-list .nav-item').allTextContents().then((items) => items.map((text) => text.trim())), navLabels);
    for (const label of navLabels) {
      await page.locator('.nav-list').getByRole('button', { name: label, exact: true }).click();
      await page.getByRole('heading', { name: label, exact: true }).waitFor();
      await page.evaluate(() => new Promise((done) => requestAnimationFrame(() => requestAnimationFrame(done))));
      assert.equal(await page.locator('.nav-list [aria-current="page"]').textContent().then((value) => value.trim()), label);
      assert.equal(await page.locator('.live-page-purpose').count(), 0);
      if (await page.locator('.live-environment-bar').count()) assert.equal(await page.locator('.live-environment-bar').evaluate(element => getComputedStyle(element).backgroundImage), 'none');
      const overflow = await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1);
      assert.equal(overflow, false, `${label} horizontal viewport overflow at ${viewport.width}`);
      if (label === '운용 전략') {
        await page.getByRole('heading', { name: '모의거래에서 받은 검증 근거', exact: true }).waitFor();
        await page.getByRole('button', { name: /전체 검증 단계/ }).click();
        assert.deepEqual(await page.locator('.live-lifecycle-timeline strong').allTextContents(), ['백테스트', '모의 검증', '제한 실거래', '실전 운용']);
        assert.equal(await page.locator('.live-lifecycle-timeline article.current strong').textContent(), '백테스트');
        const cards = page.locator('.live-strategy-summary-grid');
        assert.equal(await cards.getByText('저장본 검증 단계', { exact: true }).count(), 1);
        assert.equal(await cards.getByText('백테스트 완료', { exact: true }).count(), 1);
        assert.equal(await cards.getByText('배포 운용 상태', { exact: true }).count(), 1);
        assert.equal(await cards.getByText('제한 실거래 대기', { exact: true }).count(), 1);
        assert.equal(await page.getByRole('button', { name: '실전 운용 단계로 승인', exact: true }).isDisabled(), true);
        await page.screenshot({ path: resolve(output, `strategy-${viewport.width}.png`) });
      }
      if (label === '실거래 운용') {
        assert.equal(report.apiRequests.filter(({ path, viewport: width }) => path.includes('validation-small-live') && width === viewport.width).length, 0, 'advanced diagnostics must not load passively');
        const before = report.apiRequests.length;
        await page.getByRole('button', { name: '제한 실거래', exact: true }).click();
        assert.equal(await page.getByRole('button', { name: '제한 실거래 시작', exact: true }).isDisabled(), true);
        assert.equal(await page.getByRole('button', { name: '1회 진단', exact: true }).count(), 0);
        assert.equal(await page.getByText('API 요청 미리보기', { exact: true }).count(), 0);
        assert.ok(report.apiRequests.slice(before).every(({ method }) => method === 'GET'), 'mode selection alone must not mutate runtime');
        await page.getByRole('tab', { name: '한도·안전장치', exact: true }).click();
        await page.getByRole('heading', { name: '현재 리스크 사용량', exact: true }).waitFor();
        const checklist = page.getByRole('region', { name: '운영 체크리스트', exact: true });
        assert.equal(await checklist.getByRole('checkbox').count(), 5);
        assert.equal(await checklist.getByRole('checkbox').first().isDisabled(), true);
        const manualCheck = checklist.getByRole('checkbox').nth(1);
        const originalCheck = await manualCheck.isChecked();
        // This controlled checkbox changes only after the API acknowledges it.
        await manualCheck.click();
        await page.getByText(/위험 한도 검토 .*저장했습니다/).waitFor();
        assert.equal(await manualCheck.isChecked(), !originalCheck);
        assert.equal(await page.getByRole('tab', { name: '한도·안전장치', exact: true }).getAttribute('aria-selected'), 'true');
        await page.locator('.live-section-tabs').evaluate(async (element) => Promise.all(element.getAnimations({ subtree: true }).map((animation) => animation.finished.catch(() => undefined))));
        report.views.push({ viewport: viewport.width, label: '한도·안전장치', tabs: await page.locator('.live-section-tabs [role="tab"]').evaluateAll((nodes) => nodes.map((node) => ({ text: node.textContent, selected: node.getAttribute('aria-selected'), className: node.className, background: getComputedStyle(node).backgroundColor }))) });
        assert.equal(await page.locator('.nav-list [aria-current="page"]').textContent().then((value) => value.trim()), '실거래 운용');
        await page.screenshot({ path: resolve(output, `safety-${viewport.width}.png`) });
        await page.reload();
        await page.getByRole('tab', { name: '한도·안전장치', exact: true }).waitFor();
        assert.equal(await page.getByRole('tab', { name: '한도·안전장치', exact: true }).getAttribute('aria-selected'), 'true');
      }
      if (label === '계좌·잔고') {
        const allocation = page.getByRole('region', { name: '내 실계좌 구성', exact: true });
        await allocation.getByLabel('계좌 범위', { exact: true }).selectOption('kis');
        assert.equal(await allocation.locator('tbody tr').count(), 2);
        assert.equal(await allocation.locator('.ts-allocation-center-value').textContent(), '400,000원');
        assert.match(await allocation.locator('tbody tr').first().innerText(), /50.00%/);
        await allocation.getByLabel('정렬', { exact: true }).selectOption('value-asc');
        assert.match(await allocation.locator('tbody tr').first().innerText(), /035420/);
        await page.screenshot({ path: resolve(output, `allocation-${viewport.width}.png`) });
        await page.evaluate(() => { document.documentElement.dataset.uiTheme = 'light'; });
        assert.equal(await allocation.evaluate(element => getComputedStyle(element).backgroundImage), 'none');
        await page.screenshot({ path: resolve(output, `allocation-light-${viewport.width}.png`) });
        await page.evaluate(() => { document.documentElement.dataset.uiTheme = 'dark'; });

        const originalAccounts = snapshot.accounts;
        const originalPositions = snapshot.positions;
        snapshot.accounts = snapshot.accounts.map(account => account.broker_id === 'kis' ? { ...account, broker_cash_value: 0, broker_equity_value: 0 } : account);
        snapshot.positions = snapshot.positions.map(position => position.broker_id === 'kis' ? { ...position, current_price: 0, broker_value: 0 } : position);
        await allocation.getByRole('button', { name: '계좌 다시 읽기', exact: true }).click();
        await allocation.locator('.ts-allocation-center-value').filter({ hasText: '평가 미확인' }).waitFor();
        assert.equal(await allocation.getByText('보유 자산이 없습니다.', { exact: true }).count(), 0);
        assert.equal(await allocation.locator('tbody tr').count(), 2);
        assert.match(await allocation.locator('tbody tr').first().innerText(), /평가 미확인/);
        await page.screenshot({ path: resolve(output, `allocation-missing-price-${viewport.width}.png`) });
        snapshot.accounts = originalAccounts;
        snapshot.positions = originalPositions;
        await allocation.getByRole('button', { name: '계좌 다시 읽기', exact: true }).click();
        await allocation.locator('.ts-allocation-center-value').filter({ hasText: '400,000원' }).waitFor();
        report.views.push({ viewport: viewport.width, label: '시세·총액 누락 계좌', missingValueHidden: true });
        assert.equal(await page.locator('.three-way-reconciliation-panel').getByText('조회됨', { exact: true }).count(), 0);
        await page.locator('.three-way-reconciliation-panel').getByText('대조 미확인', { exact: true }).waitFor();
      }
      if (label === '실행 기록') {
        await page.getByRole('tab', { name: '상세 로그', exact: true }).click();
        await page.getByRole('heading', { name: '기술 로그', exact: true }).waitFor();
        assert.equal(await page.locator('.nav-list [aria-current="page"]').textContent().then((value) => value.trim()), '실행 기록');
      }
      if (label === '주문 연결 시험') {
        await page.getByText('비승급 · promotionEligible=false', { exact: true }).waitFor();
        await page.locator('.functional-test-safety-strip').waitFor();
        await page.screenshot({ path: resolve(output, `broker-test-${viewport.width}.png`) });
      }
      if(fontReview){
        await page.evaluate(theme=>{document.documentElement.dataset.uiTheme=theme;},theme);
        report.styles.push(await probeRestoredUi(page,label));
        if(label==='실행 기록') {
          assert.ok(new Set(report.styles.at(-1).logs.map(log=>log.background)).size>=3,'info/warning/error retain distinct semantic colors');
          await page.screenshot({path:resolve(output,`restored-logs-${theme}-${viewport.width}.png`)});
        }
        if(label==='연결·설정') {
          await page.getByRole('button',{name:/화면·레이아웃·Telegram/}).click();
          report.styles.push(await probeRestoredUi(page,`${label} / 화면 설정`));
          await page.screenshot({path:resolve(output,`restored-settings-${theme}-${viewport.width}.png`)});
        }
        await inspectRestoredTabs(page,label,report.styles,theme);
      }
      report.views.push({ viewport: viewport.width, label, overflow });
    }
    assert.deepEqual(errors, [], 'no runtime errors');
    await context.close();
  }
  assert.deepEqual(report.externalRequests, [], 'all requests stay in the fixture origin');
  assert.deepEqual(report.styles.flatMap(s=>s.fontFailures),[]);
  report.ok = true;
} finally {
  await browser.close();
  await new Promise((done) => server.close(done));
  await writeFile(resolve(output, 'report.json'), JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
}
