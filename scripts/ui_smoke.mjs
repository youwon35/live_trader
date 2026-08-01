import { chromium } from "playwright-core";

const baseUrl = process.env.LIVE_TRADER_SMOKE_URL || "http://127.0.0.1:18795";
const chromePath = process.env.CHROME_PATH || "";
const viewports = [
  { name: "right-monitor-100", width: 1707, height: 960, desktopScale: 1 },
  { name: "right-monitor-125", width: 1366, height: 768, desktopScale: 1.25 },
  { name: "right-monitor-150", width: 1138, height: 640, desktopScale: 1.5 },
  { name: "compact-desktop", width: 1280, height: 800, desktopScale: 1 },
];
const tabs = [
  { label: "운영 현황", requiredHeadings: ["현재 Deployment", "Preflight 범위·유효성"] },
  { label: "배포·승급", requiredHeadings: ["승급 준비 큐", "데이터·전략 계보"] },
  { label: "계좌·포지션", requiredHeadings: ["계좌·포지션 3자 대조", "내 계좌·보유 포지션"] },
  { label: "주문·체결", requiredHeadings: ["주문 상태 원장", "주문 타임라인", "체결 원장", "실행 품질"] },
  { label: "리스크·안전", requiredHeadings: ["현재 리스크 사용량", "요청별 재시도 원칙"] },
  { label: "실거래 운영", requiredHeadings: ["Runtime 구성 요소", "Live Watchdog"] },
  { label: "사고·감사", requiredHeadings: ["운영 사고", "감사 이벤트"] },
  { label: "기술 로그", requiredHeadings: ["기술 로그"] },
  { label: "설정·진단", requiredHeadings: ["Secret 보호 상태", "설정·Runtime 자체 검사"] },
];
const expectedNavigationLabels = tabs.map((tab) => tab.label);
const issues = [];
const views = [];
const browser = await chromium.launch(chromePath
  ? { headless: true, executablePath: chromePath }
  : { channel: "msedge", headless: true });

try {
  for (const viewport of viewports) {
    const page = await browser.newPage({ viewport: { width: viewport.width, height: viewport.height } });
    const consoleErrors = [];
    page.on("console", (message) => {
      if (message.type() === "error") consoleErrors.push(message.text());
    });
    page.on("pageerror", (error) => consoleErrors.push(error.message));

    const response = await page.goto(baseUrl, { waitUntil: "domcontentloaded" });
    await page.locator(".nav-item").first().waitFor({ state: "visible" });
    if (!response?.ok()) issues.push(`${viewport.name}: app response ${response?.status() || "missing"}`);
    if (await page.locator(".api-connection-banner").count()) {
      issues.push(`${viewport.name}: Python API connection banner is visible`);
    }
    if (await page.locator(".nav-item-badge").count()) {
      issues.push(`${viewport.name}: sidebar numeric badges are still visible`);
    }

    const navigationLabels = await page.locator(".nav-item").allTextContents();
    const normalizedNavigationLabels = navigationLabels.map((label) => label.trim());
    if (JSON.stringify(normalizedNavigationLabels) !== JSON.stringify(expectedNavigationLabels)) {
      issues.push(`${viewport.name}: navigation must contain exactly the 9 operational menus (${normalizedNavigationLabels.join(", ")})`);
    }

    const environmentBar = page.locator('[aria-label="LIVE 환경 및 안전 상태"]');
    if (await environmentBar.count() !== 1 || !await environmentBar.isVisible()) {
      issues.push(`${viewport.name}: persistent LIVE environment bar is missing`);
    } else {
      for (const requiredLabel of ["LIVE · 실계좌", "현재 Deployment", "실거래 잠금", "신규 진입", "위험 증가 주문", "Broker 전송", "전역 Kill"]) {
        if (!await environmentBar.getByText(requiredLabel, { exact: true }).count()) {
          issues.push(`${viewport.name}: LIVE environment bar is missing '${requiredLabel}'`);
        }
      }
    }

    for (const tab of tabs) {
      await page.getByRole("button", { name: tab.label, exact: true }).first().click();
      await page.waitForTimeout(150);
      const layout = await page.evaluate(() => {
        const root = document.documentElement;
        const body = document.body;
        const workspace = document.querySelector("main.workspace");
        const pageView = document.querySelector(".page-view");
        const viewportWidth = window.innerWidth;
        const escapedControlBoxes = [...document.querySelectorAll("button, input, select, textarea")]
          .filter((element) => {
            const box = element.getBoundingClientRect();
            const style = window.getComputedStyle(element);
            return style.display !== "none"
              && style.visibility !== "hidden"
              && box.width > 0
              && box.height > 0
              && box.bottom > 0
              && box.top < window.innerHeight
              && (box.left < -1 || box.right > viewportWidth + 1);
          })
          .map((element) => {
            const box = element.getBoundingClientRect();
            const ancestors = [];
            let parent = element.parentElement;
            while (parent && ancestors.length < 5) {
              const parentBox = parent.getBoundingClientRect();
              ancestors.push({
                className: parent.className || parent.tagName,
                left: Math.round(parentBox.left),
                right: Math.round(parentBox.right),
                width: Math.round(parentBox.width),
              });
              parent = parent.parentElement;
            }
            return {
              label: element.getAttribute("aria-label") || element.textContent?.trim() || element.tagName,
              left: Math.round(box.left),
              right: Math.round(box.right),
              width: Math.round(box.width),
              viewportWidth,
              ancestors,
            };
          });
        return {
          documentOverflow: root.scrollWidth > root.clientWidth + 1 || body.scrollWidth > body.clientWidth + 1,
          workspaceOverflow: Boolean(workspace && workspace.scrollWidth > workspace.clientWidth + 1),
          pageTextLength: pageView?.textContent?.trim().length || 0,
          escapedControls: escapedControlBoxes.map((item) => item.label),
          escapedControlBoxes,
        };
      });
      if (layout.documentOverflow) issues.push(`${viewport.name}/${tab.label}: document horizontal overflow`);
      if (layout.workspaceOverflow) issues.push(`${viewport.name}/${tab.label}: workspace horizontal overflow`);
      if (!layout.pageTextLength) issues.push(`${viewport.name}/${tab.label}: empty page view`);
      if (layout.escapedControls.length) {
        issues.push(`${viewport.name}/${tab.label}: controls outside viewport (${layout.escapedControls.join(", ")})`);
      }

      const currentPageTitle = (await page.locator(".topbar-title-block strong").textContent())?.trim();
      if (currentPageTitle !== tab.label) {
        issues.push(`${viewport.name}/${tab.label}: topbar title is '${currentPageTitle || "missing"}'`);
      }
      for (const heading of tab.requiredHeadings) {
        if (!await page.getByRole("heading", { name: heading, exact: true }).count()) {
          issues.push(`${viewport.name}/${tab.label}: required section '${heading}' is missing`);
        }
      }

      if (tab.label === "운영 현황" && await page.getByText("포지션·계좌 대조 요약", { exact: true }).count()) {
        issues.push(`${viewport.name}/${tab.label}: legacy reconciliation summary is still visible`);
      }
      if (tab.label === "계좌·포지션") {
        if (!await page.getByRole("heading", { name: "계좌 자본·포지션 노출", exact: true }).count()) {
          issues.push(`${viewport.name}/${tab.label}: account allocation visualization is missing`);
        }
        if (!await page.getByText("10초 자동 갱신·대조", { exact: true }).count()) {
          issues.push(`${viewport.name}/${tab.label}: automatic refresh/reconciliation label is missing`);
        }
        if (!await page.getByRole("button", { name: "현재 계좌를 기준 원장으로 승인", exact: true }).count()) {
          issues.push(`${viewport.name}/${tab.label}: explicit program-ledger baseline action is missing`);
        }
      }
      if (tab.label === "주문·체결" && !await page.getByText("Client Order ID", { exact: true }).count()) {
        issues.push(`${viewport.name}/${tab.label}: idempotent order identifier column is missing`);
      }
      if (tab.label === "사고·감사" && !await page.getByText(/append-only 원장/).count()) {
        issues.push(`${viewport.name}/${tab.label}: immutable audit guidance is missing`);
      }
      if (tab.label === "리스크·안전" && !await page.getByText(/주문 POST 재전송을 분리/).count()) {
        issues.push(`${viewport.name}/${tab.label}: retry safety contract is missing`);
      }
      views.push({ viewport: viewport.name, desktopScale: viewport.desktopScale, tab: tab.label, ...layout });
    }

    if (consoleErrors.length) issues.push(`${viewport.name}: console errors (${consoleErrors.join(" | ")})`);
    await page.close();
  }
} finally {
  await browser.close();
}

const report = { ok: issues.length === 0, baseUrl, checkedAt: new Date().toISOString(), views, issues };
console.log(JSON.stringify(report, null, 2));
if (issues.length) process.exitCode = 1;
