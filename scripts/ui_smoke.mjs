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
  { label: "실행 준비", requiredHeadings: ["실거래 Doctor", "실거래 승인 패키지"] },
  { label: "배포 검증", requiredHeadings: ["선택한 배포 전략"], forbiddenHeadings: ["승급 준비 큐", "데이터·전략 계보", "포트폴리오 Artifact"] },
  { label: "주문 기능 검증", requiredHeadings: ["KIS 기간형 기능시험", "시험 대상과 기간", "기간과 활성화 상태", "현재 실제 적용 한도", "현재 차단 항목"] },
  { label: "위험 관리", requiredHeadings: ["현재 리스크 사용량", "운영 차단 설정"] },
  { label: "실거래 실행", requiredHeadings: ["브로커별 자동화", "Runtime 구성 요소", "Live Watchdog"] },
  { label: "계좌 대조", requiredHeadings: ["계좌·포지션 3자 대조", "내 계좌·보유 포지션"] },
  { label: "주문 추적", requiredHeadings: ["주문 상태 원장", "실행 품질"] },
  { label: "운영 기록", requiredHeadings: ["감사 이벤트"], forbiddenHeadings: ["운영 사고"] },
  { label: "기술 로그", requiredHeadings: ["기술 로그"] },
  {
    label: "연결 설정",
    requiredHeadings: ["브로커 실계좌 연결", "설정·Runtime 자체 검사"],
    forbiddenHeadings: ["Secret 보호 상태", "브로커 Capability", "어댑터 인터페이스 계약", "브로커 준비 항목"],
  },
];
const expectedNavigationLabels = tabs.map((tab) => tab.label);
const issues = [];
const views = [];

async function nonUniformCardBorders(locator) {
  return locator.evaluateAll((nodes) => nodes.flatMap((node) => {
    const style = window.getComputedStyle(node);
    const widths = [style.borderTopWidth, style.borderRightWidth, style.borderBottomWidth, style.borderLeftWidth];
    const colors = [style.borderTopColor, style.borderRightColor, style.borderBottomColor, style.borderLeftColor];
    return new Set(widths).size === 1 && new Set(colors).size === 1
      ? []
      : [{ className: node.className, widths, colors }];
  }));
}

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
      issues.push(`${viewport.name}: navigation must contain exactly the 10 operational menus (${normalizedNavigationLabels.join(", ")})`);
    }

    const environmentBar = page.locator('[aria-label="LIVE 환경 및 안전 상태"]');
    if (await environmentBar.count() !== 1 || !await environmentBar.isVisible()) {
      issues.push(`${viewport.name}: persistent LIVE environment bar is missing`);
    } else {
      for (const requiredLabel of ["LIVE · 실계좌", "현재 Deployment", "실거래 잠금", "Preflight", "신규 진입", "위험 증가 주문", "Broker 전송", "전역 Kill"]) {
        if (!await environmentBar.getByText(requiredLabel, { exact: true }).count()) {
          issues.push(`${viewport.name}: LIVE environment bar is missing '${requiredLabel}'`);
        }
      }
      const deploymentOptions = await environmentBar.locator("select option").evaluateAll((options) => options.map((option) => ({
        label: option.textContent?.trim() || "",
        value: option.value,
      })));
      const deploymentValues = deploymentOptions.map((option) => option.value).filter(Boolean);
      const deploymentLabels = deploymentOptions.map((option) => option.label).filter(Boolean);
      if (new Set(deploymentValues).size !== deploymentValues.length || new Set(deploymentLabels).size !== deploymentLabels.length) {
        issues.push(`${viewport.name}: current Deployment selector contains duplicate values or labels`);
      }
      const hiddenTerminalVisible = deploymentOptions.some((option) => (
        !option.label.startsWith("[현재 세션]")
        && / · (retired|paused|archived) · #/i.test(option.label)
      ));
      if (hiddenTerminalVisible) {
        issues.push(`${viewport.name}: retired/paused/archived Deployment is visible in the default selector`);
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

      const currentPageTitle = (await page.locator(".topbar-title-block h1").textContent())?.trim();
      if (currentPageTitle !== tab.label) {
        issues.push(`${viewport.name}/${tab.label}: topbar title is '${currentPageTitle || "missing"}'`);
      }
      for (const heading of tab.requiredHeadings) {
        if (!await page.getByRole("heading", { name: heading, exact: true }).count()) {
          issues.push(`${viewport.name}/${tab.label}: required section '${heading}' is missing`);
        }
      }
      for (const heading of tab.forbiddenHeadings ?? []) {
        if (await page.getByRole("heading", { name: heading, exact: true }).count()) {
          issues.push(`${viewport.name}/${tab.label}: removed section '${heading}' is visible`);
        }
      }

      if (tab.label === "배포 검증") {
        const deploymentSelect = environmentBar.locator("select");
        const observedDeployments = [];
        for (let index = 0; index < 6; index += 1) {
          observedDeployments.push(await deploymentSelect.inputValue());
          await page.waitForTimeout(80);
        }
        if (new Set(observedDeployments).size > 1) {
          issues.push(`${viewport.name}/${tab.label}: current Deployment changed without operator input (${observedDeployments.join(" -> ")})`);
        }
        const borderMismatches = await nonUniformCardBorders(page.locator(
          ".portfolio-artifact-item[data-tone], .promotion-readiness-row[data-tone]",
        ));
        if (borderMismatches.length) {
          issues.push(`${viewport.name}/${tab.label}: status cards still use a colored side border (${JSON.stringify(borderMismatches.slice(0, 3))})`);
        }
      }

      if (tab.label === "실행 준비" && await page.getByText("포지션·계좌 대조 요약", { exact: true }).count()) {
        issues.push(`${viewport.name}/${tab.label}: legacy reconciliation summary is still visible`);
      }
      if (tab.label === "계좌 대조") {
        if (!await page.getByRole("button", { name: /자본 배분·포지션 노출/ }).count()) {
          issues.push(`${viewport.name}/${tab.label}: account allocation disclosure is missing`);
        }
        if (!await page.getByText("10초 자동 갱신·대조", { exact: true }).count()) {
          issues.push(`${viewport.name}/${tab.label}: automatic refresh/reconciliation label is missing`);
        }
        if (!await page.getByRole("button", { name: "현재 계좌를 기준 원장으로 승인", exact: true }).count()) {
          issues.push(`${viewport.name}/${tab.label}: explicit program-ledger baseline action is missing`);
        }
      }
      if (tab.label === "주문 추적") {
        if (!await page.getByText("Client Order ID", { exact: true }).count()) {
          issues.push(`${viewport.name}/${tab.label}: idempotent order identifier column is missing`);
        }
        if (!await page.getByRole("button", { name: "CSV", exact: true }).count()) {
          issues.push(`${viewport.name}/${tab.label}: filtered order CSV export is missing`);
        }
      }
      if (tab.label === "실거래 실행") {
        await page.getByRole("tab", { name: "주식/ETF", exact: true }).waitFor({ state: "visible", timeout: 5000 });
        if (!await page.locator(".runtime-deployment-binding").count()) {
          issues.push(`${viewport.name}/${tab.label}: Deployment/runtime binding status is missing`);
        }
        if (!await page.getByText(/Run을 누르기 전에는 runtime 설정을 변경하지 않습니다/).count()) {
          issues.push(`${viewport.name}/${tab.label}: mode selection safety guidance is missing`);
        }
        if (!await page.getByRole("button", { name: /MONITOR Run/, exact: true }).count()) {
          issues.push(`${viewport.name}/${tab.label}: Deployment-bound MONITOR Run action is missing`);
        }
      }
      if (tab.label === "주문 기능 검증") {
        if (!await page.getByText(/promotionEligible=false/).count()) {
          issues.push(`${viewport.name}/${tab.label}: non-promotion boundary is missing`);
        }
        for (const safetyLabel of ["계정 범위", "세션", "승인 계약", "API·전송", "전역 Kill"]) {
          if (!await page.locator('.functional-test-safety-strip').getByText(safetyLabel, { exact: true }).count()) {
            issues.push(`${viewport.name}/${tab.label}: functional safety strip is missing '${safetyLabel}'`);
          }
        }
        if (!await page.getByRole("button", { name: "허가서 준비", exact: true }).count()) {
          issues.push(`${viewport.name}/${tab.label}: permit readiness action is missing`);
        }
        if (!await page.getByRole("button", { name: "오늘 활성화", exact: true }).count()) {
          issues.push(`${viewport.name}/${tab.label}: daily activation action is missing`);
        }
      }
      if (tab.label === "운영 기록" && !await page.getByText(/append-only 원장/).count()) {
        issues.push(`${viewport.name}/${tab.label}: immutable audit guidance is missing`);
      }
      if (tab.label === "위험 관리") {
        const policyDisclosure = page.getByRole("button", { name: /리스크 정책·재시도·선물 계산/ });
        if (!await policyDisclosure.count()) {
          issues.push(`${viewport.name}/${tab.label}: risk policy disclosure is missing`);
        } else {
          await policyDisclosure.click();
          if (!await page.getByText(/주문 POST 재전송을 분리/).count()) {
            issues.push(`${viewport.name}/${tab.label}: retry safety contract is missing`);
          }
        }
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
