import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright-core";

const configuredBaseUrl = process.env.LIVE_TRADER_LAYOUT_SMOKE_URL;
const baseUrl = configuredBaseUrl || "http://127.0.0.1:4180";
const appRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const tolerance = 10;
const minimumGap = 7;
const issues = [];
const observations = {};
let browser;
let devServer;
let devServerClosed = false;
let devServerSpawnError;
const devServerOutput = [];

const closeEnough = (left, right) => Math.abs(Number(left) - Number(right)) <= tolerance;
const processEnded = (child) => child.exitCode !== null || child.signalCode !== null;

const delay = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds));

async function canReach(url) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 1_000);
  try {
    const response = await fetch(url, { signal: controller.signal });
    return response.ok;
  } catch {
    return false;
  } finally {
    clearTimeout(timeout);
  }
}

function rememberServerOutput(chunk) {
  devServerOutput.push(String(chunk));
  if (devServerOutput.length > 40) devServerOutput.shift();
}

async function ensureApplication() {
  if (await canReach(baseUrl)) return;
  if (configuredBaseUrl) {
    throw new Error(`지정한 Live Trader URL에 연결할 수 없습니다: ${baseUrl}`);
  }

  const parsedUrl = new URL(baseUrl);
  const viteEntry = path.join(appRoot, "node_modules", "vite", "bin", "vite.js");
  devServer = spawn(process.execPath, [
    viteEntry,
    "--host",
    parsedUrl.hostname,
    "--port",
    parsedUrl.port || "4180",
    "--strictPort",
  ], {
    cwd: appRoot,
    stdio: ["ignore", "pipe", "pipe"],
    windowsHide: true,
  });
  devServer.on("error", (error) => {
    devServerSpawnError = error;
    rememberServerOutput(error.message);
  });
  devServer.once("close", () => {
    devServerClosed = true;
  });
  devServer.stdout.on("data", rememberServerOutput);
  devServer.stderr.on("data", rememberServerOutput);

  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    if (devServerSpawnError) {
      throw new Error(`Live Trader 개발 서버를 시작하지 못했습니다. ${devServerSpawnError.message}`);
    }
    if (processEnded(devServer)) {
      throw new Error(`Live Trader 개발 서버가 조기 종료되었습니다. ${devServerOutput.join("").trim()}`);
    }
    if (await canReach(baseUrl)) return;
    await delay(200);
  }
  throw new Error(`Live Trader 개발 서버가 30초 안에 준비되지 않았습니다. ${devServerOutput.join("").trim()}`);
}

async function waitForApplicationClose(timeoutMilliseconds) {
  if (!devServer || devServerClosed) return true;
  return new Promise((resolve) => {
    let settled = false;
    const finish = (closed) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      devServer.off("close", handleClose);
      resolve(closed);
    };
    const handleClose = () => finish(true);
    const timeout = setTimeout(() => finish(devServerClosed), timeoutMilliseconds);
    devServer.once("close", handleClose);
  });
}

async function forceStopWindowsProcessTree() {
  if (process.platform !== "win32" || !devServer?.pid || processEnded(devServer)) return "";
  return new Promise((resolve) => {
    const output = [];
    const killer = spawn("taskkill", ["/PID", String(devServer.pid), "/T", "/F"], {
      stdio: ["ignore", "pipe", "pipe"],
      windowsHide: true,
    });
    killer.stdout.on("data", (chunk) => output.push(String(chunk)));
    killer.stderr.on("data", (chunk) => output.push(String(chunk)));
    killer.once("error", (error) => resolve(error.message));
    killer.once("close", () => resolve(output.join("").trim()));
  });
}

async function stopApplication() {
  // Only `ensureApplication` assigns devServer, so an externally supplied URL
  // can never reach the process termination paths below.
  if (!devServer) return;
  if (!processEnded(devServer)) devServer.kill();
  let closed = await waitForApplicationClose(3_000);
  let taskkillOutput = "";
  if ((!closed || !processEnded(devServer)) && process.platform === "win32") {
    taskkillOutput = await forceStopWindowsProcessTree();
    closed = await waitForApplicationClose(5_000);
  }
  if (!processEnded(devServer)) {
    devServer.kill("SIGKILL");
    closed = await waitForApplicationClose(1_000);
  }
  if (!closed && processEnded(devServer)) {
    devServer.stdout?.destroy();
    devServer.stderr?.destroy();
    closed = await waitForApplicationClose(1_000);
  }
  if (!closed || !processEnded(devServer)) {
    devServer.unref();
    const detail = taskkillOutput ? ` taskkill: ${taskkillOutput}` : "";
    throw new Error(`Live Trader 개발 서버 프로세스를 종료하지 못했습니다.${detail}`);
  }
}

function sameRect(left, right, properties = ["x", "y", "width", "height"]) {
  return Boolean(left && right && properties.every((property) => closeEnough(left[property], right[property])));
}

async function dragResizeHandle(page, handle, deltaX, deltaY) {
  await handle.scrollIntoViewIfNeeded();
  await page.waitForTimeout(30);
  const handleBox = await handle.boundingBox();
  if (!handleBox) return false;
  const startX = handleBox.x + handleBox.width / 2;
  const startY = handleBox.y + handleBox.height / 2;
  await page.mouse.move(startX, startY);
  await page.mouse.down();
  await page.mouse.move(startX + deltaX, startY + deltaY, { steps: 8 });
  await page.mouse.up();
  await page.waitForTimeout(80);
  return true;
}

async function documentBox(locator) {
  return locator.evaluate((element) => {
    const rect = element.getBoundingClientRect();
    let ancestorScrollLeft = 0;
    let ancestorScrollTop = 0;
    for (let ancestor = element.parentElement; ancestor; ancestor = ancestor.parentElement) {
      ancestorScrollLeft += ancestor.scrollLeft || 0;
      ancestorScrollTop += ancestor.scrollTop || 0;
    }
    return {
      height: rect.height,
      width: rect.width,
      // Compare layout coordinates, not viewport coordinates. A resize handle
      // can scroll the workspace into view without moving the panel itself.
      x: rect.left + window.scrollX + ancestorScrollLeft,
      y: rect.top + window.scrollY + ancestorScrollTop,
    };
  });
}

try {
  await ensureApplication();
  browser = await chromium.launch({ channel: "msedge", headless: true });
  const page = await browser.newPage({ viewport: { width: 1707, height: 960 } });
  await page.goto(baseUrl, { waitUntil: "domcontentloaded" });
  await page.locator(".nav-item").first().waitFor({ state: "visible" });
  await page.evaluate(() => {
    localStorage.removeItem("live-trader.panelSizes.v1");
    localStorage.removeItem("live-trader.panelPositions.v1");
    localStorage.removeItem("live-trader.layoutMode.v1");
  });
  await page.reload({ waitUntil: "domcontentloaded" });
  await page.getByRole("button", { name: /^연결 설정/ }).click();
  await page.getByRole("button", { name: /화면·레이아웃·Telegram/ }).click();
  const layoutEditor = page.locator('[data-layout-control="editor"]');
  await layoutEditor.waitFor({ state: "visible" });
  await layoutEditor.locator("xpath=..").getByRole("button", { name: "초기화", exact: true }).click();
  // The desktop persists layout mode through its API as well as localStorage.
  // Force a locked -> edit transition so entering edit mode captures a fresh
  // baseline even when the operator previously saved edit mode.
  if (await layoutEditor.getAttribute("aria-pressed") === "true") {
    await layoutEditor.click();
    await page.waitForFunction(() => document.documentElement.dataset.layoutMode === "locked");
  }

  // A previously saved short edit height must not become a clipping mask in
  // normal operation. Locked panels keep that value only as a minimum and
  // grow to the complete content height.
  const lockedPanel = page.locator(".appearance-panel");
  const lockedPanelKey = await lockedPanel.getAttribute("data-layout-key");
  const lockedPanelBox = await lockedPanel.boundingBox();
  if (!lockedPanelKey || !lockedPanelBox) {
    issues.push("잠금 화면의 콘텐츠 높이 복구 대상을 찾지 못했습니다.");
  } else {
    await page.evaluate(({ key, width }) => {
      localStorage.setItem("live-trader.panelSizes.v1", JSON.stringify({
        [key]: { width, height: 64 },
      }));
      window.dispatchEvent(new Event("live-trader-layout-restore"));
    }, { key: lockedPanelKey, width: lockedPanelBox.width });
    await page.waitForTimeout(50);
    const lockedVisibility = await lockedPanel.evaluate((panel) => ({
      clientHeight: panel.clientHeight,
      fixedHeight: panel.style.height,
      minimumHeight: panel.style.minHeight,
      scrollHeight: panel.scrollHeight,
    }));
    observations.lockedContentVisibility = lockedVisibility;
    if (lockedVisibility.fixedHeight) {
      issues.push("잠금 화면에서 저장된 높이가 여전히 고정 높이로 적용됩니다.");
    }
    if (lockedVisibility.clientHeight + 1 < lockedVisibility.scrollHeight) {
      issues.push("잠금 화면 패널이 전체 콘텐츠 높이까지 확장되지 않았습니다.");
    }
    await layoutEditor.locator("xpath=..").getByRole("button", { name: "초기화", exact: true }).click();
  }

  const operationalTabs = [
    "실행 준비",
    "배포 검증",
    "주문 기능 검증",
    "위험 관리",
    "실거래 실행",
    "계좌 대조",
    "주문 추적",
    "운영 기록",
    "기술 로그",
    "연결 설정",
  ];
  observations.lockedPanelClipping = {};
  for (const tabLabel of operationalTabs) {
    await page.getByRole("button", { name: tabLabel, exact: true }).click();
    await page.waitForTimeout(40);
    const clippedPanels = await page.locator(".page-view .panel").evaluateAll((panels) => panels.flatMap((panel) => {
      if (panel.scrollHeight <= panel.clientHeight + 1) return [];
      const style = window.getComputedStyle(panel);
      const parent = panel.parentElement;
      const parentStyle = parent ? window.getComputedStyle(parent) : null;
      return [{
        className: panel.className,
        clientHeight: panel.clientHeight,
        computedHeight: style.height,
        computedMinHeight: style.minHeight,
        inlineHeight: panel.style.height,
        inlineMinHeight: panel.style.minHeight,
        overflow: style.overflow,
        parentClassName: parent?.className || "",
        parentHeight: parentStyle?.height || "",
        parentOverflow: parentStyle?.overflow || "",
        scrollHeight: panel.scrollHeight,
        title: panel.querySelector(".panel-header h2")?.textContent?.trim() || "제목 없음",
      }];
    }));
    observations.lockedPanelClipping[tabLabel] = clippedPanels;
    if (clippedPanels.length) {
      issues.push(`${tabLabel} 탭에서 내용이 잘린 패널이 ${clippedPanels.length}개 있습니다.`);
    }
  }
  if (await page.locator('[data-program-journey="true"]').count()) {
    issues.push("제거 요청한 전체 프로그램 여정 배너가 남아 있습니다.");
  }
  await page.getByRole("button", { name: "연결 설정", exact: true }).click();
  await page.getByRole("button", { name: /화면·레이아웃·Telegram/ }).click();
  await layoutEditor.waitFor({ state: "visible" });

  await layoutEditor.click();
  await page.waitForFunction(() => document.documentElement.dataset.layoutMode === "edit");

  // Exercise both resize axes on one real, top-level panel in every Live tab.
  // The selected height delta always shrinks when possible so the operation
  // cannot be rejected merely because a following grid row is occupied.
  const tabResizeResults = [];
  for (const tabLabel of operationalTabs) {
    await page.getByRole("button", { name: tabLabel, exact: true }).click();
    await page.waitForTimeout(80);
    const candidates = await page.locator(".page-view .panel.resizable-panel").evaluateAll((panels) => panels
      .map((panel, index) => {
        const rect = panel.getBoundingClientRect();
        const parentPanel = panel.parentElement?.closest(".panel");
        return {
          height: rect.height,
          index,
          key: panel.getAttribute("data-layout-key") || "",
          topLevel: !parentPanel,
          width: rect.width,
        };
      })
      .filter((panel) => panel.key && panel.width >= 260 && panel.height >= 112)
      .sort((left, right) => Number(right.topLevel) - Number(left.topLevel) || right.height - left.height));
    const selected = candidates[0];
    if (!selected) {
      issues.push(`${tabLabel} 탭에서 양축 크기 조절에 적합한 대표 패널을 찾지 못했습니다.`);
      continue;
    }

    const panel = page.locator(`.page-view .panel[data-layout-key=${JSON.stringify(selected.key)}]`).first();
    const eastResizeHandle = panel.locator(":scope > .panel-resize-east");
    await eastResizeHandle.scrollIntoViewIfNeeded();
    const before = await documentBox(panel);
    const horizontalDragged = await dragResizeHandle(
      page,
      eastResizeHandle,
      -48,
      0,
    );
    const afterHorizontal = await documentBox(panel);
    if (!horizontalDragged || !before || !afterHorizontal || before.width - afterHorizontal.width < 24) {
      issues.push(`${tabLabel} 탭 대표 패널의 가로 크기가 실제 드래그로 조절되지 않았습니다.`);
    }
    if (!sameRect(before, afterHorizontal, ["x", "y", "height"])) {
      issues.push(`${tabLabel} 탭 가로 조절 중 위치 또는 높이가 함께 바뀌었습니다.`);
    }

    const availableVerticalShrink = Math.max(0, (afterHorizontal?.height || 0) - 88);
    const verticalDelta = -Math.min(48, availableVerticalShrink);
    const southResizeHandle = panel.locator(":scope > .panel-resize-south");
    await southResizeHandle.scrollIntoViewIfNeeded();
    const verticalDragged = verticalDelta <= -24 && await dragResizeHandle(
      page,
      southResizeHandle,
      0,
      verticalDelta,
    );
    const afterVertical = await documentBox(panel);
    if (!verticalDragged || !afterHorizontal || !afterVertical || afterHorizontal.height - afterVertical.height < 16) {
      issues.push(`${tabLabel} 탭 대표 패널의 세로 크기가 실제 드래그로 조절되지 않았습니다.`);
    }
    if (!sameRect(afterHorizontal, afterVertical, ["x", "y", "width"])) {
      issues.push(`${tabLabel} 탭 세로 조절 중 위치 또는 너비가 함께 바뀌었습니다.`);
    }

    const storedSize = await page.evaluate((key) => {
      const sizes = JSON.parse(localStorage.getItem("live-trader.panelSizes.v1") || "{}");
      return sizes[key] || null;
    }, selected.key);
    if (!storedSize || !afterVertical
      || !closeEnough(storedSize.width, afterVertical.width)
      || !closeEnough(storedSize.height, afterVertical.height)) {
      issues.push(`${tabLabel} 탭 대표 패널의 양축 크기 저장값이 실제 크기와 일치하지 않습니다.`);
    }
    tabResizeResults.push({
      afterHorizontal,
      afterVertical,
      before,
      key: selected.key,
      storedSize,
      tabLabel,
    });
  }
  observations.tabResize = tabResizeResults;

  // Verify the saved geometry is reapplied to the actual panel after reload.
  await page.reload({ waitUntil: "domcontentloaded" });
  await page.waitForFunction(() => document.documentElement.dataset.layoutMode === "edit");
  const tabResizeRestores = [];
  for (const result of tabResizeResults) {
    await page.getByRole("button", { name: result.tabLabel, exact: true }).click();
    await page.waitForTimeout(60);
    const restored = await page.locator(
      `.page-view .panel[data-layout-key=${JSON.stringify(result.key)}]`,
    ).first().boundingBox();
    tabResizeRestores.push({ key: result.key, restored, tabLabel: result.tabLabel });
    if (!restored || !result.storedSize
      || !closeEnough(restored.width, result.storedSize.width)
      || !closeEnough(restored.height, result.storedSize.height)) {
      issues.push(`${result.tabLabel} 탭 대표 패널의 저장된 양축 크기가 새로고침 후 복원되지 않았습니다.`);
    }
  }
  observations.tabResizeRestores = tabResizeRestores;

  // Once layout editing is locked, saved short heights must become minimums
  // and every panel must reveal all live content instead of clipping it.
  if (await page.evaluate(() => document.documentElement.dataset.layoutMode) === "edit") {
    // The keyboard route remains reachable even when an intentionally short
    // edit height temporarily places a panel control under its resize edge.
    // Escape is the documented edit-mode exit. A first press may close an
    // open disclosure, so repeat once only when edit mode is still active.
    await page.keyboard.press("Escape");
    if (await page.evaluate(() => document.documentElement.dataset.layoutMode) === "edit") {
      await page.keyboard.press("Escape");
    }
    await page.waitForFunction(() => document.documentElement.dataset.layoutMode === "locked");
  }
  const lockedAfterResize = [];
  for (const result of tabResizeResults) {
    await page.getByRole("button", { name: result.tabLabel, exact: true }).click();
    await page.waitForTimeout(60);
    const visibility = await page.locator(
      `.page-view .panel[data-layout-key=${JSON.stringify(result.key)}]`,
    ).first().evaluate((panel) => ({
      clientHeight: panel.clientHeight,
      fixedHeight: panel.style.height,
      minimumHeight: panel.style.minHeight,
      scrollHeight: panel.scrollHeight,
    }));
    lockedAfterResize.push({ ...visibility, key: result.key, tabLabel: result.tabLabel });
    if (visibility.fixedHeight || visibility.clientHeight + 1 < visibility.scrollHeight) {
      issues.push(`${result.tabLabel} 탭 대표 패널이 잠금 후 전체 콘텐츠 높이로 확장되지 않았습니다.`);
    }
  }
  observations.lockedAfterTabResize = lockedAfterResize;

  await page.getByRole("button", { name: "연결 설정", exact: true }).click();
  await page.getByRole("button", { name: /화면·레이아웃·Telegram/ }).click();
  await layoutEditor.locator("xpath=..").getByRole("button", { name: "초기화", exact: true }).click();
  if (await layoutEditor.getAttribute("aria-pressed") !== "true") {
    await layoutEditor.click();
    await page.waitForFunction(() => document.documentElement.dataset.layoutMode === "edit");
  }
  await page.getByRole("button", { name: /^위험 관리/ }).click();
  const riskPolicyDisclosure = page.getByRole("button", { name: /리스크 정책·재시도·선물 계산/ });
  if (await riskPolicyDisclosure.getAttribute("aria-expanded") !== "true") {
    await riskPolicyDisclosure.click();
  }
  await page.locator(".operational-safeguards-panel").waitFor({ state: "visible" });
  await page.waitForTimeout(100);

  const active = page.locator(".operational-safeguards-panel");
  const target = page.locator(".risk-settings-panel");
  await target.waitFor({ state: "visible" });
  const activeKey = await active.getAttribute("data-layout-key");
  const targetKey = await target.getAttribute("data-layout-key");

  const activeBeforeResize = await active.boundingBox();
  const targetBeforeResize = await target.boundingBox();
  const storageBeforeResize = await page.evaluate(() => ({
    positions: JSON.parse(localStorage.getItem("live-trader.panelPositions.v1") || "{}"),
    sizes: JSON.parse(localStorage.getItem("live-trader.panelSizes.v1") || "{}"),
  }));
  const eastHandle = target.locator(":scope > .panel-resize-east");
  const eastBox = await eastHandle.boundingBox();
  if (!activeBeforeResize || !targetBeforeResize || !eastBox) {
    issues.push("독립 크기 조절 전 패널 좌표 또는 리사이즈 핸들을 읽지 못했습니다.");
  } else {
    await page.mouse.move(eastBox.x + eastBox.width / 2, eastBox.y + eastBox.height / 2);
    await page.mouse.down();
    await page.mouse.move(eastBox.x + eastBox.width / 2 - 80, eastBox.y + eastBox.height / 2, { steps: 8 });
    await page.mouse.up();
    await page.waitForTimeout(100);
  }

  const activeAfterResize = await active.boundingBox();
  const targetAfterResize = await target.boundingBox();
  const storageAfterResize = await page.evaluate(() => ({
    positions: JSON.parse(localStorage.getItem("live-trader.panelPositions.v1") || "{}"),
    sizes: JSON.parse(localStorage.getItem("live-trader.panelSizes.v1") || "{}"),
  }));
  observations.resize = {
    activeBefore: activeBeforeResize,
    activeAfter: activeAfterResize,
    targetBefore: targetBeforeResize,
    targetAfter: targetAfterResize,
  };
  if (!targetAfterResize || !targetBeforeResize || targetBeforeResize.width - targetAfterResize.width < 40) {
    issues.push("리스크 패널의 너비가 독립적으로 조절되지 않았습니다.");
  }
  if (!sameRect(targetBeforeResize, targetAfterResize, ["x", "y", "height"])) {
    issues.push("리스크 패널 너비 조절 중 패널의 위치 또는 높이가 함께 바뀌었습니다.");
  }
  if (!sameRect(activeBeforeResize, activeAfterResize)) {
    issues.push("리스크 패널 크기 조절이 운영 차단 패널의 위치 또는 크기를 변경했습니다.");
  }
  if (JSON.stringify(storageBeforeResize.positions[activeKey]) !== JSON.stringify(storageAfterResize.positions[activeKey])) {
    issues.push("리스크 패널 크기 조절 중 운영 차단 패널 위치 저장값이 변경됐습니다.");
  }
  if (JSON.stringify(storageBeforeResize.sizes[activeKey]) !== JSON.stringify(storageAfterResize.sizes[activeKey])) {
    issues.push("리스크 패널 크기 조절 중 운영 차단 패널 크기 저장값이 변경됐습니다.");
  }
  if (!storageAfterResize.sizes[targetKey]) issues.push("조절한 리스크 패널 크기가 저장되지 않았습니다.");

  await active.scrollIntoViewIfNeeded();
  await page.waitForTimeout(100);
  const activeEastHandle = active.locator(":scope > .panel-resize-east");
  const activeEastBox = await activeEastHandle.boundingBox();
  if (!activeEastBox) {
    issues.push("운영 차단 패널의 동쪽 리사이즈 핸들을 찾지 못했습니다.");
  } else {
    await page.mouse.move(activeEastBox.x + activeEastBox.width / 2, activeEastBox.y + activeEastBox.height / 2);
    await page.mouse.down();
    await page.mouse.move(activeEastBox.x + activeEastBox.width / 2 - 160, activeEastBox.y + activeEastBox.height / 2, { steps: 12 });
    await page.mouse.up();
    await page.waitForTimeout(100);
  }
  const activeBeforeMove = await active.boundingBox();
  const targetBeforeMove = await target.boundingBox();
  const storageBeforeMove = await page.evaluate(() => ({
    positions: JSON.parse(localStorage.getItem("live-trader.panelPositions.v1") || "{}"),
    sizes: JSON.parse(localStorage.getItem("live-trader.panelSizes.v1") || "{}"),
  }));
  const headerBox = await active.locator(":scope > .panel-header").boundingBox();
  if (!activeBeforeMove || !targetBeforeMove || !headerBox) {
    issues.push("독립 이동 전 패널 좌표를 읽지 못했습니다.");
  } else {
    await page.mouse.move(headerBox.x + headerBox.width / 2, headerBox.y + headerBox.height / 2);
    await page.mouse.down();
    await page.mouse.move(headerBox.x + headerBox.width / 2 + 80, headerBox.y + headerBox.height / 2, { steps: 12 });
    await page.mouse.up();
    await page.waitForTimeout(150);
  }

  const activeAfterMove = await active.boundingBox();
  const targetAfterMove = await target.boundingBox();
  const storageAfterMove = await page.evaluate(() => ({
    positions: JSON.parse(localStorage.getItem("live-trader.panelPositions.v1") || "{}"),
    sizes: JSON.parse(localStorage.getItem("live-trader.panelSizes.v1") || "{}"),
  }));
  observations.move = {
    activeBefore: activeBeforeMove,
    activeAfter: activeAfterMove,
    targetBefore: targetBeforeMove,
    targetAfter: targetAfterMove,
  };
  if (!activeAfterMove || !activeBeforeMove || sameRect(activeBeforeMove, activeAfterMove, ["x", "y"])) {
    issues.push("운영 차단 패널이 독립적으로 이동하지 않았습니다.");
  }
  if (!sameRect(activeBeforeMove, activeAfterMove, ["width", "height"])) {
    issues.push("운영 차단 패널을 이동할 때 자체 크기가 바뀌었습니다.");
  }
  if (!sameRect(targetBeforeMove, targetAfterMove)) {
    issues.push("운영 차단 패널 이동이 리스크 패널의 위치 또는 크기를 변경했습니다.");
  }
  if (JSON.stringify(storageBeforeMove.positions[targetKey]) !== JSON.stringify(storageAfterMove.positions[targetKey])) {
    issues.push("운영 차단 패널 이동 중 리스크 패널 위치 저장값이 변경됐습니다.");
  }
  if (JSON.stringify(storageBeforeMove.sizes[targetKey]) !== JSON.stringify(storageAfterMove.sizes[targetKey])) {
    issues.push("운영 차단 패널 이동 중 리스크 패널 크기 저장값이 변경됐습니다.");
  }
  if (!storageAfterMove.positions[activeKey]) issues.push("이동한 운영 차단 패널 위치가 저장되지 않았습니다.");

  const collisionPairs = await page.evaluate(({ activeLayoutKey, gap }) => {
    const activePanel = [...document.querySelectorAll(".page-view .panel")]
      .find((panel) => panel.getAttribute("data-layout-key") === activeLayoutKey);
    if (!activePanel) return [["active-panel-missing"]];
    const activeRect = activePanel.getBoundingClientRect();
    const pairs = [];
    for (const peer of document.querySelectorAll(".page-view .panel")) {
      if (peer === activePanel || activePanel.contains(peer) || peer.contains(activePanel)) continue;
      const peerRect = peer.getBoundingClientRect();
      if (!peerRect.width || !peerRect.height) continue;
      const tooClose = activeRect.left < peerRect.right + gap
        && activeRect.right + gap > peerRect.left
        && activeRect.top < peerRect.bottom + gap
        && activeRect.bottom + gap > peerRect.top;
      if (tooClose) {
        pairs.push([
          activePanel.getAttribute("data-layout-key") || activePanel.className,
          peer.getAttribute("data-layout-key") || peer.className,
        ]);
      }
    }
    return pairs;
  }, { activeLayoutKey: activeKey, gap: minimumGap });
  observations.collisionPairs = collisionPairs;
  if (collisionPairs.length) issues.push(`이동 후 패널 간 최소 간격 위반이 ${collisionPairs.length}건 있습니다.`);

  const beforeReload = await page.evaluate(() => ({
    positions: localStorage.getItem("live-trader.panelPositions.v1"),
    sizes: localStorage.getItem("live-trader.panelSizes.v1"),
  }));
  await page.reload({ waitUntil: "domcontentloaded" });
  await page.getByRole("button", { name: /^위험 관리/ }).click();
  await page.locator(".operational-safeguards-panel").waitFor({ state: "visible" });
  await page.waitForTimeout(150);
  const afterReload = await page.evaluate(() => ({
    positions: localStorage.getItem("live-trader.panelPositions.v1"),
    sizes: localStorage.getItem("live-trader.panelSizes.v1"),
  }));
  observations.persistence = { beforeReload, afterReload };
  if (beforeReload.positions !== afterReload.positions || beforeReload.sizes !== afterReload.sizes) {
    issues.push("새로고침 후 독립 위치 또는 크기 저장값이 유지되지 않았습니다.");
  }

  await page.getByRole("button", { name: /^연결 설정/ }).click();
  await page.getByRole("button", { name: /화면·레이아웃·Telegram/ }).click();
  await page.getByRole("button", { name: "초기화", exact: true }).click();
  await page.getByRole("button", { name: /^위험 관리/ }).click();
  await page.locator(".operational-safeguards-panel").waitFor({ state: "visible" });
  await page.waitForTimeout(100);
  const resetState = await page.evaluate(() => ({
    positions: localStorage.getItem("live-trader.panelPositions.v1"),
    sizes: localStorage.getItem("live-trader.panelSizes.v1"),
    transformed: [...document.querySelectorAll(".page-view .panel")]
      .some((panel) => panel.style.transform || panel.style.width || panel.style.height),
  }));
  observations.reset = resetState;
  if (resetState.positions || resetState.sizes || resetState.transformed) {
    issues.push("레이아웃 초기화가 저장 위치·크기 또는 인라인 배치를 제거하지 못했습니다.");
  }

  await page.close();
} catch (error) {
  issues.push(error instanceof Error ? error.message : String(error));
  observations.fatalError = error instanceof Error
    ? { message: error.message, name: error.name }
    : { message: String(error), name: "Error" };
} finally {
  try {
    if (browser) await browser.close();
  } catch (error) {
    issues.push(`Playwright 브라우저 종료 실패: ${error instanceof Error ? error.message : String(error)}`);
  }
  try {
    await stopApplication();
  } catch (error) {
    issues.push(`Live Trader 개발 서버 종료 실패: ${error instanceof Error ? error.message : String(error)}`);
  }
}

const expectedTabCount = 10;
const tabResize = observations.tabResize || [];
const tabResizeRestores = observations.tabResizeRestores || [];
const lockedAfterTabResize = observations.lockedAfterTabResize || [];
const lockedPanelClipping = observations.lockedPanelClipping || {};
const horizontalAxisIsolationPassed = tabResize.length === expectedTabCount
  && tabResize.every(({ afterHorizontal, before }) => (
    before && afterHorizontal
    && before.width - afterHorizontal.width >= 24
    && sameRect(before, afterHorizontal, ["x", "y", "height"])
  ));
const verticalAxisIsolationPassed = tabResize.length === expectedTabCount
  && tabResize.every(({ afterHorizontal, afterVertical }) => (
    afterHorizontal && afterVertical
    && afterHorizontal.height - afterVertical.height >= 16
    && sameRect(afterHorizontal, afterVertical, ["x", "y", "width"])
  ));
const persistencePassed = tabResize.length === expectedTabCount
  && tabResizeRestores.length === expectedTabCount
  && tabResize.every(({ key, storedSize, tabLabel }) => {
    const restored = tabResizeRestores.find((candidate) => (
      candidate.key === key && candidate.tabLabel === tabLabel
    ))?.restored;
    return restored && storedSize
      && closeEnough(restored.width, storedSize.width)
      && closeEnough(restored.height, storedSize.height);
  })
  && observations.persistence?.beforeReload?.positions === observations.persistence?.afterReload?.positions
  && observations.persistence?.beforeReload?.sizes === observations.persistence?.afterReload?.sizes;
const initialLockedVisibility = observations.lockedContentVisibility;
const lockedContentVisibilityPassed = Boolean(
  initialLockedVisibility
  && !initialLockedVisibility.fixedHeight
  && initialLockedVisibility.clientHeight + 1 >= initialLockedVisibility.scrollHeight
  && Object.keys(lockedPanelClipping).length === expectedTabCount
  && Object.values(lockedPanelClipping).every((clippedPanels) => clippedPanels.length === 0)
  && lockedAfterTabResize.length === expectedTabCount
  && lockedAfterTabResize.every((visibility) => (
    !visibility.fixedHeight && visibility.clientHeight + 1 >= visibility.scrollHeight
  )),
);
const resetPassed = Boolean(
  observations.reset
  && !observations.reset.positions
  && !observations.reset.sizes
  && !observations.reset.transformed,
);

const checkResults = {
  horizontalAxisIsolation: {
    status: horizontalAxisIsolationPassed ? "pass" : "fail",
    details: { passedTabs: tabResize.filter(({ afterHorizontal, before }) => (
      before && afterHorizontal
      && before.width - afterHorizontal.width >= 24
      && sameRect(before, afterHorizontal, ["x", "y", "height"])
    )).length, totalTabs: expectedTabCount },
  },
  verticalAxisIsolation: {
    status: verticalAxisIsolationPassed ? "pass" : "fail",
    details: { passedTabs: tabResize.filter(({ afterHorizontal, afterVertical }) => (
      afterHorizontal && afterVertical
      && afterHorizontal.height - afterVertical.height >= 16
      && sameRect(afterHorizontal, afterVertical, ["x", "y", "width"])
    )).length, totalTabs: expectedTabCount },
  },
  persistence: {
    status: persistencePassed ? "pass" : "fail",
    details: { restoredTabs: tabResizeRestores.length, totalTabs: expectedTabCount },
  },
  lockedContentVisibility: {
    status: lockedContentVisibilityPassed ? "pass" : "fail",
    details: {
      checkedTabs: Object.keys(lockedPanelClipping).length,
      clippedPanels: Object.values(lockedPanelClipping)
        .reduce((total, clippedPanels) => total + clippedPanels.length, 0),
      totalTabs: expectedTabCount,
    },
  },
  reset: {
    status: resetPassed ? "pass" : "fail",
    details: observations.reset || null,
  },
};
for (const [checkName, check] of Object.entries(checkResults)) {
  if (check.status === "fail") issues.push(`layout contract check failed: ${checkName}`);
}

const report = {
  schemaVersion: "trading-system.layout-regression.v1",
  appId: "live_trader",
  status: issues.length === 0 ? "pass" : "fail",
  generatedAt: new Date().toISOString(),
  checks: checkResults,
  issues: [...new Set(issues)],
  tabs: tabResize.map(({ afterHorizontal, afterVertical, before, key, tabLabel }) => ({
    id: key,
    label: tabLabel,
    horizontalDelta: before && afterHorizontal ? afterHorizontal.width - before.width : null,
    horizontalOrthogonalDelta: before && afterHorizontal ? {
      height: afterHorizontal.height - before.height,
      x: afterHorizontal.x - before.x,
      y: afterHorizontal.y - before.y,
    } : null,
    verticalDelta: afterHorizontal && afterVertical ? afterVertical.height - afterHorizontal.height : null,
    verticalOrthogonalDelta: afterHorizontal && afterVertical ? {
      width: afterVertical.width - afterHorizontal.width,
      x: afterVertical.x - afterHorizontal.x,
      y: afterVertical.y - afterHorizontal.y,
    } : null,
  })),
};
console.log(`LAYOUT_CONTRACT_RESULT=${JSON.stringify(report)}`);
if (report.status === "fail") process.exitCode = 1;
