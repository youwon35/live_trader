import { chromium } from "playwright-core";

const baseUrl = process.env.LIVE_TRADER_LAYOUT_SMOKE_URL || "http://127.0.0.1:4180";
const tolerance = 10;
const minimumGap = 7;
const issues = [];
const observations = {};
const browser = await chromium.launch({ channel: "msedge", headless: true });

const closeEnough = (left, right) => Math.abs(Number(left) - Number(right)) <= tolerance;

function sameRect(left, right, properties = ["x", "y", "width", "height"]) {
  return Boolean(left && right && properties.every((property) => closeEnough(left[property], right[property])));
}

try {
  const page = await browser.newPage({ viewport: { width: 1707, height: 960 } });
  await page.goto(baseUrl, { waitUntil: "domcontentloaded" });
  await page.locator(".nav-item").first().waitFor({ state: "visible" });
  await page.evaluate(() => {
    localStorage.removeItem("live-trader.panelSizes.v1");
    localStorage.removeItem("live-trader.panelPositions.v1");
    localStorage.removeItem("live-trader.layoutMode.v1");
  });
  await page.reload({ waitUntil: "domcontentloaded" });
  await page.getByRole("button", { name: /^설정·진단/ }).click();
  await page.getByRole("button", { name: "편집 모드", exact: true }).click();
  await page.waitForFunction(() => document.documentElement.dataset.layoutMode === "edit");
  await page.getByRole("button", { name: /^리스크·안전/ }).click();
  await page.locator(".operational-safeguards-panel").waitFor({ state: "visible" });
  await page.waitForTimeout(100);

  const active = page.locator(".operational-safeguards-panel");
  const target = page.locator(".panel").filter({
    has: page.getByRole("heading", { name: "리스크 한도 설정", exact: true }),
  });
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
  await page.getByRole("button", { name: /^리스크·안전/ }).click();
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

  await page.getByRole("button", { name: /^설정·진단/ }).click();
  await page.getByRole("button", { name: "초기화", exact: true }).click();
  await page.getByRole("button", { name: /^리스크·안전/ }).click();
  await page.locator(".operational-safeguards-panel").waitFor({ state: "visible" });
  await page.waitForTimeout(100);
  const resetState = await page.evaluate(() => ({
    positions: localStorage.getItem("live-trader.panelPositions.v1"),
    sizes: localStorage.getItem("live-trader.panelSizes.v1"),
    transformed: [...document.querySelectorAll(".page-view .panel")]
      .some((panel) => panel.style.transform || panel.style.width || panel.style.height),
  }));
  if (resetState.positions || resetState.sizes || resetState.transformed) {
    issues.push("레이아웃 초기화가 저장 위치·크기 또는 인라인 배치를 제거하지 못했습니다.");
  }

  await page.close();
} finally {
  await browser.close();
}

const report = {
  baseUrl,
  checkedAt: new Date().toISOString(),
  observations,
  issues,
  ok: issues.length === 0,
};
console.log(JSON.stringify(report, null, 2));
if (issues.length) process.exitCode = 1;
