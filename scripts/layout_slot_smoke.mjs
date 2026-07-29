import { chromium } from "playwright-core";

const baseUrl = process.env.LIVE_TRADER_LAYOUT_SMOKE_URL || "http://127.0.0.1:4180";
const tolerance = 10;
const issues = [];
const observations = {};
const browser = await chromium.launch({ channel: "msedge", headless: true });

const closeEnough = (left, right) => Math.abs(Number(left) - Number(right)) <= tolerance;
const overlaps = (left, right) => (
  left.left < right.right
  && left.right > right.left
  && left.top < right.bottom
  && left.bottom > right.top
);

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
  await page.getByRole("button", { name: /^실거래 준비/ }).click();
  await page.locator(".operational-safeguards-panel").waitFor({ state: "visible" });
  await page.locator(".layout-mode-button").click();
  await page.waitForFunction(() => document.documentElement.dataset.layoutMode === "edit");
  await page.waitForTimeout(100);

  const active = page.locator(".operational-safeguards-panel");
  const target = page.locator(".panel").filter({
    has: page.getByRole("heading", { name: "리스크 한도 설정", exact: true }),
  });

  const eastHandle = target.locator(":scope > .panel-resize-east");
  const eastBox = await eastHandle.boundingBox();
  if (!eastBox) {
    issues.push("리스크 패널의 동쪽 리사이즈 핸들을 찾지 못했습니다.");
  } else {
    await page.mouse.move(eastBox.x + eastBox.width / 2, eastBox.y + eastBox.height / 2);
    await page.mouse.down();
    await page.mouse.move(eastBox.x + eastBox.width / 2 + 96, eastBox.y + eastBox.height / 2, { steps: 8 });
    await page.mouse.up();
  }

  const activeBefore = await active.boundingBox();
  const targetBefore = await target.boundingBox();
  observations.before = { active: activeBefore, target: targetBefore };
  const headerBox = await active.locator(":scope > .panel-header").boundingBox();
  if (!activeBefore || !targetBefore || !headerBox) {
    issues.push("슬롯 교환 전 패널 좌표를 읽지 못했습니다.");
  } else {
    if (closeEnough(activeBefore.width, targetBefore.width)) {
      issues.push("서로 다른 크기 슬롯을 준비하지 못했습니다.");
    }
    await page.mouse.move(headerBox.x + headerBox.width / 2, headerBox.y + headerBox.height / 2);
    await page.mouse.down();
    await page.mouse.move(targetBefore.x + targetBefore.width / 2, targetBefore.y + 28, { steps: 16 });
    await page.mouse.up();
    await page.waitForTimeout(150);

    const activeAfter = await active.boundingBox();
    const targetAfter = await target.boundingBox();
    observations.after = { active: activeAfter, target: targetAfter };
    if (!activeAfter || !targetAfter) {
      issues.push("슬롯 교환 후 패널 좌표를 읽지 못했습니다.");
    } else {
      const activeRect = {
        left: activeAfter.x,
        right: activeAfter.x + activeAfter.width,
        top: activeAfter.y,
        bottom: activeAfter.y + activeAfter.height,
      };
      const targetRect = {
        left: targetAfter.x,
        right: targetAfter.x + targetAfter.width,
        top: targetAfter.y,
        bottom: targetAfter.y + targetAfter.height,
      };
      if (!closeEnough(activeAfter.x, targetBefore.x) || !closeEnough(activeAfter.y, targetBefore.y)) {
        issues.push("운영 차단 설정이 오른쪽 위 리스크 슬롯으로 이동하지 않았습니다.");
      }
      if (!closeEnough(targetAfter.x, activeBefore.x) || !closeEnough(targetAfter.y, activeBefore.y)) {
        issues.push("리스크 패널이 운영 차단 설정의 이전 슬롯으로 이동하지 않았습니다.");
      }
      if (!closeEnough(activeAfter.width, targetBefore.width) || !closeEnough(activeAfter.height, targetBefore.height)) {
        issues.push("운영 차단 설정이 대상 슬롯 크기를 이어받지 않았습니다.");
      }
      if (!closeEnough(targetAfter.width, activeBefore.width) || !closeEnough(targetAfter.height, activeBefore.height)) {
        issues.push("리스크 패널이 원래 운영 차단 슬롯 크기를 이어받지 않았습니다.");
      }
      if (overlaps(activeRect, targetRect)) issues.push("교환한 두 패널이 서로 겹칩니다.");

      const overlapCount = await page.evaluate(() => {
        const panels = [...document.querySelectorAll(".page-view .panel")]
          .filter((panel) => {
            const rect = panel.getBoundingClientRect();
            return rect.width > 0 && rect.height > 0;
          });
        const moved = panels.filter((panel) =>
          panel.classList.contains("operational-safeguards-panel")
          || panel.querySelector(":scope > .panel-header h2")?.textContent === "리스크 한도 설정");
        return moved.reduce((count, panel) => {
          const rect = panel.getBoundingClientRect();
          return count + panels.filter((peer) => {
            if (peer === panel || panel.contains(peer) || peer.contains(panel)) return false;
            const peerRect = peer.getBoundingClientRect();
            return rect.left < peerRect.right
              && rect.right > peerRect.left
              && rect.top < peerRect.bottom
              && rect.bottom > peerRect.top;
          }).length;
        }, 0);
      });
      if (overlapCount) issues.push(`교환 후 다른 패널과 ${overlapCount}건 겹칩니다.`);

      const stored = await page.evaluate(() => ({
        positions: JSON.parse(localStorage.getItem("live-trader.panelPositions.v1") || "{}"),
        sizes: JSON.parse(localStorage.getItem("live-trader.panelSizes.v1") || "{}"),
      }));
      const activeKey = await active.getAttribute("data-layout-key");
      const targetKey = await target.getAttribute("data-layout-key");
      if (!stored.positions[activeKey] || !stored.positions[targetKey]) issues.push("두 패널 위치가 모두 저장되지 않았습니다.");
      if (!stored.sizes[activeKey] || !stored.sizes[targetKey]) issues.push("두 패널 슬롯 크기가 모두 저장되지 않았습니다.");

      await page.reload({ waitUntil: "domcontentloaded" });
      await page.getByRole("button", { name: /^실거래 준비/ }).click();
      await page.locator(".operational-safeguards-panel").waitFor({ state: "visible" });
      await page.waitForTimeout(150);
      const activeReloaded = await page.locator(".operational-safeguards-panel").boundingBox();
      const targetReloaded = await page.locator(".panel").filter({
        has: page.getByRole("heading", { name: "리스크 한도 설정", exact: true }),
      }).boundingBox();
      observations.reloaded = { active: activeReloaded, target: targetReloaded };
      if (
        !activeReloaded
        || !targetReloaded
        || !closeEnough(activeReloaded.x, activeAfter.x)
        || !closeEnough(activeReloaded.y, activeAfter.y)
        || !closeEnough(activeReloaded.width, activeAfter.width)
        || !closeEnough(activeReloaded.height, activeAfter.height)
        || !closeEnough(targetReloaded.x, targetAfter.x)
        || !closeEnough(targetReloaded.y, targetAfter.y)
        || !closeEnough(targetReloaded.width, targetAfter.width)
        || !closeEnough(targetReloaded.height, targetAfter.height)
      ) {
        issues.push("새로고침 후 교환한 위치 또는 크기가 유지되지 않았습니다.");
      }

      await page.locator(".layout-reset-button").click();
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
    }
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
