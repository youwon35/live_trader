import { chromium } from "playwright-core";

const tolerance = 12;
const specs = [
  {
    active: ".artifact-repository-panel",
    handle: ":scope > .section-heading",
    modeKey: "paper_trader.layoutEditMode.v1",
    name: "paper_trader",
    panelSelector: ".panel",
    resetSelector: ".layout-reset-button",
    sizeKeys: ["paper_trader.layoutSizes.v2"],
    target: ".strategy-risk-settings-panel",
    url: process.env.PAPER_TRADER_LAYOUT_SMOKE_URL || "http://127.0.0.1:4181",
  },
  {
    active: ".command-panel",
    handle: ":scope > .panel-header",
    modeKey: "stock_data_scraper.layoutEditMode.v1",
    name: "stock_data_scraper",
    panelSelector: ".panel",
    resetSelector: ".layout-reset-button",
    sizeKeys: ["stock_data_scraper.layoutSizes.v1"],
    target: ".queue-panel",
    url: process.env.STOCK_DATA_SCRAPER_LAYOUT_SMOKE_URL || "http://127.0.0.1:4182",
  },
  {
    active: ".strategy-composer-panel",
    handle: ":scope > .panel-header",
    modeKey: "backtester.layoutMode.v1",
    name: "backtester",
    panelSelector: ".resizable-panel",
    resetSelector: ".layout-reset-button",
    sizeKeys: ["backtester.panelSizes.v1", "backtester.panelPositions.v1"],
    target: ".saved-strategy-manager-panel",
    url: process.env.BACKTESTER_LAYOUT_SMOKE_URL || "http://127.0.0.1:4183",
  },
  {
    active: '[data-layout-id="app.stock_data_scraper"]',
    handle: ":scope > [data-layout-drag-handle]",
    modeKey: "hub-center.layoutMode.v1",
    name: "hub_center",
    panelSelector: "[data-layout-id]",
    resetSelector: ".layout-reset-button",
    sizeKeys: ["hub-center.layoutSizes.v2"],
    target: '[data-layout-id="system.resources"]',
    url: process.env.HUB_CENTER_LAYOUT_SMOKE_URL || "http://127.0.0.1:4184",
  },
];

const closeEnough = (left, right) => Math.abs(Number(left) - Number(right)) <= tolerance;
const overlaps = (left, right) => (
  left.x < right.x + right.width
  && left.x + left.width > right.x
  && left.y < right.y + right.height
  && left.y + left.height > right.y
);
const sameSlot = (actual, expected) => (
  closeEnough(actual?.x, expected?.x)
  && closeEnough(actual?.y, expected?.y)
  && closeEnough(actual?.width, expected?.width)
  && closeEnough(actual?.height, expected?.height)
);
const documentBox = (locator) => locator.evaluate((element) => {
  const rect = element.getBoundingClientRect();
  return {
    height: rect.height,
    width: rect.width,
    x: rect.x + window.scrollX,
    y: rect.y + window.scrollY,
  };
});

const browser = await chromium.launch({ channel: "msedge", headless: true });
const report = { checkedAt: new Date().toISOString(), results: [] };

try {
  for (const spec of specs) {
    const issues = [];
    const observations = {};
    const page = await browser.newPage({ viewport: { width: 1707, height: 960 } });
    try {
      await page.goto(spec.url, { waitUntil: "domcontentloaded" });
      await page.locator(spec.active).waitFor({ state: "visible" });
      await page.evaluate(({ modeKey, sizeKeys }) => {
        localStorage.removeItem(modeKey);
        sizeKeys.forEach((key) => localStorage.removeItem(key));
      }, spec);
      await page.reload({ waitUntil: "domcontentloaded" });
      await page.locator(spec.active).waitFor({ state: "visible" });
      await page.locator(
        spec.name === "paper_trader"
          ? '.layout-mode-button[data-layout-control="editor"]'
          : ".layout-mode-button",
      ).click();
      await page.waitForFunction(() => (
        document.documentElement.dataset.layoutMode === "edit"
        || document.querySelector(".app-shell")?.classList.contains("layout-edit-mode")
      ));
      await page.waitForTimeout(100);

      const active = page.locator(spec.active);
      const target = page.locator(spec.target);
      const activeBefore = await documentBox(active);
      const targetBefore = await documentBox(target);
      const targetViewportBefore = await target.boundingBox();
      observations.beforeScroll = await page.evaluate(() => ({
        elements: [...document.querySelectorAll("*")]
          .filter((element) => element.scrollTop)
          .map((element) => ({ className: element.className, scrollTop: element.scrollTop, tag: element.tagName })),
        windowY: window.scrollY,
      }));
      observations.before = { active: activeBefore, target: targetBefore };
      if (!activeBefore || !targetBefore || !targetViewportBefore) {
        issues.push("교환 전 두 패널의 좌표를 읽지 못했습니다.");
        continue;
      }
      if (closeEnough(activeBefore.width, targetBefore.width) && closeEnough(activeBefore.height, targetBefore.height)) {
        issues.push("서로 다른 크기의 슬롯을 선택하지 못했습니다.");
      }

      const handleBox = await active.locator(spec.handle).boundingBox();
      if (!handleBox) {
        issues.push("드래그 핸들을 찾지 못했습니다.");
        continue;
      }
      await page.mouse.move(handleBox.x + Math.min(handleBox.width / 2, 220), handleBox.y + handleBox.height / 2);
      await page.mouse.down();
      await page.mouse.move(
        targetViewportBefore.x + targetViewportBefore.width / 2,
        targetViewportBefore.y + 24,
        { steps: 18 },
      );
      await page.mouse.up();
      await page.waitForTimeout(180);
      observations.dragEndScroll = await page.evaluate(() => ({
        elements: [...document.querySelectorAll("*")]
          .filter((element) => element.scrollTop)
          .map((element) => ({ className: element.className, scrollTop: element.scrollTop, tag: element.tagName })),
        windowY: window.scrollY,
      }));
      await page.evaluate(() => {
        window.scrollTo(0, 0);
        [...document.querySelectorAll("*")].forEach((element) => {
          if (element.scrollTop) element.scrollTop = 0;
          if (element.scrollLeft) element.scrollLeft = 0;
        });
      });

      const activeAfter = await documentBox(active);
      const targetAfter = await documentBox(target);
      observations.after = { active: activeAfter, target: targetAfter };
      observations.layoutState = await page.evaluate(({ active, target }) => Object.fromEntries(
        [["active", document.querySelector(active)], ["target", document.querySelector(target)]].map(([name, element]) => {
          const rect = element?.getBoundingClientRect();
          const parentRect = element?.parentElement?.getBoundingClientRect();
          return [name, {
            offsetTop: element?.offsetTop,
            parentTop: parentRect?.top,
            rectTop: rect?.top,
            transform: element?.style.transform,
            x: element?.dataset.layoutOffsetX,
            y: element?.dataset.layoutOffsetY,
          }];
        }),
      ), spec);
      if (!sameSlot(activeAfter, targetBefore)) issues.push("첫 패널이 대상 슬롯의 위치·크기를 이어받지 못했습니다.");
      if (!sameSlot(targetAfter, activeBefore)) issues.push("대상 패널이 첫 패널의 원래 슬롯을 이어받지 못했습니다.");
      if (activeAfter && targetAfter && overlaps(activeAfter, targetAfter)) issues.push("교환한 두 패널이 겹칩니다.");

      const peerOverlapCount = await page.evaluate(({ active, panelSelector, target }) => {
        const first = document.querySelector(active);
        const second = document.querySelector(target);
        const peers = [...document.querySelectorAll(panelSelector)].filter((element) => {
          const rect = element.getBoundingClientRect();
          return rect.width > 0 && rect.height > 0;
        });
        return [first, second].reduce((count, element) => {
          if (!element) return count;
          const rect = element.getBoundingClientRect();
          return count + peers.filter((peer) => {
            if (peer === element || peer.contains(element) || element.contains(peer)) return false;
            const peerRect = peer.getBoundingClientRect();
            return rect.left < peerRect.right
              && rect.right > peerRect.left
              && rect.top < peerRect.bottom
              && rect.bottom > peerRect.top;
          }).length;
        }, 0);
      }, spec);
      if (peerOverlapCount) issues.push(`다른 패널과 ${peerOverlapCount}건 겹칩니다.`);

      const storedCounts = await page.evaluate(({ sizeKeys }) => Object.fromEntries(
        sizeKeys.map((key) => {
          const value = JSON.parse(localStorage.getItem(key) || "{}");
          return [key, Object.keys(value).length];
        }),
      ), spec);
      observations.storedCounts = storedCounts;
      observations.storedValues = await page.evaluate(({ sizeKeys }) => Object.fromEntries(
        sizeKeys.map((key) => [key, JSON.parse(localStorage.getItem(key) || "{}")]),
      ), spec);
      if (Object.values(storedCounts).some((count) => count < 2)) {
        issues.push("교환한 두 패널의 위치·크기가 모두 저장되지 않았습니다.");
      }

      await page.reload({ waitUntil: "domcontentloaded" });
      await page.locator(spec.active).waitFor({ state: "visible" });
      await page.waitForTimeout(180);
      await page.evaluate(() => {
        window.scrollTo(0, 0);
        [...document.querySelectorAll("*")].forEach((element) => {
          if (element.scrollTop) element.scrollTop = 0;
          if (element.scrollLeft) element.scrollLeft = 0;
        });
      });
      const activeReloaded = await documentBox(page.locator(spec.active));
      const targetReloaded = await documentBox(page.locator(spec.target));
      observations.reloaded = { active: activeReloaded, target: targetReloaded };
      if (!sameSlot(activeReloaded, activeAfter) || !sameSlot(targetReloaded, targetAfter)) {
        issues.push("새로고침 후 교환한 위치·크기가 유지되지 않았습니다.");
      }

      await page.locator(spec.resetSelector).evaluate((button) => button.click());
      await page.waitForTimeout(120);
      const resetState = await page.evaluate(({ panelSelector, sizeKeys }) => ({
        inlineLayout: [...document.querySelectorAll(panelSelector)].some((element) => (
          element.style.width || element.style.height || element.style.transform
        )),
        stored: sizeKeys.some((key) => localStorage.getItem(key)),
      }), spec);
      observations.resetState = resetState;
      if (resetState.inlineLayout || resetState.stored) {
        issues.push("초기화가 저장값 또는 인라인 배치를 제거하지 못했습니다.");
      }
    } catch (error) {
      issues.push(error instanceof Error ? error.message : String(error));
    } finally {
      await page.close();
      report.results.push({
        issues,
        name: spec.name,
        observations,
        ok: issues.length === 0,
        url: spec.url,
      });
    }
  }
} finally {
  await browser.close();
}

report.ok = report.results.every((result) => result.ok);
console.log(JSON.stringify(report, null, 2));
if (!report.ok) process.exitCode = 1;
