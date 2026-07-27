import { chromium } from "playwright-core";

const baseUrl = process.env.LIVE_TRADER_SMOKE_URL || "http://127.0.0.1:18795";
const chromePath = process.env.CHROME_PATH || "";
const viewports = [
  { name: "right-monitor", width: 1707, height: 960 },
  { name: "compact-desktop", width: 1280, height: 800 },
];
const tabs = ["사전점검", "실거래 준비", "자동화", "로그", "설정"];
const issues = [];
const views = [];
const browser = await chromium.launch(chromePath
  ? { headless: true, executablePath: chromePath }
  : { channel: "msedge", headless: true });

try {
  for (const viewport of viewports) {
    const page = await browser.newPage({ viewport });
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

    for (const tab of tabs) {
      await page.getByRole("button", { name: new RegExp(`^${tab}\\d*$`) }).click();
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
      if (layout.documentOverflow) issues.push(`${viewport.name}/${tab}: document horizontal overflow`);
      if (layout.workspaceOverflow) issues.push(`${viewport.name}/${tab}: workspace horizontal overflow`);
      if (!layout.pageTextLength) issues.push(`${viewport.name}/${tab}: empty page view`);
      if (layout.escapedControls.length) {
        issues.push(`${viewport.name}/${tab}: controls outside viewport (${layout.escapedControls.join(", ")})`);
      }
      views.push({ viewport: viewport.name, tab, ...layout });
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
