import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [baseUrl, finishedTaskId, runningTaskId] = process.argv.slice(2);

if (!baseUrl || !finishedTaskId || !runningTaskId) {
  console.error("Usage: node scripts/ui_smoke_check.mjs <baseUrl> <finishedTaskId> <runningTaskId>");
  process.exit(1);
}

const outputDir = path.resolve("output/playwright");
fs.mkdirSync(outputDir, { recursive: true });

const playwrightModulePath = path.resolve("dashboard-ui/node_modules/playwright/index.mjs");
if (!fs.existsSync(playwrightModulePath)) {
  console.error("Local Playwright dependency is missing. Run `npm install` in dashboard-ui first.");
  process.exit(1);
}

const { chromium } = await import(pathToFileURL(playwrightModulePath).href);

async function ensureText(page, text, label = text) {
  const locator = page.getByText(text, { exact: false }).first();
  await locator.waitFor({ state: "visible", timeout: 15000 }).catch(() => null);
  const visible = await locator.isVisible().catch(() => false);
  if (!visible) {
    throw new Error(`Expected to find text "${label}" on ${page.url()}`);
  }
}

async function ensureSelector(page, selector, label = selector) {
  const locator = page.locator(selector).first();
  await locator.waitFor({ state: "visible", timeout: 15000 }).catch(() => null);
  const visible = await locator.isVisible().catch(() => false);
  if (!visible) {
    throw new Error(`Expected to find selector "${label}" on ${page.url()}`);
  }
}

async function ensureNoRawJsonError(page) {
  const bodyText = await page.locator("body").innerText();
  if (bodyText.includes('{"detail"') || bodyText.includes('"Not found."')) {
    throw new Error(`Page rendered a raw JSON error: ${page.url()}`);
  }
}

async function ensureNonEmptyContent(page) {
  const contentText = await page.locator(".content-shell").first().innerText();
  if (!contentText || contentText.trim().length < 20) {
    throw new Error(`Page content is unexpectedly empty: ${page.url()}`);
  }
}

async function checkPage(page, route, { texts = [], selectors = [] }, screenshotName) {
  await page.goto(`${baseUrl}${route}`, { waitUntil: "domcontentloaded", timeout: 45000 });
  await page.locator(".content-shell").first().waitFor({ state: "visible", timeout: 15000 });
  await ensureNoRawJsonError(page);
  await ensureNonEmptyContent(page);
  for (const selector of selectors) {
    await ensureSelector(page, selector);
  }
  for (const text of texts) {
    await ensureText(page, text);
  }
  await page.screenshot({ path: path.join(outputDir, screenshotName), fullPage: true });
}

const browser = await chromium.launch({ headless: true });

try {
  const page = await browser.newPage();

  await checkPage(
    page,
    "/",
    {
      texts: ["TradingAgents"],
      selectors: [".hero-card", ".stats-grid .stat-card", ".overnight-home-grid", 'a[href="/overnight"]']
    },
    "ui-smoke-home.png"
  );

  await checkPage(
    page,
    "/overnight",
    {
      texts: ["Total Score"],
      selectors: [".analysis-main-grid", ".overnight-home-grid", ".breakdown-table", ".audit-grid"]
    },
    "ui-smoke-overnight.png"
  );

  await checkPage(
    page,
    "/analyze",
    {
      texts: ["分析配置"],
      selectors: [".analysis-shell", ".analysis-overview-card", ".date-picker-shell"]
    },
    "ui-smoke-analyze.png"
  );

  await page.goto(`${baseUrl}/analyze`, { waitUntil: "domcontentloaded", timeout: 45000 });
  await page.locator(".date-picker-trigger").first().click();
  await ensureSelector(page, ".date-picker-popover");
  await page.screenshot({ path: path.join(outputDir, "ui-smoke-analyze-datepicker.png"), fullPage: true });

  await checkPage(
    page,
    "/tasks",
    {
      texts: ["任务中心"],
      selectors: [".filter-toolbar", ".filter-group", ".task-table", 'a[href^="/tasks/"]']
    },
    "ui-smoke-tasks.png"
  );

  const runningFilter = page.getByRole("button", { name: "运行中" }).first();
  const allFilter = page.getByRole("button", { name: "全部" }).first();
  await runningFilter.click();
  await page.waitForFunction(
    () => {
      const button = Array.from(document.querySelectorAll("button")).find(
        (item) => item.textContent?.trim() === "运行中"
      );
      return button?.classList.contains("filter-active");
    },
    undefined,
    { timeout: 15000 }
  );
  await runningFilter.click();
  await page.waitForFunction(
    () => {
      const button = Array.from(document.querySelectorAll("button")).find(
        (item) => item.textContent?.trim() === "全部"
      );
      return button?.classList.contains("filter-active");
    },
    undefined,
    { timeout: 15000 }
  );
  await page.screenshot({ path: path.join(outputDir, "ui-smoke-tasks-filters.png"), fullPage: true });

  await checkPage(
    page,
    `/tasks/${runningTaskId}`,
    {
      selectors: [".stage-strip", ".hero-actions button", ".report-shell", ".tab-row"]
    },
    "ui-smoke-task-running.png"
  );

  await checkPage(
    page,
    `/tasks/${finishedTaskId}`,
    {
      texts: ["HOLD"],
      selectors: [".factor-summary-shell", ".download-row a", ".report-shell"]
    },
    "ui-smoke-task-finished.png"
  );
} finally {
  await browser.close();
}
