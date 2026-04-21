import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { chromium } from "playwright";
import { ImapFlow } from "imapflow";

const APP_URL = "https://app.hecto.finance";
const AUTH_URL = `${APP_URL}/auth`;
const OUTPUT_DIR = path.join(process.cwd(), "output", "playwright");
const ACCOUNTS_PATH = path.join(process.cwd(), "accounts.json");
const TARGET_ACCOUNT_NAME = process.env.HECTO_ACCOUNT_NAME?.trim() || "account-1";
const MAX_PAGES = 12;

function log(message) {
  console.log(`[${new Date().toISOString()}] ${message}`);
}

function normalizeSpaces(value) {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function sanitizeFilePart(value) {
  return String(value ?? "")
    .replace(/[^a-z0-9._-]+/gi, "-")
    .replace(/^-+|-+$/g, "")
    .toLowerCase();
}

async function ensureDir(dir) {
  await fs.mkdir(dir, { recursive: true });
}

async function readJsonFile(filePath) {
  const text = await fs.readFile(filePath, "utf8");
  return JSON.parse(text);
}

function summarizeJsonShape(value, depth = 0) {
  if (depth > 2) {
    return Array.isArray(value) ? `[array:${value.length}]` : typeof value;
  }
  if (Array.isArray(value)) {
    return {
      type: "array",
      length: value.length,
      sample:
        value.length > 0 && value.length <= 3
          ? value.map((entry) => summarizeJsonShape(entry, depth + 1))
          : value.length > 0
            ? summarizeJsonShape(value[0], depth + 1)
            : null,
    };
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .slice(0, 20)
        .map(([key, entry]) => [key, summarizeJsonShape(entry, depth + 1)]),
    );
  }
  return value;
}

async function waitForOtp(email, appPassword, earliestTime) {
  const client = new ImapFlow({
    host: "imap.gmail.com",
    port: 993,
    secure: true,
    auth: {
      user: email,
      pass: appPassword,
    },
    logger: false,
  });

  const deadline = Date.now() + 180_000;

  try {
    await client.connect();
    const lock = await client.getMailboxLock("INBOX");
    try {
      while (Date.now() < deadline) {
        const since = new Date(Math.max(0, earliestTime - 60_000));
        const uids = await client.search({ since });
        const ordered = [...uids].sort((left, right) => right - left);

        for (const uid of ordered) {
          const message = await client.fetchOne(uid, {
            envelope: true,
            internalDate: true,
            source: true,
          });

          if (!message?.source) {
            continue;
          }

          const sentAt = message.internalDate ? new Date(message.internalDate).getTime() : 0;
          if (sentAt && sentAt < earliestTime - 60_000) {
            continue;
          }

          const fromAddress =
            message.envelope?.from?.map((entry) => entry.address ?? "").join(" ") ?? "";
          const subject = message.envelope?.subject ?? "";
          const sourceText = message.source.toString("utf8");
          const haystack = `${fromAddress}\n${subject}\n${sourceText}`;

          if (!/privy/i.test(haystack) && !/confirmation code/i.test(haystack)) {
            continue;
          }

          const match = haystack.match(/\b(\d{6})\b/);
          if (match) {
            return match[1];
          }
        }

        await new Promise((resolve) => setTimeout(resolve, 4_000));
      }
    } finally {
      lock.release();
    }
  } finally {
    await client.logout().catch(() => {});
  }

  throw new Error("Timed out waiting for Privy OTP email");
}

async function clickIfVisible(page, selector) {
  const locator = page.locator(selector).first();
  if (await locator.count()) {
    if (await locator.isVisible().catch(() => false)) {
      await locator.click();
      return true;
    }
  }
  return false;
}

async function isVisible(page, selector) {
  const locator = page.locator(selector).first();
  if (!(await locator.count())) {
    return false;
  }
  return locator.isVisible().catch(() => false);
}

async function waitForAny(page, selectors, timeout = 30_000) {
  const started = Date.now();
  while (Date.now() - started < timeout) {
    for (const selector of selectors) {
      const locator = page.locator(selector).first();
      if (await locator.count()) {
        if (await locator.isVisible().catch(() => false)) {
          return selector;
        }
      }
    }
    await page.waitForTimeout(250);
  }
  throw new Error(`Timed out waiting for selectors: ${selectors.join(", ")}`);
}

async function fetchJson(page, url) {
  return page.evaluate(async (resource) => {
    const response = await fetch(resource, { credentials: "include" });
    const text = await response.text();
    let json = null;

    try {
      json = JSON.parse(text);
    } catch {
      json = null;
    }

    return {
      ok: response.ok,
      status: response.status,
      contentType: response.headers.get("content-type") || "",
      text,
      json,
    };
  }, url);
}

async function ensureLoggedIn(page, credentials) {
  await page.goto(AUTH_URL, { waitUntil: "domcontentloaded" });
  await page.waitForLoadState("networkidle").catch(() => {});

  const authenticated = await page.evaluate(async () => {
    const response = await fetch("/api/hecto/auth/me", { credentials: "include" });
    const payload = await response.json().catch(() => null);
    return Boolean(payload?.user);
  });

  if (authenticated) {
    log("Existing Hecto session is already active");
    return;
  }

  if (
    (await isVisible(page, 'text=/sign message/i')) &&
    ((await isVisible(page, 'button:has-text("Sign")')) || (await isVisible(page, 'button:has-text("SIGN")')))
  ) {
    log("Detected direct wallet verification prompt on auth page");
    if (await clickIfVisible(page, 'button:has-text("Sign")')) {
      log('Clicked "Sign" on direct auth challenge');
    } else {
      await clickIfVisible(page, 'button:has-text("SIGN")');
      log('Clicked "SIGN" on direct auth challenge');
    }
    await page.waitForURL((url) => !/\/auth(?:$|\?)/.test(url.toString()), { timeout: 90_000 });
    return;
  }

  log("Submitting email/password login");
  await waitForAny(page, [
    'input[type="email"]',
    'input[name*="email" i]',
    'input[autocomplete="email"]',
    '[role="textbox"][aria-label*="email" i]',
  ], 30_000);

  if (await isVisible(page, 'input[type="email"]')) {
    await page.locator('input[type="email"]').first().fill(credentials.email);
  } else if (await isVisible(page, 'input[name*="email" i]')) {
    await page.locator('input[name*="email" i]').first().fill(credentials.email);
  } else if (await isVisible(page, 'input[autocomplete="email"]')) {
    await page.locator('input[autocomplete="email"]').first().fill(credentials.email);
  } else {
    await page.getByRole("textbox", { name: /email/i }).first().fill(credentials.email);
  }

  if (await isVisible(page, 'input[type="password"]')) {
    await page.locator('input[type="password"]').first().fill(credentials.password);
  } else if (await isVisible(page, 'input[name*="password" i]')) {
    await page.locator('input[name*="password" i]').first().fill(credentials.password);
  } else {
    await page.getByLabel(/password/i).first().fill(credentials.password);
  }
  await page.getByRole("button", { name: /sign in/i }).click();

  await page.waitForTimeout(2_000);
  if (!/\/auth(?:$|\?)/.test(page.url())) {
    log("Email/password login completed without wallet reconnect");
    return;
  }

  await waitForAny(page, [
    'button:has-text("Supanova Wallet")',
    'button:has-text("CONNECT WALLET")',
    'input[placeholder="your@email.com"]',
    'text=/enter confirmation code/i',
  ], 30_000);

  log("Starting Supanova wallet connect");
  if (await isVisible(page, 'button:has-text("CONNECT WALLET")')) {
    await page.getByRole("button", { name: /connect wallet/i }).click();
  }
  if (await isVisible(page, 'button:has-text("Supanova Wallet")')) {
    await page.getByRole("button", { name: /supanova wallet/i }).click();
  }

  await waitForAny(page, [
    'input[placeholder="your@email.com"]',
    'input[placeholder*="@"]',
    'text=/enter confirmation code/i',
  ], 30_000);

  if (await isVisible(page, 'input[placeholder="your@email.com"]')) {
    const emailInput = page.locator('input[placeholder="your@email.com"]').first();
    await emailInput.click();
    await emailInput.fill("");
    await emailInput.type(credentials.email, { delay: 40 });
  } else if (await isVisible(page, 'input[placeholder*="@"]')) {
    const emailInput = page.locator('input[placeholder*="@"]').first();
    await emailInput.click();
    await emailInput.fill("");
    await emailInput.type(credentials.email, { delay: 40 });
  }

  if (await isVisible(page, 'button:has-text("Submit")')) {
    const submitButton = page.getByRole("button", { name: /^submit$/i }).first();
    await submitButton.waitFor({ state: "visible", timeout: 10_000 });
    await submitButton.click();
  } else if (await isVisible(page, 'button:has-text("Continue")')) {
    await page.getByRole("button", { name: /continue/i }).click();
  }

  const otpStart = Date.now();
  const otpCode = await waitForOtp(credentials.email, credentials.gmailAppPassword, otpStart);
  log("OTP received");

  const otpInputs = page.locator('input[inputmode="numeric"], input[autocomplete="one-time-code"]');
  const otpCount = await otpInputs.count();
  if (otpCount >= 6) {
    for (let index = 0; index < 6; index += 1) {
      await otpInputs.nth(index).fill(otpCode[index]);
    }
  } else {
    await page.keyboard.type(otpCode);
  }

  await waitForAny(page, [
    'button:has-text("Sign")',
    'button:has-text("SIGN")',
    'text=/sign message/i',
  ], 90_000);

  if (await clickIfVisible(page, 'button:has-text("Sign")')) {
    log('Clicked "Sign" on message prompt');
  } else {
    await clickIfVisible(page, 'button:has-text("SIGN")');
    log('Clicked "SIGN" on message prompt');
  }

  await page.waitForURL((url) => !/\/auth(?:$|\?)/.test(url.toString()), { timeout: 90_000 });
}

function toSameOriginPath(href) {
  try {
    const url = new URL(href, APP_URL);
    if (url.origin !== APP_URL) {
      return null;
    }
    return `${url.pathname}${url.search}`;
  } catch {
    return null;
  }
}

async function collectPageSummary(page) {
  return page.evaluate(() => {
    const normalize = (value) => String(value ?? "").replace(/\s+/g, " ").trim();
    const isVisible = (element) => {
      if (!(element instanceof HTMLElement)) {
        return false;
      }
      const style = window.getComputedStyle(element);
      const rect = element.getBoundingClientRect();
      return style.visibility !== "hidden" && style.display !== "none" && rect.width > 0 && rect.height > 0;
    };
    const uniqueBy = (items, keyFn) => {
      const seen = new Set();
      const output = [];
      for (const item of items) {
        const key = keyFn(item);
        if (seen.has(key)) {
          continue;
        }
        seen.add(key);
        output.push(item);
      }
      return output;
    };

    const headings = uniqueBy(
      Array.from(document.querySelectorAll("h1,h2,h3,h4,h5,h6"))
        .filter(isVisible)
        .map((element) => normalize(element.textContent))
        .filter(Boolean),
      (value) => value.toLowerCase(),
    ).slice(0, 20);

    const buttons = uniqueBy(
      Array.from(document.querySelectorAll("button,[role='button']"))
        .filter(isVisible)
        .map((element) => normalize(element.textContent || element.getAttribute("aria-label")))
        .filter(Boolean),
      (value) => value.toLowerCase(),
    ).slice(0, 50);

    const links = uniqueBy(
      Array.from(document.querySelectorAll("a[href]"))
        .filter(isVisible)
        .map((element) => ({
          text: normalize(element.textContent || element.getAttribute("aria-label")),
          href: element.href,
        }))
        .filter((entry) => entry.href),
      (entry) => `${entry.href}|${entry.text.toLowerCase()}`,
    ).slice(0, 100);

    const inputs = Array.from(document.querySelectorAll("input,textarea,select"))
      .filter(isVisible)
      .map((element) => ({
        tag: element.tagName.toLowerCase(),
        type: element.getAttribute("type") || "",
        name: element.getAttribute("name") || "",
        placeholder: element.getAttribute("placeholder") || "",
        ariaLabel: element.getAttribute("aria-label") || "",
        valueLength: String(element.value ?? "").length,
        disabled: Boolean(element.disabled),
        readOnly: Boolean(element.readOnly),
      }))
      .slice(0, 20);

    const tableRows = Array.from(document.querySelectorAll("tr,[role='row']"))
      .filter(isVisible)
      .map((element) => normalize(element.textContent))
      .filter(Boolean)
      .slice(0, 25);

    const textLines = normalize(document.body?.innerText ?? "")
      .split(/(?<=\S)\s{2,}|\n+/)
      .map((line) => normalize(line))
      .filter(Boolean)
      .slice(0, 30);

    return {
      title: document.title,
      url: location.href,
      path: `${location.pathname}${location.search}`,
      headings,
      buttons,
      links,
      inputs,
      tableRows,
      textLines,
    };
  });
}

function extractDiscoveredRoutes(summary) {
  return (summary.links ?? [])
    .map((entry) => toSameOriginPath(entry.href))
    .filter(Boolean);
}

function summarizeApiResult(result, extra = {}) {
  return {
    ok: Boolean(result?.ok),
    status: Number(result?.status ?? 0),
    contentType: result?.contentType ?? "",
    shape: summarizeJsonShape(result?.json),
    ...extra,
  };
}

async function inspectApis(page) {
  const meResult = await fetchJson(page, "/api/hecto/auth/me");
  const partyId = meResult.json?.user?.partyId ? String(meResult.json.user.partyId) : "";
  const tableResult = await fetchJson(page, "/api/allocator/table");
  const companiesResult = await fetchJson(page, "/api/allocator/companies");
  const pricesResult = await fetchJson(page, "/api/prices/latest");
  const balancesResult = await page.evaluate(async () => {
    const response = await fetch("https://api.supanova.app/canton/api/balances", {
      credentials: "include",
    });
    const text = await response.text();
    let json = null;
    try {
      json = JSON.parse(text);
    } catch {
      json = null;
    }
    return {
      ok: response.ok,
      status: response.status,
      contentType: response.headers.get("content-type") || "",
      json,
    };
  }).catch(() => ({
    ok: false,
    status: 0,
    contentType: "",
    json: null,
  }));

  const locksResult = partyId
    ? await fetchJson(page, `/api/locks?userPartyId=${encodeURIComponent(partyId)}`)
    : null;

  const rows = Array.isArray(tableResult.json?.rows) ? tableResult.json.rows : [];
  const companies = Array.isArray(companiesResult.json) ? companiesResult.json : [];
  const prices = pricesResult.json?.prices && typeof pricesResult.json.prices === "object"
    ? pricesResult.json.prices
    : {};
  const bestPriceEntry = Object.entries(prices)
    .map(([companyId, entry]) => ({
      companyId,
      changePct: Number(entry?.changePct ?? Number.NEGATIVE_INFINITY),
      price: entry?.price ?? null,
    }))
    .sort((left, right) => right.changePct - left.changePct)[0] ?? null;

  return {
    me: summarizeApiResult(meResult, {
      authenticated: Boolean(meResult.json?.user),
      partyId,
      email: meResult.json?.user?.email ? "[present]" : "",
    }),
    allocatorTable: summarizeApiResult(tableResult, {
      rowCount: rows.length,
      firstRows: rows.slice(0, 5).map((row) => ({
        companyId: row.companyId,
        companyName: row.companyName,
        performancePct: row.performancePct,
        userLocked: row.userLocked,
        totalLocked: row.totalLocked,
      })),
    }),
    companies: summarizeApiResult(companiesResult, {
      companyCount: companies.length,
      firstCompanies: companies.slice(0, 5).map((entry) => ({
        id: entry.id,
        name: entry.name,
      })),
    }),
    prices: summarizeApiResult(pricesResult, {
      priceCount: Object.keys(prices).length,
      bestPriceEntry,
    }),
    locks: locksResult
      ? summarizeApiResult(locksResult, {
          lockCount: Array.isArray(locksResult.json?.locks) ? locksResult.json.locks.length : 0,
          firstLocks: Array.isArray(locksResult.json?.locks)
            ? locksResult.json.locks.slice(0, 5).map((entry) => ({
                status: entry.status,
                amount: entry.lock?.amount,
                context: entry.lock?.context,
              }))
            : [],
        })
      : null,
    balances: summarizeApiResult(balancesResult, {
      tokenCount: Array.isArray(balancesResult.json?.tokens) ? balancesResult.json.tokens.length : 0,
      hectoToken: Array.isArray(balancesResult.json?.tokens)
        ? balancesResult.json.tokens.find((token) => String(token?.instrumentId?.id ?? "").toUpperCase() === "HECTO") ?? null
        : null,
    }),
  };
}

function buildMarkdownReport(report) {
  const lines = [
    `# Hecto Live Inspection`,
    ``,
    `- generatedAt: ${report.generatedAt}`,
    `- account: ${report.account.name}`,
    `- profile: ${report.account.profileName}`,
    `- authenticated: ${report.apis.me.authenticated}`,
    `- partyId: ${report.apis.me.partyId || "-"}`,
    `- allocator rows: ${report.apis.allocatorTable.rowCount}`,
    `- companies: ${report.apis.companies.companyCount}`,
    `- prices: ${report.apis.prices.priceCount}`,
    `- locks: ${report.apis.locks?.lockCount ?? 0}`,
    ``,
    `## Best Price Entry`,
    ``,
    `\`${JSON.stringify(report.apis.prices.bestPriceEntry ?? null)}\``,
    ``,
    `## Pages`,
    ``,
  ];

  for (const page of report.pages) {
    lines.push(`### ${page.path}`);
    lines.push(`- title: ${page.title || "-"}`);
    lines.push(`- screenshot: ${page.screenshotName}`);
    lines.push(`- headings: ${page.headings.slice(0, 6).join(" | ") || "-"}`);
    lines.push(`- buttons: ${page.buttons.slice(0, 10).join(" | ") || "-"}`);
    lines.push(`- inputs: ${page.inputs.map((entry) => `${entry.tag}:${entry.type || "-"}/${entry.placeholder || entry.name || entry.ariaLabel || "-"}`).join(" | ") || "-"}`);
    lines.push(`- discoveredLinks: ${page.links.slice(0, 10).map((entry) => `${entry.text || "[no-text]"} -> ${entry.href}`).join(" | ") || "-"}`);
    lines.push(`- tableRows: ${page.tableRows.slice(0, 5).join(" || ") || "-"}`);
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}

async function main() {
  await ensureDir(OUTPUT_DIR);
  const accounts = await readJsonFile(ACCOUNTS_PATH);
  const account = accounts.find((entry) => entry?.name === TARGET_ACCOUNT_NAME);

  if (!account) {
    throw new Error(`Account ${TARGET_ACCOUNT_NAME} not found in accounts.json`);
  }

  const profileName = account.profileName?.trim() || account.name || TARGET_ACCOUNT_NAME;
  const profileDir = path.join(process.cwd(), ".profile", profileName);
  const headless = /^true$/i.test(String(process.env.HECTO_HEADLESS ?? "false"));
  log(`Launching Chrome profile ${profileName} (headless=${headless})`);

  const context = await chromium.launchPersistentContext(profileDir, {
    headless,
    viewport: { width: 1440, height: 960 },
    channel: "chrome",
  });

  const page = context.pages()[0] ?? (await context.newPage());

  try {
    await ensureLoggedIn(page, {
      email: account.email,
      password: account.password,
      gmailAppPassword: account.gmailAppPassword,
    });

    const apis = await inspectApis(page);
    const discoveredRoutes = new Set(["/", "/auth", "/allocate"]);
    if (apis.prices.bestPriceEntry?.companyId) {
      discoveredRoutes.add(`/allocate?project=${encodeURIComponent(apis.prices.bestPriceEntry.companyId)}`);
    }

    const pages = [];
    const visited = new Set();

    while (discoveredRoutes.size && pages.length < MAX_PAGES) {
      const nextPath = Array.from(discoveredRoutes).find((entry) => !visited.has(entry));
      if (!nextPath) {
        break;
      }

      discoveredRoutes.delete(nextPath);
      visited.add(nextPath);

      const targetUrl = new URL(nextPath, APP_URL).toString();
      log(`Inspecting ${targetUrl}`);
      await page.goto(targetUrl, { waitUntil: "domcontentloaded" });
      await page.waitForLoadState("networkidle").catch(() => {});
      await page.waitForTimeout(1_500);

      const summary = await collectPageSummary(page);
      const screenshotName = `live-${String(pages.length + 1).padStart(2, "0")}-${sanitizeFilePart(summary.path || nextPath)}.png`;
      await page.screenshot({
        path: path.join(OUTPUT_DIR, screenshotName),
        fullPage: true,
      }).catch(() => {});

      for (const route of extractDiscoveredRoutes(summary)) {
        if (!visited.has(route)) {
          discoveredRoutes.add(route);
        }
      }

      pages.push({
        ...summary,
        screenshotName,
      });
    }

    const report = {
      generatedAt: new Date().toISOString(),
      account: {
        name: account.name,
        profileName,
      },
      apis,
      pages,
    };

    const stamp = Date.now();
    const jsonPath = path.join(OUTPUT_DIR, `live-inspect-${stamp}.json`);
    const mdPath = path.join(OUTPUT_DIR, `live-inspect-${stamp}.md`);
    await fs.writeFile(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
    await fs.writeFile(mdPath, buildMarkdownReport(report), "utf8");

    log(`Inspection JSON saved to ${jsonPath}`);
    log(`Inspection Markdown saved to ${mdPath}`);
  } finally {
    await context.close();
  }
}

main().catch((error) => {
  console.error(`[${new Date().toISOString()}] fatal: ${error.message}`);
  process.exitCode = 1;
});
