import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { chromium } from "playwright";
import { ImapFlow } from "imapflow";

const APP_URL = "https://app.hecto.finance";
const AUTH_URL = `${APP_URL}/auth`;
const ALLOCATE_URL = `${APP_URL}/allocate`;
const OUTPUT_DIR = path.join(process.cwd(), "output", "playwright");
const RUNTIME_DIR = path.join(process.cwd(), ".runtime");
const ACCOUNT_LOCKS_DIR = path.join(RUNTIME_DIR, "account-locks");
const CONFIG_PATH = path.join(process.cwd(), "config.json");
const ACCOUNTS_PATH = path.join(process.cwd(), "accounts.json");
const SCHEDULE_STATE_PATH = path.join(RUNTIME_DIR, "schedule-state.json");
let LOG_PREFIX = "";

function log(message) {
  const prefix = LOG_PREFIX ? ` [${LOG_PREFIX}]` : "";
  console.log(`[${new Date().toISOString()}]${prefix} ${message}`);
}

function requiredEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable ${name}`);
  }
  return value;
}

function parseNumber(value) {
  if (typeof value === "number") {
    return value;
  }
  const cleaned = String(value ?? "")
    .replace(/[%,$]/g, "")
    .replace(/\s+/g, "")
    .trim();
  const parsed = Number.parseFloat(cleaned);
  return Number.isFinite(parsed) ? parsed : 0;
}

function normalizeSpaces(value) {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function escapeRegExp(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

async function ensureDir(dir) {
  await fs.mkdir(dir, { recursive: true });
}

async function readJsonFileOrDefault(filePath, fallbackValue) {
  try {
    const text = await fs.readFile(filePath, "utf8");
    return JSON.parse(text);
  } catch (error) {
    if (error?.code === "ENOENT") {
      return fallbackValue;
    }
    throw error;
  }
}

async function loadScheduleState() {
  return readJsonFileOrDefault(SCHEDULE_STATE_PATH, { lastCompletedDateKey: "" });
}

async function saveScheduleState(state) {
  await ensureDir(path.dirname(SCHEDULE_STATE_PATH));
  await fs.writeFile(SCHEDULE_STATE_PATH, `${JSON.stringify(state, null, 2)}\n`, "utf8");
}

function setLogPrefix(prefix) {
  LOG_PREFIX = prefix ? String(prefix) : "";
}

async function readJsonFileIfExists(filePath) {
  try {
    const text = await fs.readFile(filePath, "utf8");
    return JSON.parse(text);
  } catch (error) {
    if (error?.code === "ENOENT") {
      return null;
    }
    throw error;
  }
}

function weekdayNameToIndex(value) {
  const normalized = String(value ?? "").trim().toLowerCase();
  const map = new Map([
    ["sunday", 0],
    ["sun", 0],
    ["monday", 1],
    ["mon", 1],
    ["selasa", 2],
    ["tuesday", 2],
    ["tue", 2],
    ["wednesday", 3],
    ["wed", 3],
    ["rabu", 3],
    ["thursday", 4],
    ["thu", 4],
    ["kamis", 4],
    ["friday", 5],
    ["fri", 5],
    ["jumat", 5],
    ["saturday", 6],
    ["sat", 6],
    ["sabtu", 6],
    ["minggu", 0],
  ]);
  return map.has(normalized) ? map.get(normalized) : Number.parseInt(normalized, 10);
}

function getZonedParts(date, timeZone) {
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
    weekday: "short",
  });
  const parts = Object.fromEntries(
    formatter.formatToParts(date).filter((entry) => entry.type !== "literal").map((entry) => [entry.type, entry.value]),
  );
  const weekdayMap = new Map([
    ["Sun", 0],
    ["Mon", 1],
    ["Tue", 2],
    ["Wed", 3],
    ["Thu", 4],
    ["Fri", 5],
    ["Sat", 6],
  ]);
  return {
    year: Number(parts.year),
    month: Number(parts.month),
    day: Number(parts.day),
    hour: Number(parts.hour),
    minute: Number(parts.minute),
    second: Number(parts.second),
    weekday: weekdayMap.get(parts.weekday) ?? 0,
    dateKey: `${parts.year}-${parts.month}-${parts.day}`,
  };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function formatDurationMinutes(ms) {
  const minutes = Math.max(1, Math.round(Number(ms) / 60000));
  return `${minutes} minute${minutes === 1 ? "" : "s"}`;
}

function defaultConfig() {
  return {
    timezone: "Asia/Jakarta",
    schedule: {
      enabled: true,
      hour: 4,
      minute: 30,
      weekdays: [1, 2, 3, 4, 5],
      pollIntervalMs: 30_000,
      runOnStart: false,
      dedupeAcrossRestarts: true,
    },
    execution: {
      headless: false,
      execute: true,
      closeBrowser: true,
      browserChannel: "chrome",
      accountDelayMs: 5_000,
      accountLockTtlMs: 6 * 60 * 60 * 1000,
      retryOnFailure: true,
      retryDelayMs: 15 * 60 * 1000,
      maxAttemptsPerCycle: 2,
    },
    locking: {
      minAmount: 5_000,
      amountMode: "max",
      amountValue: 5_000,
      amountMax: 0,
      unlockAllBeforeLock: true,
    },
  };
}

function mergeDeep(base, override) {
  if (!override || typeof override !== "object" || Array.isArray(override)) {
    return override ?? base;
  }
  const output = { ...base };
  for (const [key, value] of Object.entries(override)) {
    if (value && typeof value === "object" && !Array.isArray(value) && typeof output[key] === "object" && output[key] !== null && !Array.isArray(output[key])) {
      output[key] = mergeDeep(output[key], value);
    } else {
      output[key] = value;
    }
  }
  return output;
}

async function loadRuntimeConfig() {
  const fileConfig = await readJsonFileIfExists(CONFIG_PATH);
  const config = mergeDeep(defaultConfig(), fileConfig ?? {});
  config.schedule.weekdays = (config.schedule.weekdays ?? [1, 2, 3, 4, 5])
    .map(weekdayNameToIndex)
    .filter((value) => Number.isInteger(value) && value >= 0 && value <= 6);
  if (!config.schedule.weekdays.length) {
    config.schedule.weekdays = [1, 2, 3, 4, 5];
  }
  return config;
}

function normalizeAccount(account, index, config) {
  const locking = mergeDeep(config.locking, account.locking ?? {});
  return {
    name: account.name?.trim() || account.email?.trim() || `account-${index + 1}`,
    enabled: account.enabled !== false,
    email: String(account.email ?? "").trim(),
    password: String(account.password ?? "").trim(),
    gmailAppPassword: String(account.gmailAppPassword ?? "").trim(),
    profileName: account.profileName?.trim() || account.name?.trim() || `account-${index + 1}`,
    locking,
  };
}

async function loadAccounts(config) {
  const accountsFile = await readJsonFileIfExists(ACCOUNTS_PATH);
  if (Array.isArray(accountsFile) && accountsFile.length) {
    return accountsFile.map((account, index) => normalizeAccount(account, index, config)).filter((account) => account.enabled);
  }

  return [
    normalizeAccount(
      {
        name: process.env.HECTO_ACCOUNT_NAME || "primary",
        email: requiredEnv("HECTO_EMAIL"),
        password: requiredEnv("HECTO_PASSWORD"),
        gmailAppPassword: requiredEnv("HECTO_GMAIL_APP_PASSWORD"),
        profileName: process.env.HECTO_PROFILE_NAME?.trim() || "primary",
        locking: {
          amountMode: process.env.HECTO_AMOUNT_MODE?.trim() || process.env.HECTO_LOCK_MODE?.trim(),
          amountValue: process.env.HECTO_LOCK_AMOUNT,
          amountMax: process.env.HECTO_LOCK_AMOUNT_MAX,
        },
      },
      0,
      config,
    ),
  ];
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

async function clickIfVisible(page, target) {
  const locator = page.locator(target).first();
  if (await locator.count()) {
    if (await locator.isVisible().catch(() => false)) {
      await locator.click();
      return true;
    }
  }
  return false;
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

async function isVisible(page, selector) {
  const locator = page.locator(selector).first();
  if (!(await locator.count())) {
    return false;
  }
  return locator.isVisible().catch(() => false);
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
    log(`Dashboard session re-established at ${page.url()}`);
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
    log("Filled Privy email field");
  } else if (await isVisible(page, 'input[placeholder*="@"]')) {
    const emailInput = page.locator('input[placeholder*="@"]').first();
    await emailInput.click();
    await emailInput.fill("");
    await emailInput.type(credentials.email, { delay: 40 });
    log("Filled Privy email field via fallback selector");
  }

  if (await isVisible(page, 'button:has-text("Submit")')) {
    const submitButton = page.getByRole("button", { name: /^submit$/i }).first();
    await submitButton.waitFor({ state: "visible", timeout: 10_000 });
    await submitButton.click();
  } else if (await isVisible(page, 'button:has-text("Continue")')) {
    await page.getByRole("button", { name: /continue/i }).click();
  }

  const otpStart = Date.now();

  log("Waiting for OTP from Gmail");
  const otpCode = await waitForOtp(credentials.email, credentials.gmailAppPassword, otpStart);
  log("OTP received, filling code");

  const otpInputs = page.locator('input[inputmode="numeric"], input[autocomplete="one-time-code"]');
  const otpCount = await otpInputs.count();
  if (otpCount >= 6) {
    for (let index = 0; index < 6; index += 1) {
      await otpInputs.nth(index).fill(otpCode[index]);
    }
  } else {
    await page.keyboard.type(otpCode);
  }

  log("Waiting for sign message prompt");
  await waitForAny(page, [
    'button:has-text("Sign")',
    'button:has-text("SIGN")',
    'text=/sign message/i',
  ], 90_000);

  if (await clickIfVisible(page, 'button:has-text("Sign")')) {
    log("Clicked Sign on message prompt");
  } else {
    await clickIfVisible(page, 'button:has-text("SIGN")');
    log("Clicked SIGN on message prompt");
  }

  await page.waitForURL((url) => !/\/auth(?:$|\?)/.test(url.toString()), { timeout: 90_000 });
  log(`Dashboard session established at ${page.url()}`);
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
      text,
      json,
    };
  }, url);
}

function mapAllocateRow(row) {
  const allocatorPerformancePct = Number(row.performancePct ?? 0);
  return {
    companyId: row.companyId,
    company: row.companyName,
    totalLocked: row.totalLocked,
    performancePct: allocatorPerformancePct,
    change: `${(allocatorPerformancePct * 100).toFixed(2)}%`,
    youLockedValue: Number(row.userLocked ?? 0),
    youLocked: String(row.userLocked ?? 0),
    changeValue: allocatorPerformancePct * 100,
    allocatorPerformancePct,
  };
}

function mergeBestCompanyData(rows, companies, prices) {
  const companyNameById = new Map(
    (Array.isArray(companies) ? companies : [])
      .filter((item) => item?.id)
      .map((item) => [String(item.id), String(item.name ?? item.id)]),
  );
  const priceMap = prices && typeof prices === "object" ? prices : {};

  return rows.map((row) => {
    const companyId = String(row.companyId ?? "");
    const latestPrice = priceMap[companyId];
    const latestChangePct = Number(latestPrice?.changePct);
    const hasLatestChange = Number.isFinite(latestChangePct);

    return {
      ...row,
      company: companyNameById.get(companyId) ?? row.company,
      performancePct: hasLatestChange ? latestChangePct / 100 : row.performancePct,
      changeValue: hasLatestChange ? latestChangePct : row.changeValue,
      change: hasLatestChange ? `${latestChangePct.toFixed(2)}%` : row.change,
      latestPrice: Number.isFinite(Number(latestPrice?.price)) ? Number(latestPrice.price) : null,
    };
  });
}

function summarizeActiveLocks(locks, rows) {
  const companyById = new Map(rows.map((row) => [row.companyId, row]));
  const totals = new Map();
  for (const item of locks) {
    if (String(item?.status ?? "").toLowerCase() !== "locked") {
      continue;
    }
    const companyId = String(item?.lock?.context ?? "").trim();
    if (!companyId) {
      continue;
    }
    const amount = Number(item?.lock?.amount ?? 0);
    totals.set(companyId, (totals.get(companyId) ?? 0) + amount);
  }

  return Array.from(totals.entries())
    .map(([companyId, amount]) => {
      const row = companyById.get(companyId);
      return {
        companyId,
        company: row?.company ?? companyId,
        youLockedValue: amount,
        youLocked: String(amount),
        totalLocked: row?.totalLocked ?? 0,
        performancePct: row?.performancePct ?? 0,
        change: row?.change ?? `${((row?.performancePct ?? 0) * 100).toFixed(2)}%`,
      };
    })
    .sort((left, right) => right.youLockedValue - left.youLockedValue);
}

async function fetchAllocateState(page) {
  const meResult = await fetchJson(page, "/api/hecto/auth/me");
  if (!meResult.ok || !meResult.json?.user?.partyId) {
    throw new Error(`Failed to read auth session: ${meResult.status}`);
  }

  const partyId = meResult.json.user.partyId;
  const tableResult = await fetchJson(page, "/api/allocator/table");
  const companiesResult = await fetchJson(page, "/api/allocator/companies");
  const pricesResult = await fetchJson(page, "/api/prices/latest");
  const locksResult = await fetchJson(page, `/api/locks?userPartyId=${encodeURIComponent(partyId)}`);

  if (!tableResult.ok || !Array.isArray(tableResult.json?.rows)) {
    throw new Error(`Failed to read allocator table: ${tableResult.status}`);
  }
  if (!companiesResult.ok || !Array.isArray(companiesResult.json)) {
    throw new Error(`Failed to read allocator companies: ${companiesResult.status}`);
  }
  if (!pricesResult.ok || !pricesResult.json?.prices || typeof pricesResult.json.prices !== "object") {
    throw new Error(`Failed to read latest prices: ${pricesResult.status}`);
  }
  if (!locksResult.ok || !Array.isArray(locksResult.json?.locks)) {
    throw new Error(`Failed to read locks state: ${locksResult.status}`);
  }

  const rows = mergeBestCompanyData(tableResult.json.rows.map(mapAllocateRow), companiesResult.json, pricesResult.json.prices);
  const activeLockedCompanies = summarizeActiveLocks(locksResult.json.locks, rows);

  return {
    user: meResult.json.user,
    partyId,
    rows,
    rawLocks: locksResult.json.locks,
    activeLockedCompanies,
  };
}

async function ensureAllocateUiReady(page) {
  await page.goto(ALLOCATE_URL, { waitUntil: "domcontentloaded" });
  await page.waitForLoadState("networkidle").catch(() => {});
  await waitForAny(page, [
    'button:has-text("LOCK $HECTO")',
    'button:has-text("UNLOCK $HECTO")',
    'text=/Locking Hecto/i',
    'text=/Balance\\s+[\\d,]+\\s+HECTO/i',
  ], 60_000);
  await page.waitForTimeout(1_000);
}

function chooseBestCompany(rows) {
  return [...rows].sort((left, right) => right.changeValue - left.changeValue)[0] ?? null;
}

function chooseCurrentCompanies(rows) {
  return rows.filter((row) => row.youLockedValue > 0);
}

function nearlyEqual(left, right, epsilon = 0.0001) {
  return Math.abs(left - right) <= epsilon;
}

async function unlockCurrent(page, currentCompany) {
  log(`Attempting unlock for ${currentCompany.company}`);
  await expandCompanyRow(page, currentCompany.company, currentCompany.companyId);
  log(`Expanded ${currentCompany.company} row`);
  let unlockCount = 0;
  let remainingLocked = currentCompany.youLockedValue ?? 0;
  while (true) {
    const buttonText = await waitForUnlockAction(page, currentCompany);
    if (!buttonText) {
      break;
    }
    const clicked = await clickFirstVisibleUnlockButton(page);
    if (!clicked) {
      if (unlockCount > 0) {
        log(`No visible unlock buttons remain for ${currentCompany.company}`);
        break;
      }
      throw new Error(`Unable to click unlock button for ${currentCompany.company}`);
    }
    unlockCount += 1;
    log(`Triggered unlock action for ${currentCompany.company} via ${clicked.mode} (${clicked.text})`);
    await confirmTransaction(page);
    log(`Unlock submitted via ${clicked.mode} (${clicked.text})`);
    const refreshedLock = await waitForUnlockProgress(page, currentCompany, remainingLocked);
    remainingLocked = refreshedLock?.youLockedValue ?? 0;
    if (remainingLocked <= 0) {
      log(`No active lock remains for ${currentCompany.company}`);
      break;
    }
  }
  log(`Finished unlock loop for ${currentCompany.company}; submitted ${unlockCount} unlock(s)`);
}

async function readLockingBalance(page) {
  try {
    const responsePromise = page.waitForResponse((response) => {
      return response.url().includes("api.supanova.app/canton/api/balances") && response.ok();
    }, { timeout: 8_000 });
    await page.reload({ waitUntil: "domcontentloaded" }).catch(() => {});
    await page.waitForLoadState("networkidle").catch(() => {});
    const response = await responsePromise;
    const text = await response.text();
    const json = JSON.parse(text);
    if (json && Array.isArray(json.tokens)) {
      const hectoToken = json.tokens.find((token) => {
        return String(token?.instrumentId?.id ?? "").toUpperCase() === "HECTO";
      });
      if (hectoToken) {
        return parseNumber(hectoToken.totalUnlockedBalance ?? 0);
      }
    }
  } catch {
    // Fall back to visible allocate text if the balance poll is not observed in time.
  }

  const result = await page.evaluate(() => {
    const normalize = (value) => String(value ?? "").replace(/\s+/g, " ").trim();
    return normalize(document.body?.innerText ?? "");
  });

  const match = String(result ?? "").match(/balance\s+([\d.,]+)\s+hecto/i);
  return match ? parseNumber(match[1]) : 0;
}

async function waitForLockingBalance(page, minimum, timeout = 120_000) {
  const started = Date.now();
  while (Date.now() - started < timeout) {
    const balance = await readLockingBalance(page);
    log(`Locking panel balance check: ${balance} HECTO (need ${minimum})`);
    if (balance >= minimum) {
      return balance;
    }
    await page.waitForTimeout(2_000);
    await page.reload({ waitUntil: "domcontentloaded" }).catch(() => {});
    await page.waitForLoadState("networkidle").catch(() => {});
  }
  throw new Error(`Timed out waiting for locking balance >= ${minimum}`);
}

async function waitForTargetLocked(page, targetCompany, minimumAmount, timeout = 90_000) {
  const started = Date.now();
  while (Date.now() - started < timeout) {
    await page.waitForTimeout(4_000);
    await page.reload({ waitUntil: "domcontentloaded" }).catch(() => {});
    await page.waitForLoadState("networkidle").catch(() => {});

    const state = await fetchAllocateState(page);
    const refreshedTarget =
      state.activeLockedCompanies.find((row) => row.companyId === targetCompany.companyId) ??
      state.rows.find((row) => row.companyId === targetCompany.companyId);
    const currentLocked = refreshedTarget?.youLockedValue ?? 0;
    log(`Post-lock check for ${targetCompany.company}: youLocked=${currentLocked}`);
    if (currentLocked >= minimumAmount || currentLocked > 0) {
      return refreshedTarget ?? targetCompany;
    }
  }

  throw new Error(`Timed out waiting for ${targetCompany.company} lock to appear in allocator state`);
}

async function lockIntoBest(page, bestCompany, amountText) {
  log(`Attempting lock into ${bestCompany.company} with amount ${amountText}`);
  await selectCompany(page, bestCompany.company, bestCompany.companyId);
  log(`Selected company ${bestCompany.company}`);
  await setLockAmount(page, amountText);
  log(`Updated lock amount to ${amountText}`);

  const clicked = await invokeVisibleButtonByText(page, [
    `LOCK ${amountText} $HECTO`,
    "LOCK $HECTO",
  ]);
  if (!clicked) {
    throw new Error(`Unable to click lock button for ${bestCompany.company}`);
  }
  log(`Triggered lock action for ${bestCompany.company} via ${clicked.mode}`);
  await confirmTransaction(page);
  log(`Lock submitted via ${clicked.mode}`);
}

async function unlockAllVisible(page) {
  while (true) {
    const button = page.locator("button").filter({ hasText: /^UNLOCK\b/i }).first();
    if (!(await button.count()) || !(await button.isVisible().catch(() => false))) {
      break;
    }

    await button.click();
    await confirmTransaction(page);
    log("Unlock submitted from visible batch row");
    await page.waitForTimeout(5_000);
    await page.reload({ waitUntil: "domcontentloaded" }).catch(() => {});
    await page.waitForLoadState("networkidle").catch(() => {});
  }
}

async function confirmTransaction(page) {
  log("Waiting for sign confirmation modal");
  const started = Date.now();
  let nextDebugAt = 0;
  while (Date.now() - started < 90_000) {
    const pages = page.context().pages();
    for (const candidate of pages) {
      if (await clickIfVisible(candidate, 'button:has-text("Sign & Send")')) {
        log(`Clicked "Sign & Send" on ${candidate.url() || "popup"}`);
        return;
      }
      if (await clickIfVisible(candidate, 'button:has-text("Sign")')) {
        log(`Clicked "Sign" on ${candidate.url() || "popup"}`);
        return;
      }
      if (await clickIfVisible(candidate, 'button:has-text("SIGN")')) {
        log(`Clicked "SIGN" on ${candidate.url() || "popup"}`);
        return;
      }
    }

    if (Date.now() >= nextDebugAt) {
      const urls = pages.map((candidate) => candidate.url() || "about:blank").join(" | ");
      log(`Sign modal not visible yet; open pages=${urls}`);
      nextDebugAt = Date.now() + 5_000;
    }

    await page.waitForTimeout(1_000);
  }

  throw new Error("Timed out waiting for sign confirmation modal");
}

async function clickFirstVisibleUnlockButton(page) {
  const button = page.locator("button").filter({ hasText: /^UNLOCK\b/i }).first();
  if (!(await button.count().catch(() => 0))) {
    return null;
  }

  const text = ((await button.textContent().catch(() => "")) ?? "").replace(/\s+/g, " ").trim();
  await button.scrollIntoViewIfNeeded().catch(() => {});

  try {
    await button.click({ force: true, timeout: 5_000 });
    return {
      mode: "playwright",
      text,
    };
  } catch {
    // Fall back to DOM/React invocation below.
  }

  const result = await button.evaluate((element) => {
    const key = Object.getOwnPropertyNames(element).find((name) => name.startsWith("__reactProps"));
    const props = key ? element[key] : null;
    element.click();
    if (typeof props?.onClick === "function") {
      return { mode: "dom+react-available" };
    }
    return { mode: "dom" };
  }).catch(() => null);

  if (!result?.mode) {
    return null;
  }

  return {
    mode: result.mode,
    text,
  };
}

function findCompanyRow(page, company) {
  return page
    .getByRole("row")
    .filter({ hasText: new RegExp(escapeRegExp(company), "i") })
    .filter({ hasText: /LOCK \$HECTO|UNLOCK \$HECTO/i })
    .first();
}

function findAnyCompanyRow(page, company) {
  return page
    .getByRole("row")
    .filter({ hasText: new RegExp(escapeRegExp(company), "i") })
    .first();
}

async function selectCompany(page, company, companyId = "") {
  if (companyId) {
    await page.goto(`${ALLOCATE_URL}?project=${encodeURIComponent(companyId)}`, {
      waitUntil: "domcontentloaded",
    });
    await page.waitForLoadState("networkidle").catch(() => {});
    await waitForAny(page, [
      `text=/${escapeRegExp(String(company))}/i`,
      'text=/Locking Hecto/i',
      'button:has-text("LOCK $HECTO")',
      'button:has-text("UNLOCK $HECTO")',
    ], 30_000);
    log(`selectCompany(${company}) -> project-query`);
    await page.waitForTimeout(750);
    return;
  }

  const row = findCompanyRow(page, company);
  const hasRow = (await row.count().catch(() => 0)) > 0;
  if (hasRow) {
    await row.scrollIntoViewIfNeeded().catch(() => {});
    const directResult = await row.evaluate((element) => {
      const getReactProps = (entry) => {
        const key = Object.getOwnPropertyNames(entry).find((name) => name.startsWith("__reactProps"));
        return key ? entry[key] : null;
      };

      const props = getReactProps(element);
      if (typeof props?.onClick === "function") {
        props.onClick();
        return { ok: true, mode: "locator-react-row" };
      }

      element.click();
      return { ok: true, mode: "locator-dom-row" };
    });

    if (directResult?.ok) {
      log(`selectCompany(${company}) -> ${directResult.mode}`);
      await page.waitForTimeout(750);
      return;
    }
  }

  const result = await page.evaluate((targetCompany) => {
    const normalize = (value) => String(value ?? "").replace(/\s+/g, " ").trim().toLowerCase();
    const getReactProps = (element) => {
      const key = Object.getOwnPropertyNames(element).find((entry) => entry.startsWith("__reactProps"));
      return key ? element[key] : null;
    };
    const isVisible = (element) => {
      if (!(element instanceof HTMLElement)) {
        return false;
      }
      const style = window.getComputedStyle(element);
      const rect = element.getBoundingClientRect();
      return style.visibility !== "hidden" && style.display !== "none" && rect.width > 0 && rect.height > 0;
    };

    const rows = Array.from(document.querySelectorAll("tr,[role='row']"));
    const wanted = normalize(targetCompany);
    const row = rows.find((entry) => {
      const text = normalize(entry.textContent);
      return text.includes(wanted) && text.includes("lock $hecto");
    });

    if (!row || !isVisible(row)) {
      return { ok: false, reason: "row-not-found" };
    }

    row.scrollIntoView({ behavior: "instant", block: "center" });
    const rowProps = getReactProps(row);
    if (typeof rowProps?.onClick === "function") {
      rowProps.onClick();
      return { ok: true, mode: "react-row" };
    }

    const actionButton = Array.from(row.querySelectorAll("button")).find((button) =>
      normalize(button.textContent).includes("lock $hecto") ||
      normalize(button.textContent).includes("unlock $hecto"),
    );

    if (actionButton && isVisible(actionButton)) {
      const buttonProps = getReactProps(actionButton);
      if (typeof buttonProps?.onClick === "function") {
        buttonProps.onClick({
          preventDefault() {},
          stopPropagation() {},
          currentTarget: actionButton,
          target: actionButton,
        });
        return { ok: true, mode: "react-button" };
      }

      actionButton.click();
      return { ok: true, mode: "dom-button" };
    }

    row.click();
    return { ok: true, mode: "dom-row" };
  }, company);

  if (!result?.ok) {
    throw new Error(`Unable to select company row for ${company}`);
  }
  log(`selectCompany(${company}) -> ${result.mode}`);

  await page.waitForTimeout(750);
}

async function setLockAmount(page, amountText) {
  const result = await page.evaluate((value) => {
    const isVisible = (element) => {
      if (!(element instanceof HTMLElement)) {
        return false;
      }
      const style = window.getComputedStyle(element);
      const rect = element.getBoundingClientRect();
      return style.visibility !== "hidden" && style.display !== "none" && rect.width > 0 && rect.height > 0;
    };
    const getReactProps = (element) => {
      const key = Object.getOwnPropertyNames(element).find((entry) => entry.startsWith("__reactProps"));
      return key ? element[key] : null;
    };

    const input = Array.from(document.querySelectorAll("input")).find((entry) => {
      const element = entry;
      return isVisible(element) && (element.type === "text" || element.type === "" || element.getAttribute("type") === null);
    });

    if (!input) {
      return { ok: false, reason: "input-not-found" };
    }

    const normalizedValue = String(value);
    const prototype = window.HTMLInputElement.prototype;
    const descriptor = Object.getOwnPropertyDescriptor(prototype, "value");
    descriptor?.set?.call(input, "");
    input.dispatchEvent(new Event("input", { bubbles: true }));
    descriptor?.set?.call(input, normalizedValue);
    input.dispatchEvent(new Event("input", { bubbles: true }));
    input.dispatchEvent(new Event("change", { bubbles: true }));
    input.focus();

    const props = getReactProps(input);
    if (typeof props?.onChange === "function") {
      props.onChange({
        target: { value: normalizedValue },
        currentTarget: { value: normalizedValue },
      });
      return { ok: true, mode: "react" };
    }

    return { ok: true, mode: "dom" };
  }, amountText);

  if (!result?.ok) {
    throw new Error(`Unable to set lock amount input to ${amountText}`);
  }
  log(`setLockAmount(${amountText}) -> ${result.mode}`);

  await page.waitForTimeout(750);
}

async function invokeVisibleButtonByText(page, labels) {
  const result = await page.evaluate((wantedLabels) => {
    const normalize = (value) => String(value ?? "").replace(/\s+/g, " ").trim().toLowerCase();
    const normalizedLabels = wantedLabels.map(normalize);
    const getReactProps = (element) => {
      const key = Object.getOwnPropertyNames(element).find((entry) => entry.startsWith("__reactProps"));
      return key ? element[key] : null;
    };
    const isVisible = (element) => {
      if (!(element instanceof HTMLElement)) {
        return false;
      }
      const style = window.getComputedStyle(element);
      const rect = element.getBoundingClientRect();
      return style.visibility !== "hidden" && style.display !== "none" && rect.width > 0 && rect.height > 0;
    };

    const buttons = Array.from(document.querySelectorAll("button"))
      .filter(isVisible)
      .map((button) => ({
        button,
        text: normalize(button.textContent),
        area: button.getBoundingClientRect().width * button.getBoundingClientRect().height,
      }))
      .sort((left, right) => right.area - left.area);

      for (const label of normalizedLabels) {
        const match = buttons.find((entry) => entry.text.includes(label));
        if (match) {
          match.button.scrollIntoView({ behavior: "instant", block: "center" });
          const props = getReactProps(match.button);
          if (typeof props?.onClick === "function") {
            props.onClick({
              preventDefault() {},
              stopPropagation() {},
              currentTarget: match.button,
              target: match.button,
            });
            return { ok: true, mode: "react", text: match.text };
          }

          match.button.click();
          return { ok: true, mode: "dom", text: match.text };
        }
      }

      return { ok: false };
    }, labels);

  return result?.ok ? result : null;
}

async function expandCompanyRow(page, company, companyId = "") {
  await selectCompany(page, company, companyId);
}

async function waitForUnlockAction(page, currentCompany, timeout = 60_000) {
  const started = Date.now();
  let nextDebugAt = 0;
  while (Date.now() - started < timeout) {
    const unlockButton = page.locator("button").filter({ hasText: /^UNLOCK\b/i }).first();
    if ((await unlockButton.count().catch(() => 0)) > 0) {
      const text = ((await unlockButton.textContent().catch(() => "")) ?? "").replace(/\s+/g, " ").trim();
      log(`Unlock action detected for ${currentCompany.company}: ${text}`);
      return text;
    }

    if (Date.now() >= nextDebugAt) {
      const unlockTexts = await page
        .locator("button")
        .evaluateAll((nodes) =>
          nodes
            .map((node) => (node.textContent || "").replace(/\s+/g, " ").trim())
            .filter((text) => /^UNLOCK\b/i.test(text)),
        )
        .catch(() => []);
      log(`Unlock buttons currently detected for ${currentCompany.company}: ${unlockTexts.join(" | ") || "none"}`);
      nextDebugAt = Date.now() + 5_000;
    }

    const row = findAnyCompanyRow(page, currentCompany.company);
    if ((await row.count().catch(() => 0)) > 0) {
      await row.scrollIntoViewIfNeeded().catch(() => {});
      await row.evaluate((element) => {
        const key = Object.getOwnPropertyNames(element).find((name) => name.startsWith("__reactProps"));
        const props = key ? element[key] : null;
        if (typeof props?.onClick === "function") {
          props.onClick();
          return;
        }
        element.click();
      }).catch(() => {});
    }

    await page.waitForTimeout(2_500);
    await page.goto(`${ALLOCATE_URL}?project=${encodeURIComponent(currentCompany.companyId)}`, {
      waitUntil: "domcontentloaded",
    }).catch(() => {});
      await page.waitForLoadState("networkidle").catch(() => {});
    }

  throw new Error(`Timed out waiting for unlock action for ${currentCompany.company}`);
}

async function waitForUnlockProgress(page, currentCompany, previousLockedAmount, timeout = 90_000) {
  const started = Date.now();
  while (Date.now() - started < timeout) {
    await page.waitForTimeout(5_000);
    await page.goto(`${ALLOCATE_URL}?project=${encodeURIComponent(currentCompany.companyId)}`, {
      waitUntil: "domcontentloaded",
    }).catch(() => {});
    await page.waitForLoadState("networkidle").catch(() => {});

    const state = await fetchAllocateState(page);
    const refreshedLock =
      state.activeLockedCompanies.find((row) => row.companyId === currentCompany.companyId) ?? null;
    const currentLocked = refreshedLock?.youLockedValue ?? 0;
    log(`Post-unlock check for ${currentCompany.company}: youLocked=${currentLocked}`);
    if (currentLocked < previousLockedAmount) {
      return refreshedLock;
    }
  }

  throw new Error(`Timed out waiting for ${currentCompany.company} lock amount to decrease after unlock`);
}

function sanitizeFilePart(value) {
  return String(value ?? "")
    .replace(/[^a-z0-9._-]+/gi, "-")
    .replace(/^-+|-+$/g, "")
    .toLowerCase();
}

function hasDisplayServer() {
  return Boolean(process.env.DISPLAY?.trim() || process.env.WAYLAND_DISPLAY?.trim());
}

function resolveHeadlessMode(config) {
  if (Boolean(config.execution.headless)) {
    return true;
  }

  if (process.platform === "linux" && !hasDisplayServer()) {
    log("No DISPLAY/WAYLAND detected on Linux, forcing headless browser mode. Use xvfb-run if you need headed Chrome.");
    return true;
  }

  return false;
}

function getAccountLockKey(account) {
  return sanitizeFilePart(account.profileName || account.name || "account");
}

function getAccountLockPath(account) {
  return path.join(ACCOUNT_LOCKS_DIR, `${getAccountLockKey(account)}.lock`);
}

async function releaseAccountLock(lockPath) {
  try {
    await fs.unlink(lockPath);
  } catch (error) {
    if (error?.code !== "ENOENT") {
      throw error;
    }
  }
}

async function acquireAccountLock(account, config) {
  const lockPath = getAccountLockPath(account);
  const ttlMs = Math.max(60_000, Number(config.execution.accountLockTtlMs ?? 6 * 60 * 60 * 1000));
  const payload = {
    accountName: account.name,
    profileName: account.profileName,
    pid: process.pid,
    acquiredAt: new Date().toISOString(),
  };

  await ensureDir(ACCOUNT_LOCKS_DIR);

  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      const handle = await fs.open(lockPath, "wx");
      await handle.writeFile(`${JSON.stringify(payload, null, 2)}\n`, "utf8");
      await handle.close();
      return {
        acquired: true,
        lockPath,
        release: () => releaseAccountLock(lockPath),
      };
    } catch (error) {
      if (error?.code !== "EEXIST") {
        throw error;
      }

      try {
        const stat = await fs.stat(lockPath);
        const ageMs = Date.now() - stat.mtimeMs;
        if (ageMs > ttlMs) {
          log(`Removing stale account lock for ${account.name} (age ${Math.round(ageMs / 1000)}s)`);
          await releaseAccountLock(lockPath);
          continue;
        }
      } catch (statError) {
        if (statError?.code === "ENOENT") {
          continue;
        }
        throw statError;
      }

      return {
        acquired: false,
        lockPath,
        release: async () => {},
      };
    }
  }

  return {
    acquired: false,
    lockPath,
    release: async () => {},
  };
}

function resolveLockTarget(unlockedBalance, locking) {
  const minimum = Math.max(5_000, parseNumber(locking.minAmount ?? 5_000));
  const amountMode = String(locking.amountMode ?? "value").trim().toLowerCase();
  const configuredValue = parseNumber(locking.amountValue ?? minimum);
  const configuredMax = parseNumber(locking.amountMax ?? 0);

  let target = 0;
  if (amountMode === "max") {
    target = configuredMax > 0 ? Math.min(unlockedBalance, configuredMax) : unlockedBalance;
  } else {
    target = configuredValue;
    if (configuredMax > 0) {
      target = Math.min(target, configuredMax);
    }
  }

  if (target < minimum) {
    return 0;
  }
  return Math.min(target, unlockedBalance);
}

async function runAccountCycle(account, config) {
  const previousPrefix = LOG_PREFIX;
  setLogPrefix(account.name);
  const credentials = {
    email: account.email,
    password: account.password,
    gmailAppPassword: account.gmailAppPassword,
  };
  const headless = resolveHeadlessMode(config);
  const executeActions = Boolean(config.execution.execute);
  const unlockAfterLock = Boolean(config.execution.unlockAfterLock);
  const unlockAllOnly = Boolean(config.execution.unlockAllOnly);
  const browserChannel = config.execution.browserChannel?.trim() || "";
  const profileDir = path.join(process.cwd(), ".profile", account.profileName);

  await ensureDir(path.dirname(profileDir));
  await ensureDir(OUTPUT_DIR);

  const context = await chromium.launchPersistentContext(profileDir, {
    headless,
    viewport: { width: 1440, height: 960 },
    ...(browserChannel ? { channel: browserChannel } : {}),
  });

  const page = context.pages()[0] ?? (await context.newPage());

  try {
    await ensureLoggedIn(page, credentials);
    const allocateState = await fetchAllocateState(page);
    const rows = allocateState.rows;
    if (!rows.length) {
      throw new Error("Allocator API returned no rows");
    }
    const unlockedBalance = await readLockingBalance(page);

    log(`Allocator API rows found: ${rows.length}`);
    for (const row of rows) {
      log(`row -> company=${row.company} 1D=${row.change} youLocked=${row.youLocked} totalLocked=${row.totalLocked}`);
    }
    log(`HECTO unlocked balance=${unlockedBalance}`);

    const bestCompany = chooseBestCompany(rows);
    let currentCompanies = allocateState.activeLockedCompanies;

    if (!bestCompany) {
      throw new Error("Unable to determine best company from allocator API");
    }

    log(`Best company by 1D is ${bestCompany.company} (${bestCompany.change})`);
    if (currentCompanies.length) {
      for (const currentCompany of currentCompanies) {
        log(`Active locked company is ${currentCompany.company} (${currentCompany.youLocked})`);
      }
    } else {
      log("No active locked company detected in locks API");
    }

    if (!executeActions) {
      log("Execution is disabled, so unlock/lock actions are skipped on this run");
    } else {
      await ensureAllocateUiReady(page);

      if (unlockAllOnly) {
        if (!currentCompanies.length) {
          log("Unlock-all mode is enabled, but there are no active locked companies");
        } else {
          for (const currentCompany of currentCompanies) {
            log(`Unlock-all mode: unlocking ${currentCompany.company} (${currentCompany.youLocked})`);
            await unlockCurrent(page, currentCompany);
            await page.waitForTimeout(5_000);
            await ensureAllocateUiReady(page);
          }
        }
        return;
      }

      if (config.locking.unlockAllBeforeLock !== false && currentCompanies.length) {
        for (const currentCompany of currentCompanies) {
          log(`Unlocking ${currentCompany.company} before rebalancing`);
          await unlockCurrent(page, currentCompany);
          await page.waitForTimeout(5_000);
          await ensureAllocateUiReady(page);
        }
        const postUnlockState = await fetchAllocateState(page);
        currentCompanies = postUnlockState.activeLockedCompanies;
        if (currentCompanies.length) {
          throw new Error(`Some companies remain locked after unlock phase: ${currentCompanies.map((item) => item.company).join(", ")}`);
        }
      } else {
        log("No active locks need to be cleared before locking");
      }

      const refreshedUnlockedBalance = await readLockingBalance(page);
      const targetAmount = resolveLockTarget(refreshedUnlockedBalance, account.locking);
      const minimum = Math.max(5_000, parseNumber(account.locking.minAmount ?? 5_000));

      if (targetAmount < minimum) {
        log(`Skipping lock because resolved target amount ${targetAmount} is below minimum ${minimum}`);
      } else {
        log(`Lock target resolved to ${targetAmount} HECTO using mode=${account.locking.amountMode}`);
        await lockIntoBest(page, bestCompany, String(targetAmount));
        const verifiedTarget = await waitForTargetLocked(page, bestCompany, targetAmount);
        if (unlockAfterLock) {
          await ensureAllocateUiReady(page);
          log(`Unlock-after-lock test enabled, unlocking ${verifiedTarget.company}`);
          await unlockCurrent(page, verifiedTarget);
          await page.waitForTimeout(8_000);
          await page.reload({ waitUntil: "domcontentloaded" }).catch(() => {});
          await page.waitForLoadState("networkidle").catch(() => {});
        }
      }
    }

    await page.screenshot({
      path: path.join(OUTPUT_DIR, `${sanitizeFilePart(account.name)}-${Date.now()}.png`),
      fullPage: true,
    });
  } finally {
    if (config.execution.closeBrowser !== false) {
      await context.close();
    }
    setLogPrefix(previousPrefix);
  }
}

async function runAccountsOnce(config, accounts) {
  log(`Starting cycle for ${accounts.length} account(s)`);
  for (let index = 0; index < accounts.length; index += 1) {
    const account = accounts[index];
    const retryOnFailure = config.execution.retryOnFailure !== false;
    const retryDelayMs = Math.max(60_000, Number(config.execution.retryDelayMs ?? 15 * 60 * 1000));
    const maxAttemptsPerCycle = Math.max(1, Number.parseInt(String(config.execution.maxAttemptsPerCycle ?? 2), 10) || 2);
    let completed = false;

    for (let attempt = 1; attempt <= maxAttemptsPerCycle; attempt += 1) {
      const accountLock = await acquireAccountLock(account, config);
      if (!accountLock.acquired) {
        const previousPrefix = LOG_PREFIX;
        setLogPrefix(account.name);
        log(`Skipping account because another process is already handling it (${path.basename(accountLock.lockPath)})`);
        setLogPrefix(previousPrefix);
        break;
      }

      try {
        if (attempt > 1) {
          const previousPrefix = LOG_PREFIX;
          setLogPrefix(account.name);
          log(`Starting retry attempt ${attempt}/${maxAttemptsPerCycle}`);
          setLogPrefix(previousPrefix);
        }
        await runAccountCycle(account, config);
        log(`Cycle finished for ${account.name}`);
        completed = true;
        break;
      } catch (error) {
        const previousPrefix = LOG_PREFIX;
        setLogPrefix(account.name);
        log(`Account cycle failed on attempt ${attempt}/${maxAttemptsPerCycle}: ${error.message}`);
        setLogPrefix(previousPrefix);
      } finally {
        await accountLock.release();
      }

      if (!retryOnFailure || attempt >= maxAttemptsPerCycle) {
        break;
      }

      const previousPrefix = LOG_PREFIX;
      setLogPrefix(account.name);
      log(`Retrying in ${formatDurationMinutes(retryDelayMs)}`);
      setLogPrefix(previousPrefix);
      await sleep(retryDelayMs);
    }

    if (!completed && retryOnFailure && maxAttemptsPerCycle > 1) {
      const previousPrefix = LOG_PREFIX;
      setLogPrefix(account.name);
      log("Account cycle finished without success after retry attempts");
      setLogPrefix(previousPrefix);
    }

    if (index < accounts.length - 1 && config.execution.accountDelayMs > 0) {
      await sleep(config.execution.accountDelayMs);
    }
  }
}

function shouldRunScheduledCycle(config, lastRunDateKey, persistedDateKey) {
  const parts = getZonedParts(new Date(), config.timezone);
  const weekdays = new Set(config.schedule.weekdays);
  const lastCompletedDateKey = config.schedule.dedupeAcrossRestarts === false ? "" : String(persistedDateKey ?? "");
  return (
    config.schedule.enabled &&
    weekdays.has(parts.weekday) &&
    parts.hour === Number(config.schedule.hour) &&
    parts.minute === Number(config.schedule.minute) &&
    parts.dateKey !== lastRunDateKey &&
    parts.dateKey !== lastCompletedDateKey
  )
    ? parts.dateKey
    : null;
}

async function runScheduler(config, accounts) {
  let lastRunDateKey = "";
  const scheduleState = await loadScheduleState();
  let persistedDateKey = String(scheduleState.lastCompletedDateKey ?? "");
  if (persistedDateKey) {
    log(`Loaded scheduler state: lastCompletedDateKey=${persistedDateKey}`);
  }
  if (config.schedule.runOnStart) {
    log("runOnStart is enabled, executing cycle immediately");
    await runAccountsOnce(config, accounts);
    lastRunDateKey = getZonedParts(new Date(), config.timezone).dateKey;
    if (config.schedule.dedupeAcrossRestarts !== false) {
      persistedDateKey = lastRunDateKey;
      await saveScheduleState({ lastCompletedDateKey: persistedDateKey });
    }
  }

  const weekdaysText = config.schedule.weekdays.join(", ");
  log(
    `Scheduler active for timezone=${config.timezone} at ${String(config.schedule.hour).padStart(2, "0")}:${String(
      config.schedule.minute,
    ).padStart(2, "0")} on weekdays=${weekdaysText}`,
  );

  while (true) {
    const runDateKey = shouldRunScheduledCycle(config, lastRunDateKey, persistedDateKey);
    if (runDateKey) {
      log(`Scheduled cycle triggered for ${runDateKey}`);
      await runAccountsOnce(config, accounts);
      lastRunDateKey = runDateKey;
      if (config.schedule.dedupeAcrossRestarts !== false) {
        persistedDateKey = runDateKey;
        await saveScheduleState({ lastCompletedDateKey: persistedDateKey });
      }
    }
    await sleep(config.schedule.pollIntervalMs);
  }
}

async function main() {
  const config = await loadRuntimeConfig();
  const accounts = await loadAccounts(config);
  if (!accounts.length) {
    throw new Error("No enabled accounts configured");
  }

  config.execution.unlockAfterLock =
    /^true$/i.test(String(process.env.HECTO_UNLOCK_AFTER_LOCK ?? config.execution.unlockAfterLock ?? "false"));
  config.execution.unlockAllOnly =
    /^true$/i.test(String(process.env.HECTO_UNLOCK_ALL ?? config.execution.unlockAllOnly ?? "false"));
  config.execution.execute =
    /^true$/i.test(String(process.env.HECTO_EXECUTE ?? config.execution.execute ?? "true"));
  config.execution.headless =
    /^true$/i.test(String(process.env.HECTO_HEADLESS ?? config.execution.headless ?? "false"));
  config.execution.closeBrowser =
    /^true$/i.test(String(process.env.HECTO_CLOSE_BROWSER ?? config.execution.closeBrowser ?? "true"));
  if (process.env.HECTO_BROWSER_CHANNEL?.trim()) {
    config.execution.browserChannel = process.env.HECTO_BROWSER_CHANNEL.trim();
  }

  log(`Loaded ${accounts.length} account(s)`);
  if (!config.schedule.enabled) {
    await runAccountsOnce(config, accounts);
    return;
  }

  await runScheduler(config, accounts);
}

main().catch((error) => {
  console.error(`[${new Date().toISOString()}] fatal: ${error.message}`);
  process.exitCode = 1;
});
