import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { chromium } from "playwright";
import { ImapFlow } from "imapflow";

const APP_URL = "https://app.hecto.finance";
const AUTH_URL = `${APP_URL}/auth`;
const ALLOCATE_URL = `${APP_URL}/allocate`;
const ACCOUNTS_PATH = path.join(process.cwd(), "accounts.json");
const OUTPUT_DIR = path.join(process.cwd(), "output", "playwright");
const TARGET_ACCOUNT_NAME = process.env.HECTO_ACCOUNT_NAME?.trim() || "account-1";
const TRACE_POLL_ATTEMPTS = 6;
const TRACE_POLL_INTERVAL_MS = 5_000;

function log(message) {
  console.log(`[${new Date().toISOString()}] ${message}`);
}

function parseNumber(value) {
  if (typeof value === "number") {
    return value;
  }
  const cleaned = String(value ?? "")
    .replace(/[%,$]/g, "")
    .replace(/\s+/g, "")
    .replace(/\.(?=\d{3}\b)/g, "")
    .replace(/,(?=\d{3}\b)/g, "")
    .trim();
  const parsed = Number.parseFloat(cleaned);
  return Number.isFinite(parsed) ? parsed : 0;
}

function normalizePercentageValue(value) {
  const numeric = Number(value ?? 0);
  return Number.isFinite(numeric) ? numeric : 0;
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
    return;
  }

  await waitForAny(page, [
    'button:has-text("Supanova Wallet")',
    'button:has-text("CONNECT WALLET")',
    'input[placeholder="your@email.com"]',
    'text=/enter confirmation code/i',
  ], 30_000);

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
    await page.getByRole("button", { name: /^submit$/i }).first().click();
  } else if (await isVisible(page, 'button:has-text("Continue")')) {
    await page.getByRole("button", { name: /continue/i }).click();
  }

  const otpCode = await waitForOtp(credentials.email, credentials.gmailAppPassword, Date.now());
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

  if (!(await clickIfVisible(page, 'button:has-text("Sign")'))) {
    await clickIfVisible(page, 'button:has-text("SIGN")');
  }

  await page.waitForURL((url) => !/\/auth(?:$|\?)/.test(url.toString()), { timeout: 90_000 });
}

function createNetworkRecorder(page) {
  const events = [];
  let currentPhase = "startup";
  const startTime = Date.now();

  const shouldCapture = (url) =>
    url.includes("app.hecto.finance/api/") || url.includes("api.supanova.app/canton/api/");

  page.on("response", async (response) => {
    const url = response.url();
    if (!shouldCapture(url)) {
      return;
    }

    const request = response.request();
    const resourceType = request.resourceType();
    if (!["fetch", "xhr", "document"].includes(resourceType)) {
      return;
    }

    const entry = {
      atMs: Date.now() - startTime,
      phase: currentPhase,
      method: request.method(),
      url,
      status: response.status(),
      resourceType,
      requestPostData: request.postData() ?? "",
      responseText: "",
      responseJsonShape: null,
    };

    try {
      const text = await response.text();
      entry.responseText = text.slice(0, 10_000);
      try {
        entry.responseJsonShape = summarizeJsonShape(JSON.parse(text));
      } catch {
        entry.responseJsonShape = null;
      }
    } catch {
      entry.responseText = "";
      entry.responseJsonShape = null;
    }

    events.push(entry);
  });

  return {
    events,
    setPhase(phase) {
      currentPhase = phase;
      log(`Phase -> ${phase}`);
    },
  };
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

async function captureApiSnapshot(page) {
  const meResult = await fetchJson(page, "/api/hecto/auth/me");
  const partyId = meResult.json?.user?.partyId ? String(meResult.json.user.partyId) : "";
  const tableResult = await fetchJson(page, "/api/allocator/table");
  const companiesResult = await fetchJson(page, "/api/allocator/companies");
  const pricesResult = await fetchJson(page, "/api/prices/latest");
  const locksResult = partyId
    ? await fetchJson(page, `/api/locks?userPartyId=${encodeURIComponent(partyId)}`)
    : null;

  return {
    me: {
      ok: meResult.ok,
      status: meResult.status,
      shape: summarizeJsonShape(meResult.json),
      partyId,
    },
    allocatorTable: {
      ok: tableResult.ok,
      status: tableResult.status,
      rowCount: Array.isArray(tableResult.json?.rows) ? tableResult.json.rows.length : 0,
      firstRows: Array.isArray(tableResult.json?.rows)
        ? tableResult.json.rows.slice(0, 6).map((row) => ({
            companyId: row.companyId,
            companyName: row.companyName,
            performancePct: row.performancePct,
            userLocked: row.userLocked,
            totalLocked: row.totalLocked,
          }))
        : [],
      raw: tableResult.json,
    },
    companies: {
      ok: companiesResult.ok,
      status: companiesResult.status,
      count: Array.isArray(companiesResult.json) ? companiesResult.json.length : 0,
      raw: companiesResult.json,
    },
    prices: {
      ok: pricesResult.ok,
      status: pricesResult.status,
      count: pricesResult.json?.prices ? Object.keys(pricesResult.json.prices).length : 0,
      raw: pricesResult.json,
    },
    locks: {
      ok: locksResult?.ok ?? false,
      status: locksResult?.status ?? 0,
      count: Array.isArray(locksResult?.json?.locks) ? locksResult.json.locks.length : 0,
      activeLocked: Array.isArray(locksResult?.json?.locks)
        ? locksResult.json.locks
            .filter((entry) => String(entry?.status ?? "").toLowerCase() === "locked")
            .map((entry) => ({
              amount: entry.lock?.amount,
              context: entry.lock?.context,
              userNextAction: entry.userNextAction,
              serverNextAction: entry.serverNextAction,
            }))
        : [],
      raw: locksResult?.json ?? null,
    },
  };
}

async function readAllocateUiState(page) {
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

    const bodyText = normalize(document.body?.innerText ?? "");
    const visibleButtons = Array.from(document.querySelectorAll("button"))
      .filter(isVisible)
      .map((element) => normalize(element.textContent))
      .filter(Boolean);

    const tableRows = Array.from(document.querySelectorAll("tr,[role='row']"))
      .filter(isVisible)
      .map((element) => normalize(element.textContent))
      .filter(Boolean)
      .slice(0, 20);

    const hasUnlockButton = visibleButtons.some((text) => /^unlock\b/i.test(text));
    const hasEditButton = visibleButtons.some((text) => /^edit\b/i.test(text));
    const panelMode = /your position/i.test(bodyText) && hasUnlockButton ? "position" : /locking hecto/i.test(bodyText) ? "locking" : "unknown";
    const definedLockMatch = bodyText.match(/your defined lock\s+([\d.,]+)\s+hecto/i);
    const lockingBalanceMatch = bodyText.match(/locking balance\s+([\d.,]+)\s+hecto/i);
    const totalAllocatedMatch = bodyText.match(/total allocated\s+([\d.,]+)\s+hecto/i);

    return {
      title: document.title,
      url: location.href,
      panelMode,
      hasUnlockButton,
      hasEditButton,
      definedLockText: definedLockMatch?.[1] ?? "",
      definedLockValue: definedLockMatch ? Number(definedLockMatch[1].replace(/[.,](?=\d{3}\b)/g, "")) : 0,
      lockingBalanceText: lockingBalanceMatch?.[1] ?? "",
      lockingBalanceValue: lockingBalanceMatch ? Number(lockingBalanceMatch[1].replace(/[.,](?=\d{3}\b)/g, "")) : 0,
      totalAllocatedText: totalAllocatedMatch?.[1] ?? "",
      totalAllocatedValue: totalAllocatedMatch ? Number(totalAllocatedMatch[1].replace(/[.,](?=\d{3}\b)/g, "")) : 0,
      visibleButtons,
      tableRows,
      bodyText: bodyText.slice(0, 6_000),
    };
  });
}

function chooseBestCompany(rows) {
  return [...rows]
    .map((row) => ({
      companyId: row.companyId,
      company: row.companyName,
      changeValue: normalizePercentageValue(row.performancePct ?? 0),
    }))
    .sort((left, right) => right.changeValue - left.changeValue)[0] ?? null;
}

async function setCompanyLockAmount(page, company, amountText) {
  const result = await page.evaluate(({ targetCompany, value }) => {
    const normalize = (entry) => String(entry ?? "").replace(/\s+/g, " ").trim().toLowerCase();
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
    const findContextScore = (element, wantedText) => {
      let node = element;
      let bestScore = Number.POSITIVE_INFINITY;
      let matchedText = "";
      while (node instanceof HTMLElement) {
        const text = normalize(node.textContent);
        if (text && text.includes(wantedText) && text.length < bestScore) {
          bestScore = text.length;
          matchedText = text;
        }
        node = node.parentElement;
      }
      return {
        score: bestScore,
        matchedText,
      };
    };

    const wantedCompany = normalize(targetCompany);
    const inputs = Array.from(document.querySelectorAll("input[type='number'], input[inputmode='numeric'], input[inputmode='decimal']"))
      .filter(isVisible)
      .map((input) => {
        const context = findContextScore(input, wantedCompany);
        const rect = input.getBoundingClientRect();
        return {
          input,
          score: context.score,
          matchedText: context.matchedText,
          area: rect.width * rect.height,
        };
      })
      .sort((left, right) => {
        if (left.score !== right.score) {
          return left.score - right.score;
        }
        return right.area - left.area;
      });

    const match = inputs.find((entry) => Number.isFinite(entry.score)) ?? null;
    if (!match?.input) {
      return {
        ok: false,
        reason: "company-input-not-found",
        inspectedInputs: inputs.slice(0, 6).map((entry) => ({
          score: entry.score,
          matchedText: entry.matchedText,
        })),
      };
    }

    const input = match.input;
    const normalizedValue = String(value).trim();
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
      return {
        ok: true,
        mode: "react",
        matchedText: match.matchedText,
      };
    }

    return {
      ok: true,
      mode: "dom",
      matchedText: match.matchedText,
    };
  }, { targetCompany: company, value: amountText });

  if (!result?.ok) {
    throw new Error(`Unable to set ${company} lock amount input to ${amountText}`);
  }
  log(`setCompanyLockAmount(${company}, ${amountText}) -> ${result.mode}`);
  return result;
}

async function clickVisibleActionButton(page, action, company = "") {
  const result = await page.evaluate(({ wantedAction, wantedCompany }) => {
    const normalize = (value) => String(value ?? "").replace(/\s+/g, " ").trim().toLowerCase();
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
    const findContextScore = (element, targetCompany) => {
      if (!targetCompany) {
        return Number.POSITIVE_INFINITY;
      }
      let node = element;
      let bestScore = Number.POSITIVE_INFINITY;
      while (node instanceof HTMLElement) {
        const text = normalize(node.textContent);
        if (text && text.includes(targetCompany) && text.length < bestScore) {
          bestScore = text.length;
        }
        node = node.parentElement;
      }
      return bestScore;
    };

    const normalizedAction = normalize(wantedAction);
    const normalizedCompany = normalize(wantedCompany);
    const buttons = Array.from(document.querySelectorAll("button"))
      .filter(isVisible)
      .map((button) => {
        const text = normalize(button.textContent);
        const rect = button.getBoundingClientRect();
        return {
          button,
          text,
          disabled: button.disabled || button.getAttribute("aria-disabled") === "true",
          area: rect.width * rect.height,
          contextScore: findContextScore(button, normalizedCompany),
        };
      })
      .filter((entry) => {
        if (!entry.text.startsWith(normalizedAction)) {
          return false;
        }
        return normalizedAction === "unlock" || entry.text.includes("$hecto");
      })
      .sort((left, right) => {
        const leftHasContext = Number.isFinite(left.contextScore);
        const rightHasContext = Number.isFinite(right.contextScore);
        if (leftHasContext !== rightHasContext) {
          return leftHasContext ? -1 : 1;
        }
        if (left.contextScore !== right.contextScore) {
          return left.contextScore - right.contextScore;
        }
        return right.area - left.area;
      });

    const match = buttons.find((entry) => !entry.disabled) ?? null;
    if (!match) {
      return {
        ok: false,
        reason: "button-not-found",
        action: normalizedAction,
        buttons: buttons.map((entry) => entry.text).slice(0, 10),
      };
    }

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
  }, { wantedAction: action, wantedCompany: company });

  if (!result?.ok) {
    throw new Error(`Unable to click ${action} button${company ? ` for ${company}` : ""}`);
  }
  log(`clickVisibleActionButton(${action}, ${company || "-"}) -> ${result.mode} (${result.text})`);
  return result;
}

async function confirmTransaction(page) {
  log("Waiting for sign confirmation modal");
  const started = Date.now();
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
    await page.waitForTimeout(1_000);
  }
  throw new Error("Timed out waiting for sign confirmation modal");
}

async function saveScreenshot(page, label) {
  const filePath = path.join(OUTPUT_DIR, `trace-${Date.now()}-${sanitizeFilePart(label)}.png`);
  await page.screenshot({
    path: filePath,
    fullPage: true,
  });
  return filePath;
}

async function sampleState(page, label, report) {
  const ui = await readAllocateUiState(page);
  const apis = await captureApiSnapshot(page);
  const screenshotPath = await saveScreenshot(page, label);
  const sample = {
    label,
    at: new Date().toISOString(),
    ui,
    apis,
    screenshotPath,
  };
  report.samples.push(sample);
  log(`${label}: panel=${ui.panelMode} definedLock=${ui.definedLockText || "-"} lockingBalance=${ui.lockingBalanceText || "-"}`);
  return sample;
}

async function waitBetweenSamples(page, labelPrefix, report, options = {}) {
  const { attempts = TRACE_POLL_ATTEMPTS, intervalMs = TRACE_POLL_INTERVAL_MS, reloadEachAttempt = true } = options;
  for (let index = 0; index < attempts; index += 1) {
    if (reloadEachAttempt) {
      await page.reload({ waitUntil: "domcontentloaded" }).catch(() => {});
      await page.waitForLoadState("networkidle").catch(() => {});
      await page.waitForTimeout(1_500);
    } else {
      await page.waitForTimeout(intervalMs);
    }
    await sampleState(page, `${labelPrefix}-${index + 1}`, report);
    if (!reloadEachAttempt) {
      await page.waitForTimeout(intervalMs);
    }
  }
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
  const context = await chromium.launchPersistentContext(profileDir, {
    headless: false,
    viewport: { width: 1440, height: 960 },
    channel: "chrome",
  });

  const page = context.pages()[0] ?? (await context.newPage());
  const network = createNetworkRecorder(page);
  const report = {
    generatedAt: new Date().toISOString(),
    account: {
      name: account.name,
      profileName,
    },
    actions: [],
    samples: [],
    networkEvents: network.events,
  };
  const stamp = Date.now();
  const reportPath = path.join(OUTPUT_DIR, `trace-lock-cycle-${stamp}.json`);

  try {
    await ensureLoggedIn(page, {
      email: account.email,
      password: account.password,
      gmailAppPassword: account.gmailAppPassword,
    });

    network.setPhase("initial-load");
    await page.goto(ALLOCATE_URL, { waitUntil: "domcontentloaded" });
    await page.waitForLoadState("networkidle").catch(() => {});
    await page.waitForTimeout(2_000);

    const initial = await sampleState(page, "initial", report);
    const bestCompany = chooseBestCompany(initial.apis.allocatorTable.firstRows);
    if (!bestCompany) {
      throw new Error("Unable to determine best company from allocator table");
    }
    report.bestCompany = bestCompany;
    log(`Best company from allocator table is ${bestCompany.company} (${bestCompany.changeValue.toFixed(2)}%)`);

    const initialDefinedLock = Math.max(
      parseNumber(initial.ui.definedLockText),
      parseNumber(initial.ui.totalAllocatedText),
      parseNumber(initial.apis.allocatorTable.firstRows.find((row) => Number(row.userLocked) > 0)?.userLocked ?? 0),
    );
    report.initialDefinedLock = initialDefinedLock;

    if (initial.ui.hasUnlockButton) {
      network.setPhase("unlock");
      report.actions.push({
        type: "unlock",
        companyContext: bestCompany.company,
        amountHint: initialDefinedLock,
      });
      await clickVisibleActionButton(page, "unlock");
      await confirmTransaction(page);
      await sampleState(page, "after-unlock-submit", report);
      await waitBetweenSamples(page, "after-unlock-reload", report, {
        attempts: 4,
        intervalMs: TRACE_POLL_INTERVAL_MS,
        reloadEachAttempt: true,
      });
    } else {
      log("Initial state has no unlock button; skipping unlock step");
    }

    network.setPhase("prepare-lock");
    await page.goto(ALLOCATE_URL, { waitUntil: "domcontentloaded" });
    await page.waitForLoadState("networkidle").catch(() => {});
    await page.waitForTimeout(2_000);

    const preLock = await sampleState(page, "before-lock", report);
    const lockAmount = Math.max(
      parseNumber(preLock.ui.lockingBalanceText),
      parseNumber(preLock.ui.totalAllocatedText),
      initialDefinedLock,
    );
    if (lockAmount <= 0) {
      throw new Error("Lock amount resolved to 0 after unlock step");
    }
    report.lockAmount = lockAmount;
    log(`Resolved lock amount for relock: ${lockAmount}`);

    network.setPhase("lock");
    report.actions.push({
      type: "lock",
      company: bestCompany.company,
      companyId: bestCompany.companyId,
      amount: lockAmount,
    });
    await setCompanyLockAmount(page, bestCompany.company, String(lockAmount));
    await clickVisibleActionButton(page, "lock", bestCompany.company);
    await confirmTransaction(page);
    await sampleState(page, "after-lock-submit", report);
    await waitBetweenSamples(page, "after-lock-reload", report, {
      attempts: 6,
      intervalMs: TRACE_POLL_INTERVAL_MS,
      reloadEachAttempt: true,
    });

    const finalSample = report.samples[report.samples.length - 1];
    report.finalSummary = {
      uiPanelMode: finalSample?.ui?.panelMode ?? "",
      definedLockText: finalSample?.ui?.definedLockText ?? "",
      lockingBalanceText: finalSample?.ui?.lockingBalanceText ?? "",
      allocatorRows: finalSample?.apis?.allocatorTable?.firstRows ?? [],
      activeLocked: finalSample?.apis?.locks?.activeLocked ?? [],
    };
  } catch (error) {
    report.error = {
      message: error.message,
      stack: error.stack,
    };
    throw error;
  } finally {
    await fs.writeFile(reportPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
    log(`Trace report saved to ${reportPath}`);
    await context.close();
  }
}

main().catch((error) => {
  console.error(`[${new Date().toISOString()}] fatal: ${error.message}`);
  process.exitCode = 1;
});
