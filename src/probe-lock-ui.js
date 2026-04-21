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
const PROBE_AMOUNT = String(process.env.HECTO_PROBE_AMOUNT?.trim() || "5000");

function log(message) {
  console.log(`[${new Date().toISOString()}] ${message}`);
}

async function readJsonFile(filePath) {
  const text = await fs.readFile(filePath, "utf8");
  return JSON.parse(text);
}

async function ensureDir(dir) {
  await fs.mkdir(dir, { recursive: true });
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

async function fetchJson(page, url) {
  return page.evaluate(async (resource) => {
    const response = await fetch(resource, { credentials: "include" });
    return response.json();
  }, url);
}

function normalizePercentageValue(value) {
  const numeric = Number(value ?? 0);
  if (!Number.isFinite(numeric)) {
    return 0;
  }
  return numeric;
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
  return page.evaluate(({ targetCompany, value }) => {
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
    }

    const visibleButtons = Array.from(document.querySelectorAll("button"))
      .filter(isVisible)
      .map((button) => String(button.textContent ?? "").replace(/\s+/g, " ").trim());

    const totalAllocatedText =
      Array.from(document.querySelectorAll("body *"))
        .filter(isVisible)
        .map((element) => String(element.textContent ?? "").replace(/\s+/g, " ").trim())
        .find((text) => /total allocated/i.test(text) && /hecto/i.test(text)) ?? "";

    return {
      ok: true,
      matchedText: match.matchedText,
      visibleButtons,
      totalAllocatedText,
    };
  }, { targetCompany: company, value: amountText });
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

  try {
    await ensureLoggedIn(page, {
      email: account.email,
      password: account.password,
      gmailAppPassword: account.gmailAppPassword,
    });

    const table = await fetchJson(page, "/api/allocator/table");
    const rows = Array.isArray(table?.rows) ? table.rows : [];
    const bestCompany = chooseBestCompany(rows);
    if (!bestCompany) {
      throw new Error("Unable to determine best company from allocator table");
    }

    log(`Best company by allocator table: ${bestCompany.company} (${bestCompany.changeValue.toFixed(2)}%)`);

    await page.goto(ALLOCATE_URL, { waitUntil: "domcontentloaded" });
    await page.waitForLoadState("networkidle").catch(() => {});
    await page.waitForTimeout(1_500);

    const probe = await setCompanyLockAmount(page, bestCompany.company, PROBE_AMOUNT);
    if (!probe?.ok) {
      throw new Error(`Probe failed: ${JSON.stringify(probe)}`);
    }

    const screenshotPath = path.join(OUTPUT_DIR, `probe-lock-ui-${Date.now()}.png`);
    await page.screenshot({
      path: screenshotPath,
      fullPage: true,
    });

    log(`Matched input context: ${probe.matchedText}`);
    log(`Visible buttons after fill: ${probe.visibleButtons.join(" | ")}`);
    log(`Total allocated text after fill: ${probe.totalAllocatedText || "-"}`);
    log(`Screenshot saved to ${screenshotPath}`);
  } finally {
    await context.close();
  }
}

main().catch((error) => {
  console.error(`[${new Date().toISOString()}] fatal: ${error.message}`);
  process.exitCode = 1;
});
