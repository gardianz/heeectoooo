import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { AsyncLocalStorage } from "node:async_hooks";
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
const LOCK_CONTROLLER_CONTRACTS_URL =
  "https://api.supanova.app/canton/api/active_contracts?templateIds=%23hecto-lock-v1%3ALock%3ALockController";
const DASHBOARD_LOG_LIMIT = 18;
let LOG_PREFIX = "";
const logScopeStorage = new AsyncLocalStorage();

const dashboardState = {
  enabled: false,
  initialized: false,
  config: null,
  accounts: [],
  accountRows: new Map(),
  logs: [],
  startedAt: 0,
  cycleNumber: 0,
  cycleStartedAt: 0,
  cycleStatus: "IDLE",
  currentAccountName: "",
  overallStatus: "BOOTING",
  lastSuccessText: "",
  lastErrorText: "",
  renderTimer: null,
  clockTimer: null,
};

function stripAnsi(value) {
  return String(value ?? "").replace(/\x1b\[[0-9;]*m/g, "");
}

function colorize(value, colorCode) {
  if (!dashboardState.enabled || !colorCode) {
    return String(value ?? "");
  }
  return `\x1b[${colorCode}m${value}\x1b[0m`;
}

function fitText(value, width) {
  const normalized = normalizeSpaces(value);
  if (width <= 0) {
    return "";
  }
  if (normalized.length <= width) {
    return normalized;
  }
  if (width <= 3) {
    return normalized.slice(0, width);
  }
  return `${normalized.slice(0, Math.max(0, width - 3))}...`;
}

function padText(value, width, align = "left") {
  const text = fitText(value, width);
  if (align === "right") {
    return text.padStart(width, " ");
  }
  if (align === "center") {
    const totalPadding = Math.max(0, width - text.length);
    const leftPadding = Math.floor(totalPadding / 2);
    const rightPadding = totalPadding - leftPadding;
    return `${" ".repeat(leftPadding)}${text}${" ".repeat(rightPadding)}`;
  }
  return text.padEnd(width, " ");
}

function formatClockDuration(ms) {
  const totalSeconds = Math.max(0, Math.floor(Number(ms ?? 0) / 1000));
  const hours = String(Math.floor(totalSeconds / 3600)).padStart(2, "0");
  const minutes = String(Math.floor((totalSeconds % 3600) / 60)).padStart(2, "0");
  const seconds = String(totalSeconds % 60).padStart(2, "0");
  return `${hours}:${minutes}:${seconds}`;
}

function formatDashboardNumber(value) {
  const numeric = Number(value ?? 0);
  if (!Number.isFinite(numeric)) {
    return "-";
  }
  return new Intl.NumberFormat("en-US", {
    maximumFractionDigits: numeric % 1 === 0 ? 0 : 2,
  }).format(numeric);
}

function formatDashboardPercent(value) {
  const numeric = Number(value ?? 0);
  if (!Number.isFinite(numeric)) {
    return "-";
  }
  const sign = numeric > 0 ? "+" : "";
  return `${sign}${numeric.toFixed(2)}%`;
}

function formatDashboardTimestamp(date, timeZone) {
  const formatter = new Intl.DateTimeFormat("en-GB", {
    timeZone: timeZone || "UTC",
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
    timeZoneName: "short",
  });
  return formatter.format(date).replace(",", "");
}

function createDashboardAccountRow(account) {
  return {
    name: account.name,
    status: "QUEUED",
    attemptText: "0/0",
    balanceText: "-",
    lockText: "-",
    bestText: "-",
    targetText: "-",
    actionText: "Waiting",
    updatedAt: 0,
  };
}

function getDashboardModeText(config) {
  if (!config?.execution?.execute) {
    return "READ-ONLY";
  }
  if (config.execution.unlockAllOnly) {
    return "UNLOCK-ONLY";
  }
  if (config.execution.unlockAfterLock) {
    return "LOCK-TEST";
  }
  return `REBALANCE-${String(config?.locking?.amountMode ?? "value").toUpperCase()}`;
}

function getDashboardSchedulerText(config) {
  if (!config?.schedule?.enabled) {
    return "OFF";
  }
  const hour = String(config.schedule.hour ?? 0).padStart(2, "0");
  const minute = String(config.schedule.minute ?? 0).padStart(2, "0");
  return `${hour}:${minute} ${config.timezone}`;
}

function getDashboardBrowserText(config) {
  const channel = config?.execution?.browserChannel?.trim() || "chromium";
  const mode = config?.execution?.headless ? "headless" : "headed";
  return `${channel}/${mode}`;
}

function getDashboardConcurrencyText(config, accountCount) {
  const limit = resolveMaxConcurrentAccounts(config, accountCount);
  return `${limit}/${accountCount}`;
}

function getStatusColorCode(status) {
  switch (String(status ?? "").toUpperCase()) {
    case "DONE":
      return "32";
    case "RUNNING":
      return "36";
    case "RETRY":
      return "33";
    case "FAILED":
      return "31";
    case "SKIPPED":
      return "35";
    default:
      return "37";
  }
}

function detectLogLevel(message) {
  const normalized = String(message ?? "").toLowerCase();
  if (/fatal|failed|timed out|unable|error/.test(normalized)) {
    return "error";
  }
  if (/retry|skipping|cooldown|waiting/.test(normalized)) {
    return "warn";
  }
  if (/finished|verified|submitted|loaded|resolved|updated|triggered/.test(normalized)) {
    return "success";
  }
  return "info";
}

function getLogColorCode(level) {
  switch (level) {
    case "error":
      return "31";
    case "warn":
      return "33";
    case "success":
      return "32";
    default:
      return "37";
  }
}

function summarizeDashboardCounts() {
  const counts = {
    queued: 0,
    running: 0,
    done: 0,
    retry: 0,
    failed: 0,
    skipped: 0,
  };

  for (const row of dashboardState.accountRows.values()) {
    switch (String(row.status ?? "").toUpperCase()) {
      case "RUNNING":
        counts.running += 1;
        break;
      case "DONE":
        counts.done += 1;
        break;
      case "RETRY":
        counts.retry += 1;
        break;
      case "FAILED":
        counts.failed += 1;
        break;
      case "SKIPPED":
        counts.skipped += 1;
        break;
      default:
        counts.queued += 1;
        break;
    }
  }

  return counts;
}

function buildDashboardTable(columns, rows) {
  const header = columns.map((column) => padText(column.title, column.width, column.align)).join(" | ");
  const separator = columns.map((column) => "-".repeat(column.width)).join("-+-");
  const lines = [header, separator];

  for (const row of rows) {
    const cells = columns.map((column) => {
      const rawValue = typeof column.value === "function" ? column.value(row) : row[column.key];
      const padded = padText(rawValue, column.width, column.align);
      const colorCode = typeof column.color === "function" ? column.color(row, rawValue) : column.color;
      return colorize(padded, colorCode);
    });
    lines.push(cells.join(" | "));
  }

  return lines;
}

function renderDashboardBox(text, innerWidth) {
  return `| ${padText(text, innerWidth)} |`;
}

function renderDashboardAnsiBox(text, innerWidth) {
  const plainLength = stripAnsi(text).length;
  const padded = `${text}${" ".repeat(Math.max(0, innerWidth - plainLength))}`;
  return `| ${padded} |`;
}

function scheduleDashboardRender(force = false) {
  if (!dashboardState.enabled) {
    return;
  }
  if (force) {
    renderDashboard();
    return;
  }
  if (dashboardState.renderTimer) {
    return;
  }
  dashboardState.renderTimer = setTimeout(() => {
    dashboardState.renderTimer = null;
    renderDashboard();
  }, 25);
}

function initializeDashboard(config, accounts) {
  const enabled = process.stdout.isTTY && config?.execution?.dashboard !== false;
  dashboardState.enabled = Boolean(enabled);
  dashboardState.config = config;
  dashboardState.accounts = accounts.map((account) => account.name);
  dashboardState.accountRows = new Map(accounts.map((account) => [account.name, createDashboardAccountRow(account)]));
  dashboardState.logs = [];
  dashboardState.startedAt = Date.now();
  dashboardState.cycleNumber = 0;
  dashboardState.cycleStartedAt = 0;
  dashboardState.cycleStatus = "IDLE";
  dashboardState.currentAccountName = "";
  dashboardState.overallStatus = config?.schedule?.enabled ? "SCHEDULER" : "MANUAL";
  dashboardState.lastSuccessText = "";
  dashboardState.lastErrorText = "";
  dashboardState.initialized = true;

  if (!dashboardState.enabled) {
    return;
  }

  if (dashboardState.clockTimer) {
    clearInterval(dashboardState.clockTimer);
  }
  dashboardState.clockTimer = setInterval(() => {
    scheduleDashboardRender(true);
  }, 1000);
  dashboardState.clockTimer.unref?.();
  scheduleDashboardRender(true);
}

function updateDashboardMeta(updates = {}) {
  Object.assign(dashboardState, updates);
  scheduleDashboardRender();
}

function updateDashboardAccount(accountName, updates = {}) {
  if (!accountName) {
    return;
  }
  const row = dashboardState.accountRows.get(accountName) ?? createDashboardAccountRow({ name: accountName });
  Object.assign(row, updates, { updatedAt: Date.now() });
  dashboardState.accountRows.set(accountName, row);
  scheduleDashboardRender();
}

function updateCurrentDashboardAccount(updates = {}) {
  const prefix = getLogPrefix();
  if (prefix) {
    updateDashboardAccount(prefix, updates);
  }
}

function pushDashboardLog(line) {
  const timestamp = new Date();
  const prefix = getLogPrefix() || "";
  const message = String(line ?? "");
  dashboardState.logs.push({
    timeText: formatDashboardTimestamp(timestamp, dashboardState.config?.timezone ?? "UTC").split(" ")[1] ?? timestamp.toISOString(),
    prefix,
    message,
    level: detectLogLevel(message),
  });
  if (dashboardState.logs.length > DASHBOARD_LOG_LIMIT) {
    dashboardState.logs.splice(0, dashboardState.logs.length - DASHBOARD_LOG_LIMIT);
  }
  scheduleDashboardRender();
}

function startDashboardCycle(accounts, config) {
  dashboardState.cycleNumber += 1;
  dashboardState.cycleStartedAt = Date.now();
  dashboardState.cycleStatus = "RUNNING";
  dashboardState.currentAccountName = "";
  dashboardState.overallStatus = config?.schedule?.enabled ? "SCHEDULER" : "MANUAL";

  for (const account of accounts) {
    const previous = dashboardState.accountRows.get(account.name) ?? createDashboardAccountRow(account);
    dashboardState.accountRows.set(account.name, {
      ...previous,
      status: "QUEUED",
      attemptText: `0/${Math.max(1, Number.parseInt(String(config.execution.maxAttemptsPerCycle ?? 2), 10) || 2)}`,
      balanceText: "-",
      lockText: "-",
      bestText: "-",
      actionText: "Waiting cycle",
      targetText: "-",
      updatedAt: Date.now(),
    });
  }
  scheduleDashboardRender();
}

function finishDashboardCycle() {
  const counts = summarizeDashboardCounts();
  dashboardState.cycleStatus = counts.failed > 0 ? "DONE-WITH-FAILURES" : "DONE";
  dashboardState.currentAccountName = "";
  scheduleDashboardRender();
}

function renderDashboard() {
  if (!dashboardState.enabled) {
    return;
  }

  const width = Math.max(120, Math.min(process.stdout.columns || 160, 180));
  const innerWidth = width - 4;
  const counts = summarizeDashboardCounts();
  const accountRows = dashboardState.accounts.map((name) => dashboardState.accountRows.get(name) ?? createDashboardAccountRow({ name }));
  const activeAccounts = accountRows
    .filter((row) => ["RUNNING", "RETRY"].includes(String(row.status ?? "").toUpperCase()))
    .map((row) => row.name)
    .join(", ");
  const runtimeText = formatClockDuration(Date.now() - dashboardState.startedAt);
  const cycleRuntimeText = dashboardState.cycleStartedAt ? formatClockDuration(Date.now() - dashboardState.cycleStartedAt) : "00:00:00";
  const currentTimeText = formatDashboardTimestamp(new Date(), dashboardState.config?.timezone ?? "UTC");
  const headerText = ` Hecto Finance Bot | ${currentTimeText} | ${dashboardState.accounts.length} akun | Mode: ${getDashboardModeText(dashboardState.config)} `;
  const summaryLine1 = `Runtime: ${runtimeText} | Cycle: #${dashboardState.cycleNumber || 0} ${dashboardState.cycleStatus} (${cycleRuntimeText}) | Scheduler: ${getDashboardSchedulerText(dashboardState.config)} | Browser: ${getDashboardBrowserText(dashboardState.config)}`;
  const summaryLine2 = `Akun: total ${dashboardState.accounts.length} | queued ${counts.queued} | running ${counts.running} | success ${counts.done} | retry ${counts.retry} | fail ${counts.failed} | skipped ${counts.skipped}`;
  const summaryLine3 = `Aktif: ${activeAccounts || "-"} | Paralel: ${getDashboardConcurrencyText(dashboardState.config, dashboardState.accounts.length)} | Last success: ${dashboardState.lastSuccessText || "-"} | Last error: ${dashboardState.lastErrorText || "-"}`;

  let lockWidth = 18;
  let bestWidth = 18;
  const fixedWithoutAction = 14 + 12 + 9 + 12 + lockWidth + bestWidth + 10;
  const separatorWidth = 3 * 7;
  let actionWidth = innerWidth - (fixedWithoutAction + separatorWidth);
  if (actionWidth < 24) {
    lockWidth = 16;
    bestWidth = 16;
    actionWidth = innerWidth - (14 + 12 + 9 + 12 + lockWidth + bestWidth + 10 + separatorWidth);
  }
  actionWidth = Math.max(20, actionWidth);

  const accountColumns = [
    { title: "Akun", key: "name", width: 14 },
    { title: "Status", key: "status", width: 12, color: (row) => getStatusColorCode(row.status) },
    { title: "Attempt", key: "attemptText", width: 9, align: "center" },
    { title: "Unlocked", key: "balanceText", width: 12, align: "right", color: "36" },
    { title: "Current Lock", key: "lockText", width: lockWidth },
    { title: "Best 1D", key: "bestText", width: bestWidth, color: "32" },
    { title: "Target", key: "targetText", width: 10, align: "right", color: "33" },
    { title: "Action", key: "actionText", width: actionWidth },
  ];
  const accountTableLines = buildDashboardTable(accountColumns, accountRows);

  const logLines = dashboardState.logs.length
    ? dashboardState.logs.map((entry) => {
        const scope = entry.prefix ? `${entry.prefix}` : "system";
        const text = `[${entry.timeText}] ${scope} | ${entry.message}`;
        return colorize(fitText(text, innerWidth), getLogColorCode(entry.level));
      })
    : [colorize("Belum ada log eksekusi.", "90")];

  const lines = [];
  const horizontal = "+".padEnd(width - 1, "-") + "+";

  lines.push(horizontal);
  lines.push(renderDashboardBox(headerText, innerWidth));
  lines.push(renderDashboardBox(summaryLine1, innerWidth));
  lines.push(renderDashboardBox(summaryLine2, innerWidth));
  lines.push(renderDashboardBox(summaryLine3, innerWidth));
  lines.push(horizontal);
  lines.push(renderDashboardBox("--- Account Overview ---", innerWidth));
  for (const line of accountTableLines) {
    lines.push(renderDashboardAnsiBox(line, innerWidth));
  }
  lines.push(horizontal);
  lines.push(renderDashboardBox(`--- Execution Logs (last ${dashboardState.logs.length || 0}) ---`, innerWidth));
  for (const line of logLines) {
    lines.push(renderDashboardAnsiBox(line, innerWidth));
  }
  lines.push(horizontal);

  process.stdout.write(`\x1b[2J\x1b[H${lines.join("\n")}`);
}

function log(message) {
  const scopedPrefix = getLogPrefix();
  const prefix = scopedPrefix ? ` [${scopedPrefix}]` : "";
  const line = `[${new Date().toISOString()}]${prefix} ${message}`;
  if (dashboardState.enabled) {
    pushDashboardLog(String(message ?? ""));
    return;
  }
  console.log(line);
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
    .replace(/\.(?=\d{3}\b)/g, "")
    .replace(/,(?=\d{3}\b)/g, "")
    .trim();
  const parsed = Number.parseFloat(cleaned);
  return Number.isFinite(parsed) ? parsed : 0;
}

function normalizePercentageValue(value) {
  const numeric = Number(value ?? 0);
  if (!Number.isFinite(numeric)) {
    return 0;
  }
  return numeric;
}

function normalizeSpaces(value) {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function formatBestCompanyText(company) {
  if (!company?.company) {
    return "-";
  }
  return `${company.company} ${formatDashboardPercent(company.changeValue ?? parseNumber(company.change ?? 0))}`;
}

function formatLockedCompaniesText(companies, hasCurrentDefinedLock = false, definedLockValue = 0) {
  if (Array.isArray(companies) && companies.length) {
    return companies
      .slice(0, 2)
      .map((company) => `${company.company} ${formatDashboardNumber(company.youLockedValue ?? company.youLocked ?? 0)}`)
      .join(", ");
  }
  if (hasCurrentDefinedLock && definedLockValue > 0) {
    return `Defined ${formatDashboardNumber(definedLockValue)}`;
  }
  return "None";
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

function getLogPrefix() {
  return logScopeStorage.getStore()?.prefix ?? LOG_PREFIX;
}

async function withLogPrefix(prefix, handler) {
  return logScopeStorage.run({ prefix: prefix ? String(prefix) : "" }, handler);
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

function resolveMaxConcurrentAccounts(config, accountCount) {
  const raw = Number.parseInt(String(config?.execution?.maxConcurrentAccounts ?? 1), 10);
  if (!Number.isFinite(raw) || raw <= 0) {
    return Math.max(1, Number(accountCount) || 1);
  }
  return Math.max(1, Math.min(Number(accountCount) || 1, raw));
}

function getExecutionSetting(key, fallbackValue) {
  const value = dashboardState.config?.execution?.[key];
  return value ?? fallbackValue;
}

function getRandomInt(minimum, maximum) {
  const min = Math.ceil(Number(minimum ?? 0));
  const max = Math.floor(Number(maximum ?? min));
  if (!Number.isFinite(min) || !Number.isFinite(max)) {
    return 0;
  }
  if (max <= min) {
    return min;
  }
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

async function humanDelay(multiplier = 1) {
  const minMs = Math.max(0, Number(getExecutionSetting("actionDelayMinMs", 250)));
  const maxMs = Math.max(minMs, Number(getExecutionSetting("actionDelayMaxMs", 900)));
  const duration = Math.max(0, Math.round(getRandomInt(minMs, maxMs) * Math.max(0, Number(multiplier ?? 1))));
  if (duration <= 0) {
    return;
  }
  await sleep(duration);
}

function getTypingDelay() {
  return Math.max(20, Number(getExecutionSetting("typingDelayMs", 65)));
}

async function clickLocator(locator, options = {}) {
  const { timeout = 10_000, delayMultiplier = 1 } = options;
  await locator.waitFor({ state: "visible", timeout }).catch(() => {});
  await humanDelay(0.7 * delayMultiplier);
  await locator.click();
  await humanDelay(0.8 * delayMultiplier);
}

async function clearAndType(locator, value, options = {}) {
  const { timeout = 10_000, typingDelay = getTypingDelay(), initialClear = true } = options;
  await locator.waitFor({ state: "visible", timeout }).catch(() => {});
  await humanDelay(0.6);
  await locator.click();
  await humanDelay(0.4);
  if (initialClear) {
    await locator.fill("");
    await humanDelay(0.3);
  }
  await locator.type(String(value ?? ""), { delay: typingDelay });
  await humanDelay(0.5);
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
      dashboard: true,
      closeBrowser: true,
      browserChannel: "chrome",
      maxConcurrentAccounts: 0,
      parallelStartGapMs: 4_000,
      accountDelayMs: 5_000,
      accountLockTtlMs: 6 * 60 * 60 * 1000,
      retryOnFailure: true,
      retryDelayMs: 15 * 60 * 1000,
      maxAttemptsPerCycle: 2,
      actionDelayMinMs: 250,
      actionDelayMaxMs: 900,
      typingDelayMs: 65,
      otpMaxAttempts: 3,
      otpVerificationTimeoutMs: 20_000,
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

async function withInboxClient(email, appPassword, handler) {
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

  try {
    await client.connect();
    const lock = await client.getMailboxLock("INBOX");
    try {
      return await handler(client);
    } finally {
      lock.release();
    }
  } finally {
    await client.logout().catch(() => {});
  }

}

async function getOtpCheckpoint(email, appPassword, earliestTime = Date.now()) {
  return withInboxClient(email, appPassword, async (client) => {
    const since = new Date(Math.max(0, earliestTime - 24 * 60 * 60 * 1000));
    const uids = await client.search({ since }).catch(() => []);
    const lastUid = uids.length ? Math.max(...uids) : 0;
    return { lastUid };
  }).catch(() => ({ lastUid: 0 }));
}

async function waitForOtp(email, appPassword, options = {}) {
  const earliestTime = Number(options.earliestTime ?? Date.now());
  const minUid = Math.max(0, Number(options.minUid ?? 0));
  const usedCodes = options.usedCodes instanceof Set ? options.usedCodes : new Set();
  const pollIntervalMs = Math.max(2_000, Number(getExecutionSetting("otpPollIntervalMs", 4_000)));
  const deadline = Date.now() + 180_000;

  while (Date.now() < deadline) {
    const code = await withInboxClient(email, appPassword, async (client) => {
      const since = new Date(Math.max(0, earliestTime - 5 * 60 * 1000));
      const uids = await client.search({ since });
      const ordered = [...uids]
        .filter((uid) => Number(uid) > minUid)
        .sort((left, right) => right - left);

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
        if (sentAt && sentAt < earliestTime - 30_000) {
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
        if (match && !usedCodes.has(match[1])) {
          return match[1];
        }
      }

      return "";
    }).catch(() => "");

    if (code) {
      return code;
    }

    await sleep(pollIntervalMs);
  }

  throw new Error("Timed out waiting for Privy OTP email");
}

async function fillOtpCode(page, otpCode) {
  const otpInputs = page.locator('input[inputmode="numeric"], input[autocomplete="one-time-code"]');
  const otpCount = await otpInputs.count();
  if (otpCount >= 6) {
    const firstInput = otpInputs.first();
    await firstInput.waitFor({ state: "visible", timeout: 10_000 }).catch(() => {});
    await humanDelay(0.25);
    await firstInput.click();
    await humanDelay(0.15);
    for (let index = 0; index < otpCount; index += 1) {
      await otpInputs.nth(index).fill("").catch(() => {});
    }
    await humanDelay(0.1);
    await page.keyboard.type(otpCode, { delay: getTypingDelay() });
    await humanDelay(0.4);
    const joinedValue = await otpInputs
      .evaluateAll((nodes) => nodes.map((node) => String(node.value ?? "")).join(""))
      .catch(() => "");
    if (joinedValue.slice(0, otpCode.length) !== otpCode) {
      for (let index = 0; index < 6; index += 1) {
        const input = otpInputs.nth(index);
        await input.waitFor({ state: "visible", timeout: 10_000 }).catch(() => {});
        await humanDelay(0.2);
        await input.click().catch(() => {});
        await humanDelay(0.1);
        await input.fill("").catch(() => {});
        await humanDelay(0.1);
        await input.type(String(otpCode[index] ?? ""), { delay: getTypingDelay() });
      }
    }
  } else {
    await humanDelay(0.4);
    await page.keyboard.type(otpCode, { delay: getTypingDelay() });
  }
  await humanDelay(0.5);
}

async function waitForOtpResolution(page, timeout = 20_000) {
  const started = Date.now();
  while (Date.now() - started < timeout) {
    if (!/\/auth(?:$|\?)/.test(page.url())) {
      return true;
    }
    const hasSignPrompt =
      (await isVisible(page, 'button:has-text("Sign")')) ||
      (await isVisible(page, 'button:has-text("SIGN")')) ||
      (await isVisible(page, 'text=/sign message/i'));
    if (hasSignPrompt) {
      return true;
    }
    const hasOtpError =
      (await isVisible(page, 'text=/invalid code/i')) ||
      (await isVisible(page, 'text=/incorrect code/i')) ||
      (await isVisible(page, 'text=/wrong code/i')) ||
      (await isVisible(page, 'text=/expired code/i'));
    if (hasOtpError) {
      return false;
    }
    await page.waitForTimeout(500);
  }
  return false;
}

async function resendOtpCode(page) {
  const resendTargets = [
    'text=/resend code/i',
    'button:has-text("Resend code")',
    'button:has-text("Resend")',
  ];
  for (const target of resendTargets) {
    if (await clickIfVisible(page, target)) {
      log("Requested a new OTP code from Privy");
      return true;
    }
  }
  return false;
}

async function completeOtpChallenge(page, credentials) {
  const maxAttempts = Math.max(1, Number(getExecutionSetting("otpMaxAttempts", 3)));
  const verificationTimeoutMs = Math.max(10_000, Number(getExecutionSetting("otpVerificationTimeoutMs", 20_000)));
  const usedCodes = new Set();
  let checkpoint = credentials.otpCheckpoint ?? (await getOtpCheckpoint(credentials.email, credentials.gmailAppPassword, Date.now()));

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const otpStart = Date.now();
    if (attempt > 1) {
      const resent = await resendOtpCode(page);
      if (!resent) {
        log("Privy resend button not visible, waiting for a fresher OTP email");
      }
      checkpoint = await getOtpCheckpoint(credentials.email, credentials.gmailAppPassword, otpStart);
    }

    updateCurrentDashboardAccount({
      status: "RUNNING",
      actionText: `Waiting OTP ${attempt}/${maxAttempts}`,
    });
    log(`Waiting for OTP from Gmail (attempt ${attempt}/${maxAttempts})`);
    const otpCode = await waitForOtp(credentials.email, credentials.gmailAppPassword, {
      earliestTime: otpStart,
      minUid: checkpoint.lastUid,
      usedCodes,
    });
    usedCodes.add(otpCode);
    log(`OTP received, filling code (attempt ${attempt}/${maxAttempts})`);
    await fillOtpCode(page, otpCode);

    updateCurrentDashboardAccount({
      status: "RUNNING",
      actionText: `Verifying OTP ${attempt}/${maxAttempts}`,
    });
    const resolved = await waitForOtpResolution(page, verificationTimeoutMs);
    if (resolved) {
      return;
    }

    log(`OTP attempt ${attempt}/${maxAttempts} did not advance auth flow`);
  }

  throw new Error("Privy OTP verification did not resolve after multiple attempts");
}

async function clickIfVisible(page, target) {
  const locator = page.locator(target).first();
  if (await locator.count()) {
    if (await locator.isVisible().catch(() => false)) {
      await clickLocator(locator, { timeout: 5_000 });
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
  await humanDelay(0.8);

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
    await clearAndType(page.locator('input[type="email"]').first(), credentials.email);
  } else if (await isVisible(page, 'input[name*="email" i]')) {
    await clearAndType(page.locator('input[name*="email" i]').first(), credentials.email);
  } else if (await isVisible(page, 'input[autocomplete="email"]')) {
    await clearAndType(page.locator('input[autocomplete="email"]').first(), credentials.email);
  } else {
    await clearAndType(page.getByRole("textbox", { name: /email/i }).first(), credentials.email);
  }

  if (await isVisible(page, 'input[type="password"]')) {
    await clearAndType(page.locator('input[type="password"]').first(), credentials.password);
  } else if (await isVisible(page, 'input[name*="password" i]')) {
    await clearAndType(page.locator('input[name*="password" i]').first(), credentials.password);
  } else {
    await clearAndType(page.getByLabel(/password/i).first(), credentials.password);
  }
  await clickLocator(page.getByRole("button", { name: /sign in/i }).first());

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
    await clickLocator(page.getByRole("button", { name: /connect wallet/i }).first());
  }
  if (await isVisible(page, 'button:has-text("Supanova Wallet")')) {
    await clickLocator(page.getByRole("button", { name: /supanova wallet/i }).first());
  }

  await waitForAny(page, [
    'input[placeholder="your@email.com"]',
    'input[placeholder*="@"]',
    'text=/enter confirmation code/i',
  ], 30_000);

  if (await isVisible(page, 'input[placeholder="your@email.com"]')) {
    const emailInput = page.locator('input[placeholder="your@email.com"]').first();
    await clearAndType(emailInput, credentials.email);
    log("Filled Privy email field");
  } else if (await isVisible(page, 'input[placeholder*="@"]')) {
    const emailInput = page.locator('input[placeholder*="@"]').first();
    await clearAndType(emailInput, credentials.email);
    log("Filled Privy email field via fallback selector");
  }

  const otpCheckpoint = await getOtpCheckpoint(credentials.email, credentials.gmailAppPassword, Date.now());
  if (await isVisible(page, 'button:has-text("Submit")')) {
    const submitButton = page.getByRole("button", { name: /^submit$/i }).first();
    await submitButton.waitFor({ state: "visible", timeout: 10_000 });
    await clickLocator(submitButton);
  } else if (await isVisible(page, 'button:has-text("Continue")')) {
    await clickLocator(page.getByRole("button", { name: /continue/i }).first());
  }

  await humanDelay(0.8);
  await completeOtpChallenge(page, {
    ...credentials,
    otpCheckpoint,
  });

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

async function fetchJsonOrNull(page, url) {
  try {
    return await fetchJson(page, url);
  } catch {
    return null;
  }
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
      .map((element) => ({
        text: normalize(element.textContent),
        cells: Array.from(element.querySelectorAll("th,td,[role='columnheader'],[role='cell']"))
          .filter(isVisible)
          .map((cell) => normalize(cell.textContent))
          .filter(Boolean),
      }))
      .filter((entry) => entry.text || entry.cells.length)
      .slice(0, 20);

    const hasUnlockButton = visibleButtons.some((text) => /^unlock\b/i.test(text));
    const hasEditButton = visibleButtons.some((text) => /^edit\b/i.test(text));
    const isLockPending = /locking \$hecto/i.test(bodyText);
    const isUnlockPending = /terminating lock/i.test(bodyText);
    const panelMode = /locking hecto/i.test(bodyText) ? "locking" : /your position/i.test(bodyText) ? "position" : "unknown";
    const definedLockMatch = bodyText.match(/your defined lock\s+([\d.,]+)\s+hecto/i);
    const lockingBalanceMatch = bodyText.match(/locking balance\s+([\d.,]+)\s+hecto/i);
    const totalAllocatedMatch = bodyText.match(/total allocated\s+([\d.,]+)\s+hecto/i);

    return {
      title: document.title,
      url: location.href,
      panelMode,
      hasUnlockButton,
      hasEditButton,
      isLockPending,
      isUnlockPending,
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

async function fetchLockControllerContracts(page) {
  const response = await fetchJsonOrNull(page, LOCK_CONTROLLER_CONTRACTS_URL);
  if (!response?.ok || !Array.isArray(response.json)) {
    return [];
  }

  return response.json
    .map((entry) => {
      const allocations = Array.isArray(entry?.createArgument?.allocations) ? entry.createArgument.allocations : [];
      return {
        contractId: String(entry?.contractId ?? ""),
        owner: String(entry?.createArgument?.owner ?? ""),
        lockedHoldingCid: String(entry?.createArgument?.lockedHoldingCid ?? ""),
        allocations: allocations
          .map((allocation) => ({
            context: String(allocation?.context ?? ""),
            amount: String(allocation?.amount ?? ""),
            amountValue: parseNumber(allocation?.amount ?? 0),
          }))
          .filter((allocation) => allocation.context),
      };
    })
    .filter((entry) => entry.contractId);
}

function summarizeLockControllerAllocations(rows, contracts) {
  const companyById = new Map(rows.map((row) => [String(row.companyId), row]));
  const totals = new Map();

  for (const contract of contracts) {
    for (const allocation of contract.allocations ?? []) {
      if (!allocation?.context) {
        continue;
      }
      totals.set(allocation.context, (totals.get(allocation.context) ?? 0) + parseNumber(allocation.amountValue ?? allocation.amount ?? 0));
    }
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
        change: row?.change ?? `${normalizePercentageValue(row?.performancePct ?? 0).toFixed(2)}%`,
      };
    })
    .sort((left, right) => right.youLockedValue - left.youLockedValue);
}

function summarizeUiTableLocks(rows, uiState) {
  const uiRows = Array.isArray(uiState?.tableRows) ? uiState.tableRows : [];

  return rows
    .map((row) => {
      const normalizedCompany = normalizeSpaces(row.company).toLowerCase();
      const matchedRow = uiRows.find((entry) => normalizeSpaces(entry?.text ?? "").toLowerCase().includes(normalizedCompany));
      if (!matchedRow) {
        return null;
      }

      const candidates = Array.isArray(matchedRow.cells) && matchedRow.cells.length ? [...matchedRow.cells].reverse() : [matchedRow.text];
      const rawValue = candidates.find((candidate) => /^-?$/.test(String(candidate).trim()) || /([\d.,]+)\s*$/.test(String(candidate).trim())) ?? "";
      if (/^-?$/.test(String(rawValue).trim())) {
        return null;
      }

      const match = String(rawValue).match(/([\d.,]+)\s*$/);
      const amount = match ? parseNumber(match[1]) : 0;
      if (amount <= 0) {
        return null;
      }

      return {
        companyId: row.companyId,
        company: row.company,
        youLockedValue: amount,
        youLocked: String(amount),
        totalLocked: row.totalLocked,
        performancePct: row.performancePct,
        change: row.change,
      };
    })
    .filter(Boolean)
    .sort((left, right) => right.youLockedValue - left.youLockedValue);
}

async function inspectAllocateLockState(page, rows = []) {
  const [uiState, lockControllerContracts] = await Promise.all([
    readAllocateUiState(page).catch(() => null),
    fetchLockControllerContracts(page).catch(() => []),
  ]);

  const contractLockedCompanies = summarizeLockControllerAllocations(rows, lockControllerContracts);
  const uiLockedCompanies = summarizeUiTableLocks(rows, uiState);
  const activeLockedCompanies = contractLockedCompanies.length ? contractLockedCompanies : uiLockedCompanies;
  const definedLockValue = Math.max(
    uiState?.definedLockValue ?? 0,
    uiState?.totalAllocatedValue ?? 0,
    activeLockedCompanies.reduce((total, row) => total + parseNumber(row?.youLockedValue ?? 0), 0),
  );
  const hasCurrentDefinedLock =
    Boolean(uiState?.hasUnlockButton) ||
    Boolean(uiState?.panelMode === "position") ||
    definedLockValue > 0 ||
    activeLockedCompanies.length > 0;

  return {
    uiState,
    lockControllerContracts,
    activeLockedCompanies,
    definedLockValue,
    hasCurrentDefinedLock,
  };
}

function mapAllocateRow(row) {
  const allocatorPerformancePct = Number(row.performancePct ?? 0);
  const changeValue = normalizePercentageValue(allocatorPerformancePct);
  return {
    companyId: row.companyId,
    company: row.companyName,
    totalLocked: row.totalLocked,
    performancePct: allocatorPerformancePct,
    change: `${changeValue.toFixed(2)}%`,
    youLockedValue: Number(row.userLocked ?? 0),
    youLocked: String(row.userLocked ?? 0),
    changeValue,
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
    const hasLatestChange = Number.isFinite(latestChangePct) && Math.abs(row.changeValue) <= 0.0001;

    return {
      ...row,
      company: companyNameById.get(companyId) ?? row.company,
      performancePct: hasLatestChange ? latestChangePct : row.performancePct,
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
        change: row?.change ?? `${normalizePercentageValue(row?.performancePct ?? 0).toFixed(2)}%`,
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

  if (!tableResult.ok || !Array.isArray(tableResult.json?.rows)) {
    throw new Error(`Failed to read allocator table: ${tableResult.status}`);
  }
  if (!companiesResult.ok || !Array.isArray(companiesResult.json)) {
    throw new Error(`Failed to read allocator companies: ${companiesResult.status}`);
  }
  if (!pricesResult.ok || !pricesResult.json?.prices || typeof pricesResult.json.prices !== "object") {
    throw new Error(`Failed to read latest prices: ${pricesResult.status}`);
  }

  const rows = mergeBestCompanyData(tableResult.json.rows.map(mapAllocateRow), companiesResult.json, pricesResult.json.prices);

  return {
    user: meResult.json.user,
    partyId,
    rows,
    rawLocks: [],
    activeLockedCompanies: [],
  };
}

async function ensureAllocateUiReady(page) {
  await page.goto(ALLOCATE_URL, { waitUntil: "domcontentloaded" });
  await page.waitForLoadState("networkidle").catch(() => {});
  const started = Date.now();
  while (Date.now() - started < 60_000) {
    const uiState = await readAllocateUiState(page).catch(() => null);
    if (
      uiState &&
      (
        uiState.panelMode !== "unknown" ||
        uiState.visibleButtons.some((text) => /^(lock|unlock|edit)\b/i.test(text)) ||
        /which hectocorn will grow the most today/i.test(uiState.bodyText)
      )
    ) {
      await page.waitForTimeout(1_000);
      return;
    }
    await page.waitForTimeout(500);
  }
  throw new Error("Timed out waiting for allocate UI to become ready");
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

async function unlockCurrent(page, currentCompany = null, rows = []) {
  let currentState = await inspectAllocateLockState(page, rows);
  const unlockLabel = currentCompany?.company || currentState.activeLockedCompanies[0]?.company || "current defined lock";
  const expectedAmount = Math.max(
    parseNumber(currentCompany?.youLockedValue ?? 0),
    parseNumber(currentState.definedLockValue ?? 0),
  );

  if (!currentState.uiState?.hasUnlockButton && !currentState.hasCurrentDefinedLock) {
    log("No active defined lock detected in allocate UI");
    return currentState;
  }

  for (let attempt = 1; attempt <= 3; attempt += 1) {
    const uiState = currentState.uiState ?? {};
    const currentBalance = parseNumber(uiState.lockingBalanceValue ?? 0);
    if (uiState.panelMode === "locking" && !uiState.isUnlockPending && currentBalance >= expectedAmount) {
      updateCurrentDashboardAccount({
        status: "RUNNING",
        balanceText: formatDashboardNumber(currentBalance),
        lockText: "None",
        actionText: "Unlock completed",
      });
      log(`Unlock verified via allocate UI after ${attempt - 1} submission(s); locking balance now ${currentBalance}`);
      return currentState;
    }

    if (!uiState.hasUnlockButton && !currentState.hasCurrentDefinedLock) {
      updateCurrentDashboardAccount({
        status: "RUNNING",
        lockText: "None",
        actionText: "Unlock completed",
      });
      log(`Unlock verified via allocate UI after ${attempt - 1} submission(s); no defined lock remains`);
      return currentState;
    }

    if (!uiState.hasUnlockButton && uiState.isUnlockPending) {
      updateCurrentDashboardAccount({
        status: "RUNNING",
        actionText: `Waiting unlock transition ${attempt}/3`,
      });
      const transition = await waitForUnlockTransition(page, rows, currentState, expectedAmount);
      currentState = transition.state;
      if (transition.unlocked) {
        updateCurrentDashboardAccount({
          status: "RUNNING",
          balanceText: formatDashboardNumber(transition.lockingBalance),
          lockText: "None",
          actionText: "Unlock completed",
        });
        log(`Unlock verified via allocate UI after ${attempt - 1} submission(s); locking balance now ${transition.lockingBalance}`);
        return currentState;
      }
      continue;
    }

    updateCurrentDashboardAccount({
      status: "RUNNING",
      actionText: `Unlock step ${attempt}/3`,
    });
    log(`Attempting unlock for ${unlockLabel} (step ${attempt})`);
    const clicked = await clickFirstVisibleUnlockButton(page);
    if (!clicked) {
      throw new Error(`Unable to click unlock button for ${unlockLabel}`);
    }
    log(`Triggered unlock action for ${unlockLabel} via ${clicked.mode} (${clicked.text})`);
    await confirmTransaction(page);
    log(`Unlock submitted via ${clicked.mode} (${clicked.text})`);

    const transition = await waitForUnlockTransition(page, rows, currentState, expectedAmount);
    currentState = transition.state;
    if (transition.unlocked) {
      updateCurrentDashboardAccount({
        status: "RUNNING",
        balanceText: formatDashboardNumber(transition.lockingBalance),
        lockText: "None",
        actionText: "Unlock completed",
      });
      log(`Unlock verified via allocate UI after ${attempt} submission(s); locking balance now ${transition.lockingBalance}`);
      return currentState;
    }
  }

  throw new Error(`Timed out waiting for ${unlockLabel} to fully unlock from allocate UI`);
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

function describeLockState(state) {
  const uiState = state?.uiState ?? {};
  const contracts = Array.isArray(state?.lockControllerContracts) ? state.lockControllerContracts : [];
  const contractSignature = contracts
    .map((contract) => `${contract.contractId}:${(contract.allocations ?? []).map((entry) => `${entry.context}:${entry.amountValue}`).join(",")}`)
    .join("|");
  const rowSignature = (state?.activeLockedCompanies ?? [])
    .map((entry) => `${entry.companyId}:${entry.youLockedValue}`)
    .join("|");
  return [
    uiState.panelMode ?? "",
    uiState.hasUnlockButton ? "unlock" : "no-unlock",
    uiState.isUnlockPending ? "pending" : "idle",
    parseNumber(uiState.definedLockValue ?? 0),
    rowSignature,
    contractSignature,
  ].join(";");
}

async function waitForUnlockTransition(page, rows = [], previousState = null, expectedBalance = 0, timeout = 120_000) {
  const started = Date.now();
  const previousSignature = describeLockState(previousState);
  updateCurrentDashboardAccount({
    status: "RUNNING",
    actionText: "Waiting unlock confirmation",
  });
  while (Date.now() - started < timeout) {
    await page.waitForTimeout(4_000);
    await page.reload({ waitUntil: "domcontentloaded" }).catch(() => {});
    await page.waitForLoadState("networkidle").catch(() => {});

    const state = await inspectAllocateLockState(page, rows);
    const lockingBalance = Math.max(
      parseNumber(state.uiState?.lockingBalanceValue ?? 0),
      await readLockingBalance(page),
    );
    const signature = describeLockState(state);
    log(
      `Post-unlock check: panel=${state.uiState?.panelMode ?? "unknown"} definedLock=${state.definedLockValue} lockingBalance=${lockingBalance} activeAllocations=${state.activeLockedCompanies.length}`,
    );

    if (
      state.uiState?.panelMode === "locking" &&
      !state.uiState?.isUnlockPending &&
      lockingBalance >= expectedBalance
    ) {
      return {
        state,
        unlocked: true,
        lockingBalance,
      };
    }

    if (!state.uiState?.isUnlockPending && signature !== previousSignature) {
      return {
        state,
        unlocked: false,
        lockingBalance,
      };
    }
  }

  return {
    state: await inspectAllocateLockState(page, rows),
    unlocked: false,
    lockingBalance: 0,
  };
}

async function waitForTargetLocked(page, targetCompany, minimumAmount, rows = [], timeout = 90_000) {
  const started = Date.now();
  updateCurrentDashboardAccount({
    status: "RUNNING",
    actionText: `Verifying ${targetCompany.company}`,
  });
  while (Date.now() - started < timeout) {
    await page.waitForTimeout(4_000);
    await page.reload({ waitUntil: "domcontentloaded" }).catch(() => {});
    await page.waitForLoadState("networkidle").catch(() => {});

    const state = await inspectAllocateLockState(page, rows);
    const refreshedTarget =
      state.activeLockedCompanies.find((row) => row.companyId === targetCompany.companyId) ??
      null;
    const currentLocked = refreshedTarget?.youLockedValue ?? 0;
    log(
      `Post-lock check for ${targetCompany.company}: panel=${state.uiState?.panelMode ?? "unknown"} definedLock=${state.definedLockValue} targetLocked=${currentLocked}`,
    );
    if (currentLocked >= minimumAmount || currentLocked > 0) {
      return refreshedTarget ?? { ...targetCompany, youLockedValue: currentLocked, youLocked: String(currentLocked) };
    }
    if (
      state.uiState?.panelMode === "position" &&
      !state.uiState?.isLockPending &&
      state.definedLockValue >= minimumAmount &&
      (!state.activeLockedCompanies.length || currentLocked > 0)
    ) {
      return {
        ...targetCompany,
        youLockedValue: Math.max(currentLocked, state.definedLockValue),
        youLocked: String(Math.max(currentLocked, state.definedLockValue)),
      };
    }
  }

  throw new Error(`Timed out waiting for ${targetCompany.company} lock to appear in allocate UI state`);
}

async function lockIntoBest(page, bestCompany, amountText) {
  updateCurrentDashboardAccount({
    status: "RUNNING",
    targetText: formatDashboardNumber(amountText),
    actionText: `Locking ${bestCompany.company}`,
  });
  log(`Attempting lock into ${bestCompany.company} with amount ${amountText}`);
  await ensureAllocateUiReady(page);
  await setCompanyLockAmount(page, bestCompany.company, amountText);
  log(`Updated ${bestCompany.company} amount to ${amountText}`);

  const clicked = await clickPrimaryLockButton(page);
  if (!clicked) {
    throw new Error(`Unable to click lock button for ${bestCompany.company}`);
  }
  log(`Triggered lock action for ${bestCompany.company} via ${clicked.mode} (${clicked.text})`);
  updateCurrentDashboardAccount({
    status: "RUNNING",
    actionText: "Waiting signature",
  });
  await confirmTransaction(page);
  updateCurrentDashboardAccount({
    status: "RUNNING",
    actionText: `Lock submitted to ${bestCompany.company}`,
  });
  log(`Lock submitted via ${clicked.mode}`);
}

async function unlockAllVisible(page) {
  while (true) {
    const button = page.locator("button").filter({ hasText: /^UNLOCK\b/i }).first();
    if (!(await button.count()) || !(await button.isVisible().catch(() => false))) {
      break;
    }

    await clickLocator(button, { timeout: 5_000 });
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

async function clickFirstVisibleUnlockButton(page, company = "") {
  return clickVisibleActionButton(page, "unlock", company);
}

function findCompanyRow(page, company) {
  return page
    .getByRole("row")
    .filter({ hasText: new RegExp(escapeRegExp(company), "i") })
    .first();
}

function findAnyCompanyRow(page, company) {
  return page
    .getByRole("row")
    .filter({ hasText: new RegExp(escapeRegExp(company), "i") })
    .first();
}

async function selectCompany(page, company, companyId = "") {
  const row = findCompanyRow(page, company);
  const hasRow = (await row.count().catch(() => 0)) > 0;
  if (hasRow) {
    await row.scrollIntoViewIfNeeded().catch(() => {});
    await humanDelay(0.5);
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
      await humanDelay(0.4);
      await page.waitForTimeout(750);
      return;
    }
  }

  await humanDelay(0.5);
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
      return text.includes(wanted);
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

  if (result?.ok) {
    log(`selectCompany(${company}) -> ${result.mode}`);
    await humanDelay(0.4);
    await page.waitForTimeout(750);
    return;
  }

  if (companyId) {
    await page.goto(`${ALLOCATE_URL}?project=${encodeURIComponent(companyId)}`, {
      waitUntil: "domcontentloaded",
    });
    await page.waitForLoadState("networkidle").catch(() => {});
    await waitForAny(page, [
      `text=/${escapeRegExp(String(company))}/i`,
      'text=/Locking Hecto/i',
      'button:has-text("LOCK")',
      'button:has-text("UNLOCK")',
    ], 30_000);
    log(`selectCompany(${company}) -> project-query`);
    await page.waitForTimeout(750);
    return;
  }

  throw new Error(`Unable to select company row for ${company}`);
}

async function setCompanyLockAmount(page, company, amountText) {
  await humanDelay(0.7);
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

  await humanDelay(0.5);
  await page.waitForTimeout(750);
}

async function clickVisibleActionButton(page, action, company = "") {
  await humanDelay(0.6);
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

  if (result?.ok) {
    await humanDelay(0.5);
    return result;
  }
  return null;
}

async function clickPrimaryLockButton(page) {
  return clickVisibleActionButton(page, "lock");
}

async function invokeVisibleButtonByText(page, labels) {
  await humanDelay(0.6);
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

  if (result?.ok) {
    await humanDelay(0.5);
    return result;
  }
  return null;
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
  updateDashboardMeta({
    currentAccountName: account.name,
    cycleStatus: "RUNNING",
  });
  updateDashboardAccount(account.name, {
    status: "RUNNING",
    actionText: "Opening browser",
  });
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
    updateDashboardAccount(account.name, {
      status: "RUNNING",
      actionText: "Restoring session",
    });
    await ensureLoggedIn(page, credentials);
    updateDashboardAccount(account.name, {
      status: "RUNNING",
      actionText: "Reading allocate state",
    });
    const allocateState = await fetchAllocateState(page);
    const rows = allocateState.rows;
    if (!rows.length) {
      throw new Error("Allocator API returned no rows");
    }
    await ensureAllocateUiReady(page);
    const currentLockState = await inspectAllocateLockState(page, rows);
    const unlockedBalance = await readLockingBalance(page);

    log(`Allocator API rows found: ${rows.length}`);
    for (const row of rows) {
      log(`row -> company=${row.company} 1D=${row.change} youLocked=${row.youLocked} totalLocked=${row.totalLocked}`);
    }
    log(`HECTO unlocked balance=${unlockedBalance}`);

    const bestCompany = chooseBestCompany(rows);
    let currentCompanies = currentLockState.activeLockedCompanies;
    let hasCurrentDefinedLock = currentLockState.hasCurrentDefinedLock;
    let definedLockValue = currentLockState.definedLockValue;

    if (!bestCompany) {
      throw new Error("Unable to determine best company from allocator API");
    }

    log(`Best company by 1D is ${bestCompany.company} (${bestCompany.change})`);
    updateDashboardAccount(account.name, {
      status: "RUNNING",
      balanceText: formatDashboardNumber(unlockedBalance),
      lockText: formatLockedCompaniesText(currentCompanies, hasCurrentDefinedLock, definedLockValue),
      bestText: formatBestCompanyText(bestCompany),
      actionText: executeActions ? "Evaluating rebalance" : "Read-only inspection",
    });
    if (currentCompanies.length) {
      for (const currentCompany of currentCompanies) {
        log(`Active locked company is ${currentCompany.company} (${currentCompany.youLocked})`);
      }
    } else if (hasCurrentDefinedLock) {
      log(`Active defined lock detected in allocate UI (${definedLockValue} HECTO), but company breakdown is not exposed by allocator API`);
    } else {
      log("No active defined lock detected in allocate UI");
    }

    if (!executeActions) {
      updateDashboardAccount(account.name, {
        status: "DONE",
        actionText: "Read-only completed",
      });
      log("Execution is disabled, so unlock/lock actions are skipped on this run");
    } else {
      if (unlockAllOnly) {
        if (!hasCurrentDefinedLock) {
          updateDashboardAccount(account.name, {
            status: "DONE",
            actionText: "No lock to unlock",
            lockText: "None",
          });
          log("Unlock-all mode is enabled, but there is no active defined lock");
        } else {
          updateDashboardAccount(account.name, {
            status: "RUNNING",
            actionText: "Unlock-only mode",
          });
          const unlockedState = await unlockCurrent(page, currentCompanies[0] ?? null, rows);
          currentCompanies = unlockedState.activeLockedCompanies;
          hasCurrentDefinedLock = unlockedState.hasCurrentDefinedLock;
          definedLockValue = unlockedState.definedLockValue;
          updateDashboardAccount(account.name, {
            status: "DONE",
            balanceText: formatDashboardNumber(unlockedState.uiState?.lockingBalanceValue ?? 0),
            lockText: formatLockedCompaniesText(currentCompanies, hasCurrentDefinedLock, definedLockValue),
            actionText: "Unlock-only completed",
          });
        }
        return;
      }

      if (config.locking.unlockAllBeforeLock !== false && hasCurrentDefinedLock) {
        updateDashboardAccount(account.name, {
          status: "RUNNING",
          actionText: "Unlocking current lock",
        });
        log("Unlocking current defined lock before rebalancing");
        const unlockedState = await unlockCurrent(page, currentCompanies[0] ?? null, rows);
        currentCompanies = unlockedState.activeLockedCompanies;
        hasCurrentDefinedLock = unlockedState.hasCurrentDefinedLock;
        definedLockValue = unlockedState.definedLockValue;
        updateDashboardAccount(account.name, {
          status: "RUNNING",
          balanceText: formatDashboardNumber(unlockedState.uiState?.lockingBalanceValue ?? 0),
          lockText: formatLockedCompaniesText(currentCompanies, hasCurrentDefinedLock, definedLockValue),
          actionText: "Unlock verified",
        });
        if (hasCurrentDefinedLock) {
          const labels = currentCompanies.length
            ? currentCompanies.map((item) => item.company).join(", ")
            : `remaining defined lock ${definedLockValue}`;
          throw new Error(`Some HECTO still appear locked after unlock phase: ${labels}`);
        }
      } else {
        log("No active defined lock needs to be cleared before locking");
      }

      const refreshedUnlockedBalance = await readLockingBalance(page);
      const targetAmount = resolveLockTarget(refreshedUnlockedBalance, account.locking);
      const minimum = Math.max(5_000, parseNumber(account.locking.minAmount ?? 5_000));
      updateDashboardAccount(account.name, {
        status: "RUNNING",
        balanceText: formatDashboardNumber(refreshedUnlockedBalance),
        targetText: targetAmount > 0 ? formatDashboardNumber(targetAmount) : "-",
      });

      if (targetAmount < minimum) {
        updateDashboardAccount(account.name, {
          status: "DONE",
          actionText: `Skipped: below minimum ${minimum}`,
        });
        log(`Skipping lock because resolved target amount ${targetAmount} is below minimum ${minimum}`);
      } else {
        log(`Lock target resolved to ${targetAmount} HECTO using mode=${account.locking.amountMode}`);
        await lockIntoBest(page, bestCompany, String(targetAmount));
        const verifiedTarget = await waitForTargetLocked(page, bestCompany, targetAmount, rows);
        updateDashboardAccount(account.name, {
          status: "DONE",
          balanceText: "0",
          lockText: `${verifiedTarget.company} ${formatDashboardNumber(verifiedTarget.youLockedValue ?? targetAmount)}`,
          targetText: formatDashboardNumber(targetAmount),
          bestText: formatBestCompanyText(bestCompany),
          actionText: `Locked to ${verifiedTarget.company}`,
        });
        if (unlockAfterLock) {
          await ensureAllocateUiReady(page);
          log(`Unlock-after-lock test enabled, unlocking ${verifiedTarget.company || "current defined lock"}`);
          updateDashboardAccount(account.name, {
            status: "RUNNING",
            actionText: "Unlock-after-lock test",
          });
          await unlockCurrent(page, verifiedTarget, rows);
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
  }
}

async function waitForParallelStartSlot(config, account, index, cycleStartedAt, concurrency) {
  const gapMs = Math.max(0, Number(config.execution.parallelStartGapMs ?? 0));
  if (concurrency <= 1 || gapMs <= 0 || index <= 0) {
    return;
  }

  const scheduledStartAt = cycleStartedAt + index * gapMs;
  const remainingMs = scheduledStartAt - Date.now();
  if (remainingMs <= 0) {
    return;
  }

  updateDashboardAccount(account.name, {
    status: "QUEUED",
    actionText: `Stagger start ${Math.ceil(remainingMs / 1000)}s`,
  });
  await sleep(remainingMs);
}

async function runAccountWithRetries(account, config, options = {}) {
  const retryOnFailure = config.execution.retryOnFailure !== false;
  const retryDelayMs = Math.max(60_000, Number(config.execution.retryDelayMs ?? 15 * 60 * 1000));
  const maxAttemptsPerCycle = Math.max(1, Number.parseInt(String(config.execution.maxAttemptsPerCycle ?? 2), 10) || 2);
  const concurrency = Math.max(1, Number(options.concurrency ?? 1));
  const cycleStartedAt = Number(options.cycleStartedAt ?? Date.now());
  const accountIndex = Number(options.accountIndex ?? 0);
  let completed = false;

  return withLogPrefix(account.name, async () => {
    await waitForParallelStartSlot(config, account, accountIndex, cycleStartedAt, concurrency);

    for (let attempt = 1; attempt <= maxAttemptsPerCycle; attempt += 1) {
      updateDashboardMeta({
        currentAccountName: account.name,
      });
      updateDashboardAccount(account.name, {
        status: "RUNNING",
        attemptText: `${attempt}/${maxAttemptsPerCycle}`,
        actionText: attempt > 1 ? `Retry attempt ${attempt}/${maxAttemptsPerCycle}` : "Starting account cycle",
      });
      const accountLock = await acquireAccountLock(account, config);
      if (!accountLock.acquired) {
        updateDashboardAccount(account.name, {
          status: "SKIPPED",
          actionText: "Skipped: lockfile active",
        });
        log(`Skipping account because another process is already handling it (${path.basename(accountLock.lockPath)})`);
        return;
      }

      try {
        if (attempt > 1) {
          log(`Starting retry attempt ${attempt}/${maxAttemptsPerCycle}`);
        }
        await runAccountCycle(account, config);
        const completedRow = dashboardState.accountRows.get(account.name);
        updateDashboardAccount(account.name, {
          status: "DONE",
          attemptText: `${attempt}/${maxAttemptsPerCycle}`,
          actionText: "Cycle finished",
        });
        updateDashboardMeta({
          lastSuccessText: completedRow?.lockText && completedRow.lockText !== "-"
            ? `${account.name} -> ${completedRow.lockText}`
            : `${account.name} completed`,
        });
        log(`Cycle finished for ${account.name}`);
        completed = true;
        return;
      } catch (error) {
        updateDashboardAccount(account.name, {
          status: "FAILED",
          attemptText: `${attempt}/${maxAttemptsPerCycle}`,
          actionText: fitText(error.message, 48),
        });
        updateDashboardMeta({
          lastErrorText: `${account.name}: ${fitText(error.message, 56)}`,
        });
        log(`Account cycle failed on attempt ${attempt}/${maxAttemptsPerCycle}: ${error.message}`);
      } finally {
        await accountLock.release();
      }

      if (!retryOnFailure || attempt >= maxAttemptsPerCycle) {
        break;
      }

      updateDashboardAccount(account.name, {
        status: "RETRY",
        attemptText: `${attempt}/${maxAttemptsPerCycle}`,
        actionText: `Retrying in ${formatDurationMinutes(retryDelayMs)}`,
      });
      log(`Retrying in ${formatDurationMinutes(retryDelayMs)}`);
      await sleep(retryDelayMs);
    }

    if (!completed && retryOnFailure && maxAttemptsPerCycle > 1) {
      updateDashboardAccount(account.name, {
        status: "FAILED",
        actionText: "Retry attempts exhausted",
      });
      log("Account cycle finished without success after retry attempts");
    }
  });
}

async function runAccountsOnce(config, accounts) {
  startDashboardCycle(accounts, config);
  const concurrency = resolveMaxConcurrentAccounts(config, accounts.length);
  log(`Starting cycle for ${accounts.length} account(s) with concurrency ${concurrency}/${accounts.length}`);

  if (concurrency <= 1) {
    for (let index = 0; index < accounts.length; index += 1) {
      await runAccountWithRetries(accounts[index], config, {
        accountIndex: index,
        concurrency,
        cycleStartedAt: dashboardState.cycleStartedAt,
      });

      if (index < accounts.length - 1 && config.execution.accountDelayMs > 0) {
        updateDashboardMeta({
          currentAccountName: "",
        });
        await sleep(config.execution.accountDelayMs);
      }
    }
    finishDashboardCycle();
    return;
  }

  let nextIndex = 0;
  const workerCount = Math.min(concurrency, accounts.length);
  const workers = Array.from({ length: workerCount }, async () => {
    while (true) {
      const accountIndex = nextIndex;
      nextIndex += 1;
      if (accountIndex >= accounts.length) {
        return;
      }

      await runAccountWithRetries(accounts[accountIndex], config, {
        accountIndex,
        concurrency,
        cycleStartedAt: dashboardState.cycleStartedAt,
      });
    }
  });

  await Promise.all(workers);
  updateDashboardMeta({
    currentAccountName: "",
  });
  finishDashboardCycle();
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
  updateDashboardMeta({
    overallStatus: "SCHEDULER",
    cycleStatus: "WAITING",
  });
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
      updateDashboardMeta({
        cycleStatus: "TRIGGERED",
      });
      log(`Scheduled cycle triggered for ${runDateKey}`);
      await runAccountsOnce(config, accounts);
      lastRunDateKey = runDateKey;
      if (config.schedule.dedupeAcrossRestarts !== false) {
        persistedDateKey = runDateKey;
        await saveScheduleState({ lastCompletedDateKey: persistedDateKey });
      }
      updateDashboardMeta({
        cycleStatus: "WAITING",
      });
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
  if (process.env.HECTO_DASHBOARD?.trim()) {
    config.execution.dashboard = /^true$/i.test(process.env.HECTO_DASHBOARD.trim());
  }
  config.execution.closeBrowser =
    /^true$/i.test(String(process.env.HECTO_CLOSE_BROWSER ?? config.execution.closeBrowser ?? "true"));
  if (process.env.HECTO_BROWSER_CHANNEL?.trim()) {
    config.execution.browserChannel = process.env.HECTO_BROWSER_CHANNEL.trim();
  }
  if (process.env.HECTO_MAX_CONCURRENT_ACCOUNTS?.trim()) {
    config.execution.maxConcurrentAccounts = Number.parseInt(process.env.HECTO_MAX_CONCURRENT_ACCOUNTS.trim(), 10);
  }
  if (process.env.HECTO_PARALLEL_START_GAP_MS?.trim()) {
    config.execution.parallelStartGapMs = Number.parseInt(process.env.HECTO_PARALLEL_START_GAP_MS.trim(), 10);
  }
  if (process.env.HECTO_ACTION_DELAY_MIN_MS?.trim()) {
    config.execution.actionDelayMinMs = Number.parseInt(process.env.HECTO_ACTION_DELAY_MIN_MS.trim(), 10);
  }
  if (process.env.HECTO_ACTION_DELAY_MAX_MS?.trim()) {
    config.execution.actionDelayMaxMs = Number.parseInt(process.env.HECTO_ACTION_DELAY_MAX_MS.trim(), 10);
  }
  if (process.env.HECTO_TYPING_DELAY_MS?.trim()) {
    config.execution.typingDelayMs = Number.parseInt(process.env.HECTO_TYPING_DELAY_MS.trim(), 10);
  }
  if (process.env.HECTO_OTP_MAX_ATTEMPTS?.trim()) {
    config.execution.otpMaxAttempts = Number.parseInt(process.env.HECTO_OTP_MAX_ATTEMPTS.trim(), 10);
  }
  if (process.env.HECTO_OTP_VERIFICATION_TIMEOUT_MS?.trim()) {
    config.execution.otpVerificationTimeoutMs = Number.parseInt(process.env.HECTO_OTP_VERIFICATION_TIMEOUT_MS.trim(), 10);
  }

  initializeDashboard(config, accounts);
  log(`Loaded ${accounts.length} account(s)`);
  if (!config.schedule.enabled) {
    await runAccountsOnce(config, accounts);
    return;
  }

  await runScheduler(config, accounts);
}

main().catch((error) => {
  if (dashboardState.enabled) {
    updateDashboardMeta({
      cycleStatus: "FATAL",
      lastErrorText: fitText(error.message, 72),
    });
    pushDashboardLog(`fatal: ${error.message}`);
    scheduleDashboardRender(true);
  } else {
    console.error(`[${new Date().toISOString()}] fatal: ${error.message}`);
  }
  process.exitCode = 1;
});
