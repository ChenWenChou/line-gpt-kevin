import express from "express";
import line from "@line/bot-sdk";
import OpenAI from "openai";
// 求籤
import fs from "fs";
import path from "path";

// 星座 會用到 Redis  資料庫
import Redis from "ioredis";

const REDIS_URL = process.env.REDIS_URL;
const REDIS_ERROR_COOLDOWN_MS = Number(
  process.env.REDIS_ERROR_COOLDOWN_MS || 30000
);
let redisUnavailableUntil = 0;
let redisLastErrorAt = 0;
const redisClient = REDIS_URL
  ? new Redis(REDIS_URL, {
      lazyConnect: true,
      enableOfflineQueue: false,
      maxRetriesPerRequest: 1,
      connectTimeout: Number(process.env.REDIS_CONNECT_TIMEOUT_MS || 1500),
      retryStrategy(times) {
        return Math.min(times * 200, 2000);
      },
    })
  : null;

if (redisClient) {
  redisClient.on("error", (err) => {
    const now = Date.now();
    if (now - redisLastErrorAt > 10000) {
      console.error("[redis] connection error:", err?.message || err);
      redisLastErrorAt = now;
    }
  });
}

function markRedisUnavailable(err, hint = "operation failed") {
  redisUnavailableUntil = Date.now() + REDIS_ERROR_COOLDOWN_MS;
  const now = Date.now();
  if (now - redisLastErrorAt > 10000) {
    console.error(`[redis] ${hint}:`, err?.message || err);
    redisLastErrorAt = now;
  }
}

async function ensureRedisReady() {
  if (!redisClient) return false;
  if (Date.now() < redisUnavailableUntil) return false;

  if (redisClient.status === "ready") return true;
  if (redisClient.status === "connect") return true;
  if (redisClient.status === "connecting") return true;
  if (redisClient.status === "reconnecting") return false;

  try {
    await redisClient.connect();
    return true;
  } catch (err) {
    const msg = String(err?.message || err || "");
    if (/already\s+(connecting|connected)/i.test(msg)) {
      return true;
    }
    markRedisUnavailable(err, "connect failed");
    return false;
  }
}

async function redisGet(key) {
  if (!(await ensureRedisReady())) return null;
  try {
    return await redisClient.get(key);
  } catch (err) {
    markRedisUnavailable(err, `GET failed (${key})`);
    return null;
  }
}

async function redisSet(key, value, ...args) {
  if (!(await ensureRedisReady())) return false;
  try {
    await redisClient.set(key, value, ...args);
    return true;
  } catch (err) {
    markRedisUnavailable(err, `SET failed (${key})`);
    return false;
  }
}

async function redisSetNx(key, value, ttlSeconds) {
  if (!(await ensureRedisReady())) return null;
  try {
    const ttl =
      Number.isFinite(ttlSeconds) && ttlSeconds > 0 ? ttlSeconds : 10 * 60;
    const result = await redisClient.set(key, value, "EX", ttl, "NX");
    return result === "OK";
  } catch (err) {
    markRedisUnavailable(err, `SET NX failed (${key})`);
    return null;
  }
}

async function redisZAdd(key, score, member) {
  if (!(await ensureRedisReady())) return false;
  try {
    await redisClient.zadd(key, score, member);
    return true;
  } catch (err) {
    markRedisUnavailable(err, `ZADD failed (${key})`);
    return false;
  }
}

async function redisZRangeByScore(key, min, max, limit = 20) {
  if (!(await ensureRedisReady())) return [];
  try {
    const safeLimit =
      Number.isFinite(limit) && limit > 0 ? Math.min(limit, 200) : 20;
    const result = await redisClient.zrangebyscore(
      key,
      min,
      max,
      "LIMIT",
      0,
      safeLimit
    );
    return Array.isArray(result) ? result : [];
  } catch (err) {
    markRedisUnavailable(err, `ZRANGEBYSCORE failed (${key})`);
    return [];
  }
}

async function redisZRem(key, member) {
  if (!(await ensureRedisReady())) return 0;
  try {
    const removed = await redisClient.zrem(key, member);
    return Number.isFinite(removed) ? removed : 0;
  } catch (err) {
    markRedisUnavailable(err, `ZREM failed (${key})`);
    return 0;
  }
}

async function redisSAdd(key, member) {
  if (!(await ensureRedisReady())) return false;
  try {
    await redisClient.sadd(key, member);
    return true;
  } catch (err) {
    markRedisUnavailable(err, `SADD failed (${key})`);
    return false;
  }
}

async function redisSMembers(key) {
  if (!(await ensureRedisReady())) return [];
  try {
    const members = await redisClient.smembers(key);
    return Array.isArray(members) ? members : [];
  } catch (err) {
    markRedisUnavailable(err, `SMEMBERS failed (${key})`);
    return [];
  }
}

async function redisRPush(key, value) {
  if (!(await ensureRedisReady())) return false;
  try {
    await redisClient.rpush(key, value);
    return true;
  } catch (err) {
    markRedisUnavailable(err, `RPUSH failed (${key})`);
    return false;
  }
}

async function redisLRange(key, start, stop) {
  if (!(await ensureRedisReady())) return [];
  try {
    const values = await redisClient.lrange(key, start, stop);
    return Array.isArray(values) ? values : [];
  } catch (err) {
    markRedisUnavailable(err, `LRANGE failed (${key})`);
    return [];
  }
}

async function redisLTrim(key, start, stop) {
  if (!(await ensureRedisReady())) return false;
  try {
    await redisClient.ltrim(key, start, stop);
    return true;
  } catch (err) {
    markRedisUnavailable(err, `LTRIM failed (${key})`);
    return false;
  }
}

async function redisExpire(key, ttlSeconds) {
  if (!(await ensureRedisReady())) return false;
  try {
    await redisClient.expire(key, ttlSeconds);
    return true;
  } catch (err) {
    markRedisUnavailable(err, `EXPIRE failed (${key})`);
    return false;
  }
}

async function redisDel(key) {
  if (!(await ensureRedisReady())) return 0;
  try {
    const deleted = await redisClient.del(key);
    return Number.isFinite(deleted) ? deleted : 0;
  } catch (err) {
    markRedisUnavailable(err, `DEL failed (${key})`);
    return 0;
  }
}

const LINE_TEXT_LIMIT = 5000;
const LINE_SKIP_REDELIVERY = !/^(0|false|no)$/i.test(
  process.env.LINE_SKIP_REDELIVERY || "true"
);
const LINE_EVENT_DEDUP_TTL_SECONDS = Number(
  process.env.LINE_EVENT_DEDUP_TTL_SECONDS || 10 * 60
);
const lineEventMemoryDedup = new Map();

function normalizeLineMessage(msg) {
  if (!msg || typeof msg !== "object") return msg;
  if (msg.type !== "text" || typeof msg.text !== "string") return msg;
  if (msg.text.length <= LINE_TEXT_LIMIT) return msg;
  return {
    ...msg,
    text: `${msg.text.slice(0, LINE_TEXT_LIMIT - 3)}...`,
  };
}

function normalizeLineMessages(messages) {
  if (Array.isArray(messages)) {
    return messages.slice(0, 5).map(normalizeLineMessage);
  }
  return normalizeLineMessage(messages);
}

function getLineErrorData(err) {
  return err?.originalError?.response?.data || err?.response?.data || null;
}

function isInvalidReplyTokenError(err) {
  if (Number(err?.statusCode) !== 400) return false;
  const data = getLineErrorData(err);
  const detailText = Array.isArray(data?.details)
    ? data.details.map((d) => d?.message || "").join(" ")
    : "";
  const text = `${data?.message || ""} ${detailText}`.toLowerCase();
  return text.includes("reply token");
}

function getLinePushTargetId(event) {
  return (
    event?.source?.userId || event?.source?.groupId || event?.source?.roomId || null
  );
}

function getLineEventDedupKey(event) {
  if (event?.webhookEventId) return `line:event:${event.webhookEventId}`;

  const sourceId =
    event?.source?.userId || event?.source?.groupId || event?.source?.roomId;
  const messageId = event?.message?.id;
  const timestamp = event?.timestamp;
  if (!sourceId || !messageId || !timestamp) return null;
  return `line:event:fallback:${sourceId}:${messageId}:${timestamp}`;
}

function cleanupLineEventMemoryDedup(now = Date.now()) {
  for (const [key, expiresAt] of lineEventMemoryDedup.entries()) {
    if (expiresAt <= now) lineEventMemoryDedup.delete(key);
  }
}

async function shouldProcessLineEvent(event) {
  const key = getLineEventDedupKey(event);
  if (!key) return true;

  const ttl =
    Number.isFinite(LINE_EVENT_DEDUP_TTL_SECONDS) &&
    LINE_EVENT_DEDUP_TTL_SECONDS > 0
      ? LINE_EVENT_DEDUP_TTL_SECONDS
      : 10 * 60;
  const now = Date.now();

  cleanupLineEventMemoryDedup(now);
  const inMemoryUntil = lineEventMemoryDedup.get(key);
  if (inMemoryUntil && inMemoryUntil > now) return false;

  const redisAcquired = await redisSetNx(key, "1", ttl);
  if (redisAcquired === false) return false;

  lineEventMemoryDedup.set(key, now + ttl * 1000);
  return true;
}

async function replyMessageWithFallback(event, messages) {
  const safeMessages = normalizeLineMessages(messages);

  try {
    await client.replyMessage(event.replyToken, safeMessages);
    return;
  } catch (err) {
    const data = getLineErrorData(err);
    console.error("LINE replyMessage failed:", {
      statusCode: err?.statusCode,
      statusMessage: err?.statusMessage,
      data,
    });

    if (isInvalidReplyTokenError(err)) {
      if (event?.deliveryContext?.isRedelivery) {
        console.warn(
          "reply token invalid on redelivery event, skip push fallback"
        );
        return;
      }
      const targetId = getLinePushTargetId(event);
      if (targetId) {
        try {
          await client.pushMessage(targetId, safeMessages);
          console.warn("reply token invalid, fallback to pushMessage success");
          return;
        } catch (pushErr) {
          console.error("pushMessage fallback failed:", {
            statusCode: pushErr?.statusCode,
            statusMessage: pushErr?.statusMessage,
            data: getLineErrorData(pushErr),
          });
        }
      }
      console.warn("reply token invalid, no push fallback target");
      return;
    }

    throw err;
  }
}

function isReplyEvent(target) {
  return Boolean(
    target &&
      typeof target === "object" &&
      typeof target.replyToken === "string"
  );
}

async function sendLineReply(target, messages) {
  const safeMessages = normalizeLineMessages(messages);
  if (isReplyEvent(target)) {
    return replyMessageWithFallback(target, safeMessages);
  }
  return client.replyMessage(target, safeMessages);
}

const __dirname = new URL(".", import.meta.url).pathname;
const mazuLots = JSON.parse(
  fs.readFileSync(path.join(__dirname, "mazu_lots.json"), "utf8")
);

const BOT_USER_ID = "U51d2392e43f851607a191adb3ec49b26";
const app = express();

app.use(express.static("public"));

// LINE 設定
const config = {
  channelAccessToken: process.env.LINE_TOKEN,
  channelSecret: process.env.LINE_SECRET,
};

const client = new line.Client(config);

// OpenAI
const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

const OPENCLAW_CHAT_URL = process.env.OPENCLAW_CHAT_URL;
const OPENCLAW_API_KEY = process.env.OPENCLAW_API_KEY;
const OPENCLAW_MODEL_SIMPLE =
  process.env.OPENCLAW_MODEL_SIMPLE || "openai/gpt-4o-mini";
const OPENCLAW_MODEL_COMPLEX =
  process.env.OPENCLAW_MODEL_COMPLEX ||
  process.env.OPENCLAW_MODEL ||
  "openai/gpt-5-mini";
const OPENCLAW_ROUTE_ENABLED = !/^(0|false|no)$/i.test(
  process.env.OPENCLAW_ROUTE_ENABLED || "true"
);
const OPENCLAW_COMPLEX_TEXT_LENGTH = Number(
  process.env.OPENCLAW_COMPLEX_TEXT_LENGTH || 120
);
const OPENCLAW_TIMEOUT_MS_SIMPLE = Number(
  process.env.OPENCLAW_TIMEOUT_MS_SIMPLE || 12000
);
const OPENCLAW_TIMEOUT_MS = Number(process.env.OPENCLAW_TIMEOUT_MS || 20000);
const OPENCLAW_RETRY_MODEL = process.env.OPENCLAW_RETRY_MODEL || "";
const OPENCLAW_RETRY_TIMEOUT_MS = Number(
  process.env.OPENCLAW_RETRY_TIMEOUT_MS || 12000
);
const OPENCLAW_REQUEST_CONTENT_TYPE =
  process.env.OPENCLAW_REQUEST_CONTENT_TYPE ||
  (OPENCLAW_CHAT_URL?.includes(".up.railway.app")
    ? "text/plain"
    : "application/json");
const OPENCLAW_FORCE_ONLY = /^(1|true|yes)$/i.test(
  process.env.OPENCLAW_FORCE_ONLY || ""
);

const WHEN_LABEL = {
  today: "今日",
  tomorrow: "明日",
  day_after: "後天",
};

const TW_CITY_MAP = {
  台北: "Taipei",
  臺北: "Taipei",
  新北: "New Taipei",
  台中: "Taichung",
  臺中: "Taichung",
  台南: "Tainan",
  臺南: "Tainan",
  高雄: "Kaohsiung",
  桃園: "Taoyuan",
  新竹: "Hsinchu",
  嘉義: "Chiayi",
  宜蘭: "Yilan",
  花蓮: "Hualien",
  台東: "Taitung",
  臺東: "Taitung",
};
const WEATHER_CONTEXT_TTL_SECONDS = Number(
  process.env.WEATHER_CONTEXT_TTL_SECONDS || 60 * 60 * 24
);
const CHAT_HISTORY_ROUNDS = Number(process.env.CHAT_HISTORY_ROUNDS || 6);
const CHAT_HISTORY_TTL_SECONDS = Number(
  process.env.CHAT_HISTORY_TTL_SECONDS || 45 * 60
);
const CHAT_HISTORY_MAX_CHARS = Number(
  process.env.CHAT_HISTORY_MAX_CHARS || 500
);
const GENERAL_REPLY_MAX_CHARS = Number(
  process.env.GENERAL_REPLY_MAX_CHARS || 260
);
const REMINDER_QUEUE_KEY = "reminder:due";
const REMINDER_ITEM_PREFIX = "reminder:item:";
const REMINDER_TTL_SECONDS = Number(
  process.env.REMINDER_TTL_SECONDS || 60 * 60 * 24 * 7
);
const REMINDER_MAX_TEXT_CHARS = Number(
  process.env.REMINDER_MAX_TEXT_CHARS || 80
);
const REMINDER_SCAN_BATCH = Number(process.env.REMINDER_SCAN_BATCH || 20);
const REMINDER_MAX_RETRIES = Number(process.env.REMINDER_MAX_RETRIES || 3);
const REMINDER_RETRY_DELAY_SECONDS = Number(
  process.env.REMINDER_RETRY_DELAY_SECONDS || 60
);
const TAIPEI_UTC_OFFSET_MINUTES = Number(
  process.env.TAIPEI_UTC_OFFSET_MINUTES || 8 * 60
);
const localChatHistory = new Map();

function getWeatherContextKey(userId) {
  if (!userId) return null;
  return `weather:last:${userId}`;
}

async function getLastWeatherContext(userId) {
  const key = getWeatherContextKey(userId);
  if (!key) return null;

  try {
    const raw = await redisGet(key);
    if (!raw) return null;
    return JSON.parse(raw);
  } catch (err) {
    console.error("getLastWeatherContext error:", err);
    return null;
  }
}

async function setLastWeatherContext(userId, payload) {
  const key = getWeatherContextKey(userId);
  if (!key || !payload) return;

  try {
    await redisSet(
      key,
      JSON.stringify(payload),
      "EX",
      Number.isFinite(WEATHER_CONTEXT_TTL_SECONDS) &&
        WEATHER_CONTEXT_TTL_SECONDS > 0
        ? WEATHER_CONTEXT_TTL_SECONDS
        : 60 * 60 * 24
    );
  } catch (err) {
    console.error("setLastWeatherContext error:", err);
  }
}

function getConversationId(event) {
  return (
    event?.source?.userId || event?.source?.groupId || event?.source?.roomId || null
  );
}

function getChatHistoryKey(conversationId) {
  if (!conversationId) return null;
  return `chat:history:${conversationId}`;
}

function sanitizeChatContent(text) {
  const t = String(text || "").trim();
  if (!t) return "";
  const max =
    Number.isFinite(CHAT_HISTORY_MAX_CHARS) && CHAT_HISTORY_MAX_CHARS > 0
      ? CHAT_HISTORY_MAX_CHARS
      : 500;
  return t.length > max ? `${t.slice(0, max - 3)}...` : t;
}

function getChatHistoryMaxMessages() {
  const rounds =
    Number.isFinite(CHAT_HISTORY_ROUNDS) && CHAT_HISTORY_ROUNDS > 0
      ? CHAT_HISTORY_ROUNDS
      : 6;
  return Math.max(2, rounds * 2);
}

function normalizeChatHistory(raw) {
  if (!Array.isArray(raw)) return [];
  return raw
    .map((m) => ({
      role: m?.role === "assistant" ? "assistant" : "user",
      content: sanitizeChatContent(m?.content),
    }))
    .filter((m) => m.content);
}

function trimChatHistory(raw) {
  const messages = normalizeChatHistory(raw);
  const maxMessages = getChatHistoryMaxMessages();
  if (messages.length <= maxMessages) return messages;
  return messages.slice(messages.length - maxMessages);
}

function compactGeneralReply(text) {
  const raw = String(text || "").trim();
  if (!raw) return raw;

  const maxChars =
    Number.isFinite(GENERAL_REPLY_MAX_CHARS) && GENERAL_REPLY_MAX_CHARS > 0
      ? GENERAL_REPLY_MAX_CHARS
      : 260;
  if (raw.length <= maxChars) return raw;

  const sentenceParts = raw
    .split(/(?<=[。！？!?])/)
    .map((x) => x.trim())
    .filter(Boolean);
  if (sentenceParts.length >= 2) {
    return `${sentenceParts.slice(0, 2).join("")}（要我展開再說）`;
  }

  const lineParts = raw
    .split("\n")
    .map((x) => x.trim())
    .filter(Boolean);
  if (lineParts.length >= 2) {
    return `${lineParts.slice(0, 2).join("\n")}\n（要我展開再說）`;
  }

  return `${raw.slice(0, Math.max(40, maxChars - 8))}…（要我展開再說）`;
}

function cleanupLocalChatHistory(now = Date.now()) {
  for (const [key, value] of localChatHistory.entries()) {
    if (!value || value.expiresAt <= now) localChatHistory.delete(key);
  }
}

async function getRecentChatHistory(conversationId) {
  const key = getChatHistoryKey(conversationId);
  if (!key) return [];

  try {
    const raw = await redisGet(key);
    if (raw) {
      const parsed = trimChatHistory(JSON.parse(raw));
      if (parsed.length) return parsed;
    }
  } catch (err) {
    console.error("getRecentChatHistory parse error:", err);
  }

  cleanupLocalChatHistory();
  const local = localChatHistory.get(key);
  if (!local) return [];
  return trimChatHistory(local.messages);
}

async function setRecentChatHistory(conversationId, messages) {
  const key = getChatHistoryKey(conversationId);
  if (!key) return;

  const sanitized = trimChatHistory(messages);
  const ttl =
    Number.isFinite(CHAT_HISTORY_TTL_SECONDS) && CHAT_HISTORY_TTL_SECONDS > 0
      ? CHAT_HISTORY_TTL_SECONDS
      : 45 * 60;

  localChatHistory.set(key, {
    messages: sanitized,
    expiresAt: Date.now() + ttl * 1000,
  });

  await redisSet(key, JSON.stringify(sanitized), "EX", ttl);
}

async function appendRecentChatHistory(conversationId, userText, assistantText) {
  if (!conversationId) return;

  const userContent = sanitizeChatContent(userText);
  const assistantContent = sanitizeChatContent(assistantText);
  if (!userContent || !assistantContent) return;

  const history = await getRecentChatHistory(conversationId);
  history.push({ role: "user", content: userContent });
  history.push({ role: "assistant", content: assistantContent });

  await setRecentChatHistory(conversationId, history);
}

function getReminderItemKey(reminderId) {
  return `${REMINDER_ITEM_PREFIX}${reminderId}`;
}

function pad2(v) {
  return String(v).padStart(2, "0");
}

function taipeiPartsFromUtcMs(ms) {
  const offsetMs = TAIPEI_UTC_OFFSET_MINUTES * 60 * 1000;
  const shifted = new Date(ms + offsetMs);
  return {
    year: shifted.getUTCFullYear(),
    month: shifted.getUTCMonth() + 1,
    day: shifted.getUTCDate(),
    hour: shifted.getUTCHours(),
    minute: shifted.getUTCMinutes(),
  };
}

function utcMsFromTaipeiParts(parts) {
  const offsetMs = TAIPEI_UTC_OFFSET_MINUTES * 60 * 1000;
  return (
    Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute) -
    offsetMs
  );
}

function taipeiDateKey(ms) {
  const p = taipeiPartsFromUtcMs(ms);
  return `${p.year}-${pad2(p.month)}-${pad2(p.day)}`;
}

function reminderDateLabel(dueAtMs, nowMs = Date.now()) {
  const due = taipeiPartsFromUtcMs(dueAtMs);
  const dueTime = `${pad2(due.hour)}:${pad2(due.minute)}`;

  const today = taipeiDateKey(nowMs);
  const tomorrow = taipeiDateKey(nowMs + 24 * 60 * 60 * 1000);
  const dayAfter = taipeiDateKey(nowMs + 2 * 24 * 60 * 60 * 1000);
  const dueKey = taipeiDateKey(dueAtMs);

  if (dueKey === today) return `今天 ${dueTime}`;
  if (dueKey === tomorrow) return `明天 ${dueTime}`;
  if (dueKey === dayAfter) return `後天 ${dueTime}`;
  return `${due.month}/${due.day} ${dueTime}`;
}

function parseChineseNumberToken(token) {
  const t = String(token || "")
    .trim()
    .replace(/兩/g, "二")
    .replace(/〇/g, "零");
  if (!t) return null;

  const map = {
    零: 0,
    一: 1,
    二: 2,
    三: 3,
    四: 4,
    五: 5,
    六: 6,
    七: 7,
    八: 8,
    九: 9,
  };

  if (/^\d+$/.test(t)) return Number.parseInt(t, 10);

  if (t.includes("十")) {
    const parts = t.split("十");
    const tensPart = parts[0];
    const onesPart = parts[1];
    const tens = tensPart ? map[tensPart] : 1;
    const ones = onesPart ? map[onesPart] : 0;
    if (!Number.isFinite(tens) || !Number.isFinite(ones)) return null;
    return tens * 10 + ones;
  }

  if (t.length === 1 && Number.isFinite(map[t])) return map[t];
  return null;
}

function parseNumberToken(token) {
  const n = parseChineseNumberToken(token);
  return Number.isFinite(n) ? n : null;
}

function cleanReminderText(text) {
  const raw = String(text || "")
    .replace(/[。！？!?,，\s]+$/g, "")
    .trim();
  if (!raw) return "";
  const max =
    Number.isFinite(REMINDER_MAX_TEXT_CHARS) && REMINDER_MAX_TEXT_CHARS > 0
      ? REMINDER_MAX_TEXT_CHARS
      : 80;
  return raw.length > max ? `${raw.slice(0, max - 3)}...` : raw;
}

function parseReminderCommand(rawText, nowMs = Date.now()) {
  const text = String(rawText || "").trim();
  if (!text.includes("提醒")) return null;

  const relMinute = text.match(
    /([0-9零〇一二兩三四五六七八九十]{1,3})\s*(分鐘|分钟|分鍾)\s*後提醒(?:我)?(.+)/
  );
  if (relMinute) {
    const n = parseNumberToken(relMinute[1]);
    const content = cleanReminderText(relMinute[3]);
    if (!Number.isFinite(n) || n <= 0 || !content) return null;
    return {
      dueAt: nowMs + n * 60 * 1000,
      text: content,
    };
  }

  const relHour = text.match(
    /([0-9零〇一二兩三四五六七八九十]{1,2})\s*(小時|小时)\s*後提醒(?:我)?(.+)/
  );
  if (relHour) {
    const n = parseNumberToken(relHour[1]);
    const content = cleanReminderText(relHour[3]);
    if (!Number.isFinite(n) || n <= 0 || !content) return null;
    return {
      dueAt: nowMs + n * 60 * 60 * 1000,
      text: content,
    };
  }

  const contentMatch = text.match(/提醒(?:我)?(.+)$/);
  const reminderText = cleanReminderText(contentMatch?.[1] || "");
  if (!reminderText) return null;

  let hour = null;
  let minute = 0;

  const colonMatch = text.match(
    /([0-9零〇一二兩三四五六七八九十]{1,3})\s*[:：]\s*([0-9零〇一二兩三四五六七八九十]{1,2})/
  );
  const halfMatch = text.match(/([0-9零〇一二兩三四五六七八九十]{1,3})\s*點半/);
  const hourMatch = text.match(
    /([0-9零〇一二兩三四五六七八九十]{1,3})\s*點(?:\s*([0-9零〇一二兩三四五六七八九十]{1,2})\s*分?)?/
  );

  if (colonMatch) {
    hour = parseNumberToken(colonMatch[1]);
    minute = parseNumberToken(colonMatch[2]);
  } else if (halfMatch) {
    hour = parseNumberToken(halfMatch[1]);
    minute = 30;
  } else if (hourMatch) {
    hour = parseNumberToken(hourMatch[1]);
    minute = hourMatch[2] ? parseNumberToken(hourMatch[2]) : 0;
  } else {
    return null;
  }

  if (
    !Number.isFinite(hour) ||
    !Number.isFinite(minute) ||
    hour < 0 ||
    hour > 23 ||
    minute < 0 ||
    minute > 59
  ) {
    return null;
  }

  const hasPmHint = /(下午|晚上|今晚|傍晚)/.test(text);
  const hasAmHint = /(凌晨|清晨|早上|上午)/.test(text);
  if (hasPmHint && hour < 12) hour += 12;
  if (/凌晨/.test(text) && hour === 12) hour = 0;

  let dayOffset = 0;
  if (/後天/.test(text)) dayOffset = 2;
  else if (/明天|明早|明晚/.test(text)) dayOffset = 1;

  const nowParts = taipeiPartsFromUtcMs(nowMs);
  const baseUtc = utcMsFromTaipeiParts({
    year: nowParts.year,
    month: nowParts.month,
    day: nowParts.day,
    hour: 0,
    minute: 0,
  });
  const targetDayUtc = baseUtc + dayOffset * 24 * 60 * 60 * 1000;
  const targetDayParts = taipeiPartsFromUtcMs(targetDayUtc);

  let dueAt = utcMsFromTaipeiParts({
    year: targetDayParts.year,
    month: targetDayParts.month,
    day: targetDayParts.day,
    hour,
    minute,
  });

  if (!hasPmHint && !hasAmHint && dayOffset === 0 && hour >= 1 && hour <= 11) {
    const plus12 = dueAt + 12 * 60 * 60 * 1000;
    if (dueAt <= nowMs && plus12 > nowMs) dueAt = plus12;
  }

  if (dueAt <= nowMs + 30 * 1000) {
    dueAt += 24 * 60 * 60 * 1000;
  }

  return {
    dueAt,
    text: reminderText,
  };
}

function buildReminderTarget(event) {
  const type = event?.source?.type;
  if (type === "user" && event?.source?.userId) {
    return { targetType: "user", targetId: event.source.userId };
  }
  if (type === "group" && event?.source?.groupId) {
    return { targetType: "group", targetId: event.source.groupId };
  }
  if (type === "room" && event?.source?.roomId) {
    return { targetType: "room", targetId: event.source.roomId };
  }
  return null;
}

async function scheduleReminder(event, parsedReminder) {
  const target = buildReminderTarget(event);
  if (!target || !parsedReminder?.text || !Number.isFinite(parsedReminder?.dueAt)) {
    return null;
  }

  const reminderId = `r_${Date.now()}_${Math.random()
    .toString(36)
    .slice(2, 10)}`;
  const payload = {
    id: reminderId,
    dueAt: parsedReminder.dueAt,
    text: parsedReminder.text,
    targetType: target.targetType,
    targetId: target.targetId,
    conversationId: getConversationId(event),
    creatorId: event?.source?.userId || null,
    retries: 0,
    createdAt: Date.now(),
  };

  const itemKey = getReminderItemKey(reminderId);
  const saved = await redisSet(
    itemKey,
    JSON.stringify(payload),
    "EX",
    Number.isFinite(REMINDER_TTL_SECONDS) && REMINDER_TTL_SECONDS > 0
      ? REMINDER_TTL_SECONDS
      : 60 * 60 * 24 * 7
  );
  if (!saved) return null;

  const queued = await redisZAdd(REMINDER_QUEUE_KEY, payload.dueAt, reminderId);
  if (!queued) {
    await redisDel(itemKey);
    return null;
  }

  return payload;
}

async function processDueReminders() {
  const now = Date.now();
  const dueIds = await redisZRangeByScore(
    REMINDER_QUEUE_KEY,
    0,
    now,
    REMINDER_SCAN_BATCH
  );
  if (!dueIds.length) {
    return { scanned: 0, sent: 0, retried: 0, failed: 0 };
  }

  let sent = 0;
  let retried = 0;
  let failed = 0;

  for (const reminderId of dueIds) {
    const claimed = await redisZRem(REMINDER_QUEUE_KEY, reminderId);
    if (!claimed) continue;

    const itemKey = getReminderItemKey(reminderId);
    const raw = await redisGet(itemKey);
    if (!raw) continue;

    let payload;
    try {
      payload = JSON.parse(raw);
    } catch (err) {
      console.error("reminder payload parse error:", err);
      await redisDel(itemKey);
      failed++;
      continue;
    }

    const text = cleanReminderText(payload?.text || "");
    const targetId = payload?.targetId;
    if (!text || !targetId) {
      await redisDel(itemKey);
      failed++;
      continue;
    }

    try {
      await client.pushMessage(targetId, normalizeLineMessage({
        type: "text",
        text: `⏰ 提醒：${text}`,
      }));
      await redisDel(itemKey);
      sent++;
    } catch (err) {
      const retries = Number(payload?.retries || 0) + 1;
      if (retries <= REMINDER_MAX_RETRIES) {
        const retryDueAt =
          Date.now() +
          (Number.isFinite(REMINDER_RETRY_DELAY_SECONDS) &&
          REMINDER_RETRY_DELAY_SECONDS > 0
            ? REMINDER_RETRY_DELAY_SECONDS
            : 60) *
            1000;
        const retryPayload = {
          ...payload,
          retries,
          dueAt: retryDueAt,
        };
        await redisSet(
          itemKey,
          JSON.stringify(retryPayload),
          "EX",
          Number.isFinite(REMINDER_TTL_SECONDS) && REMINDER_TTL_SECONDS > 0
            ? REMINDER_TTL_SECONDS
            : 60 * 60 * 24 * 7
        );
        await redisZAdd(REMINDER_QUEUE_KEY, retryDueAt, reminderId);
        retried++;
      } else {
        await redisDel(itemKey);
        failed++;
      }
      console.error("reminder push failed:", err?.message || err);
    }
  }

  return {
    scanned: dueIds.length,
    sent,
    retried,
    failed,
  };
}

function isGroupAllowed(event) {
  const sourceType = event.source.type;

  // ① 私聊：一律放行
  if (sourceType === "user") {
    return true;
  }

  // ② 群組 / room：只處理文字
  if (sourceType === "group" || sourceType === "room") {
    if (event.message?.type !== "text") return false;

    const text = event.message.text.trim();

    // ✅ 只認「明確叫我」
    return /^\s*(助理|KevinBot|kevinbot)/i.test(text);
  }

  return false;
}

function stripBotName(text = "") {
  return text.replace(/^(助理|KevinBot|kevinbot)\s*/i, "").trim();
}

function isTaiwanLocation(raw = "") {
  return /(台灣|臺灣|台湾|台北|臺北|新北|台中|臺中|台南|臺南|高雄|桃園|新竹|嘉義|宜蘭|花蓮|台東|臺東|澎湖|金門|馬祖|南竿|北竿|東引)/.test(
    raw
  );
}

function quickWeatherParse(text) {
  const t = text.trim();

  const when = t.includes("後天")
    ? "day_after"
    : t.includes("明天")
    ? "tomorrow"
    : "today";

  const cityMatch = t.match(
    /(台北|臺北|新北|台中|臺中|台南|臺南|高雄|桃園|新竹|嘉義|宜蘭|花蓮|台東|臺東|南竿|北竿|東引|馬祖|金門|澎湖)/
  );

  const isWeather = /(天氣|氣溫|下雨|冷不冷|熱不熱|會不會下雨)/.test(t);

  if (!isWeather) return null;
  if (!cityMatch) return null;

  return {
    city: cityMatch?.[1],
    when,
  };
}

function cleanCity(raw) {
  if (!raw) return raw;

  let c = raw.trim();

  // 去掉常見雜詞
  c = c
    .replace(/天氣/g, "")
    .replace(/氣溫/g, "")
    .replace(/如何/g, "")
    .replace(/會不會下雨/g, "")
    .replace(/下雨嗎/g, "")
    .replace(/明天/g, "")
    .replace(/後天/g, "")
    .replace(/今天/g, "")
    .replace(/台灣/g, "")
    .replace(/臺灣/g, "")
    .replace(/台湾/g, "")
    .replace(/的/g, "")
    .replace(/市/g, "")
    .replace(/縣/g, "")
    .replace(/區/g, "")
    .replace(/鄉/g, "")
    .trim();

  // 有 "台中" 就固定成台中
  if (c.includes("台中") || c.includes("臺中")) return "台中";
  if (c.includes("台北") || c.includes("臺北")) return "台北";
  if (c.includes("新北")) return "新北";
  if (c.includes("桃園")) return "桃園";
  if (c.includes("高雄")) return "高雄";
  if (c.includes("台南") || c.includes("臺南")) return "台南";
  if (c.includes("新竹")) return "新竹";
  if (c.includes("嘉義")) return "嘉義";
  if (c.includes("宜蘭")) return "宜蘭";
  if (c.includes("花蓮")) return "花蓮";
  if (c.includes("台東") || c.includes("臺東")) return "台東";

  // 無法判斷就用原字串
  return c;
}

function fixTaiwanCity(raw) {
  if (!raw) return raw;
  const trimmed = raw.trim();
  return TW_CITY_MAP[trimmed] || trimmed;
}

function normalizeWhen(raw = "today") {
  const text = String(raw).toLowerCase();

  if (text.includes("後天")) return "day_after";
  if (text.includes("明天") || text.includes("明日")) return "tomorrow";

  return "today";
}

function buildOutfitAdvice(temp, feelsLike, rainProbability) {
  const t = feelsLike ?? temp;
  let top = "短袖或輕薄排汗衫";
  let bottom = "短褲或薄長褲";
  let outer = "可不用外套，室內冷氣可備薄外套";
  let warmth = "1 / 5";

  if (t >= 33) {
    top = "超輕薄短袖 / 無袖排汗衫";
    bottom = "短褲或運動短褲";
    outer = "不用外套，盡量待室內補水";
    warmth = "1 / 5";
  } else if (t >= 27) {
    top = "短袖 / POLO / 透氣襯衫";
    bottom = "薄長褲或短褲";
    outer = "薄外套可有可無";
    warmth = "1-2 / 5";
  } else if (t >= 22) {
    top = "薄長袖或 T 恤";
    bottom = "長褲";
    outer = "輕薄外套或襯衫當外層";
    warmth = "2 / 5";
  } else if (t >= 17) {
    top = "長袖 T 恤或薄針織";
    bottom = "長褲";
    outer = "薄風衣 / 輕薄外套";
    warmth = "3 / 5";
  } else if (t >= 12) {
    top = "長袖 + 針織或薄毛衣";
    bottom = "長褲";
    outer = "中等厚度外套 / 風衣";
    warmth = "3-4 / 5";
  } else if (t >= 7) {
    top = "長袖 + 毛衣";
    bottom = "長褲 + 厚襪子";
    outer = "厚外套 / 大衣，騎車加圍巾";
    warmth = "4 / 5";
  } else {
    top = "保暖發熱衣 + 毛衣";
    bottom = "長褲 + 發熱褲";
    outer = "羽絨衣 / 厚大衣 + 圍巾 + 毛帽";
    warmth = "5 / 5";
  }

  const rainExtra =
    rainProbability >= 0.5
      ? "降雨機率高，記得帶傘或穿防水外套。"
      : rainProbability >= 0.2
      ? "可能會下雨，建議帶折傘備用。"
      : "";

  return [
    `上身：${top}`,
    `下身：${bottom}`,
    `外層：${outer}`,
    `保暖等級：${warmth}`,
    rainExtra,
  ]
    .filter(Boolean)
    .join("\n");
}

// 台灣離島人工座標
const TAIWAN_ISLANDS = {
  南竿: { lat: 26.1597, lon: 119.9519, name: "南竿（馬祖）" },
  北竿: { lat: 26.2244, lon: 119.9987, name: "北竿（馬祖）" },
  東引: { lat: 26.3667, lon: 120.4833, name: "東引（馬祖）" },
  金門: { lat: 24.4367, lon: 118.3186, name: "金門" },
  烏丘: { lat: 24.9986, lon: 119.3347, name: "烏丘" },
  澎湖: { lat: 23.565, lon: 119.586, name: "澎湖" },
  馬祖: { lat: 26.1597, lon: 119.9519, name: "馬祖" },
  馬祖列島: { lat: 26.1597, lon: 119.9519, name: "馬祖列島" },
};

function findTaiwanIsland(raw) {
  if (!raw) return null;
  const c = raw.trim();
  const lower = c.toLowerCase();

  if (lower.includes("nangan")) return TAIWAN_ISLANDS["南竿"];
  if (lower.includes("beigan")) return TAIWAN_ISLANDS["北竿"];
  if (lower.includes("dongyin")) return TAIWAN_ISLANDS["東引"];
  if (lower.includes("matsu")) return TAIWAN_ISLANDS["馬祖"];
  if (lower.includes("kinmen") || lower.includes("jinmen"))
    return TAIWAN_ISLANDS["金門"];
  if (lower.includes("penghu")) return TAIWAN_ISLANDS["澎湖"];

  for (const key of Object.keys(TAIWAN_ISLANDS)) {
    if (c.includes(key)) return TAIWAN_ISLANDS[key];
  }
  return null;
}
function pickWeatherImage(desc = "", rainPercent = 0) {
  const d = desc.toLowerCase();

  if (rainPercent >= 40 || d.includes("雨")) {
    return "https://raw.githubusercontent.com/ChenWenChou/line-gpt-kevin/main/public/image/rain.png";
  }

  if (d.includes("晴")) {
    return "https://raw.githubusercontent.com/ChenWenChou/line-gpt-kevin/main/public/image/sun.png";
  }

  return "https://raw.githubusercontent.com/ChenWenChou/line-gpt-kevin/main/public/image/cloud.png";
}

function buildWeatherFlex({
  city,
  whenLabel,
  desc,
  minTemp,
  maxTemp,
  feels,
  humidity,
  rainPercent,
  outfitText,
}) {
  const imageUrl = pickWeatherImage(desc, rainPercent);
  return {
    type: "flex",
    altText: `${city}${whenLabel}天氣`,
    contents: {
      type: "bubble",
      size: "mega",

      // HERO IMAGE
      hero: {
        type: "image",
        url: imageUrl,
        size: "full",
        aspectRatio: "20:13",
        aspectMode: "cover",
      },
      body: {
        type: "box",
        layout: "vertical",
        spacing: "md",
        contents: [
          {
            type: "text",
            text: `🌦 ${city}｜${whenLabel}天氣`,
            weight: "bold",
            size: "lg",
          },
          {
            type: "text",
            text: desc,
            size: "md",
            color: "#666666",
          },
          {
            type: "separator",
          },
          {
            type: "box",
            layout: "vertical",
            spacing: "sm",
            contents: [
              {
                type: "text",
                text: `🌡 ${minTemp}°C ～ ${maxTemp}°C（體感 ${feels}°C）`,
              },
              {
                type: "text",
                text: `💧 濕度 ${humidity}%`,
              },
              {
                type: "text",
                text: `☔ 降雨機率 ${rainPercent}%`,
              },
            ],
          },
          {
            type: "separator",
          },
          {
            type: "text",
            text: "【穿搭建議】",
            weight: "bold",
          },
          {
            type: "text",
            text: outfitText,
            wrap: true,
            size: "sm",
          },
        ],
      },
    },
  };
}

async function geocodeCity(city, apiKey) {
  const c = city.trim();

  // ① 先檢查是否為台灣離島
  const island = findTaiwanIsland(c);
  if (island) return island;

  // ① 若使用者明確輸入「國家 城市」
  //    例如「日本 大阪」「韓國 首爾」「美國 紐約」
  if (c.includes(" ")) {
    const url = `https://api.openweathermap.org/geo/1.0/direct?q=${encodeURIComponent(
      c
    )}&limit=1&appid=${apiKey}`;

    const res = await fetch(url);
    if (res.ok) {
      const [geo] = await res.json();
      if (geo) {
        return {
          lat: geo.lat,
          lon: geo.lon,
          name: geo.local_names?.zh || geo.name || c,
        };
      }
    }
  }

  // ② 日本常見城市（避免跑到中國）
  const JP_MAP = {
    大阪: "Osaka,JP",
    東京: "Tokyo,JP",
    京都: "Kyoto,JP",
    札幌: "Sapporo,JP",
    橫濱: "Yokohama,JP",
  };

  if (JP_MAP[c]) {
    const url = `https://api.openweathermap.org/geo/1.0/direct?q=${JP_MAP[c]}&limit=1&appid=${apiKey}`;
    const res = await fetch(url);
    if (res.ok) {
      const [geo] = await res.json();
      if (geo) {
        return {
          lat: geo.lat,
          lon: geo.lon,
          name: geo.local_names?.zh || geo.name || c,
        };
      }
    }
  }

  // ③ 台灣優先（你原本的規則）
  const TW_MAP = {
    台北: "Taipei, TW",
    臺北: "Taipei, TW",
    新北: "New Taipei, TW",
    台中: "Taichung, TW",
    臺中: "Taichung, TW",
    台南: "Tainan, TW",
    臺南: "Tainan, TW",
    高雄: "Kaohsiung, TW",
    桃園: "Taoyuan, TW",
    新竹: "Hsinchu, TW",
    嘉義: "Chiayi, TW",
    宜蘭: "Yilan, TW",
    花蓮: "Hualien, TW",
    台東: "Taitung, TW",
    臺東: "Taitung, TW",
  };

  if (TW_MAP[c]) {
    const url = `https://api.openweathermap.org/geo/1.0/direct?q=${encodeURIComponent(
      TW_MAP[c]
    )}&limit=1&appid=${apiKey}`;
    const res = await fetch(url);
    if (res.ok) {
      const [geo] = await res.json();
      if (geo) {
        return {
          lat: geo.lat,
          lon: geo.lon,
          name: geo.local_names?.zh || geo.name || c,
        };
      }
    }
  }

  // ④ 最後才用原字串查一次（世界城市）
  const url = `https://api.openweathermap.org/geo/1.0/direct?q=${encodeURIComponent(
    c
  )}&limit=1&appid=${apiKey}`;
  const res = await fetch(url);
  if (res.ok) {
    const [geo] = await res.json();
    if (geo) {
      return {
        lat: geo.lat,
        lon: geo.lon,
        name: geo.local_names?.zh || geo.name || c,
      };
    }
  }

  return null;
}

// 查天氣 + 穿搭建議（支援城市名或座標、今天/明天/後天、降雨機率）
// 使用 free plan 的 forecast API
async function getWeatherAndOutfit({
  city = "Taipei",
  lat,
  lon,
  when = "today",
  address,
} = {}) {
  const apiKey = process.env.WEATHER_API_KEY;
  if (!apiKey) {
    return "後端沒有設定 WEATHER_API_KEY，請先到 Vercel 設定環境變數。";
  }

  try {
    let resolvedCity = city;
    let resolvedLat = lat;
    let resolvedLon = lon;

    const isTW = isTaiwanLocation(resolvedCity);

    // 台灣離島先用人工座標
    const island = findTaiwanIsland(resolvedCity);
    if (!resolvedLat && !resolvedLon && island) {
      resolvedLat = island.lat;
      resolvedLon = island.lon;
      resolvedCity = island.name;
    }

    if (!resolvedLat || !resolvedLon) {
      const geo = await geocodeCity(city, apiKey);
      if (!geo) {
        // 無法 geocode，改用城市名稱直接查 forecast（預設國家為台灣）
        resolvedCity = city;
      } else {
        resolvedLat = geo.lat;
        resolvedLon = geo.lon;
        resolvedCity = geo.name;
      }
    }

    const forecastUrl =
      resolvedLat && resolvedLon
        ? `https://api.openweathermap.org/data/2.5/forecast?lat=${resolvedLat}&lon=${resolvedLon}&units=metric&lang=zh_tw&appid=${apiKey}`
        : `https://api.openweathermap.org/data/2.5/forecast?q=${encodeURIComponent(
            isTW ? `${resolvedCity},TW` : resolvedCity
          )}&units=metric&lang=zh_tw&appid=${apiKey}`;
    const res = await fetch(forecastUrl);
    if (!res.ok) {
      const text = await res.text();
      console.error("Weather API error:", res.status, text);
      return `查天氣失敗（status: ${res.status}）\n${text.slice(0, 200)}`;
    }

    const data = await res.json();

    // ================================
    // ✅ 用 forecast 第一筆當「今天」
    // ================================
    const offsetSec = data.city?.timezone ?? 0;

    // local date helper（只保留這一個）
    function getLocalDateString(dt, offsetSec) {
      const d = new Date((dt + offsetSec) * 1000);
      return d.toISOString().slice(0, 10);
    }

    const firstItem = data.list?.[0];
    if (!firstItem) {
      return "暫時查不到天氣資料，請稍後再試。";
    }

    const baseDateStr = getLocalDateString(firstItem.dt, offsetSec);

    const dayIndex = when === "tomorrow" ? 1 : when === "day_after" ? 2 : 0;

    const targetDate = new Date(baseDateStr);
    targetDate.setDate(targetDate.getDate() + dayIndex);
    const targetDateStr = targetDate.toISOString().slice(0, 10);

    const pickSlot = (list) => {
      const sameDay = list.filter((item) => {
        const local = getLocalDateString(item.dt, offsetSec);
        return local === targetDateStr;
      });

      if (sameDay.length === 0) {
        // 👉 fallback：用 forecast 第一筆
        return list[0] || null;
      }

      // ✅ 改成「距離中午最近的一筆」
      const targetHour = 12;

      return sameDay.reduce((closest, curr) => {
        const currHour = new Date((curr.dt + offsetSec) * 1000).getUTCHours();
        const closestHour = new Date(
          (closest.dt + offsetSec) * 1000
        ).getUTCHours();

        return Math.abs(currHour - targetHour) <
          Math.abs(closestHour - targetHour)
          ? curr
          : closest;
      }, sameDay[0]);
    };

    const slot = pickSlot(data.list || []);

    const sameDayEntries = (data.list || []).filter((item) => {
      const local = getLocalDateString(item.dt, offsetSec);
      return local === targetDateStr;
    });

    // ✅ 計算「當日最高降雨機率」
    let maxPop = 0;

    if (sameDayEntries.length > 0) {
      maxPop = Math.max(
        ...sameDayEntries.map((i) => (typeof i.pop === "number" ? i.pop : 0))
      );
    }

    const rainPercent = Math.round(maxPop * 100);

    // 如果找到同日資料 → 計算 max / min
    let maxTemp = null;
    let minTemp = null;

    if (sameDayEntries.length > 0) {
      const temps = sameDayEntries.map((i) => i.main?.temp).filter(Boolean);
      maxTemp = Math.max(...temps);
      minTemp = Math.min(...temps);
    }
    // --- 計算體感溫度區間 ---
    let maxFeels = null;
    let minFeels = null;

    if (sameDayEntries.length > 0) {
      const feels = sameDayEntries
        .map((i) => i.main?.feels_like)
        .filter(Boolean);
      maxFeels = Math.max(...feels);
      minFeels = Math.min(...feels);
    }

    // 格式化（避免 undefined）
    const tempRangeText =
      maxTemp !== null
        ? `氣溫：${minTemp.toFixed(1)}°C ～ ${maxTemp.toFixed(1)}°C\n`
        : "";

    const feelsRangeText =
      maxFeels !== null
        ? `體感：${minFeels.toFixed(1)}°C ～ ${maxFeels.toFixed(1)}°C\n`
        : "";

    if (!slot) {
      return "暫時查不到這個時間點的天氣，等等再試一次。";
    }

    const temp = slot.main?.temp;
    const feels = slot.main?.feels_like ?? temp;

    const safeMin =
      minTemp != null ? minTemp.toFixed(1) : temp?.toFixed(1) ?? "--";
    const safeMax =
      maxTemp != null ? maxTemp.toFixed(1) : temp?.toFixed(1) ?? "--";
    const safeFeels = feels != null ? feels.toFixed(1) : "--";

    const humidity = slot.main?.humidity ?? "NA";
    const desc = slot.weather?.[0]?.description || "未知";
    const rainText = `降雨機率：${rainPercent}%`;
    const locationLabel = address
      ? `${address}（座標）`
      : resolvedCity || city || "未命名地點";
    const whenLabel = WHEN_LABEL[when] || WHEN_LABEL.today;
    const outfit = buildOutfitAdvice(temp, feels, maxPop);
    const maxMinText =
      maxTemp !== null
        ? `最高溫：${maxTemp.toFixed(1)}°C\n最低溫：${minTemp.toFixed(1)}°C\n`
        : "";

    const weatherText =
      `【${locationLabel}｜${whenLabel}天氣】\n` +
      `狀態：${desc}\n` +
      tempRangeText +
      feelsRangeText +
      `濕度：${humidity}%\n` +
      `${rainText}\n\n` +
      `【穿搭建議】\n` +
      outfit;

    return {
      text: weatherText,
      data: {
        city: locationLabel,
        whenLabel,
        desc,
        minTemp: safeMin,
        maxTemp: safeMax,
        feels: safeFeels,
        humidity,
        rainPercent,
        outfitText: outfit,
      },
    };
  } catch (err) {
    console.error("Weather fetch error:", err);
    return "查天氣時發生例外錯誤，等等再試一次。";
  }
}

async function replyWeather(replyTarget, result) {
  // 如果整個 result 就是錯誤字串 → 直接回文字
  if (!result || typeof result === "string" || !result.data) {
    await sendLineReply(replyTarget, {
      type: "text",
      text: typeof result === "string" ? result : "天氣資料取得失敗",
    });
    return;
  }

  // 嘗試送 Flex
  try {
    await sendLineReply(replyTarget, buildWeatherFlex(result.data));
    return;
  } catch (err) {
    console.error("Flex 回傳失敗，fallback 文字", err);
    await sendLineReply(replyTarget, {
      type: "text",
      text: result.text,
    });
  }
}

// 求籤方式
function drawMazuLot() {
  return mazuLots[Math.floor(Math.random() * mazuLots.length)];
}

function buildMazuLotFlex({ title, poem, advice }) {
  return {
    type: "flex",
    altText: `媽祖靈籤｜${title}`,
    contents: {
      type: "bubble",
      size: "mega",
      body: {
        type: "box",
        layout: "vertical",
        spacing: "md",
        contents: [
          {
            type: "text",
            text: "🙏 媽祖靈籤",
            weight: "bold",
            size: "sm",
            color: "#B71C1C",
          },
          {
            type: "text",
            text: title,
            weight: "bold",
            size: "xl",
          },
          { type: "separator" },

          // 籤詩
          ...poem.map((line) => ({
            type: "text",
            text: line,
            size: "md",
            wrap: true,
          })),

          { type: "separator" },

          {
            type: "text",
            text: "【白話建議】",
            weight: "bold",
            margin: "md",
          },
          {
            type: "text",
            text: advice,
            size: "sm",
            wrap: true,
            color: "#555555",
          },
        ],
      },
    },
  };
}

async function explainLotPlain(poem) {
  try {
    const text = poem.join(" ");

    const res = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      messages: [
        {
          role: "system",
          content:
            "你是一位理性溫和的文字解說者，請用口語白話解釋籤詩的『提醒方向』，避免預言、避免保證性語句，控制在 2~3 句。",
        },
        { role: "user", content: text },
      ],
      max_tokens: 120,
    });

    return res.choices[0].message.content.trim();
  } catch (err) {
    console.error("❌ 解籤失敗", err);
    return "這支籤提醒你放慢腳步，先觀察局勢，再做決定。";
  }
}

// 星座
const ZODIAC_MAP = {
  牡羊: "aries",
  金牛: "taurus",
  雙子: "gemini",
  巨蟹: "cancer",
  獅子: "leo",
  處女: "virgo",
  天秤: "libra",
  天蠍: "scorpio",
  射手: "sagittarius",
  摩羯: "capricorn",
  水瓶: "aquarius",
  雙魚: "pisces",
};

function getTodayKey(offset = 0) {
  const d = new Date();
  d.setDate(d.getDate() + offset);
  return d.toISOString().slice(0, 10);
}
function renderStars(n = 0) {
  return "★".repeat(n) + "☆".repeat(5 - n);
}
function calcStar(date, signEn) {
  // 簡單 deterministic hash
  const base = [...(date + signEn)].reduce((a, c) => a + c.charCodeAt(0), 0);
  return (base % 5) + 1; // 1~5
}

function calcLuckyNumber(date, signEn) {
  // 先把日期變成穩定數字（YYYY-MM-DD）
  const dateBase = date.replace(/-/g, "");
  let seed = parseInt(dateBase, 10);

  // 星座影響（小幅偏移）
  for (const c of signEn) {
    seed += c.charCodeAt(0);
  }

  // 轉成 1~99
  return (seed % 99) + 1;
}

function buildHoroscopeFlexV2({ signZh, signEn, whenLabel, data }) {
  const imageUrl = `https://raw.githubusercontent.com/ChenWenChou/line-gpt-kevin/main/public/image/${signEn}.png`;

  return {
    type: "flex",
    altText: `${whenLabel}${signZh}座運勢`,
    contents: {
      type: "bubble",
      size: "mega",
      hero: {
        type: "image",
        url: imageUrl,
        size: "full",
        aspectRatio: "20:13",
        aspectMode: "cover",
      },
      body: {
        type: "box",
        layout: "vertical",
        spacing: "md",
        contents: [
          {
            type: "text",
            text: `🔮 ${whenLabel}${signZh}座運勢`,
            size: "xl",
            weight: "bold",
          },
          {
            type: "text",
            text: renderStars(data.overall ?? 0),
            size: "lg",
            color: "#F5A623",
          },
          { type: "separator" },

          {
            type: "text",
            text: `💼 工作：${data.work ?? "今日適合穩定推進"}`,
            wrap: true,
          },
          {
            type: "text",
            text: `❤️ 感情：${data.love ?? "多一點體貼就很加分"}`,
            wrap: true,
          },
          {
            type: "text",
            text: `💰 財運：${data.money ?? "保守理財較安心"}`,
            wrap: true,
          },
          {
            type: "text",
            text: `🎯 幸運數字：${data.luckyNumber ?? "-"}`,
            wrap: true,
            weight: "bold",
          },

          { type: "separator", margin: "md" },
          {
            type: "text",
            text: "※ 我無法知道星相，跟國師會有落差！",
            size: "xs",
            color: "#ff0741",
          },
        ],
      },
    },
  };
}

async function getDailyHoroscope(signZh, when = "today") {
  const sign = ZODIAC_MAP[signZh];
  if (!sign) return null;

  const date = when === "tomorrow" ? getTodayKey(1) : getTodayKey(0);

  const kvKey = `horoscope:v5:${date}:${sign}`;

  // ① 先查 KV
  const cached = await redisGet(kvKey);
  if (cached) return JSON.parse(cached);

  // ② 沒有才問 GPT（只會發生一次）
  const whenLabel = when === "tomorrow" ? "明日" : "今日";

  const res = await openai.chat.completions.create({
    model: "gpt-4o-mini",
    messages: [
      {
        role: "system",
        content:
          "你是理性、不渲染極端的星座運勢撰寫者，避免極端好壞、避免保證性語句、同時帶點生活詼諧幽默感。請只回傳 JSON，不要多任何文字。",
      },
      {
        role: "user",
        content: `
請產生「${whenLabel}${signZh}座」運勢。
請明顯反映「${signZh}座的典型性格」。

格式：
{
  "work": "...",
  "love": "...",
  "money": "..."
}

限制：
- 每句 20 字內
- 不要過度中性
- 同一天不同星座請有明顯差異
`,
      },
    ],
    max_tokens: 200,
  });

  const text = res.choices[0].message.content.trim();

  let data;
  try {
    data = JSON.parse(res.choices[0].message.content);
  } catch {
    throw new Error("Horoscope JSON parse failed");
  }
  const overall = calcStar(date, sign);
  const luckyNumber = calcLuckyNumber(date, sign);

  const payload = {
    sign: signZh,
    when,
    overall,
    luckyNumber,
    ...data,
  };

  // ③ 存 KV（一天）
  await redisSet(kvKey, JSON.stringify(payload), "EX", 60 * 60 * 24);

  return payload;
}

// 計算熱量
function parseFoodList(text) {
  // 常見分隔符號
  return text
    .replace(/^(助理|KevinBot|kevinbot)\s*/i, "")
    .replace(/我(今天|剛剛)?吃了/g, "")
    .split(/、|,|，|跟|和|\n/)
    .map((s) => s.trim())
    .filter(Boolean);
}

async function estimateFoodCalorie(food) {
  const today = getTodayKey(0);
  const key = `food:estimate:${today}:${food}`;

  const cached = await redisGet(key);
  if (cached) return JSON.parse(cached);

  const res = await openai.chat.completions.create({
    model: "gpt-4o-mini",
    messages: [
      {
        role: "system",
        content:
          "你是生活型熱量估算助理，只能提供『熱量區間』，不可給精準數字。請只回 JSON。",
      },
      {
        role: "user",
        content: `
請估算以下食物的熱量區間（台灣常見份量）：

食物：${food}

格式：
{
  "food": "${food}",
  "min": 0,
  "max": 0,
  "note": "一句影響因素"
}
`,
      },
    ],
    max_tokens: 150,
  });

  const data = JSON.parse(res.choices[0].message.content);

  await redisSet(key, JSON.stringify(data), "EX", 60 * 60 * 24);

  return data;
}

// 股市 15分鐘延遲

const STOCKS_REDIS_KEY = "twse:stocks:all";
const TWSE_STOCKS_OPENAPI_URL =
  "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL";
const TWSE_STOCKS_CSV_URL =
  "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=open_data";
const TPEX_STOCKS_JSON_URL =
  "https://www.tpex.org.tw/www/zh-tw/afterTrading/otc?date=&type=EW&response=json";
const MIN_TWSE_STOCK_COUNT = 500;
const MIN_TPEX_STOCK_COUNT = 300;
const STOCK_QUOTE_TIMEOUT_MS = Number(process.env.STOCK_QUOTE_TIMEOUT_MS || 6000);
const STOCK_HISTORY_TIMEOUT_MS = Number(
  process.env.STOCK_HISTORY_TIMEOUT_MS || 8000
);
const STOCK_REALTIME_TIMEOUT_MS = Number(
  process.env.STOCK_REALTIME_TIMEOUT_MS || 5000
);
const STOCK_INTRADAY_WATCHLIST_KEY = "stock:intraday:watchlist";
const STOCK_INTRADAY_TTL_SECONDS = Number(
  process.env.STOCK_INTRADAY_TTL_SECONDS || 60 * 60 * 36
);
const STOCK_INTRADAY_MAX_POINTS = Number(
  process.env.STOCK_INTRADAY_MAX_POINTS || 320
);
const STOCK_INTRADAY_MIN_POINTS = Number(
  process.env.STOCK_INTRADAY_MIN_POINTS || 3
);
const STOCK_INTRADAY_COLLECT_LIMIT = Number(
  process.env.STOCK_INTRADAY_COLLECT_LIMIT || 20
);
const QUICKCHART_CREATE_URL = "https://quickchart.io/chart/create";
const QUICKCHART_TIMEOUT_MS = Number(process.env.QUICKCHART_TIMEOUT_MS || 8000);

const COMMON_TW_ETF_ALIASES = {
  "0050": {
    name: "元大台灣50",
    aliases: ["台灣50", "元大50", "元大台灣50"],
  },
  "0056": {
    name: "元大高股息",
    aliases: ["高股息", "元大高股息"],
  },
  "006208": {
    name: "富邦台50",
    aliases: ["富邦台灣50", "富邦台50"],
  },
  "00713": {
    name: "元大台灣高息低波",
    aliases: ["高息低波", "元大高息低波", "元大台灣高息低波"],
  },
  "00878": {
    name: "國泰永續高股息",
    aliases: ["國泰高股息", "永續高股息", "國泰永續高股息"],
  },
  "00881": {
    name: "國泰台灣5G+",
    aliases: ["國泰5G", "台灣5G", "國泰台灣5G"],
  },
  "00900": {
    name: "富邦特選高股息30",
    aliases: ["富邦高股息", "富邦特選高股息"],
  },
  "00919": {
    name: "群益台灣精選高息",
    aliases: ["群益高息", "群益台灣精選高息", "台灣精選高息"],
  },
  "00929": {
    name: "復華台灣科技優息",
    aliases: ["復華科技優息", "科技優息", "台灣科技優息"],
  },
  "00939": {
    name: "統一台灣高息動能",
    aliases: ["統一高息動能", "台灣高息動能"],
  },
  "00940": {
    name: "元大台灣價值高息",
    aliases: ["元大價值高息", "台灣價值高息"],
  },
};

function normalizeStockText(value) {
  return String(value || "")
    .toUpperCase()
    .replace(/[\s　]+/g, "")
    .replace(/[()（）[\]【】「」『』,，.。．、:：;；_\-]/g, "");
}

function normalizeStockCode(value) {
  const code = String(value || "").trim().toUpperCase();
  if (!/^(?:\d{4,6}|\d{4,5}[A-Z])$/.test(code)) return "";
  return code;
}

function extractStockCodeFromQuery(query) {
  const upper = String(query || "").toUpperCase();
  const match = upper.match(/(?:^|[^0-9A-Z])((?:\d{4,6}|\d{4,5}[A-Z]))(?=$|[^0-9A-Z])/);
  return match ? match[1] : "";
}

function getCommonEtfAliases(code) {
  return COMMON_TW_ETF_ALIASES[code]?.aliases || [];
}

function addStockRecord(stocks, { code, name, market = "TWSE", symbol }) {
  const normalizedCode = normalizeStockCode(code);
  const normalizedName = String(name || "").trim();
  if (!normalizedCode || !normalizedName) return false;

  stocks[normalizedCode] = {
    code: normalizedCode,
    name: normalizedName,
    market,
    symbol: symbol || `${normalizedCode}.${market === "TPEX" ? "TWO" : "TW"}`,
    aliases: getCommonEtfAliases(normalizedCode),
  };
  return true;
}

function getStockCandidateSymbols(stock) {
  const code = normalizeStockCode(stock?.code);
  const symbols = [stock?.symbol].filter(Boolean);
  if (code) {
    symbols.push(`${code}.TW`, `${code}.TWO`);
  }
  return [...new Set(symbols)];
}

function findCommonEtfByAlias(query, stocks = {}) {
  const normalizedQuery = normalizeStockText(query);
  const candidates = Object.entries(COMMON_TW_ETF_ALIASES)
    .flatMap(([code, info]) =>
      [info.name, ...(info.aliases || [])].map((alias) => ({
        code,
        name: info.name,
        alias,
        normalizedAlias: normalizeStockText(alias),
      }))
    )
    .filter((item) => item.normalizedAlias)
    .sort((a, b) => b.normalizedAlias.length - a.normalizedAlias.length);

  const matched = candidates.find((item) =>
    normalizedQuery.includes(item.normalizedAlias)
  );
  if (!matched) return null;

  return (
    stocks[matched.code] || {
      code: matched.code,
      name: matched.name,
      market: "TWSE",
      symbol: `${matched.code}.TW`,
      aliases: getCommonEtfAliases(matched.code),
    }
  );
}

function parseCsvLine(line) {
  const s = String(line || "").replace(/\r/g, "");
  const out = [];
  let cur = "";
  let inQuotes = false;

  for (let i = 0; i < s.length; i++) {
    const ch = s[i];

    if (inQuotes) {
      if (ch === '"') {
        if (s[i + 1] === '"') {
          cur += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        cur += ch;
      }
    } else if (ch === '"') {
      inQuotes = true;
    } else if (ch === ",") {
      out.push(cur);
      cur = "";
    } else {
      cur += ch;
    }
  }

  out.push(cur);
  return out.map((x) => x.replace(/^\uFEFF/, "").trim());
}

function parseTwseOpenApiStocks(items) {
  const stocks = {};
  if (!Array.isArray(items)) return stocks;

  for (const item of items) {
    addStockRecord(stocks, {
      code: item?.["證券代號"] || item?.Code || item?.code,
      name: item?.["證券名稱"] || item?.Name || item?.name,
      market: "TWSE",
    });
  }

  return stocks;
}

function parseTwseCsvStocks(text) {
  const allLines = String(text || "")
    .split(/\n/)
    .map((l) => l.trim())
    .filter(Boolean);

  const headerIndex = allLines.findIndex(
    (l) => l.includes("證券代號") && l.includes("證券名稱")
  );
  if (headerIndex < 0) return { stocks: {}, headerIndex, header: [] };

  const header = parseCsvLine(allLines[headerIndex]);
  const codeIndex = header.findIndex((c) => c.includes("證券代號"));
  const nameIndex = header.findIndex((c) => c.includes("證券名稱"));
  const stocks = {};

  if (codeIndex < 0 || nameIndex < 0) {
    return { stocks, headerIndex, header };
  }

  for (let i = headerIndex + 1; i < allLines.length; i++) {
    const cols = parseCsvLine(allLines[i]);
    addStockRecord(stocks, {
      code: cols[codeIndex],
      name: cols[nameIndex],
      market: "TWSE",
    });
  }

  return { stocks, headerIndex, header };
}

function parseTpexJsonStocks(json) {
  const stocks = {};
  const tables = Array.isArray(json?.tables) ? json.tables : [];

  for (const table of tables) {
    const fields = Array.isArray(table?.fields) ? table.fields : [];
    const data = Array.isArray(table?.data) ? table.data : [];
    const codeIndex = fields.findIndex((f) => String(f).includes("代號"));
    const nameIndex = fields.findIndex((f) => String(f).includes("名稱"));

    if (codeIndex < 0 || nameIndex < 0) continue;

    for (const row of data) {
      if (!Array.isArray(row)) continue;
      addStockRecord(stocks, {
        code: row[codeIndex],
        name: row[nameIndex],
        market: "TPEX",
      });
    }
  }

  return stocks;
}

async function findStock(query) {
  console.log("findStock query =", query);
  const raw = await redisGet(STOCKS_REDIS_KEY);
  let stocks = {};

  if (raw) {
    try {
      stocks = JSON.parse(raw);
    } catch (err) {
      console.error("stock cache parse failed:", err);
      stocks = {};
    }
  }

  const q = query.trim();

  // 1. 從句子中抓台股/ETF 代號，支援 00980A 這類英數代號。
  const code = extractStockCodeFromQuery(q);
  if (code) {
    if (stocks[code]) return stocks[code];
    if (COMMON_TW_ETF_ALIASES[code]) {
      return {
        code,
        name: COMMON_TW_ETF_ALIASES[code].name,
        market: "TWSE",
        symbol: `${code}.TW`,
        aliases: getCommonEtfAliases(code),
      };
    }
    return {
      code,
      name: code,
      market: "UNKNOWN",
      symbol: `${code}.TW`,
      aliases: [],
    };
  }

  // 2. 常見 ETF 別名優先，讓「高股息」「台灣50」這類問法能命中。
  const aliasMatched = findCommonEtfByAlias(q, stocks);
  if (aliasMatched) return aliasMatched;

  // 3. 名稱模糊（台積電 / 鴻海 / ETF 全名）。
  const normalizedQuery = normalizeStockText(q);
  return (
    Object.values(stocks).find((s) => {
      const names = [s.name, ...(Array.isArray(s.aliases) ? s.aliases : [])];
      return names.some((name) =>
        normalizedQuery.includes(normalizeStockText(name))
      );
    }) || null
  );
}

async function getStockQuote(symbol) {
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${symbol}?interval=1d&range=5d`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), STOCK_QUOTE_TIMEOUT_MS);

  let res;
  try {
    res = await fetch(url, {
      signal: controller.signal,
      headers: {
        "user-agent": "Mozilla/5.0",
        accept: "application/json",
      },
    });
  } catch (err) {
    console.error("Yahoo chart fetch failed", symbol, err?.message || err);
    return null;
  } finally {
    clearTimeout(timeout);
  }

  if (!res.ok) {
    const t = await res.text();
    console.error("Yahoo chart status", res.status, t.slice(0, 100));
    return null;
  }

  const json = await res.json();
  const result = json.chart?.result?.[0];
  if (!result) return null;

  const meta = result.meta || {};
  const quote = result.indicators?.quote?.[0] || {};
  const closes = result.indicators?.quote?.[0]?.close || [];
  const validCloses = closes.filter((v) => typeof v === "number");

  // ✅ 價格：優先用 regularMarketPrice，不行就用最後一根 close
  const price =
    meta.regularMarketPrice ??
    validCloses.slice(-1)[0];

  // ✅ 開盤價
  const open =
    meta.regularMarketOpen ??
    quote.open?.filter((v) => typeof v === "number")[0];

  // Yahoo 對部分 .TWO 商品的 previousClose 會不穩，改用多個來源交叉備援。
  const previousValidClose =
    validCloses.length >= 2 ? validCloses[validCloses.length - 2] : undefined;
  const prevCloseCandidates = [
    meta.chartPreviousClose,
    meta.previousClose,
    previousValidClose,
  ].filter((v) => typeof v === "number" && v > 0);
  const prevClose =
    prevCloseCandidates.find((v) => Math.abs(v - price) > 0.000001) ??
    prevCloseCandidates[0];

  if (typeof price !== "number" || typeof prevClose !== "number") {
    return null;
  }

  const changeRaw = price - prevClose;
  const change = Math.abs(changeRaw) < 0.005 ? 0 : changeRaw;
  const changePercentRaw = (change / prevClose) * 100;
  const changePercent = Math.abs(changePercentRaw) < 0.005 ? 0 : changePercentRaw;

  return {
    price,
    open,
    prevClose,
    change,
    changePercent,
    volume: meta.regularMarketVolume,
  };
}

async function getStockQuoteWithFallback(stock) {
  const realtime = await fetchTwseMisRealtime(stock);
  if (
    realtime &&
    typeof realtime.price === "number" &&
    typeof realtime.previousClose === "number" &&
    realtime.previousClose > 0
  ) {
    const changeRaw = realtime.price - realtime.previousClose;
    const change = Math.abs(changeRaw) < 0.005 ? 0 : changeRaw;
    const changePercentRaw = (change / realtime.previousClose) * 100;
    const changePercent =
      Math.abs(changePercentRaw) < 0.005 ? 0 : changePercentRaw;
    return {
      quote: {
        price: realtime.price,
        open: realtime.open,
        prevClose: realtime.previousClose,
        change,
        changePercent,
        volume:
          typeof realtime.cumulativeVolume === "number"
            ? realtime.cumulativeVolume * 1000
            : undefined,
      },
      symbol: stock.symbol || `${stock.code}.TW`,
      source: realtime.source,
    };
  }

  for (const symbol of getStockCandidateSymbols(stock)) {
    const quote = await getStockQuote(symbol);
    if (quote) return { quote, symbol };
  }
  return null;
}

function getStockMarketLabelBySymbol(symbol, stock) {
  if (symbol?.endsWith(".TWO") || stock?.market === "TPEX") return "上櫃";
  if (symbol?.endsWith(".TW") || stock?.market === "TWSE") return "上市";
  return "台股";
}

function getRealtimeExchangePrefix(stock) {
  return stock?.market === "TPEX" || stock?.symbol?.endsWith(".TWO")
    ? "otc"
    : "tse";
}

function parseMarketNumber(value) {
  const text = String(value ?? "")
    .trim()
    .replace(/,/g, "");
  if (!text || text === "-" || text === "--") return null;
  const n = Number(text);
  return Number.isFinite(n) ? n : null;
}

function timestampFromTwseMis(row) {
  const tlong = Number(row?.tlong);
  if (Number.isFinite(tlong) && tlong > 0) {
    return Math.floor(tlong / 1000);
  }

  const d = String(row?.d || "");
  const t = String(row?.t || "");
  const dateMatch = d.match(/^(\d{4})(\d{2})(\d{2})$/);
  const timeMatch = t.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (!dateMatch || !timeMatch) return Math.floor(Date.now() / 1000);

  return Math.floor(
    utcMsFromTaipeiParts({
      year: Number(dateMatch[1]),
      month: Number(dateMatch[2]),
      day: Number(dateMatch[3]),
      hour: Number(timeMatch[1]),
      minute: Number(timeMatch[2]),
    }) / 1000
  );
}

function isTaiwanTradingCollectionTime(nowMs = Date.now()) {
  const parts = taipeiPartsFromUtcMs(nowMs);
  const shifted = new Date(nowMs + TAIPEI_UTC_OFFSET_MINUTES * 60 * 1000);
  const weekday = shifted.getUTCDay();
  if (weekday === 0 || weekday === 6) return false;

  const minutes = parts.hour * 60 + parts.minute;
  return minutes >= 8 * 60 + 55 && minutes <= 13 * 60 + 35;
}

function getIntradaySnapshotKey(code, dateKey = taipeiDateKey(Date.now())) {
  return `stock:intraday:${dateKey}:${code}`;
}

async function fetchTwseMisRealtime(stock) {
  const code = normalizeStockCode(stock?.code);
  if (!code) return null;

  const exchangePrefix = getRealtimeExchangePrefix(stock);
  const exCh = `${exchangePrefix}_${code}.tw`;
  const url = `https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=${encodeURIComponent(
    exCh
  )}&json=1&delay=0&_=${Date.now()}`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), STOCK_REALTIME_TIMEOUT_MS);

  try {
    const res = await fetch(url, {
      signal: controller.signal,
      headers: {
        "user-agent": "Mozilla/5.0",
        accept: "application/json,text/plain,*/*",
        referer: `https://mis.twse.com.tw/stock/fibest.jsp?stock=${code}`,
      },
    });

    if (!res.ok) {
      const text = await res.text();
      console.warn("TWSE MIS realtime status", code, res.status, text.slice(0, 80));
      return null;
    }

    const json = await res.json();
    const row = Array.isArray(json?.msgArray) ? json.msgArray[0] : null;
    if (!row) return null;

    const price = parseMarketNumber(row.z);
    if (typeof price !== "number") return null;

    const open = parseMarketNumber(row.o) ?? price;
    const high = parseMarketNumber(row.h) ?? Math.max(open, price);
    const low = parseMarketNumber(row.l) ?? Math.min(open, price);
    const previousClose = parseMarketNumber(row.y);
    const cumulativeVolume = parseMarketNumber(row.v);
    const tradeVolume = parseMarketNumber(row.tv);

    return {
      code,
      name: row.n || stock.name || code,
      market: exchangePrefix === "otc" ? "TPEX" : "TWSE",
      timestamp: timestampFromTwseMis(row),
      open,
      high,
      low,
      close: price,
      price,
      previousClose,
      cumulativeVolume,
      tradeVolume,
      source: exchangePrefix === "otc" ? "tpex-realtime" : "twse-realtime",
    };
  } catch (err) {
    console.warn("TWSE MIS realtime fetch failed", code, err?.message || err);
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

async function rememberIntradayWatchStock(stock) {
  const code = normalizeStockCode(stock?.code);
  if (!code) return false;

  return redisSAdd(
    STOCK_INTRADAY_WATCHLIST_KEY,
    JSON.stringify({
      code,
      name: stock.name || code,
      market: stock.market || "TWSE",
      symbol: stock.symbol || `${code}.TW`,
    })
  );
}

async function collectWatchedStockIntradaySnapshots() {
  if (!isTaiwanTradingCollectionTime()) {
    return { watched: 0, collected: 0, skipped: "outside-trading-time" };
  }

  const members = await redisSMembers(STOCK_INTRADAY_WATCHLIST_KEY);
  const limit =
    Number.isFinite(STOCK_INTRADAY_COLLECT_LIMIT) &&
    STOCK_INTRADAY_COLLECT_LIMIT > 0
      ? STOCK_INTRADAY_COLLECT_LIMIT
      : 20;
  const watchedStocks = members
    .slice(0, limit)
    .map((raw) => {
      try {
        return JSON.parse(raw);
      } catch {
        return null;
      }
    })
    .filter((stock) => normalizeStockCode(stock?.code));

  let collected = 0;

  for (const stock of watchedStocks) {
    const snapshot = await fetchTwseMisRealtime(stock);
    if (!snapshot) continue;

    const key = getIntradaySnapshotKey(snapshot.code);
    const latestRaw = await redisLRange(key, -1, -1);
    let latest = null;
    try {
      latest = latestRaw[0] ? JSON.parse(latestRaw[0]) : null;
    } catch {
      latest = null;
    }
    if (latest?.timestamp === snapshot.timestamp) continue;

    const saved = await redisRPush(key, JSON.stringify(snapshot));
    if (!saved) continue;

    const maxPoints =
      Number.isFinite(STOCK_INTRADAY_MAX_POINTS) &&
      STOCK_INTRADAY_MAX_POINTS > 0
        ? STOCK_INTRADAY_MAX_POINTS
        : 320;
    await redisLTrim(key, -maxPoints, -1);
    await redisExpire(
      key,
      Number.isFinite(STOCK_INTRADAY_TTL_SECONDS) &&
        STOCK_INTRADAY_TTL_SECONDS > 0
        ? STOCK_INTRADAY_TTL_SECONDS
        : 60 * 60 * 36
    );
    collected++;
  }

  return { watched: watchedStocks.length, collected };
}

async function getCollectedIntradayHistory(stock) {
  const code = normalizeStockCode(stock?.code);
  if (!code) return null;

  const raw = await redisLRange(getIntradaySnapshotKey(code), 0, -1);
  const snapshots = raw
    .map((item) => {
      try {
        return JSON.parse(item);
      } catch {
        return null;
      }
    })
    .filter((p) => p && typeof p.close === "number" && Number.isFinite(p.timestamp))
    .sort((a, b) => a.timestamp - b.timestamp);

  const minPoints =
    Number.isFinite(STOCK_INTRADAY_MIN_POINTS) && STOCK_INTRADAY_MIN_POINTS > 0
      ? STOCK_INTRADAY_MIN_POINTS
      : 3;
  if (snapshots.length < minPoints) return null;

  const points = snapshots.map((point, index) => {
    const previous = snapshots[index - 1];
    const cumulative = point.cumulativeVolume;
    const previousCumulative = previous?.cumulativeVolume;
    const volume =
      typeof cumulative === "number" && typeof previousCumulative === "number"
        ? Math.max(cumulative - previousCumulative, 0)
        : point.tradeVolume || 0;

    return {
      timestamp: point.timestamp,
      label: formatStockChartLabel(point.timestamp, "intraday"),
      open: point.open,
      high: point.high,
      low: point.low,
      close: point.close,
      volume,
    };
  });

  return {
    symbol: stock.symbol || `${code}.TW`,
    chartType: "intraday",
    displayChartType: "intraday",
    range: "1d",
    interval: "realtime",
    source: "twse-tpex-realtime",
    points: downsamplePoints(points, 75),
  };
}

function getPublicBaseUrl() {
  const raw =
    process.env.PUBLIC_BASE_URL ||
    process.env.LINE_BOT_BASE_URL ||
    (process.env.VERCEL_URL ? `https://${process.env.VERCEL_URL}` : "") ||
    "https://line-gpt-kevin.vercel.app";
  return raw.replace(/\/+$/, "");
}

function getStockChartMenuMessage(stock) {
  const marketLabel = getStockMarketLabelBySymbol(stock?.symbol, stock);
  const code = stock.code;
  const name = stock.name || code;

  return {
    type: "text",
    text: `請選擇 ${name}（${code}｜${marketLabel}）的線圖類型`,
    quickReply: {
      items: [
        {
          type: "action",
          action: {
            type: "message",
            label: "當日分時圖",
            text: `助理 ${code} 分時`,
          },
        },
        {
          type: "action",
          action: {
            type: "message",
            label: "日 K 線圖",
            text: `助理 ${code} K線`,
          },
        },
      ],
    },
  };
}

function formatStockChartLabel(timestampSeconds, chartType, range = "") {
  const date = new Date(timestampSeconds * 1000);
  if (chartType === "intraday") {
    if (range === "5d") {
      const monthDay = date.toLocaleDateString("zh-TW", {
        timeZone: "Asia/Taipei",
        month: "2-digit",
        day: "2-digit",
      });
      const time = date.toLocaleTimeString("zh-TW", {
        timeZone: "Asia/Taipei",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
      });
      return `${monthDay} ${time}`;
    }
    return date.toLocaleTimeString("zh-TW", {
      timeZone: "Asia/Taipei",
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    });
  }
  return date.toLocaleDateString("zh-TW", {
    timeZone: "Asia/Taipei",
    month: "2-digit",
    day: "2-digit",
  });
}

function downsamplePoints(points, maxPoints) {
  if (points.length <= maxPoints) return points;
  const step = Math.ceil(points.length / maxPoints);
  return points.filter((_, i) => i % step === 0 || i === points.length - 1);
}

function takeLastPoints(points, maxPoints) {
  if (points.length <= maxPoints) return points;
  return points.slice(-maxPoints);
}

function movingAverage(values, period) {
  return values.map((_, index) => {
    if (index + 1 < period) return null;
    const slice = values.slice(index + 1 - period, index + 1);
    const valid = slice.filter((v) => typeof v === "number");
    if (valid.length < period) return null;
    return Number((valid.reduce((sum, v) => sum + v, 0) / period).toFixed(2));
  });
}

function getStockChartTitleLabel(stock, history) {
  const marketLabel = getStockMarketLabelBySymbol(history.symbol, stock);
  const displayChartType = history.displayChartType || history.chartType;

  if (displayChartType === "intraday") {
    return `${stock.name} (${stock.code} | ${marketLabel}) 當日分時`;
  }

  if (displayChartType === "intraday5d") {
    return `${stock.name} (${stock.code} | ${marketLabel}) 近 5 日分時`;
  }

  return `${stock.name} (${stock.code} | ${marketLabel}) 日 K 走勢`;
}

function escapeXml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function nicePriceTicks(min, max, count = 6) {
  if (!Number.isFinite(min) || !Number.isFinite(max) || min === max) {
    return [min || 0];
  }
  const rawStep = (max - min) / Math.max(count - 1, 1);
  const magnitude = 10 ** Math.floor(Math.log10(rawStep));
  const normalized = rawStep / magnitude;
  const niceNormalized =
    normalized <= 1 ? 1 : normalized <= 2 ? 2 : normalized <= 5 ? 5 : 10;
  const step = niceNormalized * magnitude;
  const start = Math.floor(min / step) * step;
  const end = Math.ceil(max / step) * step;
  const ticks = [];
  for (let v = start; v <= end + step * 0.5; v += step) {
    ticks.push(Number(v.toFixed(2)));
  }
  return ticks.slice(-8);
}

function svgPolyline(points, valueKey, xForIndex, yForPrice) {
  const parts = [];
  let current = [];

  points.forEach((point, index) => {
    const value = point[valueKey];
    if (typeof value !== "number") {
      if (current.length) {
        parts.push(current);
        current = [];
      }
      return;
    }
    current.push(`${xForIndex(index).toFixed(2)},${yForPrice(value).toFixed(2)}`);
  });

  if (current.length) parts.push(current);
  return parts
    .filter((segment) => segment.length >= 2)
    .map((segment) => `<polyline points="${segment.join(" ")}" />`)
    .join("");
}

function buildStockKLineSvg({ stock, symbol, points }) {
  const width = 1200;
  const height = 720;
  const chartX = 78;
  const chartY = 112;
  const chartW = 1040;
  const chartH = 405;
  const volumeY = 555;
  const volumeH = 92;
  const axisY = 676;
  const n = points.length;
  const marketLabel = getStockMarketLabelBySymbol(symbol, stock);
  const xStep = n > 1 ? chartW / (n - 1) : chartW;
  const candleW = Math.max(4, Math.min(10, xStep * 0.58));
  const volumeW = Math.max(2, Math.min(8, xStep * 0.45));
  const priceValues = [];

  points.forEach((p) => {
    [p.high, p.low, p.open, p.close, p.ma5, p.ma20, p.ma60].forEach((value) => {
      if (typeof value === "number") priceValues.push(value);
    });
  });

  const rawMin = Math.min(...priceValues);
  const rawMax = Math.max(...priceValues);
  const pad = Math.max((rawMax - rawMin) * 0.08, rawMax * 0.01, 0.2);
  const minPrice = rawMin - pad;
  const maxPrice = rawMax + pad;
  const priceTicks = nicePriceTicks(minPrice, maxPrice, 6);
  const maxVolume = Math.max(
    ...points.map((p) => (typeof p.volume === "number" ? p.volume : 0)),
    1
  );
  const xForIndex = (index) => chartX + index * xStep;
  const yForPrice = (price) =>
    chartY + ((maxPrice - price) / (maxPrice - minPrice)) * chartH;
  const yForVolume = (volume) =>
    volumeY + volumeH - (Math.max(volume, 0) / maxVolume) * volumeH;
  const dateTickIndexes = [...new Set([
    0,
    Math.round((n - 1) * 0.2),
    Math.round((n - 1) * 0.4),
    Math.round((n - 1) * 0.6),
    Math.round((n - 1) * 0.8),
    n - 1,
  ])].filter((i) => i >= 0 && i < n);

  const priceGrid = priceTicks
    .map((tick) => {
      const y = yForPrice(tick);
      return `
        <line x1="${chartX}" y1="${y}" x2="${chartX + chartW}" y2="${y}" stroke="rgba(0,0,0,0.07)" />
        <text x="${chartX - 14}" y="${y + 5}" text-anchor="end" class="price-label">${tick.toFixed(2)}</text>
      `;
    })
    .join("");

  const dateLabels = dateTickIndexes
    .map((index) => {
      const x = xForIndex(index);
      const label = points[index]?.label || "";
      return `
        <line x1="${x}" y1="${chartY}" x2="${x}" y2="${volumeY + volumeH}" stroke="rgba(0,0,0,0.045)" />
        <text x="${x}" y="${axisY}" text-anchor="middle" class="date-label">${escapeXml(label)}</text>
      `;
    })
    .join("");

  const candles = points
    .map((point, index) => {
      const x = xForIndex(index);
      const open = typeof point.open === "number" ? point.open : point.close;
      const close = point.close;
      const high = typeof point.high === "number" ? point.high : Math.max(open, close);
      const low = typeof point.low === "number" ? point.low : Math.min(open, close);
      const up = close >= open;
      const color = up ? "#df3f50" : "#079b55";
      const yHigh = yForPrice(high);
      const yLow = yForPrice(low);
      const yOpen = yForPrice(open);
      const yClose = yForPrice(close);
      const bodyY = Math.min(yOpen, yClose);
      const bodyH = Math.max(Math.abs(yClose - yOpen), 1.2);
      return `
        <line x1="${x}" y1="${yHigh}" x2="${x}" y2="${yLow}" stroke="#5f6368" stroke-width="1.3" />
        <rect x="${x - candleW / 2}" y="${bodyY}" width="${candleW}" height="${bodyH}" fill="${color}" stroke="${color}" rx="0.8" />
      `;
    })
    .join("");

  const volumes = points
    .map((point, index) => {
      const x = xForIndex(index);
      const open = typeof point.open === "number" ? point.open : points[index - 1]?.close ?? point.close;
      const up = point.close >= open;
      const color = up ? "rgba(223,63,80,0.34)" : "rgba(7,155,85,0.34)";
      const volume = typeof point.volume === "number" ? point.volume : 0;
      const y = yForVolume(volume);
      return `<rect x="${x - volumeW / 2}" y="${y}" width="${volumeW}" height="${volumeY + volumeH - y}" fill="${color}" />`;
    })
    .join("");

  const ma5 = svgPolyline(points, "ma5", xForIndex, yForPrice);
  const ma20 = svgPolyline(points, "ma20", xForIndex, yForPrice);
  const ma60 = svgPolyline(points, "ma60", xForIndex, yForPrice);
  const latest = points[points.length - 1] || {};
  const latestClose = typeof latest.close === "number" ? latest.close.toFixed(2) : "--";

  return `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">
  <style>
    .title { font: 700 30px -apple-system, BlinkMacSystemFont, "Noto Sans TC", "PingFang TC", Arial, sans-serif; fill: #222; }
    .subtitle { font: 500 15px -apple-system, BlinkMacSystemFont, "Noto Sans TC", "PingFang TC", Arial, sans-serif; fill: #777; }
    .legend { font: 500 15px -apple-system, BlinkMacSystemFont, "Noto Sans TC", "PingFang TC", Arial, sans-serif; fill: #777; }
    .price-label { font: 500 14px -apple-system, BlinkMacSystemFont, Arial, sans-serif; fill: #df3f50; }
    .date-label { font: 500 14px -apple-system, BlinkMacSystemFont, Arial, sans-serif; fill: #777; }
    .note { font: 500 13px -apple-system, BlinkMacSystemFont, "Noto Sans TC", "PingFang TC", Arial, sans-serif; fill: #888; }
    .ma5 { fill: none; stroke: #4f8cff; stroke-width: 2.1; stroke-linejoin: round; stroke-linecap: round; }
    .ma20 { fill: none; stroke: #f0a202; stroke-width: 2.1; stroke-linejoin: round; stroke-linecap: round; }
    .ma60 { fill: none; stroke: #d8a600; stroke-width: 2.0; stroke-linejoin: round; stroke-linecap: round; }
  </style>
  <rect width="100%" height="100%" fill="#ffffff" />
  <text x="${width / 2}" y="42" text-anchor="middle" class="title">${escapeXml(stock.name)}（${escapeXml(stock.code)}｜${escapeXml(marketLabel)}）日 K 走勢</text>
  <text x="${width / 2}" y="68" text-anchor="middle" class="subtitle">最新收盤 ${latestClose}｜資料來源 Yahoo Finance｜日 K 通常於收盤後更新</text>

  <g transform="translate(418 92)">
    <rect x="0" y="-10" width="13" height="13" fill="#5f6368" /><text x="22" y="2" class="legend">K棒</text>
    <line x1="76" y1="-4" x2="101" y2="-4" stroke="#4f8cff" stroke-width="3" /><text x="110" y="2" class="legend">5MA</text>
    <line x1="164" y1="-4" x2="189" y2="-4" stroke="#f0a202" stroke-width="3" /><text x="198" y="2" class="legend">20MA</text>
    <line x1="260" y1="-4" x2="285" y2="-4" stroke="#d8a600" stroke-width="3" /><text x="294" y="2" class="legend">60MA</text>
    <rect x="370" y="-10" width="13" height="13" fill="rgba(223,63,80,0.34)" /><text x="392" y="2" class="legend">成交量</text>
  </g>

  <rect x="${chartX}" y="${chartY}" width="${chartW}" height="${chartH}" fill="#fff" stroke="rgba(0,0,0,0.12)" />
  ${priceGrid}
  ${dateLabels}
  <g>${candles}</g>
  <g class="ma5">${ma5}</g>
  <g class="ma20">${ma20}</g>
  <g class="ma60">${ma60}</g>

  <rect x="${chartX}" y="${volumeY}" width="${chartW}" height="${volumeH}" fill="#fff" stroke="rgba(0,0,0,0.08)" />
  <g>${volumes}</g>
  <text x="${chartX}" y="${volumeY - 8}" class="note">成交量</text>
  <text x="${chartX}" y="${height - 18}" class="note">台股紅漲綠跌；均線以完整 6 個月日資料計算，顯示最近 ${n} 根。</text>
</svg>`;
}

function getStockKLineSvgUrl(stock) {
  const params = new URLSearchParams({
    code: stock.code,
    t: String(Date.now()),
  });
  return `${getPublicBaseUrl()}/api/stock-kline.svg?${params.toString()}`;
}

async function getStockHistory(symbol, { range, interval, chartType }) {
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${symbol}?interval=${interval}&range=${range}`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), STOCK_HISTORY_TIMEOUT_MS);

  let res;
  try {
    res = await fetch(url, {
      signal: controller.signal,
      headers: {
        "user-agent": "Mozilla/5.0",
        accept: "application/json",
      },
    });
  } catch (err) {
    console.error("Yahoo history fetch failed", symbol, err?.message || err);
    return null;
  } finally {
    clearTimeout(timeout);
  }

  if (!res.ok) {
    const t = await res.text();
    console.error("Yahoo history status", symbol, res.status, t.slice(0, 100));
    return null;
  }

  const json = await res.json();
  const result = json.chart?.result?.[0];
  const timestamps = result?.timestamp || [];
  const quote = result?.indicators?.quote?.[0] || {};
  const opens = quote.open || [];
  const highs = quote.high || [];
  const lows = quote.low || [];
  const closes = quote.close || [];
  const volumes = quote.volume || [];

  const points = timestamps
    .map((ts, index) => ({
      timestamp: ts,
      label: formatStockChartLabel(ts, chartType, range),
      open: opens[index],
      high: highs[index],
      low: lows[index],
      close: closes[index],
      volume: volumes[index],
    }))
    .filter((p) => typeof p.close === "number");

  if (points.length < 2) return null;
  if (chartType === "daily") {
    const allCloses = points.map((p) => p.close);
    const ma5 = movingAverage(allCloses, 5);
    const ma20 = movingAverage(allCloses, 20);
    const ma60 = movingAverage(allCloses, 60);
    points.forEach((point, index) => {
      point.ma5 = ma5[index];
      point.ma20 = ma20[index];
      point.ma60 = ma60[index];
    });
  }
  return chartType === "daily"
    ? takeLastPoints(points, 75)
    : downsamplePoints(points, 75);
}

async function getStockHistoryWithFallback(stock, chartType) {
  if (chartType === "intraday") {
    const collected = await getCollectedIntradayHistory(stock);
    if (collected) return collected;
  }

  const requests =
    chartType === "intraday"
      ? [
          {
            range: "1d",
            interval: "5m",
            chartType,
            displayChartType: "intraday",
            fallbackLabel: "",
          },
          {
            range: "5d",
            interval: "15m",
            chartType,
            displayChartType: "intraday5d",
            fallbackLabel: "近 5 日分時",
          },
        ]
      : [
          {
            range: "6mo",
            interval: "1d",
            chartType,
            displayChartType: "daily",
            fallbackLabel: "",
          },
        ];

  for (const request of requests) {
    for (const symbol of getStockCandidateSymbols(stock)) {
      const points = await getStockHistory(symbol, request);
      if (points) {
        return {
          points,
          symbol,
          chartType,
          displayChartType: request.displayChartType,
          fallbackLabel: request.fallbackLabel,
        };
      }
    }
  }

  if (chartType === "intraday") {
    const daily = await getStockHistoryWithFallback(stock, "daily");
    if (daily) {
      return {
        ...daily,
        fallbackLabel: "分時資料暫時不足，改提供日 K 線圖",
      };
    }
  }

  return null;
}

function buildStockChartConfig({ stock, symbol, points, chartType, titleLabel }) {
  const labels = points.map((p) => p.label);
  const prices = points.map((p) => Number(p.close.toFixed(2)));
  const candleData = points.map((p) => ({
    x: p.label,
    o: Number((p.open ?? p.close).toFixed(2)),
    h: Number((p.high ?? p.close).toFixed(2)),
    l: Number((p.low ?? p.close).toFixed(2)),
    c: Number(p.close.toFixed(2)),
  }));
  const volumes = points.map((p) =>
    typeof p.volume === "number" ? Math.round(p.volume / 1000) : 0
  );
  const maxVolume = Math.max(...volumes, 1);
  const pointColors = points.map((point, index) => {
    const open = typeof point.open === "number" ? point.open : prices[index - 1] ?? point.close;
    return point.close >= open ? "#e04352" : "#0caa5a";
  });
  const volumeColors = pointColors.map((color) => {
    return color === "#e04352"
      ? "rgba(224, 67, 82, 0.22)"
      : "rgba(12, 170, 90, 0.22)";
  });
  const marketLabel = getStockMarketLabelBySymbol(symbol, stock);
  const title =
    titleLabel || `${stock.name} (${stock.code} | ${marketLabel}) 日 K 走勢`;
  const dataPoint = (label, value) =>
    typeof value === "number" ? { x: label, y: value } : { x: label, y: null };
  const datasets =
    chartType === "daily"
      ? [
          {
            type: "candlestick",
            label: "K棒",
            data: candleData,
            yAxisID: "price",
            color: {
              up: "#e04352",
              down: "#0caa5a",
              unchanged: "#777777",
            },
          },
        ]
      : [
          {
            type: "line",
            label: "價格",
            data: labels.map((label, index) => dataPoint(label, prices[index])),
            yAxisID: "price",
            borderColor: "#e04352",
            backgroundColor: "rgba(224, 67, 82, 0.12)",
            borderWidth: 2.4,
            pointRadius: 0,
            fill: true,
            tension: 0.22,
          },
        ];

  if (chartType === "daily") {
    datasets.push(
      {
        type: "line",
        label: "5MA",
        data: labels.map((label, index) => dataPoint(label, points[index].ma5)),
        yAxisID: "price",
        borderColor: "#4f8cff",
        borderWidth: 1.6,
        pointRadius: 0,
        fill: false,
        tension: 0.12,
      },
      {
        type: "line",
        label: "20MA",
        data: labels.map((label, index) => dataPoint(label, points[index].ma20)),
        yAxisID: "price",
        borderColor: "#f0a202",
        borderWidth: 1.6,
        pointRadius: 0,
        fill: false,
        tension: 0.12,
      },
      {
        type: "line",
        label: "60MA",
        data: labels.map((label, index) => dataPoint(label, points[index].ma60)),
        yAxisID: "price",
        borderColor: "#d8a600",
        borderWidth: 1.4,
        pointRadius: 0,
        fill: false,
        tension: 0.12,
      }
    );
  }

  if (chartType !== "daily") {
    datasets.push({
      type: "bar",
      label: "成交量(張)",
      data: labels.map((label, index) => dataPoint(label, volumes[index])),
      xAxisID: "x",
      yAxisID: "volume",
      backgroundColor: volumeColors,
      borderWidth: 0,
      barPercentage: 0.55,
      categoryPercentage: 0.75,
      maxBarThickness: 10,
      order: 20,
    });
  }

  return {
    type: chartType === "daily" ? "candlestick" : "line",
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      layout: {
        padding: {
          top: 8,
          right: 14,
          bottom: 4,
          left: 8,
        },
      },
      plugins: {
        title: {
          display: true,
          text: title,
          color: "#222222",
          font: {
            size: 22,
            weight: "bold",
          },
          padding: {
            top: 6,
            bottom: 12,
          },
        },
        legend: {
          display: true,
          position: "top",
          labels: {
            boxWidth: 12,
            boxHeight: 12,
            color: "#777777",
            font: { size: 13 },
            padding: 14,
          },
        },
      },
      scales: {
        x: {
          type: "category",
          ticks: {
            color: "#777777",
            autoSkip: true,
            maxTicksLimit: chartType === "intraday" ? 6 : 7,
            maxRotation: 0,
          },
          grid: { color: "rgba(0,0,0,0.055)" },
          border: { color: "rgba(0,0,0,0.18)" },
        },
        price: {
          type: "linear",
          position: "left",
          ticks: {
            color: "#e04352",
            padding: 8,
          },
          grid: { color: "rgba(0,0,0,0.065)" },
          border: { color: "rgba(0,0,0,0.18)" },
        },
        volume: {
          type: "linear",
          position: "right",
          beginAtZero: true,
          max: chartType === "daily" ? maxVolume * 4 : maxVolume * 2.4,
          ticks: {
            display: false,
          },
          grid: { display: false },
          border: { display: false },
        },
      },
    },
  };
}

function buildQuickChartLongUrl(chartConfig) {
  const query = new URLSearchParams({
    version: "4",
    width: "900",
    height: "520",
    backgroundColor: "white",
    c: JSON.stringify(chartConfig),
  });
  return `https://quickchart.io/chart?${query.toString()}`;
}

async function createQuickChartUrl(chartConfig) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), QUICKCHART_TIMEOUT_MS);

  try {
    const res = await fetch(QUICKCHART_CREATE_URL, {
      method: "POST",
      signal: controller.signal,
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        version: "4",
        width: 900,
        height: 520,
        backgroundColor: "white",
        chart: chartConfig,
      }),
    });

    const text = await res.text();
    if (res.ok) {
      const json = JSON.parse(text);
      if (json?.url && /^https:\/\//.test(json.url)) return json.url;
    }

    console.error("QuickChart create failed", res.status, text.slice(0, 100));
  } catch (err) {
    console.error("QuickChart create error", err?.message || err);
  } finally {
    clearTimeout(timeout);
  }

  const longUrl = buildQuickChartLongUrl(chartConfig);
  return longUrl.length < 1900 ? longUrl : null;
}

async function replyStockChart(event, stock, chartType) {
  await rememberIntradayWatchStock(stock);
  const history = await getStockHistoryWithFallback(stock, chartType);
  if (!history) {
    await replyMessageWithFallback(event, {
      type: "text",
      text: "線圖資料暫時取得失敗，請稍後再試。",
    });
    return;
  }

  const chartConfig = buildStockChartConfig({
    stock,
    symbol: history.symbol,
    points: history.points,
    chartType: history.chartType || chartType,
    titleLabel: getStockChartTitleLabel(stock, history),
  });
  const chartUrl = await createQuickChartUrl(chartConfig);
  if (!chartUrl) {
    await replyMessageWithFallback(event, {
      type: "text",
      text: "線圖產生失敗，請稍後再試。",
    });
    return;
  }

  const marketLabel = getStockMarketLabelBySymbol(history.symbol, stock);
  const resolvedChartType = history.chartType || chartType;
  const displayChartType = history.displayChartType || resolvedChartType;
  const typeLabel =
    history.fallbackLabel ||
    (resolvedChartType === "intraday" ? "當日分時圖" : "日 K 線圖");
  const noteText =
    displayChartType === "daily"
      ? "\n※ 日 K 資料通常於收盤後更新，盤中可能停在上一交易日。"
      : displayChartType === "intraday5d"
        ? "\n※ Yahoo 未提供完整當日分時時，會改用近 5 日分時資料。"
        : history.source === "twse-tpex-realtime"
          ? "\n※ 使用 TWSE/TPEX 盤中快照資料；新查詢股票會先收集幾分鐘後逐步完整。"
        : "";
  await replyMessageWithFallback(event, [
    {
      type: "text",
      text: `📈 ${stock.name}（${stock.code}｜${marketLabel}）${typeLabel}${noteText}`,
    },
    {
      type: "image",
      originalContentUrl: chartUrl,
      previewImageUrl: chartUrl,
    },
  ]);
}


function fmtTWPrice(n) {
  if (typeof n !== "number") return "--";
  const fixed = n >= 100 ? n.toFixed(1) : n.toFixed(2);
  return Object.is(Number(fixed), -0) ? fixed.replace("-", "") : fixed;
}
// 聖經小卡（50 節，適合每日抽）
const BIBLE_VERSES = [
  { ref: "約翰福音 3:16" },
  { ref: "詩篇 23:1" },
  { ref: "以賽亞書 41:10" },
  { ref: "馬太福音 11:28" },
  { ref: "羅馬書 8:28" },

  { ref: "詩篇 46:1" },
  { ref: "箴言 3:5" },
  { ref: "箴言 3:6" },
  { ref: "詩篇 34:4" },
  { ref: "詩篇 37:5" },

  { ref: "詩篇 119:105" },
  { ref: "以賽亞書 40:31" },
  { ref: "耶利米書 29:11" },
  { ref: "約書亞記 1:9" },
  { ref: "詩篇 55:22" },

  { ref: "詩篇 91:1" },
  { ref: "詩篇 121:1" },
  { ref: "詩篇 121:2" },
  { ref: "箴言 16:3" },
  { ref: "傳道書 3:1" },

  { ref: "馬太福音 6:34" },
  { ref: "馬太福音 7:7" },
  { ref: "馬太福音 5:16" },
  { ref: "馬太福音 28:20" },
  { ref: "約翰福音 14:27" },

  { ref: "約翰福音 16:33" },
  { ref: "羅馬書 12:2" },
  { ref: "羅馬書 15:13" },
  { ref: "哥林多前書 13:13" },
  { ref: "哥林多後書 5:7" },

  { ref: "加拉太書 6:9" },
  { ref: "以弗所書 3:20" },
  { ref: "以弗所書 6:10" },
  { ref: "腓立比書 4:6" },
  { ref: "腓立比書 4:7" },

  { ref: "腓立比書 4:13" },
  { ref: "歌羅西書 3:23" },
  { ref: "提摩太後書 1:7" },
  { ref: "希伯來書 11:1" },
  { ref: "希伯來書 13:5" },

  { ref: "雅各書 1:5" },
  { ref: "彼得前書 5:7" },
  { ref: "約翰一書 4:18" },
];

function buildBibleCardFlex({ verse, encouragement, reference }) {
  return {
    type: "flex",
    altText: `📖 今日經文｜${reference}`,
    contents: {
      type: "bubble",
      size: "mega",
      body: {
        type: "box",
        layout: "vertical",
        spacing: "md",
        contents: [
          {
            type: "text",
            text: "📖 今日一節",
            size: "sm",
            color: "#888888",
            weight: "bold",
          },
          {
            type: "text",
            text: verse,
            wrap: true,
            size: "lg",
            weight: "bold",
          },
          {
            type: "separator",
            margin: "lg",
          },
          {
            type: "text",
            text: encouragement,
            wrap: true,
            size: "md",
            color: "#555555",
          },
          {
            type: "text",
            text: `— ${reference}`,
            size: "sm",
            color: "#999999",
            align: "end",
            margin: "md",
          },
        ],
      },
    },
  };
}

async function fetchBibleVerse(ref) {
  const url = `https://bible-api.com/${encodeURIComponent(
    ref
  )}?translation=cuv`;

  const r = await fetch(url);
  const j = await r.json();

  return {
    verse: j.text.trim(),
    reference: j.reference,
  };
}
async function getTodayBibleCard() {
  const key = `bible:card:${new Date().toISOString().slice(0, 10)}`;
  const raw = await redisGet(key);
  return raw ? JSON.parse(raw) : null;
}
async function generateEncouragement(verseText) {
  const res = await openai.chat.completions.create({
    model: "gpt-4o-mini",
    messages: [
      {
        role: "system",
        content: "你是一位溫柔、不說教、不預言的文字陪伴者，只寫安靜的提醒。",
      },
      {
        role: "user",
        content: `請根據以下經文，寫 2~3 句溫柔的勉勵文字：\n${verseText}`,
      },
    ],
    max_tokens: 120,
  });

  return res.choices[0].message.content.trim();
}
async function generateBibleCardForDate(dateKey) {
  const index =
    Math.abs([...dateKey].reduce((a, c) => a + c.charCodeAt(0), 0)) %
    BIBLE_VERSES.length;

  const verseMeta = BIBLE_VERSES[index];
  const verseData = await fetchBibleVerse(verseMeta.ref);
  const encouragement = await generateEncouragement(verseData.verse);

  const payload = {
    date: dateKey,
    verse: verseData.verse,
    encouragement,
    reference: verseData.reference,
  };

  await redisSet(
    `bible:card:${dateKey}`,
    JSON.stringify(payload),
    "EX",
    60 * 60 * 24 * 40
  );

  return payload;
}

function extractAssistantText(payload) {
  if (!payload) return null;

  if (typeof payload === "string") {
    const t = payload.trim();
    return t || null;
  }

  const fromChoices = payload?.choices?.[0]?.message?.content;
  if (typeof fromChoices === "string" && fromChoices.trim()) {
    return fromChoices.trim();
  }
  if (Array.isArray(fromChoices)) {
    const text = fromChoices
      .map((x) => (typeof x?.text === "string" ? x.text : ""))
      .join("")
      .trim();
    if (text) return text;
  }

  const fromOutputText = payload?.output_text;
  if (typeof fromOutputText === "string" && fromOutputText.trim()) {
    return fromOutputText.trim();
  }

  const fromSimple = payload?.reply ?? payload?.text ?? payload?.message;
  if (typeof fromSimple === "string" && fromSimple.trim()) {
    return fromSimple.trim();
  }

  return null;
}

function isAbortLikeError(err) {
  if (!err) return false;
  if (err.name === "AbortError") return true;
  const msg = String(err?.message || err || "").toLowerCase();
  return msg.includes("aborted") || msg.includes("timeout");
}

function isComplexChatRequest(userText) {
  const t = String(userText || "").trim();
  if (!t) return false;

  if (
    Number.isFinite(OPENCLAW_COMPLEX_TEXT_LENGTH) &&
    OPENCLAW_COMPLEX_TEXT_LENGTH > 0 &&
    t.length >= OPENCLAW_COMPLEX_TEXT_LENGTH
  ) {
    return true;
  }

  if (t.split(/\n+/).length >= 3) return true;

  return /(分析|比較|規劃|策略|架構|程式|debug|錯誤|為什麼|詳細|步驟|總結|報告|翻譯|法律|醫療|投資|優化)/i.test(
    t
  );
}

function selectOpenClawRoute(userText) {
  if (!OPENCLAW_ROUTE_ENABLED) {
    return {
      reason: "route_disabled",
      model: OPENCLAW_MODEL_COMPLEX,
      timeoutMs: OPENCLAW_TIMEOUT_MS,
    };
  }

  if (isComplexChatRequest(userText)) {
    return {
      reason: "complex",
      model: OPENCLAW_MODEL_COMPLEX,
      timeoutMs: OPENCLAW_TIMEOUT_MS,
    };
  }

  return {
    reason: "simple",
    model: OPENCLAW_MODEL_SIMPLE,
    timeoutMs: OPENCLAW_TIMEOUT_MS_SIMPLE,
  };
}

async function requestOpenClawChat({
  systemPrompt,
  userText,
  model,
  timeoutMs,
  historyMessages = [],
}) {
  const controller = new AbortController();
  const timer = setTimeout(
    () => controller.abort(),
    Number.isFinite(timeoutMs) && timeoutMs > 0 ? timeoutMs : 20000
  );

  try {
    const headers = {
      "content-type": OPENCLAW_REQUEST_CONTENT_TYPE,
      accept: "application/json",
    };
    if (OPENCLAW_API_KEY) {
      headers.authorization = `Bearer ${OPENCLAW_API_KEY}`;
    }

    const payload = {
      model,
      stream: false,
      messages: [
        { role: "system", content: systemPrompt },
        ...normalizeChatHistory(historyMessages),
        { role: "user", content: userText },
      ],
    };

    const r = await fetch(OPENCLAW_CHAT_URL, {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
      signal: controller.signal,
    });
    const raw = await r.text();

    if (!r.ok) {
      throw new Error(`OpenClaw HTTP ${r.status}: ${(raw || "").slice(0, 200)}`);
    }

    let json;
    try {
      json = JSON.parse(raw);
    } catch {
      throw new Error(`OpenClaw 回傳不是 JSON: ${(raw || "").slice(0, 200)}`);
    }

    const text = extractAssistantText(json);
    if (text) return text;

    throw new Error("OpenClaw 回傳找不到文字內容");
  } finally {
    clearTimeout(timer);
  }
}

async function getGeneralAssistantReply(userText, conversationId = null) {
  const systemPrompt =
    "你是 Kevin 的專屬助理，語氣自然、冷靜又帶點幽默。你是 Kevin 自己架在 Vercel 上的 LINE Bot。回覆規則：1) 預設精簡，先直接回答重點。2) 預設 2-4 句，除非使用者要求詳細，否則不要長篇。3) 不要主動給 A/B 或 1-6 選單。4) 最多只問 1 個必要追問。5) 不要提及你有工作區、檔案記憶或系統內部機制。6) 不能假設自己有上網查詢能力；若缺即時資料，直接明講限制並給可行替代方案。";
  const historyMessages = await getRecentChatHistory(conversationId);

  async function finalizeReply(text, provider) {
    const compacted = compactGeneralReply(text);
    await appendRecentChatHistory(conversationId, userText, compacted);
    return { text: compacted, provider };
  }

  if (OPENCLAW_CHAT_URL) {
    const route = selectOpenClawRoute(userText);
    try {
      console.log("openclaw route:", route.reason, route.model);
      const text = await requestOpenClawChat({
        systemPrompt,
        userText,
        model: route.model,
        timeoutMs: route.timeoutMs,
        historyMessages,
      });
      return finalizeReply(text, "openclaw");
    } catch (primaryErr) {
      let lastErr = primaryErr;
      const primaryReason = primaryErr?.message || String(primaryErr);
      console.error("OpenClaw failed:", {
        reason: primaryReason,
        url: OPENCLAW_CHAT_URL,
        model: route.model,
        timeoutMs: route.timeoutMs,
        contentType: OPENCLAW_REQUEST_CONTENT_TYPE,
      });

      const resolvedRetryModel =
        OPENCLAW_RETRY_MODEL ||
        (route.model === OPENCLAW_MODEL_SIMPLE
          ? OPENCLAW_MODEL_COMPLEX
          : OPENCLAW_MODEL_SIMPLE);
      const canRetryWithFastModel =
        isAbortLikeError(primaryErr) &&
        resolvedRetryModel &&
        resolvedRetryModel !== route.model;

      if (canRetryWithFastModel) {
        try {
          const text = await requestOpenClawChat({
            systemPrompt,
            userText,
            model: resolvedRetryModel,
            timeoutMs: OPENCLAW_RETRY_TIMEOUT_MS,
            historyMessages,
          });
          console.warn("OpenClaw retry model success:", {
            model: resolvedRetryModel,
            timeoutMs: OPENCLAW_RETRY_TIMEOUT_MS,
          });
          return finalizeReply(text, "openclaw_retry");
        } catch (retryErr) {
          lastErr = retryErr;
          console.error("OpenClaw retry failed:", {
            reason: retryErr?.message || String(retryErr),
            model: resolvedRetryModel,
            timeoutMs: OPENCLAW_RETRY_TIMEOUT_MS,
          });
        }
      }

      if (OPENCLAW_FORCE_ONLY) {
        return {
          text: `OpenClaw 暫時不可用（${String(
            lastErr?.message || lastErr || "unknown"
          ).slice(0, 80)}），請稍後再試。`,
          provider: "openclaw_error",
        };
      }
      console.error("fallback to OpenAI");
    }
  } else if (OPENCLAW_FORCE_ONLY) {
    return {
      text: "OpenClaw 未設定（OPENCLAW_CHAT_URL 缺失），請先修正環境變數。",
      provider: "openclaw_error",
    };
  }

  const reply = await openai.chat.completions.create({
    model: "gpt-4o-mini",
    messages: [
      { role: "system", content: systemPrompt },
      ...normalizeChatHistory(historyMessages),
      { role: "user", content: userText },
    ],
  });

  const text =
    reply.choices?.[0]?.message?.content?.trim() || "我剛剛斷線了，再說一次";
  return finalizeReply(text, "openai");
}

function isCronAuthorized(req) {
  const secret = String(process.env.CRON_SECRET || "").trim();
  if (!secret) return false;

  if (req.headers.authorization === `Bearer ${secret}`) return true;

  const queryKey =
    typeof req.query?.key === "string" ? req.query.key.trim() : "";
  if (queryKey && queryKey === secret) return true;

  return false;
}

app.post("/api/generate-bible-cards", async (req, res) => {
  if (!isCronAuthorized(req)) {
    return res.status(401).json({ error: "unauthorized" });
  }

  const today = new Date();
  let created = 0;
  let skipped = 0;

  for (let i = 0; i < 30; i++) {
    const d = new Date(today);
    d.setDate(d.getDate() + i);
    const dateKey = d.toISOString().slice(0, 10);
    const redisKey = `bible:card:${dateKey}`;

    // ✅ 已存在就跳過
    const exists = await redisGet(redisKey);
    if (exists) {
      skipped++;
      continue;
    }

    const verseMeta = BIBLE_VERSES[i % BIBLE_VERSES.length];

    const verseData = await fetchBibleVerse(verseMeta.ref);
    const encouragement = await generateEncouragement(verseData.verse);

    const payload = {
      date: dateKey,
      verse: verseData.verse,
      encouragement,
      reference: verseData.reference,
    };

    await redisSet(
      redisKey,
      JSON.stringify(payload),
      "EX",
      60 * 60 * 24 * 40 // 40 天保險
    );

    created++;
  }

  res.json({
    ok: true,
    created,
    skipped,
  });
});

app.get("/api/run-reminders", async (req, res) => {
  if (!isCronAuthorized(req)) {
    return res.status(401).json({ error: "unauthorized" });
  }

  let scanned = 0;
  let sent = 0;
  let retried = 0;
  let failed = 0;
  let stockSnapshots = { watched: 0, collected: 0 };

  for (let i = 0; i < 5; i++) {
    const stats = await processDueReminders();
    scanned += stats.scanned;
    sent += stats.sent;
    retried += stats.retried;
    failed += stats.failed;

    if (stats.scanned < REMINDER_SCAN_BATCH) break;
  }

  stockSnapshots = await collectWatchedStockIntradaySnapshots();

  return res.json({
    ok: true,
    scanned,
    sent,
    retried,
    failed,
    stockSnapshots,
  });
});

app.post("/webhook", line.middleware(config), async (req, res) => {
  const events = req.body.events || [];

  for (const event of events) {
    try {
      if (event.type !== "message") continue;
      if (LINE_SKIP_REDELIVERY && event?.deliveryContext?.isRedelivery) {
        console.log("skip LINE redelivery event", {
          webhookEventId: event?.webhookEventId || null,
          messageId: event?.message?.id || null,
        });
        continue;
      }

      const shouldProcess = await shouldProcessLineEvent(event);
      if (!shouldProcess) {
        console.log("skip duplicated LINE event", {
          webhookEventId: event?.webhookEventId || null,
          messageId: event?.message?.id || null,
          isRedelivery: Boolean(event?.deliveryContext?.isRedelivery),
        });
        continue;
      }

      // ─────────────────────────────────────
      // 0️⃣ 群組 / 房間 gate（最外層）
      // ─────────────────────────────────────
      if (!isGroupAllowed(event)) continue;

      // ─────────────────────────────────────
      // 1️⃣ location message（最高優先）
      // ─────────────────────────────────────
      if (event.message.type === "location") {
        const { address, latitude, longitude } = event.message;

        const result = await getWeatherAndOutfit({
          lat: latitude,
          lon: longitude,
          address,
          when: "today",
        });

        await setLastWeatherContext(event.source.userId, {
          city: address,
          lat: latitude,
          lon: longitude,
        });

        await replyWeather(event, result);
        continue;
      }

      if (event.message.type !== "text") continue;
      const rawMessage = event.message.text.trim();
      const userMessage = rawMessage; // 判斷用（gate）
      const parsedMessage = stripBotName(rawMessage); // 邏輯用 / GPT 用
      const userId = event.source.userId;
      const conversationId = getConversationId(event);

      const parsedReminder = parseReminderCommand(parsedMessage || userMessage);
      if (parsedReminder) {
        const scheduled = await scheduleReminder(event, parsedReminder);
        if (!scheduled) {
          await replyMessageWithFallback(event, {
            type: "text",
            text: "提醒建立失敗（可能是暫時連不上資料庫），請晚點再試一次。",
          });
          continue;
        }

        const targetLabel =
          event.source.type === "user"
            ? "你"
            : event.source.type === "group"
            ? "本群"
            : "這個聊天室";
        await replyMessageWithFallback(event, {
          type: "text",
          text: `好，我會在 ${reminderDateLabel(
            scheduled.dueAt
          )} 提醒${targetLabel}：${scheduled.text}`,
        });
        continue;
      }

      // ─────────────────────────────────────
      // 🎴 媽祖抽籤指令
      // ─────────────────────────────────────
      if (/(抽籤|求籤|媽祖指示)/.test(userMessage)) {
        const lot = drawMazuLot();
        const advice = await explainLotPlain(lot.poem);

        const flex = buildMazuLotFlex({
          title: lot.title,
          poem: lot.poem,
          advice,
        });

        await replyMessageWithFallback(event, flex);
        continue;
      }
      // ─────────────────────────────────────
      // 🍽 食物熱量估算（支援多道菜）
      // ─────────────────────────────────────
      if (/吃了|熱量|卡路里/.test(userMessage)) {
        const foods = parseFoodList(userMessage);

        if (foods.length === 0) {
          await replyMessageWithFallback(event, {
            type: "text",
            text: "你吃了什麼？可以一次列多道菜喔 😄",
          });
          continue;
        }

        const results = [];
        let totalMin = 0;
        let totalMax = 0;

        for (const food of foods) {
          const r = await estimateFoodCalorie(food);
          results.push(r);
          totalMin += r.min;
          totalMax += r.max;
        }

        // 文字版（先穩）
        const lines = results.map(
          (r) => `• ${r.food}：${r.min}～${r.max} 大卡`
        );

        lines.push("");
        lines.push(`👉 總熱量：約 ${totalMin}～${totalMax} 大卡`);
        lines.push("※ 快速估算，非精準營養計算");

        await replyMessageWithFallback(event, {
          type: "text",
          text: lines.join("\n"),
        });

        continue;
      }

      // ─────────────────────────────────────
      // 📈 股票線圖（Quick Reply + QuickChart）
      // ─────────────────────────────────────
      if (/線圖|走勢|K線|日K|分時/.test(parsedMessage)) {
        const stock = await findStock(parsedMessage);

        if (!stock) {
          await replyMessageWithFallback(event, {
            type: "text",
            text: "我找不到這檔股票 😅\n可以試試「3221 線圖」或「2330 K線」",
          });
          continue;
        }

        if (/分時|當日/.test(parsedMessage)) {
          await replyStockChart(event, stock, "intraday");
          continue;
        }

        if (/K線|日K|日線/.test(parsedMessage)) {
          await replyStockChart(event, stock, "daily");
          continue;
        }

        await replyMessageWithFallback(event, getStockChartMenuMessage(stock));
        continue;
      }

      // ─────────────────────────────────────
      // 📈 股票行情查詢（完整版，Redis + Yahoo）
      // ─────────────────────────────────────
      if (/行情|股價|多少錢/.test(userMessage)) {
        const cleaned = stripBotName(userMessage);

        // 👉 用你已經寫好的 findStock
        const stock = await findStock(cleaned);

        if (!stock) {
          await replyMessageWithFallback(event, {
            type: "text",
            text: "我找不到這檔股票 😅\n可以試試「2330 行情」或「台積電 股價」",
          });
          continue;
        }

        try {
          const result = await getStockQuoteWithFallback(stock);
          if (!result) throw new Error("no data");
          const q = result.quote;

          const sign = q.change > 0 ? "+" : "";
          const percent =
            typeof q.changePercent === "number"
              ? q.changePercent.toFixed(2)
              : "--";
          const percentSign = q.changePercent > 0 ? "+" : "";
          const volumeLots =
            typeof q.volume === "number"
              ? Math.round(q.volume / 1000).toLocaleString()
              : "--";
          const marketLabel =
            result.symbol.endsWith(".TWO") ? "上櫃" : "上市";
          const sourceNote = result.source?.includes("realtime")
            ? "※ 資料來源：TWSE/TPEX 盤中即時快照"
            : "※ 資料來源：Yahoo Finance（延遲報價）";

          const text = `📊 ${stock.name}（${stock.code}｜${marketLabel}）

現價：${fmtTWPrice(q.price)}
漲跌：${sign}${fmtTWPrice(q.change)}（${percentSign}${percent}%）
開盤：${fmtTWPrice(q.open)}
昨收：${fmtTWPrice(q.prevClose)}
成交量：${volumeLots} 張

${sourceNote}`;

          await replyMessageWithFallback(event, {
            type: "text",
            text,
          });
        } catch (err) {
          console.error("Stock error:", err);
          await replyMessageWithFallback(event, {
            type: "text",
            text: "股價資料暫時取得失敗，請稍後再試。",
          });
        }

        continue; // 🔴 非常重要
      }

      // ─────────────────────────────────────
      // 聖經小卡
      // ─────────────────────────────────────
      if (/抽經文|今日經文|聖經小卡/.test(userMessage)) {
        const todayKey = new Date().toISOString().slice(0, 10);

        let card = await getTodayBibleCard();

        // 🧯 自救：沒有就立刻補
        if (!card) {
          card = await generateBibleCardForDate(todayKey);
        }

        const flex = buildBibleCardFlex(card);
        await replyMessageWithFallback(event, flex);
        continue;
      }

      // ─────────────────────────────────────
      // 星座運勢
      // ─────────────────────────────────────
      const cleanedMessage = userMessage.replace(
        /^(助理|KevinBot|kevinbot)\s*/i,
        ""
      );
      const zodiacMatch = cleanedMessage.match(
        /(牡羊|金牛|雙子|巨蟹|獅子|處女|天秤|天蠍|射手|摩羯|水瓶|雙魚)座/
      );

      const when =
        userMessage.includes("明天") || userMessage.includes("明日")
          ? "tomorrow"
          : "today";

      if (zodiacMatch) {
        const signZh = zodiacMatch[1];

        const result = await getDailyHoroscope(signZh, when);

        if (!result) {
          await replyMessageWithFallback(event, {
            type: "text",
            text: "這個星座我暫時還看不懂，再試一次？",
          });
          continue;
        }

        const whenLabel = when === "tomorrow" ? "明日" : "今日";

        const flex = buildHoroscopeFlexV2({
          signZh,
          signEn: ZODIAC_MAP[signZh],
          whenLabel,
          data: result,
        });

        await replyMessageWithFallback(event, flex);

        continue;
      }

      // ─────────────────────────────────────
      // 2️⃣ 只有時間（那明天呢 / 後天）
      // ─────────────────────────────────────
      const onlyWhen = /^(那)?(今天|明天|後天)(呢|啊)?$/.test(userMessage);

      if (onlyWhen) {
        const last = await getLastWeatherContext(userId);
        if (last) {
          const when = normalizeWhen(userMessage);

          const result = await getWeatherAndOutfit({
            city: last.city,
            when,
            lat: last.lat,
            lon: last.lon,
          });

          await replyWeather(event, result);
          continue;
        }
      }

      // ─────────────────────────────────────
      // 3️⃣ quickWeatherParse（不用 GPT）
      // ─────────────────────────────────────
      const quick = quickWeatherParse(userMessage);

      if (quick) {
        const last = await getLastWeatherContext(userId);
        const cityClean = cleanCity(
          quick.city || last?.city
        );
        const island = findTaiwanIsland(cityClean);
        const city = island ? island.name : fixTaiwanCity(cityClean);

        const result = await getWeatherAndOutfit({
          city,
          when: quick.when,
          lat: island?.lat,
          lon: island?.lon,
        });

        await setLastWeatherContext(userId, {
          city,
          lat: island?.lat,
          lon: island?.lon,
        });

        await replyWeather(event, result);
        continue;
      }

      // ─────────────────────────────────────
      // 4️⃣ GPT WEATHER intent
      // ─────────────────────────────────────
      const intent = await openai.chat.completions.create({
        model: "gpt-4o-mini",
        messages: [
          {
            role: "system",
            content:
              "你是一個意圖判斷與解析器。【地點判斷規則】1. 使用者提到的台灣城市（台北、台中、桃園、新竹、嘉義、台南、高雄、花蓮、宜蘭、馬祖、金門、澎湖、南竿、北竿、東引等）一律視為台灣的城市或離島。2. 如果只講「台中」「台南」「台北」這類簡稱，也必須自動解析為「台灣台中市」「台灣台南市」「台灣台北市」。3. 除非使用者明確說「中國的 XXX」，否則地點預設為台灣。4. 如果使用者提到「國家 + 城市」如「日本大阪」「韓國首爾」「美國紐約」，直接視為該國城市。5. 如果只講國際城市（如大阪、東京、紐約、巴黎等），推論最常見的國家（大阪→日本）。【意圖規則】如果訊息是在問天氣、氣溫、下雨、冷不冷、穿什麼，請回：WEATHER|城市名稱（英文名）|whenwhen 僅能是 today / tomorrow / day_after（使用者問「明天」就回 tomorrow，「後天」就回 day_after）其他請回：NO",
          },
          { role: "user", content: parsedMessage },
        ],
      });

      const intentText = intent.choices[0].message.content?.trim() ?? "NO";

      if (intentText.startsWith("WEATHER")) {
        const [, cityRaw, whenRaw] = intentText.split("|");
        const when = normalizeWhen(whenRaw || "today");

        const cityClean = cleanCity(cityRaw);
        const island = findTaiwanIsland(cityClean);

        const result = await getWeatherAndOutfit({
          city: island ? island.name : fixTaiwanCity(cityClean),
          when,
          lat: island?.lat,
          lon: island?.lon,
        });

        await setLastWeatherContext(userId, {
          city: island ? island.name : cityClean,
          lat: island?.lat,
          lon: island?.lon,
        });

        await replyWeather(event, result);
        continue;
      }

      // ─────────────────────────────────────
      // 5️⃣ 一般聊天（優先 OpenClaw，失敗 fallback OpenAI）
      // ─────────────────────────────────────
      const reply = await getGeneralAssistantReply(parsedMessage, conversationId);
      console.log("chat provider:", reply.provider);

      await replyMessageWithFallback(event, {
        type: "text",
        text: reply.text,
      });
    } catch (err) {
      if (isInvalidReplyTokenError(err)) {
        console.warn("skip invalid reply token event");
        continue;
      }
      console.error("Error handling event:", err);
    }
  }

  res.status(200).end();
});

app.get("/api/stock-kline.svg", async (req, res) => {
  const code = normalizeStockCode(String(req.query.code || ""));

  if (!code || !/^[0-9A-Z]{4,6}$/.test(code)) {
    return res.status(400).type("text/plain").send("invalid stock code");
  }

  try {
    const stock = await findStock(`${code} 股價`);
    if (!stock) {
      return res.status(404).type("text/plain").send("stock not found");
    }

    const history = await getStockHistoryWithFallback(stock, "daily");
    if (!history?.points?.length) {
      return res.status(502).type("text/plain").send("stock history unavailable");
    }

    const svg = buildStockKLineSvg({
      stock,
      symbol: history.symbol,
      points: history.points,
    });

    res.setHeader("Content-Type", "image/svg+xml; charset=utf-8");
    res.setHeader("Cache-Control", "no-store");
    res.status(200).send(svg);
  } catch (err) {
    console.error("stock kline svg failed:", err?.message || err);
    res.status(500).type("text/plain").send("stock chart failed");
  }
});

app.get("/api/update-stocks", async (req, res) => {
  if (!isCronAuthorized(req)) {
    return res.status(401).json({ error: "unauthorized" });
  }

  const debug = [];
  let stocks = {};
  let source = "";
  let twseCount = 0;
  let tpexCount = 0;

  try {
    const r = await fetch(TWSE_STOCKS_OPENAPI_URL, {
      headers: {
        "user-agent": "Mozilla/5.0",
        accept: "application/json",
      },
    });
    const contentType = r.headers.get("content-type") || "";
    const text = await r.text();
    const looksLikeHTML = /<html|<!doctype html|頁面無法執行/i.test(text);

    debug.push({
      source: "twse-openapi",
      status: r.status,
      contentType,
      looksLikeHTML,
      head120: text.slice(0, 120),
    });

    if (!r.ok || looksLikeHTML) {
      throw new Error(`TWSE OpenAPI unavailable: ${r.status}`);
    }

    stocks = parseTwseOpenApiStocks(JSON.parse(text));
    twseCount = Object.keys(stocks).length;
    source = "twse-openapi";
  } catch (err) {
    console.warn("TWSE OpenAPI stock update failed:", err?.message || err);
    debug.push({
      source: "twse-openapi",
      error: String(err?.message || err),
    });
  }

  if (Object.keys(stocks).length < MIN_TWSE_STOCK_COUNT) {
    try {
      const r = await fetch(TWSE_STOCKS_CSV_URL, {
        headers: {
          "user-agent": "Mozilla/5.0",
          accept: "text/csv,*/*;q=0.8",
        },
      });
      const contentType = r.headers.get("content-type") || "";
      const text = await r.text();
      const looksLikeHTML = /<html|<!doctype html|頁面無法執行/i.test(text);
      const linesRaw = text
        .split(/\n/)
        .slice(0, 5)
        .map((l) => l.slice(0, 200));
      const parsed = looksLikeHTML
        ? { stocks: {}, headerIndex: -1, header: [] }
        : parseTwseCsvStocks(text);

      debug.push({
        source: "twse-csv",
        status: r.status,
        contentType,
        looksLikeHTML,
        head120: text.slice(0, 120),
        first5Lines: linesRaw,
        headerIndex: parsed.headerIndex,
        header: parsed.header,
        count: Object.keys(parsed.stocks).length,
      });

      if (!r.ok || looksLikeHTML) {
        throw new Error(`TWSE CSV unavailable: ${r.status}`);
      }

      stocks = parsed.stocks;
      twseCount = Object.keys(stocks).length;
      source = "twse-csv";
    } catch (err) {
      console.warn("TWSE CSV stock update failed:", err?.message || err);
      debug.push({
        source: "twse-csv",
        error: String(err?.message || err),
      });
    }
  }

  try {
    const r = await fetch(TPEX_STOCKS_JSON_URL, {
      headers: {
        "user-agent": "Mozilla/5.0",
        accept: "application/json",
      },
    });
    const contentType = r.headers.get("content-type") || "";
    const text = await r.text();
    const looksLikeHTML = /<html|<!doctype html|頁面無法執行/i.test(text);

    if (!r.ok || looksLikeHTML) {
      throw new Error(`TPEX JSON unavailable: ${r.status}`);
    }

    const tpexStocks = parseTpexJsonStocks(JSON.parse(text));
    tpexCount = Object.keys(tpexStocks).length;
    if (tpexCount < MIN_TPEX_STOCK_COUNT) {
      throw new Error(`TPEX parsed too few records: ${tpexCount}`);
    }

    debug.push({
      source: "tpex-json",
      status: r.status,
      contentType,
      looksLikeHTML,
      count: tpexCount,
      head120: text.slice(0, 120),
    });

    Object.assign(stocks, tpexStocks);
    source = source ? `${source}+tpex-json` : "tpex-json";
  } catch (err) {
    console.warn("TPEX JSON stock update failed:", err?.message || err);
    debug.push({
      source: "tpex-json",
      error: String(err?.message || err),
    });
  }

  const count = Object.keys(stocks).length;
  const sampleParsed = Object.values(stocks).slice(0, 5);

  if (twseCount < MIN_TWSE_STOCK_COUNT) {
    return res.status(502).json({
      ok: false,
      error: "TWSE source parsed too few records; keep existing Redis cache",
      count,
      twseCount,
      tpexCount,
      minCount: MIN_TWSE_STOCK_COUNT,
      debug,
    });
  }

  const saved = await redisSet(STOCKS_REDIS_KEY, JSON.stringify(stocks));
  if (!saved) {
    return res.status(503).json({
      ok: false,
      error: "Redis unavailable; stock cache not updated",
      count,
      source,
      sampleParsed,
      debug,
    });
  }

  return res.json({
    ok: true,
    source,
    count,
    twseCount,
    tpexCount,
    debug: {
      sampleParsed,
      sources: debug,
    },
  });
});

// Default route
app.get("/", (req, res) => res.send("Kevin LINE GPT Bot Running"));

export default app;
