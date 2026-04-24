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
const twAdminDivisions = JSON.parse(
  fs.readFileSync(path.join(__dirname, "data", "tw-admin-divisions.json"), "utf8")
);
const epsQuarterlyHistory = JSON.parse(
  fs.readFileSync(
    path.join(__dirname, "data", "eps-quarterly-history.json"),
    "utf8"
  )
);

function createTaiVariants(value = "") {
  const text = String(value || "").trim();
  if (!text) return [];
  const variants = new Set([text]);
  if (text.includes("臺")) variants.add(text.replace(/臺/g, "台"));
  if (text.includes("台")) variants.add(text.replace(/台/g, "臺"));
  return [...variants].filter(Boolean);
}

function stripTaiwanAdminSuffix(name = "") {
  const text = String(name || "").trim();
  if (text.length <= 2) return "";
  return /[市鎮鄉區]$/.test(text) ? text.slice(0, -1) : "";
}

function buildCountyAliases(name = "") {
  const aliases = new Set(createTaiVariants(name));
  for (const variant of createTaiVariants(stripTaiwanAdminSuffix(name))) {
    aliases.add(variant);
  }
  return [...aliases].filter(Boolean);
}

function buildDistrictAliases(name = "") {
  const aliases = new Set(createTaiVariants(name));
  const stripped = stripTaiwanAdminSuffix(name);
  if (stripped) {
    for (const variant of createTaiVariants(stripped)) {
      aliases.add(variant);
    }
  }
  return [...aliases].filter(Boolean);
}

const COUNTY_ALIAS_OVERRIDES = {
  台北: "臺北市",
  臺北: "臺北市",
  台中: "臺中市",
  臺中: "臺中市",
  台南: "臺南市",
  臺南: "臺南市",
  台東: "臺東縣",
  臺東: "臺東縣",
  新竹: "新竹市",
  嘉義: "嘉義市",
  馬祖: "連江縣",
  南竿: "連江縣",
  北竿: "連江縣",
  東引: "連江縣",
};

function buildCountyAliasMap(counties = []) {
  const candidates = new Map();
  for (const county of counties) {
    const countyName = String(county?.name || "").trim();
    if (!countyName) continue;
    for (const alias of buildCountyAliases(countyName)) {
      if (!candidates.has(alias)) candidates.set(alias, new Set());
      candidates.get(alias).add(countyName);
    }
  }

  const map = {};
  for (const [alias, names] of candidates.entries()) {
    if (names.size === 1) {
      map[alias] = [...names][0];
    }
  }

  for (const [alias, countyName] of Object.entries(COUNTY_ALIAS_OVERRIDES)) {
    map[alias] = countyName;
  }

  return map;
}

function buildDistrictLookup(counties = []) {
  const aliasCandidates = new Map();
  const districts = [];

  for (const county of counties) {
    const countyName = String(county?.name || "").trim();
    const subdivisions = Array.isArray(county?.subdivisions) ? county.subdivisions : [];
    for (const rawName of subdivisions) {
      const displayName = String(rawName || "").trim();
      if (!displayName) continue;
      const district = {
        displayName,
        countyName,
        aliases: buildDistrictAliases(displayName),
      };
      districts.push(district);
      for (const alias of district.aliases) {
        if (!aliasCandidates.has(alias)) aliasCandidates.set(alias, []);
        aliasCandidates.get(alias).push({
          displayName,
          countyName,
        });
      }
    }
  }

  const aliasMap = {};
  const ambiguousAliases = new Set();
  const ambiguousCandidates = {};

  for (const [alias, candidates] of aliasCandidates.entries()) {
    const uniq = candidates.filter(
      (candidate, index, arr) =>
        arr.findIndex(
          (item) =>
            item.displayName === candidate.displayName &&
            item.countyName === candidate.countyName
        ) === index
    );

    if (uniq.length === 1) {
      aliasMap[alias] = uniq[0];
      continue;
    }

    ambiguousAliases.add(alias);
    ambiguousCandidates[alias] = uniq.sort((a, b) => {
      if (a.displayName !== b.displayName) {
        return a.displayName.localeCompare(b.displayName, "zh-Hant");
      }
      return a.countyName.localeCompare(b.countyName, "zh-Hant");
    });
  }

  return {
    districts: districts.sort((a, b) => {
      if (a.countyName !== b.countyName) {
        return a.countyName.localeCompare(b.countyName, "zh-Hant");
      }
      return a.displayName.localeCompare(b.displayName, "zh-Hant");
    }),
    aliasMap,
    ambiguousAliases,
    ambiguousCandidates,
  };
}

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
const USDA_API_KEY = String(process.env.USDA_API_KEY || "").trim();
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

const WEATHER_TIME_LABEL = {
  now: "現在",
  today: "今天",
  tonight: "今晚",
  tomorrow: "明天",
  tomorrow_morning: "明天早上",
  tomorrow_evening: "明天晚上",
  day_after: "後天",
  afternoon: "今天下午",
  evening: "今天晚上",
  soon: "接下來 1~3 小時",
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
const TW_WEATHER_LOCATIONS = [
  ...Object.keys(TW_CITY_MAP),
  "基隆",
  "苗栗",
  "彰化",
  "南投",
  "雲林",
  "屏東",
  "連江",
  "馬祖",
  "南竿",
  "北竿",
  "東引",
  "金門",
  "澎湖",
  "烏丘",
];
const GLOBAL_WEATHER_LOCATIONS = [
  "大阪",
  "東京",
  "京都",
  "札幌",
  "橫濱",
  "首爾",
  "釜山",
  "紐約",
  "巴黎",
  "倫敦",
  "新加坡",
  "香港",
  "曼谷",
];
const WEATHER_CONTEXT_TTL_SECONDS = Number(
  process.env.WEATHER_CONTEXT_TTL_SECONDS || 60 * 60 * 24
);
const CWA_API_KEY = String(process.env.CWA_API_KEY || "").trim();
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
const LINE_PUSH_TARGETS_KEY = "line:pushTargets";
const CWA_EARTHQUAKE_LAST_PUSHED_KEY = "cwa:earthquake:lastPushed";
const CWA_EARTHQUAKE_DATASET = "E-A0015-001";
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
    return sentenceParts.slice(0, 2).join("");
  }

  const lineParts = raw
    .split("\n")
    .map((x) => x.trim())
    .filter(Boolean);
  if (lineParts.length >= 2) {
    return lineParts.slice(0, 3).join("\n");
  }

  return `${raw.slice(0, Math.max(40, maxChars - 1))}…`;
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

async function rememberLinePushTarget(event) {
  const target = buildReminderTarget(event);
  if (!target?.targetId) return false;

  const payload = {
    targetType: target.targetType,
    targetId: target.targetId,
    sourceType: event?.source?.type || null,
  };

  return redisSAdd(LINE_PUSH_TARGETS_KEY, JSON.stringify(payload));
}

async function getKnownLinePushTargets() {
  const members = await redisSMembers(LINE_PUSH_TARGETS_KEY);
  const targetMap = new Map();

  for (const member of members) {
    let payload = null;
    try {
      payload = JSON.parse(member);
    } catch {
      continue;
    }

    const targetId = String(payload?.targetId || "").trim();
    if (!targetId) continue;
    const targetType = String(
      payload?.targetType || payload?.sourceType || "unknown"
    ).trim();
    const key = `${targetType}:${targetId}`;
    targetMap.set(key, {
      targetType,
      targetId,
    });
  }

  return [...targetMap.values()];
}

function maskIdentifier(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  if (text.length <= 8) {
    return `${text.slice(0, 2)}***${text.slice(-2)}`;
  }
  return `${text.slice(0, 4)}...${text.slice(-4)}`;
}

async function loadStockCacheDebugSummary() {
  let meta = null;
  let stocks = {};

  try {
    const rawMeta = await redisGet(STOCKS_META_REDIS_KEY);
    meta = rawMeta ? JSON.parse(rawMeta) : null;
  } catch {
    meta = null;
  }

  try {
    const rawStocks = await redisGet(STOCKS_REDIS_KEY);
    stocks = rawStocks ? JSON.parse(rawStocks) : {};
  } catch {
    stocks = {};
  }

  const values = Object.values(stocks);
  return {
    meta,
    cacheCount: values.length,
    industryCount: values.filter((item) => item?.industry).length,
    epsCount: values.filter(
      (item) =>
        typeof item?.epsLatestQuarter === "number" &&
        Number.isFinite(item.epsLatestQuarter)
    ).length,
    epsTtmCount: values.filter(
      (item) => typeof item?.epsTtm === "number" && Number.isFinite(item.epsTtm)
    ).length,
    peCount: values.filter(
      (item) => typeof item?.peRatio === "number" && Number.isFinite(item.peRatio)
    ).length,
    attentionCount: values.filter((item) => item?.isAttentionStock === true).length,
    dispositionCount: values.filter(
      (item) => item?.isDispositionStock === true
    ).length,
    sample: values.slice(0, 3).map((item) => ({
      code: item?.code || null,
      name: item?.name || null,
      market: item?.market || null,
      industry: item?.industry || null,
      peRatio:
        typeof item?.peRatio === "number" && Number.isFinite(item.peRatio)
          ? item.peRatio
          : null,
    })),
  };
}

async function loadHoroscopeCacheDebugSummary() {
  const periods = [
    { when: "today", dateKey: getTodayKey(0), dayKey: "today" },
    { when: "tomorrow", dateKey: getTodayKey(1), dayKey: "tomorrow" },
  ];
  const signs = Object.entries(ZODIAC_MAP);
  const snapshots = [];

  for (const period of periods) {
    const rows = await Promise.all(
      signs.map(async ([signZh, signEn]) => {
        const key = `horoscope:v6:${period.dateKey}:${signEn}:${period.dayKey}`;
        try {
          const raw = await redisGet(key);
          if (!raw) return null;
          const parsed = JSON.parse(raw);
          return {
            sign: signZh,
            when: period.when,
            source: parsed?.source || "unknown",
            sourceDate: parsed?.sourceDate || null,
          };
        } catch {
          return null;
        }
      })
    );
    snapshots.push(...rows.filter(Boolean));
  }

  const bySource = snapshots.reduce((acc, item) => {
    const key = item.source || "unknown";
    acc[key] = (acc[key] || 0) + 1;
    return acc;
  }, {});

  return {
    cacheCount: snapshots.length,
    bySource,
    sample: snapshots.slice(0, 6),
  };
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

function looksLikeWeatherRequest(text = "") {
  return /(天氣|氣溫|溫度|下雨|降雨|冷不冷|熱不熱|體感|濕度|悶不悶|風大|穿什麼|穿搭|外套|短袖|長袖|帶傘|傘|淋雨)/.test(
    text
  );
}

function parseWeatherTimeIntent(text = "") {
  const t = String(text || "").trim();
  const checks = [
    [/明天早上|明早/, "tomorrow_morning"],
    [/明天晚上|明晚/, "tomorrow_evening"],
    [/今天晚上|今晚/, "tonight"],
    [/後天/, "day_after"],
    [/明天|明日/, "tomorrow"],
    [/等等|待會|等一下|稍後|接下來/, "soon"],
    [/現在|目前|此刻|當下/, "now"],
    [/今天下午|下午/, "afternoon"],
    [/今天晚上|晚上|晚一點/, "evening"],
    [/今天|今日/, "today"],
  ];

  for (const [pattern, timeIntent] of checks) {
    const matched = t.match(pattern);
    if (matched) {
      return { timeIntent, matchedText: matched[0] || null };
    }
  }

  return { timeIntent: "today", matchedText: null };
}

function parseWeatherQueryType(text = "") {
  const t = String(text || "").trim();
  const clothing = t.match(/穿什麼|穿搭|外套|短袖|長袖|要穿/i);
  if (clothing) {
    return { queryType: "clothing", matchedText: clothing[0] || null };
  }

  const umbrella = t.match(/帶傘|要不要傘|會不會下雨|會下雨嗎|會淋雨嗎|下雨嗎/i);
  if (umbrella) {
    return { queryType: "umbrella", matchedText: umbrella[0] || null };
  }

  return { queryType: "weather", matchedText: null };
}

function resolveTaiwanDistrict(text = "", contextCountyName = "") {
  const input = String(text || "").trim();
  if (!input) return null;
  const contextCounty = String(contextCountyName || "").trim();
  const explicitCounty =
    Object.entries(CWA_LOCATION_MAP)
      .sort((a, b) => b[0].length - a[0].length)
      .find(([alias]) => input.includes(alias))?.[1] || "";

  for (const key of TW_DISTRICT_ALIAS_KEYS) {
    if (!input.includes(key)) continue;

    if (AMBIGUOUS_DISTRICT_ALIASES.has(key)) {
      const candidates = AMBIGUOUS_DISTRICT_CANDIDATES[key] || [];
      if (explicitCounty) {
        const countyMatchedCandidate = candidates.find(
          (candidate) => candidate.countyName === explicitCounty
        );
        if (countyMatchedCandidate) {
          return {
            input: key,
            displayName: countyMatchedCandidate.displayName,
            countyName: countyMatchedCandidate.countyName,
            districtName: countyMatchedCandidate.displayName,
            cwaLocationName: countyMatchedCandidate.countyName,
            matchedLevel: "district",
          };
        }
      }

      const explicitCandidate = candidates.find((candidate) =>
        [candidate.countyName].some((hint) =>
          hint ? input.includes(hint) : false
        )
      );
      if (explicitCandidate) {
        return {
          input: key,
          displayName: explicitCandidate.displayName,
          countyName: explicitCandidate.countyName,
          districtName: explicitCandidate.displayName,
          cwaLocationName: explicitCandidate.countyName,
          matchedLevel: "district",
        };
      }

      if (contextCounty) {
        const contextCandidate = candidates.find(
          (candidate) => candidate.countyName === contextCounty
        );
        if (contextCandidate) {
          return {
            input: key,
            displayName: contextCandidate.displayName,
            countyName: contextCandidate.countyName,
            districtName: contextCandidate.displayName,
            cwaLocationName: contextCandidate.countyName,
            matchedLevel: "district",
          };
        }
      }

      const prioritizedCandidate = (AMBIGUOUS_DISTRICT_PRIORITY[key] || [])
        .map((countyName) =>
          candidates.find((candidate) => candidate.countyName === countyName)
        )
        .find(Boolean);
      if (prioritizedCandidate) {
        return {
          input: key,
          displayName: prioritizedCandidate.displayName,
          countyName: prioritizedCandidate.countyName,
          districtName: prioritizedCandidate.displayName,
          cwaLocationName: prioritizedCandidate.countyName,
          matchedLevel: "district",
        };
      }

      continue;
    }

    const matched = TW_DISTRICT_ALIAS_MAP[key];
    if (!matched) continue;
    return {
      input: key,
      displayName: matched.displayName,
      countyName: matched.countyName,
      districtName: matched.displayName,
      cwaLocationName: matched.countyName,
      matchedLevel: "district",
    };
  }

  return null;
}

function resolveTaiwanWeatherLocation(text = "", contextCountyName = "") {
  const input = String(text || "").trim();
  if (!input) return null;

  const district = resolveTaiwanDistrict(input, contextCountyName);
  if (district) return district;

  const countyEntries = Object.entries(CWA_LOCATION_MAP).sort((a, b) => b[0].length - a[0].length);
  for (const [key, countyName] of countyEntries) {
    if (!input.includes(key)) continue;
    return {
      input: key,
      displayName: countyName,
      countyName,
      districtName: null,
      cwaLocationName: countyName,
      matchedLevel: "county",
    };
  }

  return null;
}

function parseWeatherLocationText(text = "", contextCountyName = "") {
  const t = String(text || "").trim();
  const taiwanLocation = resolveTaiwanWeatherLocation(t, contextCountyName);
  if (taiwanLocation) return taiwanLocation.displayName;

  const twMatch = TW_WEATHER_LOCATIONS.find((location) => t.includes(location));
  if (twMatch) return twMatch;

  const foreignExplicit = t.match(
    /(日本|韓國|美國|法國|英國|新加坡|香港|泰國)\s*([^\s，。！？]+)/i
  );
  if (foreignExplicit) {
    return `${foreignExplicit[1]} ${foreignExplicit[2]}`.trim();
  }

  const foreignSimple = GLOBAL_WEATHER_LOCATIONS.find((location) =>
    t.includes(location)
  );
  if (foreignSimple) return foreignSimple;

  return null;
}

function parseWeatherRequest(text = "", options = {}) {
  const rawText = String(text || "").trim();
  if (!rawText || !looksLikeWeatherRequest(rawText)) {
    return null;
  }
  const contextCountyName =
    options?.contextResolvedLocation?.countyName || options?.contextCountyName || "";

  const { timeIntent } = parseWeatherTimeIntent(rawText);
  const { queryType } = parseWeatherQueryType(rawText);
  const locationText = parseWeatherLocationText(rawText, contextCountyName);
  const resolvedLocation = resolveTaiwanWeatherLocation(rawText, contextCountyName);

  return {
    rawText,
    locationText,
    timeIntent,
    queryType,
    resolvedLocation,
  };
}

function getLocalDateString(dt, offsetSec = 0) {
  const d = new Date((Number(dt) + Number(offsetSec || 0)) * 1000);
  return d.toISOString().slice(0, 10);
}

function getLocalHour(dt, offsetSec = 0) {
  return new Date((Number(dt) + Number(offsetSec || 0)) * 1000).getUTCHours();
}

function shiftDateString(dateStr, offsetDays) {
  const d = new Date(`${dateStr}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + offsetDays);
  return d.toISOString().slice(0, 10);
}

function filterSlotsByHourRange(slots, offsetSec, startHour, endHour) {
  return slots.filter((item) => {
    const hour = getLocalHour(item.dt, offsetSec);
    return hour >= startHour && hour <= endHour;
  });
}

function pickForecastSlotsByIntent(list, offsetSec = 0, timeIntent = "today") {
  const items = Array.isArray(list) ? list : [];
  if (!items.length) {
    return { slots: [], label: WEATHER_TIME_LABEL[timeIntent] || "今天" };
  }

  if (timeIntent === "now") {
    return { slots: items.slice(0, 1), label: WEATHER_TIME_LABEL.now };
  }

  if (timeIntent === "soon") {
    return { slots: items.slice(0, 3), label: WEATHER_TIME_LABEL.soon };
  }

  const baseDateStr = getLocalDateString(items[0].dt, offsetSec);
  const targetDateOffset =
    timeIntent === "tomorrow" ||
    timeIntent === "tomorrow_morning" ||
    timeIntent === "tomorrow_evening"
      ? 1
      : timeIntent === "day_after"
      ? 2
      : 0;
  const targetDateStr = shiftDateString(baseDateStr, targetDateOffset);
  const sameDaySlots = items.filter(
    (item) => getLocalDateString(item.dt, offsetSec) === targetDateStr
  );

  let slots = sameDaySlots;

  if (timeIntent === "tonight" || timeIntent === "tomorrow_evening" || timeIntent === "evening") {
    slots = filterSlotsByHourRange(sameDaySlots, offsetSec, 18, 23);
  } else if (timeIntent === "tomorrow_morning") {
    slots = filterSlotsByHourRange(sameDaySlots, offsetSec, 6, 11);
  } else if (timeIntent === "afternoon") {
    slots = filterSlotsByHourRange(sameDaySlots, offsetSec, 14, 17);
  }

  if (!slots.length) {
    slots = sameDaySlots.length ? sameDaySlots : items.slice(0, 3);
  }

  return {
    slots,
    label: WEATHER_TIME_LABEL[timeIntent] || WEATHER_TIME_LABEL.today,
  };
}

function buildForecastSummaryFromSlots(slots = []) {
  const list = Array.isArray(slots) ? slots : [];
  if (!list.length) {
    return {
      tempMin: null,
      tempMax: null,
      feelsMin: null,
      feelsMax: null,
      humidityAvg: null,
      maxPop: null,
      weatherText: "未知",
      windSpeedAvg: null,
    };
  }

  const temps = list.map((item) => item?.main?.temp).filter(Number.isFinite);
  const feels = list
    .map((item) => item?.main?.feels_like)
    .filter(Number.isFinite);
  const humidities = list
    .map((item) => item?.main?.humidity)
    .filter(Number.isFinite);
  const pops = list
    .map((item) =>
      typeof item?.pop === "number" && Number.isFinite(item.pop)
        ? item.pop * 100
        : null
    )
    .filter(Number.isFinite);
  const windSpeeds = list
    .map((item) => item?.wind?.speed)
    .filter(Number.isFinite);
  const descCounter = new Map();

  for (const item of list) {
    const desc = String(item?.weather?.[0]?.description || "").trim();
    if (!desc) continue;
    descCounter.set(desc, (descCounter.get(desc) || 0) + 1);
  }

  const weatherText =
    [...descCounter.entries()].sort((a, b) => b[1] - a[1])[0]?.[0] ||
    list[0]?.weather?.[0]?.description ||
    "未知";

  return {
    tempMin: temps.length ? Math.min(...temps) : null,
    tempMax: temps.length ? Math.max(...temps) : null,
    feelsMin: feels.length ? Math.min(...feels) : null,
    feelsMax: feels.length ? Math.max(...feels) : null,
    humidityAvg: humidities.length ? calcAverage(humidities) : null,
    maxPop: pops.length ? Math.round(Math.max(...pops)) : null,
    weatherText,
    windSpeedAvg: windSpeeds.length ? calcAverage(windSpeeds) : null,
    currentTemp: temps.length ? temps[0] : null,
  };
}

function buildUmbrellaAdvice(summary, timeIntent = "today") {
  const pop = Number.isFinite(summary?.maxPop) ? summary.maxPop : 0;
  const weatherText = String(summary?.weatherText || "");
  const isStormLike = /雷|陣雨|大雨|暴雨/.test(weatherText);

  if (pop >= 50 || (isStormLike && pop >= 30)) {
    return { level: "yes", text: "建議帶傘。" };
  }
  if (pop >= 30 || isStormLike) {
    return { level: "maybe", text: "建議帶折傘，比較保險。" };
  }
  if (timeIntent === "soon" && pop >= 20) {
    return { level: "maybe", text: "短時間內有零星雨機會，帶折傘較穩。" };
  }

  return { level: "no", text: "原則上可不帶傘。" };
}

function buildClothingAdvice(summary, timeIntent = "today") {
  const feelsMax = Number.isFinite(summary?.feelsMax)
    ? summary.feelsMax
    : summary?.tempMax;
  const feelsMin = Number.isFinite(summary?.feelsMin)
    ? summary.feelsMin
    : summary?.tempMin;
  const pop = Number.isFinite(summary?.maxPop) ? summary.maxPop : 0;
  const wind = Number.isFinite(summary?.windSpeedAvg) ? summary.windSpeedAvg : 0;
  const swing =
    Number.isFinite(feelsMax) && Number.isFinite(feelsMin) ? feelsMax - feelsMin : 0;

  let level = "warm";
  let text = "短袖即可。";

  if (feelsMax >= 30) {
    level = "hot";
    text = "短袖為主，外出注意悶熱與防曬。";
  } else if (feelsMax >= 24) {
    level = "warm";
    text = "短袖或薄長袖都可以。";
  } else if (feelsMax >= 18) {
    level = "cool";
    text = "建議薄外套或長袖，會比較穩。";
  } else {
    level = "cold";
    text = "建議穿外套，偏涼時段要注意保暖。";
  }

  if (swing >= 6) {
    text += " 日夜溫差偏大，最好多帶一件薄外套。";
  } else if (wind >= 8 && level !== "hot") {
    text += " 風感會更明顯，外層衣物會更實用。";
  }

  if (pop >= 40) {
    text += " 若要久待戶外，鞋子盡量選不怕濕的。";
  }

  return { level, text };
}

function buildWeatherDecision(summary, request) {
  const umbrella = buildUmbrellaAdvice(summary, request?.timeIntent);
  const clothing = buildClothingAdvice(summary, request?.timeIntent);
  const feelsMax = Number.isFinite(summary?.feelsMax)
    ? summary.feelsMax
    : summary?.tempMax;
  const humidity = Number.isFinite(summary?.humidityAvg)
    ? Math.round(summary.humidityAvg)
    : null;
  const pop = Number.isFinite(summary?.maxPop) ? summary.maxPop : 0;

  let tempTone = "舒適";
  if (feelsMax >= 30) tempTone = "偏熱";
  else if (feelsMax >= 24) tempTone = "溫暖";
  else if (feelsMax >= 18) tempTone = "微涼";
  else tempTone = "偏冷";

  const rainPhrase =
    request?.timeIntent === "now"
      ? pop >= 50
        ? "短時間內有明顯降雨機會"
        : pop >= 30
        ? "短時間內有局部降雨機會"
        : "目前降雨機率不高"
      : pop >= 50
      ? "有明顯下雨機會"
      : pop >= 30
      ? "有短暫雨機會"
      : "降雨機率不高";
  const summaryLine = `${tempTone}，${rainPhrase}。`;

  const comfortNote =
    summary?.comfortText
      ? String(summary.comfortText).trim()
      : humidity !== null && humidity >= 80
      ? "體感會比實際溫度再悶一些。"
      : humidity !== null && humidity <= 55
      ? "空氣相對乾爽。"
      : "";

  const riskNote =
    pop >= 40
      ? "降雨型態偏局部，時段前後仍可能變化。"
      : request?.timeIntent === "soon"
      ? "短時間天氣變化較快。"
      : "";

  return {
    summaryLine,
    umbrellaAdvice: umbrella.text,
    clothingAdvice: clothing.text,
    comfortNote,
    riskNote,
  };
}

function formatWeatherReplyV2(request, summary, decision, meta = {}) {
  const locationLabel = meta.displayLocation || request?.locationText || "這裡";
  const timeLabel =
    meta.timeLabel || WEATHER_TIME_LABEL[request?.timeIntent] || "今天";
  const isCurrent = meta.mode === "current" || request?.timeIntent === "now";
  const currentTemp =
    Number.isFinite(meta.currentTemp) ? meta.currentTemp : summary?.currentTemp;
  const tempRange =
    Number.isFinite(summary?.tempMin) && Number.isFinite(summary?.tempMax)
      ? isCurrent && Number.isFinite(currentTemp)
        ? `${currentTemp.toFixed(1)}°C`
        : `${summary.tempMin.toFixed(1)}~${summary.tempMax.toFixed(1)}°C`
      : "資料不足";
  const popText = Number.isFinite(summary?.maxPop) ? `${summary.maxPop}%` : "—";

  if (request?.queryType === "clothing") {
    return [
      `${locationLabel}${timeLabel} ${decision.summaryLine}`,
      "",
      decision.clothingAdvice,
      decision.umbrellaAdvice,
      decision.comfortNote,
      decision.riskNote,
    ]
      .filter(Boolean)
      .join("\n");
  }

  if (request?.queryType === "umbrella") {
    return [
      decision.umbrellaAdvice,
      "",
      `${locationLabel}${timeLabel}${decision.summaryLine}`,
      `氣溫：約 ${tempRange}`,
      `降雨機率：${popText}`,
      decision.riskNote,
    ]
      .filter(Boolean)
      .join("\n");
  }

  return [
    `${locationLabel}${timeLabel}${decision.summaryLine}`,
    "",
    isCurrent
      ? `目前氣溫：約 ${tempRange}`
      : `氣溫：約 ${tempRange}`,
    isCurrent ? `短時降雨機率：${popText}` : `降雨機率：${popText}`,
    `建議：${decision.umbrellaAdvice.replace(/。$/, "")}`,
    decision.clothingAdvice,
    decision.comfortNote,
    decision.riskNote,
  ]
    .filter(Boolean)
    .join("\n");
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
const TW_ADMIN_DIVISION_COUNTIES = Array.isArray(twAdminDivisions?.counties)
  ? twAdminDivisions.counties
  : [];
const CWA_LOCATION_MAP = buildCountyAliasMap(TW_ADMIN_DIVISION_COUNTIES);
const TW_DISTRICT_LOOKUP = buildDistrictLookup(TW_ADMIN_DIVISION_COUNTIES);
const TW_DISTRICT_LIST = TW_DISTRICT_LOOKUP.districts;
const TW_DISTRICT_ALIAS_MAP = TW_DISTRICT_LOOKUP.aliasMap;
const AMBIGUOUS_DISTRICT_ALIASES = TW_DISTRICT_LOOKUP.ambiguousAliases;
const AMBIGUOUS_DISTRICT_CANDIDATES = TW_DISTRICT_LOOKUP.ambiguousCandidates;
const AMBIGUOUS_DISTRICT_PRIORITY = {
  信義區: ["臺北市", "基隆市"],
  中正區: ["臺北市", "基隆市"],
  中山區: ["臺北市", "基隆市"],
  東區: ["臺南市", "新竹市", "嘉義市"],
  北區: ["臺中市", "臺南市", "新竹市"],
  南區: ["臺中市", "臺南市"],
  西區: ["臺中市", "嘉義市"],
};
const TW_DISTRICT_ALIAS_KEYS = [
  ...new Set([
    ...Object.keys(TW_DISTRICT_ALIAS_MAP),
    ...AMBIGUOUS_DISTRICT_ALIASES,
  ]),
].sort((a, b) => b.length - a.length);

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

function normalizeCwaLocationName(raw = "") {
  const text = String(raw || "").trim();
  if (!text) return "";

  for (const [key, value] of Object.entries(CWA_LOCATION_MAP)) {
    if (text.includes(key)) return value;
  }

  return CWA_LOCATION_MAP[text] || text;
}

function getCwaLocationVariants(raw = "") {
  const normalized = normalizeCwaLocationName(raw);
  const variants = new Set([normalized, String(raw || "").trim()]);

  for (const [key, value] of Object.entries(CWA_LOCATION_MAP)) {
    if (value === normalized) variants.add(key);
  }

  return [...variants].filter(Boolean);
}

function getCwaHeaders() {
  return {
    accept: "application/json",
    "user-agent": "Mozilla/5.0",
  };
}

async function fetchJsonWithTimeout(url, options = {}, timeoutMs = 10000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      ...options,
      signal: controller.signal,
    });
    const text = await res.text();
    if (!res.ok) {
      throw new Error(`HTTP ${res.status}: ${text.slice(0, 160)}`);
    }
    return text ? JSON.parse(text) : null;
  } finally {
    clearTimeout(timer);
  }
}

function simpleStableHash(text) {
  let hash = 0;
  const input = String(text || "");
  for (let i = 0; i < input.length; i++) {
    hash = (hash * 31 + input.charCodeAt(i)) | 0;
  }
  return Math.abs(hash).toString(36);
}

async function fetchCwaSignificantEarthquakes() {
  if (!CWA_API_KEY) {
    throw new Error("CWA_API_KEY is missing");
  }

  const url = `https://opendata.cwa.gov.tw/api/v1/rest/datastore/${CWA_EARTHQUAKE_DATASET}?Authorization=${encodeURIComponent(
    CWA_API_KEY
  )}&format=JSON`;
  const json = await fetchJsonWithTimeout(url, { headers: getCwaHeaders() });
  const records = json?.records || {};
  if (Array.isArray(records?.Earthquake)) return records.Earthquake;
  if (Array.isArray(records?.earthquake)) return records.earthquake;
  if (Array.isArray(records?.dataset?.earthquake)) {
    return records.dataset.earthquake;
  }
  return [];
}

function extractEarthquakeMagnitude(eq) {
  const magnitude =
    eq?.EarthquakeInfo?.EarthquakeMagnitude?.MagnitudeValue ??
    eq?.EarthquakeInfo?.EarthquakeMagnitude?.magnitudeValue ??
    eq?.EarthquakeInfo?.Magnitude?.MagnitudeValue ??
    eq?.Magnitude ??
    eq?.magnitude;
  return parseMarketNumber(magnitude);
}

function extractEarthquakeDepth(eq) {
  const depth =
    eq?.EarthquakeInfo?.FocalDepth ??
    eq?.EarthquakeInfo?.focalDepth ??
    eq?.FocalDepth ??
    eq?.depth;
  return parseMarketNumber(depth);
}

function extractEarthquakeOriginTime(eq) {
  return String(
    eq?.EarthquakeInfo?.OriginTime ||
      eq?.EarthquakeInfo?.originTime ||
      eq?.OriginTime ||
      eq?.originTime ||
      ""
  ).trim();
}

function extractEarthquakeLocation(eq) {
  const epicenter = eq?.EarthquakeInfo?.Epicenter || eq?.EarthquakeInfo?.epicenter;
  return String(
    epicenter?.Location ||
      epicenter?.location ||
      eq?.ReportContent?.match(/位於(.+?)[，,。]/)?.[1] ||
      eq?.ReportContent ||
      ""
  ).trim();
}

function extractEarthquakeMaxIntensity(eq) {
  const areas = Array.isArray(eq?.Intensity?.ShakingArea)
    ? eq.Intensity.ShakingArea
    : Array.isArray(eq?.Intensity?.shakingArea)
    ? eq.Intensity.shakingArea
    : [];
  const intensities = areas
    .map((area) =>
      String(
        area?.AreaIntensity ||
          area?.areaIntensity ||
          area?.CountyIntensity ||
          area?.countyIntensity ||
          ""
      ).trim()
    )
    .filter(Boolean);
  return intensities[0] || "";
}

function normalizeCwaEarthquake(eq) {
  const reportNo = String(
    eq?.EarthquakeNo || eq?.earthquakeNo || eq?.ReportNo || eq?.reportNo || ""
  ).trim();
  const originTime = extractEarthquakeOriginTime(eq);
  const magnitude = extractEarthquakeMagnitude(eq);
  const depth = extractEarthquakeDepth(eq);
  const location = extractEarthquakeLocation(eq);
  const maxIntensity = extractEarthquakeMaxIntensity(eq);
  const reportContent = String(eq?.ReportContent || eq?.reportContent || "").trim();
  const web = String(eq?.Web || eq?.web || "").trim();
  const imageUrl = String(eq?.ReportImageURI || eq?.reportImageURI || "").trim();
  const id = reportNo || `${originTime}:${simpleStableHash(reportContent || location)}`;

  return {
    id,
    reportNo,
    originTime,
    magnitude,
    depth,
    location,
    maxIntensity,
    reportContent,
    web,
    imageUrl,
  };
}

function pickLatestEarthquake(events) {
  const normalized = events.map(normalizeCwaEarthquake).filter((item) => item.id);
  normalized.sort((a, b) => {
    const at = Date.parse(a.originTime || "");
    const bt = Date.parse(b.originTime || "");
    if (Number.isFinite(bt) && Number.isFinite(at) && bt !== at) return bt - at;
    return String(b.id).localeCompare(String(a.id));
  });
  return normalized[0] || null;
}

function formatEarthquakePushText(event, { isTest = false } = {}) {
  const lines = [
    isTest ? "【測試功能｜顯著有感地震報告】" : "【顯著有感地震報告】",
  ];
  if (event.reportNo) lines.push(`編號：${event.reportNo}`);
  if (event.originTime) lines.push(`發震時間：${event.originTime}`);
  if (Number.isFinite(event.magnitude)) lines.push(`芮氏規模：${event.magnitude}`);
  if (Number.isFinite(event.depth)) lines.push(`震源深度：${event.depth} 公里`);
  if (event.location) lines.push(`位置：${event.location}`);
  if (event.maxIntensity) lines.push(`最大震度：${event.maxIntensity}`);
  if (event.web) lines.push("", `詳細資料：${event.web}`);
  lines.push("", "資料來源：中央氣象署");
  return lines.join("\n").slice(0, LINE_TEXT_LIMIT);
}

async function pushLineTextToKnownTargets(text) {
  const targets = await getKnownLinePushTargets();
  let sent = 0;
  let failed = 0;

  for (const target of targets) {
    try {
      await client.pushMessage(
        target.targetId,
        normalizeLineMessage({ type: "text", text })
      );
      sent++;
    } catch (err) {
      failed++;
      console.error("LINE earthquake push failed:", {
        targetType: target.targetType,
        targetId: target.targetId,
        statusCode: err?.statusCode,
        statusMessage: err?.statusMessage,
        data: getLineErrorData(err),
      });
    }
  }

  return {
    targetCount: targets.length,
    sent,
    failed,
  };
}

async function fetchCwa36HourForecast(locationName) {
  if (!CWA_API_KEY || !locationName) return null;

  const url = `https://opendata.cwa.gov.tw/api/v1/rest/datastore/F-C0032-001?Authorization=${encodeURIComponent(
    CWA_API_KEY
  )}&format=JSON&locationName=${encodeURIComponent(locationName)}`;
  const res = await fetch(url, { headers: getCwaHeaders() });
  if (!res.ok) return null;
  const json = await res.json();
  return json;
}

function getCwaForecastElementMap(locationData) {
  const elements = Array.isArray(locationData?.weatherElement)
    ? locationData.weatherElement
    : [];
  const map = {};

  for (const element of elements) {
    const name = String(element?.elementName || "");
    if (!name) continue;
    map[name] = Array.isArray(element?.time) ? element.time : [];
  }

  return map;
}

function buildSummaryFromCwaForecast(locationData, timeIntent = "today") {
  const elementMap = getCwaForecastElementMap(locationData);
  const wxTimes = elementMap.Wx || [];
  const popTimes = elementMap.PoP || [];
  const minTTimes = elementMap.MinT || [];
  const maxTTimes = elementMap.MaxT || [];
  const ciTimes = elementMap.CI || [];

  if (!wxTimes.length && !minTTimes.length && !maxTTimes.length) return null;

  const selectIndices = (() => {
    if (timeIntent === "tonight" || timeIntent === "evening") return [1];
    if (timeIntent === "tomorrow" || timeIntent === "tomorrow_morning")
      return [2];
    if (timeIntent === "tomorrow_evening") return [2];
    if (timeIntent === "afternoon") return [0];
    return [0];
  })();

  const getValue = (times, index) => {
    const item = times[index];
    const params = Array.isArray(item?.parameter) ? item.parameter : [];
    const value =
      params[0]?.parameterValue ??
      params[0]?.parameterName ??
      item?.parameter?.parameterName ??
      null;
    return value;
  };

  const weatherTexts = selectIndices
    .map((index) => getValue(wxTimes, index))
    .filter(Boolean)
    .map(String);
  const pops = selectIndices
    .map((index) => Number(getValue(popTimes, index)))
    .filter(Number.isFinite);
  const mins = selectIndices
    .map((index) => Number(getValue(minTTimes, index)))
    .filter(Number.isFinite);
  const maxs = selectIndices
    .map((index) => Number(getValue(maxTTimes, index)))
    .filter(Number.isFinite);
  const ciTexts = selectIndices
    .map((index) => getValue(ciTimes, index))
    .filter(Boolean)
    .map(String);

  return {
    mode: "forecast",
    source: "cwa",
    tempMin: mins.length ? Math.min(...mins) : null,
    tempMax: maxs.length ? Math.max(...maxs) : null,
    feelsMin: mins.length ? Math.min(...mins) : null,
    feelsMax: maxs.length ? Math.max(...maxs) : null,
    humidityAvg: null,
    maxPop: pops.length ? Math.max(...pops) : null,
    weatherText: weatherTexts[0] || ciTexts[0] || "未知",
    windSpeedAvg: null,
    comfortText: ciTexts[0] || "",
  };
}

function estimateApparentTemperature(tempC, humidityPct, windMps = 0) {
  if (
    !Number.isFinite(tempC) ||
    !Number.isFinite(humidityPct) ||
    humidityPct < 0 ||
    humidityPct > 100
  ) {
    return null;
  }

  const vaporPressure =
    (humidityPct / 100) *
    6.105 *
    Math.exp((17.27 * tempC) / (237.7 + tempC));
  const apparent = tempC + 0.33 * vaporPressure - 0.7 * (windMps || 0) - 4.0;

  return Number.isFinite(apparent) ? apparent : null;
}

function shouldShowFeelsLike(tempValue, feelsValue) {
  if (!Number.isFinite(tempValue) || !Number.isFinite(feelsValue)) return false;
  return Math.abs(feelsValue - tempValue) >= 0.5;
}

async function fetchCwaCurrentObservation(locationText, resolvedLocation = null) {
  if (!CWA_API_KEY || !locationText) return null;

  const url = `https://opendata.cwa.gov.tw/api/v1/rest/datastore/O-A0003-001?Authorization=${encodeURIComponent(
    CWA_API_KEY
  )}&format=JSON`;
  const res = await fetch(url, { headers: getCwaHeaders() });
  if (!res.ok) return null;
  const json = await res.json();
  const records = json?.records || {};
  const stations = Array.isArray(records?.Station)
    ? records.Station
    : Array.isArray(records?.station)
    ? records.station
    : Array.isArray(records?.locations?.[0]?.station)
    ? records.locations[0].station
    : [];
  if (!stations.length) return null;

  const variants = getCwaLocationVariants(
    resolvedLocation?.countyName || locationText
  );
  const districtVariants = resolvedLocation?.districtName
    ? [resolvedLocation.districtName, String(resolvedLocation.input || "")]
        .filter(Boolean)
        .map((text) => text.replace(/[區鎮鄉市]$/g, ""))
    : [];
  const scored = stations
    .map((station) => {
      const geo = station?.GeoInfo || station?.geoInfo || {};
      const county = String(geo?.CountyName || geo?.countyName || "").trim();
      const town = String(geo?.TownName || geo?.townName || "").trim();
      const stationName = String(
        station?.StationName || station?.stationName || ""
      ).trim();
      let score = 0;

      if (variants.some((v) => county.includes(v) || v.includes(county))) score += 4;
      if (variants.some((v) => town.includes(v) || v.includes(town))) score += 3;
      if (
        districtVariants.some((v) =>
          v ? town.replace(/[區鎮鄉市]$/g, "").includes(v) : false
        )
      ) {
        score += 5;
      }
      if (variants.some((v) => stationName.includes(v) || v.includes(stationName))) {
        score += 2;
      }

      return { station, score };
    })
    .filter((item) => item.score > 0)
    .sort((a, b) => b.score - a.score);

  const selected = scored[0]?.station || null;
  if (!selected) return null;

  const obs = selected?.WeatherElement || selected?.weatherElement || {};
  const relativeHumidity = Number(
    obs?.RelativeHumidity ?? obs?.HUMD ?? obs?.relativeHumidity
  );
  const humidity =
    Number.isFinite(relativeHumidity) && relativeHumidity <= 1
      ? Math.round(relativeHumidity * 100)
      : Number.isFinite(relativeHumidity)
      ? Math.round(relativeHumidity)
      : null;
  const airTemp = Number(
    obs?.AirTemperature ?? obs?.TEMP ?? obs?.airTemperature
  );
  const windSpeed = Number(obs?.WindSpeed ?? obs?.WDSD ?? obs?.windSpeed);
  const precipitation = Number(
    obs?.Now?.Precipitation ??
      obs?.Precipitation ??
      obs?.RAIN ??
      obs?.precipitation
  );
  const weatherText = String(
    obs?.Weather || obs?.WeatherDescription || obs?.weather || "未知"
  ).trim();
  const apparentTemp = estimateApparentTemperature(airTemp, humidity, windSpeed);
  const resolvedFeels = Number.isFinite(apparentTemp) ? apparentTemp : airTemp;
  const showFeels = shouldShowFeelsLike(airTemp, resolvedFeels);

  return {
    mode: "current",
    source: "cwa",
    tempMin: Number.isFinite(airTemp) ? airTemp : null,
    tempMax: Number.isFinite(airTemp) ? airTemp : null,
    feelsMin: Number.isFinite(resolvedFeels) ? resolvedFeels : null,
    feelsMax: Number.isFinite(resolvedFeels) ? resolvedFeels : null,
    humidityAvg: humidity,
    maxPop:
      Number.isFinite(precipitation) && precipitation > 0
        ? Math.min(100, Math.round(precipitation * 10))
        : 0,
    weatherText: weatherText || "未知",
    windSpeedAvg: Number.isFinite(windSpeed) ? windSpeed : null,
    currentTemp: Number.isFinite(airTemp) ? airTemp : null,
    showFeels,
    observationStation:
      selected?.StationName || selected?.stationName || locationText,
  };
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
  mode,
  currentTemp,
  rainLabel,
  showFeels,
}) {
  const imageUrl = pickWeatherImage(desc, rainPercent);
  const currentValue =
    currentTemp ?? (typeof minTemp === "string" ? minTemp : `${minTemp}`);
  const feelsSuffix =
    showFeels && feels && feels !== "--" ? `（體感 ${feels}°C）` : "";
  const temperatureText =
    mode === "current"
      ? `🌡 ${currentValue}°C${feelsSuffix}`
      : `🌡 ${minTemp}°C ～ ${maxTemp}°C${feelsSuffix}`;
  const rainText =
    mode === "current"
      ? `☔ ${rainLabel || `短時降雨機率 ${rainPercent}%`}`
      : `☔ 降雨機率 ${rainPercent}%`;
  const detailContents = [
    {
      type: "text",
      text: temperatureText,
    },
  ];

  if (humidity !== null && humidity !== undefined && humidity !== "NA") {
    detailContents.push({
      type: "text",
      text: `💧 濕度 ${humidity}%`,
    });
  }

  detailContents.push({
    type: "text",
    text: rainText,
  });
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
            contents: detailContents,
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
  rawText = "",
  timeIntent,
  queryType,
  resolvedLocation,
} = {}) {
  const apiKey = process.env.WEATHER_API_KEY;
  if (!apiKey && !CWA_API_KEY) {
    return "後端沒有設定 WEATHER_API_KEY，請先到 Vercel 設定環境變數。";
  }

  try {
    const parsedRequest = rawText ? parseWeatherRequest(rawText) : null;
    const resolvedTaiwanLocation =
      resolvedLocation || parsedRequest?.resolvedLocation || null;
    const resolvedTimeIntent =
      timeIntent ||
      parsedRequest?.timeIntent ||
      (when === "tomorrow"
        ? "tomorrow"
        : when === "day_after"
        ? "day_after"
        : "today");
    const resolvedQueryType =
      queryType || parsedRequest?.queryType || "weather";
    const requestedLocation =
      resolvedTaiwanLocation?.displayName ||
      parsedRequest?.locationText ||
      address ||
      city ||
      "Taipei";

    let resolvedCity = requestedLocation;
    let resolvedLat = lat;
    let resolvedLon = lon;

    const isTW = isTaiwanLocation(requestedLocation);
    const cwaLocationName =
      resolvedTaiwanLocation?.cwaLocationName ||
      normalizeCwaLocationName(requestedLocation);
    const cwaSupportsIntent = [
      "now",
      "soon",
      "today",
      "afternoon",
      "tonight",
      "evening",
      "tomorrow",
      "tomorrow_morning",
      "tomorrow_evening",
    ].includes(resolvedTimeIntent);

    // 台灣離島先用人工座標
    const island = findTaiwanIsland(requestedLocation);
    if (!resolvedLat && !resolvedLon && island) {
      resolvedLat = island.lat;
      resolvedLon = island.lon;
      resolvedCity = island.name;
    }

    if (!resolvedLat || !resolvedLon) {
      const geo = await geocodeCity(
        resolvedTaiwanLocation?.countyName || requestedLocation,
        apiKey
      );
      if (!geo) {
        // 無法 geocode，改用城市名稱直接查 forecast（預設國家為台灣）
        resolvedCity = requestedLocation;
      } else {
        resolvedLat = geo.lat;
        resolvedLon = geo.lon;
        resolvedCity = geo.name;
      }
    }

    if (isTW && CWA_API_KEY && cwaSupportsIntent) {
      const cwaSummary =
        resolvedTimeIntent === "now" || resolvedTimeIntent === "soon"
          ? await fetchCwaCurrentObservation(
              cwaLocationName,
              resolvedTaiwanLocation
            )
          : await (async () => {
              const forecastJson = await fetchCwa36HourForecast(cwaLocationName);
              const locationData = forecastJson?.records?.location?.[0];
              return locationData
                ? buildSummaryFromCwaForecast(locationData, resolvedTimeIntent)
                : null;
            })();

      if (cwaSummary) {
        const locationLabel = address
          ? `${address}（座標）`
          : resolvedTaiwanLocation?.matchedLevel === "district"
          ? `${resolvedTaiwanLocation.displayName}（${resolvedTaiwanLocation.countyName}）`
          : cwaLocationName || resolvedCity || requestedLocation || "未命名地點";
        const request = {
          rawText,
          locationText: locationLabel,
          timeIntent: resolvedTimeIntent,
          queryType: resolvedQueryType,
        };
        const decision = buildWeatherDecision(cwaSummary, request);
        const label =
          resolvedTimeIntent === "now"
            ? WEATHER_TIME_LABEL.now
            : WEATHER_TIME_LABEL[resolvedTimeIntent] || WEATHER_TIME_LABEL.today;
        const weatherText = formatWeatherReplyV2(request, cwaSummary, decision, {
          displayLocation: locationLabel,
          timeLabel: label,
          mode: cwaSummary.mode,
          currentTemp: cwaSummary.currentTemp,
        });
        const safeCurrent = Number.isFinite(cwaSummary.currentTemp)
          ? cwaSummary.currentTemp.toFixed(1)
          : null;
        const safeMin =
          cwaSummary.tempMin != null ? cwaSummary.tempMin.toFixed(1) : "--";
        const safeMax =
          cwaSummary.tempMax != null ? cwaSummary.tempMax.toFixed(1) : "--";
        const safeFeels =
          cwaSummary.feelsMin != null && cwaSummary.feelsMax != null
            ? cwaSummary.feelsMin === cwaSummary.feelsMax
              ? cwaSummary.feelsMax.toFixed(1)
              : `${cwaSummary.feelsMin.toFixed(1)}～${cwaSummary.feelsMax.toFixed(1)}`
            : "--";
        const humidity =
          cwaSummary.humidityAvg != null ? Math.round(cwaSummary.humidityAvg) : null;
        const rainPercent =
          cwaSummary.maxPop != null ? cwaSummary.maxPop : 0;
        const outfitText = [decision.clothingAdvice, decision.umbrellaAdvice]
          .filter(Boolean)
          .join("\n");

        return {
          text: weatherText,
          preferTextOnly: resolvedQueryType !== "weather",
          data: {
            city: locationLabel,
            whenLabel: label,
            desc: cwaSummary.weatherText,
            minTemp: safeCurrent || safeMin,
            maxTemp: safeCurrent || safeMax,
            feels: safeFeels,
            humidity,
            rainPercent,
            outfitText,
            mode: cwaSummary.mode,
            currentTemp: safeCurrent,
            showFeels:
              typeof cwaSummary.showFeels === "boolean"
                ? cwaSummary.showFeels
                : shouldShowFeelsLike(cwaSummary.currentTemp, cwaSummary.feelsMax),
            rainLabel:
              cwaSummary.mode === "current"
                ? `短時降雨機率 ${rainPercent}%`
                : null,
          },
        };
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

    const offsetSec = data.city?.timezone ?? 0;
    const forecastList = Array.isArray(data.list) ? data.list : [];
    if (!forecastList.length) {
      return "暫時查不到天氣資料，請稍後再試。";
    }

    const { slots, label } = pickForecastSlotsByIntent(
      forecastList,
      offsetSec,
      resolvedTimeIntent
    );
    if (!slots.length) {
      return "暫時查不到這個時間點的天氣，等等再試一次。";
    }

    const summary = buildForecastSummaryFromSlots(slots);
    const locationLabel = address
      ? `${address}（座標）`
      : resolvedTaiwanLocation?.matchedLevel === "district"
      ? `${resolvedTaiwanLocation.displayName}（${resolvedTaiwanLocation.countyName}）`
      : resolvedCity || requestedLocation || "未命名地點";
    const request = {
      rawText,
      locationText: locationLabel,
      timeIntent: resolvedTimeIntent,
      queryType: resolvedQueryType,
    };
    const decision = buildWeatherDecision(summary, request);
    const weatherText = formatWeatherReplyV2(request, summary, decision, {
      displayLocation: locationLabel,
      timeLabel: label,
    });
    const safeCurrent =
      summary.currentTemp != null ? summary.currentTemp.toFixed(1) : null;
    const safeMin =
      summary.tempMin != null ? summary.tempMin.toFixed(1) : "--";
    const safeMax =
      summary.tempMax != null ? summary.tempMax.toFixed(1) : "--";
    const safeFeels =
      summary.feelsMin != null && summary.feelsMax != null
        ? summary.feelsMin === summary.feelsMax
          ? summary.feelsMax.toFixed(1)
          : `${summary.feelsMin.toFixed(1)}～${summary.feelsMax.toFixed(1)}`
        : "--";
    const humidity =
      summary.humidityAvg != null ? Math.round(summary.humidityAvg) : null;
    const rainPercent = summary.maxPop != null ? summary.maxPop : 0;
    const outfitText = [decision.clothingAdvice, decision.umbrellaAdvice]
      .filter(Boolean)
      .join("\n");
    const showFeels =
      resolvedTimeIntent === "now"
        ? shouldShowFeelsLike(summary.currentTemp, summary.feelsMax)
        : Number.isFinite(summary.tempMin) &&
          Number.isFinite(summary.tempMax) &&
          Number.isFinite(summary.feelsMin) &&
          Number.isFinite(summary.feelsMax) &&
          (Math.abs(summary.feelsMin - summary.tempMin) >= 0.5 ||
            Math.abs(summary.feelsMax - summary.tempMax) >= 0.5);

    return {
      text: weatherText,
      preferTextOnly: resolvedQueryType !== "weather",
      data: {
        city: locationLabel,
        whenLabel: label,
        desc: summary.weatherText,
        minTemp: resolvedTimeIntent === "now" ? safeCurrent || safeMin : safeMin,
        maxTemp: resolvedTimeIntent === "now" ? null : safeMax,
        feels: safeFeels,
        humidity,
        rainPercent,
        outfitText,
        mode: resolvedTimeIntent === "now" ? "current" : summary.mode,
        currentTemp: resolvedTimeIntent === "now" ? safeCurrent || safeMin : null,
        showFeels,
      },
    };
  } catch (err) {
    console.error("Weather fetch error:", err);
    return "查天氣時發生例外錯誤，等等再試一次。";
  }
}

async function replyWeather(replyTarget, result) {
  // 如果整個 result 就是錯誤字串 → 直接回文字
  if (!result || typeof result === "string" || !result.data || result.preferTextOnly) {
    await sendLineReply(replyTarget, {
      type: "text",
      text:
        typeof result === "string"
          ? result
          : result?.text || "天氣資料取得失敗",
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
  return taipeiDateKey(Date.now() + offset * 24 * 60 * 60 * 1000);
}
function renderStars(n = 0) {
  const safeN = Math.max(1, Math.min(5, Math.round(Number(n) || 3)));
  return "★".repeat(safeN) + "☆".repeat(5 - safeN);
}

function buildHoroscopeFlexV2({ signZh, signEn, whenLabel, data }) {
  const imageUrl = `https://raw.githubusercontent.com/ChenWenChou/line-gpt-kevin/main/public/image/${signEn}.png`;
  const sourceText =
    data?.source === "aztro"
      ? "資料來源：aztro API，AI 中文整理，娛樂參考"
      : data?.source === "freehoroscopeapi"
      ? "資料來源：FreeHoroscopeAPI，AI 中文整理，娛樂參考"
      : "資料來源：AI 備援生成，娛樂參考";
  const hasValue = (value) => {
    const text = String(value ?? "").trim();
    return Boolean(text && text !== "-");
  };
  const luckyParts = [];
  if (hasValue(data?.luckyColor)) luckyParts.push(`幸運色：${data.luckyColor}`);
  if (hasValue(data?.luckyNumber)) luckyParts.push(`數字：${data.luckyNumber}`);
  const extraParts = [];
  if (hasValue(data?.luckyTime)) extraParts.push(`幸運時間：${data.luckyTime}`);
  if (hasValue(data?.compatibility)) extraParts.push(`速配：${data.compatibility}`);

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
            text: `整體 ${renderStars(data.overallStars ?? 3)}｜${data.mood ?? "平穩"}`,
            size: "lg",
            color: "#F5A623",
            wrap: true,
          },
          {
            type: "text",
            text: data.overall ?? "今天適合照自己的節奏走，別被外界牽著跑。",
            wrap: true,
            color: "#444444",
          },
          { type: "separator" },

          {
            type: "text",
            text: `💼 工作：${data.work ?? "先處理最卡的一件事。"}`,
            wrap: true,
          },
          {
            type: "text",
            text: `❤️ 感情：${data.love ?? "少猜測，直接說需求會更順。"}`,
            wrap: true,
          },
          {
            type: "text",
            text: `💰 財運：${data.money ?? "消費前先等一下，避免情緒下單。"}`,
            wrap: true,
          },
          {
            type: "text",
            text: `🩺 健康：${data.health ?? "留意作息與精神消耗。"}`,
            wrap: true,
          },
          ...(luckyParts.length
            ? [
                {
                  type: "text",
                  text: `🎯 ${luckyParts.join("｜")}`,
                  wrap: true,
                  weight: "bold",
                },
              ]
            : []),
          ...(extraParts.length
            ? [
                {
                  type: "text",
                  text: `🕒 ${extraParts.join("｜")}`,
                  wrap: true,
                  size: "sm",
                  color: "#555555",
                },
              ]
            : []),

          { type: "separator", margin: "md" },
          {
            type: "text",
            text: `今日提醒：${data.advice ?? "把重點放在能控制的事情上。"}`,
            size: "sm",
            wrap: true,
            color: "#333333",
          },
          {
            type: "text",
            text: `※ ${sourceText}`,
            size: "xs",
            color: "#888888",
            wrap: true,
          },
        ],
      },
    },
  };
}

function parseJsonObjectFromText(text) {
  const raw = String(text || "").trim();
  const unfenced = raw
    .replace(/^```(?:json)?/i, "")
    .replace(/```$/i, "")
    .trim();
  try {
    return JSON.parse(unfenced);
  } catch {
    const start = unfenced.indexOf("{");
    const end = unfenced.lastIndexOf("}");
    if (start >= 0 && end > start) {
      return JSON.parse(unfenced.slice(start, end + 1));
    }
    throw new Error("JSON object not found");
  }
}

function getAztroDay(when) {
  return when === "tomorrow" ? "tomorrow" : "today";
}

async function fetchAztroHoroscope(signEn, when) {
  const day = getAztroDay(when);
  const url = `https://aztro.sameerkumar.website/?sign=${encodeURIComponent(
    signEn
  )}&day=${encodeURIComponent(day)}`;
  const data = await fetchJsonWithTimeout(
    url,
    {
      method: "POST",
      headers: {
        accept: "application/json",
      },
    },
    9000
  );
  if (!data?.description) {
    throw new Error("Aztro horoscope response missing description");
  }
  return data;
}

async function fetchFreeHoroscopeApi(signEn, when) {
  const day = getAztroDay(when);
  const url = `https://freehoroscopeapi.com/api/v1/get-horoscope/daily?sign=${encodeURIComponent(
    signEn
  )}${day === "tomorrow" ? "&day=tomorrow" : ""}`;
  const json = await fetchJsonWithTimeout(
    url,
    {
      method: "GET",
      headers: {
        accept: "application/json",
      },
    },
    9000
  );
  const data = json?.data || json;
  if (!data?.horoscope) {
    throw new Error("FreeHoroscopeAPI response missing horoscope");
  }
  return {
    current_date: data.date || null,
    mood: "",
    compatibility: "",
    lucky_number: "",
    lucky_time: "",
    color: "",
    date_range: "",
    description: data.horoscope,
    sourceApi: "freehoroscopeapi",
  };
}

async function fetchExternalHoroscope(signEn, when) {
  try {
    const raw = await fetchAztroHoroscope(signEn, when);
    return {
      raw: {
        ...raw,
        sourceApi: "aztro",
      },
      source: "aztro",
    };
  } catch (err) {
    console.warn("Aztro horoscope API failed:", err?.message || err);
  }

  const raw = await fetchFreeHoroscopeApi(signEn, when);
  return {
    raw,
    source: "freehoroscopeapi",
  };
}

async function localizeExternalHoroscope({ signZh, whenLabel, raw }) {
  const res = await openai.chat.completions.create({
    model: "gpt-4o-mini",
    response_format: { type: "json_object" },
    messages: [
      {
        role: "system",
        content:
          "你是繁體中文星座內容編輯。根據外部星座 API 的英文資料整理成自然中文。不要宣稱準確預測，不要新增恐嚇或保證。只回 JSON。",
      },
      {
        role: "user",
        content: `
請把以下外部每日星座資料整理成 LINE 卡片內容。

星座：${signZh}
時間：${whenLabel}
外部資料：
${JSON.stringify(raw)}

請回傳 JSON，格式：
{
  "overallStars": 1,
  "mood": "2~6字中文心情",
  "overall": "整體運勢，35字內",
  "work": "工作運，35字內",
  "love": "感情運，35字內",
  "money": "財運，35字內",
  "health": "健康提醒，35字內",
  "advice": "今日提醒，35字內",
  "luckyColor": "中文顏色",
  "luckyNumber": "沿用 API 數字",
  "luckyTime": "沿用 API 時間並中文化",
  "compatibility": "中文星座"
}

規則：
- overallStars 必須依照原始 description 的順逆感評 1~5，不可固定。
- luckyNumber / luckyTime / color / compatibility 若外部資料有提供就優先沿用；沒有就合理留空或補娛樂值。
- 文字要像每日星座網站，但不要太空泛。
- 不要提到 AI、API、英文原文。
`,
      },
    ],
    max_tokens: 500,
  });

  const localized = parseJsonObjectFromText(res.choices[0].message.content);
  return {
    overallStars: Math.max(
      1,
      Math.min(5, Math.round(Number(localized.overallStars) || 3))
    ),
    mood: String(localized.mood || raw.mood || "平穩").trim(),
    overall: String(localized.overall || raw.description || "").trim(),
    work: String(localized.work || "工作先求穩，再慢慢推進。").trim(),
    love: String(localized.love || "互動放自然一點，別急著定義。").trim(),
    money: String(localized.money || "財務適合保守，少做衝動決定。").trim(),
    health: String(localized.health || "留意精神消耗與休息品質。").trim(),
    advice: String(localized.advice || "今天先顧好自己的節奏。").trim(),
    luckyColor: String(localized.luckyColor || raw.color || "-").trim(),
    luckyNumber: String(localized.luckyNumber || raw.lucky_number || "-").trim(),
    luckyTime: String(localized.luckyTime || raw.lucky_time || "-").trim(),
    compatibility: String(
      localized.compatibility || raw.compatibility || "-"
    ).trim(),
  };
}

async function buildFallbackHoroscope(signZh, whenLabel) {
  const res = await openai.chat.completions.create({
    model: "gpt-4o-mini",
    response_format: { type: "json_object" },
    messages: [
      {
        role: "system",
        content:
          "你是繁體中文星座內容編輯。外部星座資料暫時不可用時，請用星座性格寫娛樂型每日提醒。只回 JSON。",
      },
      {
        role: "user",
        content: `
請產生「${whenLabel}${signZh}座」娛樂型每日提醒。
JSON 欄位同下：
{
  "overallStars": 1,
  "mood": "",
  "overall": "",
  "work": "",
  "love": "",
  "money": "",
  "health": "",
  "advice": "",
  "luckyColor": "",
  "luckyNumber": "",
  "luckyTime": "",
  "compatibility": ""
}
每句 35 字內，不要保證，不要太空泛。
`,
      },
    ],
    max_tokens: 500,
  });
  return parseJsonObjectFromText(res.choices[0].message.content);
}

async function getDailyHoroscope(signZh, when = "today") {
  const sign = ZODIAC_MAP[signZh];
  if (!sign) return null;

  const date = when === "tomorrow" ? getTodayKey(1) : getTodayKey(0);

  const kvKey = `horoscope:v6:${date}:${sign}:${getAztroDay(when)}`;

  // ① 先查 KV
  const cached = await redisGet(kvKey);
  if (cached) return JSON.parse(cached);

  const whenLabel = when === "tomorrow" ? "明日" : "今日";
  let raw = null;
  let data = null;
  let source = "aztro";
  try {
    const external = await fetchExternalHoroscope(sign, when);
    raw = external.raw;
    source = external.source;
    data = await localizeExternalHoroscope({ signZh, whenLabel, raw });
  } catch (err) {
    console.warn("External horoscope failed, fallback to OpenAI:", err?.message || err);
    source = "openai-fallback";
    data = await buildFallbackHoroscope(signZh, whenLabel);
  }

  const payload = {
    sign: signZh,
    when,
    source,
    sourceDate: raw?.current_date || date,
    raw,
    ...data,
  };

  // ③ 存 KV（一天）
  await redisSet(kvKey, JSON.stringify(payload), "EX", 60 * 60 * 24);

  return payload;
}

// 計算熱量
const LOCAL_TW_FOOD_CALORIE_DB = [
  {
    aliases: ["雞腿便當", "炸雞腿便當", "滷雞腿便當"],
    min: 700,
    max: 900,
    note: "主要差在配菜、炸或滷、飯量多寡。",
  },
  {
    aliases: ["排骨便當", "炸排骨便當", "滷排骨便當"],
    min: 750,
    max: 950,
    note: "排骨做法與白飯份量差異最大。",
  },
  {
    aliases: ["滷肉飯", "魯肉飯"],
    min: 450,
    max: 650,
    note: "肥肉比例、滷汁與份量會拉開差距。",
  },
  {
    aliases: ["雞排", "炸雞排"],
    min: 500,
    max: 700,
    note: "厚度、裹粉與油量會影響很多。",
  },
  {
    aliases: ["鹽酥雞"],
    min: 450,
    max: 700,
    note: "份量、是否加九層塔與配料差異很大。",
  },
  {
    aliases: ["蛋餅", "原味蛋餅"],
    min: 250,
    max: 350,
    note: "餅皮大小、油量與醬料會影響熱量。",
  },
  {
    aliases: ["起司蛋餅"],
    min: 320,
    max: 430,
    note: "起司與醬料會再往上加。",
  },
  {
    aliases: ["飯糰"],
    min: 300,
    max: 450,
    note: "內餡、油條與飯量是主要差異。",
  },
  {
    aliases: ["鮪魚飯糰"],
    min: 330,
    max: 460,
    note: "美乃滋與飯量不同，熱量會再往上。",
  },
  {
    aliases: ["蘿蔔糕"],
    min: 220,
    max: 320,
    note: "煎油量與份量會影響區間。",
  },
  {
    aliases: ["鍋貼", "一份鍋貼"],
    min: 350,
    max: 500,
    note: "以一份約 5 到 6 顆估算。",
  },
  {
    aliases: ["水煎包"],
    min: 180,
    max: 260,
    note: "內餡與大小不同，熱量差距不小。",
  },
  {
    aliases: ["小籠包"],
    min: 350,
    max: 500,
    note: "以一籠常見份量估算。",
  },
  {
    aliases: ["牛肉麵"],
    min: 550,
    max: 800,
    note: "湯頭油脂、麵量與牛肉份量差很多。",
  },
  {
    aliases: ["陽春麵"],
    min: 300,
    max: 450,
    note: "油蔥與麵量不同會有差異。",
  },
  {
    aliases: ["乾麵"],
    min: 350,
    max: 550,
    note: "醬料與麵量是主要差異。",
  },
  {
    aliases: ["滷味"],
    min: 250,
    max: 600,
    note: "配料差異很大，僅適合粗估。",
  },
  {
    aliases: ["臭豆腐"],
    min: 350,
    max: 550,
    note: "炸或蒸、醬料與泡菜量都會影響。",
  },
  {
    aliases: ["鹹酥雞"],
    min: 450,
    max: 700,
    note: "份量與是否搭配配料差很多。",
  },
  {
    aliases: ["蔥抓餅"],
    min: 300,
    max: 450,
    note: "油量與是否加蛋差異不小。",
  },
  {
    aliases: ["蔥抓餅加蛋"],
    min: 380,
    max: 520,
    note: "加蛋與醬料後通常會再往上。",
  },
  {
    aliases: ["漢堡"],
    min: 280,
    max: 450,
    note: "肉排、醬料與起司會影響差距。",
  },
  {
    aliases: ["三明治"],
    min: 220,
    max: 380,
    note: "內餡與美乃滋差異最大。",
  },
  {
    aliases: ["吐司夾蛋", "蛋吐司"],
    min: 250,
    max: 380,
    note: "是否抹醬與吐司片數會影響。",
  },
  {
    aliases: ["茶葉蛋"],
    min: 65,
    max: 85,
    note: "通常與一般雞蛋熱量接近。",
  },
  {
    aliases: ["荷包蛋"],
    min: 90,
    max: 120,
    note: "煎油量是主要差異。",
  },
  {
    aliases: ["白吐司"],
    min: 70,
    max: 100,
    note: "以一片常見份量估算。",
  },
  {
    aliases: ["白飯"],
    min: 230,
    max: 320,
    note: "以一碗常見份量估算。",
  },
  {
    aliases: ["地瓜球"],
    min: 250,
    max: 400,
    note: "份量與裹糖會影響不少。",
  },
  {
    aliases: ["紅茶"],
    min: 80,
    max: 180,
    note: "甜度與容量是主要差異。",
  },
  {
    aliases: ["綠茶"],
    min: 0,
    max: 120,
    note: "無糖幾乎很低，有糖會往上。",
  },
  {
    aliases: ["咖啡"],
    min: 5,
    max: 180,
    note: "黑咖啡很低，加糖奶精後會往上。",
  },
  {
    aliases: ["拿鐵"],
    min: 120,
    max: 250,
    note: "奶量、糖量與容量差異最大。",
  },
  {
    aliases: ["炒飯"],
    min: 600,
    max: 850,
    note: "油量與配料會讓熱量差很多。",
  },
  {
    aliases: ["炒麵"],
    min: 500,
    max: 750,
    note: "麵量與油量是主要變因。",
  },
  {
    aliases: ["珍珠奶茶", "珍奶"],
    min: 350,
    max: 550,
    note: "甜度、容量與珍珠份量影響最大。",
  },
  {
    aliases: ["奶茶"],
    min: 180,
    max: 350,
    note: "全糖大杯會明顯更高。",
  },
  {
    aliases: ["無糖豆漿"],
    min: 90,
    max: 150,
    note: "以中杯到大杯常見容量估算。",
  },
  {
    aliases: ["豆漿"],
    min: 120,
    max: 220,
    note: "是否加糖與容量會拉開差距。",
  },
];

const USDA_QUERY_ALIAS_MAP = {
  雞蛋: "egg",
  蛋: "egg",
  茶葉蛋: "egg",
  荷包蛋: "fried egg",
  香蕉: "banana",
  蘋果: "apple",
  柳丁: "orange",
  橘子: "orange",
  葡萄: "grapes",
  草莓: "strawberries",
  藍莓: "blueberries",
  牛奶: "milk",
  豆漿: "soy milk",
  雞胸肉: "chicken breast",
  雞胸: "chicken breast",
  雞腿: "chicken leg",
  牛肉: "beef",
  豬肉: "pork",
  鮭魚: "salmon",
  鮪魚: "tuna",
  白飯: "white rice cooked",
  糙米飯: "brown rice cooked",
  燕麥: "oats",
  地瓜: "sweet potato",
  馬鈴薯: "potato",
  優格: "yogurt",
  吐司: "toast",
  白吐司: "white bread",
  花生醬: "peanut butter",
  起司: "cheese",
  豆腐: "tofu",
  高麗菜: "cabbage",
  青花菜: "broccoli",
  花椰菜: "broccoli",
  番茄: "tomato",
  小黃瓜: "cucumber",
  胡蘿蔔: "carrot",
  玉米: "corn",
  水餃: "dumpling",
  麵條: "noodles",
  白麵: "noodles",
  義大利麵: "spaghetti cooked",
};

function normalizeFoodName(food) {
  return String(food || "")
    .trim()
    .replace(/^(幫我算|幫我估|請問|我今天|我剛剛|我有|我吃了|我喝了)/g, "")
    .replace(/(熱量|卡路里|大卡|幾卡|多少卡|是多少|大約|約)$/g, "")
    .replace(/^(一個|一顆|一份|一杯|一碗|一盤|一盒|一條)/, "")
    .replace(/[（）()]/g, "")
    .replace(/\s+/g, "")
    .trim();
}

function parseFoodList(text) {
  return text
    .replace(/^(助理|KevinBot|kevinbot)\s*/i, "")
    .replace(/我(今天|剛剛)?吃了/g, "")
    .replace(/我(今天|剛剛)?喝了/g, "")
    .replace(/(熱量|卡路里|大卡|幾卡|多少卡|是多少)/g, "")
    .replace(/加/g, "、")
    .replace(/\+/g, "、")
    .replace(/外加/g, "、")
    .replace(/再來一份/g, "、")
    .replace(/配/g, "、")
    .split(/、|,|，|跟|和|以及|\n/)
    .map((s) => normalizeFoodName(s))
    .filter(Boolean);
}

function findLocalTwFoodCalorie(food) {
  const normalized = normalizeFoodName(food);
  if (!normalized) return null;

  for (const item of LOCAL_TW_FOOD_CALORIE_DB) {
    if (item.aliases.some((alias) => normalizeFoodName(alias) === normalized)) {
      return {
        food,
        min: item.min,
        max: item.max,
        note: item.note,
        source: "local-tw",
      };
    }
  }

  return null;
}

function getUsdaQuery(food) {
  const normalized = normalizeFoodName(food);
  return USDA_QUERY_ALIAS_MAP[normalized] || "";
}

function extractUsdaCalories(food) {
  const labelCalories = food?.labelNutrients?.calories?.value;
  if (Number.isFinite(labelCalories)) return Number(labelCalories);

  const nutrients = Array.isArray(food?.foodNutrients) ? food.foodNutrients : [];
  for (const nutrient of nutrients) {
    const name = String(
      nutrient?.nutrientName ||
        nutrient?.nutrient?.name ||
        nutrient?.name ||
        ""
    ).toLowerCase();
    const value = Number(
      nutrient?.value ?? nutrient?.amount ?? nutrient?.nutrientValue
    );
    if (name.includes("energy") && Number.isFinite(value)) {
      return value;
    }
  }
  return null;
}

function getUsdaServingText(food) {
  if (
    Number.isFinite(food?.servingSize) &&
    String(food?.servingSizeUnit || "").trim()
  ) {
    return `${food.servingSize}${food.servingSizeUnit}`;
  }
  if (String(food?.householdServingFullText || "").trim()) {
    return String(food.householdServingFullText).trim();
  }
  return "每份";
}

async function fetchUsdaFoodCalorie(food) {
  if (!USDA_API_KEY) return null;

  const query = getUsdaQuery(food);
  if (!query) return null;

  const url = `https://api.nal.usda.gov/fdc/v1/foods/search?api_key=${encodeURIComponent(
    USDA_API_KEY
  )}`;
  const json = await fetchJsonWithTimeout(
    url,
    {
      method: "POST",
      headers: {
        accept: "application/json",
        "content-type": "application/json",
      },
      body: JSON.stringify({
        query,
        pageSize: 3,
      }),
    },
    10000
  );

  const foods = Array.isArray(json?.foods) ? json.foods : [];
  const first = foods[0];
  if (!first) return null;

  const calories = extractUsdaCalories(first);
  if (!Number.isFinite(calories)) return null;

  return {
    food,
    min: Math.max(0, Math.round(calories)),
    max: Math.max(0, Math.round(calories)),
    note: `參考 USDA 資料，${getUsdaServingText(first)}估算。`,
    source: "usda",
  };
}

async function estimateFoodCalorie(food) {
  const today = getTodayKey(0);
  const normalizedFood = normalizeFoodName(food);
  const key = `food:v2:estimate:${today}:${normalizedFood}`;

  const cached = await redisGet(key);
  if (cached) return JSON.parse(cached);

  const localEstimate = findLocalTwFoodCalorie(food);
  if (localEstimate) {
    await redisSet(key, JSON.stringify(localEstimate), "EX", 60 * 60 * 24);
    return localEstimate;
  }

  try {
    const usdaEstimate = await fetchUsdaFoodCalorie(food);
    if (usdaEstimate) {
      await redisSet(key, JSON.stringify(usdaEstimate), "EX", 60 * 60 * 24);
      return usdaEstimate;
    }
  } catch (err) {
    console.warn("USDA calorie lookup failed:", err?.message || err);
  }

  const res = await openai.chat.completions.create({
    model: "gpt-4o-mini",
    response_format: { type: "json_object" },
    messages: [
      {
        role: "system",
        content:
          "你是生活型熱量估算助理，只能提供熱量區間，不可假裝精準營養標示。請只回 JSON。",
      },
      {
        role: "user",
        content: `
請估算以下食物的熱量區間（台灣常見份量）：

食物：${normalizedFood}

格式：
{
  "food": "${normalizedFood}",
  "min": 0,
  "max": 0,
  "note": "一句影響因素"
}
`,
      },
      ],
    max_tokens: 150,
  });

  const parsed = parseJsonObjectFromText(res.choices[0].message.content);
  const data = {
    food: String(parsed.food || normalizedFood).trim() || normalizedFood,
    min: Math.max(0, Math.round(Number(parsed.min) || 0)),
    max: Math.max(
      Math.round(Number(parsed.min) || 0),
      Math.round(Number(parsed.max) || Number(parsed.min) || 0)
    ),
    note: String(parsed.note || "份量與做法會影響實際熱量。").trim(),
    source: "openai",
  };

  await redisSet(key, JSON.stringify(data), "EX", 60 * 60 * 24);

  return data;
}

// 股市 15分鐘延遲

const STOCKS_REDIS_KEY = "twse:stocks:all";
const STOCKS_META_REDIS_KEY = "twse:stocks:meta";
const TWSE_STOCKS_OPENAPI_URL =
  "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL";
const TWSE_STOCKS_CSV_URL =
  "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=open_data";
const TPEX_STOCKS_JSON_URL =
  "https://www.tpex.org.tw/www/zh-tw/afterTrading/otc?date=&type=EW&response=json";
const TWSE_BASIC_INFO_CSV_URL =
  "https://mopsfin.twse.com.tw/opendata/t187ap03_L.csv";
const TPEX_BASIC_INFO_CSV_URL =
  "https://mopsfin.twse.com.tw/opendata/t187ap03_O.csv";
const TWSE_EPS_CSV_URL = "https://mopsfin.twse.com.tw/opendata/t187ap14_L.csv";
const TPEX_EPS_CSV_URL = "https://mopsfin.twse.com.tw/opendata/t187ap14_O.csv";
const TWSE_PE_JSON_URL = "https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_d";
const TPEX_PE_HTML_URL =
  "https://www.tpex.org.tw/web/stock/aftertrading/peratio_analysis/pera_result.php?l=zh-tw&o=htm";
const TWSE_ATTENTION_HTML_URL =
  "https://www.twse.com.tw/announcement/notice?response=html";
const TWSE_DISPOSITION_HTML_URL =
  "https://www.twse.com.tw/announcement/punish?response=html";
const TPEX_ATTENTION_QUERY_URL =
  "https://www.tpex.org.tw/web/bulletin/attention_information/trading_attention_information.php";
const TPEX_DISPOSITION_QUERY_URL =
  "https://www.tpex.org.tw/web/bulletin/disposal_information/disposal_information.php";
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
const POSTMARKET_PICKS_CACHE_PREFIX = "stock:postmarket:recommend:v8:";
const DAILY_QUOTES_CACHE_PREFIX = "stock:dailyquotes:v2:";
const POSTMARKET_SCAN_TTL_SECONDS = Number(
  process.env.POSTMARKET_SCAN_TTL_SECONDS || 60 * 60 * 20
);
const POSTMARKET_REQUIRED_TRADING_DAYS = Number(
  process.env.POSTMARKET_REQUIRED_TRADING_DAYS || 21
);
const POSTMARKET_MAX_CALENDAR_LOOKBACK = Number(
  process.env.POSTMARKET_MAX_CALENDAR_LOOKBACK || 40
);
const POSTMARKET_REPLY_LIMIT = Number(process.env.POSTMARKET_REPLY_LIMIT || 5);
const POSTMARKET_HIGHRISK_REPLY_LIMIT = Number(
  process.env.POSTMARKET_HIGHRISK_REPLY_LIMIT || 3
);
const POSTMARKET_MAX_PER_INDUSTRY = Number(
  process.env.POSTMARKET_MAX_PER_INDUSTRY || 2
);
const POSTMARKET_PE_RED_FLAG_THRESHOLD = Number(
  process.env.POSTMARKET_PE_RED_FLAG_THRESHOLD || 40
);
const POSTMARKET_PE_LOW_LIQUIDITY_VALUE = Number(
  process.env.POSTMARKET_PE_LOW_LIQUIDITY_VALUE || 2e8
);
const STOCK_INDUSTRY_NAME_MAP = {
  "01": "水泥工業",
  "02": "食品工業",
  "03": "塑膠工業",
  "04": "紡織纖維",
  "05": "電機機械",
  "06": "電器電纜",
  "08": "玻璃陶瓷",
  "09": "造紙工業",
  "10": "鋼鐵工業",
  "11": "橡膠工業",
  "12": "汽車工業",
  "14": "建材營造",
  "15": "航運業",
  "16": "觀光餐旅",
  "17": "金融保險",
  "18": "貿易百貨",
  "20": "其他",
  "21": "化學工業",
  "22": "生技醫療",
  "23": "油電燃氣",
  "24": "半導體業",
  "25": "電腦及週邊設備業",
  "26": "光電業",
  "27": "通信網路業",
  "28": "電子零組件業",
  "29": "電子通路業",
  "30": "資訊服務業",
  "31": "其他電子業",
  "32": "文化創意業",
  "33": "農業科技業",
  "34": "電子商務",
  "35": "綠能環保",
  "36": "數位雲端",
  "37": "運動休閒",
};
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

function addStockRecord(
  stocks,
  {
    code,
    name,
    market = "TWSE",
    symbol,
    industry = null,
    epsLatestQuarter = null,
    epsYear = null,
    epsQuarter = null,
    epsTtm = null,
    epsTtmYear = null,
    epsTtmQuarter = null,
    peRatio = null,
    isAttentionStock = null,
    isDispositionStock = null,
  }
) {
  const normalizedCode = normalizeStockCode(code);
  const normalizedName = String(name || "").trim();
  if (!normalizedCode || !normalizedName) return false;

  const current = stocks[normalizedCode] || {};
  stocks[normalizedCode] = {
    ...current,
    code: normalizedCode,
    name: normalizedName,
    market,
    symbol: symbol || `${normalizedCode}.${market === "TPEX" ? "TWO" : "TW"}`,
    aliases:
      Array.isArray(current.aliases) && current.aliases.length
        ? current.aliases
        : getCommonEtfAliases(normalizedCode),
    industry:
      typeof industry === "string" && industry.trim()
        ? industry.trim()
        : current.industry || null,
    epsLatestQuarter:
      typeof epsLatestQuarter === "number" && Number.isFinite(epsLatestQuarter)
        ? epsLatestQuarter
        : typeof current.epsLatestQuarter === "number" &&
          Number.isFinite(current.epsLatestQuarter)
        ? current.epsLatestQuarter
        : null,
    epsYear:
      Number.isFinite(epsYear)
        ? Number(epsYear)
        : Number.isFinite(current.epsYear)
        ? Number(current.epsYear)
        : null,
    epsQuarter:
      Number.isFinite(epsQuarter)
        ? Number(epsQuarter)
        : Number.isFinite(current.epsQuarter)
        ? Number(current.epsQuarter)
        : null,
    epsTtm:
      typeof epsTtm === "number" && Number.isFinite(epsTtm)
        ? epsTtm
        : typeof current.epsTtm === "number" && Number.isFinite(current.epsTtm)
        ? current.epsTtm
        : null,
    epsTtmYear:
      Number.isFinite(epsTtmYear)
        ? Number(epsTtmYear)
        : Number.isFinite(current.epsTtmYear)
        ? Number(current.epsTtmYear)
        : null,
    epsTtmQuarter:
      Number.isFinite(epsTtmQuarter)
        ? Number(epsTtmQuarter)
        : Number.isFinite(current.epsTtmQuarter)
        ? Number(current.epsTtmQuarter)
        : null,
    peRatio:
      typeof peRatio === "number" && Number.isFinite(peRatio)
        ? peRatio
        : typeof current.peRatio === "number" && Number.isFinite(current.peRatio)
        ? current.peRatio
        : null,
    isAttentionStock:
      typeof isAttentionStock === "boolean"
        ? isAttentionStock
        : typeof current.isAttentionStock === "boolean"
        ? current.isAttentionStock
        : false,
    isDispositionStock:
      typeof isDispositionStock === "boolean"
        ? isDispositionStock
        : typeof current.isDispositionStock === "boolean"
        ? current.isDispositionStock
        : false,
  };
  return true;
}

function mergeStockEnrichmentFromCache(stocks, cachedStocks = {}) {
  const enrichmentFields = [
    "industry",
    "epsLatestQuarter",
    "epsYear",
    "epsQuarter",
    "epsTtm",
    "epsTtmYear",
    "epsTtmQuarter",
    "peRatio",
  ];
  let merged = 0;

  for (const [code, stock] of Object.entries(stocks || {})) {
    const cached = cachedStocks?.[code];
    if (!stock || !cached) continue;
    let touched = false;

    for (const field of enrichmentFields) {
      if (stock[field] != null) continue;
      const value = cached[field];
      if (value == null) continue;
      stock[field] = value;
      touched = true;
    }

    if (touched) merged++;
  }

  return merged;
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

function normalizeIndustryName(value) {
  const text = String(value || "").trim();
  if (!text || text === "--" || text === "N/A") return null;
  return text;
}

function normalizeNumericText(value) {
  return String(value || "")
    .trim()
    .replace(/,/g, "")
    .replace(/^\((.*)\)$/, "-$1");
}

function normalizeEpsValue(value) {
  const text = normalizeNumericText(value);
  if (!text || text === "--" || text === "N/A") return null;
  const number = Number(text);
  return Number.isFinite(number) ? number : null;
}

function parseEpsQuarterOrder(value) {
  const text = String(value || "").trim().toUpperCase();
  if (!text) return null;
  const match = text.match(/([1-4])/);
  if (!match) return null;
  return Number(match[1]);
}

function isNewerQuarterPeriod(aYear, aQuarter, bYear, bQuarter) {
  if (!Number.isFinite(aYear) || !Number.isFinite(aQuarter)) return false;
  if (!Number.isFinite(bYear) || !Number.isFinite(bQuarter)) return true;
  if (aYear !== bYear) return aYear > bYear;
  return aQuarter > bQuarter;
}

function parseCompanyBasicInfoCsv(text, market = "TWSE") {
  const lines = String(text || "")
    .split(/\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  if (!lines.length) return {};

  const header = parseCsvLine(lines[0]);
  const codeIndex = header.findIndex((c) => c.includes("公司代號"));
  const nameIndex = header.findIndex((c) => c.includes("公司名稱"));
  const industryIndex = header.findIndex((c) => c.includes("產業別"));
  if (codeIndex < 0 || nameIndex < 0 || industryIndex < 0) return {};

  const stocks = {};
  for (let i = 1; i < lines.length; i++) {
    const cols = parseCsvLine(lines[i]);
    addStockRecord(stocks, {
      code: cols[codeIndex],
      name: cols[nameIndex],
      market,
      industry: normalizeIndustryName(cols[industryIndex]),
    });
  }

  return stocks;
}

function parseCompanyEpsCsv(text, market = "TWSE") {
  const lines = String(text || "")
    .split(/\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  if (!lines.length) return {};

  const header = parseCsvLine(lines[0]);
  const codeIndex = header.findIndex((c) => c.includes("公司代號"));
  const nameIndex = header.findIndex((c) => c.includes("公司名稱"));
  const yearIndex = header.findIndex((c) => c.includes("年度"));
  const quarterIndex = header.findIndex((c) => c.includes("季別"));
  const epsIndex = header.findIndex((c) => c.includes("基本每股盈餘"));
  if (
    codeIndex < 0 ||
    nameIndex < 0 ||
    yearIndex < 0 ||
    quarterIndex < 0 ||
    epsIndex < 0
  ) {
    return {};
  }

  const latestByCode = new Map();
  let latestYear = null;
  let latestQuarter = null;

  for (let i = 1; i < lines.length; i++) {
    const cols = parseCsvLine(lines[i]);
    const code = normalizeStockCode(cols[codeIndex]);
    const name = String(cols[nameIndex] || "").trim();
    const year = Number(normalizeNumericText(cols[yearIndex]));
    const quarter = parseEpsQuarterOrder(cols[quarterIndex]);
    const eps = normalizeEpsValue(cols[epsIndex]);
    if (!code || !name || !Number.isFinite(year) || !quarter || eps == null) {
      continue;
    }

    const current = latestByCode.get(code);
    if (
      !current ||
      year > current.year ||
      (year === current.year && quarter > current.quarter)
    ) {
      latestByCode.set(code, { code, name, market, year, quarter, eps });
    }
    if (isNewerQuarterPeriod(year, quarter, latestYear, latestQuarter)) {
      latestYear = year;
      latestQuarter = quarter;
    }
  }

  const stocks = {};
  for (const row of latestByCode.values()) {
    addStockRecord(stocks, {
      code: row.code,
      name: row.name,
      market,
      epsLatestQuarter: Number(row.eps.toFixed(2)),
      epsYear: row.year,
      epsQuarter: row.quarter,
    });
  }

  return { stocks, latestYear, latestQuarter };
}

function parseTwsePeCsv(text) {
  const lines = String(text || "")
    .split(/\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  if (!lines.length) return {};

  const headerIndex = lines.findIndex(
    (line) => line.includes("證券代號") && line.includes("本益比")
  );
  if (headerIndex < 0) return {};

  const header = parseCsvLine(lines[headerIndex]);
  const codeIndex = header.findIndex((c) => c.includes("證券代號"));
  const nameIndex = header.findIndex((c) => c.includes("證券名稱"));
  const peIndex = header.findIndex((c) => c.includes("本益比"));
  if (codeIndex < 0 || nameIndex < 0 || peIndex < 0) return {};

  const stocks = {};
  for (let i = headerIndex + 1; i < lines.length; i++) {
    const cols = parseCsvLine(lines[i]);
    const code = normalizeStockCode(cols[codeIndex]);
    const name = String(cols[nameIndex] || "").trim();
    const peRatio = normalizeEpsValue(cols[peIndex]);
    if (!code || !name || peRatio == null) continue;
    addStockRecord(stocks, {
      code,
      name,
      market: "TWSE",
      peRatio: Number(peRatio.toFixed(2)),
    });
  }

  return stocks;
}

function decodeHtmlEntities(text) {
  return String(text || "")
    .replace(/&nbsp;|&#160;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'");
}

function stripHtmlToText(html) {
  return decodeHtmlEntities(
    String(html || "")
      .replace(/<br\s*\/?>/gi, " ")
      .replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ")
      .trim()
  );
}

function parseTwsePeOpenApi(items) {
  const stocks = {};
  if (!Array.isArray(items)) return stocks;

  for (const item of items) {
    const code =
      item?.["證券代號"] ||
      item?.["股票代號"] ||
      item?.SecuritiesCompanyCode ||
      item?.Code;
    const name =
      item?.["證券名稱"] ||
      item?.["股票名稱"] ||
      item?.SecuritiesCompanyName ||
      item?.Name;
    const peRatio = normalizeEpsValue(item?.["本益比"] || item?.PERatio || item?.PEratio);
    if (!normalizeStockCode(code) || !String(name || "").trim() || peRatio == null) {
      continue;
    }
    addStockRecord(stocks, {
      code,
      name,
      market: "TWSE",
      peRatio: Number(peRatio.toFixed(2)),
    });
  }

  return stocks;
}

function parseTpexPeHtml(text) {
  const rows = [...String(text || "").matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/gi)];
  const stocks = {};
  let codeIndex = -1;
  let nameIndex = -1;
  let peIndex = -1;

  for (const match of rows) {
    const cells = [...match[1].matchAll(/<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi)].map(
      (cellMatch) => stripHtmlToText(cellMatch[1])
    );
    if (!cells.length) continue;

    if (
      cells.some((cell) => cell.includes("股票代號") || cell.includes("代號")) &&
      cells.some((cell) => cell.includes("本益比"))
    ) {
      codeIndex = cells.findIndex((cell) => cell.includes("股票代號") || cell.includes("代號"));
      nameIndex = cells.findIndex((cell) => cell.includes("名稱"));
      peIndex = cells.findIndex((cell) => cell.includes("本益比"));
      continue;
    }

    if (codeIndex < 0 || nameIndex < 0 || peIndex < 0) continue;
    if (cells.length <= Math.max(codeIndex, nameIndex, peIndex)) continue;

    const code = normalizeStockCode(cells[codeIndex]);
    const name = String(cells[nameIndex] || "").trim();
    const peRatio = normalizeEpsValue(cells[peIndex]);
    if (!code || !name || peRatio == null) continue;

    addStockRecord(stocks, {
      code,
      name,
      market: "TPEX",
      peRatio: Number(peRatio.toFixed(2)),
    });
  }

  return stocks;
}

function parseHtmlTableRows(text) {
  const rows = [...String(text || "").matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/gi)];
  return rows.map((match) =>
    [...match[1].matchAll(/<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi)].map((cellMatch) =>
      stripHtmlToText(cellMatch[1])
    )
  );
}

function parseTwseAttentionHtml(text) {
  const rows = parseHtmlTableRows(text);
  const stocks = {};
  let codeIndex = -1;
  let nameIndex = -1;

  for (const cells of rows) {
    if (!cells.length) continue;

    if (
      cells.some((cell) => cell.includes("證券代號")) &&
      cells.some((cell) => cell.includes("證券名稱"))
    ) {
      codeIndex = cells.findIndex((cell) => cell.includes("證券代號"));
      nameIndex = cells.findIndex((cell) => cell.includes("證券名稱"));
      continue;
    }

    if (codeIndex < 0 || nameIndex < 0) continue;
    if (cells.length <= Math.max(codeIndex, nameIndex)) continue;

    const code = normalizeStockCode(cells[codeIndex]);
    const name = String(cells[nameIndex] || "").trim();
    if (!code || !name) continue;

    addStockRecord(stocks, {
      code,
      name,
      market: "TWSE",
      isAttentionStock: true,
    });
  }

  return stocks;
}

function parseTwseDispositionHtml(text) {
  const rows = parseHtmlTableRows(text);
  const stocks = {};
  let codeIndex = -1;
  let nameIndex = -1;

  for (const cells of rows) {
    if (!cells.length) continue;

    if (
      cells.some((cell) => cell.includes("證券代號")) &&
      cells.some((cell) => cell.includes("證券名稱"))
    ) {
      codeIndex = cells.findIndex((cell) => cell.includes("證券代號"));
      nameIndex = cells.findIndex((cell) => cell.includes("證券名稱"));
      continue;
    }

    if (codeIndex < 0 || nameIndex < 0) continue;
    if (cells.length <= Math.max(codeIndex, nameIndex)) continue;

    const code = normalizeStockCode(cells[codeIndex]);
    const name = String(cells[nameIndex] || "").trim();
    if (!code || !name) continue;

    addStockRecord(stocks, {
      code,
      name,
      market: "TWSE",
      isDispositionStock: true,
    });
  }

  return stocks;
}

function parseTpexAttentionJson(json) {
  const stocks = {};
  const tables = Array.isArray(json?.tables) ? json.tables : [];

  for (const table of tables) {
    const fields = Array.isArray(table?.fields) ? table.fields.map(String) : [];
    const data = Array.isArray(table?.data) ? table.data : [];
    const codeIndex = fields.findIndex((field) => field.includes("證券代號"));
    const nameIndex = fields.findIndex((field) => field.includes("證券名稱"));
    if (codeIndex < 0 || nameIndex < 0) continue;

    for (const row of data) {
      if (!Array.isArray(row)) continue;
      const code = normalizeStockCode(row[codeIndex]);
      const name = String(row[nameIndex] || "").trim();
      if (!code || !name) continue;
      addStockRecord(stocks, {
        code,
        name,
        market: "TPEX",
        isAttentionStock: true,
      });
    }
  }

  return stocks;
}

function parseTpexDispositionJson(json) {
  const stocks = {};
  const tables = Array.isArray(json?.tables) ? json.tables : [];

  for (const table of tables) {
    const fields = Array.isArray(table?.fields) ? table.fields.map(String) : [];
    const data = Array.isArray(table?.data) ? table.data : [];
    const codeIndex = fields.findIndex((field) => field.includes("證券代號"));
    const nameIndex = fields.findIndex((field) => field.includes("證券名稱"));
    if (codeIndex < 0 || nameIndex < 0) continue;

    for (const row of data) {
      if (!Array.isArray(row)) continue;
      const code = normalizeStockCode(row[codeIndex]);
      const name = String(row[nameIndex] || "").trim();
      if (!code || !name) continue;
      addStockRecord(stocks, {
        code,
        name,
        market: "TPEX",
        isDispositionStock: true,
      });
    }
  }

  return stocks;
}

async function fetchTextResponse(url, options = {}) {
  const res = await fetch(url, options);
  const contentType = res.headers.get("content-type") || "";
  const arrayBuffer = await res.arrayBuffer();
  const bytes = new Uint8Array(arrayBuffer);
  const utf8Text = new TextDecoder("utf-8").decode(bytes);
  return { res, contentType, bytes, text: utf8Text };
}

async function fetchStockIndustryCsv(url, market, sourceLabel) {
  const res = await fetch(url, {
    headers: {
      "user-agent": "Mozilla/5.0",
      accept: "text/csv,*/*;q=0.8",
    },
  });
  const contentType = res.headers.get("content-type") || "";
  const text = await res.text();
  const looksLikeHTML = /<html|<!doctype html|頁面無法執行/i.test(text);

  if (!res.ok || looksLikeHTML) {
    throw new Error(`${sourceLabel} unavailable: ${res.status}`);
  }

  return {
    contentType,
    head120: text.slice(0, 120),
    stocks: parseCompanyBasicInfoCsv(text, market),
  };
}

async function fetchStockEpsCsv(url, market, sourceLabel) {
  const res = await fetch(url, {
    headers: {
      "user-agent": "Mozilla/5.0",
      accept: "text/csv,*/*;q=0.8",
    },
  });
  const contentType = res.headers.get("content-type") || "";
  const text = await res.text();
  const looksLikeHTML = /<html|<!doctype html|頁面無法執行/i.test(text);

  if (!res.ok || looksLikeHTML) {
    throw new Error(`${sourceLabel} unavailable: ${res.status}`);
  }

  return {
    contentType,
    head120: text.slice(0, 120),
    ...parseCompanyEpsCsv(text, market),
  };
}

async function fetchTwsePeCsv() {
  const res = await fetch(TWSE_PE_JSON_URL, {
    headers: {
      "user-agent": "Mozilla/5.0",
      accept: "application/json,*/*;q=0.8",
    },
  });
  const contentType = res.headers.get("content-type") || "";
  const items = await res.json();
  if (!res.ok || !Array.isArray(items)) {
    throw new Error(`twse-pe unavailable: ${res.status}`);
  }
  return {
    contentType,
    head120: JSON.stringify(items).slice(0, 120),
    stocks: parseTwsePeOpenApi(items),
  };
}

async function fetchTpexPeJson() {
  const { res, contentType, text } = await fetchTextResponse(TPEX_PE_HTML_URL, {
    headers: {
      "user-agent": "Mozilla/5.0",
      accept: "text/html,application/xhtml+xml,*/*;q=0.8",
    },
  });
  const looksLikeHTML = /<html|<!doctype html|頁面無法執行/i.test(text);
  if (!res.ok || !looksLikeHTML) {
    throw new Error(`tpex-pe unavailable: ${res.status}`);
  }
  return {
    contentType,
    head120: text.slice(0, 120),
    stocks: parseTpexPeHtml(text),
  };
}

async function fetchTwseAttentionHtml() {
  const endDate = compactDateKeyFromDate(getTaipeiCalendarDate(0));
  const startDate = compactDateKeyFromDate(getTaipeiCalendarDate(1));
  const url = `${TWSE_ATTENTION_HTML_URL}&startDate=${startDate}&endDate=${endDate}`;
  const { res, contentType, text } = await fetchTextResponse(url, {
    headers: {
      "user-agent": "Mozilla/5.0",
      accept: "text/html,*/*;q=0.8",
    },
  });
  const looksLikeHTML = /<html|<!doctype html/i.test(text);
  if (!res.ok || !looksLikeHTML) {
    throw new Error(`twse-attention unavailable: ${res.status}`);
  }
  return {
    contentType,
    head120: text.slice(0, 120),
    stocks: parseTwseAttentionHtml(text),
  };
}

async function fetchTwseDispositionHtml() {
  const { res, contentType, text } = await fetchTextResponse(
    TWSE_DISPOSITION_HTML_URL,
    {
      headers: {
        "user-agent": "Mozilla/5.0",
        accept: "text/html,*/*;q=0.8",
      },
    }
  );
  const looksLikeHTML = /<html|<!doctype html/i.test(text);
  if (!res.ok || !looksLikeHTML) {
    throw new Error(`twse-disposition unavailable: ${res.status}`);
  }
  return {
    contentType,
    head120: text.slice(0, 120),
    stocks: parseTwseDispositionHtml(text),
  };
}

async function fetchTpexAttentionJson() {
  const res = await fetch("https://www.tpex.org.tw/www/zh-tw/bulletin/attention", {
    headers: {
      "user-agent": "Mozilla/5.0",
      accept: "application/json,text/plain,*/*",
    },
  });
  const contentType = res.headers.get("content-type") || "";
  const text = await res.text();
  if (!res.ok) {
    throw new Error(`tpex-attention unavailable: ${res.status}`);
  }
  const json = JSON.parse(text);
  if (String(json?.stat || "").toLowerCase() !== "ok") {
    throw new Error(`tpex-attention unavailable: ${res.status}`);
  }
  return {
    contentType,
    head120: text.slice(0, 120),
    stocks: parseTpexAttentionJson(json),
  };
}

async function fetchTpexDispositionJson() {
  const res = await fetch(
    "https://www.tpex.org.tw/www/zh-tw/bulletin/disposal",
    {
      headers: {
        "user-agent": "Mozilla/5.0",
        accept: "application/json,text/plain,*/*",
      },
    }
  );
  const contentType = res.headers.get("content-type") || "";
  const text = await res.text();
  if (!res.ok) {
    throw new Error(`tpex-disposition unavailable: ${res.status}`);
  }
  const json = JSON.parse(text);
  if (String(json?.stat || "").toLowerCase() !== "ok") {
    throw new Error(`tpex-disposition unavailable: ${res.status}`);
  }
  return {
    contentType,
    head120: text.slice(0, 120),
    stocks: parseTpexDispositionJson(json),
  };
}

function getEpsHistorySnapshot(market, year, quarter) {
  const marketKey = String(market || "").toUpperCase();
  const yearKey = `${Number(year)}Q${Number(quarter)}`;
  const snapshots = epsQuarterlyHistory?.snapshots?.[marketKey];
  if (!snapshots || typeof snapshots !== "object") return null;
  const snapshot = snapshots[yearKey];
  return snapshot && typeof snapshot === "object" ? snapshot : null;
}

function buildTrailingFourQuarterEpsFromSeed(market, latestStocks, latestYear, latestQuarter) {
  if (!Number.isFinite(latestYear) || !Number.isFinite(latestQuarter) || latestQuarter <= 0) {
    return {};
  }

  const stocks = {};
  const previousYearAnnual = getEpsHistorySnapshot(market, latestYear - 1, 4);
  const previousYearSameQuarter =
    latestQuarter === 4
      ? null
      : getEpsHistorySnapshot(market, latestYear - 1, latestQuarter);

  Object.entries(latestStocks || {}).forEach(([code, info]) => {
    if (!info || typeof info.epsLatestQuarter !== "number" || !Number.isFinite(info.epsLatestQuarter)) {
      return;
    }

    let epsTtm = null;
    if (latestQuarter === 4) {
      epsTtm = info.epsLatestQuarter;
    } else {
      const priorAnnual = normalizeEpsValue(previousYearAnnual?.[code]);
      const priorSameQuarter = normalizeEpsValue(previousYearSameQuarter?.[code]);
      if (priorAnnual != null && priorSameQuarter != null) {
        epsTtm = info.epsLatestQuarter + priorAnnual - priorSameQuarter;
      }
    }

    if (epsTtm == null || !Number.isFinite(epsTtm)) return;

    addStockRecord(stocks, {
      code,
      name: info.name,
      market,
      epsLatestQuarter: info.epsLatestQuarter,
      epsYear: info.epsYear,
      epsQuarter: info.epsQuarter,
      epsTtm: Number(epsTtm.toFixed(2)),
      epsTtmYear: latestYear,
      epsTtmQuarter: latestQuarter,
    });
  });

  return stocks;
}

function getPostMarketPicksCacheKey(dateKey) {
  return `${POSTMARKET_PICKS_CACHE_PREFIX}${dateKey}`;
}

function getStockIndustryKey(industry) {
  const text = normalizeStockText(industry);
  return text || "__UNKNOWN__";
}

function formatStockIndustryName(industry) {
  const code = String(industry || "").trim();
  if (!code) return "未知";
  return STOCK_INDUSTRY_NAME_MAP[code] || code;
}

function applyIndustryDiversityLimit(
  picks,
  maxPerIndustry = POSTMARKET_MAX_PER_INDUSTRY
) {
  const limit =
    Number.isFinite(maxPerIndustry) && maxPerIndustry > 0 ? maxPerIndustry : 2;
  const counters = new Map();
  const selected = [];

  for (const pick of Array.isArray(picks) ? picks : []) {
    const key = getStockIndustryKey(pick?.industry);
    const count = counters.get(key) || 0;
    if (count >= limit) continue;
    counters.set(key, count + 1);
    selected.push(pick);
  }

  return selected;
}

function getDailyQuotesCacheKey(market, dateKey) {
  return `${DAILY_QUOTES_CACHE_PREFIX}${market}:${dateKey}`;
}

function formatTwseQueryDate(date) {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return `${year}${month}${day}`;
}

function formatTpexQueryDate(date) {
  const year = date.getUTCFullYear() - 1911;
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return `${year}/${month}/${day}`;
}

function dateKeyFromDate(date) {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function compactDateKeyFromDate(date) {
  return dateKeyFromDate(date).replace(/-/g, "");
}

function getTaipeiCalendarDate(daysAgo = 0) {
  const parts = taipeiPartsFromUtcMs(
    Date.now() - daysAgo * 24 * 60 * 60 * 1000
  );
  return new Date(Date.UTC(parts.year, parts.month - 1, parts.day));
}

function parseDailyQuoteVolumeToLots(rawVolume) {
  const shares = parseMarketNumber(rawVolume);
  if (typeof shares !== "number") return null;
  return shares / 1000;
}

function findTableByFields(tables, fieldPatterns) {
  return tables.find((table) => {
    const fields = Array.isArray(table?.fields) ? table.fields.map(String) : [];
    return fieldPatterns.every((pattern) =>
      fields.some((field) => field.includes(pattern))
    );
  });
}

function parseTwseDailyQuotes(json, dateKey) {
  const tables = Array.isArray(json?.tables) ? json.tables : [];
  const table = findTableByFields(tables, [
    "證券代號",
    "證券名稱",
    "成交股數",
    "收盤價",
  ]);
  if (!table) return {};

  const fields = table.fields.map(String);
  const rows = Array.isArray(table.data) ? table.data : [];
  const idx = {
    code: fields.findIndex((f) => f.includes("證券代號")),
    name: fields.findIndex((f) => f.includes("證券名稱")),
    volume: fields.findIndex((f) => f.includes("成交股數")),
    value: fields.findIndex((f) => f.includes("成交金額")),
    open: fields.findIndex((f) => f.includes("開盤價")),
    high: fields.findIndex((f) => f.includes("最高價")),
    low: fields.findIndex((f) => f.includes("最低價")),
    close: fields.findIndex((f) => f.includes("收盤價")),
  };
  const quotes = {};

  for (const row of rows) {
    if (!Array.isArray(row)) continue;
    const code = normalizeStockCode(row[idx.code]);
    if (!code) continue;

    quotes[code] = {
      code,
      name: String(row[idx.name] || "").trim(),
      market: "TWSE",
      date: dateKey,
      open: parseMarketNumber(row[idx.open]),
      high: parseMarketNumber(row[idx.high]),
      low: parseMarketNumber(row[idx.low]),
      close: parseMarketNumber(row[idx.close]),
      volumeLots: parseDailyQuoteVolumeToLots(row[idx.volume]),
      tradeValue: parseMarketNumber(row[idx.value]),
    };
  }

  return quotes;
}

function parseTpexDailyQuotes(json, dateKey) {
  const tables = Array.isArray(json?.tables) ? json.tables : [];
  const table = findTableByFields(tables, ["代號", "名稱", "收盤", "成交股數"]);
  if (!table) return {};

  const fields = table.fields.map(String);
  const rows = Array.isArray(table.data) ? table.data : [];
  const idx = {
    code: fields.findIndex((f) => f.includes("代號")),
    name: fields.findIndex((f) => f.includes("名稱")),
    volume: fields.findIndex((f) => f.includes("成交股數")),
    value: fields.findIndex((f) => f.includes("成交金額")),
    open: fields.findIndex((f) => f.includes("開盤")),
    high: fields.findIndex((f) => f.includes("最高")),
    low: fields.findIndex((f) => f.includes("最低")),
    close: fields.findIndex((f) => f.includes("收盤")),
  };
  const quotes = {};

  for (const row of rows) {
    if (!Array.isArray(row)) continue;
    const code = normalizeStockCode(row[idx.code]);
    if (!code) continue;

    quotes[code] = {
      code,
      name: String(row[idx.name] || "").trim(),
      market: "TPEX",
      date: dateKey,
      open: parseMarketNumber(row[idx.open]),
      high: parseMarketNumber(row[idx.high]),
      low: parseMarketNumber(row[idx.low]),
      close: parseMarketNumber(row[idx.close]),
      volumeLots: parseDailyQuoteVolumeToLots(row[idx.volume]),
      tradeValue: parseMarketNumber(row[idx.value]),
    };
  }

  return quotes;
}

async function fetchTwseDailyQuotesByDate(date) {
  const dateKey = dateKeyFromDate(date);
  const cacheKey = getDailyQuotesCacheKey("TWSE", dateKey);
  const cached = await redisGet(cacheKey);
  if (cached) {
    try {
      return JSON.parse(cached);
    } catch {
      // ignore broken cache and refetch
    }
  }

  const url = `https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date=${formatTwseQueryDate(
    date
  )}&type=ALLBUT0999`;
  const res = await fetch(url, {
    headers: {
      "user-agent": "Mozilla/5.0",
      accept: "application/json,text/plain,*/*",
    },
  });
  if (!res.ok) return {};

  const json = await res.json();
  if (String(json?.stat || "").toUpperCase() !== "OK") return {};

  const quotes = parseTwseDailyQuotes(json, dateKey);
  if (Object.keys(quotes).length) {
    await redisSet(cacheKey, JSON.stringify(quotes), "EX", 60 * 60 * 24 * 7);
  }
  return quotes;
}

async function fetchTpexDailyQuotesByDate(date) {
  const dateKey = dateKeyFromDate(date);
  const cacheKey = getDailyQuotesCacheKey("TPEX", dateKey);
  const cached = await redisGet(cacheKey);
  if (cached) {
    try {
      return JSON.parse(cached);
    } catch {
      // ignore broken cache and refetch
    }
  }

  const url = `https://www.tpex.org.tw/www/zh-tw/afterTrading/otc?date=${encodeURIComponent(
    formatTpexQueryDate(date)
  )}&type=EW&response=json`;
  const res = await fetch(url, {
    headers: {
      "user-agent": "Mozilla/5.0",
      accept: "application/json,text/plain,*/*",
    },
  });
  if (!res.ok) return {};

  const json = await res.json();
  if (String(json?.stat || "").toLowerCase() !== "ok") return {};

  const quotes = parseTpexDailyQuotes(json, dateKey);
  if (Object.keys(quotes).length) {
    await redisSet(cacheKey, JSON.stringify(quotes), "EX", 60 * 60 * 24 * 7);
  }
  return quotes;
}

async function fetchMergedDailyQuotesByDate(date) {
  const dateKey = dateKeyFromDate(date);
  const [twseQuotes, tpexQuotes] = await Promise.all([
    fetchTwseDailyQuotesByDate(date),
    fetchTpexDailyQuotesByDate(date),
  ]);
  const quotes = {
    ...twseQuotes,
    ...tpexQuotes,
  };

  return {
    dateKey,
    quotes,
    count: Object.keys(quotes).length,
  };
}

async function getRecentTradingDaySnapshots(requiredTradingDays = 21) {
  const snapshots = [];
  const maxLookback =
    Number.isFinite(POSTMARKET_MAX_CALENDAR_LOOKBACK) &&
    POSTMARKET_MAX_CALENDAR_LOOKBACK > requiredTradingDays
      ? POSTMARKET_MAX_CALENDAR_LOOKBACK
      : 40;

  for (let offset = 0; offset < maxLookback; offset++) {
    const date = getTaipeiCalendarDate(offset);

    const snapshot = await fetchMergedDailyQuotesByDate(date);
    if (!snapshot.count) continue;

    snapshots.push(snapshot);
    if (snapshots.length >= requiredTradingDays) break;
  }

  return snapshots;
}

function calcAverage(values) {
  const valid = values.filter((v) => typeof v === "number" && Number.isFinite(v));
  if (!valid.length) return null;
  return valid.reduce((sum, value) => sum + value, 0) / valid.length;
}

function calcMovingAverage(values, period) {
  if (!Array.isArray(values) || values.length < period) return null;
  return calcAverage(values.slice(values.length - period));
}

function calcPercentChange(current, previous) {
  if (
    typeof current !== "number" ||
    typeof previous !== "number" ||
    !Number.isFinite(current) ||
    !Number.isFinite(previous) ||
    previous <= 0
  ) {
    return null;
  }
  return ((current - previous) / previous) * 100;
}

function isEtfStock(stock) {
  const code = normalizeStockCode(stock?.code);
  const name = String(stock?.name || "");
  if (!code) return true;
  if (!/^\d{4}$/.test(code)) return true;
  if (/^(?:00|02|91)/.test(code)) return true;
  if (/-DR|甲特|乙特|丙特|特別股/.test(name)) return true;

  const etfKeywords = [
    "ETF",
    "ETN",
    "槓桿",
    "反向",
    "正2",
    "反1",
    "高股息",
    "台灣50",
    "MSCI",
    "ESG",
    "永續",
    "債",
  ];
  return etfKeywords.some((keyword) => name.includes(keyword));
}

function scorePostMarketCandidate(metrics) {
  const score5d = Math.min(25, Math.max(0, metrics.change5d * 2.5));
  const score20d = Math.min(20, Math.max(0, metrics.change20d));
  const scoreVolume = Math.min(20, Math.max(0, (metrics.volumeRatio - 1.1) * 40));
  const scoreLiquidity =
    metrics.tradeValue >= 1e8 ? 15 : metrics.avgVol20 >= 2000 ? 10 : 0;

  let scoreBias = 0;
  if (metrics.bias20 >= 0.01 && metrics.bias20 <= 0.08) scoreBias += 10;
  if (metrics.bias5 >= 0 && metrics.bias5 <= 0.03) scoreBias += 10;

  return {
    score5d,
    score20d,
    scoreVolume,
    scoreLiquidity,
    scoreBias,
    totalScore: Number(
      (score5d + score20d + scoreVolume + scoreLiquidity + scoreBias).toFixed(2)
    ),
  };
}

function countConsecutiveUpDays(closes) {
  if (!Array.isArray(closes) || closes.length < 2) return 0;

  let streak = 0;
  for (let i = closes.length - 1; i > 0; i--) {
    if (
      typeof closes[i] !== "number" ||
      typeof closes[i - 1] !== "number" ||
      !Number.isFinite(closes[i]) ||
      !Number.isFinite(closes[i - 1])
    ) {
      break;
    }
    if (closes[i] > closes[i - 1]) streak += 1;
    else break;
  }
  return streak;
}

function buildPostMarketRiskLevel(metrics) {
  if (metrics.isOverheated || metrics.isWeakLiquidity) {
    return "高追價風險";
  }

  if (
    metrics.change20d <= 20 &&
    metrics.bias5 <= 0.03 &&
    metrics.bias20 <= 0.08 &&
    metrics.consecutiveUpDays <= 3 &&
    metrics.liquidityPass
  ) {
    return "型態剛轉強";
  }

  return "相對低追價風險";
}

function buildPostMarketTags(metrics) {
  const tags = [];

  if (metrics.change20d <= 12 && metrics.change5d >= 2 && metrics.bias20 <= 0.08) {
    tags.push("剛轉強");
  } else {
    tags.push("續強");
  }

  if (metrics.isDispositionStock) {
    tags.push("處置股");
  } else if (metrics.isAttentionStock) {
    tags.push("注意股");
  }

  if (metrics.isFundamentallyWeak) {
    tags.push("基本面偏弱");
  }

  if (metrics.isValuationExpensive) {
    tags.push("估值偏高");
  }

  if (metrics.liquidityPass && metrics.volumeRatio >= 1.2) {
    tags.push("量能健康");
  }

  if (metrics.bias5 <= 0.03 && metrics.bias20 <= 0.08) {
    tags.push("接近均線");
  }

  if (metrics.bias5 > 0.04 || metrics.bias20 > 0.08) {
    tags.push("追價風險");
  }

  if (metrics.isOverheated) {
    tags.push("過熱");
  }

  if (metrics.isWeakLiquidity) {
    tags.push("流動性偏弱");
  }

  return [...new Set(tags)].slice(0, 3);
}

function buildPostMarketLiquidityComment(metrics) {
  if (metrics.tradeValue >= 5e8) {
    return "成交額大、流動性佳";
  }
  if (metrics.tradeValue >= 2e8) {
    return "量能充足、進出相對順";
  }
  if (metrics.tradeValue >= 1e8) {
    return "成交額達標，流動性還算夠";
  }
  return "量能雖放大，但流動性仍偏一般";
}

function buildPostMarketStructureComment(metrics) {
  if (metrics.isOverheated) {
    return "已明顯偏離短中期均線";
  }
  if (metrics.change20d <= 12 && metrics.bias20 <= 0.08) {
    return "剛站回中期均線，偏向轉強初段";
  }
  if (metrics.bias5 <= 0.03 && metrics.bias20 <= 0.08) {
    return "沿均線上行，型態仍算整齊";
  }
  return "趨勢延續，但已經漲了一段";
}

function buildPostMarketStockStyleComment(metrics) {
  if (metrics.close < 20 && metrics.tradeValue >= 2e8) {
    return "低價高量，容易吸引短線資金";
  }
  if (metrics.tradeValue >= 5e8) {
    return "較偏主流量能股，走勢通常相對穩一些";
  }
  if (metrics.tradeValue >= 1e8) {
    return "中型股彈性仍在，但波動會比大型股明顯";
  }
  return "股性偏活，進出節奏要抓得更緊";
}

function buildPostMarketRedFlagLabels(pick) {
  const flags = [];
  if (pick?.isDispositionStock) flags.push("處置股");
  if (pick?.isAttentionStock) flags.push("注意股");
  if (pick?.isFundamentallyWeak) flags.push("基本面偏弱");
  if (pick?.isValuationExpensive) flags.push("估值偏高");
  if (pick?.isWeakLiquidity) flags.push("流動性偏弱");
  if (pick?.isOverheated) flags.push("過熱");
  return [...new Set(flags)];
}

function formatLastUpdatedAtZh(isoString) {
  if (!isoString) return null;
  const date = new Date(isoString);
  if (Number.isNaN(date.getTime())) return null;
  return new Intl.DateTimeFormat("zh-TW", {
    timeZone: "Asia/Taipei",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(date);
}

function buildPostMarketPickComment(metrics) {
  if (metrics.isDispositionStock) {
    return "列為處置股，交易限制較多，僅適合高風險觀察。";
  }
  if (metrics.isAttentionStock && metrics.isFundamentallyWeak) {
    return "已列注意股，且近四季 EPS 偏弱，不列入主推薦。";
  }
  if (metrics.isAttentionStock && metrics.isValuationExpensive) {
    return "已列注意股，且估值偏高，不列入主推薦。";
  }
  if (metrics.isAttentionStock) {
    return `${buildPostMarketStructureComment(
      metrics
    )}，但已列注意股，交易風險較高。`;
  }
  if (metrics.isFundamentallyWeak && metrics.isValuationExpensive) {
    return "近四季 EPS 偏弱，且估值偏高，不列入主推薦。";
  }
  if (metrics.isFundamentallyWeak && metrics.isOverheated) {
    return "近四季 EPS 偏弱，且短線已過熱，不列入主推薦。";
  }
  if (metrics.isFundamentallyWeak) {
    return `${buildPostMarketStructureComment(
      metrics
    )}，但近四季 EPS 偏弱，先列高風險觀察。`;
  }
  if (metrics.isValuationExpensive && metrics.isOverheated) {
    return "估值偏高，且短線已過熱，不列入主推薦。";
  }
  if (metrics.isValuationExpensive) {
    return `${buildPostMarketStructureComment(
      metrics
    )}，但估值偏高，較不適合列主推薦。`;
  }
  if (metrics.isOverheated && metrics.isWeakLiquidity) {
    return "短線已過熱，且流動性不足，不列入主推薦";
  }
  if (metrics.isOverheated) {
    return `${buildPostMarketStructureComment(metrics)}，追價壓力偏高，先列觀察。`;
  }
  if (metrics.isWeakLiquidity) {
    return `${buildPostMarketStructureComment(metrics)}，但流動性偏弱，進出風險較高。`;
  }
  if (metrics.riskLevel === "型態剛轉強") {
    if (metrics.tradeValue >= 5e8) {
      return `${buildPostMarketLiquidityComment(
        metrics
      )}，型態剛轉強，適合當觀察首選。`;
    }
    if (metrics.tradeValue >= 3e8) {
      return `${buildPostMarketStructureComment(metrics)}，${buildPostMarketLiquidityComment(
        metrics
      )}，可優先列入觀察。`;
    }
    if (metrics.tradeValue >= 1e8 && metrics.close >= 30) {
      return `${buildPostMarketStockStyleComment(
        metrics
      )}，${buildPostMarketStructureComment(metrics)}，但波動會比大型股明顯。`;
    }
    if (metrics.close < 25 && metrics.tradeValue >= 1e8) {
      return `${buildPostMarketStockStyleComment(
        metrics
      )}，${buildPostMarketStructureComment(metrics)}，偏短線資金型態。`;
    }
    if (metrics.tradeValue < 1e8) {
      return `均線結構轉強，但${buildPostMarketLiquidityComment(
        metrics
      )}，進出要更保守。`;
    }
    return `${buildPostMarketStructureComment(metrics)}，${buildPostMarketLiquidityComment(
      metrics
    )}，較適合優先觀察。`;
  }
  return `${buildPostMarketLiquidityComment(metrics)}，${buildPostMarketStockStyleComment(
    metrics
  )}。`;
}

function normalizePostMarketPick(pick) {
  if (!pick || typeof pick !== "object") return pick;

  const bias5 =
    typeof pick.bias5 === "number" && Number.isFinite(pick.bias5)
      ? pick.bias5
      : typeof pick.close === "number" &&
        typeof pick.ma5 === "number" &&
        Number.isFinite(pick.close) &&
        Number.isFinite(pick.ma5) &&
        pick.ma5 > 0
      ? (pick.close - pick.ma5) / pick.ma5
      : null;
  const bias20 =
    typeof pick.bias20 === "number" && Number.isFinite(pick.bias20)
      ? pick.bias20
      : typeof pick.close === "number" &&
        typeof pick.ma20 === "number" &&
        Number.isFinite(pick.close) &&
        Number.isFinite(pick.ma20) &&
        pick.ma20 > 0
      ? (pick.close - pick.ma20) / pick.ma20
      : null;
  const tradeValue =
    typeof pick.tradeValue === "number" && Number.isFinite(pick.tradeValue)
      ? pick.tradeValue
      : 0;
  const avgVol20 =
    typeof pick.avgVol20 === "number" && Number.isFinite(pick.avgVol20)
      ? pick.avgVol20
      : 0;
  const epsLatestQuarter =
    typeof pick.epsLatestQuarter === "number" &&
    Number.isFinite(pick.epsLatestQuarter)
      ? pick.epsLatestQuarter
      : null;
  const peRatio =
    typeof pick.peRatio === "number" && Number.isFinite(pick.peRatio)
      ? pick.peRatio
      : null;
  const isAttentionStock =
    typeof pick.isAttentionStock === "boolean" ? pick.isAttentionStock : false;
  const isDispositionStock =
    typeof pick.isDispositionStock === "boolean"
      ? pick.isDispositionStock
      : false;
  const epsTtm =
    typeof pick.epsTtm === "number" && Number.isFinite(pick.epsTtm)
      ? pick.epsTtm
      : null;
  const consecutiveUpDays =
    typeof pick.consecutiveUpDays === "number" &&
    Number.isFinite(pick.consecutiveUpDays)
      ? pick.consecutiveUpDays
      : 0;
  const liquidityPass =
    typeof pick.liquidityPass === "boolean"
      ? pick.liquidityPass
      : avgVol20 >= 2000 || tradeValue >= 1e8;
  const isWeakLiquidity =
    typeof pick.isWeakLiquidity === "boolean"
      ? pick.isWeakLiquidity
      : avgVol20 < 2000 && tradeValue < 1e8;
  const isOverheated =
    typeof pick.isOverheated === "boolean"
      ? pick.isOverheated
      : (typeof pick.change20d === "number" && pick.change20d > 35) ||
        consecutiveUpDays >= 6 ||
        (typeof bias5 === "number" && bias5 > 0.06) ||
        (typeof bias20 === "number" && bias20 > 0.12);
  const isFundamentallyWeak =
    typeof pick.isFundamentallyWeak === "boolean"
      ? pick.isFundamentallyWeak
      : typeof epsTtm === "number" && epsTtm <= 0;
  const isValuationExpensive =
    typeof pick.isValuationExpensive === "boolean"
      ? pick.isValuationExpensive
      : typeof peRatio === "number" &&
        peRatio > POSTMARKET_PE_RED_FLAG_THRESHOLD &&
        tradeValue < POSTMARKET_PE_LOW_LIQUIDITY_VALUE;
  const normalizedLegacyRiskLevel =
    pick.riskLevel === "低風險"
      ? "型態剛轉強"
      : pick.riskLevel === "中風險"
      ? "相對低追價風險"
      : pick.riskLevel === "高風險"
      ? "高追價風險"
      : pick.riskLevel;
  const riskLevel =
    normalizedLegacyRiskLevel ||
    buildPostMarketRiskLevel({
      ...pick,
      bias5,
      bias20,
      tradeValue,
      avgVol20,
      consecutiveUpDays,
      liquidityPass,
      isWeakLiquidity,
      isOverheated,
    });
  const tags = Array.isArray(pick.tags)
    ? pick.tags
    : buildPostMarketTags({
        ...pick,
        bias5,
        bias20,
        tradeValue,
        avgVol20,
        peRatio,
        isAttentionStock,
        isDispositionStock,
        consecutiveUpDays,
        liquidityPass,
        isWeakLiquidity,
        isOverheated,
        isFundamentallyWeak,
        isValuationExpensive,
      });

  return {
    ...pick,
    industry: normalizeIndustryName(pick.industry),
    peRatio,
    isAttentionStock,
    isDispositionStock,
    epsLatestQuarter,
    epsYear: Number.isFinite(pick.epsYear) ? Number(pick.epsYear) : null,
    epsQuarter: Number.isFinite(pick.epsQuarter) ? Number(pick.epsQuarter) : null,
    epsTtm,
    epsTtmYear:
      Number.isFinite(pick.epsTtmYear) ? Number(pick.epsTtmYear) : null,
    epsTtmQuarter:
      Number.isFinite(pick.epsTtmQuarter) ? Number(pick.epsTtmQuarter) : null,
    bias5,
    bias20,
    tradeValue,
    avgVol20,
    consecutiveUpDays,
    liquidityPass,
    isWeakLiquidity,
    isOverheated,
    isFundamentallyWeak,
    isValuationExpensive,
    riskLevel,
    tags,
    comment:
      pick.comment ||
      buildPostMarketPickComment({
        ...pick,
        bias5,
        bias20,
        tradeValue,
        avgVol20,
        peRatio,
        isAttentionStock,
        isDispositionStock,
        consecutiveUpDays,
        liquidityPass,
        isWeakLiquidity,
        isOverheated,
        isFundamentallyWeak,
        isValuationExpensive,
        riskLevel,
      }),
  };
}

function fmtTradeValueYi(n) {
  if (typeof n !== "number" || !Number.isFinite(n) || n <= 0) return "--";
  const value = n / 1e8;
  return `${value >= 10 ? value.toFixed(1) : value.toFixed(2)}億`;
}

async function getRecentPostMarketStreakCodes(requiredDays = 3) {
  if (!Number.isFinite(requiredDays) || requiredDays < 2) {
    return new Set();
  }

  const snapshots = await getRecentTradingDaySnapshots(requiredDays);
  if (snapshots.length < requiredDays) {
    return new Set();
  }

  const dailySets = [];

  for (const snapshot of snapshots.slice(0, requiredDays)) {
    const cached = await redisGet(getPostMarketPicksCacheKey(snapshot.dateKey));
    if (!cached) {
      return new Set();
    }

    try {
      const parsed = JSON.parse(cached);
      const picks = [
        ...(Array.isArray(parsed?.recommendedPicks) ? parsed.recommendedPicks : []),
        ...(Array.isArray(parsed?.highRiskPicks) ? parsed.highRiskPicks : []),
        ...(Array.isArray(parsed?.picks) ? parsed.picks : []),
      ].map(normalizePostMarketPick);
      if (!picks.length) {
        return new Set();
      }

      dailySets.push(
        new Set(
          picks
            .map((pick) => pick?.code)
            .filter((code) => typeof code === "string" && code)
        )
      );
    } catch {
      return new Set();
    }
  }

  if (!dailySets.length) {
    return new Set();
  }

  const [firstSet, ...restSets] = dailySets;
  return new Set(
    [...firstSet].filter((code) => restSets.every((set) => set.has(code)))
  );
}

async function decoratePostMarketPicksPayload(payload) {
  if (!payload || typeof payload !== "object") {
    return payload;
  }

  const streakCodes = await getRecentPostMarketStreakCodes(3);
  let stockIndustryByCode = new Map();
  let meta = null;
  try {
    const rawStocks = await redisGet(STOCKS_REDIS_KEY);
    const stocks = rawStocks ? JSON.parse(rawStocks) : {};
    stockIndustryByCode = new Map(
      Object.values(stocks)
        .filter((stock) => stock?.code)
        .map((stock) => [
          stock.code,
          normalizeIndustryName(stock?.industry) || null,
        ])
    );
  } catch {
    stockIndustryByCode = new Map();
  }
  try {
    const rawMeta = await redisGet(STOCKS_META_REDIS_KEY);
    meta = rawMeta ? JSON.parse(rawMeta) : null;
  } catch {
    meta = null;
  }
  const decorateList = (list) =>
    (Array.isArray(list) ? list : []).map((pick) => {
      const normalized = normalizePostMarketPick(pick);
      const industry =
        normalized?.industry ||
        stockIndustryByCode.get(normalized?.code) ||
        null;
      return {
        ...normalized,
        industry,
        streak3d: streakCodes.has(normalized?.code),
      };
    });

  return {
    ...payload,
    lastUpdatedAt: payload?.lastUpdatedAt || meta?.lastUpdatedAt || null,
    recommendedPicks: decorateList(
      payload.recommendedPicks || payload.picks || []
    ),
    highRiskPicks: decorateList(payload.highRiskPicks || []),
  };
}

function formatPostMarketPicksText(payload) {
  const dateKey = payload?.dateKey || "--";
  const lastUpdatedLabel = formatLastUpdatedAtZh(payload?.lastUpdatedAt);
  const recommendedPicks = Array.isArray(payload?.recommendedPicks)
    ? payload.recommendedPicks.map(normalizePostMarketPick)
    : [];
  const highRiskPicks = Array.isArray(payload?.highRiskPicks)
    ? payload.highRiskPicks.map(normalizePostMarketPick)
    : [];

  const lines = [
    `📌 今日盤後推薦股（${dateKey}）`,
    "依趨勢、量能、流動性與風險過濾",
    ...(lastUpdatedLabel ? [`資料更新：${lastUpdatedLabel}`] : []),
    "",
    "【主推薦】",
  ];

  if (!recommendedPicks.length) {
    lines.push("目前沒有符合主推薦條件的標的。");
  } else {
    recommendedPicks.forEach((pick, index) => {
      lines.push("");
      lines.push(
        `${index + 1}. ${pick.code} ${pick.name}${
          pick.streak3d ? "【3日中選】" : ""
        }`
      );
      lines.push(`判讀：${pick.riskLevel}`);
      lines.push(`產業：${formatStockIndustryName(pick.industry)}`);
      lines.push(`標籤：${(pick.tags || []).join("、") || "—"}`);
      lines.push(
        `收盤 ${fmtTWPrice(pick.close)}｜5日 ${pick.change5d.toFixed(
          1
        )}%｜20日 ${pick.change20d.toFixed(1)}%`
      );
      lines.push(
        `量比 ${pick.volumeRatio.toFixed(2)}｜20日均量 ${Math.round(
          pick.avgVol20
        ).toLocaleString("zh-TW")}張｜成交額 ${fmtTradeValueYi(pick.tradeValue)}${
          typeof pick.peRatio === "number" && Number.isFinite(pick.peRatio)
            ? `｜本益比 ${pick.peRatio.toFixed(2)}`
            : ""
        }`
      );
      lines.push(`短評：${pick.comment}`);
    });
  }

  lines.push("");
  lines.push("【高風險強勢股】");

  if (!highRiskPicks.length) {
    lines.push("目前沒有符合高風險強勢條件的標的。");
  } else {
    highRiskPicks.forEach((pick, index) => {
      lines.push("");
      lines.push(
        `${index + 1}. ${pick.code} ${pick.name}${
          pick.streak3d ? "【3日中選】" : ""
        }`
      );
      lines.push(`判讀：${pick.riskLevel}`);
      lines.push(`產業：${formatStockIndustryName(pick.industry)}`);
      const redFlags = buildPostMarketRedFlagLabels(pick);
      const dedupedTags = (pick.tags || []).filter(
        (tag) => !redFlags.includes(tag)
      );
      if (redFlags.length) {
        lines.push(`紅燈：${redFlags.join("、")}`);
      }
      lines.push(`標籤：${dedupedTags.join("、") || "—"}`);
      lines.push(
        `收盤 ${fmtTWPrice(pick.close)}｜5日 ${pick.change5d.toFixed(
          1
        )}%｜20日 ${pick.change20d.toFixed(1)}%`
      );
      lines.push(
        `量比 ${pick.volumeRatio.toFixed(2)}｜20日均量 ${Math.round(
          pick.avgVol20
        ).toLocaleString("zh-TW")}張｜成交額 ${fmtTradeValueYi(pick.tradeValue)}${
          typeof pick.peRatio === "number" && Number.isFinite(pick.peRatio)
            ? `｜本益比 ${pick.peRatio.toFixed(2)}`
            : ""
        }`
      );
      lines.push(`短評：${pick.comment}`);
    });
  }

  lines.push("");
  lines.push(
    "條件：技術面轉強，並排除過熱、近四季EPS偏弱、估值偏高與注意/處置股。"
  );
  return lines.join("\n");
}

function buildSingleStockRiskLabel(metrics) {
  if (
    metrics.isDispositionStock ||
    metrics.isAttentionStock ||
    metrics.isFundamentallyWeak ||
    metrics.isValuationExpensive
  ) {
    return "高風險觀察";
  }
  return buildPostMarketRiskLevel(metrics);
}

async function buildSingleStockInsight(stock) {
  if (!stock?.code) return null;

  const snapshots = await getRecentTradingDaySnapshots(21);
  if (snapshots.length < 21) return null;

  const newestToOldest = snapshots;
  const series = newestToOldest
    .map((snapshot) => snapshot.quotes[stock.code] || null)
    .filter(
      (quote) =>
        quote &&
        typeof quote.close === "number" &&
        typeof quote.volumeLots === "number"
    )
    .reverse();

  if (series.length < 21) return null;

  const closes = series.map((quote) => quote.close);
  const volumes = series.map((quote) => quote.volumeLots);
  const latestQuote = series[series.length - 1];
  const close = closes[closes.length - 1];
  const ma5 = calcMovingAverage(closes, 5);
  const ma20 = calcMovingAverage(closes, 20);
  const change5d = calcPercentChange(close, closes[closes.length - 6]);
  const change20d = calcPercentChange(close, closes[closes.length - 21]);
  const avgVol5 = calcAverage(volumes.slice(volumes.length - 5));
  const avgVol20 = calcAverage(volumes.slice(volumes.length - 20));
  const tradeValue =
    typeof latestQuote?.tradeValue === "number" && Number.isFinite(latestQuote.tradeValue)
      ? latestQuote.tradeValue
      : 0;
  const peRatio =
    typeof stock?.peRatio === "number" && Number.isFinite(stock.peRatio)
      ? stock.peRatio
      : null;
  const epsTtm =
    typeof stock?.epsTtm === "number" && Number.isFinite(stock.epsTtm)
      ? stock.epsTtm
      : null;
  const isAttentionStock =
    typeof stock?.isAttentionStock === "boolean" ? stock.isAttentionStock : false;
  const isDispositionStock =
    typeof stock?.isDispositionStock === "boolean" ? stock.isDispositionStock : false;

  if (
    [close, ma5, ma20, change5d, change20d, avgVol5, avgVol20].some(
      (value) => typeof value !== "number" || !Number.isFinite(value)
    )
  ) {
    return null;
  }

  const volumeRatio = avgVol20 > 0 ? avgVol5 / avgVol20 : 0;
  const bias5 = ma5 > 0 ? (close - ma5) / ma5 : -1;
  const bias20 = ma20 > 0 ? (close - ma20) / ma20 : -1;
  const consecutiveUpDays = countConsecutiveUpDays(closes);
  const liquidityPass = avgVol20 >= 2000 || tradeValue >= 1e8;
  const isWeakLiquidity = avgVol20 < 2000 && tradeValue < 1e8;
  const isOverheated =
    change20d > 35 ||
    consecutiveUpDays >= 6 ||
    bias5 > 0.06 ||
    bias20 > 0.12;
  const isFundamentallyWeak =
    typeof epsTtm === "number" && Number.isFinite(epsTtm) && epsTtm <= 0;
  const isValuationExpensive =
    typeof peRatio === "number" &&
    Number.isFinite(peRatio) &&
    peRatio > POSTMARKET_PE_RED_FLAG_THRESHOLD &&
    tradeValue < POSTMARKET_PE_LOW_LIQUIDITY_VALUE;

  const metrics = {
    close,
    tradeValue,
    change5d,
    change20d,
    bias5,
    bias20,
    volumeRatio,
    avgVol20,
    consecutiveUpDays,
    liquidityPass,
    isWeakLiquidity,
    isOverheated,
    isAttentionStock,
    isDispositionStock,
    isFundamentallyWeak,
    isValuationExpensive,
    peRatio,
    riskLevel: null,
  };

  const riskLevel = buildSingleStockRiskLabel(metrics);
  return {
    riskLevel,
    industry: normalizeIndustryName(stock?.industry),
    tags: buildPostMarketTags({ ...metrics, riskLevel }),
    volumeRatio,
    comment: buildPostMarketPickComment({ ...metrics, riskLevel }),
    ma5,
    ma20,
    latestClose: close,
  };
}

function adjustSingleStockInsightForIntraday(insight, quote, source) {
  if (!insight || !quote || !isTaiwanStockMarketOpen()) return insight;
  if (!source?.includes("realtime") && source !== "yahoo-delayed") return insight;
  if (typeof quote.price !== "number" || !Number.isFinite(quote.price)) return insight;

  const currentPrice = quote.price;
  const ma5 = typeof insight.ma5 === "number" ? insight.ma5 : null;
  const ma20 = typeof insight.ma20 === "number" ? insight.ma20 : null;
  const changePercent =
    typeof quote.changePercent === "number" && Number.isFinite(quote.changePercent)
      ? quote.changePercent
      : null;

  const brokeMa5 = typeof ma5 === "number" && currentPrice < ma5;
  const brokeMa20 = typeof ma20 === "number" && currentPrice < ma20;
  const isSharpIntradayDrop = typeof changePercent === "number" && changePercent <= -6;

  if (!brokeMa5 && !brokeMa20 && !isSharpIntradayDrop) return insight;

  const nextTags = new Set(
    (Array.isArray(insight.tags) ? insight.tags : []).filter(
      (tag) => !["接近均線", "剛轉強"].includes(tag)
    )
  );
  nextTags.add("盤中走弱");
  if (isSharpIntradayDrop) nextTags.add("短線賣壓");

  const riskLevel =
    insight.riskLevel === "高風險觀察" ? insight.riskLevel : "盤中走弱";
  let comment = "盤中走勢轉弱，短線賣壓偏重，先不要把它當成型態整齊。";
  if (brokeMa5 && brokeMa20) {
    comment = "盤中已跌破短中期均線，走勢轉弱，短線賣壓偏重。";
  } else if (brokeMa5) {
    comment = "盤中已跌破短期均線，走勢轉弱，先以保守觀察為主。";
  } else if (isSharpIntradayDrop) {
    comment = "盤中跌幅擴大，短線賣壓偏重，暫不宜把它解讀成沿均線整理。";
  }

  return {
    ...insight,
    riskLevel,
    tags: Array.from(nextTags),
    comment,
  };
}

async function computePostMarketPicks() {
  const raw = await redisGet(STOCKS_REDIS_KEY);
  const stocks = raw ? JSON.parse(raw) : {};
  const snapshots = await getRecentTradingDaySnapshots(
    Number.isFinite(POSTMARKET_REQUIRED_TRADING_DAYS) &&
      POSTMARKET_REQUIRED_TRADING_DAYS > 20
      ? POSTMARKET_REQUIRED_TRADING_DAYS
      : 21
  );

  if (snapshots.length < 21) {
    throw new Error("not enough trading day snapshots");
  }

  const newestToOldest = snapshots;
  const latestDateKey = newestToOldest[0].dateKey;
  const orderedSnapshots = [...newestToOldest].reverse();
  const recommendedCandidates = [];
  const highRiskCandidates = [];

  for (const stock of Object.values(stocks)) {
    if (!stock || isEtfStock(stock)) continue;

    const series = orderedSnapshots
      .map((snapshot) => snapshot.quotes[stock.code] || null)
      .filter(
        (quote) =>
          quote &&
          typeof quote.close === "number" &&
          typeof quote.volumeLots === "number"
      );

    if (series.length < 21) continue;

    const closes = series.map((quote) => quote.close);
    const volumes = series.map((quote) => quote.volumeLots);
    const latestQuote = series[series.length - 1];
    const close = closes[closes.length - 1];
    const ma5 = calcMovingAverage(closes, 5);
    const ma20 = calcMovingAverage(closes, 20);
    const change5d = calcPercentChange(close, closes[closes.length - 6]);
    const change20d = calcPercentChange(close, closes[closes.length - 21]);
    const avgVol5 = calcAverage(volumes.slice(volumes.length - 5));
    const avgVol20 = calcAverage(volumes.slice(volumes.length - 20));
    const tradeValue =
      typeof latestQuote?.tradeValue === "number" &&
      Number.isFinite(latestQuote.tradeValue)
        ? latestQuote.tradeValue
        : 0;
    const epsLatestQuarter =
      typeof stock?.epsLatestQuarter === "number" &&
      Number.isFinite(stock.epsLatestQuarter)
        ? stock.epsLatestQuarter
        : null;
    const peRatio =
      typeof stock?.peRatio === "number" && Number.isFinite(stock.peRatio)
        ? stock.peRatio
        : null;
    const isAttentionStock =
      typeof stock?.isAttentionStock === "boolean" ? stock.isAttentionStock : false;
    const isDispositionStock =
      typeof stock?.isDispositionStock === "boolean"
        ? stock.isDispositionStock
        : false;
    const epsTtm =
      typeof stock?.epsTtm === "number" && Number.isFinite(stock.epsTtm)
        ? stock.epsTtm
        : null;

    if (
      [close, ma5, ma20, change5d, change20d, avgVol5, avgVol20].some(
        (value) => typeof value !== "number" || !Number.isFinite(value)
      )
    ) {
      continue;
    }

    const volumeRatio = avgVol20 > 0 ? avgVol5 / avgVol20 : 0;
    const bias5 = ma5 > 0 ? (close - ma5) / ma5 : -1;
    const bias20 = ma20 > 0 ? (close - ma20) / ma20 : -1;
    const consecutiveUpDays = countConsecutiveUpDays(closes);
    const liquidityPass = avgVol20 >= 2000 || tradeValue >= 1e8;
    const isWeakLiquidity = avgVol20 < 2000 && tradeValue < 1e8;
    const isOverheated =
      change20d > 35 ||
      consecutiveUpDays >= 6 ||
      bias5 > 0.06 ||
      bias20 > 0.12;
    const isFundamentallyWeak =
      typeof epsTtm === "number" && Number.isFinite(epsTtm) && epsTtm <= 0;
    const isValuationExpensive =
      typeof peRatio === "number" &&
      Number.isFinite(peRatio) &&
      peRatio > POSTMARKET_PE_RED_FLAG_THRESHOLD &&
      tradeValue < POSTMARKET_PE_LOW_LIQUIDITY_VALUE;

    if (close < 5 || close > 40) continue;
    if (!(ma5 > ma20)) continue;
    if (!(close > ma20)) continue;
    if (!(change5d > 0)) continue;
    if (!(change20d > 0)) continue;
    if (!(avgVol5 >= avgVol20 * 1.1)) continue;

    const score = scorePostMarketCandidate({
      change5d,
      change20d,
      volumeRatio,
      avgVol20,
      tradeValue,
      bias5,
      bias20,
    });
    const riskLevel = buildPostMarketRiskLevel({
      change20d,
      bias5,
      bias20,
      consecutiveUpDays,
      liquidityPass,
      isWeakLiquidity,
      isOverheated,
    });
    const metrics = {
      change5d,
      change20d,
      volumeRatio,
      avgVol20,
      tradeValue,
      peRatio,
      isAttentionStock,
      isDispositionStock,
      bias5,
      bias20,
      consecutiveUpDays,
      liquidityPass,
      isWeakLiquidity,
      isOverheated,
      isFundamentallyWeak,
      isValuationExpensive,
      riskLevel,
    };
    const candidate = {
      code: stock.code,
      name: stock.name,
      market: stock.market,
      industry: stock.industry || null,
      peRatio,
      isAttentionStock,
      isDispositionStock,
      epsLatestQuarter,
      epsYear:
        Number.isFinite(stock?.epsYear) && stock.epsYear > 0
          ? Number(stock.epsYear)
          : null,
      epsQuarter:
        Number.isFinite(stock?.epsQuarter) && stock.epsQuarter > 0
          ? Number(stock.epsQuarter)
          : null,
      epsTtm,
      epsTtmYear:
        Number.isFinite(stock?.epsTtmYear) && stock.epsTtmYear > 0
          ? Number(stock.epsTtmYear)
          : null,
      epsTtmQuarter:
        Number.isFinite(stock?.epsTtmQuarter) && stock.epsTtmQuarter > 0
          ? Number(stock.epsTtmQuarter)
          : null,
      close,
      ma5,
      ma20,
      change5d,
      change20d,
      avgVol5,
      avgVol20,
      tradeValue,
      volumeRatio,
      bias5,
      bias20,
      consecutiveUpDays,
      liquidityPass,
      isWeakLiquidity,
      isOverheated,
      isFundamentallyWeak,
      isValuationExpensive,
      riskLevel,
      tags: buildPostMarketTags(metrics),
      comment: buildPostMarketPickComment(metrics),
      ...score,
    };

    if (
      liquidityPass &&
      !isOverheated &&
      !isFundamentallyWeak &&
      !isValuationExpensive &&
      !isAttentionStock &&
      !isDispositionStock
    ) {
      recommendedCandidates.push(candidate);
    } else {
      highRiskCandidates.push({
        ...candidate,
        riskLevel: "高追價風險",
      });
    }
  }

  recommendedCandidates.sort((a, b) => {
    const riskRank = { "型態剛轉強": 0, "相對低追價風險": 1 };
    const aRisk = riskRank[a.riskLevel] ?? 9;
    const bRisk = riskRank[b.riskLevel] ?? 9;
    if (aRisk !== bRisk) return aRisk - bRisk;
    if (b.totalScore !== a.totalScore) return b.totalScore - a.totalScore;
    if (b.volumeRatio !== a.volumeRatio) return b.volumeRatio - a.volumeRatio;
    return Math.abs(a.bias20 - 0.05) - Math.abs(b.bias20 - 0.05);
  });

  highRiskCandidates.sort((a, b) => {
    if (b.totalScore !== a.totalScore) return b.totalScore - a.totalScore;
    if (b.change20d !== a.change20d) return b.change20d - a.change20d;
    if (b.consecutiveUpDays !== a.consecutiveUpDays) {
      return b.consecutiveUpDays - a.consecutiveUpDays;
    }
    if (b.volumeRatio !== a.volumeRatio) return b.volumeRatio - a.volumeRatio;
    return (b.tradeValue || 0) - (a.tradeValue || 0);
  });

  const diversifiedRecommendedCandidates = applyIndustryDiversityLimit(
    recommendedCandidates
  );
  const recommendedPicks = diversifiedRecommendedCandidates.slice(
    0,
    Number.isFinite(POSTMARKET_REPLY_LIMIT) && POSTMARKET_REPLY_LIMIT > 0
      ? POSTMARKET_REPLY_LIMIT
      : 5
  );
  const highRiskPicks = highRiskCandidates.slice(
    0,
    Number.isFinite(POSTMARKET_HIGHRISK_REPLY_LIMIT) &&
      POSTMARKET_HIGHRISK_REPLY_LIMIT > 0
      ? POSTMARKET_HIGHRISK_REPLY_LIMIT
      : 3
  );
  const payload = {
    dateKey: latestDateKey,
    generatedAt: new Date().toISOString(),
    totalCandidates: recommendedCandidates.length + highRiskCandidates.length,
    recommendedPicks,
    highRiskPicks,
  };

  await redisSet(
    getPostMarketPicksCacheKey(latestDateKey),
    JSON.stringify(payload),
    "EX",
    Number.isFinite(POSTMARKET_SCAN_TTL_SECONDS) && POSTMARKET_SCAN_TTL_SECONDS > 0
      ? POSTMARKET_SCAN_TTL_SECONDS
      : 60 * 60 * 20
  );

  return payload;
}

async function getOrBuildPostMarketPicks() {
  const snapshots = await getRecentTradingDaySnapshots(1);
  if (!snapshots.length) {
    throw new Error("latest trading date unavailable");
  }

  const latestDateKey = snapshots[0].dateKey;
  const cached = await redisGet(getPostMarketPicksCacheKey(latestDateKey));
  if (cached) {
    try {
      const parsed = JSON.parse(cached);
      const normalizedPayload = {
        ...parsed,
        recommendedPicks: Array.isArray(parsed?.recommendedPicks)
          ? parsed.recommendedPicks.map(normalizePostMarketPick)
          : Array.isArray(parsed?.picks)
          ? parsed.picks.map(normalizePostMarketPick)
          : [],
        highRiskPicks: Array.isArray(parsed?.highRiskPicks)
          ? parsed.highRiskPicks.map(normalizePostMarketPick)
          : [],
      };
      const allPicks = [
        ...normalizedPayload.recommendedPicks,
        ...normalizedPayload.highRiskPicks,
      ];
      const hasMissingIndustry = allPicks.some(
        (pick) => !String(pick?.industry || "").trim()
      );
      if (!hasMissingIndustry) {
        return decoratePostMarketPicksPayload(normalizedPayload);
      }
    } catch {
      // ignore and rebuild
    }
  }

  return decoratePostMarketPicksPayload(await computePostMarketPicks());
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
        high: realtime.high,
        low: realtime.low,
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

  const officialDaily = await getLatestOfficialDailyQuote(stock);
  if (officialDaily && !isTaiwanStockMarketOpen()) {
    return officialDaily;
  }

  for (const symbol of getStockCandidateSymbols(stock)) {
    const quote = await getStockQuote(symbol);
    if (quote) {
      const officialPrevClose =
        typeof officialDaily?.quote?.price === "number" &&
        Number.isFinite(officialDaily.quote.price) &&
        officialDaily.quote.price > 0
          ? officialDaily.quote.price
          : null;
      if (officialPrevClose != null) {
        quote.prevClose = officialPrevClose;
        const changeRaw = quote.price - officialPrevClose;
        quote.change = Math.abs(changeRaw) < 0.005 ? 0 : changeRaw;
        const changePercentRaw = (quote.change / officialPrevClose) * 100;
        quote.changePercent =
          Math.abs(changePercentRaw) < 0.005 ? 0 : changePercentRaw;
      }
      return { quote, symbol, source: "yahoo-delayed" };
    }
  }

  return officialDaily;
}

async function getLatestOfficialDailyQuote(stock) {
  const code = normalizeStockCode(stock?.code);
  if (!code) return null;

  const snapshots = await getRecentTradingDaySnapshots(2);
  const latestSnapshot = snapshots[0];
  const previousSnapshot = snapshots[1];
  const latestQuote = latestSnapshot?.quotes?.[code] || null;
  if (!latestQuote || typeof latestQuote.close !== "number") return null;

  const prevClose =
    typeof previousSnapshot?.quotes?.[code]?.close === "number"
      ? previousSnapshot.quotes[code].close
      : null;
  const changeRaw =
    typeof prevClose === "number" ? latestQuote.close - prevClose : null;
  const change =
    typeof changeRaw === "number" && Math.abs(changeRaw) >= 0.005 ? changeRaw : 0;
  const changePercentRaw =
    typeof prevClose === "number" && prevClose > 0
      ? (change / prevClose) * 100
      : null;
  const changePercent =
    typeof changePercentRaw === "number" && Math.abs(changePercentRaw) >= 0.005
      ? changePercentRaw
      : 0;

  return {
    quote: {
      price: latestQuote.close,
      open: latestQuote.open,
      high: latestQuote.high,
      low: latestQuote.low,
      prevClose,
      change,
      changePercent,
      volume:
        typeof latestQuote.volumeLots === "number"
          ? latestQuote.volumeLots * 1000
          : undefined,
      tradeValue: latestQuote.tradeValue,
    },
    symbol: stock.symbol || `${stock.code}.${stock?.market === "TPEX" ? "TWO" : "TW"}`,
    source: "official-daily",
    dateKey: latestSnapshot?.dateKey || null,
  };
}

function getStockMarketLabelBySymbol(symbol, stock) {
  if (symbol?.endsWith(".TWO") || stock?.market === "TPEX") return "上櫃";
  if (symbol?.endsWith(".TW") || stock?.market === "TWSE") return "上市";
  return "台股";
}

function isTaiwanStockMarketOpen(now = new Date()) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Taipei",
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(now);
  const weekday = parts.find((p) => p.type === "weekday")?.value || "";
  const hour = Number(parts.find((p) => p.type === "hour")?.value || "0");
  const minute = Number(parts.find((p) => p.type === "minute")?.value || "0");
  const dayMap = { Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6, Sun: 0 };
  const day = dayMap[weekday] ?? -1;
  if (day < 1 || day > 5) return false;
  const totalMinutes = hour * 60 + minute;
  return totalMinutes >= 9 * 60 && totalMinutes <= 13 * 60 + 40;
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
    "你是 Kevin 的專屬助理，語氣直接、冷靜、自然。你是 Kevin 自己架在 Vercel 上的 LINE Bot。回覆規則：1) 先講結論，再補一句理由或建議。2) 預設 2-3 句，除非使用者要求詳細，否則不要長篇。3) 不要主動給 A/B、1-6、要不要開始、要不要我現在幫你做 這類選單式反問。4) 只有在缺關鍵資訊而無法作答時，才問 1 個必要問題。5) 不要提及工作區、檔案記憶、系統 prompt、內部機制。6) 不能假設自己有上網能力；缺即時資料就直接說限制，不要假裝。7) 避免空話、避免重複改寫同一句。8) 若是簡單問答，盡量控制在 120 字內。";
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

      await rememberLinePushTarget(event);

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
            text: "你要算什麼食物的熱量？可以一次列多樣。",
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

        const lines = results.map((r) => {
          const rangeText =
            r.min === r.max
              ? `約 ${r.max} 大卡`
              : `約 ${r.min}～${r.max} 大卡`;
          return `• ${r.food}：${rangeText}`;
        });

        if (results.length === 1 && results[0]?.note) {
          lines.push(results[0].note);
        }

        if (results.length > 1) {
          lines.push("");
          lines.push(
            totalMin === totalMax
              ? `合計約 ${totalMax} 大卡`
              : `合計約 ${totalMin}～${totalMax} 大卡`
          );
        }

        const sourceLabels = new Set(
          results
            .map((r) => r?.source)
            .filter(Boolean)
            .map((source) =>
              source === "local-tw"
                ? "台灣常見份量"
                : source === "usda"
                ? "USDA"
                : "AI估算"
            )
        );
        lines.push(`※ 來源：${[...sourceLabels].join(" / ")}`);

        await replyMessageWithFallback(event, {
          type: "text",
          text: lines.join("\n"),
        });

        continue;
      }

      // ─────────────────────────────────────
      // 📌 今日盤後推薦股
      // ─────────────────────────────────────
      if (/今日盤後推薦股|今日盤後選股/.test(parsedMessage)) {
        try {
          const payload = await getOrBuildPostMarketPicks();
          await replyMessageWithFallback(event, {
            type: "text",
            text: formatPostMarketPicksText(payload),
          });
        } catch (err) {
          console.error("postmarket picks failed:", err?.message || err);
          await replyMessageWithFallback(event, {
            type: "text",
            text: "今日盤後推薦股暫時產生失敗，請晚點再試。",
          });
        }
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
          const rawInsight = await buildSingleStockInsight(stock);
          const insight = adjustSingleStockInsightForIntraday(
            rawInsight,
            q,
            result.source
          );

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
          const isRealtimeQuote = result.source?.includes("realtime");
          const isOfficialDailyQuote = result.source === "official-daily";
          const isDelayedQuote = result.source === "yahoo-delayed";
          const priceLabel = isRealtimeQuote
            ? "現價"
            : isOfficialDailyQuote
              ? "收盤"
              : isTaiwanStockMarketOpen()
                ? "最新價（延遲）"
                : "收盤";
          const quoteSourceNote = isRealtimeQuote
            ? "※ 行情來源：TWSE/TPEX 盤中即時快照"
            : isOfficialDailyQuote
              ? "※ 行情來源：TWSE/TPEX 官方日資料"
              : "※ 行情來源：Yahoo Finance（延遲報價）";
          const analysisSourceNote = insight
            ? "※ 分析來源：TWSE/TPEX 日資料快取"
            : null;
          const marketLines = [
            "【行情】",
            `${priceLabel}：${fmtTWPrice(q.price)}`,
            `漲跌：${sign}${fmtTWPrice(q.change)}（${percentSign}${percent}%）`,
            `昨收：${fmtTWPrice(q.prevClose)}`,
          ];

          if (isRealtimeQuote || isOfficialDailyQuote) {
            marketLines.splice(3, 0, `開盤：${fmtTWPrice(q.open)}`);
            if (typeof q.high === "number") {
              marketLines.splice(4, 0, `最高：${fmtTWPrice(q.high)}`);
            }
            if (typeof q.low === "number") {
              const lowInsertIndex = marketLines.findIndex((line) =>
                line.startsWith("昨收：")
              );
              marketLines.splice(lowInsertIndex, 0, `最低：${fmtTWPrice(q.low)}`);
            }
            marketLines.push(`成交量：${volumeLots} 張`);
          } else if (isDelayedQuote) {
            marketLines.push("註：盤中延遲報價不顯示開盤與成交量");
          }

          const analysisLines = insight
            ? [
                "【分析】",
                `判讀：${insight.riskLevel}`,
                `產業：${formatStockIndustryName(insight.industry)}`,
                `標籤：${(insight.tags || []).join("、") || "—"}`,
                `量比：${
                  typeof insight.volumeRatio === "number" &&
                  Number.isFinite(insight.volumeRatio)
                    ? insight.volumeRatio.toFixed(2)
                    : "--"
                }`,
                `短評：${insight.comment}`,
              ]
            : [];

          const text = `📊 ${stock.name}（${stock.code}｜${marketLabel}）

${marketLines.join("\n")}

${analysisLines.join("\n")}${analysisLines.length ? "\n\n" : ""}${quoteSourceNote}${
            analysisSourceNote ? `\n${analysisSourceNote}` : ""
          }`;

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
            resolvedLocation: last.resolvedLocation,
          });

          await replyWeather(event, result);
          continue;
        }
      }

      // ─────────────────────────────────────
      // 3️⃣ v2 weather parser（穿搭 / 帶傘 / 時段）
      // ─────────────────────────────────────
      const last = await getLastWeatherContext(userId);
      const directWeatherRequest = parseWeatherRequest(parsedMessage, {
        contextResolvedLocation: last?.resolvedLocation,
      });

      if (directWeatherRequest) {
        const resolvedLocationRaw = directWeatherRequest.locationText || last?.city;

        if (!resolvedLocationRaw) {
          await replyMessageWithFallback(event, {
            type: "text",
            text: "你要查哪裡的天氣？直接給我城市就行，例如「桃園今天要帶傘嗎」。",
          });
          continue;
        }

        const cityClean = cleanCity(resolvedLocationRaw);
        const island = findTaiwanIsland(cityClean);
        const city = island ? island.name : fixTaiwanCity(cityClean);
        const result = await getWeatherAndOutfit({
          city,
          timeIntent: directWeatherRequest.timeIntent,
          queryType: directWeatherRequest.queryType,
          rawText: parsedMessage,
          resolvedLocation: directWeatherRequest.resolvedLocation || last?.resolvedLocation,
          lat: directWeatherRequest.locationText ? island?.lat : last?.lat || island?.lat,
          lon: directWeatherRequest.locationText ? island?.lon : last?.lon || island?.lon,
        });

        await setLastWeatherContext(userId, {
          city,
          lat: directWeatherRequest.locationText ? island?.lat : last?.lat || island?.lat,
          lon: directWeatherRequest.locationText ? island?.lon : last?.lon || island?.lon,
          resolvedLocation:
            directWeatherRequest.resolvedLocation || last?.resolvedLocation || null,
        });

        await replyWeather(event, result);
        continue;
      }

      // ─────────────────────────────────────
      // 4️⃣ quickWeatherParse（不用 GPT）
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
      // 5️⃣ GPT WEATHER intent
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

  const mode =
    String(req.query?.mode || "fast").trim().toLowerCase() === "full"
      ? "full"
      : "fast";
  const isFullUpdate = mode === "full";
  const debug = [];
  let stocks = {};
  let cachedStocks = {};
  let cachedEnrichmentCount = 0;
  let source = "";
  let twseCount = 0;
  let tpexCount = 0;
  let twseIndustryCount = 0;
  let tpexIndustryCount = 0;
  let twseEpsCount = 0;
  let tpexEpsCount = 0;
  let twseEpsTtmCount = 0;
  let tpexEpsTtmCount = 0;
  let twsePeCount = 0;
  let tpexPeCount = 0;
  let twseAttentionCount = 0;
  let twseDispositionCount = 0;
  let tpexAttentionCount = 0;
  let tpexDispositionCount = 0;

  if (!isFullUpdate) {
    try {
      const raw = await redisGet(STOCKS_REDIS_KEY);
      cachedStocks = raw ? JSON.parse(raw) : {};
    } catch (err) {
      console.warn("Stock cache load failed for fast update:", err?.message || err);
      cachedStocks = {};
    }
  }

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

  if (isFullUpdate) {
  try {
    const result = await fetchStockIndustryCsv(
      TWSE_BASIC_INFO_CSV_URL,
      "TWSE",
      "twse-basic-info"
    );
    twseIndustryCount = Object.keys(result.stocks).length;
    debug.push({
      source: "twse-basic-info",
      status: 200,
      contentType: result.contentType,
      count: twseIndustryCount,
      head120: result.head120,
    });
    Object.entries(result.stocks).forEach(([code, info]) => {
      if (stocks[code]) {
        stocks[code].industry = info.industry || stocks[code].industry || null;
      }
    });
    source = source ? `${source}+twse-basic-info` : "twse-basic-info";
  } catch (err) {
    console.warn("TWSE basic info update failed:", err?.message || err);
    debug.push({
      source: "twse-basic-info",
      error: String(err?.message || err),
    });
  }

  try {
    const result = await fetchStockIndustryCsv(
      TPEX_BASIC_INFO_CSV_URL,
      "TPEX",
      "tpex-basic-info"
    );
    tpexIndustryCount = Object.keys(result.stocks).length;
    debug.push({
      source: "tpex-basic-info",
      status: 200,
      contentType: result.contentType,
      count: tpexIndustryCount,
      head120: result.head120,
    });
    Object.entries(result.stocks).forEach(([code, info]) => {
      if (stocks[code]) {
        stocks[code].industry = info.industry || stocks[code].industry || null;
      }
    });
    source = source ? `${source}+tpex-basic-info` : "tpex-basic-info";
  } catch (err) {
    console.warn("TPEX basic info update failed:", err?.message || err);
    debug.push({
      source: "tpex-basic-info",
      error: String(err?.message || err),
    });
  }

  try {
    const result = await fetchStockEpsCsv(TWSE_EPS_CSV_URL, "TWSE", "twse-eps");
    twseEpsCount = Object.keys(result.stocks).length;
    debug.push({
      source: "twse-eps",
      status: 200,
      contentType: result.contentType,
      count: twseEpsCount,
      head120: result.head120,
    });
    Object.entries(result.stocks).forEach(([code, info]) => {
      if (stocks[code]) {
        stocks[code].epsLatestQuarter =
          typeof info.epsLatestQuarter === "number" &&
          Number.isFinite(info.epsLatestQuarter)
            ? info.epsLatestQuarter
            : stocks[code].epsLatestQuarter ?? null;
        stocks[code].epsYear =
          Number.isFinite(info.epsYear) && info.epsYear > 0
            ? Number(info.epsYear)
            : stocks[code].epsYear ?? null;
        stocks[code].epsQuarter =
          Number.isFinite(info.epsQuarter) && info.epsQuarter > 0
            ? Number(info.epsQuarter)
            : stocks[code].epsQuarter ?? null;
      }
    });
    if (
      Number.isFinite(result.latestYear) &&
      Number.isFinite(result.latestQuarter) &&
      result.latestQuarter > 0
    ) {
      const trailing = buildTrailingFourQuarterEpsFromSeed(
        "TWSE",
        result.stocks,
        result.latestYear,
        result.latestQuarter
      );
      twseEpsTtmCount = Object.keys(trailing).length;
      debug.push({
        source: `eps-seed-twse-${result.latestYear}Q${result.latestQuarter}`,
        count: twseEpsTtmCount,
      });
      Object.entries(trailing).forEach(([code, info]) => {
        if (stocks[code]) {
          stocks[code].epsTtm =
            typeof info.epsTtm === "number" && Number.isFinite(info.epsTtm)
              ? info.epsTtm
              : stocks[code].epsTtm ?? null;
          stocks[code].epsTtmYear =
            Number.isFinite(info.epsTtmYear) && info.epsTtmYear > 0
              ? Number(info.epsTtmYear)
              : stocks[code].epsTtmYear ?? null;
          stocks[code].epsTtmQuarter =
            Number.isFinite(info.epsTtmQuarter) && info.epsTtmQuarter > 0
              ? Number(info.epsTtmQuarter)
              : stocks[code].epsTtmQuarter ?? null;
        }
      });
    }
    source = source ? `${source}+twse-eps` : "twse-eps";
  } catch (err) {
    console.warn("TWSE EPS update failed:", err?.message || err);
    debug.push({
      source: "twse-eps",
      error: String(err?.message || err),
    });
  }

  try {
    const result = await fetchStockEpsCsv(TPEX_EPS_CSV_URL, "TPEX", "tpex-eps");
    tpexEpsCount = Object.keys(result.stocks).length;
    debug.push({
      source: "tpex-eps",
      status: 200,
      contentType: result.contentType,
      count: tpexEpsCount,
      head120: result.head120,
    });
    Object.entries(result.stocks).forEach(([code, info]) => {
      if (stocks[code]) {
        stocks[code].epsLatestQuarter =
          typeof info.epsLatestQuarter === "number" &&
          Number.isFinite(info.epsLatestQuarter)
            ? info.epsLatestQuarter
            : stocks[code].epsLatestQuarter ?? null;
        stocks[code].epsYear =
          Number.isFinite(info.epsYear) && info.epsYear > 0
            ? Number(info.epsYear)
            : stocks[code].epsYear ?? null;
        stocks[code].epsQuarter =
          Number.isFinite(info.epsQuarter) && info.epsQuarter > 0
            ? Number(info.epsQuarter)
            : stocks[code].epsQuarter ?? null;
      }
    });
    if (
      Number.isFinite(result.latestYear) &&
      Number.isFinite(result.latestQuarter) &&
      result.latestQuarter > 0
    ) {
      const trailing = buildTrailingFourQuarterEpsFromSeed(
        "TPEX",
        result.stocks,
        result.latestYear,
        result.latestQuarter
      );
      tpexEpsTtmCount = Object.keys(trailing).length;
      debug.push({
        source: `eps-seed-tpex-${result.latestYear}Q${result.latestQuarter}`,
        count: tpexEpsTtmCount,
      });
      Object.entries(trailing).forEach(([code, info]) => {
        if (stocks[code]) {
          stocks[code].epsTtm =
            typeof info.epsTtm === "number" && Number.isFinite(info.epsTtm)
              ? info.epsTtm
              : stocks[code].epsTtm ?? null;
          stocks[code].epsTtmYear =
            Number.isFinite(info.epsTtmYear) && info.epsTtmYear > 0
              ? Number(info.epsTtmYear)
              : stocks[code].epsTtmYear ?? null;
          stocks[code].epsTtmQuarter =
            Number.isFinite(info.epsTtmQuarter) && info.epsTtmQuarter > 0
              ? Number(info.epsTtmQuarter)
              : stocks[code].epsTtmQuarter ?? null;
        }
      });
    }
    source = source ? `${source}+tpex-eps` : "tpex-eps";
  } catch (err) {
    console.warn("TPEX EPS update failed:", err?.message || err);
    debug.push({
      source: "tpex-eps",
      error: String(err?.message || err),
    });
  }

  try {
    const result = await fetchTwsePeCsv();
    twsePeCount = Object.keys(result.stocks).length;
    debug.push({
      source: "twse-pe",
      status: 200,
      contentType: result.contentType,
      count: twsePeCount,
      head120: result.head120,
    });
    Object.entries(result.stocks).forEach(([code, info]) => {
      if (stocks[code]) {
        stocks[code].peRatio =
          typeof info.peRatio === "number" && Number.isFinite(info.peRatio)
            ? info.peRatio
            : stocks[code].peRatio ?? null;
      }
    });
    source = source ? `${source}+twse-pe` : "twse-pe";
  } catch (err) {
    console.warn("TWSE PE update failed:", err?.message || err);
    debug.push({
      source: "twse-pe",
      error: String(err?.message || err),
    });
  }

  try {
    const result = await fetchTpexPeJson();
    tpexPeCount = Object.keys(result.stocks).length;
    debug.push({
      source: "tpex-pe",
      status: 200,
      contentType: result.contentType,
      count: tpexPeCount,
      head120: result.head120,
    });
    Object.entries(result.stocks).forEach(([code, info]) => {
      if (stocks[code]) {
        stocks[code].peRatio =
          typeof info.peRatio === "number" && Number.isFinite(info.peRatio)
            ? info.peRatio
            : stocks[code].peRatio ?? null;
      }
    });
    source = source ? `${source}+tpex-pe` : "tpex-pe";
  } catch (err) {
    console.warn("TPEX PE update failed:", err?.message || err);
    debug.push({
      source: "tpex-pe",
      error: String(err?.message || err),
    });
  }
  } else {
    cachedEnrichmentCount = mergeStockEnrichmentFromCache(stocks, cachedStocks);
    debug.push({
      source: "cached-enrichment",
      mode,
      count: cachedEnrichmentCount,
      skipped: [
        "twse-basic-info",
        "tpex-basic-info",
        "twse-eps",
        "tpex-eps",
        "twse-pe",
        "tpex-pe",
      ],
    });
    source = source ? `${source}+cached-enrichment` : "cached-enrichment";
  }

  try {
    const result = await fetchTwseAttentionHtml();
    twseAttentionCount = Object.keys(result.stocks).length;
    debug.push({
      source: "twse-attention",
      status: 200,
      contentType: result.contentType,
      count: twseAttentionCount,
      head120: result.head120,
    });
    Object.entries(result.stocks).forEach(([code, info]) => {
      if (stocks[code]) {
        stocks[code].isAttentionStock =
          typeof info.isAttentionStock === "boolean"
            ? info.isAttentionStock
            : stocks[code].isAttentionStock ?? false;
      }
    });
    source = source ? `${source}+twse-attention` : "twse-attention";
  } catch (err) {
    console.warn("TWSE attention update failed:", err?.message || err);
    debug.push({
      source: "twse-attention",
      error: String(err?.message || err),
    });
  }

  try {
    const result = await fetchTwseDispositionHtml();
    twseDispositionCount = Object.keys(result.stocks).length;
    debug.push({
      source: "twse-disposition",
      status: 200,
      contentType: result.contentType,
      count: twseDispositionCount,
      head120: result.head120,
    });
    Object.entries(result.stocks).forEach(([code, info]) => {
      if (stocks[code]) {
        stocks[code].isDispositionStock =
          typeof info.isDispositionStock === "boolean"
            ? info.isDispositionStock
            : stocks[code].isDispositionStock ?? false;
      }
    });
    source = source ? `${source}+twse-disposition` : "twse-disposition";
  } catch (err) {
    console.warn("TWSE disposition update failed:", err?.message || err);
    debug.push({
      source: "twse-disposition",
      error: String(err?.message || err),
    });
  }

  try {
    const result = await fetchTpexAttentionJson();
    tpexAttentionCount = Object.keys(result.stocks).length;
    debug.push({
      source: "tpex-attention",
      status: 200,
      contentType: result.contentType,
      count: tpexAttentionCount,
      head120: result.head120,
    });
    Object.entries(result.stocks).forEach(([code, info]) => {
      if (stocks[code]) {
        stocks[code].isAttentionStock =
          typeof info.isAttentionStock === "boolean"
            ? info.isAttentionStock
            : stocks[code].isAttentionStock ?? false;
      }
    });
    source = source ? `${source}+tpex-attention` : "tpex-attention";
  } catch (err) {
    console.warn("TPEX attention update failed:", err?.message || err);
    debug.push({
      source: "tpex-attention",
      error: String(err?.message || err),
    });
  }

  try {
    const result = await fetchTpexDispositionJson();
    tpexDispositionCount = Object.keys(result.stocks).length;
    debug.push({
      source: "tpex-disposition",
      status: 200,
      contentType: result.contentType,
      count: tpexDispositionCount,
      head120: result.head120,
    });
    Object.entries(result.stocks).forEach(([code, info]) => {
      if (stocks[code]) {
        stocks[code].isDispositionStock =
          typeof info.isDispositionStock === "boolean"
            ? info.isDispositionStock
            : stocks[code].isDispositionStock ?? false;
      }
    });
    source = source ? `${source}+tpex-disposition` : "tpex-disposition";
  } catch (err) {
    console.warn("TPEX disposition update failed:", err?.message || err);
    debug.push({
      source: "tpex-disposition",
      error: String(err?.message || err),
    });
  }

  const count = Object.keys(stocks).length;
  const lastUpdatedAt = new Date().toISOString();
  const sampleParsed = Object.values(stocks).slice(0, 5);
  const industryCount = Object.values(stocks).filter((item) => item?.industry).length;
  const epsCount = Object.values(stocks).filter((item) =>
    typeof item?.epsLatestQuarter === "number" &&
    Number.isFinite(item.epsLatestQuarter)
  ).length;
  const epsTtmCount = Object.values(stocks).filter(
    (item) => typeof item?.epsTtm === "number" && Number.isFinite(item.epsTtm)
  ).length;
  const peCount = Object.values(stocks).filter(
    (item) => typeof item?.peRatio === "number" && Number.isFinite(item.peRatio)
  ).length;
  const attentionCount = Object.values(stocks).filter(
    (item) => item?.isAttentionStock === true
  ).length;
  const dispositionCount = Object.values(stocks).filter(
    (item) => item?.isDispositionStock === true
  ).length;

  if (twseCount < MIN_TWSE_STOCK_COUNT) {
    return res.status(502).json({
      ok: false,
      error: "TWSE source parsed too few records; keep existing Redis cache",
      count,
      twseCount,
      tpexCount,
      twseIndustryCount,
      tpexIndustryCount,
      twseEpsCount,
      tpexEpsCount,
      twseEpsTtmCount,
      tpexEpsTtmCount,
      twsePeCount,
      tpexPeCount,
      twseAttentionCount,
      twseDispositionCount,
      tpexAttentionCount,
      tpexDispositionCount,
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
      industryCount,
      epsCount,
      epsTtmCount,
      peCount,
      attentionCount,
      dispositionCount,
      sampleParsed,
      debug,
    });
  }

  const metaPayload = {
    lastUpdatedAt,
    source,
    count,
    mode,
  };
  const metaSaved = await redisSet(
    STOCKS_META_REDIS_KEY,
    JSON.stringify(metaPayload)
  );
  if (!metaSaved) {
    console.warn("Stock meta update failed: Redis unavailable");
  }

  return res.json({
    ok: true,
    mode,
    lastUpdatedAt,
    source,
    count,
    twseCount,
    tpexCount,
    twseIndustryCount,
    tpexIndustryCount,
    twseEpsCount,
    tpexEpsCount,
    twseEpsTtmCount,
    tpexEpsTtmCount,
    twsePeCount,
    tpexPeCount,
    twseAttentionCount,
    twseDispositionCount,
    tpexAttentionCount,
    tpexDispositionCount,
    industryCount,
    epsCount,
    epsTtmCount,
    peCount,
    attentionCount,
    dispositionCount,
    cachedEnrichmentCount,
    metaSaved,
    debug: {
      sampleParsed,
      sources: debug,
    },
  });
});

app.get("/api/check-earthquake", async (req, res) => {
  if (!isCronAuthorized(req)) {
    return res.status(401).json({ error: "unauthorized" });
  }

  const force = String(req.query?.force || "").trim() === "1";
  const notifyFirst = String(req.query?.notifyFirst || "").trim() === "1";

  try {
    const rawEvents = await fetchCwaSignificantEarthquakes();
    const latest = pickLatestEarthquake(rawEvents);
    if (!latest) {
      return res.json({
        ok: true,
        pushed: false,
        reason: "no-earthquake-report",
        count: rawEvents.length,
      });
    }

    const lastPushedId = await redisGet(CWA_EARTHQUAKE_LAST_PUSHED_KEY);
    const isFirstRun = !lastPushedId;
    if (!force && lastPushedId && lastPushedId === latest.id) {
      return res.json({
        ok: true,
        pushed: false,
        reason: "duplicate",
        eventId: latest.id,
        earthquake: latest,
      });
    }

    if (!force && isFirstRun && !notifyFirst) {
      await redisSet(CWA_EARTHQUAKE_LAST_PUSHED_KEY, latest.id);
      return res.json({
        ok: true,
        pushed: false,
        initialized: true,
        reason: "baseline-created",
        eventId: latest.id,
        earthquake: latest,
      });
    }

    const text = formatEarthquakePushText(latest, { isTest: force });
    const pushStats = await pushLineTextToKnownTargets(text);
    await redisSet(CWA_EARTHQUAKE_LAST_PUSHED_KEY, latest.id);

    return res.json({
      ok: true,
      pushed: pushStats.sent > 0,
      eventId: latest.id,
      earthquake: latest,
      ...pushStats,
    });
  } catch (err) {
    console.error("Earthquake check failed:", err);
    return res.status(500).json({
      ok: false,
      error: String(err?.message || err),
    });
  }
});

app.get("/api/health", async (req, res) => {
  let redisReady = false;
  try {
    redisReady = await ensureRedisReady();
  } catch {
    redisReady = false;
  }

  const stockSummary = await loadStockCacheDebugSummary();

  return res.json({
    ok: true,
    now: new Date().toISOString(),
    timezone: "Asia/Taipei",
    uptimeSeconds: Math.round(process.uptime()),
    services: {
      redis: {
        configured: Boolean(REDIS_URL),
        ready: redisReady,
      },
      line: {
        tokenConfigured: Boolean(process.env.LINE_TOKEN),
        secretConfigured: Boolean(process.env.LINE_SECRET),
      },
      openai: {
        configured: Boolean(process.env.OPENAI_API_KEY),
      },
      openclaw: {
        configured: Boolean(OPENCLAW_CHAT_URL),
        routeEnabled: OPENCLAW_ROUTE_ENABLED,
      },
      weather: {
        cwaConfigured: Boolean(CWA_API_KEY),
        openWeatherConfigured: Boolean(process.env.WEATHER_API_KEY),
      },
    },
    stocks: {
      lastUpdatedAt: stockSummary?.meta?.lastUpdatedAt || null,
      source: stockSummary?.meta?.source || null,
      mode: stockSummary?.meta?.mode || null,
      count: stockSummary?.meta?.count || stockSummary?.cacheCount || 0,
    },
  });
});

app.get("/api/debug/stocks-meta", async (req, res) => {
  if (!isCronAuthorized(req)) {
    return res.status(401).json({ error: "unauthorized" });
  }

  const summary = await loadStockCacheDebugSummary();
  return res.json({
    ok: true,
    ...summary,
  });
});

app.get("/api/debug/weather-source", async (req, res) => {
  if (!isCronAuthorized(req)) {
    return res.status(401).json({ error: "unauthorized" });
  }

  return res.json({
    ok: true,
    providers: {
      taiwanCurrent: Boolean(CWA_API_KEY) ? "CWA O-A0003-001" : null,
      taiwanForecast: Boolean(CWA_API_KEY) ? "CWA F-C0032-001" : null,
      globalFallback: process.env.WEATHER_API_KEY ? "OpenWeather" : null,
    },
    configured: {
      cwaApiKey: Boolean(CWA_API_KEY),
      openWeatherApiKey: Boolean(process.env.WEATHER_API_KEY),
      weatherContextTtlSeconds: WEATHER_CONTEXT_TTL_SECONDS,
    },
    taiwanCoverage: {
      countyCount: TW_ADMIN_DIVISION_COUNTIES.length,
      districtCount: TW_DISTRICT_LIST.length,
      districtAliasCount: Object.keys(TW_DISTRICT_ALIAS_MAP).length,
      ambiguousAliasCount: AMBIGUOUS_DISTRICT_ALIASES.size,
      ambiguousPrioritySample: Object.entries(AMBIGUOUS_DISTRICT_PRIORITY).slice(0, 6),
    },
    islands: Object.keys(TAIWAN_ISLANDS),
  });
});

app.get("/api/debug/horoscope-source", async (req, res) => {
  if (!isCronAuthorized(req)) {
    return res.status(401).json({ error: "unauthorized" });
  }

  const cache = await loadHoroscopeCacheDebugSummary();
  return res.json({
    ok: true,
    chain: ["aztro", "freehoroscopeapi", "openai-fallback"],
    cache,
  });
});

app.get("/api/debug/line-push-targets", async (req, res) => {
  if (!isCronAuthorized(req)) {
    return res.status(401).json({ error: "unauthorized" });
  }

  const targets = await getKnownLinePushTargets();
  return res.json({
    ok: true,
    count: targets.length,
    targets: targets.slice(0, 20).map((target) => ({
      targetType: target.targetType,
      targetIdMasked: maskIdentifier(target.targetId),
    })),
  });
});

app.get("/api/weather-locations", (req, res) => {
  const counties = TW_ADMIN_DIVISION_COUNTIES.map((county) => ({
    displayName: county.name,
    subdivisions: Array.isArray(county.subdivisions) ? county.subdivisions : [],
    aliases: buildCountyAliases(county.name),
  })).sort((a, b) => a.displayName.localeCompare(b.displayName, "zh-Hant"));
  const districts = TW_DISTRICT_LIST.map((district) => ({
    displayName: district.displayName,
    countyName: district.countyName,
    aliases: district.aliases,
  }));

  return res.json({
    ok: true,
    countyCount: counties.length,
    districtCount: districts.length,
    districtAliasCount: Object.keys(TW_DISTRICT_ALIAS_MAP).length,
    ambiguousAliases: [...AMBIGUOUS_DISTRICT_ALIASES].sort((a, b) =>
      a.localeCompare(b, "zh-Hant")
    ),
    counties,
    districts,
  });
});

// Default route
app.get("/", (req, res) => res.send("Kevin LINE GPT Bot Running"));

export default app;
