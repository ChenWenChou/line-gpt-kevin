import express from "express";
import line from "@line/bot-sdk";
import OpenAI from "openai";
// 求籤
import fs from "fs";
import path from "path";

// 星座 會用到 Redis  資料庫
import Redis from "ioredis";

const REDIS_URL = process.env.REDIS_URL;
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
    console.error("[redis] connection error:", err?.message || err);
  });
}

async function redisGet(key) {
  if (!redisClient) return null;
  try {
    return await redisClient.get(key);
  } catch (err) {
    console.error(`[redis] GET failed (${key}):`, err?.message || err);
    return null;
  }
}

async function redisSet(key, value, ...args) {
  if (!redisClient) return false;
  try {
    await redisClient.set(key, value, ...args);
    return true;
  } catch (err) {
    console.error(`[redis] SET failed (${key}):`, err?.message || err);
    return false;
  }
}

async function redisSetNx(key, value, ttlSeconds) {
  if (!redisClient) return null;
  try {
    const ttl =
      Number.isFinite(ttlSeconds) && ttlSeconds > 0 ? ttlSeconds : 10 * 60;
    const result = await redisClient.set(key, value, "EX", ttl, "NX");
    return result === "OK";
  } catch (err) {
    console.error(`[redis] SET NX failed (${key}):`, err?.message || err);
    return null;
  }
}

const LINE_TEXT_LIMIT = 5000;
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
        throw err;
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
const OPENCLAW_MODEL = process.env.OPENCLAW_MODEL || "openai/gpt-5-mini";
const OPENCLAW_TIMEOUT_MS = Number(process.env.OPENCLAW_TIMEOUT_MS || 20000);
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

async function findStock(query) {
  console.log("findStock query =", query);
  const raw = await redisGet("twse:stocks:all");
  if (!raw) return null;

  const stocks = JSON.parse(raw);

  const q = query.trim();

  // ✅ 1️⃣ 從句子中抓 4~6 碼股票代號（最重要）
  const codeMatch = q.match(/\b\d{4,6}\b/);
  if (codeMatch) {
    const code = codeMatch[0];
    if (stocks[code]) return stocks[code];
  }

  // ✅ 2️⃣ 名稱模糊（台積電 / 鴻海）
  return Object.values(stocks).find((s) => q.includes(s.name)) || null;
}

async function getStockQuote(symbol) {
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${symbol}?interval=1d&range=2d`;

  const res = await fetch(url, {
    headers: {
      "user-agent": "Mozilla/5.0",
      accept: "application/json",
    },
  });

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

  // ✅ 價格：優先用 regularMarketPrice，不行就用最後一根 close
  const price =
    meta.regularMarketPrice ??
    closes.filter((v) => typeof v === "number").slice(-1)[0];

  // ✅ 開盤價
  const open =
    meta.regularMarketOpen ??
    quote.open?.filter((v) => typeof v === "number")[0];

  // ✅ 昨收
  const prevClose =
    meta.previousClose ?? closes.filter((v) => typeof v === "number")[0];

  if (typeof price !== "number" || typeof prevClose !== "number") {
    return null;
  }

  const change = price - prevClose;
  const changePercent = (change / prevClose) * 100;

  return {
    price,
    open,
    change,
    changePercent,
    volume: meta.regularMarketVolume,
  };
}


function fmtTWPrice(n) {
  if (typeof n !== "number") return "--";
  return n >= 100 ? n.toFixed(1) : n.toFixed(2);
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

async function getGeneralAssistantReply(userText) {
  const systemPrompt =
    "你是 Kevin 的專屬助理，語氣自然、冷靜又帶點幽默。你是 Kevin 自己架在 Vercel 上的 LINE Bot。";

  if (OPENCLAW_CHAT_URL) {
    const controller = new AbortController();
    const timer = setTimeout(
      () => controller.abort(),
      Number.isFinite(OPENCLAW_TIMEOUT_MS) ? OPENCLAW_TIMEOUT_MS : 8000
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
        model: OPENCLAW_MODEL,
        stream: false,
        messages: [
          { role: "system", content: systemPrompt },
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
        throw new Error(
          `OpenClaw HTTP ${r.status}: ${(raw || "").slice(0, 200)}`
        );
      }

      let json;
      try {
        json = JSON.parse(raw);
      } catch {
        throw new Error(`OpenClaw 回傳不是 JSON: ${(raw || "").slice(0, 200)}`);
      }

      const text = extractAssistantText(json);
      if (text) return { text, provider: "openclaw" };

      throw new Error("OpenClaw 回傳找不到文字內容");
    } catch (err) {
      const reason = err?.message || String(err);
      console.error("OpenClaw failed:", {
        reason,
        url: OPENCLAW_CHAT_URL,
        model: OPENCLAW_MODEL,
        timeoutMs: OPENCLAW_TIMEOUT_MS,
        contentType: OPENCLAW_REQUEST_CONTENT_TYPE,
      });
      if (OPENCLAW_FORCE_ONLY) {
        return {
          text: `OpenClaw 暫時不可用（${reason.slice(0, 80)}），請稍後再試。`,
          provider: "openclaw_error",
        };
      }
      console.error("fallback to OpenAI");
    } finally {
      clearTimeout(timer);
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
      { role: "user", content: userText },
    ],
  });

  return {
    text: reply.choices?.[0]?.message?.content?.trim() || "我剛剛斷線了，再說一次",
    provider: "openai",
  };
}

app.post("/api/generate-bible-cards", async (req, res) => {
  if (req.headers.authorization !== `Bearer ${process.env.CRON_SECRET}`) {
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

app.post("/webhook", line.middleware(config), async (req, res) => {
  const events = req.body.events || [];

  for (const event of events) {
    try {
      if (event.type !== "message") continue;

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
          const q = await getStockQuote(stock.symbol);
          if (!q) throw new Error("no data");

          const sign = q.change >= 0 ? "+" : "";
          const percent =
            typeof q.changePercent === "number"
              ? q.changePercent.toFixed(2)
              : "--";

          const text = `📊 ${stock.name}（${stock.code}）

現價：${fmtTWPrice(q.price)}
漲跌：${sign}${fmtTWPrice(q.change)}（${sign}${percent}%）
開盤：${fmtTWPrice(q.open)}
成交量：${q.volume?.toLocaleString() ?? "--"} 張

※ 資料來源：Yahoo Finance（延遲報價）`;

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
      const reply = await getGeneralAssistantReply(parsedMessage);
      console.log("chat provider:", reply.provider);

      await replyMessageWithFallback(event, {
        type: "text",
        text: reply.text,
      });
    } catch (err) {
      console.error("Error handling event:", err);
    }
  }

  res.status(200).end();
});

app.get("/api/update-stocks", async (req, res) => {
  if (req.headers.authorization !== `Bearer ${process.env.CRON_SECRET}`) {
    return res.status(401).json({ error: "unauthorized" });
  }

  const url =
    "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=open_data";

  const r = await fetch(url, {
    headers: {
      "user-agent": "Mozilla/5.0",
      accept: "text/csv,application/json;q=0.9,*/*;q=0.8",
    },
  });

  const contentType = r.headers.get("content-type") || "";
  const text = await r.text();

  // ---- ✅ 診斷：前 3 行 + content-type + 前 120 字 ----
  const head120 = text.slice(0, 120);
  const linesRaw = text
    .split(/\n/)
    .slice(0, 5)
    .map((l) => l.slice(0, 200));

  // 如果根本不是 CSV（常見：HTML 被擋、或回 JSON）
  const looksLikeHTML = /<html|<!doctype html/i.test(text);
  const looksLikeJSON = /^\s*[\[{]/.test(text);

  // ---- ✅ 真正 CSV parser：支援引號/逗號/空欄位 ----
  function parseCsvLine(line) {
    const s = line.replace(/\r/g, ""); // 重要：去掉 \r
    const out = [];
    let cur = "";
    let inQuotes = false;

    for (let i = 0; i < s.length; i++) {
      const ch = s[i];

      if (inQuotes) {
        if (ch === '"') {
          // "" 代表 escaped quote
          if (s[i + 1] === '"') {
            cur += '"';
            i++;
          } else {
            inQuotes = false;
          }
        } else {
          cur += ch;
        }
      } else {
        if (ch === '"') inQuotes = true;
        else if (ch === ",") {
          out.push(cur);
          cur = "";
        } else {
          cur += ch;
        }
      }
    }
    out.push(cur);
    return out.map((x) => x.replace(/^\uFEFF/, "").trim()); // 去 BOM + trim
  }

  // ---- ✅ 解析 ----
  const allLines = text
    .split(/\n/)
    .map((l) => l.trim())
    .filter(Boolean);

  // 找 header 行（避免前面是空白或 BOM）
  const headerIndex = allLines.findIndex(
    (l) => l.includes("證券代號") && l.includes("證券名稱")
  );
  const startIndex = headerIndex >= 0 ? headerIndex + 1 : 1; // 找不到就假設第 1 行是 header

  const stocks = {};
  const samples = [];

  for (let i = startIndex; i < allLines.length; i++) {
    const line = allLines[i];
    if (!line) continue;

    const cols = parseCsvLine(line);

    const code = cols[1]?.trim(); // ✅ 證券代號
    const name = cols[2]?.trim(); // ✅ 證券名稱

    if (!/^\d{4,6}$/.test(code)) continue;

    stocks[code] = {
      code,
      name,
      symbol: `${code}.TW`,
    };
  }

  await redisSet("twse:stocks:all", JSON.stringify(stocks));

  return res.json({
    ok: true,
    count: Object.keys(stocks).length,
    debug: {
      status: r.status,
      contentType,
      looksLikeHTML,
      looksLikeJSON,
      head120,
      first5Lines: linesRaw,
      headerIndex,
      sampleParsed: samples,
    },
  });
});

// Default route
app.get("/", (req, res) => res.send("Kevin LINE GPT Bot Running"));

export default app;
