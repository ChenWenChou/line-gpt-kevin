import express from "express";
import line from "@line/bot-sdk";
import OpenAI from "openai";

const BOT_USER_ID = "U51d2392e43f851607a191adb3ec49b26";
const app = express();

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

const WHEN_LABEL = {
  today: "今日",
  tomorrow: "明日",
  day_after: "後天",
};

const TW_CITY_MAP = {
  "台北": "Taipei",
  "臺北": "Taipei",
  "新北": "New Taipei",
  "台中": "Taichung",
  "臺中": "Taichung",
  "台南": "Tainan",
  "臺南": "Tainan",
  "高雄": "Kaohsiung",
  "桃園": "Taoyuan",
  "新竹": "Hsinchu",
  "嘉義": "Chiayi",
  "宜蘭": "Yilan",
  "花蓮": "Hualien",
  "台東": "Taitung",
  "臺東": "Taitung",
};

function fixTaiwanCity(raw) {
  if (!raw) return raw;
  const trimmed = raw.trim();
  return TW_CITY_MAP[trimmed] || trimmed;
}

function normalizeWhen(raw = "today") {
  const text = raw.toLowerCase();
  if (["tomorrow", "明天", "明日"].includes(text)) return "tomorrow";
  if (["day_after", "後天", "day after", "day-after", "後日"].includes(text)) {
    return "day_after";
  }
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

async function geocodeCity(city, apiKey) {
  // 優先用「Taiwan + 城市」避免跑到中國同名地
  const queries = [`Taiwan ${city}`, city];

  for (const q of queries) {
    const geoUrl = `https://api.openweathermap.org/geo/1.0/direct?q=${encodeURIComponent(
      q
    )}&limit=1&appid=${apiKey}`;
    const geoRes = await fetch(geoUrl);
    if (!geoRes.ok) continue;
    const [geo] = await geoRes.json();
    if (!geo) continue;
    const name = geo.local_names?.zh || geo.name || city;
    return { lat: geo.lat, lon: geo.lon, name };
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

    if (!resolvedLat || !resolvedLon) {
      const geo = await geocodeCity(city, apiKey);
      if (!geo) {
        return "查不到這個城市的天氣，再確認一下城市名稱。";
      }
      resolvedLat = geo.lat;
      resolvedLon = geo.lon;
      resolvedCity = geo.name;
    }

    const forecastUrl = `https://api.openweathermap.org/data/2.5/forecast?lat=${resolvedLat}&lon=${resolvedLon}&units=metric&lang=zh_tw&appid=${apiKey}`;
    const res = await fetch(forecastUrl);
    if (!res.ok) {
      const text = await res.text();
      console.error("Weather API error:", res.status, text);
      return "查天氣時發生錯誤，可能是城市名稱或座標有問題，或 API 暫時掛了。";
    }

    const data = await res.json();
    const dayIndex = when === "tomorrow" ? 1 : when === "day_after" ? 2 : 0;

    const offsetSec = data.city?.timezone ?? 0;
    const now = Date.now();
    const targetDateStr = new Date(now + dayIndex * 24 * 60 * 60 * 1000)
      .toISOString()
      .slice(0, 10);

    const pickSlot = (list) => {
      const sameDay = list.filter((item) => {
        const local = new Date((item.dt + offsetSec) * 1000)
          .toISOString()
          .slice(0, 10);
        return local === targetDateStr;
      });
      if (sameDay.length === 0) return null;
      const noon =
        sameDay.find((item) => item.dt_txt?.includes("12:00:00")) || sameDay[0];
      return noon;
    };

    const slot = pickSlot(data.list || []);

    // --- 計算當日最高 / 最低溫 ---
    const sameDayEntries = (data.list || []).filter((item) => {
      const local = new Date((item.dt + offsetSec) * 1000)
        .toISOString()
        .slice(0, 10);
      return local === targetDateStr;
    });

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
    const humidity = slot.main?.humidity ?? "NA";
    const desc = slot.weather?.[0]?.description || "未知";
    const pop = typeof slot.pop === "number" ? slot.pop : 0;
    const rainPercent = Math.round(pop * 100);
    const rainText = `降雨機率：${rainPercent}%`;
    const locationLabel = address
      ? `${address}（座標）`
      : resolvedCity || city || "未命名地點";
    const whenLabel = WHEN_LABEL[when] || WHEN_LABEL.today;
    const outfit = buildOutfitAdvice(temp, feels, pop);
    const maxMinText =
      maxTemp !== null
        ? `最高溫：${maxTemp.toFixed(1)}°C\n最低溫：${minTemp.toFixed(1)}°C\n`
        : "";

    return (
      `【${locationLabel}｜${whenLabel}天氣】\n` +
      `狀態：${desc}\n` +
      tempRangeText +
      feelsRangeText +
      `濕度：${humidity}%\n` +
      `${rainText}\n\n` +
      `【穿搭建議】\n` +
      outfit
    );
  } catch (err) {
    console.error("Weather fetch error:", err);
    return "查天氣時發生例外錯誤，等等再試一次。";
  }
}

app.post("/webhook", line.middleware(config), async (req, res) => {
  const events = req.body.events || [];

  for (const event of events) {
    try {
      if (event.type !== "message") continue;

      // ① 使用者分享定位 → 直接查天氣
      if (event.message.type === "location") {
        const { address, latitude, longitude } = event.message;
        const info = await getWeatherAndOutfit({
          lat: latitude,
          lon: longitude,
          address,
          when: "today",
        });

        await client.replyMessage(event.replyToken, {
          type: "text",
          text: info,
        });
        continue;
      }

      if (event.message.type !== "text") continue;

      const userMessage = event.message.text.trim();

      // ② 群組 / 房間模式：只有「真的 @」或「叫名字開頭」才回
      if (event.source.type === "group" || event.source.type === "room") {
        const mention = event.message?.mention;

        const mentionedBot =
          mention &&
          Array.isArray(mention.mentionees) &&
          mention.mentionees.some((m) => m.userId === BOT_USER_ID);

        // 用文字叫名字也算，比如：
        // @KevinBot 桃園 明天天氣
        // KevinBot 桃園 明天天氣
        const calledByName =
          userMessage.startsWith("@KevinBot") ||
          userMessage.startsWith("KevinBot") ||
          userMessage.startsWith("kevinbot") ||
          userMessage.startsWith("Kevin") ||
          userMessage.startsWith("kevin") ||
          userMessage.startsWith("文哥") ||
          userMessage.startsWith("周真豬");

        if (!mentionedBot && !calledByName) {
          // 沒真的 @，也沒有以名字開頭 → 不回應
          continue;
        }
      }

      // ③ 用 GPT 判斷是不是在問天氣 / 穿搭
      const intent = await openai.chat.completions.create({
        model: "gpt-4o-mini",
        messages: [
        {
          role: "system",
          content: `
你是一個意圖判斷與解析器。

【地點判斷規則】
1. 使用者提到的台灣城市（台北、台中、桃園、新竹、嘉義、台南、高雄、花蓮、宜蘭等）一律優先視為「台灣」的城市。
2. 如果使用者只講「台中」「台南」「台北」這類簡稱，也必須自動解析為「台灣台中市」「台灣台南市」「台灣台北市」。
3. 除非使用者明確說「中國的 XXX」，否則地點一律以「台灣」為預設國家。

【意圖規則】
如果訊息是在問天氣、氣溫、下雨、穿什麼、冷不冷，請回：
WEATHER|城市名稱（盡量從文字中推論，推論不到請回 Taipei）|when

when 僅能是 today / tomorrow / day_after
（使用者問「明天」就回 tomorrow，「後天」就回 day_after）

如果不是，請回：
NO
            `,
        },
          { role: "user", content: userMessage },
        ],
      });

      const intentText = intent.choices[0].message.content?.trim?.() ?? "NO";

      if (intentText.startsWith("WEATHER")) {
        const [, cityRaw, whenRaw] = intentText.split("|");
        const city = fixTaiwanCity(cityRaw || "Taipei");
        const when = normalizeWhen(whenRaw || "today");

        const info = await getWeatherAndOutfit({ city, when });

        await client.replyMessage(event.replyToken, {
          type: "text",
          text: info,
        });
        continue;
      }

      // ④ 一般聊天 → GPT 回覆
      const reply = await openai.chat.completions.create({
        model: "gpt-4o-mini",
        messages: [
          {
            role: "system",
            content: `
你是 Kevin 的專屬助理，語氣自然、冷靜又帶點幽默。
你是 Kevin 自己架在 Vercel 上的 LINE Bot，由 OpenAI API 驅動。
            `,
          },
          { role: "user", content: userMessage },
        ],
      });

      await client.replyMessage(event.replyToken, {
        type: "text",
        text: reply.choices[0].message.content,
      });
    } catch (err) {
      console.error("Error handling event:", err);
      // 失敗也回 200，避免 LINE 一直重送
    }
  }

  res.status(200).end();
});

// Default route
app.get("/", (req, res) => res.send("Kevin LINE GPT Bot Running"));

export default app;
