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
  const geoUrl = `https://api.openweathermap.org/geo/1.0/direct?q=${encodeURIComponent(
    city
  )}&limit=1&appid=${apiKey}`;
  const geoRes = await fetch(geoUrl);
  if (!geoRes.ok) return null;
  const [geo] = await geoRes.json();
  if (!geo) return null;
  const name = geo.local_names?.zh || geo.name || city;
  return { lat: geo.lat, lon: geo.lon, name };
}

// 查天氣 + 穿搭建議（支援城市名或座標、今天/明天/後天、降雨機率）
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

    // 如果沒座標，先用城市名找座標
    if (!resolvedLat || !resolvedLon) {
      const geo = await geocodeCity(city, apiKey);
      if (!geo) {
        return "查不到這個城市的天氣，再確認一下城市名稱。";
      }
      resolvedLat = geo.lat;
      resolvedLon = geo.lon;
      resolvedCity = geo.name;
    }

    const forecastUrl = `https://api.openweathermap.org/data/3.0/onecall?lat=${resolvedLat}&lon=${resolvedLon}&units=metric&lang=zh_tw&exclude=minutely,alerts&appid=${apiKey}`;
    const res = await fetch(forecastUrl);
    if (!res.ok) {
      console.error("Weather API error:", res.status, res.statusText);
      return "查天氣時發生錯誤，可能是城市名稱或座標有問題，或 API 暫時掛了。";
    }

    const data = await res.json();
    const dayIndex = when === "tomorrow" ? 1 : when === "day_after" ? 2 : 0;
    const targetDaily = data.daily?.[dayIndex];

    if (!targetDaily) {
      return "暫時查不到這個時間點的天氣，等等再試一次。";
    }

    const temp = targetDaily.temp?.day ?? data.current?.temp;
    const feels = targetDaily.feels_like?.day ?? data.current?.feels_like;
    const humidity = targetDaily.humidity ?? data.current?.humidity;
    const desc =
      targetDaily.weather?.[0]?.description ||
      data.current?.weather?.[0]?.description ||
      "未知";
    const pop = typeof targetDaily.pop === "number" ? targetDaily.pop : 0;
    const rainPercent = Math.round(pop * 100);
    const rainText = `降雨機率：${rainPercent}%`;
    const locationLabel = address
      ? `${address}（座標）`
      : resolvedCity || city || "未命名地點";
    const whenLabel = WHEN_LABEL[when] || WHEN_LABEL.today;
    const outfit = buildOutfitAdvice(temp, feels, pop);

    return (
      `【${locationLabel}｜${whenLabel}天氣】\n` +
      `狀態：${desc}\n` +
      `溫度：${temp.toFixed(1)}°C（體感 ${feels.toFixed(1)}°C）\n` +
      `濕度：${humidity}%\n` +
      `${rainText}\n` +
      `\n【穿搭建議】\n` +
      outfit
    );
  } catch (err) {
    console.error("Weather fetch error:", err);
    return "查天氣時發生例外錯誤，等等再試一次。";
  }
}

app.post("/webhook", line.middleware(config), async (req, res) => {
  const events = req.body.events;

  for (const event of events) {
    if (event.type !== "message") continue;

    // ----------------------------
    // ① 處理使用者分享的定位，直接查天氣
    // ----------------------------
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

    // ----------------------------
    // ② GROUP MODE: 只有被 @ 才理
    // ----------------------------
    if (event.source.type === "group") {
      const mention = event.message?.mention;

      if (!mention || !mention.mentionees) continue;

      const mentionedBot = mention.mentionees.some(
        (m) => m.userId === BOT_USER_ID
      );

      if (!mentionedBot) continue;
    }

    // ----------------------------
    // ③ 用 GPT 判斷是否是問天氣（含明天/後天）
    // ----------------------------
    const intent = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      messages: [
        {
          role: "system",
          content: `
你是一個意圖判斷與解析器。判斷訊息是否為「詢問天氣」或「詢問穿搭」。

如果訊息是在問天氣、氣溫、下雨、穿什麼、冷不冷，請回：
WEATHER|城市名稱（盡量從文字中推論，推論不到請回 Taipei）|when
when 僅能是 today / tomorrow / day_after （使用者問「明天」就回 tomorrow，「後天」就回 day_after）

如果不是，請回：
NO
          `,
        },
        { role: "user", content: userMessage },
      ],
    });

    const intentText = intent.choices[0].message.content.trim();

    if (intentText.startsWith("WEATHER")) {
      const [, cityRaw, whenRaw] = intentText.split("|");
      const city = (cityRaw || "Taipei").trim();
      const when = normalizeWhen(whenRaw || "today");

      const info = await getWeatherAndOutfit({ city, when });

      await client.replyMessage(event.replyToken, {
        type: "text",
        text: info,
      });

      continue; // 不進入一般 GPT 回覆
    }

    // ----------------------------
    // ④ 一般聊天 → GPT 回覆
    // ----------------------------
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
  }

  res.status(200).end();
});

// Default route
app.get("/", (req, res) => res.send("Kevin LINE GPT Bot Running"));

export default app;
