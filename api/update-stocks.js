// /api/update-stocks.js
import Redis from "ioredis";

const redis = new Redis(process.env.REDIS_URL);

export default async function handler(req, res) {
  if (req.headers.authorization !== `Bearer ${process.env.CRON_SECRET}`) {
    return res.status(401).json({ error: "unauthorized" });
  }

  const url =
    "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=open_data";

  const r = await fetch(url);
  const text = await r.text();

  const lines = text.split("\n").slice(1);
  const stocks = {};

  for (const line of lines) {
    const cols = line.split(",");
    if (cols.length < 2) continue;

    const code = cols[0]?.trim();
    const name = cols[1]?.trim();

    if (!/^\d{4}$/.test(code)) continue;

    stocks[code] = {
      code,
      name,
      symbol: `${code}.TW`,
    };
  }

  await redis.set("twse:stocks:all", JSON.stringify(stocks));

  res.json({
    ok: true,
    count: Object.keys(stocks).length,
  });
}