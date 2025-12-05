import express from "express";
import line from "@line/bot-sdk";
import OpenAI from "openai";

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

// 處理 LINE 事件
app.post("/webhook", line.middleware(config), async (req, res) => {
  const events = req.body.events;

  for (const event of events) {
    if (event.type === "message" && event.message.type === "text") {
      const userMessage = event.message.text;

      const reply = await openai.chat.completions.create({
        model: "gpt-4o-mini",
        messages: [
          {
            role: "system",
            content: "你是 Kevin 的專屬助手，語氣自然、冷靜、有點幽默。",
          },
          { role: "user", content: userMessage },
        ],
      });

      await client.replyMessage(event.replyToken, {
        type: "text",
        text: reply.choices[0].message.content,
      });
    }
  }

  res.status(200).end();
});

// Default route
app.get("/", (req, res) => res.send("Kevin LINE GPT Bot Running"));

export default app;