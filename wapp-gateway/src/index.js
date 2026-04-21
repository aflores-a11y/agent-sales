require("dotenv").config();
const { Client, LocalAuth, MessageMedia } = require("whatsapp-web.js");
const Redis = require("ioredis");
const qrcode = require("qrcode-terminal");
const QRCode = require("qrcode");
const express = require("express");

const REDIS_URL = process.env.REDIS_URL || "redis://localhost:6379";
const INCOMING_QUEUE = "queue:incoming";
const OUTGOING_QUEUE = "queue:outgoing";
const QR_PORT = parseInt(process.env.PORT || process.env.QR_PORT || "3100");

const redis = new Redis(REDIS_URL);
const redisSub = new Redis(REDIS_URL);

let currentQR = null;
let clientReady = false;

// --- QR Web Server ---
const app = express();

app.get("/", (req, res) => {
  if (clientReady) {
    res.send("<h1>WhatsApp Gateway</h1><p style='color:green;font-size:24px'>Connected</p>");
  } else if (currentQR) {
    res.redirect("/qr");
  } else {
    res.send("<h1>WhatsApp Gateway</h1><p>Initializing... refresh in a few seconds.</p>");
  }
});

app.get("/qr", async (req, res) => {
  if (clientReady) {
    return res.send("<h1>Already connected</h1><p style='color:green'>WhatsApp session is active.</p>");
  }
  if (!currentQR) {
    return res.send("<h1>No QR yet</h1><p>Waiting for QR code... refresh in a few seconds.</p>");
  }
  try {
    const qrDataUrl = await QRCode.toDataURL(currentQR, { width: 400, margin: 2 });
    res.send(`
      <html><head><meta http-equiv="refresh" content="15"><title>WhatsApp QR</title></head>
      <body style="display:flex;flex-direction:column;align-items:center;justify-content:center;height:100vh;font-family:sans-serif">
        <h1>Scan QR to connect WhatsApp</h1>
        <img src="${qrDataUrl}" alt="QR Code" />
        <p style="color:gray">Page refreshes automatically. QR expires in ~60s.</p>
      </body></html>
    `);
  } catch (err) {
    res.status(500).send("Error generating QR image");
  }
});

app.get("/status", (req, res) => {
  res.json({ ready: clientReady, hasQR: !!currentQR });
});

app.listen(QR_PORT, () => console.log(`QR web server on port ${QR_PORT}`));

// --- WhatsApp Client ---
const PAIRING_NUMBER = process.env.PAIRING_NUMBER || "";

const client = new Client({
  authStrategy: new LocalAuth(),
  puppeteer: { headless: true, args: ["--no-sandbox"] },
});

client.on("qr", async (qr) => {
  if (PAIRING_NUMBER) {
    try {
      const code = await client.requestPairingCode(PAIRING_NUMBER);
      console.log(`PAIRING CODE: ${code} — enter this in WhatsApp > Linked Devices > Link with phone number`);
    } catch (err) {
      console.error("Pairing code error:", err.message);
    }
  } else {
    currentQR = qr;
    console.log("Scan this QR code to log in:");
    qrcode.generate(qr, { small: true });
    console.log(`Or open http://localhost:${QR_PORT}/qr in your browser`);
  }
});

client.on("ready", () => {
  currentQR = null;
  clientReady = true;
  console.log("WhatsApp client is ready");
  pollOutgoing();
});

client.on("message", async (msg) => {
  const payload = {
    from: msg.from,
    body: msg.body || "",
    timestamp: msg.timestamp,
    messageId: msg.id._serialized,
  };

  // For voice notes / audio, download media and attach base64
  if (msg.hasMedia && (msg.type === "ptt" || msg.type === "audio")) {
    try {
      console.log(`Voice note from ${msg.from}, downloading...`);
      const media = await msg.downloadMedia();
      payload.audio = media.data; // base64-encoded
      payload.mimetype = media.mimetype;
    } catch (err) {
      console.error(`Error downloading voice note from ${msg.from}:`, err);
      return;
    }
  }

  // Skip messages with no text and no audio
  if (!payload.body && !payload.audio) return;

  await redis.lpush(INCOMING_QUEUE, JSON.stringify(payload));
  console.log(`Incoming from ${msg.from}: ${payload.audio ? "[voice note]" : payload.body}`);
});

async function pollOutgoing() {
  while (true) {
    try {
      const result = await redisSub.brpop(OUTGOING_QUEUE, 0);
      if (!result) continue;

      const message = JSON.parse(result[1]);
      const delay = (message.delay || 1 + Math.random() * 2) * 1000;

      await new Promise((r) => setTimeout(r, delay));
      if (message.attachment) {
        const media = new MessageMedia(
          message.attachment.mimetype,
          message.attachment.data,
          message.attachment.filename,
        );
        await client.sendMessage(message.to, media, { caption: message.body });
        console.log(`Outgoing to ${message.to}: [attachment] ${message.attachment.filename}`);
      } else {
        await client.sendMessage(message.to, message.body, { parseVCards: true });
        console.log(`Outgoing to ${message.to}: ${message.body}`);
      }
    } catch (err) {
      console.error("Error processing outgoing message:", err);
    }
  }
}

client.on("auth_failure", (msg) => {
  console.error("WhatsApp auth failure:", msg);
  currentQR = null;
});

client.on("disconnected", (reason) => {
  console.log("WhatsApp disconnected:", reason);
  clientReady = false;
  currentQR = null;
  setTimeout(() => client.initialize(), 5000);
});

client.initialize().catch((err) => {
  console.error("WhatsApp init error (will retry):", err.message);
  setTimeout(() => client.initialize(), 10000);
});
