import 'dotenv/config';
import WebSocket from 'ws';
import { Kafka } from 'kafkajs';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
const KAFKA_BROKER = 'localhost:9092';
const KAFKA_TOPIC = 'market-ticks';
const ALPACA_WS_URL = 'wss://stream.data.alpaca.markets/v2/test';
const MOCK_SYMBOL = 'FAKEPACA';

const { ALPACA_KEY_ID, ALPACA_SECRET_KEY } = process.env;

if (!ALPACA_KEY_ID || !ALPACA_SECRET_KEY) {
  console.error('[producer] Missing ALPACA_KEY_ID or ALPACA_SECRET_KEY in .env');
  process.exit(1);
}

// ---------------------------------------------------------------------------
// Kafka producer setup
// ---------------------------------------------------------------------------
const kafka = new Kafka({
  clientId: 'market-tick-producer',
  brokers: [KAFKA_BROKER],
});

const producer = kafka.producer();

// ---------------------------------------------------------------------------
// Alpaca WebSocket helpers
// ---------------------------------------------------------------------------

/** Send a JSON payload over the open WebSocket connection. */
function send(ws, payload) {
  ws.send(JSON.stringify(payload));
}

/**
 * Alpaca streams messages as JSON arrays. Each element may be a control
 * message (auth success, subscription ack) or a market-data event.
 */
function handleAlpacaMessage(raw, ws) {
  let messages;

  try {
    messages = JSON.parse(raw);
  } catch {
    console.warn('[producer] Received non-JSON WebSocket frame:', raw);
    return;
  }

  if (!Array.isArray(messages)) {
    messages = [messages];
  }

  for (const msg of messages) {
    switch (msg.T) {
      case 'success':
        console.log(`[producer] Alpaca: ${msg.msg}`);

        if (msg.msg === 'authenticated') {
          console.log('[producer] Subscribing to', MOCK_SYMBOL, 'trades');
          send(ws, {
            action: 'subscribe',
            trades: [MOCK_SYMBOL],
          });
        }
        break;

      case 'subscription':
        console.log('[producer] Subscription confirmed:', msg);
        break;

      case 'error':
        console.error('[producer] Alpaca error:', msg);
        break;

      case 't':
        // Trade event — extract fields and publish to Kafka
        publishTrade({
          symbol: msg.S,
          price: msg.p,
          size: msg.s,
          timestamp: msg.t,
        });
        break;

      default:
        // Ignore quotes, bars, heartbeats, etc.
        break;
    }
  }
}

/** Format a trade and push it to the Kafka topic. */
async function publishTrade({ symbol, price, size, timestamp }) {
  const payload = JSON.stringify({ symbol, price, size, timestamp });

  try {
    await producer.send({
      topic: KAFKA_TOPIC,
      messages: [{ value: payload }],
    });

    console.log('[producer] Published trade:', payload);
  } catch (err) {
    console.error('[producer] Failed to publish to Kafka:', err.message);
  }
}

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------
async function start() {
  console.log('[producer] Connecting to Kafka broker at', KAFKA_BROKER);
  await producer.connect();
  console.log('[producer] Kafka producer connected');

  console.log('[producer] Opening Alpaca WebSocket at', ALPACA_WS_URL);
  const ws = new WebSocket(ALPACA_WS_URL);

  ws.on('open', () => {
    console.log('[producer] WebSocket connected — sending auth handshake');

    send(ws, {
      action: 'auth',
      key: ALPACA_KEY_ID,
      secret: ALPACA_SECRET_KEY,
    });
  });

  ws.on('message', (data) => {
    handleAlpacaMessage(data.toString(), ws);
  });

  ws.on('error', (err) => {
    console.error('[producer] WebSocket error:', err.message);
  });

  ws.on('close', (code, reason) => {
    console.log(`[producer] WebSocket closed (code=${code}, reason=${reason || 'none'})`);
  });

  // Graceful shutdown
  const shutdown = async () => {
    console.log('[producer] Shutting down...');
    ws.close();
    await producer.disconnect();
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

start().catch((err) => {
  console.error('[producer] Fatal error:', err);
  process.exit(1);
});
