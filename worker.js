import { randomUUID } from 'crypto';
import { Kafka } from 'kafkajs';
import { initTraceback, publishTelemetry } from './traceback.js';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
const KAFKA_BROKER = 'localhost:9092';
const KAFKA_TOPIC = 'market-ticks';
const CONSUMER_GROUP = 'trading-execution-group';
const PROCESSING_WARN_MS = Number(process.env.PROCESSING_WARN_MS ?? 500);
const HEARTBEAT_INTERVAL_MS = 60_000;

// ---------------------------------------------------------------------------
// Kafka consumer setup
// ---------------------------------------------------------------------------
const kafka = new Kafka({
  clientId: 'market-tick-worker',
  brokers: [KAFKA_BROKER],
});

const consumer = kafka.consumer({ groupId: CONSUMER_GROUP });

// ---------------------------------------------------------------------------
// Runtime stats (reported in periodic heartbeats)
// ---------------------------------------------------------------------------
const stats = {
  processed: 0,
  errors: 0,
  latencies: [],
  lastOffset: null,
  lastPartition: null,
  lastTopic: null,
};

function recordLatency(ms) {
  stats.latencies.push(ms);
  if (stats.latencies.length > 1000) {
    stats.latencies.shift();
  }
}

function avgLatency() {
  if (stats.latencies.length === 0) return 0;
  const sum = stats.latencies.reduce((a, b) => a + b, 0);
  return Math.round(sum / stats.latencies.length);
}

function p95Latency() {
  if (stats.latencies.length === 0) return 0;
  const sorted = [...stats.latencies].sort((a, b) => a - b);
  const idx = Math.ceil(0.95 * sorted.length) - 1;
  return sorted[Math.max(0, idx)];
}

// ---------------------------------------------------------------------------
// Message processing
// ---------------------------------------------------------------------------

/**
 * Route a trade flagged for execution by the upstream enrichment service.
 * Assumes routing metadata was attached earlier in the pipeline.
 */
function routeOrder(trade) {
  const notional = trade.price * trade.size;

  const order = {
    symbol: trade.symbol,
    side: trade.side,
    notional: notional.toFixed(2),
    venue: trade.routing.primary.toUpperCase(),
    submittedAt: new Date(trade.timestamp).toISOString(),
  };

  console.log('[worker] Order routed:', order);
}

/**
 * Process a single Kafka message. Parsing and business logic live inside a
 * structured try/catch so Traceback receives ERROR telemetry on failure.
 */
async function processMessage(rawPayload, { traceId, topic, partition, offset }) {
  const start = Date.now();

  try {
    const trade = JSON.parse(rawPayload);

    // Test hook: inject artificial delay to simulate bottlenecks
    if (trade.__slowMs) {
      await new Promise((resolve) => setTimeout(resolve, trade.__slowMs));
    }

    console.log('[worker] Trade received:', {
      symbol: trade.symbol,
      price: trade.price,
      size: trade.size,
      timestamp: trade.timestamp,
    });

    if (trade.execute) {
      routeOrder(trade);
    }
  } catch (err) {
    stats.errors++;

    const telemetryMessage =
      `[worker/processMessage] ${err.name}: ${err.message}\n` +
      `Payload: ${rawPayload}\n` +
      `${err.stack}`;

    console.error('[worker] Processing failed:', {
      errorName: err.name,
      errorMessage: err.message,
      payload: rawPayload,
      stack: err.stack,
    });

    await publishTelemetry({
      level: 'ERROR',
      message: telemetryMessage,
      traceId,
      context: { topic, partition, offset },
    });
  } finally {
    const elapsed = Date.now() - start;
    stats.processed++;
    recordLatency(elapsed);
    stats.lastTopic = topic;
    stats.lastPartition = partition;
    stats.lastOffset = offset;

    if (elapsed > PROCESSING_WARN_MS) {
      const warnMessage =
        `[worker/bottleneck] message processing took ${elapsed}ms ` +
        `(threshold ${PROCESSING_WARN_MS}ms) ` +
        `topic=${topic} partition=${partition} offset=${offset}`;

      console.warn(warnMessage);

      await publishTelemetry({
        level: 'WARN',
        message: warnMessage,
        traceId,
      });
    }
  }
}

// ---------------------------------------------------------------------------
// Heartbeat
// ---------------------------------------------------------------------------
let heartbeatTimer = null;

function startHeartbeat() {
  heartbeatTimer = setInterval(async () => {
    const message =
      `[worker/heartbeat] processed=${stats.processed} errors=${stats.errors} ` +
      `avg_latency_ms=${avgLatency()} p95_latency_ms=${p95Latency()} ` +
      `last_offset=${stats.lastOffset ?? 'none'}`;

    console.log(message);

    await publishTelemetry({
      level: 'INFO',
      message,
      traceId: randomUUID(),
    });
  }, HEARTBEAT_INTERVAL_MS);
}

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------
async function start() {
  await initTraceback();

  console.log('[worker] Connecting to Kafka broker at', KAFKA_BROKER);
  await consumer.connect();
  console.log('[worker] Consumer connected');

  console.log(`[worker] Subscribing to topic "${KAFKA_TOPIC}" (group: ${CONSUMER_GROUP})`);
  await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: true });

  startHeartbeat();

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const rawPayload = message.value?.toString() ?? '';
      const traceId = randomUUID();

      console.log(
        `[worker] Message received (topic=${topic}, partition=${partition}, offset=${message.offset})`,
      );

      await processMessage(rawPayload, {
        traceId,
        topic,
        partition,
        offset: message.offset,
      });
    },
  });
}

// Graceful shutdown
const shutdown = async () => {
  console.log('[worker] Shutting down...');
  if (heartbeatTimer) clearInterval(heartbeatTimer);
  await consumer.disconnect();
  process.exit(0);
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

start().catch((err) => {
  console.error('[worker] Fatal error:', err);
  process.exit(1);
});
