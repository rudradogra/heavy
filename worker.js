import { Kafka } from 'kafkajs';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
const KAFKA_BROKER = 'localhost:9092';
const KAFKA_TOPIC = 'market-ticks';
const CONSUMER_GROUP = 'trading-execution-group';

// ---------------------------------------------------------------------------
// Kafka consumer setup
// ---------------------------------------------------------------------------
const kafka = new Kafka({
  clientId: 'market-tick-worker',
  brokers: [KAFKA_BROKER],
});

const consumer = kafka.consumer({ groupId: CONSUMER_GROUP });

// ---------------------------------------------------------------------------
// Message processing
// ---------------------------------------------------------------------------

/**
 * Process a single Kafka message. Parsing and business logic live inside a
 * structured try/catch so Traceback can hook into the telemetry on failure.
 */
function processMessage(rawPayload) {
  try {
    const trade = JSON.parse(rawPayload);

    console.log('[worker] Trade received:', {
      symbol: trade.symbol,
      price: trade.price,
      size: trade.size,
      timestamp: trade.timestamp,
    });
  } catch (err) {
    // Detailed telemetry object for error-monitoring tools (e.g. Traceback)
    console.error('[worker] Processing failed:', {
      errorName: err.name,
      errorMessage: err.message,
      payload: rawPayload,
      stack: err.stack,
    });
  }
}

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------
async function start() {
  console.log('[worker] Connecting to Kafka broker at', KAFKA_BROKER);
  await consumer.connect();
  console.log('[worker] Consumer connected');

  console.log(`[worker] Subscribing to topic "${KAFKA_TOPIC}" (group: ${CONSUMER_GROUP})`);
  await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const rawPayload = message.value?.toString() ?? '';

      console.log(
        `[worker] Message received (topic=${topic}, partition=${partition}, offset=${message.offset})`,
      );

      processMessage(rawPayload);
    },
  });
}

// Graceful shutdown
const shutdown = async () => {
  console.log('[worker] Shutting down...');
  await consumer.disconnect();
  process.exit(0);
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

start().catch((err) => {
  console.error('[worker] Fatal error:', err);
  process.exit(1);
});
