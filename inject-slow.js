import { Kafka } from 'kafkajs';

// Simulates a processing bottleneck by injecting artificial delay via __slowMs.
const SLOW_PAYLOAD = {
  symbol: 'FAKEPACA',
  price: 134.56,
  size: 3,
  timestamp: new Date().toISOString(),
  __slowMs: 600,
};

const kafka = new Kafka({
  clientId: 'slow-injector',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer();

async function inject() {
  await producer.connect();

  const payload = JSON.stringify(SLOW_PAYLOAD);

  await producer.send({
    topic: 'market-ticks',
    messages: [{ value: payload }],
  });

  console.log('[inject-slow] Sent slow-processing message (__slowMs=600):');
  console.log(payload);
  console.log('[inject-slow] Expected: WARN bottleneck telemetry in Traceback');

  await producer.disconnect();
}

inject().catch((err) => {
  console.error('[inject-slow] Failed:', err.message);
  process.exit(1);
});
