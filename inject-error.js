import { Kafka } from 'kafkajs';

// Simulates upstream enrichment flagging a trade for execution but dropping routing metadata.
const POISON_PAYLOAD = {
  symbol: 'FAKEPACA',
  price: 134.56,
  size: 3,
  timestamp: new Date().toISOString(),
  side: 'buy',
  execute: true,
};

const kafka = new Kafka({
  clientId: 'error-injector',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer();

async function inject() {
  await producer.connect();

  const payload = JSON.stringify(POISON_PAYLOAD);

  await producer.send({
    topic: 'market-ticks',
    messages: [{ value: payload }],
  });

  console.log('[inject-error] Sent poison message (valid JSON, missing routing metadata):');
  console.log(payload);
  console.log('[inject-error] Expected: TypeError on trade.routing.primary → ERROR in Traceback');

  await producer.disconnect();
}

inject().catch((err) => {
  console.error('[inject-error] Failed:', err.message);
  process.exit(1);
});
