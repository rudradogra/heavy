import { Kafka } from 'kafkajs';

// Connect to the Kafka container running on port 9092
const kafka = new Kafka({
  clientId: 'pipeline-initializer',
  brokers: ['localhost:9092']
});

const admin = kafka.admin();

async function init() {
  console.log(' Connecting to Kafka Broker...');
  await admin.connect();
  console.log(' Admin Connected.');

  console.log(' Creating topic: market-ticks...');
  await admin.createTopics({
    topics: [{
      topic: 'market-ticks',
      numPartitions: 1, // Simple single partition for local dev
      replicationFactor: 1
    }]
  });
  
  console.log('Topic "market-ticks" created successfully!');
  await admin.disconnect();
}

init().catch(console.error);