// Local stand-in for the cloud trigger (docker-compose.full.yml only).
// Consumes raw-events and invokes the normalizer handler with the Kafka event
// shape it expects (the same shape src/gcf.js builds from the Confluent
// connector's HTTP request). Not packaged into the function source.
import { Kafka, logLevel } from 'kafkajs';
import { handler } from '../src/handler.js';

const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:29092').split(',').map((b) => b.trim());
const topic = process.env.RAW_TOPIC ?? 'raw-events';

const kafka = new Kafka({ clientId: 'event-normalizer-local', brokers, logLevel: logLevel.ERROR });
const consumer = kafka.consumer({ groupId: 'event-normalizer-local-cg' });

await consumer.connect();
await consumer.subscribe({ topic, fromBeginning: true });
console.log(JSON.stringify({ msg: 'local normalizer runner started', topic, brokers }));

await consumer.run({
  eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
    const event = {
      eventSource: 'aws:kafka',
      records: {
        [`${batch.topic}-${batch.partition}`]: batch.messages.map((m) => ({
          topic: batch.topic,
          partition: batch.partition,
          offset: Number(m.offset),
          timestamp: Number(m.timestamp),
          value: m.value.toString('base64'),
        })),
      },
    };
    const result = await handler(event);
    console.log(JSON.stringify({ msg: 'lambda handler invoked', partition: batch.partition, ...result }));
    resolveOffset(batch.messages[batch.messages.length - 1].offset);
    await heartbeat();
  },
});

for (const sig of ['SIGTERM', 'SIGINT']) {
  process.once(sig, async () => {
    await consumer.disconnect();
    process.exit(0);
  });
}
