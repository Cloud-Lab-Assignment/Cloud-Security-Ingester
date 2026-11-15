import { Kafka } from 'kafkajs';
import { config } from './config/env.js';
import { createDetectionEngine } from './services/detection-engine.js';
import { consumer, producer } from './services/kafka.js';

const REWIND_RECORDS_PER_PARTITION = 20000;

const kafkaConfig = {
  clientId: `${config.serviceName}-admin`,
  brokers: config.brokers,
  ssl: process.env.KAFKA_SSL === 'true',
  retry: {
    retries: 5,
    initialRetryTime: 300,
    multiplier: 2,
    factor: 0,
    maxRetryTime: 30000
  }
};

if (process.env.KAFKA_USERNAME && process.env.KAFKA_PASSWORD) {
  kafkaConfig.sasl = {
    mechanism: process.env.KAFKA_SASL_MECHANISM ?? 'plain',
    username: process.env.KAFKA_USERNAME,
    password: process.env.KAFKA_PASSWORD
  };
}

const kafka = new Kafka(kafkaConfig);
const admin = kafka.admin();
const engine = createDetectionEngine();

const warmupUntil = new Map();

function partitionKey(topic, partition) {
  return `${topic}-${partition}`;
}

async function commitMessage(topic, partition, offset) {
  await consumer.commitOffsets([{
    topic,
    partition,
    offset: (BigInt(offset) + 1n).toString()
  }]);
}

async function sendToDlq(topic, partition, message, error) {
  const MAX_ORIGINAL_BYTES = 160 * 1024;
  const originalBytes = message.value;
  const truncated = originalBytes.length > MAX_ORIGINAL_BYTES;
  const storedBytes = truncated
    ? originalBytes.subarray(0, MAX_ORIGINAL_BYTES)
    : originalBytes;

  const dlqMessage = {
    schemaVersion: '1.0',
    failedAt: new Date().toISOString(),
    stage: 'detection-rules',
    errorCode: 'NORMALIZED_EVENT_PROCESSING_FAILED',
    errorMessage: error.message,
    attempts: 1,
    original: {
      topic,
      partition,
      offset: message.offset,
      key: message.key?.toString() ?? null,
      timestamp: message.timestamp,
      valueEncoding: 'base64',
      value: storedBytes.toString('base64'),
      ...(truncated ? { truncated: true } : {})
    },
    failedBy: {
      name: config.serviceName,
      version: config.serviceVersion
    }
  };

  await producer.send({
    topic: config.dlqTopic,
    acks: -1,
    messages: [{
      key: `${topic}-${partition}-${message.offset}`,
      value: JSON.stringify(dlqMessage)
    }]
  });
}

async function recoverAssignedPartitions(assignment) {
  const partitions = assignment[config.inputTopic] ?? [];

  if (partitions.length === 0) return;

  // Pause assigned partitions before fetching offsets or seeking.
  consumer.pause([{
    topic: config.inputTopic,
    partitions
  }]);

  try {
    const [committedTopics, topicOffsets] = await Promise.all([
      admin.fetchOffsets({
        groupId: config.groupId,
        topics: [config.inputTopic],
        resolveOffsets: false
      }),
      admin.fetchTopicOffsets(config.inputTopic)
    ]);

    const committedPartitions =
      committedTopics.find((item) => item.topic === config.inputTopic)?.partitions ?? [];

    const committedByPartition = new Map(
      committedPartitions.map((item) => [
        item.partition,
        item.offset === '-1' ? null : BigInt(item.offset)
      ])
    );

    const topicOffsetsByPartition = new Map(
      topicOffsets.map((item) => [item.partition, item])
    );

    for (const partition of partitions) {
      const offsets = topicOffsetsByPartition.get(partition);

      if (!offsets) {
        throw new Error(`No Kafka offsets found for partition ${partition}`);
      }

      const low = BigInt(offsets.low);
      const high = BigInt(offsets.high);
      const committed = committedByPartition.get(partition);

      const savedOffset =
        committed === null || committed === undefined ? low : committed;

      // Keep the recovery boundary inside Kafka's available offset range.
      const committedOffset =
        savedOffset < low ? low : savedOffset > high ? high : savedOffset;

      const rewindOffset =
        committedOffset > BigInt(REWIND_RECORDS_PER_PARTITION)
          ? committedOffset - BigInt(REWIND_RECORDS_PER_PARTITION)
          : 0n;

      const boundedStart =
        rewindOffset < low
          ? low
          : rewindOffset > high
            ? high
            : rewindOffset;

      warmupUntil.set(
        partitionKey(config.inputTopic, partition),
        committedOffset.toString()
      );

      consumer.seek({
        topic: config.inputTopic,
        partition,
        offset: boundedStart.toString()
      });

      console.log(
        `Partition ${partition}: low=${low}, committed=${committedOffset}, start=${boundedStart}`
      );
    }

    // Resume only after every assigned partition has been positioned.
    consumer.resume([{
      topic: config.inputTopic,
      partitions
    }]);
  } catch (error) {
    console.error('Partition recovery failed:', error);
    throw error;
  }
}

async function start() {
  await admin.connect();
  await producer.connect();
  await consumer.connect();

  consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
    engine.reset();

    recoverAssignedPartitions(payload.memberAssignment).catch(async (error) => {
      console.error('Unable to recover Kafka partitions:', error);
      process.exitCode = 1;
      await consumer.stop();
    });
  });

  await consumer.subscribe({
    topic: config.inputTopic,
    fromBeginning: true
  });

  await consumer.run({
    autoCommit: false,

    eachMessage: async ({ topic, partition, message }) => {
      const committedOffset = warmupUntil.get(
        partitionKey(topic, partition)
      );

      const isWarmup =
        committedOffset !== undefined &&
        BigInt(message.offset) < BigInt(committedOffset);

      let alerts;

      try {
        const event = JSON.parse(message.value.toString('utf8'));
        alerts = engine.evaluate(event, { warmup: isWarmup });
      } catch (error) {
        // Historical messages were already handled before their offsets
        // were committed. Do not publish them to the DLQ again.
        if (!isWarmup) {
          await sendToDlq(topic, partition, message, error);
        }

        await commitMessage(topic, partition, message.offset);
        return;
      }

      for (const alert of alerts) {
        await producer.send({
          topic: config.alertTopic,
          acks: -1,
          messages: [{
            key: alert.entityKey,
            value: JSON.stringify(alert)
          }]
        });
      }

      await commitMessage(topic, partition, message.offset);
    }
  });
}

async function shutdown() {
  try {
    await consumer.disconnect();
    await producer.disconnect();
    await admin.disconnect();
  } catch (error) {
    console.error('Shutdown error:', error);
  }
}

start().catch((error) => {
  console.error('Detection Rules service failed:', error);
  process.exitCode = 1;
});

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
