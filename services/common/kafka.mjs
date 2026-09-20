import { Kafka, Partitioners, logLevel } from 'kafkajs';
import { deadLetterEnvelope, deliver } from './envelope.mjs';
import { TOPICS } from './topics.mjs';
import { sleep } from './util.mjs';

/**
 * Thin wrapper over kafkajs. Services only see this interface:
 *   bus.producer.send(topic, key, envelope)
 *   bus.producer.sendBatch([{ topic, key, envelope }])
 *   bus.consume({ groupId, topics, handler })
 *   bus.close()
 * Tests use the same interface with an in-memory bus.
 */
export async function createBus({ clientId, brokers, log }) {
  const kafka = new Kafka({
    clientId,
    brokers,
    logLevel: logLevel.WARN,
    retry: { initialRetryTime: 300, retries: 12 },
  });

  const rawProducer = kafka.producer({
    allowAutoTopicCreation: false,
    createPartitioner: Partitioners.DefaultPartitioner,
  });
  await rawProducer.connect();
  const consumers = [];
  const timers = [];

  // Message key = orderId, so every event of one order goes to the same partition
  // and is read in order.
  async function sendBatch(items) {
    const byTopic = new Map();
    for (const { topic, key, envelope } of items) {
      if (!byTopic.has(topic)) byTopic.set(topic, []);
      byTopic.get(topic).push({
        key,
        value: JSON.stringify(envelope),
        headers: { traceId: envelope.traceId, type: envelope.type },
      });
    }
    await rawProducer.sendBatch({
      acks: -1, // wait for the broker to confirm
      topicMessages: [...byTopic].map(([topic, messages]) => ({ topic, messages })),
    });
  }
  const send = (topic, key, envelope) => sendBatch([{ topic, key, envelope }]);

  async function consume({ groupId, topics, handler, maxAttempts = 3, pausedCheck }) {
    const consumer = kafka.consumer({ groupId, sessionTimeout: 15000, heartbeatInterval: 3000 });
    await consumer.connect();
    for (const topic of topics) await consumer.subscribe({ topic, fromBeginning: true });
    await consumer.run({
      // autoCommit is on: the offset is committed after this function returns,
      // so delivery is at-least-once. Handlers must be idempotent.
      eachMessage: async ({ topic, partition, message }) => {
        const raw = message.value?.toString() ?? '';
        const meta = { topic, partition, offset: message.offset, groupId };
        await deliver({
          raw,
          meta,
          handler,
          maxAttempts,
          sleep,
          log,
          onDead: async (d) => {
            log?.error('sending to dead-letter', { reason: d.reason, ...meta });
            await send(TOPICS.DEAD_LETTER, `${topic}:${partition}:${message.offset}`, deadLetterEnvelope(d));
          },
        });
      },
    });
    consumers.push(consumer);

    // Optional "pause" switch (used by the dashboard's chaos page): when pausedCheck()
    // is true we stop fetching, so lag grows exactly like with a stopped container.
    if (pausedCheck) {
      let paused = false;
      const timer = setInterval(async () => {
        try {
          const want = await pausedCheck();
          if (want && !paused) {
            consumer.pause(topics.map((topic) => ({ topic })));
            paused = true;
            log?.warn('consumer paused', { groupId });
          } else if (!want && paused) {
            consumer.resume(topics.map((topic) => ({ topic })));
            paused = false;
            log?.warn('consumer resumed', { groupId });
          }
        } catch {
          /* redis hiccup: try again next tick */
        }
      }, 500);
      timers.push(timer);
    }
  }

  async function close() {
    timers.forEach(clearInterval);
    for (const c of consumers) await c.disconnect().catch(() => {});
    await rawProducer.disconnect().catch(() => {});
  }

  return { producer: { send, sendBatch }, consume, close };
}

export { Kafka };
