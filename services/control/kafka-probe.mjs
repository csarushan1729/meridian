import { Kafka, logLevel } from 'kafkajs';
import { TOPIC_SPECS } from '../common/topics.mjs';

/**
 * The control service's window into Kafka:
 *  - admin API: log-end offsets per partition, consumer group offsets (=> lag), cluster info
 *  - an "observer" consumer that keeps the last messages of every topic for the Bus page
 */
export async function createKafkaProbe({ brokers, log }) {
  const kafka = new Kafka({ clientId: 'control', brokers, logLevel: logLevel.WARN, retry: { initialRetryTime: 300, retries: 12 } });
  const admin = kafka.admin();
  await admin.connect();

  const events = [];
  const lastTypes = {};
  const consumer = kafka.consumer({ groupId: 'control-observer' });
  await consumer.connect();
  for (const { topic } of TOPIC_SPECS) await consumer.subscribe({ topic, fromBeginning: false });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      let env = {};
      try {
        env = JSON.parse(message.value?.toString() ?? '{}');
      } catch {
        /* keep empty */
      }
      const data = env.type === 'dead-letter' ? { reason: env.data?.reason, from: env.data?.from } : env.data;
      lastTypes[topic] = env.type ?? 'invalid';
      events.unshift({
        offset: Number(message.offset),
        partition,
        topic,
        key: message.key?.toString() ?? '',
        type: env.type ?? 'invalid',
        ts: env.ts ?? Number(message.timestamp),
        traceId: env.traceId ?? '',
        payload: JSON.stringify(data ?? {}).slice(0, 160),
      });
      events.length = Math.min(events.length, 60);
    },
  });

  return {
    async topicOffsets() {
      const out = [];
      for (const { topic } of TOPIC_SPECS) {
        const offs = await admin.fetchTopicOffsets(topic);
        out.push({
          topic,
          partitions: offs.map((o) => ({ partition: o.partition, low: Number(o.low), high: Number(o.high) })),
        });
      }
      return out;
    },
    async groupOffsets(groupId, topics) {
      return admin.fetchOffsets({ groupId, topics });
    },
    async clusterInfo() {
      const c = await admin.describeCluster();
      return { clusterId: c.clusterId, controller: c.controller, brokers: c.brokers.map((b) => ({ nodeId: b.nodeId, host: b.host, port: b.port })) };
    },
    recentEvents: () => events.slice(),
    lastTypes: () => ({ ...lastTypes }),
    async close() {
      await consumer.disconnect().catch(() => {});
      await admin.disconnect().catch(() => {});
      log?.info('kafka probe closed');
    },
  };
}
