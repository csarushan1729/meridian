// One-shot job: create the Kafka topics, then exit. Other containers wait for this to finish.
import { Kafka, logLevel } from 'kafkajs';
import { makeLogger } from '../common/log.mjs';
import { TOPIC_SPECS } from '../common/topics.mjs';
import { sleep } from '../common/util.mjs';

const log = makeLogger('init');
const kafka = new Kafka({
  clientId: 'init',
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
  logLevel: logLevel.NOTHING,
});
const admin = kafka.admin();

for (let attempt = 1; ; attempt++) {
  try {
    await admin.connect();
    const created = await admin.createTopics({
      waitForLeaders: true,
      topics: TOPIC_SPECS.map((t) => ({ topic: t.topic, numPartitions: t.partitions, replicationFactor: 1 })),
    });
    log.info('topics ready', { newlyCreated: created, all: TOPIC_SPECS.map((t) => `${t.topic}:${t.partitions}`) });
    await admin.disconnect();
    break;
  } catch (err) {
    if (attempt >= 30) {
      log.error('giving up', { err: err.message });
      process.exit(1);
    }
    log.warn('kafka not ready, retrying', { attempt, err: err.message });
    await admin.disconnect().catch(() => {});
    await sleep(2000);
  }
}
process.exit(0);
