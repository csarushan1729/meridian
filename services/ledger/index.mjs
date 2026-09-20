import { makePool, waitForDb } from '../common/db.mjs';
import { startHttp } from '../common/http.mjs';
import { createBus } from '../common/kafka.mjs';
import { isPaused } from '../common/chaos.mjs';
import { makeLogger } from '../common/log.mjs';
import { Metrics, metricsRoute } from '../common/metrics.mjs';
import { makeRedis } from '../common/redis.mjs';
import { onShutdown } from '../common/run.mjs';
import { createLedger } from './service.mjs';

const log = makeLogger('ledger');
const pool = makePool(process.env.DATABASE_URL ?? 'postgres://meridian:meridian@localhost:5433/ledger');
await waitForDb(pool, log);
const redis = makeRedis(process.env.REDIS_URL ?? 'redis://localhost:6379');
const metrics = new Metrics();
const bus = await createBus({
  clientId: 'ledger',
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
  log,
});

const svc = createLedger({ pool, log });
await svc.migrate();
const server = startHttp({
  port: Number(process.env.PORT ?? 4005),
  routes: [...svc.routes, metricsRoute('ledger', metrics, async () => ({ paused: await isPaused(redis, 'ledger') }))],
  log,
});
await bus.consume({
  groupId: svc.groupId,
  topics: svc.topics,
  handler: metrics.wrap(svc.handle),
  pausedCheck: () => isPaused(redis, 'ledger'),
});
log.info('ledger ready', { groupId: svc.groupId });

onShutdown(log, () => bus.close(), () => redis.quit(), () => pool.end(), () => server.close());
