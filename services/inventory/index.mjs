import { makePool, waitForDb } from '../common/db.mjs';
import { startHttp } from '../common/http.mjs';
import { createBus } from '../common/kafka.mjs';
import { makeLogger } from '../common/log.mjs';
import { makeRedis } from '../common/redis.mjs';
import { onShutdown } from '../common/run.mjs';
import { createInventory } from './service.mjs';

const log = makeLogger('inventory');
const pool = makePool(process.env.DATABASE_URL ?? 'postgres://meridian:meridian@localhost:5433/inventory');
await waitForDb(pool, log);
const redis = makeRedis(process.env.REDIS_URL ?? 'redis://localhost:6379');
const bus = await createBus({
  clientId: 'inventory',
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
  log,
});

const svc = createInventory({ pool, redis, bus, log });
await svc.migrate();
const server = startHttp({ port: Number(process.env.PORT ?? 4003), routes: svc.routes, log });
await bus.consume({ groupId: svc.groupId, topics: svc.topics, handler: svc.handle });
log.info('inventory ready', { groupId: svc.groupId });

onShutdown(log, () => bus.close(), () => redis.quit(), () => pool.end(), () => server.close());
