import { CircuitBreaker } from '../common/breaker.mjs';
import { makePool, waitForDb } from '../common/db.mjs';
import { startHttp } from '../common/http.mjs';
import { createBus } from '../common/kafka.mjs';
import { makeLogger } from '../common/log.mjs';
import { onShutdown } from '../common/run.mjs';
import { createOrders } from './service.mjs';

const log = makeLogger('orders');
const pool = makePool(process.env.DATABASE_URL ?? 'postgres://meridian:meridian@localhost:5433/orders');
await waitForDb(pool, log);

const bus = await createBus({
  clientId: 'orders',
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
  log,
});

const breaker = new CircuitBreaker({
  threshold: Number(process.env.BREAKER_THRESHOLD ?? 3),
  cooldownMs: Number(process.env.BREAKER_COOLDOWN_MS ?? 10000),
  onChange: (from, to) => log.warn('circuit breaker (payments) changed', { from, to }),
});

const svc = createOrders({
  pool,
  bus,
  log,
  breaker,
  reserveTimeoutMs: Number(process.env.RESERVE_TIMEOUT_MS ?? 5000),
  payTimeoutMs: Number(process.env.PAY_TIMEOUT_MS ?? 6000),
});
await svc.migrate();

const server = startHttp({ port: Number(process.env.PORT ?? 4002), routes: svc.routes, log });
await bus.consume({ groupId: svc.groupId, topics: svc.topics, handler: svc.handle });
const stopLoops = svc.startLoops();
log.info('orders ready', { groupId: svc.groupId, topics: svc.topics });

onShutdown(log, stopLoops, () => bus.close(), () => pool.end(), () => server.close());
