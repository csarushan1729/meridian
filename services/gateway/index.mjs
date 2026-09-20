import { startHttp } from '../common/http.mjs';
import { createBus } from '../common/kafka.mjs';
import { makeLogger } from '../common/log.mjs';
import { makeRedis } from '../common/redis.mjs';
import { onShutdown } from '../common/run.mjs';
import { createGateway } from './service.mjs';

const log = makeLogger('gateway');
const redis = makeRedis(process.env.REDIS_URL ?? 'redis://localhost:6379');
const bus = await createBus({
  clientId: 'gateway',
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
  log,
});

const gw = createGateway({
  redis,
  bus,
  log,
  ordersUrl: process.env.ORDERS_URL ?? 'http://localhost:4002',
  rateLimit: Number(process.env.RATE_LIMIT ?? 120),
});
const server = startHttp({ port: Number(process.env.PORT ?? 4000), routes: gw.routes, log });
log.info('gateway ready');

onShutdown(log, () => bus.close(), () => redis.quit(), () => server.close());
