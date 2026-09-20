import { startHttp } from '../common/http.mjs';
import { makeLogger } from '../common/log.mjs';
import { makeRedis } from '../common/redis.mjs';
import { onShutdown } from '../common/run.mjs';
import { createControl } from './service.mjs';
import { createKafkaProbe } from './kafka-probe.mjs';

const log = makeLogger('control');
const redis = makeRedis(process.env.REDIS_URL ?? 'redis://localhost:6379');
const kafka = await createKafkaProbe({
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
  log,
});

const control = createControl({
  redis,
  kafka,
  log,
  urls: {
    gateway: process.env.GATEWAY_URL ?? 'http://localhost:4000',
    orders: process.env.ORDERS_URL ?? 'http://localhost:4002',
    inventory: process.env.INVENTORY_URL ?? 'http://localhost:4003',
    payments: process.env.PAYMENTS_URL ?? 'http://localhost:4004',
    ledger: process.env.LEDGER_URL ?? 'http://localhost:4005',
  },
  defaultDeclineRate: Number(process.env.PAYMENT_DECLINE_RATE ?? 0.05),
});

const server = startHttp({ port: Number(process.env.PORT ?? 4010), routes: control.routes, log });
const stop = control.start();
log.info('control ready');

onShutdown(log, stop, () => kafka.close(), () => redis.quit(), () => server.close());
