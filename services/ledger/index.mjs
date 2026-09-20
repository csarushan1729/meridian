import { makePool, waitForDb } from '../common/db.mjs';
import { startHttp } from '../common/http.mjs';
import { createBus } from '../common/kafka.mjs';
import { makeLogger } from '../common/log.mjs';
import { onShutdown } from '../common/run.mjs';
import { createLedger } from './service.mjs';

const log = makeLogger('ledger');
const pool = makePool(process.env.DATABASE_URL ?? 'postgres://meridian:meridian@localhost:5433/ledger');
await waitForDb(pool, log);
const bus = await createBus({
  clientId: 'ledger',
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
  log,
});

const svc = createLedger({ pool, log });
await svc.migrate();
const server = startHttp({ port: Number(process.env.PORT ?? 4005), routes: svc.routes, log });
await bus.consume({ groupId: svc.groupId, topics: svc.topics, handler: svc.handle });
log.info('ledger ready', { groupId: svc.groupId });

onShutdown(log, () => bus.close(), () => pool.end(), () => server.close());
