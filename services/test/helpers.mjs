import pg from 'pg';
import Redis from 'ioredis';
import { makePool } from '../common/db.mjs';
import { deadLetterEnvelope, deliver } from '../common/envelope.mjs';
import { makeRedis } from '../common/redis.mjs';
import { TOPIC_SPECS, TOPICS } from '../common/topics.mjs';
import { once } from 'node:events';
import { CircuitBreaker } from '../common/breaker.mjs';
import { isPaused } from '../common/chaos.mjs';
import { startHttp } from '../common/http.mjs';
import { Metrics, metricsRoute } from '../common/metrics.mjs';
import { createGateway } from '../gateway/service.mjs';
import { createInventory } from '../inventory/service.mjs';
import { createLedger } from '../ledger/service.mjs';
import { createOrders } from '../orders/service.mjs';
import { createPayments } from '../payments/service.mjs';

// The tests use REAL Postgres and REAL Redis (start them with: docker compose up -d redis postgres).
// Only Kafka is replaced by an in-memory bus, so tests are fast and deterministic.
// Redis DB 15 and databases named mt_* are used, so your dev data is not touched.
const ADMIN = process.env.TEST_PG_ADMIN_URL ?? 'postgres://meridian:meridian@localhost:5433/postgres';
const REDIS = process.env.TEST_REDIS_URL ?? 'redis://localhost:6379/15';

export async function infraAvailable() {
  const p = new pg.Pool({ connectionString: ADMIN, connectionTimeoutMillis: 1500 });
  const r = new Redis(REDIS, { lazyConnect: true, retryStrategy: () => null, maxRetriesPerRequest: 0, connectTimeout: 1500 });
  try {
    await p.query('select 1');
    await r.connect();
    await r.ping();
    return true;
  } catch {
    return false;
  } finally {
    await p.end().catch(() => {});
    r.disconnect();
  }
}

export const quietLog = { info() {}, warn() {}, error() {} };

/** In-memory stand-in for Kafka with the same interface as common/kafka.mjs. */
export class MemoryBus {
  constructor() {
    this.subs = [];
    this.queue = [];
    this.paused = new Set();
    this.held = new Map();
    this.dead = [];
    this.failNextSends = 0;
    this.offset = 0;
    this.counts = {}; // messages produced per topic (= Kafka log end offset)
    this.committed = {}; // messages handled per `group:topic` (= committed offset)
    this.events = []; // newest first
    this.producer = {
      send: (topic, key, envelope) => this.producer.sendBatch([{ topic, key, envelope }]),
      sendBatch: async (items) => {
        if (this.failNextSends > 0) {
          this.failNextSends -= 1;
          throw new Error('broker down');
        }
        for (const it of items) {
          this.queue.push({ topic: it.topic, key: it.key, raw: JSON.stringify(it.envelope) });
          this.counts[it.topic] = (this.counts[it.topic] ?? 0) + 1;
          this.events.unshift({ topic: it.topic, partition: 0, offset: this.counts[it.topic] - 1, key: it.key, type: it.envelope.type, ts: it.envelope.ts, traceId: it.envelope.traceId, payload: JSON.stringify(it.envelope.data).slice(0, 160) });
        }
      },
    };
  }

  async consume({ groupId, topics, handler, maxAttempts = 3 }) {
    this.subs.push({ groupId, topics, handler, maxAttempts });
  }

  injectRaw(topic, raw) {
    this.queue.push({ topic, key: 'x', raw });
  }

  /** Simulates `docker stop <service>`: messages pile up (lag) until resume. */
  pause(groupId) {
    this.paused.add(groupId);
    if (!this.held.has(groupId)) this.held.set(groupId, []);
  }

  async resume(groupId) {
    this.paused.delete(groupId);
    const msgs = this.held.get(groupId) ?? [];
    this.held.set(groupId, []);
    const sub = this.subs.find((s) => s.groupId === groupId);
    for (const m of msgs) await this.#deliver(sub, m);
  }

  heldTypes(groupId) {
    return (this.held.get(groupId) ?? []).map((m) => JSON.parse(m.raw).type);
  }

  async #deliver(sub, msg) {
    const ck = `${sub.groupId}:${msg.topic}`;
    this.committed[ck] = (this.committed[ck] ?? 0) + 1;
    await deliver({
      raw: msg.raw,
      meta: { topic: msg.topic, partition: 0, offset: String(this.offset++), groupId: sub.groupId },
      handler: sub.handler,
      maxAttempts: sub.maxAttempts,
      sleep: async () => {},
      onDead: async (d) => {
        this.dead.push(d);
        this.queue.push({ topic: TOPICS.DEAD_LETTER, key: 'dlq', raw: JSON.stringify(deadLetterEnvelope(d)) });
      },
    });
  }

  async drain(limit = 2000) {
    let n = 0;
    while (this.queue.length) {
      if (++n > limit) throw new Error('drain: too many messages, is there a loop?');
      const msg = this.queue.shift();
      for (const sub of this.subs) {
        if (!sub.topics.includes(msg.topic)) continue;
        if (this.paused.has(sub.groupId)) this.held.get(sub.groupId).push(msg);
        else await this.#deliver(sub, msg);
      }
    }
  }
}

/** What the control service needs from Kafka, answered from the MemoryBus. */
export function fakeKafkaProbe(bus) {
  return {
    async topicOffsets() {
      return TOPIC_SPECS.map((t) => ({ topic: t.topic, partitions: [{ partition: 0, low: 0, high: bus.counts[t.topic] ?? 0 }] }));
    },
    async groupOffsets(groupId, topics) {
      return topics.map((topic) => ({ topic, partitions: [{ partition: 0, offset: String(bus.committed[`${groupId}:${topic}`] ?? -1) }] }));
    },
    async clusterInfo() {
      return { clusterId: 'test-cluster', controller: 1, brokers: [{ nodeId: 1, host: 'kafka', port: 19092 }] };
    },
    recentEvents: () => bus.events.slice(0, 60),
    lastTypes: () => Object.fromEntries(bus.events.map((e) => [e.topic, e.type]).reverse()),
  };
}

/** A full system: 5 services, own database each, real Redis, in-memory bus. */
export async function makeWorld({ breaker, rateLimit = 1000 } = {}) {
  const admin = new pg.Pool({ connectionString: ADMIN });
  const pools = {};
  for (const name of ['orders', 'inventory', 'payments', 'ledger']) {
    const db = `mt_${name}`;
    await admin.query(`DROP DATABASE IF EXISTS ${db} WITH (FORCE)`);
    await admin.query(`CREATE DATABASE ${db}`);
    const u = new URL(ADMIN);
    u.pathname = `/${db}`;
    pools[name] = makePool(u.toString());
  }
  await admin.end();

  const redis = makeRedis(REDIS);
  await redis.flushdb();
  const bus = new MemoryBus();
  const log = quietLog;

  const orders = createOrders({
    pool: pools.orders,
    bus,
    log,
    breaker: breaker ?? new CircuitBreaker({ threshold: 3, cooldownMs: 60_000 }),
  });
  const inventory = createInventory({ pool: pools.inventory, redis, bus, log });
  const payments = createPayments({ pool: pools.payments, redis, bus, log, defaultDeclineRate: 0 });
  const ledger = createLedger({ pool: pools.ledger, log });
  const gateway = createGateway({ redis, bus, log, rateLimit });

  const metrics = { gateway: gateway.metrics };
  for (const [name, s] of [['orders', orders], ['inventory', inventory], ['payments', payments], ['ledger', ledger]]) {
    metrics[name] = new Metrics();
    await s.migrate();
    await bus.consume({ groupId: s.groupId, topics: s.topics, handler: metrics[name].wrap(s.handle) });
  }

  const q = async (name, sql, params = []) => (await pools[name].query(sql, params)).rows;

  return {
    pools,
    redis,
    bus,
    orders,
    inventory,
    payments,
    ledger,
    gateway,
    q,
    metrics,
    /** Run the whole system until nothing is left to do (outbox relay + bus). */
    async pump() {
      for (let i = 0; i < 50; i++) {
        await bus.drain();
        const sent = await orders.relayOnce();
        if (sent === 0 && bus.queue.length === 0) return;
      }
      throw new Error('pump: system never went quiet');
    },
    async order(id) {
      return (await q('orders', 'SELECT * FROM orders WHERE id = $1', [id]))[0];
    },
    async stock(sku) {
      return (await q('inventory', 'SELECT available, reserved FROM stock WHERE sku_id = $1', [sku]))[0];
    },
    /** Pretend time passed: every waiting saga step is now past its deadline. */
    async expireDeadlines() {
      await q('orders', `UPDATE orders SET step_deadline = now() - interval '1 second' WHERE step_deadline IS NOT NULL`);
    },
    async close() {
      for (const p of Object.values(pools)) await p.end().catch(() => {});
      await redis.quit().catch(() => {});
    },
  };
}


/** Start real HTTP servers (random ports) for the five services, like the containers do. */
export async function serveWorld(w) {
  const servers = {};
  const urls = {};
  const routesOf = {
    gateway: w.gateway.routes,
    orders: [...w.orders.routes, metricsRoute('orders', w.metrics.orders, async () => ({ paused: await isPaused(w.redis, 'orders') }))],
    inventory: [...w.inventory.routes, metricsRoute('inventory', w.metrics.inventory, async () => ({ paused: await isPaused(w.redis, 'inventory') }))],
    payments: [...w.payments.routes, metricsRoute('payments', w.metrics.payments, async () => ({ paused: await isPaused(w.redis, 'payments') }))],
    ledger: [...w.ledger.routes, metricsRoute('ledger', w.metrics.ledger, async () => ({ paused: await isPaused(w.redis, 'ledger') }))],
  };
  for (const [name, routes] of Object.entries(routesOf)) {
    const server = startHttp({ port: 0, routes, log: quietLog });
    await once(server, 'listening');
    servers[name] = server;
    urls[name] = `http://127.0.0.1:${server.address().port}`;
  }
  return {
    urls,
    servers,
    async close() {
      for (const s of Object.values(servers)) s.close();
    },
  };
}
