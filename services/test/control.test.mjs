import assert from 'node:assert/strict';
import { after, describe, it } from 'node:test';
import { createControl } from '../control/service.mjs';
import { fakeKafkaProbe, infraAvailable, makeWorld, quietLog, serveWorld } from './helpers.mjs';

const infra = await infraAvailable();

describe('control service (dashboard backend)', { skip: !infra && 'start redis + postgres: docker compose up -d redis postgres' }, () => {
  let w;
  let srv;
  const open = async (opts) => {
    await srv?.close();
    await w?.close();
    w = await makeWorld(opts);
    srv = await serveWorld(w);
    return createControl({ redis: w.redis, kafka: fakeKafkaProbe(w.bus), urls: srv.urls, log: quietLog });
  };
  after(async () => {
    await srv?.close();
    await w?.close();
  });

  it('builds a snapshot from real service data', async () => {
    const control = await open();
    for (let i = 0; i < 4; i++) await w.gateway.placeOrder({ skuId: 'halo-bottle', qty: 1, idemKey: `k${i}` });
    await w.pump();
    const raw = await control.poll();

    assert.deepEqual(Object.values(raw.services).map((s) => s.up), [true, true, true, true, true]);
    assert.equal(raw.orders.length, 4);
    assert.ok(raw.orders[0].steps.length >= 4, 'orders include their saga steps');
    assert.equal(raw.ordersStats.recent.confirmed, 4);
    assert.ok(raw.ordersStats.recent.latencyMs.p99 >= 0);
    assert.equal(raw.ordersStats.sagasInFlight, 0);
    assert.equal(raw.ledger.summary.balanced, true);
    assert.equal(raw.stock.find((s) => s.sku_id === 'halo-bottle').reserved, 4);
    assert.equal(raw.breaker.state, 'closed');

    const topic = (name) => raw.kafka.topics.find((t) => t.topic === name);
    assert.ok(topic('orders.commands').produced >= 4);
    assert.ok(topic('payments.events').produced >= 4);
    assert.equal(raw.kafka.cluster.clusterId, 'test-cluster');
    for (const c of raw.kafka.consumers) assert.equal(c.lag, 0, `${c.groupId} lag on ${c.topic}`);
    assert.ok(raw.kafka.recentEvents.length > 0);
    assert.ok(raw.redis.idempotencyKeys >= 4);
    assert.ok(raw.redis.keyList.some((k) => k.kind === 'idempotency' && k.ttl > 0));
  });

  it('chaos switches are written to Redis and read back', async () => {
    const control = await open();
    const c = await control.setChaos({
      declineRate: 0.5,
      inventoryCrashRate: 0.25,
      latencyMs: { payments: 1500, orders: 999 }, // orders has no latency switch: ignored
      paused: { payments: true, gateway: true }, // gateway cannot be paused: ignored
      traffic: 999, // capped at 50
    });
    assert.equal(c.declineRate, 0.5);
    assert.equal(c.inventoryCrashRate, 0.25);
    assert.equal(c.latencyMs.payments, 1500);
    assert.equal(c.latencyMs.orders, undefined);
    assert.equal(c.paused.payments, true);
    assert.equal(c.paused.gateway, undefined);
    assert.equal(c.traffic, 50);
    assert.equal(await w.redis.get('chaos:payments:declinerate'), '0.5');
    assert.equal(await w.redis.get('chaos:payments:paused'), '1');

    const off = await control.setChaos({ paused: { payments: false }, inventoryCrashRate: 0, latencyMs: { payments: 0 } });
    assert.equal(off.paused.payments, false);
    assert.equal(await w.redis.get('chaos:payments:paused'), null);
    assert.equal(await w.redis.get('chaos:inventory:crashrate'), null);
  });

  it('payments paused: lag grows, breaker incident is reported', async () => {
    const control = await open();
    await control.poll(); // first poll = baseline
    w.bus.pause('payments');
    await control.setChaos({ paused: { payments: true } });
    for (let i = 0; i < 3; i++) await w.gateway.placeOrder({ skuId: 'halo-bottle', qty: 1, idemKey: `p${i}` });
    await w.pump();

    let raw = await control.poll();
    const lag = raw.kafka.consumers.find((c) => c.groupId === 'payments').lag;
    assert.equal(lag, 3, 'the 3 payment.capture commands are waiting in Kafka');
    assert.equal(raw.services.payments.paused, true);
    assert.ok(raw.incidents.some((i) => i.title === 'payments paused'));
    assert.equal(raw.ordersStats.sagasInFlight, 3);

    await w.expireDeadlines();
    await w.orders.sweepTimeouts();
    await w.pump();
    raw = await control.poll();
    assert.equal(raw.breaker.state, 'open');
    assert.ok(raw.incidents.some((i) => i.pattern === 'circuit-open' && i.severity === 'crit'));
    assert.ok(raw.incidents.some((i) => i.pattern === 'compensation'));
    assert.ok(raw.ordersStats.recent.systemFailures >= 3);
    assert.ok(raw.ordersStats.sysErrRate > 0);
  });

  it('a stopped service shows as down', async () => {
    const control = await open();
    await control.poll();
    srv.servers.payments.close();
    srv.servers.payments.closeAllConnections?.();
    const raw = await control.poll();
    assert.equal(raw.services.payments.up, false);
    assert.equal(raw.services.orders.up, true);
    assert.ok(raw.incidents.some((i) => i.title === 'payments is not answering'));
  });

  it('traffic generator sends orders through the real gateway; restock works', async () => {
    const control = await open();
    await control.setChaos({ traffic: 5 });
    await control.trafficTick();
    await w.pump();
    assert.equal((await w.q('orders', 'SELECT count(*)::int n FROM orders'))[0].n, 5);

    await w.q('inventory', `UPDATE stock SET available = 3 WHERE sku_id = 'ridge-boot'`);
    await control.restock();
    assert.equal((await w.stock('ridge-boot')).available, 140);

    const placed = await control.placeOrder({ skuId: 'halo-bottle', qty: 2 });
    assert.equal(placed.status, 202);
    assert.match(placed.body.orderId, /^ord_/);
  });
});
