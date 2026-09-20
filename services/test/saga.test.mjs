import assert from 'node:assert/strict';
import { after, before, describe, it } from 'node:test';
import { CircuitBreaker } from '../common/breaker.mjs';
import { withLock } from '../common/redis.mjs';
import { TOPICS } from '../common/topics.mjs';
import { sleep } from '../common/util.mjs';
import { infraAvailable, makeWorld } from './helpers.mjs';

const infra = await infraAvailable();

describe('order saga (real Postgres + real Redis, in-memory Kafka)', { skip: !infra && 'start redis + postgres: docker compose up -d redis postgres' }, () => {
  let w;
  // Each test gets a fresh world (fresh databases). Close the old one first, because
  // dropping its databases would kill its open connections.
  const open = async (opts) => {
    await w?.close();
    w = await makeWorld(opts);
    return w;
  };
  after(async () => w?.close());

  it('happy path: reserve -> pay -> confirmed, ledger balanced, same idempotency key = same order', async () => {
    await open();
    const a = await w.gateway.placeOrder({ skuId: 'halo-bottle', qty: 2, idemKey: 'key-1' });
    const b = await w.gateway.placeOrder({ skuId: 'halo-bottle', qty: 2, idemKey: 'key-1' });
    assert.equal(b.replayed, true);
    assert.equal(b.orderId, a.orderId);
    await w.pump();

    assert.equal((await w.order(a.orderId)).status, 'CONFIRMED');
    assert.equal((await w.q('orders', 'SELECT count(*)::int n FROM orders'))[0].n, 1);
    assert.deepEqual(await w.stock('halo-bottle'), { available: 1198, reserved: 2 });
    assert.equal((await w.q('payments', 'SELECT status FROM payments'))[0].status, 'CAPTURED');
    const l = await w.q('ledger', 'SELECT sum(debit)::int d, sum(credit)::int c, count(*)::int n FROM ledger_entries');
    assert.deepEqual(l[0], { d: 6800, c: 6800, n: 2 }); // 2 x 3400
  });

  it('out of stock: order is cancelled, no payment is attempted', async () => {
    await open();
    await w.q('inventory', `UPDATE stock SET available = 1 WHERE sku_id = 'nimbus-down'`);
    const o = await w.gateway.placeOrder({ skuId: 'nimbus-down', qty: 2, idemKey: 'k' });
    await w.pump();
    const row = await w.order(o.orderId);
    assert.equal(row.status, 'CANCELLED');
    assert.equal(row.reason, 'out_of_stock');
    assert.equal((await w.q('payments', 'SELECT count(*)::int n FROM payments'))[0].n, 0);
  });

  it('card declined: saga compensates and stock comes back', async () => {
    await open();
    await w.redis.set('chaos:payments:declinerate', '1');
    const o = await w.gateway.placeOrder({ skuId: 'ridge-boot', qty: 1, idemKey: 'k' });
    await w.pump();
    const row = await w.order(o.orderId);
    assert.equal(row.status, 'CANCELLED');
    assert.equal(row.reason, 'card_declined');
    assert.deepEqual(await w.stock('ridge-boot'), { available: 140, reserved: 0 });
    assert.equal((await w.q('ledger', 'SELECT count(*)::int n FROM ledger_entries'))[0].n, 0);
    assert.equal(w.orders.breaker.state, 'closed'); // a decline is not an outage
  });

  it('payments down (docker stop payments): timeout -> compensate -> late capture is refunded', async () => {
    await open();
    w.bus.pause('payments'); // payments container is stopped, its commands pile up in Kafka
    const o = await w.gateway.placeOrder({ skuId: 'lattice-knit', qty: 3, idemKey: 'k' });
    await w.pump();
    assert.equal((await w.order(o.orderId)).status, 'PAYING');
    assert.deepEqual(await w.stock('lattice-knit'), { available: 797, reserved: 3 });

    await w.expireDeadlines();
    assert.equal(await w.orders.sweepTimeouts(), 1);
    await w.pump();
    const row = await w.order(o.orderId);
    assert.equal(row.status, 'CANCELLED');
    assert.equal(row.reason, 'payment_timeout');
    assert.deepEqual(await w.stock('lattice-knit'), { available: 800, reserved: 0 }); // stock released

    // payments comes back and processes the old command: money is taken for a cancelled order...
    await w.bus.resume('payments');
    await w.pump();
    // ...so the saga refunds it.
    assert.equal((await w.q('payments', 'SELECT status FROM payments'))[0].status, 'REFUNDED');
    assert.equal((await w.order(o.orderId)).refund_requested, true);
    const acc = await w.q('ledger', 'SELECT account, sum(debit)::int d, sum(credit)::int c FROM ledger_entries GROUP BY account ORDER BY account');
    assert.deepEqual(acc, [
      { account: 'cash', d: 28800, c: 28800 }, // in and out: net zero
      { account: 'revenue', d: 28800, c: 28800 },
    ]);
  });

  it('circuit breaker opens after 3 payment timeouts, then orders fail fast without calling payments', async () => {
    await open({ breaker: new CircuitBreaker({ threshold: 3, cooldownMs: 60_000 }) });
    w.bus.pause('payments');
    for (let i = 0; i < 3; i++) await w.gateway.placeOrder({ skuId: 'halo-bottle', qty: 1, idemKey: `k${i}` });
    await w.pump();
    await w.expireDeadlines();
    assert.equal(await w.orders.sweepTimeouts(), 3);
    assert.equal(w.orders.breaker.state, 'open');
    await w.pump();

    const fast = await w.gateway.placeOrder({ skuId: 'halo-bottle', qty: 1, idemKey: 'k-fast' });
    await w.pump();
    const row = await w.order(fast.orderId);
    assert.equal(row.status, 'CANCELLED');
    assert.equal(row.reason, 'circuit_open:payments');
    assert.equal(w.bus.heldTypes('payments').filter((t) => t === 'payment.capture').length, 3); // the 4th never reached payments
    assert.deepEqual(await w.stock('halo-bottle'), { available: 1200, reserved: 0 });
  });

  it('poison message and crashing handler go to the dead-letter topic', async () => {
    await open();
    w.bus.injectRaw(TOPICS.INVENTORY_COMMANDS, 'this is not json');
    await w.pump();
    assert.equal(w.bus.dead.length, 1);
    assert.match(w.bus.dead[0].reason, /^poison/);

    await w.redis.set('chaos:inventory:crashrate', '1');
    const o = await w.gateway.placeOrder({ skuId: 'halo-bottle', qty: 1, idemKey: 'k' });
    await w.pump();
    const crashed = w.bus.dead.find((d) => d.env?.type === 'inventory.reserve');
    assert.equal(crashed.attempts, 3); // retried 3 times, then dead letter
    assert.match(crashed.reason, /chaos/);
    // the saga does not hang forever: the timeout cancels the order
    assert.equal((await w.order(o.orderId)).status, 'RESERVING');
    await w.expireDeadlines();
    await w.orders.sweepTimeouts();
    assert.equal((await w.order(o.orderId)).reason, 'inventory_timeout');
  });

  it('outbox: a Kafka outage does not lose messages', async () => {
    await open();
    const o = await w.gateway.placeOrder({ skuId: 'halo-bottle', qty: 1, idemKey: 'k' });
    await w.bus.drain(); // orders handles order.create, writes state + outbox in one transaction
    w.bus.failNextSends = 1;
    await assert.rejects(w.orders.relayOnce(), /broker down/);
    assert.equal((await w.q('orders', 'SELECT count(*)::int n FROM outbox WHERE sent_at IS NULL'))[0].n, 2);
    await w.pump(); // broker is back, relay sends the saved messages
    assert.equal((await w.order(o.orderId)).status, 'CONFIRMED');
  });

  it('gateway rate limit: 4th request in the window gets 429', async () => {
    await open({ rateLimit: 3 });
    for (let i = 0; i < 3; i++) await w.gateway.checkRateLimit('9.9.9.9');
    await assert.rejects(w.gateway.checkRateLimit('9.9.9.9'), (e) => e.status === 429);
    await w.gateway.checkRateLimit('8.8.8.8'); // another client is not affected
  });

  it('redis lock: one holder at a time, and never deletes a lock that is not yours', async () => {
    await open();
    let active = 0;
    let max = 0;
    await Promise.all(
      [1, 2, 3, 4, 5].map(() =>
        withLock(w.redis, 'lock:t', 2000, async () => {
          active += 1;
          max = Math.max(max, active);
          await sleep(15);
          active -= 1;
        }),
      ),
    );
    assert.equal(max, 1);

    const slow = withLock(w.redis, 'lock:x', 50, () => sleep(150)); // lock expires while we still work
    await sleep(80);
    assert.equal(await w.redis.set('lock:x', 'someone-else', 'PX', 1000, 'NX'), 'OK');
    await slow;
    assert.equal(await w.redis.get('lock:x'), 'someone-else'); // our unlock script did not delete it
  });

  it('concurrent orders never oversell', async () => {
    await open();
    await w.q('inventory', `UPDATE stock SET available = 5 WHERE sku_id = 'nimbus-down'`);
    // 12 different orders for the last 5 pieces; inventory handlers run at the same time
    const ids = [];
    for (let i = 0; i < 12; i++) ids.push((await w.gateway.placeOrder({ skuId: 'nimbus-down', qty: 1, idemKey: `c${i}` })).orderId);
    await w.orders.relayOnce();
    await w.bus.drain();
    await w.orders.relayOnce(); // sends 12 inventory.reserve commands
    const cmds = w.bus.queue.splice(0).filter((m) => m.topic === TOPICS.INVENTORY_COMMANDS);
    const sub = w.bus.subs.find((s) => s.groupId === 'inventory');
    await Promise.all(cmds.map((m) => sub.handler(JSON.parse(m.raw))));
    const s = await w.stock('nimbus-down');
    assert.deepEqual(s, { available: 0, reserved: 5 });
    const r = await w.q('inventory', `SELECT status, count(*)::int n FROM reservations GROUP BY status ORDER BY status`);
    assert.deepEqual(r, [{ status: 'REJECTED', n: 7 }, { status: 'RESERVED', n: 5 }]);
    assert.equal(ids.length, 12);
  });
});
