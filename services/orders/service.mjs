import { CircuitBreaker } from '../common/breaker.mjs';
import { skuById } from '../common/catalog.mjs';
import { tx } from '../common/db.mjs';
import { makeEnvelope } from '../common/envelope.mjs';
import { TOPICS } from '../common/topics.mjs';
import { SCHEMA } from './schema.mjs';

/**
 * Order orchestrator (the saga).
 *
 *   order.create  -> RESERVING --inventory.reserved--> PAYING --payment.captured--> CONFIRMED
 *                        |                                |
 *                        | inventory.rejected / timeout   | payment.failed / timeout / breaker open
 *                        v                                v
 *                    CANCELLED  <---- compensate: release stock (and refund if a late capture arrives)
 *
 * Rules that make it safe:
 *  - Every state change is guarded by the current status and done under a row lock,
 *    so redelivered messages do nothing (idempotent).
 *  - Messages we send are written to the outbox in the same DB transaction.
 */
export function createOrders({
  pool,
  bus,
  log,
  breaker = new CircuitBreaker(),
  reserveTimeoutMs = 5000,
  payTimeoutMs = 6000,
}) {
  const emit = (c, topic, key, type, traceId, data) =>
    c.query('INSERT INTO outbox (topic, key, envelope) VALUES ($1, $2, $3)', [
      topic,
      key,
      JSON.stringify(makeEnvelope({ type, traceId, data })),
    ]);

  const step = (c, orderId, name, status, detail = null) =>
    c.query('INSERT INTO order_steps (order_id, name, status, detail) VALUES ($1, $2, $3, $4)', [
      orderId,
      name,
      status,
      detail,
    ]);

  async function lockOrder(c, orderId) {
    const { rows } = await c.query('SELECT * FROM orders WHERE id = $1 FOR UPDATE', [orderId]);
    return rows[0];
  }

  async function cancel(c, order, reason, { release }) {
    await c.query(
      `UPDATE orders SET status = 'CANCELLED', reason = $2, step_deadline = NULL, updated_at = now() WHERE id = $1`,
      [order.id, reason],
    );
    await step(c, order.id, 'saga.compensate', 'warn', reason);
    if (release) {
      await emit(c, TOPICS.INVENTORY_COMMANDS, order.id, 'inventory.release', order.trace_id, { orderId: order.id });
      await step(c, order.id, 'inventory.release.requested', 'ok');
    }
    await emit(c, TOPICS.ORDERS_EVENTS, order.id, 'order.cancelled', order.trace_id, { orderId: order.id, reason });
    log.warn('order cancelled', { orderId: order.id, traceId: order.trace_id, reason });
  }

  // ---------- event handlers ----------

  async function onCreate(env) {
    const { orderId, skuId, qty, idempotencyKey } = env.data;
    const sku = skuById(skuId);
    if (!sku) throw new Error(`unknown sku ${skuId}`);
    if (!Number.isInteger(qty) || qty < 1) throw new Error(`bad qty ${qty}`);
    const amount = sku.price * qty; // orders computes the price itself, never trusts the caller

    await tx(pool, async (c) => {
      const ins = await c.query(
        `INSERT INTO orders (id, sku_id, qty, amount, status, idempotency_key, trace_id, step_deadline)
         VALUES ($1, $2, $3, $4, 'RESERVING', $5, $6, now() + ($7::int * interval '1 millisecond'))
         ON CONFLICT (id) DO NOTHING RETURNING id`,
        [orderId, skuId, qty, amount, idempotencyKey ?? null, env.traceId, reserveTimeoutMs],
      );
      if (!ins.rowCount) return; // same command delivered twice
      await step(c, orderId, 'order.created', 'ok', `${qty} x ${skuId}`);
      await emit(c, TOPICS.ORDERS_EVENTS, orderId, 'order.created', env.traceId, { orderId, skuId, qty, amount });
      await emit(c, TOPICS.INVENTORY_COMMANDS, orderId, 'inventory.reserve', env.traceId, { orderId, skuId, qty });
      await step(c, orderId, 'inventory.reserve.requested', 'ok');
    });
    log.info('order created', { orderId, traceId: env.traceId, skuId, qty, amount });
  }

  async function onReserved(env) {
    const { orderId } = env.data;
    await tx(pool, async (c) => {
      const o = await lockOrder(c, orderId);
      if (!o) return log.warn('reserved for unknown order', { orderId });
      if (o.status === 'RESERVING') {
        await step(c, orderId, 'inventory.reserved', 'ok');
        if (!breaker.allow()) {
          // Circuit is open: do not even try payments. Fail fast and give the stock back.
          await cancel(c, o, 'circuit_open:payments', { release: true });
          return;
        }
        await c.query(
          `UPDATE orders SET status = 'PAYING', updated_at = now(),
                  step_deadline = now() + ($2::int * interval '1 millisecond') WHERE id = $1`,
          [orderId, payTimeoutMs],
        );
        await emit(c, TOPICS.PAYMENTS_COMMANDS, orderId, 'payment.capture', o.trace_id, {
          orderId,
          amount: Number(o.amount),
        });
        await step(c, orderId, 'payment.capture.requested', 'ok');
      } else if (o.status === 'CANCELLED') {
        // Stock was reserved AFTER we gave up on this order (timeout). Give it back.
        await step(c, orderId, 'inventory.reserved.late', 'warn', 'order already cancelled, releasing stock');
        await emit(c, TOPICS.INVENTORY_COMMANDS, orderId, 'inventory.release', o.trace_id, { orderId });
      }
    });
  }

  async function onRejected(env) {
    const { orderId, reason } = env.data;
    await tx(pool, async (c) => {
      const o = await lockOrder(c, orderId);
      if (o?.status === 'RESERVING') {
        await step(c, orderId, 'inventory.rejected', 'warn', reason);
        await cancel(c, o, reason ?? 'inventory_rejected', { release: false });
      }
    });
  }

  async function onCaptured(env) {
    const { orderId } = env.data;
    await tx(pool, async (c) => {
      const o = await lockOrder(c, orderId);
      if (!o) return log.warn('payment for unknown order', { orderId });
      if (o.status === 'PAYING') {
        breaker.success();
        await c.query(
          `UPDATE orders SET status = 'CONFIRMED', reason = NULL, step_deadline = NULL, updated_at = now() WHERE id = $1`,
          [orderId],
        );
        await step(c, orderId, 'payment.captured', 'ok');
        await emit(c, TOPICS.ORDERS_EVENTS, orderId, 'order.confirmed', o.trace_id, {
          orderId,
          amount: Number(o.amount),
        });
        log.info('order confirmed', { orderId, traceId: o.trace_id });
      } else if (o.status === 'CANCELLED' && !o.refund_requested) {
        // Money was taken for an order we already cancelled (payments was slow or down).
        await c.query('UPDATE orders SET refund_requested = true, updated_at = now() WHERE id = $1', [orderId]);
        await step(c, orderId, 'payment.captured.late', 'warn', 'order already cancelled, refunding');
        await emit(c, TOPICS.PAYMENTS_COMMANDS, orderId, 'payment.refund', o.trace_id, {
          orderId,
          amount: Number(o.amount),
        });
        await step(c, orderId, 'payment.refund.requested', 'ok');
        log.warn('late capture, refund requested', { orderId, traceId: o.trace_id });
      }
    });
  }

  async function onFailed(env) {
    const { orderId, reason } = env.data;
    await tx(pool, async (c) => {
      const o = await lockOrder(c, orderId);
      if (o?.status === 'PAYING') {
        breaker.success(); // a decline is a normal answer: payments is alive
        await step(c, orderId, 'payment.failed', 'warn', reason);
        await cancel(c, o, reason ?? 'payment_failed', { release: true });
      }
    });
  }

  const info = (name) => async (env) => {
    await pool.query('INSERT INTO order_steps (order_id, name, status) VALUES ($1, $2, $3)', [
      env.data.orderId,
      name,
      'ok',
    ]);
  };

  const handlers = {
    'order.create': onCreate,
    'inventory.reserved': onReserved,
    'inventory.rejected': onRejected,
    'inventory.released': info('inventory.released'),
    'payment.captured': onCaptured,
    'payment.failed': onFailed,
    'payment.refunded': info('payment.refunded'),
  };

  async function handle(env) {
    const h = handlers[env.type];
    if (h) await h(env);
  }

  // ---------- timers ----------

  /** Orders that waited too long for the next step are cancelled and compensated. */
  async function sweepTimeouts() {
    const { rows } = await pool.query(
      `SELECT id FROM orders WHERE status IN ('RESERVING','PAYING') AND step_deadline < now()
       ORDER BY step_deadline LIMIT 50`,
    );
    let count = 0;
    for (const { id } of rows) {
      const did = await tx(pool, async (c) => {
        const { rows: r } = await c.query(
          `SELECT * FROM orders WHERE id = $1 AND status IN ('RESERVING','PAYING') AND step_deadline < now() FOR UPDATE`,
          [id],
        );
        const o = r[0];
        if (!o) return false;
        if (o.status === 'RESERVING') {
          await cancel(c, o, 'inventory_timeout', { release: true });
        } else {
          breaker.fail(); // payments did not answer in time
          await cancel(c, o, 'payment_timeout', { release: true });
        }
        return true;
      });
      if (did) count += 1;
    }
    return count;
  }

  /** Publish unsent outbox rows to Kafka, in order. Safe to call from many replicas. */
  async function relayOnce() {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const { rows } = await client.query(
        `SELECT id, topic, key, envelope FROM outbox WHERE sent_at IS NULL
         ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED`,
      );
      if (!rows.length) {
        await client.query('COMMIT');
        return 0;
      }
      await bus.producer.sendBatch(rows.map((r) => ({ topic: r.topic, key: r.key, envelope: r.envelope })));
      await client.query('UPDATE outbox SET sent_at = now() WHERE id = ANY($1::bigint[])', [rows.map((r) => r.id)]);
      await client.query('COMMIT');
      return rows.length;
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      throw err; // rows stay unsent and are retried on the next loop
    } finally {
      client.release();
    }
  }

  function startLoops() {
    const loop = (name, ms, fn) => {
      let busy = false;
      const t = setInterval(async () => {
        if (busy) return;
        busy = true;
        try {
          await fn();
        } catch (err) {
          log.error(`${name} failed`, { err: err.message });
        } finally {
          busy = false;
        }
      }, ms);
      return () => clearInterval(t);
    };
    const stops = [loop('outbox relay', 150, relayOnce), loop('timeout sweep', 1000, sweepTimeouts)];
    return () => stops.forEach((s) => s());
  }

  // ---------- read API (used by the gateway and the dashboard) ----------

  const toOrder = (r) => ({
    id: r.id,
    skuId: r.sku_id,
    qty: r.qty,
    amount: Number(r.amount),
    status: r.status,
    reason: r.reason,
    traceId: r.trace_id,
    createdAt: r.created_at,
    updatedAt: r.updated_at,
  });

  const routes = [
    ['GET', '/health', async () => ({ body: { ok: true, service: 'orders' } })],
    [
      'GET',
      '/orders',
      async ({ query }) => {
        const limit = Math.min(Number(query.get('limit')) || 50, 200);
        const status = query.get('status');
        const { rows } = status
          ? await pool.query('SELECT * FROM orders WHERE status = $2 ORDER BY created_at DESC LIMIT $1', [limit, status])
          : await pool.query('SELECT * FROM orders ORDER BY created_at DESC LIMIT $1', [limit]);
        return { body: { orders: rows.map(toOrder) } };
      },
    ],
    [
      'GET',
      '/orders/:id',
      async ({ params }) => {
        const { rows } = await pool.query('SELECT * FROM orders WHERE id = $1', [params.id]);
        if (!rows[0]) return { status: 404, body: { error: 'order_not_found' } };
        const steps = await pool.query(
          'SELECT name, status, detail, at FROM order_steps WHERE order_id = $1 ORDER BY id',
          [params.id],
        );
        return { body: { ...toOrder(rows[0]), refundRequested: rows[0].refund_requested, steps: steps.rows } };
      },
    ],
    [
      'GET',
      '/stats',
      async () => {
        const byStatus = await pool.query('SELECT status, count(*)::int AS n FROM orders GROUP BY status');
        const outbox = await pool.query('SELECT count(*)::int AS n FROM outbox WHERE sent_at IS NULL');
        return {
          body: {
            byStatus: Object.fromEntries(byStatus.rows.map((r) => [r.status, r.n])),
            outboxUnsent: outbox.rows[0].n,
            breaker: breaker.snapshot(),
          },
        };
      },
    ],
    ['GET', '/breaker', async () => ({ body: breaker.snapshot() })],
  ];

  return {
    migrate: () => pool.query(SCHEMA),
    handle,
    sweepTimeouts,
    relayOnce,
    startLoops,
    routes,
    breaker,
    topics: [TOPICS.ORDERS_COMMANDS, TOPICS.INVENTORY_EVENTS, TOPICS.PAYMENTS_EVENTS],
    groupId: 'orders-saga',
  };
}
