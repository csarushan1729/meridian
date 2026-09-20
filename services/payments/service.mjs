import { randomUUID } from 'node:crypto';
import { applyChaos, chaosNumber } from '../common/chaos.mjs';
import { tx } from '../common/db.mjs';
import { makeEnvelope } from '../common/envelope.mjs';
import { TOPICS } from '../common/topics.mjs';
import { SCHEMA } from './schema.mjs';

/**
 * Payments. The "payment provider" here is SIMULATED (no real card network):
 * a random decline rate you can change live in Redis (chaos:payments:declinerate).
 * Everything around it (idempotency, events, refunds) is real.
 */
export function createPayments({ pool, redis, bus, log, defaultDeclineRate = 0.05 }) {
  const send = (type, env, data) =>
    bus.producer.send(TOPICS.PAYMENTS_EVENTS, data.orderId, makeEnvelope({ type, traceId: env.traceId, data }));

  const answer = (env, p) => {
    if (p.status === 'CAPTURED') {
      return send('payment.captured', env, { orderId: p.order_id, amount: Number(p.amount), pspRef: p.psp_ref });
    }
    if (p.status === 'DECLINED') {
      return send('payment.failed', env, { orderId: p.order_id, reason: 'card_declined', kind: 'declined' });
    }
    return send('payment.refunded', env, { orderId: p.order_id, amount: Number(p.amount) });
  };

  async function capture(env) {
    const { orderId, amount } = env.data;
    await applyChaos(redis, 'payments'); // fake latency / random crash
    const rate = await chaosNumber(redis, 'payments', 'declinerate', defaultDeclineRate);
    const declined = Math.random() < rate;

    const payment = await tx(pool, async (c) => {
      const ins = await c.query(
        `INSERT INTO payments (order_id, amount, status, psp_ref) VALUES ($1, $2, $3, $4)
         ON CONFLICT (order_id) DO NOTHING RETURNING *`,
        [orderId, amount, declined ? 'DECLINED' : 'CAPTURED', declined ? null : `psp_${randomUUID().slice(0, 10)}`],
      );
      // Already processed before? Use the stored result, not a new coin flip.
      return ins.rows[0] ?? (await c.query('SELECT * FROM payments WHERE order_id = $1', [orderId])).rows[0];
    });
    log.info('capture', { orderId, traceId: env.traceId, status: payment.status });
    await answer(env, payment);
  }

  async function refund(env) {
    const { orderId } = env.data;
    await applyChaos(redis, 'payments');
    const payment = await tx(pool, async (c) => {
      const cur = (await c.query('SELECT * FROM payments WHERE order_id = $1 FOR UPDATE', [orderId])).rows[0];
      if (!cur) return null;
      if (cur.status === 'CAPTURED') {
        return (
          await c.query(`UPDATE payments SET status = 'REFUNDED', updated_at = now() WHERE order_id = $1 RETURNING *`, [
            orderId,
          ])
        ).rows[0];
      }
      return cur;
    });
    if (!payment || payment.status === 'DECLINED') {
      log.warn('refund ignored, nothing was captured', { orderId, traceId: env.traceId });
      return;
    }
    log.info('refund', { orderId, traceId: env.traceId });
    await answer(env, payment);
  }

  const handlers = { 'payment.capture': capture, 'payment.refund': refund };
  async function handle(env) {
    const h = handlers[env.type];
    if (h) await h(env);
  }

  const routes = [
    ['GET', '/health', async () => ({ body: { ok: true, service: 'payments' } })],
    [
      'GET',
      '/summary',
      async () => {
        const { rows } = await pool.query(
          'SELECT status, count(*)::int AS n, coalesce(sum(amount),0)::bigint AS total FROM payments GROUP BY status',
        );
        return { body: { byStatus: rows.map((r) => ({ status: r.status, count: r.n, total: Number(r.total) })) } };
      },
    ],
  ];

  return {
    migrate: () => pool.query(SCHEMA),
    handle,
    routes,
    topics: [TOPICS.PAYMENTS_COMMANDS],
    groupId: 'payments',
  };
}
