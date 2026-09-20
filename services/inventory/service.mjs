import { SKUS } from '../common/catalog.mjs';
import { applyChaos } from '../common/chaos.mjs';
import { tx } from '../common/db.mjs';
import { makeEnvelope } from '../common/envelope.mjs';
import { withLock } from '../common/redis.mjs';
import { TOPICS } from '../common/topics.mjs';
import { SCHEMA } from './schema.mjs';

/**
 * Inventory: reserve and release stock.
 *
 * Two layers protect the stock number:
 *  1. Redis lock per SKU (SET NX PX) - one worker at a time per SKU, even with many replicas.
 *  2. The SQL guard "WHERE available >= qty" plus CHECK (available >= 0) - correct even if the lock fails.
 *
 * Idempotent: one row per order in `reservations`. If the same command arrives again,
 * we do not touch stock again, we just send the same answer again.
 */
export function createInventory({ pool, redis, bus, log }) {
  async function migrate() {
    await pool.query(SCHEMA);
    for (const s of SKUS) {
      await pool.query(
        `INSERT INTO stock (sku_id, name, available) VALUES ($1, $2, $3) ON CONFLICT (sku_id) DO NOTHING`,
        [s.id, s.name, s.stock],
      );
    }
  }

  const answer = (env, reservation) => {
    const orderId = reservation.order_id;
    if (reservation.status === 'RESERVED') {
      return send('inventory.reserved', env, { orderId, skuId: reservation.sku_id, qty: reservation.qty });
    }
    if (reservation.status === 'REJECTED') {
      return send('inventory.rejected', env, { orderId, reason: reservation.reason });
    }
    return send('inventory.released', env, { orderId });
  };

  const send = (type, env, data) =>
    bus.producer.send(TOPICS.INVENTORY_EVENTS, data.orderId, makeEnvelope({ type, traceId: env.traceId, data }));

  async function reserve(env) {
    const { orderId, skuId, qty } = env.data;
    await applyChaos(redis, 'inventory');

    const existing = (await pool.query('SELECT * FROM reservations WHERE order_id = $1', [orderId])).rows[0];
    if (existing) return answer(env, existing);

    const reservation = await withLock(redis, `lock:sku:${skuId}`, 3000, () =>
      tx(pool, async (c) => {
        const again = (await c.query('SELECT * FROM reservations WHERE order_id = $1', [orderId])).rows[0];
        if (again) return again;
        const upd = await c.query(
          `UPDATE stock SET available = available - $2, reserved = reserved + $2
           WHERE sku_id = $1 AND available >= $2`,
          [skuId, qty],
        );
        let status = 'RESERVED';
        let reason = null;
        if (!upd.rowCount) {
          status = 'REJECTED';
          const known = await c.query('SELECT 1 FROM stock WHERE sku_id = $1', [skuId]);
          reason = known.rowCount ? 'out_of_stock' : 'unknown_sku';
        }
        const ins = await c.query(
          `INSERT INTO reservations (order_id, sku_id, qty, status, reason) VALUES ($1, $2, $3, $4, $5) RETURNING *`,
          [orderId, skuId, qty, status, reason],
        );
        return ins.rows[0];
      }),
    );
    log.info('reservation', { orderId, traceId: env.traceId, skuId, qty, status: reservation.status });
    await answer(env, reservation);
  }

  async function release(env) {
    const { orderId } = env.data;
    await applyChaos(redis, 'inventory');
    const row = await tx(pool, async (c) => {
      const cur = (await c.query('SELECT * FROM reservations WHERE order_id = $1 FOR UPDATE', [orderId])).rows[0];
      if (!cur) {
        // Release arrived before any reserve: leave a tombstone so a later reserve gives no stock.
        const ins = await c.query(
          `INSERT INTO reservations (order_id, sku_id, qty, status, reason) VALUES ($1, '-', 0, 'RELEASED', 'released_first')
           ON CONFLICT (order_id) DO NOTHING RETURNING *`,
          [orderId],
        );
        return ins.rows[0] ?? (await c.query('SELECT * FROM reservations WHERE order_id = $1', [orderId])).rows[0];
      }
      if (cur.status === 'RESERVED') {
        await c.query(`UPDATE stock SET available = available + $2, reserved = reserved - $2 WHERE sku_id = $1`, [
          cur.sku_id,
          cur.qty,
        ]);
        const upd = await c.query(
          `UPDATE reservations SET status = 'RELEASED', updated_at = now() WHERE order_id = $1 RETURNING *`,
          [orderId],
        );
        return upd.rows[0];
      }
      return cur;
    });
    log.info('release', { orderId, traceId: env.traceId, status: row.status });
    await answer(env, { ...row, status: 'RELEASED' });
  }

  const handlers = { 'inventory.reserve': reserve, 'inventory.release': release };
  async function handle(env) {
    const h = handlers[env.type];
    if (h) await h(env);
  }

  const routes = [
    ['GET', '/health', async () => ({ body: { ok: true, service: 'inventory' } })],
    [
      'GET',
      '/stock',
      async () => ({ body: { stock: (await pool.query('SELECT * FROM stock ORDER BY sku_id')).rows } }),
    ],
  ];

  return { migrate, handle, routes, topics: [TOPICS.INVENTORY_COMMANDS], groupId: 'inventory' };
}
