import { randomUUID } from 'node:crypto';
import { SKUS, skuById } from '../common/catalog.mjs';
import { makeEnvelope, newTraceId } from '../common/envelope.mjs';
import { TOPICS } from '../common/topics.mjs';
import { Metrics, metricsRoute } from '../common/metrics.mjs';
import { httpError } from '../common/util.mjs';

/**
 * Edge gateway. It does three things before an order enters Kafka:
 *   1. Rate limit per client IP        -> Redis INCR (+ EXPIRE)
 *   2. Idempotency-Key                 -> Redis SET NX PX  (same key = same order, no duplicate)
 *   3. Validate the request, then publish `order.create` to orders.commands
 * It answers 202 Accepted: the order is processed asynchronously by the saga.
 */
export function createGateway({
  redis,
  bus,
  log,
  ordersUrl = 'http://orders:4002',
  rateLimit = 120,
  windowSec = 60,
  fetchImpl = fetch,
  metrics = new Metrics(),
}) {
  const IDEM_TTL_MS = 24 * 60 * 60 * 1000;

  async function checkRateLimit(ip) {
    const bucket = `rl:${ip}:${Math.floor(Date.now() / (windowSec * 1000))}`;
    const n = await redis.incr(bucket);
    if (n === 1) await redis.expire(bucket, windowSec + 5);
    if (n > rateLimit) {
      metrics.inc('rateLimited');
      throw httpError(429, 'rate_limited', { limit: rateLimit, windowSec });
    }
  }

  async function placeOrder({ skuId, qty, idemKey }) {
    if (!skuById(skuId)) throw httpError(400, 'unknown_sku', { valid: SKUS.map((s) => s.id) });
    if (!Number.isInteger(qty) || qty < 1 || qty > 10) throw httpError(400, 'qty_must_be_1_to_10');

    const orderId = `ord_${randomUUID().slice(0, 8)}`;
    const claimed = await redis.set(`idem:${idemKey}`, orderId, 'PX', IDEM_TTL_MS, 'NX');
    if (claimed !== 'OK') {
      const existing = await redis.get(`idem:${idemKey}`);
      if (existing) return { orderId: existing, replayed: true };
      throw httpError(409, 'idempotency_key_race_retry');
    }

    const traceId = newTraceId();
    try {
      await bus.producer.send(
        TOPICS.ORDERS_COMMANDS,
        orderId,
        makeEnvelope({ type: 'order.create', traceId, data: { orderId, skuId, qty, idempotencyKey: idemKey } }),
      );
    } catch (err) {
      await redis.del(`idem:${idemKey}`); // let the client retry with the same key
      log.error('publish failed', { orderId, err: err.message });
      throw httpError(503, 'kafka_unavailable');
    }
    log.info('order accepted', { orderId, traceId, skuId, qty });
    return { orderId, traceId, replayed: false };
  }

  async function proxy(path) {
    try {
      const r = await fetchImpl(ordersUrl + path, { signal: AbortSignal.timeout(2000) });
      return { status: r.status, body: await r.json() };
    } catch {
      return { status: 502, body: { error: 'orders_unavailable' } };
    }
  }

  const routes = [
    [
      'GET',
      '/health',
      async () => {
        try {
          return { body: { ok: (await redis.ping()) === 'PONG', service: 'gateway' } };
        } catch {
          return { status: 503, body: { ok: false, error: 'redis_unavailable' } };
        }
      },
    ],
    [
      'POST',
      '/orders',
      ({ ip, query, body, headers }) => metrics.track(async () => {
        await checkRateLimit(ip);
        const skuId = body.skuId ?? query.get('skuId') ?? SKUS[Math.floor(Math.random() * SKUS.length)].id;
        const qty = Number(body.qty ?? query.get('qty') ?? 1);
        const idemKey =
          headers['idempotency-key'] ?? body.idempotencyKey ?? query.get('idempotencyKey') ?? `auto_${randomUUID()}`;
        const out = await placeOrder({ skuId, qty, idemKey });
        return { status: out.replayed ? 200 : 202, body: { status: out.replayed ? 'REPLAYED' : 'ACCEPTED', ...out } };
      }),
    ],
    // Handy for demos: POST /demo/burst?count=20
    [
      'POST',
      '/demo/burst',
      ({ ip, query }) => metrics.track(async () => {
        await checkRateLimit(ip);
        const count = Math.min(Math.max(Number(query.get('count')) || 10, 1), 200);
        const ids = [];
        for (let i = 0; i < count; i++) {
          const skuId = SKUS[Math.floor(Math.random() * SKUS.length)].id;
          const out = await placeOrder({ skuId, qty: 1 + Math.floor(Math.random() * 2), idemKey: `burst_${randomUUID()}` });
          ids.push(out.orderId);
        }
        return { status: 202, body: { accepted: ids.length, orderIds: ids } };
      }),
    ],
    metricsRoute('gateway', metrics),
    ['GET', '/orders', async ({ query }) => proxy(`/orders?limit=${encodeURIComponent(query.get('limit') ?? 50)}`)],
    ['GET', '/orders/:id', async ({ params }) => proxy(`/orders/${encodeURIComponent(params.id)}`)],
  ];

  return { routes, placeOrder, checkRateLimit, metrics };
}
