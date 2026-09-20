import { TOPIC_SPECS, TOPICS } from '../common/topics.mjs';

const clamp = (n, lo, hi) => Math.min(hi, Math.max(lo, Number(n) || 0));
const SERVICE_IDS = ['gateway', 'orders', 'inventory', 'payments', 'ledger'];
const PAUSABLE = ['orders', 'inventory', 'payments', 'ledger'];
const LATENCY = ['inventory', 'payments'];
// Which Kafka consumer group belongs to which service.
const GROUPS = [
  { service: 'orders', groupId: 'orders-saga', topics: [TOPICS.ORDERS_COMMANDS, TOPICS.INVENTORY_EVENTS, TOPICS.PAYMENTS_EVENTS] },
  { service: 'inventory', groupId: 'inventory', topics: [TOPICS.INVENTORY_COMMANDS] },
  { service: 'payments', groupId: 'payments', topics: [TOPICS.PAYMENTS_COMMANDS] },
  { service: 'ledger', groupId: 'ledger', topics: [TOPICS.PAYMENTS_EVENTS] },
];

export async function defaultFetchJson(url, { method = 'GET', timeoutMs = 1500 } = {}) {
  try {
    const r = await fetch(url, { method, signal: AbortSignal.timeout(timeoutMs) });
    if (!r.ok) return null;
    return await r.json();
  } catch {
    return null;
  }
}

const safe = async (p, fallback) => {
  try {
    return await p;
  } catch {
    return fallback;
  }
};

/**
 * Control service = the dashboard's backend.
 * It only READS: service /metrics + /stats over HTTP, Kafka offsets, Redis stats.
 * And it can change chaos switches in Redis. It builds one JSON snapshot every second.
 */
export function createControl({
  redis,
  kafka, // { topicOffsets(), groupOffsets(groupId, topics), clusterInfo(), recentEvents(), lastTypes() }
  urls, // { gateway, orders, inventory, payments, ledger }
  log,
  fetchJson = defaultFetchJson,
  defaultDeclineRate = 0.05,
  now = () => Date.now(),
}) {
  const state = {
    startedAt: now(),
    raw: null,
    prev: null,
    traffic: 0,
    carry: 0,
    metrics: [],
    incidents: [],
    seq: 0,
    seenCancelled: new Set(),
    topicTotals: new Map(),
    committedTotals: new Map(),
    lastPoll: 0,
  };

  const incident = (severity, title, detail, pattern, service) => {
    state.incidents.unshift({ id: `inc_${++state.seq}`, at: now(), severity, title, detail, pattern, service });
    state.incidents.length = Math.min(state.incidents.length, 40);
    log.info('incident', { severity, title });
  };

  // ---------- chaos switches (all live in Redis, services read them) ----------
  async function readChaos() {
    const num = async (k, d) => {
      const v = await redis.get(k);
      return v == null || !Number.isFinite(Number(v)) ? d : Number(v);
    };
    const paused = {};
    for (const s of PAUSABLE) paused[s] = (await redis.get(`chaos:${s}:paused`)) === '1';
    const latencyMs = {};
    for (const s of LATENCY) latencyMs[s] = await num(`chaos:${s}:latency_ms`, 0);
    return {
      traffic: state.traffic,
      declineRate: await num('chaos:payments:declinerate', defaultDeclineRate),
      inventoryCrashRate: await num('chaos:inventory:crashrate', 0),
      latencyMs,
      paused,
    };
  }

  async function setChaos(body = {}) {
    if (body.declineRate != null) await redis.set('chaos:payments:declinerate', String(clamp(body.declineRate, 0, 1)));
    if (body.inventoryCrashRate != null) {
      const v = clamp(body.inventoryCrashRate, 0, 1);
      if (v > 0) await redis.set('chaos:inventory:crashrate', String(v));
      else await redis.del('chaos:inventory:crashrate');
    }
    for (const [svc, ms] of Object.entries(body.latencyMs ?? {})) {
      if (!LATENCY.includes(svc)) continue;
      const v = clamp(ms, 0, 30_000);
      if (v > 0) await redis.set(`chaos:${svc}:latency_ms`, String(v));
      else await redis.del(`chaos:${svc}:latency_ms`);
    }
    for (const [svc, on] of Object.entries(body.paused ?? {})) {
      if (!PAUSABLE.includes(svc)) continue;
      if (on) await redis.set(`chaos:${svc}:paused`, '1');
      else await redis.del(`chaos:${svc}:paused`);
    }
    if (body.traffic != null) state.traffic = clamp(body.traffic, 0, 50);
    return readChaos();
  }

  // ---------- Redis ----------
  async function redisStats() {
    const info = await safe(redis.info('stats'), '');
    const field = (name) => Number((info.match(new RegExp(`${name}:(\\d+)`)) ?? [])[1] ?? 0);
    const keys = await safe(redis.dbsize(), 0);
    let cursor = '0';
    const found = [];
    try {
      do {
        const [next, batch] = await redis.scan(cursor, 'COUNT', 500);
        cursor = next;
        found.push(...batch);
      } while (cursor !== '0' && found.length < 3000);
    } catch {
      /* ignore */
    }
    const kindOf = (k) =>
      k.startsWith('idem:') ? 'idempotency' : k.startsWith('lock:') ? 'lock' : k.startsWith('rl:') ? 'rate-limit' : k.startsWith('chaos:') ? 'chaos' : 'other';
    const show = found.filter((k) => !k.startsWith('rl:')).slice(0, 20).concat(found.filter((k) => k.startsWith('rl:')).slice(0, 8));
    const keyList = [];
    for (const key of show) {
      const ttl = await safe(redis.ttl(key), -1);
      keyList.push({ key, kind: kindOf(key), ttl: ttl >= 0 ? ttl : null });
    }
    return {
      keys,
      hits: field('keyspace_hits'),
      misses: field('keyspace_misses'),
      ops: field('total_commands_processed'),
      expired: field('expired_keys'),
      locks: found.filter((k) => k.startsWith('lock:')).length,
      idempotencyKeys: found.filter((k) => k.startsWith('idem:')).length,
      keyList,
    };
  }

  // ---------- Kafka numbers ----------
  async function kafkaStats(dt) {
    const offsets = await safe(kafka.topicOffsets(), []);
    const topics = offsets.map((t) => {
      const produced = t.partitions.reduce((n, p) => n + (p.high - p.low), 0);
      const prevTotal = state.topicTotals.get(t.topic);
      state.topicTotals.set(t.topic, produced);
      const rate = prevTotal == null || dt <= 0 ? 0 : Math.max(0, (produced - prevTotal) / dt);
      return {
        topic: t.topic,
        produced,
        rate,
        lastType: kafka.lastTypes?.()[t.topic] ?? '',
        partitions: t.partitions.map((p) => ({ partition: p.partition, low: p.low, high: p.high })),
      };
    });
    const highOf = new Map(offsets.map((t) => [t.topic, new Map(t.partitions.map((p) => [p.partition, p]))]));

    const consumers = [];
    for (const g of GROUPS) {
      const res = await safe(kafka.groupOffsets(g.groupId, g.topics), []);
      for (const t of res) {
        let lag = 0;
        let committedSum = 0;
        const parts = [];
        for (const p of t.partitions) {
          const hp = highOf.get(t.topic)?.get(p.partition);
          if (!hp) continue;
          const committed = Number(p.offset) >= 0 ? Number(p.offset) : hp.low;
          const plag = Math.max(0, hp.high - committed);
          committedSum += committed;
          lag += plag;
          parts.push({ partition: p.partition, lag: plag });
        }
        const key = `${g.groupId}:${t.topic}`;
        const prevC = state.committedTotals.get(key);
        state.committedTotals.set(key, committedSum);
        consumers.push({
          groupId: g.groupId,
          service: g.service,
          topic: t.topic,
          lag,
          partitions: parts,
          rate: prevC == null || dt <= 0 ? 0 : Math.max(0, (committedSum - prevC) / dt),
        });
      }
    }
    // depth of a partition = messages still waiting for the slowest consumer group of that topic
    for (const t of topics) {
      for (const p of t.partitions) {
        p.depth = Math.max(
          0,
          ...consumers.filter((c) => c.topic === t.topic).map((c) => c.partitions.find((x) => x.partition === p.partition)?.lag ?? 0),
        );
      }
    }
    const dlq = topics.find((t) => t.topic === TOPICS.DEAD_LETTER)?.produced ?? 0;
    return { topics, consumers, dlq, cluster: await safe(kafka.clusterInfo(), null), recentEvents: kafka.recentEvents?.() ?? [] };
  }

  // ---------- one poll ----------
  async function poll() {
    const t = now();
    const dt = state.lastPoll ? (t - state.lastPoll) / 1000 : 1;
    state.lastPoll = t;

    const [metricsList, ordersList, ordersStats, stock, payments, ledgerSummary, ledgerEntries, kf, rs, chaos] = await Promise.all([
      Promise.all(SERVICE_IDS.map((id) => fetchJson(`${urls[id]}/metrics`))),
      fetchJson(`${urls.orders}/orders?limit=40&steps=1`),
      fetchJson(`${urls.orders}/stats`),
      fetchJson(`${urls.inventory}/stock`),
      fetchJson(`${urls.payments}/summary`),
      fetchJson(`${urls.ledger}/summary`),
      fetchJson(`${urls.ledger}/entries?limit=24`),
      kafkaStats(dt),
      safe(redisStats(), { keys: 0, hits: 0, misses: 0, ops: 0, expired: 0, locks: 0, idempotencyKeys: 0, keyList: [] }),
      safe(readChaos(), null),
    ]);

    const services = {};
    SERVICE_IDS.forEach((id, i) => {
      const m = metricsList[i];
      services[id] = { up: !!m, paused: !!m?.paused, metrics: m ?? null };
    });

    const by = ordersStats?.byStatus ?? {};
    const sagasInFlight = (by.RESERVING ?? 0) + (by.PAYING ?? 0);
    const recent = ordersStats?.recent ?? { confirmed: 0, cancelled: 0, systemFailures: 0, latencyMs: { p50: 0, p95: 0, p99: 0 }, createdPerSec: 0 };
    const decided = recent.confirmed + recent.cancelled;
    const sysErrRate = decided ? recent.systemFailures / decided : 0;

    const raw = {
      now: t,
      startedAt: state.startedAt,
      services,
      breaker: ordersStats?.breaker ?? { state: 'closed', failures: 0, threshold: 3, cooldownMs: 10000 },
      orders: ordersList?.orders ?? [],
      ordersStats: { byStatus: by, outboxUnsent: ordersStats?.outboxUnsent ?? 0, recent, sagasInFlight, sysErrRate },
      stock: stock?.stock ?? [],
      reservations: stock?.reservations ?? 0,
      payments: payments?.byStatus ?? [],
      ledger: { summary: ledgerSummary, entries: ledgerEntries?.entries ?? [] },
      kafka: kf,
      redis: rs,
      chaos,
      rateLimited: services.gateway.metrics?.counters?.rateLimited ?? 0,
      incidents: state.incidents,
      metrics: state.metrics,
    };

    detectIncidents(raw, state.prev);
    state.metrics.push({
      t,
      rps: recent.createdPerSec,
      errors: recent.createdPerSec * sysErrRate,
      p99: recent.latencyMs.p99,
      inflight: sagasInFlight,
    });
    if (state.metrics.length > 48) state.metrics.shift();

    state.prev = raw;
    state.raw = raw;
    return raw;
  }

  function detectIncidents(cur, prev) {
    if (!prev) {
      // First poll after start: remember old cancelled orders, do not report history as new incidents.
      cur.orders.filter((o) => o.status === 'CANCELLED').forEach((o) => state.seenCancelled.add(o.id));
      return;
    }
    for (const id of SERVICE_IDS) {
      const a = prev.services[id];
      const b = cur.services[id];
      if (a.up && !b.up) incident('crit', `${id} is not answering`, 'Health check failed. If it is a consumer, its lag will grow.', 'backpressure', id);
      else if (!a.up && b.up) incident('info', `${id} is back`, 'Health check is OK again.', 'backpressure', id);
      if (!a.paused && b.paused) incident('warn', `${id} paused`, 'Stopped reading from Kafka. Messages wait in the topic (lag grows).', 'backpressure', id);
      else if (a.paused && !b.paused) incident('info', `${id} resumed`, 'Reading from Kafka again and catching up.', 'backpressure', id);
    }
    if (prev.breaker.state !== cur.breaker.state) {
      const s = cur.breaker.state;
      if (s === 'open') incident('crit', 'Circuit breaker opened: payments', 'Payments did not answer in time. New orders fail fast and give stock back.', 'circuit-open', 'payments');
      else if (s === 'half_open') incident('warn', 'Circuit breaker half-open: payments', 'Letting one probe order through to test payments.', 'circuit-half-open', 'payments');
      else incident('info', 'Circuit breaker closed: payments', 'Payments answered again. Normal traffic resumed.', 'circuit-open', 'payments');
    }
    const dlqNew = cur.kafka.dlq - prev.kafka.dlq;
    if (dlqNew > 0) {
      const last = [...cur.kafka.recentEvents].find((e) => e.topic === TOPICS.DEAD_LETTER);
      incident('warn', `${dlqNew} message(s) sent to dead-letter`, last ? `Latest: ${last.payload}` : 'A handler failed 3 times, or the message was not valid.', 'dlq');
    }
    const fresh = cur.orders.filter((o) => o.status === 'CANCELLED' && !state.seenCancelled.has(o.id));
    if (fresh.length) {
      const reasons = {};
      for (const o of fresh) {
        reasons[o.reason ?? 'unknown'] = (reasons[o.reason ?? 'unknown'] ?? 0) + 1;
        state.seenCancelled.add(o.id);
      }
      const system = fresh.some((o) => ['inventory_timeout', 'payment_timeout'].includes(o.reason) || String(o.reason).startsWith('circuit_open'));
      incident(
        system ? 'warn' : 'info',
        `${fresh.length} order(s) cancelled, saga compensated`,
        Object.entries(reasons).map(([r, n]) => `${r} x${n}`).join(', '),
        'compensation',
      );
    }
    if (cur.rateLimited > prev.rateLimited) {
      incident('info', 'Gateway rate limit hit', `${cur.rateLimited - prev.rateLimited} request(s) got 429.`, 'rate-limit', 'gateway');
    }
  }

  // ---------- actions ----------
  async function placeOrder({ skuId, qty }) {
    const q = new URLSearchParams();
    if (skuId) q.set('skuId', skuId);
    if (qty) q.set('qty', String(qty));
    const r = await fetch(`${urls.gateway}/orders?${q}`, { method: 'POST', signal: AbortSignal.timeout(3000) });
    return { status: r.status, body: await r.json() };
  }

  async function restock() {
    return fetchJson(`${urls.inventory}/restock`, { method: 'POST' });
  }

  async function trafficTick() {
    state.carry += state.traffic;
    const n = Math.min(Math.floor(state.carry), 200);
    if (n < 1) return;
    state.carry -= n;
    // One burst call per second (keeps us under the gateway rate limit).
    await fetchJson(`${urls.gateway}/demo/burst?count=${n}`, { method: 'POST', timeoutMs: 4000 });
  }

  function start(pollMs = 1000) {
    const loop = (name, ms, fn) => {
      let busy = false;
      const timer = setInterval(async () => {
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
      return () => clearInterval(timer);
    };
    const stops = [loop('poll', pollMs, poll), loop('traffic', 1000, trafficTick)];
    return () => stops.forEach((s) => s());
  }

  const routes = [
    ['GET', '/health', async () => ({ body: { ok: true, service: 'control' } })],
    ['GET', '/snapshot', async () => ({ body: state.raw ?? (await poll()) })],
    ['GET', '/chaos', async () => ({ body: await readChaos() })],
    ['POST', '/chaos', async ({ body }) => ({ body: await setChaos(body) })],
    ['POST', '/place-order', async ({ body }) => {
      const out = await placeOrder({ skuId: body.skuId, qty: body.qty });
      return { status: out.status, body: out.body };
    }],
    ['POST', '/restock', async () => ({ body: (await restock()) ?? { ok: false } })],
  ];

  return { poll, setChaos, readChaos, placeOrder, restock, trafficTick, start, routes, state, TOPIC_SPECS };
}
