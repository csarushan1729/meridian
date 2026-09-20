// Turns the control service's raw JSON into the dashboard's Snapshot type.
// Pure functions only (no fetch, no React) so it can be unit tested.
import { SERVICES, SKUS } from "./catalog";
import type {
  BusEvent,
  ConsumerSnapshot,
  EdgeSnapshot,
  Incident,
  LedgerEntry,
  LiveChaos,
  MetricSample,
  Order,
  OrderStatus,
  PatternFlag,
  ServiceId,
  ServiceSnapshot,
  Snapshot,
  Span,
  StockRow,
  TopicId,
  TopicSnapshot,
  Trace,
} from "./types";

// ---- shape of what the control service returns (services/control/service.mjs) ----
interface RawMetrics {
  total: number;
  errors: number;
  inflight: number;
  rps: number;
  errRate: number;
  p50: number;
  p95: number;
  p99: number;
  counters: Record<string, number>;
  paused?: boolean;
}
interface RawOrder {
  id: string;
  skuId: string;
  qty: number;
  amount: number;
  status: "RESERVING" | "PAYING" | "CONFIRMED" | "CANCELLED";
  reason: string | null;
  traceId: string | null;
  idempotencyKey?: string | null;
  createdAt: string;
  updatedAt: string;
  steps?: { name: string; status: string; detail: string | null; at: string }[];
}
export interface LiveRaw {
  now: number;
  startedAt: number;
  services: Record<
    string,
    { up: boolean; paused: boolean; metrics: RawMetrics | null }
  >;
  breaker: {
    state: string;
    failures: number;
    threshold: number;
    cooldownMs: number;
  };
  orders: RawOrder[];
  ordersStats: {
    byStatus: Record<string, number>;
    outboxUnsent: number;
    recent: {
      confirmed: number;
      cancelled: number;
      systemFailures: number;
      latencyMs: { p50: number; p95: number; p99: number };
      createdPerSec: number;
    };
    sagasInFlight: number;
    sysErrRate: number;
  };
  stock: {
    sku_id: string;
    name: string;
    available: number;
    reserved: number;
  }[];
  reservations: number;
  payments: { status: string; count: number; total: number }[];
  ledger: {
    summary: {
      entries: number;
      totalDebit: number;
      totalCredit: number;
      balanced: boolean;
      accounts: { account: string; debit: number; credit: number }[];
    } | null;
    entries: {
      id: number;
      event_id: string;
      order_id: string;
      account: string;
      debit: string | number;
      credit: string | number;
      memo: string | null;
      ts: string;
    }[];
  };
  kafka: {
    topics: {
      topic: string;
      produced: number;
      rate: number;
      lastType: string;
      partitions: {
        partition: number;
        low: number;
        high: number;
        depth?: number;
      }[];
    }[];
    consumers: {
      groupId: string;
      service: string;
      topic: string;
      lag: number;
      rate: number;
    }[];
    dlq: number;
    cluster: {
      clusterId: string;
      controller: number;
      brokers: { nodeId: number; host: string; port: number }[];
    } | null;
    recentEvents: BusEvent[];
  };
  redis: {
    keys: number;
    hits: number;
    misses: number;
    ops: number;
    expired: number;
    locks: number;
    idempotencyKeys: number;
    keyList: { key: string; kind: string; ttl: number | null }[];
  };
  chaos: LiveChaos | null;
  rateLimited: number;
  incidents: Incident[];
  metrics: MetricSample[];
}

const LIVE_SERVICES: ServiceId[] = [
  "gateway",
  "orders",
  "inventory",
  "payments",
  "ledger",
];
const REGION = "us-east-1" as const;
const SLO = 0.995;

const STATUS: Record<RawOrder["status"], OrderStatus> = {
  RESERVING: "reserving",
  PAYING: "charging",
  CONFIRMED: "completed",
  CANCELLED: "failed",
};

const ts = (v: string | number) => (typeof v === "number" ? v : Date.parse(v));

function circuitOf(state: string): "closed" | "open" | "half-open" {
  return state === "open"
    ? "open"
    : state === "half_open"
      ? "half-open"
      : "closed";
}

function serviceOfStep(name: string): ServiceId {
  if (name.endsWith(".requested")) return "orders";
  if (name.startsWith("inventory")) return "inventory";
  if (name.startsWith("payment")) return "payments";
  return "orders";
}

export function mapOrders(raw: RawOrder[]): Order[] {
  return raw.map((o) => {
    const sku = SKUS.find((s) => s.id === o.skuId);
    return {
      id: o.id,
      skuId: o.skuId,
      skuName: sku?.name ?? o.skuId,
      qty: o.qty,
      amount: o.amount,
      region: REGION,
      status: STATUS[o.status] ?? "accepted",
      idempotencyKey: o.idempotencyKey ?? "",
      traceId: o.traceId ?? "",
      createdAt: ts(o.createdAt),
      updatedAt: ts(o.updatedAt),
      steps: (o.steps ?? []).map((s) => ({
        name: s.name,
        at: ts(s.at),
        ok: s.status === "ok",
        detail: s.detail ?? "",
      })),
      compensateReason:
        o.status === "CANCELLED" ? (o.reason ?? undefined) : undefined,
    };
  });
}

/** One trace per order. The saga steps become spans (start = step time, end = next step). */
export function mapTraces(orders: Order[]): Trace[] {
  const traces: Trace[] = [];
  for (const o of orders) {
    if (!o.traceId) continue;
    const done = o.status === "completed" || o.status === "failed";
    const rootId = `${o.traceId}-root`;
    const spans: Span[] = [
      {
        spanId: rootId,
        traceId: o.traceId,
        parentSpanId: null,
        service: "gateway",
        name: "POST /orders",
        start: o.createdAt,
        end: done ? o.updatedAt : null,
        status: o.status === "failed" ? "error" : done ? "ok" : "unset",
        detail: `${o.qty} x ${o.skuName}`,
      },
    ];
    o.steps.forEach((step, i) => {
      const next = o.steps[i + 1];
      spans.push({
        spanId: `${o.traceId}-${i}`,
        traceId: o.traceId,
        parentSpanId: rootId,
        service: serviceOfStep(step.name),
        name: step.name,
        start: step.at,
        end: next ? next.at : done ? Math.max(step.at + 1, o.updatedAt) : null,
        status: step.ok ? "ok" : "error",
        detail: step.detail,
      });
    });
    traces.push({
      traceId: o.traceId,
      orderId: o.id,
      rootService: "gateway",
      start: o.createdAt,
      end: done ? o.updatedAt : null,
      status: o.status === "failed" ? "error" : done ? "ok" : "unset",
      spans,
    });
  }
  return traces;
}

export function mapServices(raw: LiveRaw): ServiceSnapshot[] {
  const breaker = circuitOf(raw.breaker.state);
  return LIVE_SERVICES.map((id) => {
    const meta = SERVICES.find((s) => s.id === id)!;
    const r = raw.services[id];
    const m = r?.metrics;
    const circuit = id === "payments" ? breaker : "closed";
    const errRate = m?.errRate ?? 0;
    const status: ServiceSnapshot["status"] =
      !r?.up || r.paused
        ? "down"
        : circuit === "open" || errRate > 0.12
          ? "degraded"
          : "healthy";
    return {
      id,
      title: meta.title,
      region: REGION,
      rps: m?.rps ?? 0,
      errRate,
      p50: m?.p50 ?? 0,
      p95: m?.p95 ?? 0,
      p99: m?.p99 ?? 0,
      inflight: m?.inflight ?? 0,
      maxInflight: meta.maxInflight,
      status,
      circuit,
      breakerName: id === "payments" ? "orders→payments" : id,
    };
  });
}

export function mapEdges(raw: LiveRaw): EdgeSnapshot[] {
  const m = (id: string) => raw.services[id]?.metrics;
  const breaker = circuitOf(raw.breaker.state);
  return [
    {
      from: "client",
      to: "gateway",
      rps: m("gateway")?.rps ?? 0,
      errRate: m("gateway")?.errRate ?? 0,
      circuit: "closed",
    },
    {
      from: "gateway",
      to: "orders",
      rps: raw.ordersStats.recent.createdPerSec,
      errRate: 0,
      circuit: "closed",
    },
    {
      from: "orders",
      to: "inventory",
      rps: m("inventory")?.rps ?? 0,
      errRate: m("inventory")?.errRate ?? 0,
      circuit: "closed",
    },
    {
      from: "orders",
      to: "payments",
      rps: m("payments")?.rps ?? 0,
      errRate: m("payments")?.errRate ?? 0,
      circuit: breaker,
    },
    {
      from: "payments",
      to: "ledger",
      rps: m("ledger")?.rps ?? 0,
      errRate: m("ledger")?.errRate ?? 0,
      circuit: "closed",
    },
  ];
}

function mapTopics(raw: LiveRaw): TopicSnapshot[] {
  return raw.kafka.topics.map((t) => ({
    id: t.topic as TopicId,
    produced: t.produced,
    rate: t.rate,
    partitions: t.partitions.map((p) => ({
      id: p.partition,
      produced: p.high,
      depth: p.depth ?? 0,
      lastType: t.lastType,
    })),
  }));
}

function mapConsumers(raw: LiveRaw): ConsumerSnapshot[] {
  return raw.kafka.consumers.map((c) => ({
    id: c.groupId,
    topic: c.topic as TopicId,
    lag: c.lag,
    rate: c.rate,
  }));
}

function mapLedger(raw: LiveRaw): { entries: LedgerEntry[]; net: number } {
  // Only the cash side: debit = money in (capture), credit = money out (refund).
  const entries = raw.ledger.entries
    .filter((e) => e.account === "cash")
    .map((e) => {
      const debit = Number(e.debit);
      return {
        id: `led_${e.id}`,
        at: ts(e.ts),
        orderId: e.order_id,
        kind: debit > 0 ? ("capture" as const) : ("refund" as const),
        amount: debit > 0 ? debit : Number(e.credit),
      };
    });
  const cash = raw.ledger.summary?.accounts.find((a) => a.account === "cash");
  return { entries, net: cash ? cash.debit - cash.credit : 0 };
}

function patterns(raw: LiveRaw, services: ServiceSnapshot[]): PatternFlag[] {
  const open = raw.breaker.state === "open";
  const half = raw.breaker.state === "half_open";
  const recentCancelled = raw.orders.some(
    (o) => o.status === "CANCELLED" && Date.now() - ts(o.updatedAt) < 30_000,
  );
  const paused = services.some((s) => s.status === "down");
  const lagging = raw.kafka.consumers.some((c) => c.lag > 5);
  return [
    {
      id: "saga",
      label: "Saga orchestration",
      active: raw.ordersStats.sagasInFlight > 0,
      hint: "Orders service drives reserve → pay → confirm across inventory and payments.",
    },
    {
      id: "compensation",
      label: "Compensating txn",
      active: recentCancelled,
      hint: "A step failed: stock is released and late captures are refunded.",
    },
    {
      id: "circuit-open",
      label: "Circuit open",
      active: open,
      hint: "Payments timed out too often. New orders fail fast until the cooldown ends.",
    },
    {
      id: "circuit-half-open",
      label: "Circuit half-open",
      active: half,
      hint: "One probe order is allowed through. Success closes the breaker.",
    },
    {
      id: "backpressure",
      label: "Consumer lag",
      active: paused || lagging,
      hint: "A consumer is paused, down or slow, so messages wait in Kafka.",
    },
    {
      id: "rate-limit",
      label: "Token bucket",
      active: raw.rateLimited > 0,
      hint: "Gateway counts requests per client in Redis and returns 429 over the limit.",
    },
    {
      id: "outbox",
      label: "Transactional outbox",
      active: raw.ordersStats.outboxUnsent > 0,
      hint: "Orders saves messages in Postgres first, then a relay publishes them to Kafka.",
    },
    {
      id: "idempotency",
      label: "Idempotency keys",
      active: raw.redis.idempotencyKeys > 0,
      hint: "Same Idempotency-Key gives the same order (Redis SET NX PX).",
    },
    {
      id: "dlq",
      label: "Dead-letter queue",
      active: raw.kafka.dlq > 0,
      hint: "Messages that failed 3 times, or were not valid, land on dead-letter.",
    },
    {
      id: "retry",
      label: "Bounded retries",
      active: raw.kafka.dlq > 0 || open,
      hint: "Handlers retry 3 times with backoff before dead-letter.",
    },
  ];
}

function headline(raw: LiveRaw, services: ServiceSnapshot[]): string {
  const down = services.filter((s) => s.status === "down");
  if (down.length) {
    const paused = raw.services[down[0]!.id]?.paused;
    return `${down[0]!.title} is ${paused ? "paused" : "not answering"}. Messages wait in Kafka; orders may time out and compensate.`;
  }
  if (raw.breaker.state === "open")
    return "Circuit orders→payments is open. New orders fail fast until probes succeed.";
  return `${raw.ordersStats.sagasInFlight} sagas in flight. ${raw.kafka.topics.reduce((n, t) => n + t.produced, 0)} messages in Kafka. Live data from Docker services.`;
}

const AWS_ROWS = [
  {
    layer: "Kafka",
    here: "Apache Kafka 3.9 (KRaft, single node) in Docker",
    aws: "Amazon MSK",
  },
  { layer: "Redis", here: "Redis 7 in Docker", aws: "ElastiCache for Redis" },
  {
    layer: "Postgres",
    here: "Postgres 16 in Docker, one database per service",
    aws: "RDS PostgreSQL / Aurora",
  },
  {
    layer: "Services",
    here: "5 Node containers (Docker Compose)",
    aws: "ECS/EKS per service",
  },
  {
    layer: "Gateway",
    here: "Redis rate limit + idempotency keys",
    aws: "API Gateway + ALB",
  },
];

export function mapLive(raw: LiveRaw): Snapshot {
  const services = mapServices(raw);
  const orders = mapOrders(raw.orders);
  const traces = mapTraces(orders);
  const { entries: ledger, net: ledgerNet } = mapLedger(raw);
  const r = raw.ordersStats.recent;
  const decided = r.confirmed + r.cancelled;
  // Availability counts only system failures (timeouts, open circuit). Declines and
  // out-of-stock are normal business outcomes, not outages.
  const availability = decided ? 1 - r.systemFailures / decided : 1;
  const burn = (1 - availability) / (1 - SLO);
  const errorBudget = Math.max(0, Math.min(1, 1 - burn));
  const by = raw.ordersStats.byStatus;
  const total = Object.values(by).reduce((a, b) => a + b, 0);
  const totalMsgs = raw.kafka.topics.reduce((n, t) => n + t.produced, 0);
  const partitions = raw.kafka.topics.reduce(
    (n, t) => n + t.partitions.length,
    0,
  );
  const hitTotal = raw.redis.hits + raw.redis.misses;
  const stock: StockRow[] = raw.stock.map((s) => ({
    skuId: s.sku_id,
    name: s.name,
    available: s.available,
    reserved: s.reserved,
  }));
  const lat = r.latencyMs;

  return {
    mode: "live",
    live: {
      chaos: raw.chaos,
      cluster: raw.kafka.cluster,
      consumerGroups: raw.kafka.consumers.map((c) => ({
        groupId: c.groupId,
        service: c.service as ServiceId,
        topic: c.topic as TopicId,
        lag: c.lag,
        rate: c.rate,
      })),
      breaker: raw.breaker,
      outboxUnsent: raw.ordersStats.outboxUnsent,
    },
    now: raw.now,
    startedAt: raw.startedAt,
    headline: headline(raw, services),
    rps: r.createdPerSec,
    errorRate: raw.ordersStats.sysErrRate,
    availability,
    p50: lat.p50,
    p95: lat.p95,
    p99: lat.p99,
    inflight: raw.ordersStats.sagasInFlight,
    errorBudget,
    burnRate: burn,
    ordersTotal: total,
    ordersOk: by.CONFIRMED ?? 0,
    ordersFailed: by.CANCELLED ?? 0,
    sagasInFlight: raw.ordersStats.sagasInFlight,
    shed: 0,
    rateLimited: raw.rateLimited,
    dlq: raw.kafka.dlq,
    leader: REGION,
    term: 1,
    services,
    edges: mapEdges(raw),
    orders,
    traces,
    topics: mapTopics(raw),
    consumers: mapConsumers(raw),
    recentEvents: raw.kafka.recentEvents,
    raft: [
      {
        id: REGION,
        role: "leader",
        term: 1,
        lastHeartbeat: raw.now,
        logIndex: totalMsgs,
        lagMs: 0,
        votes: 1,
        partitioned: false,
      },
    ],
    incidents: raw.incidents,
    metrics: raw.metrics,
    ledger,
    ledgerNet,
    stock,
    chaos: {
      traffic: raw.chaos?.traffic ?? 0,
      paymentFail: raw.chaos?.declineRate ?? 0,
      inventoryFail: raw.chaos?.inventoryCrashRate ?? 0,
      dropRate: 0,
      killed: Object.fromEntries(
        Object.entries(raw.chaos?.paused ?? {}).filter(([, v]) => v),
      ) as Snapshot["chaos"]["killed"],
      latencyMult: {
        gateway: 1,
        orders: 1,
        inventory: 1,
        payments: 1,
        ledger: 1,
        fulfillment: 1,
        shipping: 1,
        notify: 1,
      },
      partitioned: {},
      clockSkew: 1,
    },
    patterns: patterns(raw, services),
    ticker: raw.incidents.slice(0, 12).map((i) => i.title),
    platform: {
      dbBackend: "postgres",
      kafka: {
        name: "kafka",
        implementation: `Apache Kafka 3.9 (KRaft)${raw.kafka.cluster ? ` · cluster ${raw.kafka.cluster.clusterId}` : ""}`,
        topics: raw.kafka.topics.length,
        partitions,
        messages: totalMsgs,
        persisted: totalMsgs,
        consumerGroups: new Set(raw.kafka.consumers.map((c) => c.groupId)).size,
        dlq: raw.kafka.dlq,
        walDepth: raw.kafka.consumers.reduce((n, c) => n + c.lag, 0),
      },
      redis: {
        name: "redis",
        implementation:
          "Redis 7 · SET NX PX locks, idempotency keys, INCR rate limits",
        keys: raw.redis.keys,
        hits: raw.redis.hits,
        misses: raw.redis.misses,
        hitRate: hitTotal ? raw.redis.hits / hitTotal : 0,
        ops: raw.redis.ops,
        locks: raw.redis.locks,
        idempotencyKeys: raw.redis.idempotencyKeys,
        expired: raw.redis.expired,
      },
      redisKeys: raw.redis.keyList,
      postgres: {
        orders: total,
        inventory: raw.reservations,
        ledger: raw.ledger.summary?.entries ?? 0,
        kafkaLog: 0,
        redisKeys: 0,
      },
      aws: AWS_ROWS,
    },
  };
}
