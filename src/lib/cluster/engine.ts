import { KafkaBroker } from "@/platform/kafka";
import { RedisStore } from "@/platform/redis";
import {
  DEFAULT_CHAOS,
  MESH_EDGES,
  REGIONS,
  SERVICES,
  SKUS,
  TOPICS,
} from "./catalog";
import {
  CircuitBreaker,
  Ewma,
  Histogram,
  TokenBucket,
  hexId,
  lognormal,
  mulberry32,
  pickWeighted,
} from "./primitives";
import type {
  BusEvent,
  ChaosConfig,
  CircuitState,
  ConsumerSnapshot,
  Incident,
  LedgerEntry,
  MetricSample,
  Order,
  OrderStatus,
  PatternFlag,
  PatternId,
  RaftNode,
  RaftRole,
  RegionId,
  ServiceId,
  Snapshot,
  Span,
  TopicId,
  Trace,
} from "./types";

type Job = { at: number; run: () => void };

type ServiceRt = {
  id: ServiceId;
  inflight: number;
  maxInflight: number;
  baseLatency: number;
  failRate: number;
  hist: Histogram;
  rps: Ewma;
  err: Ewma;
  hits: number;
  errors: number;
  outbox: { at: number; topic: TopicId; key: string; type: string; traceId: string; payload: string }[];
};

type ConsumerRt = {
  id: string;
  topic: TopicId;
  rate: Ewma;
  handler: (ev: BusEvent) => void;
  service: ServiceId;
};

type EdgeRt = {
  from: ServiceId | "client";
  to: ServiceId;
  breaker: CircuitBreaker;
  rps: Ewma;
  err: Ewma;
};

const TICK_MS = 80;
const SLO = 0.999;
const WINDOW_ORDERS = 400;

export class ClusterEngine {
  now: number;
  readonly startedAt: number;
  private readonly rng: () => number;
  private timer: ReturnType<typeof setInterval> | null = null;
  private jobs: Job[] = [];
  private seq = 0;
  chaos: ChaosConfig = structuredClone(DEFAULT_CHAOS);
  readonly kafka: KafkaBroker;
  readonly redis: RedisStore;
  private readonly services: Record<ServiceId, ServiceRt>;
  private readonly consumers: ConsumerRt[] = [];
  private readonly edges: EdgeRt[];
  private readonly gatewayLimiter: TokenBucket;
  private readonly stock = new Map<string, { available: number; reserved: number }>();
  private readonly orders = new Map<string, Order>();
  private readonly traces = new Map<string, Trace>();
  private readonly spanIndex = new Map<string, Span>();
  private incidents: Incident[] = [];
  private ledger: LedgerEntry[] = [];
  private metrics: MetricSample[] = [];
  private recentEvents: BusEvent[] = [];
  private ticker: string[] = [];
  private shed = 0;
  private rateLimited = 0;
  private orderCount = 0;
  private orderOk = 0;
  private orderFail = 0;
  private errorBudget = 1;
  private onSnapshot: ((snap: Snapshot) => void) | null = null;
  private lastSample = 0;
  private lastSnap = 0;
  private windowOutcomes: boolean[] = [];
  private lastRpsHits = 0;
  private lastErrHits = 0;
  private globalHits = 0;
  private globalErr = 0;
  private latencyHist = new Histogram(360);
  private rpsEwma = new Ewma(0.18);
  private errEwma = new Ewma(0.18);
  private p99Ewma = new Ewma(0.2);

  private raft: {
    nodes: Record<RegionId, { role: RaftRole; term: number; lastHb: number; logIndex: number; votes: number }>;
    leader: RegionId;
    term: number;
    electionAt: number;
  };

  constructor(seed = 0x5eedc0de, start = Date.now()) {
    this.now = start;
    this.startedAt = start;
    this.rng = mulberry32(seed);
    this.gatewayLimiter = new TokenBucket(28, 18, this.now);
    this.kafka = new KafkaBroker(TOPICS);
    this.redis = new RedisStore();

    this.services = {} as Record<ServiceId, ServiceRt>;
    for (const s of SERVICES) {
      this.services[s.id] = {
        id: s.id,
        inflight: 0,
        maxInflight: s.maxInflight,
        baseLatency: s.baseLatency,
        failRate: s.failRate,
        hist: new Histogram(160),
        rps: new Ewma(0.22),
        err: new Ewma(0.22),
        hits: 0,
        errors: 0,
        outbox: [],
      };
    }

    this.edges = MESH_EDGES.map((e) => ({
      from: e.from,
      to: e.to,
      breaker: new CircuitBreaker(),
      rps: new Ewma(0.25),
      err: new Ewma(0.25),
    }));

    for (const sku of SKUS) {
      this.stock.set(sku.id, { available: sku.stock, reserved: 0 });
      this.redis.hset(`stock:${sku.id}`, "available", String(sku.stock));
      this.redis.hset(`stock:${sku.id}`, "reserved", "0");
      this.redis.hset(`stock:${sku.id}`, "name", sku.name);
    }

    this.raft = {
      leader: "us-east-1",
      term: 4,
      electionAt: this.now + 8000,
      nodes: {
        "us-east-1": { role: "leader", term: 4, lastHb: this.now, logIndex: 240, votes: 0 },
        "eu-west-1": { role: "follower", term: 4, lastHb: this.now, logIndex: 238, votes: 0 },
        "ap-south-1": { role: "follower", term: 4, lastHb: this.now, logIndex: 236, votes: 0 },
      },
    };

    this.consumers.push(
      {
        id: "ledger-cg",
        topic: "payments.events",
        rate: new Ewma(0.2),
        handler: (ev) => this.onPaymentEvent(ev),
        service: "ledger",
      },
      {
        id: "notify-cg",
        topic: "shipping.events",
        rate: new Ewma(0.2),
        handler: (ev) => this.onShippingEvent(ev),
        service: "notify",
      },
      {
        id: "orders-cg",
        topic: "orders.commands",
        rate: new Ewma(0.2),
        handler: () => undefined,
        service: "orders",
      },
    );
    for (const cg of this.consumers) this.kafka.ensureGroup(cg.id, cg.topic);
  }

  start(onSnapshot: (snap: Snapshot) => void, opts?: { warm?: number }) {
    this.onSnapshot = onSnapshot;
    if (this.timer != null) {
      onSnapshot(this.snapshot());
      return;
    }
    const warm = opts?.warm ?? 28;
    for (let i = 0; i < warm; i++) this.tick();
    onSnapshot(this.snapshot());
    this.timer = setInterval(() => this.tick(), TICK_MS);
  }

  stop() {
    if (this.timer != null) clearInterval(this.timer);
    this.timer = null;
  }

  setChaos(partial: Partial<ChaosConfig>) {
    this.chaos = {
      ...this.chaos,
      ...partial,
      killed: { ...this.chaos.killed, ...(partial.killed ?? {}) },
      latencyMult: { ...this.chaos.latencyMult, ...(partial.latencyMult ?? {}) },
      partitioned: { ...this.chaos.partitioned, ...(partial.partitioned ?? {}) },
    };
    this.emit();
  }

  restock() {
    for (const sku of SKUS) {
      const row = this.stock.get(sku.id);
      if (!row) continue;
      row.available = sku.stock;
      row.reserved = 0;
      this.redis.hset(`stock:${sku.id}`, "available", String(sku.stock));
      this.redis.hset(`stock:${sku.id}`, "reserved", "0");
    }
    this.note("info", "inventory", "Warehouse restocked", "Available inventory reset to catalog baselines.", "outbox");
    this.emit();
  }

  placeOrder(input?: { skuId?: string; qty?: number; region?: RegionId; idempotencyKey?: string }): string {
    const sku = SKUS.find((s) => s.id === input?.skuId) ?? pickWeighted(this.rng, SKUS);
    const region = input?.region ?? pickWeighted(this.rng, REGIONS).id;
    const qty = input?.qty ?? 1;
    const key = input?.idempotencyKey ?? `idem_${hexId(this.rng, 6)}`;
    return this.admit({ skuId: sku.id, qty, region, key, manual: true });
  }

  warm(n = 28) {
    for (let i = 0; i < n; i++) this.tick();
  }

  private tick() {
    this.now += TICK_MS * this.chaos.clockSkew;
    this.tickRaft();
    this.spawnTraffic();
    this.flushJobs();
    this.drainOutboxes();
    this.drainConsumers();
    if (this.now - this.lastSample >= 1000) this.sample();
    if (this.now - this.lastSnap >= 140) this.emit();
  }

  private spawnTraffic() {
    const lambda = 0.22 * this.chaos.traffic;
    if (this.rng() < lambda) this.admit();
    if (this.rng() < lambda * 0.18) this.admit();
  }

  private admit(input?: {
    skuId: string;
    qty: number;
    region: RegionId;
    key: string;
    manual?: boolean;
  }): string {
    const sku = SKUS.find((s) => s.id === input?.skuId) ?? pickWeighted(this.rng, SKUS);
    const region = input?.region ?? pickWeighted(this.rng, REGIONS).id;
    const qty = input?.qty ?? 1;
    const key = input?.key ?? `idem_${hexId(this.rng, 6)}`;
    const orderId = `ord_${hexId(this.rng, 5)}`;
    const traceId = hexId(this.rng, 8);

    if (this.redis.get(`idem:${key}`)) {
      this.note("info", "orders", "Idempotent replay", `Key ${key} already committed — request coalesced.`, "idempotency");
      this.pushTicker(`idempotency hit ${key.slice(0, 12)}`);
      return this.redis.get(`idem:${key}`) ?? orderId;
    }

    const trace: Trace = {
      traceId,
      orderId,
      rootService: "client",
      start: this.now,
      end: null,
      status: "unset",
      spans: [],
    };
    this.traces.set(traceId, trace);
    this.trimMap(this.traces, 48);

    this.invoke({
      from: "client",
      to: "gateway",
      traceId,
      parentSpanId: null,
      name: "POST /v1/orders",
      work: (span) => {
        if (!this.gatewayLimiter.take(this.now)) {
          this.rateLimited += 1;
          span.status = "error";
          span.detail = "429 token bucket empty";
          this.finishTrace(traceId, "error");
          this.note("warn", "gateway", "Rate limited", "Edge token bucket shed a request.", "rate-limit");
          return;
        }
        this.invoke({
          from: "gateway",
          to: "orders",
          traceId,
          parentSpanId: span.spanId,
          name: "CreateOrder",
          work: (orderSpan) => this.createOrder({ orderId, sku, qty, region, key, traceId, span: orderSpan }),
        });
      },
    });
    return orderId;
  }

  private createOrder(args: {
    orderId: string;
    sku: (typeof SKUS)[number];
    qty: number;
    region: RegionId;
    key: string;
    traceId: string;
    span: Span;
  }) {
    if (this.redis.get(`idem:${args.key}`)) {
      args.span.detail = "idempotent hit";
      return;
    }
    this.redis.setnx(`idem:${args.key}`, args.orderId, 86_400_000);
    const order: Order = {
      id: args.orderId,
      skuId: args.sku.id,
      skuName: args.sku.name,
      qty: args.qty,
      amount: args.sku.price * args.qty,
      region: args.region,
      status: "accepted",
      idempotencyKey: args.key,
      traceId: args.traceId,
      createdAt: this.now,
      updatedAt: this.now,
      steps: [{ name: "accepted", at: this.now, ok: true, detail: "Order written locally" }],
    };
    this.orders.set(order.id, order);
    this.trimMap(this.orders, 80);
    this.orderCount += 1;

    this.enqueueOutbox("orders", "orders.commands", order.id, "CreateOrder", args.traceId, order.id);
    this.enqueueOutbox("orders", "orders.events", order.id, "OrderCreated", args.traceId, args.sku.id);
    args.span.detail = `${order.id} ${args.sku.name}`;
    this.advance(order, "reserving", "saga: reserve inventory");
    this.invoke({
      from: "orders",
      to: "inventory",
      traceId: args.traceId,
      parentSpanId: args.span.spanId,
      name: "ReserveStock",
      work: (span) => this.reserve(order, span),
      onError: (reason) => this.failOrder(order, reason),
    });
  }

  private reserve(order: Order, span: Span) {
    if (this.rng() < this.chaos.inventoryFail) {
      throw new Error("warehouse timeout");
    }
    const lockKey = `lock:sku:${order.skuId}`;
    if (!this.redis.setnx(lockKey, order.id, 1800)) {
      throw new Error("inventory lock contention");
    }
    try {
      const cached = this.redis.hgetall(`stock:${order.skuId}`);
      const row = this.stock.get(order.skuId);
      if (cached.available != null && row) {
        row.available = Number(cached.available);
        row.reserved = Number(cached.reserved) || row.reserved;
      }
      if (!row || row.available < order.qty) {
        span.detail = "insufficient stock";
        this.enqueueOutbox("inventory", "inventory.events", order.id, "StockRejected", order.traceId, order.skuId);
        this.failOrder(order, "stock unavailable");
        return;
      }
      row.available -= order.qty;
      row.reserved += order.qty;
      this.redis.hset(`stock:${order.skuId}`, "available", String(row.available));
      this.redis.hset(`stock:${order.skuId}`, "reserved", String(row.reserved));
      span.detail = `${order.skuId} x${order.qty}`;
      this.enqueueOutbox("inventory", "inventory.events", order.id, "StockReserved", order.traceId, order.skuId);
      this.step(order, "reserved", true, "Inventory reserved");
      this.advance(order, "charging", "saga: capture payment");
      const parent = span;
      this.invoke({
        from: "orders",
        to: "payments",
        traceId: order.traceId,
        parentSpanId: parent.spanId,
        name: "CapturePayment",
        work: (paySpan) => this.capture(order, paySpan),
        onError: (reason) => this.compensate(order, reason, parent.spanId),
      });
    } finally {
      this.redis.del(lockKey);
    }
  }

  private capture(order: Order, span: Span) {
    if (this.rng() < this.chaos.paymentFail) {
      throw new Error("card declined");
    }
    span.detail = `${(order.amount / 100).toFixed(2)} USD`;
    this.enqueueOutbox("payments", "payments.events", order.id, "PaymentCaptured", order.traceId, String(order.amount));
    this.step(order, "captured", true, "Payment captured");
    this.advance(order, "fulfilling", "saga: allocate warehouse");
    this.invoke({
      from: "orders",
      to: "fulfillment",
      traceId: order.traceId,
      parentSpanId: span.spanId,
      name: "Allocate",
      work: (fSpan) => this.allocate(order, fSpan),
      onError: (reason) => this.compensate(order, reason, span.spanId),
    });
  }

  private allocate(order: Order, span: Span) {
    span.detail = `${order.region} warehouse`;
    this.enqueueOutbox("fulfillment", "fulfillment.events", order.id, "Allocated", order.traceId, order.region);
    this.step(order, "allocated", true, `Allocated in ${order.region}`);
    this.advance(order, "shipping", "saga: create label");
    this.invoke({
      from: "fulfillment",
      to: "shipping",
      traceId: order.traceId,
      parentSpanId: span.spanId,
      name: "CreateLabel",
      work: (sSpan) => this.label(order, sSpan),
      onError: (reason) => this.compensate(order, reason, span.spanId),
    });
  }

  private label(order: Order, span: Span) {
    const carrier = this.rng() > 0.5 ? "Helix Freight" : "Northline";
    span.detail = carrier;
    this.enqueueOutbox("shipping", "shipping.events", order.id, "LabelCreated", order.traceId, carrier);
    this.step(order, "labeled", true, `${carrier} label`);
    this.advance(order, "notifying", "saga: notify customer");
    this.invoke({
      from: "shipping",
      to: "notify",
      traceId: order.traceId,
      parentSpanId: span.spanId,
      name: "SendReceipt",
      work: (nSpan) => {
        nSpan.detail = "email + webhook";
        this.enqueueOutbox("notify", "notify.commands", order.id, "SendReceipt", order.traceId, order.id);
        this.step(order, "notified", true, "Receipt dispatched");
        this.complete(order);
      },
      onError: () => this.complete(order),
    });
  }

  private compensate(order: Order, reason: string, parentSpanId: string) {
    if (order.status === "failed" || order.status === "completed") return;
    this.advance(order, "compensating", reason);
    order.compensateReason = reason;
    this.note("warn", "orders", "Saga compensating", `${order.id}: ${reason}`, "compensation");
    this.pushTicker(`compensate ${order.id} · ${reason}`);

    const row = this.stock.get(order.skuId);
    if (row && row.reserved >= order.qty) {
      row.reserved -= order.qty;
      row.available += order.qty;
      this.redis.hset(`stock:${order.skuId}`, "available", String(row.available));
      this.redis.hset(`stock:${order.skuId}`, "reserved", String(row.reserved));
    }
    this.enqueueOutbox("inventory", "inventory.events", order.id, "StockReleased", order.traceId, order.skuId);

    if (order.steps.some((s) => s.name === "captured" && s.ok)) {
      this.invoke({
        from: "orders",
        to: "payments",
        traceId: order.traceId,
        parentSpanId,
        name: "RefundPayment",
        work: (span) => {
          span.detail = `${(order.amount / 100).toFixed(2)} USD`;
          this.enqueueOutbox("payments", "payments.events", order.id, "PaymentRefunded", order.traceId, String(order.amount));
          this.step(order, "refunded", true, "Payment refunded");
        },
      });
    }
    this.failOrder(order, reason);
  }

  private complete(order: Order) {
    const row = this.stock.get(order.skuId);
    if (row) {
      row.reserved = Math.max(0, row.reserved - order.qty);
      this.redis.hset(`stock:${order.skuId}`, "reserved", String(row.reserved));
    }
    this.advance(order, "completed", "saga closed");
    this.orderOk += 1;
    this.windowOutcomes.push(true);
    this.trimArr(this.windowOutcomes, WINDOW_ORDERS);
    this.finishTrace(order.traceId, "ok");
    this.pushTicker(`${order.id} completed · ${order.skuName}`);
  }

  private failOrder(order: Order, reason: string) {
    if (order.status === "failed") return;
    this.advance(order, "failed", reason);
    this.orderFail += 1;
    this.windowOutcomes.push(false);
    this.trimArr(this.windowOutcomes, WINDOW_ORDERS);
    this.finishTrace(order.traceId, "error");
    this.enqueueOutbox("orders", "dead-letter", order.id, "OrderFailed", order.traceId, reason);
    this.note("crit", "orders", "Order failed", `${order.id}: ${reason}`, "dlq");
  }

  private onPaymentEvent(ev: BusEvent) {
    if (ev.type !== "PaymentCaptured" && ev.type !== "PaymentRefunded") return;
    const order = this.orders.get(ev.key);
    const amount = Number(ev.payload) || order?.amount || 0;
    this.ledger.unshift({
      id: `led_${this.nextSeq()}`,
      at: this.now,
      orderId: ev.key,
      kind: ev.type === "PaymentCaptured" ? "capture" : "refund",
      amount,
    });
    this.trimArr(this.ledger, 40);
    this.invoke({
      from: "payments",
      to: "ledger",
      traceId: ev.traceId,
      parentSpanId: null,
      name: ev.type === "PaymentCaptured" ? "PostDebit" : "PostCredit",
      work: (span) => {
        span.detail = `${ev.type} ${(amount / 100).toFixed(2)}`;
      },
    });
  }

  private onShippingEvent(ev: BusEvent) {
    if (ev.type !== "LabelCreated") return;
    this.pushTicker(`label ${ev.payload} · ${ev.key}`);
  }

  private invoke(opts: {
    from: ServiceId | "client";
    to: ServiceId;
    traceId: string;
    parentSpanId: string | null;
    name: string;
    work: (span: Span) => void;
    onError?: (reason: string) => void;
  }) {
    const span = this.startSpan(opts.traceId, opts.parentSpanId, opts.to, opts.name);
    const edge = this.edges.find((e) => e.from === opts.from && e.to === opts.to);
    const dest = this.services[opts.to];

    if (this.chaos.dropRate > 0 && this.rng() < this.chaos.dropRate) {
      span.status = "error";
      span.detail = "message dropped";
      span.end = this.now;
      this.finishSpan(span, dest, edge, false);
      opts.onError?.("network drop");
      return;
    }

    if (this.chaos.killed[opts.to]) {
      span.status = "error";
      span.detail = "service process down";
      span.end = this.now + 2;
      this.finishSpan(span, dest, edge, false);
      edge?.breaker.fail(this.now);
      opts.onError?.(`${opts.to} down`);
      return;
    }

    if (edge && !edge.breaker.allow(this.now)) {
      span.status = "error";
      span.detail = "circuit open — fail fast";
      span.end = this.now + 1;
      this.finishSpan(span, dest, edge, false);
      this.note("crit", opts.to, "Circuit open", `${opts.from} → ${opts.to} failing fast.`, "circuit-open");
      opts.onError?.("circuit open");
      return;
    }

    if (dest.inflight >= dest.maxInflight) {
      this.shed += 1;
      span.status = "error";
      span.detail = "semaphore exhausted — shed";
      span.end = this.now + 1;
      this.finishSpan(span, dest, edge, false);
      this.note("warn", opts.to, "Backpressure", `${opts.to} shed a request (pool ${dest.maxInflight}).`, "backpressure");
      opts.onError?.("backpressure");
      return;
    }

    dest.inflight += 1;
    const latency = lognormal(this.rng, dest.baseLatency * (this.chaos.latencyMult[opts.to] ?? 1));
    this.schedule(latency, () => {
      dest.inflight = Math.max(0, dest.inflight - 1);
      dest.hits += 1;
      this.globalHits += 1;
      const failP = opts.to === "payments" ? this.chaos.paymentFail : dest.failRate;
      try {
        if (this.rng() < failP * 0.35 && opts.to !== "payments" && opts.to !== "inventory") {
          throw new Error("internal error");
        }
        opts.work(span);
        if (span.status === "unset") span.status = "ok";
        if (span.status === "error") throw new Error(span.detail || "handler error");
        span.end = this.now;
        dest.hist.push(latency);
        this.latencyHist.push(latency);
        this.finishSpan(span, dest, edge, true);
        edge?.breaker.success();
      } catch (err) {
        dest.errors += 1;
        this.globalErr += 1;
        span.status = "error";
        span.detail = err instanceof Error ? err.message : "failed";
        span.end = this.now;
        dest.hist.push(latency);
        this.latencyHist.push(latency);
        this.finishSpan(span, dest, edge, false);
        edge?.breaker.fail(this.now);
        if (edge?.breaker.state === "open") {
          this.note("crit", opts.to, "Breaker tripped", `${opts.from} → ${opts.to} opened after consecutive failures.`, "circuit-open");
        }
        opts.onError?.(span.detail);
      }
    });
  }

  private finishSpan(span: Span, dest: ServiceRt, edge: EdgeRt | undefined, ok: boolean) {
    dest.rps.push(1);
    dest.err.push(ok ? 0 : 1);
    edge?.rps.push(1);
    edge?.err.push(ok ? 0 : 1);
  }

  private startSpan(
    traceId: string,
    parentSpanId: string | null,
    service: ServiceId | "client",
    name: string,
  ): Span {
    const span: Span = {
      spanId: hexId(this.rng, 4),
      traceId,
      parentSpanId,
      service,
      name,
      start: this.now,
      end: null,
      status: "unset",
      detail: "",
    };
    const trace = this.traces.get(traceId);
    if (trace) {
      trace.spans.push(span);
      if (trace.spans.length > 24) trace.spans.shift();
    }
    this.spanIndex.set(span.spanId, span);
    return span;
  }

  private finishTrace(traceId: string, status: "ok" | "error") {
    const trace = this.traces.get(traceId);
    if (!trace) return;
    trace.end = this.now;
    trace.status = status;
    for (const span of trace.spans) {
      if (span.end == null) span.end = this.now;
    }
  }

  private enqueueOutbox(
    service: ServiceId,
    topic: TopicId,
    key: string,
    type: string,
    traceId: string,
    payload: string,
  ) {
    this.services[service].outbox.push({
      at: this.now + 3 + this.rng() * 8,
      topic,
      key,
      type,
      traceId,
      payload,
    });
  }

  private drainOutboxes() {
    for (const svc of Object.values(this.services)) {
      if (svc.outbox.length === 0) continue;
      const due: typeof svc.outbox = [];
      const rest: typeof svc.outbox = [];
      for (const item of svc.outbox) {
        if (item.at <= this.now) due.push(item);
        else rest.push(item);
      }
      svc.outbox = rest.slice(-40);
      for (const item of due) this.produce(item.topic, item.key, item.type, item.traceId, item.payload);
    }
  }

  private produce(topicId: TopicId, key: string, type: string, traceId: string, payload: string) {
    const ev = this.kafka.produce({
      topic: topicId,
      key,
      type,
      traceId,
      payload,
      ts: this.now,
    });
    this.recentEvents.unshift(ev);
    this.trimArr(this.recentEvents, 36);
  }

  private drainConsumers() {
    for (const cg of this.consumers) {
      if (this.chaos.killed[cg.service]) continue;
      const batch = this.kafka.poll(cg.id, cg.topic, 3);
      if (batch.length === 0) continue;
      let n = 0;
      for (const next of batch) {
        if (this.rng() < 0.35) continue;
        try {
          cg.handler(next);
        } catch {
          this.produce("dead-letter", next.key, "ConsumerFailed", next.traceId, cg.id);
        }
        this.kafka.commit(cg.id, cg.topic, next.partition, next.offset + 1);
        n += 1;
      }
      if (n) cg.rate.push(n);
    }
  }

  private tickRaft() {
    const leader = this.raft.nodes[this.raft.leader];
    const leaderPartitioned = !!this.chaos.partitioned[this.raft.leader];
    for (const region of REGIONS) {
      const node = this.raft.nodes[region.id];
      const isolated = !!this.chaos.partitioned[region.id];
      if (region.id === this.raft.leader && !isolated) {
        node.lastHb = this.now;
        node.logIndex += this.rng() < 0.6 ? 1 : 0;
        node.role = "leader";
        node.term = this.raft.term;
      } else if (!isolated && !leaderPartitioned) {
        node.lastHb = this.now;
        node.role = "follower";
        node.term = this.raft.term;
        if (node.logIndex < leader.logIndex && this.rng() < 0.7) node.logIndex += 1;
      } else {
        node.role = node.role === "leader" ? "candidate" : node.role;
      }
    }

    if (leaderPartitioned || this.now >= this.raft.electionAt) {
      const candidates = REGIONS.map((r) => r.id).filter((id) => !this.chaos.partitioned[id]);
      if (candidates.length === 0) return;
      const winner = candidates[Math.floor(this.rng() * candidates.length)]!;
      if (winner !== this.raft.leader || leaderPartitioned) {
        this.raft.term += 1;
        this.raft.leader = winner;
        for (const region of REGIONS) {
          const node = this.raft.nodes[region.id];
          node.term = this.raft.term;
          node.votes = region.id === winner ? candidates.length : 0;
          node.role = region.id === winner ? "leader" : this.chaos.partitioned[region.id] ? "candidate" : "follower";
          if (node.role !== "candidate") node.lastHb = this.now;
        }
        this.note(
          "warn",
          undefined,
          "Leader elected",
          `${winner} won term ${this.raft.term} (${candidates.length}/3 votes).`,
          "leader-election",
          winner,
        );
        this.pushTicker(`raft ${winner} leader · term ${this.raft.term}`);
      }
      this.raft.electionAt = this.now + 9000 + this.rng() * 6000;
    }
  }

  private schedule(ms: number, run: () => void) {
    this.jobs.push({ at: this.now + ms, run });
    if (this.jobs.length > 600) this.jobs.splice(0, this.jobs.length - 600);
  }

  private flushJobs() {
    if (this.jobs.length === 0) return;
    const due: Job[] = [];
    const rest: Job[] = [];
    for (const job of this.jobs) {
      if (job.at <= this.now) due.push(job);
      else rest.push(job);
    }
    this.jobs = rest;
    for (const job of due) job.run();
  }

  private sample() {
    const dt = Math.max(0.2, (this.now - this.lastSample) / 1000);
    const hits = this.globalHits - this.lastRpsHits;
    const errs = this.globalErr - this.lastErrHits;
    this.lastRpsHits = this.globalHits;
    this.lastErrHits = this.globalErr;
    this.lastSample = this.now;
    const rps = hits / dt;
    const errRate = hits > 0 ? errs / hits : 0;
    this.rpsEwma.push(rps);
    this.errEwma.push(errRate);
    this.p99Ewma.push(this.latencyHist.percentile(0.99));

    const okN = this.windowOutcomes.filter(Boolean).length;
    const avail = this.windowOutcomes.length ? okN / this.windowOutcomes.length : 1;
    const consumed = Math.max(0, SLO - avail);
    const budget = Math.max(0, 1 - consumed / (1 - SLO));
    this.errorBudget = 0.65 * this.errorBudget + 0.35 * budget;

    this.metrics.push({
      t: this.now,
      rps: this.rpsEwma.value,
      errors: this.errEwma.value * this.rpsEwma.value,
      p99: this.p99Ewma.value,
      inflight: this.totalInflight(),
    });
    this.trimArr(this.metrics, 48);

    for (const svc of Object.values(this.services)) {
      svc.rps.push(0.15);
      svc.err.push(0.02);
    }
  }

  private emit() {
    this.lastSnap = this.now;
    this.onSnapshot?.(this.snapshot());
  }

  snapshot(): Snapshot {
    const services = SERVICES.map((meta) => {
      const rt = this.services[meta.id];
      const edge = this.edges.find((e) => e.to === meta.id && e.from !== "client") ?? this.edges.find((e) => e.to === meta.id);
      const circuit: CircuitState = edge?.breaker.state ?? "closed";
      const down = !!this.chaos.killed[meta.id];
      const errRate = rt.err.value;
      const status: "healthy" | "degraded" | "down" = down
        ? "down"
        : circuit === "open" || errRate > 0.12
          ? "degraded"
          : "healthy";
      return {
        id: meta.id,
        title: meta.title,
        region: meta.region,
        rps: Math.max(0, rt.rps.value * 6),
        errRate,
        p50: rt.hist.percentile(0.5),
        p95: rt.hist.percentile(0.95),
        p99: rt.hist.percentile(0.99),
        inflight: rt.inflight,
        maxInflight: rt.maxInflight,
        status,
        circuit,
        breakerName: edge ? `${edge.from}→${meta.id}` : meta.id,
      };
    });

    const edges = this.edges.map((e) => ({
      from: e.from,
      to: e.to,
      rps: Math.max(0, e.rps.value * 6),
      errRate: e.err.value,
      circuit: e.breaker.state,
    }));

    const topics = this.kafka.topicSnapshots();
    const consumers: ConsumerSnapshot[] = this.kafka.consumerSnapshots(
      this.consumers.map((cg) => ({ id: cg.id, topic: cg.topic })),
    );

    const raft: RaftNode[] = REGIONS.map((r) => {
      const n = this.raft.nodes[r.id];
      return {
        id: r.id,
        role: n.role,
        term: n.term,
        lastHeartbeat: n.lastHb,
        logIndex: n.logIndex,
        lagMs: Math.max(0, this.now - n.lastHb),
        votes: n.votes,
        partitioned: !!this.chaos.partitioned[r.id],
      };
    });

    const orders = [...this.orders.values()].sort((a, b) => b.createdAt - a.createdAt).slice(0, 36);
    const traces = [...this.traces.values()].sort((a, b) => b.start - a.start).slice(0, 28);
    const sagasInFlight = orders.filter((o) =>
      ["accepted", "reserving", "charging", "fulfilling", "shipping", "notifying", "compensating"].includes(
        o.status,
      ),
    ).length;

    const avail = this.windowOutcomes.length
      ? this.windowOutcomes.filter(Boolean).length / this.windowOutcomes.length
      : 1;
    const ledgerNet = this.ledger.reduce((s, e) => s + (e.kind === "capture" ? e.amount : -e.amount), 0);
    const burn = this.errorBudget < 0.4 ? 2.4 : this.errorBudget < 0.7 ? 1.1 : 0.4;

    return {
      now: this.now,
      startedAt: this.startedAt,
      headline: this.headline(services, raft, sagasInFlight),
      rps: Math.max(0, this.rpsEwma.value),
      errorRate: Math.max(0, this.errEwma.value),
      availability: avail,
      p50: this.latencyHist.percentile(0.5),
      p95: this.latencyHist.percentile(0.95),
      p99: this.p99Ewma.value || this.latencyHist.percentile(0.99),
      inflight: this.totalInflight(),
      errorBudget: this.errorBudget,
      burnRate: burn,
      ordersTotal: this.orderCount,
      ordersOk: this.orderOk,
      ordersFailed: this.orderFail,
      sagasInFlight,
      shed: this.shed,
      rateLimited: this.rateLimited,
      dlq: this.kafka.dlq,
      leader: this.raft.leader,
      term: this.raft.term,
      services,
      edges,
      orders,
      traces,
      topics,
      consumers,
      recentEvents: this.recentEvents.slice(0, 24),
      raft,
      incidents: this.incidents.slice(0, 28),
      metrics: this.metrics.slice(),
      ledger: this.ledger.slice(0, 18),
      ledgerNet,
      stock: SKUS.map((s) => {
        const row = this.stock.get(s.id)!;
        return { skuId: s.id, name: s.name, available: row.available, reserved: row.reserved };
      }),
      chaos: {
        ...this.chaos,
        killed: { ...this.chaos.killed },
        latencyMult: { ...this.chaos.latencyMult },
        partitioned: { ...this.chaos.partitioned },
      },
      patterns: this.patterns(services, raft, sagasInFlight),
      ticker: this.ticker.slice(0, 18),
      platform: this.platformSnapshot(),
    };
  }

  private platformSnapshot(): Snapshot["platform"] {
    const kafka = this.kafka.stats();
    const redis = this.redis.stats();
    return {
      dbBackend: "memory",
      kafka,
      redis,
      redisKeys: this.redis.scan("", 28),
      postgres: { orders: 0, inventory: 0, ledger: 0, kafkaLog: 0, redisKeys: 0 },
      aws: [
        { layer: "Kafka", here: "helix-kafka (partitioned log + Postgres WAL)", aws: "Amazon MSK / Kafka on EKS" },
        { layer: "Redis", here: "helix-redis (SETNX · hashes · TTL)", aws: "ElastiCache for Redis" },
        { layer: "Postgres", here: "PGLite locally · Neon when deployed", aws: "RDS PostgreSQL / Aurora" },
        { layer: "Services", here: "8 in-process workers, isolated schemas", aws: "ECS/EKS per service" },
        { layer: "Gateway", here: "Token-bucket admission + tracing", aws: "API Gateway + ALB" },
      ],
    };
  }

  private headline(services: Snapshot["services"], raft: RaftNode[], sagas: number): string {
    const down = services.filter((s) => s.status === "down");
    const open = services.filter((s) => s.circuit === "open");
    if (down.length) return `${down[0]!.title} is down. Requests fail fast; sagas are compensating.`;
    if (open.length) return `Circuit ${open[0]!.breakerName} is open. Downstream is fenced until probes succeed.`;
    const isolated = raft.filter((n) => n.partitioned);
    if (isolated.length) return `Network partition on ${isolated[0]!.id}. Remaining nodes hold quorum.`;
    return `${this.raft.leader} is leader (term ${this.raft.term}). ${sagas} sagas in flight. Error budget ${Math.round(this.errorBudget * 100)}%.`;
  }

  private patterns(services: Snapshot["services"], raft: RaftNode[], sagas: number): PatternFlag[] {
    const open = services.some((s) => s.circuit === "open");
    const half = services.some((s) => s.circuit === "half-open");
    const compensating = [...this.orders.values()].some((o) => o.status === "compensating");
    return [
      { id: "saga", label: "Saga orchestration", active: sagas > 0, hint: "Orders service drives a stateful workflow across inventory, payments, fulfillment." },
      { id: "compensation", label: "Compensating txn", active: compensating, hint: "A downstream step failed — inventory is released and captures are refunded." },
      { id: "circuit-open", label: "Circuit open", active: open, hint: "Fail-fast after an error threshold. No calls until the reset timeout." },
      { id: "circuit-half-open", label: "Circuit half-open", active: half, hint: "Limited probes are allowed. Success closes the breaker; failure re-opens it." },
      { id: "backpressure", label: "Load shedding", active: this.shed > 0 && this.totalInflight() > 10, hint: "Service thread pools are full. Excess is shed rather than queued unboundedly." },
      { id: "rate-limit", label: "Token bucket", active: this.rateLimited > 0, hint: "Edge gateway admits bursts up to capacity, then 429s." },
      { id: "leader-election", label: "Raft election", active: raft.some((n) => n.role === "candidate"), hint: "A region missed heartbeats and started a new term." },
      { id: "partition", label: "Net split", active: raft.some((n) => n.partitioned), hint: "A region cannot see the leader. Writes stall there until heal." },
      { id: "outbox", label: "Transactional outbox", active: Object.values(this.services).some((s) => s.outbox.length > 0), hint: "Events drain from a local outbox so publishes survive handler crashes." },
      { id: "idempotency", label: "Idempotency keys", active: true, hint: "CreateOrder coalesces on a client key so retries do not double-charge." },
      { id: "dlq", label: "Dead-letter queue", active: this.kafka.dlq > 0, hint: "Poison messages land on dead-letter after exhaustion." },
      { id: "retry", label: "Bounded retries", active: open || half, hint: "Callers retry with jitter only while the breaker is closed." },
    ];
  }

  private advance(order: Order, status: OrderStatus, detail: string) {
    order.status = status;
    order.updatedAt = this.now;
    this.step(order, status, status !== "failed", detail);
  }

  private step(order: Order, name: string, ok: boolean, detail: string) {
    order.steps.push({ name, at: this.now, ok, detail });
    if (order.steps.length > 16) order.steps.shift();
  }

  private note(
    severity: Incident["severity"],
    service: ServiceId | undefined,
    title: string,
    detail: string,
    pattern: PatternId,
    region?: RegionId,
  ) {
    const last = this.incidents[0];
    if (last && last.title === title && this.now - last.at < 2200) return;
    this.incidents.unshift({
      id: `inc_${this.nextSeq()}`,
      at: this.now,
      severity,
      title,
      detail,
      pattern,
      service,
      region,
    });
    this.trimArr(this.incidents, 40);
  }

  private pushTicker(line: string) {
    this.ticker.unshift(line);
    this.trimArr(this.ticker, 24);
  }

  private totalInflight() {
    let n = 0;
    for (const s of Object.values(this.services)) n += s.inflight;
    return n;
  }

  private nextSeq() {
    this.seq += 1;
    return this.seq.toString(36);
  }

  dumpDomain() {
    return {
      orders: [...this.orders.values()],
      stock: [...this.stock.entries()].map(([skuId, row]) => ({ skuId, ...row })),
      ledger: this.ledger.slice(),
      incidents: this.incidents.slice(0, 40),
    };
  }

  loadDomain(input: {
    orders: Order[];
    stock: { skuId: string; available: number; reserved: number }[];
    ledger: LedgerEntry[];
    incidents: Incident[];
  }) {
    this.orders.clear();
    for (const o of input.orders) this.orders.set(o.id, o);
    this.orderCount = input.orders.length;
    this.orderOk = input.orders.filter((o) => o.status === "completed").length;
    this.orderFail = input.orders.filter((o) => o.status === "failed").length;
    for (const row of input.stock) {
      this.stock.set(row.skuId, { available: row.available, reserved: row.reserved });
    }
    this.ledger = input.ledger.slice();
    this.incidents = input.incidents.slice();
  }

  private trimArr<T>(arr: T[], cap: number) {
    if (arr.length > cap) arr.length = cap;
  }

  private trimMap<T>(map: Map<string, T>, cap: number) {
    if (map.size <= cap) return;
    const extra = map.size - cap;
    let i = 0;
    for (const key of map.keys()) {
      map.delete(key);
      i += 1;
      if (i >= extra) break;
    }
  }
}

export function bootSnapshot(): Snapshot {
  const engine = new ClusterEngine(0x5eedc0de, 1_720_000_000_000);
  engine.warm(28);
  return engine.snapshot();
}
