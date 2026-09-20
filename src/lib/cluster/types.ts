export type RegionId = "us-east-1" | "eu-west-1" | "ap-south-1";
export type ServiceId =
  | "gateway"
  | "orders"
  | "inventory"
  | "payments"
  | "ledger"
  | "fulfillment"
  | "shipping"
  | "notify";

export type TopicId =
  | "orders.commands"
  | "orders.events"
  | "inventory.events"
  | "payments.events"
  | "fulfillment.events"
  | "shipping.events"
  | "notify.commands"
  | "dead-letter";

export type CircuitState = "closed" | "open" | "half-open";
export type RaftRole = "leader" | "follower" | "candidate";
export type SpanStatus = "ok" | "error" | "unset";
export type OrderStatus =
  | "accepted"
  | "reserving"
  | "charging"
  | "fulfilling"
  | "shipping"
  | "notifying"
  | "completed"
  | "compensating"
  | "failed";

export type PatternId =
  | "saga"
  | "compensation"
  | "circuit-open"
  | "circuit-half-open"
  | "backpressure"
  | "rate-limit"
  | "leader-election"
  | "partition"
  | "outbox"
  | "idempotency"
  | "dlq"
  | "retry";

export interface ChaosConfig {
  traffic: number;
  paymentFail: number;
  inventoryFail: number;
  dropRate: number;
  killed: Partial<Record<ServiceId, boolean>>;
  latencyMult: Record<ServiceId, number>;
  partitioned: Partial<Record<RegionId, boolean>>;
  clockSkew: number;
}

export interface Sku {
  id: string;
  name: string;
  price: number;
  weight: number;
}

export interface Order {
  id: string;
  skuId: string;
  skuName: string;
  qty: number;
  amount: number;
  region: RegionId;
  status: OrderStatus;
  idempotencyKey: string;
  traceId: string;
  createdAt: number;
  updatedAt: number;
  steps: { name: string; at: number; ok: boolean; detail: string }[];
  compensateReason?: string;
}

export interface Span {
  spanId: string;
  traceId: string;
  parentSpanId: string | null;
  service: ServiceId | "client";
  name: string;
  start: number;
  end: number | null;
  status: SpanStatus;
  detail: string;
}

export interface Trace {
  traceId: string;
  orderId?: string;
  rootService: ServiceId | "client";
  start: number;
  end: number | null;
  status: SpanStatus;
  spans: Span[];
}

export interface BusEvent {
  offset: number;
  partition: number;
  topic: TopicId;
  key: string;
  type: string;
  ts: number;
  traceId: string;
  payload: string;
}

export interface TopicSnapshot {
  id: TopicId;
  partitions: {
    id: number;
    produced: number;
    depth: number;
    lastType: string;
  }[];
  produced: number;
  rate: number;
}

export interface ConsumerSnapshot {
  id: string;
  topic: TopicId;
  lag: number;
  rate: number;
}

export interface ServiceSnapshot {
  id: ServiceId;
  title: string;
  region: RegionId;
  rps: number;
  errRate: number;
  p50: number;
  p95: number;
  p99: number;
  inflight: number;
  maxInflight: number;
  status: "healthy" | "degraded" | "down";
  circuit: CircuitState;
  breakerName: string;
}

export interface EdgeSnapshot {
  from: ServiceId | "client";
  to: ServiceId;
  rps: number;
  errRate: number;
  circuit: CircuitState;
}

export interface RaftNode {
  id: RegionId;
  role: RaftRole;
  term: number;
  lastHeartbeat: number;
  logIndex: number;
  lagMs: number;
  votes: number;
  partitioned: boolean;
}

export interface Incident {
  id: string;
  at: number;
  severity: "info" | "warn" | "crit";
  title: string;
  detail: string;
  pattern: PatternId;
  service?: ServiceId;
  region?: RegionId;
}

export interface MetricSample {
  t: number;
  rps: number;
  errors: number;
  p99: number;
  inflight: number;
}

export interface LedgerEntry {
  id: string;
  at: number;
  orderId: string;
  kind: "capture" | "refund";
  amount: number;
}

export interface StockRow {
  skuId: string;
  name: string;
  available: number;
  reserved: number;
}

export interface PatternFlag {
  id: PatternId;
  label: string;
  active: boolean;
  hint: string;
}

export interface PlatformSnapshot {
  dbBackend: "pglite" | "neon" | "memory";
  kafka: {
    name: string;
    implementation: string;
    topics: number;
    partitions: number;
    messages: number;
    persisted: number;
    consumerGroups: number;
    dlq: number;
    walDepth: number;
  };
  redis: {
    name: string;
    implementation: string;
    keys: number;
    hits: number;
    misses: number;
    hitRate: number;
    ops: number;
    locks: number;
    idempotencyKeys: number;
    expired: number;
  };
  redisKeys: { key: string; kind: string; ttl: number | null }[];
  postgres: {
    orders: number;
    inventory: number;
    ledger: number;
    kafkaLog: number;
    redisKeys: number;
  };
  aws: { layer: string; here: string; aws: string }[];
}

export interface Snapshot {
  now: number;
  startedAt: number;
  headline: string;
  rps: number;
  errorRate: number;
  availability: number;
  p50: number;
  p95: number;
  p99: number;
  inflight: number;
  errorBudget: number;
  burnRate: number;
  ordersTotal: number;
  ordersOk: number;
  ordersFailed: number;
  sagasInFlight: number;
  shed: number;
  rateLimited: number;
  dlq: number;
  leader: RegionId;
  term: number;
  services: ServiceSnapshot[];
  edges: EdgeSnapshot[];
  orders: Order[];
  traces: Trace[];
  topics: TopicSnapshot[];
  consumers: ConsumerSnapshot[];
  recentEvents: BusEvent[];
  raft: RaftNode[];
  incidents: Incident[];
  metrics: MetricSample[];
  ledger: LedgerEntry[];
  ledgerNet: number;
  stock: StockRow[];
  chaos: ChaosConfig;
  patterns: PatternFlag[];
  ticker: string[];
  platform: PlatformSnapshot;
}
