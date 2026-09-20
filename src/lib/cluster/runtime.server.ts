import { SKUS } from "@/lib/cluster/catalog";
import { ClusterEngine } from "@/lib/cluster/engine";
import type {
  Incident,
  LedgerEntry,
  Order,
  Snapshot,
  TopicId,
} from "@/lib/cluster/types";

type Sql = import("@/lib/db").Sql;

type GlobalRt = typeof globalThis & {
  __helixRuntimePromise__?: Promise<HelixRuntime>;
};

async function trySql(): Promise<{ sql: Sql; backend: "pglite" | "neon" } | null> {
  try {
    const db = await import("@/lib/db");
    const sql = await db.getSql();
    return { sql, backend: db.dbSource };
  } catch (err) {
    console.error("[helix] database unavailable — running in-memory", err);
    return null;
  }
}

class HelixRuntime {
  readonly engine: ClusterEngine;
  private persistTimer: ReturnType<typeof setInterval> | null = null;
  private pg = { orders: 0, inventory: 0, ledger: 0, kafkaLog: 0, redisKeys: 0 };
  private flushing = false;
  private sql: Sql | null = null;
  private backend: "pglite" | "neon" | "memory" = "memory";

  private constructor(engine: ClusterEngine) {
    this.engine = engine;
  }

  static async create(): Promise<HelixRuntime> {
    const db = await trySql();
    const engine = new ClusterEngine(0x5eedc0de ^ (Date.now() & 0xffff), Date.now());
    if (db) await hydrate(engine, db.sql);
    const rt = new HelixRuntime(engine);
    rt.sql = db?.sql ?? null;
    rt.backend = db?.backend ?? "memory";
    const restored = engine.kafka.stats().messages > 0;
    engine.start(() => undefined, { warm: restored ? 0 : 28 });
    if (rt.sql) {
      rt.persistTimer = setInterval(() => {
        void rt.flush();
      }, 450);
      void rt.flush();
    }
    return rt;
  }

  snapshot(): Snapshot {
    const snap = this.engine.snapshot();
    snap.platform.dbBackend = this.backend;
    snap.platform.postgres = { ...this.pg };
    snap.platform.kafka.persisted = this.engine.kafka.persisted;
    return snap;
  }

  private async flush() {
    if (this.flushing || !this.sql) return;
    this.flushing = true;
    try {
      const sql = this.sql;
      const engine = this.engine;
      const wal = engine.kafka.takeWal();
      for (const rec of wal) {
        await sql.query(
          `insert into kafka_log (topic, partition, msg_offset, key, type, value, trace_id, ts)
           values ($1,$2,$3,$4,$5,$6,$7,$8)
           on conflict (topic, partition, msg_offset) do nothing`,
          [rec.topic, rec.partition, rec.offset, rec.key, rec.type, rec.payload, rec.traceId, rec.ts],
        );
        engine.kafka.persisted += 1;
      }
      for (const off of engine.kafka.offsetDump()) {
        await sql.query(
          `insert into kafka_offsets (group_id, topic, partition, committed)
           values ($1,$2,$3,$4)
           on conflict (group_id, topic, partition) do update set committed = excluded.committed`,
          [off.groupId, off.topic, off.partition, off.committed],
        );
      }
      for (const row of engine.redis.takeDirty()) {
        if (row.expiresAt === 0 && row.value === "") {
          await sql.query(`delete from redis_kv where key = $1`, [row.key]);
          continue;
        }
        await sql.query(
          `insert into redis_kv (key, kind, value, expires_at)
           values ($1,$2,$3,$4)
           on conflict (key) do update set kind = excluded.kind, value = excluded.value, expires_at = excluded.expires_at`,
          [row.key, row.kind, row.value, row.expiresAt],
        );
      }
      const domain = engine.dumpDomain();
      for (const o of domain.orders.slice(0, 80)) {
        await sql.query(
          `insert into helix_orders
             (id, sku_id, sku_name, qty, amount, region, status, idempotency_key, trace_id, created_at, updated_at, steps, compensate_reason)
           values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13)
           on conflict (id) do update set
             status = excluded.status,
             updated_at = excluded.updated_at,
             steps = excluded.steps,
             compensate_reason = excluded.compensate_reason`,
          [
            o.id,
            o.skuId,
            o.skuName,
            o.qty,
            o.amount,
            o.region,
            o.status,
            o.idempotencyKey,
            o.traceId,
            o.createdAt,
            o.updatedAt,
            JSON.stringify(o.steps),
            o.compensateReason ?? null,
          ],
        );
      }
      for (const row of domain.stock) {
        const skuName = SKUS.find((s) => s.id === row.skuId)?.name ?? row.skuId;
        await sql.query(
          `insert into helix_inventory (sku_id, name, available, reserved)
           values ($1,$2,$3,$4)
           on conflict (sku_id) do update set available = excluded.available, reserved = excluded.reserved`,
          [row.skuId, skuName, row.available, row.reserved],
        );
      }
      for (const e of domain.ledger.slice(0, 40)) {
        await sql.query(
          `insert into helix_ledger (id, at, order_id, kind, amount)
           values ($1,$2,$3,$4,$5)
           on conflict (id) do nothing`,
          [e.id, e.at, e.orderId, e.kind, e.amount],
        );
      }
      for (const inc of domain.incidents.slice(0, 24)) {
        await sql.query(
          `insert into helix_incidents (id, at, severity, title, detail, pattern, service, region)
           values ($1,$2,$3,$4,$5,$6,$7,$8)
           on conflict (id) do nothing`,
          [inc.id, inc.at, inc.severity, inc.title, inc.detail, inc.pattern, inc.service ?? null, inc.region ?? null],
        );
      }
      const counts = await sql.query<{
        orders: number;
        inventory: number;
        ledger: number;
        kafka_log: number;
        redis_keys: number;
      }>(
        `select
           (select count(*) from helix_orders) as orders,
           (select count(*) from helix_inventory) as inventory,
           (select count(*) from helix_ledger) as ledger,
           (select count(*) from kafka_log) as kafka_log,
           (select count(*) from redis_kv) as redis_keys`,
      );
      const row = counts[0];
      if (row) {
        this.pg = {
          orders: Number(row.orders) || 0,
          inventory: Number(row.inventory) || 0,
          ledger: Number(row.ledger) || 0,
          kafkaLog: Number(row.kafka_log) || 0,
          redisKeys: Number(row.redis_keys) || 0,
        };
      }
    } catch (err) {
      console.error("[helix] persist flush failed", err);
    } finally {
      this.flushing = false;
    }
  }
}

async function hydrate(engine: ClusterEngine, sql: Sql) {
  try {
    const log = await sql.query<{
      topic: TopicId;
      partition: number;
      msg_offset: number;
      key: string;
      type: string;
      value: string;
      trace_id: string;
      ts: number;
    }>(
      `select topic, partition, msg_offset, key, type, value, trace_id, ts
       from kafka_log order by ts asc, msg_offset asc limit 400`,
    );
    const offs = await sql.query<{
      group_id: string;
      topic: TopicId;
      partition: number;
      committed: number;
    }>(`select group_id, topic, partition, committed from kafka_offsets`);
    engine.kafka.load(
      log.map((r) => ({
        topic: r.topic,
        partition: r.partition,
        offset: r.msg_offset,
        key: r.key,
        type: r.type,
        payload: r.value,
        traceId: r.trace_id,
        ts: r.ts,
      })),
      offs.map((r) => ({
        groupId: r.group_id,
        topic: r.topic,
        partition: r.partition,
        committed: r.committed,
      })),
    );

    const kv = await sql.query<{
      key: string;
      kind: "string" | "hash";
      value: string;
      expires_at: number | null;
    }>(`select key, kind, value, expires_at from redis_kv`);
    engine.redis.load(
      kv.map((r) => ({
        key: r.key,
        kind: r.kind,
        value: r.value,
        expiresAt: r.expires_at,
      })),
    );

    const orders = await sql.query<{
      id: string;
      sku_id: string;
      sku_name: string;
      qty: number;
      amount: number;
      region: Order["region"];
      status: Order["status"];
      idempotency_key: string;
      trace_id: string;
      created_at: number;
      updated_at: number;
      steps: Order["steps"] | string;
      compensate_reason: string | null;
    }>(
      `select id, sku_id, sku_name, qty, amount, region, status, idempotency_key, trace_id, created_at, updated_at, steps, compensate_reason
       from helix_orders order by created_at desc limit 80`,
    );
    const stock = await sql.query<{ sku_id: string; available: number; reserved: number }>(
      `select sku_id, available, reserved from helix_inventory`,
    );
    const ledger = await sql.query<{
      id: string;
      at: number;
      order_id: string;
      kind: LedgerEntry["kind"];
      amount: number;
    }>(`select id, at, order_id, kind, amount from helix_ledger order by at desc limit 40`);
    const incidents = await sql.query<{
      id: string;
      at: number;
      severity: Incident["severity"];
      title: string;
      detail: string;
      pattern: Incident["pattern"];
      service: Incident["service"];
      region: Incident["region"];
    }>(
      `select id, at, severity, title, detail, pattern, service, region
       from helix_incidents order by at desc limit 40`,
    );

    engine.loadDomain({
      orders: orders.map((o) => ({
        id: o.id,
        skuId: o.sku_id,
        skuName: o.sku_name,
        qty: o.qty,
        amount: o.amount,
        region: o.region,
        status: o.status,
        idempotencyKey: o.idempotency_key,
        traceId: o.trace_id,
        createdAt: o.created_at,
        updatedAt: o.updated_at,
        steps: typeof o.steps === "string" ? JSON.parse(o.steps) : o.steps ?? [],
        compensateReason: o.compensate_reason ?? undefined,
      })),
      stock: stock.map((s) => ({ skuId: s.sku_id, available: s.available, reserved: s.reserved })),
      ledger: ledger.map((e) => ({
        id: e.id,
        at: e.at,
        orderId: e.order_id,
        kind: e.kind,
        amount: e.amount,
      })),
      incidents: incidents.map((i) => ({
        id: i.id,
        at: i.at,
        severity: i.severity,
        title: i.title,
        detail: i.detail,
        pattern: i.pattern,
        service: i.service ?? undefined,
        region: i.region ?? undefined,
      })),
    });
  } catch (err) {
    console.error("[helix] hydrate skipped", err);
  }
}

export function getRuntime(): Promise<HelixRuntime> {
  const g = globalThis as GlobalRt;
  g.__helixRuntimePromise__ ??= HelixRuntime.create().catch((err) => {
    g.__helixRuntimePromise__ = undefined;
    throw err;
  });
  return g.__helixRuntimePromise__;
}
