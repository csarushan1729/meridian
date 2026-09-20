/**
 * Redis-compatible in-process store.
 *
 * Strings, hashes, SETNX (locks), INCR, TTL. Used for idempotency keys,
 * inventory cache, rate-limit counters, and SKU reservation locks.
 *
 * AWS swap: replace with ioredis against ElastiCache. The command surface
 * below is a deliberate subset of Redis so that swap is mechanical.
 */

type RedisKind = "string" | "hash";

type Entry = {
  kind: RedisKind;
  value: string | Record<string, string>;
  expiresAt: number | null;
};

export type RedisDump = {
  key: string;
  kind: RedisKind;
  value: string;
  expiresAt: number | null;
};

export class RedisStore {
  readonly name = "helix-redis";
  readonly implementation = "Redis-compatible keystore (AOF → Postgres)";
  private readonly data = new Map<string, Entry>();
  private dirty = new Set<string>();
  hits = 0;
  misses = 0;
  ops = 0;
  expired = 0;

  private now() {
    return Date.now();
  }

  private alive(key: string, e: Entry | undefined): Entry | undefined {
    if (!e) return undefined;
    if (e.expiresAt != null && e.expiresAt <= this.now()) {
      this.data.delete(key);
      this.dirty.add(key);
      this.expired += 1;
      return undefined;
    }
    return e;
  }

  get(key: string): string | null {
    this.ops += 1;
    const e = this.alive(key, this.data.get(key));
    if (!e || e.kind !== "string") {
      this.misses += 1;
      return null;
    }
    this.hits += 1;
    return e.value as string;
  }

  set(key: string, value: string, pxMs?: number) {
    this.ops += 1;
    this.data.set(key, {
      kind: "string",
      value,
      expiresAt: pxMs != null ? this.now() + pxMs : null,
    });
    this.dirty.add(key);
  }

  /** SET key value NX PX ms — used for idempotency and distributed locks. */
  setnx(key: string, value: string, pxMs?: number): boolean {
    this.ops += 1;
    if (this.alive(key, this.data.get(key))) return false;
    this.data.set(key, {
      kind: "string",
      value,
      expiresAt: pxMs != null ? this.now() + pxMs : null,
    });
    this.dirty.add(key);
    return true;
  }

  del(key: string): boolean {
    this.ops += 1;
    const had = this.data.delete(key);
    if (had) this.dirty.add(key);
    return had;
  }

  incr(key: string): number {
    this.ops += 1;
    const cur = this.alive(key, this.data.get(key));
    const n = (cur && cur.kind === "string" ? Number(cur.value) || 0 : 0) + 1;
    this.data.set(key, {
      kind: "string",
      value: String(n),
      expiresAt: cur?.expiresAt ?? null,
    });
    this.dirty.add(key);
    return n;
  }

  hset(key: string, field: string, value: string) {
    this.ops += 1;
    const cur = this.alive(key, this.data.get(key));
    const hash =
      cur && cur.kind === "hash" ? { ...(cur.value as Record<string, string>) } : {};
    hash[field] = value;
    this.data.set(key, { kind: "hash", value: hash, expiresAt: cur?.expiresAt ?? null });
    this.dirty.add(key);
  }

  hget(key: string, field: string): string | null {
    this.ops += 1;
    const e = this.alive(key, this.data.get(key));
    if (!e || e.kind !== "hash") {
      this.misses += 1;
      return null;
    }
    this.hits += 1;
    return (e.value as Record<string, string>)[field] ?? null;
  }

  hgetall(key: string): Record<string, string> {
    this.ops += 1;
    const e = this.alive(key, this.data.get(key));
    if (!e || e.kind !== "hash") {
      this.misses += 1;
      return {};
    }
    this.hits += 1;
    return { ...(e.value as Record<string, string>) };
  }

  ttl(key: string): number | null {
    const e = this.alive(key, this.data.get(key));
    if (!e) return null;
    if (e.expiresAt == null) return -1;
    return Math.max(0, e.expiresAt - this.now());
  }

  scan(prefix = "", limit = 32): { key: string; kind: RedisKind; ttl: number | null }[] {
    const out: { key: string; kind: RedisKind; ttl: number | null }[] = [];
    for (const [key, raw] of this.data) {
      if (prefix && !key.startsWith(prefix)) continue;
      const e = this.alive(key, raw);
      if (!e) continue;
      out.push({
        key,
        kind: e.kind,
        ttl: e.expiresAt == null ? null : Math.max(0, e.expiresAt - this.now()),
      });
      if (out.length >= limit) break;
    }
    return out;
  }

  takeDirty(): RedisDump[] {
    const rows: RedisDump[] = [];
    for (const key of this.dirty) {
      const e = this.data.get(key);
      if (!e) {
        rows.push({ key, kind: "string", value: "", expiresAt: 0 });
        continue;
      }
      rows.push({
        key,
        kind: e.kind,
        value: typeof e.value === "string" ? e.value : JSON.stringify(e.value),
        expiresAt: e.expiresAt,
      });
    }
    this.dirty.clear();
    return rows;
  }

  load(rows: RedisDump[]) {
    for (const row of rows) {
      if (row.expiresAt === 0 && row.value === "") continue;
      const value =
        row.kind === "hash"
          ? (JSON.parse(row.value) as Record<string, string>)
          : row.value;
      this.data.set(row.key, { kind: row.kind, value, expiresAt: row.expiresAt });
    }
  }

  stats() {
    let locks = 0;
    let idem = 0;
    for (const key of this.data.keys()) {
      if (key.startsWith("lock:")) locks += 1;
      if (key.startsWith("idem:")) idem += 1;
    }
    const lookups = this.hits + this.misses;
    return {
      name: this.name,
      implementation: this.implementation,
      keys: this.data.size,
      hits: this.hits,
      misses: this.misses,
      hitRate: lookups ? this.hits / lookups : 1,
      ops: this.ops,
      locks,
      idempotencyKeys: idem,
      expired: this.expired,
    };
  }
}
