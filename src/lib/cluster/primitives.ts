export function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export function fnv1a(input: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    h ^= input.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

export function hexId(rng: () => number, bytes: number): string {
  let out = "";
  for (let i = 0; i < bytes; i++) {
    out += Math.floor(rng() * 256)
      .toString(16)
      .padStart(2, "0");
  }
  return out;
}

export function pickWeighted<T extends { weight: number }>(
  rng: () => number,
  items: T[],
): T {
  let total = 0;
  for (const item of items) total += item.weight;
  let cursor = rng() * total;
  for (const item of items) {
    cursor -= item.weight;
    if (cursor <= 0) return item;
  }
  return items[items.length - 1]!;
}

export function lognormal(rng: () => number, median: number, sigma = 0.35): number {
  const u1 = Math.max(rng(), 1e-9);
  const u2 = rng();
  const z = Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2);
  return Math.max(0.4, median * Math.exp(sigma * z));
}

export class Ewma {
  value = 0;
  private initialized = false;
  constructor(private readonly alpha: number) {}
  push(x: number): number {
    if (!this.initialized) {
      this.value = x;
      this.initialized = true;
      return x;
    }
    this.value = this.alpha * x + (1 - this.alpha) * this.value;
    return this.value;
  }
}

export class Histogram {
  private readonly samples: number[] = [];
  constructor(private readonly cap = 240) {}
  push(x: number) {
    this.samples.push(x);
    if (this.samples.length > this.cap) this.samples.shift();
  }
  percentile(p: number): number {
    if (this.samples.length === 0) return 0;
    const sorted = this.samples.slice().sort((a, b) => a - b);
    const idx = Math.min(sorted.length - 1, Math.max(0, Math.ceil(p * sorted.length) - 1));
    return sorted[idx]!;
  }
  get count() {
    return this.samples.length;
  }
}

export class TokenBucket {
  tokens: number;
  private last: number;
  constructor(
    public readonly capacity: number,
    public readonly refillPerSec: number,
    now: number,
  ) {
    this.tokens = capacity;
    this.last = now;
  }
  take(now: number, n = 1): boolean {
    const elapsed = Math.max(0, (now - this.last) / 1000);
    this.tokens = Math.min(this.capacity, this.tokens + elapsed * this.refillPerSec);
    this.last = now;
    if (this.tokens >= n) {
      this.tokens -= n;
      return true;
    }
    return false;
  }
}

export class CircuitBreaker {
  state: "closed" | "open" | "half-open" = "closed";
  private window: boolean[] = [];
  private openedAt = 0;
  private probes = 0;
  constructor(
    private readonly size = 24,
    private readonly minSamples = 8,
    private readonly threshold = 0.5,
    private readonly resetMs = 4200,
    private readonly maxProbes = 2,
  ) {}

  allow(now: number): boolean {
    if (this.state === "open") {
      if (now - this.openedAt >= this.resetMs) {
        this.state = "half-open";
        this.probes = 0;
      } else {
        return false;
      }
    }
    if (this.state === "half-open") {
      if (this.probes >= this.maxProbes) return false;
      this.probes += 1;
    }
    return true;
  }

  success() {
    this.record(true);
    if (this.state === "half-open") {
      this.state = "closed";
      this.window = [];
      this.probes = 0;
    }
  }

  fail(now: number) {
    this.record(false);
    if (this.state === "half-open") {
      this.trip(now);
      return;
    }
    if (this.window.length >= this.minSamples) {
      const fails = this.window.filter((ok) => !ok).length;
      if (fails / this.window.length >= this.threshold) this.trip(now);
    }
  }

  private record(ok: boolean) {
    this.window.push(ok);
    if (this.window.length > this.size) this.window.shift();
  }

  private trip(now: number) {
    this.state = "open";
    this.openedAt = now;
    this.probes = 0;
  }
}

export class HashRing {
  private readonly ring: { hash: number; node: number }[] = [];
  constructor(nodes: number, vnodes = 48) {
    for (let n = 0; n < nodes; n++) {
      for (let v = 0; v < vnodes; v++) {
        this.ring.push({ hash: fnv1a(`${n}:${v}`), node: n });
      }
    }
    this.ring.sort((a, b) => a.hash - b.hash);
  }
  node(key: string, nodes: number): number {
    if (nodes <= 1) return 0;
    const h = fnv1a(key);
    let lo = 0;
    let hi = this.ring.length - 1;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (this.ring[mid]!.hash < h) lo = mid + 1;
      else hi = mid;
    }
    return this.ring[lo]!.node % nodes;
  }
}

export class LruSet {
  private readonly map = new Map<string, number>();
  constructor(private readonly cap = 400) {}
  has(key: string): boolean {
    return this.map.has(key);
  }
  add(key: string, now: number) {
    if (this.map.has(key)) this.map.delete(key);
    this.map.set(key, now);
    if (this.map.size > this.cap) {
      const first = this.map.keys().next().value;
      if (first !== undefined) this.map.delete(first);
    }
  }
}
