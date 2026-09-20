// Small in-memory metrics for one service: request/message counts, error rate,
// latency percentiles over the last few seconds, and how many are running right now.
// Every service exposes it at GET /metrics. The control service collects them.
const pct = (sorted, q) => (sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))] : 0);

export class Metrics {
  constructor({ windowMs = 10_000, maxSamples = 3000 } = {}) {
    this.windowMs = windowMs;
    this.maxSamples = maxSamples;
    this.total = 0;
    this.errors = 0;
    this.inflight = 0;
    this.counters = {};
    this.samples = []; // { t, ms, ok }
  }

  inc(name, n = 1) {
    this.counters[name] = (this.counters[name] ?? 0) + n;
  }

  /** Call start(), then call the returned function with ok=true/false when finished. */
  start() {
    this.inflight += 1;
    const t0 = performance.now();
    return (ok = true) => {
      this.inflight -= 1;
      this.total += 1;
      if (!ok) this.errors += 1;
      const now = Date.now();
      this.samples.push({ t: now, ms: performance.now() - t0, ok });
      const cutoff = now - 60_000;
      while (this.samples.length > this.maxSamples || (this.samples.length && this.samples[0].t < cutoff)) {
        this.samples.shift();
      }
    };
  }

  /** Wrap a Kafka message handler. A thrown error counts as an error (it will be retried). */
  wrap(handler) {
    return async (env, meta) => {
      const done = this.start();
      try {
        const out = await handler(env, meta);
        done(true);
        return out;
      } catch (err) {
        done(false);
        throw err;
      }
    };
  }

  /** Wrap an HTTP action. 4xx (rate limit, bad input) is not an error, 5xx is. */
  async track(fn) {
    const done = this.start();
    try {
      const out = await fn();
      done(true);
      return out;
    } catch (err) {
      done(err.status !== undefined && err.status < 500);
      throw err;
    }
  }

  snapshot(now = Date.now()) {
    const recent = this.samples.filter((s) => now - s.t <= this.windowMs);
    const ms = recent.map((s) => s.ms).sort((a, b) => a - b);
    return {
      total: this.total,
      errors: this.errors,
      inflight: this.inflight,
      rps: recent.length / (this.windowMs / 1000),
      errRate: recent.length ? recent.filter((s) => !s.ok).length / recent.length : 0,
      p50: pct(ms, 0.5),
      p95: pct(ms, 0.95),
      p99: pct(ms, 0.99),
      counters: { ...this.counters },
    };
  }
}

/** GET /metrics route. `extra` can add fields, for example { paused }. */
export function metricsRoute(service, metrics, extra) {
  return [
    'GET',
    '/metrics',
    async () => ({ body: { service, ...metrics.snapshot(), ...((await extra?.()) ?? {}) } }),
  ];
}
