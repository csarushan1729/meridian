import { sleep } from './util.mjs';

// Chaos knobs live in Redis, so you can change them while the system is running:
//   redis-cli set chaos:payments:latency_ms 3000
//   redis-cli set chaos:payments:crashrate 0.5
//   redis-cli del chaos:payments:latency_ms
export async function chaosNumber(redis, service, name, fallback = 0) {
  const raw = await redis.get(`chaos:${service}:${name}`);
  if (raw == null) return fallback;
  const n = Number(raw);
  return Number.isFinite(n) ? n : fallback;
}

/** Adds fake latency and random crashes (a thrown error -> retries -> dead letter). */
export async function applyChaos(redis, service) {
  const latency = await chaosNumber(redis, service, 'latency_ms', 0);
  if (latency > 0) await sleep(latency);
  const crash = await chaosNumber(redis, service, 'crashrate', 0);
  if (crash > 0 && Math.random() < crash) throw new Error(`chaos: simulated crash in ${service}`);
}
