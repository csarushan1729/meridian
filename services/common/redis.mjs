import { randomUUID } from 'node:crypto';
import Redis from 'ioredis';
import { sleep } from './util.mjs';

export function makeRedis(url) {
  return new Redis(url, {
    maxRetriesPerRequest: 3,
    retryStrategy: (n) => Math.min(n * 200, 2000),
  });
}

// Delete the lock only if it is still ours. Doing "GET then DEL" from Node would
// have a race, so it runs inside Redis as one atomic Lua script.
const UNLOCK = `
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
else
  return 0
end`;

/**
 * Distributed lock:  SET key token NX PX ttl
 * NX = only if the key does not exist. PX = auto-expire, so a crashed holder
 * cannot block everyone forever.
 */
export async function withLock(redis, key, ttlMs, fn, { tries = 40, waitMs = 25 } = {}) {
  const token = randomUUID();
  for (let i = 0; i < tries; i++) {
    const ok = await redis.set(key, token, 'PX', ttlMs, 'NX');
    if (ok === 'OK') {
      try {
        return await fn();
      } finally {
        await redis.eval(UNLOCK, 1, key, token);
      }
    }
    await sleep(waitMs + Math.random() * waitMs);
  }
  throw new Error(`lock_timeout:${key}`);
}
