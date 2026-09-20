import { randomBytes, randomUUID } from 'node:crypto';

// Every Kafka message value is this JSON envelope:
//   { id, type, traceId, ts, data }
// id      - unique per event (consumers use it to be idempotent)
// traceId - same for all events of one order, so you can follow it in the logs
export const newTraceId = () => randomBytes(8).toString('hex');

export function makeEnvelope({ type, traceId, data }) {
  return { id: randomUUID(), type, traceId: traceId ?? newTraceId(), ts: Date.now(), data: data ?? {} };
}

export function parseEnvelope(raw) {
  const env = JSON.parse(raw);
  if (!env || typeof env.type !== 'string' || typeof env.id !== 'string') {
    throw new Error('invalid envelope: id and type are required');
  }
  return env;
}

/**
 * Run a handler for one raw message with retries.
 *  - bad JSON / bad envelope  -> dead letter straight away (poison message)
 *  - handler throws           -> retry with backoff, then dead letter
 * Returns 'ok' or 'dead'. Consumers commit the offset either way, so one bad
 * message never blocks a partition forever.
 */
export async function deliver({ raw, meta, handler, maxAttempts = 3, backoffMs = 200, sleep, onDead, log }) {
  let env;
  try {
    env = parseEnvelope(raw);
  } catch (err) {
    await onDead({ reason: `poison: ${err.message}`, attempts: 0, raw, meta });
    return 'dead';
  }
  for (let attempt = 1; ; attempt++) {
    try {
      await handler(env, meta);
      return 'ok';
    } catch (err) {
      log?.warn('handler failed', { type: env.type, traceId: env.traceId, attempt, err: err.message });
      if (attempt >= maxAttempts) {
        await onDead({ reason: err.message, attempts: attempt, raw, meta, env });
        return 'dead';
      }
      await sleep(backoffMs * 2 ** (attempt - 1));
    }
  }
}

export function deadLetterEnvelope({ reason, attempts, raw, meta, env }) {
  return makeEnvelope({
    type: 'dead-letter',
    traceId: env?.traceId,
    data: {
      reason,
      attempts,
      groupId: meta.groupId,
      from: { topic: meta.topic, partition: meta.partition, offset: meta.offset },
      original: raw,
    },
  });
}
