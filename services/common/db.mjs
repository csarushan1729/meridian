import pg from 'pg';
import { sleep } from './util.mjs';

export function makePool(connectionString) {
  return new pg.Pool({ connectionString, max: 10 });
}

export async function waitForDb(pool, log, tries = 40) {
  for (let i = 1; i <= tries; i++) {
    try {
      await pool.query('select 1');
      return;
    } catch (err) {
      log.warn('waiting for postgres', { attempt: i, err: err.message });
      await sleep(1000);
    }
  }
  throw new Error('postgres not reachable');
}

/** Run fn inside one database transaction. */
export async function tx(pool, fn) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const out = await fn(client);
    await client.query('COMMIT');
    return out;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}
