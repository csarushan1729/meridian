import { tx } from '../common/db.mjs';
import { TOPICS } from '../common/topics.mjs';
import { SCHEMA } from './schema.mjs';

/**
 * Double-entry ledger. Every event creates two rows: one debit, one credit, same amount.
 * So total debits must always equal total credits. GET /summary checks that.
 *   payment.captured -> debit cash,    credit revenue
 *   payment.refunded -> debit revenue, credit cash
 */
export function createLedger({ pool, log }) {
  async function book(env, rows, memo) {
    await tx(pool, async (c) => {
      for (const r of rows) {
        await c.query(
          `INSERT INTO ledger_entries (event_id, order_id, account, debit, credit, memo)
           VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (event_id, account) DO NOTHING`,
          [env.id, env.data.orderId, r.account, r.debit, r.credit, memo],
        );
      }
    });
    log.info('booked', { orderId: env.data.orderId, traceId: env.traceId, memo });
  }

  const handlers = {
    'payment.captured': (env) =>
      book(
        env,
        [
          { account: 'cash', debit: env.data.amount, credit: 0 },
          { account: 'revenue', debit: 0, credit: env.data.amount },
        ],
        'payment captured',
      ),
    'payment.refunded': (env) =>
      book(
        env,
        [
          { account: 'revenue', debit: env.data.amount, credit: 0 },
          { account: 'cash', debit: 0, credit: env.data.amount },
        ],
        'payment refunded',
      ),
  };

  async function handle(env) {
    const h = handlers[env.type];
    if (h) await h(env);
  }

  const routes = [
    ['GET', '/health', async () => ({ body: { ok: true, service: 'ledger' } })],
    [
      'GET',
      '/summary',
      async () => {
        const t = (
          await pool.query(
            'SELECT count(*)::int AS entries, coalesce(sum(debit),0)::bigint AS debit, coalesce(sum(credit),0)::bigint AS credit FROM ledger_entries',
          )
        ).rows[0];
        const accounts = (
          await pool.query(
            'SELECT account, coalesce(sum(debit),0)::bigint AS debit, coalesce(sum(credit),0)::bigint AS credit FROM ledger_entries GROUP BY account ORDER BY account',
          )
        ).rows;
        return {
          body: {
            entries: t.entries,
            totalDebit: Number(t.debit),
            totalCredit: Number(t.credit),
            balanced: Number(t.debit) === Number(t.credit),
            accounts: accounts.map((a) => ({ account: a.account, debit: Number(a.debit), credit: Number(a.credit) })),
          },
        };
      },
    ],
    [
      'GET',
      '/entries',
      async ({ query }) => {
        const limit = Math.min(Number(query.get('limit')) || 50, 200);
        const { rows } = await pool.query('SELECT * FROM ledger_entries ORDER BY id DESC LIMIT $1', [limit]);
        return { body: { entries: rows } };
      },
    ],
  ];

  return { migrate: () => pool.query(SCHEMA), handle, routes, topics: [TOPICS.PAYMENTS_EVENTS], groupId: 'ledger' };
}
