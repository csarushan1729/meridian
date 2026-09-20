export const SCHEMA = `
CREATE TABLE IF NOT EXISTS ledger_entries (
  id       bigserial PRIMARY KEY,
  event_id text NOT NULL,                    -- the Kafka event that caused this entry
  order_id text NOT NULL,
  account  text NOT NULL,                    -- cash | revenue
  debit    bigint NOT NULL DEFAULT 0,
  credit   bigint NOT NULL DEFAULT 0,
  memo     text,
  ts       timestamptz NOT NULL DEFAULT now(),
  UNIQUE (event_id, account)                 -- same event twice = no double booking
);
`;
