// Database owned by the orders service only (database-per-service).
export const SCHEMA = `
CREATE TABLE IF NOT EXISTS orders (
  id               text PRIMARY KEY,
  sku_id           text NOT NULL,
  qty              int NOT NULL,
  amount           bigint NOT NULL,
  status           text NOT NULL,          -- RESERVING | PAYING | CONFIRMED | CANCELLED
  reason           text,
  idempotency_key  text,
  trace_id         text,
  refund_requested boolean NOT NULL DEFAULT false,
  step_deadline    timestamptz,            -- if the saga is still waiting after this, we compensate
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS orders_deadline_idx ON orders (step_deadline) WHERE status IN ('RESERVING','PAYING');
CREATE INDEX IF NOT EXISTS orders_created_idx ON orders (created_at DESC);

CREATE TABLE IF NOT EXISTS order_steps (
  id       bigserial PRIMARY KEY,
  order_id text NOT NULL,
  name     text NOT NULL,
  status   text NOT NULL,                  -- ok | warn
  detail   text,
  at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS order_steps_order_idx ON order_steps (order_id, id);

-- Transactional outbox: the state change and the messages to send are saved in ONE
-- database transaction. A relay loop publishes them to Kafka afterwards.
CREATE TABLE IF NOT EXISTS outbox (
  id         bigserial PRIMARY KEY,
  topic      text NOT NULL,
  key        text NOT NULL,
  envelope   jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  sent_at    timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_unsent_idx ON outbox (id) WHERE sent_at IS NULL;
`;
