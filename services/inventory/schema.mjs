export const SCHEMA = `
CREATE TABLE IF NOT EXISTS stock (
  sku_id    text PRIMARY KEY,
  name      text NOT NULL,
  available int NOT NULL CHECK (available >= 0),
  reserved  int NOT NULL DEFAULT 0 CHECK (reserved >= 0)
);
CREATE TABLE IF NOT EXISTS reservations (
  order_id   text PRIMARY KEY,              -- one reservation per order = idempotent
  sku_id     text NOT NULL,
  qty        int NOT NULL,
  status     text NOT NULL,                 -- RESERVED | REJECTED | RELEASED
  reason     text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
`;
