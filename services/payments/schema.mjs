export const SCHEMA = `
CREATE TABLE IF NOT EXISTS payments (
  order_id   text PRIMARY KEY,               -- one payment per order = idempotent
  amount     bigint NOT NULL,
  status     text NOT NULL,                  -- CAPTURED | DECLINED | REFUNDED
  psp_ref    text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
`;
