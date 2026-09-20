-- Helix commerce cluster: Kafka WAL, Redis keyspace, domain tables.
-- Swap: kafka_log → MSK, redis_kv → ElastiCache, helix_* → RDS.

create table if not exists kafka_log (
  topic       text not null,
  partition   int not null,
  msg_offset  bigint not null,
  key         text not null,
  type        text not null,
  value       text not null,
  trace_id    text not null,
  ts          bigint not null,
  primary key (topic, partition, msg_offset)
);
create index if not exists kafka_log_topic_ts on kafka_log (topic, ts);

create table if not exists kafka_offsets (
  group_id  text not null,
  topic     text not null,
  partition int not null,
  committed bigint not null,
  primary key (group_id, topic, partition)
);

create table if not exists redis_kv (
  key        text primary key,
  kind       text not null,
  value      text not null,
  expires_at bigint
);

create table if not exists helix_orders (
  id                text primary key,
  sku_id            text not null,
  sku_name          text not null,
  qty               int not null,
  amount            int not null,
  region            text not null,
  status            text not null,
  idempotency_key   text not null,
  trace_id          text not null,
  created_at        bigint not null,
  updated_at        bigint not null,
  steps             jsonb not null default '[]',
  compensate_reason text
);
create unique index if not exists helix_orders_idem_idx on helix_orders (idempotency_key);

create table if not exists helix_inventory (
  sku_id    text primary key,
  name      text not null,
  available int not null,
  reserved  int not null
);

create table if not exists helix_ledger (
  id       text primary key,
  at       bigint not null,
  order_id text not null,
  kind     text not null,
  amount   int not null
);

create table if not exists helix_incidents (
  id       text primary key,
  at       bigint not null,
  severity text not null,
  title    text not null,
  detail   text not null,
  pattern  text not null,
  service  text,
  region   text
);
