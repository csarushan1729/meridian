# Meridian

A simulated multi-region commerce control plane — eight services, a Kafka-style event bus, a Redis-style store, and Postgres, all coordinated through sagas, circuit breakers, and Raft-style leader election.

**Live demo:** [meridian-demo.duckdns.org](https://meridian-demo.duckdns.org/)

![Stack](https://img.shields.io/badge/stack-TanStack%20Start%20%C2%B7%20React%2019%20%C2%B7%20Postgres-0a0b0d)

---

## What this is

Meridian models what happens when an order is placed on a commerce platform running across three regions (`us-east-1`, `eu-west-1`, `ap-south-1`), with independent services for admission, order orchestration, inventory, payments, fulfillment, shipping, and notifications.

It's built to make distributed-systems failure modes visible and interactive, rather than just diagrammed:

- **Sagas with compensation** — the order flow (`reserve → capture → allocate → ship → notify`) rolls back cleanly on a mid-flight failure: inventory is released, payments are refunded, and the saga's compensating steps are visible in the trace.
- **Circuit breakers** — each service tracks its own error rate and opens/half-opens/closes independently under load.
- **Event bus semantics** — a partitioned log (multiple topics, consumer groups, offsets, lag) sits underneath the services, with a dead-letter topic for poison messages.
- **Idempotency & locking** — a key-value store handles idempotency keys and SKU-level locks the way Redis would in production.
- **Raft-style leader election** — one of the simulated brokers models leader/follower state and failover.
- **Chaos mode** — kill a service (e.g. payments) mid-flight and watch the breaker open, the saga compensate, and the dead-letter queue absorb the fallout in real time.
- **Distributed tracing** — every order produces a trace you can click through, span by span, across all eight services.

The event bus and key-value store are custom implementations that mirror Kafka's and Redis's semantics (partitions/consumer groups/offsets; TTLs/locks), not managed instances of Kafka or Redis. In production, the same interfaces would point at something like MSK/Kinesis and ElastiCache — that swap is the intended design boundary.

## Stack

- **Frontend/runtime:** TanStack Start, React 19, Tailwind
- **Persistence:** Postgres (Neon in production, PGLite for local dev)
- **Services:** 8 independent Node processes (gateway, orders, inventory, payments, ledger, fulfillment, shipping, notify)
- **Deployment:** Dockerized, running behind Caddy with HTTPS

## Architecture

```
                     ┌────────────┐
   client ──────────▶│  gateway   │  admission · token bucket
                     └─────┬──────┘
                           │
                     ┌─────▼──────┐
                     │   orders   │  saga orchestrator · outbox
                     └─────┬──────┘
              ┌────────────┼────────────┬─────────────┐
        ┌─────▼─────┐ ┌────▼─────┐ ┌────▼──────┐ ┌─────▼─────┐
        │ inventory │ │ payments │ │  ledger   │ │fulfillment│
        └───────────┘ └──────────┘ └───────────┘ └─────┬─────┘
                                                   ┌─────▼─────┐
                                                   │ shipping  │
                                                   └─────┬─────┘
                                                   ┌─────▼─────┐
                                                   │  notify   │
                                                   └───────────┘

   all services publish to / consume from a partitioned event bus
   (orders.commands, orders.events, inventory.events, payments.events,
    fulfillment.events, shipping.events, notify.commands, dead-letter)
```

## Running locally

```bash
git clone https://github.com/csarushan1729/meridian.git
cd meridian
npm install
npm run dev
```

Open `http://localhost:8080`. Postgres is optional locally — the app falls back to PGLite. No login required, no external services needed to explore the demo.

To persist data (orders, event log, KV store) across restarts, set `DATABASE_URL` to a Postgres connection string (e.g. from [Neon](https://neon.tech)).

## Scripts

| Command | What it does |
|---|---|
| `npm run dev` | Start the live cluster + UI |
| `npm run build` | Production build |
| `npm run typecheck` | TypeScript check |
| `npm test` | Run the test suite |

## Project layout

```
src/lib/cluster/     simulation engine — services, breakers, sagas, the event bus
services/            the 8 service processes (gateway, orders, payments, ...)
src/routes/          UI: platform overview, orders, traces, bus, mesh, chaos, regions
migrations/          Postgres schema
infra/               deployment config
```
