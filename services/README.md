# Meridian real backend (Kafka + Redis + Postgres + 5 microservices)

Run everything (from the project root, where `docker-compose.yml` is):

    docker compose up -d --build
    docker compose ps

Place orders (Windows CMD, keep the quotes):

    curl -X POST "http://localhost:4000/orders?skuId=halo-bottle&qty=1"
    curl -X POST "http://localhost:4000/demo/burst?count=20"
    curl http://localhost:4000/orders
    curl http://localhost:4005/summary

## Services

| Service   | Port | Owns (own database)                    | Talks through Kafka topics                                   |
|-----------|------|----------------------------------------|--------------------------------------------------------------|
| gateway   | 4000 | nothing (Redis: rate limit, idem keys) | writes `orders.commands`                                     |
| orders    | 4002 | orders, order_steps, outbox            | reads `orders.commands`, `inventory.events`, `payments.events`; writes `inventory.commands`, `payments.commands`, `orders.events` |
| inventory | 4003 | stock, reservations                    | reads `inventory.commands`; writes `inventory.events`        |
| payments  | 4004 | payments                               | reads `payments.commands`; writes `payments.events`          |
| ledger    | 4005 | ledger_entries                         | reads `payments.events`                                      |
| control   | 4010 | nothing (reads metrics, Kafka, Redis)  | dashboard backend: `GET /snapshot`, `POST /chaos`            |

## Saga

    order.create -> inventory.reserve -> inventory.reserved -> payment.capture -> payment.captured -> CONFIRMED

Failure paths: out of stock, card declined, payment timeout, circuit open.
All of them release the stock. A capture that arrives after cancel is refunded.

## Redis keys

    idem:<key>              order id for an Idempotency-Key (SET NX PX 24h)
    rl:<ip>:<minute>        rate limit counter (INCR + EXPIRE)
    lock:sku:<sku>          per-SKU lock (SET NX PX, released with a Lua script)
    chaos:<svc>:latency_ms  fake latency
    chaos:<svc>:crashrate   0..1, handler throws -> retries -> dead-letter
    chaos:payments:declinerate  0..1

## Tests

Need Redis + Postgres (`docker compose up -d redis postgres`), then:

    cd services
    npm install
    npm test

The tests use real Redis and real Postgres. Kafka is replaced by an in-memory bus.

## What is simulated

The payment provider (random declines). Everything else, including Kafka, Redis and Postgres, is real.

## Dashboard live mode

The dashboard (`npm run dev`, port 8080) tries `http://localhost:4010` (the control service).
- Reachable: header badge says "live · real Kafka" and every page shows real data.
- Not reachable: it falls back to the built-in simulation ("demo · simulated").
- `CONTROL_URL` changes the address. `DASHBOARD_MODE=demo` forces the simulation.

Chaos page in live mode changes Redis keys that the services read:

    chaos:payments:declinerate   chaos:inventory:crashrate
    chaos:<svc>:latency_ms       chaos:<svc>:paused   (paused = stop reading Kafka)

Only Regions (3-region Raft) is simulation-only.
