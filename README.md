# Meridian

Live multi-region commerce **control plane**. Eight services, a Kafka-compatible bus, a Redis-compatible store, and Postgres — with a UI you can demo in an SDE interview.

**Stack:** TanStack Start · React 19 · Postgres · Kafka-compatible broker · Redis-compatible store · sagas, circuit breakers, Raft, traces.

---

## 1. Run on Windows (Command Prompt)

### Prerequisites

1. Install **Node.js 22 LTS** from [https://nodejs.org](https://nodejs.org) (include npm).
2. Close and reopen Command Prompt after installing.
3. Check it worked:

```bat
node -v
npm -v
```

You want `v22.x` (or at least v20) and an npm version.

### Run the app

Unzip `meridian.zip`, then:

```bat
cd /d %USERPROFILE%\Downloads\meridian
npm install
npm run dev
```

When it says the server is ready, open a browser to:

```
http://localhost:8080
```

Leave that Command Prompt window open while you demo.

Stop the app with `Ctrl+C`.

---

## 2. Put it on GitHub (so you can share the code)

Install [Git for Windows](https://git-scm.com/download/win). In the project folder:

```bat
cd /d %USERPROFILE%\Downloads\meridian
git init
git add .
git commit -m "Meridian: Kafka, Redis, Postgres commerce control plane"
git branch -M main
```

Create an empty repo on [github.com/new](https://github.com/new) named `meridian` (do not add a README there). Then:

```bat
git remote add origin https://github.com/YOUR_USER/meridian.git
git push -u origin main
```

Replace `YOUR_USER` with your GitHub username.

---

## 3. Public URL on Vercel (what you send in applications)

This app is a Vercel Node server. Postgres is optional locally (PGLite). **On Vercel, set `DATABASE_URL`** so orders, the Kafka WAL, and Redis keys persist.

### A. Neon Postgres (free)

1. Sign up at [https://console.neon.tech](https://console.neon.tech)
2. Create a project
3. Copy the connection string (`postgresql://...`)

### B. Deploy

```bat
cd /d %USERPROFILE%\Downloads\meridian
npm install -g vercel
vercel login
vercel env add DATABASE_URL
```

Paste the Neon URL when asked (Production, Preview, and Development). Then:

```bat
vercel --prod
```

It prints a URL like `https://meridian-xxx.vercel.app`. That is the demo link.

**Or:** import the GitHub repo at [https://vercel.com/new](https://vercel.com/new), add `DATABASE_URL` under Settings → Environment Variables, deploy.

### Build settings (if the dashboard asks)

| Setting | Value |
|---|---|
| Framework | Other / Vite |
| Build command | `npm run build` |
| Install command | `npm install` |
| Node version | 22.x |

---

## 4. Interview walkthrough (5 minutes)

Open the live URL. Speak in this order:

1. **Overview** — “This is the control plane for Helix. Eight services, live RPS, p99, error budget.”
2. **Platform** — “Kafka is a partitioned log with consumer groups; WAL is Postgres. Redis holds idempotency keys and SKU locks. Domain tables are Postgres. Same APIs swap to MSK, ElastiCache, RDS on AWS.”
3. **Place order** — “Gateway admits via token bucket. Orders service runs a saga: reserve → capture → allocate → ship → notify.”
4. **Sagas + Traces** — click the order. Walk the compensating steps and the distributed trace.
5. **Chaos** — kill **payments**. “Breaker opens, inventory is released, payment is refunded, poison messages go to the DLQ.”
6. **Kafka / Regions** — offsets, lag, Raft leader.

**Be honest if asked:** this ships as one Node app with Kafka-compatible and Redis-compatible brokers persisted in Postgres. Production AWS would point the same interfaces at MSK and ElastiCache. Do not say you run Apache Kafka as a separate cluster unless you have.

---

## Scripts

| Command | What it does |
|---|---|
| `npm run dev` | Live cluster + UI |
| `npm run build` | Production bundle |
| `npm run typecheck` | TypeScript |

No login is required. Do not put secrets in the repo. `DATABASE_URL` lives only in Vercel / your machine env.
