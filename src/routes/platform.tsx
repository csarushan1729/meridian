import { createFileRoute } from "@tanstack/react-router";
import { useCluster } from "@/lib/store";
import { fmtCompact, fmtPct } from "@/lib/format";
import { Panel, PanelHeader } from "@/components/shared/panel";
import { Metric } from "@/components/shared/metric";
import { Badge } from "@/components/ui/badge";
import { StatusPill } from "@/components/shared/status-pill";

export const Route = createFileRoute("/platform")({ component: PlatformPage });

function PlatformPage() {
  const snapshot = useCluster((s) => s.snapshot);
  const p = snapshot.platform;
  const dbLabel = p.dbBackend === "neon" ? "Neon Postgres" : p.dbBackend === "pglite" ? "PGLite (Postgres)" : "Memory";

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-4">
      <div>
        <p className="text-xs tracking-wide text-subtle uppercase">Infrastructure</p>
        <h1 className="mt-1 text-2xl font-medium tracking-tight">Platform</h1>
        <p className="mt-2 max-w-2xl text-sm text-muted">
          Eight services talk only over Kafka. Idempotency, locks, and inventory
          cache live in Redis. Domain state is Postgres. The control plane is
          this UI.
        </p>
      </div>

      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <Panel>
          <Metric label="Kafka messages" value={fmtCompact(p.kafka.messages)} hint={`${p.kafka.partitions} partitions`} />
        </Panel>
        <Panel>
          <Metric
            label="WAL persisted"
            value={fmtCompact(p.kafka.persisted)}
            hint={`${p.kafka.walDepth} in flight`}
            tone={p.kafka.walDepth > 80 ? "warn" : "ok"}
          />
        </Panel>
        <Panel>
          <Metric
            label="Redis hit rate"
            value={fmtPct(p.redis.hitRate, 0)}
            hint={`${fmtCompact(p.redis.keys)} keys`}
          />
        </Panel>
        <Panel>
          <Metric label="Postgres" value={dbLabel} hint={`${fmtCompact(p.postgres.orders)} orders`} />
        </Panel>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Panel>
          <PanelHeader
            title="Kafka broker"
            aside={<StatusPill live tone="ok" label={p.kafka.name} />}
          />
          <p className="text-sm text-muted">{p.kafka.implementation}</p>
          <dl className="mt-4 grid grid-cols-2 gap-3 font-mono text-xs">
            <Row k="Topics" v={String(p.kafka.topics)} />
            <Row k="Partitions" v={String(p.kafka.partitions)} />
            <Row k="Consumer groups" v={String(p.kafka.consumerGroups)} />
            <Row k="Dead-letter" v={String(p.kafka.dlq)} />
            <Row k="Produced" v={fmtCompact(p.kafka.messages)} />
            <Row k="Persisted" v={fmtCompact(p.kafka.persisted)} />
          </dl>
          <p className="mt-4 text-xs text-subtle">
            Produce is append-only with consistent hashing. Groups commit offsets
            independently (at-least-once). The WAL flush writes kafka_log in Postgres
            so a restart replays the log.
          </p>
        </Panel>

        <Panel>
          <PanelHeader
            title="Redis"
            aside={<StatusPill live tone="ok" label={p.redis.name} />}
          />
          <p className="text-sm text-muted">{p.redis.implementation}</p>
          <dl className="mt-4 grid grid-cols-2 gap-3 font-mono text-xs">
            <Row k="Keys" v={String(p.redis.keys)} />
            <Row k="Ops" v={fmtCompact(p.redis.ops)} />
            <Row k="Hits" v={fmtCompact(p.redis.hits)} />
            <Row k="Misses" v={fmtCompact(p.redis.misses)} />
            <Row k="Locks" v={String(p.redis.locks)} />
            <Row k="Idempotency" v={String(p.redis.idempotencyKeys)} />
          </dl>
          <p className="mt-4 text-xs text-subtle">
            SETNX + TTL for idempotency keys and SKU reservation locks. Inventory
            is a hash per SKU (cache-aside over helix_inventory).
          </p>
        </Panel>
      </div>

      <Panel>
        <PanelHeader title="Postgres tables" />
        <div className="grid grid-cols-2 gap-3 md:grid-cols-5">
          <Count label="helix_orders" n={p.postgres.orders} />
          <Count label="helix_inventory" n={p.postgres.inventory} />
          <Count label="helix_ledger" n={p.postgres.ledger} />
          <Count label="kafka_log" n={p.postgres.kafkaLog} />
          <Count label="redis_kv" n={p.postgres.redisKeys} />
        </div>
      </Panel>

      <div className="grid gap-4 lg:grid-cols-2">
        <Panel>
          <PanelHeader title="Redis keyspace" />
          {p.redisKeys.length === 0 ? (
            <p className="text-sm text-muted">No keys yet — place an order.</p>
          ) : (
            <ul className="divide-y divide-border">
              {p.redisKeys.map((row) => (
                <li key={row.key} className="flex items-center justify-between gap-3 py-2">
                  <span className="truncate font-mono text-xs">{row.key}</span>
                  <span className="shrink-0 font-mono text-xs text-muted">
                    {row.kind}
                    {row.ttl != null && row.ttl >= 0 ? ` · ttl ${Math.round(row.ttl / 1000)}s` : ""}
                  </span>
                </li>
              ))}
            </ul>
          )}
        </Panel>

        <Panel>
          <PanelHeader title="AWS mapping" />
          <p className="mb-3 text-sm text-muted">
            Same contracts. Different brokers. This is the swap list for a cloud
            deploy — no code change in the services.
          </p>
          <ul className="space-y-3">
            {p.aws.map((row) => (
              <li key={row.layer} className="rounded-sm bg-bg-subtle px-3 py-2.5">
                <div className="flex items-center gap-2">
                  <Badge>{row.layer}</Badge>
                </div>
                <p className="mt-1.5 font-mono text-xs text-muted">{row.here}</p>
                <p className="mt-0.5 font-mono text-xs text-accent">{row.aws}</p>
              </li>
            ))}
          </ul>
        </Panel>
      </div>
    </div>
  );
}

function Row({ k, v }: { k: string; v: string }) {
  return (
    <div>
      <dt className="text-subtle uppercase tracking-wide">{k}</dt>
      <dd className="mt-0.5 text-sm text-fg">{v}</dd>
    </div>
  );
}

function Count({ label, n }: { label: string; n: number }) {
  return (
    <div className="rounded-sm bg-bg-subtle px-3 py-3">
      <p className="font-mono text-xs text-subtle">{label}</p>
      <p className="mt-1 font-mono text-lg tabular-nums">{fmtCompact(n)}</p>
    </div>
  );
}
