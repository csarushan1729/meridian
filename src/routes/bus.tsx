import { createFileRoute } from "@tanstack/react-router";
import { useCluster } from "@/lib/store";
import { fmtClock, fmtCompact, fmtId, fmtRps } from "@/lib/format";
import { Panel, PanelHeader } from "@/components/shared/panel";
import { Badge } from "@/components/ui/badge";
import { Metric } from "@/components/shared/metric";

export const Route = createFileRoute("/bus")({ component: BusPage });

function BusPage() {
  const snapshot = useCluster((s) => s.snapshot);
  if (!snapshot) return null;

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-4">
      <div>
        <p className="text-xs tracking-wide text-subtle uppercase">Event fabric</p>
        <h1 className="mt-1 text-2xl font-medium tracking-tight">Bus</h1>
        <p className="mt-2 max-w-2xl text-sm text-muted">
          Topics are Kafka partitions with consistent hashing. Producers write
          through a transactional outbox; the WAL persists to Postgres. Consumers
          commit offsets independently and can lag.
        </p>
      </div>

      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <Panel>
          <Metric
            label="DLQ"
            value={fmtCompact(snapshot.dlq)}
            tone={snapshot.dlq > 0 ? "warn" : undefined}
          />
        </Panel>
        <Panel>
          <Metric label="Shed" value={fmtCompact(snapshot.shed)} />
        </Panel>
        <Panel>
          <Metric label="Rate limited" value={fmtCompact(snapshot.rateLimited)} />
        </Panel>
        <Panel>
          <Metric label="Ledger net" value={`$${(snapshot.ledgerNet / 100).toFixed(0)}`} />
        </Panel>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Panel>
          <PanelHeader title="Topics" />
          <ul className="space-y-4">
            {snapshot.topics.map((topic) => (
              <li key={topic.id}>
                <div className="flex items-baseline justify-between gap-2">
                  <span className="font-mono text-sm">{topic.id}</span>
                  <span className="font-mono text-xs text-muted">
                    {fmtRps(topic.rate)}/s · {fmtCompact(topic.produced)}
                  </span>
                </div>
                <div className="mt-2 flex gap-1">
                  {topic.partitions.map((p) => (
                    <div key={p.id} className="min-w-0 flex-1">
                      <div className="h-8 overflow-hidden rounded-sm bg-bg-subtle">
                        <div
                          className="h-full bg-accent/40"
                          style={{
                            height: "100%",
                            width: `${Math.min(100, 8 + p.depth * 3)}%`,
                          }}
                        />
                      </div>
                      <div className="mt-1 font-mono text-xs text-subtle">
                        p{p.id} {p.depth}
                      </div>
                    </div>
                  ))}
                </div>
              </li>
            ))}
          </ul>
        </Panel>

        <div className="flex flex-col gap-4">
          <Panel>
            <PanelHeader title="Consumer groups" />
            <ul className="space-y-3">
              {snapshot.consumers.map((cg) => (
                <li key={cg.id} className="flex items-center justify-between gap-3">
                  <div>
                    <div className="text-sm">{cg.id}</div>
                    <div className="font-mono text-xs text-subtle">{cg.topic}</div>
                  </div>
                  <Badge tone={cg.lag > 12 ? "warn" : "ok"}>lag {cg.lag}</Badge>
                </li>
              ))}
            </ul>
          </Panel>
          <Panel>
            <PanelHeader title="Tail" />
            <ul className="max-h-64 space-y-2 overflow-auto font-mono text-xs">
              {snapshot.recentEvents.map((ev, i) => (
                <li key={`${ev.topic}-${ev.partition}-${ev.offset}-${i}`} className="text-muted">
                  <span className="text-subtle">{fmtClock(ev.ts)}</span>{" "}
                  <span className="text-fg">{ev.type}</span> {ev.topic}[{ev.partition}] #
                  {ev.offset} {fmtId(ev.key, 8)}
                </li>
              ))}
            </ul>
          </Panel>
        </div>
      </div>
    </div>
  );
}
