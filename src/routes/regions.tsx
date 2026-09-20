import { createFileRoute } from "@tanstack/react-router";
import { REGIONS } from "@/lib/cluster/catalog";
import { useCluster } from "@/lib/store";
import { fmtCompact, fmtMs, fmtTerm } from "@/lib/format";
import { Panel, PanelHeader } from "@/components/shared/panel";
import { Badge } from "@/components/ui/badge";
import { StatusPill } from "@/components/shared/status-pill";
import { cn } from "@/lib/utils";

export const Route = createFileRoute("/regions")({ component: RegionsPage });

function RegionsPage() {
  const snapshot = useCluster((s) => s.snapshot);
  if (!snapshot) return null;

  if (snapshot.mode === "live") {
    const c = snapshot.live?.cluster;
    return (
      <div className="mx-auto flex max-w-6xl flex-col gap-4">
        <div>
          <p className="text-xs tracking-wide text-subtle uppercase">Consensus</p>
          <h1 className="mt-1 text-2xl font-medium tracking-tight">Regions</h1>
          <p className="mt-2 max-w-2xl text-sm text-muted">
            Live mode runs on one host, so there is no multi-region Raft here. The three-region Raft view is part of
            the simulation only. What is real: Kafka runs as one KRaft node that is both broker and controller.
          </p>
        </div>
        <Panel>
          <PanelHeader title="Kafka cluster (real)" />
          {c ? (
            <dl className="space-y-1 font-mono text-sm">
              <div>cluster id: {c.clusterId}</div>
              <div>controller: node {c.controller}</div>
              {c.brokers.map((b) => (
                <div key={b.nodeId}>
                  broker {b.nodeId}: {b.host}:{b.port}
                </div>
              ))}
            </dl>
          ) : (
            <p className="text-sm text-muted">Cluster info is not available right now.</p>
          )}
        </Panel>
      </div>
    );
  }

  const healthy = snapshot.raft.filter((n) => !n.partitioned).length;

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-4">
      <div>
        <p className="text-xs tracking-wide text-subtle uppercase">Consensus</p>
        <h1 className="mt-1 text-2xl font-medium tracking-tight">Regions</h1>
        <p className="mt-2 max-w-2xl text-sm text-muted">
          Three regions run a simplified Raft group. Heartbeats keep the leader;
          isolate a node in Chaos and watch a new term elect.
        </p>
      </div>

      <div className="flex flex-wrap gap-3">
        <Badge tone="accent">leader {snapshot.leader}</Badge>
        <Badge tone="neutral">{fmtTerm(snapshot.term)}</Badge>
        <Badge tone={healthy >= 2 ? "ok" : "crit"}>quorum {healthy}/3</Badge>
      </div>

      <div className="grid gap-4 md:grid-cols-3">
        {snapshot.raft.map((node) => {
          const meta = REGIONS.find((r) => r.id === node.id);
          return (
            <Panel key={node.id} className="relative overflow-hidden">
              <div
                className={cn(
                  "absolute inset-x-0 top-0 h-0.5",
                  node.role === "leader" && "bg-ok",
                  node.role === "candidate" && "bg-warn",
                  node.role === "follower" && "bg-border-strong",
                )}
              />
              <div className="flex items-start justify-between gap-2">
                <div>
                  <div className="text-sm font-medium">{node.id}</div>
                  <div className="text-xs text-muted">{meta?.city}</div>
                </div>
                <Badge
                  tone={
                    node.partitioned
                      ? "crit"
                      : node.role === "leader"
                        ? "ok"
                        : node.role === "candidate"
                          ? "warn"
                          : "neutral"
                  }
                >
                  {node.partitioned ? "partitioned" : node.role}
                </Badge>
              </div>
              <dl className="mt-4 grid grid-cols-2 gap-3 text-sm">
                <div>
                  <dt className="text-xs text-subtle">Term</dt>
                  <dd className="font-mono tabular-nums">{fmtTerm(node.term)}</dd>
                </div>
                <div>
                  <dt className="text-xs text-subtle">Log index</dt>
                  <dd className="font-mono tabular-nums">{fmtCompact(node.logIndex)}</dd>
                </div>
                <div>
                  <dt className="text-xs text-subtle">Heartbeat age</dt>
                  <dd className="font-mono tabular-nums">{fmtMs(node.lagMs)}</dd>
                </div>
                <div>
                  <dt className="text-xs text-subtle">Votes</dt>
                  <dd className="font-mono tabular-nums">{node.votes}</dd>
                </div>
              </dl>
            </Panel>
          );
        })}
      </div>

      <Panel>
        <PanelHeader title="How this election works" />
        <ol className="space-y-3 text-sm text-muted">
          <li>
            <span className="text-fg">Heartbeats.</span> The leader stamps followers
            every tick. Isolation stops the stamp, so lag climbs.
          </li>
          <li>
            <span className="text-fg">Timeout.</span> When a follower (or the
            partitioned leader) expires, remaining nodes start a new term.
          </li>
          <li>
            <span className="text-fg">Majority.</span> A candidate needs 2 of 3
            votes. A partitioned region cannot vote, so a 2-node split still has
            quorum.
          </li>
          <li>
            <span className="text-fg">Replication lag.</span> Followers catch the
            log index after they can see the leader again.
          </li>
        </ol>
        <div className="mt-4">
          <StatusPill
            live
            tone={healthy >= 2 ? "ok" : "crit"}
            label={
              healthy >= 2
                ? "Cluster can still take writes"
                : "No quorum — writes should stall"
            }
          />
        </div>
      </Panel>
    </div>
  );
}
