import { createFileRoute, Link } from "@tanstack/react-router";
import {
  Area,
  AreaChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { useCluster } from "@/lib/store";
import { fmtCompact, fmtMs, fmtPct, fmtRps } from "@/lib/format";
import { Metric } from "@/components/shared/metric";
import { Panel, PanelHeader } from "@/components/shared/panel";
import { StatusPill, healthTone } from "@/components/shared/status-pill";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";

export const Route = createFileRoute("/")({ component: OverviewPage });

function OverviewPage() {
  const snapshot = useCluster((s) => s.snapshot);

  const budgetTone =
    snapshot.errorBudget < 0.3 ? "crit" : snapshot.errorBudget < 0.6 ? "warn" : "ok";

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-4">
      <div>
        <p className="text-xs tracking-wide text-subtle uppercase">Helix production</p>
        <h1 className="mt-1 text-2xl font-medium tracking-tight md:text-3xl">
          Control plane
        </h1>
        <p className="mt-2 max-w-2xl text-sm text-muted">{snapshot.headline}</p>
        <p className="mt-1 font-mono text-xs text-subtle">
          Kafka · Redis · {snapshot.platform.dbBackend === "neon" ? "Neon" : "Postgres"} · {snapshot.services.length} services
        </p>
      </div>

      <div className="grid grid-cols-2 gap-3 md:grid-cols-5">
        <Panel>
          <Metric
            label="Availability"
            value={fmtPct(snapshot.availability, 2)}
            hint="rolling window"
            tone={snapshot.availability < 0.99 ? "warn" : "ok"}
          />
        </Panel>
        <Panel>
          <Metric label="p99" value={fmtMs(snapshot.p99)} hint="edge to service" />
        </Panel>
        <Panel>
          <Metric
            label="Error budget"
            value={fmtPct(snapshot.errorBudget, 0)}
            hint={`${snapshot.burnRate.toFixed(1)}x burn`}
            tone={budgetTone}
          />
        </Panel>
        <Panel>
          <Metric label="Throughput" value={`${fmtRps(snapshot.rps)} rps`} hint="admitted" />
        </Panel>
        <Panel className="col-span-2 md:col-span-1">
          <Metric
            label="Sagas in flight"
            value={String(snapshot.sagasInFlight)}
            hint={`${fmtCompact(snapshot.ordersTotal)} total`}
          />
        </Panel>
      </div>

      <div className="grid gap-4 lg:grid-cols-3">
        <Panel className="lg:col-span-2">
          <PanelHeader
            title="Live traffic"
            aside={
              <StatusPill live tone="ok" label={`${snapshot.inflight} in-flight`} />
            }
          />
          <div className="h-44">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={snapshot.metrics} margin={{ top: 4, right: 4, left: 0, bottom: 0 }}>
                <defs>
                  <linearGradient id="rpsFill" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor="#c5ccd4" stopOpacity={0.22} />
                    <stop offset="100%" stopColor="#c5ccd4" stopOpacity={0} />
                  </linearGradient>
                </defs>
                <XAxis dataKey="t" hide />
                <YAxis hide domain={[0, "auto"]} />
                <Tooltip
                  contentStyle={{
                    background: "#121316",
                    border: "1px solid #eceef129",
                    borderRadius: 8,
                    fontSize: 12,
                    fontFamily: "IBM Plex Mono, ui-monospace, monospace",
                  }}
                  labelFormatter={() => "sample"}
                  formatter={(value, name) => [
                    typeof value === "number" ? value.toFixed(2) : String(value),
                    String(name),
                  ]}
                />
                <Area
                  type="monotone"
                  dataKey="rps"
                  stroke="#c5ccd4"
                  fill="url(#rpsFill)"
                  strokeWidth={1.5}
                  isAnimationActive={false}
                />
                <Area
                  type="monotone"
                  dataKey="errors"
                  stroke="#c47d74"
                  fill="none"
                  strokeWidth={1.25}
                  isAnimationActive={false}
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </Panel>

        <Panel>
          <PanelHeader title="Patterns firing" />
          <ul className="flex flex-col gap-2">
            {snapshot.patterns
              .filter((p) => p.active)
              .slice(0, 6)
              .map((p) => (
                <li key={p.id} className="flex items-start gap-2">
                  <span className="mt-1.5 size-1.5 shrink-0 rounded-full bg-ok" />
                  <div>
                    <div className="text-sm">{p.label}</div>
                    <div className="text-xs text-muted">{p.hint}</div>
                  </div>
                </li>
              ))}
            {snapshot.patterns.filter((p) => p.active).length === 0 ? (
              <li className="text-sm text-muted">Idle — inject chaos to trip patterns.</li>
            ) : null}
          </ul>
        </Panel>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Panel>
          <PanelHeader
            title="Services"
            aside={
              <Link to="/mesh" className="text-xs text-muted hover:text-fg">
                Mesh
              </Link>
            }
          />
          <div className="overflow-x-auto">
            <table className="w-full text-left text-sm">
              <thead className="text-xs text-subtle">
                <tr>
                  <th className="pb-2 font-medium">Service</th>
                  <th className="pb-2 font-medium">RPS</th>
                  <th className="pb-2 font-medium">p95</th>
                  <th className="pb-2 font-medium">Breaker</th>
                </tr>
              </thead>
              <tbody>
                {snapshot.services.map((svc) => (
                  <tr key={svc.id} className="border-t border-border">
                    <td className="py-2.5">
                      <div className="flex items-center gap-2">
                        <StatusPill tone={healthTone(svc.status)} label={svc.title} />
                      </div>
                    </td>
                    <td className="py-2.5 font-mono tabular-nums text-muted">
                      {fmtRps(svc.rps)}
                    </td>
                    <td className="py-2.5 font-mono tabular-nums text-muted">
                      {fmtMs(svc.p95)}
                    </td>
                    <td className="py-2.5">
                      <Badge
                        tone={
                          svc.circuit === "open"
                            ? "crit"
                            : svc.circuit === "half-open"
                              ? "warn"
                              : "ok"
                        }
                      >
                        {svc.circuit}
                      </Badge>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>

        <Panel>
          <PanelHeader
            title="Recent incidents"
            aside={
              <Link to="/incidents" className="text-xs text-muted hover:text-fg">
                All
              </Link>
            }
          />
          <ul className="flex flex-col gap-3">
            {snapshot.incidents.slice(0, 6).map((inc) => (
              <li key={inc.id} className="flex gap-3">
                <span
                  className={cn(
                    "mt-1.5 size-1.5 shrink-0 rounded-full",
                    inc.severity === "crit" && "bg-crit",
                    inc.severity === "warn" && "bg-warn",
                    inc.severity === "info" && "bg-info",
                  )}
                />
                <div className="min-w-0">
                  <div className="truncate text-sm">{inc.title}</div>
                  <div className="truncate text-xs text-muted">{inc.detail}</div>
                </div>
              </li>
            ))}
            {snapshot.incidents.length === 0 ? (
              <li className="text-sm text-muted">Cluster is quiet.</li>
            ) : null}
          </ul>
        </Panel>
      </div>
    </div>
  );
}
