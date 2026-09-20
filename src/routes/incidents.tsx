import { createFileRoute } from "@tanstack/react-router";
import { useCluster } from "@/lib/store";
import { fmtClock } from "@/lib/format";
import { Panel, PanelHeader } from "@/components/shared/panel";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";

export const Route = createFileRoute("/incidents")({ component: IncidentsPage });

function IncidentsPage() {
  const snapshot = useCluster((s) => s.snapshot);
  if (!snapshot) return null;

  const crit = snapshot.incidents.filter((i) => i.severity === "crit").length;
  const warn = snapshot.incidents.filter((i) => i.severity === "warn").length;

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-4">
      <div>
        <p className="text-xs tracking-wide text-subtle uppercase">Timeline</p>
        <h1 className="mt-1 text-2xl font-medium tracking-tight">Incidents</h1>
        <p className="mt-2 max-w-2xl text-sm text-muted">
          Breaker trips, elections, compensations, and shed events — the audit
          trail of the kernel, not a mock feed.
        </p>
      </div>

      <div className="flex gap-2">
        <Badge tone="crit">{crit} critical</Badge>
        <Badge tone="warn">{warn} warning</Badge>
        <Badge tone="neutral">{snapshot.incidents.length} in buffer</Badge>
      </div>

      <Panel>
        <PanelHeader title="Newest first" />
        {snapshot.incidents.length === 0 ? (
          <p className="text-sm text-muted">
            Nothing page-worthy. Raise payment failures or kill payments to create
            one.
          </p>
        ) : (
          <ol className="relative space-y-0">
            {snapshot.incidents.map((inc, i) => (
              <li key={inc.id} className="flex gap-4">
                <div className="flex w-4 flex-col items-center">
                  <span
                    className={cn(
                      "mt-1.5 size-2 rounded-full",
                      inc.severity === "crit" && "bg-crit",
                      inc.severity === "warn" && "bg-warn",
                      inc.severity === "info" && "bg-info",
                    )}
                  />
                  {i < snapshot.incidents.length - 1 ? (
                    <span className="w-px flex-1 bg-border" />
                  ) : null}
                </div>
                <div className="pb-5">
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="text-sm">{inc.title}</span>
                    <Badge
                      tone={
                        inc.severity === "crit"
                          ? "crit"
                          : inc.severity === "warn"
                            ? "warn"
                            : "info"
                      }
                    >
                      {inc.pattern}
                    </Badge>
                  </div>
                  <p className="mt-1 text-sm text-muted">{inc.detail}</p>
                  <p className="mt-1 font-mono text-xs text-subtle">
                    {fmtClock(inc.at)}
                    {inc.service ? ` · ${inc.service}` : ""}
                    {inc.region ? ` · ${inc.region}` : ""}
                  </p>
                </div>
              </li>
            ))}
          </ol>
        )}
      </Panel>
    </div>
  );
}
