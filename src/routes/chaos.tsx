import type { ReactNode } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { SERVICES, REGIONS } from "@/lib/cluster/catalog";
import type { RegionId, ServiceId } from "@/lib/cluster/types";
import { restock, setChaos, useCluster } from "@/lib/store";
import { Panel, PanelHeader } from "@/components/shared/panel";
import { Switch } from "@/components/ui/switch";
import { Slider } from "@/components/ui/slider";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { fmtPct } from "@/lib/format";
import { LiveChaosPage } from "@/components/chaos/live-chaos";

export const Route = createFileRoute("/chaos")({ component: ChaosPage });

function ChaosPage() {
  const snapshot = useCluster((s) => s.snapshot);
  if (!snapshot) return null;
  if (snapshot.mode === "live" && snapshot.live?.chaos) return <LiveChaosPage snapshot={snapshot} />;
  const chaos = snapshot.chaos;

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-4">
      <div>
        <p className="text-xs tracking-wide text-subtle uppercase">Game day</p>
        <h1 className="mt-1 text-2xl font-medium tracking-tight">Chaos</h1>
        <p className="mt-2 max-w-2xl text-sm text-muted">
          Break the cluster on purpose. Payment failures trip the saga compensator;
          killing a service opens its breaker; partitioning a region forces Raft to
          elect.
        </p>
      </div>

      <div className="flex flex-wrap gap-2">
        {snapshot.patterns
          .filter((p) => p.active)
          .map((p) => (
            <Badge key={p.id} tone="warn">
              {p.label}
            </Badge>
          ))}
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Panel>
          <PanelHeader title="Traffic and faults" />
          <Control
            label="Traffic"
            value={`${chaos.traffic.toFixed(1)}x`}
          >
            <Slider
              min={0.15}
              max={4}
              step={0.05}
              value={[chaos.traffic]}
              onValueChange={([v]) => setChaos({ traffic: v ?? 1 })}
            />
          </Control>
          <Control
            label="Payment fail rate"
            value={fmtPct(chaos.paymentFail, 0)}
          >
            <Slider
              min={0}
              max={0.85}
              step={0.01}
              value={[chaos.paymentFail]}
              onValueChange={([v]) => setChaos({ paymentFail: v ?? 0 })}
            />
          </Control>
          <Control
            label="Inventory fail rate"
            value={fmtPct(chaos.inventoryFail, 0)}
          >
            <Slider
              min={0}
              max={0.7}
              step={0.01}
              value={[chaos.inventoryFail]}
              onValueChange={([v]) => setChaos({ inventoryFail: v ?? 0 })}
            />
          </Control>
          <Control label="Drop rate" value={fmtPct(chaos.dropRate, 0)}>
            <Slider
              min={0}
              max={0.4}
              step={0.01}
              value={[chaos.dropRate]}
              onValueChange={([v]) => setChaos({ dropRate: v ?? 0 })}
            />
          </Control>
          <Control label="Clock skew" value={`${chaos.clockSkew.toFixed(2)}x`}>
            <Slider
              min={0.5}
              max={2}
              step={0.05}
              value={[chaos.clockSkew]}
              onValueChange={([v]) => setChaos({ clockSkew: v ?? 1 })}
            />
          </Control>
          <div className="mt-4">
            <Button variant="outline" onClick={() => restock()}>
              Restock warehouse
            </Button>
          </div>
        </Panel>

        <Panel>
          <PanelHeader title="Kill a process" />
          <ul className="space-y-1">
            {SERVICES.map((svc) => (
              <li
                key={svc.id}
                className="flex min-h-11 items-center justify-between gap-3"
              >
                <div>
                  <div className="text-sm">{svc.title}</div>
                  <div className="font-mono text-xs text-subtle">{svc.id}</div>
                </div>
                <Switch
                  checked={!!chaos.killed[svc.id]}
                  onCheckedChange={(checked) =>
                    setChaos({
                      killed: { ...chaos.killed, [svc.id]: checked },
                    })
                  }
                  aria-label={`Kill ${svc.id}`}
                />
              </li>
            ))}
          </ul>
        </Panel>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Panel>
          <PanelHeader title="Partition a region" />
          <ul className="space-y-1">
            {REGIONS.map((region) => (
              <li
                key={region.id}
                className="flex min-h-11 items-center justify-between gap-3"
              >
                <div>
                  <div className="text-sm">{region.id}</div>
                  <div className="text-xs text-muted">{region.city}</div>
                </div>
                <Switch
                  checked={!!chaos.partitioned[region.id]}
                  onCheckedChange={(checked) =>
                    setChaos({
                      partitioned: {
                        ...chaos.partitioned,
                        [region.id as RegionId]: checked,
                      },
                    })
                  }
                  aria-label={`Partition ${region.id}`}
                />
              </li>
            ))}
          </ul>
        </Panel>

        <Panel>
          <PanelHeader title="Inject latency" />
          <ul className="space-y-2">
            {(Object.keys(chaos.latencyMult) as ServiceId[]).map((id) => (
              <li key={id}>
                <Control label={id} value={`${chaos.latencyMult[id]!.toFixed(1)}x`}>
                  <Slider
                    min={1}
                    max={8}
                    step={0.1}
                    value={[chaos.latencyMult[id] ?? 1]}
                    onValueChange={([v]) =>
                      setChaos({
                        latencyMult: { ...chaos.latencyMult, [id]: v ?? 1 },
                      })
                    }
                  />
                </Control>
              </li>
            ))}
          </ul>
        </Panel>
      </div>
    </div>
  );
}

function Control({
  label,
  value,
  children,
}: {
  label: string;
  value: string;
  children: ReactNode;
}) {
  return (
    <div className="py-2">
      <div className="flex items-baseline justify-between gap-2">
        <span className="text-sm">{label}</span>
        <span className="font-mono text-xs tabular-nums text-muted">{value}</span>
      </div>
      {children}
    </div>
  );
}
