import { useEffect, useState, type ReactNode } from "react";
import type { LiveChaos, Snapshot } from "@/lib/cluster/types";
import { restock, setLiveChaos } from "@/lib/store";
import { Panel, PanelHeader } from "@/components/shared/panel";
import { Switch } from "@/components/ui/switch";
import { Slider } from "@/components/ui/slider";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { fmtPct } from "@/lib/format";

const PAUSABLE = [
  {
    id: "orders",
    title: "Orders (saga)",
    note: "stops the saga: nothing moves forward",
  },
  { id: "inventory", title: "Inventory", note: "reservations wait in Kafka" },
  {
    id: "payments",
    title: "Payments",
    note: "orders time out, breaker opens, late captures get refunded",
  },
  {
    id: "ledger",
    title: "Ledger",
    note: "bookings wait in Kafka, then catch up",
  },
] as const;

/** Chaos page for live mode. Every control changes a real switch in Redis. */
export function LiveChaosPage({ snapshot }: { snapshot: Snapshot }) {
  const chaos = snapshot.live?.chaos as LiveChaos;

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-4">
      <div>
        <p className="text-xs tracking-wide text-subtle uppercase">
          Game day · live
        </p>
        <h1 className="mt-1 text-2xl font-medium tracking-tight">Chaos</h1>
        <p className="mt-2 max-w-2xl text-sm text-muted">
          These switches are real. They are saved in Redis and the running
          services read them. Break payments, then watch lag grow on the Bus
          page, the breaker open on the Mesh page, and the saga compensate on
          the Orders page.
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
          <CommitSlider
            label="Traffic (orders per second)"
            value={chaos.traffic}
            min={0}
            max={20}
            step={1}
            format={(v) => (v === 0 ? "off" : `${v}/s`)}
            onCommit={(v) => setLiveChaos({ traffic: v })}
          />
          <CommitSlider
            label="Card decline rate"
            value={chaos.declineRate}
            min={0}
            max={1}
            step={0.01}
            format={(v) => fmtPct(v, 0)}
            onCommit={(v) => setLiveChaos({ declineRate: v })}
          />
          <CommitSlider
            label="Inventory crash rate"
            value={chaos.inventoryCrashRate}
            min={0}
            max={1}
            step={0.01}
            format={(v) => fmtPct(v, 0)}
            onCommit={(v) => setLiveChaos({ inventoryCrashRate: v })}
          />
          <p className="mt-1 text-xs text-subtle">
            A crash means the handler throws. It is retried 3 times, then the
            message goes to the dead-letter topic.
          </p>
          <div className="mt-4">
            <Button variant="outline" onClick={() => restock()}>
              Restock warehouse
            </Button>
          </div>
        </Panel>

        <Panel>
          <PanelHeader title="Pause a service" />
          <ul className="space-y-1">
            {PAUSABLE.map((svc) => (
              <li
                key={svc.id}
                className="flex min-h-11 items-center justify-between gap-3"
              >
                <div>
                  <div className="text-sm">{svc.title}</div>
                  <div className="text-xs text-subtle">{svc.note}</div>
                </div>
                <Switch
                  checked={!!chaos.paused[svc.id]}
                  onCheckedChange={(checked) =>
                    setLiveChaos({ paused: { [svc.id]: checked } })
                  }
                  aria-label={`Pause ${svc.id}`}
                />
              </li>
            ))}
          </ul>
          <p className="mt-3 text-xs text-subtle">
            Pause makes the service stop reading from Kafka, like a stopped
            container. For a real kill, run{" "}
            <span className="font-mono">docker compose stop payments</span> in a
            terminal. The dashboard will show it as down.
          </p>
        </Panel>
      </div>

      <Panel>
        <PanelHeader title="Inject latency" />
        <div className="grid gap-2 lg:grid-cols-2">
          {(["inventory", "payments"] as const).map((id) => (
            <CommitSlider
              key={id}
              label={id}
              value={chaos.latencyMs[id] ?? 0}
              min={0}
              max={5000}
              step={100}
              format={(v) => `${v} ms`}
              onCommit={(v) => setLiveChaos({ latencyMs: { [id]: v } })}
            />
          ))}
        </div>
        <p className="mt-1 text-xs text-subtle">
          Above about 6000 ms in payments, orders time out and the saga
          compensates.
        </p>
      </Panel>
    </div>
  );
}

/** Slider that follows your finger while dragging and sends the value once, when you let go. */
function CommitSlider({
  label,
  value,
  min,
  max,
  step,
  format,
  onCommit,
}: {
  label: string;
  value: number;
  min: number;
  max: number;
  step: number;
  format: (v: number) => string;
  onCommit: (v: number) => void;
}) {
  const [local, setLocal] = useState(value);
  const [dragging, setDragging] = useState(false);
  useEffect(() => {
    if (!dragging) setLocal(value);
  }, [value, dragging]);

  return (
    <Row label={label} value={format(local)}>
      <Slider
        min={min}
        max={max}
        step={step}
        value={[local]}
        onValueChange={([v]) => {
          setDragging(true);
          setLocal(v ?? 0);
        }}
        onValueCommit={([v]) => {
          setDragging(false);
          onCommit(v ?? 0);
        }}
      />
    </Row>
  );
}

function Row({
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
        <span className="font-mono text-xs tabular-nums text-muted">
          {value}
        </span>
      </div>
      {children}
    </div>
  );
}
