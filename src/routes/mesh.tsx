import { createFileRoute } from "@tanstack/react-router";
import { MESH_LAYOUT } from "@/lib/cluster/catalog";
import { useCluster } from "@/lib/store";
import { fmtMs, fmtPct, fmtRps } from "@/lib/format";
import { Panel } from "@/components/shared/panel";
import { Badge } from "@/components/ui/badge";
import { StatusPill, healthTone } from "@/components/shared/status-pill";
import { cn } from "@/lib/utils";

export const Route = createFileRoute("/mesh")({ component: MeshPage });

const W = 1000;
const H = 640;

function MeshPage() {
  const snapshot = useCluster((s) => s.snapshot);
  if (!snapshot) return null;

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-4">
      <div>
        <p className="text-xs tracking-wide text-subtle uppercase">Service mesh</p>
        <h1 className="mt-1 text-2xl font-medium tracking-tight">Topology</h1>
        <p className="mt-2 max-w-2xl text-sm text-muted">
          Eight services, circuit-broken edges, and live RPS on each hop. Kill a
          node in Chaos to watch breakers open and traffic fail fast.
        </p>
      </div>

      <Panel className="hidden overflow-hidden p-2 md:block">
        <svg viewBox={`0 0 ${W} ${H}`} className="h-auto w-full text-fg" role="img">
          <title>Helix service mesh</title>
          {snapshot.edges.map((edge) => {
            const from =
              edge.from === "client"
                ? { x: 0.5, y: -0.04 }
                : MESH_LAYOUT[edge.from];
            const to = MESH_LAYOUT[edge.to];
            const x1 = from.x * W;
            const y1 = from.y * H;
            const x2 = to.x * W;
            const y2 = to.y * H;
            const open = edge.circuit === "open";
            return (
              <g key={`${edge.from}-${edge.to}`}>
                <line
                  x1={x1}
                  y1={y1}
                  x2={x2}
                  y2={y2}
                  stroke={open ? "#c47d74" : "#eceef1"}
                  strokeOpacity={open ? 0.7 : Math.min(0.55, 0.12 + edge.rps / 40)}
                  strokeWidth={open ? 2 : 1.25 + Math.min(2, edge.rps / 12)}
                />
              </g>
            );
          })}
          {snapshot.services.map((svc) => {
            const pos = MESH_LAYOUT[svc.id];
            const x = pos.x * W;
            const y = pos.y * H;
            const fill =
              svc.status === "down"
                ? "#c47d74"
                : svc.status === "degraded"
                  ? "#c4a56a"
                  : "#7d9e86";
            return (
              <g key={svc.id} transform={`translate(${x}, ${y})`}>
                <rect
                  x={-92}
                  y={-28}
                  width={184}
                  height={56}
                  rx={12}
                  fill="#121316"
                  stroke="#eceef129"
                />
                <circle cx={-72} cy={0} r={5} fill={fill} />
                <text
                  x={-60}
                  y={-4}
                  fill="#eceef1"
                  fontSize={13}
                  fontFamily="IBM Plex Sans, sans-serif"
                >
                  {svc.title}
                </text>
                <text
                  x={-60}
                  y={14}
                  fill="#8b909a"
                  fontSize={11}
                  fontFamily="IBM Plex Mono, monospace"
                >
                  {fmtRps(svc.rps)} rps · {svc.circuit}
                </text>
              </g>
            );
          })}
        </svg>
      </Panel>

      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        {snapshot.services.map((svc) => (
          <Panel key={svc.id} className="p-4">
            <div className="flex items-start justify-between gap-2">
              <div>
                <div className="text-sm font-medium">{svc.title}</div>
                <div className="font-mono text-xs text-subtle">{svc.id}</div>
              </div>
              <StatusPill tone={healthTone(svc.status)} label={svc.status} />
            </div>
            <dl className="mt-4 grid grid-cols-2 gap-2 text-xs">
              <div>
                <dt className="text-subtle">RPS</dt>
                <dd className="font-mono tabular-nums">{fmtRps(svc.rps)}</dd>
              </div>
              <div>
                <dt className="text-subtle">p99</dt>
                <dd className="font-mono tabular-nums">{fmtMs(svc.p99)}</dd>
              </div>
              <div>
                <dt className="text-subtle">Errors</dt>
                <dd className="font-mono tabular-nums">{fmtPct(svc.errRate, 1)}</dd>
              </div>
              <div>
                <dt className="text-subtle">Pool</dt>
                <dd className="font-mono tabular-nums">
                  {svc.inflight}/{svc.maxInflight}
                </dd>
              </div>
            </dl>
            <div className="mt-3">
              <Badge
                tone={
                  svc.circuit === "open"
                    ? "crit"
                    : svc.circuit === "half-open"
                      ? "warn"
                      : "ok"
                }
              >
                {svc.breakerName} · {svc.circuit}
              </Badge>
            </div>
            <div className="mt-3 h-1.5 overflow-hidden rounded-full bg-bg-subtle">
              <div
                className={cn(
                  "h-full rounded-full",
                  svc.inflight / svc.maxInflight > 0.8 ? "bg-warn" : "bg-accent",
                )}
                style={{
                  width: `${Math.min(100, (svc.inflight / svc.maxInflight) * 100)}%`,
                }}
              />
            </div>
          </Panel>
        ))}
      </div>
    </div>
  );
}
