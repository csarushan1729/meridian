import { createFileRoute } from "@tanstack/react-router";
import { useCluster } from "@/lib/store";
import { fmtId, fmtMs } from "@/lib/format";
import { Panel, PanelHeader } from "@/components/shared/panel";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import type { Trace } from "@/lib/cluster/types";

export const Route = createFileRoute("/traces")({ component: TracesPage });

function TracesPage() {
  const snapshot = useCluster((s) => s.snapshot);
  const selectedId = useCluster((s) => s.selectedTraceId);
  const selectTrace = useCluster((s) => s.selectTrace);
  const selectOrder = useCluster((s) => s.selectOrder);
  if (!snapshot) return null;

  const selected =
    snapshot.traces.find((t) => t.traceId === selectedId) ?? snapshot.traces[0] ?? null;

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-4">
      <div>
        <p className="text-xs tracking-wide text-subtle uppercase">Observability</p>
        <h1 className="mt-1 text-2xl font-medium tracking-tight">Traces</h1>
        <p className="mt-2 max-w-2xl text-sm text-muted">
          Every hop is a span. Waterfalls show where time went — and which breaker
          short-circuited a call.
        </p>
      </div>

      <div className="grid gap-4 lg:grid-cols-[minmax(0,0.9fr)_minmax(0,1.3fr)]">
        <Panel className="p-0">
          <div className="p-4 pb-0">
            <PanelHeader title="Recent traces" />
          </div>
          <ul className="max-h-[28rem] overflow-auto">
            {snapshot.traces.map((trace) => (
              <li key={trace.traceId}>
                <button
                  type="button"
                  onClick={() => {
                    selectTrace(trace.traceId);
                    if (trace.orderId) selectOrder(trace.orderId);
                  }}
                  className={cn(
                    "flex w-full min-h-14 items-center justify-between gap-3 border-t border-border px-4 py-3 text-left",
                    selected?.traceId === trace.traceId && "bg-bg-subtle",
                  )}
                >
                  <div className="min-w-0">
                    <div className="font-mono text-sm">{fmtId(trace.traceId, 12)}</div>
                    <div className="text-xs text-muted">
                      {trace.spans.length} spans
                      {trace.orderId ? ` · ${fmtId(trace.orderId)}` : ""}
                    </div>
                  </div>
                  <Badge tone={trace.status === "error" ? "crit" : "ok"}>
                    {trace.end && trace.start
                      ? fmtMs(trace.end - trace.start)
                      : "live"}
                  </Badge>
                </button>
              </li>
            ))}
          </ul>
        </Panel>
        {selected ? <Waterfall trace={selected} /> : <Panel>No traces yet.</Panel>}
      </div>
    </div>
  );
}

function Waterfall({ trace }: { trace: Trace }) {
  const start = trace.start;
  const end = Math.max(
    trace.end ?? start + 1,
    ...trace.spans.map((s) => s.end ?? s.start + 1),
  );
  const window = Math.max(8, end - start);

  return (
    <Panel>
      <PanelHeader
        title={`Trace ${fmtId(trace.traceId, 14)}`}
        aside={
          <Badge tone={trace.status === "error" ? "crit" : "ok"}>
            {trace.status}
          </Badge>
        }
      />
      <ul className="space-y-2">
        {trace.spans.map((span) => {
          const spanEnd = span.end ?? end;
          const left = ((span.start - start) / window) * 100;
          const width = Math.max(2, ((spanEnd - span.start) / window) * 100);
          return (
            <li key={span.spanId}>
              <div className="flex items-baseline justify-between gap-2">
                <span className="truncate text-xs">
                  <span className="text-subtle">{span.service}</span>
                  {"  "}
                  {span.name}
                </span>
                <span className="shrink-0 font-mono text-xs tabular-nums text-muted">
                  {fmtMs(spanEnd - span.start)}
                </span>
              </div>
              <div className="mt-1 h-2 rounded-full bg-bg-subtle">
                <div
                  className={cn(
                    "h-2 rounded-full",
                    span.status === "error" ? "bg-crit" : "bg-accent",
                  )}
                  style={{ marginLeft: `${left}%`, width: `${width}%` }}
                />
              </div>
              {span.detail ? (
                <p className="mt-1 font-mono text-xs text-subtle">{span.detail}</p>
              ) : null}
            </li>
          );
        })}
      </ul>
    </Panel>
  );
}
