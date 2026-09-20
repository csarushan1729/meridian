import { createFileRoute } from "@tanstack/react-router";
import { useCluster } from "@/lib/store";
import { fmtClock, fmtId, fmtMoney } from "@/lib/format";
import { Panel, PanelHeader } from "@/components/shared/panel";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import type { Order, OrderStatus } from "@/lib/cluster/types";

export const Route = createFileRoute("/orders")({ component: OrdersPage });

const TONE: Record<OrderStatus, "ok" | "warn" | "crit" | "info" | "neutral"> = {
  accepted: "info",
  reserving: "info",
  charging: "info",
  fulfilling: "info",
  shipping: "info",
  notifying: "info",
  completed: "ok",
  compensating: "warn",
  failed: "crit",
};

function OrdersPage() {
  const snapshot = useCluster((s) => s.snapshot);
  const selectedId = useCluster((s) => s.selectedOrderId);
  const selectOrder = useCluster((s) => s.selectOrder);
  const selectTrace = useCluster((s) => s.selectTrace);
  if (!snapshot) return null;

  const selected =
    snapshot.orders.find((o) => o.id === selectedId) ?? snapshot.orders[0] ?? null;

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-4">
      <div>
        <p className="text-xs tracking-wide text-subtle uppercase">Orchestration</p>
        <h1 className="mt-1 text-2xl font-medium tracking-tight">Sagas</h1>
        <p className="mt-2 max-w-2xl text-sm text-muted">
          Each order is a state machine. If payments decline, inventory is released
          and captures are refunded — a compensating transaction, not a distributed
          lock.
        </p>
      </div>

      <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(0,1.1fr)]">
        <Panel className="p-0">
          <div className="p-4 pb-0">
            <PanelHeader
              title="Live orders"
              aside={
                <span className="font-mono text-xs text-muted">
                  {snapshot.ordersOk} ok · {snapshot.ordersFailed} failed
                </span>
              }
            />
          </div>
          <ul className="max-h-[28rem] overflow-auto">
            {snapshot.orders.map((order) => (
              <li key={order.id}>
                <button
                  type="button"
                  onClick={() => {
                    selectOrder(order.id);
                    selectTrace(order.traceId);
                  }}
                  className={cn(
                    "flex w-full min-h-14 items-center justify-between gap-3 border-t border-border px-4 py-3 text-left",
                    selected?.id === order.id && "bg-bg-subtle",
                  )}
                >
                  <div className="min-w-0">
                    <div className="truncate text-sm">{order.skuName}</div>
                    <div className="font-mono text-xs text-subtle">
                      {fmtId(order.id)} · {order.region}
                    </div>
                  </div>
                  <Badge tone={TONE[order.status]}>{order.status}</Badge>
                </button>
              </li>
            ))}
            {snapshot.orders.length === 0 ? (
              <li className="px-4 pb-4 text-sm text-muted">
                No orders yet — traffic is spinning up.
              </li>
            ) : null}
          </ul>
        </Panel>

        {selected ? <SagaDetail order={selected} /> : <Panel>Awaiting a saga.</Panel>}
      </div>
    </div>
  );
}

function SagaDetail({ order }: { order: Order }) {
  return (
    <Panel>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-medium">{order.skuName}</h2>
          <p className="mt-1 font-mono text-xs text-muted">
            {order.id} · {fmtMoney(order.amount)} · {order.region}
          </p>
        </div>
        <Badge tone={TONE[order.status]}>{order.status}</Badge>
      </div>
      {order.compensateReason ? (
        <p className="mt-3 text-sm text-warn">{order.compensateReason}</p>
      ) : null}
      <ol className="mt-5 space-y-0">
        {order.steps.map((step, i) => (
          <li key={`${step.name}-${step.at}-${i}`} className="flex gap-3">
            <div className="flex w-4 flex-col items-center">
              <span
                className={cn(
                  "mt-1 size-2 rounded-full",
                  step.ok ? "bg-ok" : "bg-crit",
                )}
              />
              {i < order.steps.length - 1 ? (
                <span className="w-px flex-1 bg-border" />
              ) : null}
            </div>
            <div className="pb-4">
              <div className="text-sm">{step.name}</div>
              <div className="text-xs text-muted">{step.detail}</div>
              <div className="font-mono text-xs text-subtle">{fmtClock(step.at)}</div>
            </div>
          </li>
        ))}
      </ol>
      <p className="font-mono text-xs text-subtle">
        idem {order.idempotencyKey} · trace {order.traceId}
      </p>
    </Panel>
  );
}
