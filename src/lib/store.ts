import { create } from "zustand";
import { getClusterSnapshot, placeOrderFn, restockFn, setChaosFn } from "@/lib/cluster/api";
import { bootSnapshot } from "@/lib/cluster/engine";
import type { ChaosConfig, RegionId, Snapshot } from "@/lib/cluster/types";

const IDLE = bootSnapshot();

interface ClusterState {
  ready: boolean;
  snapshot: Snapshot;
  selectedOrderId: string | null;
  selectedTraceId: string | null;
  selectOrder: (id: string | null) => void;
  selectTrace: (id: string | null) => void;
}

export const useCluster = create<ClusterState>((set) => ({
  ready: false,
  snapshot: IDLE,
  selectedOrderId: null,
  selectedTraceId: null,
  selectOrder: (id) => set({ selectedOrderId: id }),
  selectTrace: (id) => set({ selectedTraceId: id }),
}));

let pollTimer: ReturnType<typeof setInterval> | null = null;

export function hydrateCluster(snapshot: Snapshot) {
  useCluster.setState({ snapshot, ready: true });
}

export function startClusterPolling() {
  if (typeof window === "undefined") return;
  if (pollTimer != null) return;
  const tick = async () => {
    try {
      const snapshot = await getClusterSnapshot();
      useCluster.setState({ snapshot, ready: true });
    } catch {
      /* keep last snapshot */
    }
  };
  void tick();
  pollTimer = setInterval(() => void tick(), 280);
}

export function placeOrder(input?: {
  skuId?: string;
  qty?: number;
  region?: RegionId;
  idempotencyKey?: string;
}) {
  void (async () => {
    const id = await placeOrderFn({ data: input ?? {} });
    useCluster.setState({ selectedOrderId: id });
    try {
      const snapshot = await getClusterSnapshot();
      useCluster.setState({ snapshot, ready: true });
    } catch {
      /* next poll */
    }
  })();
}

export function setChaos(partial: Partial<ChaosConfig>) {
  void setChaosFn({ data: partial });
}

export function restock() {
  void restockFn();
}
