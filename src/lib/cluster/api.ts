import { createServerFn } from "@tanstack/react-start";
import { authMiddleware } from "@/lib/auth/middleware";
import type {
  ChaosConfig,
  LiveChaos,
  RegionId,
  Snapshot,
} from "@/lib/cluster/types";

// Live data (Docker backend) when the control service is reachable, otherwise the simulation.
// The decision is made in snapshot.server.ts.

export const getClusterSnapshot = createServerFn({ method: "GET" })
  .middleware([authMiddleware])
  .handler(async (): Promise<Snapshot> => {
    const { getSnapshot } = await import("./snapshot.server");
    return getSnapshot();
  });

export const placeOrderFn = createServerFn({ method: "POST" })
  .middleware([authMiddleware])
  .validator(
    (d: {
      skuId?: string;
      qty?: number;
      region?: RegionId;
      idempotencyKey?: string;
    }) => d,
  )
  .handler(async ({ data }): Promise<string> => {
    const { placeOrder } = await import("./snapshot.server");
    return placeOrder(data);
  });

/** Simulation chaos (demo mode only). */
export const setChaosFn = createServerFn({ method: "POST" })
  .middleware([authMiddleware])
  .validator((d: Partial<ChaosConfig>) => d)
  .handler(async ({ data }): Promise<void> => {
    const { getRuntime } = await import("./runtime.server");
    const rt = await getRuntime();
    rt.engine.setChaos(data);
  });

/** Real chaos (live mode): switches in Redis that the Docker services read. */
export const setLiveChaosFn = createServerFn({ method: "POST" })
  .middleware([authMiddleware])
  .validator((d: Partial<LiveChaos>) => d)
  .handler(async ({ data }): Promise<void> => {
    const { setLiveChaos } = await import("./snapshot.server");
    await setLiveChaos(data);
  });

export const restockFn = createServerFn({ method: "POST" })
  .middleware([authMiddleware])
  .handler(async (): Promise<void> => {
    const { restock } = await import("./snapshot.server");
    await restock();
  });
