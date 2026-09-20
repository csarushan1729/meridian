import { createServerFn } from "@tanstack/react-start";
import type { ChaosConfig, RegionId, Snapshot } from "@/lib/cluster/types";

export const getClusterSnapshot = createServerFn({ method: "GET" }).handler(
  async (): Promise<Snapshot> => {
    const { getRuntime } = await import("./runtime.server");
    const rt = await getRuntime();
    return rt.snapshot();
  },
);

export const placeOrderFn = createServerFn({ method: "POST" })
  .validator((d: { skuId?: string; qty?: number; region?: RegionId; idempotencyKey?: string }) => d)
  .handler(async ({ data }): Promise<string> => {
    const { getRuntime } = await import("./runtime.server");
    const rt = await getRuntime();
    return rt.engine.placeOrder(data);
  });

export const setChaosFn = createServerFn({ method: "POST" })
  .validator((d: Partial<ChaosConfig>) => d)
  .handler(async ({ data }): Promise<void> => {
    const { getRuntime } = await import("./runtime.server");
    const rt = await getRuntime();
    rt.engine.setChaos(data);
  });

export const restockFn = createServerFn({ method: "POST" }).handler(async (): Promise<void> => {
  const { getRuntime } = await import("./runtime.server");
  const rt = await getRuntime();
  rt.engine.restock();
});
