// Server-only. Decides where dashboard data comes from:
//   1. Live: the control service of the Docker backend (real Kafka, Redis, Postgres)
//   2. Otherwise: the built-in simulation (so the old demo still works, e.g. on Vercel)
// Set DASHBOARD_MODE=demo to force the simulation. Set CONTROL_URL to point elsewhere.
import { mapLive, type LiveRaw } from "./live-map";
import type { LiveChaos, RegionId, Snapshot } from "./types";

const base = () =>
  (process.env.CONTROL_URL ?? "http://localhost:4010").replace(/\/$/, "");

let downUntil = 0; // if the control service is unreachable, do not hammer it for a few seconds

export interface ControlReply<T> {
  ok: boolean;
  status: number;
  data: T;
}

/** null = control service is not reachable (so callers may fall back to the simulation). */
export async function controlCall<T>(
  path: string,
  init?: { method?: string; body?: unknown },
): Promise<ControlReply<T> | null> {
  if (process.env.DASHBOARD_MODE === "demo") return null;
  if (Date.now() < downUntil) return null;
  try {
    const res = await fetch(`${base()}${path}`, {
      method: init?.method ?? "GET",
      headers: init?.body ? { "content-type": "application/json" } : undefined,
      body: init?.body ? JSON.stringify(init.body) : undefined,
      signal: AbortSignal.timeout(2500),
    });
    downUntil = 0;
    return { ok: res.ok, status: res.status, data: (await res.json()) as T };
  } catch {
    downUntil = Date.now() + 3000;
    return null;
  }
}

export async function getSnapshot(): Promise<Snapshot> {
  const live = await controlCall<LiveRaw>("/snapshot");
  if (live?.ok) return mapLive(live.data);
  const { getRuntime } = await import("./runtime.server");
  return (await getRuntime()).snapshot();
}

export async function placeOrder(input: {
  skuId?: string;
  qty?: number;
  region?: RegionId;
  idempotencyKey?: string;
}): Promise<string> {
  const live = await controlCall<{ orderId?: string; error?: string }>(
    "/place-order",
    {
      method: "POST",
      body: { skuId: input.skuId, qty: input.qty },
    },
  );
  if (live) {
    // Live backend answered: never fall back to the simulation, show the real result.
    if (live.ok && live.data.orderId) return live.data.orderId;
    throw new Error(live.data.error ?? `order rejected (${live.status})`);
  }
  const { getRuntime } = await import("./runtime.server");
  return (await getRuntime()).engine.placeOrder(input);
}

export async function restock(): Promise<void> {
  if (await controlCall("/restock", { method: "POST" })) return;
  const { getRuntime } = await import("./runtime.server");
  (await getRuntime()).engine.restock();
}

export async function setLiveChaos(input: Partial<LiveChaos>): Promise<void> {
  await controlCall("/chaos", { method: "POST", body: input });
}
