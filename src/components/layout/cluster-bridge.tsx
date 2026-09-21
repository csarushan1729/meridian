import { useEffect, useState, type ReactNode } from "react";
import { hydrateCluster, startClusterPolling } from "@/lib/store";
import type { Snapshot } from "@/lib/cluster/types";

export function ClusterBridge({
  initial,
  children,
}: {
  initial: Snapshot | null;
  children: ReactNode;
}) {
  useState(() => {
    if (initial) hydrateCluster(initial);
    return true;
  });

  useEffect(() => {
    if (initial) hydrateCluster(initial);
    startClusterPolling();
  }, [initial]);

  return children;
}
