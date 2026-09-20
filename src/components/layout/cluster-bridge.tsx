import { useEffect, useState, type ReactNode } from "react";
import { hydrateCluster, startClusterPolling } from "@/lib/store";
import type { Snapshot } from "@/lib/cluster/types";

export function ClusterBridge({
  initial,
  children,
}: {
  initial: Snapshot;
  children: ReactNode;
}) {
  useState(() => {
    hydrateCluster(initial);
    return true;
  });

  useEffect(() => {
    hydrateCluster(initial);
    startClusterPolling();
  }, [initial]);

  return children;
}
