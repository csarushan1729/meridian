export const NAV = [
  { to: "/", label: "Overview", hint: "SLOs and live traffic" },
  { to: "/mesh", label: "Mesh", hint: "Service topology" },
  { to: "/orders", label: "Sagas", hint: "Order workflows" },
  { to: "/traces", label: "Traces", hint: "Distributed spans" },
  { to: "/bus", label: "Kafka", hint: "Topics and lag" },
  { to: "/platform", label: "Platform", hint: "Kafka · Redis · Postgres" },
  { to: "/regions", label: "Regions", hint: "Raft quorum" },
  { to: "/chaos", label: "Chaos", hint: "Inject failure" },
  { to: "/incidents", label: "Incidents", hint: "Timeline" },
] as const;

export const PRIMARY_NAV = NAV.slice(0, 4);
export const MORE_NAV = NAV.slice(4);
