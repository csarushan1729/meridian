import type { RegionId, ServiceId, Sku, TopicId } from "./types";

export const REGIONS: { id: RegionId; city: string; weight: number }[] = [
  { id: "us-east-1", city: "Ashburn", weight: 0.52 },
  { id: "eu-west-1", city: "Dublin", weight: 0.28 },
  { id: "ap-south-1", city: "Mumbai", weight: 0.2 },
];

export const SERVICES: {
  id: ServiceId;
  title: string;
  subtitle: string;
  region: RegionId;
  maxInflight: number;
  baseLatency: number;
  failRate: number;
}[] = [
  {
    id: "gateway",
    title: "Edge gateway",
    subtitle: "Admission · token bucket · TLS",
    region: "us-east-1",
    maxInflight: 24,
    baseLatency: 4,
    failRate: 0.002,
  },
  {
    id: "orders",
    title: "Order orchestrator",
    subtitle: "Saga · outbox · idempotency",
    region: "us-east-1",
    maxInflight: 16,
    baseLatency: 8,
    failRate: 0.004,
  },
  {
    id: "inventory",
    title: "Inventory",
    subtitle: "Reservation · sharding",
    region: "us-east-1",
    maxInflight: 12,
    baseLatency: 11,
    failRate: 0.01,
  },
  {
    id: "payments",
    title: "Payments",
    subtitle: "Capture · refund · PSP",
    region: "us-east-1",
    maxInflight: 8,
    baseLatency: 38,
    failRate: 0.03,
  },
  {
    id: "ledger",
    title: "Ledger",
    subtitle: "Double-entry consumer",
    region: "us-east-1",
    maxInflight: 10,
    baseLatency: 7,
    failRate: 0.002,
  },
  {
    id: "fulfillment",
    title: "Fulfillment",
    subtitle: "Warehouse allocation",
    region: "us-east-1",
    maxInflight: 10,
    baseLatency: 16,
    failRate: 0.008,
  },
  {
    id: "shipping",
    title: "Shipping",
    subtitle: "Carrier labels",
    region: "eu-west-1",
    maxInflight: 8,
    baseLatency: 22,
    failRate: 0.012,
  },
  {
    id: "notify",
    title: "Notify",
    subtitle: "Email · SMS · webhooks",
    region: "ap-south-1",
    maxInflight: 14,
    baseLatency: 14,
    failRate: 0.006,
  },
];

export const SKUS: (Sku & { stock: number })[] = [
  { id: "atlas-jacket", name: "Atlas Shell Jacket", price: 24800, weight: 0.18, stock: 420 },
  { id: "meridian-pack", name: "Meridian 32L Pack", price: 18900, weight: 0.16, stock: 310 },
  { id: "lattice-knit", name: "Lattice Merino Crew", price: 9600, weight: 0.22, stock: 800 },
  { id: "ridge-boot", name: "Ridge Trail Boot", price: 22000, weight: 0.12, stock: 140 },
  { id: "halo-bottle", name: "Halo Insulated Bottle", price: 3400, weight: 0.2, stock: 1200 },
  { id: "nimbus-down", name: "Nimbus Down Vest", price: 17600, weight: 0.12, stock: 95 },
];

export const TOPICS: { id: TopicId; partitions: number }[] = [
  { id: "orders.commands", partitions: 4 },
  { id: "orders.events", partitions: 6 },
  { id: "inventory.events", partitions: 4 },
  { id: "payments.events", partitions: 4 },
  { id: "fulfillment.events", partitions: 3 },
  { id: "shipping.events", partitions: 3 },
  { id: "notify.commands", partitions: 2 },
  { id: "dead-letter", partitions: 2 },
];

export const MESH_EDGES: { from: ServiceId | "client"; to: ServiceId }[] = [
  { from: "client", to: "gateway" },
  { from: "gateway", to: "orders" },
  { from: "orders", to: "inventory" },
  { from: "orders", to: "payments" },
  { from: "payments", to: "ledger" },
  { from: "orders", to: "fulfillment" },
  { from: "fulfillment", to: "shipping" },
  { from: "shipping", to: "notify" },
];

export const MESH_LAYOUT: Record<ServiceId, { x: number; y: number }> = {
  gateway: { x: 0.5, y: 0.07 },
  orders: { x: 0.5, y: 0.26 },
  inventory: { x: 0.16, y: 0.46 },
  payments: { x: 0.5, y: 0.46 },
  ledger: { x: 0.84, y: 0.46 },
  fulfillment: { x: 0.5, y: 0.66 },
  shipping: { x: 0.32, y: 0.88 },
  notify: { x: 0.68, y: 0.88 },
};

export const DEFAULT_CHAOS = {
  traffic: 1,
  paymentFail: 0.03,
  inventoryFail: 0.01,
  dropRate: 0,
  killed: {} as Partial<Record<ServiceId, boolean>>,
  latencyMult: {
    gateway: 1,
    orders: 1,
    inventory: 1,
    payments: 1,
    ledger: 1,
    fulfillment: 1,
    shipping: 1,
    notify: 1,
  },
  partitioned: {} as Partial<Record<RegionId, boolean>>,
  clockSkew: 1,
};

export function serviceTitle(id: ServiceId): string {
  return SERVICES.find((s) => s.id === id)?.title ?? id;
}
