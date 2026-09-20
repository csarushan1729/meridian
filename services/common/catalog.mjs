// Same products as the original Meridian catalog (src/lib/cluster/catalog.ts).
// price is in minor units (integer), so we never do money math with floats.
export const SKUS = [
  { id: 'atlas-jacket', name: 'Atlas Shell Jacket', price: 24800, stock: 420 },
  { id: 'meridian-pack', name: 'Meridian 32L Pack', price: 18900, stock: 310 },
  { id: 'lattice-knit', name: 'Lattice Merino Crew', price: 9600, stock: 800 },
  { id: 'ridge-boot', name: 'Ridge Trail Boot', price: 22000, stock: 140 },
  { id: 'halo-bottle', name: 'Halo Insulated Bottle', price: 3400, stock: 1200 },
  { id: 'nimbus-down', name: 'Nimbus Down Vest', price: 17600, stock: 95 },
];

export const skuById = (id) => SKUS.find((s) => s.id === id);
