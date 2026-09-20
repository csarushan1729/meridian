// Topic names. The first five come from the original Meridian catalog;
// the two *.commands topics are new (orders sends commands, services reply with events).
export const TOPICS = {
  ORDERS_COMMANDS: 'orders.commands', // gateway  -> orders
  ORDERS_EVENTS: 'orders.events', //     orders   -> anyone (audit, dashboard)
  INVENTORY_COMMANDS: 'inventory.commands', // orders -> inventory
  INVENTORY_EVENTS: 'inventory.events', //     inventory -> orders
  PAYMENTS_COMMANDS: 'payments.commands', //   orders -> payments
  PAYMENTS_EVENTS: 'payments.events', //       payments -> orders, ledger
  DEAD_LETTER: 'dead-letter',
};

export const TOPIC_SPECS = [
  { topic: TOPICS.ORDERS_COMMANDS, partitions: 4 },
  { topic: TOPICS.ORDERS_EVENTS, partitions: 6 },
  { topic: TOPICS.INVENTORY_COMMANDS, partitions: 4 },
  { topic: TOPICS.INVENTORY_EVENTS, partitions: 4 },
  { topic: TOPICS.PAYMENTS_COMMANDS, partitions: 4 },
  { topic: TOPICS.PAYMENTS_EVENTS, partitions: 4 },
  { topic: TOPICS.DEAD_LETTER, partitions: 2 },
];
