import { Ewma, HashRing } from "@/lib/cluster/primitives";
import type { BusEvent, ConsumerSnapshot, TopicId, TopicSnapshot } from "@/lib/cluster/types";
import { TOPICS } from "@/lib/cluster/catalog";

/**
 * Kafka-compatible broker.
 *
 * Producers write to a partitioned, append-only log. Consumer groups track
 * committed offsets independently (at-least-once). Poison messages go to a
 * dead-letter topic. The in-memory log is the page cache; `takeWal()` is the
 * write-ahead log the runtime flushes to Postgres.
 *
 * AWS swap: keep this class's methods and point a KafkaJS adapter at MSK.
 * Callers (services) never talk to Postgres directly for events.
 */

export type KafkaRecord = BusEvent;

type Partition = { next: number; events: KafkaRecord[] };

type TopicRt = {
  id: TopicId;
  partitions: Partition[];
  ring: HashRing;
  produced: number;
  rate: Ewma;
};

type GroupCursor = { topic: TopicId; offsets: number[] };

export class KafkaBroker {
  readonly name = "helix-kafka";
  readonly implementation = "Partitioned log + Postgres WAL (Kafka-compatible)";
  private readonly topics: Record<TopicId, TopicRt> = {} as Record<TopicId, TopicRt>;
  private readonly groups = new Map<string, GroupCursor>();
  private readonly groupRate = new Map<string, Ewma>();
  private wal: KafkaRecord[] = [];
  persisted = 0;

  constructor(specs = TOPICS) {
    for (const t of specs) {
      this.topics[t.id] = {
        id: t.id,
        partitions: Array.from({ length: t.partitions }, () => ({ next: 0, events: [] })),
        ring: new HashRing(t.partitions),
        produced: 0,
        rate: new Ewma(0.2),
      };
    }
  }

  ensureGroup(groupId: string, topic: TopicId) {
    if (this.groups.has(groupId)) return;
    const n = this.topics[topic]!.partitions.length;
    this.groups.set(groupId, { topic, offsets: Array.from({ length: n }, () => 0) });
    this.groupRate.set(groupId, new Ewma(0.2));
  }

  produce(input: {
    topic: TopicId;
    key: string;
    type: string;
    traceId: string;
    payload: string;
    ts: number;
  }): KafkaRecord {
    const topic = this.topics[input.topic];
    const n = topic.partitions.length;
    const partition = topic.ring.node(input.key, n);
    const part = topic.partitions[partition]!;
    const rec: KafkaRecord = {
      offset: part.next++,
      partition,
      topic: input.topic,
      key: input.key,
      type: input.type,
      ts: input.ts,
      traceId: input.traceId,
      payload: input.payload,
    };
    part.events.push(rec);
    if (part.events.length > 64) part.events.shift();
    topic.produced += 1;
    topic.rate.push(1);
    this.wal.push(rec);
    if (this.wal.length > 400) this.wal.splice(0, this.wal.length - 400);
    return rec;
  }

  /** Fetch uncommitted records for a group. Does not advance the commit. */
  poll(groupId: string, topic: TopicId, max = 3): KafkaRecord[] {
    this.ensureGroup(groupId, topic);
    const cursor = this.groups.get(groupId)!;
    const t = this.topics[topic];
    const out: KafkaRecord[] = [];
    for (let p = 0; p < t.partitions.length && out.length < max; p++) {
      const want = cursor.offsets[p] ?? 0;
      const next = t.partitions[p]!.events.find((e) => e.offset >= want);
      if (next) out.push(next);
    }
    return out;
  }

  commit(groupId: string, topic: TopicId, partition: number, nextOffset: number) {
    this.ensureGroup(groupId, topic);
    const cursor = this.groups.get(groupId)!;
    cursor.offsets[partition] = Math.max(cursor.offsets[partition] ?? 0, nextOffset);
    this.groupRate.get(groupId)?.push(1);
  }

  lag(groupId: string, topic: TopicId): number {
    const cursor = this.groups.get(groupId);
    if (!cursor) return 0;
    const t = this.topics[topic];
    let n = 0;
    for (let i = 0; i < t.partitions.length; i++) {
      n += Math.max(0, t.partitions[i]!.next - (cursor.offsets[i] ?? 0));
    }
    return n;
  }

  takeWal(): KafkaRecord[] {
    const batch = this.wal;
    this.wal = [];
    return batch;
  }

  load(records: KafkaRecord[], offsets: { groupId: string; topic: TopicId; partition: number; committed: number }[]) {
    for (const rec of records) {
      const topic = this.topics[rec.topic];
      if (!topic) continue;
      const part = topic.partitions[rec.partition];
      if (!part) continue;
      part.events.push(rec);
      part.next = Math.max(part.next, rec.offset + 1);
      topic.produced += 1;
      this.persisted += 1;
    }
    for (const t of Object.values(this.topics)) {
      for (const p of t.partitions) {
        if (p.events.length > 64) p.events.splice(0, p.events.length - 64);
      }
    }
    for (const off of offsets) {
      this.ensureGroup(off.groupId, off.topic);
      const cursor = this.groups.get(off.groupId)!;
      cursor.offsets[off.partition] = off.committed;
    }
  }

  offsetDump(): { groupId: string; topic: TopicId; partition: number; committed: number }[] {
    const out: { groupId: string; topic: TopicId; partition: number; committed: number }[] = [];
    for (const [groupId, cursor] of this.groups) {
      cursor.offsets.forEach((committed, partition) => {
        out.push({ groupId, topic: cursor.topic, partition, committed });
      });
    }
    return out;
  }

  topicSnapshots(): TopicSnapshot[] {
    return TOPICS.map((meta) => {
      const rt = this.topics[meta.id];
      return {
        id: meta.id,
        partitions: rt.partitions.map((p, id) => ({
          id,
          produced: p.next,
          depth: p.events.length,
          lastType: p.events[p.events.length - 1]?.type ?? "—",
        })),
        produced: rt.produced,
        rate: Math.max(0, rt.rate.value * 8),
      };
    });
  }

  consumerSnapshots(ids: { id: string; topic: TopicId }[]): ConsumerSnapshot[] {
    return ids.map((cg) => ({
      id: cg.id,
      topic: cg.topic,
      lag: this.lag(cg.id, cg.topic),
      rate: Math.max(0, (this.groupRate.get(cg.id)?.value ?? 0) * 6),
    }));
  }

  stats() {
    let partitions = 0;
    let messages = 0;
    for (const t of Object.values(this.topics)) {
      partitions += t.partitions.length;
      messages += t.produced;
    }
    return {
      name: this.name,
      implementation: this.implementation,
      topics: TOPICS.length,
      partitions,
      messages,
      persisted: this.persisted,
      consumerGroups: this.groups.size,
      dlq: this.topics["dead-letter"]?.produced ?? 0,
      walDepth: this.wal.length,
    };
  }

  get dlq(): number {
    return this.topics["dead-letter"]?.produced ?? 0;
  }
}
