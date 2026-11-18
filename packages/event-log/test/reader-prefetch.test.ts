import { afterEach, expect, it, vi } from "vitest";
import type { Message, KafkaJS } from "@confluentinc/kafka-javascript";

const fixture = vi.hoisted(() => ({ client: undefined as any }));
vi.mock("@confluentinc/kafka-javascript", async () => {
  const { EventEmitter } = await import("node:events");
  return {
    default: {
      KafkaConsumer: class extends EventEmitter {
        history: any[] = [];
        frames: any[] = [];
        assigned = new Map<number, number>();
        paused = new Set<number>();
        pauseCalls: number[] = [];
        commits: any[][] = [];
        stores: any[] = [];
        seeks: any[] = [];
        automaticCommits = true;
        constructor(readonly options: any) {
          super();
          fixture.client = this;
        }
        setDefaultConsumeTimeout() {}
        connect(_options: unknown, done: () => void) {
          done();
          this.options.rebalance_cb({ code: 1 }, [
            { topic: "prefetch", partition: 0, offset: 0 },
            { topic: "prefetch", partition: 1, offset: 0 },
          ]);
        }
        subscribe() {}
        assign(values: any[]) {
          this.assigned = new Map(values.map((v) => [v.partition, v.offset ?? 0]));
          this.frames = this.history.filter(
            (m) => this.assigned.has(m.partition) && m.offset >= this.assigned.get(m.partition)!,
          );
        }
        unassign() {
          this.assigned.clear();
          this.frames = [];
        }
        pause(values: any[]) {
          for (const p of values) {
            this.paused.add(p.partition);
            this.pauseCalls.push(p.partition);
          }
        }
        resume(values: any[]) {
          for (const p of values) this.paused.delete(p.partition);
        }
        add(partition: number, offset: number, body = "x", eof = false) {
          const frame = {
            topic: "prefetch",
            partition,
            offset,
            value: Buffer.from(body),
            key: Buffer.from("key"),
            eof,
          };
          this.history.push(frame);
          this.frames.push(frame);
        }
        consume(_count: number, done: (e: unknown, values: Message[]) => void) {
          setTimeout(() => {
            const at = this.frames.findIndex(
              (m) => this.assigned.has(m.partition) && !this.paused.has(m.partition),
            );
            if (at < 0) {
              done(null, []);
              return;
            }
            const [m] = this.frames.splice(at, 1);
            if (m.eof) {
              this.emit("partition.eof", m);
              done(null, []);
            } else done(null, [m]);
          }, 1);
        }
        offsetsStore(values: any[]) {
          this.stores.push(...values);
        }
        seek(position: any, _timeout: number, done: () => void) {
          this.seeks.push(position);
          setTimeout(() => {
            this.frames = this.frames.filter((m) => m.partition !== position.partition);
            this.frames.push(
              ...this.history.filter(
                (m) => m.partition === position.partition && m.offset >= position.offset,
              ),
            );
            done();
          }, 1);
        }
        commit(offsets: any[]) {
          this.commits.push(offsets);
          if (this.automaticCommits)
            queueMicrotask(() => this.emit("offset.commit", null, offsets));
        }
        disconnect(done: () => void) {
          done();
        }
      },
    },
  };
});
import { KafkaPartitionReader } from "../src/kafka-partition-reader.ts";
import type { PrefetchLimits } from "../src/partition-prefetch.ts";
const readers: KafkaPartitionReader[] = [];
afterEach(async () => {
  await Promise.allSettled(readers.splice(0).map((r) => r.disconnect()));
});
async function create(handler: KafkaJS.EachBatchHandler, prefetch?: PrefetchLimits) {
  const positions: [number, bigint][] = [],
    errors: unknown[] = [];
  const reader = new KafkaPartitionReader({
    brokers: ["unused"],
    clientId: "prefetch",
    groupId: "prefetch",
    topic: "prefetch",
    ...(prefetch ? { prefetch } : {}),
    logger: { error: (e: unknown) => errors.push(e) } as unknown as KafkaJS.Logger,
    rebalance: async (_e, a, f) => f.assign(a),
    onPosition: (p, o) => positions.push([p, o]),
  });
  readers.push(reader);
  await reader.connect();
  reader.seedCommitted(
    new Map([
      [0, 0n],
      [1, 0n],
    ]),
  );
  await reader.subscribe({ topics: ["prefetch"] });
  await reader.run({ eachBatch: handler });
  return { reader, native: fixture.client, positions, errors };
}

it("re-observes an EOF-only tail when asynchronous assignment finishes inside a poll", async () => {
  const install = Promise.withResolvers<void>();
  const positions: [number, bigint][] = [];
  const reader = new KafkaPartitionReader({
    brokers: ["unused"],
    clientId: "late-assignment",
    groupId: "late-assignment",
    topic: "prefetch",
    logger: { error() {} } as unknown as KafkaJS.Logger,
    rebalance: async (_error, assignments, functions) => {
      await install.promise;
      functions.assign(assignments.map((a) => ({ ...a, offset: 7 })));
    },
    onPosition: (partition, offset) => positions.push([partition, offset]),
  });
  readers.push(reader);
  await reader.connect();
  const native = fixture.client;
  native.add(0, 7, "", true);
  native.add(1, 7, "", true);
  const consume = native.consume.bind(native);
  native.consume = (count: number, callback: unknown) => {
    // Poll captures the new generation before the async PG bootstrap assigns
    // partitions. Only EOF is returned: there is no data message to trigger rewind.
    queueMicrotask(() => install.resolve());
    consume(count, callback);
  };
  await reader.subscribe({ topics: ["prefetch"] });
  await reader.run({
    eachBatch: async () => {
      throw new Error("No data should be delivered");
    },
  });
  await vi.waitFor(
    () => {
      expect(positions).toContainEqual([0, 7n]);
      expect(positions).toContainEqual([1, 7n]);
    },
    { timeout: 500 },
  );
});

it("prefetches without per-record pause and holds EOF behind queued and active records", async () => {
  const release = Promise.withResolvers<void>(),
    entered = Promise.withResolvers<void>();
  const seen: string[] = [];
  const { reader, native, positions, errors } = await create(async ({ batch, resolveOffset }) => {
    const m = batch.messages[0]!;
    if (batch.partition === 0 && m.offset === "0") {
      entered.resolve();
      await release.promise;
    }
    seen.push(`${batch.partition}:${m.offset}`);
    resolveOffset(m.offset);
  });
  try {
    native.add(0, 0);
    native.add(0, 1);
    native.add(0, 2);
    native.add(0, 8, "", true);
    native.add(1, 0);
    await entered.promise;
    await vi.waitFor(() => expect(seen).toEqual(["1:0"]));
    expect(reader.statistics().queuedRecords).toBe(2);
    expect(positions).not.toContainEqual([0, 8n]);
    expect(native.pauseCalls).toEqual([]);
    release.resolve();
    await vi.waitFor(() => expect(positions).toContainEqual([0, 8n]));
    expect(seen).toEqual(["1:0", "0:0", "0:1", "0:2"]);
    expect(reader.statistics().records).toBe(0);
    expect(errors).toEqual([]);
  } finally {
    release.resolve();
  }
});

it("pauses only a full partition and preserves other partition progress", async () => {
  const release = Promise.withResolvers<void>(),
    seen: string[] = [];
  const { reader, native } = await create(
    async ({ batch }) => {
      if (batch.partition === 0 && batch.messages[0]!.offset === "0") await release.promise;
      seen.push(`${batch.partition}:${batch.messages[0]!.offset}`);
    },
    { maximumRecords: 20, maximumBytes: 100000, partitionRecords: 2, partitionBytes: 50000 },
  );
  try {
    for (let i = 0; i < 6; i++) native.add(0, i);
    native.add(1, 0);
    await vi.waitFor(() => expect(seen).toEqual(["1:0"]));
    expect(reader.statistics().records).toBe(2);
    expect(native.paused.has(0)).toBe(true);
    expect(native.paused.has(1)).toBe(false);
    release.resolve();
    await vi.waitFor(() => expect(seen.length).toBe(7));
    expect(seen.filter((s) => s.startsWith("0:"))).toEqual(
      Array.from({ length: 6 }, (_, i) => `0:${i}`),
    );
    expect(reader.statistics().peakRecords).toBeLessThanOrEqual(3);
    expect(reader.statistics().records).toBe(0);
  } finally {
    release.resolve();
  }
});

it("retains one oversized record without discarding it or admitting an unbounded batch", async () => {
  const release = Promise.withResolvers<void>();
  let count = 0;
  const { reader, native } = await create(
    async () => {
      if (count === 0) await release.promise;
      count++;
    },
    { maximumRecords: 4, maximumBytes: 1024, partitionRecords: 4, partitionBytes: 1024 },
  );
  try {
    native.add(0, 0, "x".repeat(4096));
    native.add(1, 0);
    await vi.waitFor(() => expect(reader.statistics().records).toBe(1));
    expect(reader.statistics().estimatedBytes).toBeGreaterThan(4096);
    expect(reader.statistics().pausedPartitions).toBe(2);
    release.resolve();
    await vi.waitFor(() => expect(count).toBe(2));
    expect(reader.statistics().records).toBe(0);
  } finally {
    release.resolve();
  }
});

it("drops queued old-epoch records on seek and resumes exactly at the requested record", async () => {
  const release = Promise.withResolvers<void>(),
    entered = Promise.withResolvers<void>();
  const seen: string[] = [];
  const { reader, native, positions, errors } = await create(
    async ({ batch, isStale, resolveOffset }) => {
      const m = batch.messages[0]!;
      if (m.offset === "0") {
        entered.resolve();
        await release.promise;
      }
      if (!isStale()) {
        seen.push(m.offset);
        resolveOffset(m.offset);
      }
    },
  );
  try {
    native.add(0, 0);
    native.add(0, 1);
    native.add(0, 2);
    native.add(0, 9, "", true);
    await entered.promise;
    await vi.waitFor(() => expect(reader.statistics().queuedRecords).toBe(2));
    reader.pause([{ topic: "prefetch", partitions: [0] }]);
    reader.seek({ topic: "prefetch", partition: 0, offset: "1" });
    release.resolve();
    await vi.waitFor(() => expect(reader.statistics().records).toBe(0));
    expect(positions).not.toContainEqual([0, 9n]);
    reader.resume([{ topic: "prefetch", partitions: [0] }]);
    await vi.waitFor(() => expect(positions).toContainEqual([0, 9n]));
    expect(seen).toEqual(["1", "2"]);
    expect(errors).toEqual([]);
  } finally {
    release.resolve();
  }
});

it("replays queued data in a new assignment without allowing an old completion to advance it", async () => {
  const release = Promise.withResolvers<void>(),
    entered = Promise.withResolvers<void>();
  const seen: string[] = [];
  const { reader, native, errors } = await create(async ({ batch, isStale, resolveOffset }) => {
    const m = batch.messages[0]!;
    if (m.offset === "0") {
      entered.resolve();
      await release.promise;
    }
    if (!isStale()) {
      seen.push(m.offset);
      resolveOffset(m.offset);
    }
  });
  try {
    native.add(0, 0);
    native.add(0, 1);
    native.add(0, 2);
    await entered.promise;
    await vi.waitFor(() => expect(reader.statistics().queuedRecords).toBe(2));
    native.options.rebalance_cb({ code: 1 }, [{ topic: "prefetch", partition: 0, offset: 1 }]);
    release.resolve();
    await vi.waitFor(() => expect(seen).toEqual(["1", "2"]));
    expect(native.stores.every((s: any) => s.offset >= 2)).toBe(true);
    expect(errors).toEqual([]);
  } finally {
    release.resolve();
  }
});

it("coalesces ready offsets and resolves no request on incomplete callback coverage", async () => {
  const { reader, native } = await create(async () => {});
  native.automaticCommits = false;
  let done = 0;
  const a = reader
    .commitOffset({ topic: "prefetch", partition: 0, offset: "3" })
    .then(() => done++);
  const b = reader
    .commitOffset({ topic: "prefetch", partition: 1, offset: "5" })
    .then(() => done++);
  try {
    await vi.waitFor(() => expect(native.commits.length).toBe(1));
    expect(native.commits[0]).toHaveLength(2);
    native.emit("offset.commit", null, [native.commits[0][0]]);
    await new Promise((r) => setTimeout(r, 10));
    expect(done).toBe(0);
    native.emit("offset.commit", null, native.commits[0]);
    await Promise.all([a, b]);
    expect(done).toBe(2);
    await reader.commitOffset({ topic: "prefetch", partition: 1, offset: "4" });
    expect(native.commits).toHaveLength(1);
  } finally {
    await reader.disconnect();
    await Promise.allSettled([a, b]);
  }
});

it("rejects queued offset work on disposal without sending it", async () => {
  const { reader, native } = await create(async () => {});
  const first = reader.commitOffset({ topic: "prefetch", partition: 0, offset: "1" });
  const second = reader.commitOffset({ topic: "prefetch", partition: 1, offset: "1" });
  const settled = Promise.allSettled([first, second]);
  await reader.disconnect();
  expect((await settled).every((result) => result.status === "rejected")).toBe(true);
  expect(native.commits).toHaveLength(0);
});

it("does not adopt an old offset acknowledgement into a replacement assignment", async () => {
  const { reader, native, errors } = await create(async () => {});
  native.automaticCommits = false;
  const old = reader.commitOffset({ topic: "prefetch", partition: 0, offset: "5" });
  const oldOutcome = Promise.allSettled([old]);
  await vi.waitFor(() => expect(native.commits).toHaveLength(1));
  native.options.rebalance_cb({ code: 1 }, [{ topic: "prefetch", partition: 0, offset: 0 }]);
  reader.seedCommitted(new Map([[0, 0n]]));
  native.emit("offset.commit", null, native.commits[0]);
  expect((await oldOutcome)[0]!.status).toBe("rejected");
  native.automaticCommits = true;
  await reader.commitOffset({ topic: "prefetch", partition: 0, offset: "3" });
  expect(native.commits).toHaveLength(2);
  expect(native.commits[1][0].offset).toBe(3);
  expect(errors).toEqual([]);
});
