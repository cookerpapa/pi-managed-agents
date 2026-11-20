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
        timeoutMs = 0;
        polls: { count: number; timeoutMs: number }[] = [];
        returnedCounts: number[] = [];
        constructor(readonly options: any) {
          super();
          fixture.client = this;
        }
        setDefaultConsumeTimeout(timeoutMs: number) {
          this.timeoutMs = timeoutMs;
        }
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
        consume(count: number, done: (e: unknown, values: Message[]) => void) {
          this.polls.push({ count, timeoutMs: this.timeoutMs });
          setTimeout(() => {
            const records = [];
            while (records.length < count) {
              const at = this.frames.findIndex(
                (m) => this.assigned.has(m.partition) && !this.paused.has(m.partition),
              );
              if (at < 0) break;
              const [m] = this.frames.splice(at, 1);
              if (m.eof) this.emit("partition.eof", m);
              else records.push(m);
            }
            this.returnedCounts.push(records.length);
            done(null, records);
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
import { PartitionPrefetch, type PrefetchLimits } from "../src/partition-prefetch.ts";
const readers: KafkaPartitionReader[] = [];
afterEach(async () => {
  await Promise.allSettled(readers.splice(0).map((r) => r.disconnect()));
});
async function create(
  handler: KafkaJS.EachBatchHandler,
  prefetch?: PrefetchLimits,
  readyPrefixes = false,
) {
  const positions: [number, bigint][] = [],
    errors: unknown[] = [];
  const reader = new KafkaPartitionReader({
    brokers: ["unused"],
    clientId: "prefetch",
    groupId: "prefetch",
    topic: "prefetch",
    batchRead: true,
    readyPrefixes,
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

it("bounds a ready prefix by count, bytes, generation and seek epoch", () => {
  const p = new PartitionPrefetch();
  const push = (index: number, bytes = 1, generation = 1, epoch = 0) =>
    p.push(
      { partition: 0, offset: index, value: Buffer.alloc(bytes) } as Message,
      generation,
      epoch,
    );
  for (let i = 0; i < 18; i++) push(i);
  const take = () => {
    const records = p.takeReady(p.take(0)!);
    for (const r of records) p.release(r);
    return records.map((r) => r.message.offset);
  };
  expect(take()).toHaveLength(16);
  expect(take()).toEqual([16, 17]);
  push(18, 300_000);
  push(19, 140_000);
  push(20, 140_000);
  push(21, 1, 1, 7);
  push(22, 1, 2, 7);
  for (let i = 18; i <= 22; i++) expect(take()).toEqual([i]);
  expect(p.statistics().records).toBe(0);
});

it("dispatches a lone record immediately and holds EOF until the last already-buffered batch settles", async () => {
  const first = Promise.withResolvers<void>(),
    firstRelease = Promise.withResolvers<void>();
  const batchReady = Promise.withResolvers<void>(),
    batchRelease = Promise.withResolvers<void>();
  const lengths: number[] = [];
  const { native, reader, positions } = await create(
    async ({ batch }) => {
      lengths.push(batch.messages.length);
      if (batch.messages[0]!.offset === "0") {
        first.resolve();
        await firstRelease.promise;
      } else if (batch.messages[0]!.offset === "1") {
        batchReady.resolve();
        await batchRelease.promise;
      }
    },
    undefined,
    true,
  );
  try {
    native.add(0, 0);
    await first.promise;
    expect(lengths).toEqual([1]);
    for (let i = 1; i <= 21; i++) native.add(0, i);
    native.add(0, 22, "", true);
    await vi.waitFor(() => expect(reader.statistics().queuedRecords).toBe(21));
    firstRelease.resolve();
    await batchReady.promise;
    expect(lengths).toEqual([1, 16]);
    expect(reader.statistics().records).toBe(21);
    expect(positions).not.toContainEqual([0, 22n]);
    batchRelease.resolve();
    await vi.waitFor(() => expect(positions).toContainEqual([0, 22n]));
    expect(lengths).toEqual([1, 16, 5]);
  } finally {
    firstRelease.resolve();
    batchRelease.resolve();
  }
});

it("re-observes an EOF-only tail when asynchronous assignment finishes inside a poll", async () => {
  const install = Promise.withResolvers<void>();
  const positions: [number, bigint][] = [];
  const reader = new KafkaPartitionReader({
    brokers: ["unused"],
    clientId: "late-assignment",
    groupId: "late-assignment",
    topic: "prefetch",
    batchRead: true,
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

it("refetches only the first unadmitted suffix per partition and holds its EOF behind it", async () => {
  const held = Promise.withResolvers<void>();
  const seen: string[] = [];
  const { reader, native, positions } = await create(
    async ({ batch, resolveOffset }) => {
      const offset = batch.messages[0]!.offset;
      if (batch.partition === 0 && offset === "0") await held.promise;
      seen.push(batch.partition + ":" + offset);
      resolveOffset(offset);
    },
    { maximumRecords: 20, maximumBytes: 100000, partitionRecords: 2, partitionBytes: 50000 },
  );
  try {
    for (let i = 0; i < 6; i++) native.add(0, i);
    native.add(0, 9, "", true);
    native.add(1, 0);
    await vi.waitFor(() => expect(seen).toEqual(["1:0"]));
    expect(native.polls).toContainEqual({ count: 1, timeoutMs: 25 });
    expect(native.polls).toContainEqual({ count: 32, timeoutMs: 0 });
    expect(
      native.polls.some(
        (p: { count: number; timeoutMs: number }) => p.count > 1 && p.timeoutMs !== 0,
      ),
    ).toBe(false);
    expect(native.returnedCounts).toContain(6);
    expect(reader.statistics().records).toBe(2);
    expect(positions).not.toContainEqual([0, 9n]);
    held.resolve();
    await vi.waitFor(() => expect(positions).toContainEqual([0, 9n]));
    expect(seen).toEqual(["1:0", "0:0", "0:1", "0:2", "0:3", "0:4", "0:5"]);
    // Offset 2 is the first blocking read after rewind. It settles before the
    // next drain accepts 3 and 4, so the first remaining offset is 5.
    expect(native.seeks.map((s: { offset: number }) => s.offset)).toEqual([2, 5]);
  } finally {
    held.resolve();
  }
});

it("retains one oversized record without discarding it or admitting an unbounded batch", async () => {
  const release = Promise.withResolvers<void>();
  let count = 0;
  const { reader, native } = await create(
    async ({ batch }) => {
      if (batch.partition === 0) await release.promise;
      count++;
    },
    { maximumRecords: 4, maximumBytes: 1024, partitionRecords: 4, partitionBytes: 1024 },
  );
  try {
    native.add(1, 0);
    native.add(0, 0, "x".repeat(4096));
    native.add(1, 1);
    await vi.waitFor(() => expect(reader.statistics().estimatedBytes).toBeGreaterThan(4096));
    expect(reader.statistics().records).toBe(1);
    expect(native.returnedCounts).toContain(2);
    expect(reader.statistics().pausedPartitions).toBe(2);
    release.resolve();
    await vi.waitFor(() => expect(count).toBe(3));
    expect(reader.statistics().records).toBe(0);
    expect(reader.statistics().peakRecords).toBe(1);
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
    { maximumRecords: 10, maximumBytes: 100000, partitionRecords: 3, partitionBytes: 50000 },
  );
  try {
    native.add(0, 0);
    native.add(0, 1);
    native.add(0, 2);
    native.add(0, 3);
    native.add(0, 4);
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
    expect(seen).toEqual(["1", "2", "3", "4"]);
    expect(errors).toEqual([]);
  } finally {
    release.resolve();
  }
});

it("replays queued data in a new assignment without allowing an old completion to advance it", async () => {
  const release = Promise.withResolvers<void>(),
    entered = Promise.withResolvers<void>();
  const seen: string[] = [];
  const { reader, native, errors } = await create(
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
    { maximumRecords: 10, maximumBytes: 100000, partitionRecords: 2, partitionBytes: 50000 },
  );
  try {
    native.add(0, 0);
    native.add(0, 1);
    native.add(0, 2);
    await entered.promise;
    await vi.waitFor(() => expect(reader.statistics().queuedRecords).toBe(1));
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
