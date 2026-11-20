import { beforeEach, expect, it, vi } from "vitest";
import type { KafkaJS } from "@confluentinc/kafka-javascript";

const f = vi.hoisted(() => ({
  committed: "3",
  low: "0",
  high: "5",
  assignments: [] as number[][],
  reassign: undefined as (() => Promise<void>) | undefined,
  stale: false,
  commit: vi.fn(),
  consumers: 0,
  partitions: 1,
  logger: undefined as KafkaJS.Logger | undefined,
  batch: undefined as ((payload: KafkaJS.EachBatchPayload) => Promise<void>) | undefined,
}));
vi.mock("@confluentinc/kafka-javascript", () => ({
  default: {
    CODES: { ERRORS: { ERR__ASSIGN_PARTITIONS: 1 } },
    KafkaJS: {
      logLevel: { NOTHING: 0 },
      Kafka: class {
        admin() {
          return {
            async connect() {},
            async disconnect() {},
            async fetchTopicOffsets() {
              return Array.from({ length: f.partitions }, (_, partition) => ({
                partition,
                low: f.low,
                high: f.high,
              }));
            },
            async fetchOffsets() {
              return [
                {
                  partitions: Array.from({ length: f.partitions }, (_, partition) => ({
                    partition,
                    offset: f.committed,
                  })),
                },
              ];
            },
          };
        }
      },
    },
  },
}));
vi.mock("../src/kafka-partition-reader.ts", () => ({
  KafkaPartitionReader: class {
    constructor(
      readonly options: import("../src/kafka-partition-reader.ts").PartitionReaderOptions,
    ) {
      f.consumers++;
      f.logger = options.logger;
      f.reassign = () => this.connect();
    }
    async connect() {
      await this.options.rebalance(
        { code: 1 },
        Array.from({ length: f.partitions }, (_, partition) => ({ topic: "log", partition })),
        {
          assign(values) {
            f.assignments.push(values.map((v) => v.offset!));
          },
          unassign() {},
        },
      );
    }
    async subscribe() {}
    async disconnect() {}
    pause() {}
    seek() {}
    resume() {}
    seedCommitted() {}
    async run(options: { eachBatch: typeof f.batch }) {
      f.batch = options.eachBatch;
    }
    commitOffset = f.commit;
  },
}));
import { KafkaLogConsumer, type KafkaLogConsumerOptions } from "../src/kafka-log-consumer.ts";

beforeEach(() => {
  f.committed = "3";
  f.low = "0";
  f.high = "5";
  f.assignments = [];
  f.reassign = undefined;
  f.stale = false;
  f.batch = undefined;
  f.consumers = 0;
  f.partitions = 1;
  f.logger = undefined;
  f.commit.mockReset().mockResolvedValue(undefined);
});
async function fixture(options?: {
  memory?: boolean;
  beforeHandle?: () => Promise<void>;
  prepareReady?: KafkaLogConsumerOptions<{ command: boolean; n: number }>["prepareReady"];
}) {
  const effects: number[] = [],
    projected: number[] = [];
  const consumer = new KafkaLogConsumer<{ command: boolean; n: number }>({
    brokers: ["fake"],
    topic: "log",
    groupId: "group",
    clientId: "test",
    ...(options?.prepareReady ? { prepareReady: options.prepareReady } : {}),
    ...(options?.memory
      ? { commitMessages: false, resumeFromMemory: true }
      : {
          groupRecovery: true,
          orderedEffects: true,
          replayOffsets: async () =>
            new Map(Array.from({ length: f.partitions }, (_, p) => [p, 0n])),
        }),
    decode: (value) => JSON.parse(value.toString()),
    handler: async ({ fact }, _current, commit) => {
      await options?.beforeHandle?.();
      projected.push(fact.n);
      if (fact.command && (await commit!())) effects.push(fact.n);
    },
  });
  await consumer.start();
  await consumer.waitUntilAssigned(1000);
  await vi.waitFor(() => expect(f.batch).toBeDefined());
  const deliver = (entries: Array<[number, boolean]>, partition = 0) =>
    f.batch!({
      batch: {
        topic: "log",
        partition,
        messages: entries.map(([n, command]) => ({
          offset: String(n),
          value: Buffer.from(JSON.stringify({ n, command })),
        })),
      },
      isStale: () => f.stale,
      resolveOffset() {},
    } as unknown as KafkaJS.EachBatchPayload);
  return { consumer, effects, projected, deliver };
}

it("waits for each durable prefix, isolates a failed prefix on retry and keeps effect grants ordered", async () => {
  const entered = Promise.withResolvers<void>(),
    gate = Promise.withResolvers<void>();
  const planned: number[][] = [];
  let failOnce = true;
  const t = await fixture({
    prepareReady: async (records) => {
      const positions = records.map((r) => Number(r.offset));
      planned.push(positions);
      if (failOnce) {
        failOnce = false;
        throw new Error("rolled back");
      }
      const barrier = records.findIndex((r) => r.fact.command);
      if (positions[0] === 4) {
        entered.resolve();
        await gate.promise;
      }
      return barrier < 0 ? records.length : barrier + 1;
    },
  });
  const input: Array<[number, boolean]> = [
    [3, false],
    [4, true],
    [5, false],
  ];
  try {
    await t.deliver(input);
    expect(t.projected).toEqual([]);
    const retry = t.deliver(input);
    await entered.promise;
    expect(t.projected).toEqual([3]);
    expect(f.commit).not.toHaveBeenCalled();
    gate.resolve();
    await retry;
    expect(planned).toEqual([[3, 4, 5], [3], [4, 5], [5]]);
    expect(t.projected).toEqual([3, 4, 5]);
    expect(t.effects).toEqual([4]);
    expect(f.commit.mock.calls.map(([p]) => p.offset)).toEqual(["5"]);
  } finally {
    gate.resolve();
    await t.consumer.close();
  }
});

it("resumes boot-local callback progress across assignment and reader recreation without commits", async () => {
  const test = await fixture({ memory: true });
  try {
    await test.deliver([
      [0, false],
      [1, false],
    ]);
    await f.reassign!();
    expect(f.assignments.at(-1)).toEqual([2]);
    f.logger!.error("LibrdKafkaError");
    await vi.waitFor(() => expect(f.consumers).toBe(2));
    await test.consumer.waitUntilAssigned(1000);
    expect(f.assignments.at(-1)).toEqual([2]);
    await test.deliver([[2, false]]);
    expect(f.commit).not.toHaveBeenCalled();
  } finally {
    await test.consumer.close();
  }
});

it("opens a partition after applying its captured recovery cut even as newer records arrive", async () => {
  f.high = "3";
  f.committed = "3";
  f.partitions = 2;
  const entered = Promise.withResolvers<void>(),
    release = Promise.withResolvers<void>();
  const t = await fixture({
    prepareReady: async (records) => {
      if (records[0]!.offset === 2n) {
        entered.resolve();
        await release.promise;
      }
      return records.length;
    },
  });
  try {
    f.high = "5";
    await t.deliver([
      [0, false],
      [1, false],
    ]);
    const tail = t.deliver([[2, false]]);
    await entered.promise;
    await expect(t.consumer.waitForPartition(0, 20)).rejects.toThrow("unavailable");
    release.resolve();
    await tail;
    await t.consumer.waitForPartition(0, 100);
    expect(t.projected).toEqual([0, 1, 2]);
    await expect(t.consumer.waitForPartition(1, 20)).rejects.toThrow("unavailable");
  } finally {
    release.resolve();
    await t.consumer.close();
  }
});

it("discards a PG snapshot invalidated during assignment and rejoins at the recovered offset", async () => {
  const entered = Promise.withResolvers<void>(),
    release = Promise.withResolvers<void>();
  let reads = 0;
  const reader = new KafkaLogConsumer({
    brokers: ["fake"],
    topic: "log",
    groupId: "group",
    clientId: "test",
    groupRecovery: true,
    orderedEffects: true,
    replayOffsets: async () => {
      if (++reads === 1) {
        entered.resolve();
        await release.promise;
        return new Map([[0, 2n]]);
      }
      return new Map([[0, 1n]]);
    },
    decode: () => 0,
    handler: async () => {},
  });
  try {
    await reader.start();
    await entered.promise;
    reader.reloadRecoveryState();
    expect(reader.ownsPartition(0)).toBe(false);
    release.resolve();
    await reader.waitUntilAssigned(2500);
    expect(f.assignments).toEqual([[1]]);
  } finally {
    release.resolve();
    await reader.close();
  }
});

it("starts a fresh callback consumer at the retained floor, without another boot's progress", async () => {
  const old = await fixture({ memory: true });
  await old.deliver([
    [0, false],
    [1, false],
  ]);
  await old.consumer.close();
  f.low = "1";
  const fresh = await fixture({ memory: true });
  try {
    expect(f.assignments.at(-1)).toEqual([1]);
  } finally {
    await fresh.consumer.close();
  }
});

it("clamps reclaimed callback progress to retention without claiming missing callbacks were delivered", async () => {
  const test = await fixture({ memory: true });
  try {
    await test.deliver([[0, false]]);
    f.low = "3";
    await f.reassign!();
    expect(f.assignments.at(-1)).toEqual([3]);
    expect(test.projected).toEqual([0]);
    await test.deliver([[3, false]]);
    expect(test.projected).toEqual([0, 3]);
  } finally {
    await test.consumer.close();
  }
});

it("retains an impossible local cursor through repeated assignments until metadata is consistent", async () => {
  const test = await fixture({ memory: true });
  try {
    await test.deliver([
      [0, false],
      [1, false],
      [2, false],
    ]);
    f.high = "2";
    for (let i = 0; i < 2; i++) {
      await f.reassign!();
      expect(() => test.consumer.checkHealth()).toThrow("unhealthy");
    }
    f.high = "5";
    await f.reassign!();
    expect(f.assignments.at(-1)).toEqual([3]);
    expect(() => test.consumer.checkHealth()).not.toThrow();
  } finally {
    await test.consumer.close();
  }
});

it("does not skip a reply which failed decoding when the partition is reassigned", async () => {
  const test = await fixture({ memory: true });
  try {
    await f.batch!({
      batch: {
        topic: "log",
        partition: 0,
        messages: [{ offset: "0", value: Buffer.from("invalid JSON") }],
      },
      isStale: () => false,
      resolveOffset() {},
    } as unknown as KafkaJS.EachBatchPayload);
    await f.reassign!();
    expect(f.assignments.at(-1)).toEqual([0]);
    expect(test.projected).toEqual([]);
  } finally {
    await test.consumer.close();
  }
});

it("joins an old in-flight handler before volatile reassignment and does not skip its stale delivery", async () => {
  const entered = Promise.withResolvers<void>(),
    gate = Promise.withResolvers<void>();
  const test = await fixture({
    memory: true,
    beforeHandle: async () => {
      entered.resolve();
      await gate.promise;
    },
  });
  try {
    const handling = test.deliver([[0, false]]);
    await entered.promise;
    let assigned = false;
    const assignment = f.reassign!().then(() => {
      assigned = true;
    });
    await Promise.resolve();
    expect(assigned).toBe(false);
    gate.resolve();
    await Promise.all([handling, assignment]);
    expect(f.assignments.at(-1)).toEqual([0]);
  } finally {
    gate.resolve();
    await test.consumer.close();
  }
});

it.each([
  { commitMessages: true },
  { groupRecovery: true },
  { groupRecovery: true, orderedEffects: true },
  { replayOffsets: async () => new Map<number, bigint>() },
])("refuses volatile progress with incompatible durability options: %j", (extra) => {
  expect(
    () =>
      new KafkaLogConsumer({
        brokers: ["fake"],
        topic: "reply",
        clientId: "test",
        groupId: "reply",
        commitMessages: false,
        resumeFromMemory: true,
        decode: (value) => JSON.parse(value.toString()),
        handler: async () => {},
        ...extra,
      }),
  ).toThrow("Memory resume");
});

it("replays an unfinished display prefix without executing commands below the committed dispatch boundary", async () => {
  const test = await fixture();
  try {
    await test.deliver([
      [0, false],
      [1, true],
      [2, false],
      [3, true],
      [4, false],
    ]);
    expect(test.projected).toEqual([0, 1, 2, 3, 4]);
    expect(test.effects).toEqual([3]);
    expect(f.commit.mock.calls.map(([offsets]) => offsets.offset)).toEqual(["4"]);
  } finally {
    await test.consumer.close();
  }
});
it("does not move commit backwards while rebuilding memory", async () => {
  const test = await fixture();
  try {
    await test.deliver([
      [0, false],
      [1, true],
    ]);
    expect(f.commit).not.toHaveBeenCalled();
    expect(test.effects).toEqual([]);
  } finally {
    await test.consumer.close();
  }
});

it("does not add a synchronous commit for every text fragment", async () => {
  vi.useFakeTimers();
  const starting = fixture();
  await vi.advanceTimersByTimeAsync(100);
  const test = await starting;
  try {
    for (let n = 3; n < 103; n++) await test.deliver([[n, false]]);
    expect(f.commit).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1000);
    expect(f.commit).toHaveBeenCalledOnce();
    expect(f.commit.mock.calls[0]![0].offset).toBe("103");
    await test.deliver([[103, true]]);
    expect(f.commit.mock.calls.at(-1)![0].offset).toBe("104");
    expect(test.effects).toEqual([103]);
  } finally {
    await test.consumer.close();
    vi.useRealTimers();
  }
});

it("keeps periodic progress moving for another partition over multiple blocked ticks", async () => {
  f.partitions = 2;
  const test = await fixture(),
    release = Promise.withResolvers<void>();
  try {
    await test.deliver([[3, false]], 0);
    await test.deliver([[3, false]], 1);
    f.commit
      .mockClear()
      .mockImplementation((position) =>
        position.partition === 1 ? release.promise : Promise.resolve(),
      );
    await test.deliver([[4, false]], 0);
    await test.deliver([[4, false]], 1);
    await vi.waitFor(
      () =>
        expect(f.commit.mock.calls.map(([p]) => [p.partition, p.offset])).toEqual([
          [0, "5"],
          [1, "5"],
        ]),
      { timeout: 2500, interval: 5 },
    );
    await test.deliver([[5, false]], 0);
    await vi.waitFor(
      () =>
        expect(f.commit.mock.calls.map(([p]) => [p.partition, p.offset])).toEqual([
          [0, "5"],
          [1, "5"],
          [0, "6"],
        ]),
      { timeout: 2500, interval: 5 },
    );
  } finally {
    release.resolve();
    await test.consumer.close();
  }
});
it("cannot dispatch before commit acknowledgement", async () => {
  const test = await fixture();
  let confirm!: () => void;
  f.commit.mockImplementationOnce(
    () =>
      new Promise<void>((resolve) => {
        confirm = resolve;
      }),
  );
  try {
    const pending = test.deliver([[3, true]]);
    await vi.waitFor(() => expect(f.commit).toHaveBeenCalledOnce());
    expect(test.effects).toEqual([]);
    confirm();
    await pending;
    expect(test.effects).toEqual([3]);
  } finally {
    await test.consumer.close();
  }
});
it("a lost commit acknowledgement cannot start an effect", async () => {
  const test = await fixture();
  f.commit.mockRejectedValueOnce(new Error("commit acknowledgement lost"));
  try {
    await test.deliver([[3, true]]);
    expect(test.effects).toEqual([]);
  } finally {
    await test.consumer.close();
  }
});
it("rebalance invalidates a continuation that just obtained commit acknowledgement", async () => {
  const test = await fixture();
  f.commit.mockImplementationOnce(async () => {
    f.stale = true;
  });
  try {
    await test.deliver([[3, true]]);
    expect(test.effects).toEqual([]);
  } finally {
    await test.consumer.close();
  }
});

it("surfaces a background client error and reconstructs the consumer instead of silently hanging", async () => {
  const test = await fixture();
  try {
    f.logger!.error("Consumer encountered error while consuming: Unknown topic");
    expect(() => test.consumer.checkHealth()).toThrow("unhealthy");
    await vi.waitFor(() => expect(f.consumers).toBe(2), { timeout: 2000 });
    await test.consumer.waitUntilAssigned(1000);
    await test.deliver([[3, true]]);
    expect(test.effects).toEqual([3]);
  } finally {
    await test.consumer.close();
  }
});
