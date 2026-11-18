import { randomUUID } from "node:crypto";
import native from "@confluentinc/kafka-javascript";
import { expect, it, vi } from "vitest";
import { KafkaLogConsumer } from "../src/kafka-log-consumer.ts";
import { KafkaPartitionReader } from "../src/kafka-partition-reader.ts";

const brokers = process.env.PI_CLOUD_KAFKA_INTEGRATION_BROKERS?.split(",");
it.skipIf(!brokers)(
  "reaches an idle transaction-marker tail after delayed assignment without a new message",
  async () => {
    const topic = `idle-eof-${randomUUID()}`;
    const kafka = new native.KafkaJS.Kafka({
      kafkaJS: { brokers: brokers!, logLevel: native.KafkaJS.logLevel.NOTHING },
    });
    const admin = kafka.admin();
    const producer = kafka.producer({
      kafkaJS: { acks: -1, idempotent: true, transactionalId: topic },
    });
    let reader: KafkaPartitionReader | undefined;
    try {
      await admin.connect();
      await admin.createTopics({
        topics: [
          {
            topic,
            numPartitions: 4,
            replicationFactor: 3,
            configEntries: [{ name: "min.insync.replicas", value: "2" }],
          },
        ],
      });
      await producer.connect();
      const tx = await producer.transaction();
      await tx.send({
        topic,
        messages: Array.from({ length: 4 }, (_, partition) => ({ partition, value: "seed" })),
      });
      await tx.commit();
      let ends = new Map<number, bigint>();
      // The transaction coordinator's acknowledgement can precede visibility
      // of every partition's replicated marker to a ListOffsets request.
      await vi.waitFor(
        async () => {
          ends = new Map(
            (await admin.fetchTopicOffsets(topic, { isolationLevel: 1 })).map((b) => [
              b.partition,
              BigInt(b.offset),
            ]),
          );
          expect(ends.size).toBe(4);
          expect([...ends.values()].filter((n) => n < 2n)).toEqual([]);
        },
        { timeout: 10000 },
      );
      const positions = new Map<number, bigint>();
      const errors: unknown[] = [];
      reader = new KafkaPartitionReader({
        brokers: brokers!,
        clientId: topic,
        groupId: topic,
        topic,
        logger: {
          error: (error: unknown) => errors.push(error),
        } as unknown as native.KafkaJS.Logger,
        rebalance: async (error, assignments, functions) => {
          if (error.code !== native.CODES.ERRORS.ERR__ASSIGN_PARTITIONS) {
            functions.unassign(assignments);
            return;
          }
          // Simulate asynchronous authority/bootstrap reads while polls continue.
          await new Promise((resolve) => setTimeout(resolve, 40));
          functions.assign(
            assignments.map((a) => ({ ...a, offset: Number(ends.get(a.partition)! - 1n) })),
          );
        },
        onPosition: (partition, offset) => positions.set(partition, offset),
      });
      await reader.connect();
      await reader.subscribe({ topics: [topic] });
      await reader.run({
        eachBatch: async () => {
          throw new Error("Only transaction markers remain");
        },
      });
      await vi.waitFor(
        () => {
          for (const [partition, end] of ends) expect(positions.get(partition)).toBe(end);
        },
        { timeout: 10000 },
      );
      expect(errors).toEqual([]);
      expect(reader.statistics().records).toBe(0);
    } finally {
      await reader?.disconnect();
      await producer.disconnect();
      await admin.deleteTopics({ topics: [topic] });
      await admin.disconnect();
    }
  },
  60000,
);

it.skipIf(!brokers)(
  "keeps buffered EOF behind work and retries one partition without replaying confirmed effects",
  async () => {
    const topic = `buffered-reader-${randomUUID()}`;
    const kafka = new native.KafkaJS.Kafka({
      kafkaJS: { brokers: brokers!, logLevel: native.KafkaJS.logLevel.NOTHING },
    });
    const admin = kafka.admin(),
      producer = kafka.producer({ kafkaJS: { acks: -1 } });
    const aborter = kafka.producer({
      kafkaJS: { acks: -1, idempotent: true, transactionalId: topic + "-abort" },
    });
    const release = Promise.withResolvers<void>(),
      entered = Promise.withResolvers<void>();
    const seen: string[] = [],
      attempts = new Map<string, number>();
    const reader = new KafkaLogConsumer<{ id: string }>({
      brokers: brokers!,
      topic,
      groupId: topic,
      clientId: topic,
      groupRecovery: true,
      orderedEffects: true,
      decode: (value) => JSON.parse(value.toString()),
      replayOffsets: async (bounds) => new Map(bounds.map((b) => [b.partition, b.low])),
      handler: async (record, current, commit) => {
        const id = record.fact.id;
        attempts.set(id, (attempts.get(id) ?? 0) + 1);
        if (id === "a0") {
          entered.resolve();
          await release.promise;
        }
        if (id === "a1" && attempts.get(id) === 1) throw new Error("owned retry before effect");
        if (current?.() && (await commit!())) seen.push(id);
      },
    });
    try {
      await admin.connect();
      await admin.createTopics({
        topics: [
          {
            topic,
            numPartitions: 2,
            replicationFactor: 3,
            configEntries: [{ name: "min.insync.replicas", value: "2" }],
          },
        ],
      });
      await producer.connect();
      await aborter.connect();
      await reader.start();
      await reader.waitUntilAssigned(30000);
      await producer.send({
        topic,
        messages: [
          ...Array.from({ length: 40 }, (_, i) => ({
            partition: 0,
            key: "a",
            value: JSON.stringify({ id: `a${i}` }),
          })),
          { partition: 1, key: "b", value: '{"id":"healthy"}' },
        ],
      });
      await entered.promise;
      await vi.waitFor(() => expect(seen).toContain("healthy"), { timeout: 10000 });
      await vi.waitFor(() => expect(reader.statistics().queuedRecords).toBeGreaterThan(10), {
        timeout: 10000,
      });
      const tx = await aborter.transaction();
      await tx.send({ topic, messages: [{ partition: 0, value: '{"id":"aborted"}' }] });
      await tx.abort();
      const ends = await reader.captureEndOffsets();
      let caughtUp = false;
      const catchup = reader.waitUntilInitialReplay(ends, 15000).then(() => {
        caughtUp = true;
      });
      await new Promise((r) => setTimeout(r, 50));
      expect(caughtUp).toBe(false);
      release.resolve();
      await catchup;
      expect(seen.filter((id) => id.startsWith("a"))).toEqual(
        Array.from({ length: 40 }, (_, i) => `a${i}`),
      );
      expect(attempts.get("a1")).toBe(2);
      expect(seen).not.toContain("aborted");
      expect(reader.statistics().records).toBe(0);
      reader.checkHealth();
    } finally {
      release.resolve();
      await reader.close();
      await aborter.disconnect();
      await producer.disconnect();
      await admin.deleteTopics({ topics: [topic] });
      await admin.disconnect();
    }
  },
  90000,
);
