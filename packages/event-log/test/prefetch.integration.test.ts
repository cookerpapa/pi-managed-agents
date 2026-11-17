import { randomUUID } from "node:crypto";
import native from "@confluentinc/kafka-javascript";
import { expect, it, vi } from "vitest";
import { KafkaLogConsumer } from "../src/kafka-log-consumer.ts";

const brokers = process.env.PI_CLOUD_KAFKA_INTEGRATION_BROKERS?.split(",");
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
