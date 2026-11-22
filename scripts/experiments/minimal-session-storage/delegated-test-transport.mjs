// Isolated RF3 conformance transport. Tests wait for the exact projected fact
// to observe its state deterministically; this is not a throughput benchmark.
import native from "@confluentinc/kafka-javascript";
import { KafkaLogConsumer } from "../../../packages/event-log/src/kafka-log-consumer.ts";
import { KafkaWorkerProducer } from "../../../packages/event-log/src/kafka-worker-producer.ts";
import { decodeWorkspaceFact, encodeWorkspaceFact } from "./workspace-operations.mjs";

export async function openDelegatedTransport(brokers, topic, apply) {
  const kafka = new native.KafkaJS.Kafka({
    kafkaJS: { brokers, logLevel: native.KafkaJS.logLevel.NOTHING },
  });
  const admin = kafka.admin();
  await admin.connect();
  const waiters = new Map(),
    producers = new Map();
  const reader = new KafkaLogConsumer({
    brokers,
    topic,
    clientId: topic,
    groupId: topic,
    commitMessages: false,
    replayOffsets: async (bounds) => new Map(bounds.map((b) => [b.partition, b.low])),
    decode: (v) => {
      try {
        return decodeWorkspaceFact(v).fact;
      } catch (error) {
        for (const p of waiters.values()) p.reject(error);
        throw error;
      }
    },
    handler: async (record, current) => {
      const receipt = waiters.get(record.fact.factId);
      try {
        await apply(record, current);
        receipt?.resolve();
      } catch (error) {
        receipt?.reject(error);
        throw error;
      }
    },
  });
  async function close() {
    try {
      await reader.close();
      await Promise.all([...producers.values()].map((p) => p.close()));
    } finally {
      try {
        await admin.deleteTopics({ topics: [topic] });
      } finally {
        await admin.disconnect();
      }
    }
  }
  try {
    await admin.createTopics({
      topics: [
        {
          topic,
          numPartitions: 1,
          replicationFactor: 3,
          configEntries: [{ name: "min.insync.replicas", value: "2" }],
        },
      ],
    });
    await reader.start();
    await reader.waitUntilAssigned(20000);
    return {
      close,
      async publish(fact) {
        let producer = producers.get(fact.scope.writerId);
        if (!producer) {
          producer = new KafkaWorkerProducer({
            brokers,
            topic,
            clientId: topic + fact.scope.writerId,
          });
          producers.set(fact.scope.writerId, producer);
          await producer.start();
        }
        const receipt = Promise.withResolvers();
        const signal = AbortSignal.timeout(12000),
          timeout = () => receipt.reject(new Error("Projection acknowledgement timed out"));
        signal.addEventListener("abort", timeout, { once: true });
        waiters.set(fact.factId, receipt);
        try {
          await Promise.all([
            producer.append(0, {
              sessionId: fact.scope.piSessionId,
              factId: fact.factId,
              value: encodeWorkspaceFact(fact),
            }),
            receipt.promise,
          ]);
        } finally {
          signal.removeEventListener("abort", timeout);
          waiters.delete(fact.factId);
        }
      },
    };
  } catch (error) {
    await close();
    throw error;
  }
}
