import { durableControlIntent } from "@pi-cloud/protocol";
import kafkaNative from "@confluentinc/kafka-javascript";
import { KafkaLogConsumer } from "../../../packages/event-log/src/kafka-log-consumer.ts";
import { KafkaWorkerProducer } from "../../../packages/event-log/src/kafka-worker-producer.ts";
import { encodeWorkspaceFact, decodeWorkspaceFact } from "./workspace-operations.mjs";

export async function readRetainedPrefix(brokers, topic, native, plan, consumer) {
  const admin = new kafkaNative.KafkaJS.Kafka({
    kafkaJS: { brokers, logLevel: kafkaNative.KafkaJS.logLevel.NOTHING },
  }).admin();
  const producer = new KafkaWorkerProducer({ brokers, topic, clientId: topic + "-writer" });
  const found = [],
    end = Promise.withResolvers();
  let start;
  const reader = new KafkaLogConsumer({
    brokers,
    topic,
    clientId: topic + "-restart",
    groupId: topic + "-restart",
    commitMessages: false,
    replayOffsets: async (bounds) => {
      const { starts } = await plan(bounds);
      start = Number(starts.get(0));
      return starts;
    },
    decode: (v) => decodeWorkspaceFact(v).fact,
    handler: async (r, current) => {
      await consumer?.observe(r, current, reader);
      for (const ref of r.fact.tools)
        found.push({
          offset: Number(r.offset),
          op: ref.operationId,
          reply: ref.replyTopic,
          args: durableControlIntent(r.fact.commit, ref).arguments,
        });
      if (r.offset === BigInt(native.length - 1)) end.resolve();
    },
  });
  await admin.connect();
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
    await producer.start();
    for (const f of native)
      await producer.append(0, {
        sessionId: f.scope.piSessionId,
        factId: f.factId,
        value: encodeWorkspaceFact(f),
      });
    await reader.start();
    await reader.waitUntilAssigned(15000);
    const signal = AbortSignal.timeout(15000),
      timeout = () => end.reject(Error("Retained prefix did not drain"));
    signal.addEventListener("abort", timeout, { once: true });
    try {
      await end.promise;
      await reader.waitForPartition(0, 15000);
      await consumer?.drained();
    } finally {
      signal.removeEventListener("abort", timeout);
    }
    return { start, found };
  } finally {
    try {
      await reader.close();
      await producer.close();
      await admin.deleteTopics({ topics: [topic] });
    } finally {
      await admin.disconnect();
    }
  }
}
