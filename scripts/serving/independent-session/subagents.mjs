import { randomUUID } from "node:crypto";
import { KafkaToolReplyPublisher } from "@pi-cloud/event-log";
import { serveSegmentDelegations } from "../../experiments/minimal-session-storage/segment-delegation-service.mjs";
import { ForkStorage } from "../../experiments/minimal-session-storage/fork-storage.mjs";
import { SegmentInputs } from "../../experiments/minimal-session-storage/segment-inputs.mjs";
import { installation, openStore, integer, secret } from "./config.mjs";

export async function startIndependentDelegations() {
  const config = await installation();
  const token = await secret("PI_CLOUD_SUPERVISOR_MANAGEMENT_TOKEN");
  const store = new ForkStorage(await openStore(config));
  const inputs = (store.inputs = new SegmentInputs(store.db));
  const replies = new KafkaToolReplyPublisher(config.brokers, "pma-subagent-" + randomUUID());
  let service;
  async function close() {
    try {
      await service?.close();
    } finally {
      await Promise.all([replies.close(), store.db.destroy()]);
    }
  }
  try {
    await replies.start();
    service = await serveSegmentDelegations(
      { store, inputs, topic: config.topic },
      {
        connectionString: config.write,
        token,
        publish: (destination, reply) => replies.publish(destination, reply),
        host: process.env.HOST ?? "127.0.0.1",
        port: integer("PORT", 4320),
      },
    );
    return { close };
  } catch (error) {
    await close();
    throw error;
  }
}
