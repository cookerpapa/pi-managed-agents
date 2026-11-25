import Fastify from "fastify";
import { readFile } from "node:fs/promises";
import { createDatabase } from "@pi-cloud/database";
import { ACCEPTED_FACT_TOPIC, KafkaToolReplyPublisher } from "@pi-cloud/event-log";
import { sql } from "kysely";
import { NamedSubagentExecutor } from "./named-subagent-executor.ts";
import { installNamedSubagentEndpoint } from "./named-subagent-endpoint.ts";
import { registerSubagent } from "./named-subagent-store.ts";

async function setting(key: string) {
  const file = process.env[key + "_FILE"];
  const value = file ? (await readFile(file, "utf8")).trim() : process.env[key];
  if (!value) throw Error("Missing " + key);
  return value;
}
async function main() {
  const url = await setting("DATABASE_URL"),
    token = await setting("PI_CLOUD_SUPERVISOR_MANAGEMENT_TOKEN");
  if (token.length < 32) throw Error("Invalid management credential");
  const topic = process.env.PI_CLOUD_SESSION_LOG_TOPIC ?? ACCEPTED_FACT_TOPIC;
  const notifications =
    process.env.DATABASE_NOTIFICATION_URL_FILE || process.env.DATABASE_NOTIFICATION_URL
      ? await setting("DATABASE_NOTIFICATION_URL")
      : url;
  const db = createDatabase({ connectionString: url, maxConnections: 12 });
  const publisher = new KafkaToolReplyPublisher(
    (await setting("PI_CLOUD_KAFKA_BROKERS")).split(","),
    "named-subagent-executor",
  );
  const service = new NamedSubagentExecutor({
    db,
    connectionString: notifications,
    token,
    publish: (t, r) => publisher.publish(t, r),
  });
  const api = Fastify({ logger: false, bodyLimit: 1024 * 1024 });
  let ready = false;
  api.get("/health/ready", (_req, res) => res.code(ready ? 200 : 503).send({ ready }));
  installNamedSubagentEndpoint(
    api,
    token,
    () => ready,
    async (offer) => {
      await registerSubagent(db, topic, offer);
      service.wake();
    },
  );
  const stop = Promise.withResolvers<void>();
  process.once("SIGTERM", stop.resolve);
  process.once("SIGINT", stop.resolve);
  try {
    await sql`select d.operation_id,r.worker_id,r.fencing_token,r.agent_name,h.management_base_url from subagent_deliveries d
       join runs r on r.id=d.root_id join workers w on w.id=r.worker_id join supervisor_hosts h on h.supervisor_id=w.supervisor_id limit 0`.execute(
      db,
    );
    await publisher.start();
    service.start();
    ready = true;
    const address = await api.listen({
      host: process.env.HOST ?? "127.0.0.1",
      port: Number(process.env.PORT ?? 4313),
    });
    process.send?.({ ready: address });
    await stop.promise;
  } finally {
    ready = false;
    try {
      await api.close();
      await service.close();
    } finally {
      try {
        await publisher.close();
      } finally {
        await db.destroy();
      }
    }
  }
}
main().catch(() => {
  console.error("subagent.executor.start_failed");
  process.exitCode = 1;
});
