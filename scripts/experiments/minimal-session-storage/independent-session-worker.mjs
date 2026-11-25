// Process entry point used by the isolated Worker acceptance. Claims use no
// parent ID or target hint. SIGKILL stops direct renewal without releasing PG.
import pg from "pg";
import { randomUUID } from "node:crypto";
import { KafkaWorkerProducer } from "@pi-cloud/event-log";
import { encodeWorkspaceFact } from "./workspace-operations.mjs";
import { openSegmentNative } from "./independent-session-runtime.mjs";
import { ForkStorage } from "./fork-storage.mjs";
import { SegmentWorker } from "./independent-session-leases.mjs";
import { MemoryStorage, Harness, createRegistry } from "@earendil-works/pi-durable";
import { createModels } from "@earendil-works/pi-ai/models";
import { fauxProvider, fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import { once } from "node:events";

const [cfg] = await once(process, "message");
const storage = new ForkStorage(new pg.Pool({ connectionString: cfg.url, max: 4 }));
const worker = new SegmentWorker({ database: storage.db, ...cfg });
let job, native, root, history;
const producer = cfg.brokers
  ? new KafkaWorkerProducer({ brokers: cfg.brokers, topic: cfg.topic, clientId: cfg.bootId })
  : undefined;
await producer?.start();
async function publish(fact) {
  await producer.append(0, {
    factId: fact.factId,
    sessionId: fact.scope.piSessionId,
    value: encodeWorkspaceFact(fact),
  });
}
process.on("message", async (msg) => {
  try {
    if (msg.op === "claim") {
      job = await worker.claimNext();
      process.send({ kind: "claim", scope: job?.scope ?? null, flow: job?.flow_id ?? null });
    } else if (msg.op === "execute") {
      const scope = job.scope;
      let sequence = 0;
      const opened = await openSegmentNative(storage, job, cfg.topic, async (commit) => {
        sequence = commit.seq;
        await publish({
          kind: "durable_commit",
          scope,
          commit,
          factId: randomUUID(),
          occurredAt: new Date().toISOString(),
          origins: [],
          display: [],
          tools: [],
          cancellations: [],
        });
      });
      const models = createModels(),
        fake = fauxProvider({ provider: "independent-child", tokensPerSecond: 0 });
      models.setProvider(fake.provider);
      fake.setResponses([
        async (context) => {
          history = context.messages.map((m) =>
            typeof m.content === "string" ? m.content : m.content.map((v) => v.text ?? "").join(""),
          );
          if (msg.hold) {
            process.send({ kind: "in-model", sequence, history });
            await new Promise(() => {});
          }
          return fauxAssistantMessage("independent answer: " + job.initial_input);
        },
      ]);
      native = await Harness.open(opened.storage, { models, registry: createRegistry() }, ctx);
      const model = fake.getModel(),
        agent = { model: { provider: model.provider, modelId: model.id } };
      root = await native.root(ctx, { agent });
      if (opened.resumed) native.resume();
      else {
        await root.configure(agent, ctx);
        await root.submit({ type: "input", content: job.initial_input, requestId: job.id }, ctx);
      }
      await root.waitForIdle(ctx);
      job.authority.assertActive();
      await publish({
        kind: "work_finished",
        scope,
        factId: randomUUID(),
        agentId: "root",
        baseSequence: 0,
        nativeSequence: sequence,
        terminal: { type: "turn.completed", payload: { stopReason: "stop" } },
        occurredAt: new Date().toISOString(),
      });
      process.send({ kind: "finished", history, resumed: opened.resumed });
    } else if (msg.op === "answer") {
      job.authority.assertActive();
      const memory = new MemoryStorage();
      await storage.hydrate(job.tenant_id, job.logical_id, memory);
      const models = createModels(),
        fake = fauxProvider({ tokensPerSecond: 0 });
      models.setProvider(fake.provider);
      fake.setResponses([
        (context) => {
          history = context.messages.map((m) =>
            typeof m.content === "string" ? m.content : m.content.map((c) => c.text ?? "").join(""),
          );
          return fauxAssistantMessage("checked: " + job.initial_input);
        },
      ]);
      native = await Harness.open(memory, { models, registry: createRegistry() }, ctx);
      const model = fake.getModel(),
        agent = { model: { provider: model.provider, modelId: model.id } };
      root = await native.root(ctx, { agent });
      await root.configure(agent, ctx);
      const input = await root.submit(
        { type: "input", content: job.initial_input, requestId: job.id },
        ctx,
      );
      await root.waitForIdle(ctx);
      await input.wait(ctx);
      const final = (await root.context(ctx)).messages.filter((m) => m.role === "assistant").at(-1);
      job.authority.assertActive();
      process.send({
        kind: "answer",
        history,
        text: final.content.map((p) => p.text ?? "").join(""),
      });
    } else if (msg.op === "release") {
      await native?.close(ctx);
      native = undefined;
      await job?.release();
      job = undefined;
      process.send({ kind: "released" });
    } else if (msg.op === "exit") {
      await native?.close(ctx);
      await worker.close();
      await producer?.close();
      await storage.db.destroy();
      process.disconnect();
    }
  } catch (e) {
    process.send({ kind: "error", message: e.stack });
  }
});
process.send({ kind: "ready" });
