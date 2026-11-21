import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import pg from "pg";
import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import {
  Harness,
  MemoryStorage,
  createRegistry,
  defineExtension,
} from "@earendil-works/pi-durable";
import { createModels } from "@earendil-works/pi-ai/models";
import {
  fauxProvider,
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { KafkaWorkerProducer } from "../../../packages/event-log/src/kafka-worker-producer.ts";
import { KafkaToolReplyMailbox } from "../../../packages/event-log/src/tool-replies.ts";
import { encodeWorkspaceFact } from "./workspace-operations.mjs";
import { FamilyPresentation } from "./family-presentation.mjs";
import { loadPriorRoot } from "./retained-family-read.mjs";
import { claimRun } from "./serving-family-claim.mjs";
import { continuationBash } from "./continuation-bash.mjs";

const cfg = await new Promise((res) => process.once("message", res));
const claim = await claimRun(cfg.dbUrl, cfg.runId),
  scope = claim.scope,
  presentation = new FamilyPresentation();
process.send({ bootId: claim.bootId });
const db = new pg.Client({
  connectionString: cfg.dbUrl,
  options: "-c default_transaction_read_only=on",
});
const sender = new KafkaWorkerProducer({
  topic: cfg.topic,
  brokers: cfg.brokers,
  clientId: "root-" + claim.bootId,
});
const mailbox = new KafkaToolReplyMailbox({
  brokers: cfg.replyBrokers,
  bootId: claim.bootId,
  replicas: 3,
});
const bash = continuationBash(
  mailbox,
  scope.runId,
  undefined,
  "echo once >> executions; while [ ! -f release ]; do sleep 0.1; done; cat result",
);
async function publish(f) {
  await sender.append(0, {
    sessionId: scope.piSessionId,
    factId: f.factId,
    value: encodeWorkspaceFact(f),
  });
}
class Published extends MemoryStorage {
  async commit(writes) {
    const p = this.prepareCommit(writes),
      commit = { seq: p.seq, nextId: this.nextId, writes: p.writes },
      display = presentation.prepare(commit);
    await publish({
      kind: "durable_commit",
      scope,
      commit,
      factId: randomUUID(),
      occurredAt: new Date().toISOString(),
      workspaceOperations: await bash.intents(p.writes),
      tools: [],
      cancellations: [],
      display: display.display,
      origins: p.writes
        .filter((w) => ["task", "entry", "submission"].includes(w.type))
        .map((w) => ({
          recordId: w.value.id,
          turnId: scope.turnId,
          runId: scope.runId,
          sessionId: scope.sessionId,
        })),
    });
    const seq = p.apply();
    display.confirmed();
    return seq;
  }
}
const store = new Published(),
  models = createModels(),
  fake = fauxProvider({ tokensPerSecond: 0 }),
  registry = createRegistry();
models.setProvider(fake.provider);
registry.install(defineExtension({ name: "workspace-bash", tools: [bash.tool] }));
let harness, restoredUserPrompts;
const extract = (m) =>
  typeof m.content === "string"
    ? m.content
    : m.content
        .filter((b) => b.type === "text")
        .map((b) => b.text)
        .join("");
try {
  await db.connect();
  await sender.start();
  await mailbox.start();
  const prior = await loadPriorRoot(db, scope, store);
  const base = claim.request.nextEventSeq - 1;
  presentation.bind(1, scope);
  presentation.sequences.set(scope.sessionId, base);
  if (prior.baseSequence === 0) {
    fake.setResponses([
      fauxAssistantMessage(fauxToolCall("bash", { command: bash.command }), {
        stopReason: "toolUse",
      }),
      (context) => {
        const m = context.messages.at(-1);
        assert.equal(m.role, "toolResult");
        return fauxAssistantMessage("First answer: " + extract(m));
      },
    ]);
  } else {
    fake.setResponses([
      (context) => {
        const calls = context.messages.filter(
          (m) => m.role === "assistant" && m.content.some((b) => b.type === "toolCall"),
        );
        const results = context.messages.filter((m) => m.role === "toolResult");
        assert.equal(calls.length, 1);
        assert.equal(results.length, 1);
        const proposals = calls[0].content.filter((b) => b.type === "toolCall");
        assert.equal(proposals.length, 1);
        assert.equal(proposals[0].id, results[0].toolCallId);
        assert.equal(proposals[0].name, results[0].toolName);
        const value = extract(results[0]);
        assert(
          context.messages.some(
            (m) => m.role === "assistant" && extract(m) === "First answer: " + value,
          ),
        );
        restoredUserPrompts = context.messages.filter((m) => m.role === "user").map(extract);
        assert.equal(restoredUserPrompts.length, 2);
        assert.equal(restoredUserPrompts[1], claim.request.input.prompt);
        return fauxAssistantMessage("Second answer: " + value);
      },
    ]);
  }
  harness = await Harness.open(
    store,
    { models, registry, settings: { retry: { maxRetries: 0 }, compaction: { enabled: false } } },
    ctx,
  );
  const model = fake.getModel(),
    agent = { model: { provider: model.provider, modelId: model.id } },
    root = await harness.root(ctx, { agent });
  if (prior.baseSequence) await root.configure(agent, ctx);
  const sub = await root.submit(
    { type: "input", requestId: scope.runId, content: claim.request.input.prompt },
    ctx,
  );
  await root.waitForIdle(ctx);
  assert.equal((await sub.wait(ctx)).status, "done");
  await harness.close(ctx);
  harness = undefined;
  await claim.finish(() =>
    publish({
      kind: "work_finished",
      scope,
      factId: randomUUID(),
      occurredAt: new Date().toISOString(),
      agentId: "root",
      baseSequence: base,
      nativeSequence: store.nextSeq - 1,
      terminal: { type: "turn.completed", payload: { stopReason: "stop" } },
    }),
  );
  process.send({
    done: true,
    restoredUserPrompts,
    priorEntries: prior.priorPgEntries,
    modelCalls: fake.state.callCount,
  });
} finally {
  try {
    await harness?.close(ctx);
    bash.close();
    await mailbox.shutdown();
    await sender.close();
    await db.end();
  } finally {
    await claim.close();
    process.disconnect();
  }
}
