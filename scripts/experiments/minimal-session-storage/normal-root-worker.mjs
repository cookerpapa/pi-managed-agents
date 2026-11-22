import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import pg from "pg";
import { createDatabase } from "@pi-cloud/database";
import { readSealedFamily, hydrateSealedFamily } from "@pi-cloud/pi-session-postgres/durable";
import { NATIVE_HUMAN_INPUT_DOCUMENT, createExecutionReference } from "@pi-cloud/protocol";
import { DurableHumanInputs } from "../../../packages/sandbox-supervisor/src/durable/human-inputs.ts";
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
const claim = await claimRun(cfg.dbUrl, cfg.runId, Boolean(cfg.inputMode)),
  scope = claim.scope,
  presentation = new FamilyPresentation();
process.send({ bootId: claim.bootId, workerId: claim.workerId, writerId: scope.writerId });
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
  cfg.inputMode ? 60000 : 10000,
);
async function publish(f) {
  await sender.append(0, {
    sessionId: scope.piSessionId,
    factId: f.factId,
    value: encodeWorkspaceFact(f),
  });
}
const inputDocuments = new Set();
class Published extends MemoryStorage {
  async commit(writes) {
    const p = this.prepareCommit(writes),
      commit = { seq: p.seq, nextId: this.nextId, writes: p.writes },
      display = presentation.prepare(commit);
    let inputChanged = false;
    if (cfg.inputMode)
      for (const w of p.writes) {
        if (
          w.type === "document.create" &&
          w.record.scope.conversationId === 1 &&
          [NATIVE_HUMAN_INPUT_DOCUMENT, "pi.inbox"].includes(w.record.kind)
        ) {
          inputDocuments.add(w.record.id);
          inputChanged = true;
        } else if (
          (w.type === "document.change" || w.type === "document.retire") &&
          inputDocuments.has(w.id)
        ) {
          inputChanged = true;
          if (w.type === "document.retire") inputDocuments.delete(w.id);
        }
      }
    await publish({
      ...(inputChanged ? { inputQueueSessions: [scope.sessionId] } : {}),
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
let harness,
  restoredUserPrompts,
  acceptingInputs = true,
  stopWork;
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
  let prior, queuedInput;
  if (cfg.resumeInput) {
    const cold = createDatabase({ connectionString: cfg.dbUrl });
    try {
      const family = await readSealedFamily(cold, scope, cfg.topic);
      await hydrateSealedFamily(family, store);
      const native = family.handoff.state;
      for (const { write } of native.records) {
        if (write.record?.kind === NATIVE_HUMAN_INPUT_DOCUMENT) inputDocuments.add(write.record.id);
        if (write.record?.kind === "pi.live")
          presentation.live[write.record.id] = {
            conversation: write.record.scope.conversationId,
            value: write.content.value,
          };
      }
      presentation.sequences = new Map(native.sequences);
      const [q] = family.queues;
      inputDocuments.add(Number(q.doc_id));
      assert.equal(q.items.length, cfg.stopInputs ? 2 : 1);
      if (cfg.stopInputs) {
        assert(claim.request.stopOnResume);
        assert.equal(q.items[1].mode, "steer");
      }
      assert.equal(q.items[0].mode, cfg.inputMode === "steer" ? "steer" : "followUp");
      queuedInput = await store.submission(q.items[0].id, ctx);
      assert.equal(queuedInput.status, "queued");
      assert.equal(
        (await store.submissionByRequest(1, queuedInput.requestId, ctx)).id,
        queuedInput.id,
      );
      assert.equal(family.intents.length, 1);
      await bash.restore(family.intents[0]);
      prior = { priorPgEntries: family.entries.length, baseSequence: native.seq };
      process.send({
        restoredInput: queuedInput.requestId,
        originalTopic: family.intents[0].reply.topic,
        oldWriter: family.handoff.scope.writerId,
      });
    } finally {
      await cold.destroy();
    }
  } else prior = await loadPriorRoot(db, scope, store);
  const base = claim.request.nextEventSeq - 1;
  presentation.bind(1, scope);
  presentation.sequences.set(scope.sessionId, base);
  if (cfg.resumeInput) {
    const answer = (context) => {
      const result = context.messages.find((m) => m.role === "toolResult");
      assert(result);
      const prompts = context.messages.filter((m) => m.role === "user").map(extract);
      assert.equal(prompts[0], claim.request.input.prompt);
      assert(prompts.length <= 2);
      restoredUserPrompts = prompts;
      return fauxAssistantMessage(
        prompts.length === 1
          ? "Original tool finished."
          : "Recovered input: " + prompts[1] + " / " + extract(result),
      );
    };
    fake.setResponses([answer, answer]);
  } else if (cfg.stopInputs && prior.baseSequence) {
    fake.setResponses([
      (context) => {
        restoredUserPrompts = context.messages.filter((m) => m.role === "user").map(extract);
        assert.equal(restoredUserPrompts.length, 2);
        assert.equal(restoredUserPrompts.at(-1), claim.request.input.prompt);
        return fauxAssistantMessage("Received edited draft.");
      },
    ]);
  } else if (prior.baseSequence === 0) {
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
  if (cfg.inputMode) {
    const inputs = new DurableHumanInputs({
      conversation: root,
      runId: scope.runId,
      storage: store,
      sequence: () => store.nextSeq - 1,
      assertCurrent: claim.inputAuthority.assertCurrent,
      accepting: () => acceptingInputs,
      prepare: async (text, uploads) => {
        assert.equal(uploads.length, 0);
        return text;
      },
    });
    process.on("message", async (m) => {
      if (m.stop && cfg.stopInputs) {
        acceptingInputs = false;
        stopWork ??= claim.cancel(() => root.abort(ctx));
        void stopWork.catch((e) => {
          console.error("Stop dispatch failed", e.name);
        });
        return;
      }
      if (!m.command) return;
      try {
        const c = m.command;
        assert.equal(
          c.executionReference,
          createExecutionReference(scope.writerId, scope.runId, scope.fencingToken),
        );
        assert(
          c.runId === scope.runId &&
            c.sessionId === scope.sessionId &&
            c.tenantId === scope.tenantId,
        );
        const value =
          c.action === "submit" ? await inputs.submit(c.input) : await inputs.mutate(c.mutation);
        process.send({ rpc: m.rpc, value });
      } catch (e) {
        process.send({ rpc: m.rpc, error: e.message });
      }
    });
  }
  if (prior.baseSequence && !cfg.resumeInput) await root.configure(agent, ctx);
  const sub = cfg.resumeInput
    ? undefined
    : await root.submit(
        { type: "input", requestId: scope.runId, content: claim.request.input.prompt },
        ctx,
      );
  if (claim.request.stopOnResume) {
    assert(cfg.resumeInput && cfg.stopInputs);
    acceptingInputs = false;
    stopWork = claim.cancel(() => root.abort(ctx));
    await stopWork;
  } else if (cfg.resumeInput) harness.resume();
  await root.waitForIdle(ctx);
  acceptingInputs = false;
  await stopWork;
  if (sub && !stopWork) assert.equal((await sub.wait(ctx)).status, "done");
  if (cfg.resumeInput) {
    const placed = await store.submissionByRequest(1, queuedInput.requestId, ctx);
    assert.equal(placed.status, stopWork ? "unanswered" : "done");
    if (stopWork) assert.equal(placed.reason, "aborted");
    assert.equal(placed.id, queuedInput.id);
  }
  await harness.close(ctx);
  harness = undefined;
  await claim.finish(
    (terminal) =>
      publish({
        kind: "work_finished",
        scope,
        factId: randomUUID(),
        occurredAt: new Date().toISOString(),
        agentId: "root",
        baseSequence: base,
        nativeSequence: store.nextSeq - 1,
        terminal,
      }),
    Boolean(stopWork),
  );
  process.send({
    done: true,
    stoppedOnResume: !!claim.request.stopOnResume,
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
