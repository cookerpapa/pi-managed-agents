// Disposable A/B actors for the retained-log experiment. Each has independent
// Pi, model client, producer and reply mailbox memory.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import pg from "pg";
import { MemoryStorage, InboxDoc } from "@earendil-works/pi-durable";
import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import { Type } from "@earendil-works/pi-ai";
import { createModels } from "@earendil-works/pi-ai/models";
import {
  fauxAssistantMessage,
  fauxProvider,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { DurableAgentSession } from "../../../packages/sandbox-supervisor/src/durable/agent-session.ts";
import { fixedRemoteToolDefinitions } from "../../../packages/sandbox-supervisor/src/fixed-remote-tool-definitions.ts";
import { KafkaWorkerProducer } from "../../../packages/event-log/src/kafka-worker-producer.ts";
import { KafkaToolReplyMailbox } from "../../../packages/event-log/src/tool-replies.ts";
import { encodeSessionLogFact } from "../../../packages/runtime-core/src/kafka-accepted-fact.ts";
import { loadPriorRoot, readRetainedFamily } from "./retained-family-read.mjs";

const init = await new Promise((resolve) => process.once("message", resolve));
const { scope, topic, brokers, dbUrl, binding, seal, mode } = init;
const send = (kind, properties = {}) => process.send({ kind, ...properties });
const db = new pg.Client({
    connectionString: dbUrl,
    options: "-c default_transaction_read_only=on",
  }),
  producer = new KafkaWorkerProducer({ brokers, topic, clientId: "family-" + scope.writerId });
const mailbox = new KafkaToolReplyMailbox({ brokers, bootId: scope.writerId, replicas: 3 });
const life = new AbortController();
let runtime,
  restored,
  child,
  finalModelAt,
  publications = 0,
  newIntents = 0,
  childModelCalls = 0,
  parentModelCalls = 0;
const operations = new Map();
const textOf = (m) =>
  m?.content
    ?.filter((x) => x.type === "text")
    .map((x) => x.text)
    .join("") ?? "";
async function publish(fact) {
  await producer.append(0, {
    sessionId: scope.piSessionId,
    factId: fact.factId,
    value: encodeSessionLogFact(fact),
  });
}
class Published extends MemoryStorage {
  async commit(writes) {
    const p = this.prepareCommit(writes),
      commit = { seq: p.seq, nextId: this.nextId, writes: p.writes };
    const effects = runtime?.preparePublication(commit);
    const tools = (effects?.tools ?? []).map(({ taskId, command }) => {
      assert(command.request?.operation === "tool.execute");
      const { args, toolCallId, ...request } = command.request;
      newIntents++;
      return {
        target: "cube",
        taskId,
        scope,
        request,
        replyRecovery: command.replyRecovery,
        replyTopic: command.replyRecovery.topic,
        occurredAt: command.occurredAt,
      };
    });
    await publish({
      kind: "durable_commit",
      scope,
      commit,
      factId: randomUUID(),
      occurredAt: new Date().toISOString(),
      tools,
      cancellations: [],
      display: [],
      origins: p.writes
        .filter((w) => ["entry", "task", "submission"].includes(w.type))
        .map((w) => ({
          recordId: w.value.id,
          sessionId: scope.sessionId,
          runId: scope.runId,
          turnId: scope.turnId,
        })),
    });
    const seq = p.apply();
    effects?.confirmed();
    publications++;
    return seq;
  }
}
const store = new Published(),
  models = createModels();
const parentModel = fauxProvider({
    provider: "family-parent",
    api: "faux-family-parent",
    tokensPerSecond: 0,
  }),
  childModel = fauxProvider({
    provider: "family-child",
    api: "faux-family-child",
    tokensPerSecond: 0,
  });
models.setProvider(childModel.provider);
models.setProvider({
  ...parentModel.provider,
  streamSimple(...args) {
    const stream = parentModel.provider.streamSimple(...args);
    void stream.result().then((m) => {
      if (m.stopReason === "stop") finalModelAt = Date.now();
    });
    return stream;
  },
});
const oldAnswer = "PREVIOUS_PG_ONLY_ANSWER";

const command =
  'printf "once\\n" >> executions; while [ ! -f release ]; do sleep 0.1; done; cat result';
function resultText(context, name) {
  const m = context.messages.findLast((m) => m.role === "toolResult" && m.toolName === name);
  assert(m, "Missing original Tool result in model context");
  return textOf(m);
}
parentModel.setResponses(
  seal
    ? [
        (context) => {
          parentModelCalls++;
          const text = resultText(context, "delegate");
          assert.match(text, /CHILD.*GUEST/);
          if (mode === "prior-rotated" || mode === "pending")
            assert(
              context.messages.some((m) => m.role === "assistant" && textOf(m) === oldAnswer),
              "Missing predecessor PG answer",
            );
          return fauxAssistantMessage("PARENT " + text);
        },
      ]
    : [
        () => {
          parentModelCalls++;
          return mode === "seed"
            ? fauxAssistantMessage(oldAnswer)
            : fauxAssistantMessage(fauxToolCall("delegate", {}), { stopReason: "toolUse" });
        },
      ],
);
childModel.setResponses(
  seal
    ? Array.from({ length: 2 }, (_, i) => (context) => {
        childModelCalls++;
        const text = resultText(context, "bash");
        assert.match(text, /GUEST/);
        if (i === 1)
          assert(
            context.messages.some(
              (m) =>
                m.role === "user" &&
                (typeof m.content === "string" ? m.content : textOf(m)).includes(
                  "queued-child-input",
                ),
            ),
          );
        return fauxAssistantMessage("CHILD " + text.trim());
      })
    : [
        () => {
          childModelCalls++;
          return fauxAssistantMessage(fauxToolCall("bash", { command, timeout: 90 }), {
            stopReason: "toolUse",
          });
        },
      ],
);
const delegate = {
  name: "delegate",
  description: "Await the owned child",
  parameters: Type.Object({}),
  replay: "safe",
  async execute(_args, api) {
    let requestId = randomUUID();
    if (seal) {
      const { items } = await store.scanDocuments(
        { scope: { kind: "session" }, kind: "pi-cloud.conversation-creation", at: "current" },
        10,
        undefined,
        ctx,
      );
      assert.equal(items.length, 1);
      const doc = await store.document(items[0].id, "current", ctx);
      assert.equal(doc.value.ownerTaskId, api.taskId);
      requestId = items[0].key;
    }
    const id = await runtime.createConversation({
      requestId,
      ownerTaskId: api.taskId,
      parentConversationId: 1,
    });
    if (!seal) {
      child = await runtime.bind(configuration(id));
      await child.submit(
        { type: "input", requestId: scope.runId, content: "Run the gated Bash once." },
        ctx,
      );
    } else assert.equal(child?.id, id, "Recovery changed child identity");
    await child.waitForIdle(ctx);
    const last = (await child.context(ctx)).messages.findLast((m) => m.role === "assistant");
    return { content: [{ type: "text", text: textOf(last) }] };
  },
};
const tool = fixedRemoteToolDefinitions().find((t) => t.name === "bash");
function configuration(id) {
  const root = id === 1,
    m = root ? parentModel.getModel() : childModel.getModel();
  return {
    runId: scope.runId,
    conversationId: id,
    model: m,
    agent: { instructions: root ? "parent instructions" : "retained child instructions" },
    context: { sessionId: scope.sessionId, turnId: scope.turnId },
    tools: root ? [delegate] : [],
    remoteTools: () => (root ? [] : ["bash"]),
    assertCurrent: async () => life.signal.throwIfAborted(),
    openModel: async (req) => ({ model: m, options: req.options, release() {} }),
    prepareRemote: async (inv) => {
      assert.equal(inv.toolName, "bash");
      assert.equal(inv.args.command, command);
      let op, ref, wait;
      if (inv.recovery) {
        op = operations.get(inv.taskId);
        assert(
          op && op.conversationId === id && op.reply.toolCallId === inv.callId,
          "Unknown saved wait",
        );
        ref = op.reply;
        wait = mailbox.resumeWait(op.operationId, ref, life.signal);
        send("reattached", {
          deadline: ref.deadlineAt,
          operationId: op.operationId,
          originalTopic: ref.topic,
        });
      } else {
        assert(!seal, "Replacement attempted a new Bash");
        ref = {
          ...(await mailbox.replyPosition()),
          toolName: "bash",
          toolCallId: inv.callId,
          deadlineAt: Date.now() + 120000,
        };
        op = { operationId: randomUUID(), taskId: inv.taskId, conversationId: id, reply: ref };
        wait = mailbox.wait(op.operationId, inv.callId, "bash", life.signal);
      }
      return {
        command: {
          executionReference: "fixture",
          toolCallId: "task:" + inv.taskId,
          occurredAt: new Date().toISOString(),
          replyTopic: ref.topic,
          replyRecovery: ref,
          request: {
            type: "tool_sandbox.operation",
            toolBrokerProtocolVersion: 1,
            operation: "tool.execute",
            activationId: binding,
            operationId: op.operationId,
            toolCallId: inv.callId,
            toolName: "bash",
            args: inv.args,
            timeoutMs: 90000,
            maximumOutputBytes: 51200,
          },
        },
        result: async () => {
          const r = await wait.result;
          return { ...r.result, isError: r.isError };
        },
        completed: () => wait.executionSettled(),
        dispose() {},
      };
    },
  };
}
try {
  await db.connect();
  await producer.start();
  await mailbox.start();
  const prior = await loadPriorRoot(db, scope, store);
  if (seal) {
    const start = performance.now();
    restored = await readRetainedFamily(db, brokers, topic, seal, scope, store, ctx);
    if (mode === "expired") {
      throw Error("Incomplete prefix unexpectedly restored");
    }
    for (const op of restored.intents) operations.set(op.taskId, op);
    assert.equal(operations.size, 1);
    const tasks = (await store.scanTasks({ status: "waiting" }, 10, undefined, ctx)).items;
    const generators = tasks.filter((t) => t.kind === "pi.generation");
    assert.equal(generators.length, 2);
    const original = restored.intents[0];
    const leaf = generators.find((t) => t.conversationId === original.conversationId);
    const rootGeneration = generators.find((t) => t.conversationId === 1);
    assert.deepEqual(leaf.state.on, [original.taskId]);
    assert.equal(
      (await store.conversation(leaf.conversationId, ctx)).owner.taskId,
      rootGeneration.id,
    );
    assert.equal((await store.task(rootGeneration.state.on[0], ctx)).kind, "pi.tool");

    const replyStart = performance.now();
    await mailbox.restore(
      restored.intents.map(({ operationId, reply }) => ({ operationId, reply })),
      life.signal,
    );
    send("cold", {
      stats: { ...restored.stats, ...prior, mailboxRestoreMs: performance.now() - replyStart },
      coldMs: performance.now() - start,
    });
  }
  runtime = await DurableAgentSession.open(store, {
    models,
    remoteTools: [tool],
    infrastructureTools: [delegate],
    settings: { compaction: { enabled: false }, retry: { maxRetries: 0 } },
  });
  const root = await runtime.bind(configuration(1), seal ? "reattach" : "initialize");
  if (seal) {
    const id = restored.intents[0].conversationId;
    child = await runtime.bind(configuration(id), "reattach");
    assert.equal((await runtime.harness.snapshot(InboxDoc, id, ctx)).items.length, 1);
    runtime.harness.resume();
  } else {
    process.on("message", async (m) => {
      if (m.kind !== "queue") return;
      try {
        await child.submit(
          {
            type: "input",
            requestId: "queued-child-input",
            whenBusy: "followUp",
            content: "queued-child-input",
          },
          ctx,
        );
        send("queued");
      } catch (e) {
        send("error", { message: e.message });
      }
    });
    await root.submit(
      { type: "input", requestId: scope.runId, content: "Delegate to the child." },
      ctx,
    );
  }
  await root.waitForIdle(ctx);
  assert(seal || mode === "seed", "Initial Worker finished unexpectedly");
  await publish({
    kind: "work_finished",
    scope,
    agentId: "root",
    baseSequence: prior.baseSequence,
    nativeSequence: store.nextSeq - 1,
    factId: randomUUID(),
    occurredAt: new Date().toISOString(),
    terminal: { type: "turn.completed", payload: { stopReason: "stop" } },
  });
  if (mode === "seed") {
    send("seedFinished");
  } else {
    const history = (await child.context(ctx)).messages,
      rootHistory = (await root.context(ctx)).messages;
    const results = history.filter((m) => m.role === "toolResult" && m.toolName === "bash");
    assert.equal(results.length, 1);
    assert.equal(
      rootHistory.filter((m) => m.role === "toolResult" && m.toolName === "delegate").length,
      1,
    );
    assert.equal(newIntents, 0);
    assert.equal(childModelCalls, 2);
    assert.equal(parentModelCalls, 1);
    assert.equal(
      (await store.submissionByRequest(child.id, "queued-child-input", ctx)).status,
      "done",
    );
    send("finished", {
      finalModelAt,
      parentText: textOf(rootHistory.at(-1)),
      results: results.length,
      newIntents,
      childModelCalls,
      parentModelCalls,
      publications,
    });
  }
} catch (e) {
  if (mode === "expired" && /Incomplete accepted native prefix/.test(e.message))
    send("expired", { reason: e.message, writes: publications });
  else {
    send("error", { message: e.message, where: e.stack?.split("\n").slice(1, 4) });
    process.exitCode = 1;
  }
} finally {
  life.abort();
  try {
    await runtime?.close();
  } finally {
    await mailbox.shutdown();
    await producer.close();
    await db.end();
    process.disconnect();
  }
}
