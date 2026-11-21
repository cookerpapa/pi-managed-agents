// Disposable A/B actors for the retained-log experiment. Each has independent
// Pi, model client, producer and reply mailbox memory.
import { FamilyPresentation } from "./family-presentation.mjs";
import { durableToolState } from "@pi-cloud/protocol";
import assert from "node:assert/strict";
import { randomUUID, createHash } from "node:crypto";
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
const { topic, brokers, dbUrl, binding, mode } = init;
const takeover = init.pgResume || !!init.seal;
const send = (kind, properties = {}) => process.send({ kind, ...properties });
const claim = init.controlUrl
  ? await (await import("./serving-family-claim.mjs")).claimRun(init.controlUrl, init.runId)
  : undefined;
const scope = claim?.scope ?? init.scope;
const bootId = claim?.bootId ?? scope.writerId;
if (claim) {
  const ready = new Promise((resolve) => process.once("message", resolve));
  send("claimed", { scope, bootId, stopOnResume: !!claim.request.stopOnResume });
  assert.equal((await ready).kind, "start");
}
const db = new pg.Client({
    connectionString: dbUrl,
    options: "-c default_transaction_read_only=on",
  }),
  producer = new KafkaWorkerProducer({ brokers, topic, clientId: "family-" + bootId });
const mailbox = new KafkaToolReplyMailbox({
  brokers: init.replyBrokers ?? brokers,
  bootId,
  replicas: 3,
});
const presentation = claim ? new FamilyPresentation() : undefined;
const life = new AbortController();
let queuedKey = "queued-child-input";
const hashKey = (v) => createHash("sha256").update(v).digest("hex");
let runtime,
  restored,
  child,
  childView,
  finalModelAt,
  publications = 0,
  newIntents = 0,
  childModelCalls = 0,
  parentModelCalls = 0;
const operations = new Map(),
  cancelledTasks = new Set();
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
    const display = presentation?.prepare(commit);
    const effects = runtime?.preparePublication(commit);
    const tools = (effects?.tools ?? []).map(({ taskId, command }) => {
      assert(command.request?.operation === "tool.execute");
      const { args, toolCallId, ...request } = command.request;
      newIntents++;
      return {
        target: "cube",
        taskId,
        scope: scopeFor(
          p.writes.find((w) => w.type === "task" && w.value.id === taskId).value.conversationId,
        ),
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
      cancellations: p.writes.flatMap((w) => {
        const task = w.type === "task" && durableToolState(w.value),
          op = task && operations.get(task.id);
        if (!task?.aborted || !op || cancelledTasks.has(task.id)) return [];
        cancelledTasks.add(task.id);
        return [
          {
            kind: "tool_cancel",
            scope: scopeFor(task.conversationId),
            request: {
              activationId: op.activationId,
              operationId: op.operationId,
              originalWriterId: op.writerId,
            },
          },
        ];
      }),
      display: display?.display ?? [],
      origins: p.writes
        .filter((w) => ["entry", "task", "submission"].includes(w.type))
        .map((w) => ({
          recordId: w.value.id,
          sessionId: scopeFor(w.value.conversationId).sessionId,
          runId: scope.runId,
          turnId: scopeFor(w.value.conversationId).turnId,
        })),
    });
    const seq = p.apply();
    effects?.confirmed();
    display?.confirmed();
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
const normal = mode === "normal";
function beginParent(context) {
  parentModelCalls++;
  if (claim)
    assert(
      context.messages.some(
        (m) =>
          m.role === "user" &&
          (typeof m.content === "string" ? m.content : textOf(m)) === claim.request.input.prompt,
      ),
    );
  return mode === "seed"
    ? fauxAssistantMessage(oldAnswer)
    : fauxAssistantMessage(fauxToolCall("delegate", { task: "Run the gated Bash once." }), {
        stopReason: "toolUse",
      });
}
function finishParent(context) {
  parentModelCalls++;
  const text = resultText(context, "delegate");
  assert.match(
    text,
    mode === "reply-timeout" ? /CHILD UNKNOWN tool_reply_timeout/ : /CHILD.*GUEST/,
  );
  if (mode === "prior-rotated" || mode === "pending")
    assert(
      context.messages.some((m) => m.role === "assistant" && textOf(m) === oldAnswer),
      "Missing predecessor PG answer",
    );
  return fauxAssistantMessage("PARENT " + text);
}
function beginChild() {
  childModelCalls++;
  return fauxAssistantMessage(fauxToolCall("bash", { command, timeout: 90 }), {
    stopReason: "toolUse",
  });
}
function finishChild(context) {
  childModelCalls++;
  const text = resultText(context, "bash");
  assert.match(text, mode === "reply-timeout" ? /original Tool reply deadline/i : /GUEST/);
  if (childModelCalls === (normal ? 3 : 2))
    assert(
      context.messages.some(
        (m) =>
          m.role === "user" &&
          (typeof m.content === "string" ? m.content : textOf(m)).includes("queued-child-input"),
      ),
    );
  return fauxAssistantMessage(
    mode === "reply-timeout"
      ? "CHILD UNKNOWN tool_reply_timeout: original Tool reply deadline elapsed"
      : "CHILD " + text.trim(),
  );
}
parentModel.setResponses([
  ...(takeover ? [] : [beginParent]),
  ...(takeover || normal ? [finishParent] : []),
]);
childModel.setResponses([
  ...(takeover ? [] : [beginChild]),
  ...(takeover || normal ? [finishChild, finishChild] : []),
]);
const delegate = {
  name: "delegate",
  description: "Await the owned child",
  parameters: Type.Object({ task: Type.String() }),
  replay: "safe",
  async execute(_args, api) {
    let requestId = randomUUID();
    if (takeover) {
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
    if (claim && !takeover) {
      const start = new Promise((resolve) => {
        const handle = (m) => {
          if (m.kind === "startChild") {
            process.off("message", handle);
            resolve(m);
          }
        };
        process.on("message", handle);
      });
      send("delegateRequest", { requestId, taskId: api.taskId, sequence: store.nextSeq - 1 });
      childView = (await start).view;
      assert.equal(childView.id, requestId);
    }
    const id = await runtime.createConversation({
      requestId,
      ownerTaskId: api.taskId,
      parentConversationId: 1,
      ...(childView ? { view: childView } : {}),
    });
    if (!takeover) {
      child = await runtime.bind(configuration(id));
      await child.submit(
        { type: "input", requestId: scope.runId, content: "Run the gated Bash once." },
        ctx,
      );
      if (claim) send("childStarted", { id });
    } else assert.equal(child?.id, id, "Recovery changed child identity");
    await child.waitForIdle(ctx);
    const last = (await child.context(ctx)).messages.findLast((m) => m.role === "assistant");
    return { content: [{ type: "text", text: textOf(last) }] };
  },
};
const tool = fixedRemoteToolDefinitions().find((t) => t.name === "bash");
function scopeFor(id) {
  return id !== 1 && childView
    ? { ...scope, sessionId: childView.sessionId, turnId: childView.turnId }
    : scope;
}
function configuration(id) {
  presentation?.bind(id, scopeFor(id));
  const root = id === 1,
    m = root ? parentModel.getModel() : childModel.getModel();
  return {
    runId: scope.runId,
    conversationId: id,
    model: m,
    agent: { instructions: root ? "parent instructions" : "retained child instructions" },
    context: { sessionId: scopeFor(id).sessionId, turnId: scopeFor(id).turnId },
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
        assert(!takeover, "Replacement attempted a new Bash");
        ref = {
          ...(await mailbox.replyPosition()),
          toolName: "bash",
          toolCallId: inv.callId,
          deadlineAt: Date.now() + (mode === "reply-timeout" ? 18000 : 120000),
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
            activationId: op.activationId ?? binding,
            operationId: op.operationId,
            toolCallId: inv.callId,
            toolName: "bash",
            args: inv.args,
            timeoutMs: 90000,
            maximumOutputBytes: 51200,
          },
        },
        result: async () => {
          let r;
          try {
            r = await wait.result;
          } catch (e) {
            if (mode === "reply-timeout") {
              assert.equal(e.code, "tool_reply_timeout");
              assert.equal(e.effect, "unknown");
              const at = Date.now(),
                probeId = randomUUID(),
                signal = AbortSignal.timeout(10000);
              const echo = mailbox.waitProbe(probeId, op.activationId, signal);
              await publish({
                kind: "path_probe",
                factId: probeId,
                scope,
                occurredAt: new Date(at).toISOString(),
                execution: {
                  replyTopic: mailbox.topic,
                  bindings: [{ bindingId: op.activationId, scope: scopeFor(id), probe: true }],
                },
              });
              assert.equal((await echo).evidence.bindingId, op.activationId);
              send("unknown", { code: e.code, effect: e.effect, at, deadline: ref.deadlineAt });
            }
            throw e;
          }
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
  if (takeover) {
    const start = performance.now();
    let pgEvidence = {};
    if (init.pgHandoff) {
      const { readFamilyPgControl } = await import("./pg-family-reader.mjs");
      const pgOnly = await readFamilyPgControl({
        db,
        dbUrl,
        scope,
        topic,
        store,
        ctx,
        presentation,
        models,
        rootConfig: configuration(1),
      });
      restored = pgOnly.restored;
      queuedKey = pgOnly.queuedKey;
      pgEvidence = { ...pgOnly.evidence, requestHash: hashKey(queuedKey) };
      assert.equal(publications + newIntents + childModelCalls + parentModelCalls, 0);
    } else
      restored = await readRetainedFamily(
        db,
        brokers,
        topic,
        init.seal,
        scope,
        store,
        ctx,
        presentation,
      );
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
      ...pgEvidence,
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
  const root = await runtime.bind(configuration(1), takeover ? "reattach" : "initialize");
  if (takeover) {
    const id = restored.intents[0].conversationId;
    if (claim) {
      const receipt = (
        await store.scanDocuments(
          { scope: { kind: "session" }, kind: "pi-cloud.conversation-creation", at: "current" },
          2,
          undefined,
          ctx,
        )
      ).items;
      assert.equal(receipt.length, 1);
      childView = (await store.document(receipt[0].id, "current", ctx)).value.view;
      assert.equal(childView.id, receipt[0].key);
    }
    child = await runtime.bind(configuration(id), "reattach");
    assert.equal((await runtime.harness.snapshot(InboxDoc, id, ctx)).items.length, 1);
    if (claim?.request.stopOnResume) {
      const receipt = await claim.cancel(async () => {
        await child.abort(ctx);
        await root.abort(ctx);
      });
      send("stopAcknowledged", { receipt });
    }
    runtime.harness.resume();
  } else {
    process.on("message", async (m) => {
      if (m.kind !== "queue") return;
      try {
        if (init.pgHandoff) queuedKey = randomUUID();
        await child.submit(
          {
            type: "input",
            requestId: queuedKey,
            whenBusy: "followUp",
            content: init.pgHandoff ? "before edit" : "queued-child-input",
          },
          ctx,
        );
        if (init.pgHandoff)
          await child.commit(async (tx) => {
            const inbox = await tx.doc(InboxDoc, child.id);
            assert.equal(inbox.items.length, 1);
            inbox.items[0].content = "queued-child-input";
          }, ctx);
        send(
          "queued",
          init.pgHandoff ? { requestHash: hashKey(queuedKey), seq: store.nextSeq - 1 } : {},
        );
      } catch (e) {
        send("error", { message: e.message });
      }
    });
    await root.submit(
      {
        type: "input",
        requestId: scope.runId,
        content: claim?.request.input.prompt ?? "Delegate to the child.",
      },
      ctx,
    );
  }
  await root.waitForIdle(ctx);
  assert(takeover || normal || mode === "seed", "Initial Worker finished unexpectedly");
  if (!claim?.request.stopOnResume) {
    const last = (await root.context(ctx)).messages.at(-1);
    assert.equal(last?.role, "assistant", "Pi has no final parent assistant answer");
    assert.equal(
      last.stopReason,
      "stop",
      last.errorMessage ?? "Parent model did not finish normally",
    );
    assert(finalModelAt, "No successful final provider completion");
    if (mode !== "seed")
      assert.match(
        textOf(last),
        mode === "reply-timeout"
          ? /^PARENT CHILD UNKNOWN tool_reply_timeout/
          : /^PARENT CHILD GUEST/,
      );
  }
  const finish = (terminal) =>
    publish({
      kind: "work_finished",
      scope,
      agentId: "root",
      baseSequence: claim ? Number(claim.request.nextEventSeq) - 1 : prior.baseSequence,
      nativeSequence: store.nextSeq - 1,
      factId: randomUUID(),
      occurredAt: new Date().toISOString(),
      terminal,
    });
  if (claim) await claim.finish(finish);
  else await finish({ type: "turn.completed", payload: { stopReason: "stop" } });
  if (claim?.request.stopOnResume) {
    assert.equal(childModelCalls + parentModelCalls, 0);
    const queued = await store.submissionByRequest(child.id, queuedKey, ctx);
    send("stopped", {
      queuedStatus: queued.status,
      newIntents,
      models: childModelCalls + parentModelCalls,
      cancellations: cancelledTasks.size,
    });
  } else if (mode === "seed") {
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
    assert.equal(newIntents, normal ? 1 : 0);
    assert.equal(childModelCalls, normal ? 3 : 2);
    assert.equal(parentModelCalls, normal ? 2 : 1);
    assert.equal((await store.submissionByRequest(child.id, queuedKey, ctx)).status, "done");
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
    await claim?.close();
    process.disconnect();
  }
}
