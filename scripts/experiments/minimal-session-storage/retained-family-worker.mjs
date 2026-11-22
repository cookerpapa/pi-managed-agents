// Disposable A/B actors for the retained-log experiment. Each has independent
// Pi, model client, producer and reply mailbox memory.
import { FamilyPresentation } from "./family-presentation.mjs";
import {
  createExecutionReference,
  durableToolState,
  MAX_TOOL_EXECUTION_TIMEOUT_MS,
} from "@pi-cloud/protocol";
import assert from "node:assert/strict";
import { randomUUID, createHash } from "node:crypto";
import pg from "pg";
import { MemoryStorage, InboxDoc } from "@earendil-works/pi-durable";
import { BACKGROUND_CONTEXT as ctx, awaitWithContext } from "@earendil-works/chord/context";
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
import { encodeWorkspaceFact } from "./workspace-operations.mjs";

import { loadPriorRoot, readRetainedFamily } from "./retained-family-read.mjs";

const init = await new Promise((resolve) => process.once("message", resolve));
const { topic, brokers, dbUrl, mode } = init;
assert(!("binding" in init || "activationId" in init));
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
let restoredIdFloor = Infinity;
const hashKey = (v) => createHash("sha256").update(v).digest("hex");
let runtime,
  restored,
  child,
  childView,
  finalModelAt,
  publications = 0,
  newIntents = 0,
  cancellationCount = 0,
  childModelCalls = 0,
  parentModelCalls = 0;
let sessionHost,
  sessionHandle,
  coldDb,
  mailboxRestoreMs = 0;
const operations = new Map(),
  cancelledTasks = new Set();
const textOf = (m) =>
  m?.content
    ?.filter((x) => x.type === "text")
    .map((x) => x.text)
    .join("") ?? "";
async function publish(fact) {
  await sessionHandle?.authority.assertCurrent();
  await producer.append(0, {
    sessionId: scope.piSessionId,
    factId: fact.factId,
    value: encodeWorkspaceFact(fact),
  });
}
class Published extends MemoryStorage {
  nativePositions = new Map();
  get committedSequence() {
    return this.nextSeq - 1;
  }
  async recordSequence(id) {
    return this.nativePositions.get(id);
  }
  indexPositions(writes, seq) {
    for (const w of writes) this.nativePositions.set(w.value?.id ?? w.record?.id ?? w.id, seq);
  }
  async commit(writes) {
    const p = this.prepareCommit(writes),
      commit = { seq: p.seq, nextId: this.nextId, writes: p.writes };
    let display;
    if (this.hostPublish) {
      await this.hostPublish(commit, [], []);
      display = this.stagedDisplay;
      this.stagedDisplay = undefined;
    } else display = await this.publishNativeBatch(commit);
    const seq = p.apply();
    if (init.atomicHandoff) this.indexPositions(p.writes, seq);
    for (const w of p.writes) {
      const t = w.type === "task" && durableToolState(w.value);
      if (t?.terminal) {
        operations.get(t.id)?.controller?.abort();
        operations.delete(t.id);
        cancelledTasks.delete(t.id);
      }
    }
    display?.confirmed();
    publications++;
    return seq;
  }
  async publishNativeBatch(commit) {
    const display = presentation?.prepare(commit);
    const workspaceOperations = [];
    for (const write of commit.writes) {
      const task = write.type === "task" && durableToolState(write.value);
      if (!task) continue;
      let op = operations.get(task.id);
      if (task.intent && !op) {
        const stored = await this.entry(write.value.input.assistant, ctx);
        const call = stored?.entry.model?.[0]?.content.find(
          (block) => block.type === "toolCall" && block.id === task.intent.callId,
        );
        assert(call, "Tool intent has no matching native assistant Entry");
        if (call.name !== "bash") continue;
        assert(
          !takeover || task.id >= restoredIdFloor,
          "Restored Bash lacks its original operation",
        );
        const controller = new AbortController(),
          operationId = randomUUID(),
          reply = {
            ...(await mailbox.replyPosition()),
            toolName: "bash",
            toolCallId: call.id,
            deadlineAt: Date.now() + (mode === "reply-timeout" ? 18000 : 120000),
          };
        const wait = mailbox.wait(
          operationId,
          call.id,
          "bash",
          AbortSignal.any([controller.signal, life.signal]),
        );
        op = {
          operationId,
          taskId: task.id,
          conversationId: task.conversationId,
          reply,
          controller,
          wait,
        };
        operations.set(task.id, op);
        newIntents++;
        workspaceOperations.push({
          action: "execute",
          taskId: task.id,
          operationId,
          reply,
          timeoutMs:
            task.intent.arguments.timeout === undefined
              ? MAX_TOOL_EXECUTION_TIMEOUT_MS
              : task.intent.arguments.timeout * 1000,
          maximumOutputBytes: 51200,
        });
      }
      if (task?.aborted && op && !op.wait?.executionSettled() && !cancelledTasks.has(task.id)) {
        cancelledTasks.add(task.id);
        cancellationCount++;
        workspaceOperations.push({
          action: "cancel",
          taskId: task.id,
          operationId: op.operationId,
        });
      }
    }
    try {
      await publish({
        kind: "durable_commit",
        scope,
        commit,
        factId: randomUUID(),
        occurredAt: new Date().toISOString(),
        tools: [],
        cancellations: [],
        workspaceOperations,
        display: display?.display ?? [],
        origins: commit.writes
          .filter((w) => ["entry", "task", "submission"].includes(w.type))
          .map((w) => ({
            recordId: w.value.id,
            sessionId: scopeFor(w.value.conversationId).sessionId,
            runId: scope.runId,
            turnId: scopeFor(w.value.conversationId).turnId,
          })),
      });
    } catch (error) {
      life.abort(error);
      throw error;
    }
    return display;
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
const twice = ["double-pending", "double-before-open"].includes(mode);
const summary = "Retained family summary: build the shared workspace.";
function checkCompacted(context) {
  const messages = JSON.stringify(context.messages);
  assert(
    messages.includes(summary) && !messages.includes("expired-history-marker"),
    "Incorrect compacted model context",
  );
}
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
  if (twice) checkCompacted(context);
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
  return fauxAssistantMessage(
    fauxToolCall("bash", { command, timeout: init.bashFields ? "90" : 90 }),
    {
      stopReason: "toolUse",
    },
  );
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
          (typeof m.content === "string" ? m.content : textOf(m)).includes(
            mode === "double-pending" ? "queued-child-input edited-by-B" : "queued-child-input",
          ),
      ),
    );
  return fauxAssistantMessage(
    mode === "reply-timeout"
      ? "CHILD UNKNOWN tool_reply_timeout: original Tool reply deadline elapsed"
      : "CHILD " + text.trim(),
  );
}
parentModel.setResponses([
  ...(twice && !takeover
    ? [
        fauxAssistantMessage("old text ".repeat(600)),
        fauxAssistantMessage("recent text ".repeat(240)),
        fauxAssistantMessage(summary),
      ]
    : []),
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
    tools: root ? [delegate] : [workspaceBash],
    remoteTools: () => [],
    assertCurrent: async () => life.signal.throwIfAborted(),
    openModel: async (req) => ({ model: m, options: req.options, release() {} }),
    prepareRemote: async () => {
      throw Error("Workspace tools use the prototype operation port");
    },
  };
}
const workspaceBash = {
  ...tool,
  async execute(args, api, context) {
    assert.equal(args.command, command);
    const op = operations.get(api.taskId);
    assert(
      op && op.reply.toolCallId === api.callId && op.conversationId === api.conversationId,
      "Missing original native operation",
    );
    if (!op.wait) {
      op.controller = new AbortController();
      op.wait = mailbox.resumeWait(
        op.operationId,
        op.reply,
        AbortSignal.any([op.controller.signal, life.signal]),
      );
      send("reattached", {
        deadline: op.reply.deadlineAt,
        operationId: op.operationId,
        originalTopic: op.reply.topic,
      });
    }
    try {
      const r = await awaitWithContext(op.wait.result, context);
      return { ...r.result, isError: r.isError };
    } catch (e) {
      if (mode === "reply-timeout" && !context.abortSignal?.aborted) {
        assert.equal(e.code, "tool_reply_timeout");
        assert.equal(e.effect, "unknown");
        const at = Date.now(),
          probeId = randomUUID(),
          signal = AbortSignal.timeout(10000);
        const echo = new Promise((resolve, reject) => {
          const done = (m) => {
            if (
              m.kind === "probeVerdict" &&
              m.probeId === probeId &&
              m.operationId === op.operationId
            ) {
              cleanup();
              resolve(m.ready);
            }
          };
          const abort = () => {
            cleanup();
            reject(signal.reason);
          };
          const cleanup = () => {
            process.off("message", done);
            signal.removeEventListener("abort", abort);
          };
          process.on("message", done);
          signal.addEventListener("abort", abort, { once: true });
        });
        const [, ready] = await Promise.all([
          publish({
            kind: "path_probe",
            factId: probeId,
            scope,
            occurredAt: new Date(at).toISOString(),
            workspaceOperations: [
              {
                action: "probe",
                taskId: api.taskId,
                operationId: op.operationId,
                conversationId: api.conversationId,
              },
            ],
          }),
          echo,
        ]);
        assert.equal(ready, true);

        send("unknown", { code: e.code, effect: e.effect, at, deadline: op.reply.deadlineAt });
      }
      throw e;
    }
  },
};
try {
  await db.connect();
  if (init.atomicHandoff && takeover) {
    const {
      rows: [rights],
    } =
      await db.query(`select bool_or(has_table_privilege(current_user,'public.'||name,'select')) as legacy
      from unnest(array['pi_durable_commits','pi_durable_records','pi_durable_log','pi_durable_current']) as t(name)`);
    assert.equal(rights.legacy, false, "Cold data role can read the old native store");
  }
  await producer.start();
  await mailbox.start();
  const prior = await loadPriorRoot(db, scope, store);
  if (takeover) {
    const start = performance.now();
    let pgEvidence = {};
    if (init.atomicHandoff) {
      const { createDatabase } = await import("@pi-cloud/database");
      const { PostgresDurableSessionHost, hydrateSealedFamily } =
        await import("@pi-cloud/pi-session-postgres/durable");
      coldDb = createDatabase({ connectionString: dbUrl, maxConnections: 2 });
      sessionHost = new PostgresDurableSessionHost({
        database: coldDb,
        sealedRecovery: {
          topic,
          async hydrate(rows, authority, publishThroughOwner) {
            store.hostPublish = publishThroughOwner;
            store.nativePositions = await hydrateSealedFamily(rows, store);
            const native = rows.handoff.state;
            assert.equal(store.committedSequence, native.seq);
            assert.equal(store.nextId, native.nextId);
            presentation.sequences = new Map(native.sequences);
            for (const { write } of native.records)
              if (write.record?.kind === "pi.live")
                presentation.live[write.record.id] = {
                  conversation: write.record.scope.conversationId,
                  value: write.content.value,
                };
            const [pending] = rows.intents;
            const [input] = (
              await store.scanSubmissions(
                { conversationId: pending.conversationId, status: "queued" },
                2,
                undefined,
                ctx,
              )
            ).items;
            assert(
              input &&
                (await store.submissionByRequest(pending.conversationId, input.requestId, ctx))
                  .id === input.id,
            );
            queuedKey = input.requestId;
            pgEvidence = {
              oldWriter: rows.handoff.scope.writerId,
              sealOffset: rows.handoff.offset,
              requestHash: hashKey(queuedKey),
            };
            restored = {
              intents: rows.intents,
              stats: {
                candidateEntries: rows.entries.length,
                sealNativeSeq: native.seq,
                source: "worker-session-host/sealed-candidate-pg",
              },
            };
            return store;
          },
        },
        restoreReplies: async (refs, signal) => {
          const begin = performance.now();
          await mailbox.restore(refs, signal);
          mailboxRestoreMs = performance.now() - begin;
        },
      });
      claim.attachNativeHost(sessionHost);
      sessionHandle = await sessionHost.open({
        binding: {
          ...scope,
          executionReference: createExecutionReference(
            scope.writerId,
            scope.runId,
            scope.fencingToken,
          ),
          conversationId: 1,
        },
        stopOnResume: claim.request.stopOnResume,
        signal: life.signal,
        publish: async ({ commit }) => {
          store.stagedDisplay = await store.publishNativeBatch(commit);
        },
      });
      assert.equal(sessionHandle.storage, store);
      assert.equal(sessionHandle.committedSequence(), restored.stats.sealNativeSeq);
    } else if (init.pgHandoff) {
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
        bashFields: init.bashFields,
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
    restoredIdFloor = store.nextId;
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
    if (!sessionHandle)
      await mailbox.restore(
        restored.intents.map(({ operationId, reply }) => ({ operationId, reply })),
        life.signal,
      );
    send("cold", {
      ...pgEvidence,
      stats: {
        ...restored.stats,
        ...prior,
        mailboxRestoreMs: sessionHandle ? mailboxRestoreMs : performance.now() - replyStart,
      },
      coldMs: performance.now() - start,
    });
  }
  const openRuntime = () =>
    DurableAgentSession.open(sessionHandle?.storage ?? store, {
      models,
      remoteTools: [],
      infrastructureTools: [delegate, workspaceBash],
      settings: {
        compaction: { enabled: false, ...(twice ? { keepRecentTokens: 64 } : {}) },
        retry: { maxRetries: 0 },
      },
    });
  runtime = sessionHandle ? await sessionHandle.runtime(openRuntime) : await openRuntime();
  const root = await runtime.bind(configuration(1), takeover ? "reattach" : "initialize");
  let stopWork;
  let modelsBeforeStop;
  function stop() {
    modelsBeforeStop = childModelCalls + parentModelCalls;
    stopWork ??= claim
      .cancel(() => root.abort(ctx))
      .then((receipt) => send("stopAcknowledged", { receipt }));
    return stopWork;
  }
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
    const inbox = await runtime.harness.snapshot(InboxDoc, id, ctx);
    assert.equal(inbox.items.length, 1);
    if (twice)
      assert.equal(
        inbox.items[0].content,
        scope.fencingToken >= 3 && mode === "double-pending"
          ? "queued-child-input edited-by-B"
          : "queued-child-input",
      );
    if (claim?.request.stopOnResume) {
      modelsBeforeStop = childModelCalls + parentModelCalls;
      stopWork = claim
        .cancel(async () => {
          await child.abort(ctx);
          await root.abort(ctx);
        })
        .then((receipt) => send("stopAcknowledged", { receipt }));
      await stopWork;
    }
    if (twice)
      process.on("message", async (m) => {
        if (m.kind !== "confirmBEdit") return;
        try {
          await child.commit(async (tx) => {
            const inbox = await tx.doc(InboxDoc, child.id);
            assert.equal(inbox.items.length, 1);
            inbox.items[0].content = "queued-child-input edited-by-B";
          }, ctx);
          send("bEdited", { seq: store.nextSeq - 1 });
        } catch (error) {
          send("error", { message: error.message });
        }
      });
    runtime.harness.resume();
  } else {
    process.on("message", async (m) => {
      try {
        if (m.kind === "stop") {
          await stop();
          return;
        }
        if (m.kind !== "queue") return;
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
    if (twice) {
      for (const text of ["expired-history-marker " + "old ".repeat(1500), "keep the tail"]) {
        const seed = await root.submit({ type: "input", content: text }, ctx);
        await root.waitForIdle(ctx);
        assert.equal((await seed.wait(ctx)).status, "done");
      }
      await root.compact(undefined, ctx);
      await root.waitForIdle(ctx);
      checkCompacted(await root.context(ctx));
    }
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
  await stopWork;
  assert(takeover || normal || stopWork || mode === "seed", "Initial Worker finished unexpectedly");
  if (!stopWork) {
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
  if (claim) await claim.finish(finish, !!stopWork);
  else await finish({ type: "turn.completed", payload: { stopReason: "stop" } });
  if (stopWork) {
    assert.equal(childModelCalls + parentModelCalls, modelsBeforeStop);
    const queued = await store.submissionByRequest(child.id, queuedKey, ctx);
    send("stopped", {
      queuedStatus: queued.status,
      newIntents,
      models: childModelCalls + parentModelCalls - modelsBeforeStop,
      cancellations: cancellationCount,
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
    if (sessionHandle) await sessionHandle.close();
    else await runtime?.close();
  } finally {
    await sessionHost?.close();
    await coldDb?.destroy();
    await mailbox.shutdown();
    await producer.close();
    await db.end();
    await claim?.close();
    process.disconnect();
  }
}
