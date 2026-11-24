// Independent disposable Worker process. Its parent can stop it at an acknowledged
// native commit, before local adoption or the next tool/model action.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
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
import { createDatabase } from "@pi-cloud/database";
import { createExecutionReference, toolResultIsUnknown } from "@pi-cloud/protocol";
import {
  PostgresDurableSessionHost,
  hydrateSealedFamily,
} from "@pi-cloud/pi-session-postgres/durable";
import { KafkaToolReplyMailbox } from "../../../packages/event-log/src/tool-replies.ts";
import { KafkaWorkerProducer } from "../../../packages/event-log/src/kafka-worker-producer.ts";
import { DurableRemoteTools } from "../../../packages/sandbox-supervisor/src/durable/remote-tools.ts";
import { fixedRemoteToolDefinitions } from "../../../packages/sandbox-supervisor/src/fixed-remote-tool-definitions.ts";
import { FamilyPresentation } from "./family-presentation.mjs";
import { encodeWorkspaceFact } from "./workspace-operations.mjs";

const [init] = await once(process, "message");
const { scope, topic, brokers, hold, stop } = init;
const mailbox = new KafkaToolReplyMailbox({ brokers, bootId: randomUUID(), replicas: 3 });
const producer = new KafkaWorkerProducer({
  brokers,
  topic,
  clientId: "bash-loss-" + scope.writerId,
});
const view = new FamilyPresentation();
view.bind(1, scope);
let handle, db, harness, remote, publishThroughHost;
let restores = 0,
  count = 0,
  modelCalls = 0;
const park = async (stage) => {
  if (hold === stage) {
    process.send({ kind: "parked", stage });
    await new Promise(() => {});
  }
};
async function publish({ commit, tools = [] }) {
  const d = view.prepare(commit);
  const ops = tools.map(({ taskId, command }) => ({
    action: "execute",
    taskId,
    operationId: command.request.operationId,
    timeoutMs: 10000,
    maximumOutputBytes: 51200,
    reply: command.replyRecovery,
  }));
  const fact = {
    kind: "durable_commit",
    scope,
    commit,
    factId: randomUUID(),
    occurredAt: new Date().toISOString(),
    origins: commit.writes.flatMap((w) =>
      w.value?.conversationId
        ? [
            {
              recordId: w.value.id,
              sessionId: scope.sessionId,
              turnId: scope.turnId,
              runId: scope.runId,
            },
          ]
        : [],
    ),
    display: d.display,
    tools: [],
    cancellations: [],
    workspaceOperations: ops,
  };
  if (commit.writes.some((w) => w.type === "entry" && w.value.kind === "pi.tool-result"))
    await park("before-result");
  await producer.append(0, {
    sessionId: scope.piSessionId,
    factId: fact.factId,
    value: encodeWorkspaceFact(fact),
  });
  d.confirmed();
  for (const w of commit.writes)
    if (w.type === "entry") {
      if (w.value.kind === "pi.tool-result") await park("result");
      if (w.value.kind === "pi.assistant" && w.value.model[0].stopReason === "toolUse")
        await park("proposal");
    }
  if (ops.length) await park("intent");
}
class Store extends MemoryStorage {
  get committedSequence() {
    return this.nextSeq - 1;
  }
  async recordSequence(id) {
    return this.positions?.get(id);
  }
  async commit(writes) {
    const p = this.prepareCommit(writes);
    const commit = { seq: p.seq, nextId: this.nextId, writes: p.writes };
    if (publishThroughHost) await publishThroughHost(commit, [], []);
    else {
      const plan = remote.preparePublication(commit);
      await publish({ commit, tools: plan.tools });
      plan.confirmed();
    }
    return p.apply();
  }
}
const store = new Store();
try {
  await producer.start();
  await mailbox.start();
  process.send({ kind: "boot", topic: mailbox.topic });
  if (scope.fencingToken > 1) {
    db = createDatabase({ connectionString: init.url, maxConnections: 2 });
    const host = new PostgresDurableSessionHost({
      database: db,
      sealedRecovery: {
        topic,
        bashOnWorkerLoss: "unknown",
        async hydrate(family, authority, publish) {
          store.positions = await hydrateSealedFamily(family, store);
          publishThroughHost = publish;
          for (const [id, seq] of family.handoff.state.sequences) view.sequences.set(id, seq);
          view.restore({
            scope,
            commit: { writes: family.handoff.state.records.map((r) => r.write) },
            display: [],
          });
          return store;
        },
      },
      restoreReplies: async () => {
        restores++;
        throw Error("Unexpected original mailbox subscription");
      },
    });
    handle = await host.open({
      binding: {
        ...scope,
        conversationId: 1,
        executionReference: createExecutionReference(
          scope.writerId,
          scope.runId,
          scope.fencingToken,
        ),
      },
      stopOnResume: stop,
      publish,
    });
  }
  const fixed = fixedRemoteToolDefinitions().filter((t) => t.name === "bash");
  remote = new DurableRemoteTools({
    definitions: fixed,
    isInterruptedBash: handle?.isInterruptedBash,
    async prepare(i) {
      count++;
      const operationId = randomUUID(),
        controller = new AbortController();
      const reply = {
        ...(await mailbox.replyPosition()),
        toolName: i.toolName,
        toolCallId: i.callId,
        deadlineAt: Date.now() + 120000,
      };
      const waiter = mailbox.wait(operationId, i.callId, i.toolName, controller.signal);
      void waiter.result.catch(() => {});
      return {
        command: {
          toolCallId: "task:" + i.taskId,
          request: {
            operation: "tool.execute",
            toolName: i.toolName,
            toolCallId: i.callId,
            operationId,
            args: i.args,
          },
          replyRecovery: reply,
        },
        completed: () => waiter.executionSettled(),
        dispose: () => controller.abort(),
        result: async () => {
          const result = await waiter.result;
          return { ...result.result, isError: result.isError };
        },
      };
    },
    recover: async () => {
      throw Error("Unexpected Bash reply recovery");
    },
  });
  handle?.preparePublication((c) => remote.preparePublication(c));
  const registry = createRegistry();
  registry.install(
    defineExtension({ name: "fixed-bash", tools: [...remote.definitions], hooks: [remote.hook] }),
  );
  const models = createModels(),
    fake = fauxProvider({ tokensPerSecond: 0 });
  models.setProvider(fake.provider);
  const respond = (input) => {
    modelCalls++;
    const result = input.messages.findLast((m) => m.role === "toolResult");
    if (!result)
      return fauxAssistantMessage(
        fauxToolCall("bash", { command: init.command, timeout: 10 }, { id: "marker-call" }),
        { stopReason: "toolUse" },
      );
    process.send({
      kind: "modelResult",
      unknown: toolResultIsUnknown(result),
      content: result.content,
    });
    return fauxAssistantMessage("finished");
  };
  fake.setResponses(Array(3).fill(respond));
  harness = await Harness.open(
    store,
    { models, registry, settings: { retry: { maxRetries: 0 }, compaction: { enabled: false } } },
    ctx,
  );
  const model = fake.getModel(),
    root =
      scope.fencingToken === 1
        ? await harness.root(ctx, {
            agent: { model: { provider: model.provider, modelId: model.id } },
          })
        : await harness.conversation(1, ctx);
  assert(root);
  let submission;
  if (scope.fencingToken === 1)
    submission = await root.submit(
      { type: "input", requestId: scope.runId, content: "Run the marker once." },
      ctx,
    );
  else if (stop) await root.abort(ctx);
  else harness.resume();
  await root.waitForIdle(ctx);
  if (submission) assert.equal((await submission.wait(ctx)).status, "done");
  await harness.close(ctx);
  harness = undefined;
  await handle?.close();
  handle = undefined;
  process.send({ kind: "finished", count, restores, modelCalls, lastSeq: store.committedSequence });
} catch (e) {
  process.send({ kind: "error", message: e.message, where: e.stack?.split("\n").slice(1, 5) });
  process.exitCode = 1;
} finally {
  remote?.close();
  await mailbox.shutdown();
  await producer.close();
  await db?.destroy();
  process.disconnect();
}
