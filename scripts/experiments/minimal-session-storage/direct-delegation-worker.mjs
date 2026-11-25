// Native Worker endpoints for the isolated delegation handoff acceptance.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import { MemoryStorage } from "@earendil-works/pi-durable";
import { createModels } from "@earendil-works/pi-ai/models";
import {
  fauxAssistantMessage,
  fauxProvider,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import nativeKafka from "@confluentinc/kafka-javascript";
import {
  KafkaToolReplyMailbox,
  KafkaToolReplyPublisher,
  ToolReplyMailbox,
  toolReplyTopic,
} from "@pi-cloud/event-log";
import {
  hydrateSealedFamily,
  PostgresDurableSessionHost,
} from "@pi-cloud/pi-session-postgres/durable";
import { createExecutionReference, infrastructureToolDefinitions } from "@pi-cloud/protocol";
import { withToolInvocation } from "@pi-cloud/sandbox-supervisor";
import { DurableAgentSession } from "../../../packages/sandbox-supervisor/src/durable/agent-session.ts";
import { ControlToolDispatcher } from "../../../packages/control-plane/src/control-tool-dispatcher.ts";
import { delegationPorts } from "./delegation-ports.mjs";
import { WorkerToolRuntime } from "../../../packages/supervisor-host/src/tools/worker-tool-runtime.ts";
import { ControlToolRegistry } from "../../../packages/control-plane/src/control-tool-registry.ts";
import { delegatedControl } from "./delegated-runs.mjs";
import { encodeWorkspaceFact, decodeWorkspaceFact } from "./workspace-operations.mjs";

async function workerDelegationTool(scope, mailbox, nativeConversations, namedSubagents = false) {
  const bindings = await new WorkerToolRuntime({
    namedSubagents,
    catalog: new ControlToolRegistry(),
    maximumDepth: null,
    implementation: () => undefined,
    nativeConversations,
    wait: (op, call, name, signal) => mailbox.wait(op, call, name, signal),
  }).create({
    command: {
      payload: {
        ...scope,
        piSession: { conversationId: 1 },
        sessionKind: "conversation",
        executionReference: createExecutionReference(
          scope.writerId,
          scope.runId,
          scope.fencingToken,
        ),
      },
    },
    environment: () => assert.fail("Unexpected sandbox"),
  });
  return bindings.current().find((b) => b.tool.name === "subagent").tool;
}

export async function bootMailbox() {
  const bootId = randomUUID(),
    brokers = process.env.PI_CLOUD_KAFKA_INTEGRATION_BROKERS?.split(",");
  const replyTopic = toolReplyTopic(bootId);
  if (!brokers) {
    const mailbox = new ToolReplyMailbox();
    return {
      replyTopic,
      mailbox,
      publish: (topic, r) => {
        assert.equal(topic, replyTopic);
        return mailbox.accept(r);
      },
      close: async () => {},
    };
  }
  const mailbox = new KafkaToolReplyMailbox({ brokers, bootId, replicas: 3 }),
    publisher = new KafkaToolReplyPublisher(brokers, "task-return-" + bootId);
  const admin = new nativeKafka.KafkaJS.Kafka({
    kafkaJS: { brokers, logLevel: nativeKafka.KafkaJS.logLevel.NOTHING },
  }).admin();
  const close = async () => {
    try {
      await publisher.close();
      await mailbox.shutdown();
    } finally {
      try {
        await admin.deleteTopics({ topics: [replyTopic] });
      } finally {
        await admin.disconnect();
      }
    }
  };
  await admin.connect();
  try {
    await mailbox.start();
    await publisher.start();
  } catch (e) {
    await close();
    throw e;
  }
  return { replyTopic, mailbox, publish: (topic, r) => publisher.publish(topic, r), close };
}

export async function waitingDelegation(scope, mode, name) {
  const facts = [],
    handles = [];
  const boot = await bootMailbox(),
    { mailbox, replyTopic } = boot;
  const arrived = Promise.withResolvers();
  const models = createModels(),
    provider = fauxProvider({
      provider: "direct-delegation-fixture",
      api: "direct-handoff-test-api",
      tokensPerSecond: 0,
    });
  models.setProvider(provider.provider);
  provider.setResponses([
    fauxAssistantMessage(
      fauxToolCall(
        "subagent",
        { ...(name ? { name } : {}), task: "Check the tests", context: mode, workspace: null },
        { id: "delegate-once" },
      ),
      { stopReason: "toolUse" },
    ),
    fauxAssistantMessage("Child checked the tests"),
    fauxAssistantMessage("Parent finished"),
  ]);
  const definition = infrastructureToolDefinitions(Boolean(name)).find(
    (t) => t.name === "subagent",
  );
  const tool = await workerDelegationTool(
    scope,
    mailbox,
    {
      childAnchor: async (_ref, inherit, lane, callId) =>
        inherit ? String(await runtime.childAnchor(lane, callId)) : null,
    },
    Boolean(name),
  );
  let runtime;
  class Store extends MemoryStorage {
    async commit(writes) {
      const p = this.prepareCommit(writes);
      const commit = { seq: p.seq, nextId: this.nextId, writes: p.writes };
      const ready = runtime?.preparePublication(commit);
      const tools = (ready?.tools ?? []).map(({ taskId, command: c }) => ({
        target: c.target,
        taskId,
        scope,
        conversationId: c.conversationId,
        toolName: c.toolName,
        contextAnchor: c.contextAnchor,
        operationId: c.operationId,
        replyTopic,
      }));
      facts.push({
        kind: "durable_commit",
        scope,
        commit,
        factId: randomUUID(),
        occurredAt: new Date().toISOString(),
        origins: [],
        tools,
        display: [],
        cancellations: [],
      });
      const seq = p.apply();
      ready?.confirmed();
      return seq;
    }
  }
  runtime = await DurableAgentSession.open(new Store(), {
    models,
    remoteTools: [definition],
    settings: { compaction: { enabled: false } },
  });
  try {
    const before = await runtime.harness.root(ctx);
    await before.commit(
      (tx) =>
        tx.appendEntry(before.id, {
          kind: "pi.assistant",
          model: [fauxAssistantMessage("Earlier context")],
        }),
      ctx,
    );
    const root = await runtime.bind({
      runId: scope.runId,
      conversationId: 1,
      model: provider.getModel(),
      agent: {},
      context: {},
      remoteTools: () => ["subagent"],
      tools: [],
      assertCurrent: async () => {},
      openModel: async (r) => ({ model: provider.getModel(), options: r.options, release() {} }),
      prepareRemote: async (v) => {
        const prepare = () =>
          withToolInvocation({ id: "task:" + v.taskId, nativeToolCallId: v.callId }, () =>
            tool.prepareExecution(v.callId, v.args, v.context.abortSignal),
          );
        const live = await prepare();
        handles.push(live, await prepare());
        return {
          ...live,
          result: () => {
            arrived.resolve();
            return live.result();
          },
        };
      },
    });
    if (mode === "branch") {
      const newer = await root.commit(
        (tx) =>
          tx.appendEntry(root.id, {
            kind: "pi.assistant",
            model: [fauxAssistantMessage("Retained test output before delegation")],
          }),
        ctx,
      );
      await root.commit(
        (tx) =>
          tx.appendEntry(root.id, {
            kind: "pi.compaction",
            head: newer.id,
            data: { reason: "manual" },
            model: [
              {
                role: "user",
                content: [{ type: "text", text: "New parent summary" }],
                timestamp: 1,
              },
            ],
          }),
        ctx,
      );
    }
    const submission = await root.submit(
      { type: "input", requestId: scope.runId, content: "Delegate" },
      ctx,
    );
    await Promise.race([
      arrived.promise,
      submission.wait(ctx).then((result) => {
        throw Error("Delegation settled before the expected tool wait: " + result.status);
      }),
    ]);
    const prefix = facts.slice();
    assert.equal(prefix.flatMap((f) => f.tools).length, 1);
    const ids = handles.map((h) => h.command.operationId);
    assert.equal(new Set(ids).size, 2);
    return {
      prefix,
      facts,
      publish: boot.publish,
      ids,
      replyTopic,
      mailbox,
      runtime,
      provider,
      root,
      submission,
      async close() {
        for (const h of handles) h.dispose();
        try {
          await runtime.close();
        } finally {
          await boot.close();
        }
      },
    };
  } catch (error) {
    for (const h of handles) h.dispose();
    try {
      await runtime.close();
    } finally {
      await boot.close();
    }
    throw error;
  }
}

// Exercise the live native child and its waiting parent. The full PG outcome
// projection and controller wait are verified separately in delegated-runs.
export async function executeChild(pool, command, runtime, provider, current) {
  const args = command.arguments;
  const row = await delegatedControl(pool, command.scope, current).start({
    callerLane: command.conversationId,
    taskId: command.taskId,
    task: args.task,
    mode: args.context,
    workspace: args.workspace,
    at: command.contextAnchor,
  });
  assert(current());
  const lane = await runtime.createConversation({
    requestId: row.id,
    ownerTaskId: command.taskId,
    parentConversationId: command.conversationId,
    ...(row.source ? { at: row.source.at } : {}),
  });
  const child = await runtime.bind({
    runId: command.scope.runId,
    conversationId: lane,
    model: provider.getModel(),
    agent: {},
    context: {},
    remoteTools: () => [],
    tools: [],
    assertCurrent: async () => {},
    openModel: async (r) => ({ model: provider.getModel(), options: r.options, release() {} }),
  });
  const input = await child.submit({ type: "input", requestId: row.id, content: row.task }, ctx);
  assert.equal((await input.wait(ctx)).status, "done");
  const transcript = JSON.stringify((await child.context(ctx)).messages);
  const inherited = args.context === "branch";
  assert(!transcript.includes("Earlier context"), "Superseded by compaction");
  assert.equal(transcript.includes("New parent summary"), inherited);
  assert.equal(transcript.includes("Retained test output before delegation"), inherited);
  assert.equal(
    (await child.context(ctx)).messages.some(
      (m) => m.role === "user" && JSON.stringify(m.content).includes('"Delegate"'),
    ),
    inherited,
  );
  assert(
    !(await child.context(ctx)).messages.some(
      (m) =>
        m.role === "assistant" &&
        m.content.some((b) => b.type === "toolCall" && b.name === "subagent"),
    ),
  );
  assert(transcript.includes("Child checked the tests"));
  return { result: { content: [{ type: "text", text: "Child checked the tests: " + row.id }] } };
}

export async function resumeParent(
  pool,
  database,
  topic,
  scope,
  family,
  previousTopic,
  childId,
  interruptAtWait = false,
) {
  let runtime, dispatcher;
  const provider = fauxProvider({
      provider: "direct-delegation-fixture",
      api: "direct-handoff-test-api",
      tokensPerSecond: 0,
    }),
    models = createModels();
  models.setProvider(provider.provider);
  provider.setResponses([
    fauxAssistantMessage("Child checked the tests"),
    fauxAssistantMessage("Parent finished"),
  ]);
  const boot = await bootMailbox(),
    { mailbox, replyTopic } = boot;
  assert.notEqual(previousTopic, replyTopic);
  const calls = [],
    replies = [],
    accepted = [],
    waiting = Promise.withResolvers();
  class Store extends MemoryStorage {
    async commit(writes) {
      const p = this.prepareCommit(writes),
        commit = { seq: p.seq, nextId: this.nextId, writes: p.writes };
      const ready = runtime?.preparePublication(commit);
      const tools = (ready?.tools ?? []).map(({ taskId, command: c }) => {
        assert(c.recovery);
        return {
          scope,
          target: "control",
          taskId,
          toolName: c.toolName,
          operationId: c.operationId,
          conversationId: c.conversationId,
          contextAnchor: c.contextAnchor,
          replyTopic,
          recovery: c.recovery,
        };
      });
      const fact = decodeWorkspaceFact(
        Buffer.from(
          encodeWorkspaceFact({
            kind: "durable_commit",
            scope,
            commit,
            tools,
            origins: [],
            cancellations: [],
            display: [],
            factId: randomUUID(),
            occurredAt: new Date().toISOString(),
          }),
        ),
      ).fact;
      const seq = p.apply();
      ready?.confirmed();
      if (interruptAtWait) accepted.push(fact);
      await dispatcher.consume({ topic, partition: 0, offset: BigInt(seq), fact }, () => true);
      return seq;
    }
  }
  const store = new Store();
  const sessionHost = new PostgresDurableSessionHost({
    database,
    sealedRecovery: {
      topic,
      delegatedRuns: true,
      hydrate: async (rows) => {
        assert.equal(rows.handoff.sealId, family.handoff.sealId);
        const positions = await hydrateSealedFamily(rows, store);
        store.recordSequence = async (id) => positions.get(id);
        return store;
      },
    },
  });
  const { directDelegations } = delegationPorts({
    pool,
    database,
    topic,
    initialReplayComplete: () => true,
  });
  dispatcher = new ControlToolDispatcher({
    database,
    partitions: 1,
    ownsPartition: () => true,
    directDelegations,
    execute: async (c, current) => {
      calls.push(c);
      if (interruptAtWait) {
        waiting.resolve();
        return { waiting: true };
      }
      return executeChild(pool, c, runtime, provider, current);
    },
    publish: async (t, r) => {
      assert.equal(t, replyTopic);
      replies.push(r);
      await boot.publish(t, r);
    },
  });
  try {
    const handle = await sessionHost.open({
      binding: {
        ...scope,
        conversationId: 1,
        executionReference: createExecutionReference(
          scope.writerId,
          scope.runId,
          scope.fencingToken,
        ),
      },
      publish: () =>
        assert.fail("The candidate Store publishes through its matched test transport"),
    });
    const tool = await workerDelegationTool(scope, mailbox, sessionHost);
    runtime = await handle.runtime(() =>
      DurableAgentSession.open(handle.storage, {
        models,
        remoteTools: [infrastructureToolDefinitions().find((d) => d.name === "subagent")],
        settings: { compaction: { enabled: false } },
      }),
    );
    const root = await runtime.bind(
      {
        runId: scope.runId,
        conversationId: 1,
        model: provider.getModel(),
        agent: {},
        context: {},
        remoteTools: () => ["subagent"],
        tools: [],
        assertCurrent: handle.authority.assertCurrent,
        openModel: async (r) => ({ model: provider.getModel(), options: r.options, release() {} }),
        prepareRemote: (v) => {
          assert(v.recovery);
          return withToolInvocation({ id: "task:" + v.taskId, nativeToolCallId: v.callId }, () =>
            tool.prepareRecovery(v.callId, v.args, v.context.abortSignal),
          );
        },
      },
      "reattach",
    );
    runtime.harness.resume();
    if (interruptAtWait) {
      await Promise.race([
        waiting.promise,
        runtime.harness.waitForIdle(ctx).then(() => {
          throw Error("Owner ended before its recovered wait");
        }),
      ]);
      assert.equal(calls.length, 1);
      assert.equal(calls[0].recovery.operationId, childId);
      assert(accepted.some((f) => f.tools.some((t) => t.operationId === calls[0].operationId)));
      return { accepted: accepted.slice(), replyTopic, operationId: calls[0].operationId };
    }
    await runtime.harness.waitForIdle(ctx);
    const messages = (await root.context(ctx)).messages;
    const results = messages.filter((m) => m.role === "toolResult" && m.toolName === "subagent");
    assert.equal(results.length, 1);
    assert(JSON.stringify(results).includes(childId));
    assert(JSON.stringify(messages.at(-1)).includes("Parent finished"));
    assert.equal(calls.length, 1);
    assert.equal(replies.length, 1);
    assert.equal(replies[0].operationId, calls[0].operationId);
    assert.equal(calls[0].recovery.operationId, childId);
  } finally {
    await dispatcher.close();
    try {
      await sessionHost.close();
    } finally {
      await boot.close();
    }
  }
}
