// Match the serving catalogue/preparation, ordered dispatcher, controller and
// boot-scoped return mailbox. This arm exercises the three pure control reads.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import native from "@confluentinc/kafka-javascript";
import { createDatabase } from "@pi-cloud/database";
import { createExecutionReference, durableToolState } from "@pi-cloud/protocol";
import { KafkaToolReplyMailbox, KafkaToolReplyPublisher } from "@pi-cloud/event-log";
import { withToolInvocation } from "@pi-cloud/sandbox-supervisor";
import { WorkerToolRuntime } from "../../../packages/supervisor-host/src/tools/worker-tool-runtime.ts";
import { restoreToolReply } from "../../../packages/supervisor-host/src/tool-reply-result.ts";
import { SubagentController } from "../../../packages/control-plane/src/subagent-controller.ts";
import { ControlToolDispatcher } from "../../../packages/control-plane/src/control-tool-dispatcher.ts";
import { ControlToolRegistry } from "../../../packages/control-plane/src/control-tool-registry.ts";
import { DelegatedHistoryReader } from "../../../packages/control-plane/src/delegated-history-reader.ts";

export async function servingDelegatedQueries(brokers, url, schema) {
  const dsn = new URL(url);
  dsn.searchParams.set("options", "-c search_path=" + schema);
  const database = createDatabase({ connectionString: dsn.href, maxConnections: 3 }),
    admin = new native.KafkaJS.Kafka({
      kafkaJS: { brokers, logLevel: native.KafkaJS.logLevel.NOTHING },
    }).admin();
  const mailbox = new KafkaToolReplyMailbox({ brokers, bootId: randomUUID(), replicas: 3 });
  const publisher = new KafkaToolReplyPublisher(brokers, "query-" + randomUUID());
  const controller = new SubagentController({
    database,
    managementToken: "unused",
    allowInsecureHttp: true,
    delegatedReads: new DelegatedHistoryReader(database),
  });
  const pending = new Map(),
    seen = [];
  const dispatcher = new ControlToolDispatcher({
    database,
    admittedDelegatedReads: true,
    partitions: 1,
    ownsPartition: () => true,
    execute: (c, current) => controller.executeTool(c, current),
    publish: (topic, r) => {
      assert.equal(topic, mailbox.topic);
      return publisher.publish(topic, r);
    },
  });
  const runtime = new WorkerToolRuntime({
    catalog: new ControlToolRegistry(),
    implementation: () => undefined,
    maximumDepth: null,
    nativeConversations: {
      childAnchor: async () => null,
      conversationDepth: async () => 0,
      originalControlIntent: async () => assert.fail(),
    },
    wait: (op, id, name, signal, scope) =>
      restoreToolReply(mailbox.wait(op, id, name, signal), undefined, scope, signal),
  });
  const close = async () => {
    try {
      await dispatcher.close();
      await controller.close();
      await publisher.close();
      await mailbox.shutdown();
    } finally {
      try {
        await admin.deleteTopics({ topics: [mailbox.topic] });
      } finally {
        await admin.disconnect();
        await database.destroy();
      }
    }
  };
  try {
    await admin.connect();
    await mailbox.start();
    await publisher.start();
  } catch (e) {
    await close();
    throw e;
  }
  return {
    dispatcher,
    close,
    seen,
    async prepare(writes, scope) {
      const refs = [];
      for (const write of writes) {
        const task = write.type === "task" && durableToolState(write.value);
        if (!task?.intent || !["list", "status", "wait"].includes(task.intent.arguments.action))
          continue;
        const { id, conversationId, intent } = task;
        const bindings = await runtime.create({
          command: {
            payload: {
              ...scope,
              executionReference: createExecutionReference(
                scope.writerId,
                scope.runId,
                scope.fencingToken,
              ),
              sessionKind: "conversation",
              piSession: { conversationId },
            },
          },
          environment: () => assert.fail(),
        });
        const tool = bindings.current().find((t) => t.tool.name === "subagent_control").tool;
        const prepared = await withToolInvocation(
          { id: "task:" + id, nativeToolCallId: intent.callId },
          () => tool.prepareExecution(intent.callId, intent.arguments),
        );
        const c = prepared.command;
        assert.equal(c.target, "control");
        assert.deepEqual(c.arguments, intent.arguments);
        pending.set(id, prepared);
        refs.push({
          target: "control",
          scope,
          taskId: id,
          conversationId,
          operationId: c.operationId,
          toolName: c.toolName,
          contextAnchor: c.contextAnchor,
          replyTopic: mailbox.topic,
        });
      }
      return refs;
    },
    async result(taskId, args) {
      const handle = pending.get(taskId);
      assert(handle);
      try {
        const result = await handle.result();
        const data = JSON.parse(result.content[0].text);
        seen.push([args.action, data]);
        return result;
      } finally {
        pending.delete(taskId);
        handle.dispose();
      }
    },
  };
}
