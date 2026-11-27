// An independent Session sees scoped directory definitions and its own original reply waits.
import assert from "node:assert/strict";
import {
  createExecutionReference,
  durableControlIntent,
  MCP_TOOL_TIMEOUT_MS,
  TOOL_REPLY_DELIVERY_MARGIN_MS,
} from "@pi-cloud/protocol";
import { withToolInvocation } from "@pi-cloud/sandbox-supervisor";
import { WorkerToolRuntime } from "../../../packages/supervisor-host/src/tools/worker-tool-runtime.ts";
import { restoreToolReply } from "../../../packages/supervisor-host/src/tool-reply-result.ts";
import { ControlToolRegistry } from "../../../packages/control-plane/src/control-tool-registry.ts";

export async function segmentWorkerTools({ job, opened, catalog, mailbox, codec, childAnchor }) {
  const scope = job.scope;
  const originals = new Map(
    (opened.family?.intents ?? []).filter((r) => r.original).map((r) => [r.taskId, r.original]),
  );
  const life = new AbortController();
  if (originals.size)
    await mailbox.restore(
      [...originals.values()].map((o) => ({ operationId: o.command.operationId, reply: o.reply })),
      life.signal,
    );
  const allowed =
    catalog?.forActor({
      tenantId: scope.tenantId,
      userId: job.owner_user_id,
      root: !job.parent_session_id,
    }) ?? new ControlToolRegistry();
  const source = await new WorkerToolRuntime({
    namedSubagents: true,
    independentSessions: true,
    maximumDepth: null,
    catalog: allowed,
    implementation: (name) => catalog?.implementation(name),
    nativeConversations: {
      childAnchor,
      conversationDepth: async () => (job.parent_session_id ? 1 : 0),
      recoveredControlToolNames: () => [...originals.values()].map((o) => o.reply.toolName),
    },
    wait: (id, call, name, signal, imageScope, recovery) =>
      restoreToolReply(
        recovery ? mailbox.resumeWait(id, recovery, signal) : mailbox.wait(id, call, name, signal),
        codec,
        imageScope,
        signal,
      ),
  }).create({
    command: {
      payload: {
        ...scope,
        executionReference: createExecutionReference(
          scope.writerId,
          scope.runId,
          scope.fencingToken,
        ),
        piSession: { conversationId: 1 },
        sessionKind: job.parent_session_id ? "subagent" : "conversation",
        toolCapabilities: job.workspace_grant?.tools ?? [],
      },
    },
    environment: () => {
      throw Error("Control tool requested a sandbox");
    },
  });
  const current = () =>
    source
      .current()
      .filter(
        ({ tool }) =>
          tool.name === "send_message" ||
          (tool.name === "subagent" && !job.parent_session_id) ||
          (tool.name.startsWith("mcp_") && !job.parent_session_id),
      );
  return {
    definitions: { current: () => current().map((b) => b.tool), subscribe: source.subscribe },
    offered: () =>
      current()
        .filter((b) => b.offered && !b.tool.recoveryOnly)
        .map((b) => b.tool.name),
    close: () => life.abort(),
    prepare(v) {
      const tool = current().find((b) => b.tool.name === v.toolName)?.tool;
      if (!tool) throw Error("Tool is no longer available");
      return withToolInvocation({ id: "task:" + v.taskId, nativeToolCallId: v.callId }, () =>
        v.recovery
          ? tool.prepareRecovery(v.callId, v.args, v.context.abortSignal, originals.get(v.taskId))
          : tool.prepareExecution(v.callId, v.args, v.context.abortSignal),
      );
    },
  };
}

export async function segmentControlReferences(commit, effects, scope, replies) {
  const tools = effects?.tools ?? [];
  const position = tools.some(({ command: c }) => c.implementation?.catalogScope)
    ? await replies.mailbox.replyPosition()
    : undefined;
  return tools.map(({ taskId, command: c }) => {
    const ref = {
      target: c.target,
      taskId,
      scope,
      conversationId: c.conversationId,
      toolName: c.toolName,
      contextAnchor: c.contextAnchor,
      operationId: c.operationId,
      replyTopic: replies.replyTopic,
    };
    if (!c.implementation?.catalogScope) return ref;
    const intent = durableControlIntent(commit, ref);
    assert.equal(intent.callId, c.nativeCallId);
    assert.deepEqual(intent.arguments, c.arguments);
    assert.equal(position.topic, replies.replyTopic);
    return {
      ...ref,
      implementation: c.implementation,
      replyRecovery: {
        ...position,
        deadlineAt: Date.parse(c.occurredAt) + MCP_TOOL_TIMEOUT_MS + TOOL_REPLY_DELIVERY_MARGIN_MS,
        toolCallId: c.nativeCallId,
        toolName: c.toolName,
      },
    };
  });
}
