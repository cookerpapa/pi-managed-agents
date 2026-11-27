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
import { controlToolCatalog } from "@pi-cloud/runtime-core/control-tool-catalog";

const empty = controlToolCatalog([]);
const emptyDirectory = { read: () => empty, subscribe: () => () => {} };

export async function segmentWorkerTools({
  job,
  opened,
  catalog,
  mailbox,
  codec,
  childAnchor,
  versions,
}) {
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
    }) ?? emptyDirectory;
  let platform = versions ? catalog.snapshot : undefined;
  if (versions && opened.resumed) {
    const saved = opened.family.handoff.state.records.find(
      (r) => r.write.record?.kind === "pi-cloud.run" && r.write.record.scope.conversationId === 1,
    )?.write.content.value?.toolHostVersion;
    // Never substitute a newer handler for an interrupted request.
    platform = saved ? await versions.versionCatalog(saved.generation, saved.releaseId) : undefined;
  }
  let priorLive, priorPlatform, merged;
  const directory = versions
    ? {
        subscribe: allowed.subscribe,
        read() {
          const live = allowed.read();
          if (priorLive !== live || priorPlatform !== platform) {
            merged = controlToolCatalog([
              ...live.tools.filter((t) => t.name.startsWith("mcp_")),
              ...(platform?.catalog.tools ?? []).filter((t) => t.environment.kind === "none"),
            ]);
            priorLive = live;
            priorPlatform = platform;
          }
          return merged;
        },
      }
    : allowed;
  const source = await new WorkerToolRuntime({
    namedSubagents: true,
    independentSessions: true,
    maximumDepth: null,
    catalog: directory,
    implementation: (name) =>
      versions && !name.startsWith("mcp_") && platform
        ? {
            releaseId: platform.releaseId,
            generation: platform.generation,
            replay: platform.replayable.includes(name) ? "safe" : "never",
          }
        : catalog?.implementation(name),
    nativeConversations: {
      childAnchor,
      originalControlIntent: (_ref, lane, task) => {
        if (!opened.originalControlIntent) throw Error("Missing sealed original control intent");
        return opened.originalControlIntent(lane, task);
      },
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
          (tool.name.startsWith("mcp_") && !job.parent_session_id) ||
          (!!versions &&
            !!platform?.catalog.tools.some(
              (t) => t.name === tool.name && t.environment.kind === "none",
            )),
      );
  return {
    version: () =>
      platform ? { releaseId: platform.releaseId, generation: platform.generation } : null,
    advance() {
      const latest = catalog?.snapshot;
      if (!versions || latest === platform) return false;
      platform = latest;
      return true;
    },
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
      ...(c.recovery ? { recovery: c.recovery } : {}),
    };
    if (!c.implementation?.catalogScope)
      return c.implementation ? { ...ref, implementation: c.implementation } : ref;
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
