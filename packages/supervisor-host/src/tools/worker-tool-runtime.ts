import { isDeepStrictEqual } from "node:util";
import { randomUUID } from "node:crypto";
import { Type } from "typebox";
import {
  infrastructureToolDefinitions,
  skillActivationNeedsWorkspace,
  selectRunSkill,
  skillAvailability,
  effectiveToolCapabilities,
  hasConversationWorkspace,
  isConversationToolOffered,
  toolExecutionTarget,
  isControlToolName,
  parseSubagentTaskId,
  type OriginalControlToolIntent,
  type OriginalRemoteToolIntent,
  type ToolReplyRecovery,
  type ExecuteTurnCommandMessage,
  type ToolResultPayload,
  type ToolReplyWait,
  type ControlToolCatalogSource,
  type InfrastructureToolDefinition,
  type ExecutionTargetGrant,
  type ToolEnvironmentRequirement,
  type CandidateToolEffect,
  ToolExecutionFailure,
  ToolResultAttachmentError,
  executorToolBudget,
  MCP_TOOL_TIMEOUT_MS,
  TOOL_REPLY_DELIVERY_MARGIN_MS,
} from "@pi-cloud/protocol";
import {
  toolInvocationId,
  type TrustedAgentTool,
  type TrustedAgentToolSet,
  type PreparedAgentTool,
} from "@pi-cloud/sandbox-supervisor";

/** Definitions, frozen invocation metadata and reply transport only. Trusted
 * Tool business logic runs in the corresponding execution-side handler. */
export class WorkerToolRuntime {
  constructor(
    readonly options: {
      implementation(name: string): import("@pi-cloud/protocol").ToolImplementation | undefined;
      catalog: ControlToolCatalogSource;
      scopedCatalog?(scope: ExecuteTurnCommandMessage["payload"]): ControlToolCatalogSource;
      nativeConversations: {
        recoveredControlToolNames?(reference: string, conversationId: number): readonly string[];
        childAnchor(
          reference: string,
          inherit: boolean,
          conversationId: number,
        ): Promise<string | null>;
        conversationDepth(reference: string, conversationId: number): Promise<number>;
        originalControlIntent(
          reference: string,
          conversationId: number,
          taskId: number,
          signal?: AbortSignal,
        ): Promise<OriginalControlToolIntent>;
      };
      maximumDepth: number | null;
      namedSubagents?: boolean;
      subagentWaitMs?: number;
      prepareTarget?(
        command: ExecuteTurnCommandMessage,
        target: ExecutionTargetGrant,
        signal?: AbortSignal,
      ): Promise<string>;
      wait(
        operationId: string,
        callId: string,
        toolName: string,
        signal: AbortSignal,
        scope: { tenantId: string; runId: string },
        recovery?: ToolReplyRecovery,
      ): ToolReplyWait<ToolResultPayload>;
    },
  ) {}

  async create(context: {
    command: ExecuteTurnCommandMessage;
    environment(): import("@pi-cloud/protocol").ToolEnvironmentRequest;
    onExecutionUnavailable?(failure: { code: string; message: string }): void;
  }): Promise<TrustedAgentToolSet> {
    const scope = context.command.payload;
    const catalog = this.options.scopedCatalog?.(scope) ?? this.options.catalog;
    const workspaceAvailable = hasConversationWorkspace(scope);
    const targetBindings = new Map<string, Promise<string>>();
    const prepareTarget = (target: ExecutionTargetGrant, signal?: AbortSignal) => {
      let pending = targetBindings.get(target.targetId);
      if (!pending) {
        pending = this.options.prepareTarget
          ? this.options.prepareTarget(context.command, target, signal)
          : Promise.reject(new Error("Device transport is unavailable"));
        targetBindings.set(target.targetId, pending);
        void pending.catch(() => {
          if (targetBindings.get(target.targetId) === pending)
            targetBindings.delete(target.targetId);
        });
      }
      return pending;
    };
    const depth =
      scope.sessionKind === "subagent"
        ? await this.options.nativeConversations.conversationDepth(
            scope.executionReference,
            scope.piSession.conversationId,
          )
        : 0;
    const capabilities = { ...scope, depth, maximumDepth: this.options.maximumDepth };
    type Definition = Omit<InfrastructureToolDefinition, "name"> & {
      name: string;
      environment?: ToolEnvironmentRequirement;
    };
    const createBinding = (
      definition: Definition,
      ordinary: boolean,
      recoveryOnly = false,
    ): TrustedAgentTool => {
      const environment = definition.environment ?? { kind: "none" as const };
      const mcp = ordinary && definition.name.startsWith("mcp_");
      const target = toolExecutionTarget(definition, capabilities);
      const recoverOriginalReply = async (
        nativeCallId: string,
        args: unknown,
        signal?: AbortSignal,
        original?: OriginalRemoteToolIntent,
      ) => {
        signal?.throwIfAborted();
        const call = original?.command;
        if (
          !original ||
          (call?.kind !== "executor_tool" && call?.kind !== "control_tool") ||
          (mcp
            ? call.kind !== "control_tool" || !call.implementation?.catalogScope
            : call.kind !== "executor_tool") ||
          original.taskId !== parseSubagentTaskId(toolInvocationId(nativeCallId)) ||
          original.conversationId !== scope.piSession.conversationId ||
          call.scope.tenantId !== scope.tenantId ||
          call.scope.runId !== scope.runId ||
          call.scope.sessionId !== scope.sessionId ||
          call.nativeCallId !== nativeCallId ||
          call.toolName !== definition.name ||
          original.reply.toolCallId !== nativeCallId ||
          original.reply.toolName !== definition.name ||
          !isDeepStrictEqual(call.arguments, args)
        )
          throw new Error("Tool recovery has no matching original operation");
        const lifetime = new AbortController();
        const activeSignal = signal ? AbortSignal.any([signal, lifetime.signal]) : lifetime.signal;
        const pending = this.options.wait(
          call.operationId,
          nativeCallId,
          definition.name,
          activeSignal,
          { tenantId: scope.tenantId, runId: scope.runId },
          original.reply,
        );
        void pending.result.catch(() => {});
        return {
          command:
            call.kind === "control_tool"
              ? {
                  target: "control" as const,
                  executionReference: scope.executionReference,
                  operationId: call.operationId,
                  toolCallId: toolInvocationId(nativeCallId),
                  nativeCallId,
                  conversationId: scope.piSession.conversationId,
                  toolName: definition.name,
                  arguments: call.arguments,
                  contextAnchor: call.contextAnchor,
                  implementation: call.implementation,
                  replyRecovery: original.reply,
                  occurredAt: new Date(
                    original.reply.deadlineAt - MCP_TOOL_TIMEOUT_MS - TOOL_REPLY_DELIVERY_MARGIN_MS,
                  ).toISOString(),
                }
              : {
                  target: "executor" as const,
                  executionReference: scope.executionReference,
                  operationId: call.operationId,
                  bindingId: call.bindingId,
                  toolCallId: toolInvocationId(nativeCallId),
                  nativeCallId,
                  conversationId: scope.piSession.conversationId,
                  toolName: definition.name,
                  arguments: call.arguments,
                  occurredAt: call.occurredAt,
                  originalWriterId: call.scope.writerId,
                  replyRecovery: original.reply,
                },
          completed: pending.executionSettled,
          dispose: () => lifetime.abort(new Error("Recovered Tool wait closed")),
          result: async () => {
            try {
              const reply = await pending.result;
              return { ...reply.result, isError: reply.isError };
            } catch (error) {
              if (activeSignal.aborted) throw activeSignal.reason;
              if (error instanceof ToolResultAttachmentError) throw error;
              const failure =
                error instanceof ToolExecutionFailure
                  ? error
                  : new ToolExecutionFailure({
                      code: "tool_result_unknown",
                      effect: "unknown",
                      message:
                        "The original Tool result is unavailable. Inspect its state before repeating the action.",
                    });
              if (mcp)
                return {
                  isError: true,
                  content: [
                    { type: "text" as const, text: `tool_result_unknown: ${failure.message}` },
                  ],
                };
              if (failure.effect === "unknown") context.onExecutionUnavailable?.(failure);
              throw failure;
            }
          },
        };
      };
      const prepare = async (
        nativeCallId: string,
        raw: unknown,
        signal?: AbortSignal,
        recovery = false,
      ) => {
        if (recoveryOnly)
          throw new Error("tool_unavailable: this registration only restores an original reply");
        if (definition.availability === "root" && scope.sessionKind === "subagent")
          throw new Error("tool_unavailable: This Tool belongs to the root conversation");
        if (!workspaceAvailable && environment.kind === "workspace")
          throw new Error("workspace_unavailable: this child has no Linux workspace");
        const args = raw as Record<string, unknown>,
          invocationId = toolInvocationId(nativeCallId);
        if (ordinary) {
          const current = catalog.read().tools.find((tool) => tool.name === definition.name);
          if (
            !current ||
            !isDeepStrictEqual(current.parameters, definition.parameters) ||
            !isDeepStrictEqual(current.environment, environment) ||
            current.availability !== definition.availability
          )
            throw new Error(
              `tool_unavailable: ${definition.name} was withdrawn or changed before admission`,
            );
        } else if (!isControlToolName(definition.name))
          throw new Error("Invalid control Tool definition");
        const original = recovery
          ? await this.options.nativeConversations.originalControlIntent(
              scope.executionReference,
              scope.piSession.conversationId,
              parseSubagentTaskId(invocationId),
              signal,
            )
          : undefined;
        if (
          original &&
          (definition.replay !== "safe" ||
            original.taskId !== parseSubagentTaskId(invocationId) ||
            original.conversationId !== scope.piSession.conversationId ||
            original.toolName !== definition.name ||
            original.nativeCallId !== nativeCallId ||
            !isDeepStrictEqual(original.arguments, args))
        )
          throw new Error("Recovered invocation differs from its original intent");
        let needsWorkspace = environment.kind === "workspace";
        if (environment.kind === "skill" && args.action === "read") {
          const skill = selectRunSkill(scope.skillCatalog ?? [], args);
          if (
            skillAvailability(
              skill.requirements,
              effectiveToolCapabilities(capabilities, catalog.read().tools),
            ).state === "unsupported"
          )
            throw new Error(
              "skill_unavailable: this conversation does not provide the required capabilities",
            );
          needsWorkspace = skillActivationNeedsWorkspace(skill, args);
          if (needsWorkspace && !workspaceAvailable)
            throw new Error("skill_unavailable: this Skill needs workspace files");
        }
        const operationId = randomUUID();
        const implementation = ordinary ? this.options.implementation(definition.name) : undefined;
        if (ordinary && !implementation)
          throw new Error("tool_unavailable: implementation catalog is not ready");
        const logicalEnvironment = needsWorkspace ? context.environment() : undefined;
        const bindingId = logicalEnvironment
          ? logicalEnvironment.requestId
          : environment.kind === "target"
            ? target
              ? await prepareTarget(target, signal)
              : undefined
            : undefined;
        if (environment.kind === "target" && !bindingId)
          throw new Error("tool_unavailable: This conversation has no matching execution target");
        const anchor = original
          ? original.contextAnchor
          : ordinary
            ? null
            : await this.options.nativeConversations.childAnchor(
                scope.executionReference,
                definition.inheritedContext?.(args) ?? false,
                scope.piSession.conversationId,
              );
        const waiting = new AbortController();
        const namedWait = this.options.namedSubagents && isControlToolName(definition.name);
        const deadline =
          environment.kind === "target"
            ? AbortSignal.timeout(executorToolBudget(definition.name, args).replyMs)
            : mcp
              ? AbortSignal.timeout(MCP_TOOL_TIMEOUT_MS + TOOL_REPLY_DELIVERY_MARGIN_MS)
              : namedWait
                ? AbortSignal.timeout(
                    this.options.subagentWaitMs ??
                      (definition.name === "contact_supervisor" ? 10 : 5) * 60_000,
                  )
                : undefined;
        const activeSignal = AbortSignal.any([
          waiting.signal,
          ...(signal ? [signal] : []),
          ...(deadline ? [deadline] : []),
        ]);
        const pending = this.options.wait(
          operationId,
          nativeCallId,
          definition.name,
          activeSignal,
          { tenantId: scope.tenantId, runId: scope.runId },
        );
        void pending.result.catch(() => {});
        const command: CandidateToolEffect =
          environment.kind === "target"
            ? {
                target: "executor",
                executionReference: scope.executionReference,
                operationId,
                bindingId: bindingId!,
                toolCallId: invocationId,
                nativeCallId,
                conversationId: scope.piSession.conversationId,
                toolName: definition.name,
                arguments: args,
                occurredAt: new Date().toISOString(),
              }
            : {
                target: "control" as const,
                executionReference: scope.executionReference,
                operationId,
                toolCallId: invocationId,
                nativeCallId,
                conversationId: scope.piSession.conversationId,
                toolName: definition.name,
                arguments: args,
                contextAnchor: anchor === null ? null : Number(anchor),
                ...(original
                  ? { recovery: { sequence: original.sequence, operationId: original.operationId } }
                  : {}),
                ...(bindingId ? { bindingId } : {}),
                ...(logicalEnvironment ? { environment: logicalEnvironment } : {}),
                ...(implementation ? { implementation } : {}),
                occurredAt: new Date().toISOString(),
              };
        return {
          command,
          completed: pending.executionSettled,
          dispose: () => waiting.abort(new Error("Control Tool wait ended")),
          result: async () => {
            try {
              const reply = await pending.result;
              return { ...reply.result, isError: reply.isError };
            } catch (error) {
              if (signal?.aborted) throw signal.reason;
              if (deadline?.aborted && error === deadline.reason) {
                if (namedWait) {
                  const name = String(args.name ?? args.target ?? "");
                  return {
                    isError: true,
                    content: [
                      {
                        type: "text" as const,
                        text:
                          definition.name === "subagent"
                            ? `还没收到 ${name} 的回复。先查一下它的状态：subagent_control(${JSON.stringify({ action: "status", target: name })})。`
                            : `这次操作还没收到回复。请稍后再查看${name ? " " + name : ""}。`,
                      },
                    ],
                    details: { code: "subagent_reply_timeout", name },
                  };
                }
                const failure = new ToolExecutionFailure({
                  code: "tool_reply_timeout",
                  effect: "unknown",
                  message:
                    "No final Tool reply arrived. The action may have happened; inspect its state before repeating it.",
                });
                if (mcp)
                  return {
                    isError: true,
                    content: [
                      { type: "text" as const, text: `tool_result_unknown: ${failure.message}` },
                    ],
                  };
                context.onExecutionUnavailable?.(failure);
                throw failure;
              }
              if (
                environment.kind === "target" &&
                error instanceof ToolExecutionFailure &&
                error.effect === "unknown"
              )
                context.onExecutionUnavailable?.(error);
              throw error;
            }
          },
        };
      };
      return {
        executionPlane: ordinary ? ("platform" as const) : ("orchestration" as const),
        offered: !recoveryOnly && isConversationToolOffered(definition, capabilities),
        tool: {
          ...definition,
          ...(recoveryOnly ? { recoveryOnly: true as const } : {}),
          ...(environment.kind === "target" || mcp
            ? { replay: "safe" as const, recoverySource: "reply" as const }
            : {}),
          async execute() {
            throw new Error("Tool requires its committed native invocation");
          },
          prepareExecution: (callId: string, args: unknown, signal?: AbortSignal) =>
            prepare(callId, args, signal),
          ...(environment.kind === "target" || mcp
            ? { prepareRecovery: recoverOriginalReply }
            : definition.replay === "safe"
              ? {
                  prepareRecovery: (callId: string, args: unknown, signal?: AbortSignal) =>
                    prepare(callId, args, signal, true),
                }
              : {}),
        } as PreparedAgentTool,
      };
    };
    const native = infrastructureToolDefinitions(this.options.namedSubagents).map((definition) =>
      createBinding(definition, false),
    );
    let revision: string | undefined;
    let current: readonly TrustedAgentTool[] = native;
    return {
      current: () => {
        const snapshot = catalog.read();
        const restored =
          this.options.nativeConversations.recoveredControlToolNames?.(
            scope.executionReference,
            scope.piSession.conversationId,
          ) ?? [];
        const nextRevision = snapshot.revision + ":" + restored.join(",");
        if (nextRevision !== revision) {
          current = [
            ...native,
            ...snapshot.tools.map((definition) =>
              createBinding(
                {
                  name: definition.name,
                  description: definition.description,
                  parameters: definition.parameters as import("typebox").TSchema,
                  replay: "unsafe",
                  environment: definition.environment,
                  ...(definition.availability ? { availability: definition.availability } : {}),
                },
                true,
              ),
            ),
            ...restored
              .filter((name) => !snapshot.tools.some((t) => t.name === name))
              .map((name) =>
                createBinding(
                  {
                    name,
                    description: "Restore the original in-flight reply only.",
                    parameters: Type.Object({}, { additionalProperties: true }),
                    replay: "safe",
                    environment: { kind: "none" },
                    availability: "root",
                  },
                  true,
                  true,
                ),
              ),
          ];
          revision = nextRevision;
        }
        return current;
      },
      subscribe: (listener) => catalog.subscribe(listener),
    };
  }
}
