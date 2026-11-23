import { fixtureToolReply } from "../tool-api.ts";
import { nativeTestDisplay } from "./native-test-display.ts";
import { MemoryStorage, type StorageWrite, type Seq, type Id } from "@earendil-works/pi-durable";
import type { Context } from "@earendil-works/chord";
import {
  FakeModelServer,
  FAKE_MODEL_API_KEY,
  type FakeModelScenario,
} from "@pi-cloud/fake-model-server";
import type {
  CandidateToolCommand,
  DurableSessionCommit,
  EventPublishMessage,
  NativeToolEnd,
} from "@pi-cloud/protocol";
import type {
  DurableSessionHandle,
  ManagedNativeRuntime,
} from "@pi-cloud/pi-session-postgres/durable";
import {
  PiCloudTurnRunner,
  type PiCloudTurnRunnerOptions,
} from "../../src/pi-cloud-turn-runner.ts";
import { createTrustedRemoteAgentTools } from "../../src/trusted-remote-tools.ts";
import {
  createCloudExecutionContext,
  createCloudTurnContext,
  createCloudStepContext,
} from "../../src/cloud-context.ts";
import { command as defaultCommand } from "./durable-command.ts";

export async function fixture(
  scenario: FakeModelScenario = "text",
  hold = false,
  override: Partial<PiCloudTurnRunnerOptions> = {},
  bridge?: {
    command: typeof defaultCommand;
    bash: string;
    publish: (
      commit: DurableSessionCommit,
      effects: readonly { taskId: number; command: CandidateToolCommand }[],
      receive: (operation: string, value: Promise<NativeToolEnd>) => void,
    ) => Promise<void>;
  },
) {
  const command = bridge?.command ?? defaultCommand;
  const server = new FakeModelServer({
    defaultScenario: scenario,
    ...(bridge ? { holdCommand: bridge.bash } : {}),
  });
  await server.start();
  const authority = new AbortController(),
    cancelled = new AbortController(),
    events: EventPublishMessage[] = [];
  const commands: CandidateToolCommand[] = [],
    cancellations: string[] = [],
    started = Promise.withResolvers<void>();
  const replies = new Map<
    string,
    {
      request: CandidateToolCommand["request"];
      pending: ReturnType<typeof Promise.withResolvers<NativeToolEnd>>;
    }
  >();
  let preparation: Parameters<DurableSessionHandle["preparePublication"]>[0] | undefined;
  let runtime: Promise<ManagedNativeRuntime> | undefined,
    released = !hold,
    acquired = 0,
    returned = 0;
  const recordSeq = new Map<number, number>();
  const settle = () => {
    for (const value of replies.values())
      value.pending.resolve({
        type: "tool_execution_end",
        toolCallId:
          value.request.operation === "tool.execute" ? value.request.toolCallId : "workflow",
        toolName: value.request.toolName,
        isError: false,
        result: { content: [{ type: "text", text: "complete" }] },
      });
  };
  const textDisplay = nativeTestDisplay((event) =>
    events.push({
      protocolVersion: 1,
      type: "event.publish",
      messageId: event.eventId,
      sentAt: event.occurredAt,
      payload: { executionReference: command.payload.executionReference, event },
    }),
  );
  const storage = new (class extends MemoryStorage {
    allocationHead = 1;
    committed = 0;
    override async mintId<I extends Id<string>>(): Promise<I> {
      const id = await super.mintId<I>();
      this.allocationHead = Math.max(this.allocationHead, id + 1);
      return id;
    }
    override prepareCommit(writes: readonly StorageWrite[], seq?: Seq) {
      const prepared = super.prepareCommit(writes, seq);
      return {
        ...prepared,
        apply: () => {
          const position = prepared.apply();
          this.committed = Math.max(this.committed, position);
          for (const w of prepared.writes)
            this.allocationHead = Math.max(
              this.allocationHead,
              1 + ("value" in w ? w.value.id : "record" in w ? w.record.id : w.id),
            );
          return position;
        },
      };
    }
    override async commit(writes: readonly StorageWrite[], context: Context): Promise<Seq> {
      return textDisplay.serializePublication(async () => {
        const prepared = this.prepareCommit(writes);
        const confirmed = await textDisplay.compile(this, prepared.writes, prepared.seq);
        const effects = preparation?.({
          seq: prepared.seq,
          nextId: 10000,
          writes: prepared.writes,
        } as unknown as DurableSessionCommit);
        for (const tool of effects?.tools ?? []) {
          if (!("request" in tool.command)) throw new Error("Expected the Cube fixture");
          commands.push(tool.command);
          started.resolve();
        }
        for (const cancel of effects?.cancellations ?? []) cancellations.push(cancel.operationId);
        if (bridge)
          await bridge.publish(
            {
              seq: prepared.seq,
              nextId: this.allocationHead,
              writes: prepared.writes,
            } as unknown as DurableSessionCommit,
            (effects?.tools ?? []).map(({ taskId, command }) => {
              if (!("request" in command)) throw new Error("Expected guest operation");
              return { taskId, command };
            }),
            (id, promise) => {
              const wait = replies.get(id);
              if (!wait) throw new Error("Missing original waiter");
              void promise.then(wait.pending.resolve, wait.pending.reject);
            },
          );
        const seq = prepared.apply();
        effects?.confirmed();
        confirmed();
        for (const write of writes)
          recordSeq.set(
            "value" in write ? write.value.id : "record" in write ? write.record.id : write.id,
            seq,
          );
        if (released) settle();
        context.abortSignal?.throwIfAborted();
        return seq;
      });
    }
  })();
  const scope = command.payload,
    turn = createCloudTurnContext(command),
    execution = createCloudExecutionContext({
      command,
      turnContextSha256: turn.sha256,
      runtimeIdentity: { supervisorId: "test", bootId: "test", sandboxId: "test" },
    });
  const handle: DurableSessionHandle = {
    remoteToolIntent: async () => {
      throw new Error("This fixture has no pending remote operation");
    },
    ...textDisplay,
    storage,
    binding: {
      tenantId: scope.tenantId,
      sessionId: scope.sessionId,
      piSessionId: scope.piSession.id,
      conversationId: 1,
      runId: scope.runId,
      turnId: scope.turnId,
      writerId: scope.piSession.writerId,
      executionReference: scope.executionReference,
    },
    recordSequence: async (id) => recordSeq.get(id),
    committedSequence: () => storage.committed,
    authority: {
      signal: authority.signal,
      assertCurrent: async () => {
        authority.signal.throwIfAborted();
      },
      close: async () => {},
    },
    interrupt: (reason) => authority.abort(reason),
    runtime: (factory) => (runtime ??= factory()) as ReturnType<typeof factory>,
    preparePublication: (value) => {
      preparation = value;
    },
    close: async () => {},
  };
  const runner = new PiCloudTurnRunner({
    openSession: async () => handle,
    resolveModelRuntime: () => ({
      provider: scope.model.provider,
      modelId: scope.model.modelId,
      api: "openai-completions",
      apiKey: FAKE_MODEL_API_KEY,
      baseUrl: server.baseUrl,
      inputModalities: ["text", "image"],
      contextWindow: 131072,
      maxTokens: 1024,
    }),
    modelEventIdleTimeoutMs: 30000,
    acquireModelPermit: async (signal) => {
      signal?.throwIfAborted();
      acquired++;
      return () => {
        returned++;
      };
    },
    sandboxContinuity: {
      continuityId: "not-activated",
      continuity: "cold_restore",
      environmentSha256: turn.environmentSha256,
      workspaceBindingSha256: turn.workspaceBindingSha256,
      toolPolicySha256: turn.toolPolicySha256,
    },
    createAgentTools: ({ stepWorldState, captureSamplingStep }) =>
      createTrustedRemoteAgentTools({
        activationId: "20000000-0000-4000-8000-000000000001",
        executionReference: scope.executionReference,
        turnContextSha256: turn.sha256,
        executionContextSha256: execution.sha256,
        workingDirectory: scope.workingDirectory,
        supportsImages: true,
        remainingToolCalls: null,
        maximumToolOutputBytes: 65536,
        captureStepContext: (tools, purpose) =>
          captureSamplingStep(
            async (sequence) => {
              const value = await stepWorldState.capture();
              return {
                step: createCloudStepContext({
                  sequence,
                  turnContextSha256: turn.sha256,
                  executionContextSha256: execution.sha256,
                  allowedTools: turn.context.tools.names,
                  activeTools: tools,
                  worldState: value.worldState,
                }),
                modelMessages: value.modelMessages,
              };
            },
            { publishEvent: purpose !== "context_maintenance" },
          ),
        waitForToolReply: (request, { receipt: signal }) => {
          const pending = Promise.withResolvers<NativeToolEnd>();
          replies.set(request.operationId, { request, pending });
          signal.addEventListener("abort", () => pending.reject(signal.reason), { once: true });
          return fixtureToolReply(pending.promise);
        },
      }),
    ...override,
  });
  return {
    runner,
    storage,
    events,
    commands,
    cancellations,
    started,
    server,
    counts: () => ({ acquired, returned }),
    missingDirectory: () => {
      for (const { request, pending } of replies.values()) {
        if (request.operation !== "tool.execute") continue;
        pending.resolve({
          type: "tool_execution_end",
          toolCallId: request.toolCallId,
          toolName: request.toolName,
          isError: true,
          result: {
            content: [{ type: "text", text: "Working directory missing; Bash did not start." }],
            details: {
              code: "workspace_directory_missing",
              execution: "not_started",
              directory: "/home/user/missing",
            },
          },
        });
      }
    },
    release: () => {
      released = true;
      settle();
    },
    stop: () =>
      cancelled.abort({
        kind: "pi-cloud.turn-cancellation",
        reason: "user_request",
        gracePeriodMs: 1000,
      }),
    revoke: () => authority.abort(new Error("lease revoked")),
    run: (input = command) =>
      runner.run(
        input,
        (value) => {
          events.push(value);
        },
        cancelled.signal,
      ),
    close: async () => {
      cancelled.abort();
      released = true;
      settle();
      await (await runtime)?.close();
      await server.stop();
    },
  };
}
