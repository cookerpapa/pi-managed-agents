// Process entry point used by the isolated Worker acceptance. Claims use no
// parent ID or target hint. SIGKILL stops direct renewal without releasing PG.
import pg from "pg";
import { randomUUID } from "node:crypto";
import { KafkaWorkerProducer } from "@pi-cloud/event-log";
import { bootMailbox, workerDelegationTool } from "./direct-delegation-worker.mjs";
import { DurableAgentSession } from "../../../packages/sandbox-supervisor/src/durable/agent-session.ts";
import { withToolInvocation } from "@pi-cloud/sandbox-supervisor";
import { infrastructureToolDefinitions } from "@pi-cloud/protocol";
import { encodeWorkspaceFact } from "./workspace-operations.mjs";
import { turnBinding } from "./turn-binding.mjs";
import { openSegmentNative } from "./independent-session-runtime.mjs";
import { SegmentInputs } from "./segment-inputs.mjs";
import { connectSessionInputs } from "./segment-input-worker.mjs";
import { ForkStorage } from "./fork-storage.mjs";
import { SegmentWorker } from "./independent-session-leases.mjs";
import { MemoryStorage, Harness, createRegistry } from "@earendil-works/pi-durable";
import { getCurrentSystemPrompt } from "@earendil-works/pi-ai/utils/transcript";
import { createModels } from "@earendil-works/pi-ai/models";
import {
  fauxProvider,
  fauxAssistantMessage,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import { once } from "node:events";

const [cfg] = await once(process, "message");
const storage = new ForkStorage(new pg.Pool({ connectionString: cfg.url, max: 4 }));
const worker = new SegmentWorker({ database: storage.db, ...cfg });
let job, native, root, history, systemPrompt, finishGate, inputPump, runtime, replies;
const producer = cfg.brokers
  ? new KafkaWorkerProducer({ brokers: cfg.brokers, topic: cfg.topic, clientId: cfg.bootId })
  : undefined;
await producer?.start();
async function publish(fact) {
  await producer.append(0, {
    factId: fact.factId,
    sessionId: fact.scope.piSessionId,
    value: encodeWorkspaceFact(fact),
  });
}
// In directed mode the test driver observes the next acquired lease. It
// cannot cause the queue lookup. Manual claims remain available for lock races.
const stopServing = new AbortController();
let acquired = Promise.withResolvers(),
  releaseScheduled;
const serving = cfg.scheduler
  ? worker.serve({
      scheduler: cfg.scheduler,
      signal: stopServing.signal,
      execute: async (claimed) => {
        releaseScheduled = Promise.withResolvers();
        acquired.resolve(claimed);
        await releaseScheduled.promise;
      },
    })
  : undefined;
serving?.catch((e) => process.send?.({ kind: "error", message: e.stack }));
process.on("message", async (msg) => {
  try {
    if (msg.op === "claim") {
      job = await worker.claimNext();
      process.send({ kind: "claim", scope: job?.scope ?? null, flow: job?.flow_id ?? null });
    } else if (msg.op === "claimed") {
      job = await acquired.promise;
      process.send({ kind: "claimed", scope: job.scope });
    } else if (msg.op === "execute") {
      const scope = job.scope;
      let sequence = 0;
      replies ??= await bootMailbox();
      const opened = await openSegmentNative(storage, job, cfg.topic, async (commit) => {
        sequence = commit.seq;
        const effects = runtime?.preparePublication(commit);
        await publish({
          kind: "durable_commit",
          scope,
          commit,
          factId: randomUUID(),
          occurredAt: new Date().toISOString(),
          origins: [],
          display: [],
          tools: (effects?.tools ?? []).map(({ taskId, command: c }) => ({
            target: c.target,
            taskId,
            scope,
            conversationId: c.conversationId,
            toolName: c.toolName,
            contextAnchor: c.contextAnchor,
            operationId: c.operationId,
            replyTopic: replies.replyTopic,
          })),
          cancellations: [],
        });
        effects?.confirmed();
      });
      const models = createModels(),
        fake = fauxProvider({
          provider: job.model_snapshot.provider,
          api: job.model_snapshot.runtime?.api ?? "independent-child-fixture",
          models: [{ id: job.model_snapshot.modelId, reasoning: true }],
          tokensPerSecond: 0,
        });
      models.setProvider(fake.provider);
      fake.setResponses([
        async (context, _options, _state, requested) => {
          systemPrompt = getCurrentSystemPrompt(context.messages);
          history = context.messages.map((m) =>
            typeof m.content === "string" ? m.content : m.content.map((v) => v.text ?? "").join(""),
          );
          if (msg.hold) {
            process.send({
              kind: "in-model",
              sequence,
              history,
              modelId: requested.id,
              provider: requested.provider,
              cwd: job.workspace_grant?.cwd,
            });
            if (!msg.abortable) await new Promise(() => {});
            if (!_options.signal.aborted) await once(_options.signal, "abort");
            return fauxAssistantMessage("", { stopReason: "aborted" });
          }
          if (msg.report)
            return fauxAssistantMessage(
              fauxToolCall(
                "send_message",
                { to: "parent", priority: "normal", text: msg.report },
                { id: "report-once" },
              ),
              { stopReason: "toolUse" },
            );
          return fauxAssistantMessage("independent answer: " + job.initial_input);
        },
        (context) => {
          if (msg.captureInputs)
            history = context.messages
              .filter((m) => m.role === "user")
              .map((m) =>
                typeof m.content === "string"
                  ? m.content
                  : m.content
                      .filter((p) => p.type === "text")
                      .map((p) => p.text)
                      .join(""),
              );
          return fauxAssistantMessage("Report delivered");
        },
      ]);
      const tools = infrastructureToolDefinitions(true, true).filter(
        (def) => def.name === "send_message" || (!job.parent_session_id && def.name === "subagent"),
      );
      runtime = await DurableAgentSession.open(opened.storage, {
        models,
        remoteTools: tools,
        settings: { compaction: { enabled: false } },
      });
      native = runtime.harness;
      const binding = turnBinding(job, models);
      const bindings = new Map();
      for (const definition of tools)
        bindings.set(
          definition.name,
          await workerDelegationTool(
            scope,
            replies.mailbox,
            {
              childAnchor: async (_ref, inherit, lane, callId) =>
                inherit ? String(await runtime.childAnchor(lane, callId)) : null,
            },
            true,
            definition.name,
            true,
          ),
        );
      const initialRequestId = job.first_message ?? job.id;
      const attached =
        opened.resumed && (await opened.storage.submissionByRequest(1, initialRequestId, ctx));
      root = await runtime.bind(
        {
          runId: job.id,
          initialRequestId,
          afterToolRound: async (task, context) => {
            await inputPump.postTools(task, context);
            if (msg.holdAfterPlacement) {
              process.send({ kind: "at-boundary" });
              await new Promise(() => {});
            }
          },
          conversationId: 1,
          model: binding.model,
          agent: binding.agent,
          context: binding.context,
          turnEnvironment: binding.turnEnvironment,
          workspaceAvailable: binding.workspaceAvailable,
          autoCompactTokenLimit: binding.autoCompactTokenLimit,
          remoteTools: () => [...bindings.keys()],
          tools: [],
          assertCurrent: () => job.authority.assertCurrent(),
          openModel: async (r, nativeRequest) => {
            inputPump?.assertHealthy();
            return {
              model: binding.model,
              options: binding.requestOptions(r.options, nativeRequest?.purpose),
              release() {},
            };
          },
          prepareRemote: async (v) =>
            withToolInvocation({ id: "task:" + v.taskId, nativeToolCallId: v.callId }, () =>
              bindings.get(v.toolName).prepareExecution(v.callId, v.args, v.context.abortSignal),
            ),
        },
        attached ? "reattach" : "initialize",
      );
      inputPump = connectSessionInputs({
        job,
        inputs: new SegmentInputs(storage.db),
        conversation: root,
        harness: native,
        storage: opened.storage,
      });
      if (!attached && job.has_inputs) await inputPump.start();
      if (opened.resumed) native.resume();
      else if (!job.has_inputs && !job.stopSignal.aborted)
        await root.submit({ type: "input", content: job.initial_input, requestId: job.id }, ctx);
      const receipt = await inputPump.finish();
      await inputPump?.close();
      if (msg.holdBeforeFinish) {
        finishGate = Promise.withResolvers();
        process.send({ kind: "native-idle" });
        await finishGate.promise;
        finishGate = undefined;
      }
      job.authority.assertActive();
      const terminalFact = {
        kind: "work_finished",
        scope,
        factId: randomUUID(),
        agentId: "root",
        baseSequence: 0,
        nativeSequence: sequence,
        terminal:
          !receipt || (receipt.status === "unanswered" && receipt.reason === "aborted")
            ? { type: "turn.cancelled", payload: { reason: "user_request", forced: false } }
            : receipt.status === "done"
              ? { type: "turn.completed", payload: { stopReason: "stop" } }
              : {
                  type: "turn.failed",
                  payload: {
                    code: receipt.reason ?? "input_unanswered",
                    message: "The native input was not answered",
                    retryable: false,
                  },
                },
        occurredAt: new Date().toISOString(),
      };
      opened.rememberCompletion?.(terminalFact);
      await publish(terminalFact);
      process.send({
        kind: "finished",
        terminalState: terminalFact.terminal.type,
        historyLoad: opened.load,
        history,
        systemPrompt,
        resumed: opened.resumed,
        modelId: binding.model.id,
        provider: binding.model.provider,
        cwd: binding.agent.cwd,
        turnId: job.turnId,
      });
    } else if (msg.op === "finish-turn" && finishGate) {
      finishGate.resolve();
    } else if (msg.op === "answer") {
      job.authority.assertActive();
      const memory = new MemoryStorage();
      await storage.hydrate(job.tenant_id, job.logical_id, memory);
      const models = createModels(),
        fake = fauxProvider({
          provider: job.model_snapshot.provider,
          api: job.model_snapshot.runtime?.api ?? "independent-child-fixture",
          models: [{ id: job.model_snapshot.modelId, reasoning: true }],
          tokensPerSecond: 0,
        });
      models.setProvider(fake.provider);
      fake.setResponses([
        (context) => {
          history = context.messages.map((m) =>
            typeof m.content === "string" ? m.content : m.content.map((c) => c.text ?? "").join(""),
          );
          return fauxAssistantMessage("checked: " + job.initial_input);
        },
      ]);
      native = await Harness.open(memory, { models, registry: createRegistry() }, ctx);
      const model = fake.getModel(),
        agent = { model: { provider: model.provider, modelId: model.id } };
      root = await native.root(ctx, { agent });
      await root.configure(agent, ctx);
      const mail = await new SegmentInputs(storage.db).selectAt(job.scope, "start");
      const input = await root.submit(
        { type: "input", content: mail.body, requestId: mail.id },
        ctx,
      );
      await root.waitForIdle(ctx);
      await input.wait(ctx);
      const final = (await root.context(ctx)).messages.filter((m) => m.role === "assistant").at(-1);
      job.authority.assertActive();
      process.send({
        kind: "answer",
        placement: { id: mail.id, native_id: input.id, state: "placed" },
        history,
        text: final.content.map((p) => p.text ?? "").join(""),
      });
    } else if (msg.op === "release") {
      if (runtime) await runtime.close();
      else await native?.close(ctx);
      runtime = undefined;
      await replies?.close();
      replies = undefined;
      native = undefined;
      if (releaseScheduled) {
        acquired = Promise.withResolvers();
        releaseScheduled.resolve();
        releaseScheduled = undefined;
      } else await job?.release();
      job = undefined;
      process.send({ kind: "released" });
    } else if (msg.op === "exit") {
      await inputPump?.close();
      if (runtime) await runtime.close();
      else await native?.close(ctx);
      await replies?.close();
      stopServing.abort();
      releaseScheduled?.resolve();
      await serving;
      await worker.close();
      await producer?.close();
      await storage.db.destroy();
      process.disconnect();
    }
  } catch (e) {
    process.send({ kind: "error", message: e.stack });
  }
});
process.send({ kind: "ready" });
