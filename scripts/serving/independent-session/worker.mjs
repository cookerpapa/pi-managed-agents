import { randomUUID, randomBytes, createHash } from "node:crypto";
import { resolve } from "node:path";
import { FamilyModelPermits } from "../../../packages/supervisor-host/src/family-model-permits.ts";
import { MODEL_RETRY_COUNT } from "../../../packages/sandbox-supervisor/src/durable/model-wait-policy.ts";
import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import { KafkaWorkerProducer } from "@pi-cloud/event-log";
import {
  createExecutionReference,
  modelSamplingHeaders,
  modelTransportKey,
  MODEL_PURPOSE_HEADER,
  parseSupervisorToControlMessage,
} from "@pi-cloud/protocol";
import { kafkaProducerLane } from "@pi-cloud/runtime-core/kafka-accepted-fact";
import { TenantModelGateway } from "../../../packages/supervisor-host/src/model-gateway.ts";
import { CloudModelRegistry } from "../../../packages/sandbox-supervisor/src/durable/model-registry.ts";
import { DurableAgentSession } from "../../../packages/sandbox-supervisor/src/durable/agent-session.ts";
import { attributedModelFetch } from "../../../packages/sandbox-supervisor/src/durable/model-accounting.ts";
import {
  nativeProviderContext,
  replayProviderHostedTranscripts,
} from "../../../packages/sandbox-supervisor/src/provider-hosted-transcript.ts";
import {
  mergeProviderHostedTools,
  mergeProviderServiceTier,
} from "../../../packages/sandbox-supervisor/src/provider-hosted-tools.ts";
import { SupervisorBootLedger } from "../../../packages/supervisor-host/src/boot-ledger.ts";
import { SupervisorManagementServer } from "../../../packages/supervisor-host/src/management-server.ts";
import { SupervisorProvisioningClient } from "../../../packages/supervisor-host/src/provisioning-client.ts";
import { ReconnectingSupervisorWebSocketClient } from "../../../packages/sandbox-supervisor/src/reconnecting-supervisor-websocket-client.ts";
import { defaultSessionClaimBatch } from "../../experiments/minimal-session-storage/segment-wake.mjs";
import { SegmentWorker } from "../../experiments/minimal-session-storage/independent-session-leases.mjs";
import { SegmentHistoryReader } from "../../experiments/minimal-session-storage/segment-history-reader.mjs";
import { SegmentMailReader } from "../../experiments/minimal-session-storage/segment-mail-reader.mjs";
import { openSegmentNative } from "../../experiments/minimal-session-storage/open-segment-native.mjs";
import { connectSessionInputs } from "../../experiments/minimal-session-storage/segment-input-worker.mjs";
import { FamilyPresentation } from "../../experiments/minimal-session-storage/family-presentation.mjs";
import { encodeWorkspaceFact } from "../../experiments/minimal-session-storage/workspace-operations.mjs";
import { turnBinding } from "../../experiments/minimal-session-storage/turn-binding.mjs";
import { installation, openStore, secret, integer, required } from "./config.mjs";

export async function startIndependentWorker() {
  const config = await installation(),
    pool = await openStore(config),
    store = new SegmentHistoryReader(pool);
  const identity = {
    supervisorId: required("PI_CLOUD_SUPERVISOR_ID"),
    sandboxId: randomUUID(),
    bootId: randomUUID(),
  };
  const capacity = integer("PI_CLOUD_SEGMENT_CAPACITY", 8),
    stop = new AbortController(),
    stopClaims = new AbortController(),
    active = new Map();
  const permits = new FamilyModelPermits({
    maximum: integer("PI_CLOUD_SEGMENT_MODEL_CAPACITY", capacity),
    perFamily: 1,
  });
  const failed = Promise.withResolvers();
  void failed.promise.catch(() => {});
  const runtimeUrl = required("PI_CLOUD_SESSION_RUNTIME_URL"),
    managementUrl = required("PI_CLOUD_SEGMENT_MANAGEMENT_URL");
  const ledger = new SupervisorBootLedger({
    rootDirectory: required("PI_CLOUD_SEGMENT_BOOT_DIR"),
    supervisorId: identity.supervisorId,
  });
  const membership = await secret("PI_CLOUD_SUPERVISOR_MANAGEMENT_TOKEN");
  await ledger.beginBoot(identity);
  const worker = new SegmentWorker({
    database: store.db,
    workerId: identity.sandboxId,
    bootId: identity.bootId,
    capacity,
    leaseDurationMs: integer("PI_CLOUD_SEGMENT_LEASE_MS", 30000, 1000, 3600000),
    historyCache: { diskDirectory: resolve(required("PI_CLOUD_SEGMENT_BOOT_DIR"), "history") },
  });
  const publishLog = new KafkaWorkerProducer({
    brokers: config.brokers,
    topic: config.topic,
    clientId: "pma-worker-" + identity.bootId,
  });
  const port = integer("PI_CLOUD_MODEL_GATEWAY_PORT", 4102);
  const modelGateway = new TenantModelGateway({
    host: "127.0.0.1",
    port,
    advertisedBaseUrl: "http://127.0.0.1:" + port,
    providerGatewayBaseUrl: required("PI_CLOUD_PROVIDER_GATEWAY_URL"),
    providerGatewayApiKey: await secret("PI_CLOUD_PROVIDER_GATEWAY_API_KEY"),
    assertExecutionCurrent: (ref) => {
      const job = active.get(ref);
      if (!job) throw Error("No active Session model grant");
      job.authority.assertActive();
    },
  });
  const inputs = new SegmentMailReader(store.db);
  async function execute(job) {
    const scope = job.scope,
      executionReference = createExecutionReference(
        scope.writerId,
        scope.runId,
        scope.fencingToken,
      );
    const command = {
      payload: {
        ...scope,
        executionReference,
        piSession: { id: scope.sessionId, conversationId: 1 },
        model: job.model_snapshot,
      },
    };
    const userStop = new AbortController();
    const cancel = () =>
      userStop.abort({ kind: "pi-cloud.turn-cancellation", reason: "user_request" });
    if (job.stopSignal.aborted) cancel();
    else job.stopSignal.addEventListener("abort", cancel, { once: true });
    const signal = AbortSignal.any([stop.signal, job.authority.signal, userStop.signal]);
    const lane = kafkaProducerLane(scope.sessionId, config.partitions),
      presentation = new FamilyPresentation();
    const emit = async (fact) => {
      stop.signal.throwIfAborted();
      job.authority.assertActive();
      await publishLog.append(lane, {
        factId: fact.factId,
        sessionId: scope.sessionId,
        value: encodeWorkspaceFact({ ...fact, scope }),
      });
    };
    const modelTasks = new Map();
    let runtime,
      pump,
      lease,
      sequence = 0;
    active.set(executionReference, job);
    try {
      lease = modelGateway.issue(command, signal);
      const modelConfig = { ...lease.runtime, apiKey: lease.runtime.capability, transport: "sse" };
      const registry = new CloudModelRegistry(),
        model = registry.configure(modelConfig),
        binding = turnBinding(job, registry.models);
      presentation.sequences.set(scope.sessionId, Number(job.display_base));
      presentation.bind(1, scope);
      const opened = await openSegmentNative(store, job, config.topic, async (commit) => {
        const display = presentation.prepare(commit);
        await emit({
          kind: "durable_commit",
          factId: randomUUID(),
          occurredAt: new Date().toISOString(),
          commit,
          origins: [],
          tools: [],
          cancellations: [],
          display: display.display,
        });
        display.confirmed();
        sequence = commit.seq;
        for (const w of commit.writes)
          if (w.type === "task" && ["pi.generation", "pi.compaction"].includes(w.value.kind)) {
            if (w.value.state.status === "terminal") modelTasks.delete(w.value.id);
            else modelTasks.set(w.value.id, commit.seq);
          }
      });
      if (opened.family) {
        sequence = opened.family.handoff.state.seq;
        for (const rec of opened.family.handoff.state.records) {
          const w = rec.write;
          if (w.type === "task") modelTasks.set(w.value.id, rec.seq);
          if (w.type === "document.create" && w.record.kind === "pi.live")
            presentation.live[w.record.id] = { conversation: 1, value: w.content.value };
        }
      }
      runtime = await DurableAgentSession.open(opened.storage, {
        models: registry.models,
        remoteTools: [],
        settings: {
          stream: { timeoutMs: 0, maxRetries: 0 },
          retry: { maxRetries: MODEL_RETRY_COUNT },
        },
      });
      const initial = job.first_message ?? job.id;
      const attached =
        opened.resumed && (await opened.storage.submissionByRequest(1, initial, ctx));
      const root = await runtime.bind(
        {
          ...binding,
          runId: job.id,
          initialRequestId: initial,
          conversationId: 1,
          tools: [],
          remoteTools: () => [],
          assertCurrent: () => job.authority.assertCurrent(),
          prepareRemote: async () => {
            throw Error("Unregistered tool in text-only Session");
          },
          afterToolRound: (task, context) => pump.postTools(task, context),
          openModel: async (request, native) => {
            if (!native) throw Error("Missing native model task");
            await job.authority.assertCurrent();
            signal.throwIfAborted();
            const task = await runtime.harness.getTask(native.taskId, ctx),
              attempt = task?.state.checkpoint?.attempt ?? 1;
            const purpose = native.purpose,
              stepSequence = modelTasks.get(native.taskId);
            if (!stepSequence) throw Error("Sampling task checkpoint is unavailable");
            const digest = createHash("sha256")
              .update(
                JSON.stringify([
                  job.id,
                  job.model_snapshot,
                  job.workspace_grant,
                  job.environment_snapshot,
                  native.taskId,
                  stepSequence,
                  purpose,
                ]),
              )
              .digest("hex");
            const cacheKey = await modelTransportKey({
              tenantId: scope.tenantId,
              sessionId: scope.sessionId,
              conversationId: 1,
              credentialBindingId: job.model_snapshot.credentialBindingId,
              credentialBindingVersion: job.model_snapshot.credentialBindingVersion,
              purpose,
            });
            const source = nativeProviderContext(request.input, model);
            const release = await permits.acquire(scope.tenantId + ":" + job.family_id, signal);
            try {
              job.authority.assertActive();
              signal.throwIfAborted();
            } catch (e) {
              release();
              throw e;
            }
            return {
              model,
              input: source,
              eventIdleTimeoutMs: modelConfig.eventIdleTimeoutMs,
              options: {
                ...request.options,
                apiKey: modelConfig.apiKey,
                sessionId: cacheKey,
                transport: "sse",
                signal: AbortSignal.any([
                  signal,
                  ...(request.options.signal ? [request.options.signal] : []),
                ]),
                fetch: attributedModelFetch(command, {
                  taskId: native.taskId,
                  provider: model.provider,
                  model: model.id,
                  purpose,
                }),
                headers: {
                  ...request.options.headers,
                  ...modelSamplingHeaders({
                    stepSequence,
                    stepSha256: digest,
                    samplingAttempt: attempt,
                  }),
                  [MODEL_PURPOSE_HEADER]: purpose,
                },
                onPayload: (payload) =>
                  mergeProviderServiceTier(
                    mergeProviderHostedTools(
                      replayProviderHostedTranscripts(payload, request.input, model),
                      purpose === "agent" ? (modelConfig.hostedTools ?? []) : [],
                    ),
                    modelConfig.serviceTier,
                  ),
              },
              release,
            };
          },
        },
        attached ? "reattach" : "initialize",
      );
      pump = connectSessionInputs({
        job,
        inputs,
        conversation: root,
        harness: runtime.harness,
        storage: opened.storage,
      });
      if (!attached && job.has_inputs) await pump.start();
      if (opened.resumed) runtime.harness.resume();
      const receipt = await pump.finish();
      const ok = receipt?.status === "done";
      const terminal = job.stopSignal.aborted
        ? { type: "turn.cancelled", payload: { reason: "user_request", forced: false } }
        : ok
          ? { type: "turn.completed", payload: { stopReason: "stop" } }
          : {
              type: "turn.failed",
              payload: {
                code: "model_request_failed",
                message: "The model request did not complete",
                retryable: true,
              },
            };
      const fact = {
        kind: "work_finished",
        scope,
        agentId: "root",
        factId: randomUUID(),
        occurredAt: new Date().toISOString(),
        nativeSequence: sequence,
        baseSequence: presentation.sequences.get(scope.sessionId) ?? 0,
        terminal,
      };
      await emit(fact);
      if (ok) opened.rememberCompletion?.(fact);
    } finally {
      job.stopSignal.removeEventListener("abort", cancel);
      pump?.close();
      lease?.release();
      active.delete(executionReference);
      await runtime?.close();
    }
  }
  let ws,
    loop,
    ready = false;
  const management = new SupervisorManagementServer({
    host: process.env.HOST ?? "127.0.0.1",
    port: Number(new URL(managementUrl).port),
    managementToken: membership,
    identity,
    bootLedger: ledger,
    readiness: () => ready,
    stopCurrentBoot: async () => {
      ready = false;
      stopClaims.abort();
      stop.abort();
      await loop;
      await worker.close();
    },
    assignmentInventory: {
      listAssignments: async () => [],
      terminateAndConfirmAbsent: async () => {
        throw Error("No sandbox assignments in this role");
      },
      confirmAbsent: async () => {
        throw Error("No sandbox assignments in this role");
      },
    },
  });
  async function close() {
    ready = false;
    stopClaims.abort();
    await loop?.catch(() => {});
    stop.abort();
    await worker.close();
    await ws?.stop();
    permits.close();
    await modelGateway.close();
    await publishLog.close();
    await management.close();
    await store.db.destroy();
  }
  try {
    await management.listen();
    await modelGateway.start();
    await publishLog.start();
    const bootSecret = randomBytes(32).toString("base64url"),
      credentialId = randomUUID();
    await new SupervisorProvisioningClient({
      baseUrl: runtimeUrl,
      enrollmentToken: await secret("PI_CLOUD_SUPERVISOR_ENROLLMENT_TOKEN"),
      allowInsecureHttp: true,
    }).provision({
      protocolVersion: 1,
      type: "supervisor.boot.provision",
      requestId: randomUUID(),
      ...identity,
      credentialId,
      credentialSha256: createHash("sha256").update(bootSecret).digest("hex"),
      maxConcurrentSessions: capacity,
      managementBaseUrl: managementUrl,
    });
    ws = new ReconnectingSupervisorWebSocketClient({
      url: new URL("/internal/v1/supervisor", runtimeUrl).href.replace(/^http/, "ws"),
      authorizationHeader: "Bearer " + credentialId + "." + bootSecret,
      registration: { ...identity, maxConcurrentSessions: capacity },
      runtime: {
        createHeartbeat: (ids, acceptingAssignments = false) =>
          parseSupervisorToControlMessage({
            protocolVersion: 1,
            type: "supervisor.heartbeat",
            messageId: randomUUID(),
            sentAt: new Date().toISOString(),
            payload: {
              ...ids,
              acceptingAssignments,
              maxConcurrentSessions: capacity,
              families: [],
            },
          }),
        applyHeartbeatAcknowledgement() {},
        revokeAllAssignments() {
          stop.abort();
          stopClaims.abort();
        },
      },
    });
    ws.setAcceptingAssignments(false);
    await ws.start();
    loop = worker.serve({
      scheduler: {
        url: required("PI_CLOUD_SEGMENT_WAKE_URL"),
        token: membership,
        batch: integer("PI_CLOUD_SEGMENT_CLAIM_BATCH", defaultSessionClaimBatch, 1, 32),
      },
      signal: stopClaims.signal,
      execute,
      onError: (e) => {
        ready = false;
        stop.abort();
        stopClaims.abort();
        failed.reject(e);
      },
    });
    loop.catch((e) => {
      ready = false;
      stop.abort();
      failed.reject(e);
    });
    ready = true;
    return { close, failed: failed.promise };
  } catch (e) {
    await close();
    throw e;
  }
}
