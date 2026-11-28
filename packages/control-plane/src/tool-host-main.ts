import { McpToolHostRuntime, McpInvocationRefused } from "./mcp/tool-host-runtime.ts";
import { McpCredentialVault } from "./mcp/credential-vault.ts";
import Fastify from "fastify";
import { randomUUID, timingSafeEqual, createHash } from "node:crypto";
import { pathToFileURL } from "node:url";
import { createDatabase } from "@pi-cloud/database";
import { KafkaToolCatalogPublisher } from "@pi-cloud/event-log";
import { operationalLog, startServiceObservability } from "@pi-cloud/observability";
import {
  MAX_TOOL_RESPONSE_BYTES,
  parseControlToolDispatch,
  parseToolResultPayload,
  type ToolCatalogPublication,
} from "@pi-cloud/protocol";
import type { AcceptedControlToolCommand } from "@pi-cloud/runtime-core/accepted-fact";
import { configuredImageAttachments, encodeSessionImages } from "@pi-cloud/session-attachments";
import { EnvHttpProxyAgent, fetch as proxyFetch } from "undici";
import { loadToolHostConfig } from "./production-config.ts";
import { SkillMarketService } from "./skill-market-service.ts";
import { SessionScheduleService } from "./session-schedule-service.ts";
import { createOrdinaryToolRegistry } from "./ordinary-tools/registry.ts";
import { ImageGenerationClient } from "./ordinary-tools/image-generation-client.ts";
import { executionToolCatalog } from "./execution-tool-catalog.ts";
import type { ControlToolRegistry } from "./control-tool-registry.ts";
import { ToolCatalogAuthority } from "./tool-catalog-authority.ts";
import { ToolHostResourceClient } from "./tool-host-resource-client.ts";
import { ControlPlaneQuiescence } from "./control-plane-quiescence.ts";
import {
  TOOL_HOST_ACTIVATE_PATH,
  TOOL_HOST_DRAIN_PATH,
  TOOL_HOST_EXECUTE_PATH,
  TOOL_HOST_MANIFEST_PATH,
} from "./tool-host-client.ts";

/** Trusted platform handlers only. No Pi Harness, Session Kafka consumer,
 * resource owner, native writer, or user extension is constructed here. */
export async function startToolHost(
  options: {
    mcpTurnAdmittedByProjector?: boolean;
    turnReadUrl?(url: string): string;
    ordinaryTools?: ControlToolRegistry;
    versionRoute?: (serverAddress: string) => string;
    graceMs?: number;
  } = {},
) {
  const config = await loadToolHostConfig();
  const executableId = process.env.PI_CLOUD_TOOL_RELEASE_ID;
  if (!executableId || !/^[a-zA-Z0-9_.:-]{1,256}$/.test(executableId))
    throw new Error("Tool host requires an immutable code revision");
  const cleanup: Array<() => unknown | Promise<unknown>> = [];
  const active = new Set<AbortController>();
  const instanceId = randomUUID();
  const quiescence = new ControlPlaneQuiescence(instanceId, () => ({
    active: active.size,
    unsettled: 0,
  }));
  let accepting = true;
  let selected: ToolCatalogPublication | undefined;
  let closing: Promise<void> | undefined;
  let retryTimer: NodeJS.Timeout | undefined;
  let mcp: McpToolHostRuntime | undefined;
  const app = Fastify({ logger: false, bodyLimit: MAX_TOOL_RESPONSE_BYTES });
  const close = () =>
    (closing ??= (async () => {
      accepting = false;
      quiescence.close();
      clearInterval(retryTimer);
      // Fastify drains existing requests; no unrelated Session writer is drained.
      await app.close();
      for (const stop of cleanup.reverse()) await stop();
    })());
  try {
    const observability = await startServiceObservability({
      serviceName: "pi-cloud-tool-host",
      defaultMetricsPort: 9476,
    });
    cleanup.push(() => observability.close());
    const database = createDatabase({ connectionString: config.databaseUrl, maxConnections: 8 });
    cleanup.push(() => database.destroy());
    const turnReads = options.turnReadUrl
      ? createDatabase({
          connectionString: options.turnReadUrl(config.databaseUrl),
          maxConnections: 4,
        })
      : undefined;
    if (turnReads) cleanup.push(() => turnReads.destroy());
    const images = await configuredImageAttachments(database);
    if (images) cleanup.push(() => images.blobs.close());
    const egress = new EnvHttpProxyAgent();
    cleanup.push(() => egress.close());
    const data = new ToolHostResourceClient({
      baseUrl: process.env.PI_CLOUD_SESSION_EXECUTION_BASE_URL ?? "http://session-runtime:4300",
      token: config.sandboxControlToken,
    });
    let catalog: ReturnType<typeof executionToolCatalog>;
    const skills = new SkillMarketService({
      database,
      ...(turnReads ? { turnContexts: turnReads } : {}),
      ...(images ? { blobs: images.blobs } : {}),
      maximumDepth: config.subagentTreePolicy.maximumDepth,
      catalog: () => catalog.read().tools,
      fetch: ((url: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) =>
        proxyFetch(url as Parameters<typeof proxyFetch>[0], {
          ...(init as Parameters<typeof proxyFetch>[1]),
          dispatcher: egress,
        })) as unknown as typeof fetch,
    });
    await skills.initialize();
    const generation = config.imageGeneration
      ? new ImageGenerationClient(config.imageGeneration)
      : undefined;
    if (generation) cleanup.push(() => generation.close());
    const tools =
      options.ordinaryTools ??
      createOrdinaryToolRegistry({
        database,
        skills,
        schedules: new SessionScheduleService({
          database: turnReads ?? database,
          environmentImageRevision: config.environmentImageRevision,
        }),
        ...(config.enabledControlTools ? { enabled: config.enabledControlTools } : {}),
        ...(images ? { images } : {}),
        ...(generation ? { imageGeneration: generation } : {}),
        imageIo: (call, action, signal) => data.imageIo(call, action, signal),
        exportSkill: (call, path, signal) => data.exportPackage(call, path, signal),
        prepareSkill: (call, content, digest, signal) =>
          data.preparePackage(call, content, digest, signal),
        refreshServices: (call) => data.refreshServices(call),
      });
    catalog = executionToolCatalog(tools);
    // Availability configuration is part of the selected implementation too.
    // Restarting the same binary with a withdrawn tool is an explicit new release.
    const releaseId = createHash("sha256")
      .update(`${executableId}\0${catalog.read().revision}`)
      .digest("hex");
    const versioned = !!(options.versionRoute || process.env.PI_CLOUD_TOOL_HOST_VERSION_URL);
    const graceMs =
      options.graceMs ?? Number(process.env.PI_CLOUD_TOOL_HOST_GRACE_MS ?? 7 * 24 * 60 * 60 * 1000);
    const route = () => ({
      endpoint:
        options.versionRoute?.(app.listeningOrigin) ?? process.env.PI_CLOUD_TOOL_HOST_VERSION_URL!,
      graceMs,
    });
    const authority = new ToolCatalogAuthority(database);
    const publisher = new KafkaToolCatalogPublisher({
      brokers: config.kafkaBrokers,
      replicas: config.kafkaReplicas,
      instanceId,
      authorize: (snapshot) => authority.current(snapshot),
    });
    cleanup.push(() => publisher.close());
    const replayable = () =>
      tools
        .read()
        .tools.filter((tool) => tools.replayable(tool.name))
        .map((tool) => tool.name)
        .sort();
    const publishScoped = async (snapshot: ToolCatalogPublication) => {
      await publisher.publish(snapshot);
      await authority.confirmPublished(snapshot);
    };
    mcp = new McpToolHostRuntime({
      ...(options.mcpTurnAdmittedByProjector ? { turnAdmittedByProjector: true } : {}),
      database,
      databaseUrl: config.databaseUrl,
      releaseId,
      ...(config.mcpCredentialKey
        ? { vault: new McpCredentialVault(config.mcpCredentialKey) }
        : {}),
      fetch: ((url: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) =>
        proxyFetch(url as Parameters<typeof proxyFetch>[0], {
          ...(init as Parameters<typeof proxyFetch>[1]),
          dispatcher: egress,
        })) as unknown as typeof fetch,
      publish: publishScoped,
      report: (event) => operationalLog({ service: "pi-cloud-tool-host", level: "warn", event }),
    });
    cleanup.push(() => mcp!.close());
    let publicationPending = false;
    const publish = async (snapshot: ToolCatalogPublication) => {
      publicationPending = true;
      const publishSelection = async () => {
        await mcp!.adoptRelease();
        await publishScoped(snapshot);
      };
      try {
        await publishSelection();
      } catch (error) {
        // A reactivated host may still own a publisher fenced by the intervening release.
        // Republishing the same selected immutable snapshot is idempotent.
        if (!versioned || !(await authority.current(snapshot))) throw error;
        await publishSelection();
      }
      selected = snapshot;
      publicationPending = false;
    };
    const credentials = Buffer.from(`Bearer ${config.supervisorManagementToken}`);
    app.addHook("onRequest", async (request, reply) => {
      if (request.url.startsWith("/health/")) return;
      const supplied = Buffer.from(request.headers.authorization ?? "");
      if (supplied.length !== credentials.length || !timingSafeEqual(supplied, credentials))
        return reply.code(401).send({ error: "unauthorized" });
    });
    app.get("/health/live", async () => ({ live: true }));
    app.get("/health/ready", async (_request, reply) => {
      const retention = versioned
        ? await authority.retention(releaseId, route().endpoint)
        : undefined;
      const ready =
        accepting &&
        quiescence.status().phase === "open" &&
        (retention ? !!(retention.current || retention.acceptUntil) : !!selected);
      return reply.code(ready ? 200 : 503).send({ ready, active: active.size });
    });
    app.get(TOOL_HOST_MANIFEST_PATH, async () => ({
      releaseId,
      executableId,
      catalog: catalog.read(),
      replayable: replayable(),
      active: active.size,
      accepting,
      selected: await authority.read(),
      ...(versioned ? { retention: await authority.retention(releaseId, route().endpoint) } : {}),
    }));
    app.get(TOOL_HOST_DRAIN_PATH, async () => quiescence.status());
    app.post<{ Body: { id: string; action: "prepare" | "cancel" } }>(
      TOOL_HOST_DRAIN_PATH,
      async (request, reply) => {
        if (!request.body?.id || !["prepare", "cancel"].includes(request.body.action))
          return reply.code(400).send({ error: "invalid_drain_request" });
        if (request.body.action === "cancel") quiescence.cancel(request.body.id);
        else quiescence.prepare(request.body.id);
        return quiescence.status();
      },
    );
    app.post<{ Body: { expectedGeneration: number; releaseId: string; revision: string } }>(
      TOOL_HOST_ACTIVATE_PATH,
      async (request, reply) => {
        if (
          request.body.releaseId !== releaseId ||
          request.body.revision !== catalog.read().revision
        )
          return reply.code(409).send({ error: "tool_release_mismatch" });
        try {
          const snapshot = await authority.select({
            releaseId,
            catalog: catalog.read(),
            replayable: replayable(),
            expectedGeneration: request.body.expectedGeneration,
            ...(versioned ? { versioned: route() } : {}),
          });
          await publish(snapshot);
          accepting = true;
          return {
            generation: snapshot.generation,
            revision: catalog.read().revision,
            ...(versioned ? { publicationRevision: snapshot.catalog.revision } : {}),
          };
        } catch {
          return reply.code(409).send({ error: "tool_catalog_activation_unconfirmed" });
        }
      },
    );
    if (versioned)
      app.post<{ Body: { name: string; disabled: boolean; expectedGeneration: number } }>(
        "/internal/v1/tools/availability",
        async (request, reply) => {
          const { name, disabled, expectedGeneration } = request.body ?? {};
          if (
            typeof name !== "string" ||
            !/^[a-z][a-z0-9_]{0,63}$/.test(name) ||
            typeof disabled !== "boolean" ||
            !Number.isSafeInteger(expectedGeneration)
          )
            return reply.code(400).send({ error: "invalid_tool_policy" });
          try {
            const next = await authority.select({
              releaseId,
              catalog: catalog.read(),
              replayable: replayable(),
              expectedGeneration,
              versioned: { ...route(), availability: { name, disabled } },
            });
            await publish(next);
            return { generation: next.generation, disabled };
          } catch {
            return reply.code(409).send({ error: "tool_policy_update_unconfirmed" });
          }
        },
      );
    app.post<{ Body: { releaseId: string; command: AcceptedControlToolCommand } }>(
      TOOL_HOST_EXECUTE_PATH,
      async (request, reply) => {
        const call = request.body?.command;
        const retained = versioned && !!call?.implementation && !call.implementation.catalogScope;
        if (!accepting || (!retained && !selected) || quiescence.status().phase !== "open")
          return reply.code(503).send({ error: "tool_host_draining" });
        const platform = selected;
        const combinedMcpAdmission =
          options.mcpTurnAdmittedByProjector && !!call?.implementation?.catalogScope;
        if (
          request.body.releaseId !== releaseId ||
          (!retained && !combinedMcpAdmission && !(await authority.current(platform!)))
        )
          return reply.code(412).send({ error: "tool_release_changed" });
        try {
          const {
            arguments: args,
            kind,
            nativeCallId,
            nativeSequence,
            invocationEntryId: _entry,
            ...dispatch
          } = call;
          parseControlToolDispatch(dispatch);
          if (
            call.implementation?.releaseId !== releaseId ||
            (!call.implementation.catalogScope &&
              ((!retained && call.implementation.generation !== selected!.generation) ||
                (call.implementation.replay === "safe") !== replayable().includes(call.toolName)))
          )
            return reply.code(412).send({ error: "tool_implementation_changed" });
          if (
            kind !== "control_tool" ||
            typeof nativeCallId !== "string" ||
            !nativeCallId ||
            !Number.isSafeInteger(nativeSequence) ||
            nativeSequence < 1 ||
            !args ||
            typeof args !== "object"
          )
            throw new Error("Invalid ordinary Tool invocation");
          // Authenticated Projector admission owns Session/task authority. This host
          // fixes the implementation release; no duplicate Run lease query here.
        } catch {
          return reply.code(409).send({ error: "tool_invocation_invalid" });
        }
        if (retained) {
          const decision = await authority.admitOrdinary(
            call.implementation!.generation,
            releaseId,
            call.toolName,
          );
          if (decision === "disabled") return reply.code(423).send({ error: "tool_disabled" });
          if (decision !== "accepted")
            return reply.code(410).send({ error: "tool_version_expired" });
        }
        // Drain can begin while the selection check is awaiting PG. Admission
        // and increment are synchronous with respect to the drain endpoint.
        if (!accepting || quiescence.status().phase !== "open")
          return reply.code(503).send({ error: "tool_host_draining" });
        if (request.raw.aborted || reply.raw.destroyed)
          return reply.code(409).send({ error: "tool_caller_disconnected" });
        const controller = new AbortController();
        active.add(controller);
        const abort = () => {
          if (!reply.raw.writableEnded) controller.abort(new Error("Tool caller disconnected"));
        };
        request.raw.once("aborted", abort);
        reply.raw.once("close", abort);
        try {
          const outcome = call.implementation?.catalogScope
            ? await mcp!.execute(
                call,
                () => !controller.signal.aborted,
                controller.signal,
                combinedMcpAdmission ? platform : undefined,
              )
            : await tools.execute(call, () => !controller.signal.aborted, controller.signal);
          const result = parseToolResultPayload({
            toolName: call.replyMode === "hostcall" ? "subagent-control" : call.toolName,
            toolCallId: call.nativeCallId,
            isError: outcome.isError ?? false,
            result: outcome.result,
          });
          const packed = await encodeSessionImages(
            images?.codec,
            { tenantId: call.scope.tenantId, runId: call.scope.runId },
            result,
          );
          return { operationId: call.operationId, releaseId, result: packed };
        } catch (error) {
          if (error instanceof McpInvocationRefused)
            return reply.code(409).send({ error: "mcp_invocation_not_admitted" });
          return reply.code(502).send({ error: "tool_execution_unconfirmed" });
        } finally {
          active.delete(controller);
          request.raw.off("aborted", abort);
          reply.raw.off("close", abort);
        }
      },
    );
    await app.listen({
      host: config.host,
      port: Number(process.env.PI_CLOUD_TOOL_HOST_PORT ?? 4310),
    });
    const restore = async () => {
      let snapshot = await authority.read();
      if (
        !snapshot ||
        (versioned &&
          snapshot.releaseId === releaseId &&
          snapshot.generation !== selected?.generation)
      )
        snapshot = await authority.select({
          releaseId,
          catalog: catalog.read(),
          replayable: replayable(),
          expectedGeneration: snapshot?.generation ?? 0,
          ...(versioned ? { versioned: route() } : {}),
        });
      if (
        snapshot.releaseId === releaseId &&
        (versioned || snapshot.catalog.revision === catalog.read().revision) &&
        selected?.generation !== snapshot.generation
      )
        await publish(snapshot);
    };
    await restore();
    mcp.start();
    let retrying = false;
    retryTimer = setInterval(() => {
      if (retrying || closing || (selected && !publicationPending)) return;
      retrying = true;
      void restore()
        .catch(() =>
          operationalLog({
            service: "pi-cloud-tool-host",
            level: "warn",
            event: "tool.catalog_publication_pending",
          }),
        )
        .finally(() => {
          retrying = false;
        });
    }, 2000);
    retryTimer.unref();
    const stop = () => {
      void close().catch(() => {
        process.exitCode = 1;
      });
    };
    process.once("SIGTERM", stop);
    process.once("SIGINT", stop);
    return { app, close };
  } catch (error) {
    await close().catch(() => {});
    throw error;
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href)
  await startToolHost();
