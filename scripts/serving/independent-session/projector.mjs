import { randomUUID } from "node:crypto";
import { sql } from "kysely";
import { createDatabase } from "@pi-cloud/database";
import { KafkaWorkerProducer } from "@pi-cloud/event-log";
import { parsePiCloudEvent } from "@pi-cloud/protocol";
import { kafkaProducerLane } from "@pi-cloud/runtime-core/kafka-accepted-fact";
import { SessionEventStream } from "@pi-cloud/runtime-core/session-event-stream";
import { SessionLiveView } from "../../../packages/runtime-core/src/session-live-view.ts";
import { SessionLiveService } from "../../../packages/control-plane/src/session-live-service.ts";
import { SupervisorControlRuntime } from "../../../packages/control-plane/src/supervisor-control-runtime.ts";
import {
  SupervisorBootProvisioner,
  SupervisorProvisioningGateway,
  PostgresSupervisorCredentialAuthorizer,
} from "../../../packages/control-plane/src/supervisor-boot-provisioner.ts";
import {
  HttpSupervisorManagementClient,
  RoutedHttpSupervisorOwnerBoundary,
  RoutedHttpSandboxAssignmentInventory,
} from "../../../packages/control-plane/src/http-supervisor-management.ts";
import { IndependentSessionRuntime } from "../../experiments/minimal-session-storage/independent-session-runtime.mjs";
import { encodeWorkspaceFact } from "../../experiments/minimal-session-storage/workspace-operations.mjs";
import { installation, openStore, integer, secret, required } from "./config.mjs";

export async function startIndependentProjector() {
  const config = await installation(),
    pool = await openStore(config);
  const db = createDatabase({ connectionString: config.url }),
    reads = createDatabase({ connectionString: config.read });
  const rt = new IndependentSessionRuntime({
    pool,
    topic: config.topic,
    partitions: config.partitions,
  });
  const instanceId = randomUUID(),
    token = await secret("PI_CLOUD_SUPERVISOR_MANAGEMENT_TOKEN"),
    stop = new AbortController();
  const clients = new Map();
  const resolver = async ({ supervisorId }) => {
    const {
      rows: [host],
    } =
      await sql`select management_base_url from supervisor_hosts where supervisor_id=${supervisorId}`.execute(
        db,
      );
    if (!host) throw Error("Unknown management host");
    if (!clients.has(host.management_base_url))
      clients.set(
        host.management_base_url,
        new HttpSupervisorManagementClient({
          baseUrl: host.management_base_url,
          managementToken: token,
          allowInsecureHttp: true,
        }),
      );
    return clients.get(host.management_base_url);
  };
  const control = new SupervisorControlRuntime({
    database: db,
    maintenanceDatabase: db,
    instanceId,
    authorizer: new PostgresSupervisorCredentialAuthorizer({ database: db }),
    ownerBoundary: new RoutedHttpSupervisorOwnerBoundary(resolver),
    inventory: (id) => new RoutedHttpSandboxAssignmentInventory(resolver, id),
  });
  new SupervisorProvisioningGateway({
    provisioner: new SupervisorBootProvisioner({
      database: db,
      allowedSupervisorIdPrefix: "pma-segment-",
      managementBaseUrlTemplates: JSON.parse(required("PI_CLOUD_SEGMENT_MANAGEMENT_TEMPLATES")),
      maximumCapacity: integer("PI_CLOUD_SEGMENT_CAPACITY", 8),
      enrollmentToken: await secret("PI_CLOUD_SUPERVISOR_ENROLLMENT_TOKEN"),
    }),
  }).install(control.server);
  const reader = rt.reader({
    brokers: config.brokers,
    clientId: "pma-projector-" + instanceId,
    groupId: config.topic + "-projector",
  });
  const live = new SessionLiveView((_t, s) =>
    reader.waitForPartition(kafkaProducerLane(s, config.partitions)),
  );
  rt.browser = {
    accept: (t, e) => live.accept(t, e),
    cover: (t, s, n) => live.cover(t, s, n),
    reset: () => live.reset(),
    async placements(scope, flow, commit) {
      if (!live.eventHub.hasSubscribers(scope.tenantId, scope.sessionId)) return;
      const ids = commit.writes
        .filter(
          (w) =>
            w.type === "submission" &&
            w.value.type === "input" &&
            w.value.status === "placed" &&
            w.value.entry !== undefined,
        )
        .map((w) => w.value.entry);
      if (!ids.length) return;
      try {
        const { rows } =
          await sql`select m.entry_id,m.commit_seq,m.source_input_id as id,m.input_display ? 'initial' as initial,u.body,u.uploads
          from messages m join user_or_agent_messages u on u.tenant_id=m.tenant_id and u.session_id=${scope.sessionId}::uuid and u.turn_id=m.run_id and u.id::text=m.source_input_id
          where m.tenant_id=${scope.tenantId}::uuid and m.pi_session_id=${flow}::uuid and m.conversation_id=1 and m.run_id=${scope.runId}::uuid
          and m.entry_id=any(${ids}::bigint[]) and u.state='placed' and u.source='user' order by m.commit_seq,m.ordinal`.execute(
            rt.store.db,
          );
        for (const r of rows)
          live.eventHub.publishPlacement(scope.tenantId, {
            type: "input.placed",
            sessionId: scope.sessionId,
            turnId: scope.turnId,
            inputId: r.id,
            entryId: Number(r.entry_id),
            sequence: Number(r.commit_seq),
            initial: r.initial,
            text: r.body,
            uploads: r.uploads,
          });
      } catch {
        live.eventHub.resyncSession(scope.tenantId, scope.sessionId);
      }
    },
    async terminal(tx, fact, flow) {
      const s = fact.scope;
      const {
        rows: [row],
      } =
        await sql`update turns set output_display_seq=greatest(output_display_seq,${fact.baseSequence}::bigint)+1
        where tenant_id=${s.tenantId}::uuid and id=${s.runId}::uuid returning output_display_seq`.execute(
          tx,
        );
      const event = parsePiCloudEvent({
        schemaVersion: 1,
        eventId: fact.factId,
        sessionId: s.sessionId,
        turnId: s.turnId,
        agentId: fact.agentId,
        seq: Number(row.output_display_seq),
        occurredAt: fact.occurredAt,
        ...fact.terminal,
      });
      await sql`insert into session_terminal_events(event_id,tenant_id,session_id,turn_id,agent_id,run_id,seq,schema_version,type,payload,occurred_at)
      values(${event.eventId}::uuid,${s.tenantId}::uuid,${flow}::uuid,${s.turnId}::uuid,${event.agentId},${s.runId}::uuid,${event.seq},1,${event.type},${JSON.stringify(event.payload)}::jsonb,${fact.occurredAt}::timestamptz)`.execute(
        tx,
      );
      return event;
    },
  };
  new SessionLiveService({
    database: reads,
    stream: new SessionEventStream(live, live.eventHub),
    rootMessageHistory: true,
    flowRootHistory: true,
    placedInputHistory: true,
    token,
  }).install(control.server);
  const writer = new KafkaWorkerProducer({
    brokers: config.brokers,
    topic: config.topic,
    clientId: "pma-seal-" + instanceId,
  });
  let reconcile, scheduler, hosted;
  const delegations = rt.connectDelegations(required("PI_CLOUD_SUBAGENT_EXECUTOR_URL"), token);
  const close = async () => {
    stop.abort();
    try {
      await reconcile;
    } finally {
      await scheduler?.close();
      await reader.close();
      await delegations.close();
      await hosted?.close();
      const { commits, recordsInTransactions, maxRecords, syncTransactions, asyncTransactions } =
        rt.projection.view.stats;
      console.info(
        JSON.stringify({
          event: "session.projection.summary",
          commits,
          recordsInTransactions,
          maxRecords,
          syncTransactions,
          asyncTransactions,
        }),
      );
      await control.close();
      await writer.close();
      live.close();
      await Promise.all([...clients.values()].map((c) => c.close?.()));
      await Promise.all([db.destroy(), reads.destroy(), rt.store.db.destroy()]);
    }
  };
  try {
    await writer.start();
    hosted = await rt.connectToolHost({
      url: required("PI_CLOUD_TOOL_HOST_URL"),
      token,
      brokers: config.brokers,
      versioned: true,
    });
    await reader.start();
    await reader.waitUntilInitialReplay(await reader.captureEndOffsets());
    scheduler = await rt.serveScheduler({
      connectionString: config.write,
      host: process.env.HOST ?? "127.0.0.1",
      port: integer("PI_CLOUD_SEGMENT_WAKE_PORT", 4301),
      token,
    });
    reconcile = rt.leases.reconcile({
      signal: stop.signal,
      publish: (partition, f) =>
        writer.append(partition, {
          sessionId: f.scope.sessionId,
          factId: f.factId,
          value: encodeWorkspaceFact(f),
        }),
    });
    reconcile.catch(() => stop.abort());
    control.server.get("/health/ready", async (_req, res) => {
      try {
        reader.checkHealth();
        if (stop.signal.aborted) throw Error();
        return { ready: true };
      } catch {
        return res.code(503).send({ ready: false });
      }
    });
    await control.listen(integer("PORT", 4300), process.env.HOST ?? "127.0.0.1");
    return { close, failed: reconcile };
  } catch (e) {
    await close();
    throw e;
  }
}
