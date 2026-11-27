// Compose the segmented candidate with the ordinary native projection and the
// existing named return dispatcher. Kafka still orders every writer boundary.
import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { sql } from "kysely";
import Fastify from "fastify";
import { durableControlIntent } from "@pi-cloud/protocol";
import { installNamedSubagentEndpoint } from "../../../packages/control-plane/src/named-subagent-endpoint.ts";
import { NamedSubagentClient } from "../../../packages/control-plane/src/named-subagent-client.ts";
import { SubagentAdmissionRefused } from "../../../packages/control-plane/src/named-subagent-store.ts";
import {
  AcceptedFamilyCheckpoint,
  readSealedDelegationIntent,
  resumeFamilyCheckpoint,
  saveSealedFamily,
} from "@pi-cloud/pi-session-postgres/durable";
import {
  KafkaLogConsumer,
  KafkaToolCatalogConsumer,
  KafkaToolReplyPublisher,
} from "@pi-cloud/event-log";
import { ToolHostClient } from "../../../packages/control-plane/src/tool-host-client.ts";
import { ControlToolDispatcher } from "../../../packages/control-plane/src/control-tool-dispatcher.ts";
import { ToolCatalogAuthority } from "../../../packages/control-plane/src/tool-catalog-authority.ts";

import { kafkaProducerLane } from "@pi-cloud/runtime-core/kafka-accepted-fact";
import { NamedSubagentExecutor } from "../../../packages/control-plane/src/named-subagent-executor.ts";
import {
  operationsFor,
  decodeWorkspaceFact,
  isMcpReference,
  isOrdinaryHostReference,
} from "./workspace-operations.mjs";
import { ChildNameConflict, ForkStorage } from "./fork-storage.mjs";
import { installSegmentInputApi } from "./segment-input-api.mjs";
import { SegmentInputs } from "./segment-inputs.mjs";
import { serveSegmentScheduler } from "./segment-wake.mjs";
import { SegmentLeaseProjection, SegmentWorker } from "./independent-session-leases.mjs";
import { normalRootProjection } from "./normal-root-projection.mjs";
import { settledFamilyBase } from "./settled-family-base.mjs";

import { historyScope } from "./open-segment-native.mjs";
const matches = (s, r) =>
  r.writer === s.writerId &&
  r.epoch === s.fencingToken &&
  r.run === s.runId &&
  r.session === s.sessionId &&
  r.tenant === s.tenantId;

export class IndependentSessionRuntime {
  #owners = new Map();
  constructor({ pool, topic, partitions = 1, selectSandbox, resolveSandbox, bashLog = false }) {
    this.store = new ForkStorage(pool);
    this.inputs = this.store.inputs = new SegmentInputs(this.store.db);
    this.topic = topic;
    this.partitions = partitions;
    this.bashLog = bashLog;
    this.leases = new SegmentLeaseProjection(this.store.db, topic, partitions);
    const selectForTurn = (scope, lane) => {
      const owner = this.#owners.get(scope.tenantId + "/" + scope.sessionId);
      if (!owner || !matches(scope, owner) || !owner.workspace_grant)
        throw Error("No admitted Turn workspace for tool routing");
      return selectSandbox(scope, lane, owner.workspace_grant);
    };
    this.projection = normalRootProjection(pool, topic, selectForTurn, resolveSandbox, undefined, {
      partitions,
      bashLog,
      namedExecutor: false,
      independentSessions: true,
    });
  }
  async replayOffsets(bounds) {
    // On first use only an intact, empty-origin partition can create a cursor.
    // An existing durable position is never reset by a boot or rebalance.
    const fresh = bounds.filter((b) => b.low === 0n).map((b) => b.partition);
    if (fresh.length)
      await this.store.pool.query(
        "insert into accepted_fact_projection_offsets(topic,partition,next_offset) select $1,p,0 from unnest($2::int[]) p on conflict do nothing",
        [this.topic, fresh],
      );
    const starts = await this.projection.view.recover(bounds);
    const { rows } = await this.store.pool.query(
      `select output_first_partition as p,min(output_first_offset) as first from turns
      where state in ('running','cancel_requested') and output_first_topic=$1 and output_first_partition=any($2::int[])
      group by output_first_partition`,
      [this.topic, bounds.map((b) => b.partition)],
    );
    const floors = new Map(rows.map((r) => [r.p, BigInt(r.first)]));
    for (const bound of bounds) {
      const floor = floors.get(bound.partition),
        regular = starts.get(bound.partition);
      if (floor !== undefined && typeof regular === "bigint" && floor < regular)
        starts.set(
          bound.partition,
          floor < bound.low ? new Error("Active native handoff prefix expired") : floor,
        );
    }
    return starts;
  }
  reader({ brokers, clientId, groupId, onError }) {
    return (this.consumer = new KafkaLogConsumer({
      brokers,
      topic: this.topic,
      clientId,
      groupId,
      groupRecovery: true,
      orderedEffects: true,
      onRevoked: () => this.reset(),
      replayOffsets: (bounds) => this.replayOffsets(bounds),
      decode: (bytes) => {
        try {
          return decodeWorkspaceFact(bytes).fact;
        } catch (error) {
          onError?.(error);
          throw error;
        }
      },
      handler: async (record, current, grant) => {
        try {
          return await this.consume(record, current, grant);
        } catch (error) {
          onError?.(error);
          throw error;
        }
      },
    }));
  }
  installInputApi(app, authorize) {
    installSegmentInputApi(app, this.inputs, authorize);
  }
  serveScheduler(options) {
    return serveSegmentScheduler(this.store.pool, options);
  }
  worker(registration) {
    return new SegmentWorker({ database: this.store.db, ...registration });
  }
  async connectToolHost({ url, token, brokers, replicas = 3, versioned = false }) {
    if (this.hostTools) throw Error("MCP dispatcher already connected");
    const instanceId = randomUUID(),
      authority = new ToolCatalogAuthority(this.store.db);
    const catalog = new KafkaToolCatalogConsumer({
      brokers,
      replicas,
      instanceId,
      minimumGenerations: () => authority.minimumGenerations(),
    });
    let currentRoute;
    const resolveVersion = async (releaseId, generation) => {
      const latest = catalog.view.snapshot;
      if (
        currentRoute &&
        currentRoute.generation === generation &&
        currentRoute.releaseId === releaseId &&
        latest?.generation === generation
      )
        return currentRoute.endpoint;
      const endpoint = await authority.versionEndpoint(generation, releaseId);
      if (endpoint && latest?.generation === generation && latest.releaseId === releaseId)
        currentRoute = { releaseId, generation, endpoint };
      return endpoint;
    };
    const client = new ToolHostClient({
      baseUrl: url,
      token,
      catalog: (name) => catalog.view.publicationFor(name),
      ...(versioned
        ? {
            resolveVersion,
            revokedVersion: async (generation, releaseId, name) =>
              (await authority.admitOrdinary(generation, releaseId, name)) === "disabled",
            currentEndpoint: async () => {
              const p = catalog.view.snapshot;
              return p && resolveVersion(p.releaseId, p.generation);
            },
          }
        : {}),
    });
    const replies = new KafkaToolReplyPublisher(brokers, "segment-mcp-" + instanceId);
    const dispatcher = new ControlToolDispatcher({
      database: this.store.db,
      partitions: this.partitions,
      admittedHostTools: true,
      ownsPartition: (p) => this.consumer?.ownsPartition(p) === true,
      execute: (command, current, signal) => client.execute(command, current, signal),
      publish: (topic, reply) => replies.publish(topic, reply),
    });
    const close = async () => {
      this.hostTools = undefined;
      try {
        await dispatcher.close();
      } finally {
        await Promise.all([client.close(), replies.close(), catalog.close()]);
      }
    };
    try {
      await catalog.start();
      await replies.start();
      this.hostTools = dispatcher;
      return { close, catalog: catalog.view };
    } catch (error) {
      await close();
      throw error;
    }
  }
  connectDelegations(url, token) {
    if (this.subagents) throw Error("Subagent admission endpoint already configured");
    this.subagents = new NamedSubagentClient(url, token);
    return this.subagents;
  }
  async serveDelegations({ connectionString, publish, token, host = "127.0.0.1", port = 0 }) {
    const dispatcher = new NamedSubagentExecutor({
      db: this.store.db,
      connectionString,
      publish,
      independentSessions: true,
    });
    const api = Fastify({ logger: false, bodyLimit: 1024 * 1024 });
    installNamedSubagentEndpoint(
      api,
      token,
      () => true,
      async (o) => {
        if (o.tool === "send_message" && "args" in o.input) {
          await this.inputs.sendAgent(o);
          dispatcher.wake();
          return;
        }
        if (o.tool !== "subagent")
          throw new SubagentAdmissionRefused("Expected independent delegation");
        const {
          rows: [caller],
        } =
          await sql`select s.flow_id,r.model_snapshot from sessions s join turns r on r.tenant_id=s.tenant_id and r.session_id=s.flow_id
        where s.tenant_id=${o.tenant}::uuid and s.id=${o.session}::uuid and r.id=${o.root}::uuid and r.lease_id=${o.writer}::uuid
        and r.fencing_token=${o.epoch} and r.state='running' and r.output_seal_id is null`.execute(
            this.store.db,
          );
        if (!caller) throw new SubagentAdmissionRefused("Caller writer closed");
        const s = {
          tenantId: o.tenant,
          sessionId: o.session,
          piSessionId: o.session,
          runId: o.root,
          turnId: o.root,
          writerId: o.writer,
          fencingToken: o.epoch,
        };
        let intent;
        if ("recover" in o.input) {
          intent = await readSealedDelegationIntent(
            this.store.db,
            historyScope(s, caller.flow_id),
            this.topic,
            o.lane,
            o.task,
          );
          if (
            intent.operationId !== o.input.recover ||
            intent.sequence !== o.input.seq ||
            intent.contextAnchor !== o.anchor
          )
            throw new SubagentAdmissionRefused("Changed original delegation");
        } else
          intent = {
            toolName: o.tool,
            operationId: o.operation,
            taskId: o.task,
            conversationId: o.lane,
            nativeCallId: o.input.call,
            arguments: o.input.args,
            sequence: o.input.seq,
            contextAnchor: o.anchor,
            contextThrough: o.through,
            requestEntry: o.requestEntry,
            assistantEntry: o.assistantEntry,
          };
        try {
          await this.store.delegateFromIntent(
            o.tenant,
            o.session,
            { run: o.root, writer: o.writer },
            intent,
            caller.model_snapshot.modelId,
            { operationId: o.operation, topic: o.destination },
          );
        } catch (error) {
          if (!(error instanceof ChildNameConflict)) throw error;
          // A deterministic name conflict is also a durable Tool Result. A stale
          // caller or a failed PG transaction leaves its original bounded wait.
          const hash = createHash("sha256").update(JSON.stringify(o)).digest("hex");
          const { rows } =
            await sql`insert into subagent_deliveries(operation_id,root_id,writer,caller_task,caller_lane,operation_hash,destination,call_id,tool,kind,value,due_at)
          select ${o.operation}::uuid,r.id,${o.writer}::uuid,${o.task},${o.lane},${hash},${o.destination},${intent.nativeCallId},'subagent','fixed',
            jsonb_build_object('isError',true,'text',${"名字已被占用。已有子 Agent 的 UUID 是 " + error.childId + "。请换一个名字或联系已有的子 Agent。"}::text),clock_timestamp()
          from (select id from turns where id=${o.root}::uuid and tenant_id=${o.tenant}::uuid and session_id=${caller.flow_id}::uuid and state='running'
            and output_seal_id is null and lease_id=${o.writer}::uuid and fencing_token=${o.epoch} for share) r
          on conflict(operation_id) do update set operation_hash=excluded.operation_hash where subagent_deliveries.operation_hash=excluded.operation_hash returning operation_id`.execute(
              this.store.db,
            );
          if (!rows.length) throw new SubagentAdmissionRefused("Stale or conflicting delivery");
        }
        dispatcher.wake();
      },
    );
    const url = await api.listen({ host, port });
    dispatcher.start();
    return {
      url,
      async close() {
        await api.close();
        await dispatcher.close();
      },
    };
  }
  reset() {
    this.hostTools?.reset();
    this.subagents?.reset();
    this.#owners.clear();
    this.browser?.reset();
    this.projection.nativeStore.projection.reset();
  }
  async #admitted(s) {
    const key = s.tenantId + "/" + s.sessionId,
      old = this.#owners.get(key);
    if (old && matches(s, old)) return old;
    const {
      rows: [r],
    } =
      await sql`select s.flow_id,r.native_work_handoff is not null as recovering,r.workspace_grant,r.output_first_offset from sessions s
      join turns r on r.tenant_id=s.tenant_id and r.session_id=s.flow_id join session_leases l on l.tenant_id=s.tenant_id and l.pi_session_id=s.id::text and l.lease_id=r.lease_id
      where s.tenant_id=${s.tenantId}::uuid and s.id=${s.sessionId}::uuid and r.id=${s.runId}::uuid and r.lease_id=${s.writerId}::uuid
      and r.fencing_token=${s.fencingToken} and l.fencing_token=r.fencing_token and l.released_at is null and r.state in ('running','cancel_requested')`.execute(
        this.store.db,
      );
    if (!r) return;
    const owner = {
      ...r,
      tenant: s.tenantId,
      session: s.sessionId,
      writer: s.writerId,
      epoch: s.fencingToken,
      run: s.runId,
    };
    this.#owners.set(key, owner);
    return owner;
  }
  async #base(owner, local, record) {
    if (owner.recovering) return resumeFamilyCheckpoint(this.store.db, local, record);
    const seq = record.fact.commit.seq;
    return (
      (seq > 1 && (await settledFamilyBase(this.store.pool, local, seq))) ||
      new AcceptedFamilyCheckpoint()
    );
  }
  // Call once per record in the assigned partition, with the group's existing
  // dispatch grant. Revocation must call reset before another assignment.
  async consume(record, current, grant) {
    const { fact, topic, partition, offset } = record,
      s = fact.scope;
    if (
      !current() ||
      topic !== this.topic ||
      partition !== kafkaProducerLane(s.sessionId, this.partitions) ||
      s.sessionId !== s.piSessionId ||
      s.turnId !== s.runId
    )
      throw Error("Invalid independent Session record");
    const boundary = fact.kind === "execution_seal" || fact.kind === "work_finished";
    const owner = await this.#admitted(s);
    if (!current()) throw Error("Session partition revoked");
    if (!owner) {
      await this.#skip(record);
      return false;
    }
    const local = historyScope(s, owner.flow_id);
    if (boundary) {
      let terminalEvent;
      const changed = await this.leases.projectBoundary(record, async (tx, r) => {
        if (fact.kind === "execution_seal") {
          let state = owner.witness?.snapshot();
          if (
            !state &&
            r.native_work_handoff &&
            r.output_first_offset === null &&
            r.projection_first === null
          ) {
            // The successor died before its first native commit. saveSealedFamily
            // requires the previous seal and identical contents in this transaction.
            state = (await resumeFamilyCheckpoint(tx, local, record, true)).snapshot();
          }
          if (state?.seq) {
            await saveSealedFamily(tx, local, record, fact.factId, state, {
              bashIntentLog: !!this.bashLog,
            });
          } else if (r.output_first_offset !== null || r.projection_first !== null)
            throw Error("Replay the native prefix before sealing");
        } else {
          await this.projection.nativeStore.projection.checkpointLiveIn(
            tx,
            local,
            current,
            fact.nativeSequence,
          );
          await this.projection.nativeStore.settle(tx, { ...fact, scope: local }, record);
        }
        if (this.browser && fact.kind === "work_finished")
          terminalEvent = await this.browser.terminal(tx, fact, r.flow_id);
        if (!current()) throw Error("Session partition revoked");
        await this.#advance(tx, record);
      });
      if (!changed) {
        await this.#skip(record);
        return false;
      }
      if (terminalEvent) this.browser.accept(s.tenantId, terminalEvent);
      await this.hostTools?.consume(record, current);
      this.#owners.delete(s.tenantId + "/" + s.sessionId);
      this.projection.nativeStore.projection.releaseSettled(local);
      this.projection.nativeStore.controlProjected(record);
      return true;
    }
    if (fact.kind !== "durable_commit")
      throw Error("Unsupported native record in independent candidate");
    for (const item of [...fact.display, ...fact.tools])
      if (Object.keys(s).some((key) => item.scope[key] !== s[key]))
        throw Error("Cross-Session effect on an independent writer");
    const ops = operationsFor(fact);
    if (ops.some((op) => op.scope?.sessionId !== s.sessionId || op.scope.turnId !== s.turnId))
      throw Error("Cross-Session sandbox operation");
    const mapped =
      owner.flow_id === s.sessionId
        ? fact
        : { ...fact, scope: local, display: fact.display.map((d) => ({ ...d, scope: local })) };
    // Embedded tool and origin scopes stay on the original trusted Session.
    const covered = offset < (this.projection.view.next.get(partition) ?? 0n);
    if (covered && owner.output_first_offset !== null && offset < BigInt(owner.output_first_offset))
      return false;
    const opening = !owner.witness;
    owner.witness ??= await this.#base(owner, local, record);
    const result = await this.projection.nativeStore.projection.project(
      mapped,
      record,
      current,
      s.sessionId,
      opening,
      ops,
    );
    if (result.admitted || covered) owner.witness.observe(mapped);
    if (!result.admitted) return false;
    if (this.browser) {
      await this.browser.placements?.(s, owner.flow_id, fact.commit);
      for (const event of result.events) this.browser.accept(s.tenantId, event);
      for (const cover of result.covers) this.browser.cover(s.tenantId, s.sessionId, cover.through);
    }
    let permit;
    const admit = () => (permit ??= grant());
    await this.projection.dispatch({ ...record, fact: mapped }, current, admit);
    const hosted = fact.tools.filter((ref) => isMcpReference(ref) || isOrdinaryHostReference(ref));
    if (hosted.length && !this.hostTools) throw Error("Tool host is not connected");
    if (hosted.length || this.hostTools?.hasPending)
      await this.hostTools.consume({ ...record, fact: { ...fact, tools: hosted } }, current, admit);
    for (const ref of fact.tools) {
      if (ref.toolName !== "subagent" && ref.toolName !== "send_message") continue;
      if (!this.subagents) throw Error("Independent subagent admission endpoint is unavailable");
      let through, origin;
      if (!ref.recovery && ref.toolName === "subagent") {
        const source = durableControlIntent(fact.commit, ref);
        const doc = owner.witness
          .snapshot()
          .records.find(
            (row) =>
              row.write.record?.kind === "pi-cloud.run" &&
              row.write.record.scope.conversationId === ref.conversationId,
          )?.write.content.value;
        const request = doc?.delegationRequest;
        if (
          !request?.calls.includes(source.callId) ||
          (ref.contextAnchor !== null && request.at !== ref.contextAnchor) ||
          !Number.isSafeInteger(request.through)
        )
          throw Error("Delegation does not match the accepted model request");
        through = request.through;
        const task = fact.commit.writes.find((w) => w.type === "task" && w.value.id === ref.taskId);
        const assistantEntry = task?.value.input?.assistant;
        if (!Number.isSafeInteger(assistantEntry) || !Number.isSafeInteger(request.at))
          throw Error("Missing founding delegation coordinates");
        origin = { assistantEntry, requestEntry: request.at };
      }
      if (current() && (await admit()) && current())
        this.subagents.offer(fact, ref, through, origin);
    }
    return true;
  }
  async #advance(tx, r) {
    // Memory-only live frames can separate the last durable position from this
    // ordered boundary. CAS the same durable head used by project_ready.
    const expected = this.projection.view.next.get(r.partition);
    if (expected === undefined) throw Error("Session partition has no projection assignment");
    const { rows } =
      await sql`update accepted_fact_projection_offsets set next_offset=greatest(next_offset,${String(r.offset + 1n)}::bigint)
      where topic=${r.topic} and partition=${r.partition} and (next_offset=${String(expected)}::bigint or next_offset>${String(r.offset)}::bigint) returning next_offset`.execute(
        tx,
      );
    if (!rows.length) throw Error("Missing earlier projection position");
  }
  async #skip(r) {
    if ((this.projection.view.next.get(r.partition) ?? 0n) > r.offset) return;
    await this.store.db.transaction().execute((tx) => this.#advance(tx, r));
    if ((this.projection.view.next.get(r.partition) ?? 0n) <= r.offset)
      this.projection.nativeStore.controlProjected(r);
  }
}

export { openSegmentNative } from "./open-segment-native.mjs";
