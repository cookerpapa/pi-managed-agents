import { installPhysicalTurnProjection } from "./ready-projection-sql.mjs";
// Ownership adapter for the #52 segmented Run queue. Public Worker identities,
// renewable leases and the on-disk epoch are the same ones the serving coordinator uses.
import { homedir } from "node:os";
import { join } from "node:path";
import { SegmentWarmHistory } from "./segment-warm-history.mjs";
import { SteerHint } from "./steer-hint.mjs";
import { connectSegmentWorker } from "./segment-wake.mjs";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { readFile } from "node:fs/promises";
import { sql } from "kysely";
import { SessionLeaseCoordinator } from "@pi-cloud/runtime-core/session-lease-coordinator";
import { PostgresSessionLeaseObserver } from "../../../packages/pi-session-postgres/src/postgres-session-lease-observer.ts";
import { kafkaProducerLane } from "@pi-cloud/runtime-core/kafka-accepted-fact";

export async function installIndependentSessionLeases(db) {
  for (const file of [
    "subagent-deliveries.sql",
    "independent-session-leases.sql",
    "turn-snapshots.sql",
  ])
    await db.query(await readFile(new URL(file, import.meta.url), "utf8"));
  await installPhysicalTurnProjection(db);
  await db.query(await readFile(new URL("session-inputs.sql", import.meta.url), "utf8"));
}

export class SegmentWorker {
  #coordinator;
  #observer;
  #active = new Map();
  #control;
  #hintRepair;
  #fullRepairDue = false;
  #pending = 0;
  #closed = false;
  #stop = new AbortController();
  #serving = false;
  #timer;
  #renewing;
  constructor({ database, workerId, bootId, capacity, leaseDurationMs, historyCache }) {
    if (!Number.isSafeInteger(capacity) || capacity < 1) throw Error("Invalid Worker capacity");
    this.db = database;
    const diskRoot =
      process.env.PI_CLOUD_WORKER_HISTORY_CACHE_ROOT ??
      join(
        process.env.XDG_CACHE_HOME ?? join(homedir(), ".cache"),
        "pi-managed-agents",
        "worker-history",
      );
    this.warmHistory = new SegmentWarmHistory({
      diskDirectory: process.env.PI_CLOUD_WORKER_HISTORY_CACHE_DIR || join(diskRoot, workerId),
      ...historyCache,
      advertise: (...args) => this.#control?.advertise(...args),
    });
    this.workerId = workerId;
    this.bootId = bootId;
    this.capacity = capacity;
    this.#coordinator = new SessionLeaseCoordinator({ database, workerId, leaseDurationMs });
    this.#observer = new PostgresSessionLeaseObserver({ database });
  }
  async claimNext(hint) {
    return (await this.claimBatch(1, hint))[0];
  }
  async claimBatch(limit, hint) {
    if (!Number.isSafeInteger(limit) || limit < 1) throw Error("Invalid claim limit");
    const reserved = Math.min(limit, this.capacity - this.#pending - this.#active.size);
    if (this.#closed || reserved <= 0) return [];
    this.#pending += reserved;
    try {
      const jobs = await this.db.transaction().execute(async (tx) => {
        await sql`set local synchronous_commit=on`.execute(tx);
        const selectJobs = (count, target, skip) => {
          let eligible = sql`q.ready_at<=statement_timestamp()`;
          if (target) {
            const [tenant, session, turn, version, flow] = target;
            eligible = sql`r.tenant_id=${tenant}::uuid and s.id=${session}::uuid and r.id=${turn}::uuid and r.session_id=${flow}::uuid
              and exists(select from turns prev where prev.tenant_id=r.tenant_id and prev.session_id=r.session_id
                and prev.mailbox_position=r.mailbox_position-1 and prev.history_head->>'version'=${version})`;
          }
          return sql`select r.id,r.tenant_id,r.session_id as flow_id,s.id as logical_id,
          s.parent_session_id,s.agent_name,s.family_id,s.owner_user_id,
          greatest(r.output_display_seq,coalesce((select p.output_display_seq from turns p
            where p.tenant_id=r.tenant_id and p.session_id=r.session_id and p.mailbox_position<r.mailbox_position
              and p.output_display_seq>0 order by p.mailbox_position desc limit 1),0)) as display_base,r.native_work_handoff is not null as recovering,r.mailbox_position,r.stop_on_resume,r.started_at,r.initial_input,r.model_snapshot,r.workspace_grant,r.environment_snapshot,
          (select prev.workspace_grant from turns prev where prev.tenant_id=r.tenant_id and prev.session_id=r.session_id and prev.mailbox_position=r.mailbox_position-1) previous_workspace,(select m.id from user_or_agent_messages m where m.turn_id=r.id and m.state <> 'withdrawn' order by m.native_id nulls last,m.starts_turn desc,m.position limit 1) as first_message,
          exists(select from user_or_agent_messages m where m.turn_id=r.id) as has_inputs
          from ready_agent_sessions q join sessions s on s.tenant_id=q.tenant_id and s.id=q.session_id
          join turns r on r.tenant_id=s.tenant_id and r.session_id=s.flow_id
          where ${eligible} ${skip ? sql`and r.id <>${skip}::uuid` : sql``} and r.state='queued' and r.lease_id is null and r.output_seal_id is null
            and not exists(select from session_leases l where l.tenant_id=s.tenant_id and l.pi_session_id=s.id::text and l.released_at is null)
            and not exists(select from turns p where p.tenant_id=r.tenant_id and p.session_id=r.session_id and p.mailbox_position<r.mailbox_position and
              (p.state not in ('completed','failed','timed_out','cancelled') or (p.output_seal_id is not null and p.output_sealed_at is null)))
          order by q.ready_at,q.session_id,r.mailbox_position limit ${count} for update of r,s skip locked`.execute(
            tx,
          );
        };
        const candidates = hint ? (await selectJobs(1, hint)).rows : [];
        if (candidates.length < reserved)
          candidates.push(
            ...(await selectJobs(reserved - candidates.length, undefined, candidates[0]?.id)).rows,
          );
        if (!candidates.length) return [];
        const worker = await tx
          .selectFrom("workers")
          .select(["state", "boot_id", "max_concurrent_sessions"])
          .where("id", "=", this.workerId)
          .forUpdate()
          .executeTakeFirst();
        if (
          worker?.state !== "ready" ||
          worker.boot_id !== this.bootId ||
          worker.max_concurrent_sessions !== this.capacity
        )
          throw Error("Worker registration changed");
        // PG also bounds admission across uncertain claim replies and clients
        // racing on the same boot. Local pending claims avoid unnecessary polls.
        const {
          rows: [{ used }],
        } =
          await sql`select count(*)::int used from session_leases where worker_id=${this.workerId}::uuid and released_at is null`.execute(
            tx,
          );
        const jobs = [];
        for (const job of candidates.slice(0, Math.max(0, this.capacity - used))) {
          await sql`insert into pi_sessions(tenant_id,id) values(${job.tenant_id}::uuid,${job.logical_id}) on conflict do nothing`.execute(
            tx,
          );
          const {
            rows: [physical],
          } =
            await sql`select lease_epoch from pi_sessions where tenant_id=${job.tenant_id}::uuid and id=${job.logical_id} for update`.execute(
              tx,
            );
          const lease = await this.#coordinator.grantLockedPhysicalSession(
            tx,
            job.tenant_id,
            job.logical_id,
            physical.lease_epoch,
          );
          const claim =
            await sql`update turns set state=case when stop_on_resume then 'cancel_requested' else 'running' end,lease_id=${lease.lease_id}::uuid,fencing_token=${lease.fencing_token},
          started_at=coalesce(started_at,clock_timestamp()) where id=${job.id}::uuid and exists(select from supervisor_connections c join workers w on w.id=c.worker_id
            and w.boot_id=c.boot_id and w.supervisor_id=c.supervisor_id where w.id=${this.workerId}::uuid and w.state='ready'
            and c.state='active' and c.expires_at>clock_timestamp()) returning id`.execute(tx);
          if (!claim.rows.length) throw Error("Worker management connection is unavailable");
          await sql`delete from ready_agent_sessions where tenant_id=${job.tenant_id}::uuid and session_id=${job.logical_id}::uuid`.execute(
            tx,
          );
          jobs.push({
            ...job,
            turnId: job.id,
            scope: {
              tenantId: job.tenant_id,
              sessionId: job.logical_id,
              piSessionId: job.logical_id,
              runId: job.id,
              turnId: job.id,
              writerId: lease.lease_id,
              fencingToken: Number(lease.fencing_token),
            },
          });
        }
        return jobs;
      });
      if (!jobs.length || this.#closed) return [];
      const claimed = jobs.map((job) => {
        const s = job.scope,
          identity = {
            tenantId: s.tenantId,
            piSessionId: s.piSessionId,
            writerId: s.writerId,
            leaseId: s.writerId,
            fencingToken: s.fencingToken,
          };
        const guard = this.#observer.open(identity);
        const release = async () => {
          this.#active.delete(identity.leaseId);
          await guard.close();
        };
        const steerHint = new SteerHint();
        const stop = new AbortController();
        if (job.stop_on_resume) stop.abort();
        const active = { identity, guard, steerHint, stop };
        this.#active.set(identity.leaseId, active);
        return {
          ...job,
          turnId: job.id,
          authority: guard,
          warmHistory: this.warmHistory,
          steerHint,
          stopSignal: stop.signal,
          release,
        };
      });
      try {
        // Open every handle before the observer performs its coalesced PG refresh.
        await Promise.all(
          claimed.map(async (job) => {
            await job.authority.assertCurrent();
            job.authority.start();
          }),
        );
        this.#schedule();
        await this.#syncSteers(
          claimed.map((j) => this.#active.get(j.scope.writerId)).filter(Boolean),
          true,
        ).catch(() => console.warn("segment.steer_bootstrap_deferred"));
        return claimed;
      } catch (error) {
        await Promise.all(claimed.map((job) => job.release()));
        throw error;
      }
    } finally {
      this.#pending -= reserved;
    }
  }
  #notice(writer, revision) {
    const active = this.#active.get(writer);
    if (!active || this.#closed) return;
    active.steerHint.mark();
    this.#control?.ackSteer(writer, revision);
  }
  // A fresh/reconnected owner checks even previously acknowledged steers. The
  // heartbeat repairs only missing receipts in one query for this Worker boot.
  async #syncSteers(active, full) {
    if (!active.length || this.#closed) return;
    const tickets = new Map(active.map((a) => [a.identity.leaseId, a.steerHint.ticket]));
    const outstanding = full ? sql`` : sql`and m.steer_seen_epoch is null`;
    const owners = [...tickets.keys()];
    const { rows } = await sql`(select distinct on(r.lease_id) r.lease_id,m.steer_notice
      from turns r join user_or_agent_messages m on m.turn_id=r.id and m.tenant_id=r.tenant_id
      where r.lease_id=any(${owners}::uuid[]) and r.state='running' and r.output_seal_id is null
        and m.state='pending' and m.mode='steer' and m.steer_notice is not null ${outstanding}
      order by r.lease_id,m.steer_notice desc) union all select lease_id,null::bigint from turns
      where lease_id=any(${owners}::uuid[]) and state='cancel_requested' and output_seal_id is null`.execute(
      this.db,
    );
    for (const { lease_id, steer_notice } of rows) {
      tickets.delete(lease_id);
      if (steer_notice === null) this.#active.get(lease_id)?.stop.abort();
      else this.#notice(lease_id, steer_notice);
    }
    if (full)
      for (const [writer, ticket] of tickets) this.#active.get(writer)?.steerHint.clear(ticket);
  }
  #repairHints(full) {
    if (this.#closed || !this.#active.size) return;
    this.#fullRepairDue ||= full;
    if (this.#hintRepair) return;
    const scanAll = this.#fullRepairDue;
    this.#fullRepairDue = false;
    this.#hintRepair = this.#syncSteers([...this.#active.values()], scanAll)
      .catch(() => console.warn("segment.steer_reconciliation_deferred"))
      .finally(() => {
        this.#hintRepair = undefined;
        if (this.#fullRepairDue) this.#repairHints(true);
      });
  }
  #schedule() {
    if (this.#closed || this.#timer || !this.#active.size) return;
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      this.#renewing = this.renew()
        .catch(() => {})
        .finally(() => {
          this.#renewing = undefined;
          this.#schedule();
        });
    }, this.#coordinator.heartbeatIntervalMs);
    this.#timer.unref?.();
  }
  async serve({ scheduler, signal, execute, onError }) {
    if (this.#serving) throw Error("Worker already serving");
    const until = AbortSignal.any([signal, this.#stop.signal]);
    if (until.aborted) return;
    const finished = Promise.withResolvers();
    const stop = () => finished.resolve();
    const client = connectSegmentWorker({
      scheduler,
      id: this.workerId + "/" + this.bootId,
      slots: this.capacity,
      onSteer: (writer, revision) => this.#notice(writer, revision),
      onStop: (writer) => this.#active.get(writer)?.stop.abort(),
      onRegistered: () => {
        this.warmHistory.advertiseAll();
        for (const active of this.#active.values()) active.steerHint.mark();
        this.#repairHints(true);
      },
      claim: (limit, hint) => this.claimBatch(limit, hint),
      execute: async (job) => {
        try {
          await execute(job);
        } catch (error) {
          if (!onError) throw error;
          await onError(error);
        } finally {
          await job.release();
        }
      },
      fail: (error) => finished.reject(error),
    });
    this.#control = client;
    this.#serving = true;
    until.addEventListener("abort", stop, { once: true });
    try {
      await finished.promise;
    } finally {
      until.removeEventListener("abort", stop);
      await client.close();
      this.#control = undefined;
      this.#serving = false;
    }
  }
  async renew() {
    const families = [...this.#active.values()].map((v) => v.identity);
    if (!families.length || this.#closed) return;
    const identity = await this.#coordinator.heartbeatIdentity();
    if (identity.bootId !== this.bootId) throw Error("Stale Worker boot");
    const heartbeat = {
      protocolVersion: 1,
      type: "supervisor.heartbeat",
      messageId: randomUUID(),
      sentAt: new Date().toISOString(),
      payload: {
        ...identity,
        acceptingAssignments: true,
        maxConcurrentSessions: this.capacity,
        families,
      },
    };
    const at = performance.now(),
      ack = await this.#coordinator.renewFromHeartbeat(heartbeat);
    this.#observer.observeRenewals(heartbeat, ack, at);
    this.#repairHints(false);
    return ack;
  }
  async close() {
    this.#closed = true;
    const diskClosed = this.warmHistory.close();
    this.#stop.abort();
    clearTimeout(this.#timer);
    await this.#renewing;
    await this.#hintRepair;
    await this.#observer.close();
    await diskClosed;
    this.#active.clear();
  }
}

// Only the owner of the ordered Session partition invokes projectBoundary.
// On expiry PG records one immutable seal intent. Publication may be retried
// after process loss; the lease remains allocated until the seal is consumed.
export class SegmentLeaseProjection {
  constructor(db, topic, partitions) {
    this.db = db;
    this.topic = topic;
    this.partitions = partitions;
  }
  async expired(limit = 32) {
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 128) throw Error("Invalid seal batch");
    return this.db.transaction().execute(async (tx) => {
      await sql`set local synchronous_commit=on`.execute(tx);
      const { rows } =
        await sql`select r.id,r.tenant_id,s.id as session,r.lease_id,r.fencing_token,r.output_seal_id,r.recovery_requested_at,r.output_display_seq
        from session_leases l join turns r on r.tenant_id=l.tenant_id and r.lease_id=l.lease_id join sessions s on s.id::text=l.pi_session_id and s.tenant_id=r.tenant_id and s.flow_id=r.session_id
        where r.state in ('running','cancel_requested') and l.released_at is null and l.valid_until<=clock_timestamp() and (r.seal_offer_after is null or r.seal_offer_after<=clock_timestamp())
        order by l.valid_until,r.id limit ${limit} for update of r,l skip locked`.execute(tx);
      const seals = [];
      for (const r of rows) {
        const id = r.output_seal_id ?? randomUUID();
        const {
          rows: [intent],
        } = await sql`update turns set output_seal_id=${id}::uuid,
          recovery_requested_at=coalesce(recovery_requested_at,clock_timestamp()),seal_offer_after=clock_timestamp()+interval '1 second'
          where id=${r.id}::uuid returning recovery_requested_at`.execute(tx);
        if (!r.output_seal_id)
          await sql`update session_leases set writer_failed_at=clock_timestamp(),tool_bindings_closed_at=clock_timestamp() where lease_id=${r.lease_id}::uuid`.execute(
            tx,
          );
        seals.push({
          kind: "execution_seal",
          factId: id,
          agentId: "root",
          recover: true,
          baseSequence: Number(r.output_display_seq),
          occurredAt: intent.recovery_requested_at.toISOString(),
          scope: {
            tenantId: r.tenant_id,
            sessionId: r.session,
            piSessionId: r.session,
            runId: r.id,
            turnId: r.id,
            writerId: r.lease_id,
            fencingToken: Number(r.fencing_token),
          },
        });
      }
      return seals;
    });
  }
  async publishExpired(publish, limit = 32) {
    const pending = await this.expired(limit);
    for (const seal of pending)
      await publish(kafkaProducerLane(seal.scope.sessionId, this.partitions), seal);
    return pending.length;
  }
  async reconcile({ publish, signal, intervalMs = 1000, onError }) {
    if (!Number.isSafeInteger(intervalMs) || intervalMs < 1)
      throw Error("Invalid lease reconciliation interval");
    while (!signal.aborted) {
      try {
        if ((await this.publishExpired(publish)) === 32) continue;
      } catch (e) {
        if (!onError) throw e;
        onError(e);
      }
      try {
        await delay(intervalMs, undefined, { signal });
      } catch (e) {
        if (!signal.aborted) throw e;
      }
    }
  }
  async projectBoundary({ fact, topic, partition, offset }, settleNative) {
    const { scope: s } = fact;
    if (
      topic !== this.topic ||
      partition !== kafkaProducerLane(s.piSessionId, this.partitions) ||
      offset < 0n ||
      s.sessionId !== s.piSessionId ||
      s.turnId !== s.runId
    )
      throw Error("Invalid Session boundary position");
    const recover = fact.kind === "execution_seal" && fact.recover === true;
    if (!recover && fact.kind !== "work_finished") throw Error("Unexpected Session boundary");
    return this.db.transaction().execute(async (tx) => {
      await sql`set local synchronous_commit=on`.execute(tx);
      const {
        rows: [r],
      } =
        await sql`with target as materialized (select * from sessions where tenant_id=${s.tenantId}::uuid and id=${s.sessionId}::uuid for no key update)
        select r.state,r.output_seal_id,r.native_work_handoff,r.projection_first,r.output_first_offset,r.session_id as flow_id,l.writer_failed_at
        from turns r join target s on s.tenant_id=r.tenant_id and s.flow_id=r.session_id join session_leases l on l.tenant_id=s.tenant_id and l.pi_session_id=s.id::text and l.lease_id=r.lease_id
        where r.tenant_id=${s.tenantId}::uuid and s.id=${s.sessionId}::uuid and r.id=${s.runId}::uuid and r.lease_id=${s.writerId}::uuid
          and r.fencing_token=${s.fencingToken} and l.fencing_token=r.fencing_token and l.released_at is null and r.state in ('running','cancel_requested')
        for update of r,l`.execute(tx);
      if (
        !r ||
        (recover
          ? r.output_seal_id !== fact.factId || !r.writer_failed_at
          : r.output_seal_id !== null)
      )
        return false;
      if (recover) {
        await settleNative(tx, r);
        if (
          r.projection_first !== null ||
          r.output_first_offset !== null ||
          r.native_work_handoff !== null
        ) {
          const { rows } =
            await sql`select from turns where id=${s.runId}::uuid and native_work_handoff->>'sealId'=${fact.factId}
            and native_work_handoff->>'offset'=${String(offset)} and native_work_handoff->>'topic'=${topic}
            and native_work_handoff#>>'{scope,writerId}'=${s.writerId}`.execute(tx);
          if (!rows.length) throw Error("No native checkpoint for this writer seal");
        }
      }

      await sql`update session_leases set writer_seal_offset=${String(offset)}::bigint,writer_sealed_at=clock_timestamp(),
        tool_bindings_closed_at=coalesce(tool_bindings_closed_at,clock_timestamp()),released_at=clock_timestamp()
        where lease_id=${s.writerId}::uuid`.execute(tx);
      if (recover) {
        // All admitted publications of this writer precede this seal. A remaining
        // reservation has no canonical entry and can safely be returned to PG.
        await sql`update user_or_agent_messages set state=case when withdrawing then 'withdrawn' else 'pending' end,
          withdrawing=false,boundary_task=null where tenant_id=${s.tenantId}::uuid and session_id=${s.sessionId}::uuid
          and turn_id=${s.runId}::uuid and state='sending'`.execute(tx);
        await sql`update user_or_agent_messages set steer_seen_epoch=null where turn_id=${s.runId}::uuid and
          mode='steer' and state='pending' and steer_seen_epoch=${s.fencingToken}`.execute(tx);
        await sql`update turns set stop_on_resume=(stop_on_resume or state='cancel_requested'),state='queued',lease_id=null,fencing_token=null,recovery_requested_at=null,seal_offer_after=null,output_seal_id=null,
          output_sealed_at=null,output_seal_offset=null,output_first_offset=null,output_first_partition=null,output_first_topic=null,
          projection_first=null,projection_partition=null,projection_pin=null,projection_state=null where id=${s.runId}::uuid`.execute(
          tx,
        );
      } else {
        const state = {
          "turn.completed": "completed",
          "turn.failed": "failed",
          "turn.cancelled": "cancelled",
        }[fact.terminal.type];
        if (!state) throw Error("Invalid terminal outcome");
        await sql`update turns set state=${state},settled_at=clock_timestamp(),output_seal_id=${fact.factId}::uuid,
          output_sealed_at=clock_timestamp(),output_seal_offset=${String(offset)}::bigint where id=${s.runId}::uuid`.execute(
          tx,
        );
        await settleNative(tx, r);
        if (fact.nativeSequence > 0) {
          const { rows } =
            await sql`select from turns where id=${s.runId}::uuid and history_head->>'version'=${fact.factId}
            and (history_head->>'seq')::bigint=${fact.nativeSequence}`.execute(tx);
          if (!rows.length) throw Error("Terminal native state is not projected");
        }
      }
      return true;
    });
  }
}
