// Single fresh Run, one handoff, current native reads only. This experiment has
// no Kafka client. Worker recovery can access the original reply mailbox separately.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { AcceptedFamilyCheckpoint } from "@pi-cloud/pi-session-postgres/durable";
import { ReadyProjection } from "./ready-projection.mjs";
import { projectionTemplates } from "./projection-statements.mjs";
import { projectRecord } from "./project-record.mjs";

const snapshotTable = "candidate.study_family_handoffs";
export async function installStudyHandoff(db) {
  await db.query(`create table ${snapshotTable} (
    old_writer uuid primary key, run_id uuid not null, topic text not null, partition int not null,
    seal_offset bigint not null, seal_id uuid not null, state jsonb not null);
    create role family_snapshot_reader login; grant usage on schema candidate to family_snapshot_reader;
    grant select on ${snapshotTable} to family_snapshot_reader;
    grant select (tenant_id,id,root_run_id,parent_session_id,parent_tool_call_id,child_session_id,child_turn_id,source_kind,source_record_id)
      on public.subagent_conversations to family_reader;
    grant select (tenant_id,id,pi_conversation_id) on public.sessions to family_reader;
    create view candidate.family_recovery_seals as
      select id, tenant_id, payload#>>'{scope,runId}' as run, payload#>>'{scope,sessionId}' as session,
        payload#>>'{scope,piSessionId}' as physical_session, payload#>>'{scope,writerId}' as writer,
        payload#>>'{scope,fencingToken}' as epoch
      from public.outbox where aggregate_type='session_terminal_event' and published_at is not null
        and payload->>'kind'='execution_seal' and payload->>'recover'='true';
    grant select on candidate.family_recovery_seals to family_reader;
    grant select (tenant_id,pi_session_id,lease_id,fencing_token,writer_seal_offset,writer_sealed_at)
      on public.session_leases to family_reader`);
}

// A counterfactual uses two savepoints on the same pre-commit state. Only the
// native requestId changes. Never send the alternative to Kafka, Pi or a Guest.
export async function compareQueuedRequests(pool, view, r, env, submission) {
  const c = await pool.connect();
  const snapshot = async () =>
    (
      await c.query(
        `select
    (select to_jsonb(r) from runs r where r.id=$1) as run,
    (select jsonb_agg(to_jsonb(m) order by entry_id) from messages m where run_id=$1) as entries,
    (select jsonb_agg(to_jsonb(i) order by conversation_id) from inboxes i where pi_session_id=$2) as inboxes,
    (select next_offset from accepted_fact_projection_offsets where topic=$3 and partition=$4) as position,
    (select jsonb_agg(to_jsonb(v) order by id) from public.subagent_conversations v where root_run_id=$1) as children`,
        [r.fact.scope.runId, r.fact.scope.piSessionId, r.topic, r.partition],
      )
    ).rows[0];
  try {
    await c.query("begin isolation level repeatable read");
    const project = async (record) => {
      await c.query("savepoint before_input");
      const copy = new ReadyProjection(
        pool,
        view.topic,
        structuredClone(view.sessions),
        projectionTemplates,
      );
      copy.configure(view.width, view.policy);
      copy.next = new Map(view.next);
      const e = {
        ...env,
        sessions: copy.sessions,
        metrics: structuredClone(env.metrics),
        nextOffsets: new Map(),
      };
      await copy.apply(
        [record],
        () => true,
        (item, d) => projectRecord(item, d, e),
        undefined,
        async () => ({ query: c.query.bind(c), release() {} }),
      );
      const result = await snapshot();
      await c.query("rollback to savepoint before_input");
      return result;
    };
    const before = await snapshot();
    const real = await project(r);
    const other = structuredClone(r);
    const alternative = other.fact.commit.writes.find(
      (w) => w.type === "submission" && w.value.id === submission.id,
    );
    alternative.value.requestId = randomUUID();
    assert.notEqual(alternative.value.requestId, submission.requestId);
    const changed = await project(other);
    assert.notDeepEqual(real.inboxes, before.inboxes);
    assert.equal(BigInt(real.position), r.offset + 1n);
    assert.deepEqual(
      changed,
      real,
      "The candidate persisted a distinction between the two requests",
    );
    assert(!JSON.stringify(real).includes(submission.requestId));
    return true;
  } finally {
    try {
      await c.query("rollback");
    } finally {
      c.release();
    }
  }
}

export class SealStudy extends AcceptedFamilyCheckpoint {
  async seal(db, r, from, bashFields) {
    const { fact: f } = r;
    const state = this.snapshot();
    if (bashFields) {
      const { omitVerifiedBashWait } = await import("./bash-wait-fields.mjs");
      await omitVerifiedBashWait(db, f.scope.runId, state);
    }
    const result = await db.query(
      `with advanced as (
      update candidate.accepted_fact_projection_offsets set next_offset=$1 where topic=$2 and partition=$3 and next_offset=$4 returning topic)
      insert into ${snapshotTable}(old_writer,run_id,topic,partition,seal_offset,seal_id,state)
      select $5,$6,topic,$3,$7,$8,$9::jsonb from advanced returning octet_length(state::text) as bytes`,
      [
        String(r.offset + 1n),
        r.topic,
        r.partition,
        String(from),
        f.scope.writerId,
        f.scope.runId,
        String(r.offset),
        f.factId,
        JSON.stringify(state),
      ],
    );
    assert.equal(result.rowCount, 1);
    return result.rows[0].bytes;
  }
}
