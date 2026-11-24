// Called only on a cold opening after the first incarnation. The public control
// transaction has already sealed the old writer and admitted this one.
import { projectChildOutcomes } from "./child-outcomes.mjs";
import { foldWorld } from "./conversation-world.mjs";
import assert from "node:assert/strict";
import { NATIVE_HUMAN_INPUT_DOCUMENT, parseSubagentTaskId } from "@pi-cloud/protocol";
import { foldLive } from "./moving-checkpoint.mjs";

export async function reopenRootProjection(
  pool,
  scope,
  position,
  sequence,
  withChildren = false,
  delegatedRuns = false,
  bashLog = false,
) {
  const {
    rows: [r],
  } = await pool.query(
    `select r.native_work_handoff as h,r.projection_tools ${bashLog ? "|| bash_wait_refs(r.tenant_id,r.session_id,r.id)" : ""} as projection_tools,r.projection_pin,r.mailbox_position,
      (select p.history_head from runs p where r.mailbox_position>1 and p.tenant_id=r.tenant_id and p.session_id=r.session_id
        and p.mailbox_position=r.mailbox_position-1 and p.state in ('completed','cancelled','failed')) as prior_head,
      (select jsonb_agg(jsonb_build_object('id',i.doc_id::text,'lane',i.conversation_id::text,'seq',i.commit_seq::text,'items',i.items)) from inboxes i where i.tenant_id=r.tenant_id and i.pi_session_id=r.session_id) as queues,old.writer_seal_offset,old.fencing_token as predecessor
    from runs r join session_leases active on active.tenant_id=r.tenant_id and active.lease_id=r.lease_id
    left join session_leases old on old.tenant_id=r.tenant_id and old.pi_session_id=active.pi_session_id
      and old.lease_id=(r.native_work_handoff#>>'{scope,writerId}')::uuid and old.released_at is not null
    where r.id=$1 and r.session_id=$2 and r.tenant_id=$3 and r.lease_id=$4 and r.fencing_token=$5
      and active.fencing_token=r.fencing_token and active.pi_session_id=$2::uuid::text`,
    [scope.runId, scope.piSessionId, scope.tenantId, scope.writerId, scope.fencingToken],
  );
  assert(r?.h, "No sealed root projection for the admitted writer");
  const h = r.h,
    state = JSON.parse(h.nativeText);
  assert(
    r.queues?.some((q) => Number(q.lane) === 1) &&
      h.scope.runId === scope.runId &&
      h.scope.turnId === scope.turnId &&
      h.scope.sessionId === scope.sessionId &&
      h.scope.piSessionId === scope.piSessionId &&
      Number(r.predecessor) === h.scope.fencingToken &&
      h.scope.tenantId === scope.tenantId &&
      Number(r.predecessor) + 1 === scope.fencingToken &&
      r.writer_seal_offset === h.offset &&
      h.topic === position.topic &&
      h.partition === position.partition &&
      BigInt(h.offset) < position.offset &&
      state.seq + 1 === sequence &&
      r.queues.every((q) => Number(q.seq) <= state.seq),
    "Root projection does not match sealed input",
  );
  if (Number(r.mailbox_position) > 1)
    assert(
      r.prior_head &&
        Number.isSafeInteger(r.prior_head.floor) &&
        r.prior_head.floor >= 1 &&
        r.prior_head.nextId <= state.nextId &&
        r.prior_head.seq < state.seq,
      "Missing predecessor at projection reopen",
    );
  const writes = state.records.map((v) => v.write),
    liveDocs = foldLive({}, writes);
  const human = writes.find(
    (w) =>
      w.record?.kind === NATIVE_HUMAN_INPUT_DOCUMENT &&
      w.record.key === scope.runId &&
      w.record.scope.conversationId === 1,
  );
  const liveId = Object.keys(liveDocs).find((id) => liveDocs[id].conversation === 1);
  const marker = state.heads.filter((h) => h.conversationId === 1).sort((a, b) => b.id - a.id)[0];
  const worlds = {};
  foldWorld(worlds, writes);
  if (withChildren || delegatedRuns) {
    const { rows } = await pool.query(
      delegatedRuns
        ? "select id, source_task_id from runs where tenant_id=$1 and root_run_id=$2"
        : "select id,parent_tool_call_id from subagent_conversations where tenant_id=$1 and root_run_id=$2",
      [scope.tenantId, scope.runId],
    );
    if (rows.length) {
      worlds.childCallers = {};
      for (const row of rows)
        (worlds.childCallers[
          delegatedRuns ? Number(row.source_task_id) : parseSubagentTaskId(row.parent_tool_call_id)
        ] ??= []).push(row.id);
    }
  }
  projectChildOutcomes(worlds, { scope, commit: { writes } }, undefined, undefined, delegatedRuns);
  return {
    ...worlds,
    agentRoutes: Object.fromEntries(
      writes
        .filter((w) => w.type === "document.create" && w.record.kind === "pi.agent")
        .map((w) => [w.record.id, w.record.scope.conversationId]),
    ),
    writer: scope.writerId,
    seq: state.seq,
    nextId: state.nextId,
    baseSeq: r.prior_head?.seq ?? 0,
    floor: marker?.floor ?? r.prior_head?.floor ?? 1,
    markerId: marker?.id,
    floorChanged: true,
    count: 0,
    bytes: 0,
    liveDocs,
    ...(liveId === undefined ? {} : { liveId: Number(liveId) }),
    pendingTools: r.projection_tools,
    ...(r.projection_pin == null ? {} : { pinOffset: BigInt(r.projection_pin) }),
    ...(human ? { rootInputDoc: { id: human.record.id, value: human.content.value } } : {}),
    inboxes: Object.fromEntries(
      r.queues.map((q) => [q.id, { conversation: Number(q.lane), value: { items: q.items } }]),
    ),
  };
}
