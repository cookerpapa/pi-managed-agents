// Called only on a cold opening after the first incarnation. The public control
// transaction has already sealed the old writer and admitted this one.
import assert from "node:assert/strict";
import { NATIVE_HUMAN_INPUT_DOCUMENT } from "@pi-cloud/protocol";
import { foldLive } from "./moving-checkpoint.mjs";

export async function reopenRootProjection(pool, scope, position, sequence) {
  const {
    rows: [r],
  } = await pool.query(
    `select r.native_work_handoff as h,r.projection_tools,r.projection_pin,
      i.doc_id,i.commit_seq,i.items,old.writer_seal_offset,old.fencing_token as predecessor
    from runs r join session_leases active on active.tenant_id=r.tenant_id and active.lease_id=r.lease_id
    left join session_leases old on old.tenant_id=r.tenant_id and old.pi_session_id=active.pi_session_id
      and old.lease_id=(r.native_work_handoff#>>'{scope,writerId}')::uuid and old.released_at is not null
    left join inboxes i on i.tenant_id=r.tenant_id and i.pi_session_id=r.session_id and i.conversation_id=1
    where r.id=$1 and r.session_id=$2 and r.tenant_id=$3 and r.lease_id=$4 and r.fencing_token=$5
      and active.fencing_token=r.fencing_token and active.pi_session_id=$2::uuid::text and r.mailbox_position=1`,
    [scope.runId, scope.piSessionId, scope.tenantId, scope.writerId, scope.fencingToken],
  );
  assert(r?.h, "No sealed root projection for the admitted writer");
  const h = r.h,
    state = JSON.parse(h.nativeText);
  assert(
    r.doc_id &&
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
      Number(r.commit_seq) <= state.seq,
    "Root projection does not match sealed input",
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
  return {
    agentRoutes: Object.fromEntries(
      writes
        .filter((w) => w.type === "document.create" && w.record.kind === "pi.agent")
        .map((w) => [w.record.id, w.record.scope.conversationId]),
    ),
    writer: scope.writerId,
    seq: state.seq,
    nextId: state.nextId,
    baseSeq: 0,
    floor: marker?.floor ?? 1,
    markerId: marker?.id,
    floorChanged: true,
    count: 0,
    bytes: 0,
    liveDocs,
    ...(liveId === undefined ? {} : { liveId: Number(liveId) }),
    pendingTools: r.projection_tools,
    ...(r.projection_pin == null ? {} : { pinOffset: BigInt(r.projection_pin) }),
    ...(human ? { rootInputDoc: { id: human.record.id, value: human.content.value } } : {}),
    inboxes: { [r.doc_id]: { conversation: 1, value: { items: r.items } } },
  };
}
