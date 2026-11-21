// Only PG is available to this cold reader. Its control uses a different
// read-only role after the candidate-only test has finished.
import assert from "node:assert/strict";
import pg from "pg";
import { MemoryStorage } from "@earendil-works/pi-durable";
import { DurableAgentSession } from "../../../packages/sandbox-supervisor/src/durable/agent-session.ts";

async function readCandidateAtSeal(db, scope, topic) {
  const { rows: owners } = await db.query(
    `select r.projection_state,r.projection_tools,
      l.writer_seal_offset::text,l.lease_id as old_writer,o.id as seal_id,h.next_offset::text
    from public.runs r join public.session_leases l on l.tenant_id=r.tenant_id and l.pi_session_id=$5
    join public.outbox o on o.tenant_id=r.tenant_id and o.aggregate_type='session_terminal_event' and o.published_at is not null
      and o.payload->>'kind'='execution_seal' and o.payload->>'recover'='true'
      and o.payload#>>'{scope,writerId}'=l.lease_id::text and o.payload#>>'{scope,runId}'=r.id::text
      and o.payload#>>'{scope,piSessionId}'=l.pi_session_id and o.payload#>>'{scope,sessionId}'=r.session_id::text
      and o.payload#>>'{scope,fencingToken}'=l.fencing_token::text
    join candidate.accepted_fact_projection_offsets h on h.topic=$4 and h.partition=0
    where r.id=$1 and r.lease_id=$2 and l.fencing_token=$3 and l.writer_sealed_at is not null`,
    [scope.runId, scope.writerId, scope.fencingToken - 1, topic, scope.piSessionId],
  );
  assert.equal(owners.length, 1, "Expected this Run's verified predecessor");
  const [owner] = owners;
  assert(
    owner.old_writer !== scope.writerId &&
      BigInt(owner.next_offset) > BigInt(owner.writer_seal_offset),
  );
  const { rows: entries } = await db.query(
    "select commit_seq,ordinal,content from candidate.messages where run_id=$1 order by commit_seq,ordinal",
    [scope.runId],
  );
  const { rows: queues } = await db.query(
    "select doc_id,created_seq,commit_seq,items,conversation_id from candidate.inboxes where tenant_id=$1 and pi_session_id=$2",
    [scope.tenantId, scope.piSessionId],
  );
  const { rows: children } = await db.query(
    `select c.id,c.parent_tool_call_id,c.source_record_id,s.pi_conversation_id from public.subagent_conversations c join public.sessions s on s.tenant_id=c.tenant_id and s.id=c.child_session_id where c.tenant_id=$1 and c.root_run_id=$2`,
    [scope.tenantId, scope.runId],
  );
  assert.equal(children.length, 1);
  const [child] = children;
  const ops = Object.values(owner.projection_tools);
  assert.equal(ops.length, 1);
  assert.equal(ops[0].conversationId, Number(child.pi_conversation_id));
  const queued = queues.find((q) => Number(q.conversation_id) === ops[0].conversationId)?.items;
  assert.equal(queued?.length, 1);
  assert.equal(queued[0].mode, "followUp");
  assert(!Object.hasOwn(queued[0], "requestId"));
  return { owner, entries, queues, ops, child, queued: queued[0] };
}

// Recreate current native records for the measured family. Entry bodies and
// Inbox contents always come from the candidate, including in the positive control.
function importFamily(store, rows, state, presentation) {
  const groups = new Map();
  function append(seq, write) {
    seq = Number(seq);
    const batch = groups.get(seq) ?? [];
    batch.push(write);
    groups.set(seq, batch);
  }
  if (!state) append(1, { type: "conversation", value: { id: 1 } });
  for (const r of rows.entries)
    append(r.commit_seq, { type: "entry", value: JSON.parse(r.content) });
  for (const r of rows.queues) {
    const record = {
      id: Number(r.doc_id),
      kind: "pi.inbox",
      scope: { kind: "conversation", conversationId: Number(r.conversation_id) },
      history: "latest",
      fork: "initial",
    };
    const content = { kind: "base", version: 1, value: { items: r.items } };
    append(r.created_seq, {
      type: "document.create",
      record,
      content: r.created_seq === r.commit_seq ? content : { ...content, value: { items: [] } },
    });
    if (r.created_seq !== r.commit_seq)
      append(r.commit_seq, { type: "document.change", id: record.id, content });
  }
  for (const row of state?.records ?? []) {
    append(row.seq, row.write);
    const { record, content } = row.write;
    if (record?.kind === "pi.live")
      presentation.live[record.id] = {
        conversation: record.scope.conversationId,
        value: content.value,
      };
  }
  for (const [seq, batch] of [...groups].sort(([a], [b]) => a - b))
    store.prepareCommit(batch, seq).apply();
  if (!state) return;
  assert(store.nextSeq <= state.seq + 1 && store.nextId <= state.nextId);
  store.nextSeq = state.seq + 1;
  store.nextId = state.nextId;
  presentation.sequences = new Map(state.sequences);
}
async function readStudySnapshot(db, scope, owner, topic) {
  const {
    rows: [s],
  } = await db.query(
    `select state from candidate.study_family_handoffs where old_writer=$1 and run_id=$2 and topic=$3 and partition=0 and seal_offset=$4 and seal_id=$5`,
    [owner.old_writer, scope.runId, topic, owner.writer_seal_offset, owner.seal_id],
  );
  assert(s, "Missing matched seal state");
  return s.state;
}

export async function readFamilyPgControl({
  db,
  dbUrl,
  scope,
  topic,
  store,
  ctx,
  presentation,
  models,
  rootConfig,
}) {
  const readStart = performance.now();
  const rows = await readCandidateAtSeal(db, scope, topic);
  const candidateMs = performance.now() - readStart;
  const baseline = new MemoryStorage();
  importFamily(baseline, rows);
  const [original] = rows.ops;
  assert.equal(await baseline.task(original.taskId, ctx), undefined);
  assert.equal(await baseline.submission(rows.queued.id, ctx), undefined);
  assert.equal(
    (await baseline.scanTasks({ status: "waiting" }, 10, undefined, ctx)).items.length,
    0,
  );
  const partial = await DurableAgentSession.open(baseline, {
    models,
    remoteTools: [],
    infrastructureTools: [],
  });
  const inspected = (await partial.harness.inspect(ctx)).tasks.length;
  try {
    assert.equal(inspected, 0);
    await assert.rejects(
      partial.bind(rootConfig, "reattach"),
      /Native child restoration state is missing or changed/,
    );
  } finally {
    await partial.close();
  }
  const denied = await db.query(
    "select has_table_privilege(current_user,'candidate.study_family_handoffs','select') or has_table_privilege(current_user,'public.pi_durable_current','select') as leaked",
  );
  assert.equal(denied.rows[0].leaked, false);

  // Only the positive control has a connection to the private seal snapshot.
  const copyStart = performance.now();
  const url = new URL(dbUrl);
  url.username = "family_snapshot_reader";
  const snapDb = new pg.Client({
    connectionString: url.toString(),
    options: "-c default_transaction_read_only=on",
  });
  let state;
  try {
    await snapDb.connect();
    state = await readStudySnapshot(snapDb, scope, rows.owner, topic);
  } finally {
    await snapDb.end();
  }
  importFamily(store, rows, state, presentation);
  const sub = await store.submission(rows.queued.id, ctx);
  assert.equal(sub.status, "queued");
  const queuedKey = sub.requestId;
  assert.equal(
    (await store.submissionByRequest(original.conversationId, queuedKey, ctx)).id,
    sub.id,
  );
  const target = state.targets.find((t) => t.taskId === original.taskId);
  assert.equal(target.operationId, original.operationId);
  assert.equal(target.writerId, rows.owner.old_writer);
  const tasks = (await store.scanTasks({}, 100, undefined, ctx)).items;
  const restored = {
    intents: [{ ...original, ...target }],
    stats: {
      candidateOnlyTasksVisibleToPi: inspected,
      candidateOnlyReattachRejected: true,
      candidateEntries: rows.entries.length,
      inboxes: rows.queues.length,
      storedToolRefs: rows.ops.length,
      projectorCheckpointSeq: rows.owner.projection_state?.seq ?? null,
      sealNativeSeq: state.seq,
      nativeTasks: tasks.map((t) => ({ kind: t.kind, status: t.state.status })),
      documents: state.records
        .filter((r) => r.write.type === "document.create")
        .map((r) => r.write.record.kind),
      submissions: state.records.filter((r) => r.write.type === "submission").length,
      candidateReadMs: candidateMs,
      snapshotReadAndImportMs: performance.now() - copyStart,
    },
  };
  const evidence = {
    oldWriter: rows.owner.old_writer,
    sealOffset: rows.owner.writer_seal_offset,

    candidateChild: {
      id: rows.child.id,
      sourceTask: Number(rows.child.source_record_id),
      childConversationId: Number(rows.child.pi_conversation_id),
    },
  };
  return { restored, evidence, queuedKey };
}
