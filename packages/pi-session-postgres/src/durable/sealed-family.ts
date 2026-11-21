// Candidate cold-open contract for a fresh physical Session. Only the sealed
// native current values cross the ownership boundary. Guest addresses do not.
import type {
  ConversationRecord,
  MemoryStorage,
  Seq,
  StorageWrite,
} from "@earendil-works/pi-durable";
import { type Database } from "@pi-cloud/database";
import {
  durableToolState,
  parseToolReplyRecovery,
  type DurableSessionCommit,
  type ToolReplyRecovery,
} from "@pi-cloud/protocol";
import { sql, type Kysely, type Transaction } from "kysely";
import type { SealedNativeState } from "./accepted-family-checkpoint.ts";

type FamilyScope = {
  tenantId: string;
  piSessionId: string;
  sessionId: string;
  turnId: string;
  runId: string;
  writerId: string;
  fencingToken: number;
};
type NativeWrite = DurableSessionCommit["writes"][number];
type StoredHandoff = Omit<Handoff, "state"> & { nativeText: string };
type Handoff = {
  scope: FamilyScope;
  topic: string;
  partition: number;
  offset: string;
  sealId: string;
  state: SealedNativeState;
};
type PendingNativeTool = {
  taskId: number;
  conversationId: number;
  operationId: string;
  reply: ToolReplyRecovery;
};

function pendingReferences(state: SealedNativeState, refs: Record<string, PendingNativeTool>) {
  const tasks = new Map(
    state.records.flatMap(({ write }) =>
      write.type === "task" ? [[Number(write.value.id), write.value] as const] : [],
    ),
  );
  const operations = new Set<string>();
  return Object.entries(refs).map(([id, op]) => {
    if (String(op.taskId) !== id || !Number.isSafeInteger(op.conversationId) || !op.operationId)
      throw Error("Invalid sealed tool reference");
    const task = tasks.get(op.taskId),
      reply = parseToolReplyRecovery(op.reply);
    if (
      !task ||
      task.conversationId !== op.conversationId ||
      durableToolState(task)?.intent?.callId !== reply.toolCallId ||
      operations.has(op.operationId)
    )
      throw Error("Unmatched pending native tool at seal");
    operations.add(op.operationId);
    return {
      taskId: op.taskId,
      conversationId: op.conversationId,
      operationId: op.operationId,
      reply,
    };
  });
}

export async function saveSealedFamily(
  tx: Transaction<Database>,
  scope: FamilyScope,
  position: { topic: string; partition: number; offset: bigint },
  sealId: string,
  state: SealedNativeState,
): Promise<void> {
  // Keeping the encoded native values as text inside the envelope preserves
  // NUL and lone surrogates across PostgreSQL jsonb conversion.
  const handoff: StoredHandoff = {
    scope,
    topic: position.topic,
    partition: position.partition,
    offset: String(position.offset),
    sealId,
    nativeText: JSON.stringify(state),
  };
  const result = await sql<{
    refs: Record<string, PendingNativeTool>;
  }>`with durable as materialized (select set_config('synchronous_commit','on',true))
      update runs set native_work_handoff=${JSON.stringify(handoff)}::jsonb from durable
    where id=${scope.runId}::uuid and tenant_id=${scope.tenantId}::uuid and session_id=${scope.sessionId}::uuid
      and turn_id=${scope.turnId}::uuid and lease_id=${scope.writerId}::uuid and fencing_token=${scope.fencingToken}
      and output_seal_id=${sealId}::uuid and recovery_requested_at is not null and mailbox_position=1
      and output_first_topic=${position.topic} and output_first_partition=${position.partition} and output_first_offset<=${String(position.offset)}::bigint
      returning projection_tools as refs`.execute(tx);
  if (result.rows.length !== 1) throw Error("Native handoff does not match the requested Run seal");
  pendingReferences(state, result.rows[0]!.refs);
}

// Match Pi's nearest visible head at each native request cutoff, including an
// inherited prefix. Outstanding Tasks may also reference individual older Entries.
function sealedHistorySelection(state: SealedNativeState) {
  const conversations = new Map<number, ConversationRecord>(),
    activeTasks = new Set<number>(),
    points = new Map<number, Set<number>>(),
    pinned = new Set<number>();
  function goal(lane: number, through: number = state.nextId - 1) {
    const set = points.get(lane) ?? new Set<number>();
    set.add(through);
    points.set(lane, set);
  }
  goal(1);
  for (const { write } of state.records) {
    if (write.type === "conversation") conversations.set(write.value.id, write.value);
    if (write.type === "submission" && ["queued", "placed"].includes(write.value.status)) {
      goal(write.value.conversationId);
      if (write.value.entry !== undefined) pinned.add(write.value.entry);
    }
    if (write.type !== "task" || write.value.state.status === "terminal") continue;
    const t = write.value;
    goal(t.conversationId);
    activeTasks.add(t.id);
    const input = t.input as { assistant?: number } | null;
    if (t.kind === "pi.tool" && typeof input?.assistant === "number") pinned.add(input.assistant);
    const cp = ("checkpoint" in t.state ? t.state.checkpoint : undefined) as
      { cutoff?: number; tail?: number; assistant?: number } | undefined;
    if (t.kind === "pi.generation") {
      if (typeof cp?.assistant === "number") pinned.add(cp.assistant);
      if (typeof cp?.cutoff === "number") goal(t.conversationId, cp.cutoff);
    }
    if (t.kind === "pi.compaction" && typeof cp?.tail === "number") goal(t.conversationId, cp.tail);
  }
  for (const c of conversations.values()) {
    if (c.owner && activeTasks.has(c.owner.taskId)) goal(c.id);
    if (c.parent) pinned.add(c.parent.at);
  }
  const ranges: { lane: number; lo: number; hi: number }[] = [];
  for (const [lane, cutoffs] of points)
    for (const cutoff of cutoffs) {
      const path: { lane: number; hi: number }[] = [],
        seen = new Set<number>();
      let id = lane,
        hi = cutoff,
        head: SealedNativeState["heads"][number] | undefined;
      for (;;) {
        const c = conversations.get(id);
        if (!c || seen.has(id)) throw Error("Incomplete sealed Conversation ancestry");
        seen.add(id);
        path.push({ lane: id, hi });
        if (!head)
          for (const m of state.heads)
            if (m.conversationId === id && m.id <= hi && (!head || m.id > head.id)) head = m;
        if (!c.parent) break;
        id = c.parent.conversationId;
        hi = Math.min(hi, c.parent.at);
      }
      if (head) pinned.add(head.id);
      for (const r of path)
        if (r.hi >= (head?.floor ?? 1)) ranges.push({ ...r, lo: head?.floor ?? 1 });
    }
  return { ranges, pinned: [...pinned] };
}

export async function readSealedFamily(db: Kysely<Database>, scope: FamilyScope, topic: string) {
  return db
    .transaction()
    .setIsolationLevel("repeatable read")
    .setAccessMode("read only")
    .execute(async (tx) => {
      const {
        rows: [row],
      } = await sql<{
        handoff: StoredHandoff;
        refs: Record<string, PendingNativeTool>;
      }>`select r.native_work_handoff as handoff,r.projection_tools as refs
      from runs r join session_leases active on active.tenant_id=r.tenant_id and active.lease_id=r.lease_id
      join session_leases old on old.tenant_id=r.tenant_id and old.pi_session_id=active.pi_session_id
        and old.lease_id=(r.native_work_handoff#>>'{scope,writerId}')::uuid
        and old.fencing_token+1=active.fencing_token and old.writer_sealed_at is not null
        and old.released_at is not null and old.writer_seal_offset=(r.native_work_handoff->>'offset')::bigint
      where r.id=${scope.runId}::uuid and r.tenant_id=${scope.tenantId}::uuid and r.session_id=${scope.sessionId}::uuid
        and r.turn_id=${scope.turnId}::uuid and r.state in ('running','cancel_requested') and r.mailbox_position=1
        and active.pi_session_id=${scope.piSessionId} and active.lease_id=${scope.writerId}::uuid
        and r.fencing_token=active.fencing_token and active.fencing_token=${scope.fencingToken} and active.released_at is null
        and active.writer_failed_at is null and active.writer_sealed_at is null`.execute(tx);
      if (!row) throw Error("No matching sealed native handoff for the claimed Run");
      const { nativeText, ...source } = row.handoff;
      const h: Handoff = { ...source, state: JSON.parse(nativeText) };
      for (const k of ["tenantId", "piSessionId", "sessionId", "turnId", "runId"] as const)
        if (h.scope[k] !== scope[k]) throw Error("Sealed family identity changed");
      if (h.topic !== topic || h.scope.fencingToken + 1 !== scope.fencingToken)
        throw Error("Sealed family source changed");
      const { ranges, pinned } = sealedHistorySelection(h.state);
      const { rows: entries } = await sql<{
        entry_id: string;
        commit_seq: string;
        ordinal: number;
        content: string;
      }>`select entry_id,commit_seq,ordinal,content from messages
      where tenant_id=${scope.tenantId}::uuid and pi_session_id=${scope.piSessionId} and run_id=${scope.runId}::uuid
        and (entry_id=any(${pinned}::bigint[]) or exists(select 1 from unnest(${ranges.map((r) => r.lane)}::bigint[],${ranges.map((r) => r.lo)}::bigint[],${ranges.map((r) => r.hi)}::bigint[]) as v(lane,lo,hi)
          where v.lane=messages.conversation_id and messages.entry_id between v.lo and v.hi))
      order by commit_seq,ordinal`.execute(tx);
      const ids = new Set(entries.map((r) => Number(r.entry_id)));
      if (pinned.some((id) => !ids.has(id)))
        throw Error("Referenced native history is missing at the sealed boundary");
      const { rows: queues } = await sql<{
        doc_id: string;
        conversation_id: string;
        created_seq: string;
        commit_seq: string;
        items: unknown[];
      }>`select doc_id,conversation_id,created_seq,commit_seq,items
      from inboxes where tenant_id=${scope.tenantId}::uuid and pi_session_id=${scope.piSessionId}`.execute(
        tx,
      );
      const intents = pendingReferences(h.state, row.refs);
      return { handoff: h, entries, queues, intents };
    });
}

export type SealedFamilyRead = Awaited<ReturnType<typeof readSealedFamily>>;

// Feed a new Worker storage instance using native commit order, keeping Entry and
// Inbox bodies in their own tables. The adapter also restores Pi's allocation head.
function sealedFamilyWrites(family: SealedFamilyRead) {
  const { state } = family.handoff;
  if (
    !Number.isSafeInteger(state.seq) ||
    state.seq < 1 ||
    !Number.isSafeInteger(state.nextId) ||
    state.nextId < 2
  )
    throw Error("Invalid sealed native head");
  const batches = new Map<number, NativeWrite[]>();
  function add(seq: number, write: NativeWrite) {
    if (!Number.isSafeInteger(seq) || seq < 1 || seq > state.seq)
      throw Error("Write outside the sealed native prefix");
    const batch = batches.get(seq) ?? [];
    batch.push(write);
    batches.set(seq, batch);
  }
  for (const r of family.entries)
    add(Number(r.commit_seq), { type: "entry", value: JSON.parse(r.content) });
  for (const q of family.queues) {
    const id = Number(q.doc_id),
      conversationId = Number(q.conversation_id),
      created = Number(q.created_seq),
      updated = Number(q.commit_seq);
    if (updated < created) throw Error("Inbox update preceded creation");
    const content = { kind: "base", version: 1, value: { items: q.items } };
    add(created, {
      type: "document.create",
      record: {
        id,
        kind: "pi.inbox",
        scope: { kind: "conversation", conversationId },
        history: "latest",
        fork: "initial",
      },
      content: created === updated ? content : { ...content, value: { items: [] } },
    });
    if (created !== updated) add(updated, { type: "document.change", id, content });
  }
  for (const r of state.records) add(r.seq, r.write);
  return [...batches.entries()].sort(([a], [b]) => a - b);
}

// A fresh storage can rebuild this checkpoint without allocating a native Entry,
// publishing a commit, or assigning new identities to reserved-but-unwritten IDs.
export async function hydrateSealedFamily(
  family: SealedFamilyRead,
  store: Pick<MemoryStorage, "prepareCommit" | "mintId">,
) {
  const positions = new Map<number, number>(),
    state = family.handoff.state;
  let highestId = 1,
    lastSeq = 0;
  for (const [seq, raw] of sealedFamilyWrites(family)) {
    const writes = raw as unknown as readonly StorageWrite[];
    for (const w of writes) {
      const id = "value" in w ? w.value.id : "record" in w ? w.record.id : w.id;
      if (!Number.isSafeInteger(id) || id < 1 || id >= state.nextId)
        throw Error("Native ID outside sealed allocation");
      highestId = Math.max(highestId, id);
      positions.set(id, seq);
    }
    store.prepareCommit(writes, seq as Seq).apply();
    lastSeq = seq;
  }
  if (lastSeq < state.seq) store.prepareCommit([], state.seq as Seq).apply();
  for (let id = highestId + 1; id < state.nextId; id++)
    if (Number(await store.mintId()) !== id)
      throw Error("Native allocation changed during handoff");
  return positions;
}
