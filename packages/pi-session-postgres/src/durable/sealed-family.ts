// Candidate cold-open contract for an interrupted Run. Only the sealed
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
import { AcceptedFamilyCheckpoint, type SealedNativeState } from "./accepted-family-checkpoint.ts";

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

// The candidate worker and projector both require the same immediate predecessor.
// At a recoverable seal the current lease may already be marked closed inside
// this transaction, so only a Worker cold-read also requires an active writer.
function predecessor(scope: FamilyScope) {
  return sql`from runs r join session_leases active on active.tenant_id=r.tenant_id and active.lease_id=r.lease_id
      join session_leases old on old.tenant_id=r.tenant_id and old.pi_session_id=active.pi_session_id
        and old.lease_id=(r.native_work_handoff#>>'{scope,writerId}')::uuid
        and old.fencing_token+1=active.fencing_token and old.writer_sealed_at is not null
        and old.released_at is not null and old.writer_seal_offset=(r.native_work_handoff->>'offset')::bigint
      where r.id=${scope.runId}::uuid and r.tenant_id=${scope.tenantId}::uuid and r.session_id=${scope.sessionId}::uuid
        and r.turn_id=${scope.turnId}::uuid
        and active.pi_session_id=${scope.piSessionId} and active.lease_id=${scope.writerId}::uuid
        and r.fencing_token=active.fencing_token and active.fencing_token=${scope.fencingToken}`;
}

function decodePredecessor(stored: StoredHandoff, scope: FamilyScope, topic: string): Handoff {
  const { nativeText, ...source } = stored;
  for (const k of ["tenantId", "piSessionId", "sessionId", "turnId", "runId"] as const)
    if (source.scope[k] !== scope[k]) throw Error("Sealed family identity changed");
  if (source.topic !== topic || source.scope.fencingToken + 1 !== scope.fencingToken)
    throw Error("Sealed family source changed");
  return { ...source, state: JSON.parse(nativeText) };
}

export async function resumeFamilyCheckpoint(
  db: Kysely<Database>,
  scope: FamilyScope,
  position: { topic: string; partition: number; offset: bigint },
  atEmptySeal = false,
) {
  const {
    rows: [row],
  } = await sql<{ handoff: StoredHandoff; inboxes: { id: string; created: string }[] | null }>`
    select r.native_work_handoff as handoff,
      (select jsonb_agg(jsonb_build_object('id',i.doc_id::text,'created',i.created_seq::text))
       from inboxes i where i.tenant_id=r.tenant_id and i.pi_session_id=${scope.piSessionId}) as inboxes
    ${predecessor(scope)} and r.state in ('running','cancel_requested')
    and (not ${atEmptySeal}::boolean or r.output_first_offset is null)`.execute(db);
  if (!row) throw Error("No matched predecessor for the admitted family prefix");
  const h = decodePredecessor(row.handoff, scope, position.topic);
  if (h.partition !== position.partition || BigInt(h.offset) >= position.offset)
    throw Error("Invalid predecessor seal position");
  return AcceptedFamilyCheckpoint.resume(
    scope.writerId,
    h.state,
    (row.inboxes ?? [])
      .filter((i) => BigInt(i.created) <= BigInt(h.state.seq))
      .map((i) => Number(i.id)),
  );
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
  }>`with durable as materialized (select set_config('synchronous_commit','on',true), ${JSON.stringify(handoff)}::jsonb as handoff)
      update runs as target set native_work_handoff=durable.handoff from durable
    where id=${scope.runId}::uuid and tenant_id=${scope.tenantId}::uuid and session_id=${scope.sessionId}::uuid
      and turn_id=${scope.turnId}::uuid and lease_id=${scope.writerId}::uuid and fencing_token=${scope.fencingToken}
      and output_seal_id=${sealId}::uuid and recovery_requested_at is not null
      and (target.mailbox_position=1 or exists (
        select 1 from runs prior where prior.tenant_id=target.tenant_id and prior.session_id=target.session_id
          and prior.mailbox_position=target.mailbox_position-1 and prior.state in ('completed','cancelled')
          and (prior.history_head->>'seq')::bigint between 1 and ${state.seq - 1}
          and (prior.history_head->>'nextId')::bigint between 2 and ${state.nextId}))
      and ((output_first_topic=${position.topic} and output_first_partition=${position.partition} and output_first_offset<=${String(position.offset)}::bigint)
        or (output_first_offset is null and output_first_topic is null and output_first_partition is null
          and exists (select 1 ${predecessor(scope)}
            and r.native_work_handoff->>'topic'=${position.topic}
            and (r.native_work_handoff->>'partition')::int=${position.partition}
            and old.writer_seal_offset<${String(position.offset)}::bigint
            and r.native_work_handoff->>'nativeText'=durable.handoff->>'nativeText')))
      returning projection_tools as refs`.execute(tx);
  if (result.rows.length !== 1) throw Error("Native handoff does not match the requested Run seal");
  pendingReferences(state, result.rows[0]!.refs);
}

// Match Pi's nearest visible head at each native request cutoff, including an
// inherited prefix. Outstanding Tasks may also reference individual older Entries.
function sealedHistorySelection(
  state: SealedNativeState,
  delegatedRuns = false,
  previousNextId = 0,
) {
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
  // Several children can share one generation owner. Only a still-live
  // delegation Tool needs its child's original submission to return a result.
  const callers = new Map<number, { task: number; request: string }>();
  if (delegatedRuns)
    for (const { write } of state.records) {
      if (
        write.type !== "document.create" ||
        write.record.kind !== "pi-cloud.conversation-creation" ||
        write.content.kind !== "base"
      )
        continue;
      const v = write.content.value;
      if (
        typeof v.conversationId === "number" &&
        typeof v.ownerTaskId === "number" &&
        typeof write.record.key === "string"
      )
        callers.set(v.conversationId, { task: v.ownerTaskId, request: write.record.key });
    }
  for (const { write } of delegatedRuns ? state.records : []) {
    if (
      write.type !== "submission" ||
      write.value.type !== "input" ||
      write.value.status !== "done"
    )
      continue;
    const caller = callers.get(write.value.conversationId);
    if (caller && activeTasks.has(caller.task) && caller.request === write.value.requestId)
      pinned.add(write.value.answer);
  }
  const archives = new Map<number, { at: number; cuts: Set<number> }>();
  for (const c of conversations.values()) {
    const owner = callers.get(c.id)?.task ?? c.owner?.taskId;
    if (owner !== undefined && activeTasks.has(owner)) goal(c.id);
    if (!delegatedRuns && c.parent) pinned.add(c.parent.at);
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
        if (
          !c &&
          delegatedRuns &&
          path.length &&
          id > 1 &&
          id < previousNextId &&
          hi < previousNextId
        ) {
          // The direct child fixes the fork point. A live task may also need
          // an older cutoff on that path, e.g. while compacting the child.
          const at = conversations.get(path[path.length - 1]!.lane)!.parent!.at;
          if (at >= previousNextId) throw Error("Invalid archived fork position");
          const source = archives.get(id) ?? { at, cuts: new Set<number>() };
          if (source.at !== at) throw Error("Inconsistent delegated continuation anchor");
          source.cuts.add(hi);
          archives.set(id, source);
          break;
        }
        if (!c || seen.has(id)) throw Error("Incomplete sealed Conversation ancestry");
        seen.add(id);
        path.push({ lane: id, hi });
        if (!head)
          for (const m of state.heads)
            if (m.conversationId === id && m.id <= hi && (!head || m.id > head.id)) head = m;
        if (!c.parent) break;
        if (delegatedRuns && conversations.has(c.parent.conversationId)) pinned.add(c.parent.at);
        id = c.parent.conversationId;
        hi = Math.min(hi, c.parent.at);
      }
      if (head) pinned.add(head.id);
      for (const r of path)
        if (r.hi >= (head?.floor ?? 1)) ranges.push({ ...r, lo: head?.floor ?? 1 });
    }
  return {
    ranges,
    pinned: [...pinned],
    archives: [...archives].map(([lane, { at, cuts }]) => ({ lane, at, cuts: [...cuts] })),
    deferred: [...conversations.keys()].filter((lane) => !points.has(lane)),
    lanes: [...conversations.keys()],
  };
}

export async function readSealedFamily(
  db: Kysely<Database>,
  scope: FamilyScope,
  topic: string,
  { delegatedRuns = false } = {},
) {
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
        slot: string;
        prior: { seq: number; nextId: number; floor: number; count: number; bytes: number } | null;
      }>`select r.native_work_handoff as handoff,r.projection_tools as refs,r.mailbox_position as slot,
       (select p.history_head from runs p where p.tenant_id=r.tenant_id and p.session_id=r.session_id and
        r.mailbox_position>1 and p.mailbox_position=r.mailbox_position-1 and p.state in ('completed','cancelled')) as prior
      ${predecessor(scope)} and r.state in ('running','cancel_requested') and active.released_at is null
        and active.writer_failed_at is null and active.writer_sealed_at is null`.execute(tx);
      if (!row) throw Error("No matching sealed native handoff for the claimed Run");
      const h = decodePredecessor(row.handoff, scope, topic);
      const { ranges, pinned, archives, deferred, lanes } = sealedHistorySelection(
        h.state,
        delegatedRuns,
        row.prior?.nextId,
      );
      const prior = row.prior;
      if (
        Number(row.slot) > 1 &&
        (!prior ||
          !Number.isSafeInteger(prior.seq) ||
          prior.seq < 1 ||
          prior.seq >= h.state.seq ||
          !Number.isSafeInteger(prior.floor) ||
          !Number.isSafeInteger(prior.nextId) ||
          prior.floor < 1 ||
          prior.nextId <= prior.floor ||
          prior.nextId > h.state.nextId ||
          !Number.isSafeInteger(prior.count) ||
          prior.count < 1 ||
          !Number.isSafeInteger(prior.bytes) ||
          prior.bytes < 0)
      )
        throw Error("Missing matched settled root before family handoff");
      const selected = sql`(entry_id=any(${pinned}::bigint[]) or exists(select 1 from
        unnest(${ranges.map((r) => r.lane)}::bigint[],${ranges.map((r) => r.lo)}::bigint[],${ranges.map((r) => r.hi)}::bigint[])
        as v(lane,lo,hi) where v.lane=messages.conversation_id and messages.entry_id between v.lo and v.hi))`;
      const bodies = sql`select entry_id,conversation_id,commit_seq,ordinal,content from messages
        where tenant_id=${scope.tenantId}::uuid and pi_session_id=${scope.piSessionId}`;
      // Each arm can use its own index: this Run's ID, or the inherited root Entry range.
      // Their native sequence intervals are disjoint.
      const inheritedRoot = delegatedRuns
        ? sql`true`
        : sql`conversation_id=1 and entry_id>=${prior?.floor ?? 1}`;
      const inherited = prior
        ? sql`union all ${bodies} and ${inheritedRoot}
        and entry_id<${prior.nextId} and commit_seq<=${prior.seq} and ${selected}`
        : sql``;
      const { rows: entries } = await sql<{
        entry_id: string;
        conversation_id: string;
        commit_seq: string;
        ordinal: number;
        content: string;
      }>`${bodies} and run_id=${scope.runId}::uuid and commit_seq>${prior?.seq ?? 0}
        and commit_seq<=${h.state.seq} and ${selected}
        ${inherited} order by commit_seq,ordinal`.execute(tx);
      if (
        prior &&
        ranges.some((r) => r.lane === 1 && r.lo <= prior.floor && r.hi >= prior.nextId - 1)
      ) {
        const prefix = entries.filter(
          (e) =>
            Number(e.commit_seq) <= prior.seq &&
            Number(e.conversation_id) === 1 &&
            Number(e.entry_id) >= prior.floor,
        );
        if (
          prefix.length !== prior.count ||
          prefix.reduce((n, e) => n + Buffer.byteLength(e.content), 0) !== prior.bytes
        )
          throw Error("Incomplete inherited root at the sealed boundary");
      }
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
      from inboxes where tenant_id=${scope.tenantId}::uuid and pi_session_id=${scope.piSessionId}
        ${delegatedRuns ? sql`and conversation_id=any(${lanes}::bigint[])` : sql``}`.execute(tx);
      const intents = pendingReferences(h.state, row.refs);
      return {
        handoff: h,
        entries,
        queues,
        intents,
        ...(delegatedRuns ? { archives, deferred } : {}),
        settledThrough: prior?.seq ?? 0,
      };
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
