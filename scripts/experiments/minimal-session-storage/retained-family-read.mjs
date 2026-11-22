// Read-only experiment: one old writer in the interrupted Run, a confirmed seal,
// and optional settled root history from PG. Never grant Tool effects.
import assert from "node:assert/strict";
import { isDeepStrictEqual } from "node:util";
import { randomUUID } from "node:crypto";
import { KafkaLogConsumer } from "../../../packages/event-log/src/kafka-log-consumer.ts";
import { decodeWorkspaceFact, operationsFor } from "./workspace-operations.mjs";
import { InboxDoc } from "@earendil-works/pi-durable";

import { openingHead, priorState, readPrefix, restoreSettled } from "./session-prefix.mjs";

export async function loadPriorRoot(db, scope, storage) {
  const start = performance.now();
  const {
    rows: [row],
  } = await db.query(openingHead, [scope.runId, scope.tenantId, scope.piSessionId]);
  assert(
    ["running", "cancel_requested"].includes(row?.state) && row.lease_id === scope.writerId,
    "Unowned read",
  );
  // A predecessor can settle only with empty Inboxes. Recover its root at
  // that point, not the pending inputs of the interrupted successor.
  const prior = {
    ...row,
    tenant_id: scope.tenantId,
    session_id: scope.piSessionId,
    inbox_items: [],
  };
  const head = priorState(prior),
    prefix = await readPrefix(db, prior, head);
  if (head.version) {
    assert.deepEqual(
      Object.values(row.inbox_routes),
      [1],
      "Predecessor must be a settled root in this study",
    );
    restoreSettled(storage, prior, head, prefix.records);
    storage.nextId = Math.max(storage.nextId, head.nextId);
  }
  return {
    baseSequence: head.seq,
    priorPgEntries: prefix.records.length,
    priorPgReadMs: performance.now() - start,
  };
}

export async function readRetainedFamily(
  db,
  brokers,
  topic,
  seal,
  scope,
  storage,
  ctx,
  presentation,
) {
  const pgStart = performance.now(),
    base = storage.nextSeq - 1;
  const cut = BigInt(seal.offset);
  const {
    rows: [owner],
  } = await db.query(
    `select r.state,r.lease_id,r.projection_tools,r.projection_state,h.next_offset::text
    from runs r join accepted_fact_projection_offsets h on h.topic=$4 and h.partition=0
    where r.id=$1 and r.tenant_id=$2 and r.session_id=$3`,
    [scope.runId, scope.tenantId, scope.piSessionId, topic],
  );
  assert(
    ["running", "cancel_requested"].includes(owner?.state) &&
      owner.lease_id === scope.writerId &&
      BigInt(owner.next_offset) > cut,
    "Missing successor authority or unprojected seal",
  );
  const { rows } = await db.query(
    "select entry_id::int as id, conversation_id::int as lane,commit_seq::int as seq,ordinal,content from message_entries where tenant_id=$1 and pi_session_id=$2 and run_id=$3",
    [scope.tenantId, scope.piSessionId, scope.runId],
  );
  const { rows: queues } = await db.query(
    "select doc_id::int as id,conversation_id::int as lane,commit_seq::int as seq,items from inboxes where tenant_id=$1 and pi_session_id=$2",
    [scope.tenantId, scope.piSessionId],
  );
  const pgReadMs = performance.now() - pgStart;
  const intents = Object.values(owner.projection_tools);
  const pendingIds = new Set(intents.map((op) => op.operationId));
  const witnessed = new Map();
  const entries = new Map(rows.map((r) => [r.id, r]));
  const id = randomUUID(),
    originalWriter = seal.writerId;
  let seenSeal = false,
    failure,
    low,
    count = base,
    scanned = 0,
    skipped = 0,
    bytes = 0;
  const reader = new KafkaLogConsumer({
    brokers,
    topic,
    clientId: "cold-" + id,
    groupId: topic + "-cold-" + id,
    commitMessages: false,
    replayOffsets: async (bounds) =>
      new Map(
        bounds.map((b) => {
          low = b.low;
          assert(
            b.partition === 0 && b.low <= cut && b.high > cut,
            "Sealed prefix is outside retention",
          );
          return [b.partition, b.low];
        }),
      ),
    decode: (v) => decodeWorkspaceFact(v).fact,
    handler: async (r) => {
      if (r.offset > cut || failure) return;
      try {
        scanned++;
        const f = r.fact;
        if (r.offset === cut) {
          assert(
            f.kind === "execution_seal" &&
              f.recover === true &&
              f.factId === seal.factId &&
              f.scope.writerId === originalWriter &&
              f.scope.fencingToken === seal.epoch &&
              f.scope.tenantId === scope.tenantId &&
              f.scope.piSessionId === scope.piSessionId &&
              f.scope.runId === scope.runId,
            "Wrong takeover seal",
          );
          seenSeal = true;
          return;
        }
        if (
          f.scope?.tenantId !== scope.tenantId ||
          f.scope.piSessionId !== scope.piSessionId ||
          f.scope.runId !== scope.runId
        ) {
          skipped++;
          return;
        }
        // The PG predecessor covers earlier Runs. A second epoch within the
        // interrupted Run would need its own confirmed seal.
        if (f.kind === "execution_seal" || f.kind === "work_finished")
          throw Error("A preceding ownership interval needs a verified cold base");
        if (
          f.kind !== "durable_commit" ||
          f.scope.writerId !== originalWriter ||
          f.scope.fencingToken !== seal.epoch
        ) {
          skipped++;
          return;
        }
        const { seq, nextId, writes } = f.commit;
        if (
          seq === count &&
          isDeepStrictEqual(f, lastAccepted) &&
          isDeepStrictEqual(operationsFor(f), operationsFor(lastAccepted))
        )
          return;
        if (seq !== count + 1)
          throw Error(
            "Incomplete accepted native prefix: expected " + (count + 1) + ", got " + seq,
          );
        for (const op of operationsFor(f)) {
          if (op.action === "execute" && pendingIds.has(op.operationId))
            witnessed.set(op.operationId, op);
        }
        const recovered = writes.map((w, ordinal) => {
          if (w.type !== "entry") return w;
          const e = entries.get(w.value.id);
          assert(
            e && e.seq === seq && e.ordinal === ordinal && e.lane === w.value.conversationId,
            "PG Entry position differs",
          );
          const value = JSON.parse(e.content);
          assert.deepEqual(value, w.value);
          entries.delete(e.id);
          bytes += Buffer.byteLength(e.content);
          return { type: "entry", value };
        });
        storage.prepareCommit(recovered, seq).apply();
        storage.nextId = Math.max(storage.nextId, nextId);
        count = seq;
        presentation?.restore(f);
        lastAccepted = f;
      } catch (e) {
        failure = e;
      }
    },
  });
  let lastAccepted;
  const readStart = performance.now();
  let assignedAt, caughtAt;
  try {
    await reader.start();
    await reader.waitUntilAssigned(25000);
    assignedAt = performance.now();
    await reader.waitUntilInitialReplay([cut + 1n], 30000);
    caughtAt = performance.now();
  } finally {
    await reader.close();
  }
  if (failure) throw failure;
  assert(seenSeal && count > base && entries.size === 0, "Incomplete cold import at seal");
  assert(
    queues.some((q) => q.lane === 1),
    "Missing root Inbox",
  );
  for (const q of queues) {
    assert.equal(
      (
        await storage.findDocument(
          {
            scope: { kind: "conversation", conversationId: q.lane },
            kind: InboxDoc.definition.kind,
          },
          "current",
          ctx,
        )
      )?.id,
      q.id,
    );
    assert.deepEqual((await storage.document(q.id, "current", ctx)).value.items, q.items);
  }
  const latestProjected = Math.max(
    owner.projection_state?.seq ?? 0,
    rows.reduce((n, r) => Math.max(n, r.seq), 0),
    queues.reduce((n, q) => Math.max(n, q.seq), 0),
    intents.reduce((n, op) => Math.max(n, op.seq), 0),
  );
  assert.equal(count, latestProjected, "Last native commit lacks matching PG acceptance evidence");
  for (const original of intents) {
    const t = await storage.task(original.taskId, ctx);
    assert(
      t?.state.checkpoint?.phase === "execute" &&
        t.conversationId === original.conversationId &&
        t.input.callId === original.reply.toolCallId,
      "Pending Task and original reply disagree",
    );
  }
  return {
    intents: intents.map((op) => {
      const wire = witnessed.get(op.operationId);
      assert(wire && wire.taskId === op.taskId && wire.conversationId === op.conversationId);
      assert.deepEqual(wire.reply, op.reply);
      return op;
    }),
    stats: {
      firstAvailable: String(low),
      commits: count - base,
      pgReadMs,
      readerJoinMs: assignedAt - readStart,
      logCatchupMs: caughtAt - assignedAt,
      readerCloseMs: performance.now() - caughtAt,
      scanned,
      skipped,
      stableEntries: rows.length,
      pgBodyBytes: bytes,
      conversations: queues.length,
      protectedTools: intents.length,
    },
  };
}
