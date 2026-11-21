import { nativeNavigation } from "@pi-cloud/pi-session-postgres/durable";
import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import { isDeepStrictEqual } from "node:util";
import { kafkaProducerLane } from "@pi-cloud/runtime-core/kafka-accepted-fact";
import { foldInboxes, emptyInboxRoutes, reopenEmptyInboxes } from "./family-inboxes.mjs";
import { EMPTY_HEAD, openingHead, priorState } from "./session-prefix.mjs";
import {
  finishStatement,
  oneMessage,
  oneInbox,
  insertMessages,
  upsertInbox,
} from "./projection-statements.mjs";
import { batchHash } from "./moving-checkpoint.mjs";

export async function projectRecord(
  { fact: f, partition, offset, openingRow },
  draft,
  environment,
) {
  const {
    pool,
    sessions,
    stage,
    metrics,
    account,
    nextOffsets,
    send,
    pathStudy,
    tailProbe,
    partitions,
  } = environment;
  const t = performance.now(),
    wireMs = Date.now() - Date.parse(f.occurredAt),
    { tenantId, piSessionId, runId, writerId } = f.scope;
  assert.equal(kafkaProducerLane(piSessionId, partitions), partition);
  const views = draft?.views ?? sessions;
  const publish = (fn) => (draft ? draft.after.push(fn) : fn());
  const borrow = () =>
    draft ? draft.acquire(f) : pathStudy?.mode ? pathStudy.acquire(f) : pool.connect();
  let s = views.get(runId);
  const trace = tailProbe?.begin(partition, offset, f);
  const measure = (kind, poolMs = 0, pgMs = 0, io) => {
    if (draft && (kind === "obsolete" || kind === "duplicate")) draft.frame.rejected = true;
    publish(() => {
      trace?.complete(io);
      account(kind, t, poolMs, pgMs, wireMs);
      metrics.byPartition[partition]++;
      nextOffsets.set(partition, offset + 1n);
    });
  };
  metrics.firstReceipt ??= Date.now();
  if (f.kind === "work_finished") {
    if (s && s.writer !== writerId) {
      metrics.stale++;
      measure("obsolete");
      return;
    }
    assert.equal(f.terminal.type, "turn.completed");
    if (!s) {
      const prior = await pool.query(
        "select state,lease_id from runs where tenant_id=$1 and session_id=$2 and id=$3",
        [tenantId, piSessionId, runId],
      );
      metrics.queries++;
      if (prior.rows[0]?.lease_id !== writerId) {
        metrics.stale++;
        measure("obsolete");
        return;
      }
      assert.equal(
        prior.rows[0].state,
        "completed",
        "Fresh completion lacks a projected native head",
      );
      metrics.duplicate++;
      measure("duplicate");
      return;
    }
    assert.equal(f.nativeSequence, s.seq);
    assert(s.lastAnswerCommit !== undefined, "Completion lacks a preceding complete answer");
    const sqlStart = performance.now(),
      c = await borrow(),
      taken = performance.now();
    let result;
    try {
      let head;
      if (stage?.integrated) {
        assert.equal(f.baseSequence, s.baseSeq);
        const inboxRoutes = emptyInboxRoutes(s.inboxes);
        assert(inboxRoutes, "Run finished before Inbox settlement");
        head = {
          ...(s.floorChanged ? { recount: true } : {}),
          version: f.factId,
          seq: s.seq,
          nextId: s.nextId,
          floor: s.floor,
          count: s.count,
          bytes: s.bytes,
          ...(s.usageDocs ? { usageDocs: s.usageDocs } : {}),
        };
      }
      metrics.queries++;
      result = await c.query(finishStatement(!!head), [
        tenantId,
        piSessionId,
        runId,
        writerId,
        ...(head ? [JSON.stringify(head)] : []),
      ]);
    } finally {
      c.release();
    }
    const end = performance.now(),
      at = Date.now();
    measure(
      "work_finished",
      taken - sqlStart,
      end - taken,
      trace ? [sqlStart, taken, end].map((v) => performance.timeOrigin + v) : undefined,
    );
    if (result.rowCount === 0) {
      metrics.duplicate++;
      return;
    }

    views.delete(runId);
    publish(() => {
      metrics.lastCommit = draft ? Date.now() : at;
      if (pathStudy?.mode !== "projector")
        send("settled", {
          id: runId,
          at: metrics.lastCommit,
          terminalSinceAnswerMs: (draft ? performance.now() : end) - s.lastAnswerCommit,
          finishTransitMs: wireMs,
          finishHandlerMs: (draft ? performance.now() : end) - t,
          ...(trace ? { tail: trace.terminal } : {}),
        });
    });
    return;
  }
  assert.equal(f.kind, "durable_commit");
  const batch = f.commit;
  if (!s) {
    // If the final record already retired the view, an old Kafka delivery must
    // not re-open it. An active replacement needs native recovery beyond this assay.
    let prior = EMPTY_HEAD,
      inboxes = {};
    if (batch.seq !== 1) {
      if (stage?.integrated) {
        const row =
          openingRow === undefined
            ? (await pool.query(openingHead, [runId, tenantId, piSessionId])).rows[0]
            : openingRow;
        metrics.queries++;
        if (!["running", "cancel_requested"].includes(row?.state) || row.lease_id !== writerId) {
          metrics.stale++;
          measure("obsolete");
          return;
        }
        // The predecessor settled with an empty Inbox. During rewind the PG
        // row can already contain this Run's later, still-pending additions.
        prior = priorState(draft?.restoring ? { ...row, inbox_items: [] } : row);
        inboxes = reopenEmptyInboxes(row.inbox_routes);
        assert(inboxes[row.inbox_id]?.conversation === 1, "Predecessor root Inbox changed");
      } else {
        const prior = await pool.query(
          "select state,lease_id from runs where tenant_id=$1 and session_id=$2 and id=$3",
          [tenantId, piSessionId, runId],
        );
        metrics.queries++;
        if (
          !["running", "cancel_requested"].includes(prior.rows[0]?.state) ||
          prior.rows[0]?.lease_id !== writerId
        ) {
          metrics.stale++;
          measure("obsolete");
          return;
        }
        throw new Error("Active native suffix requires recovery before projection");
      }
    }
    s = {
      writer: writerId,
      seq: prior.seq,
      baseSeq: prior.seq,
      count: prior.count,
      bytes: prior.bytes,
      nextId: prior.nextId,
      ...(prior.usageDocs ? { usageDocs: prior.usageDocs } : {}),
      floor: prior.floor,
      inboxes,
      lastFact: null,
    };
  }
  if (s.writer !== writerId) {
    metrics.stale++;
    measure("obsolete");
    return;
  }
  if (batch.seq === s.seq) {
    assert.equal(f.factId, s.lastFact);
    if (s.lastBatch) assert(isDeepStrictEqual(batch, s.lastBatch));
    else assert.equal(batchHash(batch), s.lastHash, "Restored native commit changed");
    metrics.duplicate++;
    measure("duplicate");
    return;
  }
  assert.equal(batch.seq, s.seq + 1, "Native commit sequence gap");
  const { documents, changed } = foldInboxes(s.inboxes, batch.writes);
  const messages = [],
    inputs = new Map();
  for (const w of batch.writes)
    if (w.type === "submission" && w.value.requestId && w.value.entry !== undefined)
      inputs.set(w.value.entry, w.value.requestId);
  for (const [ordinal, w] of batch.writes.entries()) {
    if (
      w.type === "document.create" &&
      w.record.kind === "pi.live" &&
      w.record.scope.conversationId === 1
    )
      s.liveId = w.record.id;
    if (w.type === "entry") {
      const role = w.value.model?.[0]?.role;
      if (stage?.integrated || ["user", "assistant", "toolResult"].includes(role)) {
        const assistant =
          stage?.rootPresentation && role === "toolResult" && w.value.byTaskId != null
            ? (batch.writes.find(
                (v) =>
                  v.type === "task" &&
                  v.value.id === w.value.byTaskId &&
                  v.value.kind === "pi.tool",
              )?.value.input.assistant ?? s.pendingTools?.[w.value.byTaskId]?.assistantEntryId)
            : undefined;
        messages.push({
          id: w.value.id,
          conversation: w.value.conversationId,
          role: role ?? w.value.kind,
          ...(assistant != null ? { task: w.value.byTaskId, assistant } : {}),
          ...(stage?.rootPresentation ? { caption: nativeNavigation(w.value).preview } : {}),
          content: JSON.stringify(w.value),
          input: inputs.get(w.value.id) ?? null,
          ordinal,
        });
      }
      if (
        stage?.integrated &&
        w.value.conversationId === 1 &&
        w.value.head !== undefined &&
        (s.markerId ?? 0) < w.value.id
      ) {
        s.markerId = w.value.id;
        s.floor = w.value.head;
        s.floorChanged = true;
      }
    }
  }
  const rootMessages = messages.filter((m) => m.conversation === 1);
  const lastAssistant = rootMessages.some((m) => m.role === "assistant");
  const kind =
    changed.length > 0 && messages.length
      ? "inbox+messages"
      : changed.length > 0
        ? "inbox"
        : messages.length
          ? "messages"
          : batch.writes.length &&
              batch.writes.every((w) => w.type === "document.change" && w.id === s.liveId)
            ? "live-only"
            : "native-memory";
  let poolMs = 0,
    pgMs = 0;
  let io;
  if ((changed.length > 0 || messages.length) && stage?.sink !== "terminal-only") {
    metrics.peakWaiting = Math.max(metrics.peakWaiting, pool.waitingCount);
    const acq = performance.now(),
      c = await borrow(),
      q = performance.now();
    poolMs = q - acq;
    const query = (...args) => {
      metrics.queries++;
      return c.query(...args);
    };
    const inboxArgs = ({ id, conversation, ordinal, value }) => [
      tenantId,
      piSessionId,
      conversation,
      id,
      batch.seq,
      ordinal,
      JSON.stringify(value.items),
    ];
    const countInbox = (args) => {
      metrics.inboxSnapshots++;
      metrics.inboxBytes += Buffer.byteLength(args[6]);
    };
    try {
      if (stage.sink === "single" && messages.length + changed.length === 1) {
        const args = changed.length
          ? [...inboxArgs(changed[0]), runId, writerId]
          : [tenantId, piSessionId, runId, batch.seq, JSON.stringify(messages), writerId];
        const {
          rows: [proof],
        } = await query(changed.length ? oneInbox : oneMessage, args);
        if (!proof.accepted) {
          metrics.stale++;
          measure("obsolete", poolMs, performance.now() - q);
          return;
        }
        assert.equal(proof.writes, 1, "Conflicting immutable message or Inbox position");
        if (changed.length) countInbox(args);
      } else {
        await query("begin");
        const owner = await query(
          "select id from runs where tenant_id=$1 and session_id=$2 and id=$3 and lease_id=$4 and state in ('running','cancel_requested') for share",
          [tenantId, piSessionId, runId, writerId],
        );
        if (!owner.rowCount) {
          await query("rollback");
          metrics.stale++;
          measure("obsolete", poolMs, performance.now() - q);
          return;
        }
        if (messages.length)
          assert.equal(
            (
              await query(insertMessages, [
                tenantId,
                piSessionId,
                runId,
                batch.seq,
                JSON.stringify(messages),
              ])
            ).rowCount,
            messages.length,
          );
        for (const change of changed) {
          const args = inboxArgs(change);
          assert.equal((await query(upsertInbox, args)).rowCount, 1, "Conflicting Inbox position");
          countInbox(args);
        }
        await query("commit");
      }
    } catch (e) {
      await c.query("rollback");
      throw e;
    } finally {
      c.release();
    }
    pgMs = performance.now() - q;
    if (trace) io = [acq, q, q + pgMs].map((v) => performance.timeOrigin + v);
  } else metrics.skipped++;
  s.seq = batch.seq;
  if (stage?.integrated) {
    s.nextId = batch.nextId;
    s.count += rootMessages.length;
    for (const m of rootMessages) s.bytes += Buffer.byteLength(m.content);
  }
  s.lastFact = f.factId;
  if (draft) s.lastOffset = offset;
  s.lastBatch = batch;
  s.inboxes = documents;
  if (lastAssistant) s.lastAnswerCommit = performance.now();
  views.set(runId, s);
  if (lastAssistant && stage?.holdFinish) publish(() => send("answerCommitted", { id: runId }));
  measure(kind, poolMs, pgMs, io);
  if (
    stage?.scene !== "plain" &&
    (changed.length > 0 ||
      messages.some((m) => m.role === "user" && m.input != null && m.input !== runId))
  ) {
    metrics.acknowledgedInputs++;
    if (draft) draft.barrier = true;
    const placed = new Set(
      messages
        .filter((m) => m.role === "user" && m.input != null && m.input !== runId)
        .map((m) => m.conversation),
    );
    const affected = new Set([...changed.map((c) => c.conversation), ...placed]);
    const queues = Object.values(documents)
      .filter((doc) => affected.has(doc.conversation))
      .map((doc) => ({
        conversation: doc.conversation,
        depth: doc.value.items.length,
        placed: placed.has(doc.conversation),
      }));
    publish(() => send("confirmed", { id: runId, seq: batch.seq, at: Date.now(), queues }));
  }
}
