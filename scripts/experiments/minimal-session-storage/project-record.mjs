import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import { isDeepStrictEqual } from "node:util";
import { kafkaProducerLane } from "@pi-cloud/runtime-core/kafka-accepted-fact";
import { applyImmutableBatches } from "@earendil-works/chord/delta";
import { EMPTY_HEAD, openingHead, priorState } from "./session-prefix.mjs";
import {
  finishStatement,
  oneMessage,
  oneInbox,
  insertMessages,
  upsertInbox,
} from "./projection-statements.mjs";
import { batchHash } from "./moving-checkpoint.mjs";

export async function projectRecord({ fact: f, partition, offset }, draft, environment) {
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
        assert(s.inboxId && s.value.items.length === 0, "Run finished before Inbox settlement");
        if (s.floorChanged) {
          const {
            rows: [covered],
          } = await c.query(
            `select count(*)::int n,coalesce(sum(octet_length(content)),0)::bigint bytes
            from messages where tenant_id=$1 and pi_session_id=$2 and conversation_id=1 and entry_id>=$3`,
            [tenantId, piSessionId, s.floor],
          );
          metrics.queries++;
          s.count = covered.n;
          s.bytes = Number(covered.bytes);
        }
        head = {
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
      inboxId = null,
      items = [];
    if (batch.seq !== 1) {
      if (stage?.integrated) {
        const {
          rows: [row],
        } = await pool.query(openingHead, [runId, tenantId, piSessionId]);
        metrics.queries++;
        if (row?.state !== "running" || row.lease_id !== writerId) {
          metrics.stale++;
          measure("obsolete");
          return;
        }
        // The predecessor settled with an empty Inbox. During rewind the PG
        // row can already contain this Run's later, still-pending additions.
        prior = priorState(draft?.restoring ? { ...row, inbox_items: [] } : row);
        inboxId = Number(row.inbox_id);
        items = draft?.restoring ? [] : row.inbox_items;
      } else {
        const prior = await pool.query(
          "select state,lease_id from runs where tenant_id=$1 and session_id=$2 and id=$3",
          [tenantId, piSessionId, runId],
        );
        metrics.queries++;
        if (prior.rows[0]?.state !== "running" || prior.rows[0]?.lease_id !== writerId) {
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
      inboxId,
      value: { items },
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
  let inboxId = s.inboxId,
    value = s.value,
    inboxOrdinal = -1;
  const messages = [],
    inputs = new Map();
  for (const w of batch.writes)
    if (w.type === "submission" && w.value.requestId && w.value.entry !== undefined)
      inputs.set(w.value.entry, w.value.requestId);
  for (const [ordinal, w] of batch.writes.entries()) {
    if (w.type === "document.create" && w.record.kind === "pi.live") s.liveId = w.record.id;
    if (w.type === "document.create" && w.record.kind === "pi.inbox") {
      assert.equal(w.content.kind, "base");
      assert.equal(w.record.scope.conversationId, 1);
      inboxId = w.record.id;
      value = w.content.value;
      inboxOrdinal = ordinal;
    } else if (w.type === "document.change" && w.id === inboxId) {
      value =
        w.content.kind === "base" ? w.content.value : applyImmutableBatches(value, [w.content.ops]);
      inboxOrdinal = ordinal;
    }
    if (w.type === "entry") {
      const role = w.value.model?.[0]?.role;
      if (stage?.integrated || ["user", "assistant", "toolResult"].includes(role))
        messages.push({
          id: w.value.id,
          conversation: w.value.conversationId,
          role: role ?? w.value.kind,
          content: JSON.stringify(w.value),
          input: inputs.get(w.value.id) ?? null,
          ordinal,
        });
      if (stage?.integrated && w.value.head !== undefined && (s.markerId ?? 0) < w.value.id) {
        s.markerId = w.value.id;
        s.floor = w.value.head;
        s.floorChanged = true;
      }
    }
  }
  const lastAssistant = messages.some((m) => m.role === "assistant");
  const kind =
    inboxOrdinal >= 0 && messages.length
      ? "inbox+messages"
      : inboxOrdinal >= 0
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
  if ((inboxOrdinal >= 0 || messages.length) && stage?.sink !== "terminal-only") {
    metrics.peakWaiting = Math.max(metrics.peakWaiting, pool.waitingCount);
    const acq = performance.now(),
      c = await borrow(),
      q = performance.now();
    poolMs = q - acq;
    const query = (...args) => {
      metrics.queries++;
      return c.query(...args);
    };
    try {
      if (
        stage.sink === "single" &&
        ((messages.length === 1 && inboxOrdinal < 0) || (!messages.length && inboxOrdinal >= 0))
      ) {
        const items = inboxOrdinal < 0 ? null : JSON.stringify(value.items);
        const {
          rows: [proof],
        } = await query(
          items === null ? oneMessage : oneInbox,
          items === null
            ? [tenantId, piSessionId, runId, batch.seq, JSON.stringify(messages), writerId]
            : [tenantId, piSessionId, inboxId, batch.seq, inboxOrdinal, items, runId, writerId],
        );
        if (!proof.accepted) {
          metrics.stale++;
          measure("obsolete", poolMs, performance.now() - q);
          return;
        }
        assert.equal(proof.writes, 1, "Conflicting immutable message or Inbox position");
        if (items !== null) {
          metrics.inboxSnapshots++;
          metrics.inboxBytes += Buffer.byteLength(items);
        }
      } else {
        await query("begin");
        const owner = await query(
          "select id from runs where tenant_id=$1 and session_id=$2 and id=$3 and lease_id=$4 and state='running' for share",
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
        if (inboxOrdinal >= 0) {
          const items = JSON.stringify(value.items);
          assert.equal(
            (
              await query(upsertInbox, [
                tenantId,
                piSessionId,
                inboxId,
                batch.seq,
                inboxOrdinal,
                items,
              ])
            ).rowCount,
            1,
          );
          metrics.inboxSnapshots++;
          metrics.inboxBytes += Buffer.byteLength(items);
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
    s.count += messages.length;
    for (const m of messages) s.bytes += Buffer.byteLength(m.content);
  }
  s.lastFact = f.factId;
  if (draft) s.lastOffset = offset;
  s.lastBatch = batch;
  s.value = value;
  s.inboxId = inboxId;
  if (lastAssistant) s.lastAnswerCommit = performance.now();
  views.set(runId, s);
  if (lastAssistant && stage?.holdFinish) publish(() => send("answerCommitted", { id: runId }));
  measure(kind, poolMs, pgMs, io);
  if (
    stage?.scene !== "plain" &&
    (inboxOrdinal >= 0 || messages.some((m) => m.role === "user" && m.input !== runId))
  ) {
    metrics.acknowledgedInputs++;
    if (draft) draft.barrier = true;
    publish(() =>
      send("confirmed", {
        id: runId,
        seq: batch.seq,
        at: Date.now(),
        depth: value.items.length,
        placed: messages.some((m) => m.role === "user" && m.input !== runId),
      }),
    );
  }
}
