// Reopen a *normally settled* family. Historical Conversations can provide fork
// context but have no runnable Tasks. An interrupted family uses its sealed
// task/submission checkpoint instead. Keep original IDs and commit positions.
import assert from "node:assert/strict";
import { agentBaseWrite } from "./agent-configuration.mjs";
import { openingHead, priorState } from "./session-prefix.mjs";

export async function loadSettledDelegations(pool, scope, store) {
  const db = await pool.connect();
  try {
    await db.query("begin isolation level repeatable read read only");
    const row = (await db.query(openingHead, [scope.runId, scope.tenantId, scope.piSessionId]))
      .rows[0];
    assert(row?.lease_id === scope.writerId && row.state === "running", "Unexpected root owner");
    const head = priorState(row);
    if (!head.version) {
      await db.query("commit");
      return { entries: 0 };
    }
    const args = [scope.tenantId, scope.piSessionId, head.seq];
    const { rows: lanes } = await db.query(
      `select c.native_lane as lane,c.context_at as at,c.native_created_seq as seq,c.native_owner as owner,
      coalesce(caller.native_lane,1) as caller_lane,
      coalesce(p.native_lane,1) as parent from runs c join runs p on p.id=coalesce(c.previous_run_id,c.caller_run_id)
      join runs caller on caller.id=c.caller_run_id
      where c.tenant_id=$1 and c.session_id=$2 and c.native_lane is not null and c.native_outcome_seq<=$3`,
      args,
    );
    const groups = new Map();
    function add(seq, write) {
      const key = Number(seq);
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(write);
    }
    add(1, { type: "conversation", value: { id: 1 } });
    for (const child of lanes)
      add(child.seq, {
        type: "conversation",
        value: {
          id: Number(child.lane),
          owner: { conversationId: Number(child.caller_lane), taskId: Number(child.owner) },
          ...(child.at === null
            ? {}
            : { parent: { conversationId: Number(child.parent), at: Number(child.at) } }),
        },
      });
    // Every retained leaf can be addressed by an agent. Also keep each frozen
    // parent prefix that a fork inherited before a later parent compaction.
    const { rows: entries } = await db.query(
      `with tips as materialized (
        select conversation_id as lane,max(entry_id) as tip from message_entries
          where tenant_id=$1 and pi_session_id=$2 and commit_seq<=$3 group by conversation_id
        union select p.lane,p.tip from unnest($4::bigint[],$5::bigint[]) p(lane,tip)
      ), markers as materialized (
        select conversation_id as lane,entry_id,(content::json->>'head')::bigint as floor from message_entries
          where tenant_id=$1 and pi_session_id=$2 and commit_seq<=$3
            and strpos(content,'"head"')>0 and content::json->>'head' is not null
      ), windows as materialized (
        select t.lane,t.tip,coalesce((select floor from markers m where m.lane=t.lane
          and m.entry_id<=t.tip order by m.entry_id desc limit 1),1) as floor from tips t
      ), visible as (
        select lane,range_agg(int8range(floor,tip,'[]')) as ids from windows group by lane
      )
      select m.commit_seq,m.entry_id,m.conversation_id,m.content from message_entries m join visible v on v.lane=m.conversation_id and v.ids @> m.entry_id
      where m.tenant_id=$1 and m.pi_session_id=$2 and m.commit_seq<=$3
      order by m.commit_seq,m.ordinal`,
      [
        ...args,
        lanes.filter((l) => l.at !== null).map((l) => l.parent),
        lanes.filter((l) => l.at !== null).map((l) => l.at),
      ],
    );
    let rootCount = 0,
      rootBytes = 0;
    for (const e of entries) {
      const entry = JSON.parse(e.content);
      assert(
        entry.id === Number(e.entry_id) && entry.conversationId === Number(e.conversation_id),
        "Mismatched settled entry",
      );
      if (entry.conversationId === 1 && entry.id >= head.floor && entry.id < head.nextId) {
        rootCount++;
        rootBytes += Buffer.byteLength(e.content);
      }
      add(e.commit_seq, { type: "entry", value: entry });
    }
    assert(rootCount === head.count && rootBytes === head.bytes, "Incomplete settled root");
    const { rows: queues } = await db.query(
      "select * from inboxes where tenant_id=$1 and pi_session_id=$2 and commit_seq<=$3",
      args,
    );
    for (const q of queues) {
      assert.equal(q.items.length, 0, "Only settled Inboxes can use cold continuation");
      add(q.created_seq, {
        type: "document.create",
        record: {
          id: Number(q.doc_id),
          kind: "pi.inbox",
          scope: { kind: "conversation", conversationId: Number(q.conversation_id) },
          history: "latest",
          fork: "initial",
        },
        content: { kind: "base", version: 1, value: { items: [] } },
      });
    }
    const { rows: versions } = await db.query(
      `select document_id,conversation_id,content,commit_seq from messages
      where tenant_id=$1 and pi_session_id=$2 and commit_seq<=$3 and document_id is not null order by commit_seq,ordinal`,
      args,
    );
    const seen = new Set();
    for (const v of versions) {
      const id = Number(v.document_id),
        content = JSON.parse(v.content);
      if (content === null) {
        add(v.commit_seq, { type: "document.retire", id });
        continue;
      }
      add(
        v.commit_seq,
        seen.has(id)
          ? { type: "document.change", id, content }
          : agentBaseWrite(id, content, Number(v.conversation_id)),
      );
      seen.add(id);
    }
    await db.query("commit");
    if (store) {
      for (const [seq, writes] of [...groups].sort(([a], [b]) => a - b))
        store.prepareCommit(writes, seq).apply();
      if (store.nextSeq <= head.seq) store.prepareCommit([], head.seq).apply();
      store.nextId = Math.max(store.nextId, head.nextId);
      return { entries: entries.length };
    }
    // Seed the same seal fold that will watch the next root. Keep bodies and
    // Inbox values in their existing tables, including a previous child's path.
    const records = new Map(),
      heads = [];
    for (const [seq, writes] of [...groups].sort(([a], [b]) => a - b))
      for (const w of writes) {
        if (w.type === "entry") {
          if (w.value.head !== undefined)
            heads.push({
              conversationId: w.value.conversationId,
              id: w.value.id,
              floor: w.value.head,
            });
        } else if (w.type === "conversation") records.set(w.value.id, { seq, write: w });
        else if (w.type === "document.create" && w.record.kind !== "pi.inbox")
          records.set(w.record.id, { seq, write: w });
        else if (w.type === "document.change")
          records.set(w.id, { seq, write: { ...records.get(w.id).write, content: w.content } });
        else if (w.type === "document.retire") records.delete(w.id);
      }
    return {
      entries: entries.length,
      state: {
        seq: head.seq,
        nextId: head.nextId,
        heads,
        records: [...records.values()],
        sequences: [],
      },
      inboxIds: queues.map((q) => Number(q.doc_id)),
    };
  } catch (error) {
    await db.query("rollback");
    throw error;
  } finally {
    db.release();
  }
}
