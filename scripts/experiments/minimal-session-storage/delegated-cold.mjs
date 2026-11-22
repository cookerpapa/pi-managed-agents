// Cold reads are fenced by a settled head or by the exact source invocation.
// Native sequence and allocation coordinates remain those of the shared Session.
import assert from "node:assert/strict";
import { MemoryStorage } from "@earendil-works/pi-durable";
import { agentBaseWrite } from "./agent-configuration.mjs";
import { openingRootOnly, priorState } from "./session-prefix.mjs";

async function snapshot(pool, read) {
  const db = await pool.connect();
  try {
    await db.query("begin isolation level repeatable read read only");
    const result = await read(db);
    await db.query("commit");
    return result;
  } catch (error) {
    await db.query("rollback");
    throw error;
  } finally {
    db.release();
  }
}
function loadGroups(store, groups) {
  for (const [seq, writes] of [...groups].sort(([a], [b]) => a - b))
    store.prepareCommit(writes, seq).apply();
}
function add(groups, seq, write) {
  const key = Number(seq);
  if (!groups.has(key)) groups.set(key, []);
  groups.get(key).push(write);
}
async function loadConfigs(db, scope, lanes, through, groups) {
  const { rows } = await db.query(
    `select document_id,conversation_id,content,commit_seq from messages
    where tenant_id=$1 and pi_session_id=$2 and conversation_id=any($3::bigint[]) and commit_seq<=$4 and document_id is not null order by commit_seq,ordinal`,
    [scope.tenantId, scope.piSessionId, lanes, through],
  );
  const seen = new Map();
  for (const row of rows) {
    const id = Number(row.document_id),
      lane = Number(row.conversation_id),
      content = JSON.parse(row.content);
    if (!content) {
      add(groups, row.commit_seq, { type: "document.retire", id });
      seen.delete(id);
      continue;
    }
    add(
      groups,
      row.commit_seq,
      seen.has(id) ? { type: "document.change", id, content } : agentBaseWrite(id, content, lane),
    );
    seen.set(id, { seq: Number(row.commit_seq), write: agentBaseWrite(id, content, lane) });
  }
  return seen;
}
function readEntry(row, groups) {
  const entry = JSON.parse(row.content);
  assert(
    entry.id === Number(row.entry_id) && entry.conversationId === Number(row.conversation_id),
    "Mismatched settled Entry",
  );
  if (groups) add(groups, row.commit_seq, { type: "entry", value: entry });
  return entry;
}

export function enableDelegatedArchive(pool, scope, store) {
  store.loadArchive = (source) => readDelegatedSource(pool, scope, source);
}

export async function loadSettledDelegations(pool, scope, store) {
  const base = await snapshot(pool, async (db) => {
    const row = (await db.query(openingRootOnly, [scope.runId, scope.tenantId, scope.piSessionId]))
      .rows[0];
    assert(row?.lease_id === scope.writerId && row.state === "running", "Unexpected root owner");
    const head = priorState(row);
    if (!head.version) return { entries: 0 };
    const args = [scope.tenantId, scope.piSessionId, head.floor, head.nextId, head.seq];
    const groups = new Map(),
      root = { type: "conversation", value: { id: 1 } },
      heads = [];
    add(groups, 1, root);
    const { rows } = await db.query(
      `select entry_id,conversation_id,commit_seq,content from message_entries where tenant_id=$1 and pi_session_id=$2
      and conversation_id=1 and entry_id>=$3 and entry_id<$4 and commit_seq<=$5 order by commit_seq,ordinal`,
      args,
    );
    let bytes = 0;
    for (const r of rows) {
      const entry = readEntry(r, store ? groups : undefined);
      bytes += Buffer.byteLength(r.content);
      if (entry.head !== undefined)
        heads.push({ conversationId: 1, id: entry.id, floor: entry.head });
    }
    assert(rows.length === head.count && bytes === head.bytes, "Incomplete settled root");
    const docs = await loadConfigs(db, scope, [1], head.seq, groups);
    const inboxId = Number(row.inbox_id);
    if (store) {
      add(groups, row.inbox_created, {
        type: "document.create",
        record: {
          id: inboxId,
          kind: "pi.inbox",
          scope: { kind: "conversation", conversationId: 1 },
          history: "latest",
          fork: "initial",
        },
        content: { kind: "base", version: 1, value: { items: [] } },
      });
      return { entries: rows.length, groups, head };
    }
    return {
      entries: rows.length,
      state: {
        seq: head.seq,
        nextId: head.nextId,
        heads,
        records: [{ seq: 1, write: root }, ...docs.values()],
        sequences: [],
      },
      inboxIds: [inboxId],
    };
  });
  if (!store) return base;
  if (base.groups) {
    loadGroups(store, base.groups);
    if (store.nextSeq <= base.head.seq) store.prepareCommit([], base.head.seq).apply();
    store.nextId = Math.max(store.nextId, base.head.nextId);
  }
  enableDelegatedArchive(pool, scope, store);
  return { entries: base.entries };
}

async function readDelegatedSource(pool, scope, { lane, at, cuts = [] }) {
  assert(lane > 1 && Number.isSafeInteger(at) && at > 0, "Invalid delegated history source");
  return snapshot(pool, async (db) => {
    const groups = new Map(),
      path = new Map();
    let current = lane,
      cut = at,
      through;
    for (;;) {
      assert(!path.has(current), "Cycle in archived conversation ancestry");
      path.set(current, cut);
      if (current === 1) {
        add(groups, 1, { type: "conversation", value: { id: 1 } });
        break;
      }
      const {
        rows: [r],
      } = await db.query(
        `select c.native_created_seq,c.native_owner,c.native_outcome_seq,c.state,c.context_at,
        coalesce(caller.native_lane,1) as caller_lane,coalesce(p.native_lane,1) as parent_lane from runs c
        join runs caller on caller.id=c.caller_run_id join runs p on p.id=coalesce(c.previous_run_id,c.caller_run_id)
        where c.tenant_id=$1 and c.session_id=$2 and c.native_lane=$3`,
        [scope.tenantId, scope.piSessionId, current],
      );
      assert(r, "Missing delegated history source");
      if (through === undefined) {
        assert(
          ["completed", "failed", "cancelled"].includes(r.state),
          "Delegated source has not settled",
        );
        through = Number(r.native_outcome_seq);
      }
      assert(Number(r.native_created_seq) <= through, "Conversation outside archived prefix");
      const parent =
        r.context_at === null
          ? undefined
          : { conversationId: Number(r.parent_lane), at: Number(r.context_at) };
      add(groups, r.native_created_seq, {
        type: "conversation",
        value: {
          id: current,
          owner: { conversationId: Number(r.caller_lane), taskId: Number(r.native_owner) },
          ...(parent ? { parent } : {}),
        },
      });
      if (!parent) break;
      current = parent.conversationId;
      cut = Math.min(cut, parent.at);
    }
    // Select the nearest native head on the leaf's *visible* path. An ancestor
    // which compacted after the fork cannot hide the prefix inherited by A.
    const args = [scope.tenantId, scope.piSessionId, [...path.keys()], [...path.values()], through];
    args.push([...new Set([at, ...cuts])]);
    const { rows } = await db.query(
      `with path as (select * from unnest($3::bigint[],$4::bigint[]) with ordinality p(lane,tip,depth)),
      windows as materialized (select c.cut,coalesce((
        select (m.content::json->>'head')::bigint from path p join message_entries m
          on m.conversation_id=p.lane and m.entry_id<=least(c.cut,p.tip)
          where m.tenant_id=$1 and m.pi_session_id=$2 and m.commit_seq<=$5 and strpos(m.content,'"head"')>0
            and m.content::json->>'head' is not null order by p.depth,m.entry_id desc limit 1),1) as floor
        from unnest($6::bigint[]) c(cut)),
      visible as (select p.lane,range_agg(int8range(w.floor,least(w.cut,p.tip),'[]')) as ids
        from path p join windows w on least(w.cut,p.tip)>=w.floor group by p.lane)
      select m.entry_id,m.conversation_id,m.commit_seq,m.content from visible v join message_entries m
        on m.conversation_id=v.lane and v.ids @> m.entry_id
        where m.tenant_id=$1 and m.pi_session_id=$2 and m.commit_seq<=$5 order by m.commit_seq,m.ordinal`,
      args,
    );
    for (const row of rows) readEntry(row, groups);
    assert(
      rows.some((r) => Number(r.conversation_id) === lane && Number(r.entry_id) === at),
      "Missing delegated fork anchor",
    );
    const docs = await loadConfigs(db, scope, [lane], through, groups);
    const store = new MemoryStorage();
    loadGroups(store, groups);
    return { store, entries: rows.length, docs: [...docs.keys()] };
  });
}
