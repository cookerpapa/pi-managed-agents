import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { claimBatchSql, hintedClaimSql } from "./fixture.mjs";

// The same statement that claims a successor reads its already settled predecessor
// and the authoritative current Inbox. A warm hint can promote at most one Run.
const priorJoin = `left join runs prior on r.mailbox_position>1 and prior.tenant_id=r.tenant_id
 and prior.session_id=r.session_id and prior.mailbox_position=r.mailbox_position-1 and prior.state in ('completed','cancelled') and prior.history_head is not null
 left join inboxes i on prior.id is not null and i.tenant_id=r.tenant_id and i.pi_session_id=r.session_id and i.conversation_id=1`;
const columns = "prior.history_head as prior_head,i.doc_id as inbox_id,i.items as inbox_items";
const withHead = (sql) =>
  `with taken as (${sql}) select r.*, prior.environment_snapshot as previous_environment, ${columns} from taken r ${priorJoin}`;
export const claimWithHead = withHead(claimBatchSql);
export const claimPreferredWithHead = withHead(
  hintedClaimSql(
    `and exists(select 1 from runs p where p.tenant_id=r.tenant_id and p.session_id=r.session_id
   and p.mailbox_position=r.mailbox_position-1 and p.state in ('completed','cancelled') and p.history_head->>'version'=$4)`,
  ),
);
const openHead = (extra) => `select r.state,r.lease_id,r.mailbox_position,${extra}${columns},
 (select jsonb_object_agg(b.doc_id::text,b.conversation_id) from inboxes b where b.tenant_id=r.tenant_id
  and b.pi_session_id=r.session_id and b.created_seq<=(prior.history_head->>'seq')::bigint) as inbox_routes from runs r ${priorJoin}
 where r.id=$1 and r.tenant_id=$2 and r.session_id=$3`;
export const openingHead = openHead("");
export const rootOpeningHead = openHead("r.native_work_handoff is not null as has_handoff,");
export const scopeKey = (tenant, session) => tenant + "/" + session + "/1";
export const EMPTY_HEAD = Object.freeze({ seq: 0, nextId: 2, floor: 1, count: 0, bytes: 0 });

export function priorState(row) {
  if (Number(row.mailbox_position) === 1) {
    assert(!row.prior_head);
    return EMPTY_HEAD;
  }
  const h = row.prior_head;
  assert(
    h?.version &&
      Number.isSafeInteger(h.seq) &&
      h.seq > 0 &&
      Number.isSafeInteger(h.nextId) &&
      h.nextId > 1 &&
      Number.isSafeInteger(h.floor) &&
      h.floor > 0 &&
      Number.isSafeInteger(h.count) &&
      h.count >= 0 &&
      Number.isSafeInteger(h.bytes) &&
      h.bytes >= 0,
    "Missing or invalid settled history",
  );
  assert(
    row.inbox_id && Array.isArray(row.inbox_items) && row.inbox_items.length === 0,
    "A normally settled predecessor requires its confirmed empty Inbox",
  );
  return h;
}

// Values contain immutable native entries with their original commit positions.
// Only text can reside across Runs. Byte accounting uses serialized UTF-8 payloads.
export class PrefixCache {
  constructor({
    minBytes = 256 * 1024,
    maxBytes = 96 * 1024 * 1024,
    ttlMs = 300000,
    now = Date.now,
    onEvict = () => {},
  } = {}) {
    this.minBytes = minBytes;
    this.maxBytes = maxBytes;
    this.ttlMs = ttlMs;
    this.now = now;
    this.onEvict = onEvict;
    this.items = new Map();
    this.bytes = 0;
  }
  remove(key, forget = true) {
    const old = this.items.get(key);
    if (old) {
      this.items.delete(key);
      this.bytes -= old.bytes;
      if (forget) this.onEvict(key, old.version);
    }
  }
  expire() {
    const time = this.now();
    for (const [key, item] of this.items) if (time >= item.expires) this.remove(key);
  }
  get(key, version) {
    const item = this.items.get(key);
    if (!item) return;
    if (this.now() >= item.expires) {
      this.remove(key);
      return;
    }
    if (item.version !== version) return;
    this.items.delete(key);
    this.items.set(key, item);
    return item;
  }
  put(key, version, records, bytes) {
    if (
      bytes < this.minBytes ||
      bytes > this.maxBytes ||
      !records.length ||
      !records.every((r) => r.cacheable) ||
      (records.some((r) => r.hasTool) && !pairedTools(records))
    ) {
      this.remove(key);
      return false;
    }
    this.remove(key, false);
    while (this.bytes + bytes > this.maxBytes) this.remove(this.items.keys().next().value);
    this.items.set(key, {
      version,
      records: Object.freeze(records),
      bytes,
      expires: this.now() + this.ttlMs,
    });
    this.bytes += bytes;
    return true;
  }
}

export function record(entry, seq, size) {
  const bytes = size ?? Buffer.byteLength(JSON.stringify(entry));
  const messages = entry.model ?? [];
  const hasTool = messages.some(
    (m) =>
      m.role === "toolResult" ||
      (Array.isArray(m.content) && m.content.some((b) => b.type === "toolCall")),
  );
  const nativeDiagnostics =
    entry.kind === "pi.tool-result" &&
    entry.data &&
    Object.keys(entry.data).length === 1 &&
    Array.isArray(entry.data.diagnostics) &&
    !entry.data.diagnostics.length;
  const cacheable =
    !entry.edits &&
    (entry.data === undefined || nativeDiagnostics) &&
    messages.every(
      (m) =>
        (m.details === undefined || textValue(m.details)) &&
        (typeof m.content === "string" ||
          (Array.isArray(m.content) &&
            m.content.every(
              (b) =>
                b.type === "text" ||
                (m.role === "assistant" &&
                  b.type === "toolCall" &&
                  typeof b.id === "string" &&
                  textValue(b.arguments)),
            ))),
    );
  return { entry, seq, bytes, cacheable, hasTool };
}

function textValue(value) {
  if (typeof value === "string") return !value.startsWith("data:");
  if (value === null || typeof value !== "object") return true;
  if (Array.isArray(value)) return value.every(textValue);
  return (
    !("mimeType" in value || "b64_json" in value || "data" in value || value.type === "image") &&
    Object.values(value).every(textValue)
  );
}
function pairedTools(records) {
  const waiting = new Set();
  for (const { entry } of [...records].sort((a, b) => a.entry.id - b.entry.id))
    for (const m of entry.model ?? []) {
      if (m.role === "assistant") {
        if (waiting.size) return false;
        for (const b of Array.isArray(m.content) ? m.content : [])
          if (b.type === "toolCall") {
            if (waiting.has(b.id)) return false;
            waiting.add(b.id);
          }
      } else if (m.role === "toolResult" && !waiting.delete(m.toolCallId)) return false;
    }
  return !waiting.size;
}
export function toolEvidence(messages) {
  return messages.flatMap((m) =>
    m.role === "toolResult"
      ? [["result", m.toolCallId, m.toolName, m.content]]
      : m.role === "assistant" && Array.isArray(m.content)
        ? m.content
            .filter((b) => b.type === "toolCall")
            .map((b) => ["call", b.id, b.name, b.arguments])
        : [],
  );
}

export async function readPrefix(db, row, head, cache) {
  if (!head.version) return { records: [], status: "new", readBytes: 0 };
  const hit = cache?.get(scopeKey(row.tenant_id, row.session_id), head.version);
  let records;
  if (hit) records = hit.records;
  else {
    const { rows } = await db.query(
      `select entry_id,commit_seq,content from messages where tenant_id=$1 and pi_session_id=$2
      and conversation_id=1 and entry_id >= $3 and entry_id < $4 and commit_seq <= $5 order by commit_seq,ordinal`,
      [row.tenant_id, row.session_id, head.floor, head.nextId, head.seq],
    );
    records = rows.map((r) => {
      const entry = JSON.parse(r.content);
      assert(
        entry.id === Number(r.entry_id) && entry.conversationId === 1,
        "Stable entry identity mismatch",
      );
      return record(entry, Number(r.commit_seq), Buffer.byteLength(r.content));
    });
  }
  assert.equal(records.length, head.count, "Incomplete stable prefix");
  const bytes = hit?.bytes ?? records.reduce((n, r) => n + r.bytes, 0);
  assert.equal(bytes, head.bytes, "Inconsistent prefix byte coverage");
  return { records, status: hit ? "hit" : "cold", readBytes: hit ? 0 : bytes };
}

export function restoreSettled(store, row, head, records) {
  if (!head.version) return;
  const groups = new Map();
  const add = (seq, write) => {
    if (!groups.has(seq)) groups.set(seq, []);
    groups.get(seq).push(write);
  };
  add(1, { type: "conversation", value: { id: 1 } });
  add(head.seq, {
    type: "document.create",
    record: {
      id: Number(row.inbox_id),
      kind: "pi.inbox",
      scope: { kind: "conversation", conversationId: 1 },
      history: "latest",
      fork: "initial",
    },
    content: { kind: "base", version: 1, value: { items: row.inbox_items } },
  });
  for (const r of records) add(r.seq, { type: "entry", value: r.entry });
  for (const [seq, writes] of [...groups].sort(([a], [b]) => a - b))
    store.prepareCommit(writes, seq).apply();
}

// A cheap independent model-input witness for measured runs. The correctness
// waves cross-check every byte. Mutable Inbox state and system configuration
// are never supplied by the prefix cache.
export function extendWitness(prior, role, text, full = false) {
  return createHash("sha256")
    .update(prior)
    .update(JSON.stringify([role, text.length, full ? text : [text.slice(0, 32), text.slice(-32)]]))
    .digest("hex");
}
export function modelWitness(messages, full) {
  let result = "";
  for (const m of messages)
    if (m.role === "user" || m.role === "assistant") {
      const text =
        typeof m.content === "string"
          ? m.content
          : m.content
              .filter((b) => b.type === "text")
              .map((b) => b.text)
              .join("");
      result = extendWitness(result, m.role, text, full);
    }
  return result;
}
