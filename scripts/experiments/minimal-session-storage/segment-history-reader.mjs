import assert from "node:assert/strict";
import { Kysely, PostgresDialect, sql } from "kysely";
import { restoreForkPrefix } from "./session-prefix.mjs";
export class SegmentHistoryReader {
  constructor(pool) {
    this.db = new Kysely({ dialect: new PostgresDialect({ pool }) });
    this.pool = pool;
  }
  async session(tx, t, id) {
    return (
      await sql`select * from sessions where tenant_id=${t}::uuid and id=${id}::uuid for share`.execute(
        tx,
      )
    ).rows[0];
  }
  async readPage(tx, t, s, limit, cursor, floor = 1, coordinatesOnly = false) {
    assert(Number.isSafeInteger(limit) && limit > 0, "Invalid page size");
    if (cursor) assert(cursor.flow === s.flow_id, "History cursor is obsolete");
    const local = 2147483647;
    const rows = [];
    let segment = cursor?.segment ?? local;
    while (segment > 0 && rows.length < limit) {
      const range =
        segment === local
          ? {
              source_flow: s.flow_id,
              conversation_id: 1,
              first_entry: 1,
              last_entry: Number.MAX_SAFE_INTEGER,
              through_seq: Number.MAX_SAFE_INTEGER,
              segment_no: local,
            }
          : (
              await sql`select * from flow_segments where tenant_id=${t}::uuid and destination_flow=${s.flow_id}::uuid
             and segment_no<=${segment} order by segment_no desc limit 1`.execute(tx)
            ).rows[0];
      if (!range) break;
      const before = cursor?.segment === range.segment_no ? cursor.entry : null;
      const page =
        await sql`select entry_id,commit_seq,ordinal,run_id${coordinatesOnly ? sql`` : sql`,content`} from message_entries
        where tenant_id=${t}::uuid and pi_session_id=${range.source_flow}::uuid and conversation_id=${range.conversation_id}
        and entry_id between ${Math.max(floor, Number(range.first_entry))} and ${range.last_entry}
        and commit_seq<=${range.through_seq} ${before == null ? sql`` : sql`and entry_id<${before}`}
        order by entry_id desc limit ${limit - rows.length}`.execute(tx);
      for (const r of page.rows)
        rows.push({
          ...r,
          sourceFlow: range.source_flow,
          lane: Number(range.conversation_id),
          cursor: { flow: s.flow_id, segment: range.segment_no, entry: r.entry_id },
        });
      if (segment === local) segment = local - 1;
      else segment = range.segment_no - 1;
    }
    return rows;
  }
  async coldRecords(tx, t, s, inheritedOnly = false, onReadBytes, minimum = 1) {
    let receivedBytes = 0;
    const records = [];
    let cursor = inheritedOnly ? { flow: s.flow_id, segment: 2147483646 } : null,
      floor = minimum,
      head = minimum > 1;
    for (;;) {
      const rows = await this.readPage(tx, t, s, 64, cursor, floor);
      for (const row of rows) {
        const bytes = Buffer.byteLength(row.content);
        receivedBytes += bytes;
        const entry = JSON.parse(row.content);
        if (entry.id < floor) continue;
        if (!head && entry.head !== undefined) {
          floor = entry.head;
          head = true;
        }
        const { byTaskId, ...value } = entry;
        records.push({
          entry: { ...value, conversationId: 1 },
          seq: Number(row.commit_seq),
          bytes,
        });
      }
      if (rows.length < 64 || Number(rows.at(-1).entry_id) <= floor) break;
      cursor = rows.at(-1).cursor;
    }
    onReadBytes?.(receivedBytes);
    return records.reverse();
  }
  async contextPrefix(t, id, inheritedOnly = false, onReadBytes, minimum = 1) {
    return this.db
      .transaction()
      .setIsolationLevel("repeatable read")
      .execute(async (tx) => {
        const s = await this.session(tx, t, id);
        assert(s, "Unknown Fork session");
        return this.coldRecords(tx, t, s, inheritedOnly, onReadBytes, minimum);
      });
  }
  async cachedFork(tx, t, s, cache) {
    const { rows: ranges } =
      await sql`select g.source_flow,g.conversation_id,g.first_entry,g.last_entry,g.through_seq,h.history_head
      from flow_segments g left join lateral (
        select history_head from turns r where r.tenant_id=g.tenant_id and r.session_id=g.source_flow
          and r.state='completed' and r.history_head is not null order by r.mailbox_position desc limit 1
      ) h on true where g.tenant_id=${t}::uuid and g.destination_flow=${s.flow_id}::uuid order by g.segment_no desc`.execute(
        tx,
      );
    if (!ranges.length) return;
    const sources = new Map();
    let diskBytes = 0;
    for (const [i, range] of ranges.entries()) {
      if (Number(range.conversation_id) !== 1) return;
      // An intact parent Fork can already contain a grandparent's prefix.
      if (sources.has(range.source_flow)) continue;
      const clip = {
        first: Number(range.first_entry),
        last: Number(range.last_entry),
        through: Number(range.through_seq),
        ancestorLast:
          i === 0 ? ranges.slice(1).reduce((n, g) => Math.max(n, Number(g.last_entry)), 0) : 0,
      };
      const hit = await cache.sourceSnapshot(t, range.source_flow, range.history_head, clip);
      if (!hit) return;
      diskBytes += hit.diskReadBytes ?? 0;
      sources.set(
        range.source_flow,
        new Map(
          hit.records
            .filter(
              (r) => r.entry.id >= clip.first && r.entry.id <= clip.last && r.seq <= clip.through,
            )
            .map((r) => [r.entry.id, r]),
        ),
      );
      if (clip.ancestorLast && hit.inherited.length) {
        const { rows: ancestry } =
          await sql`select source_flow,first_entry,last_entry,through_seq,conversation_id from flow_segments
          where tenant_id=${t}::uuid and destination_flow=${range.source_flow}::uuid`.execute(tx);
        for (const r of hit.inherited) {
          const matches = ancestry.filter(
            (a) =>
              r.entry.id >= Number(a.first_entry) &&
              r.entry.id <= Number(a.last_entry) &&
              r.seq <= Number(a.through_seq),
          );
          if (matches.length !== 1 || Number(matches[0].conversation_id) !== 1) continue;
          const source = matches[0].source_flow;
          if (!sources.has(source)) sources.set(source, new Map());
          sources.get(source).set(r.entry.id, r);
        }
      }
    }
    // PG selects exactly the authorized immutable coordinates, including the
    // commit ceiling. A partial snapshot never supplies a partial Pi context.
    const records = [];
    let cursor,
      floor = 1,
      headSeen = false;
    for (;;) {
      const rows = await this.readPage(tx, t, s, 64, cursor, floor, true);
      for (const row of rows) {
        const id = Number(row.entry_id);
        if (id < floor) continue;
        const r = sources.get(row.sourceFlow)?.get(id);
        if (!r || r.seq !== Number(row.commit_seq) || r.entry.conversationId !== row.lane) return;
        if (!headSeen && r.entry.head !== undefined) {
          floor = r.entry.head;
          headSeen = true;
        }
        const { byTaskId, ...entry } = r.entry;
        records.push({ entry: { ...entry, conversationId: 1 }, seq: r.seq, bytes: r.bytes });
      }
      if (rows.length < 64 || Number(rows.at(-1).entry_id) <= floor) break;
      cursor = rows.at(-1).cursor;
    }
    return { records: records.reverse(), diskBytes };
  }
  async hydrate(t, id, store, onPrefix, cache) {
    return this.db
      .transaction()
      .setIsolationLevel("repeatable read")
      .execute(async (tx) => {
        const s = await this.session(tx, t, id);
        assert(s, "Unknown Fork session");
        let received = 0;
        const hit = cache?.hasSnapshots ? await this.cachedFork(tx, t, s, cache) : undefined;
        const records =
          hit?.records ??
          (await this.coldRecords(tx, t, s, false, (bytes) => {
            received = bytes;
          }));
        onPrefix?.(records, received, hit);
        if (!records.length) return 0;
        const {
          rows: [base],
        } = await sql`select f.native_id_floor,a.content,w.state from flows f
        left join lateral fork_agent_at(f.tenant_id,f.flow_id,1,9223372036854775807) a on true
        left join conversation_worlds w on w.tenant_id=f.tenant_id and w.pi_session_id=f.flow_id and w.conversation_id=1
        where f.tenant_id=${t}::uuid and f.flow_id=${s.flow_id}::uuid`.execute(tx);
        restoreForkPrefix(
          store,
          records,
          base.content == null ? null : JSON.parse(base.content),
          base.state,
          Number(base.native_id_floor),
        );
        return records.length;
      });
  }
}
