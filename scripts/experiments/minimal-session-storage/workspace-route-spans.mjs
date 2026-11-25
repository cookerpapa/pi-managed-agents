// One execution-owned row per resource selection, not one per tool. Position
// lookup is pure even when async PG projection rewinds across a resource change.
import assert from "node:assert/strict";

export async function installWorkspaceRoutes(db) {
  await db.query(`create table workspace_route_spans(
    run_id uuid not null references runs(id) on delete cascade, conversation_id bigint not null,
    tenant_id uuid not null, pi_session_id text not null, session_id uuid not null,turn_id uuid not null,
    topic text not null, partition int not null, first_offset bigint not null,
    binding_id uuid not null, writer_id uuid not null, allocation_id uuid,
    primary key(run_id,conversation_id,topic,partition,first_offset),
    unique(run_id,conversation_id,binding_id)
  )`);
}

function atOffset(spans, offset) {
  return spans?.findLast((row) => BigInt(row.from) <= offset);
}

export class WorkspaceRouteSpans {
  #cache = new Map();
  #generation = 0;
  constructor(db) {
    this.db = db;
  }
  #forRun(id) {
    let routes = this.#cache.get(id);
    if (!routes) {
      routes = new Map();
      this.#cache.set(id, routes);
    }
    return routes;
  }
  #key(s, lane, topic, p) {
    return JSON.stringify([s.tenantId, s.piSessionId, lane, topic, p]);
  }
  #read(db, s, lane, topic, p) {
    return db
      .query(
        `select first_offset::text as "from", binding_id as "bindingId", allocation_id as "allocationId", writer_id as "writerId",
      session_id as "sessionId", turn_id as "turnId" from workspace_route_spans where tenant_id=$1 and pi_session_id=$2
      and run_id=$3 and conversation_id=$4 and topic=$5 and partition=$6 order by first_offset`,
        [s.tenantId, s.piSessionId, s.runId, lane, topic, p],
      )
      .then((r) => r.rows);
  }
  cachedAt(s, lane, { topic, partition, offset }) {
    const spans = this.#cache.get(s.runId)?.get(this.#key(s, lane, topic, partition));
    return atOffset(spans, offset);
  }
  async at(s, lane, position) {
    const { topic, partition, offset } = position,
      key = this.#key(s, lane, topic, partition);
    const cache = this.#forRun(s.runId);
    let spans = cache.get(key);
    if (!spans) {
      const loaded = await this.#read(this.db, s, lane, topic, partition);
      spans = cache.get(key) ?? loaded;
      cache.set(key, spans);
    }
    return atOffset(spans, offset);
  }
  async activate(s, lane, position, selected, current) {
    const bindingId = typeof selected === "string" ? selected : selected.bindingId;
    const allocationId = selected.allocationId ?? null;
    const { topic, partition, offset } = position,
      c = await this.db.connect();
    const key = this.#key(s, lane, topic, partition),
      cache = this.#forRun(s.runId),
      generation = this.#generation;
    try {
      await c.query("begin");
      await c.query("set local synchronous_commit=on");
      // A resource transition uses the existing Run as a serialization point.
      // Ordinary bound tool dispatch/lookup never takes this lock.
      const owner = await c.query(
        "select id from runs where tenant_id=$1 and session_id=$2 and id=$3 and lease_id=$4 and state in ('running','cancel_requested') for no key update",
        [s.tenantId, s.piSessionId, s.runId, s.writerId],
      );
      assert(owner.rowCount === 1 && current(), "Workspace resource selection no longer admitted");
      const spans = await this.#read(c, s, lane, topic, partition);
      const last = spans.at(-1);
      if (last && BigInt(last.from) >= offset) {
        assert(
          last.from === String(offset) &&
            last.bindingId === bindingId &&
            last.allocationId === allocationId &&
            last.writerId === s.writerId &&
            last.sessionId === s.sessionId &&
            last.turnId === s.turnId,
          "A Workspace route boundary cannot change",
        );
      } else {
        await c.query(
          "insert into workspace_route_spans(run_id,conversation_id,tenant_id,pi_session_id,session_id,turn_id,topic,partition,first_offset,binding_id,writer_id,allocation_id) values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
          [
            s.runId,
            lane,
            s.tenantId,
            s.piSessionId,
            s.sessionId,
            s.turnId,
            topic,
            partition,
            String(offset),
            bindingId,
            s.writerId,
            allocationId,
          ],
        );
        spans.push({
          from: String(offset),
          bindingId,
          allocationId,
          writerId: s.writerId,
          sessionId: s.sessionId,
          turnId: s.turnId,
        });
      }
      assert(current() && generation === this.#generation, "Workspace route owner changed");
      await c.query("commit");
      const known = cache.get(key)?.at(-1);
      if (!known || BigInt(known.from) <= offset) cache.set(key, spans);
      return spans.at(-1);
    } catch (error) {
      cache.delete(key);
      await c.query("rollback");
      throw error;
    } finally {
      c.release();
    }
  }
  reset() {
    this.#generation++;
    this.#cache.clear();
  }
  forget(s) {
    this.#cache.delete(s.runId);
  }
}
