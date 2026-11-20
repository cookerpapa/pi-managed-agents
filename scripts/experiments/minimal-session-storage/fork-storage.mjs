import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { Kysely, PostgresDialect, sql } from "kysely";
import { HistoryDirectory } from "../../../packages/pi-session-postgres/src/durable/history-directory.ts";
import { PostgresHistoryDirectory } from "../../../packages/pi-session-postgres/src/durable/postgres-history-directory.ts";
import { projectionTemplates } from "./projection-statements.mjs";

export async function installForkStudy(pool, token) {
  assert.match(token, /^[0-9a-f]{16}$/);
  const {
    rows: [db],
  } = await pool.query(
    "select current_database() as db,current_user as role,current_setting('synchronous_commit') as sync,current_setting('fsync') as fsync",
  );
  assert.deepEqual(db, { db: "pma_sessbench_" + token, role: "bench", sync: "on", fsync: "on" });
  await pool.query(await readFile(new URL("./fork-layout.sql", import.meta.url), "utf8"));
}

// Switch only on drained, empty study tables. Kafka payloads and all calls to
// project_ready (including the finish operation) are identical across layouts.
export async function setForkLayout(pool, enabled, cover = true) {
  const c = await pool.connect();
  try {
    await c.query("begin");
    await c.query(
      "truncate messages,inboxes,runs,fork_requests,fork_sessions,fork_completed_runs,pi_history_nodes,pi_history_streams",
    );
    await c.query(
      "alter table messages drop constraint if exists messages_run_id_fkey, drop constraint if exists retained_message_stream",
    );
    await c.query("alter table runs drop constraint if exists admitting_session");
    await c.query("drop index if exists fork_message_coordinates");
    await c.query(
      enabled
        ? `alter table messages add constraint retained_message_stream foreign key(tenant_id,pi_session_id) references pi_history_streams(tenant_id,stream_uuid);
      alter table runs add constraint admitting_session foreign key(tenant_id,session_id) references fork_sessions(tenant_id,stream_id)`
        : "alter table messages add constraint messages_run_id_fkey foreign key(run_id) references runs(id)",
    );
    if (enabled && cover)
      await c.query(
        "create index fork_message_coordinates on messages(tenant_id,pi_session_id,conversation_id,entry_id) include(commit_seq,ordinal,role,run_id)",
      );
    const finish = projectionTemplates.finishAndRelease;
    const body = enabled
      ? `with done as (${finish}) insert into fork_completed_runs select $1,$2,id from done returning run_id`
      : finish;
    await c.query(
      `create or replace function prefix_finish(uuid,uuid,uuid,uuid,text) returns setof uuid language sql volatile as $fn$ ${body} $fn$`,
    );
    await c.query("commit");
  } catch (e) {
    await c.query("rollback");
    throw e;
  } finally {
    c.release();
  }
}

export class ForkStorage {
  constructor(pool) {
    this.db = new Kysely({ dialect: new PostgresDialect({ pool }) });
    this.pool = pool;
  }
  async register(tenant, ids, root = null) {
    const c = await this.pool.connect();
    try {
      await c.query("begin");
      await c.query("insert into pi_history_streams select $1,x::text from unnest($2::uuid[]) x", [
        tenant,
        ids,
      ]);
      await c.query("insert into fork_sessions select $1,x,x,$3 from unnest($2::uuid[]) x", [
        tenant,
        ids,
        root,
      ]);
      await c.query("commit");
    } catch (e) {
      await c.query("rollback");
      throw e;
    } finally {
      c.release();
    }
  }
  async session(tx, t, id) {
    return (
      await sql`select * from fork_sessions where tenant_id=${t}::uuid and id=${id}::uuid for share`.execute(
        tx,
      )
    ).rows[0];
  }
  spans(t, s, lane) {
    return sql`(select ${s.stream_id}::uuid as stream,${lane}::bigint as lane,1::bigint as lo,
      9223372036854775807::bigint as hi,9223372036854775807::bigint as seq union all
      select source_id::uuid,conversation_id,min_id,max_id,through_seq from fork_leaves(${t}::uuid,${s.root_id}::uuid))`;
  }
  visible(t, s, lane) {
    return sql`${this.spans(t, s, lane)} r join messages m on m.tenant_id=${t}::uuid and m.pi_session_id=r.stream
      and m.conversation_id=r.lane and m.entry_id between r.lo and r.hi and m.commit_seq<=r.seq`;
  }
  async freeze(tx, t, s, point) {
    const { entry, run, lane } = point;
    assert(!s.root_id || lane === 1, "An inherited child requires its own ancestry mapping");
    const {
      rows: [target],
    } = await sql`select m.entry_id from ${this.visible(t, s, lane)}
      join fork_completed_runs h on h.tenant_id=m.tenant_id and h.stream_id=m.pi_session_id and h.run_id=m.run_id
      where m.entry_id=${entry} and m.run_id=${run}::uuid and m.role='assistant' and m.content::jsonb#>>'{model,0,stopReason}'='stop'`.execute(
      tx,
    );
    assert(target, "Missing completed assistant anchor");
    const {
      rows: [head],
    } =
      await sql`select commit_seq from messages where tenant_id=${t}::uuid and pi_session_id=${s.stream_id}::uuid order by commit_seq desc,ordinal desc limit 1`.execute(
        tx,
      );
    const store = new PostgresHistoryDirectory(tx, t),
      directory = new HistoryDirectory(store);
    let root = s.root_id
      ? await directory.prefix(await directory.read(s.root_id), entry)
      : undefined;
    if (head) {
      const local = await store.range({
        sourceId: s.stream_id,
        conversationId: lane,
        throughSequence: Number(head.commit_seq),
        maximumId: entry,
        clearTask: true,
      });
      if (local) root = await directory.join(root, directory.range(local));
    }
    assert(root);
    return directory.persist(root);
  }
  async lock(tx, t, exclusive) {
    if (exclusive) await sql`select pg_advisory_xact_lock(hashtextextended(${t},17))`.execute(tx);
    else await sql`select pg_advisory_xact_lock_shared(hashtextextended(${t},17))`.execute(tx);
  }
  async fork(t, source, point, key = randomUUID()) {
    return this.db.transaction().execute(async (tx) => {
      await this.lock(tx, t, false);
      await sql`select pg_advisory_xact_lock(hashtextextended(${t + source + key},23))`.execute(tx);
      const {
        rows: [old],
      } =
        await sql`select * from fork_requests where tenant_id=${t}::uuid and source_id=${source}::uuid and request_key=${key}::uuid`.execute(
          tx,
        );
      if (old) {
        assert(
          old.run_id === point.run &&
            Number(old.entry_id) === point.entry &&
            Number(old.conversation_id) === point.lane,
          "Fork request changed",
        );
        return old.target_id;
      }
      const s = await this.session(tx, t, source);
      assert(s, "Unknown Fork source");
      const root = await this.freeze(tx, t, s, point),
        target = randomUUID();
      await sql`insert into pi_history_streams(tenant_id,id) values(${t}::uuid,${target})`.execute(
        tx,
      );
      await sql`insert into fork_sessions values(${t}::uuid,${target}::uuid,${target}::uuid,${root}::uuid)`.execute(
        tx,
      );
      await sql`insert into fork_requests values(${t}::uuid,${source}::uuid,${key}::uuid,${target}::uuid,${point.lane},${point.entry},${point.run}::uuid)`.execute(
        tx,
      );
      return target;
    });
  }
  async page(t, id, limit = 30) {
    return this.db.transaction().execute(async (tx) => {
      const s = await this.session(tx, t, id);
      assert(s);
      return (
        await sql`select m.entry_id,m.commit_seq,m.ordinal,m.run_id,m.content from ${this.visible(t, s, 1)}
        order by m.entry_id desc limit ${limit}`.execute(tx)
      ).rows;
    });
  }
  async retire(t, id, point) {
    return this.db.transaction().execute(async (tx) => {
      await this.lock(tx, t, true);
      const {
        rows: [s],
      } =
        await sql`select * from fork_sessions where tenant_id=${t}::uuid and id=${id}::uuid for update`.execute(
          tx,
        );
      assert(s);
      const stream = s.stream_id;
      const active =
        await sql`select 1 from runs where tenant_id=${t}::uuid and session_id=${stream}::uuid and state<>'completed' limit 1`.execute(
          tx,
        );
      assert(!active.rows.length, "Source still running");
      const root = point ? await this.freeze(tx, t, s, point) : null;
      await sql`delete from inboxes where tenant_id=${t}::uuid and pi_session_id=${stream}::uuid`.execute(
        tx,
      );
      await sql`delete from runs where tenant_id=${t}::uuid and session_id=${stream}::uuid`.execute(
        tx,
      );
      if (!point) {
        await sql`delete from fork_sessions where tenant_id=${t}::uuid and id=${id}::uuid`.execute(
          tx,
        );
        return;
      }
      const fresh = randomUUID();
      await sql`insert into pi_history_streams(tenant_id,id) values(${t}::uuid,${fresh})`.execute(
        tx,
      );
      await sql`update fork_sessions set root_id=${root}::uuid,stream_id=${fresh}::uuid where tenant_id=${t}::uuid and id=${id}::uuid`.execute(
        tx,
      );
      return fresh;
    });
  }
  async collect(t) {
    await this.db.transaction().execute(async (tx) => {
      await this.lock(tx, t, true);
      await sql`with recursive keep(id) as (
        select root_id from fork_sessions where tenant_id=${t}::uuid and root_id is not null union
        select x.id from keep k join pi_history_nodes n on n.tenant_id=${t}::uuid and n.id=k.id
        cross join lateral(values(n.left_id),(n.right_id)) x(id) where x.id is not null)
        delete from pi_history_nodes n where tenant_id=${t}::uuid and not exists(select from keep k where k.id=n.id)`.execute(
        tx,
      );
      await sql`delete from messages m where tenant_id=${t}::uuid and not exists(select from fork_sessions s where s.tenant_id=m.tenant_id and s.stream_id=m.pi_session_id)
        and not exists(select from pi_history_nodes n where n.tenant_id=m.tenant_id and n.source_id=m.pi_session_id::text and n.conversation_id=m.conversation_id
        and m.entry_id between n.min_id and n.max_id and m.commit_seq<=n.through_seq)`.execute(tx);
      await sql`delete from fork_completed_runs h where tenant_id=${t}::uuid and not exists(select from messages m where m.run_id=h.run_id and m.pi_session_id=h.stream_id and m.tenant_id=h.tenant_id)`.execute(
        tx,
      );
      await sql`delete from pi_history_streams g where tenant_id=${t}::uuid and not exists(select from fork_sessions s where s.tenant_id=g.tenant_id and s.stream_id=g.stream_uuid)
        and not exists(select from pi_history_nodes n where n.tenant_id=g.tenant_id and n.source_id=g.id)
        and not exists(select from messages m where m.tenant_id=g.tenant_id and m.pi_session_id=g.stream_uuid)`.execute(
        tx,
      );
    });
  }
}
