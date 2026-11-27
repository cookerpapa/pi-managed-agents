import { sql } from "kysely";
export class SegmentMailReader {
  constructor(db) {
    this.db = db;
  }
  async lastPlaced(s) {
    const {
      rows: [row],
    } =
      await sql`select native_id from user_or_agent_messages where tenant_id=${s.tenantId}::uuid and session_id=${s.sessionId}::uuid and turn_id=${s.runId}::uuid and state='placed' order by native_id desc limit 1`.execute(
        this.db,
      );
    return row ? Number(row.native_id) : undefined;
  }
  async releaseBeforePlacement(s, id) {
    await this.db.transaction().execute(async (tx) => {
      await sql`select id from sessions where tenant_id=${s.tenantId}::uuid and id=${s.sessionId}::uuid for no key update`.execute(
        tx,
      );
      await sql`update user_or_agent_messages m set state=case when m.withdrawing then 'withdrawn' else 'pending' end,withdrawing=false,boundary_task=null
      from turns r where m.tenant_id=${s.tenantId}::uuid and m.session_id=${s.sessionId}::uuid and m.id=${id}::uuid and m.turn_id=r.id
      and r.id=${s.runId}::uuid and r.lease_id=${s.writerId}::uuid and r.fencing_token=${s.fencingToken} and r.output_seal_id is null and m.state='sending'`.execute(
        tx,
      );
    });
  }
  async selectAt(s, at, taskId = null, onEmpty) {
    if (
      !["start", "idle", "postTools"].includes(at) ||
      (at === "postTools") !== Number.isSafeInteger(taskId)
    )
      throw Error("Invalid Session input boundary");
    const mode = at === "postTools" ? sql`and m.mode='steer'` : sql``;
    const maybe = await sql`select 1 from user_or_agent_messages m where m.turn_id=${s.runId}::uuid
      and m.state='pending' ${mode} limit 1`.execute(this.db);
    if (!maybe.rows.length) {
      onEmpty?.();
      return;
    }
    return this.db.transaction().execute(async (tx) => {
      await sql`set local synchronous_commit=on`.execute(tx);
      const locked =
        await sql`select flow_id from sessions where tenant_id=${s.tenantId}::uuid and id=${s.sessionId}::uuid for no key update`.execute(
          tx,
        );
      if (!locked.rows.length) return;
      const {
        rows: [m],
      } = await sql`with candidate as (
          select m.id from user_or_agent_messages m join turns r on r.id=m.turn_id
          where m.tenant_id=${s.tenantId}::uuid and m.session_id=${s.sessionId}::uuid and m.turn_id=${s.runId}::uuid and
            r.session_id=${locked.rows[0].flow_id}::uuid and r.lease_id=${s.writerId}::uuid and r.fencing_token=${s.fencingToken}
            and r.state='running' and r.output_seal_id is null and m.state='pending' ${mode}
            and (${taskId}::bigint is null or not exists(select from user_or_agent_messages prev
              where prev.turn_id=m.turn_id and prev.boundary_task=${taskId}::bigint))
          order by ${at === "start" ? sql`m.starts_turn desc,m.position` : sql`m.position`} limit 1)
        update user_or_agent_messages m set state='sending',boundary_task=${taskId}::bigint from candidate c
         where m.tenant_id=${s.tenantId}::uuid and m.session_id=${s.sessionId}::uuid and m.id=c.id returning m.*,
           ${
             at === "postTools"
               ? sql`exists(select from user_or_agent_messages more where more.turn_id=m.turn_id
             and more.state='pending' and more.mode='steer' and more.id<>m.id)`
               : sql`false`
           } as more_steers`.execute(tx);
      return m;
    });
  }
}
