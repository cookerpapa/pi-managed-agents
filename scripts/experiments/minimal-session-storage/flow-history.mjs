import { readFile } from "node:fs/promises";

export async function installFlowHistory(
  db,
  projection = false,
  withDelegates = false,
  anchor = "head",
) {
  if (!["head", "base"].includes(anchor)) throw new Error("Unknown Flow anchor");
  if (withDelegates) {
    for (const file of ["subagent-deliveries.sql", "delegated-runs.sql"])
      await db.query(await readFile(new URL(file, import.meta.url), "utf8"));
    await db.query("alter table runs drop constraint runs_tenant_id_session_id_fkey");
  }
  if (!projection && !withDelegates)
    await db.query(`alter table runs drop constraint runs_state_check,
    add check(state in ('queued','running','cancel_requested','completed','cancelled','failed','timed_out'))`);
  if (!projection) await db.query("alter table runs rename column session_id to flow_id");
  const base = anchor === "base";
  const modeFunctions = await readFile(
    new URL(`./flow-${anchor}-admission.sql`, import.meta.url),
    "utf8",
  );
  const source = await readFile(new URL("./flow-history.sql", import.meta.url), "utf8");
  await db.query(
    source
      .replace("FLOW_MODE_FUNCTIONS", () => modeFunctions)
      .replaceAll("SESSION_ANCHOR", base ? "base_run_id" : "head_run_id")
      .replace(
        "ON_QUEUE_TIP_SWAP",
        base ? "" : "update sessions set head_run_id=l where id=s and head_run_id=r;",
      )
      .replace(
        "LIVE_FLOW_GUARD",
        base ? "or exists(select from sessions where tenant_id=t and flow_id=victim_flow)" : "",
      )
      .replaceAll("RUN_FLOW", projection ? "session_id" : "flow_id")
      .replace("GC_FAMILY", withDelegates ? "or root_run_id=victim" : ""),
  );
  if (projection && base) {
    await db.query(
      await readFile(new URL("./flow-transcript-points.sql", import.meta.url), "utf8"),
    );
    const page = await readFile(new URL("./flow-page-turns.sql", import.meta.url), "utf8");
    for (const order of ["asc", "desc"]) await db.query(page.replaceAll("PAGE_ORDER", order));
  }
}
export async function removeFlowOverlay(db) {
  await db.query(`drop view if exists flow_transcript_points;drop function if exists flow_pending_tool(uuid,uuid,uuid,bigint,bigint);drop function if exists flow_page_turns_asc(uuid,uuid,bigint,bigint,boolean,integer), flow_page_turns_desc(uuid,uuid,bigint,bigint,boolean,integer);
   drop function if exists flow_read_ranges(uuid,uuid); drop function flow_swap_pending(uuid,uuid,uuid,uuid),flow_collect(uuid,int),flow_detach(uuid,uuid,uuid,uuid),flow_fork(uuid,uuid,uuid,uuid,uuid),flow_visible(uuid,uuid,uuid),flow_admit(uuid,uuid,uuid,uuid,text); drop view flow_session_tips;
   drop table sessions,flow_gc_outbox; drop index flow_unsettled;
   alter table runs drop constraint flow_predecessor,drop constraint flow_run_identity,drop column prev_run_id`);
}
export const flowPage = `with recursive path(id,prev_run_id,depth) as (
 select r.id,r.prev_run_id,1 from runs r where r.tenant_id=$1 and r.id=(select s.tip_run_id from flow_session_tips s where s.tenant_id=$1 and s.id=$2)
 union all select r.id,r.prev_run_id,p.depth+1 from path p join runs r on r.id=p.prev_run_id and r.tenant_id=$1 where p.depth<$3
) select p.id,p.depth,b.content from path p left join lateral(select m.content from messages m where m.run_id=p.id and m.tenant_id=$1 order by m.commit_seq desc,m.ordinal desc limit 1)b on true order by p.depth`;
export const flowCold = `with recursive pending(id,prev_run_id,head) as(
 select r.id,r.prev_run_id,r.history_head from runs r where r.tenant_id=$1 and r.id=(select s.tip_run_id from flow_session_tips s where s.tenant_id=$1 and s.id=$2)
 union all select r.id,r.prev_run_id,r.history_head from pending p join runs r on r.id=p.prev_run_id and r.tenant_id=$1 where p.head is null
), path(id,prev_run_id,floor,depth) as(
 select id,prev_run_id,(head->>'floor')::bigint,1 from pending where head is not null
 union all select r.id,r.prev_run_id,p.floor,p.depth+1 from path p join runs r on r.id=p.prev_run_id and r.tenant_id=$1 where r.history_head is null or (r.history_head->>'nextId')::bigint>p.floor
) select m.content from path p cross join lateral(select content,commit_seq,ordinal from messages where tenant_id=$1 and run_id=p.id and entry_id>=p.floor order by commit_seq,ordinal offset 0)m order by p.depth desc,m.commit_seq,m.ordinal`;

// Prepare the same shared history and pinned heads for both projection benchmarks.
export async function seedFlowHeads(db, t) {
  const {
    rows: [ancestor],
  } = await db.query(
    `insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state)
        values(gen_random_uuid(),$1,gen_random_uuid(),1,gen_random_uuid(),'shared prefix','fixture','completed') returning id`,
    [t],
  );
  await db.query("update runs set prev_run_id=$1 where state='running'", [ancestor.id]);
  await db.query(
    "insert into sessions(id,tenant_id,flow_id,head_run_id) select gen_random_uuid(),tenant_id,session_id,id from runs where state='running'",
  );
}
