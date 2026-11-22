// Control-side adapter for the root-only Session prototype. Native callers pass
// stable operation IDs; the root claim remains the single execution authority.
import assert from "node:assert/strict";
import { assertSubagentTaskInputBudget } from "@pi-cloud/protocol";

export async function admitDelegatedRun(pool, scope, request) {
  const { id, callerRunId, taskId, task, mode, agentId = id, at = null } = request;
  assertSubagentTaskInputBudget(task);
  assert(
    ["fresh", "branch", "continue"].includes(mode) &&
      task.trim() &&
      Number.isSafeInteger(taskId) &&
      taskId > 0,
  );
  assert(
    (mode === "branch") === (Number.isSafeInteger(at) && at > 0) &&
      (mode === "branch" || at === null),
  );
  assert((mode === "continue") === (agentId !== id));
  const db = await pool.connect();
  try {
    await db.query("begin");
    const owner = (
      await db.query(
        `select model_ref from runs where tenant_id=$1 and session_id=$2 and id=$3
      and root_run_id is null and state='running' and lease_id=$4 for update`,
        [scope.tenantId, scope.piSessionId, scope.runId, scope.writerId],
      )
    ).rows[0];
    assert(owner, "Root writer is no longer active");
    const existing = (await db.query("select * from runs where id=$1", [id])).rows[0];
    if (existing) {
      assert(
        existing.root_run_id === scope.runId &&
          existing.caller_run_id === callerRunId &&
          existing.agent_id === agentId &&
          existing.context_mode === mode &&
          existing.initial_input === task &&
          Number(existing.source_task_id) === taskId &&
          (mode !== "branch" || Number(existing.context_at) === at),
        "Delegation identity changed",
      );
      await db.query("commit");
      return existing;
    }
    const caller = (
      await db.query(
        `select id,native_lane from runs where tenant_id=$1 and session_id=$2
      and id=$3 and (id=$4 or root_run_id=$4) and state='running'`,
        [scope.tenantId, scope.piSessionId, callerRunId, scope.runId],
      )
    ).rows[0];
    assert(
      caller && (caller.id === scope.runId || caller.native_lane),
      "Caller is not active in this root",
    );
    let previous,
      anchor = at;
    if (mode === "continue") {
      previous = (
        await db.query(
          `select r.id,r.state,r.native_lane from runs r where r.tenant_id=$1 and r.session_id=$2
        and r.agent_id=$3 and not exists(select 1 from runs n where n.previous_run_id=r.id)`,
          [scope.tenantId, scope.piSessionId, agentId],
        )
      ).rows[0];
      assert(
        previous &&
          ["completed", "failed", "cancelled"].includes(previous.state) &&
          previous.native_lane,
        "Agent has no settled conversation to continue",
      );
      const tail = (
        await db.query(
          `select entry_id from message_entries where tenant_id=$1 and pi_session_id=$2
        and conversation_id=$3 order by entry_id desc limit 1`,
          [scope.tenantId, scope.piSessionId, previous.native_lane],
        )
      ).rows[0];
      assert(tail, "Previous conversation has no continuation anchor");
      anchor = tail.entry_id;
    }
    const row = (
      await db.query(
        `insert into runs(id,tenant_id,session_id,idempotency_key,initial_input,model_ref,state,root_run_id,
        caller_run_id,agent_id,previous_run_id,source_task_id,context_mode,context_at)
      values($1,$2,$3,$1,$4,$5,'preparing',$6,$7,$8,$9,$10,$11,$12) returning *`,
        [
          id,
          scope.tenantId,
          scope.piSessionId,
          task,
          owner.model_ref,
          scope.runId,
          callerRunId,
          agentId,
          previous?.id ?? null,
          taskId,
          mode,
          anchor,
        ],
      )
    ).rows[0];
    await db.query("commit");
    return row;
  } catch (error) {
    await db.query("rollback");
    throw error;
  } finally {
    db.release();
  }
}

export async function delegatedAddress(pool, scope, agentId) {
  return (
    await pool.query(
      `select id,agent_id,native_lane,state,root_run_id from runs where tenant_id=$1 and session_id=$2
    and agent_id=$3 and not exists(select 1 from runs n where n.previous_run_id=runs.id)`,
      [scope.tenantId, scope.piSessionId, agentId],
    )
  ).rows[0];
}

export async function delegatedSource(pool, row) {
  if (row.context_mode === "fresh") return;
  const {
    rows: [source],
  } = await pool.query(
    `select coalesce(native_lane,1) as lane from runs
    where tenant_id=$1 and session_id=$2 and id=$3`,
    [row.tenant_id, row.session_id, row.previous_run_id ?? row.caller_run_id],
  );
  assert(source, "Missing conversation source");
  return { lane: Number(source.lane), at: Number(row.context_at) };
}

// These methods belong to the execution/control side. A Worker only needs the
// logical operation and the native source returned by the adapter.
export function delegatedControl(pool, scope) {
  return {
    async start({ operationId, callerLane, taskId, task, mode, agentId, at }) {
      const caller =
        callerLane === 1
          ? scope.runId
          : (
              await pool.query(
                "select id from runs where tenant_id=$1 and session_id=$2 and root_run_id=$3 and native_lane=$4",
                [scope.tenantId, scope.piSessionId, scope.runId, callerLane],
              )
            ).rows[0]?.id;
      assert(caller, "Unregistered calling conversation");
      const row = await admitDelegatedRun(pool, scope, {
        id: operationId,
        callerRunId: caller,
        taskId,
        task,
        mode,
        agentId,
        at,
      });
      return {
        id: row.id,
        agentId: row.agent_id,
        task: row.initial_input,
        source: await delegatedSource(pool, row),
        sourceTaskId: Number(row.source_task_id),
      };
    },
    async address(agentId) {
      const row = await delegatedAddress(pool, scope, agentId);
      if (!row || row.root_run_id !== scope.runId || row.state !== "running") return null;
      return { id: row.id, lane: Number(row.native_lane) };
    },
    // Read the agent's own entries across explicit continuations. Pi supplies
    // inherited parent context separately at the frozen fork position.
    async history(agentId, before = null) {
      const { rows } = await pool.query(
        `select m.commit_seq,m.ordinal,m.content,c.id as run_id from runs c join message_entries m
          on m.tenant_id=c.tenant_id and m.pi_session_id=c.session_id and m.run_id=c.root_run_id and m.conversation_id=c.native_lane
         where c.tenant_id=$1 and c.session_id=$2 and c.agent_id=$3
           and ($4::bigint is null or (m.commit_seq,m.ordinal)<($4::bigint,$5::int))
         order by m.commit_seq desc,m.ordinal desc limit 33`,
        [scope.tenantId, scope.piSessionId, agentId, before?.seq ?? null, before?.ordinal ?? null],
      );
      const page = rows.slice(0, 32);
      const end = page.at(-1);
      return {
        entries: page.map((r) => ({ runId: r.run_id, entry: JSON.parse(r.content) })),
        ...(rows.length > 32 ? { before: { seq: end.commit_seq, ordinal: end.ordinal } } : {}),
      };
    },
    async list(cursor = null) {
      const { rows } = await pool.query(
        `select id,agent_id,state,left(btrim(initial_input),160) as task,caller_run_id,previous_run_id
         from runs where tenant_id=$1 and session_id=$2 and root_run_id=$3 and ($4::uuid is null or id>$4)
         order by id limit 25`,
        [scope.tenantId, scope.piSessionId, scope.runId, cursor],
      );
      const agents = rows.slice(0, 24);
      return { agents, ...(rows.length > 24 ? { nextCursor: agents.at(-1).id } : {}) };
    },
  };
}
