import { createHash } from "node:crypto";
import { sql, type Kysely, type Transaction } from "kysely";
import type { Database } from "@pi-cloud/database";
import { namedAgentId } from "@pi-cloud/protocol/delegation-identity";
import {
  infrastructureToolDefinitions,
  prepareNamedSubagentArguments,
  prepareSubagentControlArguments,
  subagentName,
} from "@pi-cloud/protocol";
import { Value } from "typebox/value";
import {
  childWorkspaceGrant,
  type DelegatedWorkspaceGrant,
} from "@pi-cloud/subagent-control/delegated-workspace-grant";
import { readSealedDelegationIntent } from "@pi-cloud/pi-session-postgres/durable";
import type { SubagentOffer } from "./named-subagent-client.ts";

export type ChildRow = {
  id: string;
  tenant_id: string;
  session_id: string;
  root_run_id: string;
  caller_run_id: string;
  agent_id: string;
  state: string;
  source_task_id: string;
  initial_input: string;
  workspace_grant: DelegatedWorkspaceGrant;
  context_at: string | null;
  agent_name: string;
  parent_agent_id: string;
  creation_hash: string;
  native_lane: string | null;
  placement_lane: string;
  placement_sequence: string;
  native_answer_id: string | null;
};
export class SubagentAdmissionRefused extends Error {}
type Tx = Transaction<Database>;
type Caller = {
  id: string;
  agent_id: string | null;
  parent_agent_id: string | null;
  workspace_grant: DelegatedWorkspaceGrant;
};
type Instruction = {
  kind: "watch" | "state" | "fixed" | "send" | "cancel" | "question";
  child?: string;
  value?: Record<string, unknown>;
};
const fixed = (text: string, isError = false): Instruction => ({
  kind: "fixed",
  value: { text, isError },
});
const hash = (value: unknown) => createHash("sha256").update(JSON.stringify(value)).digest("hex");
const uuid = /^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/i;
function canonical(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === "object")
    return Object.fromEntries(
      Object.entries(value)
        .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
        .map(([k, v]) => [k, canonical(v)]),
    );
  return value;
}

async function findChild(tx: Tx, o: SubagentOffer, parent: string, target: string) {
  let id = target;
  if (!uuid.test(target)) {
    try {
      id = namedAgentId(o.tenant, o.session, parent, subagentName(target));
    } catch {
      return undefined;
    }
  }
  return (
    await sql<
      Pick<ChildRow, "id" | "state" | "root_run_id">
    >`select id,state,root_run_id from runs where tenant_id=${o.tenant}::uuid and session_id=${o.session}::uuid
    and agent_id=${id}::uuid order by (previous_run_id is not null) desc,created_at desc limit 1`.execute(
      tx,
    )
  ).rows[0];
}
async function create(
  tx: Tx,
  o: SubagentOffer,
  parent: string,
  caller: Caller,
  seq: number,
  raw: unknown,
): Promise<Instruction> {
  const args = prepareNamedSubagentArguments(raw),
    name = args.name;
  const workspace =
    args.workspace === null
      ? null
      : {
          compute: args.workspace?.compute ?? "shared",
          cwd: args.workspace?.cwd ?? null,
          tools: args.workspace?.tools?.toSorted() ?? null,
        };
  const fingerprint = hash([args.task, args.context ?? "fresh", workspace]);
  const id = namedAgentId(o.tenant, o.session, parent, name);
  const existing = (
    await sql<
      Pick<ChildRow, "creation_hash" | "root_run_id" | "source_task_id" | "state">
    >`select creation_hash,root_run_id,source_task_id,state from runs where id=${id}::uuid and tenant_id=${o.tenant}::uuid and session_id=${o.session}::uuid`.execute(
      tx,
    )
  ).rows[0];
  if (existing) {
    if (existing.creation_hash !== fingerprint)
      return fixed(
        `名字 ${name} 已被占用。已有子 Agent 的 UUID 是 ${id}。给它发消息，或为新任务另选名字。`,
        true,
      );
    const foundingCaller =
      existing.root_run_id === o.root && Number(existing.source_task_id) === o.task;
    return {
      kind:
        foundingCaller || !["preparing", "running"].includes(existing.state) ? "watch" : "state",
      child: id,
    };
  }
  let grant: DelegatedWorkspaceGrant;
  try {
    grant = childWorkspaceGrant(caller.workspace_grant, args.workspace, id);
  } catch (error) {
    return fixed(error instanceof Error ? error.message : "Invalid child workspace grant", true);
  }
  await sql`insert into runs(id,tenant_id,session_id,idempotency_key,initial_input,model_ref,state,root_run_id,caller_run_id,agent_id,
    source_task_id,context_mode,context_at,workspace_grant,agent_name,parent_agent_id,creation_hash,placement_sequence,placement_lane,placement_due)
    select ${id}::uuid,tenant_id,session_id,${id}::uuid,${args.task},model_ref,'preparing',id,${caller.id}::uuid,${id}::uuid,
    ${o.task},${args.context ?? "fresh"},${o.anchor}::bigint,${grant === null ? null : JSON.stringify(grant)}::jsonb,${name},${parent}::uuid,${fingerprint},${seq},${o.lane},now()
    from runs where id=${o.root}::uuid`.execute(tx);
  return { kind: "watch", child: id };
}
async function control(
  tx: Tx,
  o: SubagentOffer,
  parent: string,
  caller: Caller,
  raw: unknown,
): Promise<Instruction> {
  const args = prepareSubagentControlArguments(raw);
  if (args.action === "send" && Buffer.byteLength(args.message) > 65536)
    return fixed("消息超过 64 KiB。", true);
  if (args.action === "list") {
    const rows = (
      await sql<{
        executionId: string;
        name: string;
        state: string;
        task: string;
      }>`select a.id as "executionId",r.id as "runId",a.agent_name as name,r.state,left(a.initial_input,160) as task
      from runs a join runs r on r.tenant_id=a.tenant_id and r.session_id=a.session_id and r.agent_id=a.id
        and not exists(select 1 from runs n where n.previous_run_id=r.id)
      where a.tenant_id=${o.tenant}::uuid and a.session_id=${o.session}::uuid and a.parent_agent_id=${parent}::uuid and a.agent_name is not null
      ${args.cursor ? sql`and a.id>${args.cursor}::uuid` : sql``} order by a.id limit 25`.execute(
        tx,
      )
    ).rows;
    return fixed(
      JSON.stringify({
        agents: rows.slice(0, 24),
        ...(rows.length > 24 ? { nextCursor: rows[23]!.executionId } : {}),
      }),
    );
  }
  const toParent = args.target === "parent",
    toRoot = args.target === "main";
  if (
    args.action === "send" &&
    (toRoot || toParent) &&
    (toRoot || caller.parent_agent_id === o.session)
  ) {
    return { kind: "send", value: { lane: 1, message: args.message, delivery: args.delivery } };
  }
  const target = toParent && caller.parent_agent_id ? caller.parent_agent_id : args.target;
  if (target === "parent" || target === "main") return fixed("这个目标不能用于此次操作。", true);
  const child = await findChild(tx, o, parent, target);
  if (!child)
    return fixed(`暂时查不到 ${args.target}。如果还需要它，用原来的名字和任务再试一次。`, true);
  if (args.action === "status" || args.action === "wait")
    return { kind: args.action === "status" ? "state" : "watch", child: child.id };
  if (child.root_run_id !== o.root || !["preparing", "running"].includes(child.state))
    return fixed("子 Agent 当前没有在这个 Run 中执行。");
  return {
    kind: args.action,
    child: child.id,
    ...(args.action === "send"
      ? { value: { message: args.message, delivery: args.delivery } }
      : {}),
  };
}
async function supervisor(
  tx: Tx,
  o: SubagentOffer,
  caller: Caller,
  args: Record<string, unknown>,
): Promise<Instruction> {
  const def = infrastructureToolDefinitions(true).find((t) => t.name === o.tool)!;
  if (!Value.Check(def.parameters, args) || Buffer.byteLength(JSON.stringify(args)) > 65536)
    return fixed("主管请求无效或超过 64 KiB。", true);
  if (o.tool === "contact_supervisor") {
    if (!caller.agent_id) return fixed("只有子 Agent 可以联系主管。", true);
    if (args.reason === "progress_update") return fixed("进度已收到。");
    await sql`update subagent_deliveries set due_at=now(),claim=null where child_id=${caller.id}::uuid and kind='watch' and not sent`.execute(
      tx,
    );
    return {
      kind: "question",
      child: caller.id,
      value: {
        expiresAt: new Date(Date.now() + 10 * 60_000).toISOString(),
        reason: args.reason,
        message: args.message,
        ...(args.interview ? { interview: args.interview } : {}),
      },
    };
  }
  const parent = caller.agent_id ?? o.session;
  const pending = (
    await sql<{
      id: string;
      child: string;
      invocation: string;
      value: Record<string, unknown>;
    }>`select d.operation_id as id,c.agent_id as child,c.id as invocation,d.value
      from subagent_deliveries d join runs c on c.id=d.child_id
      where d.root_id=${o.root}::uuid and c.parent_agent_id=${parent}::uuid and d.kind in ('question','answered')
      and d.writer=${o.writer}::uuid ${args.action === "pending" ? sql`and d.kind='question' and not d.sent and not(d.value?'reply') and (d.value->>'expiresAt')::timestamptz>now()` : sql`and d.operation_id::text=${typeof args.replyTo === "string" ? args.replyTo : ""}`}
      order by d.created_at,d.operation_id limit 24`.execute(tx)
  ).rows;
  if (args.action === "pending")
    return fixed(
      JSON.stringify({
        requests: pending.map((r) => ({ requestId: r.id, executionId: r.child, ...r.value })),
      }),
    );
  const question = pending[0];
  if (!args.replyTo || !question) return fixed("暂时查不到这个待答问题。", true);
  if (args.action === "reply") {
    if (typeof args.message !== "string" || !args.message.trim())
      return fixed("请输入答复。", true);
    if (question.value.reply !== undefined && question.value.reply !== args.message)
      return fixed("这个问题已经有不同的答复。", true);
    if (
      question.value.reply === undefined &&
      Date.parse(String(question.value.expiresAt)) <= Date.now()
    )
      return fixed("这个问题已经超时。", true);
    if (question.value.reply === undefined)
      await sql`update subagent_deliveries set value=value||jsonb_build_object('reply',${args.message}::text),due_at=now(),claim=null where operation_id=${question.id}::uuid`.execute(
        tx,
      );
  }
  return { kind: "watch", child: question.invocation };
}

export async function registerSubagent(db: Kysely<Database>, topic: string, o: SubagentOffer) {
  let input;
  if ("recover" in o.input) {
    const ref = await readSealedDelegationIntent(
      db,
      {
        tenantId: o.tenant,
        piSessionId: o.session,
        sessionId: o.session,
        runId: o.root,
        turnId: o.root,
        writerId: o.writer,
        fencingToken: o.epoch,
      },
      topic,
      o.lane,
      o.task,
    );
    if (
      ref.operationId !== o.input.recover ||
      ref.sequence !== o.input.seq ||
      ref.contextAnchor !== o.anchor
    )
      throw Error("Invalid delegation recovery source");
    input = { seq: ref.sequence, args: ref.arguments, call: ref.nativeCallId };
  } else input = o.input;
  const identity = hash(canonical(o));
  await db.transaction().execute(async (tx) => {
    await sql`set local synchronous_commit=on`.execute(tx);
    const root = (
      await sql<Caller>`select id,agent_id,parent_agent_id,workspace_grant from runs where id=${o.root}::uuid and tenant_id=${o.tenant}::uuid
      and session_id=${o.session}::uuid and root_run_id is null and lease_id=${o.writer}::uuid and fencing_token=${o.epoch} and state='running' for update`.execute(
        tx,
      )
    ).rows[0];
    if (!root) throw new SubagentAdmissionRefused("Root writer closed");
    const prior = (
      await sql<{
        operation_hash: string;
      }>`select operation_hash from subagent_deliveries where operation_id=${o.operation}::uuid`.execute(
        tx,
      )
    ).rows[0];
    if (prior) {
      if (prior.operation_hash !== identity)
        throw new SubagentAdmissionRefused("Delivery identity changed");
      return;
    }
    const caller =
      o.lane === 1
        ? root
        : (
            await sql<Caller>`select c.id,c.agent_id,a.parent_agent_id,c.workspace_grant from runs c join runs a on a.id=c.agent_id and a.tenant_id=c.tenant_id and a.session_id=c.session_id where c.root_run_id=${o.root}::uuid and c.tenant_id=${o.tenant}::uuid
      and c.session_id=${o.session}::uuid and c.native_lane=${o.lane} and c.state in ('preparing','running')`.execute(
              tx,
            )
          ).rows[0];
    if (!caller) throw new SubagentAdmissionRefused("Calling conversation closed");
    const parent = caller.agent_id ?? o.session;
    let instruction: Instruction;
    if (o.tool === "subagent")
      instruction = await create(tx, o, parent, caller, input.seq, input.args);
    else if (o.tool === "subagent_control")
      instruction = await control(tx, o, parent, caller, input.args);
    else instruction = await supervisor(tx, o, caller, input.args);
    await sql`insert into subagent_deliveries(operation_id,root_id,writer,caller_task,caller_lane,operation_hash,destination,call_id,tool,child_id,kind,value,due_at)
      values(${o.operation}::uuid,${o.root}::uuid,${o.writer}::uuid,${o.task},${o.lane},${identity},${o.destination},${input.call},${o.tool},
      ${instruction.child ?? null}::uuid,${instruction.kind},${JSON.stringify(instruction.value ?? {})}::jsonb,now())`.execute(
      tx,
    );
    await sql`select pg_notify('pma_named_children','')`.execute(tx);
  });
}
