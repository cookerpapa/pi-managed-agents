import { NotFoundException } from "@nestjs/common";
import { SubagentConversationError } from "@pi-cloud/subagent-control/subagent-conversations";
import type { Database } from "@pi-cloud/database";
import { sql, type Kysely } from "kysely";
import {
  ControlPlaneApiValidationError,
  HISTORY_PAGE_BYTES,
  parseDelegatedAgentDirectory,
  parseDelegatedAgentHistory,
} from "@pi-cloud/protocol";
import {
  projectDurableTranscript,
  type NativeHistoryRow,
} from "@pi-cloud/runtime-core/native-transcript-presentation";

// Shared reads over stable Agent identities and invocation-specific native lanes.
export class DelegatedHistoryReader {
  constructor(readonly db: Kysely<Database>) {}
  async directory(tenant: string, root: string, after?: string) {
    const { rows } = await sql<{
      agentId: string;
      name: string | null;
      task: string;
      state: string;
    }>`select c.agent_id as "agentId",a.agent_name as name,left(btrim(a.initial_input),160) as task,c.state from runs c
      join runs a on a.id=c.agent_id and a.tenant_id=c.tenant_id and a.session_id=c.session_id
      where c.tenant_id=${tenant}::uuid and c.session_id=${root}::uuid and c.agent_id is not null
        and not exists(select 1 from runs n where n.previous_run_id=c.id)
      ${after ? sql`and c.agent_id>${after}::uuid` : sql``} order by c.agent_id limit 25`.execute(
      this.db,
    );
    const agents = rows
      .slice(0, 24)
      .map(({ name, ...agent }) => ({ ...agent, ...(name ? { name } : {}) }));
    return parseDelegatedAgentDirectory({
      agents,
      ...(rows.length > 24 ? { nextCursor: agents.at(-1)!.agentId } : {}),
    });
  }
  async readTool(
    tenant: string,
    root: string,
    run: string,
    args: { action: "list"; cursor?: string } | { action: "status" | "wait"; target: string },
  ): Promise<Record<string, unknown> | undefined> {
    if (args.action === "list") {
      const { rows } = await sql<{
        agent: string;
        id: string;
        state: string;
        task: string;
        source: string;
      }>`select agent_id as agent,id,state,left(btrim(initial_input),160) as task,source_task_id as source
        from runs r where r.tenant_id=${tenant}::uuid and r.session_id=${root}::uuid and r.root_run_id=${run}::uuid
        and not exists(select 1 from runs n where n.previous_run_id=r.id and n.root_run_id=r.root_run_id)
        ${args.cursor ? sql`and id>${args.cursor}::uuid` : sql``} order by id limit 25`.execute(
        this.db,
      );
      return {
        agents: rows.slice(0, 24).map((r) => ({
          executionId: r.agent,
          runId: r.id,
          state: r.state,
          task: r.task,
          sourceTaskId: Number(r.source),
        })),
        ...(rows.length > 24 ? { nextCursor: rows[23]!.id } : {}),
      };
    }
    if (!/^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$/i.test(args.target))
      throw new SubagentConversationError("not_found", "Subagent is unavailable");
    const {
      rows: [r],
    } = await sql<{
      id: string;
      state: string;
      body: string | null;
    }>`select c.id,c.state,${args.action === "wait" ? sql`a.content` : sql`null::text`} body
      from runs c ${args.action === "wait" ? sql`left join lateral(select m.content from message_entries m where m.tenant_id=c.tenant_id and m.pi_session_id=c.session_id and m.run_id=c.root_run_id and m.conversation_id=c.native_lane and m.role='assistant' and c.state in ('completed','failed','cancelled') order by m.entry_id desc limit 1)a on true` : sql``}
      where c.tenant_id=${tenant}::uuid and c.session_id=${root}::uuid and c.agent_id=${args.target}::uuid and not exists(select 1 from runs n where n.previous_run_id=c.id)`.execute(
      this.db,
    );
    if (!r) throw new SubagentConversationError("not_found", "Subagent is unavailable");
    if (args.action === "wait" && ["running", "preparing"].includes(r.state)) return;
    const content = r.body
      ? JSON.parse(r.body).model?.find((m: { role: string }) => m.role === "assistant")?.content
      : undefined;
    const output = Array.isArray(content)
      ? content
          .filter((b) => b.type === "text" && typeof b.text === "string")
          .map((b) => b.text)
          .join("")
      : undefined;
    return { executionId: args.target, runId: r.id, state: r.state, ...(output ? { output } : {}) };
  }
  async history(tenant: string, root: string, agent: string, before?: string) {
    const [address, sequence, position] = before?.split(".") ?? [];
    const seq = Number(sequence),
      ordinal = Number(position);
    if (
      before !== undefined &&
      (address !== agent ||
        !/^[0-9a-f-]{36}\.[1-9]\d*\.(?:0|[1-9]\d*)$/.test(before) ||
        !Number.isSafeInteger(seq) ||
        !Number.isSafeInteger(ordinal) ||
        ordinal > 2147483647)
    )
      throw new ControlPlaneApiValidationError("Invalid delegated history cursor");
    return this.db
      .transaction()
      .setIsolationLevel("repeatable read")
      .setAccessMode("read only")
      .execute(async (tx) => {
        const origin =
          await sql`select id from runs where tenant_id=${tenant}::uuid and session_id=${root}::uuid and agent_id=id and id=${agent}::uuid`.execute(
            tx,
          );
        if (!origin.rows.length) throw new NotFoundException("Agent is unavailable");
        type Row = {
          entry_id: string;
          run_id: string;
          commit_seq: string;
          ordinal: number;
          role: string;
          content: string;
          created_at: Date;
          source_input_id: string | null;
          assistant_id: string | null;
          proposal: string | null;
          tool_task_id: string | null;
          more: boolean;
        };
        const { rows } = await sql<Row>`with recent as materialized (
        select c.id as run_id,m.entry_id,m.commit_seq,m.ordinal,m.role,m.created_at,m.source_input_id,m.tool_task_id,
        a.entry_id as assistant_id,octet_length(m.content)+coalesce(octet_length(a.content),0) as entry_bytes
        from runs c join message_entries m on m.tenant_id=c.tenant_id and m.pi_session_id=c.session_id and m.run_id=c.root_run_id and m.conversation_id=c.native_lane
        left join message_entries a on a.tenant_id=m.tenant_id and a.pi_session_id=m.pi_session_id and a.run_id=m.run_id and a.conversation_id=m.conversation_id and a.entry_id=m.assistant_entry_id
        where c.tenant_id=${tenant}::uuid and c.session_id=${root}::uuid and c.agent_id=${agent}::uuid
        and m.role in ('user','assistant','toolResult','pi.compaction')
        ${before ? sql`and (m.commit_seq,m.ordinal)<(${seq}::bigint,${ordinal}::int)` : sql``}
        order by m.commit_seq desc,m.ordinal desc limit 33),
        sized as(select *,row_number() over w as n,sum(entry_bytes) over w as bytes,count(*) over() as total
          from recent window w as(order by commit_seq desc,ordinal desc))
        select s.*,m.content,a.content as proposal,n<total as more from sized s
          join message_entries m on m.tenant_id=${tenant}::uuid and m.pi_session_id=${root}::uuid and m.entry_id=s.entry_id
          left join message_entries a on a.tenant_id=m.tenant_id and a.pi_session_id=m.pi_session_id and a.entry_id=s.assistant_id
          where n<=32 and (n=1 or bytes<=${HISTORY_PAGE_BYTES}) order by n`.execute(tx);
        const end = rows.at(-1);
        const entries = [...rows].reverse().map((r) => {
          const value = JSON.parse(r.content);
          const seq = Number(r.commit_seq);
          const record = (id: string, content: string): NativeHistoryRow => ({
            id,
            seq: r.commit_seq,
            fields: {},
            body: `{"value":${content}}`,
            committed_at: r.created_at,
          });
          const row = record(r.entry_id, r.content);
          if (r.tool_task_id) row.task_kind = '"pi.tool"';
          const input = value.model?.find((m: { role: string }) => m.role === "user")?.content;
          const text =
            typeof input === "string"
              ? input
              : Array.isArray(input)
                ? input
                    .filter((x) => x.type === "text")
                    .map((x) => x.text)
                    .join("")
                : "";
          const dep = r.proposal && r.assistant_id ? record(r.assistant_id, r.proposal) : null;
          const { items } = projectDurableTranscript(
            dep ? [dep, row] : [row],
            {
              throughSequence: seq,
              terminalSequence: null,
              stopReason: null,
              failure: null,
              cancellation: null,
              occurredAt: r.created_at.toISOString(),
            },
            new Set(dep ? [dep.id] : []),
            new Map([
              [r.entry_id, { inputId: r.source_input_id ?? r.entry_id, text, uploads: [] }],
            ]),
            root,
          );
          const calls =
            value.kind === "pi.assistant"
              ? value.model?.flatMap(
                  (m: {
                    role: string;
                    content?: Array<{ type: string; name?: string; arguments?: unknown }>;
                  }) =>
                    m.role === "assistant" && Array.isArray(m.content)
                      ? m.content
                          .filter((b) => b.type === "toolCall" && typeof b.name === "string")
                          .map((b) => ({ name: b.name, input: b.arguments ?? null }))
                      : [],
                )
              : [];
          return {
            entryId: Number(r.entry_id),
            runId: r.run_id,
            items,
            ...(calls?.length ? { calls } : {}),
          };
        });
        return parseDelegatedAgentHistory({
          entries,
          ...(end?.more ? { before: `${agent}.${end.commit_seq}.${end.ordinal}` } : {}),
        });
      });
  }
}
