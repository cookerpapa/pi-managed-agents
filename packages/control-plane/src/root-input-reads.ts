// Current-value root input reads. The original Run header remains the first
// source for retries of the message that created a Run.
import { sql, type Kysely } from "kysely";
import type { Database } from "@pi-cloud/database";
import {
  nativeCompactionState,
  type NativeCompactReceipt,
  emptyHumanInputs,
  materializeHumanQueue,
  type StoredHumanInputs,
} from "@pi-cloud/pi-session-postgres/durable";
type Scope = { tenantId: string; sessionId: string; conversationId: number };
type Submission = { id: number; status: string; entry?: number; reason?: string };
export async function rootSubmission(db: Kysely<Database>, s: Scope, id: string) {
  const {
    rows: [r],
  } = await sql<{
    run_id: string;
    body: Submission;
  }>`select r.id as run_id,r.root_input_receipts->${id} as body from runs r join sessions s on s.id=r.session_id and s.tenant_id=r.tenant_id
    where r.tenant_id=${s.tenantId}::uuid and s.pi_session_id=${s.sessionId} and s.pi_conversation_id=${s.conversationId} and r.root_input_receipts ? ${id} and r.pruned_at is null limit 1`.execute(
    db,
  );
  return r ? { ...r.body, runId: r.run_id } : undefined;
}
export async function rootQueueHead(db: Kysely<Database>, s: Omit<Scope, "conversationId">) {
  const {
    rows: [r],
  } = await sql<{
    seq: string;
  }>`select greatest(coalesce(max((r.history_head->>'seq')::bigint),0),coalesce(max(r.root_input_seq),0))::text as seq
    from runs r join sessions s on s.id=r.session_id and s.tenant_id=r.tenant_id where r.tenant_id=${s.tenantId}::uuid and s.pi_session_id=${s.sessionId} and r.pruned_at is null`.execute(
    db,
  );
  return Number(r!.seq);
}
export async function rootHumanInputs(db: Kysely<Database>, s: Scope & { runId: string }) {
  const {
    rows: [r],
  } = await sql<{
    items: Array<{ id: number; mode: string }>;
    metadata: string | null;
    seq: string;
  }>`select i.items,r.root_human_inputs as metadata,greatest(coalesce(i.commit_seq,0),r.root_input_seq,coalesce((r.history_head->>'seq')::bigint,0))::text as seq
    from runs r join sessions s on s.tenant_id=r.tenant_id and s.id=r.session_id left join inboxes i on i.tenant_id=r.tenant_id and i.pi_session_id=s.pi_session_id::uuid and i.conversation_id=${s.conversationId}
    where r.tenant_id=${s.tenantId}::uuid and s.pi_session_id=${s.sessionId} and r.id=${s.runId}::uuid and r.pruned_at is null`.execute(
    db,
  );
  if (!r) throw Error("Missing root input Run");
  return materializeHumanQueue(
    s.runId,
    r.metadata ? JSON.parse(r.metadata) : undefined,
    { items: r.items ?? [] },
    Number(r.seq),
  );
}
export async function rootInputMetadata(
  db: Kysely<Database>,
  s: Omit<Scope, "conversationId">,
  requests: readonly { runId: string; conversationId: number }[],
) {
  const { rows } = await sql<{
    id: string;
    metadata: string | null;
  }>`select r.id,r.root_human_inputs as metadata from runs r join sessions s on s.id=r.session_id and s.tenant_id=r.tenant_id
    where r.tenant_id=${s.tenantId}::uuid and s.pi_session_id=${s.sessionId} and r.pruned_at is null and r.id=any(${requests.filter((r) => r.conversationId === 1).map((r) => r.runId)}::uuid[])`.execute(
    db,
  );
  return new Map<string, StoredHumanInputs>(
    rows.map((r) => [r.id, r.metadata ? JSON.parse(r.metadata) : emptyHumanInputs(r.id)]),
  );
}

export async function rootCompaction(db: Kysely<Database>, s: Scope, commandId?: string) {
  const id = commandId ? sql`${commandId}::text` : sql`r.root_compacts->>'latest'`;
  const {
    rows: [r],
  } = await sql<{
    receipt: NativeCompactReceipt;
    task: Parameters<typeof nativeCompactionState>[0] | null;
    submission: Parameters<typeof nativeCompactionState>[1] | null;
    seq: number;
  }>`
    select x.receipt,y.task,r.root_compacts->'submissions'->(y.task#>>'{state,outcome,result,submissionId}') as submission,(r.root_compacts->>'seq')::bigint as seq
    from runs r join sessions s on s.tenant_id=r.tenant_id and s.id=r.session_id
    cross join lateral(select r.root_compacts->'requests'->(${id}) as receipt)x
    cross join lateral(select r.root_compacts->'tasks'->(x.receipt->>'taskId') as task)y
    where r.tenant_id=${s.tenantId}::uuid and s.pi_session_id=${s.sessionId} and s.pi_conversation_id=${s.conversationId} and r.pruned_at is null
      and x.receipt is not null order by r.mailbox_position desc limit 1`.execute(db);
  if (!r) return;
  if (!r.task) throw Error("Root compaction task is missing");
  return {
    ...r.receipt,
    state: nativeCompactionState(r.task, r.submission ?? undefined),
    nativeSequence: Number(r.seq),
  };
}
