import { createHash, randomUUID } from "node:crypto";
import { sql, type Transaction, type Kysely, type RawBuilder } from "kysely";
import type { Database } from "@pi-cloud/database";
import type { StorageWrite } from "@earendil-works/pi-durable";
import type { DurableScope } from "./types.ts";
import type { HistoryWriteRow } from "./current-state.ts";
import { indexNative } from "./types.ts";
import { retainedAttachmentReferences } from "@pi-cloud/session-attachments";
import { collectSharedHistory } from "./history-collection.ts";

type EntryWrite = Extract<StorageWrite, { type: "entry" }>;

function splitEntry(write: EntryWrite) {
  const { id, conversationId, byTaskId, ...payload } = write.value;
  const text = JSON.stringify(payload);
  return {
    id,
    conversationId,
    byTaskId: byTaskId ?? null,
    text,
    digest: createHash("sha256").update(text).digest("hex"),
  };
}

/** PG's logical body has a stable identity envelope; native batch bytes stay unchanged. */
export function retainedHistoryBody(write: StorageWrite): string {
  if (write.type !== "entry") return JSON.stringify(write);
  const entry = splitEntry(write);
  return (
    `{"type":"entry","value":{"id":${entry.id},"conversationId":${entry.conversationId}` +
    (entry.byTaskId === null ? "" : `,"byTaskId":${entry.byTaskId}`) +
    `,${entry.text.slice(1)}}`
  );
}

export type NativeCommitMutations = {
  historyIndexes: RawBuilder<unknown> | undefined;
  checkpoints: RawBuilder<unknown> | undefined;
  currentIndexes: RawBuilder<unknown> | undefined;
  historyIndexCount: number;
  checkpointCount: number;
  finish?: (valid: RawBuilder<boolean>) => RawBuilder<unknown>;
};

/** Persist the commit receipt and its immutable history in one statement.
 * One immutable payload object may back independently owned Sessions. */
export async function writeNativeCommit(
  tx: Transaction<Database>,
  scope: DurableScope,
  commit: {
    seq: number;
    nextId: number;
    fingerprint: string;
    writeCount: number;
    historyFingerprint: string;
    currentHashes: Record<string, string>;
    cloudMetadata?: Readonly<Record<string, unknown>>;
  },
  history: readonly HistoryWriteRow[],
  mutations: NativeCommitMutations,
): Promise<void> {
  const { seq } = commit;
  const receipt = sql`insert into pi_durable_commits(tenant_id,session_id,seq,next_id,fingerprint,write_count,history_write_count,history_fingerprint,current_hashes,cloud_metadata)
    values(${scope.tenantId}::uuid,${scope.sessionId},${seq},${commit.nextId},${commit.fingerprint},${commit.writeCount},${history.length},${commit.historyFingerprint},${JSON.stringify(commit.currentHashes)}::jsonb,${commit.cloudMetadata === undefined ? null : JSON.stringify(commit.cloudMetadata)}) returning committed_at`;
  const ctes = [sql`commit_receipt as (${receipt})`];
  if (history.length) ctes.push(retainedHistoryCtes(scope, seq, history));
  else
    ctes.push(
      sql`written_refs as (select tenant_id,session_id,seq,ordinal,retained_record_id,clear_owner from pi_durable_write_refs where false)`,
    );

  const count = (name: string, mutation: RawBuilder<unknown> | undefined) => {
    if (!mutation) return sql`0`;
    ctes.push(sql`${sql.id(name)} as (${mutation})`);
    return sql`(select count(*)::int from ${sql.table(name)})`;
  };
  const historyIndexes = count("history_indexes", mutations.historyIndexes);
  const checkpoints = count("checkpoints", mutations.checkpoints);
  const currentIndexes = count("current_indexes", mutations.currentIndexes);
  ctes.push(sql`counts as materialized (select ${historyIndexes} as history_indexes,
    ${checkpoints} as checkpoints, ${currentIndexes} as current_indexes)`);
  if (mutations.finish) {
    const valid = sql<boolean>`(select history_indexes=${mutations.historyIndexCount}
      and checkpoints=${mutations.checkpointCount} and current_indexes=${mutations.checkpointCount} from counts)`;
    ctes.push(sql`completed as (${mutations.finish(valid)})`);
  }
  const {
    rows: [result],
  } = await sql<{ history_indexes: number; checkpoints: number; current_indexes: number }>`
    with ${sql.join(ctes)} select * from counts`.execute(tx);
  if (result?.checkpoints !== mutations.checkpointCount)
    throw new Error("Native current checkpoint changed identity or regressed");
  if (
    result.history_indexes !== mutations.historyIndexCount ||
    result.current_indexes !== mutations.checkpointCount
  )
    throw new Error("Native record ID ownership changed");
}

function retainedHistoryCtes(
  scope: DurableScope,
  seq: number,
  history: readonly HistoryWriteRow[],
) {
  const entries = new Map<number, ReturnType<typeof splitEntry>>();
  const fields = new Map<number, Record<string, string>>();
  const images: { ordinal: number; imageId: string }[] = [];
  const payloads = new Map<string, { id: string; text: string }>();
  for (const row of history) {
    const write = JSON.parse(row.body) as StorageWrite;
    for (const ref of retainedAttachmentReferences(write))
      images.push({ ordinal: row.ordinal, imageId: ref.id });
    if (row.record_kind !== "entry") continue;
    const entry = splitEntry(write as EntryWrite);
    fields.set(row.ordinal, {
      ...indexNative("entry", (write as EntryWrite).value, seq, row.ordinal).fields,
      ...(row.cloud_origin ?? {}),
    });
    entries.set(row.ordinal, entry);
    const previous = payloads.get(entry.digest);
    if (previous && previous.text !== entry.text)
      throw new Error("Native Entry payload digest conflict");
    if (!previous) payloads.set(entry.digest, { id: randomUUID(), text: entry.text });
  }
  // Intern bodies and install references in one SQL round trip. The conflict lock
  // protects in-flight references; the write-body CHECK rejects a digest mismatch.
  const objects = payloads.size
    ? sql`insert into pi_history_bodies(tenant_id,id,sha256,payload_text,payload_bytes)
    values ${sql.join([...payloads].sort(([a], [b]) => a.localeCompare(b)).map(([hash, v]) => sql`(${scope.tenantId}::uuid,${v.id}::uuid,${hash},${v.text},${Buffer.byteLength(v.text)})`))}
    on conflict(tenant_id,body_kind,sha256) do update set sha256=excluded.sha256
      where pi_history_bodies.payload_text=excluded.payload_text returning id,sha256`
    : sql`select null::uuid as id,null::text as sha256 where false`;
  return sql`stream as (
    insert into pi_history_streams(tenant_id,id) values(${scope.tenantId}::uuid,${scope.sessionId}) on conflict do nothing
  ), bodies as (${objects}), writes(ordinal,record_kind,record_id,body,phase,cloud_origin,digest,conversation_id,task_id,fields) as (
    values ${sql.join(
      history.map((row) => {
        const e = entries.get(row.ordinal);
        return sql`(${row.ordinal}::int,${row.record_kind}::text,${row.record_id}::bigint,${e ? null : row.body}::text,${row.phase}::text,
        ${row.cloud_origin === null ? null : JSON.stringify(row.cloud_origin)}::jsonb,${e?.digest ?? null}::text,
        ${e?.conversationId ?? null}::bigint,${e?.byTaskId ?? null}::bigint,${JSON.stringify(fields.get(row.ordinal) ?? {})}::jsonb)`;
      }),
    )}), retained as (
    insert into pi_history_records(tenant_id,session_id,seq,ordinal,record_kind,record_id,body,phase,cloud_origin,
      entry_body_id,entry_conversation_id,entry_task_id,committed_at,fields)
    select ${scope.tenantId}::uuid,${scope.sessionId},${seq},w.ordinal,w.record_kind,w.record_id,w.body,w.phase,w.cloud_origin,
      b.id,w.conversation_id,w.task_id,
      (select committed_at from commit_receipt),w.fields
      from writes w left join bodies b on b.sha256=w.digest
      returning tenant_id,session_id,seq,ordinal,id
    ), image_refs as (
      ${
        images.length
          ? sql`insert into pi_history_record_attachments(tenant_id,retained_record_id,attachment_id)
        select r.tenant_id,r.id,v.attachment_id from retained r join (values ${sql.join(images.map((v) => sql`(${v.ordinal}::int,${v.imageId}::uuid)`))}) v(ordinal,attachment_id) using(ordinal)
        on conflict do nothing`
          : sql`select 1`
      }
    ), written_refs as (insert into pi_durable_write_refs(tenant_id,session_id,seq,ordinal,retained_record_id)
      select tenant_id,session_id,seq,ordinal,id from retained returning tenant_id,session_id,seq,ordinal,retained_record_id,clear_owner)`;
}

/** Only unreferenced PG objects; retiring native histories is a separate guarded operation. */
export async function reapUnreferencedEntryBodies(
  database: Kysely<Database>,
  limit = 64,
): Promise<number> {
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1024)
    throw new Error("Invalid Entry body cleanup batch");
  return database.transaction().execute(async (tx) => {
    await collectSharedHistory(tx, limit);
    const { rows } = await sql<{ tenant_id: string; id: string }>`
      select b.tenant_id,b.id from pi_history_bodies b
      where not exists(select 1 from pi_history_records w
        where w.tenant_id=b.tenant_id and w.entry_body_id=b.id)
        and not exists(select 1 from pi_history_frames f where f.tenant_id=b.tenant_id and f.body_id=b.id)
        and not exists(select 1 from pi_history_blocks h where h.tenant_id=b.tenant_id and h.header_body_id=b.id)
        and not exists(select 1 from pi_history_view_nodes n where n.tenant_id=b.tenant_id and n.header_body_id=b.id)

      order by b.tenant_id,b.id limit ${limit} for update of b skip locked`.execute(tx);
    if (!rows.length) return 0;
    const deleted = await sql`delete from pi_history_bodies b
      where (tenant_id,id) in (${sql.join(rows.map((r) => sql`(${r.tenant_id}::uuid,${r.id}::uuid)`))})
        and not exists(select 1 from pi_history_records w
          where w.tenant_id=b.tenant_id and w.entry_body_id=b.id)
        and not exists(select 1 from pi_history_frames f where f.tenant_id=b.tenant_id and f.body_id=b.id)
        and not exists(select 1 from pi_history_blocks h where h.tenant_id=b.tenant_id and h.header_body_id=b.id)
        and not exists(select 1 from pi_history_view_nodes n where n.tenant_id=b.tenant_id and n.header_body_id=b.id)

      returning id`.execute(tx);
    return deleted.rows.length;
  });
}
