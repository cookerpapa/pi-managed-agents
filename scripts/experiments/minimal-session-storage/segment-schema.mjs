import { readFile } from "node:fs/promises";
export async function installSessionTables(db) {
  await db.query(await readFile(new URL("./schema.sql", import.meta.url), "utf8"));
  await db.query(`alter table runs add output_first_topic text,add output_first_partition int,add output_first_offset bigint,add output_display_seq bigint default 0,add output_display_native_seq bigint default 0,
   add turn_id uuid generated always as(id) stored,add input_text text generated always as(initial_input) stored,add input_uploads jsonb default '[]',
   add pruned_at timestamptz,add operation_kind text default 'chat',add stop_reason text,add scheduled_origin jsonb,add output_seal_id uuid,add output_sealed_at timestamptz;
`);
}
