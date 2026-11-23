import { sql, type Kysely } from "kysely";
export async function up(db: Kysely<unknown>) {
  await sql`alter table sessions add column workspace_directory_missing boolean not null default false`.execute(
    db,
  );
}
export async function down(db: Kysely<unknown>) {
  await sql`alter table sessions drop column workspace_directory_missing`.execute(db);
}
