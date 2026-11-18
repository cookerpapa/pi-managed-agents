import { writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { sql } from "kysely";

/** Read-only pg_stat_activity sampling. PostgreSQL query age is not CPU time:
 * preserve backend state and wait event instead of relabelling ClientRead as SQL. */
export async function sampleProjectorSql(database, roles, path, signal) {
  const samples = [],
    queries = new Map(),
    errors = [];
  while (!signal.aborted && samples.length < 12000) {
    const at = Date.now();
    try {
      const { rows } =
        await sql`select pid,host(client_addr) as client_addr,state,wait_event_type,wait_event,
        extract(epoch from query_start)*1000 as query_start_ms,
        extract(epoch from xact_start)*1000 as transaction_start_ms,
        left(query,12000) as query
        from pg_stat_activity where datname=current_database()
        and backend_type='client backend' and pid<>pg_backend_pid() and state<>'idle'`.execute(
          database,
        );
      samples.push({
        at,
        sampledAt: Date.now(),
        rows: rows.map(({ query, client_addr, ...row }) => {
          const text = query
            .replace(/'(?:[^']|'')*'/g, "'?'")
            .replace(/\s+/g, " ")
            .trim();
          const id = createHash("sha256").update(text).digest("hex").slice(0, 16);
          if (queries.size < 2048 && !queries.has(id)) queries.set(id, text);
          return { ...row, role: roles.get(client_addr) ?? "host-or-background", queryId: id };
        }),
      });
    } catch (error) {
      errors.push({ at, code: String(error.code ?? error.name) });
    }
    await delay(Math.max(1, 100 - (Date.now() - at)));
  }
  await writeFile(
    path,
    JSON.stringify({ intervalMs: 100, samples, queries: Object.fromEntries(queries), errors }) +
      "\n",
    { mode: 0o600 },
  );
}
