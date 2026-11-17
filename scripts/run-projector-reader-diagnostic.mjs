import assert from "node:assert/strict";
import { readFile, mkdir, writeFile } from "node:fs/promises";
import { resolve, dirname } from "node:path";
import { sql } from "kysely";
import { createDatabase } from "../packages/database/src/index.ts";
import { kubectl, podFor, runtimeDirectory, serviceAddress } from "./lib/production-kubernetes.mjs";
import { serializeRelease } from "./lib/release-lock.mjs";

assert.equal(
  process.env.PI_CLOUD_READER_DIAGNOSTIC,
  "1",
  "Set PI_CLOUD_READER_DIAGNOSTIC=1 for serial owned RF3 Topic diagnostics",
);
serializeRelease(runtimeDirectory);
const output = resolve(process.argv[2] ?? ".cache/projector-reader-diagnostic.json");
const url = new URL(
  (await readFile(resolve(runtimeDirectory, "secrets/database-url"), "utf8")).trim(),
);
url.hostname = serviceAddress("postgres");
const database = createDatabase({ connectionString: url.toString(), maxConnections: 1 });
try {
  const { rows } =
    await sql`select count(*)::int as active from runs where state not in ('completed','failed','cancelled','timed_out') or recovery_requested_at is not null`.execute(
      database,
    );
  assert.equal(rows[0].active, 0, "Unrelated active work prevents this diagnostic");
} finally {
  await database.destroy();
}
const source = await readFile(
  new URL("./lib/projector-reader-benchmark.mjs", import.meta.url),
  "utf8",
);
const stdout = kubectl(
  ["exec", "-i", podFor("tool-host"), "--", "node", "--import", "tsx", "--input-type=module"],
  { input: source, timeout: 420000, maxBuffer: 4 * 1024 ** 2 },
);
const result = stdout
  .trim()
  .split("\n")
  .map((line) => JSON.parse(line))
  .findLast((row) => row.report);
assert(result?.cleanupConfirmed, "Diagnostic did not confirm owned Topic cleanup");
assert(
  result.report.results.every((row) => row.seen === result.report.total),
  "Diagnostic lost records",
);
await mkdir(dirname(output), { recursive: true, mode: 0o700 });
await writeFile(output, JSON.stringify(result, null, 2) + "\n", { mode: 0o600 });
console.log(JSON.stringify({ output, cleanupConfirmed: true, results: result.report.results }));
