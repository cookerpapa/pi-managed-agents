// Fixed prefix control for the usage study. Own the disposable database, warm
// the plans, and compare the two real projection statements on the same bytes.
import pg from "pg";
import { testPostgres } from "../../lib/test-postgres.mjs";
import {
  installProjection,
  resetProjection,
  project,
  verifyProjection,
} from "./fixed-usage-projection.mjs";
const fixture = await testPostgres({}),
  db = new pg.Client(fixture.url);
async function measure(bulk) {
  await resetProjection(db);
  const elapsed = await project(db, bulk);
  await verifyProjection(db);
  return elapsed;
}
try {
  await db.connect();
  await installProjection(db);
  await measure(false);
  await measure(true);
  const pairs = [];
  for (let i = 0; i < 12; i++) {
    const pair = {};
    for (const bulk of i % 2 ? [true, false] : [false, true])
      pair[bulk ? "bulkMs" : "rowMs"] = await measure(bulk);
    pairs.push(pair);
  }
  console.log(
    JSON.stringify(
      {
        postgres: (await db.query("show server_version")).rows[0].server_version,
        clients: 1,
        framesPerTransaction: 16,
        usageRowsPerTransaction: 8,
        transactionsPerSample: 64,
        entriesPerSample: 1024,
        pairs,
      },
      null,
      2,
    ),
  );
} finally {
  try {
    await db.end();
  } finally {
    await fixture.close();
  }
}
