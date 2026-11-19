import assert from "node:assert/strict";
import test from "node:test";
import {
  commitProfiles,
  groupController,
  groupLiveCases,
  groupReplayCases,
  verifyCommitClients,
} from "./pg-group-commit.mjs";

test("260/s switchbacks balance positions and preceding treatments", () => {
  const settings = Object.keys(commitProfiles).sort();
  const cases = groupReplayCases(false)
    .filter(([rate]) => rate === 260)
    .map((c) => c[4]);
  const preceding = new Set();
  for (let block = 0; block < 4; block++) {
    const keys = cases.slice(4 * block, 4 * block + 4);
    assert.deepEqual([...keys].sort(), settings);
    for (let i = 1; i < 4; i++) preceding.add(keys[i - 1] + "/" + keys[i]);
  }
  assert.equal(preceding.size, 12);
  for (let pos = 0; pos < 4; pos++)
    assert.deepEqual(cases.filter((_, i) => i % 4 === pos).sort(), settings);
  const live = groupLiveCases(false);
  assert.deepEqual(live.slice(0, 4), live.slice(4).reverse());
  const confirmation = groupReplayCases(false, true);
  assert.equal(confirmation.filter((c) => c[4] === "unconditional").length, 4);
  assert.equal(confirmation.filter((c) => c[4] === "default").length, 4);
  for (const rate of [10, 260]) {
    const keys = groupLiveCases(false, true)
      .filter((c) => c[0] === rate)
      .map((c) => c[1]);
    assert.deepEqual([...keys].sort(), ["default", "default", "unconditional", "unconditional"]);
  }
});
test("all twelve actual connections must agree and remain synchronous", async () => {
  const config = { ...commitProfiles.short, fsync: "on", sync: "on", method: "fdatasync" };
  const clients = Array.from({ length: 12 }, (_, i) => ({
    processID: i + 1,
    query: async () => ({ rows: [{ ...config }] }),
  }));
  assert.equal((await verifyCommitClients(clients, "short")).verifiedConnections, 12);
  await assert.rejects(verifyCommitClients(clients, "long"));
  await assert.rejects(verifyCommitClients([...clients.slice(1), clients[1]], "short"));
  clients[7].query = async () => ({ rows: [{ ...config, sync: "off" }] });
  await assert.rejects(verifyCommitClients(clients, "short"));
});
test("settings controller rejects an unrelated PG endpoint before issuing a query", async () => {
  let queries = 0;
  const pool = {
    options: { connectionString: "postgresql://bench@localhost/pma_sessbench_1234567890abcdef" },
    query() {
      queries++;
    },
  };
  await assert.rejects(groupController(pool, undefined));
  await assert.rejects(
    groupController(pool, {
      pg: "pma-cpu-1234567890abcdef-pg",
      url: new URL("postgresql://bench@different-host/postgres"),
    }),
  );
  assert.equal(queries, 0);
});
