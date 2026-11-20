import assert from "node:assert/strict";
import test from "node:test";
import { observeFlush } from "./pg-path-measure.mjs";
import {
  asyncLiveCases,
  asyncReplayCases,
  commitProfiles,
  commitController,
  groupLiveCases,
  groupReplayCases,
  verifyCommitClients,
} from "./pg-group-commit.mjs";

test("the post-ACK read-only observer holds one WAL target even when newer writes arrive", async () => {
  const target = "0/428",
    remaining = [256, -96];
  let probes = 0;
  const client = {
    async query(sql, args) {
      assert(!/checkpoint|pg_switch_wal/i.test(sql));
      if (!args) {
        assert.equal(probes++, 0);
        return { rows: [{ target, bytes: 512 }] };
      }
      assert.deepEqual(args, [target]);
      assert(!sql.includes("pg_current_wal_insert_lsn"));
      return { rows: [{ bytes: remaining.shift() }] };
    },
  };
  const result = await observeFlush(client, performance.timeOrigin + performance.now());
  assert.equal(result.initialGapBytes, 512);
  assert.equal(result.polls, 2);
  assert(result.coveredMs >= result.firstProbeMs);
  assert.equal(remaining.length, 0);
});

test("async trial pairs hold the write path and offered rate fixed", () => {
  const cases = asyncReplayCases(false);
  for (const stages of Map.groupBy(cases, ([rate, path]) => rate + "/" + path).values()) {
    assert.equal(
      stages.filter((s) => s[4] === "async").length,
      stages.filter((s) => s[4] === "default").length,
    );
    assert(stages.every((s) => s[2] && s[3] === "open"));
  }
  for (const stages of Map.groupBy(asyncLiveCases(false), ([rate]) => rate).values()) {
    assert.equal(
      stages.filter((s) => s[1] === "async").length,
      stages.filter((s) => s[1] === "default").length,
    );
  }
});
test("async mode still needs fsync, zero group delay and twelve verified writers", async () => {
  const cfg = { delayUs: 0, siblings: 5, fsync: "on", sync: "off", method: "fdatasync" };
  const clients = Array.from({ length: 12 }, (_, i) => ({
    processID: i + 1,
    query: async () => ({ rows: [{ ...cfg }] }),
  }));
  assert.equal((await verifyCommitClients(clients, "async")).sync, "off");
  await assert.rejects(verifyCommitClients(clients, "default"));
  cfg.fsync = "off";
  await assert.rejects(verifyCommitClients(clients, "async"));
});

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
  await assert.rejects(commitController(pool, undefined));
  await assert.rejects(
    commitController(pool, {
      pg: "pma-cpu-1234567890abcdef-pg",
      url: new URL("postgresql://bench@different-host/postgres"),
    }),
  );
  assert.equal(queries, 0);
});

test("only the async study can change synchronous_commit and it restores the default", async () => {
  const db = "pma_sessbench_1234567890abcdef",
    url = new URL("postgresql://bench@study-host/" + db);
  const fixture = { pg: "pma-cpu-1234567890abcdef-pg", url };
  const settings = { ...commitProfiles.default, fsync: "on", sync: "on", method: "fdatasync" };
  const changes = [];
  const pool = {
    options: { connectionString: url.href },
    async query(sql) {
      if (sql.startsWith("select current_database")) return { rows: [{ db, role: "bench" }] };
      if (sql.startsWith("select current_setting")) return { rows: [{ ...settings }] };
      if (sql.startsWith("select pg_reload_conf")) return { rows: [{ ok: true }] };
      changes.push(sql);
      if (sql === "alter system set synchronous_commit = 'off'") settings.sync = "off";
      else if (sql === "alter system reset synchronous_commit") settings.sync = "on";
      else assert.fail("Unexpected configuration mutation: " + sql);
      return { rows: [] };
    },
  };
  const group = await commitController(pool, fixture);
  await assert.rejects(group.apply("async"));
  const asyncMode = await commitController(pool, fixture, true);
  await assert.rejects(asyncMode.apply("short"));
  assert.deepEqual(changes, []);
  await asyncMode.apply("async");
  assert.equal(settings.sync, "off");
  assert.deepEqual(await asyncMode.restore(), asyncMode.start);
  assert.deepEqual(changes, [
    "alter system set synchronous_commit = 'off'",
    "alter system reset synchronous_commit",
  ]);
});
