import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";

// commit_delay is in microseconds. Zero siblings provides a sensitivity check
// when the default threshold of five other active transactions is seldom met.
export const commitProfiles = Object.freeze({
  default: { delayUs: 0, siblings: 5 },
  short: { delayUs: 500, siblings: 5 },
  long: { delayUs: 1500, siblings: 5 },
  unconditional: { delayUs: 1500, siblings: 0 },
});
export const isAsyncStudy = (name) => ["async-commit", "async-smoke", "async-peak"].includes(name);
export const isCommitStudy = (name) =>
  ["group-commit", "group-smoke", "group-confirm"].includes(name) || isAsyncStudy(name);

export function asyncReplayCases(smoke, peak = false) {
  const rows = (rate, path, keys) => keys.map((key) => [rate, path, true, "open", key]);
  const on = "default",
    off = "async";
  if (smoke) return rows(260, "projector", [on, off]);
  if (peak) return rows(520, "projector", [on, off, off, on]);
  return [
    ...rows(260, "projector", [on, off, off, on, off, on, on, off]),
    ...rows(520, "projector", [off, on, on, off]),
    ...rows(520, "direct", [on, off, off, on]),
  ];
}
export function asyncLiveCases(smoke, peak = false) {
  const on = "default",
    off = "async",
    at = (rate, keys) => keys.map((key) => [rate, key]);
  if (smoke) return at(260, [on, off]);
  if (peak) return [...at(520, [off, on, on, off]), ...at(260, [off, on])];
  return [...at(260, [on, off, off, on, off, on, on, off]), ...at(10, [on, off, off, on])];
}
function expected(key) {
  if (key === "async") return { ...commitProfiles.default, sync: "off" };
  assert(Object.hasOwn(commitProfiles, key), "Unknown PG commit setting");
  return { ...commitProfiles[key], sync: "on" };
}

export function groupReplayCases(smoke, confirm = false) {
  const rows = (rate, keys) => keys.map((key) => [rate, "projector", true, "open", key]);
  if (smoke) return rows(260, ["default", "short", "long", "unconditional"]);
  if (confirm)
    return rows(260, [
      "unconditional",
      "default",
      "default",
      "unconditional",
      "default",
      "unconditional",
      "unconditional",
      "default",
    ]);
  // Each setting occupies each ordinal once and follows every other setting.
  const balanced = [
    ["default", "short", "unconditional", "long"],
    ["short", "long", "default", "unconditional"],
    ["long", "unconditional", "short", "default"],
    ["unconditional", "default", "long", "short"],
  ];
  return [
    ...rows(260, balanced.flat()),
    ...rows(520, [
      "default",
      "short",
      "long",
      "unconditional",
      "unconditional",
      "long",
      "short",
      "default",
    ]),
  ];
}
export function groupLiveCases(smoke, confirm = false) {
  const at = (rate, keys) => keys.map((key) => [rate, key]);
  if (smoke) return at(260, ["default", "unconditional"]);
  if (confirm)
    return [
      ...at(260, ["unconditional", "default", "default", "unconditional"]),
      ...at(10, ["default", "unconditional", "unconditional", "default"]),
    ];
  return at(260, [
    "default",
    "short",
    "long",
    "unconditional",
    "unconditional",
    "long",
    "short",
    "default",
  ]);
}
const settingsSql = `select current_setting('commit_delay')::int as "delayUs",
  current_setting('commit_siblings')::int as siblings, current_setting('fsync') as fsync,
  current_setting('synchronous_commit') as sync, current_setting('wal_sync_method') as method`;
const read = async (client) => (await client.query(settingsSql)).rows[0];

export async function verifyCommitClients(clients, key) {
  const target = expected(key);
  assert.equal(clients.length, 12);
  assert.equal(new Set(clients.map((c) => c.processID)).size, 12);
  const settings = await Promise.all(clients.map(read));
  for (const s of settings) {
    assert.equal(s.delayUs, target.delayUs);
    assert.equal(s.siblings, target.siblings);
    assert.equal(s.sync, target.sync);
    assert.equal(s.fsync, "on");
    assert.equal(s.method, settings[0].method);
  }
  return { key, ...settings[0], verifiedConnections: settings.length };
}

// Only the disposable server owned by the existing K3s measurement fixture
// may change postmaster configuration. Never touch the serving PG instance.
export async function commitController(pool, fixture, asyncStudy = false) {
  assert(fixture && /^pma-cpu-[0-9a-f]{16}-pg$/.test(fixture.pg));
  const u = new URL(pool.options.connectionString);
  assert.equal(u.host, fixture.url.host);
  assert.equal(u.username, "bench");
  assert.match(u.pathname, /^\/pma_sessbench_[0-9a-f]{16}$/);
  const {
    rows: [identity],
  } = await pool.query("select current_database() as db,current_user as role");
  assert.deepEqual(identity, { db: u.pathname.slice(1), role: "bench" });
  const start = await read(pool);
  assert.deepEqual({ delayUs: start.delayUs, siblings: start.siblings }, commitProfiles.default);
  assert.equal(start.fsync, "on");
  assert.equal(start.sync, "on");
  const reload = async (target) => {
    assert.equal((await pool.query("select pg_reload_conf() as ok")).rows[0].ok, true);
    const deadline = Date.now() + 5000;
    for (;;) {
      const s = await read(pool);
      if (
        s.delayUs === target.delayUs &&
        s.siblings === target.siblings &&
        s.sync === target.sync
      ) {
        assert(s.fsync === "on" && s.method === start.method);
        return;
      }
      assert(Date.now() < deadline, "Temporary PG settings did not take effect");
      await delay(20);
    }
  };
  return {
    start,
    async apply(key) {
      const target = expected(key);
      if (asyncStudy) {
        assert(
          key === "default" || key === "async",
          "Async study must retain default group settings",
        );
        await pool.query(`alter system set synchronous_commit = '${target.sync}'`);
      } else {
        assert.equal(target.sync, "on", "Group-commit trials must remain synchronous");
        await pool.query("alter system set commit_delay = " + target.delayUs);
        await pool.query("alter system set commit_siblings = " + target.siblings);
      }
      await reload(target);
    },
    async restore() {
      if (asyncStudy) await pool.query("alter system reset synchronous_commit");
      else {
        await pool.query("alter system reset commit_delay");
        await pool.query("alter system reset commit_siblings");
      }
      await reload(start);
      return read(pool);
    },
  };
}
