// Observe the existing end-to-end workload without changing its inputs or SQL.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import pg from "pg";
import {
  runtimeDirectory,
  repositoryRoot,
  serviceAddress,
  kubectl,
} from "../../lib/production-kubernetes.mjs";
const directory = resolve(process.argv[2]);
assert.equal(process.env.PI_CLOUD_PROJECTOR_SQL_DIAGNOSTIC, "1");
const u = new URL((await readFile(runtimeDirectory + "/secrets/database-url", "utf8")).trim());
u.hostname = serviceAddress("postgres");
const db = new pg.Client({
  connectionString: u.toString(),
  application_name: "pma-notify-observer",
  options: "-c default_transaction_read_only=on -c statement_timeout=3000",
});
await db.connect();
const samples = [],
  role = new Map();
const collectPods = () => {
  for (const p of JSON.parse(kubectl(["get", "pods", "-o", "json"])).items) {
    if (!p.status.podIP) continue;
    const n = p.metadata.name;
    role.set(
      p.status.podIP,
      n.startsWith("pma-projector-sql-") && n.endsWith("-api")
        ? "session-api"
        : n.startsWith("pi-cloud-session-api-")
          ? "session-api"
          : n.startsWith("pi-cloud-session-runtime-")
            ? "session-runtime"
            : n.startsWith("pi-cloud-pi-worker-")
              ? "worker"
              : n.startsWith("pi-cloud-control-plane-")
                ? "management"
                : "other",
    );
  }
};
const statement = `with waiters as materialized (select distinct pid from pg_locks where not granted and locktype='object' and classid='pg_database'::regclass and objid=0 and mode='AccessExclusiveLock')
 select host(client_addr) as ip, count(*)::int as active, count(w.pid)::int as waiting
 from pg_stat_activity a left join waiters w on a.pid=w.pid
 where datname=current_database() and a.pid<>pg_backend_pid() and backend_type='client backend' and state<>'idle'
 group by client_addr`;
const child = spawn(
  process.execPath,
  [
    "--import",
    "tsx",
    "scripts/run-projector-sql-diagnostic.mjs",
    directory,
    "12",
    "projection-candidate",
    "startup-candidate",
    "clean",
  ],
  { cwd: repositoryRoot, stdio: "inherit" },
);
const ended = new Promise((resolve, reject) => {
  child.once("close", resolve);
  child.once("error", reject);
});
let updated = 0;
try {
  while (child.exitCode === null && child.signalCode === null) {
    if (Date.now() - updated > 12000) {
      collectPods();
      updated = Date.now();
    }
    const r = await db.query(statement);
    samples.push({ at: Date.now(), rows: r.rows });
    await Promise.race([ended, delay(125)]);
  }
  assert.equal(await ended, 0);
  const load = JSON.parse(await readFile(directory + "/load/report.json", "utf8"));
  assert(load.accepted && load.cleanupConfirmed && !load.realProviderRequests);
  const runs = load.measurements[0].load.results,
    first = Math.min(...runs.map((r) => r.submittedAt)),
    last = Math.max(...runs.map((r) => r.observedAt));
  const phase = samples.filter((s) => s.at >= first && s.at <= last);
  assert(phase.length);
  const observations = {};
  for (const s of phase) {
    const counts = {};
    for (const r of s.rows) {
      const k = role.get(r.ip) ?? "other";
      const t = (counts[k] ??= { active: 0, waiting: 0 });
      t.active += r.active;
      t.waiting += r.waiting;
    }
    for (const [k, v] of Object.entries(counts)) {
      const r = (observations[k] ??= {
        nonIdleBackendSamples: 0,
        notifyLockBackendSamples: 0,
        samplesWithWaiters: 0,
        maxSimultaneousWaiters: 0,
      });
      r.nonIdleBackendSamples += v.active;
      r.notifyLockBackendSamples += v.waiting;
      r.samplesWithWaiters += Number(v.waiting > 0);
      r.maxSimultaneousWaiters = Math.max(r.maxSimultaneousWaiters, v.waiting);
    }
  }
  await writeFile(
    directory + "/notify-observations.json",
    JSON.stringify({ samples: phase.length, observations }, null, 2) + "\n",
    { mode: 0o600 },
  );
} finally {
  await db.end();
  await ended;
}
