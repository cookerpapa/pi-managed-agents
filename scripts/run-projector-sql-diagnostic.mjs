import assert from "node:assert/strict";
import { randomUUID, createHash } from "node:crypto";
import { mkdir, readFile, writeFile, open } from "node:fs/promises";
import { resolve } from "node:path";
import { spawn, execFileSync } from "node:child_process";
import { sql } from "kysely";
import { transform } from "esbuild";
import { createDatabase } from "../packages/database/src/index.ts";
import { sampleProjectorSql } from "./lib/projector-sql-sampler.mjs";
import {
  kubectl,
  kubectlAsync,
  podFor,
  namespace,
  kubeconfig,
  runtimeDirectory,
  serviceAddress,
} from "./lib/production-kubernetes.mjs";
import { serializeRelease } from "./lib/release-lock.mjs";

assert.equal(
  process.env.PI_CLOUD_PROJECTOR_SQL_DIAGNOSTIC,
  "1",
  "Enable the owned serial diagnostic explicitly",
);
serializeRelease(runtimeDirectory);
const directory = resolve(process.argv[2] ?? ".cache/projector-sql-" + Date.now());
const poolSize = process.argv[3] === undefined ? 12 : Number(process.argv[3]);
const candidateReader = process.argv.slice(4).includes("reader-candidate");
const cpuOnly = process.argv.slice(4).includes("cpu-only");
const cpuProfile = cpuOnly || process.argv.slice(4).includes("cpu");
const serverDuration = process.argv.slice(4).includes("server-duration");
assert(
  process.argv
    .slice(4)
    .every((v) => ["reader-candidate", "cpu", "cpu-only", "server-duration"].includes(v)),
  "Unknown diagnostic option",
);
assert([12, 24, 32].includes(poolSize), "Use one of the bounded experimental pool sizes");
assert(
  !cpuOnly || (poolSize === 12 && !serverDuration),
  "CPU-only calibration preserves the serving pool and server logging",
);
await mkdir(directory, { mode: 0o700 });
const name = "pma-projector-sql-" + randomUUID().slice(0, 8),
  target = "deployment/pi-cloud-session-runtime";
const u = new URL((await readFile(runtimeDirectory + "/secrets/database-url", "utf8")).trim());
u.hostname = serviceAddress("postgres");
u.searchParams.set("application_name", "pma-projector-sql-observer");
u.searchParams.set("options", "-c default_transaction_read_only=on -c statement_timeout=3000");
const database = createDatabase({ connectionString: u.toString(), maxConnections: 1 });
const {
  rows: [activity],
} = await sql`select count(*)::int as n from runs
  where state not in ('completed','failed','cancelled','timed_out') or recovery_requested_at is not null`.execute(
  database,
);
assert.equal(activity.n, 0, "Unrelated active work prevents temporary Projector instrumentation");
const original = JSON.parse(kubectl(["get", target, "-o", "json"])).spec.template;
const modified = structuredClone(original),
  container = modified.spec.containers.find((c) => c.name === "session-runtime");
assert(container, "Session runtime container missing");
const sources = [
  "packages/runtime-core/src/session-projector.ts",
  "packages/control-plane/src/session-runtime-main.ts",
];
if (candidateReader) sources.push("packages/event-log/src/kafka-partition-reader.ts");
const deployment = JSON.parse(await readFile(runtimeDirectory + "/k3s/deployment.json", "utf8"));
const baselineRevision = deployment.components["session-runtime"].sourceRevision;
const data = {};
for (const path of sources) {
  const source = await readFile(path, "utf8");
  const expected =
    candidateReader && path.endsWith("kafka-partition-reader.ts")
      ? execFileSync("git", ["show", `${baselineRevision}:${path}`], { encoding: "utf8" })
      : source;
  const actual = kubectl([
    "exec",
    podFor("session-runtime"),
    "--",
    "node",
    "-e",
    `console.log(require('crypto').createHash('sha256').update(require('fs').readFileSync('/app/${path}')).digest('hex'))`,
  ]).trim();
  assert.equal(
    actual,
    createHash("sha256").update(expected).digest("hex"),
    "Serving source differs: " + path,
  );
  data[path.split("/").at(-1)] = source;
}
const replace = (text, from, to) => {
  assert.equal(text.split(from).length, 2, "Diagnostic source anchor changed");
  return text.replace(from, to);
};
data["session-projector.ts"] =
  'import { projectWithProbe } from "/pma-diagnostic/entry.mjs";\n' +
  replace(
    data["session-projector.ts"],
    "this.#projection.project(record, current ?? (() => true)),",
    "projectWithProbe(record, () => this.#projection.project(record, current ?? (() => true))),",
  );
if (poolSize !== 12)
  data["session-runtime-main.ts"] = replace(
    data["session-runtime-main.ts"],
    "const projectionDatabase = createDatabase({\n      connectionString: config.databaseNotificationUrl,\n      maxConnections: 12,",
    `const projectionDatabase = createDatabase({\n      connectionString: config.databaseNotificationUrl,\n      maxConnections: ${poolSize},`,
  );
for (const file of sources.map((p) => p.split("/").at(-1)))
  await transform(data[file], { loader: "ts", format: "esm" });
data["probe.mjs"] = await readFile("scripts/lib/projector-database-probe.mjs", "utf8");
data["entry.mjs"] = `import {createRequire} from 'node:module';
import {installProjectorDatabaseProbe} from './probe.mjs';
const pg=createRequire('/app/package.json')('pg');
const probe=installProjectorDatabaseProbe(pg,e=>process.stdout.write(JSON.stringify(e)+'\\n'));
export const projectWithProbe=(record,work)=>probe.run(record,work);\n`;
if (cpuProfile) {
  data["cpu.mjs"] = await readFile("scripts/lib/projector-cpu-probe.mjs", "utf8");
  data["entry.mjs"] = data["entry.mjs"].replace(
    "export const projectWithProbe=(record,work)=>probe.run(record,work);",
    `import {projectorCpuProbe} from './cpu.mjs';
const cpu=projectorCpuProbe('/tmp/pma-projector-profile.json');
process.on('SIGUSR2',()=>cpu.stop().catch(e=>console.error(JSON.stringify({event:'diag.cpu.failed',code:e.code??e.name}))));
console.log(JSON.stringify({event:'diag.cpu.ready',pid:process.pid}));
export const projectWithProbe=async(record,work)=>{await cpu.start().catch(e=>console.error(JSON.stringify({event:'diag.cpu.failed',code:e.code??e.name})));return probe.run(record,work)};`,
  );
}
if (serverDuration) {
  const {
    rows: [permission],
  } =
    await sql`select has_parameter_privilege(current_user,'log_duration','SET') as allowed`.execute(
      database,
    );
  assert(permission.allowed, "Session-scoped PostgreSQL duration logging is unavailable");
  data["entry.mjs"] += `
export function observeDurationUrl(value) {
 const u=new URL(value);
 u.searchParams.set('options',(u.searchParams.get('options')??'')+' -c log_duration=on -c log_statement=none -c log_min_duration_statement=-1 -c log_min_duration_sample=-1 -c log_parameter_max_length=0 -c log_parameter_max_length_on_error=0');
 return u.toString();
}\n`;
  data["session-runtime-main.ts"] =
    'import { observeDurationUrl } from "/pma-diagnostic/entry.mjs";\n' +
    replace(
      data["session-runtime-main.ts"],
      "const projectionDatabase = createDatabase({\n      connectionString: config.databaseNotificationUrl,",
      "const projectionDatabase = createDatabase({\n      connectionString: observeDurationUrl(config.databaseNotificationUrl),",
    );
  await transform(data["session-runtime-main.ts"], { loader: "ts", format: "esm" });
}
if (cpuOnly) {
  data["session-projector.ts"] =
    'import "/pma-diagnostic/entry.mjs";\n' +
    (await readFile("packages/runtime-core/src/session-projector.ts", "utf8"));
  data["entry.mjs"] = `import {projectorCpuProbe} from './cpu.mjs';
const cpu=projectorCpuProbe('/tmp/pma-projector-profile.json');
let started=false;
process.on('SIGUSR2',()=>{
 const job=started?cpu.stop():cpu.start().then(()=>console.log(JSON.stringify({event:'diag.cpu.started',pid:process.pid})));
 started=true;
 job.catch(e=>console.error(JSON.stringify({event:'diag.cpu.failed',code:e.code??e.name})));
});
console.log(JSON.stringify({event:'diag.cpu.ready',pid:process.pid}));\n`;
  await transform(data["session-projector.ts"], { loader: "ts", format: "esm" });
}
for (const [file, source] of Object.entries(data))
  if (file.endsWith(".mjs")) await transform(source, { loader: "js", format: "esm" });
modified.spec.volumes ??= [];
modified.spec.volumes.push({ name, configMap: { name, defaultMode: 420 } });
container.volumeMounts ??= [];
container.volumeMounts.push({ name, mountPath: "/pma-diagnostic", readOnly: true });
for (const path of sources)
  container.volumeMounts.push({
    name,
    mountPath: "/app/" + path,
    subPath: path.split("/").at(-1),
    readOnly: true,
  });
const receipt = {
  name,
  target,
  original,
  modified,
  poolSize,
  candidateReader,
  cpuProfile,
  cpuOnly,
  serverDuration,
  baselineRevision,
  phase: "prepared",
  startedAt: new Date().toISOString(),
  sourceHashes: Object.fromEntries(
    Object.entries(data).map(([k, v]) => [k, createHash("sha256").update(v).digest("hex")]),
  ),
};
const save = () =>
  writeFile(
    resolve(directory, "observation-private.json"),
    JSON.stringify(receipt, null, 2) + "\n",
    { mode: 0o600 },
  );
await save();
let changed = false,
  watching;
const abort = new AbortController(),
  jobs = [],
  files = [];
async function start(file, command, args, env = process.env) {
  const handle = await open(resolve(directory, file), "w", 0o600);
  files.push(handle);
  const child = spawn(command, args, { env, stdio: ["ignore", handle.fd, handle.fd] });
  const ended = new Promise((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", (code) => resolve(code ?? 1));
  });
  const job = { child, ended };
  jobs.push(job);
  return job;
}
try {
  if (serverDuration)
    await start("postgres.log", "/usr/local/bin/k3s", [
      "kubectl",
      "--kubeconfig",
      kubeconfig,
      "-n",
      namespace,
      "logs",
      podFor("postgres"),
      "--follow",
      "--timestamps",
      "--since-time=" + receipt.startedAt,
    ]);
  kubectl(["apply", "-f", "-"], {
    input: JSON.stringify({
      apiVersion: "v1",
      kind: "ConfigMap",
      metadata: { name, namespace },
      data,
    }),
  });
  receipt.phase = "applying";
  await save();
  changed = true; // Restore even if the patch reply is lost after application.
  kubectl([
    "patch",
    target,
    "--type=json",
    "-p",
    JSON.stringify([
      { op: "test", path: "/spec/template", value: original },
      { op: "replace", path: "/spec/template", value: modified },
    ]),
  ]);
  await kubectlAsync(["rollout", "status", target, "--timeout=180s"]);
  receipt.phase = "observing";
  await save();
  const pod = podFor("session-runtime");
  await start("runtime.log", "/usr/local/bin/k3s", [
    "kubectl",
    "--kubeconfig",
    kubeconfig,
    "-n",
    namespace,
    "logs",
    pod,
    "--follow",
  ]);
  if (cpuOnly) {
    const deadline = Date.now() + 10000;
    let ready;
    while (!ready) {
      ready = (await readFile(resolve(directory, "runtime.log"), "utf8"))
        .split("\n")
        .flatMap((line) => {
          try {
            const r = JSON.parse(line);
            return r.event === "diag.cpu.ready" ? [r] : [];
          } catch {
            return [];
          }
        })[0];
      assert(Date.now() < deadline, "CPU-only listener is not ready");
      if (!ready) await new Promise((resolve) => setTimeout(resolve, 100));
    }
    assert(Number.isSafeInteger(ready.pid) && ready.pid > 0);
    // Activate only after normal application startup. Profiling is not a module
    // initialization dependency or part of a native commit's execution path.
    kubectl(["exec", pod, "--", "node", "-e", `process.kill(${ready.pid},'SIGUSR2')`]);
    while (
      !(await readFile(resolve(directory, "runtime.log"), "utf8")).includes(
        '"event":"diag.cpu.started"',
      )
    ) {
      assert(Date.now() < deadline, "CPU-only sampler did not start");
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }
  const pods = JSON.parse(kubectl(["get", "pods", "-o", "json"])).items;
  const roles = new Map(
    pods.map((p) => [p.status.podIP, p.metadata.name.replace(/-[a-f0-9]{8,10}-[a-z0-9]{5}$/, "")]),
  );
  if (!cpuOnly)
    watching = sampleProjectorSql(
      database,
      roles,
      resolve(directory, "pg-samples.json"),
      abort.signal,
    );
  else
    await writeFile(
      resolve(directory, "pg-samples.json"),
      JSON.stringify({
        disabled: true,
        reason: "CPU-only calibration has no SQL spans or activity sampling",
      }) + "\n",
      { mode: 0o600 },
    );
  const load = await start(
    "load.log",
    process.execPath,
    [
      "--import",
      "tsx",
      "scripts/run-runtime-load.mjs",
      "--mode",
      "custom",
      "--name",
      "observed-projection-pool-" + poolSize,
      "--concurrency",
      "64",
      "--count",
      "192",
      "--kind",
      "text",
      "--first-byte-ms",
      "0",
      "--chunk-ms",
      "0",
      "--output-bytes",
      "4096",
      "--output",
      resolve(directory, "load"),
    ],
    { ...process.env, PI_CLOUD_RUNTIME_LOAD_CHECK: "1" },
  );
  receipt.loadExit = await load.ended;
  abort.abort();
  await watching;
  if (cpuProfile) {
    const lines = (await readFile(resolve(directory, "runtime.log"), "utf8")).split("\n");
    const ready = lines.flatMap((line) => {
      try {
        const row = JSON.parse(line);
        return row.event === "diag.cpu.ready" ? [row] : [];
      } catch {
        return [];
      }
    })[0];
    assert(
      Number.isSafeInteger(ready?.pid) && ready.pid > 0,
      "CPU probe signal handler not confirmed",
    );
    kubectl(["exec", pod, "--", "node", "-e", `process.kill(${ready.pid},'SIGUSR2')`]);
    const end = Date.now() + 10000;
    while (
      kubectl([
        "exec",
        pod,
        "--",
        "node",
        "-e",
        "console.log(require('fs').existsSync('/tmp/pma-projector-profile.json'))",
      ]).trim() !== "true"
    ) {
      assert(Date.now() < end, "CPU profile did not finish");
      await new Promise((resolve) => setTimeout(resolve, 200));
    }
    await writeFile(
      resolve(directory, "cpu.json"),
      kubectl(["exec", pod, "--", "cat", "/tmp/pma-projector-profile.json"], {
        maxBuffer: 32 * 1024 ** 2,
      }),
      { mode: 0o600 },
    );
  }
  receipt.phase = "measured";
  await save();
  assert.equal(receipt.loadExit, 0, "Observed workload failed; preserve private evidence");
} catch (error) {
  receipt.failure = String(error.code ?? error.message).slice(0, 600);
  const pods = JSON.parse(kubectl(["get", "pods", "-o", "json"])).items.filter((p) =>
    p.spec.volumes?.some((v) => v.configMap?.name === name),
  );
  for (const [index, pod] of pods.entries()) {
    try {
      await writeFile(
        resolve(directory, `failed-pod-${index}.log`),
        kubectl(["logs", pod.metadata.name, "--tail=100"], { maxBuffer: 512 * 1024 }),
        { mode: 0o600 },
      );
    } catch {
      /* The private receipt still identifies this observation. */
    }
  }
  await save();
  throw error;
} finally {
  abort.abort();
  await watching?.catch((error) => {
    receipt.samplingError = String(error.code ?? error.name ?? "error");
  });
  for (const job of jobs) if (job.child.exitCode === null) job.child.kill("SIGTERM");
  await Promise.allSettled(jobs.map((j) => j.ended));
  await Promise.allSettled(files.map((file) => file.close()));
  if (changed) {
    const live = JSON.parse(kubectl(["get", target, "-o", "json"])).spec.template;
    const stripped = structuredClone(live);
    stripped.spec.volumes = stripped.spec.volumes.filter((v) => v.name !== name);
    stripped.spec.containers.find((c) => c.name === "session-runtime").volumeMounts =
      stripped.spec.containers
        .find((c) => c.name === "session-runtime")
        .volumeMounts.filter((m) => m.name !== name);
    assert.deepEqual(
      stripped,
      original,
      "Unrelated template change prevents automatic restoration",
    );
    kubectl([
      "patch",
      target,
      "--type=json",
      "-p",
      JSON.stringify([
        { op: "test", path: "/spec/template", value: live },
        { op: "replace", path: "/spec/template", value: original },
      ]),
    ]);
    await kubectlAsync(["rollout", "status", target, "--timeout=180s"]);
  }
  kubectl(["delete", "configmap", name, "--ignore-not-found"]);
  receipt.phase = "restored";
  receipt.restoredAt = new Date().toISOString();
  await save();
  await database.destroy();
  console.log(JSON.stringify({ restored: true, loadExit: receipt.loadExit, poolSize, directory }));
}
