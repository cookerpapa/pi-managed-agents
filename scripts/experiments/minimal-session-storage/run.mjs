/** Isolated test of the two-table lifecycle. Run through the maintained development cluster. */
import assert from "node:assert/strict";
import { fanoutTargets } from "./fanout-fixture.mjs";
import { isCommitStudy } from "./pg-group-commit.mjs";
import { isPrefixStudy } from "./ready-projection.mjs";
import { environmentSection } from "./continuation-environment.mjs";
import { selectEnvironment } from "./continuation-cases.mjs";
import { answer, claimSql, claimBatchSql, claimHintedBatchSql } from "./fixture.mjs";
import { runWakeTrials, waitForWakeState, wakeNetwork } from "./wake-trials.mjs";
import { measureClaimPreference, runSizingTrials } from "./wake-sizing.mjs";
import {
  runLocality,
  seedCohort,
  resetCohortHead,
  verifyModelHistory,
  forkSeed,
  sessionForArrival,
  checkPreferredClaim,
} from "./locality-load.mjs";
import { randomUUID, randomBytes, createHash } from "node:crypto";
import { spawn, execFileSync } from "node:child_process";
import { createInterface } from "node:readline";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import { observeGuestHttp } from "./guest-http-timing.mjs";
import { setTimeout as delay } from "node:timers/promises";
import pg from "pg";
import { build } from "esbuild";
import { runLoad, distribution } from "../../lib/runtime-load-scheduler.mjs";
import { summarizeTerminal, precedingRunDelay } from "./tail-probe.mjs";
import {
  runtimeDirectory,
  serviceAddress,
  kubectl,
  namespace,
  kubeconfig,
  podFor,
  workload,
} from "../../lib/production-kubernetes.mjs";
import { serializeRelease } from "../../lib/release-lock.mjs";

assert.equal(process.env.PI_CLOUD_RUNTIME_LOAD_CHECK, "1");
const [output, mode = "matrix", suite = "smoke"] = process.argv.slice(2);
const readerMode = process.env.PI_CLOUD_SESSION_BENCH_READER ?? "batch";
assert(["single", "batch"].includes(readerMode));
const projectorConnections = Number(process.env.PI_CLOUD_SESSION_BENCH_PROJECTOR_CONNECTIONS ?? 12);
assert([12, 24].includes(projectorConnections));
assert(
  output &&
    [
      "smoke",
      "matrix",
      "inbox",
      "capacity",
      "diagnose",
      "intake",
      "boundary",
      "wake",
      "wake-fleet",
      "wake-sizing",
      "wake-throughput",
    ].includes(mode),
);
serializeRelease(runtimeDirectory);
process.umask(0o077);
const out = resolve(output);
await mkdir(out, { mode: 0o700 });
const token = randomUUID().replaceAll("-", "").slice(0, 16),
  name = "pma_sessbench_" + token,
  topic = "pma-session-bench-" + token,
  tenant = randomUUID();
const cpuProfile =
  mode === "wake-throughput" &&
  suite === "pg-paths" &&
  (["cpu", "cpu-smoke"].includes(process.env.PI_CLOUD_PG_PATH_PROFILE) ||
    isCommitStudy(process.env.PI_CLOUD_PG_PATH_PROFILE) ||
    isPrefixStudy(process.env.PI_CLOUD_PG_PATH_PROFILE));
const cpuFixture = cpuProfile
  ? (await import("./cpu-placement-fixture.mjs")).attachFixture(process.env.PI_CLOUD_CPU_FIXTURE)
  : undefined;
const u =
  cpuFixture?.url ??
  new URL((await readFile(runtimeDirectory + "/secrets/database-url", "utf8")).trim());
if (!cpuFixture) u.hostname = serviceAddress("postgres");
const admin = new pg.Pool({
  connectionString: u.toString(),
  max: 1,
  application_name: "pma-session-bench-operator",
});
const scratch = new URL(u);
scratch.pathname = "/" + name;
const db = new pg.Pool({
  connectionString: scratch.toString(),
  max: 12,
  application_name: "bench-intake",
  connectionTimeoutMillis: 15000,
});
const statsDb = new pg.Pool({
  connectionString: scratch.toString(),
  max: 1,
  application_name: "bench-observer",
});
const fanoutPartitions =
  process.env.PI_CLOUD_PG_PATH_PROFILE === "workspace-fanout"
    ? Number(process.env.PI_CLOUD_FANOUT_PARTITIONS ?? 32)
    : 32;
assert([32, 64].includes(fanoutPartitions));
const source = Object.fromEntries(
  await Promise.all(
    ["worker", "projector", ...(mode.startsWith("wake") ? ["wake-runtime"] : [])].map(
      async (role) => {
        const path = fileURLToPath(new URL(role + ".mjs", import.meta.url));
        const bundle = await build({
          entryPoints: [path],
          bundle: true,
          packages: "external",
          define: {
            __FANOUT_PARTITIONS__: String(fanoutPartitions),
            __FANOUT_TARGETS__: String(fanoutTargets(process.env.PI_CLOUD_FANOUT_SUITE)),
            __FANOUT_PREFIX_LIMIT__:
              process.env.PI_CLOUD_FANOUT_SUITE === "grants-display-wide" ? "128" : "32",
            __GRANT_STUDY__: String(
              process.env.PI_CLOUD_PG_PATH_PROFILE === "workspace-fanout" &&
                [
                  "grants",
                  "grants-long",
                  "grants-pg",
                  "grants-pg-backlog",
                  "grants-pg-minute",
                  "grants-display-smoke",
                  "grants-display",
                  "grants-display-backlog",
                  "grants-display-mix",
                  "grants-display-ackmix",
                  "grants-display-wide",
                  "grants-tool-loop",
                  "grants-million-day",
                ].includes(process.env.PI_CLOUD_FANOUT_SUITE),
            ),
            __PG_PATH_STUDY__: String(mode === "wake-throughput" && suite === "pg-paths"),
            __READY_STUDY__: String(
              !!cpuFixture && isPrefixStudy(process.env.PI_CLOUD_PG_PATH_PROFILE),
            ),
          },
          minify: false,
          platform: "node",
          target: "node24",
          format: "esm",
          write: false,
        });
        return [role, bundle.outputFiles[0].text];
      },
    ),
  ),
);
const sha = (x) => createHash("sha256").update(x).digest("hex");
const failure = Promise.withResolvers();
void failure.promise.catch(() => {});
const abort = new AbortController();
let fatalError,
  created = false,
  actorCleanup = false;
function fatal(error) {
  if (!fatalError) {
    fatalError = error instanceof Error ? error : new Error(String(error));
    abort.abort(fatalError);
    failure.reject(fatalError);
  }
}
async function limit(job, ms, title) {
  let timer;
  try {
    return await Promise.race([
      job,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(title + " timed out")), ms);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}
const guard = (job, ms = 120000) =>
  limit(Promise.race([job, failure.promise]), ms, "Benchmark operation");
process.on("SIGINT", () => fatal(new Error("Interrupted")));
process.on("SIGTERM", () => fatal(new Error("Terminated")));
const actors = [],
  workers = [],
  entries = new Map();
const guestEndpoints = new Map();
const guestHttp =
  process.env.PI_CLOUD_PG_PATH_PROFILE === "workspace-guest-mix" ? observeGuestHttp() : undefined;
const guestBoots = { w0: randomUUID(), w1: randomUUID() };
let projector, fixture, router, network, wakeUrl, wakeStage, transition;
function receive(a, m) {
  if (m.kind === "stopped") {
    a.ackStopped = true;
    return;
  }
  if (m.kind === "publishBursts") {
    void publishWakeBursts(m)
      .then((measurement) => a.write({ kind: "published", measurement }))
      .catch(fatal);
    return;
  }
  if (m.kind === "guestCommand") {
    assert(a === projector && wakeStage?.guestTools);
    const { cmd, ref, position, attachment } = m;
    const endpoint = guestEndpoints.get(attachment.target);
    const entry = entries.get(cmd.scope.runId);
    assert(
      endpoint && position.topic === topic && entry && !entry.guestOperationId,
      "Unexpected or duplicate Guest dispatch",
    );
    entry.guestOperationId = cmd.request.operationId;
    if (wakeStage.guestQuarters !== undefined)
      entry.submitStarted = performance.timeOrigin + performance.now();
    const submit = () => endpoint.submitAt(attachment, position, cmd, ref);
    if (guestHttp) entry.guestHttp = {};
    void (guestHttp ? guestHttp.submit(entry.guestHttp, submit) : submit()).then(
      () => {
        if (wakeStage?.guestQuarters !== undefined)
          entry.submitEnded = performance.timeOrigin + performance.now();
        a.write({ kind: "guestAccepted", op: cmd.request.operationId, ok: true });
      },
      (error) => {
        a.write({ kind: "guestAccepted", op: cmd.request.operationId, ok: false });
        if (wakeStage?.guestQuarters !== undefined)
          entry.failedSubmit = {
            at: Date.now(),
            kind: error.code ?? error.name,
            reason: typeof error.cause === "string" ? error.cause : undefined,
          };
        fatal(error);
      },
    );
    return;
  }
  if (m.kind === "fixture") {
    fixture ??= { source: m.source, batches: m.batches };
    return;
  }
  if (m.kind === "failure") {
    fatal(new Error(a.id + ": " + m.message));
    return;
  }
  if (m.kind === "cleaned") {
    actorCleanup = m.topicAndGroupRemoved;
    return;
  }
  const k = m.kind + (m.stage ?? m.n ?? "");
  const wait = a.waiters.get(k);
  if (wait) {
    a.waiters.delete(k);
    wait.resolve(m);
    return;
  }
  if (!m.id) return;
  const e = entries.get(m.id);
  if (!e) {
    fatal(new Error("Unknown trial run"));
    return;
  }
  if (m.kind === "toolTiming" || m.kind === "resultProjected") {
    assert(a === projector && wakeStage?.guestQuarters !== undefined);
    if (m.kind === "toolTiming") {
      assert(!e.toolTiming);
      e.toolTiming = m;
    } else {
      assert(!e.resultProjectedAt);
      e.resultProjectedAt = m.at;
    }
    return;
  }
  if (m.kind === "claim") {
    assert(!e.worker, "Run claimed by two Workers");
    e.worker = a;
    e.claimedAt = m.at;
    if (wakeStage?.scaleWhenClaimed && !transition) {
      const n = ++sampleId,
        stopped = workers[0].wait("draining" + n);
      workers[0].write({ kind: "drain", n });
      transition = guard(stopped).then(() => workers[1].write({ kind: "join" }));
      void transition.catch(fatal);
    }
    return;
  }
  if (m.kind === "workerDone") {
    e.work = m;
    e.workerDone.resolve();
    return;
  }
  if (m.kind === "settled") {
    e.terminal = m;
    e.projected.resolve();
    return;
  }
  if (m.kind === "confirmed") {
    assert(e.worker);
    e.worker.write({ kind: "confirmed", id: m.id, seq: m.seq, at: m.at });
    if (m.queues.some((q) => q.conversation === 1 && q.placed && q.depth === 0)) e.placed = true;
  }
  if (m.kind === "answerCommitted") e.answerCommitted = true;
  if (m.kind === "finishHeld") e.finishHeld = true;
  if (e.finishHeld && e.answerCommitted && !e.holdCheck) {
    e.holdCheck = true;
    void (async () => {
      const {
        rows: [r],
      } = await statsDb.query(
        "select r.state,jsonb_array_length(i.items) depth from runs r join inboxes i on i.tenant_id=r.tenant_id and i.pi_session_id=r.session_id where r.id=$1",
        [e.id],
      );
      assert.equal(r.state, "running");
      assert.equal(r.depth, 0);
      if (wakeStage?.environmentCases) {
        await assert.rejects(
          selectEnvironment(db, tenant, e.session, {
            workspace: "blocked",
            cwd: "/unavailable",
            processGeneration: 1,
          }),
          /Session must be quiescent/,
        );
      }
      e.checked = true;
      e.worker.write({ kind: "checked", id: e.id });
    })().catch(fatal);
  }
  if (m.kind === "secondModel") e.secondModel = true;
  if (e.placed && e.secondModel && !e.checking) {
    e.checking = true;
    void (async () => {
      const {
        rows: [state],
      } = await statsDb.query(
        "select r.state,jsonb_array_length(i.items) depth from runs r join inboxes i on i.pi_session_id=r.session_id and i.tenant_id=r.tenant_id where r.id=$1",
        [m.id],
      );
      assert.equal(state.state, "running");
      assert.equal(state.depth, 0);
      e.checked = true;
      e.worker.write({ kind: "checked", id: m.id });
    })().catch(fatal);
  }
}
async function startActor(role, id, pod, ...roleArgs) {
  const binary = process.env.PI_CLOUD_KUBECTL_BIN ?? "/usr/local/bin/k3s";
  const program = Buffer.from(source[role]);
  // Read exactly the source bytes before the actor starts its normal line
  // protocol. A virtual module under the working directory resolves installed
  // packages normally without putting the bundle in an exec argument.
  const bootstrap = `import {readSync} from 'node:fs';import {registerHooks} from 'node:module';
    import {pathToFileURL} from 'node:url';import {resolve} from 'node:path';
    const bytes=Buffer.alloc(Number(process.argv.pop()));
    for(let n=0;n<bytes.length;){const read=readSync(0,bytes,n,bytes.length-n,null);if(!read)throw Error('Incomplete actor source');n+=read;}
    const url=pathToFileURL(resolve('pma-experiment-actor.mjs')).href;
    const hook=registerHooks({
      resolve:(id,c,next)=>id===url?{url,format:'module',shortCircuit:true}:next(id,c),
      load:(id,c,next)=>id===url?{source:bytes,format:'module',shortCircuit:true}:next(id,c)
    });try{await import(url);}finally{hook.deregister();}`;
  const args = [
    ...(process.env.PI_CLOUD_KUBECTL_BIN ? [] : ["kubectl"]),
    "--kubeconfig",
    kubeconfig,
    "-n",
    namespace,
    "exec",
    "-i",
    pod,
    "--",
    "node",
    "--import",
    "tsx",
    "--input-type=module",
    "-e",
    bootstrap,
    name,
    topic,
    id,
    mode,
    ...roleArgs,
    String(program.length),
  ];
  const child = spawn(binary, args, { stdio: ["pipe", "pipe", "pipe"] });
  child.stdin.write(program);
  const a = {
    id,
    pod,
    child,
    waiters: new Map(),
    closing: false,
    stderr: "",
    write: (m) => child.stdin.write(JSON.stringify(m) + "\n"),
  };
  a.wait = (key) => {
    assert(!a.waiters.has(key));
    const p = Promise.withResolvers();
    a.waiters.set(key, p);
    return p.promise;
  };
  const ready = a.wait("ready");
  createInterface({ input: child.stdout }).on("line", (line) => {
    if (line.startsWith("SESSION_BENCH "))
      try {
        receive(a, JSON.parse(line.slice(14)));
      } catch (e) {
        fatal(e);
      }
  });
  child.stderr.on("data", (b) => {
    a.stderr = (a.stderr + b.toString()).slice(-1800);
  });
  child.stdin.on("error", (e) => {
    if (!a.closing) fatal(e);
  });
  a.exit = new Promise((resolveExit) => {
    child.once("error", (e) => {
      fatal(e);
      resolveExit(1);
    });
    child.once("close", (code) => {
      if (!a.closing) fatal(new Error(id + " exited " + code + " " + a.stderr));
      resolveExit(code);
    });
  });
  actors.push(a);
  a.ready = await guard(ready, 90000);
  return a;
}
async function configure(sc) {
  const options = { ...sc, kind: "stage", stage: sc.label, ...(sc.replay ? { fixture } : {}) };
  delete options.environmentCases;
  if (sc.replay) assert(fixture);
  wakeStage = ["directed", "broadcast"].includes(sc.wakeMode) ? sc : undefined;
  transition = undefined;
  if (wakeStage) await configureRouter(options);
  await Promise.all(
    actors
      .filter((a) => a !== router)
      .map(async (a) => {
        const response = a.wait("configured" + sc.label);
        a.write({
          ...options,
          ...(sc.guestTools
            ? a === projector
              ? { guestBoots: Object.values(guestBoots) }
              : { replyBootId: guestBoots[a.id] }
            : {}),
          ...(wakeStage && workers.includes(a) ? { wakeUrl } : {}),
        });
        await guard(response, 15000);
      }),
  );
  if (wakeStage) {
    const online = workers.length - (sc.offline?.length ?? 0);
    await waitForWakeState(
      routerState,
      (s) =>
        s.online === online && (sc.fault === "lost-ready" ? s.faults === 1 : s.waiting === online),
    );
    if (sc.fault) {
      const n = ++sampleId,
        response = router.wait("armed" + n);
      router.write({ kind: "armTimer", n });
      await guard(response);
    }
    if (sc.restartRouter) {
      await stopActors([router]);
      actors.splice(actors.indexOf(router), 1);
    }
  }
}
async function configureRouter(options) {
  const response = router.wait("configured" + options.label);
  router.write({ ...options, kind: "stage", stage: options.label });
  await guard(response);
}
async function routerState() {
  const n = ++sampleId,
    got = router.wait("status" + n);
  router.write({ kind: "status", n });
  return guard(got);
}
// A separate process and PG pool keep the generator independent of 1,000
// management sockets. Each group is committed before its notification.
async function publishWakeBursts({ label, tenant, batches }) {
  const result = await runLoad({
    mode: "arrival",
    rate: 171,
    count: batches,
    concurrency: 128,
    signal: abort.signal,
    work: async () => {
      const ids = Array.from({ length: 7 }, () => randomUUID());
      await db.query(
        `insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state)
       select id,$2,id,1,id,'queue-only fixture','none','queued' from unnest($1::uuid[]) as id`,
        [ids, tenant],
      );
      await db.query("select pg_notify('session_bench_directed',$1)", [
        JSON.stringify({ label, count: ids.length }),
      ]);
      return { success: true };
    },
  });
  assert.equal(result.results.filter((x) => x.success).length, batches);
  assert.equal(result.dropped.length, 0);
  return {
    elapsedMs: Math.round(result.elapsedMs),
    launchDelayMs: d(result.results.map((r) => r.generatorDelayMs)),
  };
}
const intakeSql = `with admitted as (insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state)
 select $1,$2,$3,1+coalesce((select mailbox_position from runs where tenant_id=$2 and session_id=$3 order by mailbox_position desc limit 1),0),$4,$5,'local-load','queued'
 on conflict(tenant_id,session_id,idempotency_key) do update set idempotency_key=excluded.idempotency_key
 where runs.initial_input=excluded.initial_input returning id) select id,pg_notify('session_bench_work','') from admitted`;
const quietIntake = intakeSql.replace(",pg_notify('session_bench_work','')", "");
const localityIntakeSql = (
  environment,
) => `with prior as materialized (select mailbox_position,history_head from runs where
 tenant_id=$2 and session_id=$3 order by mailbox_position desc limit 1), admitted as
 (insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state${environment ? ",environment_snapshot" : ""})
 select $1,$2,$3,coalesce((select mailbox_position from prior),0)+1,$4,$5,$6,'queued'${environment ? ",$7::jsonb" : ""}
 on conflict(tenant_id,session_id,idempotency_key) do update set idempotency_key=excluded.idempotency_key
 where runs.initial_input=excluded.initial_input and runs.model_ref=excluded.model_ref returning id,mailbox_position)
 select a.id,p.history_head from admitted a left join prior p on a.mailbox_position=p.mailbox_position+1`;
const localityIntake = localityIntakeSql(false),
  environmentIntake = localityIntakeSql(true);
let notifyJob,
  notifyGeneration = 0,
  wakeCount = 0,
  pendingTargets = [];
function notifyBatch(directedStage, hint) {
  notifyGeneration++;
  if (hint && pendingTargets.length < 32) pendingTargets.push(hint);
  if (notifyJob) return;
  let previous = notifyGeneration - 1;
  notifyJob = (async () => {
    try {
      let sent;
      do {
        await delay(directedStage?.windowMs ?? 5);
        sent = notifyGeneration;
        const hints = pendingTargets;
        pendingTargets = [];
        if (directedStage)
          await db.query("select pg_notify('session_bench_directed',$1)", [
            JSON.stringify({
              label: directedStage.label,
              count: sent - previous,
              ...(hints.length ? { hints } : {}),
            }),
          ]);
        else await db.query("select pg_notify('session_bench_work','')");
        previous = sent;
        wakeCount++;
      } while (sent !== notifyGeneration);
    } finally {
      notifyJob = undefined;
    }
  })();
  void notifyJob.catch(fatal);
}
async function intake(e, sc = {}) {
  for (let i = 0; ; i++)
    try {
      const withSelection = sc.environmentCases || sc.guestQuarters !== undefined;
      const started = performance.now(),
        client = await db.connect();
      const acquired = performance.now();
      let rows;
      try {
        let environment;
        let statement = sc.integrated ? localityIntake : sc.omitNotify ? quietIntake : intakeSql;
        if (withSelection) {
          await client.query("begin");
          const selected = await client.query(
            "select value from study_session_environments where tenant=$1 and session=$2 for share",
            [tenant, e.session],
          );
          assert.equal(selected.rowCount, 1);
          environment = selected.rows[0].value;
          statement = environmentIntake;
        }
        ({ rows } = await client.query(statement, [
          e.id,
          tenant,
          e.session,
          e.key,
          e.text,
          ...(sc.integrated ? [e.modelRef ?? "local-load"] : []),
          ...(environment ? [environment] : []),
        ]));
        if (withSelection) await client.query("commit");
      } catch (error) {
        if (withSelection) await client.query("rollback");
        throw error;
      } finally {
        client.release();
      }
      e.admissionPoolMs = acquired - started;
      e.admissionSqlMs = performance.now() - acquired;
      assert.equal(rows.length, 1, "Changed idempotent input");
      if (!sc.omitNotify) wakeCount++;
      if (sc.wakeMode === "relay") for (const worker of workers) worker.write({ kind: "wake" });
      if (sc.wakeMode === "coalesced") notifyBatch();
      if (sc === wakeStage) {
        const h = rows[0].history_head;
        const hint =
          sc.cacheMode === "prefer" && h?.bytes >= (sc.minCacheBytes ?? 256 * 1024)
            ? [tenant, e.session, rows[0].id, h.version]
            : undefined;
        notifyBatch(sc, hint);
      }
      return rows[0].id;
    } catch (error) {
      if (
        error.code !== "23505" ||
        error.constraint !== "runs_tenant_id_session_id_mailbox_position_key" ||
        i >= 3
      )
        throw error;
    }
}
let sampleId = 0,
  calibratedAt = 0;
const calibrationBlock = Buffer.alloc(1024 * 1024, 103);
async function sample(lockSamples) {
  const n = ++sampleId,
    got = projector.wait("sample" + n);
  projector.write({ kind: "sample", n });
  const [k, p] = await guard(
    Promise.all([
      got,
      statsDb.query(
        `select count(*) filter(where state='queued')::int queued,count(*) filter(where state='running')::int running from runs where state<>'completed'`,
      ),
    ]),
    12000,
  );
  const waits = lockSamples
    ? (
        await statsDb.query(`select a.state,a.wait_event_type,a.wait_event,l.locktype,l.mode,l.classid::regclass::text as class,l.objid,count(*)::int n
     from pg_stat_activity a left join pg_locks l on l.pid=a.pid and not l.granted
     where a.datname=current_database() and a.application_name='bench-intake' and a.state<>'idle'
     group by a.state,a.wait_event_type,a.wait_event,l.locktype,l.mode,l.classid,l.objid`)
      ).rows
    : undefined;
  let calibrationMs;
  if ((mode === "boundary" || mode === "wake-throughput") && Date.now() - calibratedAt >= 900) {
    calibratedAt = Date.now();
    const t = performance.now();
    for (let i = 0; i < 4; i++) createHash("sha256").update(calibrationBlock).digest();
    calibrationMs = performance.now() - t;
  }
  return {
    at: Date.now(),
    calibrationMs,
    waits,
    lag: k.lag,
    partitionLag: k.partitionLagMax,
    localQueue: k.queued,
    poolWaiting: k.poolWaiting,
    ...p.rows[0],
  };
}
function d(xs) {
  const dist = distribution(xs);
  dist.min = xs.length ? Math.min(...xs) : null;
  return Object.fromEntries(
    Object.entries(dist).map(([k, v]) => [k, v === null ? null : Math.round(v * 100) / 100]),
  );
}
function milliseconds(row) {
  return row.settled_at - row.created_at;
}
async function verify(sc, records, cohort) {
  const answerCount = sc.scene === "plain" && !sc.guestTools ? 1 : 2;
  const ids = records.map((r) => r.id);
  const rr = (await db.query("select * from runs where id=any($1::uuid[])", [ids])).rows;
  const msgs = (
    await db.query(
      "select run_id,role,entry_id,source_input_id,content,commit_seq,ordinal from messages where run_id=any($1::uuid[]) order by run_id,commit_seq,ordinal",
      [ids],
    )
  ).rows;
  const queues = (
    await db.query(
      "select count(*)::int n,count(*) filter(where jsonb_array_length(items)<>0)::int pending from inboxes where pi_session_id=any($1::uuid[])",
      [records.map((r) => r.session)],
    )
  ).rows[0];
  assert.equal(rr.length, records.length);
  assert(rr.every((r) => r.state === "completed" && r.lease_id && r.settled_at >= r.started_at));
  const byRun = Map.groupBy(msgs, (m) => m.run_id);
  if (sc.sink === "terminal-only") {
    assert.equal(msgs.length, 0);
    assert.equal(queues.n, 0);
  } else {
    assert.equal(queues.n, new Set(records.map((r) => r.session)).size);
    assert.equal(queues.pending, 0);
    for (const r of records) {
      const history = (byRun.get(r.id) ?? []).filter((m) =>
          ["user", "assistant", "toolResult"].includes(m.role),
        ),
        users = history.filter((m) => m.role === "user"),
        answers = history.filter((m) => m.role === "assistant");
      const extras = r.work.inputs.filter((i) => !i.withdrawn);
      assert.deepEqual(
        users.map((m) => m.source_input_id),
        [r.id, ...extras.map((i) => i.id)],
      );
      assert.deepEqual(
        users.flatMap((m) => JSON.parse(m.content).model[0].content.map((c) => c.text)),
        [r.text, ...extras.map((i) => i.text)],
      );
      assert.equal(answers.length, answerCount);
      if (sc.guestTools) {
        assert.equal(
          !!r.guestOperationId,
          r.realGuest ?? true,
          "Unexpected number of Guest dispatches",
        );
        assert.deepEqual(
          history.map((m) => m.role),
          ["user", "assistant", "toolResult", "assistant"],
        );
        const call = JSON.parse(history[1].content).model[0].content.find(
          (b) => b.type === "toolCall",
        );
        const result = JSON.parse(history[2].content).model[0];
        assert(call.name === "bash" && result.toolCallId === call.id && !result.isError);
        assert.equal(
          result.content
            .map((b) => b.text)
            .join("")
            .trimEnd(),
          r.work.observedBash,
        );
      }
      if (sc.scene === "steer") {
        assert.deepEqual(
          history.map((m) => m.role),
          ["user", "assistant", "toolResult", "user", "assistant"],
        );
      } else if (extras.length) {
        assert(BigInt(answers[0].entry_id) < BigInt(users[1].entry_id));
      }
      const body = JSON.parse(history.at(-1).content).model[0];
      if (sc.integrated) assert.equal(body.model, r.modelRef ?? "local-load");
      assert.equal(body.stopReason, "stop");
      assert.equal(body.content[0].text, answer(r.id, answerCount));
      if (sc.scene === "follow" || sc.scene === "late")
        assert.equal(JSON.parse(answers[0].content).model[0].content[0].text, answer(r.id, 1));
      if (sc.scene === "steer") {
        const call = JSON.parse(answers[0].content).model[0].content.find(
          (c) => c.type === "toolCall",
        );
        assert.equal(JSON.parse(history[2].content).model[0].toolCallId, call.id);
      }
      assert.equal(body.content[0].text.length, 4096);
      assert(body.content[0].text.startsWith(r.id + " answer " + answerCount + " "));
      if (sc.inspect || sc.holdFinish) assert(r.checked);
    }
  }
  if (sc.environmentCases) {
    const byId = new Map(rr.map((r) => [r.id, r]));
    let last;
    for (const run of records) {
      const desired = sc.environmentCases[run.index];
      assert.deepEqual(byId.get(run.id).environment_snapshot, desired);
      const updates = (byRun.get(run.id) ?? [])
        .flatMap((m) => JSON.parse(m.content).model ?? [])
        .filter(
          (m) => m.role === "system" && m.sections && Object.hasOwn(m.sections, environmentSection),
        )
        .map((m) => m.sections[environmentSection]);
      assert.deepEqual(
        updates,
        last === run.work.observedEnvironment ? [] : [run.work.observedEnvironment],
        "PG must retain the exact environment update seen by the model",
      );
      last = run.work.observedEnvironment;
    }
  }
  if (sc.integrated) verifyModelHistory(sc, rr, records, cohort, byRun);
  return {
    messages: msgs.length,
    inboxes: queues.n,
    nonemptyInboxes: queues.pending,
    sequenceVerified: true,
    queueMs: d(rr.map((r) => r.started_at - r.created_at)),
    pgLifecycleMs: d(rr.map(milliseconds)),
    ...(sc.tail && sc.cohort ? { predecessor: precedingRunDelay(rr, records) } : {}),
  };
}
const report = {
  format: 1,
  at: new Date().toISOString(),
  mode,
  ...(mode === "wake-throughput" ? { suite } : {}),
  readerMode,
  projectorConnections,
  revision: execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim(),
  actorHashes: Object.fromEntries(Object.entries(source).map(([k, s]) => [k, sha(s)])),
  tests: [],
  cleaned: false,
};
const save = () => writeFile(resolve(out, "report.json"), JSON.stringify(report, null, 2) + "\n");
async function wave(spec, measureMix) {
  const sc = {
    scene: "plain",
    modelDelayMs: 0,
    sink: "full",
    slots: 96,
    inputBytes: 96,
    concurrency: 64,
    count: 192,
    ...spec,
  };
  entries.clear();
  const cohort = measureMix
    ? await (await import("./guest-mix-study.mjs")).mixCohort(db, tenant, sc.count, sc.guestTargets)
    : sc.integrated
      ? await seedCohort(db, tenant, sc)
      : [];
  wakeCount = 0;
  await configure(sc);
  let sampling = true;
  const samples = [];
  const observing = (async () => {
    while (sampling) {
      samples.push(await sample(sc.lockSamples));
      if (sampling) await delay(sc.lockSamples ? 100 : 250);
    }
  })();
  void observing.catch(fatal);
  const start = Date.now();
  let load;
  try {
    load = await runLoad({
      mode: sc.rate ? "arrival" : "closed",
      concurrency: sc.concurrency,
      count: sc.count,
      rate: sc.rate,
      signal: abort.signal,
      work: async (index) => {
        if (fatalError) throw fatalError;
        if (sc.forkAt === index) cohort.push(await forkSeed(db, tenant, cohort[0]));
        const id = randomUUID(),
          e = {
            id,
            session: sessionForArrival(cohort, sc, index),
            index,
            key: randomUUID(),
            text: "Check the fixture " + id,
            ...(sc.changeModelAt !== undefined && index >= sc.changeModelAt
              ? { modelRef: "local-alternate" }
              : {}),
            workerDone: Promise.withResolvers(),
            projected: Promise.withResolvers(),
          };
        if (measureMix) {
          e.realGuest = ((index % 32) + Math.floor(index / 32)) % 4 < sc.guestQuarters;
          e.text = (e.realGuest ? "guest " : "fixed ") + e.text;
        }
        entries.set(id, e);
        if (sc.initialBytes)
          e.text = randomBytes(Math.ceil(sc.initialBytes / 2))
            .toString("hex")
            .slice(0, sc.initialBytes);
        if (sc.resetAt === index) e.resetText = await resetCohortHead(db, tenant, e.session);
        if (sc.switchAt === index) {
          const n = ++sampleId,
            stopped = workers[0].wait("draining" + n);
          workers[0].write({ kind: "drain", n });
          await guard(stopped);
          workers[1].write({ kind: "join" });
          await waitForWakeState(routerState, (s) => s.online === 1 && s.waiting === 1);
        }
        if (sc.joinIdleAt === index) {
          workers[1].write({ kind: "join" });
          await waitForWakeState(routerState, (s) => s.online === 2 && s.waiting === 2);
        }
        if (
          sc.environmentCases &&
          (index === 0 ||
            JSON.stringify(sc.environmentCases[index]) !==
              JSON.stringify(sc.environmentCases[index - 1]))
        )
          await selectEnvironment(db, tenant, e.session, sc.environmentCases[index]);
        e.sentAt = Date.now();
        const accepted = await intake(e, sc);
        assert.equal(accepted, id);
        e.acceptedAt = Date.now();
        if ((sc.joinAfterAdmission || sc.restartRouter) && !transition) {
          transition = (async () => {
            await waitForWakeState(
              async () => ({ accepted: [...entries.values()].filter((r) => r.acceptedAt).length }),
              (s) => s.accepted === sc.count,
            );
            await notifyJob;
            if (sc.restartRouter) {
              router = await startActor(
                "wake-runtime",
                "wake-runtime",
                router.pod,
                String(router.ready.port),
              );
              await configureRouter(sc);
              return;
            }
            await waitForWakeState(routerState, (s) => s.notifications > 0 && s.online === 0);
            for (const worker of sc.joinBoth ? workers : [workers[1]])
              worker.write({ kind: "join" });
          })();
          void transition.catch(fatal);
        }
        if (sc.idempotency) {
          const again = await intake({ ...e, id: randomUUID() }, sc);
          assert.equal(again, id);
        }
        await guard(e.projected.promise, 90000);
        return {
          id,
          success: true,
          observedAt: e.terminal.at,
          settlementMs: e.terminal.at - e.sentAt,
        };
      },
    });
    if (fatalError) throw fatalError;
    const bad = load.results.filter((r) => !r.success);
    assert.equal(bad.length, 0, bad[0]?.error);
    await guard(Promise.all([...entries.values()].map((e) => e.workerDone.promise)));
  } catch (error) {
    if (measureMix) {
      const rows = [...entries.values()];
      report.interruptedGuestMix = {
        label: sc.label,
        rate: sc.rate,
        quarters: sc.guestQuarters,
        targets: sc.guestTargets,
        launched: rows.length,
        claimed: rows.filter((e) => e.claimedAt).length,
        dispatches: rows.filter((e) => e.guestOperationId).length,
        submitConfirmed: rows.filter((e) => e.submitEnded).length,
        settled: rows.filter((e) => e.terminal).length,
        failures: rows
          .filter((e) => e.failedSubmit)
          .map((e) => ({
            runId: e.id,
            ...e.failedSubmit,
            http: e.guestHttp,
            afterAdapterMs: e.failedSubmit.at - e.submitStarted,
          })),
        samples: samples.map((s) => ({
          at: s.at - start,
          lag: s.lag,
          running: s.running,
          queued: s.queued,
        })),
      };
      await save();
    }
    throw error;
  } finally {
    sampling = false;
    await observing;
  }
  await notifyJob;
  await transition;
  const request = projector.wait("results" + sc.label);
  projector.write({ kind: "results", stage: sc.label });
  const projection = await guard(request, 40000);
  assert.equal(projection.lag, 0);
  if (sc.redeliver) {
    assert.equal(projection.stats.stale, 2 * sc.count);
    assert.equal(projection.stats.duplicate, 2 * sc.count);
  }
  const items = [...entries.values()],
    verified = await verify(sc, items, cohort),
    complete = items.filter((i) => i.terminal).length;
  assert.equal(complete, load.results.length);
  const workerResources = await Promise.all(
    workers.map(async (a) => {
      const n = ++sampleId,
        got = a.wait("metrics" + n);
      a.write({ kind: "metrics", n });
      const { cpu, loop, claimStats, residence, cacheBytes } = await guard(got);
      return { worker: a.id, cpu, loop, claimStats, residence, cacheBytes };
    }),
  );
  const peak = (key) => Math.max(0, ...samples.map((s) => s[key]));
  const closest = (at) =>
    samples.reduce(
      (best, s) => (Math.abs(s.at - at) < Math.abs(best.at - at) ? s : best),
      samples[0],
    );
  const offerEnd = sc.rate ? start + ((sc.count - 1) * 1000) / sc.rate : start;
  const elapsed = load.elapsedMs;
  const result = {
    ...sc,
    ...(sc.environmentCases
      ? {
          environmentTrace: items.map((i) => ({
            worker: i.work.actor,
            prefix: i.work.prefixStatus,
            readBytes: i.work.readBytes,
            prompt: i.work.observedEnvironment,
            ...(sc.guestTools ? { bash: i.work.observedBash, runId: i.id } : {}),
          })),
        }
      : {}),
    ...(measureMix ? { guestMix: await measureMix(items) } : {}),
    completed: complete,
    dropped: load.dropped.length,
    ...(sc.integrated
      ? {
          dropReasons: Object.fromEntries(
            Object.entries(Object.groupBy(load.dropped, (r) => r.reason)).map(([k, v]) => [
              k,
              v.length,
            ]),
          ),
          ...(sc.rate
            ? {
                secondHalfMs: d(
                  items
                    .filter((i) => i.sentAt >= start + (offerEnd - start) / 2)
                    .map((i) => i.terminal.at - i.sentAt),
                ),
                completedByOfferEnd: items.filter((i) => i.terminal.at <= offerEnd).length,
              }
            : {}),
        }
      : {}),
    elapsedMs: Math.round(elapsed),
    runsPerSecond: Math.round((complete / elapsed) * 100000) / 100,
    maxInFlight: load.maximumInFlight,
    submitToTerminalMs: d(items.map((i) => i.terminal.at - i.sentAt)),
    ...(wakeStage ? { submitToClaimMs: d(items.map((i) => i.claimedAt - i.sentAt)) } : {}),
    admitMs: d(items.map((i) => i.acceptedAt - i.sentAt)),
    claimCallMs: d(items.map((i) => i.work.claimMs)),
    claimedWorkMs: d(items.map((i) => i.work.workMs)),
    nativeKafkaAckSumMs: d(items.map((i) => i.work.ackMs)),
    nativeRecordsPerRun: d(items.map((i) => i.work.seq - (i.work.baseSeq ?? 0))),
    finishProducerAckMs: d(items.map((i) => i.work.finishAckMs)),
    finishToHandlerMs: d(items.map((i) => i.terminal.finishTransitMs)),
    finishPgMs: d(items.map((i) => i.terminal.finishHandlerMs)),
    answerCommitToRunCommitMs: d(items.map((i) => i.terminal.terminalSinceAnswerMs)),
    ...(items.length && items.every((i) => Number.isFinite(i.work.finalModelOutputAt))
      ? {
          finalModelOutputToRunSettlementMs: d(
            items.map((i) => i.terminal.at - i.work.finalModelOutputAt),
          ),
        }
      : {}),
    extraInputConfirmedMs: d(items.flatMap((i) => i.work.inputAcks)),
    inputClientWaitMs: d(items.map((i) => i.work.clientAckWaitMs)),
    workerResources,
    ...(sc.integrated
      ? {
          locality: {
            hits: items.filter((i) => i.work.prefixStatus === "hit").length,
            cold: items.filter((i) => i.work.prefixStatus === "cold").length,
            fresh: items.filter((i) => i.work.prefixStatus === "new").length,
            pgReadBytes: items.reduce((n, i) => n + i.work.readBytes, 0),
            firstModelMs: d(items.map((i) => i.work.firstModelAt - i.sentAt)),
            prefixReadMs: d(items.map((i) => i.work.restoredMs)),
            ...(sc.verifyFull ? { path: items.map((e) => e.work.prefixStatus) } : {}),
          },
        }
      : {}),
    ...(wakeStage ? { router: await routerState() } : {}),
    wakeCount,
    admissionPoolMs: d(items.map((e) => e.admissionPoolMs)),
    admissionSqlMs: d(items.map((e) => e.admissionSqlMs)),
    serialAckWorkFraction:
      sc.replay === "pipelined"
        ? null
        : items.reduce((n, e) => n + e.work.ackMs, 0) /
          items.reduce((n, e) => n + e.work.workMs, 0),
    claimDistribution: Object.fromEntries(
      workers.map((w) => [w.id, items.filter((i) => i.work.actor === w.id).length]),
    ),
    ...(wakeStage
      ? {
          claimReasons: Object.fromEntries(
            Object.entries(Object.groupBy(items, (i) => i.work.claimReason)).map(([key, list]) => [
              key,
              list.length,
            ]),
          ),
        }
      : {}),
    idleRechecks: items.reduce((a, i) => a + i.work.rechecks, 0),
    generatorDelayMs: d(load.results.map((r) => r.generatorDelayMs)),
    modelCalls: items.reduce((a, i) => a + i.work.modelCalls, 0),
    bytesToKafka: items.reduce((a, i) => a + i.work.bytes, 0),
    peakKafkaLag: peak("lag"),
    peakQueuedRuns: peak("queued"),
    peakRunningRuns: peak("running"),
    peakLocalQueue: peak("localQueue"),
    peakPoolWaiting: peak("poolWaiting"),
    endKafkaLag: projection.lag,
    samples: samples.map((s) => ({ ...s, at: s.at - start })),
    endOfOffer: sc.rate ? closest(offerEnd) : undefined,
    drainAfterOfferMs: sc.rate
      ? Math.round(Math.max(0, Math.max(...items.map((i) => i.terminal.at)) - offerEnd))
      : undefined,
    projection: projection.stats,
    ...(projection.prefix ? { prefix: projection.prefix } : {}),
    ...(projection.pgPath ? { pgPath: projection.pgPath, offeredAt: start } : {}),
    ...(sc.tail ? { tail: { ...projection.tail, phases: summarizeTerminal(items) } } : {}),
    verified,
  };
  if (wakeStage) {
    const stats = workerResources.flatMap((r) => r.claimStats ?? []);
    assert.equal(
      stats.reduce((n, s) => n + s.rows, 0),
      complete,
    );
    assert(stats.every((s) => s.maximumRunning <= sc.slots && s.maximumBatch <= sc.batch));
    assert.equal(result.router.notifications, sc.restartRouter ? 0 : wakeCount);
  }
  if (result.endOfOffer)
    result.endOfOffer = { ...result.endOfOffer, at: Math.round(result.endOfOffer.at - start) };
  report.tests.push(result);
  await save();
  console.log(
    JSON.stringify({
      stage: sc.label,
      completed: complete,
      dropped: result.dropped,
      runsPerSecond: result.runsPerSecond,
      p95: result.submitToTerminalMs.p95,
      lag: result.peakKafkaLag,
      drained: result.endKafkaLag,
      queue: result.peakQueuedRuns,
    }),
  );
  return result;
}
async function stopActors(list) {
  const cleanup = await Promise.allSettled(
    list.map(async (a) => {
      if (!a) return;
      a.closing = true;
      if (a.child.exitCode == null) {
        a.write({ kind: "stop" });
        a.child.stdin.end();
      }
      const code = await limit(a.exit, 45000, a.id + " cleanup");
      assert.equal(
        code,
        0,
        a.id + " cleanup exited " + code + " (acknowledged=" + !!a.ackStopped + "): " + a.stderr,
      );
      assert(a.ackStopped, a.id + " exited without confirming cleanup");
    }),
  );
  const e = cleanup.find((r) => r.status === "rejected");
  if (e) throw e.reason;
}
try {
  if (cpuFixture) {
    // The CPU driver checked the serving DB under the same release lock.
    // This connection is to the new, empty PG instance.
    const {
      rows: [identity],
    } = await admin.query("select current_user as role,to_regclass('public.runs') as serving_runs");
    assert.deepEqual(identity, { role: "bench", serving_runs: null });
    await cpuFixture.pin(
      isCommitStudy(process.env.PI_CLOUD_PG_PATH_PROFILE) ||
        isPrefixStudy(process.env.PI_CLOUD_PG_PATH_PROFILE)
        ? "open"
        : "shared",
    );
  } else {
    const {
      rows: [state],
    } = await admin.query(
      "select count(*) filter(where state not in ('completed','cancelled','failed','timed_out') or recovery_requested_at is not null)::int active from runs",
    );
    assert.equal(state.active, 0, "Other active work prevents a serial development benchmark");
  }
  assert.match(name, /^pma_sessbench_[a-f0-9]{16}$/);
  await admin.query("create database " + name + " template template0");
  created = true;
  assert.equal((await db.query("select current_database() n")).rows[0].n, name);
  await db.query(await readFile(new URL("./schema.sql", import.meta.url), "utf8"));
  report.settings = (
    await db.query(
      "select current_setting('server_version') postgres,current_setting('synchronous_commit') synchronous_commit,current_setting('fsync') fsync",
    )
  ).rows[0];
  // Two claimants meet two pending Runs in one Session. A locked or active predecessor must win.
  const keys = [randomUUID(), randomUUID()],
    session = randomUUID();
  for (const [i, id] of keys.entries())
    await intake({ id, session, key: id, text: "ordered fixture " + i });
  await assert.rejects(
    intake({ id: randomUUID(), session, key: keys[0], text: "different owned input" }),
    /Changed idempotent input/,
  );
  const c = await db.connect(),
    other = await db.connect();
  try {
    await c.query("begin");
    assert.deepEqual(
      (await c.query(claimBatchSql, [8])).rows.map((r) => r.id),
      [keys[0]],
    );
    assert.equal((await other.query(claimBatchSql, [8])).rowCount, 0);
    if (mode === "wake-sizing")
      assert.equal((await other.query(claimHintedBatchSql, [16, tenant, keys[1]])).rowCount, 0);
    await c.query("commit");
    assert.equal((await other.query(claimSql)).rowCount, 0);
    if (mode === "wake-sizing")
      assert.equal((await other.query(claimHintedBatchSql, [16, tenant, keys[1]])).rowCount, 0);
    await c.query("update runs set state='completed' where id=$1", [keys[0]]);
    assert.equal((await other.query(claimSql)).rows[0].id, keys[1]);
    report.sameSessionClaimOrder = true;
  } finally {
    await c.query("rollback");
    c.release();
    other.release();
  }
  await db.query("delete from runs where id=any($1::uuid[])", [keys]);
  if (mode === "wake-throughput" && suite === "smoke") await checkPreferredClaim(db, tenant);
  if (mode === "wake-sizing") {
    const { measureWarmPrefix } = await import("./warm-prefix.mjs");
    report.claimPreference = await measureClaimPreference(db);
    report.warmPrefix = await measureWarmPrefix(db);
    await save();
  }
  projector = await startActor(
    "projector",
    "projector",
    cpuFixture?.projector ?? podFor("session-runtime"),
    mode === "wake-throughput" && suite.startsWith("tail") ? "tail" : "ordinary",
    readerMode,
    String(projectorConnections),
    ...(suite === "pg-paths" ? [process.env.PI_CLOUD_PG_PATH_PROFILE ?? "full"] : []),
  );
  const pods = cpuFixture
    ? [cpuFixture.support, cpuFixture.support]
    : (() => {
        const sts = JSON.parse(kubectl(["get", workload("pi-workers"), "-o", "json"]));
        const selector = Object.entries(sts.spec.selector.matchLabels)
          .map(([k, v]) => k + "=" + v)
          .join(",");
        return JSON.parse(kubectl(["get", "pods", "-l", selector, "-o", "json"]))
          .items.filter(
            (p) =>
              !p.metadata.deletionTimestamp &&
              p.status.conditions.some((c) => c.type === "Ready" && c.status === "True"),
          )
          .map((p) => p.metadata.name)
          .sort();
      })();
  assert(pods.length >= 2);
  for (let i = 0; i < 2; i++) workers.push(await startActor("worker", "w" + i, pods[i]));
  await wave({ label: "warmup", count: 12, concurrency: 12 });
  await wave({ label: "correct-redelivery", count: 2, concurrency: 2, redeliver: true });
  await wave({ label: "correct-finish-held", count: 2, concurrency: 2, holdFinish: true });
  for (const scene of ["follow", "steer", "late"])
    await wave({
      label: "correct-" + scene,
      scene,
      inspect: true,
      count: 2,
      concurrency: 2,
      idempotency: true,
    });
  await wave({
    label: "correct-edit-withdraw",
    scene: "follow",
    mutate: true,
    depth: 3,
    inspect: true,
    count: 1,
    concurrency: 1,
  });
  if (mode === "smoke")
    await wave({ label: "quiet-admission", count: 4, concurrency: 2, omitNotify: true });
  if (mode === "matrix" || mode === "smoke") {
    const n = mode === "smoke" ? 8 : 192;
    if (mode === "matrix") {
      await wave({ label: "serial", count: 12, concurrency: 1 });
      await wave({ label: "plain-16", count: 192, concurrency: 16 });
    }
    await wave({ label: "plain-64-a", count: n, concurrency: Math.min(n, 64) });
    if (mode === "matrix") {
      await wave({ label: "burst-192", count: 192, concurrency: 192 });
      await wave({ label: "plain-terminal-control", sink: "terminal-only", count: 192 });
      await wave({ label: "plain-64-b", count: 192 });
      await wave({ label: "follow-64", scene: "follow", count: 192 });
      await wave({ label: "steer-64", scene: "steer", count: 192 });
      await wave({ label: "plain-64-c", count: 192 });
      await wave({ label: "model-wait-10s", modelDelayMs: 10000, count: 192, concurrency: 192 });
    }
  }
  if (mode === "inbox") {
    for (const size of [1, 16, 64, 128])
      await wave({
        label: "depth-" + size,
        scene: "follow",
        depth: size,
        inputBytes: 512,
        count: 6,
        concurrency: 2,
      });
  }
  if (mode === "capacity") {
    const rates = (process.env.PI_CLOUD_SESSION_BENCH_RATES ?? "20,40,80,120")
      .split(",")
      .map(Number);
    for (const rate of rates) {
      assert(Number.isInteger(rate) && rate > 0 && rate <= 400);
      const r = await wave({ label: "offered-" + rate, rate, count: rate * 30, concurrency: 384 });
      if (r.dropped > 0 || r.drainAfterOfferMs > 8000) break;
    }
  }
  if (mode === "boundary") {
    await wave({ label: "repeat-90", rate: 90, count: 90 * 60, concurrency: 384 });
    await wave({ label: "repeat-120", rate: 120, count: 120 * 30, concurrency: 384 });
    for (const [i, sink] of ["full", "single", "terminal-only", "single", "full"].entries())
      await wave({
        label: "fixed120-" + sink + "-" + i,
        rate: 120,
        count: 120 * 20,
        concurrency: 384,
        omitNotify: true,
        wakeMode: "coalesced",
        sink,
      });
  }
  if (mode === "intake") {
    const cases = [
      { wakeMode: "relay" },
      { wakeMode: "relay", omitNotify: true },
      { wakeMode: "relay", omitNotify: true },
      { wakeMode: "relay" },
      { wakeMode: "coalesced", omitNotify: true },
      {},
    ];
    for (const [i, c] of cases.entries())
      await wave({ label: "intake-" + i, ...c, count: 768, lockSamples: true });
    for (const rate of [120, 180, 240]) {
      const r = await wave({
        label: "coalesced-offer-" + rate,
        rate,
        count: rate * 20,
        concurrency: 384,
        omitNotify: true,
        wakeMode: "coalesced",
        lockSamples: true,
      });
      if (r.dropped > 0 || r.drainAfterOfferMs > 5000) break;
    }
  }
  if (mode === "diagnose") {
    await wave({
      label: "single-correct",
      sink: "single",
      scene: "follow",
      depth: 2,
      count: 4,
      concurrency: 2,
      inspect: true,
    });
    for (const variant of [
      {},
      { sink: "single" },
      { replay: "serial" },
      { replay: "pipelined" },
      { sink: "terminal-only" },
      { replay: "pipelined", sink: "terminal-only" },
      { sink: "single" },
      {},
    ])
      await wave({ label: "trace-" + report.tests.length, ...variant, count: 384 });
  }
  if (mode.startsWith("wake")) {
    const pod = cpuFixture?.support ?? podFor("session-runtime");
    router = await startActor("wake-runtime", "wake-runtime", pod);
    if (mode !== "wake-fleet") {
      if (cpuFixture) wakeUrl = "ws://127.0.0.1:" + router.ready.port + "/" + topic;
      else {
        network = wakeNetwork(pod, pods.slice(0, 2), router.ready.port, token);
        wakeUrl = network.address + "/" + topic;
      }
      if (mode === "wake-sizing") await runSizingTrials(wave);
      else if (mode === "wake-throughput" && suite === "pg-paths") {
        if (process.env.PI_CLOUD_PG_PATH_PROFILE !== "workspace-fanout")
          assert.equal(projectorConnections, 12);
        assert.equal(readerMode, "batch");
        const { runPgPaths } = await import("./pg-path-trials.mjs");
        await runPgPaths({
          guestEndpoints,
          projector,
          emitter: workers[0],
          receiver: workers[1],
          wave,
          configure,
          guard,
          observer: statsDb,
          forkPool: db,
          report,
          save,
          out,
          token,
          cpuFixture,
        });
      } else if (mode === "wake-throughput") await runLocality(wave, suite);
      else await runWakeTrials(wave);
    } else {
      await stopActors(workers);
      report.population = {};
      for (const label of ["directed-pop", "broadcast-pop", "directed-repeat"]) {
        const result = router.wait("population" + label);
        router.write({ kind: "population", stage: label });
        report.population[label] = (await guard(result, 240000)).result;
        console.log(JSON.stringify({ label, results: report.population[label] }));
        await save();
      }
    }
  }
} catch (error) {
  report.error = String(error.stack ?? error).slice(0, 2600);
  throw error;
} finally {
  guestHttp?.close();
  const cleanErrors = [];
  try {
    await stopActors(actors.filter((a) => a !== projector));
  } catch (e) {
    cleanErrors.push(e);
  }
  try {
    await stopActors(projector ? [projector] : []);
  } catch (e) {
    cleanErrors.push(e);
  }
  if (network) {
    try {
      network.close();
      report.wakePoliciesRemoved = true;
    } catch (error) {
      cleanErrors.push(error);
    }
  }
  try {
    await db.end();
    await statsDb.end();
    if (created) {
      await admin.query("drop database " + name);
      assert(!(await admin.query("select 1 from pg_database where datname=$1", [name])).rowCount);
    }
  } catch (e) {
    cleanErrors.push(e);
  }
  await admin.end();
  report.projectorResourcesRemoved = actorCleanup;
  report.cleaned = !cleanErrors.length && actorCleanup;
  if (cleanErrors.length)
    report.cleanupErrors = cleanErrors.map((e) => String(e.message).slice(0, 500));
  await save();
  if (cleanErrors.length) throw new AggregateError(cleanErrors, "Benchmark cleanup");
}
assert(report.cleaned);
console.log("Verified topic, group and scratch database cleanup.");
