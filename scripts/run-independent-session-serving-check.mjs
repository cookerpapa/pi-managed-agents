// Isolated database and RF3 brokers; the only external dependency is the
// explicitly configured provider. No credentials, prompts or responses are reported.
import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { randomUUID, randomBytes, scryptSync, createHash } from "node:crypto";
import { mkdtemp, chmod, writeFile, readFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { tmpdir } from "node:os";
import { setTimeout as delay } from "node:timers/promises";
import { createServer } from "node:net";
import pg from "pg";
import kafkaNative from "@confluentinc/kafka-javascript";
import { createDatabase } from "@pi-cloud/database";
import { createPrivateTenant } from "../packages/control-plane/src/tenant-administration.ts";
import { PiCloudApi } from "../packages/web-ui/src/api.ts";
import { kafkaProducerLane } from "@pi-cloud/runtime-core/kafka-accepted-fact";
import { testPostgres } from "./lib/test-postgres.mjs";
import { distribution } from "./lib/runtime-load-scheduler.mjs";
import { createUnsettledTurnIndex } from "./experiments/minimal-session-storage/independent-session-leases.mjs";
import { defaultSessionClaimBatch } from "./experiments/minimal-session-storage/segment-wake.mjs";
import { servingLoadFixture } from "./serving/independent-session/load-fixture.mjs";
import { withChromePage } from "./lib/chrome-cdp.mjs";
import { testKafka } from "./lib/test-kafka.mjs";
const tag = process.argv[2],
  load = process.argv[3] === "--load",
  capacity = load ? Number(process.env.PI_CLOUD_CHECK_WORKER_SLOTS ?? 64) : 4;
if (!Number.isSafeInteger(capacity) || capacity < 1 || capacity > 64)
  throw Error("Invalid load Worker capacity");
const mixed = load && process.argv.includes("--mixed");
const profileClaims = load && process.argv.includes("--profile-claims");
const probe = "scripts/experiments/minimal-session-storage/claim-timing-probe.mjs";
let provider = process.env.PI_CLOUD_PROVIDER_GATEWAY_URL,
  keyFile = process.env.PI_CLOUD_PROVIDER_GATEWAY_API_KEY_FILE;
if (!tag || (!load && (!provider || !keyFile)))
  throw Error("Specify the image tag and upstream URL/key file");
const clean = [],
  names = [],
  suffix = randomUUID().slice(0, 8),
  topic = "pma.segment." + suffix,
  partitions = 4;
async function port() {
  const s = createServer();
  await new Promise((r) => s.listen(0, "127.0.0.1", r));
  const p = s.address().port;
  await new Promise((r) => s.close(r));
  return p;
}
async function until(label, fn, ms = 120000) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    const v = await fn();
    if (v) return v;
    await delay(200);
  }
  throw Error("Timed out: " + label);
}
const docker = (args, options = {}) =>
  execFileSync("docker", args, { encoding: "utf8", ...options });
try {
  const pgFix = await testPostgres({});
  clean.push(() => pgFix.close());
  if (load && process.env.PI_CLOUD_CHECK_PG_CPUS)
    await pgFix.setCpuLimit(Number(process.env.PI_CLOUD_CHECK_PG_CPUS));
  const kafka = await testKafka({});
  clean.push(() => kafka.close());
  const tmp = await mkdtemp(join(tmpdir(), "pma-serving-"));
  clean.push(() => rm(tmp, { recursive: true, force: true }));
  await chmod(tmp, 0o711);
  const config = {
    DATABASE_URL_FILE: "/run/pma/database",
    PI_CLOUD_SEGMENT_TOPIC: topic,
    PI_CLOUD_SEGMENT_PARTITIONS: String(partitions),
    PI_CLOUD_KAFKA_BROKERS: kafka.brokers.join(","),
  };
  async function file(name, contents) {
    const f = join(tmp, name);
    await writeFile(f, contents, { mode: 0o400 });
    return f;
  }
  const dbFile = await file("database", pgFix.url),
    management = await file("management", randomBytes(40).toString("base64url")),
    enroll = await file("enroll", randomBytes(40).toString("base64url"));
  let fake;
  if (load) {
    const key = randomBytes(32).toString("hex");
    fake = await servingLoadFixture(key);
    clean.push(() => fake.close());
    provider = fake.url;
    keyFile = await file("load-key", key);
  }
  const mount = (host, name) => [
    "--mount",
    "type=bind,src=" + host + ",dst=/run/pma/" + name + ",readonly",
  ];
  const mgEnv = { PI_CLOUD_SUPERVISOR_MANAGEMENT_TOKEN_FILE: "/run/pma/management" },
    enEnv = { PI_CLOUD_SUPERVISOR_ENROLLMENT_TOKEN_FILE: "/run/pma/enroll" };
  function role(name, roleName, env, files, command = [], imageTag = tag) {
    const label = "pma-" + name + "-" + suffix;
    docker([
      "run",
      "-d",
      "--network",
      "host",
      "--add-host",
      "pma-segment-" + suffix + "-0:127.0.0.1",
      "--add-host",
      "pma-segment-" + suffix + "-1:127.0.0.1",
      "--name",
      label,
      "--user",
      process.getuid() + ":" + process.getgid(),
      ...Object.entries({ ...config, ...env }).flatMap(([k, v]) => ["-e", k + "=" + v]),
      ...mount(dbFile, "database"),
      ...files,
      "pi-managed-agents/segment-" + roleName + ":" + imageTag,
      ...command,
    ]);
    names.push(label);
    clean.push(() => docker(["rm", "-f", label]));
    return label;
  }
  const priorImage = process.env.PI_CLOUD_CHECK_QUEUE_UPGRADE_FROM;
  if (priorImage) assert(/^[a-z0-9][a-z0-9_.-]{0,90}$/.test(priorImage));
  const migration = role("migration", "migrate", {}, [], [], priorImage ?? tag);
  const exit = docker(["wait", migration]).trim();
  assert.equal(exit, "0", "migration failed");
  const db = createDatabase({ connectionString: pgFix.url });
  clean.push(() => db.destroy());
  const pool = new pg.Pool({ connectionString: pgFix.url });
  clean.push(() => pool.end());
  const {
    rows: [{ installed: claimIndexInstalled }],
  } = await pool.query(
    "select exists(select from pg_index where indexrelid=to_regclass('pma_segments.turns_unsettled_by_session') and indisvalid) as installed",
  );
  // Allows the historical serving images to participate in the same A/B study.
  const indexStudy = load && process.argv.includes("--index-unsettled") && !claimIndexInstalled;
  if (indexStudy) {
    const c = await pool.connect();
    try {
      await c.query("begin");
      await c.query("set local search_path=pma_segments,public");
      await createUnsettledTurnIndex(c);
      await c.query("commit");
    } catch (e) {
      await c.query("rollback");
      throw e;
    } finally {
      c.release();
    }
  }
  const pass = randomBytes(26).toString("base64url"),
    salt = randomBytes(16).toString("base64url"),
    username = "canary-" + suffix;
  const identity = await createPrivateTenant(db, {
    slug: "serving-" + suffix,
    ownerDisplayName: "Staging",
    webAccount: {
      username,
      role: "owner",
      passwordSalt: salt,
      passwordHash: scryptSync(pass, salt, 32, { N: 16384, r: 8, p: 1 }).toString("base64url"),
      scryptN: 16384,
      scryptR: 8,
      scryptP: 1,
    },
    initialModel: {
      provider: "openai-codex",
      modelId: process.env.PI_CLOUD_CHECK_MODEL ?? "gpt-6.1-sol",
    },
  });
  if (priorImage) {
    const { registerTestWorker } =
      await import("../packages/control-plane/test/fixtures/registered-worker.ts");
    const owner = randomUUID(),
      boot = randomUUID(),
      logical = randomUUID(),
      first = randomUUID(),
      second = randomUUID(),
      input = randomUUID();
    await db
      .insertInto("workers")
      .values({
        id: owner,
        supervisor_id: owner,
        boot_id: boot,
        state: "ready",
        max_concurrent_sessions: 1,
      })
      .execute();
    await registerTestWorker(db, owner);
    const c = await pool.connect();
    try {
      await c.query("set search_path=pma_segments,public");
      await c.query("insert into flows(tenant_id,flow_id) values($1,$2)", [
        identity.tenantId,
        logical,
      ]);
      await c.query(
        "insert into sessions(tenant_id,id,flow_id,family_id,agent_name,desired_model) values($1,$2,$2,$2,'root','{}')",
        [identity.tenantId, logical],
      );
      for (const [i, id] of [first, second].entries())
        await c.query(
          "insert into turns(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,state) values($1,$2,$3,$4,$1,'upgrade','queued')",
          [id, identity.tenantId, logical, i + 1],
        );
      await c.query(
        "insert into user_or_agent_messages(tenant_id,session_id,id,sender_id,source,mode,body,fingerprint,turn_id,request_key) values($1,$2,$3,$4,'user','follow_up','upgrade','fixture',$5,'stable')",
        [identity.tenantId, logical, input, owner, first],
      );
      const denied = role("old-boot-guard", "migrate", {}, []);
      assert.notEqual(docker(["wait", denied]).trim(), "0");
      assert.equal((await c.query("select version from installation")).rows[0].version, 1);
      await c.query(
        "update public.supervisor_connections set state='expired',close_reason='heartbeat_timeout',closed_at=clock_timestamp() where worker_id=$1",
        [owner],
      );
      const upgrade = role("queue-upgrade", "migrate", {}, []);
      assert.equal(docker(["wait", upgrade]).trim(), "0");
      assert.equal(
        (
          await c.query(
            "select version,to_regclass('pma_segments.ready_agent_sessions') as retired from installation",
          )
        ).rows[0].retired,
        null,
      );
      const { rows } = await c.query(
        "select id,ready_at is not null as ready from turns where session_id=$1 order by mailbox_position",
        [logical],
      );
      assert.deepEqual(rows, [
        { id: first, ready: true },
        { id: second, ready: false },
      ]);
      assert(
        (await c.query("select starts_turn from user_or_agent_messages where id=$1", [input]))
          .rows[0].starts_turn,
      );
      await c.query("delete from turns where session_id=$1", [logical]);
      console.log(
        "Matched queue upgrade rejected an active old boot, preserved both Turns and their input, and promoted only the head.",
      );
    } finally {
      await c.query("reset search_path");
      c.release();
    }
  }
  const admin = new kafkaNative.KafkaJS.Kafka({
    kafkaJS: { brokers: kafka.brokers, logLevel: kafkaNative.KafkaJS.logLevel.NOTHING },
  }).admin();
  await admin.connect();
  clean.push(() => admin.disconnect());
  await admin.createTopics({
    topics: [
      {
        topic,
        numPartitions: partitions,
        replicationFactor: 3,
        configEntries: [
          { name: "min.insync.replicas", value: "2" },
          { name: "retention.ms", value: "604800000" },
        ],
      },
    ],
  });
  const [projectorPort, apiPort, wakePort, managementA, managementB, modelA, modelB] =
    await Promise.all(Array.from({ length: 7 }, () => port()));
  const url = (p) => "http://127.0.0.1:" + p;
  const runtimeUrl = url(projectorPort),
    apiUrl = url(apiPort),
    hostPorts = [managementA, managementB];
  const health = (p) =>
    fetch(url(p) + "/health/ready", { signal: AbortSignal.timeout(1000) }).then(
      (r) => r.ok,
      () => false,
    );
  const started = Date.now();
  role(
    "projector",
    "projector",
    {
      ...mgEnv,
      ...enEnv,
      PORT: projectorPort,
      PI_CLOUD_SEGMENT_WAKE_PORT: wakePort,
      PI_CLOUD_SEGMENT_MANAGEMENT_TEMPLATES: JSON.stringify(
        hostPorts.map((p) => "http://{supervisorId}:" + p),
      ),
      PI_CLOUD_SEGMENT_CAPACITY: capacity,
    },
    [...mount(management, "management"), ...mount(enroll, "enroll")],
  );
  await until("Projector ready", () => health(projectorPort), 45000);
  role(
    "api",
    "api",
    { ...mgEnv, PORT: apiPort, PI_CLOUD_SESSION_RUNTIME_URL: runtimeUrl },
    mount(management, "management"),
  );
  await until("API ready", () => health(apiPort), 45000);
  const api = new PiCloudApi((p, o) => fetch(new URL(p, apiUrl), o), identity.credential.token);
  const project = await api.createProject("Serving check");
  let session, title;
  for (let i = 0; i < 16; i++) {
    title = "Serving " + i;
    session = (await api.createSession(project.projectId, project.workspaceId, title, "elastic"))
      .sessionId;
    if (kafkaProducerLane(session, partitions) !== 0) break;
  }
  assert.notEqual(kafkaProducerLane(session, partitions), 0);
  const denied = await fetch(apiUrl + "/v1/sessions/" + session + "/queued-inputs");
  assert.equal(denied.status, 401, "Queue reads require authentication");
  const pendingUsage = await fetch(apiUrl + "/v1/conversations/" + session + "/usage", {
    headers: { authorization: "Bearer " + identity.credential.token },
  });
  assert.equal(pendingUsage.status, 501, "An unmapped usage source cannot return a zero balance");
  const verifyUpgrade = process.argv.includes("--verify-claim-upgrade");
  if (verifyUpgrade) {
    assert(claimIndexInstalled, "Fresh migration missed the Turn claim index");
    // Re-create an existing pre-index installation in the disposable database.
    await pool.query("drop index pma_segments.turns_unsettled_by_session");
    const blocker = await pool.connect();
    try {
      await blocker.query("begin");
      await blocker.query("lock table pma_segments.turns in row exclusive mode");
      const {
        rows: [{ pid: blockerPid }],
      } = await blocker.query("select pg_backend_pid() as pid");
      const attempt = role("interrupted-migration", "migrate", {}, []);
      const pid = await until(
        "online Turn index build",
        async () => {
          const {
            rows: [r],
          } = await pool.query(
            `select pid from pg_stat_progress_create_index
          where relid='pma_segments.turns'::regclass and current_locker_pid=$1`,
            [blockerPid],
          );
          return r?.pid;
        },
        30000,
      );
      const writer = await pool.connect();
      try {
        await writer.query("begin");
        await writer.query("set local search_path=pma_segments,public");
        await writer.query("set local lock_timeout='1s'");
        const inserted = await writer.query(
          `insert into pma_segments.turns
          (id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,state)
          select gen_random_uuid(),tenant_id,flow_id,1,gen_random_uuid(),'index check','queued'
          from pma_segments.sessions where id=$1`,
          [session],
        );
        assert.equal(inserted.rowCount, 1);
      } finally {
        await writer.query("rollback");
        writer.release();
      }
      await pool.query("select pg_cancel_backend($1)", [pid]);
      assert.notEqual(docker(["wait", attempt]).trim(), "0");
      const {
        rows: [index],
      } = await pool.query(
        "select indisvalid from pg_index where indexrelid=to_regclass('pma_segments.turns_unsettled_by_session')",
      );
      assert.equal(index?.indisvalid, false, "Expected a recoverable incomplete index");
    } finally {
      await blocker.query("rollback");
      blocker.release();
    }
  }
  const repeat = role("migration-check", "migrate", {}, []);
  assert.equal(docker(["wait", repeat]).trim(), "0", "Migration retry with an existing Session");
  if (verifyUpgrade) {
    const validIndex = async () =>
      (
        await pool.query(
          "select indexrelid::oid as oid from pg_index where indexrelid=to_regclass('pma_segments.turns_unsettled_by_session') and indisvalid",
        )
      ).rows[0]?.oid;
    const repaired = await validIndex();
    assert.ok(repaired);
    const again = role("migration-noop", "migrate", {}, []);
    assert.equal(docker(["wait", again]).trim(), "0");
    assert.equal(await validIndex(), repaired, "Healthy index rebuilt on migration retry");
    console.log(
      "Existing installation accepted a concurrent Turn insert, recovered the interrupted index build and retained the valid index on retry.",
    );
  }
  let mixedFile, tapeHash;
  const mixedEntry = "scripts/experiments/minimal-session-storage/claim-mixed-probe.mjs";
  if (mixed) {
    assert(!profileClaims);
    const { nativeBashTrace } =
      await import("./experiments/minimal-session-storage/native-bash-trace.mjs");
    const requestId = "11111111-1111-4111-8111-111111111111";
    const tape = process.env.PI_CLOUD_CHECK_MIXED_TAPE
      ? await readFile(process.env.PI_CLOUD_CHECK_MIXED_TAPE)
      : Buffer.from(
          JSON.stringify({
            requestId,
            frames: await nativeBashTrace({ runId: requestId }, undefined, {
              callsPerRun: 20,
              callIdAt: (i) => "call-" + i,
            }),
          }),
        );
    tapeHash = createHash("sha256").update(tape).digest("hex");
    mixedFile = await file("mixed-trace", tape);
  }
  const startWorker = async (i) => {
    const label = role(
      "worker-" + i,
      "worker",
      {
        ...mgEnv,
        ...enEnv,
        HOST: "127.0.0.1",
        PI_CLOUD_SUPERVISOR_ID: "pma-segment-" + suffix + "-" + i,
        PI_CLOUD_SEGMENT_MANAGEMENT_URL:
          "http://pma-segment-" + suffix + "-" + i + ":" + hostPorts[i],
        PI_CLOUD_SESSION_RUNTIME_URL: runtimeUrl,
        PI_CLOUD_SEGMENT_WAKE_URL: "ws://127.0.0.1:" + wakePort + "/internal/segment-work",
        PI_CLOUD_SEGMENT_BOOT_DIR: "/tmp/segment-boot",
        PI_CLOUD_MODEL_GATEWAY_PORT: [modelA, modelB][i],
        PI_CLOUD_SEGMENT_CAPACITY: capacity,
        ...(profileClaims && process.argv.includes("--selector-cpu")
          ? { PI_CLOUD_TEST_SELECTOR_CPU: "1" }
          : {}),
        ...(profileClaims && process.argv.includes("--prepare-selector")
          ? { PI_CLOUD_TEST_PREPARE_SELECTOR: "1" }
          : {}),
        ...(process.env.PI_CLOUD_SEGMENT_CLAIM_BATCH
          ? { PI_CLOUD_SEGMENT_CLAIM_BATCH: process.env.PI_CLOUD_SEGMENT_CLAIM_BATCH }
          : {}),
        PI_CLOUD_PROVIDER_GATEWAY_URL: provider,
        PI_CLOUD_PROVIDER_GATEWAY_API_KEY_FILE: "/run/pma/provider",
      },
      [
        ...mount(management, "management"),
        ...mount(enroll, "enroll"),
        ...mount(keyFile, "provider"),
        ...(mixed
          ? [
              ...mount(mixedFile, "mixed-trace"),
              "--mount",
              "type=bind,src=" +
                fileURLToPath(new URL("../" + mixedEntry, import.meta.url)) +
                ",dst=/app/" +
                mixedEntry +
                ",readonly",
            ]
          : []),
        ...(profileClaims
          ? [
              "--mount",
              "type=bind,src=" +
                fileURLToPath(new URL("../" + probe, import.meta.url)) +
                ",dst=/app/" +
                probe +
                ",readonly",
            ]
          : []),
      ],
      mixed ? ["/app/" + mixedEntry] : profileClaims ? ["/app/" + probe] : [],
    );
    await until("Worker ready", () => health(hostPorts[i]), 45000);
    return label;
  };
  if (load) {
    const rate = Number(process.env.PI_CLOUD_CHECK_ARRIVALS_PER_SECOND ?? 0);
    const count = Number(process.env.PI_CLOUD_CHECK_TURNS ?? 192),
      parallel = 64;
    assert(Number.isSafeInteger(count) && count >= 1 && count <= 4096);
    assert(Number.isSafeInteger(rate) && rate >= 0 && rate <= 1000);
    const workerName = await startWorker(0);
    const sessions = [];
    for (let at = 0; at < count; at += 16)
      sessions.push(
        ...(await Promise.all(
          Array.from({ length: Math.min(16, count - at) }, (_, i) =>
            api.createSession(
              project.projectId,
              project.workspaceId,
              "Load " + (at + i),
              "elastic",
            ),
          ),
        )),
      );
    async function pgCpu() {
      const {
        rows: [r],
      } = await pool.query(`select
        pg_current_wal_insert_lsn() as lsn, substring(body from 'usage_usec ([0-9]+)')::float8/1000 cpu_ms,
        substring(body from 'nr_throttled ([0-9]+)')::int periods
        from pg_read_file('/sys/fs/cgroup/cpu.stat') body`);
      return r;
    }
    const initialCpu = await pgCpu();
    let cursor = 0;
    const begin = performance.now(),
      ids = [];
    const lagSamples = [],
      launchDelays = [],
      admissionAckSamples = [];
    let sampling = false,
      peakSampledRawLag = 0,
      sampleError;
    const measureLag = async () => {
      const offsets = await admin.fetchTopicOffsets(topic),
        positions = await pool.query(
          "select partition,next_offset from pma_segments.accepted_fact_projection_offsets where topic=$1",
          [topic],
        );
      return offsets.reduce(
        (n, o) =>
          n +
          Math.max(
            0,
            Number(
              BigInt(o.high) -
                BigInt(positions.rows.find((p) => p.partition === o.partition)?.next_offset ?? 0),
            ),
          ),
        0,
      );
    };
    const timer = setInterval(async () => {
      if (sampling) return;
      sampling = true;
      try {
        const lag = await measureLag();
        peakSampledRawLag = Math.max(peakSampledRawLag, lag);
        if (rate)
          lagSamples.push({ ms: Math.round(performance.now() - begin), admitted: ids.length, lag });
      } catch (e) {
        sampleError = e;
      } finally {
        sampling = false;
      }
    }, 250);
    timer.unref();
    clean.push(() => clearInterval(timer));
    await Promise.all(
      Array.from({ length: parallel }, async () => {
        for (;;) {
          const i = cursor++;
          if (i >= count) return;
          if (rate) {
            const due = begin + (i * 1000) / rate,
              remaining = due - performance.now();
            if (remaining > 0) await delay(remaining);
            launchDelays.push(Math.max(0, performance.now() - due));
          }
          const prompt = mixed ? "Run the fixed test tool." : fake.prompt("turn_" + i);
          const key = "turn:" + randomUUID();
          const sentAt = performance.now();
          const a = await api.acceptTurn(sessions[i].sessionId, prompt, key);
          admissionAckSamples.push(performance.now() - sentAt);
          ids.push(a.turnId);
        }
      }),
    );
    const admittedMs = performance.now() - begin;
    const admissionAckMs = distribution(admissionAckSamples);
    assert.equal(admissionAckMs.count, count);
    await until(
      "all load Turns sealed",
      async () =>
        (
          await pool.query(
            "select count(*)::int n from pma_segments.turns where id=any($1::uuid[]) and output_sealed_at is not null",
            [ids],
          )
        ).rows[0].n === count,
      300000,
    );
    const elapsedMs = performance.now() - begin;
    clearInterval(timer);
    const finalCpu = await pgCpu();
    const {
      rows: [wal],
    } = await pool.query("select pg_wal_lsn_diff($1::pg_lsn,$2::pg_lsn)::float8 bytes", [
      finalCpu.lsn,
      initialCpu.lsn,
    ]);
    if (sampleError) throw sampleError;
    const {
      rows: [timing],
    } = await pool.query(
      `select count(*) filter(where t.state='completed')::int completed,
     percentile_cont(0.95) within group(order by extract(epoch from t.started_at-t.created_at)*1000) queue_to_grant_p95_ms,
     percentile_cont(0.95) within group(order by extract(epoch from e.occurred_at-t.started_at)*1000) grant_to_terminal_p95_ms,
     percentile_cont(0.99) within group(order by extract(epoch from t.settled_at-e.occurred_at)*1000) terminal_projection_p99_ms
     from pma_segments.turns t join pma_segments.session_terminal_events e on e.run_id=t.id where t.id=any($1::uuid[])`,
      [ids],
    );
    // Reconstruct the stages from recorded clocks after the load. No extra
    // polling of the Turn tables competes with the timed workload.
    const stageSamples = rate
      ? (
          await pool.query(
            `with times as materialized (
      select t.created_at,t.started_at,e.occurred_at,t.settled_at from pma_segments.turns t
      join pma_segments.session_terminal_events e on e.run_id=t.id where t.id=any($1::uuid[])
    ), boundaries as (select q,(select min(created_at) from times)+q*($2::float8/$3)*interval '1 second' as at
      from unnest(array[0.25,0.50,0.75,1.0]::float8[]) q)
    select q,count(*) filter(where created_at<=at and started_at>at)::int waiting_claim,
      count(*) filter(where started_at<=at and occurred_at>at)::int running,
      count(*) filter(where occurred_at<=at and settled_at>at)::int terminal_in_flight,
      count(*) filter(where settled_at<=at)::int settled
      from times cross join boundaries group by q order by q`,
            [ids, count, rate],
          )
        ).rows
      : undefined;
    // Each Session in this fixture starts exactly one Turn. Its persisted
    // physical epoch therefore still carries the granting transaction's xmin.
    const {
      rows: [claims],
    } = await pool.query(
      `
      select count(*)::int claim_transactions,max(n)::int largest_claim,
        max(spread_ms) grant_spread_max_ms
      from (select p.xmin::text,count(*) n,
        extract(epoch from max(t.started_at)-min(t.started_at))*1000 as spread_ms
        from pma_segments.turns t join public.pi_sessions p on p.tenant_id=t.tenant_id
          and p.id::text=t.session_id::text where t.id=any($1::uuid[]) group by p.xmin::text) b`,
      [ids],
    );
    if (timing.completed !== count) {
      const { rows } = await pool.query(
        "select content::jsonb #>> '{model,0,errorMessage}' as model_error, count(*) from pma_segments.messages where role='assistant' group by 1 limit 3",
      );
      const { completed, rejected, errors, diagnostics } = fake.metrics();
      console.error("Synthetic load diagnostic", {
        completed,
        rejected,
        errors,
        diagnostics,
        rows,
      });
    }
    assert.equal(timing.completed, count);
    let visible;
    if (mixed) {
      const { rows } = await pool.query(
        `with outputs as (
        select run_id,commit_seq,role,content::jsonb#>'{model,0}' as msg from pma_segments.messages
        where run_id=any($1::uuid[]) and role in ('assistant','toolResult'))
        select run_id,count(distinct commit_seq)::int n,
          array_agg(msg#>>'{content,0,id}' order by commit_seq) filter(where msg#>>'{content,0,type}'='toolCall') calls,
          array_agg(msg->>'toolCallId' order by commit_seq) filter(where role='toolResult') results,
          count(*) filter(where msg#>>'{content,0,text}'='ok')::int finals
        from outputs group by run_id`,
        [ids],
      );
      assert.equal(rows.length, count);
      const expected = Array.from({ length: 20 }, (_, i) => "call-" + i);
      for (const r of rows) {
        assert.deepEqual(r.calls, expected);
        assert.deepEqual(r.results, expected);
        assert.equal(r.finals, 1);
      }
      visible = { n: rows.reduce((n, r) => n + r.n, 0) };
    } else {
      const {
        rows: [answer],
      } = await pool.query(
        "select count(*)::int n from pma_segments.messages where run_id=any($1::uuid[]) and role='assistant' and navigation_caption is not null",
        [ids],
      );
      assert.equal(answer.n, count);
      visible = answer;
    }
    const offsets = await admin.fetchTopicOffsets(topic),
      { rows: positions } = await pool.query(
        "select partition,next_offset from pma_segments.accepted_fact_projection_offsets where topic=$1",
        [topic],
      );
    const finalLag = offsets.reduce(
      (sum, o) =>
        sum +
        Number(
          BigInt(o.high) - BigInt(positions.find((v) => v.partition === o.partition).next_offset),
        ),
      0,
    );
    assert.equal(finalLag, 0);
    console.log(
      JSON.stringify({
        mode: mixed ? "mixed-claim-replay" : "text-load",
        ...(mixed
          ? {
              tapeHash,
              completeCalls: count * 20,
              completeResults: count * 20,
              completeAnswers: count,
            }
          : {}),
        claimBatchLimit: Number(
          process.env.PI_CLOUD_SEGMENT_CLAIM_BATCH ?? defaultSessionClaimBatch,
        ),
        count,
        ...(rate
          ? {
              targetPerSecond: rate,
              launchDelayP95Ms: launchDelays.sort((a, b) => a - b)[Math.ceil(count * 0.95) - 1],
              lagSamples,
              stageSamples,
              drainMs: elapsedMs - admittedMs,
            }
          : {}),
        claimIndexInstalled,
        indexStudy,
        ...(profileClaims
          ? {
              preparedSelector: process.argv.includes("--prepare-selector"),
              selectorCpu: process.argv.includes("--selector-cpu"),
            }
          : {}),
        parallel,
        partitions,
        admittedMs,
        admissionAckMs,
        elapsedMs,
        visible: visible.n,
        visiblePerSecond: (visible.n * 1000) / elapsedMs,
        finalLag,
        peakSampledRawLag,
        ...timing,
        ...claims,
        workerSlots: capacity,
        pgCpuLimit: Number(process.env.PI_CLOUD_CHECK_PG_CPUS ?? 2),
        pgWalBytesPerTurn: wal.bytes / count,
        pgCpuMs: finalCpu.cpu_ms - initialCpu.cpu_ms,
        pgThrottledPeriods: finalCpu.periods - initialCpu.periods,
        peakModelRequests: fake.metrics().maximumInflight,
        upstreamRequests: fake.metrics().completed,
      }),
    );
    if (profileClaims) {
      const timeline = await pgFix.durationTimeline();
      const samples = docker(["logs", workerName], { maxBuffer: 8 * 1024 * 1024 })
        .split("\n")
        .filter((l) => l.startsWith('{"measurement":"sessionClaimTiming"'))
        .map((l) => JSON.parse(l));
      assert.equal(
        samples.reduce((n, r) => n + r.size, 0),
        count,
      );
      for (const sample of samples) {
        assert.ok(sample.stages.commit.notices, "PG duration messages missing");
        for (const [label, pid, lo, hi, start, end] of sample.queries) {
          const phases = timeline.get(pid)?.slice(lo, hi);
          assert.equal(phases?.length, hi - lo, "Missing server timing for Worker query");
          assert.ok(phases.length);
          const part = sample.stages[label],
            first = phases[0],
            last = phases.at(-1);
          // Extended query phases are Parse, Bind and Execute. A named statement
          // already parsed on this connection sends only Bind and Execute.
          if (label === "selectCandidates") {
            assert.ok(phases.length === 3 || phases.length === 2);
            if (phases.length === 3) part.parses = (part.parses ?? 0) + 1;
            part.parseMs = (part.parseMs ?? 0) + (phases.length === 3 ? phases[0].duration : 0);
            part.bindMs = (part.bindMs ?? 0) + phases.at(-2).duration;
            part.executeMs = (part.executeMs ?? 0) + phases.at(-1).duration;
          }
          part.toPgMs = (part.toPgMs ?? 0) + first.end - first.duration - start;
          part.fromPgMs = (part.fromPgMs ?? 0) + end - last.end;
          part.pgSpanMs = (part.pgSpanMs ?? 0) + last.end - first.end + first.duration;
          part.loggedMs = (part.loggedMs ?? 0) + phases.reduce((n, i) => n + i.duration, 0);
        }
        if (process.argv.includes("--selector-cpu")) {
          const part = sample.stages.selectCandidates;
          assert.equal(part.cpu?.PARSE?.count ?? 0, part.parses ?? 0);
          assert.equal(part.cpu?.BIND?.count, part.calls);
          assert.equal(part.cpu?.EXECUTE?.count, part.calls);
        }
        for (const part of Object.values(sample.stages))
          assert.ok(Math.abs(part.loggedMs - part.serverMs) < 0.01, "PG log/notice mismatch");
        delete sample.queries;
        console.log(JSON.stringify(sample));
      }
    }
    const held = await api.acceptTurn(
      session,
      fake.prompt("stop_test", { firstByteMs: 30000 }),
      "turn:" + randomUUID(),
    );
    await until("model in flight", () => fake.metrics().inflight > 0);
    await api.cancelTurn(session, held.turnId, "stop:" + randomUUID());
    await until(
      "cancelled and sealed",
      async () =>
        (
          await pool.query(
            "select 1 from pma_segments.turns where id=$1 and state='cancelled' and output_sealed_at is not null",
            [held.turnId],
          )
        ).rowCount,
    );
    console.log("Active text Stop sealed the original Turn.");
  } else {
    const token = "PMA-" + randomBytes(5).toString("hex");
    const [webPort, adminPort, unusedProviderPort] = await Promise.all([port(), port(), port()]);
    const web = url(webPort),
      webName = "pma-web-" + suffix;
    // Bind the staging ingress to loopback, and supply only web-facing settings.
    docker([
      "run",
      "-d",
      "--network",
      "host",
      "--name",
      webName,
      "-e",
      "PI_CLOUD_WEB_LISTEN=http://127.0.0.1:" + webPort,
      "-e",
      "PI_CLOUD_ADMIN_LISTEN=http://127.0.0.1:" + adminPort,
      "-e",
      "PI_CLOUD_PROVIDER_LISTEN=http://127.0.0.1:" + unusedProviderPort,
      "-e",
      "PI_CLOUD_CONTROL_PLANE_UPSTREAM=127.0.0.1:" + apiPort,
      "-e",
      "PI_CLOUD_ADMIN_ORIGIN_BASE_URL=" + web,
      "-e",
      "PI_CLOUD_PUBLIC_ORIGIN_BASE_URL=" + web,
      "pi-managed-agents/segment-web:" + tag,
    ]);
    names.push(webName);
    clean.push(() => docker(["rm", "-f", webName]));
    await until("web ingress", () =>
      fetch(web + "/healthz").then(
        (r) => r.ok,
        () => false,
      ),
    );
    let first;
    async function terminal(number) {
      const row = await until(
        "committed Turn " + number,
        async () =>
          (
            await pool.query(
              "select id,state,output_seal_id from pma_segments.turns where session_id=$1 and mailbox_position=$2 and output_seal_id is not null",
              [session, number],
            )
          ).rows[0],
        180000,
      );
      assert.equal(row.state, "completed", "Model Turn failed");
      return row.id;
    }
    let t1, t2;
    const placements = new Set();
    const seenTerminals = new Set(),
      seenDeltas = new Set();
    const observe = new AbortController(),
      snapshot = Promise.withResolvers();
    const observing = api.streamSession({
      sessionId: session,
      signal: observe.signal,
      onStatus() {},
      onSnapshot: () => snapshot.resolve(),
      onInputPlaced: (p) => placements.add(p.turnId),
      onEvent: (e) => {
        if (e.type === "turn.completed") seenTerminals.add(e.turnId);
        if (e.type === "assistant.text.delta") seenDeltas.add(e.turnId);
      },
    });
    observing.catch((e) => snapshot.reject(e));
    try {
      await until(
        "initial live snapshot",
        () => Promise.race([snapshot.promise.then(() => true), delay(200, false)]),
        15000,
      );
      await withChromePage({ profilePrefix: "pma-serving-" }, async (page) => {
        await page.navigate(web);
        const logged = await page.evaluate(
          `fetch('/v1/auth/login',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(${JSON.stringify({ username, password: pass })})}).then(r=>r.status)`,
        );
        assert(logged < 300, "Browser login failed");
        const link = web + "/?session=" + session;
        await page.navigate(link);
        const heading =
          "document.querySelector('.product-conversation-heading strong')?.textContent===" +
          JSON.stringify(title);
        try {
          await page.waitFor(heading, 15000);
        } catch {
          await page.evaluate(
            "[...document.querySelectorAll('.product-conversation-row button')].find(b=>b.querySelector('strong')?.textContent===" +
              JSON.stringify(title) +
              ")?.click()",
          );
          await page.waitFor(heading, 15000);
        }
        const posts = [];
        page.onNetworkEvent((method, m) => {
          if (
            method === "Network.responseReceived" &&
            new URL(m.response.url).pathname.endsWith("/turns")
          )
            posts.push({ path: new URL(m.response.url).pathname, status: m.response.status });
        });
        const hasCount = (num) =>
          `[...document.querySelectorAll('.product-assistant-content')].filter(e=>e.textContent.includes(${JSON.stringify(token)})).length>=${num}`;
        async function send(prompt) {
          const selector = "document.querySelector('.product-composer textarea')";
          await page.waitFor(selector + " && !" + selector + ".disabled");
          await page.evaluate(
            `{const a=${selector};Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype,'value').set.call(a,${JSON.stringify(prompt)});a.dispatchEvent(new Event('input',{bubbles:true}));}`,
          );
          await page.waitFor(
            "document.querySelector('.product-send-button') && !document.querySelector('.product-send-button').disabled",
          );
          const at = posts.length;
          await page.evaluate("document.querySelector('.product-send-button').click()");
          const ack = await until("browser input acknowledgement", () => posts[at], 12000);
          assert.equal(ack.path, "/v1/sessions/" + session + "/turns");
          assert(ack.status < 300, "Browser admission status " + ack.status);
        }
        await send("Reply with exactly this code: " + token);
        await page.waitFor(
          "document.querySelector('[data-receipt=delivered]') && !document.querySelector('[data-receipt=read]')",
          12000,
        );
        first = await startWorker(0);
        await page.waitFor("document.querySelector('[data-receipt=read]')", 45000);
        t1 = await terminal(1);
        await page.waitFor(hasCount(1));
        await page.navigate(link);
        await page.waitFor(hasCount(1));
        docker(["stop", "-t", "10", first]);
        await startWorker(1);
        await send("Repeat just the code from my previous message.");
        t2 = await terminal(2);
        await page.waitFor(hasCount(2));
        await page.navigate(link);
        await page.waitFor(hasCount(2));
        await page.waitFor(
          "document.querySelectorAll('[data-receipt=read]').length===1 && document.querySelector('[data-receipt=read]').closest('[data-conversation-turn-id]').dataset.conversationTurnId===" +
            JSON.stringify(t2),
        );
      });
      await until(
        "two live terminal events",
        () => seenTerminals.has(t1) && seenTerminals.has(t2),
        10000,
      );
      assert(
        placements.has(t1) && placements.has(t2),
        "Both Turns should publish canonical input placement",
      );
      assert(
        seenDeltas.has(t1) && seenDeltas.has(t2),
        "Expected streamed assistant text on both Turns",
      );
    } finally {
      observe.abort();
      await observing.catch((e) => {
        if (!observe.signal.aborted) throw e;
      });
    }
    for (const turnId of [t1, t2]) {
      const detail = await api.getConversation(session, { turnId });
      assert(
        JSON.stringify(
          detail.turns.flatMap((t) => t.transcript?.items.filter((i) => i.kind === "text") ?? []),
        ).includes(token),
        "Missing saved reply",
      );
    }
    const {
      rows: [n],
    } = await pool.query(
      "select count(distinct worker_id)::int n from public.session_leases where pi_session_id=$1 and released_at is not null",
      [session],
    );
    assert.equal(n.n, 2);
    console.log(
      JSON.stringify({
        result: "passed",
        turns: [t1, t2].length,
        workers: n.n,
        browserRefreshes: 2,
        sseTurnCompletions: seenTerminals.size,
        streamedTurns: seenDeltas.size,
        partition: kafkaProducerLane(session, partitions),
        elapsedMs: Date.now() - started,
      }),
    );
  }
} catch (error) {
  for (const name of names) {
    try {
      const log = spawnSync("docker", ["logs", "--tail", "25", name], { encoding: "utf8" });
      const lines = log.stdout + "\n" + log.stderr;
      console.error(
        name,
        lines
          .split("\n")
          .filter((l) => !l.startsWith("{"))
          .slice(-12)
          .join("\n")
          .slice(0, 1100),
      );
    } catch {}
  }
  throw error;
} finally {
  for (const close of clean.reverse()) await close();
}
