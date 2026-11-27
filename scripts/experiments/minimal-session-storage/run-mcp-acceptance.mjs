// Own the catalog topic as well as the database. This check never uses an operator's brokers.
import assert from "node:assert/strict";
import { randomBytes, randomUUID } from "node:crypto";
import { fork } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";
import { once } from "node:events";
import pg from "pg";
import nativeKafka from "@confluentinc/kafka-javascript";
import { createDatabase, runMigrations } from "@pi-cloud/database";
import { KafkaWorkerProducer } from "@pi-cloud/event-log";
import { testKafka } from "../../lib/test-kafka.mjs";
import { testPostgres } from "../../lib/test-postgres.mjs";
import { startToolHost } from "../../../packages/control-plane/src/tool-host-main.ts";
import {
  createPrivateTenant,
  ControlPlaneStore,
} from "../../../packages/control-plane/src/index.ts";
import { McpConnectionService } from "../../../packages/control-plane/src/mcp/connection-service.ts";
import { McpCredentialVault } from "../../../packages/control-plane/src/mcp/credential-vault.ts";
import { httpMcpFixture } from "../../../packages/control-plane/test/mcp/http-fixture.ts";
import { registerTestWorker } from "../../../packages/control-plane/test/fixtures/registered-worker.ts";
import { IndependentSessionRuntime } from "./independent-session-runtime.mjs";
import { installPagingProjection } from "./flow-paging-fixture.mjs";
import { installForkTables, setForkLayout } from "./fork-storage.mjs";
import { installIndependentSessionLeases } from "./independent-session-leases.mjs";
import { installWorkspaceRoutes } from "./workspace-route-spans.mjs";
import { encodeWorkspaceFact } from "./workspace-operations.mjs";

if (process.argv[2] === "host") {
  const host = await startToolHost({ mcpTurnAdmittedByProjector: true });
  process.once("message", async () => {
    await host.close();
    process.disconnect();
  });
  process.send({ kind: "ready", url: "http://127.0.0.1:" + host.app.server.address().port });
} else await run();

function processActor(path, args, environment) {
  const child = fork(path, args, {
    execArgv: ["--import", "tsx"],
    env: {
      PATH: process.env.PATH,
      HOME: process.env.HOME,
      TSX_TSCONFIG_PATH: resolve("packages/control-plane/tsconfig.json"),
      ...environment,
    },
    stdio: ["ignore", "inherit", "inherit", "ipc"],
  });
  const ended = once(child, "exit");
  function expect(kind, request) {
    const signal = AbortSignal.timeout(40000);
    return new Promise((resolve, reject) => {
      function cleanup() {
        child.off("message", received);
        child.off("exit", exit);
        signal.removeEventListener("abort", timeout);
      }
      function received(v) {
        if (v.kind !== kind && v.kind !== "error") return;
        cleanup();
        v.kind === "error" ? reject(Error(v.message)) : resolve(v);
      }
      function exit() {
        cleanup();
        reject(Error("Process exited waiting for " + kind));
      }
      function timeout() {
        cleanup();
        reject(Error("Deadline waiting for " + kind));
      }
      child.on("message", received);
      child.once("exit", exit);
      signal.addEventListener("abort", timeout, { once: true });
      if (request) child.send(request);
    });
  }
  return {
    expect,
    async close() {
      if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      await ended;
    },
  };
}

async function run() {
  const clean = [];
  let failure;
  const owned = (resource, close) => {
    clean.push(close ?? (() => resource.close()));
    return resource;
  };
  async function until(label, read) {
    const end = Date.now() + 30000;
    do {
      if (failure) throw failure;
      const v = await read();
      if (v) return v;
      await delay(35);
    } while (Date.now() < end);
    throw Error("Deadline waiting for " + label);
  }
  try {
    const dbFixture = owned(await testPostgres({})),
      cluster = owned(await testKafka({})),
      brokers = cluster.brokers;
    const db = createDatabase({ connectionString: dbFixture.url });
    owned(db, () => db.destroy());
    await runMigrations(db, "up");
    const identity = await createPrivateTenant(db, {
        slug: "mcp-study",
        ownerDisplayName: "MCP test",
      }),
      t = identity.tenantId,
      user = identity.ownerUserId,
      actor = { tenantId: t, userId: user, role: "owner" };
    const control = new ControlPlaneStore({ database: db, ...identity });
    const project = await control.createProject({ name: "MCP", source: { kind: "empty" } });
    const { sessionId: s } = await control.createSession(
      project.projectId,
      project.workspaceId,
      "MCP",
      "elastic",
      { ownerUserId: user },
    );
    const schema = "segment_mcp",
      dsn = new URL(dbFixture.url);
    dsn.searchParams.set("options", "-c search_path=" + schema + ",public");
    const pool = new pg.Pool({ connectionString: dsn.href }),
      topic = "mcp-segment-" + randomUUID();
    const rt = new IndependentSessionRuntime({
      pool,
      topic,
      partitions: 1,
      selectSandbox: () => assert.fail("MCP allocated a VM"),
      resolveSandbox: () => assert.fail("MCP used a Guest"),
    });
    owned(rt, () => rt.store.db.destroy());
    await pool.query("create schema " + schema);
    await installPagingProjection(pool);
    await installForkTables(pool);
    await setForkLayout(pool, true, false);
    await installWorkspaceRoutes(pool);
    await installIndependentSessionLeases(pool);
    await rt.store.register(t, [s]);
    const admin = new nativeKafka.KafkaJS.Kafka({
      kafkaJS: { brokers, logLevel: nativeKafka.KafkaJS.logLevel.NOTHING },
    }).admin();
    await admin.connect();
    owned(admin, () => admin.disconnect());
    await admin.createTopics({
      topics: [
        {
          topic,
          numPartitions: 1,
          replicationFactor: 3,
          configEntries: [{ name: "min.insync.replicas", value: "2" }],
        },
      ],
    });
    const tmp = await mkdtemp(join(tmpdir(), "pma-mcp-"));
    owned(tmp, () => rm(tmp, { recursive: true, force: true }));
    const secret = randomBytes(32).toString("hex"),
      key = randomBytes(32).toString("base64url");
    await writeFile(join(tmp, "resource"), secret, { mode: 0o600 });
    const host = owned(
      processActor(fileURLToPath(import.meta.url), ["host"], {
        DATABASE_URL: dbFixture.url,
        PI_CLOUD_ALLOW_INLINE_SECRETS: "true",
        PI_CLOUD_KAFKA_BROKERS: brokers.join(","),
        PI_CLOUD_TOOL_RELEASE_ID: "segment-test",
        PI_CLOUD_IMAGE_REVISION: "fixture",
        PI_CLOUD_TOOL_HOST_PORT: "0",
        PI_CLOUD_SUPERVISOR_MANAGEMENT_TOKEN: secret,
        PI_CLOUD_SANDBOX_CONTROL_TOKEN_FILE: join(tmp, "resource"),
        PI_CLOUD_MCP_CREDENTIAL_MASTER_KEY: key,
        HOST: "127.0.0.1",
        NO_PROXY: "127.0.0.1,localhost",
      }),
    );
    const { url } = await host.expect("ready");
    const remote = owned(await httpMcpFixture()),
      echo = remote.state.tools[0],
      hold = { ...echo, name: "hold" };
    remote.state.tools.push(hold);
    const service = new McpConnectionService(db, new McpCredentialVault(key)),
      id = randomUUID();
    await service.save(actor, id, {
      expectedRevision: null,
      name: "example",
      endpoint: remote.url,
      visibility: "personal",
      enabled: true,
      allowedTools: null,
      credential: { kind: "bearer", token: "fixture-token" },
    });
    const route = owned(await rt.connectMcp({ url, token: secret, brokers }));
    const scope = "mcp:" + id;
    // Public names incorporate the connection label and a digest of the remote name.
    const names = async () =>
      Object.fromEntries(
        (await service.read(actor, id)).tools.map((tool) => [tool.name, tool.platformName]),
      );
    await until("initial directory", () => route.catalog.snapshotFor(scope)?.mcp?.enabled);
    let tools = await names();
    assert(tools.hold && tools.echo);
    const traces = [],
      consume = rt.consume.bind(rt);
    rt.consume = (record, ...args) => {
      if (record.fact.tools?.some((r) => r.implementation?.catalogScope)) traces.push(record);
      return consume(record, ...args);
    };
    const reader = owned(
      rt.reader({
        brokers,
        clientId: topic,
        groupId: topic,
        onError: (e) => {
          failure = e;
        },
      }),
    );
    await reader.start();
    await reader.waitForPartition(0);
    const producer = owned(new KafkaWorkerProducer({ brokers, topic, clientId: "seal-" + topic }));
    await producer.start();
    const stop = new AbortController(),
      reconcile = rt.leases.reconcile({
        intervalMs: 100,
        signal: stop.signal,
        publish: (partition, f) =>
          producer.append(partition, {
            sessionId: f.scope.piSessionId,
            factId: f.factId,
            value: encodeWorkspaceFact(f),
          }),
      });
    reconcile.catch((e) => {
      failure = e;
    });
    owned(stop, async () => {
      stop.abort();
      await reconcile;
    });
    async function worker() {
      const w = randomUUID();
      await db
        .insertInto("workers")
        .values({ id: w, boot_id: w, supervisor_id: w, max_concurrent_sessions: 1, state: "ready" })
        .execute();
      await registerTestWorker(db, w);
      const proc = owned(
        processActor(
          fileURLToPath(new URL("./independent-session-worker.mjs", import.meta.url)),
          [],
          {
            PI_CLOUD_KAFKA_INTEGRATION_BROKERS: brokers.join(","),
            PI_CLOUD_WORKER_HISTORY_CACHE_ROOT: tmp,
          },
        ),
      );
      await proc.expect("ready", {
        url: dsn.href,
        workerId: w,
        bootId: w,
        capacity: 1,
        leaseDurationMs: 4500,
        topic,
        brokers,
        mcp: true,
      });
      return proc;
    }
    const claim = async (proc) => {
      const j = await proc.expect("claim", { op: "claim" });
      assert(j.scope);
      return j.scope;
    };
    async function refresh(proc, definitions) {
      const prior = route.catalog.snapshotFor(scope).generation;
      remote.state.tools = definitions;
      remote.change();
      const revision = await until("tools/list_changed publication", () => {
        const v = route.catalog.snapshotFor(scope);
        return v.generation > prior && v.generation;
      });
      await until(
        "Worker directory",
        async () => (await proc.expect("catalog", { op: "catalog", scope })).generation >= revision,
      );
      return names();
    }
    const input = () =>
      rt.inputs.admit({
        tenant: t,
        session: s,
        id: randomUUID(),
        sender: user,
        source: "user",
        mode: "input",
        text: "Use the connected service",
      });
    const settled = (turn) =>
      until(
        "projected completion",
        async () =>
          (await pool.query("select 1 from turns where id=$1 and state='completed'", [turn]))
            .rowCount,
      );
    const a = await worker();
    await input();
    const j = await claim(a);
    const finish = a.expect("finished", { op: "execute", mcpCall: tools.hold, text: "round one" });
    void finish.catch(() => {});
    await until("remote admission", () => remote.state.calls.includes("hold"));
    const child = await rt.store.delegate(
      t,
      s,
      null,
      randomUUID(),
      "review",
      "Check tools",
      "fixture",
      { run: j.runId, writer: j.writerId },
    );
    const childWorker = await worker();
    assert.equal((await claim(childWorker)).sessionId, child);
    const childResult = await childWorker.expect("finished", { op: "execute", observeMcp: true });
    assert.deepEqual(childResult.mcpOffers, [[]]);
    await settled(childResult.turnId);
    await childWorker.expect("released", { op: "release" });
    const updated = await refresh(a, [{ ...echo, name: "lookup" }]);
    remote.release("FROM_REMOTE");
    const end = await finish;
    assert.deepEqual(new Set(end.mcpOffers[0]), new Set([tools.echo, tools.hold]));
    assert.deepEqual(end.mcpOffers[1], [updated.lookup]);
    await settled(j.runId);
    const results = async (turn) =>
      (
        await pool.query(
          "select content::jsonb#>'{model,0}' msg from messages where run_id=$1 and role='toolResult'",
          [turn],
        )
      ).rows.map((r) => r.msg);
    assert(
      (await results(j.runId)).some(
        (m) => m.toolName === tools.hold && m.content[0].text === "FROM_REMOTE",
      ),
    );
    await a.expect("released", { op: "release" });
    // A lost remote POST response must settle as unknown without repeating the write.
    const changed = await refresh(a, [hold, { ...echo, name: "lost_write" }]);
    await input();
    const lost = await claim(a);
    await a.expect("finished", { op: "execute", mcpCall: changed.lost_write, text: "write once" });
    await settled(lost.runId);
    assert.equal(remote.state.writes, 1);
    assert(
      (await results(lost.runId)).some(
        (m) => m.isError && JSON.stringify(m.content).includes("unknown"),
      ),
    );
    const stale = traces.find((r) => r.fact.scope.runId === lost.runId);
    assert(stale);
    await consume(
      stale,
      () => true,
      async () => assert.fail("A retired owner requested dispatch"),
    );
    assert.equal(remote.state.writes, 1);
    await a.expect("released", { op: "release" });
    // Kill the waiting Worker, withdraw its only tool and attach the replacement to the original reply.
    await input();
    const crashed = await claim(a);
    const interrupted = a.expect("finished", {
      op: "execute",
      mcpCall: changed.hold,
      text: "survive handoff",
    });
    void interrupted.catch(() => {});
    await until(
      "second remote admission",
      () => remote.state.calls.filter((n) => n === "hold").length === 2,
    );
    await a.close();
    remote.state.tools = [echo];
    remote.change();
    await until("withdrawal", () => !route.catalog.publicationFor(changed.hold));
    await until(
      "handoff seal",
      async () =>
        (
          await pool.query(
            "select 1 from turns where id=$1 and state='queued' and native_work_handoff is not null",
            [crashed.runId],
          )
        ).rowCount,
    );
    const replacement = await claim(childWorker);
    assert.equal(replacement.runId, crashed.runId);
    assert(replacement.fencingToken > crashed.fencingToken);
    remote.release("ORIGINAL_AFTER_HANDOFF");
    const resumed = await childWorker.expect("finished", {
      op: "execute",
      mcpCall: changed.hold,
      text: "survive handoff",
    });
    assert(resumed.resumed);
    await settled(crashed.runId);
    assert(
      (await results(crashed.runId)).some((m) => m.content[0].text === "ORIGINAL_AFTER_HANDOFF"),
    );
    assert.deepEqual(remote.state.calls, ["hold", "lost_write", "hold"]);
    console.log(
      "HTTP MCP acceptance passed: scoped catalog, live add/remove, canonical replies, single external write, Worker handoff",
    );
  } finally {
    const errors = [];
    for (const close of clean.reverse())
      try {
        await close();
      } catch (e) {
        errors.push(e);
      }
    if (errors.length) throw new AggregateError(errors, "Fixture cleanup failed");
  }
}
