// Own the catalog topic as well as the database. This check never uses an operator's brokers.
import assert from "node:assert/strict";
import { ControlToolRegistry } from "../../../packages/control-plane/src/control-tool-registry.ts";
import { Type } from "typebox";
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
  let releaseTool;
  const registry = new ControlToolRegistry();
  const version = process.argv[3];
  if (!["delisted", "short-grace"].includes(version)) {
    const field = version === "pdf" ? "title" : "sheet";
    registry.register({
      definition: {
        name: "export_report",
        description: "Export the report",
        parameters: Type.Object({ [field]: Type.String() }, { additionalProperties: false }),
        environment: { kind: "none" },
      },
      replay: "never",
      execute: async (call) => {
        if (call.arguments[field] === "accepted-before-disable") {
          const held = Promise.withResolvers();
          releaseTool = held.resolve;
          process.send({ kind: "accepted" });
          await held.promise;
        }
        return {
          result: { content: [{ type: "text", text: version + ":" + call.arguments[field] }] },
        };
      },
    });
  }
  const host = await startToolHost({
    mcpTurnAdmittedByProjector: true,
    ...(version === "standard" ? {} : { ordinaryTools: registry }),
    versionRoute: (origin) => origin,
    ...(version === "short-grace" ? { graceMs: 150 } : {}),
  });
  process.on("message", async (message) => {
    if (message.op === "release-tool") {
      releaseTool();
      process.send({ kind: "released-tool" });
      return;
    }
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
    const hosts = new Map();
    async function launchHost(revision) {
      const host = owned(
        processActor(fileURLToPath(import.meta.url), ["host", revision], {
          DATABASE_URL: dbFixture.url,
          PI_CLOUD_ALLOW_INLINE_SECRETS: "true",
          PI_CLOUD_KAFKA_BROKERS: brokers.join(","),
          PI_CLOUD_TOOL_RELEASE_ID: revision,
          PI_CLOUD_IMAGE_REVISION: "fixture",
          PI_CLOUD_TOOL_HOST_PORT: "0",
          PI_CLOUD_SUPERVISOR_MANAGEMENT_TOKEN: secret,
          PI_CLOUD_SANDBOX_CONTROL_TOKEN_FILE: join(tmp, "resource"),
          PI_CLOUD_MCP_CREDENTIAL_MASTER_KEY: key,
          HOST: "127.0.0.1",
          NO_PROXY: "127.0.0.1,localhost",
        }),
      );
      const url = (await host.expect("ready")).url;
      hosts.set(url, host);
      return url;
    }
    const url = await launchHost("pdf");
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
    const route = owned(await rt.connectToolHost({ url, token: secret, brokers, versioned: true }));
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
    let queued;
    rt.consume = async (record, ...args) => {
      if (queued && record.fact.tools?.some((r) => r.toolName === "export_report")) {
        const gate = queued;
        queued = undefined;
        gate.enter.resolve();
        await gate.release.promise;
      }
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
        versionedTools: true,
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
    await childWorker.expect("released", { op: "release" });
    async function adminCall(base, path, body) {
      const response = await fetch(new URL("/internal/v1/tools/" + path, base), {
        method: body ? "POST" : "GET",
        headers: { authorization: "Bearer " + secret, "content-type": "application/json" },
        ...(body ? { body: JSON.stringify(body) } : {}),
      });
      assert(response.ok, await response.clone().text());
      return response.json();
    }
    async function activate(base) {
      const manifest = await adminCall(base, "manifest");
      const result = await adminCall(base, "activate", {
        expectedGeneration: manifest.selected.generation,
        revision: manifest.catalog.revision,
        releaseId: manifest.releaseId,
      });
      await synced(result.generation);
      return result;
    }
    async function synced(generation) {
      await until(
        "Projector platform generation",
        () => route.catalog.snapshot?.generation === generation,
      );
      await until(
        "Worker platform generation",
        async () =>
          (await childWorker.expect("catalog", { op: "catalog", scope: "platform" })).generation ===
          generation,
      );
    }
    async function setDisabled(base, disabled) {
      const result = await adminCall(base, "availability", {
        name: "export_report",
        disabled,
        expectedGeneration: route.catalog.snapshot.generation,
      });
      await synced(result.generation);
    }
    async function round(options = {}, change) {
      await input();
      const work = await claim(childWorker);
      const accepted = options.acceptedAt
        ? hosts.get(options.acceptedAt).expect("accepted")
        : undefined;
      const entry = options.pauseResponse ? childWorker.expect("sampling") : undefined;
      const gate = options.queue
        ? { enter: Promise.withResolvers(), release: Promise.withResolvers() }
        : undefined;
      if (gate) queued = gate;
      const done = childWorker.expect("finished", {
        op: "execute",
        hostCall: "export_report",
        ...options,
      });
      void done.catch(() => {});
      try {
        if (entry) await entry;
        if (gate) await gate.enter.promise;
        if (accepted) await accepted;
        if (change) await change();
      } finally {
        if (entry) await childWorker.expect("continued", { op: "continue-model" });
        gate?.release.resolve();
        if (accepted)
          await hosts.get(options.acceptedAt).expect("released-tool", { op: "release-tool" });
      }
      const { hostOffers } = await done;
      await settled(work.runId);
      const [result] = await results(work.runId);
      assert(result);
      await childWorker.expect("released", { op: "release" });
      return { offers: hostOffers, text: result.content[0].text };
    }
    const xlsx = await launchHost("xlsx");
    assert.deepEqual(await round({ pauseResponse: true }, () => activate(xlsx)), {
      offers: ["title", "sheet"],
      text: "pdf:report",
    });
    const delisted = await launchHost("delisted");
    assert.deepEqual(await round({ queue: true }, () => activate(delisted)), {
      offers: ["sheet", null],
      text: "xlsx:report",
    });
    await activate(xlsx);
    const refused = await round({ queue: true }, () => setDisabled(xlsx, true));
    assert.deepEqual(refused.offers, ["sheet", null]);
    assert.match(refused.text, /tool_disabled.*not executed/);
    await setDisabled(xlsx, false);
    assert.deepEqual(await round(), { offers: ["sheet", "sheet"], text: "xlsx:report" });
    const delayed = await round({ queue: true }, async () => {
      await setDisabled(xlsx, true);
      await setDisabled(xlsx, false);
    });
    assert.match(delayed.text, /tool_disabled/);
    const running = await round({ acceptedAt: xlsx, text: "accepted-before-disable" }, () =>
      setDisabled(xlsx, true),
    );
    assert.deepEqual(running, { offers: ["sheet", null], text: "xlsx:accepted-before-disable" });
    await setDisabled(xlsx, false);
    const short = await launchHost("short-grace");
    const expired = await round({ queue: true }, async () => {
      const old = route.catalog.snapshot;
      await activate(short);
      await until(
        "old route expiry",
        async () =>
          (
            await pool.query(
              "select expires_at<=clock_timestamp() as expired from tool_host_versions where generation=$1",
              [old.generation],
            )
          ).rows[0]?.expired,
      );
    });
    assert.match(expired.text, /tool_version_expired/);
    assert.deepEqual(expired.offers, ["sheet", null]);
    // A provider request restarted on another Worker keeps its frozen catalog even if the reply had not arrived yet.
    await activate(xlsx);
    await input();
    const crashedModel = await claim(childWorker);
    const sampling = childWorker.expect("sampling");
    const orphan = childWorker.expect("finished", {
      op: "execute",
      hostCall: "export_report",
      pauseResponse: true,
    });
    void orphan.catch(() => {});
    await sampling;
    await activate(url);
    await childWorker.close();
    await until(
      "sampling handoff",
      async () =>
        (
          await pool.query(
            "select 1 from turns where id=$1 and state='queued' and native_work_handoff is not null",
            [crashedModel.runId],
          )
        ).rowCount,
    );
    const freshWorker = await worker();
    const retry = await claim(freshWorker);
    assert.equal(retry.runId, crashedModel.runId);
    const answer = await freshWorker.expect("finished", {
      op: "execute",
      hostCall: "export_report",
    });
    assert.deepEqual(answer.hostOffers, ["sheet", "title"]);
    await settled(retry.runId);
    assert.equal((await results(retry.runId))[0].content[0].text, "xlsx:report");
    await freshWorker.expect("released", { op: "release" });

    const standard = await launchHost("standard");
    const manifest = await adminCall(standard, "manifest");
    const active = await adminCall(standard, "activate", {
      expectedGeneration: manifest.selected.generation,
      revision: manifest.catalog.revision,
      releaseId: manifest.releaseId,
    });
    await until(
      "standard Worker view",
      async () =>
        (await freshWorker.expect("catalog", { op: "catalog", scope: "platform" })).generation ===
        active.generation,
    );
    await input();
    const standardRun = await claim(freshWorker);
    await freshWorker.expect("finished", {
      op: "execute",
      hostCall: "schedule",
      hostArguments: { action: "list" },
    });
    await settled(standardRun.runId);
    const scheduleResult = (await results(standardRun.runId))[0];
    assert.equal(scheduleResult.toolName, "schedule");
    assert(!scheduleResult.isError);
    assert.deepEqual(JSON.parse(scheduleResult.content[0].text).schedules, []);
    console.log(
      "Versioned Tool host acceptance passed: old request schema, original executable, post-delisting delivery, disable refusal and re-enable",
    );
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
