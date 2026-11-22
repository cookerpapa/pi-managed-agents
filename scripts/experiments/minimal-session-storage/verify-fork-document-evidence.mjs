// Readiness diagnostic: can the retained PG data distinguish two valid native
// Forks after the Session log has rotated? All DDL belongs to disposable schemas.
import assert from "node:assert/strict";
import { isDeepStrictEqual } from "node:util";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import pg from "pg";
import { sql } from "kysely";
import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import {
  AgentDoc,
  MemoryStorage,
  Harness,
  createRegistry,
  defineExtension,
} from "@earendil-works/pi-durable";
import { createModels } from "@earendil-works/pi-ai/models";
import { fauxProvider, fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { PiSessionWorldStateController } from "../../../packages/sandbox-supervisor/src/pi-sandbox-continuity.ts";
import { fixedRemoteToolDefinitions } from "../../../packages/sandbox-supervisor/src/fixed-remote-tool-definitions.ts";
import { FamilyPresentation } from "./family-presentation.mjs";
import { normalRootProjection } from "./normal-root-projection.mjs";
import { installWorkspaceRoutes } from "./workspace-route-spans.mjs";
import { ForkStorage, setForkLayout } from "./fork-storage.mjs";
import { restoreForkPrefix } from "./session-prefix.mjs";

const now = "2026-11-22T00:00:00.000Z";
class RecordingStorage extends MemoryStorage {
  batches = [];
  async commit(writes, context) {
    const seq = await super.commit(writes, context);
    this.batches.push({ seq, nextId: this.nextId, writes: structuredClone(writes) });
    return seq;
  }
}
const currentEnvironment = {
  continuity: "warm_reuse",
  continuityId: "fixture-generation-one",
  environmentSha256: "a".repeat(64),
  workspaceBindingSha256: "b".repeat(64),
  toolPolicySha256: "c".repeat(64),
};
async function nextEnvironmentMessages(h, id) {
  const controller = await PiSessionWorldStateController.create(h, id, currentEnvironment);
  return (await controller.capture()).modelMessages.map((m) => m.customType);
}
async function nativeHistory(extra, lastGeneration) {
  const store = new RecordingStorage(),
    models = createModels(),
    registry = createRegistry();
  const tools = fixedRemoteToolDefinitions()
    .filter((t) => ["bash", "read", "edit"].includes(t.name))
    .map((t) => ({
      ...t,
      execute() {
        throw Error("Unexpected tool execution");
      },
    }));
  const select = (names) =>
    names.map((n) => {
      const tool = tools.find((t) => t.name === n);
      assert(tool);
      return tool;
    });
  registry.install(defineExtension({ name: "fork-evidence-tools", tools }));
  const provider = fauxProvider({
    api: "fork-study-api",
    provider: "fork-study",
    tokensPerSecond: 0,
  });
  models.setProvider(provider.provider);
  const started = Promise.withResolvers(),
    finish = Promise.withResolvers(),
    timestamp = Date.parse(now);
  const suppliedTools = [],
    providerInputs = [];
  function answer(context, text) {
    providerInputs.push(structuredClone(context.messages));
    const systems = context.messages.filter((m) => m.role === "system");
    suppliedTools.push(systems.flatMap((m) => (m.toolsAdded ?? []).map((t) => t.name)));
    return fauxAssistantMessage(text, { timestamp });
  }
  provider.setResponses([
    async (context) => {
      started.resolve();
      await finish.promise;
      return answer(context, "A");
    },
    (context) => answer(context, "B"),
  ]);
  const h = await Harness.open(
    store,
    {
      models,
      registry,
      now: () => timestamp,
      settings: { retry: { maxRetries: 0 }, compaction: { enabled: false } },
    },
    ctx,
  );
  try {
    const model = provider.getModel();
    const root = await h.root(ctx, {
      agent: { model: { provider: model.provider, modelId: model.id }, tools: select(["bash"]) },
    });
    let world;
    if (lastGeneration) {
      world = await PiSessionWorldStateController.create(h, root.id, {
        ...currentEnvironment,
        continuity: "cold_restore",
      });
      await world.withInitialObservation((write) => root.commit(write, ctx));
    }
    const first = await root.submit(
      { type: "input", requestId: "request-1", content: "First input" },
      ctx,
    );
    await started.promise;
    // This uses the same public configuration call as the serving tool-catalog subscriber.
    // The provider is already processing its old request while the change is committed.
    await root.configure({ tools: select(["bash", extra]) }, ctx);
    finish.resolve();
    await root.waitForIdle(ctx);
    assert.equal((await first.wait(ctx)).status, "done");
    const anchor = store.batches
      .flatMap((b) => b.writes)
      .find((w) => w.type === "entry" && w.value.kind === "pi.assistant").value.id;
    await root.configure({ tools: select(["bash"]) }, ctx);
    const second = await root.submit(
      { type: "input", requestId: "request-2", content: "Second input" },
      ctx,
    );
    await root.waitForIdle(ctx);
    assert.equal((await second.wait(ctx)).status, "done");
    if (world) {
      // The first confirmed activation updates the observed state without adding a model message.
      await world.recordActive({ continuity: "warm_reuse", continuityId: lastGeneration });
      assert.deepEqual((await world.capture()).modelMessages, []);
    }
    const batches = store.batches.slice(),
      latest = await h.snapshot(AgentDoc, root.id, ctx);
    const branch = await root.fork(anchor, { ownership: { kind: "ownerless" } }, ctx);
    const forked = await h.snapshot(AgentDoc, branch.id, ctx);
    assert.deepEqual(forked.tools, ["bash", extra]);
    assert.deepEqual(latest.tools, ["bash"]);
    assert.equal(suppliedTools.length, 2);
    return {
      batches,
      anchor,
      latest: latest.tools,
      forked: forked.tools,
      forkDocument: forked,
      ...(world ? { nativeNotices: await nextEnvironmentMessages(h, branch.id) } : {}),
      suppliedTools,
      providerInputs,
    };
  } finally {
    finish.resolve();
    await h.close(ctx);
  }
}

async function pgHistory(base, admin, scope, trace, ids) {
  const schema = "fork_evidence_" + randomUUID().replaceAll("-", "");
  await admin.query("create schema " + schema);
  const pool = new pg.Pool({ connectionString: base, options: "-c search_path=" + schema, max: 3 });
  const forks = new ForkStorage(pool),
    topic = "fork-evidence";
  try {
    await pool.query(await readFile(new URL("./schema.sql", import.meta.url), "utf8"));
    // Match control columns supplied by the serving Run table; deterministic
    // timestamps keep incidental wall-clock differences out of the comparison.
    await pool.query(`alter table runs add output_first_topic text,add output_first_partition int,add output_first_offset bigint,
      add output_display_seq bigint default 0,add output_display_native_seq bigint default 0,add output_seal_offset bigint;
      alter table messages alter created_at set default timestamptz '2026-11-22 00:00:00+00'`);
    const { nativeStore, view } = normalRootProjection(
      pool,
      topic,
      () => assert.fail("unexpected tool"),
      () => assert.fail("unexpected tool"),
    );
    await view.install();
    await view.reset(new Map([[0, 0n]]), 16, "recorded-history");
    await pool.query(await readFile(new URL("./fork-layout.sql", import.meta.url), "utf8"));
    await setForkLayout(pool, true, false);
    await installWorkspaceRoutes(pool);
    await forks.register(scope.tenantId, [scope.piSessionId]);
    await pool.query(
      `insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,lease_id,created_at,started_at)
      values($1,$2,$3,1,$1,'same input','same','running',$4,$5,$5)`,
      [scope.runId, scope.tenantId, scope.piSessionId, scope.writerId, now],
    );
    const presentation = new FamilyPresentation();
    presentation.bind(1, scope);
    for (const [i, commit] of trace.batches.entries()) {
      const shown = presentation.prepare(commit);
      const fact = {
        kind: "durable_commit",
        scope,
        commit,
        factId: ids[i],
        occurredAt: now,
        display: shown.display,
        cancellations: [],
        tools: [],
        origins: commit.writes
          .filter((w) => ["entry", "submission", "task"].includes(w.type))
          .map((w) => ({
            recordId: w.value.id,
            sessionId: scope.sessionId,
            turnId: scope.turnId,
            runId: scope.runId,
          })),
      };
      const applied = await nativeStore.projection.project(
        fact,
        { topic, partition: 0, offset: BigInt(i) },
        () => true,
      );
      assert(applied.admitted);
      shown.confirmed();
    }
    const offset = BigInt(trace.batches.length),
      last = trace.batches.at(-1).seq;
    const position = { topic, partition: 0, offset },
      terminal = {
        scope,
        kind: "work_finished",
        terminal: { type: "turn.completed" },
        factId: ids.at(-1),
        nativeSequence: last,
      };
    await forks.db.transaction().execute(async (tx) => {
      await sql`update runs set state='completed',settled_at=${now}::timestamptz,output_seal_offset=${String(offset)}::bigint where id=${scope.runId}::uuid`.execute(
        tx,
      );
      await nativeStore.settle(tx, terminal, position);
      await sql`update accepted_fact_projection_offsets set next_offset=${String(offset + 1n)}::bigint where topic=${topic} and partition=0`.execute(
        tx,
      );
      await sql`insert into fork_completed_runs values(${scope.tenantId}::uuid,${scope.piSessionId}::uuid,${scope.runId}::uuid)`.execute(
        tx,
      );
    });
    nativeStore.projection.releaseSettled(scope);
    assert.equal(view.sessions.size, 0);
    const { rows: tables } = await pool.query(
      "select tablename from pg_tables where schemaname=current_schema() order by tablename",
    );
    const retained = {};
    for (const { tablename } of tables) {
      assert(/^[a-z_]+$/.test(tablename));
      retained[tablename] = (
        await pool.query(
          "select to_jsonb(t) as row from " + tablename + " t order by to_jsonb(t)::text",
        )
      ).rows.map((r) => r.row);
    }
    const dest = await forks.fork(scope.tenantId, scope.piSessionId, {
      lane: 1,
      run: scope.runId,
      entry: trace.anchor,
    });
    const page = await forks.page(scope.tenantId, dest);
    const expected = trace.batches
      .flatMap((b) => b.writes)
      .filter((w) => w.type === "entry" && w.value.id <= trace.anchor)
      .map((w) => w.value)
      .sort((a, b) => b.id - a.id);
    assert.deepEqual(
      page.map((r) => JSON.parse(r.content)),
      expected,
    );
    assert(expected.some((e) => e.kind === "pi.system"));
    assert.equal(Number(page[0].entry_id), trace.anchor);
    const point = { lane: 1, run: scope.runId, entry: trace.anchor },
      forkKey = randomUUID();
    const lastAnswer = trace.batches
      .flatMap((b) => b.writes)
      .findLast((w) => w.type === "entry" && w.value.kind === "pi.assistant").value.id;
    const later = await forks.fork(scope.tenantId, scope.piSessionId, {
      ...point,
      entry: lastAnswer,
    });
    assert.deepEqual(
      (await forks.agentConfiguration(scope.tenantId, later)).value.tools,
      trace.latest,
    );
    const repeated = await forks.fork(scope.tenantId, scope.piSessionId, point, forkKey);
    assert.equal(await forks.fork(scope.tenantId, scope.piSessionId, point, forkKey), repeated);
    await forks.retire(scope.tenantId, repeated);
    await forks.retire(scope.tenantId, scope.piSessionId, point);
    await forks.collect(scope.tenantId);
    assert.deepEqual(
      (await forks.agentConfiguration(scope.tenantId, scope.piSessionId)).value,
      trace.forkDocument,
    );
    await forks.retire(scope.tenantId, scope.piSessionId);
    await forks.collect(scope.tenantId);
    const nested = await forks.fork(scope.tenantId, dest, point);
    await forks.retire(scope.tenantId, dest);
    await forks.collect(scope.tenantId);
    const config = await forks.agentConfiguration(scope.tenantId, nested),
      visible = await forks.page(scope.tenantId, nested);
    assert.deepEqual(visible, page);
    assert.deepEqual(config.value, trace.forkDocument);
    const memory = new MemoryStorage(),
      records = visible
        .toReversed()
        .map((r) => ({ entry: JSON.parse(r.content), seq: Number(r.commit_seq) }));
    restoreForkPrefix(memory, records, config);
    const cold = await Harness.open(
      memory,
      { models: createModels(), registry: createRegistry() },
      ctx,
    );
    let coldState, coldNotices;
    try {
      coldState = await cold.snapshot(AgentDoc, 1, ctx);
      assert.deepEqual(coldState, trace.forkDocument);
      if (trace.nativeNotices) coldNotices = await nextEnvironmentMessages(cold, 1);
    } finally {
      await cold.close(ctx);
    }
    await forks.retire(scope.tenantId, nested);
    await forks.collect(scope.tenantId);
    // Only the branch at B survives now. Its base points to the later version,
    // but its visible A must still be forkable with the earlier configuration.
    const older = await forks.fork(scope.tenantId, later, point);
    assert.deepEqual(
      (await forks.agentConfiguration(scope.tenantId, older)).value,
      trace.forkDocument,
    );
    await forks.retire(scope.tenantId, later);
    await forks.collect(scope.tenantId);
    // A first locally authored Entry can precede any local configuration change.
    const run = randomUUID(),
      writer = randomUUID(),
      own = trace.anchor + 100;
    await pool.query(
      "insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,lease_id) values($1,$2,$3,1,$1,'local','same','running',$4)",
      [run, scope.tenantId, older, writer],
    );
    const local = {
      id: own,
      conversation: 1,
      ordinal: 0,
      role: "assistant",
      content: JSON.stringify({ ...expected[0], id: own }),
    };
    assert.equal(
      (
        await pool.query("select * from prefix_message($1,$2,$3,1,$4,$5)", [
          scope.tenantId,
          older,
          run,
          JSON.stringify([local]),
          writer,
        ])
      ).rows[0].writes,
      1,
    );
    await pool.query("select prefix_finish($1,$2,$3,$4,$5)", [
      scope.tenantId,
      older,
      run,
      writer,
      "{}",
    ]);
    const leaf = await forks.fork(scope.tenantId, older, { lane: 1, run, entry: own });
    await forks.retire(scope.tenantId, older);
    await forks.collect(scope.tenantId);
    assert.deepEqual(
      (await forks.agentConfiguration(scope.tenantId, leaf)).value,
      trace.forkDocument,
    );
    await forks.retire(scope.tenantId, leaf);
    await forks.collect(scope.tenantId);
    assert.equal((await pool.query("select count(*)::int n from messages")).rows[0].n, 0);
    return {
      retained,
      page,
      coldState,
      coldNotices,
      versions: retained.messages.filter((r) => r.document_id !== null).length,
    };
  } finally {
    await forks.db.destroy();
    await admin.query("drop schema " + schema + " cascade");
  }
}
const base = process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL;
assert(base, "Use run-with-test-postgres.mjs");
const admin = new pg.Client({ connectionString: base });
await admin.connect();
try {
  const sessionId = randomUUID();
  const scope = {
    tenantId: randomUUID(),
    sessionId,
    piSessionId: sessionId,
    runId: randomUUID(),
    turnId: randomUUID(),
    writerId: randomUUID(),
    fencingToken: 1,
  };
  const world = process.argv[2] === "--probe-current-world";
  const left = await nativeHistory("read", world ? currentEnvironment.continuityId : undefined);
  const right = await nativeHistory(
    world ? "read" : "edit",
    world ? "fixture-generation-two" : undefined,
  );
  assert.equal(left.anchor, right.anchor);
  assert.equal(left.batches.length, right.batches.length);
  assert.deepEqual(left.latest, right.latest);
  assert.deepEqual(left.suppliedTools, right.suppliedTools);
  assert.deepEqual(left.providerInputs, right.providerInputs);
  if (world) {
    assert.deepEqual(left.nativeNotices, []);
    assert.deepEqual(right.nativeNotices, ["pi-cloud.sandbox_reset"]);
  } else assert.notDeepEqual(left.forked, right.forked);
  const ids = Array.from({ length: left.batches.length + 1 }, () => randomUUID());
  const a = await pgHistory(base, admin, scope, left, ids),
    b = await pgHistory(base, admin, scope, right, ids);
  const sameData = isDeepStrictEqual(a.retained, b.retained),
    sameFork = isDeepStrictEqual(a.page, b.page);
  if (world) {
    console.log(
      JSON.stringify(
        {
          nativeBatches: left.batches.length,
          identicalAgentConfigurations: isDeepStrictEqual(a.coldState, b.coldState),
          identicalRetainedRows: sameData,
          identicalForkEntries: sameFork,
          nativeNextEnvironmentMessages: [left.nativeNotices, right.nativeNotices],
          candidateNextEnvironmentMessages: [a.coldNotices, b.coldNotices],
        },
        null,
        2,
      ),
    );
    if (
      !isDeepStrictEqual(a.coldNotices, left.nativeNotices) ||
      !isDeepStrictEqual(b.coldNotices, right.nativeNotices)
    )
      process.exitCode = 2;
  } else {
    assert(!sameData && sameFork);
    console.log(
      JSON.stringify(
        {
          nativeBatches: left.batches.length,
          anchor: left.anchor,
          latestTools: left.latest,
          identicalProviderMessages: true,
          providerRequestToolDeclarations: left.suppliedTools,
          nativeForkTools: [left.forked, right.forked],
          comparedTables: Object.keys(a.retained),
          identicalRetainedRows: sameData,
          identicalForkEntries: sameFork,
          visibleForkEntries: a.page.length,
          coldForkTools: [a.coldState.tools, b.coldState.tools],
          configRows: [a.versions, b.versions],
          sourcePruneAndDelete: true,
          nestedFork: true,
          olderAnchorAfterCollection: true,
          localEntryInheritedConfig: true,
          finalCollection: true,
          verdict: "matches_native_agent_asof",
        },
        null,
        2,
      ),
    );
  }
} finally {
  await admin.end();
}
