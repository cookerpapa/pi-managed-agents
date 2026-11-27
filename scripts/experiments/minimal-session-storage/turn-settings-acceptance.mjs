import { openPageApi } from "./flow-page-api.mjs";
// Acceptance through the public Session client, an RF3 projector, and two Worker processes.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import Fastify from "fastify";
import { PiCloudApi } from "../../../packages/web-ui/src/api.ts";
import { ControlPlaneStore } from "../../../packages/control-plane/src/control-plane-store.ts";

export async function verifyTurnSettings({ rt, pool, db, tenant, token, until, newWorker }) {
  const t = tenant.tenantId,
    user = tenant.ownerUserId,
    store = new ControlPlaneStore({ database: db, ...tenant });
  const first = await store.createProject({ name: "first", source: { kind: "empty" } });
  const second = await store.createProject({ name: "second", source: { kind: "empty" } });
  const machineId = randomUUID(),
    instanceId = randomUUID(),
    baseUrl = "http://settings-" + instanceId + ":4000";
  const ws = await db
    .updateTable("workspaces")
    .set({ workspace_kind: "development_environment" })
    .where("tenant_id", "=", t)
    .where("id", "=", second.workspaceId)
    .returning("sandbox_domain_id")
    .executeTakeFirstOrThrow();
  await db
    .insertInto("sandbox_control_instances")
    .values({
      instance_id: instanceId,
      sandbox_domain_id: ws.sandbox_domain_id,
      owner_base_url: baseUrl,
      state: "ready",
      lease_expires_at: new Date(Date.now() + 120000),
      last_heartbeat_at: new Date(),
    })
    .execute();
  await db
    .insertInto("development_environments")
    .values({
      id: machineId,
      tenant_id: t,
      owner_user_id: user,
      project_id: second.projectId,
      workspace_id: second.workspaceId,
      sandbox_domain_id: ws.sandbox_domain_id,
      environment_version_id: second.environment.environmentVersionId,
      owner_instance_id: instanceId,
      owner_base_url: baseUrl,
      runtime_id: "fixture-" + machineId,
      runtime_name: "fixture-" + machineId,
      state: "ready",
      idempotency_key: machineId,
      request_sha256: "a".repeat(64),
      profile_key: "starter",
      cpu_count: 1,
      memory_mib: 2048,
      system_disk_gib: 8,
    })
    .execute();
  const { sessionId: s } = await store.createSession(
    first.projectId,
    first.workspaceId,
    "Navigation source",
    "elastic",
    { ownerUserId: user },
  );
  await rt.store.register(t, [s]);
  await rt.inputs.settings.rebind({ tenant: t, session: s, sender: user }, "initialize-directory", {
    workspaceId: first.workspaceId,
    workingDirectory: "/workspace/game",
  });
  const schema = (await pool.query("select current_schema() as name")).rows[0].name,
    web = schema + "_web";
  await pool.query(
    "create schema " + web + "; create view " + web + ".sessions as select * from public.sessions",
  );
  const base = process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL,
    readUrl = new URL(base);
  readUrl.searchParams.set("options", "-c search_path=" + web + "," + schema + ",public");
  let historyServer;

  const app = Fastify();
  rt.installInputApi(app, async (req, id) =>
    id === s && req.headers.authorization === "Bearer " + token
      ? { tenant: t, sender: user }
      : null,
  );
  const origin = await app.listen({ host: "127.0.0.1", port: 0 }),
    api = new PiCloudApi((path, opts) => fetch(new URL(path, origin), opts), token);
  const pickA = {
    provider: "openai-codex",
    modelId: "gpt-6-astra",
    thinkingLevel: "medium",
    fastMode: true,
  };
  const pickB = {
    provider: "deepseek",
    modelId: "deepseek-v4-flash",
    thinkingLevel: "high",
    fastMode: false,
  };
  const read = async (id) =>
    (
      await pool.query(
        "select id,state,model_snapshot m,workspace_grant g from turns where id=$1",
        [id],
      )
    ).rows[0];
  const submit = (text, key) => api.acceptTurn(s, text, key);
  const where = async (id, state) => until(async () => (await read(id))?.state === state);
  try {
    const rels = (
      await pool.query(
        "select c.relname,c.relkind from pg_class c join pg_namespace n on n.oid=c.relnamespace where n.nspname=current_schema() and c.relname in ('turns','runs')",
      )
    ).rows;
    assert.deepEqual(Object.fromEntries(rels.map((row) => [row.relname, row.relkind])), {
      runs: "v",
      turns: "r",
    });
    await api.updateSessionModel(s, pickA);
    const initial = await submit("start A", "turn:" + randomUUID()),
      t17 = initial.turnId;
    const active = await read(t17);
    assert.equal(active.m.modelId, pickA.modelId);
    assert(active.m.runtime.catalogRevision);
    assert.equal(active.m.serviceTier, "fast");
    assert.equal(active.g.cwd, "/workspace/game");
    assert.equal(active.g.computeSessionId, first.workspaceId);
    await assert.rejects(
      api.rebindConversationWorkspace(
        s,
        second.workspaceId,
        "turn:" + randomUUID(),
        "/workspace/other",
      ),
      (e) => e.status === 409,
    );
    const old = await newWorker();
    const owner = (await old.ask({ op: "claimed" })).scope;
    const started = await old.ask({ op: "execute", hold: true });
    assert.equal(started.modelId, pickA.modelId);
    assert.equal(started.cwd, active.g.cwd);
    await api.updateSessionModel(s, pickB);
    assert.equal((await api.getSessionModel(s)).modelId, pickB.modelId);
    assert.deepEqual((await read(t17)).m, active.m);
    await assert.rejects(
      pool.query("update turns set workspace_grant=null where id=$1", [t17]),
      /immutable/,
    );
    await assert.rejects(
      pool.query("update turns set model_snapshot='{}'::jsonb where id=$1", [t17]),
      /immutable/,
    );
    await assert.rejects(
      api.rebindConversationWorkspace(
        s,
        second.workspaceId,
        "turn:" + randomUUID(),
        "/workspace/other",
      ),
      (e) => e.status === 409,
    );
    await until(
      async () =>
        (
          await pool.query("select 1 from turns where id=$1 and projection_first is not null", [
            t17,
          ])
        ).rowCount,
    );
    await old.kill();
    await where(t17, "queued");
    const replacement = await newWorker();
    const handoff = (await replacement.ask({ op: "claimed" })).scope;
    assert.equal(handoff.turnId, t17);
    assert(handoff.fencingToken > owner.fencingToken);
    const resumed = await replacement.ask({ op: "execute" });
    assert(resumed.resumed);
    assert.equal(resumed.historyLoad.path, "recovery");
    assert.equal(resumed.modelId, pickA.modelId);
    assert.equal(resumed.cwd, "/workspace/game");
    await where(t17, "completed");
    const entry = Number(
      (
        await pool.query(
          "select entry_id from messages where run_id=$1 and role='assistant' order by entry_id desc limit 1",
          [t17],
        )
      ).rows[0].entry_id,
    );
    historyServer = await openPageApi(
      base,
      readUrl.href,
      true,
      undefined,
      undefined,
      false,
      schema,
    );
    const historyApi = new PiCloudApi(
      (path, opts) => fetch(new URL(path, historyServer.origin), opts),
      tenant.credential.token,
    );
    const branch = (
      await historyApi.forkConversation(s, t17, String(entry), "Peer", "fork:" + randomUUID())
    ).session.sessionId;
    assert.equal((await rt.inputs.settings.model(t, branch)).modelId, pickB.modelId);
    await assert.rejects(
      rt.inputs.settings.rebind({ tenant: t, session: s, sender: randomUUID() }, "other-user", {
        workspaceId: second.workspaceId,
        workingDirectory: "/home/user/server",
      }),
      (e) => e.code === "not_found",
    );
    const key = "turn:" + randomUUID();
    const rebinding = await api.rebindConversationWorkspace(
      s,
      second.workspaceId,
      key,
      "/home/user/server",
    );
    assert.equal(rebinding.workspaceId, second.workspaceId);
    const branchWorkspace = (
      await pool.query("select desired_workspace from sessions where id=$1", [branch])
    ).rows[0].desired_workspace;
    assert.deepEqual(branchWorkspace, active.g);
    const next = await submit("continue on B", "turn:" + randomUUID()),
      t18 = next.turnId;
    assert.notEqual(t17, t18);
    assert(
      (await api.rebindConversationWorkspace(s, second.workspaceId, key, "/home/user/server"))
        .replayed,
    );
    const nextTurn = await read(t18);
    assert.equal(nextTurn.m.provider, pickB.provider);
    assert.equal(nextTurn.m.modelId, pickB.modelId);
    assert.equal(nextTurn.m.thinkingLevel, "high");
    assert.equal(nextTurn.m.serviceTier, null);
    assert.notEqual(nextTurn.m.credentialBindingId, active.m.credentialBindingId);
    assert.equal(nextTurn.g.computeSessionId, machineId);
    assert.equal(nextTurn.g.cwd, "/home/user/server");
    assert.deepEqual((await read(t17)).g, active.g);
    await replacement.ask({ op: "release" });
    assert.equal((await replacement.ask({ op: "claimed" })).scope.turnId, t18);
    const answer = await replacement.ask({ op: "execute" });
    assert.equal(answer.modelId, pickB.modelId);
    assert.equal(answer.cwd, "/home/user/server");
    assert(answer.systemPrompt.includes("Current working directory: /home/user/server"));
    assert(answer.systemPrompt.includes("Workspace changed"));
    await where(t18, "completed");
    for (const [id, model] of [
      [t17, pickA],
      [t18, pickB],
    ]) {
      const { rows } = await pool.query(
        "select content::jsonb#>>'{model,0,model}' model,content::jsonb#>>'{model,0,provider}' provider from messages where run_id=$1 and role='assistant'",
        [id],
      );
      assert(
        rows.length &&
          rows.every((r) => r.model === model.modelId && r.provider === model.provider),
      );
    }
    // Late mail already accepted into a closing Turn becomes an independently
    // scheduled successor using the Session's latest selection.
    await replacement.ask({ op: "release" });
    const t19 = (await submit("last boundary", "turn:" + randomUUID())).turnId;
    const held = (await replacement.ask({ op: "claimed" })).scope;
    assert.equal(held.turnId, t19);
    await api.updateSessionModel(s, pickA);
    const child = await rt.store.delegate(
      t,
      s,
      null,
      randomUUID(),
      "inspect",
      "Check files",
      pickB.modelId,
      { run: t19, writer: held.writerId },
      { tools: ["read"] },
    );
    const childSettings = (
      await pool.query(
        "select model_snapshot m,workspace_grant g from turns where session_id=(select flow_id from sessions where id=$1)",
        [child],
      )
    ).rows[0];
    assert.equal(childSettings.m.modelId, pickB.modelId);
    assert.deepEqual(childSettings.g.tools, ["read"]);
    assert.equal(childSettings.g.computeSessionId, machineId);
    const other = await newWorker();
    assert.equal((await other.ask({ op: "claimed" })).scope.sessionId, child);
    const childAnswer = await other.ask({ op: "execute" });
    assert.equal(childAnswer.modelId, pickB.modelId);
    await where(childAnswer.turnId, "completed");
    assert.equal(
      (await replacement.ask({ op: "execute", holdBeforeFinish: true })).kind,
      "native-idle",
    );
    const follow = await api.acceptTurn(
      s,
      "late input",
      "turn:" + randomUUID(),
      undefined,
      undefined,
      "follow_up",
    );
    const closed = await replacement.ask({ op: "finish-turn" });
    assert.equal(closed.modelId, pickB.modelId);
    await where(t19, "completed");
    const carried = (
      await pool.query("select turn_id from user_or_agent_messages where id=$1", [follow.inputId])
    ).rows[0].turn_id;
    assert.notEqual(carried, t19);
    assert.equal((await read(carried)).m.modelId, pickA.modelId);
    assert.deepEqual((await read(carried)).g, nextTurn.g);
    await replacement.ask({ op: "release" });
    assert.equal((await replacement.ask({ op: "claimed" })).scope.turnId, carried);
    const carriedAnswer = await replacement.ask({ op: "execute" });
    assert.equal(carriedAnswer.modelId, pickA.modelId);
    assert.equal(carriedAnswer.historyLoad.path, "warm");
    assert.equal(carriedAnswer.historyLoad.pgEntryBytes, 0);
    await where(carried, "completed");
    await replacement.ask({ op: "release" });
    const interrupted = (await submit("hold for Stop", "turn:" + randomUUID())).turnId;
    assert.equal((await replacement.ask({ op: "claimed" })).scope.turnId, interrupted);
    await replacement.ask({ op: "execute", hold: true, abortable: true });
    const queued = await rt.inputs.admit({
      tenant: t,
      session: s,
      sender: child,
      id: randomUUID(),
      source: "agent",
      mode: "follow_up",
      text: "two payment tests failed",
    });
    const [stopped] = await Promise.all([
      replacement.receive(),
      api.cancelTurn(s, interrupted, "stop:" + randomUUID()),
    ]);
    assert.equal(stopped.kind, "finished");
    assert.equal(stopped.terminalState, "turn.cancelled");
    await where(interrupted, "cancelled");
    assert.equal(
      (await pool.query("select state from user_or_agent_messages where id=$1", [queued.id]))
        .rows[0].state,
      "held",
    );
    await replacement.ask({ op: "release" });
    const continued = (await submit("continue payment tests", "turn:" + randomUUID())).turnId;
    assert.equal((await replacement.ask({ op: "claimed" })).scope.turnId, continued);
    const done = await replacement.ask({ op: "execute", captureInputs: true });
    const inputs = done.history.filter(
      (m) => m.includes("two payment tests failed") || m === "continue payment tests",
    );
    assert.deepEqual(inputs, [
      "continue payment tests",
      "[From Agent " + child + "]\ntwo payment tests failed",
    ]);
    await where(continued, "completed");
    const inheritedPage = async () =>
      (await historyApi.getConversationTree(branch, "focus")).branches[0].entries;
    const beforeDelete = await inheritedPage();
    assert.deepEqual(
      beforeDelete.map((e) => e.text),
      ["start A"],
    );
    await historyApi.deleteConversation(s);
    await rt.store.collect(t);
    assert.equal(
      (
        await pool.query("select count(*)::int n from user_or_agent_messages where session_id=$1", [
          s,
        ])
      ).rows[0].n,
      0,
    );
    assert.deepEqual(await inheritedPage(), beforeDelete);
    await historyApi.deleteConversation(branch);
    await rt.store.collect(t);
    assert.equal(
      (
        await pool.query(
          "select count(*)::int n from messages where run_id=$1 and input_display is not null",
          [t17],
        )
      ).rows[0].n,
      0,
    );
  } finally {
    await historyServer?.close();
    await app.close();
    await pool.query("drop schema if exists " + web + " cascade");
  }
}
