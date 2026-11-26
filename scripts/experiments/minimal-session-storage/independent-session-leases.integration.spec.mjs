import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import nativeKafka from "@confluentinc/kafka-javascript";
import { KafkaWorkerProducer, KafkaToolReplyPublisher } from "@pi-cloud/event-log";
import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import { IndependentSessionRuntime } from "./independent-session-runtime.mjs";
import { readSealedFamily } from "@pi-cloud/pi-session-postgres/durable";
import { waitingDelegation, resumeParent } from "./direct-delegation-worker.mjs";
import { encodeWorkspaceFact } from "./workspace-operations.mjs";
import { installWorkspaceRoutes } from "./workspace-route-spans.mjs";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { it } from "vitest";
import pg from "pg";
import { sql } from "kysely";
import { createDatabase, runMigrations } from "@pi-cloud/database";
import { createPrivateTenant } from "../../../packages/control-plane/src/tenant-administration.ts";
import { registerTestWorker } from "../../../packages/control-plane/test/fixtures/registered-worker.ts";
import { ForkStorage, installForkTables, setForkLayout } from "./fork-storage.mjs";
import { installPagingProjection } from "./flow-paging-fixture.mjs";
import {
  SegmentLeaseProjection,
  installIndependentSessionLeases,
} from "./independent-session-leases.mjs";
const endpoint = process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL;

async function actor(db, url, capacity = 1, transport = {}) {
  const workerId = randomUUID(),
    bootId = randomUUID();
  await db
    .insertInto("workers")
    .values({
      id: workerId,
      supervisor_id: workerId,
      boot_id: bootId,
      state: "ready",
      max_concurrent_sessions: capacity,
    })
    .execute();
  await registerTestWorker(db, workerId);
  const child = fork(
    fileURLToPath(new URL("./independent-session-worker.mjs", import.meta.url)),
    [],
    { execArgv: ["--import", "tsx"], stdio: ["ignore", "inherit", "inherit", "ipc"] },
  );
  const ended = once(child, "exit");
  async function ask(m) {
    const incoming = once(child, "message", { signal: AbortSignal.timeout(12000) });
    child.send(m);
    const [r] = await Promise.race([incoming, ended]);
    assert(r && r.kind !== "error", r?.message ?? "Worker exited");
    return r;
  }
  await ask({ url, workerId, bootId, capacity, leaseDurationMs: 2400, ...transport });
  return {
    workerId,
    ask,
    async kill() {
      if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      await ended;
    },
  };
}

it.skipIf(!endpoint)(
  "independently claims, renews, fences and replaces delegated Session writers",
  async () => {
    const schema = "independent_" + randomUUID().replaceAll("-", "");
    const url = new URL(endpoint);
    url.searchParams.set("options", "-c search_path=" + schema + ",public");
    const db = createDatabase({ connectionString: endpoint });
    const pool = new pg.Pool({ connectionString: url.href }),
      store = new ForkStorage(pool);
    const topic = "segment-" + randomUUID(),
      projection = new SegmentLeaseProjection(store.db, topic, 1);
    const actors = [];
    async function nextWorker(capacity) {
      const w = await actor(db, url.href, capacity);
      actors.push(w);
      return w;
    }
    let offset = 0n;
    const consume = (fact, apply = async () => {}) =>
      projection.projectBoundary({ fact, topic, partition: 0, offset: offset++ }, apply);
    const finish = (s) => ({
      kind: "work_finished",
      factId: randomUUID(),
      scope: s,
      agentId: "root",
      baseSequence: 0,
      nativeSequence: 0,
      terminal: { type: "turn.completed", payload: { stopReason: "stop" } },
      occurredAt: new Date().toISOString(),
    });
    try {
      await runMigrations(db, "up");
      const { tenantId: t } = await createPrivateTenant(db, {
        slug: schema.replaceAll("_", "-"),
        ownerDisplayName: "Test",
      });
      await pool.query("create schema " + schema);
      await installPagingProjection(pool);
      await installForkTables(pool);
      await setForkLayout(pool, true, false);
      await installIndependentSessionLeases(pool);
      const parent = randomUUID(),
        turn = randomUUID();
      await store.register(t, [parent]);
      await pool.query(
        "insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state) values($1,$2,$3,1,$1,'parent','fixture','queued')",
        [turn, t, parent],
      );
      const a = await nextWorker(),
        b = await nextWorker(),
        c = await nextWorker();
      const p = (await a.ask({ op: "claim" })).scope;
      assert.equal(p.sessionId, parent);
      const original = {
        id: 10,
        conversationId: 1,
        kind: "pi.user",
        model: [{ role: "user", content: "parent evidence", timestamp: 1 }],
      };
      await pool.query(
        "insert into messages(tenant_id,pi_session_id,conversation_id,entry_id,run_id,role,commit_seq,ordinal,content) values($1,$2,1,10,$3,'user',1,0,$4)",
        [t, parent, turn, JSON.stringify(original)],
      );
      const caller = { writer: p.writerId, run: turn },
        key = randomUUID();
      const child = await store.delegate(
        t,
        parent,
        { entry: 10, lane: 1, run: turn },
        key,
        "tests",
        "child task",
        "fixture",
        caller,
      );
      assert.equal(
        await store.delegate(
          t,
          parent,
          { entry: 10, lane: 1, run: turn },
          key,
          "tests",
          "child task",
          "fixture",
          caller,
        ),
        child,
      );
      const results = await Promise.all([b.ask({ op: "claim" }), c.ask({ op: "claim" })]);
      assert.equal(results.filter((v) => v.scope).length, 1);
      const ix = results.findIndex((v) => v.scope),
        childOwner = [b, c][ix],
        spare = [b, c][1 - ix],
        first = results[ix].scope;
      assert.equal(first.sessionId, child);
      assert.notEqual(first.writerId, p.writerId);
      const renewAt = async (id) =>
        (
          await pool.query("select renewed_at from session_leases where lease_id=$1", [id])
        ).rows[0].renewed_at.getTime();
      const prior = await renewAt(first.writerId);
      await a.kill();
      let seals = [];
      for (let i = 0; i < 90 && !seals.length; i++) {
        await delay(80);
        seals = await projection.expired();
      }
      assert.deepEqual(
        seals.map((s) => s.scope.writerId),
        [p.writerId],
      );
      assert((await renewAt(first.writerId)) > prior);
      assert.equal(
        (await spare.ask({ op: "claim" })).scope,
        null,
        "expiry alone cannot transfer ownership",
      );
      assert.equal(await consume({ ...seals[0], factId: randomUUID() }), false);
      await assert.rejects(
        consume(seals[0], async () => {
          throw Error("checkpoint failed");
        }),
        /checkpoint failed/,
      );
      assert.equal((await spare.ask({ op: "claim" })).scope, null);
      assert.equal(await consume(seals[0]), true);
      assert.equal(await consume(seals[0]), false);
      const parent2 = (await spare.ask({ op: "claim" })).scope;
      assert.equal(parent2.runId, turn);
      assert.equal(parent2.fencingToken, p.fencingToken + 1);
      assert.equal(await consume(finish(p)), false);
      const d = await nextWorker();
      await childOwner.kill();
      seals = [];
      for (let i = 0; i < 90 && !seals.length; i++) {
        await delay(80);
        seals = await projection.expired();
      }
      assert.deepEqual(
        seals.map((s) => s.scope.writerId),
        [first.writerId],
      );
      assert.equal((await d.ask({ op: "claim" })).scope, null);
      assert.equal(await consume(seals[0]), true);
      const second = (await d.ask({ op: "claim" })).scope;
      assert.equal(second.runId, first.runId);
      assert.equal(second.sessionId, child);
      assert.equal(second.fencingToken, first.fencingToken + 1);
      assert.equal(await consume(finish(first)), false);
      assert.equal(await consume(finish(parent2)), true);
      const reply = await d.ask({ op: "answer" });
      assert(reply.history.includes("parent evidence"));
      assert(reply.history.includes("child task"));
      assert.equal(reply.text, "checked: child task");
      assert.equal(await consume(finish(second)), true);
      assert.deepEqual(
        (await pool.query("select state from runs order by id")).rows.map((r) => r.state),
        ["completed", "completed"],
      );
      assert.equal(
        (
          await pool.query(
            "select count(*)::int n from session_leases where tenant_id=$1 and released_at is null",
            [t],
          )
        ).rows[0].n,
        0,
      );
    } finally {
      await Promise.allSettled(actors.map((w) => w.kill()));
      try {
        await pool.query("drop schema if exists " + schema + " cascade");
      } finally {
        await store.db.destroy();
        await db.destroy();
      }
    }
  },
  90000,
);

const brokers = process.env.PI_CLOUD_KAFKA_INTEGRATION_BROKERS?.split(",");
it.skipIf(!endpoint || !brokers).each(["branch", "fresh", "parent-handoff"])(
  "acknowledges %s creation independently of child completion and restores the Session through RF3",
  async (mode) => {
    const schema = "segment_log_" + randomUUID().replaceAll("-", "");
    const url = new URL(endpoint);
    url.searchParams.set("options", "-c search_path=" + schema + ",public");
    const db = createDatabase({ connectionString: endpoint });
    const pool = new pg.Pool({ connectionString: url.href }),
      topic = "segment-" + randomUUID();
    const rt = new IndependentSessionRuntime({
      pool,
      topic,
      selectSandbox: () => assert.fail(),
      resolveSandbox: () => assert.fail(),
    });
    const admin = new nativeKafka.KafkaJS.Kafka({
      kafkaJS: { brokers, logLevel: nativeKafka.KafkaJS.logLevel.NOTHING },
    }).admin();
    const writer = new KafkaWorkerProducer({ brokers, topic, clientId: topic });
    const replies = new KafkaToolReplyPublisher(brokers, topic + "-returns");
    let returns, scheduler;
    const token = randomUUID() + randomUUID();
    const actors = [];
    let reader, parent, nativeParent, failed, reconciliation;
    const stopControl = new AbortController(),
      publishedSeals = new Map();
    const send = (f) =>
      writer.append(0, {
        factId: f.factId,
        sessionId: f.scope.piSessionId,
        value: encodeWorkspaceFact(f),
      });
    async function until(fn) {
      const stop = Date.now() + 18000;
      while (Date.now() < stop) {
        if (failed) throw failed;
        const v = await fn();
        if (v) return v;
        await delay(35);
      }
      throw Error("Timeout waiting for independent Session projection");
    }
    try {
      await runMigrations(db, "up");
      const { tenantId: t } = await createPrivateTenant(db, {
        slug: schema.replaceAll("_", "-"),
        ownerDisplayName: "Test",
      });
      await pool.query("create schema " + schema);
      await installPagingProjection(pool);
      await installForkTables(pool);
      await setForkLayout(pool, true, false);
      await installIndependentSessionLeases(pool);
      await installWorkspaceRoutes(pool);
      await admin.connect();
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
      await writer.start();
      await replies.start();
      returns = await rt.serveDelegations({
        connectionString: url.href,
        publish: (dest, r) => replies.publish(dest, r),
        token,
      });
      rt.connectDelegations(returns.url, token);
      // Inspect the real broker records observed by this Projector, deduplicating
      // by position when a test deliberately restarts the reader.
      const observed = new Map(),
        consume = rt.consume.bind(rt);
      rt.consume = (record, ...args) => {
        for (const w of record.fact.commit?.writes ?? [])
          if (w.type === "submission" && w.value.requestId) {
            const key = w.value.requestId,
              positions = observed.get(key) ?? new Map();
            positions.set(String(record.offset) + "/" + w.value.status, w.value.status);
            observed.set(key, positions);
          }
        return consume(record, ...args);
      };
      const newReader = (id) =>
        rt.reader({
          brokers,
          clientId: id,
          groupId: topic,
          onError: (e) => {
            failed = e;
          },
        });
      reader = newReader(topic);
      await reader.start();
      await reader.waitForPartition(0);
      reconciliation = rt.leases.reconcile({
        signal: stopControl.signal,
        intervalMs: 100,
        publish: async (partition, f) => {
          assert.equal(partition, 0);
          await send(f);
          publishedSeals.set(f.scope.writerId, f);
        },
      });
      reconciliation.catch((e) => {
        failed = e;
      });
      const parentSession = randomUUID(),
        parentRun = randomUUID();
      await rt.store.register(t, [parentSession]);
      await pool.query(
        "insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state) values($1,$2,$3,1,$1,'Delegate','fixture','queued')",
        [parentRun, t, parentSession],
      );
      const workerId = randomUUID(),
        bootId = randomUUID();
      await db
        .insertInto("workers")
        .values({
          id: workerId,
          supervisor_id: workerId,
          boot_id: bootId,
          state: "ready",
          max_concurrent_sessions: 1,
        })
        .execute();
      await registerTestWorker(db, workerId);
      parent = rt.worker({ workerId, bootId, capacity: 1, leaseDurationMs: 12000 });
      const job = await parent.claimNext();
      nativeParent = await waitingDelegation(
        job.scope,
        mode === "parent-handoff" ? "branch" : mode,
        "review",
      );
      scheduler = await rt.serveScheduler({ connectionString: url.href, token, tickMs: 60000 });
      const directed = { topic, brokers, scheduler: { url: scheduler.url, token } };
      const one = await actor(db, url.href, 1, directed);
      actors.push(one);
      await until(() => scheduler.snapshot().waiting === 1);
      for (const f of nativeParent.prefix) await send(f);
      const child = await until(
        async () =>
          (
            await pool.query(
              "select s.id,r.id as turn from sessions s join runs r on r.session_id=s.flow_id and r.tenant_id=s.tenant_id where s.parent_session_id=$1",
              [parentSession],
            )
          ).rows[0],
      );
      const first = (await one.ask({ op: "claimed" })).scope;
      assert.equal(first.sessionId, child.id);
      if (mode === "parent-handoff") {
        // The child is already running on one. Stop the actual parent harness and
        // its renewer; the successor opens a different boot reply mailbox.
        const oldTopic = nativeParent.replyTopic;
        await parent.close();
        await nativeParent.close();
        nativeParent = undefined;
        await until(() => publishedSeals.get(job.scope.writerId));
        await until(
          async () =>
            (await pool.query("select 1 from runs where id=$1 and state='queued'", [parentRun]))
              .rowCount,
        );
        const id = randomUUID(),
          boot = randomUUID();
        await db
          .insertInto("workers")
          .values({
            id,
            supervisor_id: id,
            boot_id: boot,
            state: "ready",
            max_concurrent_sessions: 1,
          })
          .execute();
        await registerTestWorker(db, id);
        parent = rt.worker({ workerId: id, bootId: boot, capacity: 1, leaseDurationMs: 12000 });
        const replacement = await parent.claimNext(),
          scope = replacement.scope;
        assert.equal(scope.runId, parentRun);
        assert.equal(scope.fencingToken, job.scope.fencingToken + 1);
        const family = await readSealedFamily(rt.store.db, scope, topic);
        const restored = resumeParent(
          pool,
          rt.store.db,
          topic,
          scope,
          family,
          oldTopic,
          child.id,
          false,
          send,
        );
        restored.catch((e) => {
          failed = e;
        });
        await until(
          async () =>
            (
              await pool.query(
                "select 1 from subagent_deliveries where writer=$1 and value->>'text' like '%' || $2 || '%' and destination<>$3",
                [scope.writerId, child.id, oldTopic],
              )
            ).rowCount,
        );
        const end = await restored;
        assert.equal(end.results.length, 1);
        assert(JSON.stringify(end.results).includes(child.id));
        await send({
          kind: "work_finished",
          scope,
          agentId: "root",
          factId: randomUUID(),
          nativeSequence: end.sequence,
          baseSequence: 0,
          terminal: { type: "turn.completed", payload: { stopReason: "stop" } },
          occurredAt: new Date().toISOString(),
        });
        await until(
          async () =>
            (await pool.query("select 1 from runs where id=$1 and state='completed'", [parentRun]))
              .rowCount,
        );
        const answer = await one.ask({ op: "execute" });
        assert(!answer.resumed);
        await until(
          async () =>
            (
              await pool.query(
                "select count(*)::int n from runs where tenant_id=$1 and state='completed'",
                [t],
              )
            ).rows[0].n === 2,
        );
        assert.equal(
          (
            await pool.query(
              "select count(*)::int n from session_leases where tenant_id=$1 and pi_session_id=$2",
              [t, child.id],
            )
          ).rows[0].n,
          1,
        );
        return;
      }
      const model = await one.ask({ op: "execute", hold: true });
      assert.equal(model.history.includes("New parent summary"), mode === "branch");
      assert(!model.history.includes("Earlier context"));
      if (mode === "branch") {
        await until(
          async () =>
            (
              await pool.query(
                "select output_first_offset from runs where id=$1 and projection_first is not null",
                [child.turn],
              )
            ).rowCount,
        );
        await reader.close();
        rt.reset();
        reader = newReader(topic + "-replacement");
        await reader.start();
        await reader.waitForPartition(0);
      }
      // Lose the owner between PG handoff and the native submit. Withdrawal
      // is settled by the projected seal before a successor is eligible.
      const retryId = randomUUID(),
        lostId = randomUUID(),
        human = randomUUID();
      if (mode === "fresh") {
        await rt.inputs.admit({
          tenant: t,
          session: child.id,
          id: lostId,
          source: "user",
          sender: human,
          mode: "steer",
          text: "never deliver",
        });
        assert.equal((await rt.inputs.selectAt(first, "postTools", 700))?.id, lostId);
        assert((await rt.inputs.withdraw(t, child.id, human, lostId)).withdrawing);
        await rt.inputs.admit({
          tenant: t,
          session: child.id,
          id: retryId,
          sender: human,
          source: "user",
          mode: "steer",
          text: "survive unpublished reservation",
        });
        assert.equal((await rt.inputs.selectAt(first, "postTools", 701)).id, retryId);
      }
      await one.kill();
      const two = await actor(db, url.href, 1, directed);
      actors.push(two);
      await until(() => publishedSeals.get(first.writerId));
      const second = (await two.ask({ op: "claimed" })).scope;
      if (mode === "fresh")
        assert.equal(
          (await pool.query("select state from user_or_agent_messages where id=$1", [lostId]))
            .rows[0].state,
          "withdrawn",
        );
      assert.equal(second.runId, first.runId);
      assert.equal(second.fencingToken, first.fencingToken + 1);
      if (mode === "fresh") {
        const {
          rows: [retry],
        } = await pool.query("select state,boundary_task from user_or_agent_messages where id=$1", [
          retryId,
        ]);
        assert.deepEqual(retry, { state: "pending", boundary_task: null });
      }
      // The old boot can still put bytes into Kafka. Its receipt cannot append
      // a body, start a tool or settle the new owner's Run.
      await send({
        kind: "durable_commit",
        scope: first,
        factId: randomUUID(),
        occurredAt: new Date().toISOString(),
        commit: {
          seq: 1,
          nextId: 110002,
          writes: [
            {
              type: "entry",
              value: {
                kind: "pi.user",
                id: 110001,
                conversationId: 1,
                model: [{ role: "user", content: "late obsolete writer", timestamp: 1 }],
              },
            },
          ],
        },
        origins: [],
        tools: [],
        cancellations: [],
        display: [],
      });
      let successor = two;
      if (mode === "fresh") {
        await two.kill();
        await until(() => publishedSeals.get(second.writerId));
        await until(
          async () =>
            (await pool.query("select 1 from runs where id=$1 and state='queued'", [child.turn]))
              .rowCount,
        );
        successor = await actor(db, url.href, 1, directed);
        actors.push(successor);
        const third = (await successor.ask({ op: "claimed" })).scope;
        assert.equal(third.runId, first.runId);
        assert.equal(third.fencingToken, second.fencingToken + 1);
      }
      const steerIds = [randomUUID(), randomUUID()];
      if (mode === "branch") {
        for (const [i, id] of steerIds.entries())
          await rt.inputs.admit({
            tenant: t,
            session: child.id,
            sender: human,
            id,
            source: "user",
            mode: "steer",
            text: "boundary-steer-" + i,
          });
        const placed = await successor.ask({
          op: "execute",
          report: "Payment tests passed",
          holdAfterPlacement: true,
        });
        assert.equal(placed.kind, "at-boundary");
        await until(
          async () =>
            (
              await pool.query("select 1 from messages where run_id=$1 and source_input_id=$2", [
                child.turn,
                steerIds[0],
              ])
            ).rowCount,
        );
        await successor.kill();
        await until(() => publishedSeals.get(second.writerId));
        successor = await actor(db, url.href, 1, directed);
        actors.push(successor);
        const takeover = (await successor.ask({ op: "claimed" })).scope;
        assert.equal(takeover.fencingToken, second.fencingToken + 1);
      }
      const completed = await successor.ask({ op: "execute" });
      if (mode === "branch")
        await until(
          async () =>
            (
              await pool.query(
                "select 1 from user_or_agent_messages where session_id=$1 and sender_id=$2 and source='agent' and mode='follow_up' and body='Payment tests passed'",
                [parentSession, child.id],
              )
            ).rowCount,
        );
      assert.equal(
        (
          await pool.query("select 1 from messages where run_id=$1 and entry_id=110001", [
            child.turn,
          ])
        ).rowCount,
        0,
      );
      if (mode === "fresh")
        await until(
          async () =>
            (
              await pool.query(
                "select 1 from user_or_agent_messages where id=$1 and state='withdrawn' and not withdrawing",
                [lostId],
              )
            ).rowCount,
        );
      assert.equal(completed.resumed, true);
      if (mode === "fresh") {
        await until(() => observed.get(retryId)?.size === 2);
        assert(!observed.has(lostId));
        assert.deepEqual([...observed.get(retryId).values()], ["placed", "done"]);
      } else
        for (const id of steerIds) {
          await until(() => observed.get(id)?.size === 2);
          assert.deepEqual([...observed.get(id).values()], ["placed", "done"]);
        }
      if (mode === "branch") {
        assert(
          completed.history.includes("boundary-steer-0") &&
            !completed.history.includes("boundary-steer-1"),
        );
        const { rows } = await pool.query(
          "select state,boundary_task from user_or_agent_messages where id=any($1::uuid[]) order by position",
          [steerIds],
        );
        assert.equal(rows[0].state, "placed");
        assert(rows[0].boundary_task);
        assert.equal(rows[1].state, "placed");
        assert.equal(rows[1].boundary_task, null);
      } else assert.deepEqual(completed.history, model.history);
      await until(
        async () =>
          (
            await pool.query(
              "select 1 from subagent_deliveries where value->>'text' like '%' || $1 || '%' and sent",
              [child.id],
            )
          ).rowCount,
      );
      await nativeParent.root.waitForIdle(ctx);
      const outcome = await nativeParent.submission.wait(ctx);
      assert.equal(outcome.status, "done");
      const canonical = nativeParent.facts
        .flatMap((f) => f.commit.writes)
        .filter((w) => w.type === "entry" && w.value.kind === "pi.tool-result");
      assert.equal(canonical.length, 1);
      assert(JSON.stringify(canonical[0]).includes(child.id));
      assert(!JSON.stringify(canonical[0]).includes("independent answer"));
      for (const f of nativeParent.facts.slice(nativeParent.prefix.length)) await send(f);
      await send({
        kind: "work_finished",
        factId: randomUUID(),
        scope: job.scope,
        agentId: "root",
        nativeSequence: nativeParent.facts.at(-1).commit.seq,
        baseSequence: 0,
        terminal: { type: "turn.completed", payload: { stopReason: "stop" } },
        occurredAt: new Date().toISOString(),
      });
      await until(
        async () =>
          (
            await pool.query(
              "select count(*)::int n from runs where tenant_id=$1 and state='completed'",
              [t],
            )
          ).rows[0].n === 2,
      );
      assert.equal(
        (
          await pool.query("select count(*)::int n from messages where run_id=$1 and role='user'", [
            child.turn,
          ])
        ).rows[0].n,
        mode === "branch" ? 3 : 2,
      );
    } finally {
      await Promise.allSettled(actors.map((w) => w.kill()));
      await scheduler?.close();
      stopControl.abort();
      await reconciliation;
      await parent?.close();
      await nativeParent?.close();
      await reader?.close();
      await rt.subagents?.close();
      await returns?.close();
      await writer.close();
      await replies.close();
      try {
        await admin.deleteTopics({ topics: [topic] });
      } finally {
        await admin.disconnect();
        await pool.query("drop schema if exists " + schema + " cascade");
        await rt.store.db.destroy();
        await db.destroy();
      }
    }
  },
  150000,
);

it.skipIf(!endpoint)(
  "uses the ordinary Worker slots for nested children and preserves Session epochs after pruning",
  async () => {
    const schema = "segment_slots_" + randomUUID().replaceAll("-", ""),
      url = new URL(endpoint);
    url.searchParams.set("options", "-c search_path=" + schema + ",public");
    const db = createDatabase({ connectionString: endpoint }),
      pool = new pg.Pool({ connectionString: url.href });
    const runtime = new IndependentSessionRuntime({ pool, topic: "slots", partitions: 1 }),
      store = runtime.store;
    const workerId = randomUUID(),
      bootId = randomUUID();
    const worker = runtime.worker({ workerId, bootId, capacity: 2, leaseDurationMs: 6000 });
    const token = randomUUID() + randomUUID(),
      abort = new AbortController();
    let scheduler;
    const jobs = [],
      releases = [],
      finished = [];
    let server,
      offset = 0n;
    const callers = (job) => ({ run: job.id, writer: job.scope.writerId });
    async function waitCount(n) {
      for (let i = 0; jobs.length < n && i < 300; i++) await delay(15);
      assert.equal(jobs.length, n);
    }
    async function close(job, entry) {
      const fact = {
        kind: "work_finished",
        factId: randomUUID(),
        scope: job.scope,
        agentId: "root",
        nativeSequence: 1,
        baseSequence: 0,
        terminal: { type: "turn.completed", payload: { stopReason: "stop" } },
        occurredAt: new Date().toISOString(),
      };
      assert(
        await runtime.leases.projectBoundary(
          { fact, topic: "slots", partition: 0, offset: offset++ },
          async (tx) => {
            await sql`update runs set history_head=jsonb_build_object('version',${fact.factId}::text,'seq',1,'nextId',${entry + 1}::int) where id=${job.id}::uuid`.execute(
              tx,
            );
            const value = {
              id: entry,
              conversationId: 1,
              kind: "pi.assistant",
              model: [{ role: "assistant", stopReason: "stop", timestamp: 0, content: [] }],
            };
            await sql`insert into messages(tenant_id,pi_session_id,conversation_id,entry_id,run_id,role,commit_seq,ordinal,content)
        values(${job.tenant_id}::uuid,${job.flow_id}::uuid,1,${entry},${job.id}::uuid,'assistant',1,0,${JSON.stringify(value)})`.execute(
              tx,
            );
          },
        ),
      );
      releases[jobs.indexOf(job)].resolve();
    }
    try {
      await runMigrations(db, "up");
      const { tenantId: t } = await createPrivateTenant(db, {
        slug: schema.replaceAll("_", "-"),
        ownerDisplayName: "Test",
      });
      await pool.query("create schema " + schema);
      await installPagingProjection(pool);
      await installForkTables(pool);
      await setForkLayout(pool, true, false);
      await installIndependentSessionLeases(pool);
      await db
        .insertInto("workers")
        .values({
          id: workerId,
          supervisor_id: workerId,
          boot_id: bootId,
          state: "ready",
          max_concurrent_sessions: 2,
        })
        .execute();
      await registerTestWorker(db, workerId);
      const root = randomUUID();
      await store.register(t, [root]);
      async function enqueue(flow, pos) {
        const id = randomUUID();
        await pool.query(
          "insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state) values($1,$2,$3,$4,$1,'task','fixture','queued')",
          [id, t, flow, pos],
        );
        return id;
      }
      scheduler = await runtime.serveScheduler({ connectionString: url.href, token });
      server = worker.serve({
        scheduler: { url: scheduler.url, token },
        signal: abort.signal,
        onError: (e) => {
          finished.push(e);
        },
        execute: async (job) => {
          // This scheduling-only fixture stands in for Pi's first input placement.
          const m = await runtime.inputs.selectAt(job.scope, "start");
          if (m)
            await pool.query("select segment_input_receipts($1,$2,$3)", [
              job.tenant_id,
              job.id,
              JSON.stringify([{ id: m.id, state: "placed", native_id: 2 }]),
            ]);
          jobs.push(job);
          const r = Promise.withResolvers();
          releases.push(r);
          await r.promise;
        },
      });
      await enqueue(root, 1);
      await waitCount(1);
      const child = await store.delegate(
        t,
        root,
        null,
        randomUUID(),
        "research",
        "research",
        "fixture",
        callers(jobs[0]),
      );
      await waitCount(2);
      const grand = await store.delegate(
        t,
        child,
        null,
        randomUUID(),
        "notes",
        "notes",
        "fixture",
        callers(jobs[1]),
      );
      assert.equal(await worker.claimNext(), undefined);
      assert.equal(jobs[1].family_id, root);
      await close(jobs[0], 6);
      await waitCount(3);
      assert.equal(jobs[2].logical_id, grand);
      assert.equal(jobs[2].parent_session_id, child);
      assert.equal(jobs[2].family_id, root);
      await close(jobs[2], 8);
      await close(jobs[1], 7);
      const flow = await store.retire(t, child, { run: jobs[1].id, lane: 1, entry: 7 });
      const later = await enqueue(flow, 1);
      await waitCount(4);
      assert.equal(jobs[3].id, later);
      assert.equal(jobs[3].logical_id, child);
      assert.equal(jobs[3].flow_id, flow);
      assert.equal(jobs[3].scope.fencingToken, jobs[1].scope.fencingToken + 1);
      assert.equal(
        (
          await pool.query(
            "select count(*)::int n from session_leases where tenant_id=$1 and released_at is null",
            [t],
          )
        ).rows[0].n,
        1,
      );
      await close(jobs[3], 9);
      assert.deepEqual(finished, []);
    } finally {
      abort.abort();
      for (const r of releases) r.resolve();
      await server;
      await worker.close();
      await scheduler?.close();
      await pool.query("drop schema if exists " + schema + " cascade");
      await store.db.destroy();
      await db.destroy();
    }
  },
  60000,
);
