import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { once } from "node:events";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import pg from "pg";
import { it } from "vitest";
import { createDatabase } from "@pi-cloud/database";
import {
  AcceptedFamilyCheckpoint,
  resumeFamilyCheckpoint,
  saveSealedFamily,
} from "@pi-cloud/pi-session-postgres/durable";
import { projectionTemplates } from "./projection-statements.mjs";
import { ReadyProjection } from "./ready-projection.mjs";
import { projectRecord } from "./project-record.mjs";
import { loadSettledDelegations } from "./delegated-cold.mjs";
import { reopenRootProjection } from "./root-projection-handoff.mjs";

const source = process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL;
it.skipIf(!source).each([false, true])(
  "reclaims one root after SIGKILL with a prior settled child family: %s",
  async (withPrior) => {
    const schema = "delegated_handoff_" + randomUUID().replaceAll("-", "");
    const url = new URL(source);
    url.searchParams.set("options", "-c search_path=" + schema);
    const pool = new pg.Pool({ connectionString: url.href }),
      db = createDatabase({ connectionString: url.href, maxConnections: 1 });
    let root = randomUUID(),
      target;
    const session = randomUUID(),
      tenant = randomUUID(),
      topic = "handoff",
      seal = randomUUID();
    let scope = {
      sessionId: session,
      piSessionId: session,
      tenantId: tenant,
      runId: root,
      turnId: root,
      writerId: randomUUID(),
      fencingToken: 1,
    };
    let offset = 0n,
      view,
      checkpoint = new AcceptedFamilyCheckpoint();
    const actors = [];
    const fresh = () => {
      const v = new ReadyProjection(pool, topic, new Map(), projectionTemplates);
      v.delegatedRuns = true;
      return v;
    };
    const env = {
      pool,
      stage: { integrated: true, scene: "plain", sink: "single" },
      metrics: { byPartition: [0], queries: 0, skipped: 0 },
      nextOffsets: new Map(),
      account() {},
      send() {},
      partitions: 1,
    };
    function actor(recover, phase = withPrior ? "continuation" : "normal") {
      const proc = fork(new URL("./delegated-handoff-worker.mjs", import.meta.url), [], {
        execArgv: ["--import", "tsx"],
        stdio: ["ignore", "ignore", "inherit", "ipc"],
      });
      const ended = once(proc, "exit"),
        waiting = Promise.withResolvers(),
        done = Promise.withResolvers(),
        error = Promise.withResolvers();
      const local = scope;
      proc.on("message", async (m) => {
        try {
          if (m.kind === "error") throw Error(m.message);
          if (m.kind === "waiting") {
            waiting.resolve(m.op);
            return;
          }
          if (m.kind === "done") {
            done.resolve(m.seq);
            return;
          }
          assert.equal(m.kind, "commit");
          assert.deepEqual(m.fact.scope, local);
          const pos = { topic, partition: 0, offset: offset++ };
          if (recover && !view.sessions.has(root))
            view.sessions.set(
              root,
              await reopenRootProjection(pool, scope, pos, m.fact.commit.seq, false, true),
            );
          await view.apply(
            [{ ...pos, fact: m.fact }],
            () => true,
            (r, d) => projectRecord(r, d, env),
          );
          checkpoint.observe(m.fact);
          proc.send({ seq: m.fact.commit.seq });
        } catch (e) {
          error.resolve(e);
        }
      });
      const observe = (promise) =>
        Promise.race([
          promise,
          error.promise.then((e) => {
            throw e;
          }),
          ended.then(([code, signal]) => {
            if (code !== 0 || signal) throw Error("Worker exited: " + (signal ?? code));
            return promise;
          }),
        ]);
      actors.push({ proc, ended });
      proc.send({ scope: local, topic, url: url.href, recover, phase, target });
      return {
        proc,
        ended,
        wait: () => observe(waiting.promise),
        done: () => observe(done.promise),
      };
    }
    try {
      await pool.query("create schema " + schema);
      for (const f of ["schema.sql", "delegated-runs.sql"])
        await pool.query(await readFile(new URL(f, import.meta.url), "utf8"));
      await pool.query(`alter table runs add turn_id uuid,add fencing_token bigint,add recovery_requested_at timestamptz,add output_seal_id uuid,add output_first_topic text,add output_first_partition int,add output_first_offset bigint;
      create table session_leases(tenant_id uuid,pi_session_id text,lease_id uuid,fencing_token bigint,writer_sealed_at timestamptz,writer_seal_offset bigint,released_at timestamptz,writer_failed_at timestamptz)`);
      await pool.query("insert into sessions values($1,$2)", [session, tenant]);
      await pool.query(
        "insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,model_ref,initial_input,state,lease_id,turn_id,fencing_token,output_first_topic,output_first_partition,output_first_offset) values($1,$2,$3,1,$1,'fake','test','running',$4,$1,1,$5,0,0)",
        [root, tenant, session, scope.writerId, topic],
      );
      view = fresh();
      await view.install();
      await view.reset(new Map([[0, 0n]]), 16, "recorded-history");
      if (withPrior) {
        const seed = actor(false, "seed"),
          seq = await seed.done();
        assert.equal((await seed.ended)[0], 0);
        target = (await pool.query("select id from runs where root_run_id=$1", [root])).rows[0].id;
        await view.apply(
          [
            {
              topic,
              partition: 0,
              offset: offset++,
              fact: {
                kind: "work_finished",
                factId: randomUUID(),
                scope,
                baseSequence: 0,
                nativeSequence: seq,
                occurredAt: new Date().toISOString(),
                terminal: { type: "turn.completed", payload: { stopReason: "stop" } },
              },
            },
          ],
          () => true,
          (r, d) => projectRecord(r, d, env),
        );
        root = randomUUID();
        scope = { ...scope, runId: root, turnId: root, writerId: randomUUID() };
        await pool.query(
          "insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,model_ref,initial_input,state,lease_id,turn_id,fencing_token,output_first_topic,output_first_partition,output_first_offset) values($1,$2,$3,2,$1,'fake','continue','running',$4,$1,1,$5,0,$6)",
          [root, tenant, session, scope.writerId, topic, String(offset)],
        );
        const base = await loadSettledDelegations(pool, scope);
        checkpoint = AcceptedFamilyCheckpoint.resume(scope.writerId, base.state, base.inboxIds);
      }
      const a = actor(false),
        op = await a.wait();
      a.proc.kill("SIGKILL");
      assert.equal((await a.ended)[1], "SIGKILL");
      const position = { topic, partition: 0, offset: offset++ },
        next = { ...scope, writerId: randomUUID(), fencingToken: 2 };
      await pool.query(
        "update runs set recovery_requested_at=now(),output_seal_id=$1 where id=$2",
        [seal, root],
      );
      await db
        .transaction()
        .execute((tx) => saveSealedFamily(tx, scope, position, seal, checkpoint.snapshot()));
      await pool.query(
        "insert into session_leases values($1,$2,$3,1,now(),$4,now(),now()),($1,$2,$5,2,null,null,null,null)",
        [tenant, session, scope.writerId, String(position.offset), next.writerId],
      );
      await pool.query("update runs set lease_id=$1,fencing_token=2 where id=$2", [
        next.writerId,
        root,
      ]);
      scope = next;
      checkpoint = await resumeFamilyCheckpoint(db, scope, { topic, partition: 0, offset });
      // The control seal covers the preceding native prefix, as in the matched projector.
      await pool.query(
        "update accepted_fact_projection_offsets set next_offset=$1 where topic=$2 and partition=0",
        [String(offset), topic],
      );
      view = fresh();
      view.configure(16, "recorded-history");
      view.next.set(0, offset);
      const b = actor(true);
      assert.equal(await b.wait(), op);
      await b.done();
      assert.equal((await b.ended)[0], 0);
      const rows = (
        await pool.query(
          "select state,native_lane,native_answer_id from runs where root_run_id=$1",
          [root],
        )
      ).rows;
      assert.equal(rows.length, 1);
      assert.equal(rows[0].state, "completed");
      assert(rows[0].native_answer_id);
      assert.equal(
        (
          await pool.query(
            "select count(*)::int n from messages where conversation_id=$1 and role='user'",
            [rows[0].native_lane],
          )
        ).rows[0].n,
        1,
      );
    } finally {
      for (const { proc, ended } of actors) {
        if (proc.exitCode === null && proc.signalCode === null) proc.kill("SIGKILL");
        await ended;
      }
      await db.destroy();
      await pool.query("drop schema if exists " + schema + " cascade");
      await pool.end();
    }
  },
  45000,
);
