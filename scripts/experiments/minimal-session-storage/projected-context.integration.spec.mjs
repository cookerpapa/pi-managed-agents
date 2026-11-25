import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import pg from "pg";
import { it } from "vitest";
import { Kysely, PostgresDialect, sql } from "kysely";
import { estimateStoredContext } from "@pi-cloud/pi-session-postgres/durable";
import { readProjectedContextEstimate } from "../../../packages/control-plane/src/projected-context.ts";
import { receivingConversationRunId } from "@pi-cloud/runtime-core/run-readiness";

const uri = process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL;
it.skipIf(!uri)(
  "reads only the visible compacted prefix for fresh, branched and continued children; addresses root input by queue position",
  async () => {
    const schema = "context_" + randomUUID().replaceAll("-", "");
    const pool = new pg.Pool({
      connectionString: uri,
      options: "-c search_path=" + schema,
      max: 2,
    });
    let pages = 0;
    const db = new Kysely({
      dialect: new PostgresDialect({ pool }),
      log: (e) => {
        if (e.level === "query" && e.query.sql.includes("with page as materialized")) pages++;
      },
    });
    const [tenant, root, r1, r2, child, continuation, sibling] = Array.from({ length: 7 }, () =>
      randomUUID(),
    );
    const model = { provider: "test", modelId: "local" };
    const user = (id, lane, text) => ({
      id,
      conversationId: lane,
      kind: "pi.user",
      model: [{ role: "user", content: text, timestamp: id }],
    });
    const answer = (id, lane, total) => ({
      id,
      conversationId: lane,
      kind: "pi.assistant",
      model: [
        {
          role: "assistant",
          provider: model.provider,
          model: model.modelId,
          stopReason: "stop",
          content: [{ type: "text", text: "Done" }],
          timestamp: id,
          usage: { input: total - 1, output: 1, totalTokens: total, cacheRead: 0, cacheWrite: 0 },
        },
      ],
    });
    try {
      await pool.query("create schema " + schema);
      for (const f of ["schema.sql", "subagent-deliveries.sql", "delegated-runs.sql"])
        await pool.query(await readFile(new URL(f, import.meta.url), "utf8"));
      await pool.query(
        "alter table runs add pruned_at timestamptz,add recovery_requested_at timestamptz,add output_seal_id uuid,add output_sealed_at timestamptz",
      );
      await pool.query("insert into sessions values($1,$2)", [root, tenant]);
      for (const [i, id] of [r1, r2].entries())
        await pool.query(
          "insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state) values($1,$2,$3,$4,$1,'input','test','completed')",
          [id, tenant, root, i + 1],
        );
      const putChild = (id, base, previous, owner, task, mode, at, lane, state) =>
        pool.query(
          "insert into runs(id,tenant_id,session_id,idempotency_key,initial_input,model_ref,state,root_run_id,caller_run_id,agent_id,previous_run_id,source_task_id,context_mode,context_at,native_lane) values($1,$2,$3,$1,'task','test',$4,$5,$5,$6,$7,$8,$9,$10,$11)",
          [id, tenant, root, state, owner, base, previous, task, mode, at, lane],
        );
      await putChild(child, child, null, r1, 10, "branch", 4100, 3, "completed");
      await putChild(continuation, child, child, r2, 10, "continue", 4115, 4, "completed");
      await putChild(sibling, sibling, null, r2, 11, "fresh", null, 5, "unknown");
      const receiving = async () =>
        (await sql`select ${receivingConversationRunId(tenant, root)} as id`.execute(db)).rows[0]
          .id;
      assert.equal(await receiving(), r2);
      const queued = randomUUID();
      await pool.query(
        "insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state) values($1,$2,$3,3,$1,'next','test','queued')",
        [queued, tenant, root],
      );
      assert.equal(await receiving(), queued);
      // A long prefix precedes the compaction floor. The page count below detects a lifetime scan.
      await pool.query(
        `insert into messages(tenant_id,pi_session_id,entry_id,conversation_id,run_id,role,content,commit_seq,ordinal)
        select $1,$2,n,1,$3,'user',json_build_object('id',n,'conversationId',1,'kind','pi.user','model',json_build_array(json_build_object('role','user','content',repeat('x',1024),'timestamp',n)))::text,n,0 from generate_series(1,4000) n`,
        [tenant, root, r1],
      );
      const summary = { ...user(4099, 1, "summary"), kind: "pi.compaction", head: 4097 };
      const prefix = [user(4097, 1, "kept across compaction"), summary, answer(4100, 1, 320)];
      const a = [user(4110, 3, "child task"), answer(4115, 3, 121)];
      const laterParent = answer(4130, 1, 913),
        lateChild = answer(4155, 3, 651);
      const local = Array.from({ length: 75 }, (_, i) => user(4200 + i, 4, "message " + i));
      for (const e of [...prefix, ...a, laterParent, lateChild, ...local]) {
        await pool.query(
          "insert into messages(tenant_id,pi_session_id,entry_id,conversation_id,run_id,role,content,commit_seq,ordinal) values($1,$2,$3,$4,$5,$6,$7,$3,0)",
          [
            tenant,
            root,
            e.id,
            e.conversationId,
            e.conversationId === 4 ? r2 : r1,
            e.model[0].role,
            JSON.stringify(e),
          ],
        );
      }
      const status = (invocation, selected = model) =>
        db
          .transaction()
          .setIsolationLevel("repeatable read")
          .execute((tx) =>
            readProjectedContextEstimate(
              tx,
              { tenant, publicRoot: root, physical: root, fork: false, invocation },
              selected,
            ),
          );
      assert.deepEqual(await status(), estimateStoredContext([...prefix, laterParent], model));
      assert.deepEqual(
        await status(child),
        estimateStoredContext([...prefix, ...a, lateChild], model),
      );
      assert.deepEqual(await status(sibling), { method: "structural", estimatedTokens: 0 });
      const other = { provider: "other", modelId: "local" },
        before = pages;
      assert.deepEqual(
        await status(continuation, other),
        estimateStoredContext([...prefix, ...a, ...local], other),
      );
      assert.equal(pages - before, 2);
      assert.deepEqual(
        await status(continuation),
        estimateStoredContext([...prefix, ...a, ...local], model),
      );
    } finally {
      try {
        await pool.query("drop schema if exists " + schema + " cascade");
      } finally {
        await db.destroy();
      }
    }
  },
);
