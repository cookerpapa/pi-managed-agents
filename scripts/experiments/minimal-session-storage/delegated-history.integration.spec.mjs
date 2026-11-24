import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { it } from "vitest";
import pg from "pg";
import { createDatabase } from "@pi-cloud/database";
import { DelegatedHistoryReader } from "../../../packages/control-plane/src/delegated-history-reader.ts";
const source = process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL;
it.skipIf(!source)(
  "scopes the public directory and pages own entries across two invocations",
  async () => {
    const schema = "delegated_web_" + randomUUID().replaceAll("-", "");
    const url = new URL(source);
    url.searchParams.set("options", "-c search_path=" + schema);
    const pool = new pg.Pool({ connectionString: url.href, max: 1 }),
      db = createDatabase({ connectionString: url.href, maxConnections: 2 });
    try {
      await pool.query("create schema " + schema);
      for (const f of ["schema.sql", "delegated-runs.sql"])
        await pool.query(await readFile(new URL(f, import.meta.url), "utf8"));
      const [tenant, root, r1, r2] = Array.from({ length: 4 }, () => randomUUID());
      await pool.query("insert into sessions values($1,$2)", [root, tenant]);
      await pool.query(
        "insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state) values($1,$3,$4,1,$1,'r1','fake','completed'),($2,$3,$4,2,$2,'r2','fake','running')",
        [r1, r2, tenant, root],
      );
      await pool.query(
        `with agents as materialized(select n,gen_random_uuid() as id from generate_series(1,26)n)
     insert into runs(id,tenant_id,session_id,idempotency_key,initial_input,model_ref,state,root_run_id,caller_run_id,agent_id,source_task_id,context_mode,native_lane,native_created_seq,native_owner)
     select id,$1,$2,id,'job '||n,'fake','completed',$3,$3,id,n,'fresh',n+2,1,2 from agents`,
        [tenant, root, r1],
      );
      const {
        rows: [{ id: a }],
      } = await pool.query("select id from runs where native_lane=3");
      const b = randomUUID();
      await pool.query(
        `insert into runs(id,tenant_id,session_id,idempotency_key,initial_input,model_ref,state,root_run_id,caller_run_id,agent_id,previous_run_id,source_task_id,context_mode,context_at,native_lane,native_created_seq,native_owner)
     values($1,$2,$3,$1,'continue','fake','completed',$4,$4,$5,$5,7,'continue',18,32,19,8)`,
        [b, tenant, root, r2, a],
      );
      await pool.query(
        `insert into messages(tenant_id,pi_session_id,entry_id,conversation_id,run_id,role,commit_seq,ordinal,content)
     select $1,$2,n,case when n<=18 then 3 else 32 end,case when n<=18 then $3::uuid else $4::uuid end,'assistant',(n+1)/2,n%2,
       json_build_object('kind','pi.assistant','id',n,'model',json_build_array(json_build_object('role','assistant','content',json_build_array(json_build_object('type','thinking','thinking','hidden'),json_build_object('type','text','text','public '||n)))))::text from generate_series(1,45)n`,
        [tenant, root, r1, r2],
      );
      const reader = new DelegatedHistoryReader(db);
      const first = await reader.directory(tenant, root),
        rest = await reader.directory(tenant, root, first.nextCursor);
      assert.equal(new Set([...first.agents, ...rest.agents].map((x) => x.agentId)).size, 26);
      const page = await reader.history(tenant, root, a),
        older = await reader.history(tenant, root, a, page.before);
      assert(page.before && !older.before);
      const rows = [...older.entries, ...page.entries];
      assert.equal(rows.length, 45);
      assert.equal(new Set(rows.map((e) => e.entryId)).size, 45);
      assert.deepEqual(new Set(rows.map((e) => e.runId)), new Set([a, b]));
      assert(!JSON.stringify(rows).includes("hidden"));
      assert(!(await reader.directory(randomUUID(), root)).agents.length);
      await assert.rejects(reader.history(randomUUID(), root, a), /unavailable/);
      await assert.rejects(
        reader.history(
          tenant,
          root,
          first.agents.find((x) => x.agentId !== a).agentId,
          page.before,
        ),
        /cursor/,
      );
      // The continuation's result can refer to a proposal outside this page.
      const call = {
        id: 1001,
        kind: "pi.assistant",
        model: [
          {
            role: "assistant",
            stopReason: "toolUse",
            content: [{ type: "toolCall", id: "one", name: "bash", arguments: { command: "pwd" } }],
          },
        ],
      };
      const result = {
        id: 1003,
        kind: "pi.tool-result",
        byTaskId: 1002,
        model: [
          {
            role: "toolResult",
            toolCallId: "one",
            toolName: "bash",
            content: [{ type: "text", text: "done" }],
          },
        ],
      };
      await pool.query(
        `insert into messages(tenant_id,pi_session_id,run_id,conversation_id,entry_id,commit_seq,ordinal,role,content,assistant_entry_id,tool_task_id)
        values($1,$2,$3,32,1001,1,99,'assistant',$4,null,null),($1,$2,$3,32,1003,1003,0,'toolResult',$5,1001,1002)`,
        [tenant, root, r2, JSON.stringify(call), JSON.stringify(result)],
      );
      const resultInput = async () => {
        const entries = (await reader.history(tenant, root, a)).entries;
        assert(!entries.some((e) => e.entryId === 1001));
        const item = entries.at(-1).items.find((i) => i.kind === "tool");
        assert(item);
        return item.input;
      };
      assert.deepEqual(await resultInput(), { command: "pwd" });
      for (const [whichRun, lane] of [
        [r1, 32],
        [r2, 4],
      ]) {
        await pool.query("update messages set run_id=$1,conversation_id=$2 where entry_id=1001", [
          whichRun,
          lane,
        ]);
        assert.equal(await resultInput(), null);
      }
    } finally {
      await db.destroy();
      await pool.query("drop schema if exists " + schema + " cascade");
      await pool.end();
    }
  },
);
