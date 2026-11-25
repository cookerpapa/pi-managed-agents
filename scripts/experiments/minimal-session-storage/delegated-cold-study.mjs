import { DelegatedMemoryStorage } from "./delegated-memory.mjs";
// Isolated read-cost assay. Each earlier agent has its own equally sized,
// uncompacted transcript; the next ordinary root has not requested any child.
import assert from "node:assert/strict";
import { studyEntryIndexSql } from "./entry-index.mjs";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { performance } from "node:perf_hooks";
import pg from "pg";
import { loadSettledDelegations } from "./delegated-cold.mjs";
const url = process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL;
assert(url, "Use run-with-test-postgres.mjs");
for (const children of [1, 100, 500]) {
  const schema = "delegated_scale_" + randomUUID().replaceAll("-", "");
  const pool = new pg.Pool({ connectionString: url, options: "-c search_path=" + schema, max: 2 });
  const [tenant, session, root, next, writer] = Array.from({ length: 5 }, () => randomUUID());
  const count = 32,
    all = (1 + children) * count,
    bodyStart = 3 * children + 10;
  try {
    await pool.query("create schema " + schema);
    for (const f of ["schema.sql", "subagent-deliveries.sql", "delegated-runs.sql"])
      await pool.query(await readFile(new URL(f, import.meta.url), "utf8"));
    await pool.query("insert into sessions values($1,$2)", [session, tenant]);
    await pool.query(
      `insert into runs(id,tenant_id,session_id,idempotency_key,model_ref,initial_input,state,mailbox_position,lease_id)
      values($1,$3,$4,$1,'fake','previous','completed',1,null),($2,$3,$4,$2,'fake','current','running',2,$5)`,
      [root, next, tenant, session, writer],
    );
    await pool.query(
      `with lanes as materialized(select n,gen_random_uuid() id from generate_series(2,$4::int+1)n)
      insert into runs(id,tenant_id,session_id,idempotency_key,model_ref,initial_input,state,root_run_id,caller_run_id,agent_id,source_task_id,context_mode,native_lane,native_created_seq,native_owner,native_outcome_seq)
      select id,$1,$2,id,'fake','delegated','completed',$3,$3,id,n,'fresh',n,1,$4::int+4,$5 from lanes`,
      [tenant, session, root, children, all + 1],
    );
    await pool.query(
      `insert into inboxes select $1,$2,l,$3::int+2+l,1,l,'[]',1 from generate_series(1,$3::int+1) l`,
      [tenant, session, children],
    );
    await pool.query(
      `insert into messages(tenant_id,pi_session_id,entry_id,conversation_id,run_id,role,content,commit_seq,ordinal)
      select $1,$2,$4::int+n,1+(n-1)/$5::int,$3,'assistant',json_build_object('id',$4::int+n,'conversationId',1+(n-1)/$5::int,
      'kind','pi.assistant','model',json_build_array(json_build_object('role','assistant','content',json_build_array(json_build_object('type','text','text',repeat('a',512))))))::text,
      n+1,0 from generate_series(1,$6::int)n`,
      [tenant, session, root, bodyStart, count, all],
    );
    await pool.query("update runs set history_head=$1 where id=$2", [
      JSON.stringify({
        version: "scale",
        floor: 1,
        count,
        bytes: Number(
          (
            await pool.query(
              "select sum(octet_length(content)) bytes from messages where conversation_id=1",
            )
          ).rows[0].bytes,
        ),
        seq: all + 1,
        nextId: bodyStart + all + 1,
      }),
      root,
    ]);
    await pool.query("analyze runs; analyze messages; analyze inboxes");
    // Capture the actual body reads without including EXPLAIN in the timings.
    const reads = new Map();
    let phase;
    const measuredPool = {
      async connect() {
        const c = await pool.connect();
        return {
          query(sql, args) {
            if (/\bmessage_entries\b/.test(sql)) reads.set(phase, [sql, args]);
            return c.query(sql, args);
          },
          release: () => c.release(),
        };
      },
    };
    async function measureReads() {
      const times = [],
        mountTimes = [];
      const selected = { lane: 2, at: bodyStart + 2 * count };
      for (let i = 0; i < 6; i++) {
        const store = new DelegatedMemoryStorage(),
          start = performance.now();
        phase = "root";
        const result = await loadSettledDelegations(
          measuredPool,
          { tenantId: tenant, piSessionId: session, runId: next, writerId: writer },
          store,
        );
        if (i) times.push(performance.now() - start);
        assert.equal(result.entries, count);
        assert.equal(store.archiveReads?.length ?? 0, 0);
        phase = "source";
        const mountStart = performance.now();
        await Promise.all([store.ensureSource(selected), store.ensureSource(selected)]);
        if (i) mountTimes.push(performance.now() - mountStart);
        assert.deepEqual(store.archiveReads, [{ ...selected, entries: count }]);
        await store.ensureSource(selected);
        assert.equal(store.archiveReads.length, 1);
      }
      times.sort((a, b) => a - b);
      mountTimes.sort((a, b) => a - b);
      return {
        medianMs: +times[2].toFixed(1),
        maximumMs: +times.at(-1).toFixed(1),
        mountMedianMs: +mountTimes[2].toFixed(1),
        mountMaximumMs: +mountTimes.at(-1).toFixed(1),
        samples: times.length,
      };
    }
    async function explainReads() {
      const plans = {};
      for (const [label, [sql, args]] of reads) {
        const { rows } = await pool.query("explain (analyze,buffers,format json) " + sql, args);
        const plan = rows[0]["QUERY PLAN"][0].Plan;
        plans[label] = {
          rows: plan["Actual Rows"],
          bufferAccesses: plan["Shared Hit Blocks"] + plan["Shared Read Blocks"],
        };
      }
      assert(plans.root && plans.source);
      return plans;
    }
    if (process.argv.includes("--compare-index"))
      await pool.query("drop index messages_conversation_entry");
    const timings = await measureReads();
    const plans = await explainReads();
    let withTemporaryIndex, indexTimings, baselineRepeat;
    if (children === 500 && process.argv.includes("--compare-index")) {
      await pool.query(studyEntryIndexSql);
      await pool.query("analyze messages");
      indexTimings = await measureReads();
      withTemporaryIndex = await explainReads();
      await pool.query("drop index messages_conversation_entry");
      baselineRepeat = await measureReads();
    }
    console.log(
      JSON.stringify({
        children,
        storedEntries: all,
        rootEntries: count,
        mountedEntries: count,
        ...timings,
        bodyBytes: 512,
        plans,
        withTemporaryIndex,
        indexTimings,
        baselineRepeat,
      }),
    );
  } finally {
    await pool.query("drop schema if exists " + schema + " cascade");
    await pool.end();
  }
}
