// Disposable PG, deterministic message sizes, optional previous checkout for paired measurements.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { performance } from "node:perf_hooks";
import pg from "pg";
import { testPostgres } from "../../lib/test-postgres.mjs";
import { MemoryStorage, Harness, createRegistry } from "@earendil-works/pi-durable";
import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import { createModels } from "@earendil-works/pi-ai/models";
import { projectionTemplates } from "./projection-statements.mjs";
import { installPrefixSql } from "./ready-projection-sql.mjs";
import * as segments from "./fork-storage.mjs";

const fixture = await testPostgres({});
const reference = process.argv[2];
const sizes = [10, 100, 1024];
const dbs = [];
const t = randomUUID(),
  source = randomUUID();
const count = 102400,
  runLength = 128,
  lastEntry = count + 4;
const times = (s) => {
  const values = s.toSorted((a, b) => a - b);
  return Object.fromEntries(
    [50, 95, 99].map((p) => [
      "p" + p,
      Number(values[Math.ceil((values.length * p) / 100) - 1].toFixed(2)),
    ]),
  );
};
const timed = async (fn) => {
  const start = performance.now();
  const value = await fn();
  return [value, performance.now() - start];
};
async function seed(db, repo, segment, size, runIds) {
  const mod = segment ? segments : await import(new URL("fork-storage.mjs", repo));
  await db.query(await readFile(new URL("schema.sql", repo), "utf8"));
  await installPrefixSql(
    db,
    segment
      ? projectionTemplates
      : (await import(new URL("projection-statements.mjs", repo))).projectionTemplates,
  );
  if (segment) await mod.installForkTables(db);
  else await db.query(await readFile(new URL("fork-layout.sql", repo), "utf8"));
  await mod.setForkLayout(db, true, true);
  const storage = new mod.ForkStorage(db);
  await storage.register(t, [source]);
  await db.query("alter table messages alter column content set storage external");
  await db.query(
    `insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,history_head)
    select id,$1,$2,i,id,'benchmark','faux','completed',jsonb_build_object('floor',1,'nextId',i*$4::int+5,'seq',i*$4::int+4) from unnest($3::uuid[]) with ordinality u(id,i)`,
    [t, source, runIds, runLength],
  );
  if (!segment)
    await db.query("insert into fork_completed_runs select $1,$2,unnest($3::uuid[])", [
      t,
      source,
      runIds,
    ]);
  const filler = Math.ceil((size * 1024 * 1024) / count);
  for (let start = 1; start <= count; start += 2048) {
    await db.query(
      `insert into messages(tenant_id,pi_session_id,conversation_id,entry_id,run_id,role,commit_seq,ordinal,content)
      select $1,$2,1,n+4,($3::uuid[])[1+(n-1)/$6],case when n%$6=0 then 'assistant' else 'user' end,n+4,0,
      json_build_object('id',n+4,'conversationId',1,'kind',case when n=$7::int-3 then 'pi.compaction' when n%$6=0 then 'pi.assistant' else 'pi.user' end,
        'model',json_build_array(json_build_object('role',case when n%$6=0 then 'assistant' else 'user' end,'content',json_build_array(json_build_object('type','text','text',repeat('x',$5))), 'stopReason','stop','timestamp',n)))::jsonb ||
        case when n=$7::int-3 then jsonb_build_object('head',$7::int-8) else '{}'::jsonb end
      from generate_series($4::int,least($4::int+2047,$7::int)) n`,
      [t, source, runIds, start, filler, runLength, count],
    );
  }
  await db.query(
    "analyze runs; analyze messages; analyze flows".replace(
      "analyze flows",
      segment ? "analyze flows" : "analyze pi_history_streams",
    ),
  );
  return { db, storage, layout: segment ? "segments" : "tree", segment };
}
// One ordered writer with eight immutable entries in each accepted PG call.
// This isolates the impact of the directory's fan-out on the ordinary append path.
async function profileAppend(db, run, writer, first) {
  const rowsPerCall = 8,
    calls = 160,
    started = performance.now();
  for (let i = 0; i < calls; i++) {
    const sequence = first + i;
    const messages = Array.from({ length: rowsPerCall }, (_, ordinal) => {
      const id = sequence * rowsPerCall + ordinal;
      const role = ordinal % 2 ? "toolResult" : "assistant";
      return {
        id,
        ordinal,
        role,
        conversation: 1,
        content: JSON.stringify({
          id,
          conversationId: 1,
          kind: "pi." + role,
          model: [{ role, content: "x".repeat(512) }],
        }),
      };
    });
    const result = await db.query("select * from prefix_message($1,$2,$3,$4,$5,$6)", [
      t,
      source,
      run,
      sequence,
      JSON.stringify(messages),
      writer,
    ]);
    assert.deepEqual(result.rows, [{ accepted: true, writes: rowsPerCall }]);
  }
  return Math.round((1000 * calls * rowsPerCall) / (performance.now() - started));
}
async function mixedDepth(c, initial) {
  let parent = source,
    point = initial;
  for (let depth = 1; depth <= 16; depth++) {
    const [id, ms] = await timed(() => c.storage.fork(t, parent, point));
    const nextRun = randomUUID(),
      hi = lastEntry + depth * 33;
    await c.db.query(
      `insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,history_head)
      values($1,$2,$3,1,$1,'next','faux','completed',jsonb_build_object('nextId',$4::bigint+1))`,
      [nextRun, t, id, hi],
    );
    await c.db.query(
      `insert into messages(tenant_id,pi_session_id,conversation_id,entry_id,run_id,role,commit_seq,ordinal,content)
      select $1,$2,1,n,$3,'assistant',n,0,json_build_object('id',n,'conversationId',1,'kind','pi.assistant','model',
        json_build_array(json_build_object('role','assistant','content','depth','stopReason','stop')))::text
      from generate_series($4::int-32,$4::int)n`,
      [t, id, nextRun, hi],
    );
    if ([1, 4, 16].includes(depth)) {
      const [rows, pageMs] = await timed(() => c.storage.page(t, id, 70));
      assert.equal(rows.length, 70);
      assert.equal(Number(rows[0].entry_id), hi);
      console.log(
        JSON.stringify({
          depth,
          createMs: +ms.toFixed(2),
          rows: rows.length,
          pageMs: +pageMs.toFixed(2),
        }),
      );
    }
    parent = id;
    point = { run: nextRun, entry: hi, lane: 1 };
  }
  const clipped = await c.storage.fork(t, parent, initial);
  assert.equal((await c.storage.page(t, clipped, 1))[0].entry_id, String(initial.entry));
}
try {
  for (const size of sizes) {
    const runIds = Array.from({ length: count / runLength }, () => randomUUID());
    const cases = [];
    for (const [label, repo] of [
      ["segments", new URL("./", import.meta.url)],
      ...(reference
        ? [
            [
              "tree",
              pathToFileURL(
                resolve(reference, "scripts/experiments/minimal-session-storage") + "/",
              ),
            ],
          ]
        : []),
    ]) {
      const schema = "bench_" + label + size;
      const db = new pg.Pool({
        connectionString: fixture.url,
        options: "-c search_path=" + schema,
        max: 10,
      });
      dbs.push(db);
      await db.query("create schema " + schema);
      cases.push(await seed(db, repo, label === "segments", size, runIds));
    }
    const samples = new Map(cases.map((c) => [c, { create: [], read: [], grandchild: [] }]));
    const point = { entry: lastEntry, run: runIds.at(-1), lane: 1 };
    for (let trial = 0; trial < 21; trial++)
      for (const c of trial % 2 ? cases.toReversed() : cases) {
        const [id, forkMs] = await timed(() => c.storage.fork(t, source, point));
        const [rows, pageMs] = await timed(() => c.storage.page(t, id, 30));
        assert.equal(Number(rows[0].entry_id), lastEntry);
        const [, nestedMs] = await timed(() => c.storage.fork(t, id, point));
        if (trial) {
          const s = samples.get(c);
          s.create.push(forkMs);
          s.read.push(pageMs);
          s.grandchild.push(nestedMs);
        }
      }
    for (const c of cases) {
      const samplesForCase = samples.get(c);
      const bytes = Number(
        (await c.db.query("select sum(octet_length(content)) bytes from messages")).rows[0].bytes,
      );
      console.log(
        JSON.stringify({
          sizeMiB: size,
          bytes,
          layout: c.layout,
          entries: count,
          samples: 20,
          forkMs: times(samplesForCase.create),
          pageMs: times(samplesForCase.read),
          nestedMs: times(samplesForCase.grandchild),
        }),
      );
      if (size !== 1024 || !c.segment) continue;
      const webTx = [];
      for (let i = 0; i < 21; i++) {
        const [result, ms] = await timed(() =>
          c.db.query("select fork_web_window($1,$2,$3,$4,$5,$6,$7) ok", [
            t,
            source,
            point.run,
            point.entry,
            randomUUID(),
            randomUUID(),
            "benchmark",
          ]),
        );
        assert.equal(result.rows[0].ok, true);
        if (i) webTx.push(ms);
      }
      console.log(JSON.stringify({ sealedWebForkSqlMs: times(webTx), samples: webTx.length }));
      await mixedDepth(c, point);
      const head = await c.storage.fork(t, source, point);
      const memory = new MemoryStorage();
      const [active, coldMs] = await timed(async () => {
        await c.storage.hydrate(t, head, memory);
        const h = await Harness.open(
          memory,
          { registry: createRegistry(), models: createModels() },
          ctx,
        );
        try {
          return (await (await h.root(ctx)).context(ctx)).messages.length;
        } finally {
          await h.close(ctx);
        }
      });
      assert(active > 0 && active < 32);
      // 1,000 independently named child Sessions at the same accepted cutoff.
      const parent = runIds.at(-1),
        writer = randomUUID();
      await c.db.query("update runs set state='running',lease_id=$2 where id=$1", [parent, writer]);
      const startSeq = 2 * count,
        samplesBefore = [],
        samplesAfter = [];
      for (let i = 0; i < 5; i++)
        samplesBefore.push(await profileAppend(c.db, parent, writer, startSeq + i * 160));
      const lat = [],
        begin = performance.now();
      const ids = await Promise.all(
        Array.from({ length: 1000 }, async (_, i) => {
          const [id, ms] = await timed(() =>
            c.storage.delegate(t, source, point, randomUUID(), "child-" + i, "inspect", "fake", {
              run: parent,
              writer,
            }),
          );
          lat.push(ms);
          return id;
        }),
      );
      const rows = (
        await c.db.query(
          "select (select count(*)::int from flow_segments where destination_flow=any($1::uuid[])) segments,(select count(*)::int from messages where pi_session_id=any($1::uuid[])) copied,(select count(*)::int from runs where session_id=any($1::uuid[])) initial",
          [ids],
        )
      ).rows[0];
      assert.deepEqual(rows, { segments: 1000, copied: 0, initial: 1000 });
      const elapsedMs = Math.round(performance.now() - begin);
      for (let i = 0; i < 5; i++)
        samplesAfter.push(await profileAppend(c.db, parent, writer, startSeq + (i + 5) * 160));
      console.log(
        JSON.stringify({
          appendRowsPerSecond: { before: samplesBefore.slice(1), after: samplesAfter.slice(1) },
          rowsPerCall: 8,
        }),
      );
      console.log(
        JSON.stringify({
          fanout: ids.length,
          pool: 10,
          elapsedMs,
          latencyMs: times(lat),
          activeContext: active,
          coldPiMs: Number(coldMs.toFixed(2)),
          ...rows,
        }),
      );
    }
  }
} finally {
  for (const db of dbs) await db.end();
  await fixture.close();
}
