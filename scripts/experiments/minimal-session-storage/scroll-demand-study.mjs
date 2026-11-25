// A primary-only fixture. CDP wheel input follows the actual Web timeline.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import pg from "pg";
import { profileDependencyJoins } from "./scroll-dependency-probe.mjs";
import { withPagingFixture } from "./flow-paging-fixture.mjs";
import { pageCpuSnapshot, pageCpuDelta } from "./page-backend-cpu.mjs";
const round = (n) => +n.toFixed(3);

export async function measureWheelDemand(
  api,
  { id = api.windows[1], marker = "root:99970", shape = "text", checkToolRows = false } = {},
) {
  const { chrome } = api,
    u = new URL(api.address);
  u.searchParams.set("session", id);
  const geo = () =>
    chrome.evaluate(`(()=>{const e=document.querySelector('.product-chat-scroll'),r=e.getBoundingClientRect();
     return {top:Math.round(e.scrollTop),height:e.clientHeight,full:e.scrollHeight,x:Math.round(r.left+r.width*.55),y:Math.round(r.top+r.height*.55)};})()`);
  // CDP events and timestamps stay in the observer. Only aggregate metrics are published.
  let lastBefore;
  const allCursors = new Set();
  let current,
    phaseStart,
    active = new Set(),
    unexpected = 0;
  const phases = [];
  const injected = await chrome.send("Page.addScriptToEvaluateOnNewDocument", {
    source: `(()=>{
   const original=window.fetch;window.__olderPositions=[];window.__historyWheelCount=0;
   document.addEventListener('wheel',()=>window.__historyWheelCount++,{capture:true,passive:true});
   window.fetch=function(input,...rest){const u=new URL(typeof input==="string"?input:input.url??String(input),location.href);
     if(u.pathname===${JSON.stringify("/v1/conversations/" + id)} && u.searchParams.has("before")){
       const el=document.querySelector(".product-chat-scroll");if(el)window.__olderPositions.push(Math.round(el.scrollTop));
     }
     return original.call(this,input,...rest);
   };
 })()`,
  });
  const off = chrome.onNetworkEvent((method, m) => {
    if (method === "Network.requestWillBeSent" && current) {
      const url = new URL(m.request.url);
      if (url.pathname !== "/v1/conversations/" + id) return;
      const cursor = url.searchParams.get("before");
      if (!cursor) {
        current.other++;
        return;
      }
      lastBefore = cursor;
      allCursors.add(cursor);
      const at = performance.now();
      current.before++;
      current.cursors.add(cursor);
      current.offsets.push(round(at - phaseStart));
      active.add(m.requestId);
      current.peak = Math.max(current.peak, active.size);
    }
    if (
      method === "Network.responseReceived" &&
      active.has(m.requestId) &&
      m.response.status !== 200
    )
      unexpected++;
    if (method === "Network.loadingFinished" || method === "Network.loadingFailed") {
      if (active.has(m.requestId) && method === "Network.loadingFailed") unexpected++;
      active.delete(m.requestId);
    }
  });
  try {
    await chrome.navigate(u.href);
    await chrome.waitFor(
      "document.body.innerText.includes(" +
        JSON.stringify(marker) +
        ") && document.querySelector('.product-chat-scroll')",
      30000,
    );
    await delay(700);
    const initial = await geo();
    assert(initial.full >= initial.height);
    const profiles =
      shape === "one-long-run"
        ? [
            { name: "idle", count: 0, wait: 4000 },
            { name: "one-notch", count: 1, step: 0.17, interval: 100, wait: 1500 },
            { name: "no-further-input", count: 0, wait: 12000 },
            ...(checkToolRows
              ? [{ name: "reach-older-edge", count: 1, step: 1, interval: 100, wait: 3500 }]
              : []),
          ]
        : [
            { name: "idle", count: 0, wait: 5000 },
            ...(shape === "20-tools"
              ? [{ name: "one-notch", count: 1, step: 0.17, interval: 100, wait: 2500 }]
              : []),
            { name: "read", count: 40, step: 0.75, interval: 750 },
            { name: "fast", count: 100, step: 1, interval: 110 },
            { name: "pause", count: 0, wait: 3500 },
          ];
    for (const profile of profiles) {
      await chrome.evaluate("window.__olderPositions=[]");
      const startGeo = await geo();
      const delta =
        profile.name === "reach-older-edge"
          ? startGeo.top - 200
          : Math.round(initial.height * (profile.step ?? 0));
      phaseStart = performance.now();
      current = {
        name: profile.name,
        before: 0,
        other: 0,
        peak: 0,
        cursors: new Set(),
        offsets: [],
      };
      for (let i = 0; i < profile.count; i++) {
        await chrome.send("Input.dispatchMouseEvent", {
          type: "mouseWheel",
          x: initial.x,
          y: initial.y,
          deltaX: 0,
          deltaY: -delta,
        });
        await delay(Math.max(0, phaseStart + (i + 1) * profile.interval - performance.now()));
      }
      await delay(profile.wait ?? 550);
      const endGeo = await geo(),
        ms = performance.now() - phaseStart;
      phases.push({
        name: current.name,
        gestures: profile.count,
        nominalGesturePx: delta,
        seconds: round(ms / 1000),
        olderPages: current.before,
        olderPerSecond: round((1000 * current.before) / ms),
        otherHistoryGets: current.other,
        distinctCursors: current.cursors.size,
        maxOutstanding: current.peak,
        requestScrollTopPx: await chrome.evaluate("window.__olderPositions"),
        startScrollTop: startGeo.top,
        endScrollTop: endGeo.top,
        requestOffsetsMs: current.offsets,
      });
      current = undefined;
    }
    assert.equal(unexpected, 0);
    if (shape === "text") assert.equal(phases[0].olderPages, 0);
    if (shape !== "one-long-run") assert(phases.find((p) => p.name === "fast").olderPages > 0);
    const last =
      shape === "one-long-run" && lastBefore
        ? await api.page(id, { before: lastBefore })
        : undefined;
    const wheelEvents = await chrome.evaluate("window.__historyWheelCount");
    console.log(
      JSON.stringify({
        kind: "wheel-demand",
        shape,
        wheelEvents,
        distinctOlderPages: allCursors.size,
        ...(last ? { lastPageHasBefore: last.history.hasBefore } : {}),
        viewportPx: initial.height,
        initialScrollHeight: initial.full,
        prefetchPx: Math.max(320, initial.height) + 250,
        phases,
      }),
    );
    if (checkToolRows) {
      assert.deepEqual(
        phases.map((p) => p.olderPages),
        [0, 0, 0, 1],
      );
      assert(last.history.hasBefore, "The edge gesture must leave older pages available");
    }
    return { id, phases };
  } finally {
    off();
    await chrome.send("Page.removeScriptToEvaluateOnNewDocument", {
      identifier: injected.identifier,
    });
  }
}

const snapSql = `select blks_hit::float8 hits,blks_read::float8 reads,blk_read_time::float8 read_ms,temp_bytes::float8 temp_bytes
 from pg_stat_database where datname=current_database()`;
async function ioSnap(db) {
  const r = (await db.query(snapSql)).rows[0];
  const body = (await db.query("select pg_read_file('/sys/fs/cgroup/io.stat') body")).rows[0].body;
  const storage = {};
  for (const line of body.trim().split("\n").filter(Boolean)) {
    const [dev, ...fields] = line.trim().split(/\s+/);
    const attrs = Object.fromEntries(fields.map((f) => f.split("=")));
    storage[dev] = Number(attrs.rbytes ?? 0);
  }
  return { r, storage, cpu: await pageCpuSnapshot(db) };
}
function ioDelta(before, after, n) {
  const delta = (k) => after.r[k] - before.r[k],
    cpu = pageCpuDelta(before.cpu, after.cpu);
  return {
    pages: n,
    historyCpuMsPerPage: round(cpu.history / n),
    controlCpuMsPerPage: round(cpu.control / n),
    sharedHits: delta("hits"),
    sharedReads: delta("reads"),
    sharedReadMs: round(delta("read_ms")),
    tempBytes: delta("temp_bytes"),
    deviceReadBytes: Object.fromEntries(
      Object.entries(after.storage).map(([k, v]) => [k, v - (before.storage[k] ?? 0)]),
    ),
  };
}
export async function measurePageIo(api, url) {
  const { db, page } = api,
    id = api.windows[0],
    q = { before: (await page(id, {})).history.before };
  const diagUrl = new URL(url);
  diagUrl.searchParams.set("options", "-c search_path=flow_web,flow_study,public");
  const diag = new pg.Client({
    connectionString: diagUrl.href,
    application_name: "scroll-io-observer",
  });
  await diag.connect();
  try {
    await db.query("create extension if not exists pg_buffercache with schema flow_study");
    assert.equal((await db.query("show track_io_timing")).rows[0].track_io_timing, "on");
    const indexStats = async () =>
      Object.fromEntries(
        (
          await db.query(
            "select indexrelname,idx_scan::float8 n from pg_stat_user_indexes where schemaname='flow_study' and relname in ('messages','runs')",
          )
        ).rows.map((r) => [r.indexrelname, r.n]),
      );
    const sample = async (name, queries) => {
      await api.flushStats();
      const before = await ioSnap(db),
        ix = await indexStats();
      for (const input of queries) {
        const p = await page(id, input);
        assert.equal(p.turns.length, 32);
      }
      await api.flushStats();
      const after = await ioSnap(db),
        iy = await indexStats();
      console.log(
        JSON.stringify({
          kind: "io",
          name,
          ...ioDelta(before, after, queries.length),
          indexScans: Object.fromEntries(
            Object.entries(iy)
              .map(([k, n]) => [k, n - ix[k]])
              .filter(([, n]) => n > 0),
          ),
        }),
      );
    };
    for (let i = 0; i < 12; i++) await page(id, q);
    await sample("same-hot-page", Array(128).fill(q));
    const pages = [];
    let query = q;
    for (let i = 0; i < 96; i++) {
      const p = await page(id, query);
      pages.push(query);
      query = { before: p.history.before };
      assert(query.before);
    }
    await db.query("checkpoint");
    const evict =
      await db.query(`with victims as materialized(select bufferid from flow_study.pg_buffercache b join pg_class c on pg_relation_filenode(c.oid)=b.relfilenode
     where b.reldatabase=(select oid from pg_database where datname=current_database()) and c.relnamespace='flow_study'::regnamespace and c.relkind in ('r','i'))
     select count(*)::int targeted,count(*) filter(where flow_study.pg_buffercache_evict(bufferid))::int evicted from victims`);
    assert(evict.rows[0].evicted > 0);
    console.log(JSON.stringify({ kind: "pg-buffer-eviction", ...evict.rows[0] }));
    await sample("96-distinct-first-pass", pages);
    await sample("96-distinct-second-pass", pages);
    for (let i = 0; i < 6; i++) await page(id, q);
    const { stages } = await api.sqlCpu(id, q);
    console.log(
      JSON.stringify({
        kind: "older-page-SQL",
        count: stages.reduce((sum, s) => sum + s.calls, 0) / 48,
        stages,
      }),
    );
    const { statements } = await api.capture(id, q),
      plans = [];
    for (const source of statements) {
      const top = (
        await diag.query(
          "explain(analyze,buffers,timing off,format json) " + source.text,
          source.values,
        )
      ).rows[0]["QUERY PLAN"][0];
      const nodes = [];
      function visit(p) {
        if (p["Relation Name"])
          nodes.push({
            type: p["Node Type"],
            relation: p["Relation Name"],
            index: p["Index Name"],
            rows: p["Actual Rows"],
            loops: p["Actual Loops"],
            removed: p["Rows Removed by Filter"] ?? 0,
          });
        for (const c of p.Plans ?? []) visit(c);
      }
      visit(top.Plan);
      plans.push({
        hits: top.Plan["Shared Hit Blocks"],
        reads: top.Plan["Shared Read Blocks"],
        execMs: top["Execution Time"],
        nodes,
      });
    }
    console.log(JSON.stringify({ kind: "warm-plans", plans }));
    await profileDependencyJoins(diag, statements, "text");
  } finally {
    await diag.end();
  }
}
async function compareFullPages(api, id, q, shape, exact = false) {
  const expected = await api.page(id, q),
    blocks = [],
    n = exact ? 16 : 64;
  try {
    for (const gated of [false, true, true, false, true, false, false, true]) {
      await api.dependencyProbe(!gated ? "original" : exact ? "exact" : true);
      await Promise.all(Array.from({ length: 8 }, () => api.page(id, q)));
      await api.flushStats();
      const a = await pageCpuSnapshot(api.db);
      await Promise.all(
        Array.from({ length: 8 }, async () => {
          for (let i = 0; i < n / 8; i++) assert.deepEqual(await api.page(id, q), expected);
        }),
      );
      await api.flushStats();
      const d = pageCpuDelta(a, await pageCpuSnapshot(api.db));
      if (gated) {
        const { previous } = await api.dependencyProbe(false);
        assert(previous.locate >= n && previous.bodies >= n);
      }
      blocks.push({
        gated,
        pgMsPerPage: round((d.history + d.control) / n),
        containerMsPerPage: round(d.container / n),
      });
    }
  } finally {
    await api.dependencyProbe(false);
  }
  console.log(
    JSON.stringify({
      kind: "whole-page-probe",
      shape,
      pagesPerArm: 4 * n,
      identical: true,
      blocks,
    }),
  );
}
export async function seedToolsWindow(
  api,
  tenant,
  beforeAnalyze,
  { id = api.live, runs = 500, toolCount = 20 } = {},
) {
  const { db } = api,
    stride = toolCount * 3 + 4;
  if (api.directory) await api.directory.register(tenant, [id]);
  else await db.query("insert into sessions(id,tenant_id,flow_id) values($1,$2,$1)", [id, tenant]);
  await db.query(
    `insert into runs(id,tenant_id,session_id,${api.directory ? "" : "prev_run_id,"}mailbox_position,initial_input,model_ref,idempotency_key,state,history_head,output_display_seq)
 select fixture_run($1,n),$2,$1,${api.directory ? "" : "case when n>1 then fixture_run($1,n-1) end,"}n,'Review tests '||n,'fixture',fixture_run($1,n),'completed','{}',$4::int*n
 from generate_series(1,$3::int)n`,
    [id, tenant, runs, stride],
  );
  await db.query(
    `with points as (
 select n,j,(n-1)*$4::int+(j-1)*3+1 x from generate_series(1,$3::int)n cross join generate_series(1,$5::int)j)
 insert into messages(tenant_id,pi_session_id,entry_id,conversation_id,run_id,role,content,commit_seq,ordinal,assistant_entry_id,tool_task_id)
 select $2,$1::uuid,e.id,1,fixture_run($1,n),e.role,json_build_object('id',e.id,'conversationId',1,'kind',e.kind,'byTaskId',e.task,'model',json_build_array(e.msg))::text,e.id,0,e.assistant,e.task from points
 cross join lateral(values (x,'assistant','pi.assistant',null::integer,null::integer,json_build_object('role','assistant','stopReason','toolUse','timestamp',1,'content',json_build_array(json_build_object('type','toolCall','id','c-'||x,'name','bash','arguments',json_build_object('command','npm test'))))),
 (x+2,'toolResult','pi.tool-result',x,x+1,json_build_object('role','toolResult','timestamp',1,'toolCallId','c-'||x,'toolName','bash','content',json_build_array(json_build_object('type','text','text','passed'))))) e(id,role,kind,assistant,task,msg)`,
    [id, tenant, runs, stride, toolCount],
  );
  await db.query(
    `insert into messages(tenant_id,pi_session_id,entry_id,conversation_id,run_id,role,content,commit_seq,ordinal)
 select $2,$1::uuid,x,1,fixture_run($1,n),'assistant',json_build_object('id',x,'kind','pi.assistant','conversationId',1,'model',json_build_array(json_build_object('role','assistant','timestamp',1,'stopReason','stop','content',json_build_array(json_build_object('type','text','text','tool-final-'||n)))))::text,x,0
 from generate_series(1,$3::int)n cross join lateral(select n*$4::int-2 x)k`,
    [id, tenant, runs, stride],
  );
  if (beforeAnalyze) await beforeAnalyze(id);
  await db.query("vacuum (analyze) runs");
  await db.query("vacuum (analyze) messages");
  return id;
}
async function toolsWindow(api, tenant) {
  const { db } = api;
  const id = await seedToolsWindow(api, tenant, async (w) => {
    const latest = await api.page(w, {}),
      q = { before: latest.history.before };
    await compareFullPages(api, w, q, "20-tools-before-analyze");
  });
  const p = await api.page(id, {});
  assert.equal(p.turns.at(-1).transcript.items.filter((i) => i.kind === "tool").length, 20);
  await measureWheelDemand(api, { id, marker: "tool-final-500", shape: "20-tools" });
  const q = { before: p.history.before },
    older = await api.page(id, q);
  for (let i = 0; i < 12; i++) await api.page(id, q);
  await api.flushStats();
  const before = await ioSnap(db),
    repeats = 96;
  for (let i = 0; i < repeats; i++) await api.page(id, q);
  await api.flushStats();
  const after = await ioSnap(db);
  console.log(
    JSON.stringify({
      kind: "tool-page-cost",
      turns: older.turns.length,
      toolItems: older.turns
        .flatMap((t) => t.transcript?.items ?? [])
        .filter((i) => i.kind === "tool").length,
      bytes: Buffer.byteLength(JSON.stringify(older)),
      ...ioDelta(before, after, repeats),
      stages: (await api.sqlCpu(id, q)).stages,
    }),
  );
  const dsn = new URL(process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL);
  dsn.searchParams.set("options", "-c search_path=flow_web,flow_study,public");
  const diag = new pg.Client({ connectionString: dsn.href });
  await diag.connect();
  try {
    await profileDependencyJoins(diag, (await api.capture(id, q)).statements, "20-tools");
    await compareFullPages(api, id, q, "20-tools-analyzed");
  } finally {
    await diag.end();
  }
}
async function longRunStudy(api, tenant, plansOnly = false) {
  const id = await api.createScrollWindow("One long tool run");
  await seedToolsWindow(api, tenant, undefined, { id, runs: 1, toolCount: 1024 });
  const branch = await api.createScrollWindow("Inherited tool history");
  await api.db.query("select flow_fork($1,$2,fixture_run($2,1),$3,$4)", [
    tenant,
    id,
    branch,
    randomUUID(),
  ]);
  for (const window of [id, branch]) {
    const newest = await api.page(window, {});
    const before = newest.history.before;
    const expected = await api.page(window, { before });
    const tools = expected.turns.flatMap((turn) =>
      turn.transcript.items.filter((item) => item.kind === "tool"),
    );
    assert(tools.length > 0 && tools.every((tool) => tool.input.command === "npm test"));
    try {
      await api.dependencyProbe("original");
      assert.deepEqual(await api.page(window, { before }), expected);
    } finally {
      await api.dependencyProbe(false);
    }
  }
  if (!plansOnly)
    await measureWheelDemand(api, { id, marker: "tool-final-1", shape: "one-long-run" });
  const latest = await api.page(id, {}),
    q = { before: latest.history.before };
  const dsn = new URL(process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL);
  dsn.searchParams.set("options", "-c search_path=flow_web,flow_study,public");
  const diag = new pg.Client({ connectionString: dsn.href });
  await diag.connect();
  try {
    await profileDependencyJoins(
      diag,
      (await api.capture(id, q)).statements,
      "one-long-run",
      plansOnly ? 8 : 48,
    );
    await compareFullPages(api, id, q, "one-long-run-exact", true);
  } finally {
    await diag.end();
  }
}
if (import.meta.main) {
  const url = process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL;
  assert(url && new URL(url).hostname === "127.0.0.1");
  const tenant = randomUUID();
  await withPagingFixture(
    url,
    tenant,
    async (api) => {
      if (process.argv.includes("--tool-rows")) {
        const id = await api.createScrollWindow("One long tool run");
        await seedToolsWindow(api, tenant, undefined, { id, runs: 1, toolCount: 1024 });
        await measureWheelDemand(api, {
          id,
          marker: "tool-final-1",
          shape: "one-long-run",
          checkToolRows: true,
        });
        return;
      }
      if (!process.argv.includes("--tools-only")) {
        if (process.argv.includes("--long-only") || process.argv.includes("--long-plans")) {
          await longRunStudy(api, tenant, process.argv.includes("--long-plans"));
          return;
        }
        await measureWheelDemand(api);
        await measurePageIo(api, url);
      }
      await toolsWindow(api, tenant);
      await longRunStudy(api, tenant);
    },
    { ioTiming: true },
  );
}
