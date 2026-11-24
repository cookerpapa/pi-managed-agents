// Replay observed browser demand on one primary, alongside the normal Kafka fold.
import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import { measureWheelDemand, seedToolsWindow } from "./scroll-demand-study.mjs";
import { cpuMillis } from "./page-backend-cpu.mjs";
const round = (x) => +x.toFixed(3);

export async function prepareScrollLoad(api, tenant, tools) {
  const id = tools ? await seedToolsWindow(api, tenant) : api.windows[1];
  const { phases } = await measureWheelDemand(
    api,
    tools ? { id, marker: "tool-final-500", shape: "20-tools" } : undefined,
  );
  const profiles = Object.fromEntries(phases.map((p) => [p.name, p]));
  const cursors = [];
  // Distinct positions so eight readers do not all hit the same three pages.
  for (let i = 0; i < 8; i++) {
    const {
      rows: [{ turn }],
    } = await api.db.query("select fixture_run($1,$2::int) turn", [
      tools ? id : api.physicalRoot,
      tools ? 495 - i * 10 : 99000 - i * 10000,
    ]);
    const p = await api.page(id, { turnId: turn });
    assert(p.history.before);
    cursors.push(p.history.before);
  }
  const first = await api.page(id, {}),
    hot = [first.history.before];
  for (let i = 0; i < 2; i++) hot.push((await api.page(id, { before: hot.at(-1) })).history.before);
  function start(mode) {
    if (mode.startsWith("web-")) return wheelDuringProjection(api, id, mode);
    let stopped = false,
      error;
    const times = [],
      seen = new Set();
    let skipped = 0,
      dispatched = 0;
    const begin = performance.now();
    const profile =
      mode === "one-reader" ? profiles.read : mode === "eight-fast" ? profiles.fast : null;
    const tasks = Array.from({ length: mode === "one-reader" ? 1 : 8 }, async (_, i) => {
      let k = 0,
        cursor = cursors[i],
        due = begin;
      const duration = profile?.seconds * 1000;
      // Start lane zero at a measured edge crossing. Readers keep advancing
      // through history; only the captured timings repeat.
      const offsets = profile?.requestOffsetsMs;
      if (profile && !offsets.length) return;
      const cycle = profile
        ? offsets
            .map((t) => (duration + t - offsets[0] + (duration * i) / 8) % duration)
            .sort((a, b) => a - b)
        : null;
      while (!stopped && !error) {
        if (cycle) due = begin + Math.floor(k / cycle.length) * duration + cycle[k % cycle.length];
        else if (mode !== "saturated-hot") due = begin + (1000 * i) / 32 + 250 * k;
        const wait = due - performance.now();
        if (wait > 0) {
          await delay(Math.max(1, Math.min(25, wait)));
          continue;
        }
        const input = mode.endsWith("-hot") ? hot[(i + k) % hot.length] : cursor;
        const t = performance.now();
        dispatched++;
        try {
          const p = await api.page(id, { before: input });
          assert(p.turns.length > 0 && p.history.before && p.history.before !== input);
          cursor = p.history.before;
          const end = performance.now();
          times.push({ t, end });
          seen.add(input);
          k++;
          if (!p.history.hasBefore) return;
          if (cycle) {
            while (
              begin + Math.floor(k / cycle.length) * duration + cycle[k % cycle.length] <
              end
            ) {
              skipped++;
              k++;
            }
          }
          if (!profile && mode !== "saturated-hot") {
            const next = Math.max(k, Math.ceil((end - begin - (1000 * i) / 32) / 250));
            skipped += next - k;
            k = next;
          }
        } catch (e) {
          error = e;
        }
      }
    });
    return {
      async stop(start, end) {
        stopped = true;
        await Promise.all(tasks);
        if (error) throw error;
        const within = times
          .filter((p) => p.t >= start && p.end <= end)
          .map((p) => p.end - p.t)
          .sort((a, b) => a - b);
        return {
          completed: within.length,
          rate: round((1000 * within.length) / (end - start)),
          p95Ms: within.length ? round(within[Math.ceil(0.95 * within.length) - 1]) : null,
          dispatched,
          distinctCursors: seen.size,
          skipped,
        };
      },
    };
  }
  // Each timed tool trial starts from the same fresh latest page, including
  // the writer-only controls. The startup reads finish before the timer.
  const reset = async () => {
    const url = new URL(api.address);
    url.searchParams.set("session", id);
    const hook = await api.chrome.send("Page.addScriptToEvaluateOnNewDocument", {
      source: "sessionStorage.removeItem(" + JSON.stringify("pi-cloud:reading:" + id) + ")",
    });
    try {
      await api.chrome.navigate("about:blank", 30);
      await api.chrome.navigate(url.href);
      await api.chrome.waitFor("document.body.innerText.includes('tool-final-500')", 30000);
      await delay(650);
    } finally {
      await api.chrome.send("Page.removeScriptToEvaluateOnNewDocument", {
        identifier: hook.identifier,
      });
    }
  };
  return { start, reset: tools ? reset : undefined };
}
async function wheelDuringProjection(api, id, mode) {
  const { chrome } = api;
  const pos = await chrome.evaluate(
    "(()=>{const e=document.querySelector('.product-chat-scroll'),r=e.getBoundingClientRect();return {x:r.left+r.width*.55,y:r.top+r.height*.5,height:e.clientHeight};})()",
  );
  let stop = false,
    gestures = 0,
    peak = 0,
    failed = 0;
  const initiated = new Map(),
    times = [],
    cursors = new Set();
  const off = chrome.onNetworkEvent((method, p) => {
    if (method === "Network.requestWillBeSent") {
      const u = new URL(p.request.url);
      if (u.pathname === "/v1/conversations/" + id && u.searchParams.has("before")) {
        initiated.set(p.requestId, performance.now());
        cursors.add(u.searchParams.get("before"));
        peak = Math.max(peak, initiated.size);
      }
    } else if (initiated.has(p.requestId)) {
      if (method === "Network.responseReceived" && p.response.status !== 200) failed++;
      if (method === "Network.loadingFinished" || method === "Network.loadingFailed") {
        if (method === "Network.loadingFailed") failed++;
        times.push([initiated.get(p.requestId), performance.now()]);
        initiated.delete(p.requestId);
      }
    }
  });
  const step = mode === "web-read" ? 0.75 : 1,
    interval = mode === "web-read" ? 750 : 110;
  const run = (async () => {
    while (!stop) {
      const start = performance.now();
      await chrome.send("Input.dispatchMouseEvent", {
        type: "mouseWheel",
        x: pos.x,
        y: pos.y,
        deltaX: 0,
        deltaY: -Math.round(pos.height * step),
      });
      gestures++;
      while (!stop && performance.now() - start < interval) await delay(15);
    }
  })();
  run.catch(() => {});
  return {
    async stop(start, end) {
      stop = true;
      try {
        await run;
        const limit = performance.now() + 3000;
        while (initiated.size && performance.now() < limit) await delay(10);
        assert.equal(failed, 0);
        assert.equal(initiated.size, 0);
        const a = times
          .filter(([t, u]) => t >= start && u <= end)
          .map(([t, u]) => u - t)
          .sort((l, r) => l - r);
        return {
          completed: a.length,
          rate: round((a.length * 1000) / (end - start)),
          p95Ms: a.length ? round(a[Math.ceil(a.length * 0.95) - 1]) : null,
          gestures,
          peak,
          distinctCursors: cursors.size,
        };
      } finally {
        off();
      }
    },
  };
}
export function scrollTrials(tools) {
  if (tools)
    return [
      "writer",
      "web-read",
      "web-fast",
      "eight-fast",
      "writer",
      "eight-fast",
      "web-fast",
      "web-read",
      "writer",
      "web-read",
      "eight-fast",
      "web-fast",
      "writer",
    ].map((m) => ["single-pg", m]);
  const a = [
    "writer",
    "one-reader",
    "eight-fast",
    "writer",
    "32-hot",
    "32-advancing",
    "saturated-hot",
  ];
  return [...a, ...a.toReversed(), ...a.slice(3), ...a.slice(0, 3), "writer"].map((mode) => [
    "single-pg",
    mode,
  ]);
}
async function calibration(client) {
  const cpu = async () =>
    Number(
      (await client.query("select split_part(pg_read_file('/proc/self/schedstat'),' ',1) t"))
        .rows[0].t,
    );
  const query = {
    name: "scroll-cpu-calibration",
    text: "select sum(i % 109)::text total from generate_series(1,$1::int)i",
    values: [40000],
  };
  const expected = (await client.query(query)).rows[0].total,
    start = await cpu();
  for (let i = 0; i < 6; i++) assert.equal((await client.query(query)).rows[0].total, expected);
  return round(((await cpu()) - start) / 6e6);
}
export async function mixSnapshot(api, calibrateFirst) {
  await api.flushStats();
  const clients = await Promise.all(Array.from({ length: 12 }, () => api.db.connect()));
  try {
    await Promise.all(clients.map((c) => c.query("select pg_stat_force_next_flush()")));
  } finally {
    for (const c of clients) c.release();
  }
  const c = await api.db.connect();
  try {
    const cal = calibrateFirst ? await calibration(c) : null;
    const {
      rows: [info],
    } =
      await c.query(`select pg_read_file('/sys/fs/cgroup/cpu.stat') cpu,pg_read_file('/sys/fs/cgroup/io.stat') io,
   (select json_build_object('hits',blks_hit,'reads',blks_read,'readMs',blk_read_time,'writeMs',blk_write_time,'temp',temp_bytes)
    from pg_stat_database where datname=current_database()) db,
   (select json_build_object('bytes',wal_bytes,'syncMs',wal_sync_time,'writeMs',wal_write_time,'syncs',wal_sync) from pg_stat_wal) wal,
   (select json_agg(json_build_object('pid',pid,'role',application_name,'stat',pg_read_file('/proc/'||pid::text||'/stat',0,8192,true)))
    from pg_stat_activity where datname=current_database() and application_name in('flow-page','flow-projector','flow-control')) roles`);
    const io = {};
    for (const l of info.io.trim().split("\n").filter(Boolean)) {
      const [dev, ...x] = l.split(/\s+/);
      const fields = Object.fromEntries(x.map((s) => s.split("=")));
      io[dev] = { r: Number(fields.rbytes), w: Number(fields.wbytes) };
    }
    return {
      ...info,
      cpu: Object.fromEntries(
        info.cpu
          .trim()
          .split("\n")
          .map((l) => l.split(" ")),
      ),
      io,
      cal: cal ?? (await calibration(c)),
    };
  } finally {
    c.release();
  }
}
export function mixDelta(a, b) {
  const byPid = new Map(a.roles.filter((r) => r.stat).map((r) => [r.pid, cpuMillis(r.stat)]));
  const roles = {};
  for (const r of b.roles.filter((r) => r.stat))
    roles[r.role] = (roles[r.role] ?? 0) + cpuMillis(r.stat) - (byPid.get(r.pid) ?? 0);
  const diff = (x, y) =>
    Object.fromEntries(Object.keys(y).map((k) => [k, round(Number(y[k]) - Number(x[k]))]));
  return {
    backendCpuMs: roles,
    pgCpuMs: round((b.cpu.usage_usec - a.cpu.usage_usec) / 1000),
    quotaPeriods: b.cpu.nr_periods - a.cpu.nr_periods,
    throttledPeriods: b.cpu.nr_throttled - a.cpu.nr_throttled,
    buffers: diff(a.db, b.db),
    wal: diff(a.wal, b.wal),
    deviceBytes: Object.fromEntries(
      Object.entries(b.io).map(([k, v]) => [k, diff(a.io[k] ?? { r: 0, w: 0 }, v)]),
    ),
    calibrationCpuMs: [a.cal, b.cal],
  };
}
