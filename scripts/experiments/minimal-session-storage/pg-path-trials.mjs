import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { createInterface } from "node:readline";
import { setTimeout as delay } from "node:timers/promises";
import { distribution } from "../../lib/runtime-load-scheduler.mjs";
import { kubeconfig, namespace, podFor } from "../../lib/production-kubernetes.mjs";

// Ordered protocol durations for one warmed backend. Only timing, PID and our
// own begin/end markers leave the log follower. No statement/parameter logging.
export function matchDurations(spans, durations) {
  assert.equal(durations.length, 1 + spans.length * 3, "Incomplete PostgreSQL timing block");
  return spans.map(([kind, start, end], i) => {
    const parts = durations.slice(1 + 3 * i, 4 + 3 * i);
    assert(
      parts.every(([at]) => at >= start - 3 && at <= end + 3),
      "PG durations do not match client span",
    );
    return {
      kind,
      at: start,
      client: end - start,
      server: parts.reduce((s, [, ms]) => s + ms, 0),
      afterExecute: Math.max(0, end - parts[2][0]),
    };
  });
}
// PG marks an implicit transaction idle after Sync finishes and before
// ReadyForQuery. An exact query tag survives while that backend remains idle.
export function attachIdleWitness(row, span, idle) {
  if (idle === undefined) return;
  const [, sent, returned] = span,
    executeEnd = returned - row.afterExecute;
  // Server duration logs have millisecond timestamps. Exclude samples outside
  // the matched request and allow only rounding at the two boundaries.
  if (idle < executeEnd - 1.5 || idle > returned + 1.5 || idle < sent - 1.5) return;
  row.toIdle = Math.max(0, idle - sent);
  row.executeToIdle = Math.max(0, idle - executeEnd);
  row.afterIdle = Math.max(0, returned - idle);
}
function followPg(token) {
  const binary = process.env.PI_CLOUD_KUBECTL_BIN ?? "/usr/local/bin/k3s";
  const prefix = [
    ...(process.env.PI_CLOUD_KUBECTL_BIN ? [] : ["kubectl"]),
    "--kubeconfig",
    kubeconfig,
    "-n",
    namespace,
  ];
  const pod = podFor("postgres");
  const open = new Map(),
    blocks = new Map(),
    pgCpu = [];
  const marker = new RegExp("pma_path_(" + token + "_[0-9]+)_(begin|end)\\s*$");
  const children = [];
  function run(args, handle, input = false) {
    const child = spawn(binary, [...prefix, ...args], {
      stdio: [input ? "pipe" : "ignore", "pipe", "ignore"],
    });
    createInterface({ input: child.stdout }).on("line", handle);
    children.push(child);
    return child;
  }
  run(["logs", "-f", "--since=2s", pod], (line) => {
    const entry = line.match(/^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d+) UTC \[(\d+)\]\s+LOG:\s+(.*)$/);
    if (!entry) return;
    const [, time, pid, body] = entry;
    const m = body.match(marker);
    if (m) {
      if (m[2] === "begin") open.set(pid, { tag: m[1], durations: [] });
      else if (open.get(pid)?.tag === m[1]) {
        blocks.set(m[1], open.get(pid).durations);
        open.delete(pid);
      }
      return;
    }
    const current = open.get(pid);
    if (current && body.startsWith("duration:")) {
      const duration = body.match(/^duration:\s*([0-9.]+) ms/);
      if (duration)
        current.durations.push([Date.parse(time.replace(" ", "T") + "Z"), Number(duration[1])]);
    }
  });
  const counters = run(
    [
      "exec",
      "-i",
      pod,
      "--",
      "sh",
      "-c",
      `while IFS= read -r tick; do
    awk '/^usage_usec /{u=$2} /^throttled_usec /{t=$2} END{printf "%s %s ",u,t+0}' /sys/fs/cgroup/cpu.stat
    awk '/^some /{sub(/.*total=/,""); print; exit}' /sys/fs/cgroup/cpu.pressure
  done`,
    ],
    (line) => {
      if (/^\d+ \d+ \d+$/.test(line))
        pgCpu.push({ at: Date.now(), values: line.split(" ").map(Number) });
    },
    true,
  );
  const ticks = setInterval(() => {
    if (counters.stdin.writable) counters.stdin.write("\n");
  }, 1000);
  const counterExit = new Promise((resolve) => counters.once("close", resolve));
  counters.stdin.on("error", () => {});
  return {
    pgCpu,
    async read(tag) {
      const deadline = Date.now() + 4000;
      while (!blocks.has(tag) && Date.now() < deadline) await delay(50);
      const result = blocks.get(tag);
      assert(result, "PG log follower did not receive timing block " + tag);
      blocks.delete(tag);
      return result;
    },
    async close() {
      clearInterval(ticks);
      counters.stdin.end();
      for (const c of children) if (c !== counters) c.kill("SIGTERM");
      const exited = await Promise.race([counterExit, delay(3000, -1, { ref: false })]);
      if (exited === -1) counters.kill("SIGTERM");
      assert.equal(exited, 0, "PG counter sampler failed to exit on stdin EOF");
    },
  };
}

const walSql = `select clock_timestamp() as at, pg_current_wal_insert_lsn()::text as lsn,
 wal_write::float8,wal_sync::float8,wal_write_time,wal_sync_time,stats_reset::text
 from pg_stat_wal`;
function lsn(s) {
  const [hi, lo] = s.split("/");
  return BigInt("0x" + hi) * 4294967296n + BigInt("0x" + lo);
}
const buffer = Buffer.alloc(1024 * 1024);
async function observe(pool, logs, work) {
  const sample = [],
    before = (await pool.query(walSql)).rows[0];
  const timing = process.env.PI_CLOUD_PG_PATH_PROFILE === "timing";
  const witnesses = new Map();
  let stop = false,
    calibratedAt = 0;
  const sampling = (async () => {
    while (!stop) {
      const at = Date.now();
      const { rows } = await pool.query(
        timing
          ? `select state,coalesce(wait_event_type || '.' || wait_event,'on_cpu_or_runnable') event,1 n,
           case when starts_with(query,'/*pmap_') then split_part(substr(query,8,64),'*/',1) end marker,
           (extract(epoch from state_change)*1000)::float8 changed
         from pg_stat_activity where datname=current_database() and application_name='session-bench-projector'`
          : `select state,coalesce(wait_event_type || '.' || wait_event,'on_cpu_or_runnable') event,count(*)::int n
         from pg_stat_activity where datname=current_database() and application_name='session-bench-projector'
         group by state,wait_event_type,wait_event`,
      );
      const events = {};
      for (const row of rows) {
        const key = row.state + "/" + row.event;
        events[key] = (events[key] ?? 0) + row.n;
        if (row.marker && row.state === "idle") witnesses.set(row.marker, row.changed);
      }
      let calibrationMs, wal;
      if (at - calibratedAt >= 1000) {
        const t = performance.now();
        for (let i = 0; i < 4; i++) createHash("sha256").update(buffer).digest();
        calibrationMs = performance.now() - t;
        calibratedAt = at;
        wal = (await pool.query(walSql)).rows[0];
      }
      sample.push({
        at,
        events,
        calibrationMs,
        wal,
      });
      await delay(Math.max(1, 50 - (Date.now() - at)));
    }
  })();
  // Race a sampling failure with the workload, while keeping normal sampling
  // alive until the measured action is done.
  let measured;
  try {
    measured = await Promise.race([work(), sampling]);
  } finally {
    stop = true;
    await sampling;
  }
  const m = measured.measurement;
  const after = (await pool.query(walSql)).rows[0];
  assert.equal(before.stats_reset, after.stats_reset);
  const server = m.diagnostics ? matchDurations(m.spans, await logs.read(m.tag)) : [];
  for (const [i, r] of server.entries()) {
    attachIdleWitness(r, m.spans[i], witnesses.get(m.tag + "_" + i));
  }
  const selected = sample.filter((s) => s.at >= m.started && s.at <= m.ended);
  const kinds = Object.fromEntries(
    [...Map.groupBy(server, (s) => s.kind)].map(([k, s]) => [
      k,
      {
        client: distribution(s.map((x) => x.client)),
        server: distribution(s.map((x) => x.server)),
        afterExecute: distribution(s.map((x) => x.afterExecute)),
      },
    ]),
  );
  return {
    ...measured,
    observations: {
      samples: selected,
      server,
      byKind: kinds,
      pgCpu: logs.pgCpu.filter((s) => s.at >= m.started - 1000 && s.at <= m.ended + 1000),
      calibrationMs: distribution(selected.flatMap((s) => s.calibrationMs ?? [])),
      wal: {
        from: before.at,
        to: after.at,
        bytes: Number(lsn(after.lsn) - lsn(before.lsn)),
        writes: after.wal_write - before.wal_write,
        syncs: after.wal_sync - before.wal_sync,
        writeMs: after.wal_write_time - before.wal_write_time,
        syncMs: after.wal_sync_time - before.wal_sync_time,
      },
    },
  };
}
export async function runPgPaths({
  projector,
  emitter,
  wave,
  configure,
  guard,
  observer,
  report,
  save,
  token,
}) {
  const profile = process.env.PI_CLOUD_PG_PATH_PROFILE ?? "full";
  assert(["smoke", "full", "timing"].includes(profile));
  const smoke = profile === "smoke",
    timing = profile === "timing";
  const count = smoke ? 320 : timing ? 4160 : 6240;
  const call = async (actor, command, response, args = {}) => {
    const done = actor.wait(response);
    actor.write({ kind: command, ...args });
    return guard(done, 240000);
  };
  const live = {
    integrated: true,
    cacheMode: "prefer",
    wakeMode: "directed",
    batch: 32,
    slots: 128,
    omitNotify: true,
    probeUnlisted: true,
    windowMs: 5,
    tickMs: 200,
    sink: "single",
    concurrency: 384,
  };
  const runLive = async (label, mode, rate, n, diagnostics) => {
    const result = await wave({
      ...live,
      label,
      rate,
      count: n,
      pgPath: mode,
      pathDiagnostics: diagnostics,
    });
    return {
      label,
      rate,
      offeredAt: result.offeredAt,
      runsPerSecond: result.runsPerSecond,
      measurement: result.pgPath,
      completed: result.completed,
      dropped: result.dropped,
      terminalMs: result.submitToTerminalMs,
      peakBacklog: result.peakKafkaLag,
      drainMs: result.drainAfterOfferMs,
    };
  };
  const logs = followPg(token);
  report.pgPaths = { live: [], pairs: [] };
  try {
    const capture = await observe(observer, logs, () =>
      runLive("path-capture", "capture", 116, count, true),
    );
    assert.equal(capture.completed, count);
    report.pgPaths.live.push(capture);
    await configure({ label: "path-park", scene: "plain", slots: 0 });
    const { tape } = await call(projector, "freezePaths", "pathTape");
    report.pgPaths.tape = tape;
    assert.equal(tape.runs, count);
    assert.equal((await call(emitter, "loadPathFrames", "pathSourceReady")).frames, tape.frames);
    await save();
    const cases = smoke
      ? [
          [260, "direct"],
          [260, "projector"],
        ]
      : timing
        ? [
            [260, "direct"],
            [260, "projector"],
            [260, "projector"],
            [260, "direct"],
            [520, "direct"],
            [520, "projector"],
          ]
        : [
            [116, "direct"],
            [116, "projector"],
            [260, "projector"],
            [260, "direct"],
            [260, "direct"],
            [260, "projector"],
            [390, "direct"],
            [390, "projector"],
            [390, "projector"],
            [390, "direct"],
            [520, "projector"],
            [520, "direct"],
            [116, "projector"],
            [116, "direct"],
            [260, "direct", false],
            [260, "projector", false],
          ];
    for (const [rate, path, diagnostics = true] of cases) {
      const { start } = await call(projector, "preparePaths", "pathReady", {
        path,
        rate,
        diagnostics,
      });
      const result = await observe(observer, logs, async () => {
        const pending = call(projector, "runPaths", "pathResult", { path });
        const [r, emitted] = await Promise.all([
          pending,
          path === "projector"
            ? call(emitter, "emitPathFrames", "pathEmitted", { start, rate })
            : undefined,
        ]);
        return { rate, path, diagnostics, source: emitted?.source ?? r.source, ...r.result };
      });
      assert.equal(result.source.frames, tape.frames);
      report.pgPaths.pairs.push(result);
      await save();
      console.log(
        JSON.stringify({
          path,
          rate,
          diagnostics,
          done: tape.runs,
          rps: result.runsPerSecond,
          p95: result.terminalMs.p95,
          backlog: result.peakBacklog,
          drain: result.drainMs,
        }),
      );
    }
    for (const i of smoke ? [0] : timing ? [0, 1] : [0, 1, 2]) {
      const result = await observe(observer, logs, () =>
        runLive("path-full-" + i, "full", 260, smoke ? 320 : timing ? 10400 : 6500, i !== 2),
      );
      report.pgPaths.live.push(result);
      await save();
    }
  } finally {
    await logs.close();
  }
}
