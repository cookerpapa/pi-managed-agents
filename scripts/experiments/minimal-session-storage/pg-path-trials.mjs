import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { createInterface } from "node:readline";
import { setTimeout as delay } from "node:timers/promises";
import { distribution } from "../../lib/runtime-load-scheduler.mjs";
import { isPrefixStudy, isMovingStudy } from "./ready-projection.mjs";
import { kubeconfig, namespace, podFor } from "../../lib/production-kubernetes.mjs";
import {
  commitController,
  groupLiveCases,
  groupReplayCases,
  isCommitStudy,
  isAsyncStudy,
  asyncReplayCases,
  asyncLiveCases,
} from "./pg-group-commit.mjs";

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
function followPg(token, pod = podFor("postgres")) {
  const binary = process.env.PI_CLOUD_KUBECTL_BIN ?? "/usr/local/bin/k3s";
  const prefix = [
    ...(process.env.PI_CLOUD_KUBECTL_BIN ? [] : ["kubectl"]),
    "--kubeconfig",
    kubeconfig,
    "-n",
    namespace,
  ];
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
    awk '/^some /{sub(/.*total=/,""); printf "%s ",$0; exit}' /sys/fs/cgroup/cpu.pressure
    cat /sys/fs/cgroup/cpuset.cpus.effective
  done`,
    ],
    (line) => {
      if (/^\d+ \d+ \d+ [0-9,-]+$/.test(line)) {
        const parts = line.split(" ");
        pgCpu.push({ at: Date.now(), values: parts.slice(0, 3).map(Number), allowed: parts[3] });
      }
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
  const timing =
    ["timing", "cpu", "cpu-smoke"].includes(process.env.PI_CLOUD_PG_PATH_PROFILE) ||
    isCommitStudy(process.env.PI_CLOUD_PG_PATH_PROFILE);
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
  guestEndpoints,
  projector,
  emitter,
  wave,
  configure,
  guard,
  observer,
  forkPool,
  report,
  save,
  token,
  cpuFixture,
}) {
  const profile = process.env.PI_CLOUD_PG_PATH_PROFILE ?? "full";
  if (profile === "workspace-guest-mix") {
    const { runGuestMix } = await import("./guest-mix-study.mjs");
    return runGuestMix(wave, forkPool, guestEndpoints, report, save);
  }
  if (profile === "workspace-guest-continuation") {
    const { runGuestContinuation } = await import("./guest-continuation-study.mjs");
    return runGuestContinuation(wave, forkPool, guestEndpoints);
  }
  if (profile === "workspace-continuation") {
    const { runContinuationCases } = await import("./continuation-cases.mjs");
    return runContinuationCases(wave, forkPool);
  }
  if (
    [
      "workspace-rate",
      "workspace-rate-confirm",
      "workspace-rate-input",
      "message-rate",
      "message-rate-smoke",
      "message-rate-confirm",
      "fork-rate",
      "fork-rate-confirm",
      "fork-rate-smoke",
      "fork-rate-index",
      "usage-rate-bulk",
      "usage-rate-bulk-confirm",
      "usage-rate-bulk-smoke",
      "usage-rate-smoke",
      "usage-rate-confirm",
      "usage-rate",
    ].includes(profile)
  ) {
    const { messageTrials } = await import("./complete-message-rate.mjs");
    return messageTrials(
      {
        wave,
        projector,
        emitter,
        configure,
        guard,
        observer,
        forkPool,
        report,
        save,
        cpuFixture,
        token,
      },
      profile,
    );
  }
  const prefix = isPrefixStudy(profile);
  const moving = isMovingStudy(profile);
  const commitStudy = isCommitStudy(profile);
  const asyncCommit = isAsyncStudy(profile);
  const peak = profile === "async-peak";
  const commitResult = asyncCommit ? "asyncCommit" : "groupCommit";
  const confirm = profile === "group-confirm";
  const replayCommitCases = asyncCommit ? asyncReplayCases : groupReplayCases;
  const liveCommitCases = asyncCommit ? asyncLiveCases : groupLiveCases;
  assert(
    ["smoke", "full", "timing", "cpu", "cpu-smoke"].includes(profile) || commitStudy || prefix,
  );
  const smoke =
      profile === "smoke" ||
      profile === "cpu-smoke" ||
      profile === "group-smoke" ||
      profile === "async-smoke" ||
      profile === "prefix-smoke" ||
      profile === "moving-smoke",
    timing = profile === "timing";
  const prefixConfirm = profile === "prefix-confirm";
  const count = smoke
    ? cpuFixture
      ? 520
      : 320
    : timing || confirm || peak || prefixConfirm || moving
      ? 4160
      : 6240;
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
  const runLive = async (
    label,
    mode,
    rate,
    n,
    diagnostics,
    commitSetting,
    prefixWidth,
    projectionPolicy,
  ) => {
    const result = await wave({
      ...live,
      label,
      rate,
      count: n,
      pgPath: mode,
      pathDiagnostics: diagnostics,
      ...(prefix ? { prefixWidth, projectionPolicy } : {}),
      ...(commitSetting ? { commitSetting } : {}),
    });
    return {
      label,
      rate,
      offeredAt: result.offeredAt,
      runsPerSecond: result.runsPerSecond,
      measurement: result.pgPath,
      ...(prefix ? { prefixWidth, projectionPolicy, prefix: result.prefix } : {}),
      completed: result.completed,
      dropped: result.dropped,
      terminalMs: result.submitToTerminalMs,
      peakBacklog: result.peakKafkaLag,
      drainMs: result.drainAfterOfferMs,
    };
  };
  const logs = followPg(token, cpuFixture?.pg);
  report.pgPaths = { live: [], pairs: [] };
  let controller;
  const reverse = cpuFixture && process.env.PI_CLOUD_CPU_REVERSE === "1";
  if (cpuFixture)
    report.pgPaths.cpu = {
      plan: cpuFixture.plan,
      images: cpuFixture.images,
      environment: cpuFixture.environment,
      reverse,
    };
  const verifyPlacement = (result, placement) => {
    if (!placement) return;
    const m = result.measurement;
    const pgMasks = result.observations.pgCpu
      .filter((s) => s.at >= m.started && s.at <= m.ended)
      .map((s) => s.allowed);
    const projectorMasks = m.samples.map((s) => s.allowed);
    assert.deepEqual(
      [...new Set(pgMasks)],
      [placement.pg.effective],
      "PG cpuset changed during trial",
    );
    assert.deepEqual(
      [...new Set(projectorMasks)],
      [placement.projector.effective],
      "Projector cpuset changed during trial",
    );
    result.placement = {
      ...placement,
      pgSamples: pgMasks.length,
      projectorSamples: projectorMasks.length,
    };
  };
  try {
    if (prefix) {
      const candidate = {
        ...live,
        prefixWidth: 16,
        ...(moving ? { projectionPolicy: "selective" } : {}),
        count: 2,
        concurrency: 2,
      };
      await wave({ ...candidate, label: "prefix-finish-held", holdFinish: true });
      for (const scene of ["follow", "steer", "late"])
        await wave({
          ...candidate,
          label: "prefix-" + scene,
          scene,
          inspect: true,
          idempotency: true,
        });
      await wave({
        ...candidate,
        label: "prefix-edit-withdraw",
        scene: "follow",
        inspect: true,
        mutate: true,
        depth: 3,
        count: 1,
        concurrency: 1,
      });
    }
    if (commitStudy) {
      controller = await commitController(observer, cpuFixture, asyncCommit);
      report.pgPaths[commitResult] = {
        initial: controller.start,
        confirm,
        ...(asyncCommit ? { peak } : {}),
      };
    }
    const capture = await observe(observer, logs, () =>
      runLive(
        "path-capture",
        "capture",
        116,
        count,
        true,
        commitStudy || prefix ? "default" : undefined,
        0,
      ),
    );
    assert.equal(capture.completed, count);
    report.pgPaths.live.push(capture);
    await configure({ label: "path-park", scene: "plain", slots: 0 });
    const { tape } = await call(projector, "freezePaths", "pathTape");
    report.pgPaths.tape = tape;
    assert.equal(tape.runs, count);
    assert.equal((await call(emitter, "loadPathFrames", "pathSourceReady")).frames, tape.frames);
    await save();
    const cases = moving
      ? movingCases(profile).map(([rate, width, policy]) => [
          rate,
          "projector",
          true,
          "open",
          "default",
          width,
          policy,
        ])
      : prefix
        ? prefixCases(smoke, prefixConfirm)
        : cpuFixture
          ? commitStudy
            ? replayCommitCases(smoke, peak || confirm)
            : cpuReplayCases(smoke, reverse)
          : smoke
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
    for (const [
      rate,
      path,
      diagnostics = true,
      layout,
      commitSetting,
      prefixWidth,
      projectionPolicy,
    ] of cases) {
      const placement = cpuFixture ? await cpuFixture.pin(layout) : undefined;
      if (controller) await controller.apply(commitSetting);
      const { start } = await call(projector, "preparePaths", "pathReady", {
        path,
        rate,
        diagnostics,
        commitSetting,
        prefixWidth,
        projectionPolicy,
      });
      const result = await observe(observer, logs, async () => {
        const pending = call(projector, "runPaths", "pathResult", { path });
        const [r, emitted] = await Promise.all([
          pending,
          path === "projector"
            ? call(emitter, "emitPathFrames", "pathEmitted", { start, rate })
            : undefined,
        ]);
        return {
          rate,
          path,
          diagnostics,
          commitSetting,
          prefixWidth,
          projectionPolicy,
          source: emitted?.source ?? r.source,
          ...r.result,
        };
      });
      assert.equal(result.source.frames, tape.frames);
      verifyPlacement(result, placement);
      report.pgPaths.pairs.push(result);
      await save();
      console.log(
        JSON.stringify({
          path,
          layout,
          commitSetting,
          prefixWidth,
          projectionPolicy,
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
    const liveSettings = moving
      ? movingCases(profile, true).map(([rate, width, policy]) => [rate, "default", width, policy])
      : prefix
        ? prefixLiveCases(smoke, prefixConfirm)
        : commitStudy
          ? liveCommitCases(smoke, peak || confirm)
          : undefined;
    const liveLayouts = liveSettings
      ? liveSettings.map(() => "open")
      : cpuFixture
        ? smoke
          ? ["ab"]
          : reverse
            ? ["ba", "shared", "shared", "ab"]
            : ["shared", "ab", "ba", "shared"]
        : undefined;
    for (const i of liveLayouts ? liveLayouts.keys() : smoke ? [0] : timing ? [0, 1] : [0, 1, 2]) {
      const placement = liveLayouts ? await cpuFixture.pin(liveLayouts[i]) : undefined;
      const [rate, commitSetting, prefixWidth, projectionPolicy] = liveSettings?.[i] ?? [
        260,
        undefined,
      ];
      if (controller) await controller.apply(commitSetting);
      const result = await observe(observer, logs, () =>
        runLive(
          "path-full-" + i,
          "full",
          rate,
          smoke
            ? cpuFixture
              ? 520
              : 320
            : rate === 10
              ? 200
              : moving
                ? rate * 20
                : timing || cpuFixture
                  ? 10400
                  : 6500,
          !!cpuFixture || i !== 2,
          commitSetting,
          prefixWidth,
          projectionPolicy,
        ),
      );
      verifyPlacement(result, placement);
      if (commitStudy) result.commitSetting = commitSetting;
      report.pgPaths.live.push(result);
      await save();
    }
  } finally {
    try {
      if (controller) report.pgPaths[commitResult].restored = await controller.restore();
    } finally {
      await logs.close();
    }
  }
}

export function cpuReplayCases(smoke, reverse = false) {
  const cases = (rate, path, layouts) => layouts.map((layout) => [rate, path, true, layout]);
  if (smoke) return cases(260, "projector", ["shared", "ab", "ba"]);
  if (reverse)
    return cases(260, "projector", [
      "open",
      "ab",
      "shared",
      "shared",
      "ba",
      "shared",
      "ba",
      "ab",
      "shared",
      "open",
    ]);
  return [
    ...cases(260, "projector", ["shared", "ab", "ba", "shared", "ba", "shared", "shared", "ab"]),
    ...cases(260, "direct", ["shared", "ab", "ba", "shared"]),
    ...cases(390, "projector", ["shared", "ba", "ab", "shared"]),
    ...cases(260, "projector", ["overlapA", "ab", "ba", "overlapB"]),
  ];
}

export function movingCases(profile, live = false) {
  const sync = "moving-sync",
    async = "selective";
  const rates = (r, pairs) => pairs.map(([width, policy]) => [r, width, policy]);
  if (profile === "moving-finish")
    return [
      ...rates(520, [
        [16, async],
        [16, "replay-finish"],
        [16, "replay-finish"],
        [16, async],
      ]),
      ...rates(780, [
        [16, "replay-finish"],
        [16, async],
        [16, async],
        [16, "replay-finish"],
      ]),
    ];
  if (profile === "moving-smoke")
    return rates(260, [
      [1, sync],
      [1, async],
      [16, sync],
      [16, async],
    ]);
  const cases = [
    ...rates(520, [
      [1, sync],
      [1, async],
      [16, async],
      [16, sync],
    ]),
    ...rates(780, [
      [16, sync],
      [16, async],
      [16, async],
      [16, sync],
    ]),
    ...(live
      ? rates(10, [
          [16, sync],
          [16, async],
        ])
      : []),
  ];
  return profile === "moving-confirm" ? cases.reverse() : cases;
}
export function prefixCases(smoke, confirm) {
  const at = (rate, widths) =>
    widths.map((width) => [rate, "projector", true, "open", "default", width]);
  if (smoke) return at(260, [0, 1, 16]);
  if (confirm) return [...at(520, [16, 1, 1, 16]), ...at(260, [16, 1, 1, 16])];
  return [
    ...at(260, [0, 1, 16, 16, 1, 0]),
    ...at(520, [1, 16, 0, 0, 16, 1]),
    ...at(520, [16, 1, 1, 16]),
  ];
}
export function prefixLiveCases(smoke, confirm) {
  if (smoke) return [1, 16].map((n) => [260, "default", n]);
  if (confirm)
    return [
      ...[0, 16, 16, 0].map((n) => [520, "default", n]),
      ...[16, 0, 0, 16].map((n) => [260, "default", n]),
    ];
  return [
    ...[1, 16, 16, 1].map((n) => [260, "default", n]),
    ...[16, 1, 1, 16].map((n) => [520, "default", n]),
    ...[0, 16].map((n) => [10, "default", n]),
  ];
}
