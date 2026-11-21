// Start one read-only sampler in a disposable Guest. It records counts and
// pressure, never command payloads. Sampling and capture are outside PG/Kafka.
import assert from "node:assert/strict";

function sampleGuest() {
  const { readFileSync: read, writeFileSync: write, existsSync } = process.getBuiltinModule("fs");
  const { DatabaseSync } = process.getBuiltinModule("node:sqlite");
  const dir = "/tmp/pma-tool-mix-sampler";
  const db = new DatabaseSync("/run/pi-cloud-executor/state.sqlite", { readOnly: true });
  const ops = db.prepare(
    "select count(*) n, count(*) filter(where finished=1) done from operations",
  );
  const text = (p) => read(p, "utf8");
  const cpuPressure = () =>
    existsSync("/proc/pressure/cpu")
      ? Number(text("/proc/pressure/cpu").match(/^some .*total=(\d+)/m)?.[1])
      : null;
  function values() {
    const cpu = text("/proc/stat")
      .match(/^cpu +(.+)$/m)[1]
      .trim()
      .split(/\s+/)
      .slice(0, 8)
      .map(Number);
    return {
      at: Date.now(),
      pressure: cpuPressure(),
      total: cpu.reduce((a, b) => a + b, 0),
      idle: cpu[3],
      io: cpu[4],
      steal: cpu[7],
      cpus: (text("/proc/stat").match(/^cpu\d+ /gm) ?? []).length,
      oom: Number(text("/proc/vmstat").match(/^oom_kill (\d+)/m)?.[1] ?? 0),
      totalKiB: Number(text("/proc/meminfo").match(/MemTotal:\s+(\d+)/)[1]),
      freeKiB: Number(text("/proc/meminfo").match(/MemAvailable:\s+(\d+)/)[1]),
    };
  }
  const start = values();
  let peak = 0,
    doneAtPeak = 0,
    peakUnfinished = 0,
    minAvailableKiB = start.freeKiB,
    samples = 0,
    missed = 0;
  function tick() {
    const now = values();
    minAvailableKiB = Math.min(minAvailableKiB, now.freeKiB);
    try {
      const v = ops.get();
      if (v.n > peak || (v.n === peak && v.done > doneAtPeak)) {
        peak = v.n;
        doneAtPeak = v.done;
      }
      peakUnfinished = Math.max(peakUnfinished, v.n - v.done);
      samples++;
    } catch {
      missed++;
    }
    if (!existsSync(dir + "/stop")) return;
    clearInterval(timer);
    const ticks = now.total - start.total;
    const pct = (d) => (ticks ? Math.round((d / ticks) * 1000) / 10 : null);
    write(
      dir + "/result",
      JSON.stringify({
        samples,
        missed,
        cpus: now.cpus,
        memoryMiB: Math.round(now.totalKiB / 1024),
        cpuPressurePct:
          now.pressure === null
            ? null
            : Math.round((now.pressure - start.pressure) / (now.at - start.at)) / 10,
        cpuBusyPct: pct(
          ticks - (now.idle - start.idle) - (now.io - start.io) - (now.steal - start.steal),
        ),
        ioWaitPct: pct(now.io - start.io),
        minAvailableMiB: Math.round(minAvailableKiB / 1024),
        oomKills: now.oom - start.oom,
        peakOperations: peak,
        finishedAtPeak: doneAtPeak,
        peakUnfinished,
      }),
    );
    db.close();
  }
  const timer = setInterval(tick, 200);
  tick();
  write(dir + "/ready", "");
}
const dir = "/tmp/pma-tool-mix-sampler";
export async function startGuestSampler(guest) {
  const source = Buffer.from("(" + sampleGuest.toString() + ")()").toString("base64");
  await guest.command(`mkdir -p ${dir} && rm -f ${dir}/{ready,stop,result} && printf '%s' '${source}' | base64 -d > ${dir}/sample.cjs && (node ${dir}/sample.cjs </dev/null >${dir}/log 2>&1 &)
    for i in {1..60}; do test -f ${dir}/ready && exit 0; sleep 0.05; done; exit 1`);
  return async () => {
    const data = await guest.command(
      `touch ${dir}/stop; for i in {1..60}; do if test -s ${dir}/result; then cat ${dir}/result; exit 0; fi; sleep 0.05; done; exit 1`,
    );
    const summary = JSON.parse(data);
    assert(summary.samples > 0);
    return summary;
  };
}
