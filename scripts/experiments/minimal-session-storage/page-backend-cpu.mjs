// Linux PG fixture shares the host clock-tick frequency. /proc field 3 is
// the first token after the process name; fields 14 and 15 are CPU ticks.
import { execFileSync } from "node:child_process";
import assert from "node:assert/strict";
const ticks = Number(execFileSync("getconf", ["CLK_TCK"], { encoding: "utf8" }));
assert(Number.isFinite(ticks) && ticks > 0);
export function cpuMillis(stat) {
  const f = stat
    .slice(stat.lastIndexOf(")") + 2)
    .trim()
    .split(/\s+/);
  return ((Number(f[11]) + Number(f[12])) * 1000) / ticks;
}
export async function pageCpuSnapshot(db) {
  const { rows } =
    await db.query(`select application_name role,pid,pg_read_file('/proc/'||pid::text||'/stat',0,8192,true) stat
 from pg_stat_activity where datname=current_database() and application_name in ('flow-page','flow-control')`);
  const {
    rows: [{ body }],
  } = await db.query("select pg_read_file('/sys/fs/cgroup/cpu.stat') body");
  return {
    backends: new Map(
      rows.filter((r) => r.stat).map((r) => [r.pid, { role: r.role, ms: cpuMillis(r.stat) }]),
    ),
    container: Number(/^usage_usec (\d+)/m.exec(body)[1]) / 1000,
  };
}
export function pageCpuDelta(a, b) {
  const deltas = { "flow-page": 0, "flow-control": 0 };
  for (const [pid, r] of b.backends) deltas[r.role] += r.ms - (a.backends.get(pid)?.ms ?? 0);
  return {
    history: deltas["flow-page"],
    control: deltas["flow-control"],
    container: b.container - a.container,
  };
}
