import { Session } from "node:inspector/promises";
import { writeFile } from "node:fs/promises";

/** In-process V8 sampling for the temporary operator overlay; opens no port. */
export function projectorCpuProbe(path) {
  const session = new Session();
  session.connect();
  let started, stopped, startedAt;
  return {
    start() {
      if (stopped) return Promise.resolve();
      return (started ??= (async () => {
        await session.post("Profiler.enable");
        await session.post("Profiler.setSamplingInterval", { interval: 1000 });
        startedAt = Date.now();
        await session.post("Profiler.start");
      })());
    },
    stop() {
      return (stopped ??= (async () => {
        if (!started) {
          await writeFile(path, JSON.stringify({ noRecords: true }) + "\n", { mode: 0o600 });
          session.disconnect();
          return;
        }
        await started;
        const { profile } = await session.post("Profiler.stop");
        await writeFile(
          path,
          JSON.stringify({ startedAt, stoppedAt: Date.now(), profile }) + "\n",
          { mode: 0o600 },
        );
        session.disconnect();
      })());
    },
  };
}
