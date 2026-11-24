import { fork } from "node:child_process";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";
export async function until(fn, label, timeout = 60000) {
  const end = Date.now() + timeout;
  do {
    const v = await fn();
    if (v) return v;
    await delay(80);
  } while (Date.now() < end);
  throw Error("Timed out " + label);
}
export function familyWorker(options, workerModule = "./retained-family-worker.mjs") {
  const events = [];
  const proc = fork(fileURLToPath(new URL(workerModule, import.meta.url)), [], {
    execArgv: ["--import", "tsx"],
    stdio: ["ignore", "inherit", "inherit", "ipc"],
  });
  let exit;
  const ended = new Promise((resolve) =>
    proc.once("exit", (code, signal) => {
      exit = { code, signal };
      resolve(exit);
    }),
  );
  proc.on("message", (m) => events.push(m));
  proc.send(options);
  function check() {
    const error = events.find((e) => e.kind === "error");
    if (error) throw Error(error.message + " " + error.where?.join(" "));
  }
  return {
    proc,
    ended,
    check,
    wait: (kind) =>
      until(() => {
        check();
        const m = events.find((e) => e.kind === kind);
        if (!m && exit) throw Error("Actor exited before " + kind + " " + JSON.stringify(exit));
        return m;
      }, kind),
    async close() {
      if (!exit) {
        proc.kill("SIGKILL");
        await ended;
      }
    },
  };
}
