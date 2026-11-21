// The pre-crash Projector owns its own Kafka client, route cache and HTTP API.
// Only the supervised Guest endpoint and test observations cross this process boundary.
import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { once } from "node:events";
import { fileURLToPath } from "node:url";

export async function startInitialProjector(topic, resources, cases) {
  const child = fork(
    fileURLToPath(new URL("./serving-family-study.mjs", import.meta.url)),
    [process.argv[2], process.argv[3], "initial-projector"],
    {
      serialization: "advanced",
      execArgv: ["--import", "tsx"],
      stdio: ["ignore", "inherit", "inherit", "ipc"],
      env: { ...process.env, PMA_FAMILY_PROJECTOR_TOPIC: topic },
    },
  );
  const exited = once(child, "exit"),
    ready = Promise.withResolvers(),
    registered = new Map();
  let failure,
    expectedExit = false;
  child.on("error", (err) => {
    failure = err;
    ready.reject(err);
  });
  child.on("exit", (code, signal) => {
    if (!expectedExit)
      failure ??= Error("Initial Projector exited unexpectedly: " + (signal ?? code));
    if (failure) {
      ready.reject(failure);
      for (const p of registered.values()) p.reject(failure);
    }
  });
  child.on("message", async (m) => {
    try {
      if (m.kind === "ready") return ready.resolve(m.origin);
      if (m.kind === "registered") return registered.get(m.run).resolve();
      if (m.kind === "progress") {
        cases.get(m.run).seq = m.seq;
        return;
      }
      assert.equal(m.kind, "submit");
      await resources.get(m.binding).submit(m.record, m.command, m.reply);
      if (child.connected) child.send({ kind: "accepted", op: m.command.request.operationId });
    } catch (err) {
      failure = err;
      ready.reject(err);
      if (child.connected) child.kill("SIGKILL");
    }
  });
  async function kill() {
    expectedExit = true;
    child.kill("SIGKILL");
    const [, signal] = await exited;
    assert.equal(signal, "SIGKILL");
  }
  const timer = setTimeout(() => ready.reject(Error("Initial Projector startup timed out")), 30000);
  try {
    const origin = await ready.promise;
    return {
      origin,
      pid: child.pid,
      checkHealth() {
        if (failure) throw failure;
      },
      kill,
      async register(run, binding) {
        if (failure) throw failure;
        const p = Promise.withResolvers();
        registered.set(run, p);
        child.send({ kind: "register", run, binding });
        await p.promise;
        registered.delete(run);
      },
    };
  } catch (err) {
    await kill().catch(() => {});
    throw err;
  } finally {
    clearTimeout(timer);
  }
}
