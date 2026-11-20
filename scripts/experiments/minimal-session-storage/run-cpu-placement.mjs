import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { spawn } from "node:child_process";
import { readFile, mkdir, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { isCommitStudy, isAsyncStudy } from "./pg-group-commit.mjs";
import { isPrefixStudy } from "./ready-projection.mjs";
import { runtimeDirectory, serviceAddress, kubectl } from "../../lib/production-kubernetes.mjs";
import { serializeRelease } from "../../lib/release-lock.mjs";
import {
  assertUnusedFixture,
  createPods,
  localPlan,
  removePods,
} from "./cpu-placement-fixture.mjs";

assert.equal(process.env.PI_CLOUD_RUNTIME_LOAD_CHECK, "1");
const [output, profile = "cpu"] = process.argv.slice(2);
assert(
  output &&
    (["cpu", "cpu-smoke", "cpu-confirm"].includes(profile) ||
      isCommitStudy(profile) ||
      isPrefixStudy(profile)),
);
serializeRelease(runtimeDirectory);
process.umask(0o077);
const out = resolve(output);
await mkdir(dirname(out), { recursive: true, mode: 0o700 });
await mkdir(out, { recursive: false, mode: 0o700 });
const deployed = new URL(
  (await readFile(runtimeDirectory + "/secrets/database-url", "utf8")).trim(),
);
deployed.hostname = serviceAddress("postgres");
const operator = new pg.Client({
  connectionString: deployed.href,
  application_name: "pma-cpu-study-preflight",
  connectionTimeoutMillis: 5000,
});
try {
  await operator.connect();
  assert.equal(
    (
      await operator.query(
        "select count(*)::int active from runs where state not in ('completed','cancelled','failed','timed_out') or recovery_requested_at is not null",
      )
    ).rows[0].active,
    0,
    "Unrelated active work prevents measurement",
  );
} finally {
  await operator.end();
}
const prefix = "pma-cpu-" + randomBytes(8).toString("hex"),
  plan = localPlan();
assertUnusedFixture(prefix);
let child,
  interrupted = false;
function stop() {
  interrupted = true;
  child?.kill("SIGTERM");
}
process.on("SIGINT", stop);
process.on("SIGTERM", stop);
try {
  createPods(prefix);
  assert(!interrupted);
  child = spawn(
    "taskset",
    [
      "-c",
      plan.support,
      process.execPath,
      "--import",
      "tsx",
      fileURLToPath(new URL("./run.mjs", import.meta.url)),
      resolve(out, "run"),
      "wake-throughput",
      "pg-paths",
    ],
    {
      stdio: "inherit",
      env: {
        ...process.env,
        PI_CLOUD_CPU_FIXTURE: prefix,
        PI_CLOUD_PG_PATH_PROFILE: profile === "cpu-confirm" ? "cpu" : profile,
        PI_CLOUD_CPU_REVERSE: profile === "cpu-confirm" ? "1" : "0",
      },
    },
  );
  const code = await new Promise((res, rej) => {
    child.once("error", rej);
    child.once("close", res);
  });
  assert.equal(code, 0, "CPU study failed");
  if (isAsyncStudy(profile)) {
    const pod = prefix + "-pg";
    const owned = JSON.parse(kubectl(["get", "pod", pod, "-o", "json"]));
    assert.equal(owned.metadata.labels["pi-cloud.io/pg-cpu-study"], prefix);
    assert.equal(owned.metadata.labels["pi-cloud.io/pg-cpu-role"], "pg");
    assert(owned.spec.volumes.some((v) => v.name === "data" && v.emptyDir));
    const script = await readFile(new URL("./pg-async-crash.sh", import.meta.url), "utf8");
    const output = kubectl(["exec", "-i", pod, "--", "gosu", "postgres", "sh", "-s"], {
      input: script,
      timeout: 120000,
    });
    const trials = output
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line));
    assert.deepEqual(
      trials.map((x) => x.mode),
      ["on", "off", "off", "on", "on", "off"],
    );
    await writeFile(
      resolve(out, "crash-recovery.json"),
      JSON.stringify({ immediateStop: true, trials }, null, 2) + "\n",
      { mode: 0o600 },
    );
  }
} finally {
  removePods(prefix);
  await writeFile(
    resolve(out, "fixture-cleanup.json"),
    JSON.stringify({
      temporaryPodsRemoved: true,
      temporaryServiceRemoved: true,
      temporaryAuthRemoved: true,
    }) + "\n",
    { mode: 0o600 },
  );
  process.off("SIGINT", stop);
  process.off("SIGTERM", stop);
}
