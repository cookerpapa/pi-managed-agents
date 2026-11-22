import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { installRetentionFixture, seedElasticWorkspace } from "./retention-fixture.mjs";

const original = { workspace: "first", cwd: "/workspace/game", processGeneration: 1 };
const moved = { ...original, cwd: "/workspace/game/client" };
const restarted = { ...moved, processGeneration: 2 };
const other = { workspace: "second", cwd: moved.cwd, processGeneration: 1 };
const snapshots = [original, moved, restarted, restarted, other, other, other];

// A control-plane fixture. Taking the same selection lock as admission orders idle
// edits with Run creation. The next statement checks active Runs on a fresh snapshot.
export async function selectEnvironment(db, tenant, session, value) {
  const client = await db.connect();
  try {
    await client.query("begin");
    await client.query(
      "insert into study_session_environments values($1,$2,$3) on conflict do nothing",
      [tenant, session, value],
    );
    await client.query(
      "select 1 from study_session_environments where tenant=$1 and session=$2 for update",
      [tenant, session],
    );
    const result = await client.query(
      `update study_session_environments set value=$3 where tenant=$1 and session=$2
         and not exists(select 1 from runs where tenant_id=$1 and session_id=$2 and state <> 'completed')`,
      [tenant, session, value],
    );
    assert.equal(result.rowCount, 1, "Session must be quiescent to change environment");
    await client.query("commit");
  } catch (error) {
    await client.query("rollback");
    throw error;
  } finally {
    client.release();
  }
}

export async function installContinuationEnvironments(db) {
  await db.query(
    "create table study_session_environments(tenant uuid, session uuid, value jsonb not null, primary key(tenant,session))",
  );
}

export async function runContinuationCases(wave, db, tenant) {
  await installContinuationEnvironments(db);
  for (const handoff of [false, true]) {
    const result = await wave({
      label: handoff ? "continuation-cold-replacement" : "continuation-preferred",
      environmentCases: snapshots,
      integrated: true,
      cacheMode: "prefer",
      wakeMode: "directed",
      cohort: 1,
      seedBytes: 320 * 1024,
      offline: ["w1"],
      ...(handoff ? { switchAt: 2 } : { joinIdleAt: 1 }),
      count: snapshots.length,
      concurrency: 1,
      verifyFull: true,
      holdFinish: true,
      sink: "single",
      prefixWidth: 16,
      projectionPolicy: "recorded-history",
      slots: 8,
      batch: 8,
      omitNotify: true,
      probeUnlisted: true,
      windowMs: 5,
      tickMs: 10000,
    });
    const trace = result.environmentTrace;
    const expected = [
      ["Current working directory: /workspace/game"],
      [
        "Current working directory: /workspace/game/client",
        "Working directory changed from /workspace/game to /workspace/game/client.",
      ],
      [
        "Current working directory: /workspace/game/client",
        "Sandbox restarted. Workspace files are preserved; earlier processes may be gone.",
      ],
      ["Current working directory: /workspace/game/client"],
      [
        "Current working directory: /workspace/game/client",
        "Workspace changed. Inspect the current files before reusing earlier paths.",
      ],
      ["Current working directory: /workspace/game/client"],
      ["Current working directory: /workspace/game/client"],
    ];
    assert.deepEqual(
      trace.map((t) => t.prompt.split("\n")),
      expected,
    );
    assert.deepEqual(
      trace.map((t) => t.prefix),
      handoff
        ? ["cold", "hit", "cold", "hit", "hit", "hit", "hit"]
        : ["cold", "hit", "hit", "hit", "hit", "hit", "hit"],
    );
    assert.deepEqual(
      trace.map((t) => t.worker),
      handoff ? ["w0", "w0", "w1", "w1", "w1", "w1", "w1"] : Array(7).fill("w0"),
    );
    assert(result.router.preferred >= (handoff ? 5 : 6));
    assert(trace.every((t) => (t.prefix === "hit" ? t.readBytes === 0 : t.readBytes > 0)));
  }

  // These allocations exist before a pure-chat continuation. The claims carry
  // admission outcomes, while the Worker has no physical VM identifier.
  await installRetentionFixture(db);
  const states = ["unallocated", "retained", "closing", "released", "unknown", "starting"];
  const cases = [];
  for (const state of states) {
    const workspace = randomUUID();
    await seedElasticWorkspace(db, tenant, workspace, state);
    cases.push({ workspace, cwd: "/workspace", executionMode: "elastic" });
  }
  for (const cold of [false, true]) {
    const result = await wave({
      label: "elastic-admission-" + (cold ? "replacement" : "preferred"),
      // Reuse the same Workspace through two pure-chat Runs.
      environmentCases: [cases[1], ...cases.slice(1), cases[0]],
      expectedElasticStates: ["retained", ...states.slice(1), "unallocated"],
      integrated: true,
      cacheMode: "prefer",
      wakeMode: "directed",
      cohort: 1,
      seedBytes: 320 * 1024,
      offline: ["w1"],
      ...(cold ? { switchAt: 1 } : { joinIdleAt: 1 }),
      count: cases.length + 1,
      concurrency: 1,
      verifyFull: true,
      sink: "single",
      prefixWidth: 16,
      projectionPolicy: "recorded-history",
      slots: 8,
      batch: 8,
      omitNotify: true,
      probeUnlisted: true,
      windowMs: 5,
      tickMs: 10000,
    });
    const trace = result.environmentTrace;
    assert.equal(trace[1].worker, cold ? "w1" : "w0");
    assert.equal(trace[1].prefix, cold ? "cold" : "hit");
    const hints = [
      /retained the existing/,
      /retained the existing/,
      /being reclaimed/,
      /had been released/,
      /state was unconfirmed/,
      /being prepared/,
      /No elastic sandbox was allocated/,
    ];
    for (const [i, item] of trace.entries()) assert.match(item.prompt, hints[i]);
  }
}
