// Only synthetic data in an owned physical primary/standby pair.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import pg from "pg";
import { applyMovingSql } from "./ready-projection-sql.mjs";
import { createDatabase } from "@pi-cloud/database";
import { createPiCloudEventFactory } from "@pi-cloud/protocol";
import { ConversationReader } from "../../../packages/control-plane/src/conversation-reader.ts";

const pool = (url) =>
  new pg.Pool({
    connectionString: url,
    max: 2,
    application_name: "standby-observer",
    connectionTimeoutMillis: 1000,
    statement_timeout: 2000,
  });
const rounded = (n) => +n.toFixed(2);
const lsn = async (db) => (await db.query("select pg_current_wal_insert_lsn() pos")).rows[0].pos;
const gap = async (db, pos) =>
  Number(
    (
      await db.query(
        "select greatest(0,pg_wal_lsn_diff($1::pg_lsn,pg_last_wal_replay_lsn())) bytes",
        [pos],
      )
    ).rows[0].bytes,
  );
async function caughtUp(a, b) {
  const mark = await lsn(a);
  const end = performance.now() + 45000;
  while (await gap(b, mark)) {
    assert(performance.now() < end, "Standby did not catch the fixed watermark");
    await delay(5);
  }
}
export async function waitForFixtureReplay(url, standbyUrl) {
  const a = pool(url),
    b = pool(standbyUrl);
  try {
    assert((await b.query("select pg_is_in_recovery() yes")).rows[0].yes);
    const sys = "select system_identifier::text id from pg_control_system()";
    assert.equal((await a.query(sys)).rows[0].id, (await b.query(sys)).rows[0].id);
    await caughtUp(a, b);
  } finally {
    await Promise.all([a.end(), b.end()]);
  }
}
export function splitTrials(withFrozenControl) {
  if (withFrozenControl) {
    // The synthetic read windows are sealed and unmodified throughout these
    // replays. The fixed-data control gives no freshness guarantee for users.
    const order = [
      "primary",
      "frozen",
      "standby",
      "frozen",
      "standby",
      "primary",
      "standby",
      "primary",
      "frozen",
    ];
    return [
      ["primary", "writer"],
      ...order.map((arm) => [arm, "saturated"]),
      ["primary", "writer"],
    ];
  }

  const tests = [];
  for (const mode of [32, "saturated"]) {
    tests.push(["primary", "writer"]);
    for (const arm of [
      "primary",
      "standby",
      "standby",
      "primary",
      "standby",
      "primary",
      "primary",
      "standby",
    ])
      tests.push([arm, mode]);
    tests.push(["primary", "writer"]);
  }
  return tests;
}
export async function checkStudySse(split, id, onSnapshot, onEvent) {
  const stop = new AbortController();
  const timer = setTimeout(() => stop.abort(Error("SSE deadline")), 8000);
  let got = false;
  try {
    await split.stream({
      sessionId: id,
      signal: stop.signal,
      onStatus() {},
      onSnapshot(s) {
        got = true;
        onSnapshot(s, stop);
      },
      onEvent(e) {
        onEvent(e, stop);
      },
    });
    assert(got && !stop.signal.reason?.message?.includes("deadline"), "Missing SSE evidence");
  } finally {
    clearTimeout(timer);
    stop.abort();
  }
}
export async function studyReplicaConsistency(url, standbyUrl, tenant, api) {
  const a = api.db,
    b = pool(standbyUrl),
    readerUrl = new URL(standbyUrl);
  readerUrl.searchParams.set("options", "-c search_path=flow_web,flow_study,public");
  const readDb = createDatabase({ connectionString: readerUrl.href, maxConnections: 2 });
  const root = api.arms.root.id,
    id = await api.createWindow("replica prune"),
    flow = randomUUID();
  const run = randomUUID(),
    { split } = api.arms.branch,
    raw = new ConversationReader(readDb, tenant, true, true);
  const db = await a.connect();
  let paused = false;
  try {
    const {
      rows: [{ base, cut }],
    } = await a.query("select fixture_run($1::uuid,800) base,fixture_run($1::uuid,100) cut", [
      root,
    ]);
    await a.query("select flow_fork($1,$2,$3,$4,$5)", [tenant, root, base, id, flow]);
    await caughtUp(a, b);
    const latest = await split.page({}, id);
    assert.equal(latest.turns.at(-1).prompt, "prompt:800");
    const cursor = { before: latest.history.before };
    const first = await split.page(cursor, id);
    assert(first.turns.length);
    await b.query("select pg_wal_replay_pause()");
    paused = true;
    const deadline = performance.now() + 5000;
    while ((await b.query("select pg_get_wal_replay_pause_state() s")).rows[0].s !== "paused") {
      assert(performance.now() < deadline);
      await delay(5);
    }
    await db.query("begin");
    assert.equal((await db.query("show synchronous_commit")).rows[0].synchronous_commit, "on");
    await db.query("select flow_detach($1,$2,$3,$4)", [tenant, id, flow, cut]);
    const nextFlow = (await db.query("select flow_id from sessions where id=$1", [id])).rows[0]
      .flow_id;
    await db.query("select flow_admit($1,$2,$3,$4,'primary-after-prune')", [
      tenant,
      id,
      nextFlow,
      run,
    ]);
    const writer = randomUUID(),
      topic = "replica-visibility";
    await db.query("update runs set state='running',started_at=now(),lease_id=$2 where id=$1", [
      run,
      writer,
    ]);
    await db.query("insert into accepted_fact_projection_offsets values($1,0,0)", [topic]);
    await db.query("commit");
    const body = JSON.stringify({
      id: 1,
      kind: "pi.assistant",
      model: [
        {
          role: "assistant",
          content: [{ type: "text", text: "persisted-first" }],
          stopReason: "stop",
          timestamp: 1,
        },
      ],
    });
    const args = [
      tenant,
      nextFlow,
      run,
      1,
      JSON.stringify([{ id: 1, conversation: 1, role: "assistant", content: body, ordinal: 0 }]),
      writer,
    ];
    const frame = {
      offset: "0",
      nativeSeq: 1,
      history: true,
      ops: [{ kind: "message", args, count: 1 }],
      rootDisplay: { tenant, session: nextFlow, run, writer, seq: 1, through: 1 },
    };
    const projected = (
      await db.query(applyMovingSql, [topic, 0, 0, 1, JSON.stringify([frame]), true, false])
    ).rows[0];
    assert.equal(projected.outcome, "applied");
    assert.equal(projected.mode, "off");
    assert.deepEqual((await raw.getConversation(id, cursor)).turns, first.turns);
    const before = await split.counts();
    await assert.rejects(split.page(cursor, id), { status: 404 });
    const after = await split.counts();
    assert.equal(after.lagFallback - before.lagFallback, 1);
    const recent = await split.page({}, id);
    assert.equal(recent.turns.at(-1).runId, run);
    assert(recent.turns.some((t) => t.prompt === "prompt:100"));
    assert(!recent.turns.some((t) => t.mailboxPosition > 101));
    const events = createPiCloudEventFactory(
      { sessionId: id, turnId: run, agentId: "root" },
      { initialSequence: 1000010 },
    );
    function delta(text) {
      return events.next({
        type: "assistant.text.delta",
        payload: { messageId: "replica-fixture-tail", text, phase: "final_answer" },
      });
    }
    const old = delta("live-first"),
      next = delta("live-second");
    await split.liveEvent(tenant, old);
    const texts = (s) =>
      s.conversation.turns
        .find((t) => t.runId === run)
        ?.transcript?.items.filter((i) => i.kind === "text")
        .map((i) => i.text)
        .join(" ") ?? "";
    let sent,
      delivered = false;
    await checkStudySse(
      split,
      id,
      (s) => {
        const text = texts(s);
        assert(text.includes("persisted-first") && text.includes("live-first"));
        sent = split.liveEvent(tenant, next);
      },
      (e, stop) => {
        assert.equal(e.seq, next.seq);
        delivered = true;
        stop.abort();
      },
    );
    await sent;
    assert(delivered);
    await checkStudySse(
      split,
      id,
      (s, stop) => {
        const text = texts(s);
        for (const marker of ["persisted-first", "live-first", "live-second"])
          assert.equal(text.split(marker).length, 2);
        stop.abort();
      },
      () => assert.fail(),
    );
    assert.deepEqual(await split.counts(), after);
    await b.query("select pg_wal_replay_resume()");
    paused = false;
    await caughtUp(a, b);
    assert.deepEqual(JSON.parse(JSON.stringify(await raw.getConversation(id, {}))), recent);
    assert.equal(
      (
        await b.query(
          "select next_offset from flow_study.accepted_fact_projection_offsets where topic=$1 and partition=0",
          [topic],
        )
      ).rows[0].next_offset,
      "1",
    );
    await assert.rejects(raw.getConversation(id, cursor), { code: "not_found" });
    const recoveryCursor = { before: recent.history.before };
    const recoveryPage = await api.arms.branch.page(recoveryCursor, id);
    const resumeDeadline = performance.now() + 6000;
    let resumeAttempts = 0;
    // A newer WAL insertion can arrive after the fixed catch-up check above.
    // Every fallback still has to return the exact primary page.
    do {
      resumeAttempts++;
      assert.deepEqual(await split.page(recoveryCursor, id), recoveryPage);
      if ((await split.counts()).replica > after.replica) break;
      assert(performance.now() < resumeDeadline, "Standby never resumed serving eligible pages");
      await delay(25);
    } while (true);
    console.log(
      JSON.stringify({
        mode: "replica-consistency",
        asyncCommit: true,
        unguardedOldPageVisible: true,
        guardedOldCursorStatus: 404,
        latestPrimary: true,
        ssePrimarySnapshots: 2,
        liveSuffixVerified: true,
        resumedReplicaVerified: true,
        resumeAttempts,
        realProjectedMessageAndOffset: true,
      }),
    );
  } finally {
    try {
      await db.query("rollback");
      if (paused) await b.query("select pg_wal_replay_resume()");
    } finally {
      db.release();
      await Promise.all([b.end(), readDb.destroy()]);
    }
  }
}
function dist(xs) {
  const sorted = xs.toSorted((a, b) => a - b);
  if (!xs.length) return null;
  return {
    n: xs.length,
    p50: rounded(sorted[Math.ceil(xs.length * 0.5) - 1]),
    p95: rounded(sorted[Math.ceil(xs.length * 0.95) - 1]),
    max: rounded(sorted.at(-1)),
  };
}
async function sampleDb(db, primary) {
  const {
    rows: [r],
  } =
    await db.query(`select ${primary ? "pg_current_wal_insert_lsn()" : "pg_last_wal_replay_lsn()"}::text pos,
 pg_read_file('/sys/fs/cgroup/cpu.stat') cpu`);
  const cpu = Number(/^usage_usec (\d+)/m.exec(r.cpu)[1]);
  return { pos: r.pos, cpu };
}
function numeric(pos) {
  const [a, b] = pos.split("/");
  return (BigInt("0x" + a) << 32n) + BigInt("0x" + b);
}
export async function observeReplay(url, standbyUrl) {
  const a = pool(url),
    b = pool(standbyUrl),
    samples = [];
  let closed = false,
    last;
  await caughtUp(a, b);
  const running = (async () => {
    while (!closed) {
      const source = await sampleDb(a, true),
        start = performance.now(),
        replica = await sampleDb(b, false);
      const behind = Number(numeric(source.pos) - numeric(replica.pos)),
        changed = source.pos !== last;
      last = source.pos;
      let caught = behind <= 0;
      while (!caught && !closed && performance.now() - start < 1500) {
        await delay(4);
        caught = (await gap(b, source.pos)) === 0;
      }
      samples.push({
        start,
        end: performance.now(),
        bytes: Math.max(0, behind),
        caught,
        changed,
        primary: source.cpu,
        replica: replica.cpu,
      });
      await delay(75);
    }
  })();
  running.catch(() => {});
  return {
    summary(start, end) {
      const inWindow = samples.filter((s) => s.start >= start && s.end <= end),
        x = inWindow[0],
        y = inWindow.at(-1);
      assert(inWindow.length >= 2);
      const moved = inWindow.filter((s) => s.changed);
      return {
        samples: inWindow.length,
        unreplayedBytes: dist(moved.map((s) => s.bytes)),
        watermarkCatchUpMs: dist(moved.filter((s) => s.caught).map((s) => s.end - s.start)),
        censored: moved.filter((s) => !s.caught).length,
        primaryCores: rounded((y.primary - x.primary) / 1000 / (y.start - x.start)),
        replicaCores: rounded((y.replica - x.replica) / 1000 / (y.start - x.start)),
      };
    },
    async close() {
      closed = true;
      try {
        await running;
      } finally {
        await Promise.all([a.end(), b.end()]);
      }
    },
  };
}
