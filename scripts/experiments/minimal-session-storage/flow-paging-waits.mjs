// Diagnostic controls for the isolated API / Kafka fixture.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import pg from "pg";
import { applyMovingSql } from "./ready-projection-sql.mjs";

export async function provePagingVisibility(api, tenant, flow) {
  const { db } = api,
    run = randomUUID(),
    writer = randomUUID(),
    topic = "paging-visibility";
  const held = await db.connect(),
    snapshot = await db.connect(),
    second = await db.connect();
  const marker = (n) => "held-projection-answer-" + n;
  const write = async (n) => {
    const content = JSON.stringify({
      id: n,
      kind: "pi.assistant",
      model: [
        {
          role: "assistant",
          content: [{ type: "text", text: marker(n) }],
          stopReason: "stop",
          timestamp: 1,
        },
      ],
    });
    const args = [
      tenant,
      flow,
      run,
      n,
      JSON.stringify([{ id: n, conversation: 1, role: "assistant", content, ordinal: 0 }]),
      writer,
    ];
    const frame = {
      offset: String(n - 1),
      nativeSeq: n,
      history: true,
      ops: [{ kind: "message", args, count: 1 }],
      rootDisplay: { tenant, session: flow, run, writer, seq: n, through: n },
    };
    const r = await held.query(applyMovingSql, [
      topic,
      0,
      n - 1,
      n,
      JSON.stringify([frame]),
      true,
      false,
    ]);
    assert.equal(r.rows[0].outcome, "applied");
  };
  const seen = async () => {
    const t = performance.now(),
      page = await api.page(api.live);
    assert.equal(page.turns.length, 1);
    assert.equal(page.turns[0].runId, run);
    const body = JSON.stringify(page.turns[0].transcript);
    assert(body.includes(marker(1)));
    return { ms: +(performance.now() - t).toFixed(2), second: body.includes(marker(2)) };
  };
  const count = async (c) =>
    Number((await c.query("select count(*) from messages where run_id=$1", [run])).rows[0].count);
  let waiting;
  try {
    await db.query("insert into accepted_fact_projection_offsets values($1,0,0)", [topic]);
    await db.query(
      "insert into runs(id,tenant_id,session_id,initial_input,model_ref,idempotency_key,state,lease_id,mailbox_position) values($1,$2,$3,'visibility','fixture',$1,'running',$4,1)",
      [run, tenant, flow, writer],
    );
    await write(1);
    await snapshot.query("begin isolation level repeatable read read only");
    assert.equal(await count(snapshot), 1);
    await held.query("begin");
    await write(2);
    const start = performance.now();
    assert.equal(await count(held), 2);
    const before = await seen();
    assert.equal(before.second, false);
    // A competing writer is a positive control for the blocker detector.
    waiting = second.query("select id from runs where id=$1 for update", [run]);
    waiting.catch(() => {});
    let conflict;
    for (let i = 0; i < 100; i++) {
      conflict = (
        await db.query(
          "select wait_event_type,wait_event,$1::int=any(pg_blocking_pids(pid)) blocked from pg_stat_activity where pid=$2",
          [held.processID, second.processID],
        )
      ).rows[0];
      if (conflict?.blocked) break;
      await delay(10);
    }
    assert(conflict.blocked && conflict.wait_event_type === "Lock");
    const during = (
      await db.query(
        `select relation::regclass::text rel,mode from pg_locks
     where pid=$1 and granted and relation in ('runs'::regclass,'messages'::regclass,'accepted_fact_projection_offsets'::regclass) order by 1,2`,
        [held.processID],
      )
    ).rows;
    await delay(800);
    await held.query("commit");
    await waiting;
    const heldMs = +(performance.now() - start).toFixed(2);
    assert.equal(await count(snapshot), 1);
    const after = await seen();
    assert(after.second);
    await snapshot.query("commit");
    assert.equal(await count(snapshot), 2);
    console.log(
      JSON.stringify({
        mode: "same-run-visibility",
        before,
        after,
        heldMs,
        oldSnapshotCount: 1,
        freshCount: 2,
        positiveControl: conflict,
        writerRelationLocks: during,
      }),
    );
  } finally {
    try {
      await held.query("rollback");
      await snapshot.query("rollback");
      if (waiting) await waiting;
    } finally {
      held.release();
      snapshot.release();
      second.release();
    }
    await db.query("delete from messages where run_id=$1", [run]);
    await db.query("delete from runs where id=$1", [run]);
    await db.query("delete from accepted_fact_projection_offsets where topic=$1", [topic]);
  }
}

const sampleSql = `with watched as materialized (
 select pid,application_name,wait_event_type,wait_event from pg_stat_activity
 where datname=current_database() and application_name in ('flow-projector','flow-page','flow-control') and state='active'
) select coalesce((select json_agg(json_build_object('role',application_name,'event',coalesce(wait_event_type||':'||wait_event,'no reported wait'),
 'blockers',case when wait_event_type='Lock' then array(select coalesce(b.application_name,'other') from unnest(pg_blocking_pids(w.pid)) p(pid)
 left join pg_stat_activity b on b.pid=p.pid) end)) from watched w),'[]') active,
 pg_read_file('/sys/fs/cgroup/cpu.stat') cpu,pg_read_file('/sys/fs/cgroup/cpu.max') quota`;

export async function observePagingWaits(url, cpus) {
  const client = new pg.Client({ connectionString: url, application_name: "flow-wait-observer" });
  await client.connect();
  let stopped = false;
  const samples = [];
  const running = (async () => {
    while (!stopped) {
      const from = performance.now();
      const {
        rows: [r],
      } = await client.query(sampleSql);
      const cpu = Object.fromEntries(
        r.cpu
          .trim()
          .split("\n")
          .map((s) => s.split(" ")),
      );
      const [quota, period] = r.quota.trim().split(/\s+/).map(Number);
      assert.equal(quota / period, cpus);
      samples.push({ at: (from + performance.now()) / 2, cpu, active: r.active });
      await delay(20);
    }
  })();
  running.catch(() => {});
  return {
    async stop() {
      stopped = true;
      try {
        await running;
      } finally {
        await client.end();
      }
    },
    summary(start, end) {
      const selected = samples.filter((s) => s.at >= start && s.at <= end);
      assert(selected.length > 5);
      const first = selected[0],
        last = selected.at(-1),
        events = {},
        conflicts = {};
      let maxGap = 0;
      for (const [i, s] of selected.entries()) {
        if (i) maxGap = Math.max(maxGap, s.at - selected[i - 1].at);
        for (const row of s.active) {
          const bucket = (events[row.role] ??= {});
          bucket[row.event] = (bucket[row.event] ?? 0) + 1;
          for (const holder of row.blockers ?? []) {
            const key = row.role + " waits for " + holder;
            conflicts[key] = (conflicts[key] ?? 0) + 1;
          }
        }
      }
      const delta = (k) => Number(last.cpu[k]) - Number(first.cpu[k]);
      return {
        cpus,
        samples: selected.length,
        maxGapMs: +maxGap.toFixed(1),
        activeBackendObservations: events,
        blockingPairs: conflicts,
        pgCoresUsed: +(delta("usage_usec") / 1000 / (last.at - first.at)).toFixed(2),
        quotaPeriods: delta("nr_periods"),
        throttledPeriods: delta("nr_throttled"),
      };
    },
  };
}
