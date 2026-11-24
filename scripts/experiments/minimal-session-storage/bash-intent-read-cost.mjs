import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { seedToolsWindow } from "./scroll-demand-study.mjs";
import { pageCpuSnapshot, pageCpuDelta } from "./page-backend-cpu.mjs";
import { setBashLogReader } from "./bash-intent-log.mjs";

export async function seedActiveBashHistory(api, tenant) {
  const { db } = api,
    session = await api.createScrollWindow("Long active Bash history");
  const count = 1024;
  await seedToolsWindow(api, tenant, undefined, { id: session, runs: 1, toolCount: count });
  const {
    rows: [r],
  } = await db.query("select id from runs where tenant_id=$1 and session_id=$2", [tenant, session]);
  // Keep the last operation pending; leave both readable representations on
  // identical physical data and alternate the public reader's source.
  await db.query(
    "delete from messages where run_id=$1 and (role='toolResult' and tool_task_id=$2 or role='assistant' and entry_id>$2)",
    [r.id, 3 * count - 1],
  );
  await db.query(
    `insert into messages(tenant_id,pi_session_id,run_id,conversation_id,role,tool_task_id,assistant_entry_id,commit_seq,ordinal,content,display_through,bash_presentation_bytes,bash_operation,bash_dispatch_offset,bash_reply_topic,bash_reply_floor)
   select tenant_id,pi_session_id,run_id,1,'pma.bash-intent',entry_id+1,entry_id,commit_seq+1,0,jsonb_build_object('args','{}'::jsonb,'callId','c-'||entry_id)::text,
   (2*(entry_id-1)/3)+1,400,gen_random_uuid(),commit_seq+1,$2,0 from messages where run_id=$1 and role='assistant'`,
    [r.id, "pi-cloud.tool-replies.v4." + randomUUID()],
  );
  await db.query("analyze messages");
  const n = 2 * count - 1,
    writer = randomUUID();
  await db.query(
    "update runs set state='running',lease_id=$2,output_display_seq=$3,output_display_native_seq=$4 where id=$1",
    [r.id, writer, n, 3 * count - 1],
  );
  const raw = (await db.query("select bash_wait_refs($1,$2,$3) value", [tenant, session, r.id]))
    .rows[0].value;
  assert.equal(Object.keys(raw).length, 1);
  const wait = Object.values(raw)[0];
  delete wait.commandFromProposal;
  wait.arguments.command = "npm test";
  wait.reply.deadlineAt = Date.UTC(2026, 10, 26);
  const select = async (mode) => {
    await db.query(
      "update runs set projection_tools=$2,output_display_seq=$3,output_display_native_seq=$4,projection_state=$5 where id=$1",
      [
        r.id,
        mode === "combined" ? raw : {},
        mode === "combined" ? n : 0,
        mode === "combined" ? 3 * count - 1 : 0,
        mode === "checkpoint" ? { writer, seq: 3 * (count - 1) } : null,
      ],
    );
  };
  return { session, n, count, select };
}

export async function measureBashHistoryRead(api, tenant) {
  const { db } = api;
  const { session, n, count, select } = await seedActiveBashHistory(api, tenant);
  const trials = [],
    pageOracles = new Map();
  let expected;
  for (const mode of [
    "combined",
    "log",
    "checkpoint",
    "bounded",
    "bounded",
    "checkpoint",
    "log",
    "combined",
  ]) {
    await setBashLogReader(db, mode === "combined" ? false : "coverage", mode === "bounded");
    await select(mode);
    const get = () => api.page(session, {});
    const page = await get();
    assert.equal(page.turns[0].transcript.throughSequence, n);
    assert(
      page.turns[0].transcript.items.some(
        (x) => x.kind === "tool" && x.status === "running" && x.input.command === "npm test",
      ),
    );
    if (expected) assert.deepEqual(page, expected);
    else expected = page;
    const earlier = page.history.before;
    for (const direction of ["latest", "older"]) {
      const read = () => api.page(session, direction === "latest" ? {} : { before: earlier });
      const oracle = await read();
      if (pageOracles.has(direction)) assert.deepEqual(oracle, pageOracles.get(direction));
      else pageOracles.set(direction, oracle);
      await api.flushStats();
      const before = await pageCpuSnapshot(db);
      const pages = 16;
      for (let i = 0; i < pages; i++) assert.deepEqual(await read(), oracle);
      await api.flushStats();
      const used = pageCpuDelta(before, await pageCpuSnapshot(db));
      trials.push({ mode, direction, pgMsPerPage: (used.history + used.control) / pages });
    }
  }
  console.log(
    JSON.stringify({
      kind: "bash-history-read-cost",
      operations: count,
      pending: 1,
      completed: count - 1,
      pagesPerBlock: 16,
      trials,
    }),
  );
}
