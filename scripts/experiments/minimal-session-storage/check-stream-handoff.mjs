import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";

// Observe the same stream clients use, and kill the actual Worker after the
// first partials, before the corresponding canonical Entry reaches PG.
export async function checkStreamHandoff({
  api,
  pool,
  session,
  fake,
  until,
  startWorker,
  kill,
  prune,
}) {
  const first = await startWorker(0);
  if (prune) {
    let anchor;
    for (const text of ["keep", "prune"]) {
      const accepted = await api.acceptTurn(session, fake.prompt(text), "turn:" + randomUUID());
      const message = await until(
        "completed history to prune",
        async () =>
          (
            await pool.query(
              `select t.id,m.entry_id from pma_segments.turns t join pma_segments.messages m on m.run_id=t.id
           where t.id=$1 and t.state='completed' and m.role='assistant' order by m.commit_seq desc limit 1`,
              [accepted.turnId],
            )
          ).rows[0],
        30000,
      );
      anchor ??= message;
    }
    await api.pruneConversation(
      session,
      anchor.id,
      String(anchor.entry_id),
      "prune:" + randomUUID(),
    );
  }
  const abort = new AbortController();
  let connected = false;
  const positions = new Set();
  let turn;
  let terminal;
  const connection = api.streamSession({
    sessionId: session,
    signal: abort.signal,
    onStatus() {},
    onSnapshot: () => {
      connected = true;
    },
    onEvent(event) {
      if (event.type === "assistant.text.delta") {
        turn = event.turnId;
        positions.add(event.seq);
      }
      if (event.type === "turn.completed") terminal = event;
    },
  });
  try {
    await until("subscribed before the Turn", () => connected, 15000);
    await api.acceptTurn(
      session,
      fake.prompt("handoff_text", {
        outputBytes: 640,
        chunkBytes: 16,
        chunkMs: 220,
      }),
      "turn:" + randomUUID(),
    );
    await until(
      "original Worker emitted multiple visible partials",
      () => positions.size >= 2,
      30000,
    );
    await kill(first);
    const seal = await until(
      "old writer sealed",
      async () =>
        (
          await pool.query(
            `select session_id,output_display_seq,native_work_handoff from pma_segments.turns
       where id=$1 and state='queued' and native_work_handoff is not null`,
            [turn],
          )
        ).rows[0],
      60000,
    );
    const state = JSON.parse(seal.native_work_handoff.nativeText);
    const published = new Map(state.sequences).get(seal.session_id);
    assert(published >= Math.max(...positions) && published > Number(seal.output_display_seq));
    await startWorker(1);
    await until(
      "replacement completed the same Turn and delivered its SSE terminal",
      () => terminal?.turnId === turn,
      45000,
    );
    assert(terminal.seq > published);
    const {
      rows: [result],
    } = await pool.query(
      `select t.state, (select count(distinct worker_id)::int from public.session_leases l
        where l.pi_session_id=$2 and l.writer_seal_offset is not null) as owners,
        (select count(*)::int from pma_segments.messages where run_id=t.id and role='assistant'
          and content::jsonb#>>'{model,0,stopReason}'='stop') as answers
        from pma_segments.turns t where t.id=$1`,
      [turn, session],
    );
    assert.deepEqual(result, { state: "completed", owners: 2, answers: 1 });
    console.log(
      JSON.stringify({
        event: "session.stream.handoff",
        before: published,
        terminal: terminal.seq,
        ...result,
      }),
    );
  } finally {
    abort.abort();
    await connection;
  }
}

export async function checkStreamReplay({
  api,
  pool,
  session,
  project,
  partitions,
  partitionFor,
  fake,
  until,
  startWorker,
  pause,
  resume,
  restartProjector,
}) {
  const partition = partitionFor(session, partitions);
  let neighbor;
  for (let i = 0; i < 64; i++) {
    const other = await api.createSession(
      project.projectId,
      project.workspaceId,
      "Interleaved stream",
      "elastic",
    );
    if (partitionFor(other.sessionId, partitions) === partition) {
      neighbor = other.sessionId;
      break;
    }
  }
  assert(neighbor);
  const worker = await startWorker(0);
  const abort = new AbortController();
  let connected = false,
    prefix = "",
    completed = false,
    latestSnapshot;
  const connection = api.streamSession({
    sessionId: session,
    signal: abort.signal,
    onStatus() {},
    onSnapshot(s) {
      connected = true;
      latestSnapshot = s;
    },
    onEvent(e) {
      if (e.type === "assistant.text.delta")
        prefix = e.payload.replace ? e.payload.text : prefix + e.payload.text;
      if (e.type === "turn.completed") completed = true;
    },
  });
  let paused = false;
  try {
    await until("display subscribed", () => connected, 15000);
    const slow = await api.acceptTurn(
      session,
      fake.prompt("replay_prefix", { outputBytes: 2048, chunkBytes: 16, chunkMs: 180 }),
      "turn:" + randomUUID(),
    );
    await until("first text", () => prefix.length >= 48, 20000);
    const before = prefix;
    const other = await api.acceptTurn(
      neighbor,
      fake.prompt("advance_partition"),
      "turn:" + randomUUID(),
    );
    await until(
      "neighbor advanced the shared PG cursor",
      async () =>
        (
          await pool.query(
            "select from pma_segments.turns where id=$1 and state='completed' and output_seal_offset is not null",
            [other.turnId],
          )
        ).rowCount,
      15000,
    );
    pause(worker);
    paused = true;
    assert.equal(
      (
        await pool.query("select from pma_segments.messages where run_id=$1 and role='assistant'", [
          slow.turnId,
        ])
      ).rowCount,
      0,
    );
    latestSnapshot = undefined;
    await restartProjector();
    const text = await until(
      "reopened SSE view contains the original uncommitted text",
      () => {
        const items =
          latestSnapshot?.conversation.turns.find((t) => t.turnId === slow.turnId)?.transcript
            ?.items ?? [];
        const value = items
          .filter((i) => i.kind === "text")
          .map((i) => i.text)
          .join("");
        return value.startsWith(before) && value;
      },
      15000,
    );
    resume(worker);
    paused = false;
    await until("stream completed on the original writer", () => completed, 50000);
    console.log(
      JSON.stringify({
        event: "session.stream.replay",
        partition,
        observedBytes: Buffer.byteLength(before),
        recoveredBytes: Buffer.byteLength(text),
      }),
    );
  } finally {
    if (paused) resume(worker);
    abort.abort();
    await connection;
  }
}
