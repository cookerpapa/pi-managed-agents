import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { it } from "vitest";
import pg from "pg";
import { installPagingProjection } from "./flow-paging-fixture.mjs";
import {
  installBashIntentLog,
  setBashLogIndexes,
  bashLogFrames,
  foldBashProposals,
} from "./bash-intent-log.mjs";
import { applyMovingSql } from "./ready-projection-sql.mjs";

it.skipIf(!process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL)(
  "keeps each logged Bash separate across a mixed native commit, rollback, other lanes and replays",
  async () => {
    const url = process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL,
      schema = "intent_" + randomUUID().replaceAll("-", ""),
      pool = new pg.Pool({ connectionString: url, options: "-c search_path=" + schema, max: 3 });
    const [t, s, r, w, another] = Array.from({ length: 5 }, () => randomUUID()),
      topic = "scope-fixture";
    let off = 0;
    try {
      await pool.query("create schema " + schema);
      await installPagingProjection(pool);
      await installBashIntentLog(pool);
      await setBashLogIndexes(pool, "coverage");
      await pool.query("insert into accepted_fact_projection_offsets values($1,0,0)", [topic]);
      await pool.query(
        "insert into runs(id,tenant_id,session_id,lease_id,idempotency_key,mailbox_position,initial_input,model_ref,state) values($1,$2,$3,$4,$1,1,'test','fixture','running')",
        [r, t, s, w],
      );
      const root = { tenant: t, session: s, run: r, writer: w };
      const entry = (id, seq, ordinal, model, extra = {}) => ({
        kind: "message",
        count: 1,
        args: [
          t,
          s,
          r,
          seq,
          JSON.stringify([
            {
              id,
              conversation: 1,
              ordinal,
              role: model.role,
              content: JSON.stringify({ id, conversationId: 1, model: [model] }),
              ...extra,
            },
          ]),
          w,
        ],
      });
      const tool = (n, call, cmd) => ({
        kind: "tool",
        count: 1,
        args: [
          t,
          s,
          r,
          w,
          n,
          {
            taskId: n,
            conversationId: 1,
            assistantEntryId: 2,
            operationId: randomUUID(),
            offset: "1",
            startedAt: "2026-11-25T00:00:00.000Z",
            seq: 2,
            ordinal: n - 3,
            arguments: { command: cmd, timeout: 10 },
            reply: {
              toolName: "bash",
              toolCallId: call,
              deadlineAt: 1234,
              topic: "pi-cloud.tool-replies.v4." + another,
              partition: 0,
              offset: "9",
            },
          },
        ],
      });
      const apply = (client, ops, seq, through, offset = off, snapshot) =>
        client.query(applyMovingSql, [
          topic,
          0,
          offset,
          offset + 1,
          JSON.stringify(
            bashLogFrames(
              [{ offset: String(offset), ops, snapshot, rootDisplay: { ...root, seq, through } }],
              "folded",
            ),
          ),
          false,
          false,
        ]);
      const refs = async () =>
        (await pool.query("select bash_wait_refs($1,$2,$3) v", [t, s, r])).rows[0].v;
      const proposal = {
        role: "assistant",
        content: [
          { type: "text", text: "nul\0kept" },
          ...["a", "b"].map((id) => ({
            type: "toolCall",
            id,
            name: "bash",
            arguments: { command: id, timeout: "10" },
          })),
        ],
      };
      await apply(pool, [entry(2, 1, 0, proposal)], 1, 1);
      off++;
      const a = tool(3, "a", "a"),
        b = tool(4, "b", "changed b");
      foldBashProposals(
        {},
        {
          commit: {
            writes: [
              {
                type: "entry",
                value: { id: 2, conversationId: 1, kind: "pi.assistant", model: [proposal] },
              },
            ],
          },
        },
        { ops: [a, b] },
      );
      assert.equal(a.sameCommand, true);
      assert.equal(b.sameCommand, undefined);
      await apply(pool, [a, b], 2, 3);
      off++;
      const original = await refs();
      assert.deepEqual(Object.keys(original), ["3", "4"]);
      assert.equal(original[3].commandFromProposal, true);
      assert.deepEqual(original[3].arguments, { timeout: 10 });
      assert.equal(original[4].arguments.command, "changed b");
      assert.equal(
        (await pool.query("select count(*)::int n from messages where display_through=3")).rows[0]
          .n,
        1,
      );
      const bResult = entry(
        5,
        3,
        0,
        { role: "toolResult", toolCallId: "b", content: [] },
        { task: 4, assistant: 2 },
      );
      const settleB = { ...b, kind: "tool-settled" };
      const c = await pool.connect();
      try {
        await c.query("begin");
        await apply(c, [bResult, settleB], 3, 4);
        assert.deepEqual(await refs(), original);
        await c.query("rollback");
      } finally {
        c.release();
      }
      assert.deepEqual(await refs(), original);
      // An unrelated Conversation cannot pair its result with the root task.
      const wrong = structuredClone(bResult);
      const rows = JSON.parse(wrong.args[4]);
      rows[0].conversation = 9;
      wrong.args[4] = JSON.stringify(rows);
      await assert.rejects(apply(pool, [wrong, settleB], 3, 4), (e) => e.code === "P0002");
      assert.deepEqual(await refs(), original);
      assert.equal((await apply(pool, [bResult, settleB], 3, 4)).rows[0].outcome, "applied");
      off++;
      assert.equal(
        (await apply(pool, [bResult, settleB], 3, 4, off - 1)).rows[0].outcome,
        "covered",
      );
      assert.deepEqual(Object.keys(await refs()), ["3"]);
      const aResult = entry(
        6,
        4,
        0,
        { role: "toolResult", toolCallId: "a", content: [] },
        { task: 3, assistant: 2 },
      );
      const wrongId = structuredClone(a);
      wrongId.kind = "tool-settled";
      wrongId.args[5].operationId = randomUUID();
      await assert.rejects(apply(pool, [aResult, wrongId], 4, 5), (e) => e.code === "PZ002");
      const stale = structuredClone(aResult);
      stale.args[5] = another;
      assert.equal((await apply(pool, [stale], 4, 5)).rows[0].outcome, "obsolete");
      off++;
      assert.deepEqual(Object.keys(await refs()), ["3"]);
      await apply(pool, [aResult, { ...a, kind: "tool-settled" }], 4, 5);
      assert.deepEqual(await refs(), {});
      assert.equal(
        (await pool.query("select count(*)::int n from messages where role='pma.bash-intent'"))
          .rows[0].n,
        2,
      );
      off++;
      const checkpoint = { run: r, writer: w, state: { seq: 5, writer: w, floor: 2 } };
      const next = entry(7, 5, 0, { role: "assistant", content: [{ type: "text", text: "done" }] });
      const client = await pool.connect();
      try {
        await client.query("begin");
        assert.equal(
          (await apply(client, [next], 5, 6, off, checkpoint)).rows[0].outcome,
          "applied",
        );
        assert.equal(
          (await pool.query("select projection_state from runs where id=$1", [r])).rows[0]
            .projection_state,
          null,
        );
        assert.equal(
          (await pool.query("select from messages where run_id=$1 and entry_id=7", [r])).rowCount,
          0,
        );
        await client.query("rollback");
      } finally {
        client.release();
      }
      assert.equal((await apply(pool, [next], 5, 6, off, checkpoint)).rows[0].outcome, "applied");
      const persisted = (
        await pool.query(
          `select r.projection_first,r.projection_state,r.output_display_seq,m.display_through
        from runs r join messages m on m.run_id=r.id and m.entry_id=7 where r.id=$1`,
          [r],
        )
      ).rows[0];
      assert.deepEqual(persisted, {
        projection_first: String(off + 1),
        projection_state: checkpoint.state,
        output_display_seq: "0",
        display_through: "6",
      });
      // The folded checkpoint's UPDATE also fences a late writer. A covered
      // message and a newer checkpoint must succeed or fail together.
      off++;
      const late = entry(8, 6, 0, { role: "assistant", content: [] });
      late.args[5] = another;
      const lateFrame = {
        offset: String(off),
        ops: [late],
        rootDisplay: { ...root, writer: another, seq: 6, through: 7 },
        snapshot: { ...checkpoint, writer: another, state: { seq: 6, writer: another } },
      };
      const rejected = await pool.query(applyMovingSql, [
        topic,
        0,
        off,
        off + 1,
        JSON.stringify(bashLogFrames([lateFrame], "folded")),
        false,
        false,
      ]);
      assert.equal(rejected.rows[0].outcome, "obsolete");
      assert.equal((await pool.query("select from messages where entry_id=8")).rowCount, 0);
      assert.deepEqual(
        (await pool.query("select projection_state from runs where id=$1", [r])).rows[0]
          .projection_state,
        checkpoint.state,
      );
      // Other remote tools keep their original waiter and cancellation route.
      off++;
      const other = tool(12, "read-id", "unused");
      other.args[5].reply.toolName = "read";
      other.args[5].seq = 7;
      assert.equal((await apply(pool, [other], 7, 8)).rows[0].outcome, "applied");
      off++;
      assert.deepEqual(
        (await pool.query("select projection_tools from runs where id=$1", [r])).rows[0]
          .projection_tools["12"],
        other.args[5],
      );
      assert.deepEqual(await refs(), {});
      const cancel = {
        kind: "cancel",
        nativeSeq: 8,
        count: 1,
        args: [t, s, r, w, other.args[5].operationId, 12],
      };
      assert.equal((await apply(pool, [cancel], 8, 9)).rows[0].outcome, "applied");
      assert.equal(
        (
          await pool.query("select cancel_positions->$2 as target from runs where id=$1", [
            r,
            other.args[5].operationId,
          ])
        ).rows[0].target.taskId,
        12,
      );
    } finally {
      try {
        await pool.query("drop schema if exists " + schema + " cascade");
      } finally {
        await pool.end();
      }
    }
  },
);
