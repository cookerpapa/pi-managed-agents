// Hold an admitted batch across a partition handoff using two independent PG clients.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { Kysely, PostgresDialect } from "kysely";
import { ExecutionStreamBoundary } from "../../../packages/runtime-core/src/execution-stream-boundary.ts";
import { applyMovingSql } from "./ready-projection-sql.mjs";

export async function checkWriterGuard(db, install) {
  const c = await db.connect(),
    other = await db.connect();
  const read = new Kysely({ dialect: new PostgresDialect({ pool: db }) });
  await db.query(
    "alter table runs add column output_seal_offset bigint;create table session_leases as select * from public.session_leases with no data",
  );
  try {
    for (const mode of ["baseline", "cursor", "combined"]) {
      await install(mode);
      const tenant = randomUUID(),
        session = randomUUID(),
        run = randomUUID(),
        old = randomUUID(),
        next = randomUUID();
      const scope = {
        tenantId: tenant,
        piSessionId: session,
        sessionId: session,
        runId: run,
        writerId: old,
        fencingToken: 1,
      };
      const record = { topic: "guard", partition: 0, offset: 12n, fact: { scope } };
      await db.query(
        "insert into session_leases(tenant_id,pi_session_id,lease_id,fencing_token) values($1,$2,$3,1),($1,$2,$4,2)",
        [tenant, session, old, next],
      );
      const reset = async () => {
        await c.query("update session_leases set writer_seal_offset=null where lease_id=$1", [old]);
        await c.query("delete from messages where run_id=$1", [run]);
        await c.query(
          "insert into accepted_fact_projection_offsets values('guard',0,10) on conflict(topic,partition) do update set next_offset=10",
        );
        await c.query(
          "insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,lease_id) values($1,$2,$3,1,$1,'fixture','fixture','running',$4) on conflict(id) do update set lease_id=excluded.lease_id,state='running'",
          [run, tenant, session, old],
        );
      };
      const write = (client, from, to) =>
        client.query(applyMovingSql, [
          "guard",
          0,
          from,
          to,
          JSON.stringify([
            {
              offset: String(to - 1),
              ops: [
                {
                  kind: "message",
                  count: 1,
                  args: [
                    tenant,
                    session,
                    run,
                    8,
                    JSON.stringify([
                      {
                        id: 20,
                        conversation: 1,
                        role: "assistant",
                        content: "previous writer",
                        ordinal: 0,
                      },
                    ]),
                    old,
                  ],
                },
              ],
            },
          ]),
          false,
          false,
        ]);
      const seal = async (client) => {
        await client.query("update session_leases set writer_seal_offset=11 where lease_id=$1", [
          old,
        ]);
        await client.query("update runs set lease_id=$2 where id=$1", [run, next]);
        await client.query(
          "update accepted_fact_projection_offsets set next_offset=12 where topic='guard' and partition=0",
        );
      };
      await reset();
      await other.query("begin");
      await seal(other);
      // The previous Projector arrives while the successor's seal is in flight.
      const late = write(c, 10, 11);
      await other.query("commit");
      assert.equal((await late).rows[0].outcome, "covered");
      await assert.rejects(write(c, 10, 13), (e) => e.code === "40001");
      assert.equal(
        (
          await c.query(
            "select id from runs where id=$1 and not exists(select from messages where run_id=$1)",
            [run],
          )
        ).rowCount,
        1,
      );
      await reset();
      const currentView = new ExecutionStreamBoundary(read),
        retiredView = new ExecutionStreamBoundary(read);
      assert(await currentView.isOpen(record, true));
      assert(await retiredView.isOpen(record, true));
      // The valid prefix takes the partition lock first: handoff must see it.
      await c.query("begin");
      assert.equal((await write(c, 10, 11)).rows[0].outcome, "applied");
      const sealing = (async () => {
        await other.query("begin");
        await seal(other);
        await other.query("commit");
      })();
      await c.query("commit");
      await sealing;
      assert.equal((await c.query("select from messages where run_id=$1", [run])).rowCount, 1);
      // An old Worker can publish a late record at the *next* offset. Simulate
      // a stale-positive in-memory admission at the SQL boundary, not a second seal.
      currentView.closeWriter(old, 11n);
      assert.equal(await currentView.isOpen(record, true), false);
      assert.equal(await new ExecutionStreamBoundary(read).isOpen(record, true), false);
      assert.equal(await retiredView.isOpen(record, true), true);
      const fresh = JSON.stringify([
        {
          offset: "12",
          ops: [
            {
              kind: "message",
              count: 1,
              args: [
                tenant,
                session,
                run,
                9,
                JSON.stringify([
                  { id: 21, conversation: 1, role: "assistant", content: "late", ordinal: 0 },
                ]),
                old,
              ],
            },
          ],
        },
      ]);
      const result = (await c.query(applyMovingSql, ["guard", 0, 12, 13, fresh, false, false]))
        .rows[0].outcome;
      assert.equal(result, mode === "cursor" ? "applied" : "obsolete");
      if (mode === "combined") {
        const frames = JSON.parse(fresh);
        frames[0].rootDisplay = { tenant, session, run, writer: old, seq: 9, through: 2 };
        frames[0].offset = "13";
        assert.equal(
          (
            await c.query(applyMovingSql, [
              "guard",
              0,
              13,
              14,
              JSON.stringify(frames),
              false,
              false,
            ])
          ).rows[0].outcome,
          "obsolete",
        );
        assert.equal(
          (await c.query("select from messages where run_id=$1 and entry_id=21", [run])).rowCount,
          0,
        );
        frames[0].rootDisplay.writer = next;
        frames[0].ops[0].args[5] = next;
        frames[0].offset = "14";
        assert.equal(
          (
            await c.query(applyMovingSql, [
              "guard",
              0,
              14,
              15,
              JSON.stringify(frames),
              false,
              false,
            ])
          ).rows[0].outcome,
          "applied",
        );
        assert.equal(
          (await c.query("select output_display_seq from runs where id=$1", [run])).rows[0]
            .output_display_seq,
          "2",
        );
      }
      console.log(
        JSON.stringify({
          kind: "writer-fault",
          mode,
          delayedOldTransaction: "covered",
          straddled: "rejected",
          preSealRows: 1,
          lateRecordAtCurrentOffset: result,
        }),
      );
    }
  } finally {
    await Promise.allSettled([c.query("rollback"), other.query("rollback")]);
    c.release();
    other.release();
    await install("baseline");
  }
}
