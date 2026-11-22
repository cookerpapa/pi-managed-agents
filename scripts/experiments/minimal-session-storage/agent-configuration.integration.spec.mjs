// Exercise the row boundary with a real PG transaction and the candidate writer.
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import pg from "pg";
import { expect, it } from "vitest";
import { agentLane, agentVersion } from "./agent-configuration.mjs";
import { projectionTemplates } from "./projection-statements.mjs";
import { installPrefixSql } from "./ready-projection-sql.mjs";

it("keeps configuration identity, publication order and chat Entries independent", async () => {
  const url = process.env.PI_CLOUD_POSTGRES_INTEGRATION_URL,
    name = "agent_" + randomUUID().replaceAll("-", "");
  const c = new pg.Client({ connectionString: url });
  await c.connect();
  try {
    await c.query("begin");
    await c.query("create schema " + name);
    await c.query("set local search_path=" + name);
    await c.query(await readFile(new URL("./schema.sql", import.meta.url), "utf8"));
    await installPrefixSql(c, projectionTemplates);
    const t = randomUUID(),
      s = randomUUID(),
      r = randomUUID(),
      w = randomUUID(),
      scope = { tenantId: t, piSessionId: s };
    await c.query(
      "insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,lease_id) values($1,$2,$3,1,$1,'test','test','running',$4)",
      [r, t, s, w],
    );
    const insert = async (seq, rows, writer = w) =>
      (
        await c.query("select * from prefix_message($1,$2,$3,$4,$5,$6)", [
          t,
          s,
          r,
          seq,
          JSON.stringify(rows),
          writer,
        ])
      ).rows[0];
    const state = {},
      weird = "x\u0000\uD800";
    const create = (id, lane, value) => ({
      type: "document.create",
      record: {
        id,
        kind: "pi.agent",
        scope: { kind: "conversation", conversationId: lane },
        history: "rewindable",
        fork: "asOf",
      },
      content: { kind: "base", version: 1, value },
    });
    const change = (id, value) => ({
      type: "document.change",
      id,
      content: { kind: "base", version: 1, value },
    });
    const prepare = (seq, ordinal, write) =>
      agentVersion(c, state, scope, seq, write, agentLane(state.agentRoutes, write), ordinal);
    const select = async (lane, seq, tenant = t) => {
      const { rows } = await c.query("select content from agent_config_at($1,$2,$3,$4)", [
        tenant,
        s,
        lane,
        seq,
      ]);
      return rows.length ? (JSON.parse(rows[0].content)?.value ?? null) : undefined;
    };
    await insert(1, [
      await prepare(1, 0, create(4, 1, { tools: ["one"] })),
      await prepare(1, 1, create(9, 2, { tools: ["child"] })),
    ]);
    const answer = { id: 12, role: "assistant", content: "answer", conversation: 1, ordinal: 0 };
    const sameCommit = [
      answer,
      await prepare(2, 1, change(4, { tools: ["two"], instructions: weird })),
    ];
    expect((await insert(2, sameCommit)).writes).toBe(2);
    expect((await insert(2, sameCommit)).writes).toBe(2);
    expect((await insert(2, [{ ...sameCommit[1], content: "different" }])).writes).toBe(0);
    expect(await select(1, 1)).toEqual({ tools: ["one"] });
    expect(await select(1, 2)).toEqual({ tools: ["two"], instructions: weird });
    expect(await select(2, 2)).toEqual({ tools: ["child"] });
    expect(await select(1, 2, randomUUID())).toBeUndefined();
    delete state.agentValues; // Simulate a cold Projector with only its document routes.
    const delta = {
      type: "document.change",
      id: 4,
      content: { kind: "delta", version: 1, ops: [["s", ["tools"], ["three"]]] },
    };
    const edited = await prepare(3, 0, delta);
    expect((await insert(3, [edited], randomUUID())).accepted).toBe(false);
    expect(await select(1, 3)).toEqual({ tools: ["two"], instructions: weird });
    await insert(3, [edited]);
    expect(await select(1, 3)).toEqual({ tools: ["three"], instructions: weird });
    await insert(4, [await prepare(4, 0, { type: "document.retire", id: 4 })]);
    expect(await select(1, 4)).toBeNull();
    await insert(5, [await prepare(5, 0, create(50, 1, { tools: [] }))]);
    expect(await select(1, 5)).toEqual({ tools: [] });
    expect(await select(1, 3)).toEqual({ tools: ["three"], instructions: weird });
    expect((await c.query("select entry_id from message_entries")).rows).toEqual([
      { entry_id: "12" },
    ]);
    expect((await c.query("select count(*)::int n from messages")).rows[0].n).toBe(7);
  } finally {
    try {
      await c.query("rollback");
    } finally {
      await c.end();
    }
  }
});
