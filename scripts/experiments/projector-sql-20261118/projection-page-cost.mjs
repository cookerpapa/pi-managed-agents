import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";
import { isDeepStrictEqual } from "node:util";
import { sql } from "kysely";
import { ModelExecutionAccounting } from "@pi-cloud/accounting";
import { ExecutionStreamProjector } from "../../../packages/runtime-core/src/execution-stream-projection.ts";
import { readCanonicalHistoryWindow } from "../../../packages/runtime-core/src/canonical-history-window.ts";
import { decodeHistoryPosition } from "../../../packages/runtime-core/src/history-position.ts";
import { distribution } from "../../lib/runtime-load-scheduler.mjs";

/** Compare the two actual page readers for the same committed history. This
 * changes the reader argument only. It does not bypass the terminal transaction. */
export async function comparePages(db, cases) {
  const results = [];
  for (const [i, extra] of [0, 2048].entries()) {
    const { fact, end } = cases[i],
      s = fact.scope,
      source = fact.commit.writes.find((w) => w.type === "entry").value;
    const topic = "projection-paging",
      partition = i;
    let offset = 100n,
      seq = fact.commit.seq,
      nextId = fact.commit.nextId;
    const projector = new ExecutionStreamProjector(db, new ModelExecutionAccounting());
    const project = (f) => projector.project({ topic, partition, offset: ++offset, fact: f });
    assert((await project(fact)).admitted);
    for (let n = 0; n < extra; n += 64) {
      const writes = Array.from({ length: Math.min(64, extra - n) }, () => {
        const value = structuredClone(source);
        value.id = nextId++;
        delete value.byTaskId;
        const block = value.model[0].content.find((c) => c.type === "text");
        assert.equal(block.text.length, 4096);
        block.text = randomUUID() + block.text.slice(36);
        return { type: "entry", value };
      });
      const origins = writes.map((w) => ({
        recordId: w.value.id,
        sessionId: s.sessionId,
        runId: s.runId,
        turnId: s.turnId,
      }));
      const result = await project({
        ...fact,
        factId: randomUUID(),
        origins,
        modelEvidence: [],
        commit: { seq: ++seq, nextId, writes },
        display: [{ scope: s, events: [], coveredThrough: seq + 2 }],
      });
      assert(result.admitted);
    }
    const finish = await project({ ...end, nativeSequence: seq, baseSequence: seq + 2 });
    assert.equal(finish.terminal.type, "turn.completed");
    await sql`analyze`.execute(db);
    const turns = {
      turnId: s.turnId,
      piSessionId: s.piSessionId,
      originSessionId: s.sessionId,
      operationKind: "chat",
    };
    for (const direction of ["before", "after"]) {
      const times = { live: [], frozen: [] };
      let expected;
      for (let round = 0; round < 18; round++) {
        for (const frozen of round % 2 ? [true, false] : [false, true]) {
          const start = performance.now();
          const page = await readCanonicalHistoryWindow(db, {
            tenantId: s.tenantId,
            sessionId: s.sessionId,
            turns: [{ ...turns, frozen }],
            direction,
          });
          const elapsed = performance.now() - start;
          const view = {
            more: page.more,
            positions: page.positions,
            items: page.transcripts.get(s.turnId)?.items.map(({ historyCursor, ...item }) => ({
              ...item,
              ...(historyCursor
                ? { historyCursor: decodeHistoryPosition(historyCursor, s.sessionId) }
                : {}),
            })),
          };
          assert(view.items?.length);
          if (expected)
            assert(
              isDeepStrictEqual(view, expected),
              "Displayed items or decoded page positions disagree",
            );
          else expected = view;
          if (round >= 3) times[frozen ? "frozen" : "live"].push(elapsed);
        }
      }
      results.push({
        extraEntries: extra,
        direction,
        items: expected.items.length,
        more: expected.more,
        resultsIdentical: true,
        liveMs: distribution(times.live),
        frozenMs: distribution(times.frozen),
      });
    }
  }
  return results;
}
