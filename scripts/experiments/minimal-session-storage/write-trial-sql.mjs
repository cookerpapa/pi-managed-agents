// Counterfactual functions are installed only on a disposable study database.
// Save and restore exact definitions between arms; the ordinary installer has no switch.
import assert from "node:assert/strict";
import { insertMessages } from "./projection-statements.mjs";

export async function writeTrialFunctions(db) {
  const { rows } = await db.query(`select proname,pg_get_functiondef(p.oid) as definition
    from pg_proc p where pronamespace=current_schema()::regnamespace
    and proname in ('prefix_message','project_ready')`);
  assert.equal(rows.length, 2);
  const original = Object.fromEntries(rows.map((r) => [r.proname, r.definition]));
  const cursorMessage = `create or replace function prefix_message(uuid,uuid,uuid,bigint,text,uuid)
    returns table(accepted boolean,writes int) language sql volatile as $$
    with changed as (${insertMessages}) select true,count(*)::int from changed $$`;
  // Keep Tool admission, cancellation state, row locking, async/sync policy,
  // ordering and compute-use confirmation identical. Only the waiter is omitted.
  const start = "select projection_tools->(a->>4),state into snap,owner_state";
  const combined = original.project_ready;
  const flag = "combine_wait_display:=d is not null";
  assert(
    combined.includes(flag) &&
      combined.includes("if d is not null and not display_written and not combine_wait_display"),
  );
  const fused = combined.replace(flag, "combine_wait_display:=false");
  const baseline = fused.replace(
    "if d is not null and not display_written and not combine_wait_display",
    "if false and d is not null and not display_written and not combine_wait_display",
  );
  const first = baseline.indexOf("if combine_wait_display and not display_written"),
    last = baseline.indexOf("n:=1;", first);
  assert(first > 0 && last > first && baseline.includes(start));
  const memory = (
    baseline.slice(0, first) +
    `if op->>'kind'='tool-settled' and a->6 is null then
       update runs set cancel_positions=cancel_positions-(a#>>'{5,operationId}')
       where id=(a->>2)::uuid and cancel_positions ? (a#>>'{5,operationId}');
     end if;
` +
    baseline.slice(last)
  ).replace(
    start,
    "select case when op->>'kind'='tool-settled' then a->5 end,state into snap,owner_state",
  );
  return async (mode) => {
    assert(["baseline", "cursor", "memory", "fused", "combined"].includes(mode));
    await db.query(mode === "cursor" ? cursorMessage : original.prefix_message);
    await db.query(
      mode === "memory"
        ? memory
        : mode === "combined"
          ? combined
          : mode === "fused"
            ? fused
            : baseline,
    );
  };
}
