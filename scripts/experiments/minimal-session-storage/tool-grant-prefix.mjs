import assert from "node:assert/strict";
import { operationsFor } from "./workspace-operations.mjs";

// Other native writes may carry a receipt, abort, policy change or order barrier.
export function plainExecution(fact) {
  if (
    fact.kind !== "durable_commit" ||
    fact.tools.length ||
    fact.cancellations.length ||
    fact.display.length ||
    fact.modelEvidence?.length ||
    fact.inputQueueSessions?.length
  )
    return;
  const ops = operationsFor(fact),
    writes = fact.commit.writes;
  if (
    ops.length !== 1 ||
    ops[0].action !== "execute" ||
    writes.length !== 1 ||
    writes[0].type !== "task" ||
    writes[0].value.id !== ops[0].taskId ||
    writes[0].value.abortRequested
  )
    return;
  return ops[0];
}

// Project in order with one PG transaction per record. Keep admission and transport capacity
// for the whole prefix before its single Kafka grant. Never wait for a second slot while
// already holding one: competing partitions must always be able to finish their prefixes.
export async function prepareToolGrants(records, current, grant, options) {
  const { limit, sender, router, targetFor, prepareSend } = options;
  const held = [],
    plans = [];
  try {
    for (const r of records.slice(0, limit)) {
      if (!current() || !plainExecution(r.fact)) break;
      const previous = plans.at(-1)?.record;
      if (
        previous &&
        (r.partition !== previous.partition ||
          r.topic !== previous.topic ||
          r.offset !== previous.offset + 1n)
      )
        break;
      const target = targetFor(r);
      if (plans.length && !sender.canReserve(target)) break;
      const slot = plans.length ? sender.take(target) : await sender.reserve(target, current);
      if (!slot) break;
      held.push(slot);
      if (!current()) break;
      const plan = await router.projectOperation(r, current, options.markProjection);
      if (!plan || !current()) break;
      assert(plan.commands.length === 1 && !plan.cancels.length);
      plans.push({ record: r, send: prepareSend(r, plan.commands[0], slot) });
    }
    if (!plans.length || !current()) return 0;
    const start = performance.now();
    const first = await grant(plans.length);
    assert(Number.isInteger(first) && first >= 0 && first <= plans.length);
    options.observeGrant?.(plans.length, performance.now() - start);
    for (let i = first; i < plans.length && current(); i++) {
      await plans[i].send();
    }
    return plans.length;
  } finally {
    for (const slot of held) if (!slot.op) sender.release(slot);
  }
}
