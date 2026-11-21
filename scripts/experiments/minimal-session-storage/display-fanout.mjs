// Rebind an observed native intent for the isolated many-destination sweep.
// Mixed-trace measurements separately retain every original Storage commit.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { createPiCloudEventFactory } from "@pi-cloud/protocol";
import { prepareToolPresentation } from "../../../packages/sandbox-supervisor/src/durable/tool-presentation.ts";
import { nativeBashTrace } from "./native-bash-trace.mjs";
import { foldLive } from "./moving-checkpoint.mjs";
import { targets } from "./fanout-fixture.mjs";

export async function displayFanoutTemplate() {
  let live = {},
    doc;
  for (const commit of await nativeBashTrace({ runId: randomUUID() })) {
    for (const w of commit.writes) if (w.record?.kind === "pi.live") doc = w.record;
    const task = commit.writes.find(
      (w) =>
        w.type === "task" &&
        w.value.state?.checkpoint?.phase === "execute" &&
        w.value.kind === "pi.tool",
    );
    if (task) {
      const slot = live[doc.id].value.tools.find((s) => s.taskId === task.value.id);
      const delta = commit.writes.find((w) => w.type === "document.change");
      assert(slot.status === "pending" && commit.writes.length === 2);
      assert.deepEqual(delta.content.ops, [["s", ["tools", 0, "status"], "running"]]);
      return { doc, task: task.value, slot, delta };
    }
    live = foldLive(live, commit.writes);
  }
  throw Error("Pinned Harness did not issue expected tool intent");
}

export function attachStart(fact, index, seed, rounds, template) {
  const { doc, task, slot, delta } = template,
    round = Math.floor(index / targets);
  const slots = Array.from({ length: rounds }, (_, r) => ({
    ...slot,
    taskId: 9 + r,
    callId: "call-" + (r * targets + (index % targets)),
    status: r < round ? "running" : "pending",
  }));
  const before = { tools: slots },
    writes = fact.commit.writes;
  if (seed) {
    writes.push({
      type: "document.create",
      record: doc,
      content: { version: 1, kind: "base", value: before },
    });
    fact.commit.nextId = 9 + rounds;
    return;
  }
  const current = slots[round];
  writes[0].value = {
    ...task,
    id: current.taskId,
    input: { ...task.input, callId: current.callId },
  };
  writes.push({
    ...delta,
    content: { ...delta.content, ops: [["s", ["tools", round, "status"], "running"]] },
  });
  fact.commit.nextId = 9 + rounds;
  const after = { tools: slots.map((s) => (s === current ? { ...s, status: "running" } : s)) };
  const factory = createPiCloudEventFactory(
    { sessionId: fact.scope.sessionId, turnId: fact.scope.turnId, agentId: "root" },
    { initialSequence: round },
  );
  const events = prepareToolPresentation(
    fact.commit,
    [{ record: doc, before: { value: before }, after: { value: after } }],
    factory,
    { conversationId: 1, maximumOutputBytes: 51200 },
  );
  assert(events.length === 1 && events[0].event.type === "tool.started");
  fact.display = [{ scope: fact.scope, events }];
}
