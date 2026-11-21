import assert from "node:assert/strict";
import { randomUUID, createHash } from "node:crypto";
import { nativeBashTrace, nativeBashFact } from "./native-bash-trace.mjs";
import { FamilyPresentation } from "./family-presentation.mjs";
import { plainExecution } from "./tool-grant-prefix.mjs";
import { decodeWorkspaceFact, encodeWorkspaceFact } from "./workspace-operations.mjs";

// Capture all native writes in their observed order across concurrent Harnesses.
// Reuse the identical tape for both width trials; scheduling is outside measurement.
export async function mixedDisplayTape(fixture, arm, publish, callsPerRun = 1) {
  const frames = [],
    hash = createHash("sha256");
  let index = 0,
    intents = 0;
  const entries = {},
    kinds = {},
    previous = new Map(),
    adjacentSizes = [];
  function append(owner, f, intent = false, before) {
    const { partition, scope } = owner;
    const value = encodeWorkspaceFact(f);
    if (intent)
      assert(
        plainExecution(decodeWorkspaceFact(value).fact, before, true),
        "Native start excluded by projection classifier",
      );
    hash.update(value);
    frames.push({
      partition,
      id: f.factId,
      session: scope.piSessionId,
      value,
    });
    const streak = previous.get(partition) ?? 0;
    if (!intent && streak) adjacentSizes.push(streak);
    previous.set(partition, intent ? streak + 1 : 0);
    return publish?.(frames.at(-1));
  }
  async function captureNext() {
    while (index < fixture.owners.length) {
      const i = index++,
        owner = fixture.owners[i],
        { scope } = owner,
        presentation = new FamilyPresentation();
      presentation.bind(1, scope);
      let starts = 0;
      const trace = await nativeBashTrace(
        scope,
        (commit) => {
          const d = presentation.prepare(commit);
          for (const w of commit.writes)
            if (w.type === "entry") entries[w.value.kind] = (entries[w.value.kind] ?? 0) + 1;
          const indexOfCall = starts * fixture.owners.length + i;
          const f = nativeBashFact(
            scope,
            commit,
            d.display,
            fixture.op(arm, indexOfCall),
            fixture.replyTopic,
          );
          const [operation] = f.workspaceOperations;
          if (operation) {
            assert(++starts <= callsPerRun && operation.reply.toolCallId === "call-" + indexOfCall);
            intents++;
          }
          for (const group of d.display)
            for (const { event } of group.events) kinds[event.type] = (kinds[event.type] ?? 0) + 1;
          const sent = append(owner, f, !!operation, presentation.live);
          if (sent) return sent.then(d.confirmed);
          d.confirmed();
        },
        {
          waitForCommit: !!publish,
          onlyLatest: true,
          callsPerRun,
          callIdAt: (n) => "call-" + (n * fixture.owners.length + i),
          callId: "call-" + i,
          modelDelay: 10 + ((i * 109) % 75),
          toolDelay: 60 + ((i * 79) % 80),
        },
      );
      assert.equal(starts, callsPerRun);
      await append(owner, {
        kind: "work_finished",
        scope,
        agentId: "root",
        factId: randomUUID(),
        occurredAt: new Date().toISOString(),
        baseSequence: 0,
        nativeSequence: trace.at(-1).seq,
        terminal: { type: "turn.completed", payload: { stopReason: "stop" } },
      });
    }
  }
  await Promise.all(Array.from({ length: 128 }, captureNext));
  for (const n of previous.values()) if (n) adjacentSizes.push(n);
  assert.equal(intents, fixture.owners.length * callsPerRun);
  return {
    frames,
    summary: {
      intents,
      kinds,
      entries,
      runs: fixture.owners.length,
      records: frames.length,
      hash: hash.digest("hex"),
      adjacent: adjacentSizes.reduce((s, n) => s + (n > 1 ? n : 0), 0),
      longest: Math.max(...adjacentSizes),
    },
  };
}
