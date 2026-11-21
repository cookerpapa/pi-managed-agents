// Use the serving Runner presenters over the memory Storage's staged live changes.
import { createPiCloudEventFactory } from "@pi-cloud/protocol";
import { DurableTextPresentation } from "../../../packages/sandbox-supervisor/src/durable/text-presentation.ts";
import { prepareToolPresentation } from "../../../packages/sandbox-supervisor/src/durable/tool-presentation.ts";
import { foldLive } from "./moving-checkpoint.mjs";

export class FamilyPresentation {
  views = new Map();
  live = {};
  sequences = new Map();
  restore(f) {
    this.live = foldLive(this.live, f.commit.writes);
    for (const d of f.display)
      this.sequences.set(
        d.scope.sessionId,
        Math.max(
          this.sequences.get(d.scope.sessionId) ?? 0,
          d.coveredThrough ?? 0,
          ...d.events.map((e) => e.event.seq),
        ),
      );
  }
  bind(id, scope) {
    if (!this.views.has(id))
      this.views.set(id, { scope, text: new DurableTextPresentation({ conversationId: id }) });
  }
  prepare(commit) {
    const next = foldLive(this.live, commit.writes);
    const documents = Object.entries(next).flatMap(([id, after]) => {
      const before = this.live[id];
      return before === after
        ? []
        : [
            {
              record: {
                id: Number(id),
                kind: "pi.live",
                scope: { kind: "conversation", conversationId: after.conversation },
              },
              before,
              after,
            },
          ];
    });
    const display = [],
      confirm = [];
    for (const [id, { scope, text }] of this.views) {
      const factory = createPiCloudEventFactory(
        { sessionId: scope.sessionId, turnId: scope.turnId, agentId: "root" },
        { initialSequence: this.sequences.get(scope.sessionId) ?? 0 },
      );
      const toolEvents = prepareToolPresentation(commit, documents, factory, {
        conversationId: id,
        maximumOutputBytes: 51200,
      });
      const prepared = text.prepare(commit, documents, factory);
      const events = [...toolEvents, ...prepared.events];
      if (events.length || prepared.coveredThrough !== undefined)
        display.push({
          scope,
          events,
          ...(prepared.coveredThrough === undefined
            ? {}
            : { coveredThrough: prepared.coveredThrough }),
        });
      confirm.push(() => {
        prepared.confirmed();
        this.sequences.set(scope.sessionId, factory.currentSequence());
      });
    }
    return {
      display,
      confirmed: () => {
        this.live = next;
        for (const apply of confirm) apply();
      },
    };
  }
}
