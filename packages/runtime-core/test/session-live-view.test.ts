import { expect, it, vi } from "vitest";
import type { PiCloudEvent } from "@pi-cloud/protocol";
import { SessionLiveView } from "../src/session-live-view.ts";

it("keeps an in-flight snapshot when committed content is evicted", () => {
  const view = new SessionLiveView(async () => {});
  const base = {
    schemaVersion: 1 as const,
    sessionId: crypto.randomUUID(),
    turnId: crypto.randomUUID(),
    agentId: "root",
    occurredAt: new Date().toISOString(),
  };
  const event: PiCloudEvent = {
    ...base,
    eventId: crypto.randomUUID(),
    seq: 1,
    type: "assistant.text.delta",
    payload: { text: "already visible" },
  };
  try {
    view.accept("tenant", event);
    view.accept("tenant", event);
    const reader = view.snapshot("tenant", base.sessionId);
    view.accept("tenant", {
      ...base,
      eventId: crypto.randomUUID(),
      seq: 2,
      type: "turn.completed",
      payload: { stopReason: "stop" },
    });
    expect(reader.events).toEqual([event]);
    expect(view.snapshot("tenant", base.sessionId).events).toEqual([]);
    expect(view.statistics().cachedBytes).toBe(0);
  } finally {
    view.close();
  }
});

it("bounds empty holder metadata without evicting an unfinished answer", () => {
  vi.useFakeTimers();
  const view = new SessionLiveView(async () => {}, { maximumEntries: 2, maximumBytes: 10000 });
  const event = (sessionId: string): PiCloudEvent => ({
    schemaVersion: 1,
    eventId: crypto.randomUUID(),
    sessionId,
    turnId: crypto.randomUUID(),
    agentId: "root",
    seq: 1,
    occurredAt: new Date().toISOString(),
    type: "assistant.text.delta",
    payload: { text: "unfinished" },
  });
  const active = crypto.randomUUID();
  try {
    view.accept("tenant", event(active));
    for (let i = 0; i < 20; i++) {
      const id = crypto.randomUUID();
      view.accept("tenant", event(id));
      view.cover("tenant", id, 1);
      expect(view.statistics().emptyHolders).toBeLessThanOrEqual(2);
      expect(view.statistics().activeTextTails).toBe(1);
    }
    expect(view.statistics().activeSessionTails).toBe(3);
    expect(view.snapshot("tenant", active).events).toHaveLength(1);
    vi.advanceTimersByTime(360001);
    expect(view.statistics().emptyHolders).toBe(0);
    expect(view.snapshot("tenant", active).events).toHaveLength(1);
    view.cover("tenant", active, 1);
    expect(view.statistics().cachedBytes).toBe(0);
    vi.advanceTimersByTime(360001);
    expect(view.statistics().activeSessionTails).toBe(0);
  } finally {
    view.close();
    vi.useRealTimers();
  }
});
