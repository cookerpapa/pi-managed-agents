import { expect, it } from "vitest";
import { runLoad, summarizeLoad, distribution } from "./runtime-load-scheduler.mjs";

it("keeps arrival accounting independent of slow work and distinguishes dropped inputs", async () => {
  let now = 0;
  const waits = [];
  const load = await runLoad({
    mode: "arrival",
    concurrency: 2,
    count: 6,
    rate: 10,
    now: () => now,
    sleep: async (ms) => {
      now += ms;
      if (now >= 500) for (const done of waits.splice(0)) done();
    },
    work: async (i) => {
      if (now < 500) await new Promise((resolve) => waits.push(resolve));
      return { success: true, runId: String(i), settlementMs: 500 };
    },
  });
  expect(load.requested).toBe(6);
  expect(load.dropped.length).toBeGreaterThan(0);
  expect(load.maximumInFlight).toBe(2);
  expect(load.launched + load.dropped.length).toBe(load.requested);
});
it("does not count an accepted failed Run as successful throughput", async () => {
  const load = await runLoad({
    concurrency: 2,
    count: 4,
    work: async (i) => ({
      runId: String(i),
      success: i % 2 === 0,
      acknowledgementMs: 1,
      settlementMs: 20,
      state: i % 2 === 0 ? "completed" : "failed",
    }),
  });
  const summary = summarizeLoad(load);
  expect(summary.accepted).toBe(4);
  expect(summary.completed).toBe(2);
  expect(summary.failed).toBe(2);
  expect(summary.settlementMs).toMatchObject({ p50: 20, p95: 20 });
});
it("reports absent latency as absent rather than zero", () => {
  expect(distribution([])).toEqual({ count: 0, p50: null, p95: null, p99: null, max: null });
});
