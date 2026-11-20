import { expect, it } from "vitest";
import {
  runLoad,
  summarizeLoad,
  distribution,
  summarizeSingleRequestPhases,
} from "./runtime-load-scheduler.mjs";

it("summarizes long measurement windows without passing every observation as a function argument", () => {
  const samples = Array.from({ length: 200000 }, (_, i) => 200000 - i);
  expect(distribution([...samples, NaN])).toEqual({
    count: 200000,
    p50: 100000,
    p95: 190000,
    p99: 198000,
    max: 200000,
  });
});

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

it("keeps a history failure separate from an already completed Run", async () => {
  const load = await runLoad({
    concurrency: 1,
    count: 1,
    work: async () => ({ runId: "one", state: "completed", success: true, historyError: "57014" }),
  });
  expect(summarizeLoad(load)).toMatchObject({
    accepted: 1,
    completed: 1,
    failed: 0,
    historyFailures: 1,
    historyErrors: [{ index: 0, code: "57014" }],
  });
});

it("pairs one-request timings and excludes retries, missing clocks and clock inversions", () => {
  const results = ["a", "retry", "missing", "clock"].map((planId) => ({
    success: true,
    planId,
    submittedAt: 100,
    settledAt: 200,
  }));
  const request = (planId, startedAt = 120) => ({
    planId,
    startedAt,
    finishedAt: 150,
    outcome: "completed",
  });
  const summary = summarizeSingleRequestPhases(results, [
    request("a"),
    request("retry"),
    request("retry"),
    request("clock", 50),
  ]);
  expect(summary).toMatchObject({
    samples: 1,
    beforeProviderMs: { p50: 20 },
    providerMs: { p50: 30 },
    afterProviderMs: { p50: 50 },
  });
});
