import { performance } from "node:perf_hooks";
import { setTimeout as delay } from "node:timers/promises";

export function percentile(values, q) {
  const sorted = values.filter(Number.isFinite).sort((a, b) => a - b);
  return sorted.length ? sorted[Math.max(0, Math.ceil(q * sorted.length) - 1)] : null;
}
export function distribution(values) {
  const finite = values.filter(Number.isFinite);
  return {
    count: finite.length,
    p50: percentile(finite, 0.5),
    p95: percentile(finite, 0.95),
    p99: percentile(finite, 0.99),
    max: finite.length ? Math.max(...finite) : null,
  };
}
/** Test-driver arrivals only. No pending work is persisted or retried here. */
export async function runLoad({
  mode = "closed",
  concurrency,
  count,
  rate,
  durationMs,
  signal,
  work,
  now = () => performance.now(),
  sleep = (ms) => delay(ms),
}) {
  if (
    !Number.isInteger(concurrency) ||
    concurrency < 1 ||
    !Number.isInteger(count) ||
    count < 1 ||
    !["closed", "arrival"].includes(mode) ||
    (mode === "arrival" && !(rate > 0))
  )
    throw new Error("Invalid load profile");
  const started = now(),
    results = [],
    dropped = [],
    active = new Set();
  let next = 0,
    maximumInFlight = 0;
  const expired = () =>
    signal?.aborted || (durationMs !== undefined && now() - started >= durationMs);
  const launch = (index, scheduledAt) => {
    const launchedAt = now(),
      sample = {
        index,
        scheduledAt,
        launchedAt,
        generatorDelayMs: Math.max(0, launchedAt - scheduledAt),
      };
    const operation = Promise.resolve()
      .then(() => work(index, sample))
      .then(
        (value) => results.push({ ...sample, ...value }),
        (error) =>
          results.push({
            ...sample,
            success: false,
            error: String(error?.message ?? error).slice(0, 300),
          }),
      )
      .finally(() => active.delete(operation));
    active.add(operation);
    maximumInFlight = Math.max(maximumInFlight, active.size);
    return operation;
  };
  if (mode === "closed")
    await Promise.all(
      Array.from({ length: concurrency }, async () => {
        while (next < count && !expired()) {
          const index = next++;
          await launch(index, now());
        }
      }),
    );
  else {
    for (; next < count && !expired(); next++) {
      const scheduled = started + (next * 1000) / rate,
        remaining = scheduled - now();
      if (remaining > 0) await sleep(remaining);
      if (expired()) break;
      const lag = now() - scheduled;
      if (active.size >= concurrency)
        dropped.push({ index: next, reason: "inflight_budget", generatorDelayMs: lag });
      else if (lag > Math.max(100, 2000 / rate))
        dropped.push({ index: next, reason: "generator_late", generatorDelayMs: lag });
      else void launch(next, scheduled);
    }
    await Promise.all(active);
  }
  return {
    mode,
    concurrency,
    configuredCount: count,
    requested: next,
    launched: results.length,
    dropped,
    maximumInFlight,
    elapsedMs: now() - started,
    results: results.sort((a, b) => a.index - b.index),
    interrupted: !!signal?.aborted,
  };
}

export function summarizeLoad(load) {
  const values = load.results,
    ok = values.filter((v) => v.success),
    metric = (key) => distribution(values.map((v) => v[key]));
  return {
    mode: load.mode,
    concurrency: load.concurrency,
    offered: load.requested,
    accepted: values.filter((v) => v.runId).length,
    completed: ok.length,
    failed: values.length - ok.length,
    historyFailures: values.filter((v) => v.historyError).length,
    historyErrors: values
      .filter((v) => v.historyError)
      .map((v) => ({ index: v.index, code: v.historyError }))
      .slice(0, 20),
    dropped: load.dropped.length,
    elapsedMs: load.elapsedMs,
    successfulRunsPerSecond: ok.length / (load.elapsedMs / 1000),
    maximumInFlight: load.maximumInFlight,
    acknowledgementMs: metric("acknowledgementMs"),
    queueMs: metric("queueMs"),
    firstDurableTextMs: metric("firstTextMs"),
    settlementMs: metric("settlementMs"),
    generatorDelayMs: metric("generatorDelayMs"),
    sseReconnects: values.reduce((n, v) => n + (v.sseReconnects ?? 0), 0),
    errors: values
      .filter((v) => !v.success)
      .map((v) => ({ index: v.index, code: v.error ?? v.state ?? "unknown" }))
      .slice(0, 20),
  };
}

/** Pair timestamps for the same one-request Run, never subtract percentiles.
 * Multi-request and fault samples have different boundaries and are excluded. */
export function summarizeSingleRequestPhases(results, requests) {
  const plans = new Map();
  for (const request of requests) {
    const group = plans.get(request.planId) ?? [];
    group.push(request);
    plans.set(request.planId, group);
  }
  const before = [],
    provider = [],
    after = [];
  for (const result of results) {
    const group = plans.get(result.planId);
    if (!result.success || group?.length !== 1) continue;
    const request = group[0];
    const times = [result.submittedAt, request.startedAt, request.finishedAt, result.settledAt];
    if (
      request.outcome !== "completed" ||
      !times.every(Number.isFinite) ||
      times.some((time, index) => index > 0 && time < times[index - 1])
    )
      continue;
    before.push(times[1] - times[0]);
    provider.push(times[2] - times[1]);
    after.push(times[3] - times[2]);
  }
  return {
    samples: before.length,
    beforeProviderMs: distribution(before),
    providerMs: distribution(provider),
    afterProviderMs: distribution(after),
  };
}
