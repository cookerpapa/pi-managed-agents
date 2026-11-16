# Cold Run and history latency diagnosis — November 16, 2026

Follow-up to [the initial capacity baseline](runtime-capacity-20261115.md).
[Content-free evidence](runtime-capacity-diagnosis-20261116.json) retains the
paired timings, selected plan nodes, failed control cases and restoration results.

## Findings

The frozen-history read path has a confirmed query-plan amplification problem.
A small requested page can trigger millions of repeated in-memory lookups. This
reproduced both approximately seven-second reads and statement timeouts. It does
not require a large returned page, a slow model, missing indexes or exhausted RAM.

The cold-Run curve measures several costs together: ownership/writer startup,
native durable commits, canonical projection and, in the default workload, opening
SSE and loading a fresh conversation snapshot. Ten completed Runs/s is not a
configured product limit or a universal maximum. This investigation locates
expensive stages and unnecessary work; it does **not** establish one exclusive
cause for every throughput sample or a validated post-fix capacity number.

No business implementation, schema, native Pi loop, durability or fencing rule
was changed. Temporary diagnostic roles and the PG JIT experiment were restored.
Only benchmark outcome classification and derived timing reports were improved.

## Frozen-history query: a concrete cause

The implicated queries are the position and body reads in
[`frozen-history-window.ts`](../../packages/runtime-core/src/frozen-history-window.ts).
Both join through `pi_history_snapshots`. The body read repeats that expanded
view even though the preceding position read already selected the records.

In one captured body query:

- Only **three** requested records were returned.
- The optimizer estimated one row in several branches. The actual snapshot
  branch contained **67** frame positions; the tenant had **860** retained
  records and approximately **250** bodies.
- Record/body equality conditions were evaluated as join filters after the
  right-hand relations had been scanned. Existing composite indexes were used
  only by their tenant prefix, not by the full record/body identity.
- A body index was scanned **57,620 times**, with roughly 250 rows per scan and
  **14,406,072 shared-buffer hits in that branch alone**. Another entry/assistant
  branch repeated similar work. Parent and child buffer counters must not be summed.
- That body statement took **4.81 s**. A preceding position statement took
  about **2.25–2.31 s**, explaining a read in the same range as the earlier 6.87 s
  observation. Some later calls hit the normal five-second per-statement read budget.

The costly work was already in shared buffers. Increasing a timeout or assuming
disk/network bandwidth is the cause would miss the query problem. The plan's
`Index Cond` contained the tenant while the record/body ID comparison appeared
in a later `Join Filter`. The indexes exist; the selected plan uses them poorly.

The demonstrated mechanism is severe row-count underestimation plus an unfortunate
join order. Statistics, supplied parameter values and prepared-plan selection
can change that order, which is consistent with the intermittent behavior.
The exact statistics transition behind the original November 15 sample was not
recorded and cannot be reconstructed conclusively.

## Controlled reads and limits of attribution

Read-only diagnostic connections compared the same history under different plan
settings. Two histories produced these sequences:

| Trial | Default join planning, first read | Explicit join order | Default planning restored |
| --- | --- | --- | --- |
| A | 20 s diagnostic timeout | 268 ms | 267 ms |
| B | 20 s diagnostic timeout | 279 ms | 247 ms |

Successful results matched exactly and contained 64 visible items, including
32 Tools. The final default reads also became fast. This is evidence that the
data can be read cheaply and the slow plan is avoidable; it is **not** a clean
100× causal speedup claim for changing `join_collapse_limit` globally.

Small isolated fixtures and a read-only check of an existing longer conversation
did not reproduce every slow plan. They did show another cost: several expanded
history/header queries spent approximately 50–90 ms in planning while executing
in single-digit milliseconds. Reused generic plans made some complete reads much
cheaper, but forcing generic plans for every query is not an established remedy.
PostgreSQL explicitly trades custom-plan quality against generic-plan reuse and
can replan after statistics change. [PostgreSQL PREPARE documentation](https://www.postgresql.org/docs/current/sql-prepare.html)

The captured expensive frozen-history plans did **not** contain JIT. A broader
JIT on/off/on experiment was confounded by snapshot setup failures, warming and
different effective overlap; restoring JIT also produced faster samples. It
does not justify disabling JIT as the solution. JIT is a separate cost-based
mechanism. [PostgreSQL JIT decisions](https://www.postgresql.org/docs/current/jit-decision.html)

## What the cold-Run number contains

Pairing each Run with its own single simulated provider request gives:

| Concurrency | Submission → provider start p95 | Provider request duration p95 | Provider finish → Run settlement p95 |
| --- | --- | --- | --- |
| 1 | 864 ms | 5 ms | 123 ms |
| 16 | 1,378 ms | 4 ms | 688 ms |
| 32 | 2,451 ms | 4 ms | 1,964 ms |
| 64 | 5,283 ms | 4 ms | 7,276 ms |

These are distributions of paired per-Run intervals. They must not be added to
derive a total p95. The principal delays occur on the platform side of the model
request, both before dispatch and after provider completion.

Additional observations:

- The tested text Runs caused approximately **8–10 native Kafka publications
  each**. A newly active physical Session also initializes a transactional producer,
  including the required PG authorization before and after initialization.
  Those costs are amortized differently in a long Agent loop; they are not paid
  as a fresh Session for every model request.
- Larger-concurrency observations showed increased writer initialization and PG
  projection waits. PG activity sampling also found slow claim and history
  statements; it alone does not distinguish planning from execution. Claims in
  targeted direct tests were fast, so no global claim-SQL conclusion is warranted.
- Removing SSE viewers reduced some overhead and settlement tails, but did not
  establish a much larger sustainable completed-Run rate. Default cold-task tests
  include snapshot initialization; they are not a pure Agent executor benchmark.
- Raising libuv's pool from four to sixteen in an isolated native Kafka control
  did not produce a consistent gain across repeated cases. File-operation probes
  did observe pool contention. The native client's callbacks share this bounded
  resource, but increasing it alone is not the demonstrated fix.
  [Node thread-pool behavior](https://nodejs.org/api/cli.html#uv_threadpool_sizesize),
  [Confluent client documentation](https://docs.confluent.io/kafka-clients/javascript/current/overview.html).

## Bounded implementation directions

1. **Fix frozen paging first.** Select and bound visible positions before expensive
   lookups. Carry the existing immutable `entry_record_id`, `assistant_record_id`
   and body references forward, then retrieve those exact objects by their full
   tenant/ID keys. Avoid rejoining the expanded history view to rediscover IDs.
   Lateral keyed lookups or explicit materialization can preserve the intended
   bound even when estimates are wrong; global planner switches should not be
   the product contract.
2. **Make genuinely empty snapshots cheap.** Fresh ordinary Sessions currently
   enter the complex history reader too. An authoritative empty check must consider
   both own Runs and retained/shared history. A Fork with no new Runs is not empty,
   and child inherited context/cursor validation must retain their semantics.
3. **Avoid irrelevant presentation capture.** The Projector currently invokes
   `captureNativePresentation()` for every Entry or Task write. Many native Task
   transitions cannot produce a history frame. A narrower eligibility check could
   avoid that query while still applying every native Storage write; retry,
   compaction and terminal Tool cases need explicit coverage.
4. **Remeasure the whole path.** Use the same profiles with offered versus accepted
   work, observed active Sessions, snapshot costs, native commit counts and post-model
   settlement. Keep the original replica count, durability and ownership checks.

These are query/read-path improvements using existing authorities and indexes,
not a new event store, cache, queue or Harness. In particular, do not pool away
per-Session transactional identity, remove fencing, weaken Kafka acknowledgement,
or claim a multi-node capacity number from these single-host tests.

## Diagnostic and measurement hygiene

All model workload traffic used owned simulated providers. Heavy workloads ran
serially; concurrent calls within a cohort were intentional. Read-only analysis
of an existing conversation used a dedicated read-only PG connection and did not
save its transcript. Captured plans published here omit scope IDs and data values.

Temporary Worker/API/Session-runtime preloads recorded selected SQL plans without
bind-parameter values. They were bounded diagnostics, not production releases;
original templates were restored. The temporary PG JIT setting was restored,
and all benchmark fixture receipts confirm cleanup. Diagnostic cohorts are not
unmodified performance baselines.

One measurement defect was corrected: a history GET failure after a Run had
already completed used to erase that observed outcome in the load result. Run
completion and history failures are now separate fields; a history failure still
fails workload acceptance. Single-request before/provider/after intervals are
now derived by the maintained summary script. The earlier reports remain intact.
Seven targeted measurement/configuration tests passed. A live, deliberately
failed post-settlement history fetch reported one completed Run, zero failed
Runs and one history failure, while correctly rejecting workload acceptance and
completing cleanup. Original account/configuration records, two development-machine
intents and 393 Workspace files passed the preservation check; JIT is back to its
original value without an added system override.

The 6.87 s historical sample itself has no original SQL trace. The reproduced
failure identifies the same read path and a matching multi-second mechanism;
the report does not pretend to recover an execution plan that was never captured.
