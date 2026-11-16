# Run-claim query diagnosis — November 16, 2026

Follow-up to the variable throughput in the
[published repair measurements](workload-release-throughput-20261116.md).
The owner requested investigation of the first remaining bottleneck, before
deciding how to change it. No production business SQL, schema, ownership rule or
planner setting was changed in this investigation.

The subsequent [repair and acceptance](run-claim-query-repair-20261116.md) are now
deployed. This document retains the original diagnostic evidence.

## Confirmed finding

The actual loaded claim query can generate a severely amplified join plan.
The slow example spent **6,175 ms executing on PG**, versus approximately
**135 ms planning** and **6,329 ms observed by the Worker**. Existing indexes are
present, but important entity-ID equalities are applied after much broader joins.

The [content-free evidence](run-claim-query-diagnosis-20261116.json) preserves the
measured plan-node work, workload, wait samples and restoration checks. Private
SQL logs and bound identities are not published.

In the slow example:

- The query needed one locked candidate. It encountered 37 queued Runs; the
  relevant physical-Session relation contained 196 rows despite an estimate of 12.
- Publication eligibility and current-owner eligibility each refer to
  `pi_sessions`. Their placement in the chosen join order creates a large
  intermediate result before it is connected to the specific candidate Session.
- The `candidate_session` lookup executes **6,401 times**, returning about
  **173 matching rows per execution**. Its index condition uses only
  `tenant_id`, although the existing index also contains the Session ID.
- The later join filter finally checks `candidate_session.id = candidate.session_id`
  and the physical Session identity. It discards **1,104,683 combinations**.
- The statement records **1,427,511 shared-buffer hits and zero shared reads**.
  Parent and child buffer counters are inclusive and must not be added together.
- Only one candidate is returned. A separate slow case returned none after
  approximately 2.33 seconds and 1.92 million buffer hits.

The queue's outer materialized CTE already limits the *result* to one locked Run.
That boundary is too late to bound the expensive eligibility joins inside it.

## Why it sometimes looks fast

The same backend later returned a candidate in **7.196 ms**, with 6,590 buffer
hits. Its plan used full-key physical-Session and lease lookups after resolving
the candidate Session, avoiding the large intermediate combination. It still
performed tenant-prefix Session scans, so it is not a sufficient design target
for much larger tenants.
Queue contents and parameters changed between these observations; this is not
a controlled benchmark of the proposed rewrite.

The slow plan estimated one candidate where it actually found dozens, and only
a few Sessions where it actually scanned many. Row-count underestimation and
join ordering explain the observed multiplication. We have not isolated which
statistics/parameter transition selected every different plan; the report does
not claim a single cause for all earlier throughput variation.

Both custom and generic prepared-plan executions exhibited slow cases. The
largest example was a custom plan; the 2.33-second empty result used a generic
plan. Forcing generic plans globally is therefore not established as a remedy.

The captured slow plans contain no JIT section. Of 273 sampled active claim
observations, 270 had no PG wait event and three reported `ClientRead`; none
reported a lock wait. Those samples do not exclude brief waits, but together with
the plan counters they identify repeated in-memory join work as the dominant
mechanism in this example.

## Proposed correction boundary

Keep the current PG queue, one physical-Session lease and existing transaction:

1. Scan ordered ready Runs and resolve each candidate's Session/configuration by
   complete entity keys, including tenant and ID. Make the single-entity lookup boundary explicit so the
   planner cannot turn it into repeated tenant-wide scans.
2. Read the candidate's physical Session once for publication-partition and
   unreleased-writer eligibility, rather than composing two separately movable
   physical-Session subqueries.
3. Keep all eligibility decisions **before selecting the winning candidate**;
   locking an arbitrary ineligible first row would starve later eligible work.
4. Preserve `FOR UPDATE SKIP LOCKED`, queue ordering, partition/family restrictions,
   tenant policy and runtime-kind checks. Retain fresh authority locking and the
   later mutable-input read. In particular, an expired but unsealed writer cannot
   be treated as available.
5. Load remaining context through the already selected immutable IDs. The outer
   Session lookup also currently uses a tenant-only index prefix and should be
   bounded rather than merely relying on today's small row counts.

This is a query-structure correction within the existing architecture. It does
not need another queue, cache, query projection table or per-Run ownership layer.
Implementation must verify same selected candidates and concurrency/recovery
semantics, plus measured work under skewed/stale tenant statistics. A new index
or a larger timeout alone does not address the demonstrated late ID filtering.
No repaired-query throughput is claimed yet.

## Method and restoration

Two existing Workers temporarily loaded observation hooks around only
`claim_candidate`. PostgreSQL `auto_explain` ran on those connections, with
`log_analyze` enabled, per-node timing disabled and bound-parameter logging
disabled. Planner statistics were enabled only for the statement's transaction.
This follows PostgreSQL's documented session-local loading and timing-overhead
controls. [PostgreSQL auto_explain](https://www.postgresql.org/docs/17/auto-explain.html)

The existing simulated-provider driver sent 192 cold Runs at concurrency 64;
all completed. The instrumented run measured 8.28 Runs/s, which is diagnostic
evidence, not a new uninstrumented capacity baseline. In total 1,368 plans were
captured, including idle queue reads; 19 exceeded 100 ms. A separate read-only
observer sampled activity/waits every 200 ms.

The initial exact-template restore guard rejected Kubernetes' defaulted volume
fields. Restoration then removed only the owned diagnostic fields, verified that
the remaining template exactly matched the original, and used a compare-and-set
patch. Both Workers returned to their original template; the ConfigMap and all
diagnostic PG connections are gone. No business code was changed to collect data.

Final checks: `jit=on`, `plan_cache_mode=auto`, planner statistics off and
auto_explain threshold disabled. Fixture model/provider configuration was restored;
there were no model charges or foreign-provider calls. Original account/configuration
records, two ready development machines and 393 Workspace files were preserved.
