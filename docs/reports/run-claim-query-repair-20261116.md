# Run-claim query repair — November 16, 2026

The keyed eligibility rewrite is deployed as `claim-keyed-20261116-r1`, from
commit `68dd3e55617708566974dd3d49cd4bc202c81646`. It removes the join
amplification identified in the [loaded-plan diagnosis](run-claim-query-diagnosis-20261116.md).
The queue, Session lease, writer fencing and native continuation contracts are
unchanged. [Sanitized measurements](run-claim-query-repair-20261116.json) accompany
this report; private identities, SQL parameters and user content are excluded.

## Query and admission behavior

Each candidate Run now reads its Session through a correlated lookup constrained
by the complete `(tenant_id, id)` key. A single keyed physical-Session lookup
checks both publication-partition eligibility and the absence of an unreleased
owner. Agent revision and tenant policy checks are similarly constrained.
Each lookup returns at most one row before participating in the next join.
The selected Run's Session and Workspace context use the same bounded pattern.

These lookups use `LATERAL` and `LIMIT 1` over existing unique identities, keeping
PG from flattening them into the former broad intermediate joins. They do not
limit the queue to its first row before checking eligibility: all eligibility
checks still precede the ordered winning-candidate limit and
`FOR UPDATE OF candidate SKIP LOCKED`. An ineligible or locked earlier Run must
not hide a later eligible one.

The materialized candidate CTE, subsequent READ COMMITTED input refresh,
Workspace/context equality checks, physical-Session lock and Worker/lease
authorization remain in the same claim transaction. No table, index, schema,
cache, scheduler or global PG planner setting was added or changed.

## Verification and publication

77 relevant tests passed: 60 parallel-claim/atomic-admission cases, 14 queue and
expired-lease cases, and three new real-PG query cases. The new tests exercise a
new tenant with 192 ready Runs against stale statistics from 40 small tenants,
partition and family restrictions, and an earlier candidate held by another
transaction. They check the actual executor query's result and buffer work,
without requiring a particular plan string or wall-clock threshold.
Initial fixture setup rejected invalid terminal-state data; the fixture was
corrected to use valid future-available queued Runs, and all three new cases
then passed. Runtime state constraints were not weakened to accommodate the test.

Runtime-core, control-plane and supervisor-host TypeScript checks passed. The
maintained release plan built two images (`control-plane`, `supervisor-host`)
and reused twelve, preserving their artifact/scan provenance and using the
existing vulnerability databases. The existing coordinated application drain
still applies to this shared runtime change. Guest images, VM allocations,
Workspace storage and schema did not require replacement or maintenance.

## Actual loaded plans

The deployed two-Worker service ran 192 cold text Runs at concurrency 64, using
the deterministic provider. Temporary connection-scoped `auto_explain` captured
the actual claim statements, with timing instrumentation disabled and bound
parameter logging disabled. PG activity was sampled separately. This is a
diagnostic workload, not a clean throughput comparison.

| Evidence | Earlier diagnosed sample | Repaired diagnostic window |
| --- | ---: | ---: |
| Server statement execution | 6,175.00 ms | Maximum 15.35 ms across 1,310 duration records |
| Shared-buffer hits | 1,427,511 | Maximum 564 across 1,249 decoded plans |
| Slowest sample's candidate Session lookup loops | 6,401 | 19 |
| Slowest sample's candidate Session rows per loop | About 173 | 1 |
| Candidate Session index condition | Tenant prefix; ID checked later | Tenant and Session ID together |

No captured duration exceeded 100 ms. The slowest repaired sample used 336
buffer hits and examined 19 candidates. The two windows have different candidate
populations, so these are observed plan-work comparisons, not a universal speedup
ratio. Small tables may still use cheap sequential scans; the repair prevents
the broad join multiplication rather than forcing every lookup to use an index.

All 1,310 duration headers were readable. Concurrent PG log output interleaved
61 JSON plan bodies, leaving 1,249 complete plans for structural/buffer analysis.
All decoded plans recorded zero shared reads. The instrumented workload completed
all 192 Runs at 3.98/s; its extra logging makes that rate unsuitable for judging
normal serving capacity.

## Normal serving measurements

After restoring the original Worker Pod template and removing diagnostics, two
serial runs used the same workload: 64 concurrent cold text Runs, 192 total,
4,096 requested output bytes and zero configured first-byte delay. Each completed
all 192 Runs with no dropped or failed requests. Histories were not paged in
this text profile. Worker timing logs were correlated with the exact test Run IDs.

| Metric | First run | Repeat |
| --- | ---: | ---: |
| Completed Runs/s | 9.49 | 6.44 |
| Candidate/context query p95 | 8.99 ms | 9.59 ms |
| Entire claim transaction p95 | 81.39 ms | 85.67 ms |
| Queue delay p95 | 1,037 ms | 129 ms |
| First durable text p95 | 7,191 ms | 18,291 ms |
| Settlement p95 | 9,261 ms | 20,403 ms |
| Maximum observed Kafka lag | 914 | 1,554 |

The repaired candidate query no longer explains the seconds-long waits in these
samples. Overall throughput and settlement remain variable. Downstream
publication/projection/settlement queueing remains a separate investigation;
these bounded bursts establish neither sustainable rate nor fleet capacity.

## Fault acceptance and restoration

Six serial fault profiles met their expected outcomes:

- Worker handoff: four original Runs completed with replacement writers and
  four physical Tool calls, preserving logical Run identity without duplicate effects.
- Projector restart: all four Runs completed, with writers preserved and SSE
  reconnecting. The disruption still produced a roughly 40-second settlement tail.
- Stop: two selected Runs cancelled, two others completed, and late replies did
  not overwrite cancellation.
- Lost Tool reply: all four Runs completed with four physical writes.
- Synthetic rate-limit and disconnect profiles each produced eight terminal
  failures; eight subsequent user requests then completed. This records current
  error behavior, not automatic recovery/retry of every provider error.

All four evaluation invocations reported successful cleanup and restored model
and provider configuration. No real-provider requests, foreign-model calls or
charges occurred. Diagnostic connections and ConfigMaps are gone, the original
Worker template is restored, and no load-driver Pods remain. PG retains `jit=on`
and `plan_cache_mode=auto`; diagnostic logging is off.

The original 393 Workspace files and two development machines were preserved.
Both machines are ready with running intent and no failure code. Existing
accounts and credentials were not replaced. Source checks, builds, load profiles
and fault exercises ran serially on the existing single-host WSL/K3s deployment.
