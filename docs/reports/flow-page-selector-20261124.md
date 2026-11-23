# Selecting visible Runs for a history page

The opt-in Flow reader now selects Runs in the direction of travel and stops at the requested page boundary. It uses the current writable flow, frozen bases and existing mailbox index. This experiment changes read-side SQL in the prototype. Its mixed-load and public-read results below use two actual Session APIs on a shared, disposable two-CPU PostgreSQL 17.6 database.

## Why the earlier query cost CPU

The reference API at `faa2ac13` called `flow_read_ranges(tenant, window)` to find the visible prefix of each physical flow. A lateral index scan took up to 41 eligible Runs **per prefix**. PostgreSQL then ordered the candidates by their global mailbox position and took the final 41. The page layer can use up to 40 Turns, plus one to detect further history. Fetching bodies and applying the separate transcript byte/record limit happens later.

We captured the SQL and its parameters from the public HTTP route. Two costs showed up. On 100,000-Run histories, repeated planning of the expanded range query was expensive for recent and older pages. The reference sometimes already used a generic plan for paging forward. On a deliberately deep chain its plan could be cheap to build but expensive to execute: many prefixes supplied candidates to a sort even when the last prefix alone could fill the page.

The pilots ran equivalent statements on the same data. Isolating only the range calculation reduced the planner's work on short lineages but retained over 6,600 buffer hits on a 128-range latest page. Ordering by physical range and local position helped deep reads but retained substantial planning overhead on short ones. Returning only Run IDs from an early-stopping function required a second lookup for up to 41 Runs. Returning the required Run columns in the original index scan avoided those lookups. [Pilot observations and full comparisons](flow-page-selector-20261124.json).

## How the new selector works

The client transaction still validates the public Session and any cursor. It calls the ascending or descending page function with the tenant, window, **inclusive** global bounds, completion filter and page size. PostgreSQL processes non-overlapping visible ranges in the requested direction. Each index seek converts the remaining global interval to local mailbox coordinates and applies the same hidden/queued, prune and settlement rules as the previous reader. For example, if the newest range returns five eligible Runs, the next range receives a limit of 36. As soon as the total reaches 41, Run scans stop. The ancestor walk still depends on fork depth.

The key part of the descending seek is:

```sql
WHERE r.tenant_id = t
  AND r.session_id = segment.physical
  AND r.mailbox_position BETWEEN greatest(1, low_pos - segment.prior_positions)
                             AND least(segment.through_position, high_pos - segment.prior_positions)
  -- Visibility and settlement predicates also apply here.
ORDER BY r.mailbox_position DESC
LIMIT remaining
```

One SQL template creates the ascending and descending functions, each with a fixed index order. The function returns Run metadata directly. The outer SELECT orders at most one page and supplies the existing public identity fields. Its plan setting applies only during the function call. An explicit test set a different transaction-session policy, invoked the selector and checked that the previous policy was restored. Both the old and new selectors occupy one database round trip inside the existing repeatable-read transaction. [Selector implementation](../../scripts/experiments/minimal-session-storage/flow-page-turns.sql) and [PostgreSQL plan selection](https://www.postgresql.org/docs/17/sql-prepare.html).

## Measurement

Two API processes compared the previous checkout and the candidate on the same rows, credentials, indexes, database and public request parameters. We compared full HTTP resources, including order, body, identities and both cursors, and compared the results of the captured selector statements field by field. The cases were a 100,000-Run root, a window of the same visible length crossing two forks, and a 128-range chain with 48 local Runs per flow (46 inherited per earlier flow). The 128-range fixture was seeded after the shorter cases to keep its effect on planner statistics out of those measurements.

Each read-only experiment used counterbalanced blocks. Each endpoint had four blocks of 64 full pages, with eight concurrent clients, for each direction and window. Whole-page CPU is the summed PostgreSQL history and control pool CPU divided by completed requests. Selector CPU comes from the backend's Linux scheduler counter during warmed repetitions of the exact prepared statement. Planning and execution times from EXPLAIN are separate from the CPU samples. Buffer hits count repeated visits. Durations do not include browser rendering or a model call.

### Full-page CPU

The following figures are means over latest, older and newer requests, in milliseconds of PostgreSQL CPU per authenticated page. They cover the complete HTTP pages, including their other history queries and authorization. Two runs on the development laptop had different absolute CPU costs.

| History | First comparison, old → new | Confirmation, old → new | CPU saved across the two experiments |
|---|---:|---:|---:|
| Ordinary root | 10.21 → 8.06 | 5.23 → 4.14 | 20.9–21.0% |
| Two forks | 8.46 → 6.07 | 4.91 → 3.92 | 20.2–28.3% |
| 128 physical ranges | 10.18 → 5.99 | 9.74 → 5.46 | 41.2–44.0% |

### Visible-Run statement

Confirmation run with automatic planning on the API connections. Each entry gives the old and new CPU time for the selection statement alone and the shared buffer hits recorded by the representative EXPLAIN.

| History | Direction | Backend CPU, old → new | Shared buffer hits, old → new |
|---|---|---:|---:|
| root | latest | 1.65 → 0.42 ms | 20 → 20 |
| root | older | 1.73 → 0.46 ms | 20 → 20 |
| root | newer | 0.44 → 0.39 ms | 18 → 18 |
| branch | latest | 1.67 → 0.51 ms | 47 → 46 |
| branch | older | 1.69 → 0.44 ms | 42 → 36 |
| branch | newer | 0.47 → 0.48 ms | 42 → 42 |
| deep128 | latest | 5.01 → 0.84 ms | 6,689 → 1,073 |
| deep128 | older | 5.00 → 0.86 ms | 6,666 → 1,077 |
| deep128 | newer | 0.96 → 0.82 ms | 1,446 → 1,065 |

The former forward seek often already had a reusable plan. For the two-fork forward page the query cost was effectively unchanged. The large decrease in buffer visits for deep, older pages reflects eliminating earlier prefixes' Run scans. The remaining thousand-plus visits include determining the visible ranges.

### Boundary checks

An independent ordered list checked 24 combinations of scan direction, upper/lower bounds and completion filtering after introducing a wholly hidden physical segment and a mixture of Run states. Tests covered empty windows, missing or wrong tenant, overlapping physical flow IDs across tenants, a pruned Run, queued chat and compact commands, started and unstarted cancellations, failed, timed-out and active Runs, and pending or completed writer closure. Old and new HTTP readers agreed at the visible boundary and both rejected an anchor in a discarded suffix. A repeatable-read transaction retained its prior page while another connection changed visibility. The next snapshot saw the new tip.

One adverse case remains: with 4,096 consecutive unstarted chat Runs at the end of a single physical flow, the index still passes those filtered positions before filling the page. Both selectors returned the same 41 Runs and recorded 7,520 buffer hits. Early stopping across flows cannot remove a long filtered tail within one flow.

### Concurrent Kafka projection

On the same two-CPU PG container, two authenticated APIs alternated against one fixed 32-partition RF3 tape. The nested window was paged at a target of 32 requests/s. The writer and tables were identical in both arms. All ten trials drained all partitions, and each produced 2,560 tool results from 128 Runs, 23,808 Kafka records and 5,248 visible records.

| Case | Visible records/s per trial | Pooled visible records/s | Delivered pages/s per trial |
|---|---|---:|---|
| Writer controls | 456 / 711 | 556 | — |
| Old selector | 354 / 811 / 837 / 825 | 619 | 31.55 / 32.00 / 32.04 / 31.92 |
| New selector | 417 / 849 / 845 / 842 | 673 | 31.34 / 32.02 / 32.04 / 31.93 |

The pooled mixed-load improvement was about 9% in this run. The short writer-only controls also varied substantially. These data support the measured read CPU reduction and do not establish a fixed fleet-wide throughput multiplier. Production deployment is still gated on the wider Flow cutover.

[Commands and fixture setup](../../scripts/experiments/minimal-session-storage/README.md#run-boundary-flow-and-directory-recovery) and [structured evidence](flow-page-selector-20261124.json).
