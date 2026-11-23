# Fork correctness and simultaneous Session API paging and projection

The frozen-base layout passed the branching-history checks. Paging and projection compete for CPU on the same PostgreSQL instance. The Session API read path is opt-in in the candidate. Deployment remains gated.

## Shared ancestry

An independent model keeps an array of Run IDs for every open window. Fork copies an array prefix, prune truncates one array and deletion removes one array. It has no flow IDs, predecessor queries or garbage collector. Four fixed seeds each start with siblings, two empty forks at the same Run and a fork of a fork. The source is deleted, one populated branch is cut back into an ancestor's flow, and a new Run is appended there. Each seed then makes 180 generated decisions and deletes every remaining window. Single-row or small GC slices interleave with those mutations.

The four cases included 397 appends, 189 forks, 117 prunes and 197 window deletions. At most 15 windows were open simultaneously. At 9,088 window checks both the actual predecessor page (including body text) and the API reader's flow-range order matched the independent arrays. After draining the collector, the sets of Run and message IDs in PG were exactly the union of reachable IDs. All four cases ended with both sets empty.

Each seed also forced two collectors to contend at one shared ancestor. The test observed a PostgreSQL lock waiter, rolled back the first collector, allowed the other to commit and retried the pending job. An empty window continued to protect the common prefix until it too was deleted.

## Public API and browser

An isolated API process runs the public controller, bearer-token authentication, bounded read pool and repeatable-read conversation reader. The ordinary web build opens the session in Chrome. The fixture uses real control metadata and a candidate history schema in the same database, with separate API and Projector pools. The synthetic corpus starts with 100,000 sealed Runs, each with a prompt and a 1 KiB answer. Two generations of branches contain distinct answers at overlapping local positions. One more branch remains empty.

Deleting the source leaves an inherited history visible. The public client verified 96 consecutive prompts across three pages and checked the associated answer text across physical flows. Paging forward returned the original recent page. Pruning a populated branch into the original prefix invalidated a cursor into its removed tail. The source, sibling and physical cursor checks use the authenticated HTTP endpoint. During each mixed load trial, Chrome scrolled back and received an additional older page. A separate HTTP read during active projection returned 10–20 transcript items on the actual Run being written.

For the bounded reader, the Session's reachable flow prefixes establish each Turn's displayed order. SQL takes a page-sized sample from each visible prefix using the mailbox index, then gets bodies by physical stream and Run. The public Session ID scopes the cursor. The reader checks the cursor's physical stream against the visible anchor. Each page runs in one read snapshot.

## Fixed-work mixed load

A disposable RF3 Kafka topic with 32 partitions carried a preloaded capture from Pi's embedded Harness with its deterministic model and local test tool. Each replay used the same 64 Runs with 20 tool calls each, 11,904 Kafka records and 2,624 user-visible records. Each trial used a new Kafka consumer group and a reset projection. The candidate used the normal native history fold and bounded prefix preparation. It committed 5,504 PG transactions per replay. All 32 PG partition positions matched the Kafka end positions and all 1,280 tool results were present on every replay.

The server was PG 17.6 in a two-CPU, 768 MiB container. The Session API ran in a separate process from the consumer. Eight HTTP clients used the web client's response parser and alternated latest, older and newer pages. Paced clients targeted one page per second each. Saturated clients immediately requested the next page. One real Chrome tab also scrolled during each mixed trial. The repeating order was writer only, paced, saturated, saturated, paced, writer only. Timing covers the first delivered batch through the last projected record.

| Load | Visible records/s, two trials | HTTP pages/s | HTTP P95 | Drain time |
|---|---:|---:|---:|---:|
| Projector only | 327 / 275 | — | — | 8.03 / 9.53 s |
| Projector and paced readers | 271 / 239 | 8.25 / 8.02 | 485 / 352 ms | 9.70 / 10.97 s |
| Projector and saturated readers | 137 / 140 | 24.43 / 28.05 | 471 / 421 ms | 19.16 / 18.68 s |

A short reader-only control had P95 156 ms at the paced setting and 159 ms with continuous paging. The sample contained 32 and 249 in-window responses respectively. Mixed-load latency includes HTTP, authentication, the full history assembly, JSON delivery and client parsing. The one-query preview experiment used a different unit of work.

Two representative warmed HTTP requests took 30 ms for the latest page and 34 ms for an older page. The read pool issued seven SELECTs for the first, eight for the second. An older page also validates its cursor. A fresh EXPLAIN of each parameterized query visited at most 67 rows on any one base-relation scan across its loops. The sampled plans showed bounded reads to locate entries, fetch bodies and dependencies, and join terminal and environment metadata. The read-side SQL timers accounted for 14 and 17 ms in those requests. The measurement does not isolate every millisecond of the remaining HTTP time.

PG used approximately two CPU seconds per elapsed second throughout projection, and cgroup counters recorded quota throttling even in the writer-only runs. Counter intervals include reader startup. These observations support CPU contention in the shared database. The arrival stream was finite, so draining all offsets measures completeness and replay speed. It does not establish a sustainable arrival rate or a production capacity figure. This benchmark covers canonical Tool history; physical Guest delivery, a paid provider, and sustained live SSE traffic are separate workloads.

Before cutover, compare the same public requests against a separate read-capacity configuration or reduce the work of assembling a full page, keeping cursor isolation and repeatable snapshots. Browser history reads are covered here; public Fork/prune mutations, full cold handoff and retained attachments still have their own integration gates.

[Reproduction commands](../../scripts/experiments/minimal-session-storage/README.md#run-boundary-flow-and-directory-recovery) and [structured measurements](flow-tree-and-paging-20261123.json).

## Do history reads block projection writes?

On PostgreSQL 17.6, the paginated HTTP read used its existing repeatable-read snapshot. A controlled invocation of the candidate's actual projection SQL inserted a second assistant message, updated the same Run's display coverage and advanced the partition offset in one open transaction. The writer could see both messages. An authenticated HTTP request for that Run returned in 49.49 ms with the first message. We kept the transaction open for 865.72 ms after the write. After commit another HTTP request saw both messages. A separate read-only repeatable-read transaction held across that commit continued to see one message, then saw two in its next transaction.

As a positive control, a second connection requested `SELECT … FOR UPDATE` on that Run while projection held the write open. PostgreSQL reported `Lock:transactionid` with the projection connection as the blocker. The writer held granted `RowExclusiveLock` table locks on the messages, Runs and offset tables. Ordinary page reads take a compatible `AccessShareLock`. The Projector's partition-offset row lock orders writers to that offset; the page has no reason to acquire it. [PostgreSQL explicit locking](https://www.postgresql.org/docs/17/explicit-locking.html) and [MVCC](https://www.postgresql.org/docs/17/mvcc-intro.html).

We replayed one RF3 tape eight times on the same disposable database, changing only the PG CPU quota and reader load between arms. Each trial drained 32 partitions, projected 2,624 visible records, and verified 1,280 tool results. Eight concurrent clients continuously requested pages in the mixed trials. A separate observer sampled labelled API, control and Projector connections, including heavyweight lock holders when any such wait was reported.

| PG CPU quota | Reader load | Visible records/s (two trials) | Pages/s | Page P95 | PG CPU cores used |
|---:|---|---:|---:|---:|---:|
| 2 | No induced paging | 460 / 470 | — | — | 1.99 / 1.96 |
| 2 | Continuous paging | 259 / 255 | 54.61 / 54.86 | 207 / 224 ms | 2.00 / 2.00 |
| 4 | No induced paging | 1,379 / 967 | — | — | 2.99 / 2.12 |
| 4 | Continuous paging | 435 / 288 | 140.83 / 64.64 | 117 / 201 ms | 3.18 / 3.39 |

There were 1,216 snapshots, including 741 during mixed load. None of the sampled active API, control or projection backends reported wait type `Lock`. Of 885 active page-backend observations, 792 had no reported wait. One observed a buffer-content lightweight lock and one a data-file read. Across all trials the Projector frequently reported `LWLock:WALWrite` and `IO:WalSync`, including when it ran alone. These events concern PG's WAL write path. Event counts are samples of backends, not elapsed milliseconds. An active backend with no reported wait can be running or runnable. Samples were separated by up to 149 ms, so very short waits could be missed. [PostgreSQL wait events](https://www.postgresql.org/docs/17/monitoring-stats.html#MONITORING-PG-STAT-ACTIVITY-VIEW).

In the two-core mixed trials the container was throttled in 99 of 101 and 102 of 103 quota periods. With four cores the simultaneous workloads completed more history pages and projected more visible records per second. This establishes a shared CPU-capacity effect on this host. The four-core runs varied substantially and the closed-loop clients offered more work when service became faster. Those numbers are benchmarks of this fixture, not a linear scaling factor. No production settings changed. [Per-trial measurements](flow-paging-waits-20261123.json).
