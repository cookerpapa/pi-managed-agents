# Physical read splitting for older conversation pages

This isolated experiment exercises a primary and a physical PostgreSQL 17.6 hot standby, each limited to two CPUs and 768 MiB. Both ran on the same WSL host with a shared storage subsystem. The standby received a base backup, streamed WAL over its own Docker network and passed checks for recovery mode, read-only transactions and asynchronous streaming. The public deployment has no replica routing configured.

## Read contract

The API authenticates against the primary. Only an explicit `before` cursor enters the optional older-page hook. Initial pages, forward reads, queries by Turn or Entry, mutations and SSE bootstrap retain their primary path. A rejected cursor remains an API error. An older-page request routed to the standby gets the same repeatable-read conversation reader and content/byte limits as a primary request.

For each eligible request the guard samples `pg_current_wal_insert_lsn()` on the primary. It checks whether the fixed standby is in recovery and has replayed at least that position. Only after a successful check does it open the standby page's read transaction. It polls at roughly 4 ms intervals with a 25 ms retry budget, then uses the primary if replay is behind. Checkout, network and the final in-flight query can extend the wall time. Probe unavailability or a transient standby read failure can also fall back. Actual projection history transactions use their usual local synchronous-commit policy. An insertion watermark covers earlier acknowledged asynchronous commits even if their WAL was still waiting to flush.

This is a conservative freshness rule. It can make a page for Session A wait for WAL from an unrelated Session B. No per-Session receipt or second Projector was added. The test uses one fixed primary and one fixed replica throughout each replay. Production promotion, timeline validation across failover, cross-region latency, and sustained recovery conflicts under vacuum remain separate considerations. [PG streaming replication](https://www.postgresql.org/docs/17/warm-standby.html) and [PG WAL functions](https://www.postgresql.org/docs/17/functions-admin.html).

## Workload and measurement

The authenticated older pages have 32 prompts and associated 1 KiB answers on a 100,000-Run lineage with two fork points. All read arms query the same fixed historical cursors and check the contents before timing. The main experiment compares primary reads with guarded standby reads at 32 requested pages/s and with eight clients continuously reading. Both arms keep WAL streaming. Each uses the same frozen 32-partition RF3 Kafka tape and fresh consumer groups. Timing covers first delivered data through final projected record. Replay complete means all PG partition positions matched Kafka's end positions and all 2,560 tool results were present.

A separate observer samples a fixed primary insertion position and checks how long it takes to observe the standby at or beyond that position. Polling, scheduling and query trips are included in the elapsed time; it measures a detection delay, not one-way network latency. Unreplayed bytes compare the primary sample with the following standby sample. CPU figures come from each container's cgroup during the timed window. Replica route counters include requests finishing just after the measurement window, whereas delivered page rates use only requests completed within it.

## Paired results

The primary and standby each had their own CPU quota and page cache. The primary-only arm kept the same live standby replaying. At a 32 pages/s target, the two arms delivered 31.5 and 31.9 pages/s. The continuous tests kept eight readers busy; the number of completed page requests differs by arm.

| Load | Page placement | Visible records/s per trial | Pooled visible records/s | Delivered pages/s | Page P95 per trial (ms) | Served by standby |
|---|---|---|---:|---:|---|---:|
| writer | primary | 764 / 817 / 809 / 393 | 633 | — | — | — |
| 32 | primary | 669 / 250 / 302 / 728 | 393 | 31.5 | 55 / 230 / 181 / 53 | — |
| 32 | standby | 712 / 590 / 305 / 755 | 519 | 31.9 | 54 / 168 / 156 / 56 | 80.4% |
| saturated | primary | 342 / 383 / 486 / 493 | 416 | 107.5 | 221 / 181 / 93 / 92 | — |
| saturated | standby | 217 / 221 / 634 / 202 | 256 | 62.2 | 283 / 223 / 83 / 251 | 76.7% |

At the fixed rate, 1,041 reads used the standby and 254 fell back. With continuous readers, 3,948 used it and 1,196 fell back. The guard made 5,448 and 16,615 replay-position queries respectively. The primary still handles a small watermark query for every eligible page. During the guarded fixed-rate replays the observer detected catch-up in roughly 18–21 ms at P50 and 30–62 ms at P95 (ranges across the four trials). With continuous guarded reads the per-trial P95 ranged from 37 to 93 ms. A recent commit to an unrelated Session can trigger the same wait.

Across the four fixed-rate pairs the guarded arm had higher individual projection rates, while the continuously reading guarded arm had lower pooled write and page throughput in this cohort. The laptop's writer-only controls ranged from 393 to 817 visible records/s. Reported changes are observations of finite replay runs. The high variation precludes quoting a stable capacity gain.

## Read-after-prune, reconnection and outage

The fixture first forked at Run 800 and read an older page from the standby. It then paused WAL replay, waited for the pause to take effect, cut the window back to Run 100 and admitted a new active Run. In the dedicated recovery confirmation the control transaction committed synchronously; the normal projection SQL appended the first canonical text, its coverage and the partition position together with `synchronous_commit=off`. The raw standby reader still returned the old page. The guarded HTTP request for the same cursor fell back and got **404**. A request for the latest page immediately saw the new Run on the primary. After replay resumed, the raw reader also rejected the stale cursor and guarded paging returned to the standby.

The reconnection check used the normal public SSE handler and SDK. In the fault rerun the public API also relayed over the actual internal `SessionLiveService` HTTP route. The test supplied deterministic text to the owner's normal live view. A connection received the primary's persisted message and its live suffix, then received another delta. The next connection's snapshot contained each piece once. After resuming replay, the fault confirmation also checked the projected message and its exact partition checkpoint on the standby. A fresh page can still fall back if a newer watermark has appeared in the meantime. The test uses separate HTTP listeners on the same host and supplies the live events directly; the Kafka performance trials exercise the projector separately.

Finally, the fault test stopped the standby container. The first eligible history request fell back to the primary in **31.32 ms** in the first test and **38.44 ms** in the final confirmation and returned the same full page. The latest page and a relayed SSE reconnection also succeeded, with the replica-route counters unchanged for both. This tests a reachable primary during standby loss.

## Continuous paging on a second replay cohort

A second fixed tape repeated the saturated comparison and added a diagnostic control. The raw standby served only historical windows that the fixture kept immutable for these trials. All three arms executed the same authenticated older-page HTTP requests; clients kept eight requests outstanding. The raw route has no freshness guarantee for a mutable window.

| Page placement | Visible records/s per trial | Pooled visible records/s | Completed pages/s | Page P95 per trial (ms) |
|---|---:|---:|---:|---:|
| primary | 201 / 429 / 229 | 257 | 62.3 | 284 / 102 / 224 |
| standby | 581 / 625 / 228 | 390 | 92.5 | 94 / 79 / 236 |
| frozen | 210 / 675 / 267 | 300 | 90.4 | 182 / 53 / 147 |

Here the guarded split was faster than the primary-only arm, reversing the ordering of the first saturated cohort. Bypassing the freshness check also allowed the standby to continue serving reads while its replay position fell substantially behind. In the frozen-data arm, 28 sampled watermarks failed to catch up within the observer's 1.5 s observation window. Its small set of successful samples would understate lag if those censored observations were ignored. The headroom varies with scheduling, concurrent reads and replay; the data do not isolate a single cause of the between-cohort spread.

Every trial in both cohorts verified 5,248 visible records, 11,008 projection transactions (5,504 asynchronous), all 2,560 tool results and every primary partition checkpoint. These are finite-backlog rates. The standby does continuous replay in both arms, and data, CPU quotas, polling overhead and client request counts matter when comparing results.

## CPU placement at equal work

As a separate control, the two databases were warm, the Projector was idle and the API fetched exactly the same authenticated page 384 times per arm, in counterbalanced 96-page blocks with eight clients. Every returned page matched the primary. The measurements below are whole-container CPU milliseconds per completed page, including authentication, probes, replay and incidental background work.

| Route | Primary CPU ms/page | Standby CPU ms/page | Pages served by standby |
|---|---:|---:|---:|
| primary | 10.32 | 0.04 | 0 / 384 |
| standby | 0.98 | 9.55 | 384 / 384 |

This control confirms that the page computation moves off the primary when the standby is current. Mixed-load throughput still depends on replay keeping up and on how many guarded requests return to the primary.

## Decision

Keep primary reads for the latest page and SSE bootstrap. The physical standby is a viable option for older history. The current per-request, cluster-wide WAL guard has a measurable polling and fallback cost; keep it experimental. Before adopting a default split, test a narrower freshness rule tied to the particular history being returned. A production policy would need to cover pruning/deletion from another client, cursor membership and any mutable part of the requested page.

[Study commands](../../scripts/experiments/minimal-session-storage/README.md#run-boundary-flow-and-directory-recovery) and [per-trial evidence](physical-history-read-split-20261124.json).
