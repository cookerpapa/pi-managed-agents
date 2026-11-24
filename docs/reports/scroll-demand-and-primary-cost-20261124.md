# Browser scroll demand and single-primary history cost

The previous 32-pages/s figure was a specified aggregate offered load. Eight continuously fetching API clients were a saturation test. Translating either directly into people reading conversations overstates the evidence. A coding timeline with collapsed tools and a long-text timeline can generate very different request rates under identical wheel input.

## Browser experiment

The actual Web UI and authenticated Session API ran in isolated Chrome against PostgreSQL 17.6 with two CPU equivalents and 768 MiB. Chrome DevTools dispatched native mouse-wheel input inside the chat scroller. We counted completed successful older-page requests and distinct cursor URLs at the network boundary, and sampled the scroller's position when fetch started. The study opened a fresh session view at its latest page. The older fixture's one-time forced-top acceptance step targeted a different session and was excluded.

The UI begins looking for earlier history at `scrollTop < max(320, viewportHeight) + 250` with history available. A busy guard serializes ordinary page loads. After layout and anchor restoration it can check the edge again, so one gesture can produce multiple successive reads while the displayed history remains short. The timeline retains up to four browser pages. [UI trigger](../../packages/web-ui/src/ConversationTimeline.tsx#L575) and [page retention](../../packages/web-ui/src/history-window.ts#L107).

The 702 px viewport had a 952 px prefetch threshold. The text fixture contained a prompt and a 1 KiB assistant answer per Run. Its initial rendered scroll height was 12,336 px. The coding fixture contained 500 Runs, 20 completed Bash tools per Run, short results and a short final answer. Its tool groups used the default collapsed presentation and the initial scroller was only 702 px tall. These are synthetic UI interactions, not user telemetry.

## Page IO and CPU

The text-page sample used a warm authenticated read of 32 Turns, each with a prompt and an assistant answer. Eight history SELECTs ran for an older page. After measuring repeated reads we generated 96 consecutive, distinct cursors, checkpointed the disposable database and evicted the fixture relations and their indexes from PostgreSQL's shared buffer pool with `pg_buffercache_evict`. We fetched the 96 pages in order and fetched the same sequence again. The OS cache was left intact. PostgreSQL I/O timing was enabled on the fixture's history and projector connections. Counters were flushed at the sampling boundaries. Mixed-trial WAL wait times sum backend work and can overlap in wall time.

A shared hit counts a buffer access, including repeated visits to the same index or heap page. A shared read counts a block fetched into PostgreSQL buffers and may be served by the filesystem cache. The separate cgroup block-device counters show I/O charged to the PG container on WSL's virtual storage. They do not establish the servicing path inside the Windows host or performance on a genuinely cold production disk. [PostgreSQL buffer statistics](https://www.postgresql.org/docs/17/monitoring-stats.html) and [buffer eviction for developer tests](https://www.postgresql.org/docs/17/pgbuffercache.html).

## Requests the browser actually made

| Fixture | Gesture schedule | Gestures | Observation (seconds) | Older pages | Requests/s |
|---|---|---:|---:|---:|---:|
| text | idle | 0 | 5.0 | 0 | 0.00 |
| text | read | 40 | 30.6 | 1 | 0.03 |
| text | fast | 100 | 11.6 | 5 | 0.43 |
| text | pause | 0 | 3.5 | 0 | 0.00 |
| 20-tools | idle | 0 | 5.0 | 0 | 0.00 |
| 20-tools | one-notch | 1 | 2.6 | 3 | 1.15 |
| 20-tools | read | 40 | 30.6 | 44 | 1.44 |
| 20-tools | fast | 100 | 11.6 | 101 | 8.74 |
| 20-tools | pause | 0 | 3.5 | 0 | 0.00 |
| one-long-run | idle | 0 | 4.0 | 0 | 0.00 |
| one-long-run | one-notch | 1 | 1.6 | 4 | 2.50 |
| one-long-run | no-further-input | 0 | 12.0 | 28 | 2.33 |

The reading script moved up 0.75 viewport every 750 ms. The fast script moved up one viewport every 110 ms. The long-Run check sent a single 0.17-viewport wheel event after an idle interval. In the text case the first reading request began with 547 px still above the visible screen. The six reading/fast requests were distinct and at most one was outstanding. On the 20-tool history a separate single-notch observation requested three distinct pages at scrollTop 0, 466 and 810 px.

**One-gesture cascade.** A single Run with 1,024 consecutive tools had a 688 px viewport and a 688 px initial scroller. The page registered exactly one actual wheel event. After it, the browser sequentially requested 32 distinct older cursors; the last response reported `hasBefore=false`. During the final twelve seconds there were no more gestures. The timeline was still within the threshold when earlier pages settled, and the edge recheck continued fetching. This reproduces an independent UI source of unnecessary reads with the default collapsed groups.

## Indexed reads in the warmed text fixture

| Requested pages | History CPU + control CPU per page | Shared hits | Shared reads | PG block-read time, whole batch | PG cgroup device read bytes |
|---|---:|---:|---:|---:|---:|
| same-hot-page (128) | 4.30 + 0.39 ms | 184,611 | 0 | 0.00 ms | 49152 |
| 96-distinct-first-pass (96) | 4.69 + 0.42 ms | 137,058 | 2,335 | 16.34 ms | 0 |
| 96-distinct-second-pass (96) | 4.06 + 0.31 ms | 135,076 | 0 | 0.00 ms | 0 |

Recent pages used the unique tenant/flow/mailbox-position index for the Run window, Run identity indexes, the message index on `(run_id,commit_seq,ordinal)` and the terminal-event replay index. A single SQL call can perform many B-tree probes. In a sampled text page, locating entries, loading bodies and locating terminal state issued probes for multiple Turns. A block hit may repeatedly visit the very same index root. Reading from a shared buffer still uses CPU for lookup, row visibility, filtering, joins, column extraction and delivery. The physical-device counter also includes unrelated database background work during the block.

The following samples used a separate observer to read the *actual API backend's* CPU time around each history SELECT. Entries are milliseconds of CPU per call, averaged across 48 real requests. Each of these eight statements occurred once per measured page. The complete page also pays transaction, connection and authentication costs.

| Stage | Text older page | 20-tool older page |
|---|---:|---:|
| session metadata | 0.26 | 0.81 |
| resolve cursor run | 0.42 | 1.08 |
| select visible runs | 0.60 | 1.16 |
| locate page entries | 0.87 | 5.10 |
| load bodies and tool dependencies | 0.75 | 5.17 |
| load terminal metadata | 0.43 | 0.93 |
| load run headers | 0.31 | 0.68 |
| project environment | 0.20 | 0.47 |

## Projection on the same primary

Each trial replayed the same 35,712 Kafka records for 192 Runs with 20 tools, verified 3,840 Tool results, 7,872 user-visible records, 16,512 PG commits and all 32 end positions. The two independent suites used the same workload shape with separate fixtures. The model and tools are deterministic. Trial order was rotated or reversed. Writer-only controls have the same browser present without induced history paging. Rates pool equal work across trials. CPU share sums history and control backend PID CPU deltas divided by total PG-container CPU. The measurement includes small monitoring and accounting costs. It is approximate, especially for just one request over a short run.

| Corpus | Load | Actual older pages/s | PG CPU share from history and control | Visible records/s in individual trials |
|---|---|---:|---:|---|
| Text | writer | 0.00 | 0.00% | 786 / 396 / 467 / 855 / 864 / 614 / 862 |
| Text | one-reader | 0.11 | 0.18% | 807 / 893 / 887 |
| Text | eight-fast | 3.54 | 1.68% | 706 / 495 / 865 |
| Text | 32-hot | 31.24 | 13.67% | 341 / 290 / 692 |
| Text | 32-advancing | 31.76 | 13.08% | 405 / 787 / 309 |
| Text | saturated-hot | 97.69 | 32.12% | 525 / 544 / 267 |
| 20 tools | writer | 0.00 | 0.05% | 414 / 851 / 866 / 463 |
| 20 tools | web-read | 1.59 | 1.54% | 506 / 709 / 864 |
| 20 tools | web-fast | 7.12 | 6.13% | 807 / 384 / 353 |
| 20 tools | eight-fast | 45.40 | 35.00% | 547 / 240 / 366 |

In the text suite `one-reader` replays observed request timing onto the real HTTP client, starting at a prefetch boundary, so even a short trial can get a read. `eight-fast` staggers eight readers using the observed fast trace for the indicated corpus. Each advances its own cursor across data; these eight readers are simulated. In the tool suite `web-read` and `web-fast` drive a real Chrome tab *during* projection from the latest page. The fixed 32/s controls either repeat three hot pages or advance eight independent cursors. The unrestricted control keeps eight HTTP requests in flight. It bypasses the Web timeline.

At about 32 text pages/s, old and advancing cursors accounted for 13–14% of PG CPU in this experiment. The unrestricted clients delivered roughly 98 pages/s and consumed 32%. Repeated cursors create a cache-friendly test, while immediate reissue exaggerates the number of reads for someone browsing long text. For collapsed tool output, eight fast readers at distinct positions in the same conversation could in fact create aggregate load of this order.

**Independent variation in the writer.** The seven text writer-only controls ranged from 396 to 864 visible records/s. For example, the first two delivered 786 and 396 with zero measured history-backend CPU. Projector backend CPU for the same work changed from 18.83 to 37.90 seconds. The same backend CPU-only sum over 40,000 integers took 3.46/3.72 ms before/after the first run and 6.80/6.81 ms before/after the second. Across the seven controls the correlation between the mean calibration CPU and total Projector backend CPU was about 0.99. This small sample establishes a large independent change in the measured cost of CPU work. It does not establish a particular power setting, processor frequency or host scheduling cause. WAL sync time also varied and can overlap across backends. Raw differences between unpaired throughput trials cannot all be charged to a reader.

## Why some indexed tool pages really are expensive

The page needs the original tool call for certain tool-result entries. The current entry-location and body queries both join the assistant message. In the single long-Run fixture there were 2,049 stored message entries in that Run. The sampled plans used `messages_run` to fetch rows by Run repeatedly, even though the join needed one Entry. The entry-location query did this 66 times. The body query did it 64 times for the body and 64 times for its dependency. These EXPLAIN figures refer to rows emitted by the inner scan before the join condition is applied. This repeated work can consume CPU entirely from cached blocks.

We tried two read-only SQL changes in the disposable fixture. The first gates the lateral dependency lookup on a non-null assistant ID. The second looks up the Entry inside its tenant, physical stream and conversation first, checks Run membership on the joined row, and uses the same approach for the main body. The prototype already has a unique tenant/stream/Entry key. The plan chose the existing conversation/Entry index and retained the Run check. The captured SQL results were compared in full; the actual HTTP reader also returned identical complete pages under the per-process diagnostic replacement.

| Long Run, one older page | Original CPU/statement | Null-gated lookup | Exact Entry lookup | Original / exact buffer hits |
|---|---:|---:|---:|---:|
| locate | 46.48 ms | 6.95 ms | 0.68 ms | 8,460 / 140 |
| bodies | 87.99 ms | 49.57 ms | 0.82 ms | 16,640 / 640 |

For body assembly the exact plan issued 64 `messages_conversation_entry` probes for bodies and 32 for dependencies, each returning a single row. The complete older-page HTTP comparison on this stress case measured **272.5 → 11.7 ms of database CPU per page** (64 equal pages per arm, counterbalanced blocks, eight clients). This includes control and history pools. The serial statement cost and concurrent whole-page cost were measured separately.

On the 20-tools-per-Run fixture a simpler null-gated lookup reduced measured whole-page CPU from 10.43 to 6.45 ms before the explicit analyze and from 21.02 to 11.29 ms in a separate post-analyze comparison. Each pair alternated the two SQL shapes on the same data. The host varied between comparisons. The 20-tool sampled page spanned three Turns with 30 visible tool items. The original direct fetch for its repeated, warmed page took about 14.4 ms of PG history plus control CPU in the latter fixture.

## Conclusion

The stress test answers a capacity question about a specified aggregate page request rate. The browser tests explain how that rate may be generated. Long text produced very few reads under the tested gestures, ordinary collapsed tool groups produced more, and one long collapsed tool group could exhaust the entire history after a single gesture. Separately, some indexed tool queries repeatedly visited broad per-Run ranges in memory.

Prioritize bounding edge-triggered automatic loading after a gesture on compact histories and validating exact Entry lookups for tool dependencies and bodies. Preserve smooth prefetch at the top and cover boundary tool pairs, pending results, multiple lanes, fork scope and missing entries before shipping a query change. Actual user gesture distributions and genuinely cold disk performance remain unmeasured. All changes in this study are benchmark instrumentation, evidence and documentation. [Reproduction commands](../../scripts/experiments/minimal-session-storage/README.md#run-boundary-flow-and-directory-recovery) and [structured measurements](scroll-demand-and-primary-cost-20261124.json).
