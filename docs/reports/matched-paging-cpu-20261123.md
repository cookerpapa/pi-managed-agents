# PostgreSQL cost of a chat history page with and without Fork

Continuous paging reduced write capacity with Fork entirely removed. On the two-CPU fixture the direct reader took about 7.9 ms of database-backend CPU per page. The Flow path took 9.1–10.2 ms. The SQL that chooses the visible Runs accounts for a reproducible part of the difference. These results support retaining the current storage design and investigating this read query first.

## What was compared

Two physical schemas shared one disposable PG instance. The direct control ran the ordinary root message reader with Flow disabled. Its storage had zero Flow helpers, predecessor columns or predecessor indexes. The other schema installed frozen-base Flow history. Both received the original reader's turn-identity lookup indexes. Both used the same Projector implementation, message shape, authentication, HTTP client, two-CPU quota and 768 MiB memory limit. This compares candidate storage layouts. It does not compare two production releases.

Each schema had 100,014 physical Runs. The direct and Flow-root cases read one 100,000-Run history. The nested case read the same visible content through three physical flow prefixes (two forks). The latest page crossed both fork points. The older page lay in the common ancestor. All three request directions returned 32 prompts and 32 corresponding 1 KiB assistant texts per page. The benchmark checked their order, text, count and mailbox positions for equality across the three arms. Normalized user-visible JSON was about 34.4 kB. The HTTP response also contains IDs, cursors and metadata. Test messages contain text only.

## CPU per authenticated page

The two fixtures each ran three counterbalanced blocks of 96 requests per direction and layout, with eight read slots. Each layout supplied 1,728 measured pages across latest, previous and next reads. CPU time is the sum of user and system ticks of the PostgreSQL processes serving the API history and control pools, divided by completed pages. Container CPU provides an independent cross-check. Queries ran against warmed data without Projector writes in these blocks. HTTP and browser CPU are outside the figures.

| Page layout | Latest, history + control | Older | Newer | Mean history only | Mean control pool | Mean combined | Whole PG container/page |
|---|---:|---:|---:|---:|---:|---:|---:|
| Direct reader, no Fork schema | 8.0 ms | 8.0 ms | 7.8 ms | 7.4 ms | 0.5 ms | 7.9 ms | 8.1 ms |
| Flow reader, ordinary root | 11.0 ms | 11.7 ms | 8.1 ms | 9.7 ms | 0.5 ms | 10.2 ms | 10.4 ms |
| Flow reader, nested branch | 9.4 ms | 10.8 ms | 7.0 ms | 8.6 ms | 0.5 ms | 9.1 ms | 9.2 ms |

At 8 pages/s these means imply approximately 0.06–0.08 cores of read demand. At 32 pages/s the estimate is 0.25–0.33 cores. Concurrent scheduling, query plans, history shape, cache state and hardware change the observed cost. A measurement on two fork points cannot predict the cost at hundreds of fork points.

## Which statements consumed CPU?

A separate observer connection sampled the actual API backend's Linux scheduler CPU counter immediately around each history SELECT for 48 serial HTTP requests per page shape. For the older-page sample in the fixed-rate study, CPU in milliseconds per page was:

| Read operation | Direct | Flow root | Nested Flow |
|---|---:|---:|---:|
| Session metadata | 0.29 | 0.31 | 0.29 |
| Resolve cursor Run | 0.27 | 0.47 | 0.48 |
| Select visible Runs | 0.87 | 2.56 | 2.56 |
| Locate visible entries | 1.21 | 1.23 | 1.22 |
| Fetch bodies and tool dependencies | 0.97 | 0.94 | 0.92 |
| Terminal metadata for the page | 0.59 | 0.53 | 0.52 |
| Run headers | 0.35 | 0.33 | 0.35 |
| Project environment | 0.24 | 0.22 | 0.22 |

The statement measurements use a single history reader and exclude control-pool work, transaction boundaries and backend work between instrumented SELECTs. The eight-client whole-page measurements are the applicable capacity figures.

The returned branches used bounded index reads. To separate query planning from execution, we replayed the captured visible-Runs statement on one backend with the original bindings. Under PG's automatic policy, its prepared statement used 201 custom plans and zero generic plans in each sampled layout. The following diagnostic compares the same returned rows and verified identical results in both plan modes. The planning and execution columns are wall time from warmed EXPLAIN EXECUTE, using the mean of two seven-execution medians per mode. The CPU column comes from separate blocks of 64 executions.

| Older page, visible-Run selection | Plan policy | Planning | Execution | Backend CPU/call | Shared buffer hits |
|---|---|---:|---:|---:|---:|
| Direct | Custom / generic | 0.314 / 0.010 ms | 0.185 / 0.199 ms | 0.70 / 0.31 ms | 10 / 10 |
| Flow root | Custom / generic | 1.289 / 0.017 ms | 0.425 / 0.510 ms | 2.34 / 0.63 ms | 20 / 20 |
| Nested Flow | Custom / generic | 1.344 / 0.015 ms | 0.496 / 0.491 ms | 2.42 / 0.55 ms | 42 / 42 |

Replanning the more complex Flow SQL accounted for much of the incremental cost on these pages. A generic plan reduced that cost for this statement and sample. The API kept its automatic policy throughout the mixed load. Checking varying depths, tenant sizes and live/queued history would be necessary before changing how that statement is planned. [PostgreSQL prepared statement and plan selection](https://www.postgresql.org/docs/17/sql-prepare.html).

## Projector under the same read workload

Each independent suite preloaded one RF3 Kafka tape and replayed it with fresh groups in rotated or reversed order. Every one of the 42 trials drained 32 partitions, handled 128 Runs and 23,808 Kafka records, projected 5,248 user-visible records, committed 11,008 PG transactions and returned 2,560 tool results. Projector and readers used the same physical storage in each trial. A limited-rate arm spread arrivals at a fixed rate across at most eight in-flight requests. Continuous arms kept all eight slots busy. Readers used the public HTTP client; this measures serving and storage, not DOM rendering or sandbox execution.

Rates below pool the three replays of each condition as total visible records divided by total measured seconds. Parentheses give measured page requests/s.

| Main suite | Writer only | Target 8 pages/s | Eight continuously paging clients |
|---|---:|---:|---:|
| Direct, no Fork | 588 | 554 (8.02) | 347 (78.35) |
| Flow, ordinary root | 713 | 863 (8.05) | 295 (60.75) |
| Flow, nested branch | same Flow control | 649 (8.08) | 396 (85.20) |

For the direct control, writer-only rates were 468, 533 and 917 visible records/s. With continuous paging the corresponding rates were 303, 294 and 515, a reduction of 35–45% in all three blocks. With eight pages/s they were 468, 456 and 920. The higher Flow-root rate in the lightly paced arm is within substantial host/run variation and cannot establish a write benefit from reads. The three continuous arms processed different numbers of pages per second; their write rates cannot isolate the cost of Fork.

| Independent 32 pages/s suite | Adjacent writer controls, records/s | Mixed, records/s | Achieved pages/s |
|---|---:|---:|---:|
| Direct | 530 / 685 / 532 | 839 / 464 / 453 | 31.76 |
| Flow, ordinary root | 876 / 539 / 933 | 815 / 433 / 839 | 31.98 |
| Flow, nested branch | 892 / 562 / 886 | 435 / 425 / 836 | 31.71 |

Even with matched arrivals, the laptop's short-run write rates varied substantially; in one direct pair the mixed arm was faster. An exact fixed-rate throughput penalty is not supported by those data. The backend CPU measurements are the more reliable basis for attributing per-page overhead. The investigations changed the disposable benchmark only. [Reproduction](../../scripts/experiments/minimal-session-storage/README.md#run-boundary-flow-and-directory-recovery) and [raw measurements](matched-paging-cpu-20261123.json).
