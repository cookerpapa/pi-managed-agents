# What a terminal Session record waits for

[Issue #48](https://github.com/cookerpapa/pi-managed-agents/issues/48) follows the [storage-layout comparison](native-usage-and-chat-history-20261118.md). We reanalysed its five 192-Run bursts, then ran three additional synchronized bursts with entire Projector handlers and PostgreSQL server durations instrumented. Each Run used a separate fresh physical Session, a deterministic 4 KiB simulated answer and one of 32 Kafka partitions. All eight waves used two Workers, one Projector, RF3 Kafka and a 32-record diagnostic poll. These are single-host WSL/K3s measurements.

## Reconstructing the 3.48-second queue

The Worker can finish generating an answer after publishing its ordered data to Kafka while the Projector still has earlier records waiting. For each terminal record the reader timestamped local enqueue and the start of its partition handler. We intersected this interval with the handlers of earlier delivered offsets **on that partition**. A handler that delays five Runs contributes to the delay experienced by all five. CPU time and total machine elapsed time have different denominators.

| During the previously reported final-candidate queue | Share of accumulated waiting |
|---|---:|
| Earlier projection waiting for a PG connection | 42.61% |
| Earlier projection inside a PG query call until its callback | 41.96% |
| Other time inside the measured projection method | 0.96% |
| Outside that method | 14.48% |

These four intervals account for the mean of 3.48 seconds, subject to rounding. Earlier records from other Sessions contributed 53.95% of total waiting, and earlier records from the same Session contributed 31.57%. The old probe only wrapped the inner projection method. The new probe also covers admission, routing retirement, control changes and the part after native projection. Its outer SQL spans exclude those already counted by the nested probe.

One actual Run near the median waited 3.023 seconds. During that wait 67 earlier inner projections ran on its partition. Nine belonged to its own Session and 58 belonged to five others. Intervals for partially completed records are clipped. The median elsewhere in this wave was 14 overlapping records from other Sessions, so this example illustrates interleaving and should not be used as its typical record count.

## Which records take the time

In the first whole-handler trace, 70.72% of accumulated terminal waiting overlapped an earlier record belonging to another Session, 28.89% overlapped its own and 0.39% was outside the measured handlers.

| Earlier handler in the first fully traced wave | Share of terminal queue |
|---|---:|
| Run completion, including revoking Tool routes | 13.39% |
| Complete assistant answer | 11.85% |
| User Entry and associated native changes | 9.43% |
| Cloud Run and Pi Agent configuration | 8.79% |
| First live change requiring a PG replay reference | 8.36% |
| Initial Workspace/environment observation | 7.96% |
| Initial native Conversation and four documents | 7.92% |
| Native system context | 7.77% |
| Generation Task-only commit | 7.43% |
| Input queue control notifications | 7.12% |
| Model sampling completion event | 5.40% |
| Session activity | 4.07% |
| Subsequent live-only changes | 0.08% |

The eight native shapes that used PG each committed once per Run. Across 192 Runs the 312 subsequent live-only updates in this wave issued zero SQL. The root created `pi.live`, `pi.inbox`, `pi.usage` and `pi.agent` together. The Worker then saved `pi-cloud.runtime_world_state` in one native commit and `pi-cloud.run` with the `pi.agent` configuration in another. These last two commits were adjacent in the delivered Kafka sequence for 138/192 Sessions in this wave and 155/192 in the preceding final-candidate wave. Runs on other Sessions can appear between them.

Every Run had two input-queue notifications before its own terminal record. Another 181 notifications arrived during the tail of other Runs. The 192 subagent-state notifications arrived after **all** terminal records during fixture cleanup. They cost zero in the reported queue. The queue view includes unclaimed initial Runs and the native inbox. Changes to Run state or lease can therefore require a visible refresh even when the user added no follow-up. The most common synchronous control path began a transaction, advanced the partition checkpoint and committed. An idle or chat-only Run also closed its routing before completing. The extra five outer SQL calls cover closing execution-target routes and the sandbox-binding closure transaction.

## The projection pool

The serial pool experiment changed only the Projector pool size between 12, 32 and 12. Source hashes and database observations identify the pool actually used. Peak lag is the sampled sum of Kafka offset distances. All three waves completed 192/192, cleaned up their owned resources and drained their lag to zero. Server-duration logging adds overhead. Compare measurements within this experiment separately from the earlier five waves.

| Pool | Runs/s | Mean local terminal wait | Queue time overlapping pool checkout | Queue time overlapping a SQL callback | Other Sessions’ share of the queue | Peak lag |
|---|---:|---:|---:|---:|---:|---:|
| 12, first | 6.65 | 7.15 s | 49.30% | 48.86% | 70.72% | 1,698 |
| 32 | 8.28 | 5.60 s | 0.72% | 97.69% | 70.60% | 1,515 |
| 12, repeat | 15.05 | 2.87 s | 50.00% | 47.78% | 71.11% | 1,651 |

With 32 connections, checkout almost disappeared and query-call waiting grew. The 32-connection throughput fell between the two 12-connection trials. These trials establish where contention moved. They do not establish a higher sustained rate from a larger pool.

Client query timing measures the interval from calling node-postgres until its callback. The first 12-connection trace saw 20,314 calls across the dedicated projection pool, about 106 per short Run including transaction commands. The earlier figure of roughly 70 counted only the calls inside native commit projections. PostgreSQL reported **64.49 backend-seconds** against **286.11 client-query-seconds** across these 12 backend PIDs. With 32 connections those totals were **74.43** and **471.61 seconds**. These are sums across concurrently running backends. Samples also frequently found the backend idle in an open transaction, waiting for its client. The residual includes scheduling, socket and client work. This instrumentation cannot uniquely attribute it to one of them.

## A history-length-dependent SQL cost

The two variants of the native pre-write query that read the latest commit and touched state accounted for 15.17 uniquely matched backend-seconds in the first 12-connection wave. They share a lookup with the cold `head()` method. The lookup combines the Session's commit receipts with its optional inherited base and takes the highest sequence. The local branch can feed **all receipts for that Session** into the final selection.

We captured the SQL compiled by the actual projection. The most frequent traced pre-write template and the isolated benchmark had the same query digest, `9e10563334b4ca2d`. In one PG connection we copied the current table/index definitions into temporary tables, added 8,192 receipts from other Sessions and varied the target Session's history. Both query shapes ran in alternating order on the same data. The experimental query takes the newest local receipt using the existing primary key, then compares it with the single base row. Both complete result sets were checked. A separate comparison covered empty storage, base-only, a newer local receipt, matching sequence/ID and a higher base.

| Receipts in the target Session | Receipt rows returned by the head scan, original → bounded | Original execution | Bounded execution |
|---:|---:|---:|---:|
| 8 | 8 → 1 | 0.204 ms | 0.209 ms |
| 20,000 | 20,000 → 1 | 4.033 ms | 0.220 ms |
| 100,000 | 100,000 → 1 | 20.916 ms | 0.228 ms |

These timings are median PostgreSQL execution time over 25 EXPLAIN ANALYZE samples per query shape. The target Session's data and the 8,192 peers' receipts existed only in connection-local temporary tables. The new shape used one backwards index lookup on the receipt branch at both larger sizes. Each synchronized short Run had only a handful of receipts. The long-history result establishes a separate scaling defect and a bounded read that can be applied to both callers.

## Engineering implications

The bounded receipt lookup is the most localized correction. It uses the existing index, preserves independent native receipts and can be checked against cold opening, replay and Forks. For short Runs, the next comparison should target one fewer whole transaction during cold startup. The first Workspace observation and Run configuration are both cloud-owned. Test one Pi transaction for their initial writes on a new Session, with crash and input-admission checks at the boundary. Their combined queue share measures the cost of both handlers and is not a predicted saving. Existing Sessions can have a real environment change to record. The partition ordering, original Tool effect boundaries and exact post-commit input acknowledgements remain constraints on any such experiment.

The normal release stayed on its previous version throughout this investigation. The controlled deployments restored their original templates. [Content-free measurements](terminal-projector-queue-20261118.json). [Reproduction procedure](../../scripts/experiments/projector-sql-20261118/README.md).
