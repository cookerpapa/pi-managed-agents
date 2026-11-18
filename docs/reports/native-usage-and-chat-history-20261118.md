# Native usage and chat history under load

[Issue #48](https://github.com/cookerpapa/pi-managed-agents/issues/48) and [ADR-0337](../adr/0337-native-usage-and-chat-presentation.md) change what PostgreSQL retains from Pi's existing stable Storage commits. Each native system Entry still supplies the model context. The chat index skips it. The current native usage document supplies the Conversation total. The assistant Entry still carries the usage of its individual response. Provider accounting uses its own evidence.

| Information in a stable answer | Reader and retention |
|---|---|
| Assistant Entry and its per-response usage | Pi reconstructs context and the web presents the answer. The immutable native body is shared by history and Forks. |
| Generation Task, input settlement and live checkpoint | A replacement Worker can tell whether work is complete and recover any outstanding output. These keep their native retention policies. |
| Conversation usage total | The Worker and public `/usage` reader need the latest sum. One current document per native usage address is sufficient. |
| System Entry and tool declarations | Pi and a new Fork can load the original context. A separate web frame has no display consumer. |
| Model accounting evidence | Metering distinguishes provider usage from customer eligibility, including cancelled and superseded requests. The aggregate in `/usage` serves a different query. |

## Storage and behavior

| Observation | Previous implementation | Candidate |
|---|---:|---:|
| Native system Entries across 192 new Sessions | 192 | 192 |
| Chat frames for those system Entries | 192 | 0 |
| Current usage rows across the 192 Sessions | 0 | 192 |
| Usage revision index rows | 192 | 0 |
| Historical usage payloads | 384 | 0 |
| Logical usage payload after 100 cumulative updates in one Conversation | 18,884 bytes across 101 rows | 304 bytes in one current row |

These are running-database counts. The byte measurement covers the usage payload only. The shared database also stores native commit receipts, indexes, individual model answers and WAL. In both layouts the usage was 100 before and after index reconstruction. Three Sessions per burst were read through the public history and usage endpoints and compared with their canonical assistant Entries.

A clean-cutover rehearsal on a separate real PostgreSQL instance first exercised the previous layout. Preflight and migration refused the incompatible history. The ordinary conversation deletion and reference collection cleared it, then the new migration completed with its user, project and Workspace still present. The performance comparisons used fresh test Sessions. The existing development conversations did not participate in the cutover.

## Synchronized bursts

Each row released 192 short Runs simultaneously in independent Sessions on the same WSL/K3s host. The public Session HTTP/SSE path, two Workers, three Kafka brokers, one Projector, 32 Session partitions, 12 projection connections and PostgreSQL all participated. The model was simulated with a deterministic 4 KiB answer and zero configured first-token delay. The diagnostic used a 32-record native Kafka poll and a private instance of the ordinary Session API matched to each storage layout. The control was [ad0c7bc6](https://github.com/cookerpapa/pi-managed-agents/commit/ad0c7bc62204da400ac9e9bfa4aefef27967eaff).

| Trial | Runs/s | PG calls in native commits per Run | Model finish to terminal PG commit, P95 | Peak observed Kafka lag | Lag after draining |
|---|---:|---:|---:|---:|---:|
| Control 1 | 6.67 | 75.98 | 14.38 s | 1,911 | 0 |
| Candidate 1 | 9.11 | 69.96 | 10.26 s | 1,493 | 0 |
| Candidate 2 | 15.00 | 69.77 | 3.62 s | 846 | 0 |
| Control 2 | 7.20 | 75.98 | 11.34 s | 1,825 | 0 |
| Final candidate | 10.55 | 69.91 | 7.00 s | 1,343 | 0 |

Every burst finished 192/192. Native call counts include transaction commands. The system Entry went from 10 to 6 calls in every case. The most common complete assistant batch went from 16 to 14. User Entry batches used 11 in both layouts. The two earlier candidate repetitions preceded the last public usage checkpoint validation. The final candidate includes that check.

A typical 14-call batch contained one assistant Entry, one input Submission change, one generation Task change and two document changes. Its trace used three calls for BEGIN, transaction settings and COMMIT, one for Run and writer admission, three for pending receipts, native state input and the combined native write, three for model accounting evidence, three for the visible frame and one for display coverage. The previous 16-call trace also deleted and reinserted the usage revision index. The accounting context lookup and the frame's readback of the accepted Entry remain visible in the trace.

The trace pairs the last model response and the terminal commit for **each** Run. On the final candidate the mean was 4.079 s. It took 0.358 s for the terminal Kafka record to reach the local reader, 3.480 s for its partition handler to start and 0.241 s from handler start to the PG commit callback. Across this wave, 85.52% of the accumulated queue time overlapped an earlier traced record running on the same partition. Earlier complete assistant batches accounted for 11.80% of queue time. This is time experienced by the waiting Runs, so a handler can affect several waits. Handler intervals include time using or waiting for shared resources. Their CPU, pool and database costs require separate attribution.

## Fixed arrivals

The uninstrumented arrival workload used the normal single-record poll, offered 20 Runs/s for 20 seconds and allowed up to 64 in flight. Both versions accepted and completed 400/400 with zero drops.

| | Control | Candidate |
|---|---:|---:|
| Completed Runs per wall-clock second | 19.82 | 19.74 |
| Submission to terminal SSE, P95 | 1,695 ms | 589 ms |
| Peak observed Kafka lag | 322 | 140 |
| Lag after draining | 0 | 0 |

PG work fell in every burst. Bursts still vary substantially on this host. The fixed arrival test reached its offered rate on both versions, so higher sustained capacity remains unproven. The normal deployment stays on its existing version under the #48 performance gate. [Content-free measurements](native-usage-and-chat-history-20261118.json). [Reproduction procedure](../../scripts/experiments/projector-sql-20261118/README.md).

## Fault acceptance

The final serial regression ran 271 tests in 24 suites. They cover duplicate and conflicting native commits, current-document retirement and cold reads, real Pi usage, forward and backward pagination, retained child Conversations, Fork context after deletion of the original, fencing, accounting and real PostgreSQL backup/restore with attachments.

All seven development-cluster scenarios passed on the final candidate. Synthetic rate limits and disconnects produced the expected terminal error and the next user request succeeded in all 16 cases. Four Tool calls survived a Worker handoff. A Projector restart after the first visible text preserved four writers and produced four exact answers. All 32 partitions served their idle snapshot after recovery. Two Stops retained their visible 192-byte prefixes. Stop during Tool execution discarded the late result, and losing a reply produced the four expected physical writes. The driver confirmed cleanup, and the original Projector and Worker Pod templates matched their pretest values. The four pre-existing conversations were still present.
