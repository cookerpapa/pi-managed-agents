# Starting the independent Session cutover

The reference installation uses the #52 API, Worker, Projector, PostgreSQL and RF3 Kafka entry points. These are candidate measurements from one WSL host. Production was not changed.

## Mail and mid-reply takeover

A new serving check schedules an occurrence while the first Turn is already executing an HTTP MCP tool round, then sends a user steer through the authenticated API. It checks the Turn of the canonical user Entry in PG. The baseline placed it in the later scheduled Turn. Selecting the earliest eligible open Turn under the existing Session lock placed it in the original Turn. The append position for a new Turn is determined separately. The public input receipt and pending page can also represent a stable mailbox input that has no Turn yet. Eager Turn creation is still present in this revision.

A second check opens the real SSE connection, receives text, kills Worker A and waits for the ordered PG seal. The baseline replacement never delivered a terminal event before the test deadline. With the Worker initializing the logical Session from both its claim base and the last position saved under its physical Flow, the same Turn reached completion under two distinct Workers. One sample saw event 2 before the kill and terminal event 45 after takeover, with one completed assistant answer. This checks an in-flight request. It does not infer physical tool execution from a browser event.

The schedule list and post-deletion response expose a stoppable Turn only for the matching scheduled origin. Both Stop buttons send the originating schedule ID. Both the candidate and the existing Session cancellation handler verify the immutable opener in their cancellation transaction. The idempotency check includes this scope. Session-page Stop continues to use its ordinary contract. The existing deadline listener now runs in the independent command executor. In the serving check the public API was suspended when a plan came due. The plan fired, the API resumed, deletion offered the owning Turn, a schedule-scoped Stop cancelled it and a retry returned the original receipt. Another plan could not stop the human-opened Turn.

## Upgrade tail

The identical fixed tape contains 20 proposals, 20 results and one final answer per Turn. The four-partition trial admits 480 Sessions at 10/s, 64 slots and 2 PG CPUs. A time through final drain and 19,680 verified visible records define the throughput metric. The synthetic timed section delays a result by 50 ms and does not execute a shell. Separately the serving suite exercises a real HTTP tool, native Pi, scheduled input and Stop.

| Upgraded installation | Visible records/s including drain | Terminal projection p99 | Peak sampled raw lag | Final state |
| --- | ---: | ---: | ---: | --- |
| v3 unchanged | 221.7 | 23.75 s | 5,952 | 480 completed, lag 0 |
| Same images, replace only the recovery index before measurement | 392.2 | 279 ms | 496 | 480 completed, lag 0 |
| Index installed by candidate migration, repetition 1 | 390.4 | 62 ms | 134 | 480 completed, lag 0 |
| Same candidate, repetition 2 | 233.8 | 21.51 s | 8,417 | 480 completed, lag 0 |
| Same candidate, repetition 3 | Incomplete | Not reported | Not used for comparison | All 480 terminal Kafka ACKs, 443 completed, 37 queued for recovery |

The old recovery index included projection_first in its key and predicate. The field advances during stable projection. Recovery loads the active rows and their state for each partition, so the index needs only the partition and the active-state predicate. The replacement avoids index changes solely because the recovery offset advanced. The isolated index intervention reduced observed WAL from 311,037 to 272,286 bytes per Turn and PG CPU from 100.8 to 49.3 CPU seconds across the measured workload. The repetitions demonstrate substantial remaining variability. No planner method was disabled. The installer builds a replacement index before dropping the old index, and a cancelled concurrent build can be retried.

For the independent Session consumer, a stable PG projection base already precedes a warm, memory-only suffix. The consumer now reconstructs from that base when necessary. A pure non-opening suffix needs no extra pin-only Turn write. Three upgrade trials before the terminal policy change reported zero pin-only writes; 263.2–265.9 visible records/s, 14.06–15.45 s terminal p99, and about 205–206 kB of WAL per Turn. Around 44,000 Turn updates remained per trial. The current native handoff still makes the full reader request the original writer prefix in some cases.

A separate live repair gate publishes partial text in A, lets B on the same partition advance PG, suspends the Worker and kills the Projector. With no newly emitted Worker text, the new SSE snapshot reproduced the original prefix and the real Turn subsequently completed. Pruning also revealed that a logical Session with a new writable Flow retained its old display coverage; even the initial SSE subscription then failed. The candidate carries the Flow across the local cache, queued stream delivery and canonical PG read. On the same public Session, the post-prune handoff completed after two writers and one final answer.

## Terminal ordering and the owner-approved decision

On the failed third repetition, a read-only diagnostic scanned all four partition prefixes and compared them with the 37 queued Turns in PG. All 37 had a normal work_finished *before* execution_seal on the same original writer. On partition 2 one normal terminal was at offset 15985 and the failure seal at 21887. This was a live capture after the producer confirmed all 480 terminals. The benchmark Worker asserts if asked to re-execute fresh-only synthetic input, exposing the mistaken takeover. A model rerun or customer charge was not observed.

SegmentWorker removes the writer from the local renewal set when execute returns after the terminal Kafka ACK. On expiry the reconciler records a PG seal intent. The current boundary transaction rejects an otherwise matching normal terminal if that intent already exists, including when the terminal precedes the forced seal in Kafka. The ordered forced seal can consequently requeue completed work. An index can shorten the vulnerable interval. The owner selected the first valid boundary in the partition for the same writer, after validating native settlement. The settlement now preserves a previously accepted user Stop in the public result, clears the unused recovery request and leaves physical tool revocation intact. A later forced seal cannot reopen the completed Turn. The serving check can deliberately hold the partition projection transaction while the normal terminal and then the actual expiry seal arrive in RF3 Kafka; it checks the winning offset on both lease and Turn, physical revocation and the public terminal event.

After the rule change, the RF3 fault gate observed work_finished at 8 and execution_seal at 9. The winning Turn and lease both recorded 8. With no Stop the Turn completed. With a schedule-independent human Stop after expiry intent, it cancelled publicly at 8. Physical revocation remained set in both cases.

| Installation with ordered terminal handling | Visible records/s | Terminal p99 | Peak raw lag | Final |
| --- | ---: | ---: | ---: | --- |
| Upgrade 1 | 250.5 | 20.43 s | 5,138 | 480 complete, lag 0 |
| Upgrade 2 | 343.0 | 1.45 s | 1,501 | 480 complete, lag 0 |
| Upgrade 3 | 203.2 | 33.26 s | 8,065 | 480 complete, lag 0 |
| Fresh | 262.4 | 14.29 s | 3,608 | 480 complete, lag 0 |

The third upgrade completed despite the tail exceeding the default 30 s lease interval. No successful trial establishes saturation. Pure cached suffixes no longer account for a pin-only write in these runs. The common projection still updated Turns roughly 44,000 times per 480-Turn trial, including committed display and native checkpoint work.

See the [implementation plan](independent-session-cutover-plan-20261128.zh.md), [candidate decision](../adr/0379-independent-session-cutover.md) and [preceding upgrade study](independent-turn-references.md).
