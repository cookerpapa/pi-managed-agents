# Ordered Session projection and Run tail latency

November 18, 2026. [Issue #48](https://github.com/cookerpapa/pi-managed-agents/issues/48)
and [ADR-0334](../adr/0334-ordered-projection-round-trips.md).

The candidate reduces PG calls per short Run and preserves native recovery.
The experiment keeps each Pi commit separately visible and preserves the
existing ordering of Tool effects and input confirmations. The current
development deployment returns to `projector-idle-eof-20261118-r1` after each
controlled test. Publication is deferred.

## Workload and evidence

The production Session client submits fresh Sessions through the existing
Session API, two Workers, the real Pi Harness, CLIProxyAPI, RF3 Kafka, the
partition owner, PostgreSQL 17.6 and SSE. A deterministic model produces one
4 KiB response with zero configured delay for the short text cases. Real
provider requests are zero. Tests run serially on the 24 GiB WSL host.
Synthetic tenant data and the temporary provider/catalog route are removed
after each workload.

Each complete suite includes a serial case, warm-up, two closed-loop bursts
of 192 Runs with at most 64 in flight, 400 arrivals offered at 20/s over
20 seconds with 64 in-flight slots, and 128 in-flight clients using a model
with five seconds of first-byte delay. Later repetitions also exercise the
actual control Tool host with 32 concurrent clients. Burst throughput includes
setup of each stream and final drainage. Client latency begins at submission,
after the stream is ready, and ends when the client receives the terminal event.
`runs.settled_at` is stamped within the final PG transaction. For each Run the
Worker creates its `work_finished` event before submitting it to Kafka.

The two-owner experiment assigns 16 partitions to each owner, with six projection
connections per owner. The one-owner experiment serves all 32 partitions with
twelve connections. Each owner is a Session Runtime process with its normal
additional control and read pools. The operator waits for full partition
coverage before sending traffic. Original images are reused with temporary
source mounts, an idle-workload guard and restoration of the original template
and replica count. Per-stage results and source hashes are in the
[aggregate measurements](ordered-projection-tail-20261118.json).

## What changed

A simple Task update now stores its commit receipt, history, current index and
partition progress in one mutation statement. It takes **six** pg client calls
including BEGIN, transaction timeouts, ownership locking, fresh metadata and
COMMIT. The previous feasibility version took nine calls and the serving baseline
took twelve for this Task shape. A Fork retains its inherited Entry lookup, while
fresh Sessions can use their local records. Updating presentation coverage no
longer plans the complete child Conversation history view for a root identity.

Normal Run completion combines its terminal and ownership writes and verifies
the final native head under the same lock used for handoff. A counted 192-Run
candidate sample produced 101.14 pg client calls per Run. Earlier baseline samples
measured 155–159. The final assistant commit took 14–15 calls and terminal
projection took 25, down from 24 and 31 respectively in the measured baseline.
The number of live-text commits can vary with timing. The measurements include
transaction and timeout commands, and exclude the separate Worker/API pools.

## Validation

All 217 tests in the final eleven-suite PG and input/history regression run
passed. An earlier race test paused on the old statement shape and timed out.
It now pauses after the combined terminal statement while the transaction still
holds its locks, and checks the actual blocked backend and final readiness.
The tests cover duplicate/lost commit replies, changed content at an
existing sequence, old-writer closure, Stop races, child history, retained
attachments, Forks, partial-live rollback and native Storage conformance.
An explicit competing-lock test waits for a real PG blocker before releasing
the preceding transaction. A conflicting record ID must roll back both the
native receipt and any included partition progress. Replaying the original
bytes at a later Kafka position can safely advance the cursor.

The live fault suite passed provider failure, provider disconnect, Worker
handoff, Projector restart, Stop with a late Tool reply, and loss of an
original Tool result. After restart, all 32 idle partitions served initial
snapshots. The fixture uses simulated model responses and physical Tool
invocation counts. Host performance and full measurements are discussed below.

The final deployment template matched the original, with one Ready Session
Runtime, no temporary Projector ConfigMaps and 37 Ready running Pods.
Verification preserved the two existing development machines and all 393 sampled
Workspace files, as well as the original account and model configuration checks.

## End-to-end results

| Configuration | Two 64-in-flight bursts, completed Runs/s | Client P95 for each burst | 20/s arrivals issued and completed | Client P95 for issued arrivals |
| --- | ---: | ---: | ---: | ---: |
| Serving, one owner (A) | 5.96 / 5.68 | 16.43 / 19.23 s | 200/400 | 11.17 s |
| Serving, one owner (B) | 7.53 / 7.92 | 15.74 / 11.57 s | 400/400 | 1.46 s |
| Candidate, one owner | 8.57 / 9.59 | 10.71 / 8.73 s | 235/400 | 8.19 s |
| Serving, two owners (A) | 7.88 / 22.59 | 16.61 / 4.44 s | 380/400 | 6.84 s |
| Serving, two owners (B) | 7.79 / 8.61 | 14.03 / 13.49 s | 219/400 | 11.32 s |
| Candidate, two owners (A) | 25.01 / 24.64 | 3.37 / 3.50 s | 218/400 | 11.29 s |
| Candidate, two owners (B) | 13.48 / 26.85 | 9.17 / 3.27 s | 241/400 | 11.15 s |
| Candidate, two owners (C) | 27.14 / 27.18 | 3.33 / 2.90 s | 310/400 | 7.42 s |
| Candidate, two owners, generic plans | 9.12 / 10.78 | 11.31 / 9.13 s | 278/400 | 9.74 s |

All 192 Runs completed in every closed burst. In the arrivals case the generator
deliberately drops scheduled work when it cannot launch, including when all
64 slots are occupied. Drops are recorded separately from application failures.
P95 describes observed terminal SSE events. The aggregate retains their counts
and the PG duration distribution when polling wins a race with SSE.

With one owner the candidate improved both bursts in this comparison. Across
the two-owner candidate repetitions the bursts ranged from 13.48 to 27.18
Runs/s with automatic PG plans. Gains vary substantially. The repeated 20/s
workload still exceeded the in-flight budget in every two-owner candidate case.
The serving version even delivered 400/400 with one owner in one repetition,
after managing 200/400 in its first. A repeatable 20/s operating point with low
client tail latency has not been established on this host.

The additional three-Tool scenario completed 64/64 Runs with 192 actual MCP
calls and 256 model requests in each of the matched configurations. Its client
P95 was 6.76 s for the serving implementation and 13.25 s for the candidate.
These two stages occupied different host performance regimes. They establish
execution coverage, not a Tool throughput improvement. The waiting workload
exercised up to 128 simultaneously outstanding model requests in measured
cases; the aggregate distinguishes observed active work from configured slots.

## Host variation

Late in the investigation we sampled the Windows Processor Information counter
and separately timed a fixed four-MiB SHA-256 workload in WSL. For example, two
successive bursts on the *same* two-owner candidate measured 13.48 and 26.85
Runs/s. Median host calibration was 7.81 and 2.37 ms respectively, with Windows
Processor Performance readings of 51 and 164. The simulated provider processed
the same byte counts and response framing. Changes to power, cooling and host
scheduling were outside the experiment. The observations identify a material
variation in host execution speed; they do not establish its cause.

A closer comparison uses the two-owner serving repetition with 8.29/8.21 ms
calibrations and 7.79/8.61 Runs/s. The candidate's 7.81 ms calibration burst
achieved 13.48 Runs/s. For 20/s arrivals, the two-owner serving repetition issued
219/400 with an 8.21 ms calibration; the final candidate issued 310/400 with
an 8.08 ms calibration. Their observed client P95 values were 11.32 and 7.42 s.
Matching by an auxiliary observation after the fact cannot establish a hardware-
independent speedup. Host sampling began after the first four clean comparisons.
The generic-plan observation did not consistently outperform the default and
was confined to a temporary pool. All original runtime settings were restored.

## Scale boundary

An active Agent waiting for a model serves a different workload from an Agent
generating frequent durable commits or Tool operations. For example, 200,000
Agents producing one native commit every 30 seconds would offer about 6,667
commits/s before Tool replies and terminal work. This is a sizing example.
The measured host has one PG instance and two Workers. These results establish
neither a fleet-scale capacity nor an acceptable sustained production latency.

Issue #48 stays open. A subsequent architecture experiment could amortize
transactions across already buffered, adjacent commits from one Session/writer.
It would change when multiple native sequences become visible in PG. Define
barriers for exact input acknowledgement, external Tool dispatch, closure and
live-prefix protection before implementing it. Establish the target arrival rate,
client P95 and permitted backlog on stable compute, and include waiting Agents
and multi-step Tools alongside cold Run bursts.
