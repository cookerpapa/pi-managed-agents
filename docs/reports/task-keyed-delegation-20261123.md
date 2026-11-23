# One native delegation task, one child Run

In the #52 candidate a new Worker can attach a pending direct delegation before
or after its child Run has been registered. The public child UUID is derived
from the tenant, physical Session, root Run and native task ID. The child's
source is unique within the root in PG. A new user instruction can create a
separate child even if the requested work has identical text.

Each Tool wait still has a transport operation ID and a Worker boot mailbox.
On handoff the successor publishes Pi's task-owned recovery document to attach
a new waiter. The recovered task supplies the validated arguments and the call
ID in the original assistant entry. The frozen `pi-cloud.run` document supplies
the parent history cutoff. No original random mailbox ID or additional field
is written into the seal. The execution-side verifier reads the matched seal
and the indexed assistant entry. The hot Bash projection uses its existing path.

## Failure boundaries exercised

| Fault or operation | Evidence |
| --- | --- |
| Worker loses its pending delegation before a child row exists | The replacement derives the same child ID from the PG handoff. A second replacement can reopen the same waiting task before placement. The final parent receives one native tool result. |
| A child Run is registered before the Worker is lost | Reattachment returns that child. Concurrent admissions converge, changed task input is rejected, and a conflicting independent SQL insert is rejected by the unique source constraint. |
| Only the Projector restarts | The cold consumer starts at the candidate replay floor. The dispatcher waits for its partition's initial replay to finish and returns the result to the surviving Worker's original correlation ID. |
| Stop is present in the prefix | An aborted native task withdraws the pending delegation before the partition can dispatch it. The tool produces no new child or reply. |
| The parent compacts after freezing the branch point | The cold reader loads the prefix needed at that cutoff. The new child sees the earlier context. Subsequent parent text and the later summary stay outside that child. |
| The model requests two children together | Two native Pi tool tasks create distinct child Runs. The serving controller and the Kafka mailbox return their results. The children also exercise separate grants, sibling input and later continuation. |

The tests use deterministic model responses, real Pi Harness instances and
disposable PG. RF3 tests use actual Session and reply topics. The handoff test
controls the interruption at the native wait and exercises logical Worker boots.
The existing delegated-handoff test also kills a Worker process during child
work. The source experiment verifies native parent/child settlement separately
from the mixed serving test which projects child outcomes. Neither calls a paid
provider.

## Matched 20-tool throughput

Each arm ran 1,024 root Runs, 20,480 ordinary tool calls, 191,488 Session records and 44,032 complete history rows. The 32-partition Projector had 12 PG connections. The receiver simulated a 100 ms acceptance delay. Final counts and offsets matched in all six arms. The driver confirmed cleanup of the scratch database, Kafka resources, Pods and Service.

| Load | Input records/s | Completed Run/s | Peak Kafka lag | Drain after input stopped |
| --- | ---: | ---: | ---: | ---: |
| Live Pi capture | 1,972 | 10.6 | 1,020 | 0.00 s |
| Daily average, first | 2,158 | 11.4 | 4,965 | 0.69 s |
| 2× daily average | 4,310 | 12.4 | 111,142 | 38.00 s |
| 3× daily average | 6,473 | 17.4 | 79,660 | 29.14 s |
| 1.5× daily average | 3,242 | 13.3 | 63,000 | 18.03 s |
| Daily average, repeat | 2,162 | 10.2 | 21,593 | 11.70 s |

At the daily target the two bursts drained in 0.69 s and 11.70 s respectively. Both finished with zero lag. The 2× burst accumulated 111,142 records and took 38 s to drain. This host cannot promise sustained peak capacity from these runs. The 2× result was slower than the previous candidate sample (20.9 Run/s).

Passive Windows CPU performance counters covered the later arms. In short windows of the 1.5× arm, similar input of 3,249–3,261 records/s corresponded to processing of 1,691–1,925 records/s at performance readings of 59–64%, and 4,039 records/s at 171%. These observations support host performance variation as a contributor. The 2× arm did not have continuous host counter coverage, so its slowdown cannot be assigned to a specific cause. The [previous matched comparisons](candidate-release-gate-20261122.md) also observed large variation.

### Parent revision control

Repeated the 2× offer twice at `bb918b18` and twice with this change, serially on the same host. Each series used the standard live capture followed by two replays. The temporary driver selected only the 2× replays. Each replay verified the same workload counts and final offsets. Model text and tool results were deterministic, with fresh per-capture identities.

| Build | Actual input records/s | Completed Run/s | Drain after input stopped |
| --- | ---: | ---: | ---: |
| Parent, first replay | 4,320 | 20.3 | 6.03 s |
| Parent, second replay | 1,825 | 9.7 | 0.39 s |
| Candidate, first replay | 4,320 | 16.2 | 18.92 s |
| Candidate, second replay | 4,308 | 12.0 | 40.65 s |

The parent’s second producer fell short of the offer. At comparable input near 4,330 records/s, observed windows on the parent processed 3,979–4,253 records/s at host performance readings of 161–173%. Candidate windows at 163–170% processed 3,754–4,018 records/s. At 57–64% the candidate processed 1,628–1,751 records/s. Sampling used six-second windows around periodic CPU readings. This accounts for a substantial part of the spread. These sequential samples cannot isolate a smaller software effect or establish a sustained service capacity.

### Retention boundary

The PG-only Worker handoff above starts from a completed seal. An original Worker still waiting after a Projector restart needs its initial Kafka destination. The candidate protects that suffix by holding the projection checkpoint. A controlled bound check moves Kafka's simulated retention floor one record beyond the pin while a child remains active and the durable partition head is already ahead. Recovery refuses this partition. A legitimate long-running delegation can therefore exceed the default seven-day retention window even when PG was keeping up. There is no per-token PG write or indefinite-retention guarantee in this design.

## Reproduction

```sh
PI_CLOUD_DELEGATED_BROWSER=1 npm run test:transport
```

The 20-tool mixed benchmark uses the documented
[workspace-fanout profile](../../scripts/experiments/minimal-session-storage/README.md#one-million-runs-per-day-at-20-tools-per-run).
It measures complete message projection and simulated Bash handoffs on one
machine. The delegated tests above measure correctness on the separate control
path. Whole-service cutover and external-tool recovery remain separate gates.

[Decision](../adr/0353-native-task-delegation-identity.md) · [Original gap](direct-delegation-handoff-20261123.md)
