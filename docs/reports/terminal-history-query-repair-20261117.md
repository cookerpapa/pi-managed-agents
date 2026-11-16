# Terminal history query repair — November 17, 2026

Deployed as `settlement-keyed-20261117-r1`, application source
`d0378ee423a9afe36164e895bbaca9efca4e362b`. This implements the bounded SQL slice
from the [latency diagnosis](run-latency-diagnosis-20261116.md).
[Aggregate evidence](terminal-history-query-repair-20261117.json) contains the
plan comparison, load results and recovery/preservation checks without identities
or conversation content.

## Change and preserved behavior

Root history headers now read `runs` after identifying the Session kind. Native
child headers retain the existing derived conversation view. After freezing the
root, `settleChildHistory()` enumerates the specified Run's actual child identities;
an empty family skips the expensive live-history view entirely. Families with
children still use its existing terminal/dirty-history rules, bounded to those
children. The redundant preliminary retained-history lookup was removed; the
existing check for a retained prefix from another owner remains.

This changes neither PG ownership nor native task state. Normal completion and
forced closure both retain their atomic terminal/history transaction, pruning
and shared-prefix protection. No Pi patch, schema/index change, second cache,
queue, receipt, new permission check or compatibility path was introduced.

The normal-finish regression counts 28 statements, versus the prior 27, because
the Session-kind lookup is explicit. There is still one locked Run authority read;
the additional direct Run read obtains the immutable presentation header. The
benefit is avoiding expensive planning, not minimizing SQL statement count.

## Source verification and read-only plan comparison

47 distinct relevant tests passed, including three new PostgreSQL cases for root
plan dependencies/header preservation, native child settlement and pruning/
cancelled-input/compaction behavior. Existing tests cover Fork/source deletion,
child histories, pagination, lost completion returns and forced closure. Runtime
and control-plane TypeScript, targeted formatting and documentation checks passed.

One old assertion counted every direct `runs` read as an authority read. It now
distinguishes the header read from the single locking decision. Test-only access
to an untyped history table was also corrected to use the existing raw-SQL style;
the three new cases were rerun successfully. No runtime constraint was weakened.

A paired read-only EXPLAIN used the same existing completed root, with a five-second
statement budget and no global planner tuning:

| Statement | Planning | Execution | Buffer hits |
| --- | ---: | ---: | ---: |
| Previous root-header query | 52.73 ms | 2.95 ms | 14 |
| Direct root-header query | 0.40 ms | 0.23 ms | 6 |
| Previous family selection, no children | 48.37 ms | 4.05 ms | 32 |
| Actual child-identity lookup, no children | 0.11 ms | 0.02 ms | 2 |
| Previous header, repeated | 30.50 ms | 2.57 ms | 11 |
| Direct header, repeated | 0.49 ms | 0.17 ms | 6 |

The two header queries returned the same root, and both family paths found no
pending child work. These are individual query measurements, not entire Run
latency. Large native child-state queries still retain their existing complexity.

## Published workload comparison

All cases ran serially on the same single-host deployment. Each text case had
64 concurrent requests, 192 cold single-response Runs, 4,096 requested output
bytes, no Tool calls and zero configured simulated first-byte/chunk delay.
The before case used the previous serving image; both after cases used the new
release. There were no injected application hooks or paid model requests.

| Metric | Before | After | After repeat |
| --- | ---: | ---: | ---: |
| Completed Runs | 192/192 | 192/192 | 192/192 |
| Completed Runs/s | 4.92 | 6.29 | 9.04 |
| Mean submission to settlement | 10.42 s | 8.81 s | 6.33 s |
| Settlement p95 | 24.98 s | 16.59 s | 14.12 s |
| Mean provider completion to settlement | 7.78 s | 6.00 s | 2.67 s |
| Maximum observed Kafka lag | 1,550 | 1,481 | 1,412 |

Both after samples had exactly one claim for every Run and no owned
`session.projection.slow_or_failed` records above the one-second threshold.
There is still substantial downstream waiting. The earlier release had also
occasionally reached 9.49/s, so this comparison does not establish a stable new
capacity ceiling or prove all saturation causes fixed. Settlement uses the
recorded Run timestamp, not a separate database COMMIT timestamp.

The live child/history case completed four parents and eight child model requests,
with two subagent Tool results per parent and all four history reads successful.
Six fault profiles met their expected outcomes: original-Run Worker handoff,
Projector restart with preserved writers, Stop with ignored late replies,
lost-reply recovery without repeated test effects, and the existing synthetic
rate-limit/disconnect failure plus subsequent-request recovery contracts.

## Release and preservation

The maintained plan built one shared application image and reused thirteen
artifacts, including the unchanged Worker and Guest. Scanning used the existing
local vulnerability databases. The existing coordinated application drain still
applied; this is not a new zero-interruption publication scope. No native compute,
Workspace storage or schema maintenance ran.

All five evaluation invocations confirmed owned-resource cleanup and restored
model/provider configuration. Original accounts/configuration/credentials,
393 checked Workspace files and two development-machine intents were preserved.
The original machines remain ready with running intent. No real-provider calls,
foreign-model calls or charges occurred.

## Kafka initialization remains separate

The Kafka client remains pinned to 1.10.1. After-release writer preparation still
averaged 0.83/1.33 seconds in the two text cases. The SQL repair does not remove
that initialization cost or establish a fix for the prior diagnostic allocator
abort.

The [Confluent client documentation](https://docs.confluent.io/kafka-clients/javascript/current/overview.html)
confirms shared background-thread limits. The reviewed
[1.11.0 release](https://github.com/confluentinc/confluent-kafka-javascript/releases/tag/v1.11.0)
changes connection retry behavior and updates librdkafka; its
[2.16.0 notes](https://github.com/confluentinc/librdkafka/releases/tag/v2.16.0)
include client-destruction fixes for specific concurrent APIs. These do not
identify the observed allocator abort or demonstrate a startup-latency fix.
No speculative driver upgrade, producer sharing, extra process or thread-pool
tuning was folded into this SQL change. Client/process ownership changes require
the separate assessment and architecture discussion recorded in the backlog.
