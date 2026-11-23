# Gateway-only accounting and user-visible Kafka throughput

## Scope

In the matched candidate the Worker attaches the invocation UUID and native ownership to its actual gateway request. Worker Session commits, Projector writes, replay and normal or forced closure do no financial work. `/usage` counts complete provider execution receipts even if the corresponding Session message is subsequently rejected. A duplicate receipt is idempotent. Customer amounts for new requests remain unconfirmed. Historical statements keep their original decisions. Refer to [ADR-0355](../adr/0355-provider-usage-outside-session-projection.md).

## Metric

**Completed user-visible Kafka records per second** counts a handled Session record once if it contains an assistant message with public text, a complete tool call or a tool result. A record with text and two calls counts once here and three times in the output-item breakdown. The denominator is elapsed Projector time until the last record is handled. Run admission, partial text deltas, argument generation, internal task changes, dispatch confirmations and seals are excluded from the numerator. Kafka traffic, peak lag and final lag remain separate diagnostics. The rate measures completed output. First-token latency and UI responsiveness require separate measurements. The million-Run paced profile specifies its offered load in visible records/s and uses the measured visible fraction to compute the raw producer rate. Reports captured before the per-record counters were added show unavailable visible rates.

The fixed 20-call trace has 41 user-visible records per Run out of 187 Kafka records. Across 1,024 Runs the counts are 1,024 completed texts, 20,480 tool calls and 20,480 results. The mixed workload uses real Kafka and PG with warm simulated dispatch targets. Model responses and Bash output are deterministic. Preloaded trials measure backlog processing. The live-capture and paced trials also include producer pacing.

| Input mode | Native usage updates | Completed visible Kafka records/s | All Kafka records/s |
| --- | --- | ---: | ---: |
| Live Pi capture, including producer waits | Retained | 350.8 | 1,599.9 |
| Backlog, first / last arm | Retained | 625.2 / 620.9 | 2,851.5 / 2,831.9 |
| Same backlog, middle two arms | Omitted | 655.5 / 629.4 | 2,989.7 / 2,870.8 |
| Paced input of 473 visible records/s | Retained | 433.8 | 1,978.7 |

The paced arm peaked at 15,640 raw Kafka records of lag and caught up 8.0 seconds after input stopped. All six arms finished with zero lag and verified 20,480 ordered tool pairs. The backlog measurements describe processing speed with input already available. The paced result matters for predicting whether a running service keeps up. These single-host samples do not establish a fleet limit. The fixture verified scratch PG, Kafka topic/group and Kubernetes resource cleanup.

## Native usage

The fixed Pi SDK updates its latest-only `pi.usage` document inside the commit recording a response. There were 21,504 usage updates across the mixed trace and zero updates that occupied an entire record. The disposable replay keeps the initial document, removes only its updates, preserves commit order and adjusts referenced display ordinals. The serving writer retains the entire original native commit. Under the 20-call load this takes 5,754,880 bytes out of 287,413,711 encoded Kafka value bytes (2.0%). This counts application bytes before broker compression and excludes protocol overhead.

Across the two preloaded trials in each condition, combined throughput was about 623 visible records/s with usage and 642 without it (+3.1%). The two omitted samples differed by 26 records/s. This experiment does not establish a repeatable production gain. A separate replay into real Pi MemoryStorage produced the same 41 visible records per Run in both cases. The complete trace restored accumulated model totals. The stripped trace restored empty model and tool usage maps. Keep the native writes intact.

## Projection isolation

The separate 256-Run comparison uses the same 20-tool per-record PG entry point as the [previous accounting measurement](candidate-model-settlement-20261123.md). There were 47,616 native commits, 10,496 visible records and 22,016 ordinary PG projection calls in each sample. The gateway had already registered all 5,376 model invocations. Instrumentation found zero model accounting calls or execution receipts in the projection path. Previously the same workload made 48,896 PG round trips. The isolated PG timings exclude Kafka and routing. The samples took 19.63 and 13.47 seconds, or 535 and 779 completed visible records/s across the PG leg.

The RF3 transport suite passed 40 checks, including the delegated browser. The targeted accounting, Session, Worker and executor run passed 26 checks. Twelve native-trace and recovery-policy checks passed. An existing native commit with historical financial metadata replayed with its original fingerprint and produced no additional financial event. The provider-usage assertion read 240 tokens before projection and 240 again after writer closure, stale output and duplicate gateway delivery.

## Reproduce

```sh
PI_CLOUD_RUNTIME_LOAD_CHECK=1 PI_CLOUD_FANOUT_SUITE=grants-million-day PI_CLOUD_COMPARE_USAGE_WIRE=1 \
  node --import tsx scripts/experiments/minimal-session-storage/run-cpu-placement.mjs /tmp/new-usage-wire-study workspace-fanout
node scripts/experiments/minimal-session-storage/summarize-tool-loop.mjs /tmp/new-usage-wire-study
node --import tsx scripts/experiments/minimal-session-storage/model-settlement-rate.mjs 8
node --import tsx --test scripts/experiments/minimal-session-storage/native-tool-loop.test.mjs
```
