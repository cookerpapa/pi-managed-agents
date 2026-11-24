# Bash intent, canonical settlement and history coverage on one primary

This #52 experiment adds an opt-in projection beside the combined Run-row implementation. A native Bash proposal alone creates no execution record. Pi's admitted execute-task position contributes one internal `pma.bash-intent` row to `messages`. A later canonical tool result for the same tenant, physical Session, Run, Conversation, task and assistant Entry resolves it. An unknown result also resolves it. The row stays in history until that history is collected. The model receives native entries only. [Storage decision](../adr/0360-bash-intent-log-study.md) and [measurements](bash-intent-log-20261125.json).

## Information retained

The intent stores native commit/ordinal, task, original assistant Entry, model call ID, original operation UUID, dispatch offset, optional resource span and start time. The original boot topic and its read floor protect a healthy receiver. The sealed Worker receives task, Conversation and call identity. Its Bash takeover path reads no predecessor mailbox deadline. Other remote tools retain the existing Run-backed policy.

An optional bounded hint remembers command fingerprints for at most 32 calls from the latest assistant proposal. If the validated Bash command matches, the record stores its argument overrides and history takes the command from the already-selected assistant Entry. A changed command or an unavailable hint stores the validated command in full. There is no extra read round trip at admission. Native JSON with escaped NUL stays text and is decoded by the application.

One suitable root Entry or intent carries the frame's display coverage in the same transaction as its contents. The chosen variant also uses the existing conditional recovery-checkpoint UPDATE as the admission proof when the frame needs both that checkpoint and a covered Entry. Open/close, controls without an eligible Entry, child display and other cloud Run responsibilities retain their existing boundaries. Current Run updates therefore remain nonzero. The read snapshot takes the greater of the fallback coverage and the covered records. It distinguishes native commit positions from SSE event positions.

## Correctness checks

- The authenticated Session API and actual Web page showed a running Bash after refresh, then one settled row after live delivery and another refresh. The two readers returned equivalent snapshots. Forking a sealed root retained its earlier command and result.
- A separate reader found the original reply floor while Bash was pending. A fresh router recovered its immutable dispatch span, rejected a different operation ID and sent Stop to the original binding. A cancellation and result in one commit retained the exact cancellation target for recovery. An execute replay could not use the cancellation receipt to dispatch a new command.
- Two tools proposed in one assistant entry received separate intent coordinates. The second settled first. A mismatched Conversation or operation could not clear the first wait. A different writer, replay and transaction rollback preserved the admitted history. The checkpoint/Entry test verified visibility before commit and after rollback. Only the canonical result retired a wait.
- The [nine Worker fault boundaries](sealed-bash-unknown-20261125.md) also ran against the append layout. Each case preserved one canonical result and the expected physical execution count, including the dropped dispatch, old-mailbox-only reply, result in Session Kafka before PG, repeated Worker loss and explicit Stop. These use independent Worker processes, RF3 Kafka, real PG and a fixed Bash in a disposable container.

## Cost of each change

One trace had 64 Runs, 20 tools per Run, 11,904 raw Kafka records and 2,624 user-visible records. Each arm confirmed 1,280 dispatches and results, every PG partition position, and eventual drain. One Chrome client watched a Run through the product timeline. The simulated receiver acknowledged after 50 ms. The fixed tape offers native results independently of that delay. The primary had two CPU equivalents and 12 projection connections. The rates below measure this pipeline.

| Layout | Run row updates | Message inserts including internal intents | Root Entries carrying coverage | Intents carrying coverage |
|---|---:|---:|---:|---:|
| Previous combined Run row | 9,536 | 2,816 | 0 | 0 |
| Append intent, keep coverage on Run | 9,536 | 4,096 | 0 | 0 |
| Append both, with or without checkpoint fusion | 5,568 | 4,096 | 2,688 | 1,280 |

The intent-only arm still updated the same Run for the display event, so removing the mutable wait alone saved zero Run updates. Moving both removed 3,968 updates (41.6%). The command was reused in all 1,280 intents on this tape. A covered intent averaged 312 bytes by `pg_column_size` on this workload. This measures the tuple, excluding indexes and WAL. At the 475 target the previous layout generated 14.23–14.69 MiB WAL and the folded layout generated 11.44–11.51 MiB.

| Offered visible/s | Layout | Aggregate visible/s | Result-to-viewport p99 in ms, two passes | Peak sampled visible Kafka lag, two passes |
|---:|---|---:|---|---|
| 475 | Combined | 473 | 378 / 188 | 120 / 166 |
| 475 | Intent only | 474 | 225 / 259 | 102 / 101 |
| 475 | Covered record | 476 | 315 / 151 | 140 / 349 |
| 475 | Covered record with checkpoint | 476 | 225 / 193 | 146 / 86 |
| 1,200 | Combined | 423 | 2,923 / 3,582 | 1,918 / 1,876 |
| 1,200 | Intent only | 364 | 3,038 / 2,681 | 2,136 / 2,308 |
| 1,200 | Covered record | 392 | 4,794 / 1,386 | 2,107 / 1,113 |
| 1,200 | Covered record with checkpoint | 397 | 4,853 / 1,305 | 2,098 / 1,765 |

Rates divide total visible records by total processing time. A consumer sampled broker high watermarks every ~400 ms. Browser p99 is the maximum of 20 sampled results per trial, measured from offering the result to Kafka to an animation frame with its text in the viewport. Each pair used reversed trial order. In the 1,200 folded arms the backlog drained 6.16 / 2.48 seconds after input stopped. This finite single-host run does not establish sustained capacity at the higher rate.

An independent confirmation compared the previous and folded writers, with and without one additional reader targeting two older pages/s on a 1,024-tool active history. It used another identical-within-suite tape and two passes per arm:

| Target | Older-page reader | Combined visible/s | Folded visible/s | Combined viewport p99, ms | Folded viewport p99, ms |
|---:|---|---:|---:|---|---|
| 475 | Off | 476 | 475 | 1,058 / 121 | 156 / 1,164 |
| 475 | On | 475 | 475 | 157 / 147 | 157 / 155 |
| 1,200 | Off | 674 | 720 | 1,303 / 1,207 | 1,591 / 1,574 |
| 1,200 | On | 657 | 607 | 1,528 / 1,575 | 3,269 / 1,580 |

On the 475-target paging runs the older GET averaged 20–22 ms with combined and 26–27 ms with folded. The 1,200-target confirmation changed direction with the presence of a reader. Both layouts accumulated backlog. Root recovery still advances independently: all arms counted 2,816 snapshot advances and 2,688 first positions requiring replay.

## Isolating the database cost

The PG-only replay used 6,784 captured parameterized transactions per arm. It replayed the same rows in partition order in short rotating blocks on the same primary. SQL inputs were prepared outside the timed blocks. Across the final comparison, PG-leg visible records/s were 768 for combined, 802 for intent only, 838 for covered records and 854 with the existing checkpoint folded into the covered Entry write. The last case used 6.12 PG CPU-seconds versus 6.97 for combined, and fused the checkpoint boundary on 2,688 calls. A SQL-language covered-entry helper measured 695 and removing the coverage index measured 820. This localized substantial work to the body-write implementation. Fewer row updates alone did not predict end-to-end capacity.

## Cost of reconstructing the pending set

With 1,024 calls and one outstanding result, an early reader assembled the whole Run's pending set outside the requested page. A recent proven-empty checkpoint cut this work, but may be unavailable. The bounded reader walks native positions and intent positions together in indexed page order, filters settled tasks in that range, and asks for a pending body only when selected. Mean PG CPU in the last paired study was 8.75 ms/page for both latest and older pages using the Run's pending map. The full-set reconstruction took 195.62 ms for latest and 110.00 ms for older. With an available recent empty checkpoint it took 11.88 and 12.81 ms. Reading within the requested indexed window took 9.38 and 9.38 ms without that checkpoint. Each pair verified equal complete HTTP pages. The checkpoint arm used a controlled checkpoint at the last fully settled operation.

The physical source pointer, lane and Run checks still apply. The long-read study compared identical latest and older public pages on the same primary. See the counters in the [structured results](bash-intent-log-20261125.json).

## Decision

Keep this as a measured candidate. The pending Bash read, callback retention floor and implicit settlement no longer require a mutable Bash map on the root Run. A covered intent costs an extra immutable row. Existing recovery state and other tool kinds still have their own write costs. The bounded page reader is important for long active Runs. The steady 475-target result, lower WAL and controlled PG improvement support further matched testing. The high-target browser and lag measurements do not justify a serving cutover or a throughput guarantee. The study did not measure fleet-scale retention sweeps, very large rewritten commands, or multi-host capacity.

The RF3/PG transport suite passed 43 tests. The final scoped regression also exercised a stale writer at a fused checkpoint and the existing non-Bash cancellation path. Storage, runtime and control-plane type checks passed.

## Reproduce

Run the experiments serially using the candidate checkout and its built Web bundle.

```sh
npm run build --workspace @pi-cloud/web-ui
export TSX_TSCONFIG_PATH=packages/control-plane/tsconfig.json
node --import tsx scripts/experiments/minimal-session-storage/bash-intent-read-study.mjs
node --import tsx scripts/experiments/minimal-session-storage/bash-loss-study.mjs --intent-log
node --import tsx scripts/experiments/minimal-session-storage/write-load.mjs --intent-log
node --import tsx scripts/experiments/minimal-session-storage/write-load.mjs --intent-sql
node --import tsx scripts/experiments/minimal-session-storage/write-load.mjs --intent-log --confirm
npm run test:transport
```
