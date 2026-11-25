# Shared Session history on the #52 storage candidate

The Session API can create a peer at a sealed Turn, page its inherited messages, resolve navigation targets, serve its context estimate and reconnect to the live SSE tail. A delegated branch freezes the prepared model request's Entry boundary and publication position. An independent first Turn can be queued in the candidate database. Independent Worker placement is a subsequent milestone. [Decision](../adr/0362-flat-session-fork-segments.md) · [Reviewed semantics](../research/session-fork-design-review-20261125.md).

## What the branch owns

The public Session has a stable address. Its writable Flow owns new Entries. Ordered rows in `flow_segments` select the originating Flow, native Conversation, Entry interval, publication ceiling and visible Turn boundary. Forking inside an inherited segment clips that segment and omits later ones. Fresh delegation creates an empty history and its first input. A named retry resolves the original child. A new request with conflicting creation arguments fails. The storage admission uses the active caller's existing Workspace grant.

When the Worker offers the subagent tool, the model request records a small preparation marker on the existing native Run document. The response records the matching generation, call IDs and Entry cutoff only if the model asks for branch delegation. The marker survives handoff, so a reserved lower ID committed after the original model request stays invisible. This adds a native document commit to model requests with delegation enabled. The Bash throughput trace below measures an ordinary tool loadout.

The selector visits the latest visible Turns first, and retrieves Entries within the requested page's record/byte budget. The public cursor also identifies the writable Flow so a prune invalidates earlier cursors. Cold import starts at the latest visible compaction and includes its retained suffix. The destination receives its own Inbox and ID allocation floor. The historical agent configuration is resolved at the selected Entry. A Web Fork freezes the current sandbox observation at creation. A delegated Session inherits that observation only when its granted compute identity and working directory match the parent.

Closing the source window releases its outgoing directory. A restartable collector visits up to 512 immutable message coordinates per transaction and preserves incoming ranges. Unreachable Turn headers are collected beyond the last protected mailbox position. Source messages and original human-upload references retain their existing object bytes. The candidate object collector checks reachability under its existing inventory lock. Forest operations and each GC batch currently serialize against new Forks within one tenant. Reading a page and appending ordinary messages do not take this tenant lock. Deleted creation receipts keep their original identities for retries.

## Measurements

Disposable PostgreSQL 17.6, two CPUs, 768 MiB and one local client machine. Twenty warm, interleaved measurements per layout and size after one warm-up. Both layouts had the same 102,400 Entries and 800 Turns per size. The reference implementation was the previous shared directory at `dde24e4d`. Each read fetched thirty Entries through the storage adapter. Sizes below include serialized message envelopes. [Paired benchmark](../../scripts/experiments/minimal-session-storage/measure-segment-forks.mjs).

| Serialized history | Prior first Fork p50 | Segment first Fork p50 / p95 | Prior page p50 | Segment page p50 |
|---|---:|---:|---:|---:|
| 26 MiB | 247 ms | 8.0 / 9.1 ms | 45.0 ms | 6.0 ms |
| 116 MiB | 283 ms | 8.0 / 10.4 ms | 63.6 ms | 6.0 ms |
| 1.02 GiB | 241 ms | 8.0 / 9.7 ms | 35.3 ms | 8.9 ms |

On the largest fixture the single PG call used by the Web Fork transaction measured 3.96 ms p50 and 5.15 ms p95. HTTP authentication, public Session creation and browser rendering are outside that timing. Creating 1,000 named child Sessions concurrently through a ten-connection pool took 1.65 s. PG recorded 1,000 initial Turns, 1,000 ancestor references and zero copied messages. End-to-end p95 for these simultaneously submitted registrations was 1.57 s, including pool queueing. One real Pi cold open after compaction took 24 ms and produced 13 model-context messages. At depths 1, 4 and 16, subsequent 70-Entry pages took 10, 8 and 7 ms in individual probes. A new branch from the deepest Session back into the root retained only the selected root prefix.

Four warmed rounds of a serial writer appending eight messages per accepted PG call measured 2,001–2,051 rows/s before fan-out and 1,928–2,018 after fan-out. In an earlier repetition, those ranges were 2,034–2,062 and 1,998–2,110 respectively. These calls excluded Kafka, the model and the browser.

### Mixed Kafka, PG and browser load

The actual Projector consumed one real Pi recording replicated across 64 Runs on 32 RF3 Kafka partitions. Each Run used 20 Bash tools. The sandbox acceptance delay was simulated at 50 ms. Each pass had 11,904 Kafka records, 2,624 user-visible records and 1,280 canonical results. Chrome followed the live Run. In the extra reader arm the public API also paged through a fork of a 1,024-tool history at a target of two older pages/s. Trial order reversed between passes. [Load runner](../../scripts/experiments/minimal-session-storage/write-load.mjs).

| Offered visible records/s | Additional reader | Processed visible records/s, two passes | Peak sampled visible Kafka lag | Drain after input ended |
|---:|---|---|---|---|
| 475 | None | 475 / 476 | 609 / 175 | 1 / 0 ms |
| 475 | Fork pagination | 474 / 475 | 150 / 84 | 8 / 0 ms |
| 1,200 | None | 768 / 708 | 1,114 / 1,304 | 1.23 / 1.52 s |
| 1,200 | Fork pagination | 741 / 619 | 1,027 / 1,452 | 1.36 / 1.93 s |

All passes drained to the broker high watermarks. At 475 offered, the extra fork pages averaged 22–23 ms. At 1,200 offered they averaged 22–29 ms. The higher rate exceeded the measured service capacity. The two arms measure the compound effects of concurrent reads, client scheduling and projection on a single host. The ten-connection Fork registration test and these short load windows do not measure production fleet capacity.

## Reproduction and release boundary

Run these in sequence on the candidate checkout. The first command can take an optional checkout of `dde24e4d` with its dependencies available to reproduce the paired old-directory measurements.

```sh
export TSX_TSCONFIG_PATH="$PWD/packages/control-plane/tsconfig.json"
node --import tsx scripts/experiments/minimal-session-storage/measure-segment-forks.mjs
npm run build --workspace @pi-cloud/web-ui
node --import tsx scripts/experiments/minimal-session-storage/write-load.mjs --segments
npm run test:transport
node scripts/run-with-test-postgres.mjs -- node_modules/.bin/vitest run packages/pi-session-postgres/test/durable-storage-conformance.test.ts
```

The RF3/PG transport suite passed 44 checks in 26 files. The native storage conformance and attachment store suites passed 90 checks in a separate disposable PG. The focused Session API and real-Pi delegation tests were also rerun after refining sealed Turn selection. They cover exact source identity, an older native ID arriving on either side of the model request, retained files and images, stale cursors, recovery of the original named intent, subtree deletion and a Fork racing with GC.

The storage candidate is opt-in. The live default still places native children with their parent. Giving children their own Worker lease, the corresponding parent/child outcome routing, and stop/drain of a running management subtree belong to the scheduling milestone. The current storage-level subtree deletion requires quiescence and withdraws queued, unclaimed inputs. The matched release also needs the same attachment reachability schema on the reader, projection and reaper. Full provider-side and multi-host acceptance remain release gates.
