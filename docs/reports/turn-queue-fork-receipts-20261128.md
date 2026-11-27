# Direct Turn queue, input receipts and first-Turn Fork cache

The independent Session candidate implements [ADR-0376](../adr/0376-direct-turn-claims-and-fork-locality.md). The serving Session API, Projector and Worker use matched segmented installation version 2. The fleet still grants one lease per Session. The database queue is the partial FIFO index on queued Turns with a non-null ready_at. This delivery is scoped to the isolated installation.

## Admission and handoff

A new head receives its database creation timestamp as its ready time. A queued successor behind unfinished work stays unready. Admission, cancellation, ordered writer handoff and promotion preserve per-Session ordering. An ordinary lookup and an exact hint both validate the current writable Flow, head eligibility, old-lease release and capacity under PG locks. A recovery requeues the same Turn. Claiming clears its readiness as part of the state change. One listener consumes committed notifications and periodically directs a single Worker to check for missed work.

A sent message retains its ID, sender and requested mode. The PG mailbox owns editing, order, withdrawal and transfers. A normal seal examines unused mail under the Session lock. The earliest unused input opens a new successor when needed. Withdrawing its opener promotes the next pending input and updates the Turn header. A pending input that Pi actually adopted stays on the original Turn. Stop returns unread human mail and holds agent mail for explicit continuation. Recovery reconciles against the old writer’s sealed prefix. [Queue and closure SQL](../../scripts/experiments/minimal-session-storage/turn-ready.sql) · [Input tests](../../scripts/experiments/minimal-session-storage/segment-inputs.integration.spec.mjs).

The Web client displays Delivered on an accepted, outstanding human input. Read appears on the authenticated sender’s latest canonical placement. The existing Session SSE connection carries a post-commit input.placed observation. Only a placement with a matching PG history Entry and committed mailbox receipt is presented. Connected viewers can receive the ordinary human transcript; their latest read indicator is resolved from their own sender-scoped queue snapshot. Replaying a placement uses the same input ID. Focus, reload and stream reconnect recover the display from PG. The receipt does not establish provider processing or billing. [Session client contract](../SESSION_CLIENT.md).

## Cache selection and isolation

Workers advertise available slots and bounded expiring cache descriptors over the directed wake socket. Exact available RAM holders precede SSD holders. Ordinary candidates are sampled in pairs, with free slots as the primary criterion and a live large-cache offer as a tie-break. A single available Worker can take the job immediately. Hints carry tenant, destination Session and Flow, Turn, source version and source Flow. No deadline delays an eligible Turn. A concurrent FIFO claim can win before the exact hint; all choices remain advisory.

A first Turn can read immutable source entries from a checked Worker cache. The destination’s frozen PG directory bounds the source Flow, conversation, Entry interval and publication ceiling. The reader obtains canonical Entry coordinates under repeatable read, checks them individually against a PG-confirmed source head and honors native compaction boundaries. An intermediate Fork can supply inherited entries where its own frozen directory establishes their provenance. Ambiguous coordinates and incomplete coverage fall back to the existing cold reader for the required context. Metadata, configuration and environment still come from the destination. SQLite can read a clipped range without decoding the entire local suffix, and verifies stored row checksums. Deep or multi-source Forks can still miss when a usable source version cannot be identified on the chosen Worker. [Fork reader](../../scripts/experiments/minimal-session-storage/segment-history-reader.mjs).

In a 104.13 MiB inline-image fixture the ordinary cold continuation returned 109,191,143 Entry bytes from PG. A verified local continuation returned zero PG Entry bytes. The RAM trial took 7 ms versus 539 ms cold; the SSD trial read the same prefix on disk in 532 ms versus 506 ms from PG. A busy cached owner fell back to the available Worker in 23 ms in that trial. These are single-host samples through model-context construction. Web Fork and direct branch delegation both verified that a cut at marker 3 yielded markers 1, 2, 3, X after submission, excluding 4, 5 and 6. Their first-Turn warm and cold contexts had identical digests; the warm reads returned zero PG Entry-body bytes. Tests also covered a clipped SSD range, a compacted source requiring cold recovery, a nested range, eviction, expiry and a disconnected scheduler. [Tests](../../scripts/experiments/minimal-session-storage/segment-warm-history.integration.spec.mjs).

## Matched installation

Stop old admissions, drain Sessions, retire the old Worker boots and stop old serving roles before invoking the version 2 migrator with the existing database and transport configuration. Under the installer advisory lock the queue switch takes exclusive locks on the segmented Turn and Session tables. The migration rejects unreleased leases and live old management connections, captures the eligible old head set, backfills ready_at and checks the new head IDs against it. Removing the old queue and updating the installation version commit atomically. Mismatched roles fail their installation check. After commit, start matched roles. A failed migration can be retried on version 1. Reverting after commit requires a matched reverse migration or a separately prepared version 1 installation; pointing an old Worker at version 2 cannot restore the old queue.

The isolated upgrade check used the previous migrator image and two pending Turns. It failed closed with an active boot, preserved the Session, both Turns and the stable pending input through the drained upgrade, and made only the first Turn eligible. Browser acceptance used the real model gateway and Chrome. It observed Delivered before the first Worker was started, two canonical placement events, a single latest Read after reload, and successful cold continuation on a different Worker. Both Turns produced streamed text and a terminal event.

## Mixed visible message throughput

The same captured fixed-Pi trace was used for both queue implementations. Each of 48 fresh Sessions emitted 20 calls, 20 formal results and one final answer, yielding 1,968 completed visible Kafka records. The synthetic Worker used its normal PG claim, input adoption, fenced Kafka producer and ordered projection. A test-only entry supplied the tape with a 50 ms delay per simulated result. Model inference, Guest execution and external Tool routing were outside this comparison. A separate ordinary model Stop check followed every arm. Each trial used four RF3 partitions, a two-CPU PG, 64 Worker slots and four offered Turns/s (164 visible messages/s). Elapsed time includes admission, input pacing and final drain.

| Image in execution order | Completed visible messages/s | Peak sampled raw Kafka lag | Drain after last admission | Terminal-to-PG p99 |
| --- | ---: | ---: | ---: | ---: |
| Previous | 115.1 | 261 | 5.32 s | 652 ms |
| Candidate | 140.7 | 50 | 2.22 s | 60 ms |
| Candidate, repeat | 140.6 | 41 | 2.23 s | 45 ms |
| Previous, repeat | 127.5 | 456 | 3.66 s | 998 ms |

All four arms checked the same tape digest, ordered IDs for every call and result, all 48 final answers, all leases sealed and zero final Kafka lag. The reverse pair measured approximately 274,112 WAL bytes/Turn on the candidate and 279,455 on the previous installation over the full pipeline. These short, co-located trials include queueing and projection. Production fleet capacity and isolated per-index write cost require separate measurements.

A separate alternating claim-only comparison ran the previous and new Worker against their respective schemas on the same disposable PG. Five samples per size gave medians of 17.69→15.42 ms for one Session, 19.50→17.91 ms for eight and 28.40→27.15 ms for 32. Ordinary successful claim paths went from 12 to 11 SQL calls including transaction control and post-commit bootstrap. The long-history selector used the ready FIFO and predecessor indexes and returned eight of 4,105 ready Sessions in 0.58 ms of PG execution in this sample. These eligibility and source-cache measurements cover the candidate gate.

Validation included the mixed workload, browser placement and refresh on the real provider, the schema upgrade guard, workspace-wide TypeScript checks, router and disk unit tests, Web/client regressions and the RF3/PG transport suite. The original browser take-back test expected the older Sending label; it passed on rerun with the new Delivered contract, including repair of a lost steer notification and return to the draft.

## Reproduce

Run the following sequentially from the candidate checkout with the maintained role images and the repository’s test dependencies.

```sh
export TSX_TSCONFIG_PATH="$PWD/packages/control-plane/tsconfig.json"
node --test scripts/experiments/minimal-session-storage/{wake-channel,segment-disk-history}.test.mjs
PMA_WARM_LARGE=1 node scripts/run-with-test-postgres.mjs -- node_modules/.bin/vitest run --maxWorkers=1 scripts/experiments/minimal-session-storage/segment-warm-history.integration.spec.mjs
PMA_SEGMENT_BROWSER=1 npm run test:transport
PI_CLOUD_CHECK_QUEUE_UPGRADE_FROM="$PREVIOUS_TAG" PI_CLOUD_CHECK_TURNS=16 node --import tsx scripts/run-independent-session-serving-check.mjs "$IMAGE_TAG" --load
PI_CLOUD_CHECK_MIXED_TAPE="$TAPE" PI_CLOUD_CHECK_ARRIVALS_PER_SECOND=4 PI_CLOUD_CHECK_TURNS=48 node --import tsx scripts/run-independent-session-serving-check.mjs "$IMAGE_TAG" --load --mixed
```

The optional tape file contains frames and requestId from nativeBashTrace with callsPerRun=20 and call IDs call-0 through call-19. Without that file the fixture captures a trace locally. The real browser test uses the configured CLIProxyAPI URL and a read-only credential mount as described in the [serving gate](independent-session-serving-20261127.md).
