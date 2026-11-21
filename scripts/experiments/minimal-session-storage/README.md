# Minimal Session lifecycle load test

This experiment measures the proposed Run queue, immutable messages and one current Pi Inbox per Conversation. It runs the actual pinned Pi Harness on two Worker Pods with a deterministic local model, using the maintained RF3 producer and partition-ordered consumer. A separate Projector process inside the Session runtime Pod uses a private topic and a disposable logged PostgreSQL database. Each role is invoked through kubectl exec. Source changes to these experiment files do not replace any application image.

The original storage controls use fresh physical Sessions. The `wake-throughput` suites also run successive roots in the same Sessions. The input driver uses direct SQL and an experiment channel for post-commit confirmations. HTTP/SSE, charging, tool sandboxes and restoration of in-flight native Tasks are additional costs of the maintained runtime. The benchmark's only tool returns a fixed observation. Each worker applies its in-memory native batch after Kafka acknowledgement. Steer and follow-up pass through Pi's actual busy submission and placement. Only the explicitly labelled replay controls read a previously captured plain trace.

## Run serially

### Shared Forks on the message projection

The Fork profiles use the same 200-message emitter and projector as the
complete-message study. They compare the original schema with retained streams
and completed-Run headers, existing forks and concurrent Fork requests.
The index profile measures the additional covering index separately. The
timing includes a committed PostgreSQL Fork and reports wall time to read the
first page separately. Use new private directories and run serially.

```sh
export PI_CLOUD_RUNTIME_LOAD_CHECK=1
node --import tsx scripts/experiments/minimal-session-storage/run-cpu-placement.mjs /private/fork-first fork-rate
node --import tsx scripts/experiments/minimal-session-storage/run-cpu-placement.mjs /private/fork-reverse fork-rate-confirm
node --import tsx scripts/experiments/minimal-session-storage/run-cpu-placement.mjs /private/fork-index fork-rate-index
node scripts/experiments/minimal-session-storage/summarize-complete-messages.mjs /private/fork-summary.json /private/fork-first /private/fork-reverse /private/fork-index
```

### Complete messages per second

The `message-rate` profile feeds the same 256 Runs to the prototype Projector and PG. Each Run contributes 100 recorded assistant tool calls and 100 corresponding tool results with 4 KiB per payload. Run opening, input, final answer and settlement also travel through the normal projection path and are excluded from the message numerator. The test compares synchronous and transaction-local asynchronous commits at fixed offered rates and while draining an already-populated Kafka partition set. Verify ordered content against the emitter's per-Run digest. This profile measures stored history and issues no sandbox commands.

```sh
export PI_CLOUD_RUNTIME_LOAD_CHECK=1
node --import tsx scripts/experiments/minimal-session-storage/run-cpu-placement.mjs /private/message-rate message-rate
node --import tsx scripts/experiments/minimal-session-storage/run-cpu-placement.mjs /private/message-confirm message-rate-confirm
node scripts/experiments/minimal-session-storage/summarize-complete-messages.mjs /private/message-summary.json /private/message-rate /private/message-confirm
```

### Moving recovery and transaction-local commit

For the moving-checkpoint and selective-commit comparison, run the following
serially with `PI_CLOUD_RUNTIME_DIRECTORY` set to the managed development
runtime directory. The performance fixtures use the
existing three brokers and their own database. The recovery command starts its
own disposable PG and three Kafka containers and immediately stops only its
own postmaster. The full benchmark uses the default WAL writer timing. The
isolated fault fixture gives the WAL writer five seconds to make the loss window
observable.

```sh
export PI_CLOUD_RUNTIME_LOAD_CHECK=1
node --import tsx scripts/experiments/minimal-session-storage/run-cpu-placement.mjs /private/moving-first moving-study
node --import tsx scripts/experiments/minimal-session-storage/run-cpu-placement.mjs /private/moving-reverse moving-confirm
node --import tsx scripts/experiments/minimal-session-storage/run-cpu-placement.mjs /private/moving-finish moving-finish
node --import tsx scripts/experiments/minimal-session-storage/run-moving-recovery.mjs /private/moving-recovery.json
node scripts/experiments/minimal-session-storage/summarize-ready-projection.mjs /private/moving-results.json /private/moving-first /private/moving-reverse /private/moving-finish
```

The selective policy permits complete text answers, the copy of an admitted
root input, the initial empty Inbox, the first replay pin and validated sandbox
intents with an original reply reference to use a transaction-local asynchronous commit.
Any control operation which uses the conservative synchronous policy promotes
the whole ready batch. Neither mode buffers sparse arrivals to fill the batch.
The `moving-finish` profile additionally relaxes the prototype's normally
completed Run projection. The crash fixture tests a read held behind replay
after a previously acknowledged completion disappears from PG.

### Synchronous partition-prefix study

Set `PI_CLOUD_RUNTIME_DIRECTORY` to the development runtime directory. These runs use a private PG instance with the serving image, the existing three Kafka brokers, two native Pi Worker processes and a deterministic model. Run the two profiles in order. The second profile repeats the comparison using a fresh recording. The `prefix-smoke` profile provides a shorter check.

```sh
export PI_CLOUD_RUNTIME_LOAD_CHECK=1
node --import tsx scripts/experiments/minimal-session-storage/run-cpu-placement.mjs /private/prefix-first prefix-study
node --import tsx scripts/experiments/minimal-session-storage/run-cpu-placement.mjs /private/prefix-confirm prefix-confirm
node scripts/experiments/minimal-session-storage/summarize-ready-projection.mjs /private/prefix-summary.json /private/prefix-first /private/prefix-confirm
```

Width 0 uses the original prototype SQL. Width 1 co-commits each stable record and the partition checkpoint. Width 16 uses the same mutations and checkpoint, and can combine already-buffered records on the same partition. Tool intents, human confirmations and completion end a prefix. The recovery tests use isolated PG and Kafka through `npm run test:transport`. [Measurements and limits](../../../docs/reports/synchronous-ready-prefix-20261120.md).

### Other storage experiments

Set `PI_CLOUD_RUNTIME_DIRECTORY` to the managed development runtime directory. The operator checks for unrelated active Runs and takes the existing release lock. Use a new private directory per invocation. Development PG must allow creating a disposable database, and all three Kafka brokers plus the Session runtime and two Worker Pods must be ready.

```sh
export PI_CLOUD_RUNTIME_LOAD_CHECK=1
node --import tsx scripts/experiments/minimal-session-storage/run.mjs /private/acceptance smoke
node --import tsx scripts/experiments/minimal-session-storage/run.mjs /private/measured-matrix matrix
node --import tsx scripts/experiments/minimal-session-storage/run.mjs /private/measured-diagnose diagnose
node --import tsx scripts/experiments/minimal-session-storage/run.mjs /private/measured-inbox inbox
PI_CLOUD_SESSION_BENCH_RATES=20,60,90,120,160,220 \
  node --import tsx scripts/experiments/minimal-session-storage/run.mjs /private/measured-capacity capacity
node --import tsx scripts/experiments/minimal-session-storage/run.mjs /private/intake intake
node --import tsx scripts/experiments/minimal-session-storage/run.mjs /private/boundary boundary
node --test scripts/experiments/minimal-session-storage/wake-channel.test.mjs
node --import tsx scripts/experiments/minimal-session-storage/run.mjs /private/wake-pods wake
node --import tsx scripts/experiments/minimal-session-storage/run.mjs /private/wake-fleet wake-fleet
node --import tsx scripts/experiments/minimal-session-storage/run.mjs /private/wake-sizing wake-sizing
node scripts/experiments/minimal-session-storage/summarize-wake.mjs /private/wake-pods /private/wake-fleet /private/wake-summary.json
```

- `smoke` checks the key lifecycle races, replay, exact input acknowledgement, per-Session claim ordering, edit/withdraw and a quiet queue with polling fallback.
- `matrix` measures closed concurrency, independent simultaneous starts, follow-up, steer, a completion-only control and the explicitly delayed 10-second model.
- `diagnose` compares the real Pi loop against the same captured sequence sent serially or pipelined, and the usual PG transaction against a single atomic statement for one-row changes. The terminal-only sink skips message and Inbox persistence and is a diagnostic control.
- `inbox` fills one Conversation's current document at a time and counts actual serialized JSON bytes submitted to PG.
- `capacity` uses fixed arrival times, 30 seconds per rate and a maximum of 384 outstanding requests. It records unissued arrivals when the generator cannot keep up, verifies all issued Runs and reports lag through drainage. The sweep stops on a stage with unissued work or more than eight seconds of drainage.
- `intake` keeps the Worker wake mechanism identical while toggling NOTIFY inside the admission transaction and sampling the database-0 heavyweight lock. It also tests coalesced PG wakes after commit.
- `boundary` repeats 90 and 120/s, compares Projector transaction variants under coalesced wake, and periodically times a 4 MiB host hash to document variation.

For a current full-runtime comparison use the matched diagnostic. The second command adds an independent read-only observation of the PG NOTIFY lock within the actual submission-to-completion interval.

```sh
export PI_CLOUD_PROJECTOR_SQL_DIAGNOSTIC=1
node --import tsx scripts/run-projector-sql-diagnostic.mjs /private/full-control 12 projection-candidate startup-candidate clean
node --import tsx scripts/experiments/minimal-session-storage/full-control.mjs /private/full-control-repeat
node scripts/experiments/minimal-session-storage/summarize.mjs /private /private/summary.json
```

## Directed Worker wake-up experiment

This prototype compares a notification delivered to every Worker with a FIFO of waiting management connections. Admission stays in PostgreSQL. An API process coalesces committed admissions for 5 ms. A runtime process signals waiting Workers over WebSocket. A 200 ms timer signals at most one waiter per tick. A Worker offers again after a jittered timeout, checks on connection and claims at most its actual available slots. The router has no durable task, capacity or lease authority. River's [PG notification and fallback](https://github.com/riverqueue/river/blob/master/client.go) and Temporal's [waiting pollers](https://github.com/temporalio/temporal/blob/main/docs/architecture/matching-service.md) inform the comparison. The existing PG queue and Worker management connection remain the intended production boundaries.

Acceptance covers lost admission, wait and wake signals, reconnect, joining after enqueue, draining, busy Workers, multi-row claim ordering, empty checks, admission-to-claim latency and full Kafka/PG settlement. A separate lightweight socket population measures fanout without representing a thousand Pi processes.

The `wake` mode uses an experimental WebSocket endpoint in the Session runtime Pod and two real Pi processes in different Worker Pods. Two temporary additive network policies cover the randomly allocated port between the selected roles. The driver checks their removal. Production roles and their management protocol are untouched. Stage setup starts the loss-test timer after registration so each fault is injected before the first backup tick. Under the bounded recovery variant, when the waiting set is empty the runtime rotates one connected socket on the same tick. A saturated Worker discards that frame locally.

The `wake-fleet` mode stops the two experimental Pi Workers after the storage smoke checks. The runtime actor then opens 1,000 loopback WebSockets multiplexed over one process with a bounded PG pool. An independent process commits groups of seven isolated Runs and sends real PG notifications at a scheduled 171 groups/s. The broadcast control changes only how many connected clients receive the arrival frame. A lightweight PG update completes each claimed Run. The bootstrap scans of the 1,000 connections are excluded from measured groups. Per-claim SQL counts exclude job inserts, completion updates, the observer and PostgreSQL notification commands. The 30-second directed run is a queue-only sustained sample. [Measurements](../../../docs/reports/directed-worker-wake-20261119.md).

## Successive root Runs and session locality

The throughput candidate uses 32-row claims, 128 root slots per Worker, 5 ms admission coalescing and the 200 ms directed backup wake. Each successor opens a fresh native Harness under a fresh PG claim. Its detached history prefix is usable only with the tenant, physical Session, Conversation and exact version of the predecessor PG settled. Pending inputs stay in the existing Inbox current-value row. Normal FIFO admission remains open while a warm connection is preferred. A bounded cache and advisory connection directory retain neither Tasks nor model credentials.

Acceptance measures true submit-to-terminal throughput for fresh Sessions and repeated conversations, model-visible history, PG eligibility and Kafka lag through drain. Cold, local-cache and cache-aware routing controls use the same storage and workload. Failures include stale versions, missing cached bodies, eviction and absent Workers. This extends the successful foreground text lifecycle of the prototype. Full production recovery and provider traffic have their existing separate acceptance suites.

```sh
export PI_CLOUD_RUNTIME_LOAD_CHECK=1
node --test scripts/experiments/minimal-session-storage/session-prefix.test.mjs
node --import tsx scripts/experiments/minimal-session-storage/run.mjs /private/continuation-smoke wake-throughput smoke
PI_CLOUD_SESSION_BENCH_PROJECTION=single PI_CLOUD_SESSION_BENCH_RATES=90,140 \
  PI_CLOUD_SESSION_BENCH_SECONDS=60 node --import tsx scripts/experiments/minimal-session-storage/run.mjs /private/continuation-mixed wake-throughput mixed
node scripts/experiments/minimal-session-storage/summarize-throughput.mjs /private/summary.json /private/continuation-smoke /private/continuation-mixed
```

Use `fresh`, `history` or `mixed` for independent timed arrivals. Returning Sessions begin with 512 KiB of unique synthetic text and accrue genuine user and assistant Entries. The mixed workload alternates fresh and returning Sessions. `confirm` fixes the rate and duration of four foreground estimates, including 100/s for two minutes and 116/s for one minute. `burst` compares fixed client concurrency, `interactive` compares cache policies at a fixed 40 Run/s, and `large` starts each returning Session with 4 MiB. `busy` exercises a follow-up or a real native test Tool and steer per Run. `projection` alternates guarded single-statement projection with explicit transactions at the same offered rate. Every run needs a new output directory. The underlying Inbox upsert is identical in both projection variants. The candidate default, `single`, combines the owner check and one-row mutation in one PG statement. Multi-row changes remain atomic. [Measurements](../../../docs/reports/session-continuation-throughput-20261119.md).

The terminal update records a fresh UUID and the native sequence, next ID, cutoff and count/bytes of covered Entries. Cache candidates can be prepared after Kafka acknowledges the terminal, and become usable only when the next PG claim returns that exact version. A hit restores detached entries under their original commit positions. The new input, current Inbox and model selection come from the new claim. Expired or evicted cache records withdraw their advertisements. Up to 32 small candidate addresses fit in one coalesced PG notification, regardless of how many admissions it represents. A ready cache holder may promote one eligible Run within a batch, with no reservation or follow-up delay on the ordinary FIFO path. Quiescent head replacement and a new fork scope are exercised with isolated maintenance fixtures.

## Boundaries and evidence

The `wake-throughput tail` diagnostic timestamps native poll delivery and partition handler entry for each private-topic offset. It compares the normal one-record poll with a multi-record poll and separately removes message/Inbox writes in a terminal-only control. Compare the same accepted Run across creation, producer acknowledgement, native callback, handler entry and PG completion. A sampled projector PG connection records server durations with text and parameters disabled. These are attribution experiments in disposable actors, not a release change.

Use `tail-reference` for the 90-second, 160/s observed repeat, `tail-stress` for 260/s controls, `tail-switch` for four-second windows in the same process, and `tail-history` for successor eligibility. Each suite uses a new directory. To capture content-free PG durations, run a PostgreSQL Pod log follower alongside the experiment and pipe its stdout through `python3 -u scripts/experiments/minimal-session-storage/pg-duration-lines.py` into a private TSV. After all workloads finish, stop the follower.

```sh
node --import tsx scripts/experiments/minimal-session-storage/verify-tail-sql.mjs /private/observed/report.json docs/reports/session-projector-statements-20261119.sql > /private/plans.json
python3 scripts/experiments/minimal-session-storage/summarize-tail.py /private/tail.json /private/original-single-fresh --plans /private/plans.json --case /private/observed /private/durations.tsv --case /private/returning -
```

The SQL verifier requires the development runtime setting and the same opt-in load flag, and uses temporary relations in a rolled-back transaction. [Tail attribution](../../../docs/reports/session-tail-attribution-20261119.md) and [the SQL templates](../../../docs/reports/session-projector-statements-20261119.sql) document the results.

The `tail-reader` suite bundles the reader from this checkout, observes its real
callbacks and supports independent arrival rates. Compare `single` and `batch`
in alternating runs with separate output directories. Change only
`PI_CLOUD_SESSION_BENCH_PROJECTOR_CONNECTIONS` (12 or 24) to test the separate
database-pool question. Actual Session Projector settings stay untouched by the experiment.

```sh
PI_CLOUD_SESSION_BENCH_READER=batch PI_CLOUD_SESSION_BENCH_RATES=160,260 \
  PI_CLOUD_SESSION_BENCH_SECONDS=25 node --import tsx \
  scripts/experiments/minimal-session-storage/run.mjs /private/batch-run wake-throughput tail-reader
PI_CLOUD_READER_DIAGNOSTIC=1 node --import tsx \
  scripts/run-projector-reader-diagnostic.mjs /private/batch-backlog.json batch
```

The second command consumes identical preloaded offsets four times in alternating
reader modes. [Measurements and limits](../../../docs/reports/session-projector-batch-read-20261119.md).

### Direct PG and Projector comparison

With the same development runtime and load opt-in as above, run each command
serially against a new private output directory:

```sh
node --import tsx scripts/experiments/minimal-session-storage/run.mjs /private/pg-pair wake-throughput pg-paths
PI_CLOUD_PG_PATH_PROFILE=timing node --import tsx scripts/experiments/minimal-session-storage/run.mjs /private/pg-idle wake-throughput pg-paths
node scripts/experiments/minimal-session-storage/summarize-pg-paths.mjs /private/pg-summary.json /private/pg-pair/report.json /private/pg-idle/report.json
```

`PI_CLOUD_PG_PATH_PROFILE=smoke` is the 320-Run correctness check. The full
profile captures actual Worker facts and every write's SQL and bound values.
Both replay arms start from the same already-claimed owners. Each maintains 32
ordered partitions with 12 warmed projection connections. The direct arm executes
the captured commands in the same Pod and process. The Projector arm republishes
the captured bytes from another Pod using the real producer, reader and projection
handler. Compare due records to settled records and verify the three tables after
every trial. The timing profile matches an observed PG idle transition by an
opaque SQL comment on one sampled backend. Only that extra probe can distinguish
server transaction settlement from the final response interval for matched writes.
The raw timing file is private. The summarizer exports aggregate evidence.
[Results](../../../docs/reports/projector-pg-path-baseline-20261119.md).

### Compare shared and disjoint CPUs

The development-node fixture creates a disposable PG Pod using the deployed
image and a disk-backed volume, a separate Projector Pod, and a support Pod for
two Worker processes and their wake router. It uses a temporary database
credential, the existing RF3 Kafka cluster and the same release lock. Its local
driver requires `taskset` and the existing K3s `crictl` access to update only
the three verified study container IDs. Each measurement checks the granted
CPU lists before and during load. Complete SMT sibling groups stay together.

```sh
PI_CLOUD_RUNTIME_LOAD_CHECK=1 node --import tsx scripts/experiments/minimal-session-storage/run-cpu-placement.mjs /private/cpu-first cpu
PI_CLOUD_RUNTIME_LOAD_CHECK=1 node --import tsx scripts/experiments/minimal-session-storage/run-cpu-placement.mjs /private/cpu-reverse cpu-confirm
node scripts/experiments/minimal-session-storage/summarize-cpu-placement.mjs /private/cpu-summary.json /private/cpu-first /private/cpu-reverse
```

Use `cpu-smoke` for the short affinity and cleanup check. The primary comparison
shares eight guest cores or assigns four per role and reverses which four. The
independent confirmation reverses the order of the live trials. Results include
the recorded-tape digest, per-round affinity checks, host calibration and
verification of fixture cleanup. [CPU placement results](../../../docs/reports/projector-cpu-placement-20261119.md).

### PostgreSQL native group commit

The same disposable fixture supports `group-smoke`, `group-commit`, and an
independent `group-confirm` run. The
database and Projector keep the same open CPU layout throughout. The trial
reloads only the temporary PG server's `commit_delay` and `commit_siblings`,
checks all twelve writer connections, and restores the initial settings before
cleanup. Full Runs and immutable trace replays use ordinary synchronous commits.

```sh
PI_CLOUD_RUNTIME_LOAD_CHECK=1 node --import tsx scripts/experiments/minimal-session-storage/run-cpu-placement.mjs /private/pg-group group-commit
PI_CLOUD_RUNTIME_LOAD_CHECK=1 node --import tsx scripts/experiments/minimal-session-storage/run-cpu-placement.mjs /private/pg-confirm group-confirm
node scripts/experiments/minimal-session-storage/summarize-group-commit.mjs /private/group-summary.json /private/pg-group /private/pg-confirm
```

The primary series balances four settings across positions and predecessors
at 260 Run/s. A 520/s overload comparison and independent full-Run arrivals
follow. The independent confirmation compares the default and the 1,500 μs /
zero-sibling setting on a new recording, then also tests low-rate live input.
The summary verifies settings, ordering, PG content and cleanup before
exporting aggregates. [Measurements and recovery boundaries](../../../docs/reports/pg-native-group-commit-20261119.md).

### Asynchronous PG acknowledgement

The `async-commit` study alternates `synchronous_commit=on/off` on a disposable
PG instance, with `fsync=on`, `commit_delay=0`, twelve identical projection
connections and a fixed CPU layout. `async-smoke` is the short regression.
It times the return to the client. A separate read-only observation after the
timed interval waits for the WAL flush position to cover a fixed insert LSN.
After the timed workload, a second private socket-only server checks which
already acknowledged rows survive `pg_ctl stop -m immediate`; it uses a fresh
data directory inside the disposable Pod.

```sh
PI_CLOUD_RUNTIME_LOAD_CHECK=1 node --import tsx scripts/experiments/minimal-session-storage/run-cpu-placement.mjs /private/async-pg async-commit
PI_CLOUD_RUNTIME_LOAD_CHECK=1 node --import tsx scripts/experiments/minimal-session-storage/run-cpu-placement.mjs /private/async-peak async-peak
node scripts/experiments/minimal-session-storage/summarize-async-commit.mjs /private/async-pg-summary.json /private/async-pg /private/async-peak
```

[Results and acknowledgement semantics](../../../docs/reports/pg-async-commit-20261120.md).

The `wake-sizing` study varies batches 8, 16, 32 and 64 under replenished load and an already queued burst. One Worker holds 64, 128 or 256 distinct physical Sessions at the real Pi model boundary. The driver records concurrent waiters, process RSS and event-loop delay. Separately it measures a bounded, tenant-scoped preference for one Run ID and exercises fresh native context assembly with the same frozen entries fetched from PG or already in memory. The individual experiments stop short of routing successive production Runs to a retained cache. [Results](../../../docs/reports/worker-claim-sizing-20261119.md).

`schema.sql` defines the three physical tables and constraints. An independent client request is durably registered before success. Two concurrent SQL claimers cannot skip an active predecessor in the same Session. Inbox removal and the `user` Entry are committed together. The separately delivered `work_finished` updates the admitted Run under its claim identity. The experiment does not reconstruct an in-flight Pi Task after Worker death. That requires the additional stored state and recovery contract in the full runtime.

Throughput counts confirmed Runs divided by elapsed stage time, including drainage. Latency starts just before registration and ends at the Projector's successful terminal PG callback. Native-ACK totals are sums per Run, so pipelined intervals can overlap. The final gap is timed between durable answer and terminal callbacks on the same Projector process. The finish transit metric also includes time waiting behind earlier Session-partition records. Kafka lag is sampled high offset minus processed offset, and all 32 partitions must drain. A short peak between samples may be missed. JSON bytes for the Inbox are the parameter payload of current-value writes. PostgreSQL WAL and on-disk bytes have different accounting.

The serving database is only read for preflight. Every measured mutation targets the newly created database, which is dropped after closing the actors. Topic and group absence are checked through the Kafka admin API. Data published with the [report](../../../docs/reports/minimal-session-storage-load-20261119.md) contain aggregates only.


### Conversation usage

The `usage-rate` profile replays one immutable recording across both arms with
the selected lean shared-stream schema. Each assistant reply already contains
provider usage and a complete native cumulative update. The enabled arm also
upserts the current document inside the admitting PG transaction. `usage-rate-confirm`
uses a new recording and the complementary order. `usage-rate-smoke` exercises
the same guarded SQL with one Session per partition. All three run serially.

```sh
PI_CLOUD_RUNTIME_LOAD_CHECK=1 node --import tsx scripts/experiments/minimal-session-storage/run-cpu-placement.mjs /private/usage-study usage-rate
PI_CLOUD_RUNTIME_LOAD_CHECK=1 node --import tsx scripts/experiments/minimal-session-storage/run-cpu-placement.mjs /private/usage-confirm usage-rate-confirm
node scripts/experiments/minimal-session-storage/summarize-complete-messages.mjs /private/usage-summary.json /private/usage-study /private/usage-confirm
```

The native frequency test uses the pinned Harness and a streamed faux model.
The PG integration test also projects this trace one commit at a time.
[Protocol, measurements and scope](../../../docs/reports/minimal-projection-usage-20261120.md).

The `usage-rate-bulk` / `usage-rate-bulk-confirm` pair keeps cumulative usage
enabled in both arms. One writer executes each row change inside the existing
transaction. The candidate issues a multi-row upsert when at least two distinct
Conversation keys are ready together. A repeated key retains the sequential
path, including every original position and conflict check. Read the allocated
heap/index/TOAST size and live JSONB payload after each run, outside the timing
and WAL window. The one-Session-per-partition `usage-rate-bulk-smoke` covers
the sequential path with repeated keys; the formal runs cover distinct keys.

```sh
PI_CLOUD_RUNTIME_LOAD_CHECK=1 node --import tsx scripts/experiments/minimal-session-storage/run-cpu-placement.mjs /private/usage-row-bulk usage-rate-bulk
PI_CLOUD_RUNTIME_LOAD_CHECK=1 node --import tsx scripts/experiments/minimal-session-storage/run-cpu-placement.mjs /private/usage-row-bulk-confirm usage-rate-bulk-confirm
node scripts/experiments/minimal-session-storage/summarize-complete-messages.mjs /private/usage-row-bulk.json /private/usage-row-bulk /private/usage-row-bulk-confirm
```

For a source-independent PG control, run
`node scripts/experiments/minimal-session-storage/usage-sql-pair.mjs`.
It creates a disposable local PG container and times 12 warmed pairs of
fixed-size prefixes using one connection. Only aggregate timings leave the
fixture. [Results and default selection](../../../docs/reports/minimal-usage-bulk-20261120.md).

### Concurrent accounting writes

Run `node --import tsx scripts/experiments/minimal-session-storage/usage-contention.mjs --direct`
for the archived #49 model receipt SQL. Append `--settle` after `--direct` to include native model settlement evidence for the same invocation IDs in the projection transactions. Pass the migration path to run the source revision
directly, or omit the argument for the committed receiver. Uses a dedicated test container;
each comparison replays the same fixed prefixes and receipts, rotates layout order,
verifies 2,048 receipt events per concurrent sample and reads the expected Session totals.
[Conditions and results](../../../docs/reports/status-usage-contention-20261120.md).

### Family Inbox recovery (#52)

`family-inboxes.integration.spec.mjs` drives the ordinary projection code from
Pi's real Storage commits. On a pending parent delegation and child Tool it
replaces the Projector, replays the protected suffix and checks both queues,
native owner identity, input placement and root-only history bounds. The fixture
uses local deterministic tools. The Worker/Guest study below covers takeover with an original Kafka reply. In live model fixture stages,
`finalModelOutputToRunSettlementMs` measures final provider stream completion
to logical Run settlement. Recorded playback has no model-completion sample.
[Results](../../../docs/reports/minimal-family-recovery-20261120.md).

### Retained-log Worker handoff

From an operator checkout connected to the development K3s cluster, run
`PI_CLOUD_RUNTIME_LOAD_CHECK=1 node --import tsx scripts/experiments/minimal-session-storage/run-retained-family.mjs /private/retained-family.json`.
Set `PI_CLOUD_RUNTIME_DIRECTORY` to the installed runtime directory for an
isolated worktree. The driver owns a disposable PG instance, fresh Worker
processes, private RF3 Session/reply topics, and separate temporary Cube VMs.
The installed fixed Guest returns through HTTPS completion. Only the owned
Session topic is truncated in the retention experiments. One scenario keeps
previous Run history in PG while dropping its entire Kafka prefix. Another
removes records for a still-active family. No live Worker is stopped.
[Coverage and remaining integration work](../../../docs/reports/retained-family-handoff-20261121.md).


Add `--serving` after the output path to run HTTP admission, real lease expiry,
Outbox publication, the serving Projector and Worker reacquisition. This mode
creates a separate three-broker Session Kafka cluster and a disposable PG with
both projections. The candidate cold reader has a restricted database role.
The development cluster serves only the temporary Guests and their private
reply mailboxes. [Integration results](../../../docs/reports/serving-family-handoff-20261121.md).
