# Minimal Session lifecycle load test

For the isolated root-only Session and delegated-Run schema, see the [design, cold-read measurements and reproduction commands](../../../docs/reports/delegated-runs-20261122.md).

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

Use `--pg-handoff` in place of `--serving` for the isolated PG-only control.
The replacement discovers the previous seal in PG. Its existing-store reader
and its seal-snapshot reader have distinct database privileges. Before B is
started the driver verifies the old Session prefix has been removed from the
private Kafka cluster. Original reply mailboxes remain available.
[Evidence and scope](../../../docs/reports/pg-family-handoff-20261121.md).

Use `--bash-fields` for the focused pending-reply and Stop checks. The seal
writer compares the fixed Bash task and its one-call generation with records
derived from candidate PG rows, then omits both. The replacement reconstructs
them again from those rows. The other family state stays in the control snapshot.

The #52 family studies now use the prototype Workspace-operation protocol.
The Session Kafka record carries the native batch and logical execute/cancel
metadata. Execution selects the Guest and resolves prior operation positions.
Run `node --import tsx scripts/experiments/minimal-session-storage/workspace-routing-recovery.mjs`
to verify a PG restart, replay across a binding transition and cancellation from
a fresh routing adapter. The check owns a disposable PG; its Kafka grant is
controlled. The existing `--pg-handoff` and `--bash-fields` studies exercise
real Kafka, Cube, active Stop and Stop after replacement with a second live VM.
[Ownership and release scope](../../../docs/adr/0344-execution-owned-operation-routing-study.md).
For a single case, append its name after the mode, for example `--serving normal`.

For the #52 Workspace routing study, run each of the following profiles separately
with `PI_CLOUD_RUNTIME_LOAD_CHECK=1 node --import tsx scripts/experiments/minimal-session-storage/run-cpu-placement.mjs <new-private-output-directory> <profile>`.
The cluster and isolated-fixture prerequisites above apply.

- `workspace-continuation` checks the first provider request after an idle environment edit,
  with two available Workers and with the previous Worker drained. It verifies the PG claim,
  prefix reads, native system entries and model input independently.
- `workspace-guest-continuation` repeats the directory and Workspace switches using two disposable Cube
  Guests and their fixed Bash implementation. The test verifies the first provider prompt, physical
  cwd and file marker, the second provider request and the projected Tool Result. The resource fixture
  owns the private provider client. Replies travel via the maintained HTTPS completion service and
  real per-Worker Kafka mailboxes. Requires an operational Cube cluster and its configured template.
- `workspace-rate` pairs per-record routing with immediately ready, same-partition history
  batches. It verifies recorded history hashes and independently times actual Pi Runs using
  the deterministic provider.
- `workspace-rate-confirm` compares the previous direct projection and execution-side
  history batching at the same offered rate. Use the measured producer ACK rate, projection
  rate and lag samples to interpret generator backpressure.
- `workspace-rate-input` holds projection until every Kafka ACK returns at a target of 4,630
  counted entries per second, then drains and verifies the same history.

### Mixed native history and real Guest load

Use the `workspace-guest-mix` profile with the same isolated-fixture command as
`workspace-guest-continuation`. Each fresh Session gets one native Bash proposal, one
canonical result and two deterministic model calls. At a fixed open-loop arrival rate,
0%, 25%, 50% and 100% of those proposals perform real work on an owned Cube VM; the
remaining controls return fixed text without executing shell. Reverse the order on the
second pass. The driver checks the original call/result pairing, model input, physical
execution multiset per VM, and final Kafka lag.

`PI_CLOUD_GUEST_MIX_RATE` (default 4 Runs/s) and `PI_CLOUD_GUEST_MIX_COUNT` (default
64 scheduled Runs per arm, a multiple of 32) control the offered workload.
`PI_CLOUD_GUEST_MIX_CONFIRM=1` repeats only the all-real arm after warmup;
`PI_CLOUD_GUEST_MIX_TARGETS=2` divides work across two owned Guests. Run invocations
serially. The report includes per-operation latency distributions, lag samples and
Guest resource counts. The failed-arm report is a snapshot of outstanding work at
interruption. A missing acceptance ACK stops the trial and is never retried.

### Projector dispatch to 4,000 destinations

Run the `workspace-fanout` profile via `run-cpu-placement.mjs` in fresh private
output directories. The source, Projector and endpoint simulator are separate
processes. Existing authenticated cluster access and the disposable PG/Pod
prerequisites above apply. No VM or paid model is required.

`PI_CLOUD_FANOUT_SUITE=matrix` measures direct WebSocket transport and paired
serial/concurrent projection at 0, 20, 100 and 500 ms of added receipt delay,
plus paced and preloaded controls. `sustain` measures longer offers, isolates
PG and Kafka commit work without external sends, and forces dispatch backpressure.
`smoke` runs a shorter 0/20 ms check. `confirm` reverses the no-send controls and checks the saturated bounded sender again. Run the profiles serially.

Each trial uses all 32 partitions, one warm existing binding per Session and
one socket per target. Real execute-intent records enter the RF3 Session log.
Receiver checks cover destination, operation identity, order and complete counts.
The driver compares PG operation hashes and committed Kafka positions before
resetting. Network-only calibration and no-send controls are explicitly labelled.
The report retains source ACK rates, receiver arrival buckets, phase distributions,
RSS/heap samples and dispatch reservations. No script executes the payload or
returns a canonical Tool Result.

### Consecutive grants and partition count

In the same fan-out fixture use `PI_CLOUD_FANOUT_SUITE=grants` for paired
1/8/32/32/8/1 limits (32,000 intents per arm). Use `grants-long` for 92,000
intents per arm at widths 8 and 32. Set `PI_CLOUD_FANOUT_PARTITIONS` to 32 or 64.
Give each run a fresh output directory. Each invocation creates its own RF3
topic generation, balanced target routing and scratch PG. Pre-warm routes and
keep one PG transaction per Tool and 100 ms receipt delay.
`PI_CLOUD_SESSION_BENCH_PROJECTOR_CONNECTIONS` sets this Projector's pool limit
to 12 (default) or 24. To compare at 64 partitions, run `grants-long` serially
at 12, 24, 24 and 12 connections. The saved report includes the verified pool
limit and open client count, summed connection acquisition and hold time,
transaction count, actual prefix sizes, logical vs native commits and lag.
Divide `result.pgTransactions.poolMs` and `holdMs` by `total` for per-transaction
means. Both are client-side elapsed times.

Run `node --import tsx scripts/experiments/minimal-session-storage/run-grant-crash.mjs`
separately for an isolated three-broker and PG fault trial. Its PG delivery
witness permits duplicates so that re-execution would be visible. A replacement
rewinds from offset zero after the first process dies with six unsent commands.

### Projecting consecutive Tool intents in one PG transaction

Use the same `workspace-fanout` driver, with 32 partitions and 12 Projector
connections. Select `PI_CLOUD_FANOUT_SUITE=grants-pg` for the reversed 92,000-intent
comparisons at Kafka grant limits 8 and 32, `grants-pg-backlog` for a preloaded
queue, or `grants-pg-minute` for paired 276,000-intent paced trials at limit 8.
Use a new output directory for each invocation. Each trial asserts the full receiver
count, per-target order, PG operation references and Kafka positions. Compare input
rate as well as confirmed sends including final drain. Trial output records the PG
transaction count and actual grant distribution. In the grouped arm the PG phase
timers measure elapsed time per projected prefix.

For example, with the fixture prerequisites above and an unused output path:

```sh
PI_CLOUD_RUNTIME_LOAD_CHECK=1 PI_CLOUD_FANOUT_SUITE=grants-pg \
  node --import tsx scripts/experiments/minimal-session-storage/run-cpu-placement.mjs \
  /tmp/pma-intent-comparison workspace-fanout
```

Run `node --import tsx scripts/experiments/minimal-session-storage/run-intent-pg-faults.mjs`
for the separate disposable-PG rollback, acknowledgement-loss, cancellation and stale
writer checks. It also captures two fixed-Pi Bash intent commits and checks their
original live document transitions. Presentation events other than the separately tested matching Tool start retain individual
processing. [Measurements and scope](../../../docs/reports/pg-tool-intent-batch-20261121.md).

### Native tool starts and maximum grant size

Use the same `workspace-fanout` driver and a different disposable output directory per invocation:

| `PI_CLOUD_FANOUT_SUITE` | Input and goal |
|---|---|
| `grants-display-smoke` | 8,000 intent/receipt pairs at widths 1 and 8 |
| `grants-display` | Paced 32,000-command arms at widths 4, 8, 16 and 32 in forward and reverse order |
| `grants-display-backlog` | Preload the same number of commands; compare widths 4, 8, 16, 24 and 32 twice |
| `grants-display-wide` | Separate preloaded generation at 32, 64 and 128, then reverse; retains the reader's existing 256 KiB ready-prefix budget |
| `grants-display-mix` | Capture every native commit of 4,000 one-Bash Harness Runs, including the original start display, results and completion; replay the identical interleaving at limits 8 and 32 |
| `grants-display-ackmix` | Run Pi with each Storage commit waiting for its real Kafka acknowledgement, measure live at 8, then replay the same captured mixed traffic at 8 and 32 |

The intent-only sweep uses a captured native pending-to-running transition, real
Runner display descriptions, pre-established routes, actual PG projection and
RF3 dispatch grants. Display-bearing transactions retain synchronous PG commit.
The synchronous display observer resolves the original argument reference and
checks its destination and order after PG admission. The endpoint simulates a
100 ms acknowledgement. The mixed trace also verifies settled PG rows and
original Tool Result counts. The model and shell are deterministic fixtures;
browser rendering and model tokens are outside the benchmark. Inspect
`trial.mix.adjacent` for the upper bound on groupable intents and
`count - result.grantSizes["1"]` for the number that actually shared a grant.

### Tool-dominated Run throughput

Use `PI_CLOUD_FANOUT_SUITE=grants-tool-loop` with the same `workspace-fanout`
driver. This suite uses 128 active destinations across all 32 partitions and
an eight-intent grant limit. Each real Pi Run makes 200 sequential fixed Bash
calls with deterministic 1,024-byte text results, one threshold compaction and
one final assistant response. The live source awaits actual Kafka acknowledgement.
A captured, hash-checked tape is replayed at several input rates and with a
preloaded backlog. The fake tool result is generated in the Harness fixture;
the separate network receiver checks the actual dispatch and simulates its
100 ms acknowledgement. The model and external shell are outside this measurement.

Each arm validates the identities, order and sizes of all PG tool-call/result
pairs, per-kind history counts, settled Runs, displays, receiver counts and
Kafka positions. After the driver exits, run
`node scripts/experiments/minimal-session-storage/summarize-tool-loop.mjs <output-directory>`
to get Kafka records, completed history rows and tool pairs per second, plus
lag at four points in the input window and time to drain after the producer.


### One million Runs per day at 20 tools per Run

`PI_CLOUD_FANOUT_SUITE=grants-million-day` uses the same driver and summary
command as the 200-tool study. It captures 1,024 fixed-Pi Runs with 20
sequential Bash calls, 1 KiB tool results and normal closure. The captured
profile has 187 Kafka records per Run, without threshold compaction. Paced
replays offer the record rates equivalent to one, two, three, one and a half,
and again one million Runs per day. The live arm waits for the Kafka producer
on every native commit. PG Run rows and routes are prepared before timing;
the rates describe the Session Projector and warm tool dispatch. Each arm
verifies completion, original ordered call/result pairs, displays, receipts
and committed partition positions.

The summary leads with completed user-visible Kafka records per second. A record with assistant text, any number of completed tool calls or one or more results counts once. Public output item rates count text-bearing assistant messages, individual calls and results separately. Streaming deltas, tool dispatch and control receipts do not add another completed output. Set `PI_CLOUD_COMPARE_USAGE_WIRE=1` with `grants-million-day` to compare intact traces against disposable replays that omit only `pi.usage` updates, in alternating order. The serving writer always sends the original Pi commit.

The standard 20-tool profile uses a 50 ms simulated sandbox acceptance delay. Set `PI_CLOUD_COMPARE_ACK_DELAY=1` on that profile to capture one trace and replay it at both 50 and 100 ms. The backlog trials run in 100/50/50/100 ms order. Paced trials run in reverse order at the same visible-record input target. The usage, index and host-speed comparisons retain their original 100 ms setting. The configured delay applies only to the simulated receiver's acknowledgement. The model and Bash timings used to create the trace stay fixed across replays.

### Conversation Entry index cost

With the same 32-partition, 12-connection `workspace-fanout` setup, set
`PI_CLOUD_FANOUT_SUITE=grants-million-day` and `PI_CLOUD_COMPARE_ENTRY_INDEX=1`.
After live Pi capture, the driver replays the identical 1,024-Run tape eight times
from a backlog (off/on/on/off/on/off/off/on) and four times at 3,247 records/s
(off/on/on/off). It installs the index only on the empty scratch message table
for indexed arms. Both arms checkpoint their dedicated PG after preparing Runs
and routes. The interval from projection start through drain excludes index
construction, seed and verification reads from the WAL sample. Each arm reports
the actual index and table sizes. Tool PG P95 is the upper edge of the existing
5% histogram buckets for the projection phase of a Tool intent or intent group.

Run the two independent calibrations serially after the cluster fixture exits:

```sh
node --import tsx scripts/experiments/minimal-session-storage/entry-index-append-study.mjs > /tmp/entry-append.json
node scripts/run-with-test-postgres.mjs -- node --import tsx \
  scripts/experiments/minimal-session-storage/delegated-cold-study.mjs --compare-index > /tmp/entry-reads.jsonl
node scripts/experiments/minimal-session-storage/summarize-entry-index.mjs \
  /path/to/cluster-output /tmp/entry-append.json /tmp/entry-reads.jsonl
```

The append control uses an exclusive, disposable PG. It stages one native
20-Tool payload shape across eight physical Sessions and 128 Conversations each,
prepopulates 132,096 Entries and measures one server-side INSERT of 44,032 fresh
Entries. The timing includes FK trigger execution and excludes statement input
generation and commit acknowledgement. The regular Projector measurement above
includes its ordered admission, mixed transactions and simulated device replies.

### Atomic handoff after loss of the Worker and Session Projector (#52)

Run `PI_CLOUD_RUNTIME_LOAD_CHECK=1 node --import tsx scripts/experiments/minimal-session-storage/run-retained-family.mjs /private/atomic-handoff.json --atomic-handoff`
with the same isolated-cluster and Cube fixture prerequisites as the other family
studies. An optional final argument selects `serving-pending`, `already-returned`,
`reply-timeout` or `user-stop`. Each case uses a new pre-crash Projector process:
after the Guest's marker and the queued edit, terminate A and this Projector with
SIGKILL. A fresh owner rebuilds through its ordinary Session Kafka group. A
deliberate statement-boundary failure rolls back the first recoverable seal;
the retry observes the old Run lease, the unadvanced candidate head and the
absent handoff before it commits. Remove the old Session prefix before B claims.

B uses RunExecutor, PostgresDurableSessionHost.open, the candidate cold data
role and the original reply mailbox. The matched native storage adapter and
tool publisher are supplied by this isolated composition. The Session API still
uses the serving read model. Check the independent candidate result, HTTP parent
and child histories, queued input placement, physical execution count, and
removal of the handoff after closure. A second Guest witnesses exact cancellation
in the Stop case. Neither the deployed Worker configuration nor its live
Projector is switched by this command.

Use `--atomic-handoff later-run` or `--atomic-handoff later-run-compacted` on a
separate invocation to select the candidate-only composition. Both remove
Run 1’s Session log before accepting Run 2, kill the Worker while the child Bash
is pending, and rotate Run 2 through its seal before cold-open. The compacted
variant also delivers the Guest reply twice before the replacement starts.
The model requests, PG history, HTTP root and HTTP child readers are checked
independently. [Outcomes and child-reader scope](../../../docs/reports/later-root-handoff-20261122.md).

The `double-pending` scene also kills B after it has reattached to the original
operation and committed an edit to the child's Inbox. `double-before-open` kills
B after claim and before starting Pi. Both scenes compact the original parent's
context, remove both sealed Kafka prefixes, and require C to consume the original
reply at its original deadline and continue the same family. Append either scene
to the atomic-handoff command to run it separately.

### Ordinary root from the web composer

Use the operator fixture above with an available Cube/KVM runtime and Chrome:

```bash
PI_CLOUD_RUNTIME_LOAD_CHECK=1 node --import tsx \
  scripts/experiments/minimal-session-storage/run-retained-family.mjs \
  /private/normal-root.json --normal-root-web
```

Set `PI_CLOUD_RUNTIME_DIRECTORY` to the existing deployment runtime directory
if running from a separate worktree. The isolated API uses a temporary tenant,
disposable PG and an isolated Session broker. A real Guest sends its reply on
the deployment's Kafka cluster using fixture-owned boot topics. Only this test's
topics and groups are removed. Use `--normal-root` for the HTTP variant, which
also retries the original input with the same idempotency key. See the
[acceptance report](../../../docs/reports/normal-root-message-store-20261122.md).


### Browser-confirmed busy input across a root handoff

With the same Cube, Chrome and disposable database fixture, run the two cases serially:

```bash
PI_CLOUD_RUNTIME_LOAD_CHECK=1 node --import tsx scripts/experiments/minimal-session-storage/run-retained-family.mjs /private/root-follow-up.json --root-input-web follow_up
PI_CLOUD_RUNTIME_LOAD_CHECK=1 node --import tsx scripts/experiments/minimal-session-storage/run-retained-family.mjs /private/root-steer.json --root-input-web steer
```

Each case holds the projector before the busy input reaches PG, waits for the real HTTP receipt in Chrome, and terminates the first Worker. The replacement attaches to the original tool and consumes the original input. See the [acceptance report](../../../docs/reports/root-input-handoff-20261122.md).

### Stop with two pending root inputs

Run `PI_CLOUD_RUNTIME_LOAD_CHECK=1 node --import tsx scripts/experiments/minimal-session-storage/run-retained-family.mjs /private/stopped-root.json --root-stop-web` with the browser and Cube prerequisites above. The fixture queues a follow-up and a steer, clicks Stop, holds the browser's acknowledgement of the returned inputs and edits the draft. It reloads before acknowledgement and again after acknowledgement, then sends the edited text to a fresh Worker. [Checks and failure evidence](../../../docs/reports/stopped-root-inputs-20261122.md).

To kill the old Worker before clicking Stop, use `--root-stop-handoff-web`. Append `already-returned` to publish the original Guest reply before the replacement claims the Run. Both cases verify zero model calls from the replacement and exercise the same draft recovery and edited continuation.

### Release evidence for historical document Forks and throughput variance

Run `node scripts/run-with-test-postgres.mjs -- node --import tsx scripts/experiments/minimal-session-storage/verify-fork-document-evidence.mjs` to compare the retained candidate rows with native `pi.agent` Fork snapshots. At `872950bb` this diagnostic exited 2. With ADR-0345 it requires equality with each native Fork in a fresh Harness, repeated Fork after source prune/deletion, and final collection. The diagnostic uses two disposable PG schemas and two deterministic Pi histories. The separate `--probe-current-world` option holds agent configuration constant and compares the next environment-reset notice after Fork. Exit 2 identifies the remaining observation-state gap.

For a diagnostic repetition of the million-Run load, add `PI_CLOUD_REPEAT_MILLION_RATE=1` to the existing `grants-million-day` command. After the live capture it replays 1.5×, 3×, 1.5× and 1.5× daily demand, each on a drained projection. Each paced arm in an invocation uses its captured tape. The standard suite retains the original load sequence.


### Run-boundary Flow and directory recovery

`run-flow-history.mjs` builds an isolated PostgreSQL schema with Sessions, Run predecessors and ordinary message bodies. Test the joined delegated Run layout through the admission gate. Both scripts require the maintained disposable PG runner.

```sh
node scripts/run-with-test-postgres.mjs -- node --import tsx scripts/experiments/minimal-session-storage/check-matched-session-identity.mjs
node scripts/run-with-test-postgres.mjs -- node --import tsx scripts/experiments/minimal-session-storage/run-flow-history.mjs
```

For the frozen-base comparison, pass `base` to either command above. The paired benchmark uses the same 100,000 predecessors and admission IDs in two isolated schemas. It reports client latency, PG row updates, WAL and the base lookup plan. Run the page-plan diagnostic separately to compare concurrent reads with and without a named prepared query on the disposable Linux PG fixture.

```sh
node scripts/run-with-test-postgres.mjs -- node --import tsx scripts/experiments/minimal-session-storage/compare-flow-admission.mjs
node scripts/run-with-test-postgres.mjs -- node --import tsx scripts/experiments/minimal-session-storage/compare-flow-admission.mjs --page-plans
```

[Measurements and recovery behavior](../../../docs/reports/flow-base-admission-20261123.md).

The branch-tree oracle checks independent copies of visible history after mixed mutations and inspects stored Runs and messages after collection. The mixed test starts disposable PG and RF3 Kafka, the actual Session API in its own process and the built Web UI in Chrome. It reuses one preloaded 32-partition native tape across six trials. Run serially from the repository root.

```sh
node scripts/run-with-test-postgres.mjs -- node --import tsx scripts/experiments/minimal-session-storage/flow-tree-study.mjs
npm run build --workspace @pi-cloud/web-ui
TSX_TSCONFIG_PATH=packages/control-plane/tsconfig.json node --import tsx scripts/experiments/minimal-session-storage/model-settlement-rate.mjs 2 flow-pages
```

Replace `flow-pages` with `flow-page-locks` to test same-Run snapshot visibility, observe a deliberately blocked second writer and sample PG wait events during two- and four-CPU replays. The quota changes apply only to the PG container created by this benchmark.

For the cost per page, run the two profiles serially on the same machine. Each creates disposable PG and Kafka and checks the visible content of the latest, older and newer pages across a genuine no-Fork layout and two Flow views. The fixed-rate suite pairs each load with nearby writer-only controls. The last command profiles the read side alone.

```sh
TSX_TSCONFIG_PATH=packages/control-plane/tsconfig.json node --import tsx scripts/experiments/minimal-session-storage/model-settlement-rate.mjs 4 flow-page-cost
TSX_TSCONFIG_PATH=packages/control-plane/tsconfig.json node --import tsx scripts/experiments/minimal-session-storage/model-settlement-rate.mjs 4 flow-page-cost fixed32
TSX_TSCONFIG_PATH=packages/control-plane/tsconfig.json node scripts/run-with-test-postgres.mjs -- node --import tsx scripts/experiments/minimal-session-storage/matched-page-cost.mjs
```

[CPU and throughput comparison](../../../docs/reports/matched-paging-cpu-20261123.md).

To compare the optimized page selector with the original public reader, supply a second checkout of `faa2ac13` with its workspace dependencies installed and resolving to that checkout. Both API processes read the same disposable database. Run the following studies serially. `--edges-only` after the reference path limits the first command to the boundary and skew checks.

```sh
TSX_TSCONFIG_PATH=packages/control-plane/tsconfig.json node scripts/run-with-test-postgres.mjs -- node --import tsx scripts/experiments/minimal-session-storage/page-selector-study.mjs /path/to/reference-checkout
TSX_TSCONFIG_PATH=packages/control-plane/tsconfig.json node --import tsx scripts/experiments/minimal-session-storage/model-settlement-rate.mjs 4 flow-page-cost selector /path/to/reference-checkout
```

[Page selector results](../../../docs/reports/flow-page-selector-20261124.md).

To compare read placement on a real PG primary and streaming standby, use the following serial studies. Each owns its database containers, loopback ports and replication network. The tested route requires a fixed primary/standby timeline. The `standby-control` workload also times raw standby reads on deliberately immutable fixture data.

```sh
TSX_TSCONFIG_PATH=packages/control-plane/tsconfig.json node --import tsx scripts/experiments/minimal-session-storage/model-settlement-rate.mjs 4 flow-page-cost standby
TSX_TSCONFIG_PATH=packages/control-plane/tsconfig.json node --import tsx scripts/experiments/minimal-session-storage/check-standby-recovery.mjs
TSX_TSCONFIG_PATH=packages/control-plane/tsconfig.json node --import tsx scripts/experiments/minimal-session-storage/model-settlement-rate.mjs 4 flow-page-cost standby-control
```

[Read split results](../../../docs/reports/physical-history-read-split-20261124.md).

The scroll studies below use one disposable primary. The browser study uses native Chrome wheel events on long text and tool-heavy timelines. It measures cache misses, per-statement backend CPU, actual index use and two fixture-only variants of dependency joins. The first load profile replays the measured text-page arrival timing, advances eight cursors independently and compares fixed-rate and unlimited API clients. The second uses a real scrolling Chrome tab during projection and also replays eight staggered fast tool-history readers. Run these checks serially.

```sh
TSX_TSCONFIG_PATH=packages/control-plane/tsconfig.json node scripts/run-with-test-postgres.mjs -- node --import tsx scripts/experiments/minimal-session-storage/scroll-demand-study.mjs
TSX_TSCONFIG_PATH=packages/control-plane/tsconfig.json node --import tsx scripts/experiments/minimal-session-storage/model-settlement-rate.mjs 6 flow-page-cost scroll
TSX_TSCONFIG_PATH=packages/control-plane/tsconfig.json node --import tsx scripts/experiments/minimal-session-storage/model-settlement-rate.mjs 6 flow-page-cost scroll-tools
```

To check individual Tool rows, build the Web bundle used by the Chrome fixture first:

```sh
npm run build --workspace @pi-cloud/web-ui
TSX_TSCONFIG_PATH=packages/control-plane/tsconfig.json node scripts/run-with-test-postgres.mjs -- node --import tsx scripts/experiments/minimal-session-storage/scroll-demand-study.mjs --tool-rows
```

On a Run with 1,024 completed Bash calls, a 115 px upward wheel from the latest page generated zero older-page requests during the gesture and the following 12 seconds. Moving to 200 px from the loaded top fetched one older page; earlier history remained available. The same fixture with the previous collapsed Web bundle fetched 32 older pages following one notch. This measures browser demand on the primary, independently of the cost of each SQL query.

The [candidate read/write comparison](../../../docs/reports/candidate-page-and-write-cost-20261124.md) contains the matched mixed-load and failure commands.

[Workload interpretation and measurements](../../../docs/reports/scroll-demand-and-primary-cost-20261124.md).

[Mixed-load results and study boundaries](../../../docs/reports/flow-tree-and-paging-20261123.md).

Select `PI_CLOUD_COMPARE_FLOW_HISTORY=1` with `grants-million-day` for a matched 50 ms comparison on a single fixed tape. The control uses the baseline Run layout. The Flow arm seeds each independent Session at a real predecessor with a distinct public window. Both use the same projection SQL and Kafka keys. The existing benchmark calls its physical stream column `session_id`; the Flow installer maps that column to the prototype's `flow_id`. Admission and window detachment are measured independently from Kafka replay.

For the directory closure path use the existing operator fixture with `--directory-recovery-web` and an unused report path. It stages the matched fixed Guest executable only in its owned disposable VM. Real Chrome receives the returned drafts, selects a real directory and submits the next Run through the Session API. [Evidence](../../../docs/reports/directory-recovery-and-flow-20261123.md).

## Ordinary Bash after Worker loss

Run the [sealed Bash fault matrix](../../../docs/reports/sealed-bash-unknown-20261125.md) with a disposable PostgreSQL, three Kafka brokers and an isolated marker container.

```sh
TSX_TSCONFIG_PATH=packages/control-plane/tsconfig.json node --import tsx scripts/experiments/minimal-session-storage/bash-loss-study.mjs
```

The [append-layout study](../../../docs/reports/bash-intent-log-20261125.md) includes the corresponding fault, browser and load commands. Select it explicitly in the isolated worker and projection fixtures.


### Frozen message-range Forks

The [segment study](../../../docs/reports/segmented-session-forks-20261125.md) runs the opt-in Session API against a candidate schema. `measure-segment-forks.mjs` measures paired Fork creation, pagination, cold Pi import, branch depth, fan-out and the ordinary PG append path. `write-load.mjs --segments` feeds the same 20-tool recording to the RF3 Projector with Chrome following a live Turn and an optional reader paging a fork. Both commands allocate disposable infrastructure. Independent child Worker placement is tracked separately.
