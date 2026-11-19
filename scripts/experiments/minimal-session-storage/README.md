# Minimal Session lifecycle load test

This experiment measures the proposed Run queue, immutable messages and one current Pi Inbox per Conversation. It runs the actual pinned Pi Harness on two Worker Pods with a deterministic local model, using the maintained RF3 producer and partition-ordered consumer. A separate Projector process inside the Session runtime Pod uses a private topic and a disposable logged PostgreSQL database. Each role is invoked through kubectl exec. Source changes to these experiment files do not replace any application image.

The original storage controls use fresh physical Sessions. The `wake-throughput` suites also run successive roots in the same Sessions. The input driver uses direct SQL and an experiment channel for post-commit confirmations. HTTP/SSE, charging, tool sandboxes and restoration of in-flight native Tasks are additional costs of the maintained runtime. The benchmark's only tool returns a fixed observation. Each worker applies its in-memory native batch after Kafka acknowledgement. Steer and follow-up pass through Pi's actual busy submission and placement. Only the explicitly labelled replay controls read a previously captured plain trace.

## Run serially

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

The `wake-sizing` study varies batches 8, 16, 32 and 64 under replenished load and an already queued burst. One Worker holds 64, 128 or 256 distinct physical Sessions at the real Pi model boundary. The driver records concurrent waiters, process RSS and event-loop delay. Separately it measures a bounded, tenant-scoped preference for one Run ID and exercises fresh native context assembly with the same frozen entries fetched from PG or already in memory. The individual experiments stop short of routing successive production Runs to a retained cache. [Results](../../../docs/reports/worker-claim-sizing-20261119.md).

`schema.sql` defines the three physical tables and constraints. An independent client request is durably registered before success. Two concurrent SQL claimers cannot skip an active predecessor in the same Session. Inbox removal and the `user` Entry are committed together. The separately delivered `work_finished` updates the admitted Run under its claim identity. The experiment does not reconstruct an in-flight Pi Task after Worker death. That requires the additional stored state and recovery contract in the full runtime.

Throughput counts confirmed Runs divided by elapsed stage time, including drainage. Latency starts just before registration and ends at the Projector's successful terminal PG callback. Native-ACK totals are sums per Run, so pipelined intervals can overlap. The final gap is timed between durable answer and terminal callbacks on the same Projector process. The finish transit metric also includes time waiting behind earlier Session-partition records. Kafka lag is sampled high offset minus processed offset, and all 32 partitions must drain. A short peak between samples may be missed. JSON bytes for the Inbox are the parameter payload of current-value writes. PostgreSQL WAL and on-disk bytes have different accounting.

The serving database is only read for preflight. Every measured mutation targets the newly created database, which is dropped after closing the actors. Topic and group absence are checked through the Kafka admin API. Data published with the [report](../../../docs/reports/minimal-session-storage-load-20261119.md) contain aggregates only.
