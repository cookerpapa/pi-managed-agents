# Minimal Session lifecycle load test

This experiment measures the proposed Run queue, immutable messages and one current Pi Inbox per Conversation. It runs the actual pinned Pi Harness on two Worker Pods with a deterministic local model, using the maintained RF3 producer and partition-ordered consumer. A separate Projector process inside the Session runtime Pod uses a private topic and a disposable logged PostgreSQL database. Each role is invoked through kubectl exec. Source changes to these experiment files do not replace any application image.

The measured scope is a fresh physical Session with one root Run. The input driver uses direct SQL and an experiment channel for post-commit confirmations. Cloud scheduling metadata, HTTP/SSE, charging, tool sandboxes and restoration of the complete native Harness are additional costs of the maintained runtime. The benchmark's only tool returns a fixed observation. Each worker applies its in-memory native batch only after Kafka acknowledgement. Steer and follow-up pass through Pi's actual busy submission and placement. Only the explicitly labelled replay controls read a previously captured plain trace.

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

## Boundaries and evidence

`schema.sql` defines the three physical tables and constraints. An independent client request is durably registered before success. Two concurrent SQL claimers cannot skip an active predecessor in the same Session. Inbox removal and the `user` Entry are committed together. The separately delivered `work_finished` updates the admitted Run under its claim identity. The experiment does not reconstruct an in-flight Pi Task after Worker death. That requires the additional stored state and recovery contract in the full runtime.

Throughput counts confirmed Runs divided by elapsed stage time, including drainage. Latency starts just before registration and ends at the Projector's successful terminal PG callback. Native-ACK totals are sums per Run, so pipelined intervals can overlap. The final gap is timed between durable answer and terminal callbacks on the same Projector process. The finish transit metric also includes time waiting behind earlier Session-partition records. Kafka lag is sampled high offset minus processed offset, and all 32 partitions must drain. A short peak between samples may be missed. JSON bytes for the Inbox are the parameter payload of current-value writes. PostgreSQL WAL and on-disk bytes have different accounting.

The serving database is only read for preflight. Every measured mutation targets the newly created database, which is dropped after closing the actors. Topic and group absence are checked through the Kafka admin API. Data published with the [report](../../../docs/reports/minimal-session-storage-load-20261119.md) contain aggregates only.
