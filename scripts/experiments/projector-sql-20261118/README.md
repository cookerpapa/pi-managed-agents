# Projector SQL diagnosis

This is a bounded operator experiment using the maintained application and an
owned simulated provider. It changes no durable authority, schema or Kafka wire
contract. It temporarily replaces the Session runtime's source mounts, requires
an idle user workload, holds the existing release lock, and restores the original
Pod template after normal success or failure. Run every case serially.

```sh
PI_CLOUD_PROJECTOR_SQL_DIAGNOSTIC=1 node --import tsx \
  scripts/run-projector-sql-diagnostic.mjs /private/observation-12 12 cpu
PI_CLOUD_PROJECTOR_SQL_DIAGNOSTIC=1 node --import tsx \
  scripts/run-projector-sql-diagnostic.mjs /private/observation-32 32 cpu
PI_CLOUD_PROJECTOR_SQL_DIAGNOSTIC=1 node --import tsx \
  scripts/run-projector-sql-diagnostic.mjs /private/observation-server 12 server-duration
PI_CLOUD_PROJECTOR_SQL_DIAGNOSTIC=1 node --import tsx \
  scripts/run-projector-sql-diagnostic.mjs /private/cpu-calibration 12 cpu-only
```

The source checkout must match the serving files. The optional `reader-candidate`
argument explicitly overlays the working tree's Kafka reader after checking the
serving reader against its recorded Git revision; this was used to verify the
idle-EOF correction. It is not a permanent release or a compatibility mode.
Pool sizes other than twelve are also temporary interventions, not recommended
production defaults. The operator does not restart PG or retire compute/storage.

The probe wraps the public pg Pool/Client methods, preserving Promise/callback
results. SQL parameters, result bodies and connection strings are never emitted.
Statement templates redact literals. Data is bounded per record and statement
catalog; any truncated evidence is explicit. Observations use stdout and private
files, not native history, Kafka or a new telemetry service.

`cpu` uses Node's in-process Inspector profiler without opening a network port.
`cpu-only` calibrates that result without pg wrappers, per-record async context,
SQL timing logs or PG activity sampling; it keeps the serving pool size.
The installed signal handler is verified before stopping the sampler. A profile
contains code stacks, not argument values. `server-duration` sets PostgreSQL
logging options only on this temporary projection pool: `log_duration=on`, with
statement text and parameter logging disabled. Those options disappear when the
observed pool closes. PG separately reports Parse, Bind and Execute durations.

```sh
python3 scripts/experiments/projector-sql-20261118/analyze.py /private/observation-12
python3 scripts/experiments/projector-sql-20261118/analyze-cpu.py /private/observation-12
python3 scripts/experiments/projector-sql-20261118/analyze-server.py /private/observation-server
```

Interpret query durations as client call-to-callback intervals, not pure server
execution. The analyzer unions overlapping query/acquisition spans rather than
double-counting them as wall time. V8 stack samples are not whole-system CPU or
PG samples. PG activity distinguishes `ClientRead` from active execution and WAL
waits. Millisecond timestamp matching leaves ambiguous server log events; their
per-query attribution is excluded, and aggregate differences must not be labelled
pure network latency.

`observation-private.json` records the original template, exact source hashes,
scope, workload exit and restoration. Raw reports contain test resource identities
and normalized SQL; keep the directory private. If the operator is killed before
its `finally` block, inspect this receipt, finish the existing load driver's owned
cleanup, then restore the exact original template and remove only this diagnostic
ConfigMap. An unrelated template edit deliberately prevents automatic overwrite.
Do not discard the receipt while restoration or owned cleanup is uncertain.

The [research report](../../../docs/reports/projector-sql-diagnosis-20261118.md)
retains failed setup samples separately from valid measurements. No paid model
requests are used by this experiment.

## Ordered projection comparison

The `projection-tail` workload in `scripts/run-runtime-load.mjs` produces the
unmodified control. From the source candidate checkout, the commands below run
the equivalent full workload. Set `PI_CLOUD_RUNTIME_DIRECTORY` to the development
runtime directory when using a separate worktree. Each command owns its private
output directory and must complete its restoration before starting another.

```sh
PI_CLOUD_PROJECTOR_SQL_DIAGNOSTIC=1 \
  node --import tsx scripts/run-projector-sql-diagnostic.mjs /private/grouped-one 12 projection-candidate clean tail-load
PI_CLOUD_PROJECTOR_SQL_DIAGNOSTIC=1 PI_CLOUD_DIAGNOSTIC_PROJECTORS=2 \
  node --import tsx scripts/run-projector-sql-diagnostic.mjs /private/grouped-two 6 projection-candidate clean tail-load
```

Omit `projection-candidate` for the stock source. `clean` omits SQL spans and
CPU instrumentation. `generic-plans` is a comparison limited to the candidate's
dedicated projection pool. `fault-load` runs the maintained recovery workload
under a single clean owner. A counted candidate run uses `projection-candidate`
with the ordinary twelve-connection diagnostic. `summarize-tail.py` reads the
named November 18 private evidence directories and exports only aggregate
measurements, source digests and restoration results.

For the later WSL cases a separate observer timed four SHA-256 hashes of the
same 1 MiB buffer on the host every two seconds. The Windows observer sampled
`Win32_PerfFormattedData_Counters_ProcessorInformation` for `_Total` at roughly
the same interval. The report joins those observations by each timed stage's
wall-clock bounds, retains sample counts, and leaves earlier cases unsampled.
Neither observer changes system power or performance settings.

## Synchronized native live progress

Use `12 projection-candidate live-burst` to compare 192 simultaneously released text requests with one Projector, 32 Kafka partitions, a 32-record poll (waiting only for the first record), twelve projection connections and two Workers. Candidate observations mount the paired Worker flush implementation. The operator records both workload templates before applying the temporary source and restores both at exit. Set `PI_CLOUD_PROJECTOR_DIAGNOSTIC_REPETITIONS` for serial repetitions. The runtime load driver can also run `--mode live-burst` on a deployed pair.

Pass each private load `report.json` to the summary tool. It matches synthetic provider completion to this Run's terminal PG COMMIT response and collects the consumer group's actual lag.

```sh
python3 scripts/experiments/projector-sql-20261118/summarize-live-progress.py output.json \
  /private/baseline/load/report.json /private/candidate/load-1/report.json /private/candidate/load-2/report.json
```
