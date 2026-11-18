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
