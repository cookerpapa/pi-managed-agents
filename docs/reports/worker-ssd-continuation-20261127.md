# Incremental Worker SSD continuation

The isolated #52 Worker checks its parsed memory cache, then its private SSD, using the PG-confirmed predecessor version and native history bounds. A miss uses the ordinary PG history reader. A fresh Turn still gets its model, Workspace, external input queue and writing authority from their existing owners. [Decision](../adr/0371-worker-local-ssd-history.md).

## Operating the candidate

Set `PI_CLOUD_WORKER_HISTORY_CACHE_DIR` to the exact private directory of a Worker, for example `/var/cache/pi-managed-agents/worker-a`. The same directory must remain available on restart to reuse existing disk data. By default the Worker uses a subdirectory named for its registration ID under `$XDG_CACHE_HOME/pi-managed-agents/worker-history`, with `~/.cache` as the XDG default. `PI_CLOUD_WORKER_HISTORY_CACHE_ROOT` overrides that parent when assigning a directory per registration. Separate Workers should receive separate directories outside sandboxes and Workspaces. The Worker creates its directory with mode 0700 and its SQLite file with mode 0600.

The `historyCache` constructor options include `diskDirectory`, `diskMaxBytes` (default 20 GiB) and `diskTtlMs` (24 hours after a completed snapshot). Use `diskDirectory: false` for a memory-only Worker. The RAM defaults remain 256 MiB and five minutes. Entries are eligible from 256 KiB. Each local checkpoint is scoped to tenant, logical Session, writable Flow and terminal version. A snapshot may exceed the RAM budget and still fit the disk budget. At most 4,096 disk versions are indexed per Worker. Separate Flows bootstrap their own selected prefixes. After pruning or Session deletion, old local bytes can remain until expiry or eviction; offline directories are swept on reopen. PG controls which version a new Turn can read. SQLite grows on demand and limits its main database pages; a rollback journal needs temporary space during a transaction.

A matching parent version permits an incremental local transaction. The Worker sends the new Entry bodies to its SQLite I/O thread, deletes rows below a new compaction head, updates the version index and commits. The read path validates the version, native coverage and individual payload checksums. Cache I/O is batched, and under pressure optional writes may be skipped. Normal terminal publication proceeds independently of SSD completion. Losing the disk or terminating the process with a partial transaction leaves PG authoritative.

The management socket reports each available version's tier, size class and expiry. A memory eviction can downgrade an offer to SSD. Reconnect and restart rebuild the directory from local version metadata. An available memory holder is selected first, followed by an available disk holder. A large history may wait up to the existing two-second preference window for an occupied holder. Stale or missing hints use the existing ready queue.

## Acceptance

The PG fixture uses independently leased Workers, the actual Pi context builder and a deterministic local provider. It seeds and appends valid PNGs in user and tool-result entries, checks the full model-context digest, and counts bytes returned by PG. The test advances the cache clock by 31 minutes while PG leases and socket scheduling run in real time. It also compares an immediate RAM hit, a new Worker identity opening the same disk, a clipped Fork, pruning, compaction and expiry beyond 24 hours. These are development-host filesystem measurements; hardware SSD bandwidth and provider upload latency are outside this benchmark.

A separate failure test kills a process while its SQLite transaction is staged, reopens the directory, and reads the last committed version. It then damages an Entry and checks that the ordinary PG reader returns the original history. Tests cover private-directory failure, disk quota eviction, stale scope/version and scheduler tier preference.

## Large-context results

Three serial trials on the development host used 104.13 MiB of serialized image history. PG and SSD loads produced the same full-message digest in each trial.

| Measurement | Median | Range |
| --- | ---: | ---: |
| PG cold context construction | 648 ms | 397–877 ms |
| SSD context construction after RAM expiry | 676 ms | 431–692 ms |
| Historical body bytes read from PG on disk hit | 0 B | 0–0 B |
| New Entry payload appended to SSD | 530 B | 530–531 B |
| Linux write accounting for the SQLite append transaction | 90,112 B | 90,112–90,112 B |
| Linux write accounting for the disk read and the following append | 122,880 B | 122,880–122,880 B |
| Net change in main database size on that append | 0 B | 0–0 B |

Bootstrap stored 109,190,022–109,190,023 bytes of Entry JSON in each trial. After the 31-minute clock advance the disk reader returned that prefix with zero historical body bytes read from PG. Restarting a Worker on the same directory and reopening a Fork also returned zero PG body bytes. After compaction the disk read returned 836–837 bytes and no old images. Linux write_bytes measures writes charged to the local I/O thread, including SQLite pages and its rollback journal. The net file growth was zero because pages had spare capacity. SSD times include verification, JSON parsing and delivery to Pi.

```sh
TSX_TSCONFIG_PATH=packages/control-plane/tsconfig.json node --import tsx --test scripts/experiments/minimal-session-storage/segment-disk-history.test.mjs
PMA_WARM_LARGE=1 TSX_TSCONFIG_PATH=packages/control-plane/tsconfig.json node scripts/run-with-test-postgres.mjs -- node_modules/.bin/vitest run scripts/experiments/minimal-session-storage/segment-warm-history.integration.spec.mjs -t 'verified ssd' --maxWorkers=1
PMA_SEGMENT_BROWSER=1 npm run test:transport
```

The matched production storage cutover remains under #48.

The RF3/PG suite passed 54 tests in 30 files. The eight cache and routing unit cases passed, including SIGKILL during a disk write. The independent Worker process handoff case also passed against the final disk-enabled composition. A 1,000-Worker wake regression used one PG listener and observed zero idle claim queries during its observation interval.
