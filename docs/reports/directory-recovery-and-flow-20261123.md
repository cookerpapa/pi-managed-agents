# Run-linked Flow and working directory recovery

[Portable measurements](directory-recovery-and-flow-20261123.json) and [scope decision](../adr/0356-run-flow-head-and-directory-stop.md).

## Workspace recovery

A private matched Session API, the Worker turn runner and the Projector exercised an actual fixed Guest in an owned Cube VM. The fixture staged the candidate Guest executable only in that VM. Native commits used an isolated RF3 log. The scoped HTTPS completion used a real boot-scoped Kafka reply topic. A deterministic model proposed Bash. Test resource registration and the restricted picker listing adapter belonged to the fixture. The listing adapter checked the actual directories inside the VM.

The original Bash had an observable marker outside its selected directory. After that directory was removed, the marker was absent, the model was called once, and PG retained the canonical error and failed Run seal. Chrome recovered steer and follow-up in the composer, selected a different directory on the same machine and submitted a new Run. A cold Worker loaded four prior Entries. The new Bash appended exactly one line to the marker, containing the selected directory. The model received its result and completed the second Run. Cleanup of the VM, fixture-owned topics, groups and temporary database was verified.

A separate native recovery test cuts the captured commit sequence immediately after the error entry. The replacement reads that exact prefix and ends the same Run with zero model requests and zero new tool commands. A PG test holds an already queued Run, rejects rebinding while it remains queued, withdraws it and accepts its input on the replacement Workspace. The real Cube case covers reselecting a directory on an intact Workspace.

## Flow layout

The public Session stores its own writable flow, its history head and its current Workspace and directory. Enqueue inserts the Run and advances the head in one transaction. The first local Run links to the inherited head, so an empty Fork already protects its base. An explicit move of two adjacent unstarted inputs updates their provisional links under the Session lock. Once claimed, history links are fixed. Queued work retains its original environment snapshot. Pruning an idle window rotates its writing flow and points at the selected sealed Run. The guarded message append rejects old flow/writer identity.

Local membership is checked using the flow and its ordered local positions. To verify an anchor in an older flow, the query jumps to the first local Run's predecessor. Root queue positions and accepted history agree because each writable flow has a single append head, settled positions are fixed and pruning rotates the flow. The admission gate installs the delegated Run schema, verifies that children aren't separately claimed and exercises their retention during shared-root cleanup. Cold continuation uses the latest retained floor across predecessor Runs.

Foreground deletion removes the window and persists its old head as a cleanup job atomically. A collector follows predecessor links in transactions of at most 256 victims, checks the indexed Session heads and successor links and deletes unreachable Run bodies. It can rediscover an interrupted job. The race test observes an actual PG blocker while deleting a source with a concurrent Fork. The new empty branch survives. The scratch cleanup-job table represents the existing control outbox role and carries no history authority.

## Isolated PostgreSQL read and GC workload

One 1 KiB text contribution per Run, periodic simulated compaction, warmed PG cache on this development host. Latest-page queries read one preview from each of 24 Runs. The cold measurement constructs a new Pi MemoryStorage from the visible suffix on each query. It does not include Worker startup, a provider call or an operating-system cold cache. Values below are wall time in milliseconds, P95 / P99. Admission and empty-window deletion each have 100 samples per size. Deep source-window detach is separately measured once per size.

| Runs | Latest Run | Latest 24 | Fork at midpoint | Cold context | Admit | Delete empty branch |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 1.79 / 1.85 | 2.23 / 2.51 | 4.08 / 4.11 | 19.40 / 22.11 | 4.38 / 7.34 | 4.16 / 6.05 |
| 10,000 | 1.78 / 1.92 | 2.54 / 2.65 | 4.07 / 4.26 | 31.23 / 31.83 | 4.09 / 6.59 | 3.84 / 6.25 |
| 100,000 | 1.67 / 1.80 | 1.83 / 1.94 | 4.90 / 8.92 | 3.84 / 5.63 | 5.40 / 10.41 | 5.54 / 6.00 |

The 100,000-Run first-page plan visited one Session, 24 Runs and 24 message rows. At eight simultaneous readers first-page P95/P99 was 60.99 / 65.06 ms. The cold sample loaded 48 Entries at this size. The different suffix sizes at 1,000, 10,000 and 100,000 Runs matter when comparing cold-read times. A separate 10,000-Run chain crossing 1,000 distinct flows took 65.01 / 68.36 ms to fork at the oldest Run, while its latest page took 1.54 / 1.91 ms.

At 100,000 Runs, detaching the source took 3.33 ms. Collecting its 50,000 exclusive Runs took 14.32 seconds. Deleting the remaining branch then collected the shared prefix of 50,000 in 13.64 seconds. An earlier isolation without the partial unsettled-flow index took 25.29 ms for the source detach. The index covers queued work, live work and unfinished closure.

## Projection cost

Each Kafka arm replayed the same 1,024-Run trace with 20 tools per Run, 191,488 raw records and 41,984 visible records. The 32 partitions shared 12 projection connections. Simulated tool acceptance took 50 ms. All final lags were zero. The timed window excludes admission, so its measured cost is reported separately above.

| Final indexed schema | Baseline visible records/s | Flow visible records/s |
| --- | ---: | ---: |
| Backlog, samples | 888.9 / 939.0 | 690.1 / 876.1 |
| Backlog, combined | 913.3 | 772.1 |
| Paced, including drain | 436.8 | 410.4 |
| Drain after last input, seconds | 7.45 | 13.64 |

The final backlog difference is −15.5%, and the paced difference is −6.0%. A preliminary Flow layout without the unsettled-flow index recorded 797.3 / 812.5 in backlog versus 863.5 / 703.7 for its baseline. The machine varied between and within studies. These measurements do not establish sustainable fleet capacity.

A separate isolated test captured the exact PG statements and parameters from 32 ordered partitions with 128 Runs. It replayed 12 calls per partition per block and rotated three schema variants. Each variant executed 11,008 calls and verified all 2,560 tool results. Baseline measured 684 visible equivalents/s and 17.31 MB of WAL during the timed blocks. Flow measured 633 and 19.61 MB. Omitting the unsettled-flow index recovered part of the difference, reaching 657 and 19.06 MB. Table-update statistics in the JSON include fixture preparation. This control isolates PG replay from the emitter, dispatch and native fold. The extra WAL and indexed updates explain part of the cost. The varying PG and Kafka grant times do not isolate a single cause for the entire mixed-workload difference.

## Verification

The RF3 transport run passed 41 checks. The targeted Worker, Guest, control-plane and migration run passed 90 checks, the three UI suites passed 40, and the Guest admission suite passed 11. Suite counts overlap. Workspace type checking, runtime policy, role image source closure and documentation checks also passed.

## Release boundary

The confirmed directory change is implemented in the serving source and tested in isolation. Flow is a storage experiment. The matched gate that previously failed both foreign keys is green on this layout. The public tree API, per-Entry Fork behavior on the old store, full native cold/handoff reads, retained attachments and resource cleanup still require a coordinated Flow cutover. Existing deployment data has not been converted. Preserve the existing release gate while deciding the read/write tradeoff and testing the full serving composition.

## Reproduction

Use fresh output paths and run the resource-heavy checks serially.

```sh
node scripts/run-with-test-postgres.mjs -- node --import tsx scripts/experiments/minimal-session-storage/check-matched-session-identity.mjs
node scripts/run-with-test-postgres.mjs -- node --import tsx scripts/experiments/minimal-session-storage/run-flow-history.mjs
PI_CLOUD_RUNTIME_LOAD_CHECK=1 node --import tsx scripts/experiments/minimal-session-storage/run-retained-family.mjs /tmp/directory-web.json --directory-recovery-web
PI_CLOUD_RUNTIME_LOAD_CHECK=1 PI_CLOUD_FANOUT_SUITE=grants-million-day PI_CLOUD_COMPARE_FLOW_HISTORY=1 node --import tsx scripts/experiments/minimal-session-storage/run-cpu-placement.mjs /tmp/flow-mixed workspace-fanout
node scripts/experiments/minimal-session-storage/summarize-tool-loop.mjs /tmp/flow-mixed
node --import tsx scripts/experiments/minimal-session-storage/model-settlement-rate.mjs 4 flow-sql
```
