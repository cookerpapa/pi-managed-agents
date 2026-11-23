# Candidate context status and the prune admission gate

## Verified reads

The ordinary root acceptance creates a private tenant, a new Session and two Runs using the HTTP Session API. A real Cube Guest executes one Bash command and a cold Worker continues the second Run after the Session API and Projector restart. The role serving the Session API has zero SELECT privileges on the old Pi history tables. With the former status reader, both authenticated status requests returned HTTP 500. With candidate message reads, the requests returned HTTP 200 and estimated 224 and 249 tokens respectively, matching the estimator applied to the five and seven projected Entries. The model is simulated.

The candidate reads the chosen model and visible Entry ranges in one repeatable-read snapshot. It uses the existing Pi context estimator for model usage baselines, structural estimates, context edits and tool results. The active suffix includes the latest compaction summary and the earlier entries retained by its floor. Native children follow their frozen parent Entry bound. A continued child follows its previous invocation. Standalone Forks also respect the frozen source commit ceiling. The PG read helper uses the existing schema's local stream or the installed Fork layout's directory. These changes add no stored fields or per-commit projection work.

The named executor integration exercises root and child status over HTTP alongside provider usage. A separate PG check covers a fresh sibling, a branch, and a continuation whose caller and history parent differ. With 4,000 older entries and a 75-message continuation, the active context took two 64-entry pages and matched the estimator over the known visible sequence. This counts pages returned to the reader, not disk I/O or fleet latency. Root Run selection for input, status and manual compaction now excludes child invocation rows, including a child with an unknown outcome when the next root is queued.

## Reproduced write blocker

The isolated gate installs the real candidate prefix SQL, shared-history Fork layout and delegated Run layout in one disposable PG schema. It claims a root Run, stores three final entries and closes the Run through the candidate finish function. It forks at the second answer and prunes the original at the first. Both histories read correctly, with context estimates of 201 tokens for the trimmed original and 301 for the Fork. The Fork accepts and claims a new Run.

The original Session keeps its public ID while the existing prune adapter switches its active stream ID. Two attempts to enqueue a new Run on the original both fail with PostgreSQL SQLSTATE 23503:

| Value passed as runs.session_id | Existing foreign key that rejects it |
| --- | --- |
| Original public Session ID | admitting_session references fork_sessions(tenant_id, stream_id) |
| New physical stream ID | runs_tenant_id_session_id_fkey references sessions(tenant_id, id) |

This is a matched-schema admission probe. The independently deployed storage is unchanged. The probe rolls back each tentative admission, removes its temporary schema and exits with status 2 while public admission is blocked. Quiescence does not repair these conflicting references. The ordinary root fixture also currently requires public and physical IDs to be equal when it accepts a native publication.

## Decision pending

One option is to retain runs.session_id as the public queue owner, have one binding from the public Session to its current physical stream, and pin the chosen stream with the execution owner when a Run is claimed. The Fork directory continues to protect old streams and their frozen ranges. The existing sessions.pi_session_id, lease scope, message coordinates and directory references should be evaluated before adding a column. Kafka currently selects a partition from the physical Pi Session ID. Decide explicitly whether that identity stays stable when a storage stream rotates, so the storage change preserves control ordering and stale-writer fencing. Decide how quiescence, queued work and handoff fence a stream switch before altering admission or writer checks. Test continuations on both sides of a prune and late old-writer records after that decision. Whole-storage publication is gated on these checks.

## Reproduction

```sh
node scripts/run-with-test-postgres.mjs -- node --import tsx scripts/experiments/minimal-session-storage/check-matched-session-identity.mjs
PI_CLOUD_RUNTIME_LOAD_CHECK=1 node --import tsx scripts/experiments/minimal-session-storage/run-retained-family.mjs /tmp/normal-root-status.json --normal-root
PI_CLOUD_DELEGATED_BROWSER=1 npm run test:transport
```
