# ADR-0334: Reduce round trips within an ordered native projection

Status: implemented on the performance branch, under review. Publication is deferred.

Issue: [#48](https://github.com/cookerpapa/pi-managed-agents/issues/48).

## Decision

Reuse admitted root Run metadata and consolidate the native batch's preparation
reads and independent writes. Keep the existing Run, writer and native Session
locks, commit fingerprint, contiguous sequence checks and post-COMMIT adoption.
Ordered Kafka identity supplies the lookup keys; it does not revoke SQL already
issued by a former Projector or replace the durable authority check.

Native metadata is read after acquiring the Session lock, in a subsequent SQL
statement. Combining a blocking lock and later reads into one statement would
retain a pre-lock snapshot. Metadata is read once per batch, including the exact
duplicate receipt, touched current state and parent origins. Root display routing
reuses the admitted Run; only child Conversations require another lookup.

Use PostgreSQL data-modifying CTEs and RETURNING for writes within this one
transaction. Newly inserted commit timestamps come from RETURNING, never a table
read that cannot see a sibling CTE. Independent current checkpoints and their
indexes can be written together. Never modify the same row twice in a statement.
Pi document interpretation stays in TypeScript; no stored workflow or second
checkpoint authority is introduced.

The existing PG/Kysely adapter and PostgreSQL's native statement facilities meet
this bounded requirement. Adding a new database driver, cache, scheduler or log
would enlarge the correctness surface without evidence of benefit. The relevant
[PostgreSQL WITH contract](https://www.postgresql.org/docs/17/queries-with.html)
specifies shared snapshots and explicit RETURNING dependencies.

## Persistence and recovery

The cloud admission query locks the root Run, the writer and the physical Session
in that order. A separate statement reads the fresh post-lock native head and
fingerprint. It looks up current documents, Tasks and Submissions in the owned
index. A Fork consults its shared Entry directory for missing IDs within its
inherited range. Cold and replay callers still verify the original scopes and
fingerprints. A failed transaction discards its in-memory draft.

The native mutation writes its receipt, retained history, current checkpoints and
their indexes with explicit RETURNING dependencies. Row counts detect conflicting
record identities. When this ends the batch, the outer Projector can include
the partition cursor in the same statement. A conflict rolls back the cursor too.
Mixed commits keep their required accounting, display, document and image work in
the same transaction. The first uncheckpointed live offset remains protected.
Each original native sequence keeps its own transaction and replay identity.

Normal completion checks the native head once, under the physical Session lock.
The terminal event, Run closure, pending cancellation, Session state, optional
writer retirement and recovery position update together. Later reads of history
headers and child state see the completed mutations in the same transaction.

## Validation gate

The owner authorizes tests against the development production database using
owned fixtures and cleanup. Measure SQL calls and repeated complete-Run throughput
with the same synthetic workload; no paid models. Tests run serially. Check
duplicate/conflicting commits, rollback, stale identities, child views, current
state, Fork history and live recovery. Do not declare success from statement
count alone. If throughput stays in the low tens with little material gain,
report that result and discuss further architecture work before extending scope.

This decision does not authorize a routine release. Temporary controlled
experiments must restore the original serving configuration.
