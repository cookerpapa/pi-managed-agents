# ADR-0332: Projector identity-cache and live-state lifetimes

Status: accepted for implementation on 2026-11-17 (#46).

## Authority and cache

PG remains the only durable identity/closure authority. Cache complete admission
snapshots under exact tenant/physical-Session/writer/fence/Run identity. Use bounded
insertion-order Map recency for this local view; coherent business invalidation
and pending-read ownership remain in the adapter. This is not a distributed cache
or a new durable store, and needs no cache service or new dependency.

A snapshot contains the related open/closed and writer-cutoff information together.
LRU eviction removes that whole snapshot and its writer index. Closure updates
only the matching cached identities and invalidates matching pending loads, without
clearing unrelated active identities. A miss reloads PG; missing authority rejects
the candidate and unavailable PG never grants access. Coalesce identical loads.
Reset/rebalance and closure races cannot install a stale asynchronous result.

Record/byte limits apply to identity metadata; estimated bytes are not exact V8
heap/RSS accounting. Select/document defaults with measured cache memory and
reload tradeoffs. Recovery-prefix knowledge is separate from evictible admission
metadata: reloading an active identity cannot mistake an existing prefix for a
cold missing prefix or bypass first-record recovery validation.

The selected defaults are 32 MiB estimated metadata and 65,536 entries, whichever
is reached first. A forced-GC Node 24.12 measurement with realistic UUID scopes
plateaued at 28,976 cached identities and approximately 17 MiB extra heap as the
observed identity count grew from 50,000 to 100,000. Hot re-reads made no PG calls.
An empty display-holder cache is capped at 4,096 entries / 4 MiB estimated metadata,
in addition to idle expiry; pending text is excluded from that eviction policy.

## Live state

Preserve ADR-0244's Kafka suffix and PG settlement/handoff checkpoint ownership.
Do not add arbitrary LRU eviction of pending native live documents or unrepresented
display text. Expose their sizes and pending-event identities separately from the
identity cache. Remove rejected/replayed empty holders and release settled/closed
writer state only at its existing durable boundary. A late old closure cannot
remove successor state. Keep Inbox, tasks, reply/attachment references durable.

Audit completed/empty display-holder retention without truncating active tails.
Empty disposable metadata may have a bounded retention policy only where its
coverage can be safely reconstructed. Active working-set memory is workload-sized;
this issue does not claim a hard total-RSS cap. A future spill/rebuild policy needs
its own measured requirement and recovery design.

## Acceptance

Measure cache entries/estimated bytes, hits/misses, evictions, coalesced reads and
PG reloads. Test small budgets, many closed identities, active eviction, historical
prefixes, deleted Sessions, PG failure, closure/reset during a read and successor
isolation. Verify pending native bytes and display/event counts return to their
expected state after normal completion, Stop, handoff and crash recovery.
Run relevant PG/Kafka/live acceptance and memory/load checks serially; preserve
visible partial output, external-effect ordering and original user resources.
