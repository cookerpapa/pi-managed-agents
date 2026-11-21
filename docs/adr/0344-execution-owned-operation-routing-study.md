# ADR-0344. Address outstanding Workspace operations from the execution side

Status: isolated #52 prototype. The serving protocol and #48 release hold stay in effect.

The Worker publishes a complete native Tool intent and an operation ID, with its original reply reference.
A cancellation or fault probe identifies the operation within its authorized Run and Conversation.
Only execution selects the Guest binding and the original writer used for Guest fencing.
The existing pending reference records the operation's Session partition offset. Execution records
one immutable binding boundary per Run, Conversation, partition and resource selection. At a binding
change it synchronously commits the effective offset and original writer before any command can use
the new target. Ordinary bound intents retain the existing projection transaction policy. Cached
boundaries serve hot dispatch. A fresh owner loads the applicable boundaries from PG. Replaying an
older intent always uses the target effective at its original position, even if asynchronous candidate
projection was lost and the current selection has changed.

The candidate stores a cancellation locator alongside the existing Run only while required for
replay: a cancel and terminal Task may share a native commit that removes the public reply waiter.
A subsequent distinct settlement or ordered Run closure clears the locator. Binding boundaries are
retained through the active Run and removed at closure. The prototype cold-read role cannot read the resource
selection table. Original reply topics, deadlines, native Stop settlement and the Kafka group grant
before external dispatch retain their distinct purposes.

This supersedes the Worker-supplied binding in the prototype studies of ADR-0343. The serving comparator
handles public Run/lease, native presentation and ordered closure. Execution-side candidate projection
and routing use the same admitted partition callback. The isolated prototype selects its decoder at composition;
the deployed decoder still accepts only its original protocol. Production Workflow, upload and readiness API
migration remain separate. [Scope and fault evidence](../reports/workspace-operation-routing-20261121.md).

## Batched history and continuation controls

In the isolated candidate, immediately available stable Entries from the same partition may share
one PG projection. Stop before an operation, input mutation, seal, control change or terminal
boundary. Keep the original per-record grant before external dispatch. In the dual-store comparison,
native admission must succeed for each record before the secondary projection batches it; publish
live notifications and resolve Kafka delivery only after the candidate batch returns.

A preferred Worker reuses immutable history under the settled version. New Run environment facts
are frozen at admission under the same selection lock as an idle edit. A successful normal
predecessor supplies the previous snapshot in the claim. The native named prompt section
sets the current directory and distinguishes a directory move, a confirmed change of compute
generation within retained storage and selection of a different Workspace. The logical generation
is the last confirmed change, independent of availability polls. User-visible context carries
the consequences; Guest routing and provider identities remain at execution.
This control covers successive normal Runs. Recovery within an existing Run retains the
original pending operation targets under the protocol above.


For the continuation Guest acceptance, the admitted environment also selects the execution
side's attachment and tool root. A cold route may read that Run snapshot once to install
its original route boundary. The physical target stays private to execution. The Worker
sends the native command, operation identity and scoped reply position, with no path override
added to its Tool transport. Verify actual cwd and distinct same-path Workspace content
through the original HTTPS/Kafka reply and Pi's canonical Tool Result.

### Fan-out measurement boundary

In the isolated dispatch assay, use warm, pre-bound routes and one independent socket per
logical target. The comparison keeps the real PG projection and per-command Kafka group
grant. Reserve bounded count and bytes before obtaining that grant. A target admits at
most one outstanding request, while other targets can overlap their receipt waits. A lost
assignment prevents any new send from that assignment; an already issued call is never
retried. The assay handles execute intents only. It does not select a production policy
for ordered cancellation, closure, persistent handoff or device reconnection.

### Partition-local grant experiment

In an isolated topic generation, a consumer may opt into granting the immediately
available, contiguous prefix of up to 32 ordinary execute intents. Validate and
project each intent and reserve its distinct target and byte budget first. A
duplicate target, unavailable reservation, non-execute record, control change,
cancel, seal or ownership change ends the prefix. Confirm its last offset + 1
once before any sends. On rewind, the broker-confirmed boundary suppresses the
entire previously granted portion, including any unsent tail. After uncertain
confirmation or ownership loss the old owner cannot launch more work. The
accepted tradeoff is up to the actual prefix length of uncertain operations per
interrupted partition grant. The ordinary per-record shipping path stays the
default. Partition-count comparisons create separate generations so the Session
key, replay positions and binding boundaries stay consistent within each trial.

### One PG round trip per tool-intent prefix

In the isolated #52 comparison, the existing grant prefix may project its
already validated, fresh execute intents in a single call to the existing
partition projection function. Limit this to warm original bindings and distinct
Runs and targets. Keep one recovery reference per operation. The single PG
transaction commits all references and the partition head before the unchanged
Kafka grant. A failing member rolls back the batch. If PG observes a cancelled
or obsolete owner, isolate it on redelivery before allowing other sends.
Cold starts, covered records and control barriers retain their individual
admission/replay paths.
A native ToolTask may co-commit its own known
`pi.live` slot's pending-to-running change. Fold that change intact and
install the new view after PG acknowledges the batch. Commits carrying
presentation events, input receipts or other native transitions keep their
existing delivery boundary. [Scope and measurements](../reports/pg-tool-intent-batch-20261121.md).
