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
