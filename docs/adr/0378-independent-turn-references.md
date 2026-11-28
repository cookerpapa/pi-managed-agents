# ADR-0378. Bind operational records to their independent Turn or mailbox input

The isolated #52 installation has one physical Turn store. A matched, drained
migration attaches execution target bindings, schedule firing, Skill selections,
environment validations and issue-job references to that store. Preserve tenant
scope and the original deletion actions. An open target binding continues to
protect its Turn. The old public queue and its superseded control records reject
writes. Shared readers that still select their relation shapes can continue to
do so during integration.

A pending upload belongs to the externally admitted input. Its stable identity
survives promotion to a successor Turn. Validate upload scope and lock tickets
in the same transaction as admission. A canonical placement transfers retention
to history. A returned draft retains its ticket until its owner acknowledges
the return. Native placement and queue edits keep their existing ownership.

Use the existing schedule deadline listener. A fire commits the schedule cursor,
frozen Turn and first mailbox input together. Skill selections are fixed for
the Turn and survive Worker replacement. A pre-upgrade Turn keeps the empty
selection that its original Worker saw.

Only the Projector may opt into transaction-local asynchronous commit for
reconstructible projection. Today its boundary transaction explicitly requests
synchronous commit, including automatic successor creation. API admission,
claims and resource-control writes retain synchronous durability. The first
Kafka publication of a still-active writer is a reconstruction position for
the Projector’s volatile native handoff state. It is independent of how far
the partition has already projected into PG.
