# ADR-0345. Retain Pi agent configuration in the candidate message stream

The owner selected Pi Durable's native `asOf` Fork semantics for `pi.agent`.
Use the existing candidate `messages` table for immutable configuration versions.
The scoped native commit sequence and ordinal identify every row. Entry ID uniquely
identifies an Entry. A configuration row carries its native document ID and
Conversation ID, and occupies its own write position in the same transaction and
partition checkpoint as the other writes in that commit. Native writes and Kafka
envelopes keep their existing identities. Project complete configuration values
on change and a retirement marker on retirement. Keep the chat pager, model
Entry suffix, message counters and Fork history directory on Entry rows.

Select the last configuration at or before the anchor Entry's **commit sequence**,
including all writes of that atomic commit. An independent branch retains the
selected version through its stream metadata. Nested branches and prune use
the physical source and native Conversation of the selected Entry. Deletion
and collection retain configurations still reachable by a live stream or its
inherited history. The next model call applies current execution policy.

This decision covers the fixed `pi.agent` definition in the #52 prototype.
Use the existing Pi configuration and cold-read contracts and the existing PG
Fork directory. Verify against the paired native histories in the November 22
release-gate report, plus cold continuation, redelivery and collection.
