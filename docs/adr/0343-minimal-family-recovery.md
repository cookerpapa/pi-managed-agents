# ADR-0343. Preserve Conversation ownership in minimal Session recovery

The #52 performance candidate projects a native Inbox at its own Conversation
address. One physical Session writer can update several Conversations in one
native batch. Admit the whole batch under its existing Run and writer, persist
each changed Inbox once at its final ordinal, and publish acknowledgements only
after the transaction. A bounded moving checkpoint may represent empty Inboxes
by document identity and Conversation only. A pending Inbox in any lane keeps
its reconstruction suffix pinned. The single-root continuation benchmark's
history head measures root entries only; child answers and compaction markers
cannot change its count, cutoff or family completion signal.

Run the cold handoff acceptance on isolated resources before changing serving
storage or the lease model. The Kubernetes release hold in #48 still applies.

The Inbox row keeps its native creation sequence. After a settled predecessor,
a cold Projector loads prior document routes through that predecessor's sequence
in its opening query. The settled Run head does not copy the child route list.
Native Submission request identities are scoped to the physical Session and
Conversation. The cloud API's public idempotency UUIDs are unchanged.

This decision covers the Inbox and root continuation head. A Projector checkpoint
is not yet a complete Worker checkpoint. The new storage still needs a strategy
for Conversation ownership/ancestry, outstanding Tasks and Submissions, mounted
documents and original Tool/host-call bindings at takeover.
