# ADR-0338. Bind the first Workspace observation with a new root Run

The #48 short-Run trace measured two successive Worker-authored Pi transactions before the first user input. One creates the environment baseline. The next saves the Run configuration and configures the Pi Agent. The first observation has no earlier environment to compare and emits no change message.

For a prompt opening an empty physical Session, capture that baseline in the existing public Pi transaction that binds the root Run. Adopt the in-memory observation only after the transaction succeeds. Pi still creates its root Conversation first. A crash before the binding leaves the ordinary unbound root. A crash after the binding leaves both documents available to the replacement Worker. User input admission and model execution follow the committed binding.

Existing Sessions, Forks, child Conversations, resumption, manual compaction and actual environment changes continue to use their existing observation and recovery boundaries. Each published native transaction retains its own sequence, deduplication proof and atomic projection. The audit compares this Worker change separately from the bounded PostgreSQL head lookup. The #48 deployment gate applies.

[Implementation, failure acceptance and measurements](../reports/projector-pg-review-20261118.md).
