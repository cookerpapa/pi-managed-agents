# ADR-0357. Compare a frozen Session base with a moving head

Accepted for an isolated PostgreSQL experiment. Keep the ADR-0356 moving-head layout as the control. In the alternative layout the Session's base_run_id references the sealed Run at which its current flow starts. Find the latest local root through the existing tenant/flow/mailbox index, falling back to the base when the flow is empty. Exclude native child lanes. Admission still locks the Session, verifies the expected flow and idempotency identity, and inserts the new Run with its predecessor in one transaction. Only administrative Fork or prune sets the base.

Within one flow unstarted inputs follow queue order; once claimed their predecessors are fixed. Pruning rotates the writable flow. Before detachment resolve its current tip while holding the Session lock and save that tip in the cleanup job in the same transaction. Protect a live flow, a Session's frozen base and incoming predecessor references. A retry or concurrent fork must preserve all three.

Compare admission latency, write-ahead log bytes, empty and populated reads, and both orders of branch collection in disposable PG. This experiment does not change the serving schema, Kafka ordering or the release gate.
