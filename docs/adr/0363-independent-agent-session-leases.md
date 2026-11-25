# ADR-0363. Independently claim delegated Sessions on the segmented candidate

The #52 segmented history runtime schedules the first Turn inserted by named delegation through the same Worker queue as a root Turn. Both a fresh child and a history-sharing child own a stable physical Session identity. Management ancestry, inherited history and Workspace grants are independent of Writer ownership. Run order stays local to each Session. A Worker may own multiple Sessions within its capacity.

Use the existing PostgreSQL lease authority, Worker boot registration, direct heartbeat and monotonic fencing epoch. Claim the ready Turn, grant its physical Session lease and bind the writer in one PG transaction. Preserve the physical identity across history-flow rotation. A timed-out writer cannot renew. Reassignment requires the previous writer's ordered closure. A parent finishing or losing its Worker does not close its children's leases. Completion is correlated to the specific child Turn.

This is an opt-in integration on the segmented storage candidate. Existing single-physical-Session child lanes continue to use the serving path. Slot exhaustion and the #48 release decision remain separate.
