# ADR-0354. Named children and executor-owned reply delivery

A parent creates a named child within its physical Session. The first accepted
creation freezes its task, context anchor, effective Workspace grant and native
owner. Repeated creates compare the requested task and normalized logical
options. An exact match can return the existing status or original result.
A conflict returns the existing UUID. A continuation keeps its stable agent
identity and a Fork gets a new name namespace. Pi task IDs remain invocation
and recovery identities. Each boot mailbox uses its own operation ID.

The matched candidate runs an independent subagent executor. The Session
Projector offers admitted intents asynchronously after the existing ordered
dispatch grant. An unaccepted offer may be lost. The Worker returns a bounded
wait result and the name to inspect; it does not reissue creation on timeout.
PG commits the child and one delivery record per request atomically before
HTTP acceptance. The executor uses the existing Worker management interface
for native placement and Kafka for replies after settlement or a question.
A restart rechecks PG and retries only idempotently addressed placement and
delivery. An acknowledgement lost after publication can duplicate a packet.

This reuses PostgreSQL unique constraints, row locking and the transactional
outbox pattern already used for control work, and the existing Kafka publisher.
No second agent scheduler, durable broker or Session-log consumer is needed.
The historical candidate and the serving schema retain their own release gates.
Only named-executor compositions can discard replay requirements specific to a
delegation return address. Stream and Worker-handoff recovery remain separately
validated.
