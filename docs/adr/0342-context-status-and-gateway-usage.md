# ADR-0342. Separate context status and provider usage

ADR-0355 supersedes the atomic accounting paragraphs below for the matched candidate.

Accepted for the performance candidate, with publication under the existing #48 gate.
The authenticated read-only Session API exposes two independent commands. Status
reads the selected Conversation's active native context estimate and model window.
Usage aggregates unique complete positive CLIProxyAPI execution receipts for this
tenant and public Session, across models, runs, maintenance and retries. Actual
reported consumption and charge eligibility are separate. Include optional
descendants through existing subagent relations only. Forks have independent
public Session IDs. Cache reads/writes are subsets of input, reasoning is included
in output, and total comes from the provider normalization. Incomplete or absent
known invocation receipts are visible as unconfirmed. A queued source receipt
may arrive later. Neither command admits work, contacts a provider, nor updates
a projection. Usage is read on demand; model events must not issue lifetime sums.
Native document recovery and atomic accounting evidence retain their contracts.

Measure the PG contention from projection and receipt persistence on an isolated
fixture before proposing new database infrastructure. Compare projection alone,
concurrent writers to the same database, and distinct databases in one instance.

A known invocation with a missing or partial attempt contributes to the
unconfirmed request count once. Complete receipts for its other attempts still
count. Amounts have no start date or pricing requirement. The authenticated
status and usage routes are GET /v1/conversations/:sessionId/status and
GET /v1/conversations/:sessionId/usage. Unknown context capacity is returned
as null; the web client shows a dash. Protected inactive or deleted sessions
cannot expose financial records through these routes.

The serving settlement path touches model_invocations and model_metering_events
in the same transaction as the accepted session change. Moving financial source
receipts to a different database requires an explicit correlation/reconciliation
design; a second connection string alone cannot preserve that atomic boundary.
