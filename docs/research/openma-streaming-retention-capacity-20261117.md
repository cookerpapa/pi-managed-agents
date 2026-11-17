# OpenMA streaming persistence, retention and capacity evidence

Date: 2026-11-17. Source inspection at OpenMA main commit
`c60476ecf1c88afc9cd1ee2cf0ce11d5165c7322`, verified through its commit endpoint.
No OpenMA deployment or benchmark was run, and Pi Managed Agents runtime behavior
was not changed. This refines the canonical-write discussion in the
[fencing comparison](producer-reuse-and-raft-20261117.md#openma-comparison).

## Complete events are not the same as stream deltas

The managed Node runtime's
[output driver](https://github.com/openma-ai/open-managed-agents/blob/c60476ecf1c88afc9cd1ee2cf0ce11d5165c7322/packages/control-plane-node/src/lib/node-managed-session-runtime.ts#L752)
projects accepted canonical runtime events to the configured SQL store, then
publishes their live representation. Incremental frames take a different path:
the [decoder](https://github.com/openma-ai/open-managed-agents/blob/c60476ecf1c88afc9cd1ee2cf0ce11d5165c7322/packages/managed-agents-adapters-runtime/src/index.ts#L385)
excludes stream starts/deltas from persisted produced events. The explicit
[driver test](https://github.com/openma-ai/open-managed-agents/blob/c60476ecf1c88afc9cd1ee2cf0ce11d5165c7322/apps/main-node/test/managed-session-runtime-driver.test.ts#L805)
expects delivery of a delta with zero projection calls.

Accordingly, the inspected Node output path does not establish that every visible
prefix was durably accepted. A later complete or gracefully interrupted event is
a separate opportunity to persist text; it does not retroactively prove durability
before a crash during streaming. Native in-sandbox harness logs and every optional
adapter were not audited here.

Cloudflare's
[SessionDO stream helper](https://github.com/openma-ai/open-managed-agents/blob/c60476ecf1c88afc9cd1ee2cf0ce11d5165c7322/apps/agent/src/runtime/session-do.ts#L1345)
does await stream-buffer persistence before broadcasting each chunk. Its
[CfDoStreamRepo](https://github.com/openma-ai/open-managed-agents/blob/c60476ecf1c88afc9cd1ee2cf0ce11d5165c7322/packages/event-log/src/cf-do/index.ts#L434)
stores one `streams` row per message in the Durable Object's SQLite storage and
appends deltas to a JSON array. This is SQL persistence, but not PostgreSQL or the
separate D1 control database. The abort helper preserves partial text as a canonical
message; [cold recovery](https://github.com/openma-ai/open-managed-agents/blob/c60476ecf1c88afc9cd1ee2cf0ce11d5165c7322/packages/session-runtime/src/recovery.ts#L80)
joins leftover chunks into a message and finalizes the stream as interrupted.

There is also a
[SqlStreamRepo](https://github.com/openma-ai/open-managed-agents/blob/c60476ecf1c88afc9cd1ee2cf0ce11d5165c7322/packages/event-log/src/sql/index.ts#L216)
implementation for `session_streams`. With the PostgreSQL dialect, each
`appendChunk` updates a text column by casting it to JSONB, appending the delta,
and converting the result back to text. Its
[PG integration test](https://github.com/openma-ai/open-managed-agents/blob/c60476ecf1c88afc9cd1ee2cf0ce11d5165c7322/apps/main-node/test/pg-fanout.test.ts#L73)
exercises that adapter. Its existence does not show that the managed Node output
driver calls it for each delta. The legacy
[Node harness runtime](https://github.com/openma-ai/open-managed-agents/blob/c60476ecf1c88afc9cd1ee2cf0ce11d5165c7322/packages/control-plane-node/src/lib/node-harness-runtime.ts#L146)
also broadcasts stream deltas without the canonical event write.

This growing-row representation has different CPU/serialization/write costs from
independent append-only deltas. No PG load measurement here establishes those
costs or a throughput comparison with Kafka.

## Completion is not buffer reclamation

Both stream adapters' `finalize()` update status, completion time and error
metadata; they retain the chunks. The interface describes retaining completed
rows for reconnect reads, but supplies no TTL/prune operation. The published
[operations guide](https://github.com/openma-ai/open-managed-agents/blob/c60476ecf1c88afc9cd1ee2cf0ce11d5165c7322/apps/docs/src/content/docs/self-host/operations.mdx#L250)
explicitly says SessionDO event/stream rows have no automatic row TTL; its
growth discussion says stream rows remain after finalization and need an explicit
maintenance policy. File/blob cleanup and object-store lifecycle rules are separate.

The observed flow is therefore a retained completed/interrupted message plus a
retained stream row where that buffer path is used, not a demonstrated automatic
conversion into one final message followed by deletion of all deltas. Session
deletion/resource destruction must not be mistaken for proof that each runtime
table is reclaimed. This study does not certify all deletion paths.

## Public performance evidence found

Repository searches covered benchmark, throughput, p95, stress, QPS and Chinese
capacity terms, followed by relevant code and official documentation inspection.
Search absence is not proof of no private/internal testing.

| Evidence | What it establishes | What it does not establish |
| --- | --- | --- |
| [Cloudflare stress integration tests](https://github.com/openma-ai/open-managed-agents/blob/c60476ecf1c88afc9cd1ee2cf0ce11d5165c7322/test/integration/stress.test.ts#L432) | Includes ten concurrent Session creations and posting events to five Sessions, with a noop harness | Sustained Agent-loop throughput, resource saturation or tail latency under production load |
| PG stream/fanout tests | Adapter operations and delivery between two hub instances sharing PG | A measured multi-host capacity envelope |
| [Feishu multi-agent PRD measurements](https://github.com/openma-ai/open-managed-agents/blob/c60476ecf1c88afc9cd1ee2cf0ce11d5165c7322/docs/feishu-multi-agent-integration-prd.md#L103) | Reported 24 successful model/orchestration cases, three experts/two discussion rounds, p95 72.38 seconds | Real Feishu end-to-end or infrastructure throughput; the document discloses a no-op transport plus a fixed three-second estimate and a partial 24/100 rehearsal |
| Harbor/eval benchmark features | Agent task evaluation machinery | Runtime capacity measurements simply by being named benchmarks |

No public report was located in the inspected repository/docs that provides a
comparable fixed-hardware, sustained-concurrency, completed-Run-rate, latency and
database-resource baseline. The Feishu result uses a different workload and real
model time, so its seconds must not be compared directly with our simulated-model
short-Run numbers.

## Consequence for our design discussion

OpenMA's SQL canonical-publication fence is a useful ownership example. It is not
evidence that every deployment durably records every visible delta, automatically
reclaims stream buffers, or outperforms a Kafka-backed path. Preserve our explicit
[partial-output requirement](../STREAM_DURABILITY.md#why-partial-assistant-output-is-durable)
when comparing architectures, and compare persistence coverage, retention and
measured capacity separately.
