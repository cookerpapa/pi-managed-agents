# ADR-0326: Capacity benchmarks through existing runtime ports

Date: 2026-11-15. Status: accepted; implementation in progress. Issue: #42.

## Decision

Measure the maintained runtime with a deterministic provider rather than using
subscription throughput as a proxy for platform capacity. Pi, Session ownership,
the transactional RF=3 writer, Projector, PG storage, SSE and execution/completion
ports remain real. Synthetic workloads never introduce another production
scheduler, event store or execution authority.

Extend the existing fake-model package with the Responses HTTP/SSE and WebSocket
surface exercised by the pinned Pi/CLIProxyAPI adapters. Support complete and
incremental requests, paced/fast text, fragmented Tool arguments, controlled
failures and synthetic usage. Provider state is a bounded, disposable test cache;
it cannot restore Pi history or authorize replay. Retain only bounded observations
and aggregate counters, never request bodies or credentials in reports.

Use the published CLIProxyAPI with a uniquely named, temporary Codex-compatible
API-key route pointing exclusively to the owned fixture. Its model name cannot
match a real provider model. The fixture requires a benchmark plan marker and
its own credential, and has no provider credentials or external network client.
No real-provider requests are part of the default matrix.

Register that model temporarily through the existing provider configuration and
projected model-catalog mechanisms. Preserve all original routes, accounts and
defaults. Record a private restoration receipt before changing configuration,
use current revisions/ETags and remove only the owned registration after every
accepted test Run has settled. An interrupted driver must provide explicit
resume/cleanup; missing evidence is not permission to remove unrelated records.

The complete-loop measurements use owned tenants/Workspaces in the maintained
deployment, with no unrelated active Runs. Isolated PG/Kafka microbenchmarks
continue to use their disposable infrastructures. Reports identify this difference
and record idle background load. Heavy suites run serially on the shared host;
concurrency within one declared workload is intentional.

## Build versus adopt

Reuse the existing native HTTP/SSE client, fake provider, configuration utilities,
owned-resource cleanup and observability ports. k6's documented open/closed
workload models are the reference for arrival-rate accounting, but generic
virtual-user request loops do not replace the existing Session lifecycle/SSE/fault driver.
The bounded Node test driver schedules arrivals and records unfinished/dropped
work; it is not a distributed queue or product scheduler. A separate load
framework is unnecessary for the first single-host baseline.

References: [k6 workload models](https://grafana.com/docs/k6/latest/using-k6/scenarios/concepts/open-vs-closed/),
[dropped iterations](https://grafana.com/docs/k6/latest/using-k6/scenarios/concepts/dropped-iterations/).

## Measurements and interpretation

Report offered, accepted, started and completed work separately. Preserve samples
for queue/claim, first durable text, Tool round trips and settlement, and report
p50/p95/p99, errors, rejection/drop counts and draining time. Do not add nested
timings or subtract independently computed percentiles. Kafka lag is an offset
distance, not a count of semantic events.

Record actual overlap, model permits, resource limits, history/payload sizes,
generator delay, provider CPU/event-loop/memory and component/database counters.
Stop increasing offered load when the declared latency/backlog or host-headroom
budget fails, settle the owned work and report the limiting condition. Preserve
production durability and runtime limits; changing limits is a separately named
profile with restoration evidence, not a silent capacity improvement.

Run the active-Session baseline first, then controlled/real Tools, histories,
images, child Conversations and SSE variants. Run bounded soak, bursts and
existing recovery mechanisms under load. Resource lifecycle, scheduled admission
and accounting are separate measurements, never a claim that thousands of real
VMs ran on this host. Synthetic model usage remains unpriced or zero-priced for
owned tenants and is removed with the verified test fixture.

## Acceptance and non-goals

Contract tests validate Responses parsing, continuation, cancellation and failures
against the pinned Pi adapter. A smoke profile verifies one complete loop and
cleanup before the capacity matrix. Reports and CLI options make paid-provider
access, fault injection and any configuration changes explicit.

No new sandbox runtime, public fake-model product, provider SDK patch, alternate
Pi loop, database schema or relaxed fencing is introduced. A benchmark-only source
change does not require rebuilding serving application images; owned fixture code
is deployed separately using the existing cluster's tested image and ports.
