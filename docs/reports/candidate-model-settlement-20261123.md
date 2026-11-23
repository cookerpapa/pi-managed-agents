# Candidate model settlement and projection cost

## Correctness

The #52 serving candidate previously rejected native commits containing Worker model execution evidence. Its usage reader also traversed the old child-session directory. A named child has a public UUID and a founding Run in the candidate. The Session API now walks those Run relationships at read time. It reads provider receipts using the original public UUID for each agent.

On an accepted evidence-bearing commit, the candidate runs the ordinary guarded projection and `ModelExecutionAccounting.settle` on the same PG connection and transaction. An evidence-only task transition also verifies Run ownership. The existing production accounting adapter writes the execution proof with the original Kafka position. Ordinary streamed text takes its usual projection path. The native `pi.usage` wire contract and the financial schema are unchanged.

The deterministic model integration uses the real Worker receipt wrapper, fixed Pi Harness, three Kafka brokers, a separate named executor process, PG and the HTTP Session API. A simulated provider supplies a complete receipt worth 10 tokens per request. Root and child totals match the simulated provider, with the child's tokens included once in the family total. A deliberate receipt identity conflict rolls back the message and partition progress. Redelivery gives a single execution receipt. Source usage and customer eligibility can be inspected separately. This fixture consumes no paid model quota.

A second acceptance used a real Cube Guest and simulated model replies. The Session API accepted two root Runs, the Guest executed Bash once, and a cold Worker continued the second Run after the API restarted. Both financial writer closures matched the corresponding Run seal offset. The fixture verified its cleanup. The RF3/PG suite passed all 40 checks across 23 files with the delegated browser enabled.

## Projection measurement

Each arm replays the same captured native writes for 256 root Runs, with 20 sequential local tool calls, 21 model calls and an answer per Run. Each arm submits 47,616 native commits and verifies 11,264 stored rows, including 5,120 tool results. There are 32 ordered partition loops, a shared pool of 12 connections and 22,016 projected transactions in every arm. A disposable Docker PostgreSQL instance runs on the WSL development host. Gateway invocation identities are inserted before timing to reproduce the common case in which the gateway has already seen the request.

The measurement starts at the candidate Projector's per-record entry point. It excludes Kafka fetching, external dispatch, Run claim and final seal. Counts cannot be converted into a serving capacity promise. The benchmark counts driver calls, including explicit transaction boundaries. It does not count statements inside PL/pgSQL as network round trips.

| Replayed path | First / second sample, native commits/s | Combined rate | PG round trips per arm |
| --- | ---: | ---: | ---: |
| Ordinary projection, evidence omitted for comparison | 2,608 / 3,262 | 2,899 | 22,016 |
| Matched accounting path, 5,376 execution proofs | 2,004 / 1,926 | 1,964 | 48,896 |
| Accounting commit policy alone, no proof, diagnostic | 2,946 / 2,571 | 2,746 | 22,016 |
| Same policy with explicit transaction boundaries, no proof, diagnostic | 2,354 / 2,385 | 2,369 | 32,768 |

Each invocation captured a fresh trace with the same counts and reused it for all four arms. The first comparison ran ordinary/accounted/accounted/ordinary. The diagnostics ran policy/transaction/transaction/policy. Combined rates divide total records by total elapsed time across both samples. The host shows run-to-run variation.

For each of the 5,376 accepted model responses, PG receives an explicit BEGIN and COMMIT, an attempted invocation insert, a fingerprint read after the conflict and the execution-receipt insert. There are 26,880 extra calls. Sync transactions increase by just 256, from 10,496 to 10,752. The complete path is 32% slower than the ordinary path on this measurement. Explicit transaction boundaries alone also reduce throughput. These comparisons do not separately attribute all of the remaining cost to WAL, network latency or index updates.

## Release decision

Keep #48 gated on the matched projection test. The next comparison should perform the admitted projection and idempotent execution proof in one server-side transaction with one client round trip. Preserve exact invocation fingerprints, the original position, conflict rollback and writer closure. Compare the identical tape first, then the Kafka mixed-load acceptance with Guest dispatch and seals. Separate storage ownership would require a new decision under ADR-0342.

## Reproduce

Run each command after the previous command finishes on the same host.

```sh
PI_CLOUD_DELEGATED_BROWSER=1 npm run test:transport
node --import tsx scripts/experiments/minimal-session-storage/model-settlement-rate.mjs 8
node --import tsx scripts/experiments/minimal-session-storage/model-settlement-rate.mjs 8 isolate
```
