# ADR-0346. Retain existing elastic compute on human Run admission

Scope: the #52 candidate and a new root chat Run. A user continuing a conversation
can retain its managed elastic compute even when the next model call uses no tools.
At admission the resource owner uses the Workspace admission lock, logical compute
scope and PG allocation records of ADR-0314. Extend a usable allocation by its
recorded window. Store the bounded outcome in the existing Run environment snapshot
in the same transaction. A retry returns the original outcome. No provider request,
allocation creation, Worker routing identity or extra per-message projection is needed.
The candidate study composes these operations in one PG database.

Expiry and admission use the same Workspace-then-allocation lock order. A committed
closure wins over later activity. Only the owner's released state establishes a past
release. Missing allocation records, an uncertain state and a closing allocation
carry distinct meanings. The first model request uses the admission observation on
both warm and cold claims, qualified as of admission. Once its recorded deadline has
passed it can report only historical retention. A retention decision establishes
platform policy; only execution can observe a running process. The Worker continues
its existing periodic activity after claim. A queued Run does not renew indefinitely.

This decision adds no Conversation observation history and does not change the
native Fork policy for current world state. The separate Fork release gate remains open.

## Verification

The isolated `workspace-continuation` profile runs on two Worker Pods with the
pinned Pi Harness, a deterministic model, RF3 Kafka and a private PG database.
Each of the two new waves ran seven pure-chat Runs. They checked six resource
states, including two consecutive admissions against the same warm allocation.
Both waves inspected the actual first provider request and the corresponding
projected system section. The second Run reused the preferred Worker's prefix
in one wave and cold-read it on the replacement in the other. The fixture supplies
allocation states; it does not start or kill a physical VM. These small waves are
functional checks. They do not measure peak admission throughput.

The [transaction test](../../scripts/experiments/minimal-session-storage/admission-retention.integration.spec.mjs)
uses the serving idle-expiry query in a second PG transaction. It checks both
lock orders, rollback, idempotent replay before and after release, and an isolated
compute scope on a shared Workspace. The previous directory-change and cold
continuation controls and the serving repository's expiry/renewal tests also passed.

The deployed resource table has a unique index covering live allocations only.
With 32,768 unrelated released allocations, the negative history lookup rejected
all 32,768 rows. The [candidate resource index](../../scripts/experiments/minimal-session-storage/admission-retention-index.sql)
covers released rows by tenant, Workspace, domain and compute scope. The same lookup
uses that index in the disposable PG test. No production migration was applied.

Run the profile using the [existing continuation harness](../../scripts/experiments/minimal-session-storage/README.md#run-serially).
Run the targeted PG checks with a disposable database:

```sh
node scripts/run-with-test-postgres.mjs -- node_modules/.bin/vitest run \
  scripts/experiments/minimal-session-storage/admission-retention.integration.spec.mjs \
  packages/sandbox-control/test/workspace-runtime-state-repository.test.ts \
  -t 'orders Run admission|bounds the released lookup|expiry and renewal|skips a Workspace held' \
  --maxWorkers=1 --testTimeout=60000 --hookTimeout=30000
```
