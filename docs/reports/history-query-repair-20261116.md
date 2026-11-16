# History-query repair — November 16, 2026

Follow-up to the [capacity diagnosis](runtime-capacity-diagnosis-20261116.md).
Subsequently [published and measured](workload-release-throughput-20261116.md)
after the owner authorized the build-boundary work. The sections below retain
the evidence and publication decision at the initial repair checkpoint.
Implementation: `8d96644b`. Source verification is complete; publication and
post-fix live capacity measurement are pending the release-boundary discussion
below. No increased production throughput is claimed.

## Implemented behavior

- Frozen paging materializes only the bounded candidate positions, including
  immutable entry/assistant references. Dependency sizes are resolved by full
  `(tenant_id, id)` keys before the page byte budget is applied. Selected bodies
  use the same keys without reopening `pi_history_snapshots`. Bounded lateral
  lookups prevent a bad tenant cardinality estimate from turning body fetching
  into repeated tenant-wide joins. Visibility, ordering and cursor rules remain.
- An ordinary Session with neither a visible owned Run nor a shared history root
  skips the expanded history query. Both facts are checked in the existing
  repeatable-read snapshot. Forks with no new Runs retain shared history;
  subagent views and invalid-anchor/cursor checks are unchanged.
- Projector persists the entire native batch before deciding whether presentation
  capture could produce content. Entries, retry checkpoints and terminal
  Tool/compaction Tasks retain capture. Other Task-only batches skip that query.
  SQL still decides result/reference eligibility; cold reconstruction is unchanged.

No schema, protocol, storage authority, Pi Agent-loop, Kafka durability or writer
fencing change was introduced. No cache or additional projection table was added.

## Verification

Serial checks passed:

- 36 real-PG history/window/tree/child tests: three-record pages under stale
  tenant statistics, byte-budget paging, off-page Tool arguments with repeated
  provider call IDs, empty-to-active Sessions, Fork/source deletion and child history.
- Seven real-PG execution recovery tests, including the new presentation test:
  every native write remains retained; non-display phases issue no capture query;
  retries, cancellation and compaction failures survive. Ungated reconstruction
  adds no missing presentation records. Existing replacement/Stop tests pass.
- Ten native conversation/export presentation tests.
- Runtime-core and control-plane TypeScript checks; whitespace/diff check.

The small-page regression asserts measured PostgreSQL buffer accesses rather
than a machine-specific latency or a particular planner string. Initial test
assertions were corrected for equivalent cursor JSON key ordering and the
existing rule that a queued chat Run is not yet a historical Turn. No product
semantics were changed to satisfy those assertions.

Original account/provider/model records, two development-machine intents and
393 Workspace files passed before/after preservation checks. Old Docker build
cache cleanup reclaimed 19.51 GB; serving artifacts and business data were retained.

## Publication stopped for design discussion

The owner explicitly requested discussion before any necessary architectural
refactor. Read-only release planning against serving provenance
`b1a8e6acffb0a8ed250b58083928d49409cb6158` exposed build/release coupling:

- Since that baseline, the root lockfile changed only to declare `ws@8.21.1` as
  a **fake-model-server development dependency**. No new resolved package was added.
- Volume gateway, Web, SSH gateway and Guest build inputs changed **only because
  they copy that entire root lockfile**. Guest implementation inputs are unchanged.
- Worker also copies the complete runtime-core source directory, including the
  changed history reader/projector modules.
- The maintained plan therefore requests six application/Guest artifact builds
  and physical maintenance solely because the Guest input identity changed;
  schema/storage/native maintenance flags are otherwise false.

This is build-input and release-scope coupling, not evidence that the Session
execution architecture must be replaced. The planner is honoring the actual
declared Docker inputs. Ignoring a lockfile hash or overriding its maintenance
decision would hide that mismatch instead of resolving it.

Proposed next discussion: generate workload-specific dependency inputs with a
maintained monorepo/build mechanism, align each image's source closure with its
actual role, and calculate release identities from those **actual build inputs**.
Preserve reproducible installs, exact artifact/scan provenance and the existing
maintenance requirements for genuine Guest/storage changes. Do not introduce a
parallel hand-maintained set of package versions or a special-case force-reuse flag.

No release preparation/application or production restart was performed. These
build-boundary changes have not been implemented. After the boundary is agreed
and validated, publish the query fixes and compare identical simulated-provider
workloads; the earlier roughly 10 Run/s measurements remain the only production
capacity evidence for this slice.
