# ADR-0327: Workload-owned dependency build inputs

Date: 2026-11-16. Status: accepted; implementation in progress.

## Problem and decision

The root npm lockfile is copied by otherwise independent production images.
Adding a fake-provider development dependency therefore changes Guest identity
and requests physical maintenance. Worker also imports the runtime-core barrel,
loading Projector modules it does not use. The owner authorized removing this
coupling and then measuring the deployed history/query repairs.

Retain one source lockfile and npm package manager. Generate a disposable build
context per Node workload using pinned Turborepo 2.11.11 `prune`. The generator
supplies only the workload's declared production workspace manifests, plus
explicit build-time dependencies taken from the existing root/workspace pins.
Development/test dependencies are excluded unless the workload builds with them.
Turborepo owns transitive dependency/lockfile pruning; no custom package resolver
or hand-maintained secondary version list is introduced. Every retained registry
package must keep its original version, resolved URL and integrity. Missing pins,
unaccounted workspace dependencies and install inconsistencies fail publication.

The generated manifest/lockfile bytes are the bytes Docker actually consumes.
Artifact and Guest identities include them, the actual Docker source inputs and
the generation recipe/tool version. Never ignore the root lockfile in release
planning while Docker still consumes its unpruned content. Original image and
scan provenance is retained. Old Git inputs remain evidence of how old images
were built; do not relabel them as new artifacts.

Replace Worker/server barrel imports with existing narrow public subpaths. The
Worker Docker source subset must match its runtime import closure, including
static dynamic imports. Retain explicit asset/native-module checks and image
boot tests; tree-shaking is not proof that a source file is safe to omit.

This changes build boundaries, not Session ownership, Kafka/PG semantics or
resource authority. Genuine Guest/storage changes keep physical maintenance.
The initial packaging cutover may require rebuilding its affected artifacts;
future unrelated test dependencies must not do so. Runtime data/credentials and
Workspace bytes remain preserved by the maintained release operator.

## Adopt-before-build evidence

- [Turborepo prune](https://turborepo.dev/docs/reference/prune) is Vercel's MIT
  implementation for a partial workspace and pruned lockfile. Its
  [Docker guide](https://turborepo.dev/docs/guides/tools/docker) addresses this
  exact global-lockfile invalidation problem. Version 2.11.11 was verified against
  this npm v3 lockfile and a clean offline Guest dependency install.
- [npm install --package-lock-only](https://docs.npmjs.com/cli/v11/commands/npm-install/)
  with npm 11.6.2 kept removed-workspace metadata in the experimental output.
  It remains the installation authority but is not the selected pruning tool.
- [pnpm deploy](https://pnpm.io/cli/deploy) produces an isolated deploy package,
  but adopting it would also migrate the package manager, lockfile and native
  dependency installation. That is unnecessary for this bounded build change.

No remote build cache, new scheduler or runtime service is introduced. Outputs
are reproducible from committed source and disposable. Rollback selects already
verified immutable artifacts through the existing compatible-release rules.

## Acceptance

Verify unrelated fake-provider/test dependency and Projector-source edits leave
Guest/Worker inputs unchanged where not consumed; relevant production dependency,
native asset and recipe edits must change them. Check generated installs against
the source pins, secret exclusion, actual Docker inputs, all role boot/import
boundaries, scan provenance and the initial release plan. Serial live acceptance
must retain original assets, exercise cancellation/recovery and compare identical
fake-provider throughput/history workloads without paid model requests.
