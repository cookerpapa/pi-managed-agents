# ADR-0330: Model charge eligibility and platform recovery costs

Status: deployed on 2026-11-17 as `projector-admission-20261117-r1` (#45).
Extends ADR-0297/0298 accounting without enabling payment collection.

## Policy

Keep actual provider usage and its immutable price-version quote. Customer
eligibility is separate. Completed native model work with complete provider usage
is eligible. The owner explicitly selected charging actual complete usage after
a normal user Stop when execution identity was valid. Stop does not retroactively
waive earlier valid requests. Failed/superseded work and a closed writer's requests
without accepted settlement evidence are platform cost. Missing or incomplete
usage remains unconfirmed and is never estimated as a customer charge.

Customer rate creation no longer accepts `includeFailed`: native eligibility
decides failure/Stop handling. The flag remains specific to optional upstream
estimates. Raw customer tariff quotes retain full reported quantities and the
original rate version; the statement applies eligibility without rewriting them.

If a request crosses a forced handoff, its accepted native settlement determines
eligibility. Do not split aggregate provider token counts at a wall-clock seal
timestamp or infer precise played/displayed token quantities. A valid old receipt
may arrive after takeover and still be eligible. Source/settlement order does not
change the outcome. Unknown evidence is not an authorization to charge.

## Evidence and ownership

At Pi's public HTTP Models port, generate a fresh invocation UUID for each actual
HTTP request and attach bounded accounting context. Include the PG-issued writer
identity/fence and native task identity. The Worker-local Model Gateway validates
the context against its active grant before forwarding it. CLIProxyAPI records
the attribution with actual provider execution/usage; it has no live fencing view.
The fixed provider adapter uses the public fetch hook; Pi core is unchanged.

Track the request in trusted Worker memory. When Pi commits that model task's
outcome/next phase, attach bounded settlement evidence to the same complete native
commit. This includes compaction and children and creates no extra streaming
commit. A partial live-document update is not model settlement. Projector verifies
scope and persists evidence only with the accepted native PG transaction.
Unknown/unaccepted candidate results never generate customer eligibility.

Reuse `model_invocations` and `model_metering_events` for immutable request
settlement evidence, keeping provider receipt ingestion distinct from execution
evidence. Persist a bounded financial closure fact per writer when its ordered
closure commits so late receipts can be classified even after Session deletion.
These are accounting facts, not another lease/operation ledger or scheduler.
They contain no prompts, model payloads, credentials, images or Workspace bytes.

Metering combines raw usage, exact request settlement and writer closure. Raw
usage/quote evidence is immutable; customer statement eligibility is derived from
those facts. Late receipts may resolve pending amounts. Repeated source delivery
cannot add usage, and a new intentional user request has its own identity. An
ambiguous provider retry/fallback attribution is waived or held unconfirmed, not
silently charged twice. Unsupported image/search dimensions remain unpriced.

## Adoption and delivery

Reuse the current source journal, independent receiver, PG transaction/immutable
receipt identity and versioned pricing. A live CPA fencing cache would address
prevention, not historical charge attribution, and is not added. No new broker,
provider-request replay, wallet or real tariff is introduced. Provider usage is
authoritative for actual quantities but may be missing after cancellation; later
requests do not backfill it. Ordinary account/history deletion cannot erase the
financial evidence used by these decisions.

Validate deterministic per-request identity, native settlement/Stop, compaction,
child and recovery attribution, delayed/duplicate/missing usage, both arrival
orders, source/receiver restart, prune/deletion and statement/export totals. Source
observations alone must never release a lease, replay a Tool or destroy a resource.
Publish the private attribution/native metadata changes together after draining
old writers and metering delivery, with serial simulated-provider load acceptance.
