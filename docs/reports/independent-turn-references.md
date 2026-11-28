# Independent Turn reference cutover

Installation v3 is an isolated #52 candidate. Its migration and [serving acceptance](../../scripts/experiments/minimal-session-storage/check-serving-turn-references.mjs) use the same physical Turn table that the Worker claims. Current production migrations and business data are separate.

## Database ownership

| Relation | v3 owner and lifetime |
|---|---|
| session_schedules.last_run_id | Tenant and physical Turn. Collection clears only last_run_id. The deadline listener commits the occurrence, independent Turn, first input and cursor in one transaction. Its public Session can have a different writable Flow after pruning. |
| execution_target_bindings | Tenant and physical Turn, with the logical Session and exact writer retained for cancellation. A binding protects its Turn from deletion. Losing or retiring that writer closes its bindings. Cleanup requires a successful endpoint revocation and a terminal sealed Turn. |
| run_skill_catalogs | One version selection per Turn. Later installation edits apply to subsequent Turns. Existing v2 Turns retain the empty selection exposed by the v2 Worker. |
| session_upload_pins | Tenant, logical Session, stable input ID and upload ticket. Promotion can change the pending message’s Turn without moving its pin. Placement transfers retention to canonical history. A returned draft keeps the pin until acknowledged and then receives the usual 24-hour ticket grace. |
| environment_validations, source_control_issue_jobs | Tenant and canonical Turn. The foreign keys preserve their existing cascade behavior. |
| session terminal events | The segmented terminal rows share the Turn’s tenant and deletion lifetime. |
| Legacy Run, transitions, turn control requests, subagent conversation rows and terminal event rows | Shared read queries still refer to their public relation shapes. In the candidate these relations have validated empty constraints. Obsolete writes fail, with zero foreign keys to public.runs. |

An existing installation must drain its Workers before upgrading. The migration checks the public lease and management records, protects the relevant tables and commits the version marker with the constraints. An incompatible live legacy record fails the cutover. The migration does not clear evidence of an outstanding operation. Outside the candidate, the existing business writers keep their previous database contracts.

## Durability and restart

Ordinary PG admission uses synchronous commit. The Projector may disable synchronous commit locally for replayable projection batches. Its current terminal transaction explicitly uses synchronous commit. A late follow-up and the new Turn created by its closure trigger are part of that transaction. The earlier random-successor example describes a hypothetical change to this boundary.

Projector assignment first obtains the ordinary projection recovery position. It also computes the earliest first-publication offset among running and cancellation-requested Turns. This second position supports reconstruction of the in-memory native execution state used when saving a Worker handoff. Complete Entries can already be in PG while the first publication is still required for this reconstruction. Settled Turns are excluded. This change preserves those recovery semantics.

## Integration boundaries

The serving checks exercise schedule firing through the actual claim/Worker/Projector path and use an already-ready text upload ticket for the input acceptance path. The original file transfer into the Workspace, visual input preparation, ordinary Guest dispatch and manual compaction require their respective serving ports. The binding check uses the real admission and closure service with an owned test transport. The issue-job and environment-validation changes cover referential integrity; their producer services are outside this serving composition.

## Verification

The matched v2 upgrade refused an active Worker, preserved the two queued Turns and existing input after drain, and installed empty Skill snapshots for those pre-upgrade Turns. Fresh and upgraded installations reached v3. The catalog query against the physical public.runs table returned zero referencing foreign keys. The old signed-in account and provider configuration remained usable.

The serving gate admitted a due plan through the existing deadline listener, read its running Turn through the public schedules API and confirmed its Worker lease. The input test seeded a ready ticket, submitted the same ticket twice with the same idempotency key and observed a single mailbox pin. PG refused premature ticket deletion. On adoption it recorded the original attachment in history and released the pin. The binding test used the real prepare, exact-writer close and cleanup code with a fixture endpoint. A changed Skill installation did not change the versions already selected for the Turn.

At ten Turn admissions/s, two serial clean-install comparison runs used the same 480-Session trace, 19,680 visible records and a two-CPU PG limit. V2 and v3 each finished about 392.4 visible records/s including drain. The final Kafka lag was zero for both. Terminal projection p99 was 86 ms and 78 ms respectively. These are paced observations on one host.

The first high-load trial after upgrading also exposed an intermittent failure. All 480 terminal publications received a broker ACK, 453 Turns completed normally and 27 returned to the recovery queue. In the sample of completed Turns, partition 2 had about 26 s p95 terminal projection delay while partitions 0, 1 and 3 were all below one second. The Worker releases its local execution slot and stops renewing the incarnation after terminal publication. When closure projection lags long enough, lease expiry can win. The fixed-tape Worker then asserts on re-entry because it only models fresh input. The trial timed out and is excluded from the successful comparison. The original cause of the one-partition delay remains open. A successful repeat does not resolve that case.

### Upgrade-specific tail

A serial repeat on the upgraded v2 fixture completed all 480 Turns at 275.5 visible records/s, with a 22.46 s terminal p99 and zero final lag. The diagnostic comparison using pg_stat_statements reproduced the difference on the same v3 images and trace. Fresh v3 completed at 392.4 visible records/s (79 ms terminal p99); the populated-then-drained v2 upgrade completed at 269.3/s (23.07 s). Nested SQL statistics locate substantial extra work inside projection.

| Identical nested SQL shape | Calls per arm | Fresh v3 shared buffer hits | Upgraded v3 shared buffer hits |
|---|---:|---:|---:|
| Guarded presented-message update and insert | 20,640 | 742,336 | 1,627,977 |
| Advance first reconstruction position | 21,120 | 1,010,037 | 1,917,229 |
| Update projection pin | 20,160 | 363,344 | 937,884 |

These are buffer access counts. They do not imply physical disk reads. The three shapes are part of native history projection. A diagnostic ANALYZE of the Turn table immediately following upgrade still yielded a 12.69 s terminal p99. The evidence does not establish a complete explanation of plan choice or exclude host variability. A traced run also exhibited a fast tail. Keep the release gate open. Production was not changed.

The final functional gate also pruned a Session onto a different writable Flow before firing the plan and preparing its binding. Taking back one of two messages that shared a ticket retained the draft pin until acknowledgement and left the other message's pin intact. Workspace-wide type checks, the PostgreSQL schedule, Skill and execution-target suites (14 tests) and the native Session input regression passed.

A further upgraded run with sampled nested execution plans reproduced the tail. The same update of a Turn's projection pin used both an index scan on the active writer and a sequential scan. At the scan node, the sampled indexed lookups hit at most 11 buffers. One sequential lookup hit 732 and filtered 121 rows to update one Turn. The guarded presented-message operation likewise had both plans. These samples explain a source of amplified work. Their individual durations and sample counts do not attribute the entire end-to-end tail to that one node.

As a diagnostic, a disposable upgraded installation discouraged sequential scans only inside project_ready. It completed all 480 Turns at 379.2 visible records/s with a 195 ms terminal p99, 361 peak raw lag and zero final lag. This result supports investigating plan selection for the frequent point updates. It is a single intervention on the whole function. The candidate keeps normal planner settings.
