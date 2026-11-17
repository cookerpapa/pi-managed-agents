# ADR-0328: Projected native input confirmation

Status: deployed on 2026-11-17 as `projector-admission-20261117-r1`.
Implements #44 and is a prerequisite for the #43 publication cutover.

## Decision

Pi remains the only owner of Inbox and Submission state. The Worker publishes
complete native transactions and may continue after Kafka durability confirmation.
Human submit/edit/cancel/reorder/promote responses require a different boundary:
the exact input or mutation must have been accepted and committed by Projector.
Do not interpret raw Kafka acknowledgement, a numeric sequence watermark or
current Inbox membership as that proof.

The Session API registers a bounded notification waiter before delivering the
native command to the current Worker. Reuse the Session execution HTTP/SSE server,
partition-owner routing and in-memory event hub; add no Kafka consumer, service,
durable queue or input-receipt table. A readiness frame confirms subscription
before delivery. The notification contains only operation scope, identity and
fingerprint; no prompt or attachment bodies. Projector derives confirmations
from materialized native input state and publishes them only after PG commit.
Ordinary model text and other Storage commits do not wait for these notifications.

Native current input metadata already retains submission identities and mutation
receipts. Retry/cold recovery uses those durable values. Notification loss,
Projector reassignment or timeout invokes a bounded authoritative receipt lookup;
absence is unconfirmed, never rejection. Normal operation does not poll PG.
Waiting has an HTTP lifetime, does not hold a PG transaction, and closes on caller
failure. A stale-incarnation command cannot announce success. A real committed
operation remains committed after later consumption, Stop or owner replacement.

Initial Run registration remains its existing PG transaction. Keep latest-only
Inbox retention and native consumption atomicity. User drafts survive uncertain
acceptance and retries retain the same idempotency identity. Source-only test
compositions must supply the same confirmation boundary rather than silently
falling back to Worker acknowledgement.

## Adopt before build

Reuse the existing Fastify/SSE transport, Session event hub, Pi current documents
and native receipt readers. PG LISTEN/NOTIFY is an alternative wake mechanism,
but the execution role already owns partition routing and committed native
changes. A second notification broker or queue would add another ownership and
recovery boundary without supplying durable evidence. Earlier research and the
current queue contract are recorded in ADR-0243 and
`docs/research/producer-reuse-and-raft-20261117.md`.

## Acceptance and rollout

Test exact identity/fingerprint matching, rapid consumption, late notification,
same-ID retry, closed target, attachment pins, submit and every mutation, API
restart and Projector reassignment. A failed PG transaction cannot notify success.
Measure normal-path SQL and confirm no per-fragment or timer-driven receipt reads.
Deploy the private protocol together with #43 after serial source/fault/browser
checks; preserve original accounts, configuration, credentials and Workspace data.
