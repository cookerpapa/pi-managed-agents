# ADR-0365. Deliver human and agent input to independently owned Sessions

[ADR-0366](0366-pg-input-boundaries.md) supersedes the active-mail handoff and native queue editing portions of this decision.

Accepted for the isolated #52 candidate. ADR-0362 owns shared prefixes and management ancestry. ADR-0363 owns the Session lease. Keep the one-listener directed scheduler from ADR-0364. Queue one ready Session, with its earliest eligible Turn as the execution and history anchor. A busy Session receives a hint on its owner's existing management socket, including when that Worker has no spare slots. Reconnect and reconciliation must repair missed hints.

Admit inputs by source, target Session, request identity and delivery mode. Serialize idle admission on the Session. One strict start wins. Busy arrivals use the requested steer/follow-up policy. Named delegation registers an independent Session and its first input atomically. Its Tool Result acknowledges creation. A later message is a separate explicit send_message action with urgent or normal priority. An independent child's final stays in its own history. Parent Stop and ordinary completion leave children running.

Before handoff a human can take back their own pending message transactionally. After handoff Pi arbitrates withdrawal versus placement, by stable request ID. Persist withdrawal intent before confirming it. Reject edits to agent mail. Confirm native changes from the admitted projection and keep enough identity to reconcile handoff, ambiguous delivery and retries. Human editing, ordering, attachments and recovery must survive the change. An unavailable owner cannot prove withdrawal. Web forks copy history, never pending inputs.

Acceptance covers concurrent strict starts, directed hints to full Workers, reconnect, duplicate admission, both orders of withdrawal versus placement, independent named creation and explicit replies. Measure notification latency and the existing visible-message projection baseline. Suspendable waits and exhausted-slot scheduling are deferred. Serving cutover remains separately gated under #48.
