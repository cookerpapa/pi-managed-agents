# PG mailbox adoption at native boundaries

[ADR-0367](../adr/0367-worker-steer-hints.md) supersedes the unconditional Worker lookup measured here.

The #52 candidate uses the pinned, unmodified Pi Durable 1.0.0 SDK. The scheduler still wakes an available Worker for an idle Session. For an active Session, a tool round checks PG for an eligible steer. Normal native settlement checks for the next pending input. Neither path relies on the arrival of a management hint. Editing, moving, promoting, or withdrawing a pending message completes in PG. [Decision](../adr/0366-pg-input-boundaries.md).

## Storage and recovery

A normal tool boundary reserves one message. The Worker uses Pi's public conversation transaction to append a user entry, create the placed submission with the same request ID, and add it to the current native Run's inputs. The Projector saves the message's source input ID and marks the PG reservation placed in the same transaction. At idle, Pi's standard submit path directly places the input and starts a native Run. Follow-ups stay pending during ordinary tool rounds. Termination, handoff and reset settle through Pi first. Pending native maintenance writes retain their ordering before an adopted steer.

The fault seal reconciles remaining reservations before releasing the old lease. An unpublished reservation goes back to pending, or to withdrawn if the user requested take-back. A placement survives with its source ID and tool-round identifier. The latter prevents an interrupted hook from taking a second steer when the same native generation resumes. No recovering Worker needs to poll for the predecessor's seal. A late arrival after the idle check is picked up by the existing ordered completion transaction.

Cloud queue edits produce zero native commits. Pi still commits its initial Inbox identity and may use that document for internal writes such as compaction. Four numeric fields on the existing Flow identify the native document and its position. In the candidate schema, recovery reads the native shape through the `inboxes` view. Physical `native_inbox_payloads` rows exist only while nonempty maintenance items are outstanding. Empty arrays and editable cloud messages occupy zero physical Inbox rows.

## Acceptance

- Real PostgreSQL, Pi, and the Session HTTP client: a blocked tool round received 32 messages that were withdrawn before release. The native commit count remained unchanged. A promoted steer appeared in the next request, the follow-up appeared after the final answer, and each formal input had exactly one placed submission alongside its user entry.
- A passive native write temporarily created one payload row. After adoption the model saw the maintenance entry ahead of the steer, and the physical Inbox table was empty again. Headless Chrome exercised take-back and draft restoration against the candidate API.
- Three-broker Kafka and separately killed Workers: an unpublished reservation with a pending withdrawal was already withdrawn at the successor's claim; another unpublished input returned to pending and was adopted once. Killing a Worker after its formal steer was committed and before native tool-round handover preserved that steer. The same generation could not take the second steer. RF3 records contained placed and settled submissions for the surviving inputs and no submission for the withdrawn input.
- Six native boundary cases cover ordinary tools, terminate, handoff, reset, unanswered input and preparation failure. The matched transport suite passed 51 tests in 29 files. The candidate uses deterministic model responses. Serving cutover remains gated under #48.

## Performance

The Worker now makes one indexed PG check at a completed tool round even if no steer is eligible. With 32 concurrent clients, one Session per client and 256 pending follow-ups per Session, 3,200 empty steer checks took 718 ms on the disposable local PG fixture. P50/P95/P99 were 6.5/10.1/30.7 ms, including the client connection pool. This exercise measures only the Worker selection query.

The separate projection comparison used 32 Sessions and 20 tool calls per Session. After two warm-ups, 12 counterbalanced pairs gave median rates of 1,052 visible records/s for independent projection with the mailbox receipt path disabled and 980 with it enabled. The median difference within pairs was −6.0%. Each trial used 2,752 projection transactions; the candidate used one additional synchronous confirmation per Session. These samples do not compare the entire old and new Worker implementations. They exclude model, sandbox and network execution time.

```sh
node_modules/.bin/vitest run scripts/experiments/minimal-session-storage/segment-input-boundary.test.mjs
PMA_SEGMENT_BROWSER=1 npm run test:transport
export TSX_TSCONFIG_PATH="$PWD/packages/control-plane/tsconfig.json"
node --import tsx scripts/experiments/minimal-session-storage/model-settlement-rate.mjs 1 session-inputs boundaries
node --import tsx scripts/experiments/minimal-session-storage/model-settlement-rate.mjs 1 session-inputs pairs
```
