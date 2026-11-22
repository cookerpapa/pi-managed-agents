# Stop returns confirmed input to the editable draft

This isolated [#52](https://github.com/cookerpapa/pi-managed-agents/issues/52) browser acceptance stops an ordinary root while its Bash call is waiting in a real Cube Guest. Chrome first queues a follow-up, then a steer. The website displays both accepted input identities before the user clicks Stop.

## From the Stop button to the next request

| Point in the test | What is checked |
| --- | --- |
| Cancel request saved, Worker has yet to abort Pi | The return endpoint reports that settlement is pending. |
| Native abort and ordered closure finish | The Run is cancelled. The two original Submissions are unanswered with reason `aborted`. Neither has a history Entry. The return endpoint supplies the original text and IDs in user queue order. |
| Both messages are visible in the composer | The test holds the return acknowledgement before it can reach the API, edits the draft and reloads the page. Chrome retries the outstanding return. The edited draft survives both this reload and another reload after acknowledgement. |
| Waiting for the user | The database still contains only the cancelled Run. The Guest execution marker and the cancellation dispatch each occur once. |
| The user explicitly sends the edited text | A fresh Worker claims the second Run and cold-opens the cancelled predecessor's settled history. The actual Pi model request contains the original Run prompt and the edited user message. PG contains one placement for the new prompt and zero placements for the two returned IDs. |

The original Run made one model request, which produced the Bash call. This workload uses deterministic model responses, the real Session API, Chrome, Pi Durable, RF3 Kafka, PostgreSQL and Cube. The fixture routes the Worker management call through local IPC. Only aggregate assertions are retained in the [result file](stopped-root-inputs-20261122.json).

## Failure found and fixed

The old composer kept the returned IDs in React memory. Holding the acknowledgement after the browser had restored the messages, editing their text, then reloading made the old messages appear again after the edit. The HTTP response was correct on both reads.

The composer now saves the draft text, attachment metadata and the last restored position for that Stop in one browser storage write. It sends the acknowledgement after that write succeeds. On a repeated page it appends only the still-missing items. Clearing the draft retains the position until acknowledgement. A storage error leaves the return available on the server. If the acknowledgement succeeds on the server but its response is lost, at most one small return cursor can remain with that browser draft. The next Stop replaces it.

The candidate API reads the stopped Run's input metadata and native Submission outcomes for both listing and acknowledging a return. A subsequent Run can load the settled history of a user-cancelled predecessor. The send action is the only step in this scenario that creates another Run.

This case covers text inputs on one browser profile while the Worker remains reachable. The handoff of a live Worker is covered by the [separate crash scenario](root-input-handoff-20261122.md). A simultaneous Stop and Worker loss, or transfer of an edited local draft to a different device, was outside this test. Release remains gated by [#48](https://github.com/cookerpapa/pi-managed-agents/issues/48).

The previous browser steer/Worker-loss case passed again. The RF3/PG transport group passed 25 tests, including a candidate read that excludes placed and explicitly withdrawn inputs, preserves attachment metadata and rejects another tenant. The focused queue, history, draft and UI group passed 89 tests. Both affected TypeScript workspaces passed type checking. One trial timed out opening the initial chat page before submission. The completed Stop run recorded zero HTTP 5xx responses.

## Stop after losing the Worker

The additional browser gate terminates A after the browser confirms both inputs. The user clicks Stop while A's lease is outstanding. PG expiry and the ordered seal requeue the same Run. B reads its original Inbox, recovers the original mailbox and processes the persisted Stop before resuming generation.

| Original operation when B arrives | B model requests | Guest executions | Cancellation dispatches | Inputs returned / placed |
| --- | ---: | ---: | ---: | ---: |
| Bash still running | 0 | 1 | 1 | 2 / 0 |
| Original reply already published | 0 | 1 | 0 | 2 / 0 |

For the second case the driver released the Guest after the seal, then waited for the original per-boot mailbox to pass the saved offset before starting B. Both cases repeated the lost browser acknowledgement, draft edit, refresh and explicit cold continuation. [Aggregate evidence](root-stop-handoff-20261122.json).
