# Confirmed steer and follow-up after a Worker crash

This [#52](https://github.com/cookerpapa/pi-managed-agents/issues/52) study follows two ordinary root Sessions, one per busy-input mode. Chrome sends the initial prompt and one more message while the fixed Bash tool is waiting in a real Cube Guest. The model responses are deterministic and inspect the actual Pi request. The Session API, the HTTP/SSE confirmation service, Kafka, PostgreSQL, native Pi Inbox and Worker processes participate. The fixture routes Worker management calls through local IPC and selects one fixed sandbox.

## What the experiment checks

| Boundary | Observation required |
| --- | --- |
| Worker has published input, Projector is held before PG | The Worker management call has returned. A separate PG connection sees no receipt, the public queue is empty, and Chrome has received no successful response to its submission. |
| Projector commits and the API answers | The exact operation and fingerprint reach the waiting Session API. Chrome receives its accepted input ID and displays the item. The fixture immediately terminates the Worker with SIGKILL while the original tool is still waiting. |
| The old lease expires and the Run returns to the queue | The PG reaper and ordered Kafka seal operate normally. An HTTP retry using the original idempotency key returns the same accepted input. Chrome reloads before the replacement is started and can still see the pending input, the original prompt and the running tool. |
| The new Worker takes over | A new process claims the same Run under the next lease and reads the sealed native state from PG. Its Inbox contains one item in the requested native mode with the original Submission ID. It installs the original Kafka reply waiter before the fixture releases the physical Bash command. |
| The Run finishes | The replacement's model receives both user messages and reads the value returned by the actual shell. PG has one Run, one placement of the second input and one canonical tool result. The Guest has one physical execution marker. The browser sees the queue empty and the new answer both live and after refresh. |

## Measured outcomes

| Busy input | Model requests made by B | New input placements | Physical Bash executions | Root Runs |
| --- | ---: | ---: | ---: | ---: |
| Steer | 1 | 1 | 1 | 1 |
| Follow-up | 2 | 1 | 1 | 1 |

Both cases held Bash during takeover. In the steer case, the first post-tool model request already included the second user message. In the follow-up case, B first completed the current response, then made a second request with the queued input. These counts describe this deterministic two-prompt workload.

## Why the Projector needed a change

The first trial returned the busy input to the replacement Worker, but its subsequent commits stalled in projection. Ordered closure had released the old Projector view. The ordinary cold-open code then tried to load a *completed predecessor Run*. The interrupted Run was still on its first mailbox position, with an unfinished tool and a nonempty Inbox.

The normal Run lookup now checks for an existing handoff in the same SQL call. Only a sealed handoff needs the additional read. The Projector validates the old and new writer identities, Kafka partition and offset, next native commit sequence and the Inbox version. It rebuilds the root view from the existing seal and current rows. It recalculates the retained message count and bytes once when this Run finishes. A recovering, previously started Run also keeps its timeline visible while it waits for a replacement.

The [aggregate results](root-input-handoff-20261122.json) contain no prompts, reply payloads or credentials. The tested application identity had no direct access to the legacy native/history relations and the legacy storage row counts were unchanged. Each case cleans up its Guest, topics and consumer groups. The evidence covers one handoff with a live Projector, one Bash operation and one queued text input. Interruption before the browser receives its first confirmation, Stop with unconsumed input, a second crash of the replacement, and a concurrent Projector crash remain separate cases. This functional study uses an isolated composition and leaves the [#48 release gate](https://github.com/cookerpapa/pi-managed-agents/issues/48) open.

The ordinary two-Run browser regression also passed. The RF3/PG transport group passed 24 tests, and the input, history and queue group passed 48. All three affected TypeScript packages passed type checking. No capacity comparison was run for this slice.
