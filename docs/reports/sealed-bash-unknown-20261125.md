# Ordinary Bash after a sealed Worker loss

The #52 experiment enables `sealedRecovery.bashOnWorkerLoss = "unknown"` on the successor's Session host. Its unit of recovery is the original Pi tool task on its Conversation. After reading the ordered predecessor seal, the host excludes outstanding ordinary sandbox Bash tasks from mailbox restoration. When Pi resumes such a task at its execute checkpoint, the remote Tool registration returns an error result with `code=tool_operation_outcome_unknown` and `reason=worker_replaced`. Its model-visible text says the command may have run and asks the agent to inspect effects or logs before retrying. Pi creates the original call's canonical result and the successor publishes it through its writer. The Projector removes the settled wait. That result supplies no evidence that the sandbox was used.

A pending proposal still enters the usual argument checks and execute checkpoint. The successor uses its own mailbox for a fresh dispatch. If the predecessor already published the canonical result before the seal, recovery reads it from the accepted history. A physical reply in the predecessor mailbox alone leaves the outcome unknown. User Stop on takeover follows Pi's native abort path.

## Process-loss matrix

Every row ran in a separate Session. Each Worker had its own OS process, Pi Harness, Kafka producer and boot mailbox. The driver sent SIGKILL at the indicated boundary. The history projector read a real single-partition RF3 Session topic and ordinary callbacks used the per-boot topics. The successor used the sealed PG cold-read port. The fault driver issued the seal in Session-log order and drove claim/lease transitions against disposable PG; it did not measure a production lease expiry. A deterministic model requested a fixed Bash command that appended a marker inside a network-isolated scratch container. The guest process and marker outlived the Workers. The study used neither a paid model nor a deployed Cube VM.

| Boundary at process loss | Successor action and canonical outcome | Physical executions |
|---|---|---:|
| Proposal accepted, before execute intent | Proceed through validation and dispatch once from B; success | 1 |
| Execute intent accepted, callback outstanding | B settles unknown; late callback does not replace it | 1 |
| Execute intent accepted, dispatch lost before reaching the guest | B settles unknown | 0 |
| Physical reply acknowledged on A's topic, native result absent | B settles unknown | 1 |
| Native result acknowledged on the Session topic, PG projection held | PG still had zero results at crash; the result precedes the seal and B retains success | 1 |
| Native result already projected in PG | B retains success | 1 |
| B interrupted before publishing unknown; original physical reply arrives | C settles unknown exactly once | 1 |
| B interrupted after publishing unknown, before local adoption | C retains B's result and continues | 1 |
| Stop accepted before B resumes | Native task aborted; no replacement model call | 1 |

Every case verified one canonical result for the original call, zero remaining PG waiters and zero subscriptions to an old mailbox by the completing Worker. Every unknown settled before the original reply deadline. Normal continuation delivered the expected result to the model. Separate runner tests exercised the default Bash reconnect, opted-in Bash settlement and unchanged Android recovery. The Stop assertion covers native cancellation and does not establish physical process termination.

The RF3/PG transport suite passed 42 tests. Six focused Worker and remote-tool suites passed 98 tests. The storage, sandbox supervisor and Worker host type checks passed.

The experiment keeps the PG wait reference and existing broker retention policy intact. A subsequent storage comparison can evaluate replacing the mutable pending reference with an appended intent. [Decision](../adr/0359-sealed-bash-unknown.md), [aggregate outcomes](sealed-bash-unknown-20261125.json), [fault driver](../../scripts/experiments/minimal-session-storage/bash-loss-study.mjs).
