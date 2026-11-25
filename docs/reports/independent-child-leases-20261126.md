# Independent child Session ownership on the segmented candidate

A named delegation now creates a child Session and a first queued Turn. An available Worker can claim it while the parent is running. Management ancestry and inherited history stay in the existing Session and Flow directories. Each physical Session consumes one execution slot and holds one renewable lease. A Worker with two slots can run a parent and a child at the same time. [Decision](../adr/0363-independent-agent-session-leases.md).

## Execution path

1. Pi commits the parent tool intent to the Session log. After the Kafka dispatch grant, the Projector forwards the immutable request position through the bounded named-tool HTTP client. The executor commits the child's frozen history, initial Turn and return address in one PG transaction. Repeating the same name and creation arguments resolves the original child.
2. Workers claim ready Turns in mailbox order with PG row locks and `SKIP LOCKED`. Admission verifies the registered boot, the active management connection and capacity. The claim advances the child's stable physical Session epoch and inserts its lease in the same transaction. The normal PG lease coordinator and heartbeat observer handle renewal and local expiry. Parent lease state and parent placement are absent from the claim query.
3. The winning Worker opens a separate Pi root. For a first branch Turn, the Worker loads the frozen prefix and publishes a small native bootstrap. Inherited message bodies stay in their original Flow. A fresh child starts with its own input. A replacement opens the sealed Session's native state and resumes the original Turn.
4. Expiry records a recovery intent for that specific writer. The existing Session partition carries its seal; PG publishes the Turn back to the queue only when the Projector commits the matching checkpoint, seal position and lease release. A failed checkpoint leaves the old lease allocated. Late completions from an old writer fail the scope check.
5. The child's terminal transaction wakes the existing named return dispatcher. It reads the canonical child answer and sends the result to the current parent waiter's Kafka mailbox with the original tool call ID. A replacement parent establishes a new mailbox route from its sealed task. Closing a parent closes its waiters; it does not stop children. A pending return protects the answer during physical history collection.

## Fault acceptance

The acceptance uses disposable PostgreSQL, an RF3 Kafka cluster, ordinary HTTP admission, the actual Pi Harness with a simulated provider, and separate Node Worker processes. Native result and scope assertions use the same DB/log consumers as the candidate runtime. Child Worker leases were 2.4 seconds in the crash tests.

| Scenario | Observed outcome |
|---|---|
| Two eligible Workers race for the same child | One claim succeeds. Parent and child have different lease IDs. |
| A parent Worker dies while its child renews | Only the parent expires. Its original Turn becomes available after its ordered seal. The child lease continues renewing. |
| A branch child dies with an outstanding model request | Another Worker restores the same child Turn at the next epoch, with the same model input. The parent receives one canonical tool result. This case also restarts the Projector and replays its active prefix. |
| A fresh child loses two owners, the second before publishing its first native write | The third owner restores the same Turn and input. The two predecessor epochs each have a committed seal. |
| A waiting parent loses its owner after child admission | The replacement opens a new boot mailbox from the sealed delegation, receives the child's answer and finishes. The child has one lease over the entire exchange. |
| One Worker has two slots and the child creates a grandchild | Parent and child can coexist. Once a slot opens, the grandchild is claimed independently. A child receiving another Turn after history pruning keeps its Session ID and advances its epoch on the new Flow. |

The tests cover storage and coordination faults, including rollback before seal confirmation, duplicate seals, a mismatched seal ID and old-writer completion. A model invocation interrupted by a process loss can be attempted again. The durable checks count canonical user input and tool results. They do not count provider execution as exactly once.

## Running the candidate

The [runtime composition](../../scripts/experiments/minimal-session-storage/independent-session-runtime.mjs) exposes the authenticated executor endpoint, bounded HTTP client, group-aware consumer, Worker factory, native bootstrap and expiry publisher. Install the [lease schema](../../scripts/experiments/minimal-session-storage/independent-session-leases.sql) through `installIndependentSessionLeases` after the segmented tables on a database with the normal Worker/lease migrations. Each Worker uses its registered identity. Its `serve` method accepts the existing work wake, an abort signal, an execution callback and an error handler. The control role runs `reconcile` with its ordered Session-log publisher. Each scan uses PG deadlines; a failed publication retains the same seal identity for retry. [Worker and lease adapter](../../scripts/experiments/minimal-session-storage/independent-session-leases.mjs).

The RF3/PG transport suite passed 49 tests in 27 files. The existing PG admission, parallel claim and lease observer suites passed 84 tests. Run `npm run test:transport` for the isolated RF3/PG integration. The [fault scenarios](../../scripts/experiments/minimal-session-storage/independent-session-leases.integration.spec.mjs) include both parent and child takeover. The serving Helm roles retain their existing ownership model. Slot starvation when every Worker is occupied by a waiting parent, sidebar subtree controls, the full provider matrix and a matched #48 rollout are separate work. No new throughput rate was inferred from these fault tests.
