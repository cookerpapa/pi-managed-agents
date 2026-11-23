# ADR-0353. One child invocation per native delegation task

[ADR-0354](0354-named-subagent-executor.md) supersedes child naming in the matched named-executor configuration. The native task still identifies the recovery checkpoint for each separate invocation.

In the delegated-Run candidate, the tenant, physical Session, root Run and native
tool task identify one child Run. Derive its public UUID from that tuple. Enforce
uniqueness of the source task within its root in PG. A new native tool task can
create a distinct child with the same prompt. Explicit continuation still names
the previous agent and receives a new invocation.

Worker mailbox operations keep independent correlation IDs. On Worker loss the
new owner can find the admitted execute checkpoint, the parent's frozen Run
anchor and the original assistant call in the existing PG handoff. It can derive
the child ID before a row exists. Reattachment verifies the task and source
before resuming, retains the immutable grant and returns through the new mailbox.
Replaying a pending request while its Worker survives uses the destination from
that accepted Kafka record. A closed caller cannot start work. Ordinary sandbox
commands keep their original execution and cancellation rules.

The candidate is validated in isolation before the matched serving cutover.
