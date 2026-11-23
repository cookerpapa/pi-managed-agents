# Named child admission and callback recovery

The #52 candidate has an independent Node process for subagent creation,
management and supervisor questions. The existing Session Projector consumes
Kafka and obtains the ordered effect grant, then offers the request over the
private HTTP endpoint. Local in-flight requests and bytes are bounded. A
transport failure before admission may lose the offer. On a create timeout,
the model receives the name and a status call it can issue itself.

At admission PG locks the current root, resolves the stable parent and commits
the child Run and the request's receipt address in one synchronous transaction.
A name is unique by tenant, physical Session and stable parent. The public
UUID is derived from that namespace and the NFC name. The creation fingerprint
uses the submitted task, context choice and normalized requested Workspace
options. The original native owner, resolved grant and history cutoff are
frozen. Each operation keeps its own Kafka topic and call ID. The executor
places native work through the current Worker management endpoint. The
Worker's initial input for a named child uses the child invocation UUID.

The executor parks outstanding waits until a child outcome or supervisor question wakes them. It uses PG notifications, scans bounded indexed batches and
recovers timed-out claims. Terminal replies reference immutable answer entries.
The delivered observation is frozen before Kafka publication; marking a
delivery sent follows its broker acknowledgement. A repeated packet has the
same correlation and content. Completed root Runs release their delivery
records. The child history remains available for later named queries.

## Isolated acceptance

- Two real Pi tool tasks use the same name. The relay holds the earlier
  request while the later one registers first. PG records one child with the
  later caller as founder. The other caller can receive its status while
  the child is preparing.
- An ordinary history entry passes the same Kafka partition while that HTTP
  handoff is held. A new projection derives a valid recovery position even
  when the simulated Kafka low watermark moves past both original requests.
  This exercises the bounded checkpoint for a named wait. In-flight text,
  outstanding Guest operations and other independent recovery requirements
  continue to protect their own suffixes.
- Kill the executor after durable admission with placement outstanding; its
  replacement uses the same native child identity. The native parent and
  child complete through separate Kafka reply delivery.
- A child asks its parent for a decision. The parent receives the blocked child
  and request ID, answers, and sees the child's final answer.
- Hold the PG update which marks a real Kafka result sent, observe the
  packet, kill the process and roll back that update. On restart the identical
  packet is republished without starting another child. Tests advance the
  dead process's claim deadline rather than waiting for its visibility timeout.
- A caller deliberately misses an already published result, then uses a fresh
  boot mailbox in a later root Run. The same name returns the original answer
  and a changed task returns the existing UUID and a conflict.
- Before and after original admission a replacement Worker can register a fresh mailbox from its PG-sealed native task, preserving the named UUID and frozen branch point. Concurrent conflicting creates produce one child and a name conflict.
- Send and cancel can address an active child by name; the native Inbox and stop paths are exercised. A Fork can create its own child with the same name and receives a different UUID.
- The Worker timeout check uses a short injected test deadline and verifies
  one mailbox wait and the model-facing status lookup instruction.

The RF3/PG transport suite passed 40 checks across 23 files with the delegated browser enabled. The native turn runner and Worker tool tests passed 21 checks, including the named input ID and short wait deadline. Helm renders were checked with the feature both disabled and enabled.

The integration uses the fixed Pi SDK, an independently forked serving
executor, real RF3 Kafka and PG, and a Worker management HTTP fixture backed
by native Pi conversations. A deterministic model supplies text. No paid model
or Guest is needed for this control-only path.

## Running the candidate

With the matched root-only Session schema, Worker assignment and Supervisor directory installed, `global.namedSubagents=true` in
the platform Helm chart enables the executor, its private Service, the Projector
client and the matching Worker catalogue. The executor has its own Deployment and Service and currently shares the Session runtime image artifact. The setting defaults off in the existing deployment. For an isolated process use
`npm run start:subagent-executor --workspace @pi-cloud/control-plane`.
It reads the database URL, the direct notification URL, Kafka brokers and the
existing Supervisor management credential. The Session topic defaults to the
runtime's accepted-fact topic. Independently configured processes can set
`PI_CLOUD_SUBAGENT_EXECUTOR_URL` on the Projector, `PI_CLOUD_NAMED_SUBAGENTS=1`
on Workers and `PI_CLOUD_SESSION_LOG_TOPIC` on the executor for a private topic.

```sh
PI_CLOUD_DELEGATED_BROWSER=1 npm run test:transport
npm run helm:check
```

This slice remains under the #48 matched cutover gate. [Decision](../adr/0354-named-subagent-executor.md).
