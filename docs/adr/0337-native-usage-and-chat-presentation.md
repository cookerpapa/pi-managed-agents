# ADR-0337. Retain current native usage and index chat-visible history

Accepted for the controlled [#48](https://github.com/cookerpapa/pi-managed-agents/issues/48) performance candidate after the complete-message review.

## Storage ownership

Pi continues to publish its entire original atomic Storage batch on the physical Session log. A stable answer can both resolve the input and generation Task, settle live output, and replace the cumulative usage document. The assistant Entry keeps its provider usage. Cloud charge eligibility and provider evidence retain their existing independent accounting contracts.

The pinned Pi UsageDoc is conversation-scoped, has latest history, checkpoints each update and starts fresh at a Fork. Store its current complete value in the existing `pi_durable_current` table together with its record identity and the native commit's current-value digest. The shared index, replay conflict checks, retirement and rebuild use the same policy as the other current-state documents. A normal answer therefore needs no separate usage revision index and stores no earlier cumulative payload. The public session usage query reads the unkeyed document for each requested Conversation in a batch. An empty new Conversation has zero usage. An indexed usage document with a missing current body is incomplete and must fail the read.

## Presentation

The native `pi.system` Entry establishes model context and tool declarations. It remains in the immutable native history and in the shared native ancestry of a Fork. Its content has no chat item. Exclude it before live page limits and during frozen history capture, including mixed batches and cold administrative capture. A mixed batch still presents its other eligible writes. Native sequence, display coverage, user/assistant navigation, Tool pairing and fork/prune boundaries retain their respective meanings.

## Matched cutover and evidence

The writer, native readers and public history/usage readers change together. Development histories using the former storage format may be retired after drain through normal conversation deletion and reference collection. Install the new layout on a clean compatible history set. The new reader has a single source for usage. Release and measurement follow the #48 gate.

Verify repeated cumulative updates, duplicate/changed commits, missing state, cold reconstruction, initial Fork usage, original/fork model context, and forward/backward history pages before and after settlement. Compare isolated 192-client synthetic waves with the pre-change #50 implementation on the same host, including visible items, SQL calls by native batch, model-to-terminal latency, consumer lag and complete resource cleanup.

[Implementation and measurements](../reports/native-usage-and-chat-history-20261118.md).
