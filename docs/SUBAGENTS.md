# Subagents

PiCloud exposes `subagent` for direct delegation, `subagent_control` for discovery
and management. Users request these
in ordinary language; they do not configure Workers or Kafka.

## Direct task

```js
subagent({ task: "Check the collision logic", context: "branch", workspace: { compute: "shared" } })
```

`fresh` starts from the task. `branch` inherits the frozen history before the
current parent prompt, not intermediate reasoning or Tool results produced
during that Run; pass any such findings explicitly in the child task.
`workspace: { compute: "shared" }` uses the parent's compute environment; `"ephemeral"` uses a
separate temporary Cube with the **same persistent Volume**. Optional absolute
`cwd` selects an existing directory and otherwise inherits the parent's frozen
Run directory. Shared descendants keep the parent's compute scope. Context
inheritance, compute placement and directory selection are independent.

Child conversation views show an inherited text-context preview. After native
Compaction, the summary is labelled separately from retained messages. Long
previews are bounded and marked as partial, including in exports; the parent
conversation retains its full history. This display budget does not trim the
child's actual model context (ADR-0205).

Task descriptions are limited to 100,000 characters and 256 KiB after JSON UTF-8
encoding, including escaping. Oversized tasks fail before child admission;
use inherited context or a Workspace file path for larger input. Task text must
be nonempty, valid Unicode without NUL characters. The native startup record
retains its original fields and the full admitted text (ADR-0204).

The parent can run `git worktree add` through Bash, then pass that worktree's
path as `cwd`. All Cubes mount the Volume at the same path, so Git's shared
metadata remains valid and the parent can inspect or merge locally. PiCloud
does not initialize Git, commit/stash, merge, copy files or remove worktrees on
task completion. Uncommitted changes are not automatically included in a new
worktree. Sharing writable storage is collaboration, not security isolation.

Elastic storage is mounted at `/workspace`; a machine's shared home Volume is
mounted at `/home/user`. Other machine system-disk directories cannot be selected
for ephemeral compute. Missing or inaccessible directories fail before child
execution, never fall back or create directories implicitly.

Optional `workspace.tools` narrows file/shell permissions. `workspace: null` disables workspace access, while
hosted search and delegation keep their own policies. Direct tasks allocate no Cube until an actual local
operation. Temporary compute follows the existing elastic idle TTL; recycling
compute does not remove the shared Volume or worktree.

## Communicating with a child

The parent calls `subagent` once per task. It receives a result or a supervisor
question. Use the child's execution ID with `subagent_control` to check progress,
wait, send another message or cancel. `subagent_control({action:"list"})`
recovers IDs when needed. A child asks for a decision using `contact_supervisor`;
the parent answers with `subagent_supervisor` and the returned request ID.

## Cloud contract

Requests go through the owning Worker's existing Kafka publication. One
Projector reconciles control operations from native Tool intents. PostgreSQL
materializes the native log and existing child relationships; there is no second
command queue or raw-result ledger. The Worker Session Host owns all native
Conversations (Lanes) and commit sequence allocation. Pi mints each Conversation
ID and its creation receipt. Child output uses the same native log as parent
output. Internal notifications never use the public model egress proxy.

The partition owner retains pending references and a bounded cache of replies
awaiting publication. Native outcomes and committed PG supervisor changes wake
waiters; unchanged reads do not wake themselves. Timers cover known retries and
expiry. Reassignment reloads nonterminal references. An unanswered supervisor
decision takes precedence over later progress updates. Multiple decisions are
ordered by creation time, then request ID; ended native callers and replaced
writers no longer block a child or accept new replies.

A family occupies one Worker slot, one owner lease and one cloud Run. Children
are native Conversations and Tasks, not additional Run queue rows. There is no
overall Agent duration limit. Actual model requests (including Compaction) take
a fair permit and release it before waiting for children or Tools. Cancelling one
task does not revoke the common owner. Losing the owner closes that incarnation
before PG authorizes recovery of the family under a new lease.

Direct delegation is native replay-safe. The restored caller publishes a small
reference to its original immutable intent under its current writer; the
Projector resolves the same child and initial Submission, and sends the original
outcome to the current boot mailbox. It does not replay the old mailbox or select
a later unrelated child answer. Cancelled or terminal callers cannot continue.
Unsafe Tools inside the child remain unsafe; this wrapper does not replay Bash.

`subagent_control({action:"list"})` discovers existing children without an ID.
It includes completed children of the current root work, source Task identity,
parent relationship, state and a short task summary. Follow `nextCursor` for the
next page. Use the returned execution ID with `status`, `wait`, `send` or `cancel`.
These management operations are unsafe on native crash recovery.
Inspect existing effects before deciding to repeat them. Similar prompts in new
Tool calls intentionally create distinct children.

Children retain generation ownership so a direct delegation can return a
supervisor question while the child remains available for the parent's answer.

See [ADR-0166](adr/0166-log-driven-subagents.md),
[ADR-0171](adr/0171-shared-volume-subagent-compute.md),
[ADR-0264](adr/0264-subagent-recovery-and-discovery.md) and [configuration](CONFIGURATION.md).
