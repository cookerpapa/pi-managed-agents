# ADR-0351. Start the serving integration with addressed child history

Adopt the measured Conversation Entry index in the isolated #52 schema. Keep the
published performance comparisons reversible in disposable databases.

Expose a read-only Session API for the directory and the history of a stable
delegated agent within its root Session. The first delegated Run supplies the
stable address; each subsequent invocation has a separate Run ID. Only the
authenticated tenant and named root can address those entries. Use the
existing transcript presentation vocabulary and cursor-based bounded reads.
The browser can open this view without creating a child Session or starting a
Worker. Enable these routes only in the matched candidate composition.

On the write side, complete native control references may cross the candidate
Worker wire with their original reply address. The trusted native decoder validates the reference against the same
committed tool task. Physical sandbox routing stays on the execution side. Keep the
ordered eligibility and replay rules of the serving dispatcher. Verify its cold
source before switching any serving deployment to the new store.

## Workspace grant on continuation

The owner chose to intersect the previous invocation's effective Workspace
grant with the current caller's grant. Store the effective logical grant on
each child Run at admission. A retry compares the same frozen choices under
the same root writer. An explicit continuation intersects the preceding
invocation, including when a cold Worker is selected. No compatible Workspace
or no remaining tool yields no Workspace access. Keep a private ephemeral
selector separate from the parent's shared selector. Install the resulting
file-tool selection in the native child's agent document before its first
model request. An earlier prompt, tool list or forked context cannot enlarge it.

[ADR-0352](0352-retire-workflow-tool.md) retires live script hostcalls.
The release gate now exercises one native `subagent` request before and after
child admission. The [initial experiment](../reports/direct-delegation-handoff-20261123.md) isolated missing delivery identity in a PG-only handoff. [ADR-0353](0353-native-task-delegation-identity.md) resolves direct delegation by native task.
