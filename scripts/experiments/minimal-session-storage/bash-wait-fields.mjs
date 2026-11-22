// Field comparison for one ordinary fixed Bash call. The seal writer checks
// these assumptions against Pi's actual records before omitting either Task.
import assert from "node:assert/strict";
import { GenerationTask, ToolTask } from "@earendil-works/pi-durable";
import { validateToolArguments } from "@earendil-works/pi-ai/utils/validation";
import { fixedRemoteToolDefinitions } from "../../../packages/sandbox-supervisor/src/fixed-remote-tool-definitions.ts";

export function singleBashRecords({ entries, ops }) {
  assert.equal(ops.length, 1);
  const [op] = ops;
  assert.equal(op.reply.toolName, "bash");
  const candidates = entries
    .filter((row) => Number(row.commit_seq) <= op.seq)
    .flatMap((row) => {
      const entry = JSON.parse(row.content);
      if (entry.kind !== "pi.assistant" || entry.conversationId !== op.conversationId) return [];
      const calls = entry.model?.[0]?.content.filter((block) => block.type === "toolCall") ?? [];
      if (!calls.some((call) => call.id === op.reply.toolCallId && call.name === "bash")) return [];
      assert.equal(calls.length, 1, "Multi-call rounds require their own wait set");
      return [{ entry, call: calls[0], seq: Number(row.commit_seq) }];
    });
  assert.equal(candidates.length, 1, "Original assistant Entry is ambiguous or missing");
  const [{ entry, call, seq }] = candidates;
  assert(Number.isSafeInteger(entry.byTaskId));
  const bash = fixedRemoteToolDefinitions().find((tool) => tool.name === "bash");
  const args = validateToolArguments(bash, call);
  const common = { conversationId: op.conversationId, background: false, abortRequested: false };
  return [
    {
      seq,
      write: {
        type: "task",
        value: {
          ...common,
          id: entry.byTaskId,
          kind: "pi.generation",
          version: GenerationTask.definition.version,
          input: {},
          state: {
            status: "waiting",
            policy: "allSettled",
            on: [op.taskId],
            checkpoint: { phase: "tools", assistant: entry.id, tools: [op.taskId], pending: [] },
          },
        },
      },
    },
    {
      seq: op.seq,
      write: {
        type: "task",
        value: {
          ...common,
          id: op.taskId,
          owner: entry.byTaskId,
          kind: "pi.tool",
          version: ToolTask.definition.version,
          input: { assistant: entry.id, callId: op.reply.toolCallId },
          state: {
            status: "running",
            checkpoint: { phase: "execute", arguments: args, replay: bash.replay },
          },
        },
      },
    },
  ];
}

export async function omitVerifiedBashWait(db, runId, state) {
  const {
    rows: [r],
  } = await db.query("select projection_tools from runs where id=$1", [runId]);
  const ops = Object.values(r.projection_tools);
  const { rows: entries } = await db.query(
    "select commit_seq,content from message_entries where run_id=$1",
    [runId],
  );
  const records = singleBashRecords({ entries, ops });
  for (const row of records) {
    const index = state.records.findIndex(
      (saved) => saved.write.type === "task" && saved.write.value.id === row.write.value.id,
    );
    assert(index >= 0);
    assert.deepEqual(
      row,
      state.records[index],
      "Stable PG values did not reproduce the original native record",
    );
    state.records.splice(index, 1);
  }
}
