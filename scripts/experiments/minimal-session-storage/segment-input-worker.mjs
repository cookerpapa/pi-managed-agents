import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import { inputWithUploads } from "@pi-cloud/protocol";
import { sql } from "kysely";
import {
  DurableHumanInputs,
  NativeInputError,
} from "../../../packages/sandbox-supervisor/src/durable/human-inputs.ts";

// One pump per owned Session. A hint arriving during a read sets dirty so that
// an older empty result can never swallow it. Each page bounds Worker memory.
function describeFiles(text, uploads) {
  if (uploads.some((file) => file.image))
    throw Error("Image input requires the Worker attachment preparation port");
  return inputWithUploads(text, uploads);
}
export function connectSessionInputs({
  inputs,
  job,
  storage,
  harness,
  conversation,
  prepare = describeFiles,
  fail,
}) {
  const scope = job.scope;
  let stopped = false,
    dirty = false,
    draining,
    failure;
  const humans = new DurableHumanInputs({
    storage,
    conversation,
    runId: job.id,
    prepare,
    sequence: () => 0,
    accepting: () => !stopped,
    assertCurrent: async () => job.authority.assertActive(),
  });
  async function deliver(m) {
    job.authority.assertActive();
    const requestId = m.id;
    if (m.withdrawing) {
      // Reconciliation on the new fenced owner can see an intent whose original
      // submit never happened. Record that decision on the native writer too.
      await conversation.commit(async (tx) => {
        let sub = await tx.submissionByRequest(conversation.id, requestId);
        if (!sub) {
          if (m.native_id !== null)
            throw Error("Projected input is missing from restored native storage");
          sub = await tx.createSubmission({
            conversationId: conversation.id,
            requestId,
            type: "input",
            status: "queued",
          });
          tx.settleSubmission(sub.id, { status: "unanswered", reason: "aborted" });
        }
      }, ctx);
      const record = await storage.submissionByRequest(conversation.id, requestId, ctx);
      if (record?.status === "queued") {
        await harness.abortSubmission(record.id, ctx, conversation.id);
      }
      return;
    }
    if (m.source === "user") {
      await humans.submit(
        {
          inputId: requestId,
          idempotencyKey: m.request_key,
          fingerprint: m.fingerprint,
          text: m.body,
          uploads: m.uploads,
          mode: m.mode === "follow_up" ? "follow_up" : "steer",
          acceptedAt: m.accepted_at.toISOString(),
        },
        m.sender_id,
      );
    } else {
      const content = await prepare(m.body, m.uploads);
      await conversation.submit(
        {
          type: "input",
          requestId,
          whenBusy: m.mode === "follow_up" ? "followUp" : "steer",
          content:
            typeof content === "string" && m.mode !== "input" && m.sender_id !== m.session_id
              ? "[From Agent " + m.sender_id + "]\n" + content
              : content,
        },
        ctx,
      );
    }
  }
  async function reconcile() {
    let after = 0,
      rows;
    do {
      rows = await inputs.take(scope, after);
      for (const m of rows) {
        if (stopped) return;
        await deliver(m);
        after = m.position;
      }
    } while (rows.length === 32 && !stopped);
    const c = await inputs.changeFor(scope);
    if (!c || stopped) return;
    const snapshot = await humans.snapshot(),
      id = c.message_id;
    try {
      await humans.mutate(
        {
          operationId: c.id,
          fingerprint: c.id,
          inputId: id,
          revision: snapshot.revision,
          change:
            c.action === "steer" ? { action: "steer" } : { action: "move", direction: c.action },
        },
        c.other_id ?? undefined,
      );
    } catch (e) {
      if (!(e instanceof NativeInputError) || e.code !== "input_conflict") throw e;
      await sql`update session_input_changes c set state='conflict' from runs r where c.tenant_id=${scope.tenantId}::uuid and c.session_id=${scope.sessionId}::uuid and c.id=${c.id}::uuid and c.state='pending'
        and r.id=${job.id}::uuid and r.lease_id=${scope.writerId}::uuid and r.fencing_token=${scope.fencingToken} and r.output_seal_id is null`.execute(
        inputs.db,
      );
    }
  }
  function wake() {
    if (stopped) return;
    dirty = true;
    draining ??= (async () => {
      while (dirty && !stopped) {
        dirty = false;
        await reconcile();
      }
    })()
      .catch((e) => {
        stopped = true;
        failure = e;
        fail(e);
      })
      .finally(() => {
        draining = undefined;
        if (dirty && !stopped) wake();
      });
  }
  job.onInput(wake);
  return {
    async flush() {
      wake();
      while (draining) await draining;
      if (failure) throw failure;
    },
    async close() {
      stopped = true;
      await draining;
      if (failure) throw failure;
    },
  };
}
