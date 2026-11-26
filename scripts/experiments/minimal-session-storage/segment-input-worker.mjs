import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import { InboxDoc, LiveDoc, UserEntry } from "@earendil-works/pi-durable";
import { inputWithUploads } from "@pi-cloud/protocol";

function describeFiles(text, uploads) {
  if (uploads.some((file) => file.image))
    throw Error("Image input requires the Worker attachment preparation port");
  return inputWithUploads(text, uploads);
}

// All mutable mail remains in PG. Only an actual native boundary can adopt it.
export function connectSessionInputs({
  inputs,
  job,
  conversation,
  harness,
  storage,
  prepare = describeFiles,
}) {
  let closed = false,
    failure,
    lastSubmission;
  async function content(m, signal) {
    const value = await prepare(m.body, m.uploads, signal);
    return m.source === "agent" &&
      m.mode !== "input" &&
      m.sender_id !== m.session_id &&
      typeof value === "string"
      ? "[From Agent " + m.sender_id + "]\n" + value
      : value;
  }
  async function postTools(taskId, context) {
    const ticket = job.steerHint.ticket;
    if (closed) return;
    let m,
      publicationPossible = false;
    try {
      job.authority.assertActive();
      if (ticket === null) return;
      m = await inputs.selectAt(job.scope, "postTools", taskId, () => job.steerHint.clear(ticket));
      if (!m) return;
      const value = await content(m, context.abortSignal);
      const adopted = await conversation.commit(async (tx) => {
        const live = await tx.doc(LiveDoc, conversation.id);
        if (live.run?.taskId !== taskId || !live.tools?.length) return;
        const inbox = await tx.doc(InboxDoc, conversation.id);
        // Reset, handoff and tool-requested termination close the native Run.
        // The next idle submission then belongs to the new Run.
        if (inbox.items.some((i) => i.mode !== "write" || i.entry.head === "self")) return;
        const controls = await Promise.all(
          live.tools.map(async (slot) => {
            if (!slot.taskId) return;
            const task = await tx.task(slot.taskId);
            return task?.state.outcome?.result?.control;
          }),
        );
        if (controls.every((c) => c?.terminate) || controls.some((c) => c?.handoff !== undefined))
          return;
        if (await tx.submissionByRequest(conversation.id, m.id))
          throw Error("Session input was already adopted");
        context.abortSignal?.throwIfAborted();
        job.authority.assertActive();
        // Pi normally places passive writes (e.g. compaction) before steers.
        // Keep that ordering in the same public transaction as the new entry.
        if (inbox.items.length) {
          let head = (await tx.latestHeadMarker(conversation.id))?.head;
          for (const item of inbox.items) {
            const draft = item.entry;
            if (typeof draft.head === "number" && head !== undefined && draft.head < head) {
              tx.settleSubmission(item.id, { status: "unanswered", reason: "stale" });
            } else {
              const entry = await tx.appendEntry(conversation.id, draft);
              tx.placeSubmission(item.id, entry.id);
              if (draft.head !== undefined) head = draft.head === "self" ? entry.id : draft.head;
            }
          }
          inbox.items.splice(0, inbox.items.length);
        }
        const entry = await tx.appendEntry(UserEntry, conversation.id, {
          model: [{ role: "user", content: value, timestamp: Date.now() }],
        });
        const sub = await tx.createSubmission({
          conversationId: conversation.id,
          requestId: m.id,
          type: "input",
          status: "placed",
          entry: entry.id,
        });
        live.run.inputs.push(sub.id);
        publicationPossible = true;
        return true;
      }, context);
      if (!adopted) await inputs.releaseBeforePlacement(job.scope, m.id);
      else if (m.more_steers === false) job.steerHint.clear(ticket);
    } catch (error) {
      // Pi reports extension exceptions. Keep the same failure at the Worker
      // boundary so it cannot publish successful completion after a lost write.
      if (!context.abortSignal?.aborted) failure = error;
      if (m && !publicationPossible) await inputs.releaseBeforePlacement(job.scope, m.id);
      throw error;
    }
  }
  async function admitIdle(at) {
    if (closed) return false;
    if (failure) throw failure;
    job.authority.assertActive();
    const m = await inputs.selectAt(job.scope, at);
    if (!m) return false;
    let value;
    try {
      value = await content(m);
      job.authority.assertActive();
    } catch (error) {
      await inputs.releaseBeforePlacement(job.scope, m.id);
      throw error;
    }
    lastSubmission = await conversation.submit(
      { type: "input", requestId: m.id, content: value, whenBusy: "reject" },
      ctx,
    );
    return true;
  }
  return {
    postTools,
    assertHealthy() {
      if (failure) throw failure;
    },
    start: () => admitIdle("start"),
    async finish() {
      if (!lastSubmission) {
        const active = await harness.snapshot(LiveDoc, conversation.id, ctx);
        const id =
          active?.run?.inputs[0] ??
          (job.recovering ? await inputs.lastPlaced(job.scope) : undefined);
        const record = id
          ? { id }
          : job.id && (await storage.submissionByRequest(conversation.id, job.id, ctx));
        if (record) {
          lastSubmission = await harness.submission(record.id, ctx);
          if (!lastSubmission) throw Error("Restored native input is missing");
        }
      }
      let receipt;
      do {
        await conversation.waitForIdle(ctx);
        if (failure) throw failure;
        receipt = await lastSubmission?.status(ctx);
        if (receipt?.status === "unanswered" && receipt.reason !== "reset") break;
      } while (await admitIdle("idle"));
      return receipt;
    },
    close() {
      closed = true;
    },
  };
}
