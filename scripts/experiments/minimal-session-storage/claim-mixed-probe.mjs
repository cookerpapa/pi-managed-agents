// Test-only image entry. Replay one identical fixed-Pi trace through the real
// claiming Worker producer and ordered Projector. No shell is executed.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { setTimeout as delay } from "node:timers/promises";
import { KafkaWorkerProducer } from "@pi-cloud/event-log";
import { kafkaProducerLane } from "@pi-cloud/runtime-core/kafka-accepted-fact";
import { SegmentWorker } from "./independent-session-leases.mjs";
import { SegmentMailReader } from "./segment-mail-reader.mjs";
import { FamilyPresentation } from "./family-presentation.mjs";
import { encodeWorkspaceFact } from "./workspace-operations.mjs";
const { frames, requestId } = JSON.parse(await readFile("/run/pma/mixed-trace", "utf8"));
const start = KafkaWorkerProducer.prototype.start,
  serve = SegmentWorker.prototype.serve;
let producer;
KafkaWorkerProducer.prototype.start = async function (...args) {
  const result = await start.apply(this, args);
  producer = this;
  return result;
};
SegmentWorker.prototype.serve = function (options) {
  const inputs = new SegmentMailReader(this.db);
  return serve.call(this, {
    ...options,
    execute: async (job) => {
      if (job.initial_input !== "Run the fixed test tool.") return options.execute(job);
      const mail = await inputs.selectAt(job.scope, "start");
      assert(mail);
      const scope = job.scope,
        lane = kafkaProducerLane(scope.sessionId, Number(process.env.PI_CLOUD_SEGMENT_PARTITIONS)),
        presentation = new FamilyPresentation();
      presentation.bind(1, scope);
      async function emit(f) {
        job.authority.assertActive();
        await producer.append(lane, {
          factId: f.factId,
          sessionId: scope.sessionId,
          value: encodeWorkspaceFact({ ...f, scope }),
        });
      }
      for (const frame of frames) {
        const commit = {
          ...frame,
          writes: frame.writes.map((w) =>
            w.type === "submission" && w.value.requestId === requestId
              ? { ...w, value: { ...w.value, requestId: mail.id } }
              : w,
          ),
        };
        if (commit.writes.some((w) => w.type === "entry" && w.value.kind === "pi.tool-result"))
          await delay(50);
        const display = presentation.prepare(commit);
        await emit({
          kind: "durable_commit",
          factId: randomUUID(),
          occurredAt: new Date().toISOString(),
          commit,
          tools: [],
          origins: [],
          cancellations: [],
          display: display.display,
        });
        display.confirmed();
      }
      await emit({
        kind: "work_finished",
        agentId: "root",
        factId: randomUUID(),
        occurredAt: new Date().toISOString(),
        nativeSequence: frames.at(-1).seq,
        baseSequence: presentation.sequences.get(scope.sessionId),
        terminal: { type: "turn.completed", payload: { stopReason: "stop" } },
      });
    },
  });
};
await import("../../../packages/supervisor-host/src/independent-session-worker-main.mjs");
