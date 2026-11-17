import {
  parsePiCloudEvent,
  parseDurableDisplayDescription,
  parseSessionActivity,
  parsePathProbe,
  parseSessionControlChange,
  parseToolCancellation,
  parseDurableSessionCommit,
  DurableCommitIndex,
  parseModelExecutionEvidence,
  type AcceptedToolCommand,
} from "@pi-cloud/protocol";
import type { AcceptedFact, SessionLogFact } from "./accepted-fact.ts";
import { toolCommandsForFact, validateControlToolReferences } from "./tool-invocation-fact.ts";
import { expandSessionLogEnvelope } from "./session-log-envelope.ts";
export { encodeSessionLogFact } from "./session-log-envelope.ts";
export {
  ACCEPTED_FACT_TOPIC,
  loadProducerCapacity,
  type ProducerCapacity,
} from "@pi-cloud/event-log";

export function kafkaProducerLane(sessionId: string, lanes: number): number {
  if (!Number.isSafeInteger(lanes) || lanes < 1) throw new TypeError("Invalid partition count");
  const laneCount = lanes;
  let hash = 2_166_136_261;
  for (let index = 0; index < sessionId.length; index += 1) {
    hash ^= sessionId.charCodeAt(index);
    hash = Math.imul(hash, 16_777_619);
  }
  return (hash >>> 0) % laneCount;
}

export function parseKafkaAcceptedFact(value: string | Buffer): AcceptedFact {
  const prepared = decodeSessionLogFact(value);
  if (prepared.fact.kind === "control_change") throw new Error("Expected execution fact");
  return prepared.fact;
}

export function parseSessionLogFact(value: string | Buffer): SessionLogFact {
  return decodeSessionLogFact(value).fact;
}

export type PreparedSessionLogFact = Readonly<{
  fact: SessionLogFact;
  tools: readonly (
    AcceptedToolCommand | import("@pi-cloud/protocol").AcceptedExecutorToolCommand
  )[];
}>;

export function decodeSessionLogFact(value: string | Buffer): PreparedSessionLogFact {
  const parsed = expandSessionLogEnvelope(JSON.parse(value.toString()));
  return parsed?.kind === "control_change"
    ? { fact: parseSessionControlChange(parsed), tools: [] }
    : prepareAcceptedFact(parsed);
}

export function parseAcceptedFact(value: unknown): AcceptedFact {
  return prepareAcceptedFact(value).fact;
}

export function prepareAcceptedFact(
  value: unknown,
): PreparedSessionLogFact & { fact: AcceptedFact } {
  const fact = validateAcceptedFact(value);
  const index = fact.kind === "durable_commit" ? new DurableCommitIndex(fact.commit) : undefined;
  if (fact.kind === "durable_commit" && fact.modelEvidence !== undefined) {
    if (!Array.isArray(fact.modelEvidence) || fact.modelEvidence.length > 256)
      throw new Error("Invalid model accounting evidence batch");
    const identities = new Set<string>();
    for (const raw of fact.modelEvidence) {
      const { context } = parseModelExecutionEvidence(raw);
      const execution = context.execution!;
      const task = index!.task(execution.taskId!);
      const origin = fact.origins.find((origin) => origin.recordId === execution.taskId);
      if (
        context.tenantId !== fact.scope.tenantId ||
        context.piSessionId !== fact.scope.piSessionId ||
        context.runId !== fact.scope.runId ||
        execution.writerId !== fact.scope.writerId ||
        execution.fencingToken !== fact.scope.fencingToken ||
        task?.conversationId !== context.conversationId ||
        origin?.sessionId !== context.sessionId ||
        identities.has(context.invocationId)
      )
        throw new Error("Model accounting evidence changed execution identity");
      identities.add(context.invocationId);
    }
  }
  validateControlToolReferences(fact, index);
  return { fact, tools: toolCommandsForFact(fact, index) };
}

function validateAcceptedFact(value: unknown): AcceptedFact {
  const parsed = value as AcceptedFact;
  if (parsed.kind === "session_activity") return parseSessionActivity(value);
  if (parsed.kind === "path_probe") return parsePathProbe(value);
  if (parsed.kind === "durable_commit") {
    const commit = parseDurableSessionCommit(parsed.commit);
    return {
      ...parsed,
      commit,
      display: parsed.display.map((display) => {
        if (
          display.coveredThrough !== undefined &&
          (!Number.isSafeInteger(display.coveredThrough) || display.coveredThrough < 0)
        )
          throw new Error("Invalid native display coverage");
        return { ...display, events: display.events.map(parseDurableDisplayDescription) };
      }),
      cancellations: parsed.cancellations.map(parseToolCancellation),
    };
  }
  if (parsed.kind === "agent_event") {
    return { ...parsed, event: parsePiCloudEvent(parsed.event) };
  }
  if (parsed.kind === "execution_seal") {
    return parsed;
  }
  if (parsed.kind === "work_finished") {
    if (
      !Number.isSafeInteger(parsed.nativeSequence) ||
      parsed.nativeSequence < 0 ||
      !Number.isSafeInteger(parsed.baseSequence) ||
      parsed.baseSequence < 0
    )
      throw new Error("Invalid work completion boundary");
    parsePiCloudEvent({
      schemaVersion: 1,
      eventId: parsed.factId,
      sessionId: parsed.scope.sessionId,
      turnId: parsed.scope.turnId,
      agentId: parsed.agentId,
      seq: parsed.baseSequence + 1,
      occurredAt: parsed.occurredAt,
      ...parsed.terminal,
    });
    return parsed;
  }

  throw new TypeError("Kafka AcceptedFact kind is invalid");
}
