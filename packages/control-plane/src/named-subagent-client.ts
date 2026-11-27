import { Agent, fetch } from "undici";
import { Type, type Static } from "typebox";
import { Value } from "typebox/value";
import {
  durableControlIntent,
  durableControlRecovery,
  isControlToolName,
} from "@pi-cloud/protocol";
import type { DurableControlToolDispatch } from "@pi-cloud/runtime-core/accepted-fact";
import type { KafkaAcceptedFactRecord } from "@pi-cloud/runtime-core/kafka-accepted-fact-consumer";
import { TOOL_REPLY_TOPIC_PATTERN } from "@pi-cloud/event-log";

export const namedExecutorPath = "/internal/v1/subagents/accept";
const uuid = Type.String({ pattern: "^[a-fA-F0-9]{8}-(?:[a-fA-F0-9]{4}-){3}[a-fA-F0-9]{12}$" });
const integer = Type.Integer({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER });
const offerSchema = Type.Object(
  {
    tenant: uuid,
    session: uuid,
    root: uuid,
    writer: uuid,
    epoch: integer,
    lane: integer,
    task: integer,
    operation: uuid,
    destination: Type.String(),
    tool: Type.String(),
    anchor: Type.Union([Type.Null(), integer]),
    through: Type.Optional(integer),
    requestEntry: Type.Optional(integer),
    assistantEntry: Type.Optional(integer),
    input: Type.Union([
      Type.Object(
        {
          call: Type.String({ minLength: 1, maxLength: 4096 }),
          args: Type.Record(Type.String(), Type.Unknown()),
          seq: integer,
        },
        { additionalProperties: false },
      ),
      Type.Object({ recover: uuid, seq: integer }, { additionalProperties: false }),
    ]),
  },
  { additionalProperties: false },
);
export type SubagentOffer = Static<typeof offerSchema>;
export function parseSubagentOffer(value: unknown): SubagentOffer {
  if (
    !Value.Check(offerSchema, value) ||
    !TOOL_REPLY_TOPIC_PATTERN.test(value.destination) ||
    !isControlToolName(value.tool) ||
    ("recover" in value.input && value.tool !== "subagent")
  )
    throw Error("Invalid subagent offer");
  return value;
}

export class NamedSubagentClient {
  readonly #pool = new Agent({ connections: 32, connect: { timeout: 5000 } });
  readonly #active = new Set<Promise<void>>();
  #abort = new AbortController();
  #bytes = 0;
  constructor(
    readonly url: string,
    readonly token: string,
  ) {}
  get activeCount() {
    return this.#active.size;
  }
  offer(
    fact: Extract<KafkaAcceptedFactRecord["fact"], { kind: "durable_commit" }>,
    ref: DurableControlToolDispatch,
    through?: number,
    origin?: { requestEntry: number; assistantEntry: number },
  ) {
    if (this.#abort.signal.aborted || this.#active.size >= 1024) return;
    const source = ref.recovery
      ? durableControlRecovery(fact.commit, ref)
      : durableControlIntent(fact.commit, ref);
    const offer: SubagentOffer = {
      tenant: ref.scope.tenantId,
      session: ref.scope.piSessionId,
      root: ref.scope.runId,
      writer: ref.scope.writerId,
      epoch: ref.scope.fencingToken,
      lane: ref.conversationId,
      task: ref.taskId,
      operation: ref.operationId,
      destination: ref.replyTopic,
      tool: ref.toolName,
      anchor: ref.contextAnchor,
      ...origin,
      ...(through === undefined ? {} : { through }),
      input:
        "callId" in source
          ? { call: source.callId, args: source.arguments, seq: fact.commit.seq }
          : { recover: source.operationId, seq: source.sequence },
    };
    // The Kafka grant precedes this best-effort offer. Losing it leaves the
    // Worker with its bounded wait and the same name for an intentional retry.
    const body = JSON.stringify(offer),
      bytes = Buffer.byteLength(body);
    if (this.#bytes + bytes > 16 * 1024 * 1024) return;
    this.#bytes += bytes;
    const work = fetch(new URL(namedExecutorPath, this.url), {
      method: "POST",
      dispatcher: this.#pool,
      redirect: "error",
      headers: { "content-type": "application/json", authorization: "Bearer " + this.token },
      body,
      signal: AbortSignal.any([this.#abort.signal, AbortSignal.timeout(10000)]),
    })
      .then(async (response) => {
        await response.body?.cancel();
        if (!response.ok) throw Error("Offer not confirmed");
      })
      .catch(() => {
        console.warn("subagent.offer.unconfirmed");
      })
      .finally(() => {
        this.#active.delete(work);
        this.#bytes -= bytes;
      });
    this.#active.add(work);
  }
  reset() {
    this.#abort.abort();
    this.#abort = new AbortController();
  }
  async close() {
    this.#abort.abort();
    await Promise.allSettled(this.#active);
    await this.#pool.close();
  }
}
