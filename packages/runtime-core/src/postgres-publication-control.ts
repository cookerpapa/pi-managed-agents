import type { Database } from "@pi-cloud/database";
import { isDeepStrictEqual } from "node:util";
import type { Kysely } from "kysely";
import type { AcceptedExecutionSealFact, AcceptedFactReceipt } from "./accepted-fact.ts";
import {
  PublicationIngressRejected,
  type PublicationOwnership,
} from "./session-publication-ingress.ts";

/** PG authorizes closure; the existing group-owned partition publisher orders
 * its marker. Worker transports are not taken over or initialized here. */
export class PostgresPublicationControl {
  readonly #pending = new Map<
    string,
    { owner: PublicationOwnership; promise: Promise<AcceptedFactReceipt> }
  >();
  #closed = false;
  constructor(
    readonly options: {
      database: Kysely<Database>;
      current(owner: PublicationOwnership): boolean;
      beforeSeal(fact: AcceptedExecutionSealFact): Promise<void>;
      publish(
        owner: PublicationOwnership,
        fact: AcceptedExecutionSealFact,
      ): Promise<AcceptedFactReceipt>;
    },
  ) {}

  async #state(fact: AcceptedExecutionSealFact): Promise<"open" | "closed"> {
    const row = await this.options.database
      .selectFrom("outbox as o")
      .innerJoin("session_leases as l", (join) =>
        join.on("l.lease_id", "=", fact.scope.writerId).onRef("l.tenant_id", "=", "o.tenant_id"),
      )
      .leftJoin("runs as r", (join) =>
        join.on("r.id", "=", fact.scope.runId).onRef("r.tenant_id", "=", "o.tenant_id"),
      )
      .select([
        "o.payload",
        "l.writer_sealed_at",
        "l.writer_failed_at",
        "r.output_seal_id",
        "r.lease_id",
      ])
      .where("o.tenant_id", "=", fact.scope.tenantId)
      .where("o.id", "=", fact.factId)
      .where("l.pi_session_id", "=", fact.scope.piSessionId)
      .where("l.fencing_token", "=", String(fact.scope.fencingToken))
      .executeTakeFirst();
    if (!row || !isDeepStrictEqual(row.payload, fact))
      throw new PublicationIngressRejected("scope");
    if (row.writer_sealed_at !== null) return "closed";
    if (
      !row.writer_failed_at ||
      row.output_seal_id !== fact.factId ||
      row.lease_id !== fact.scope.writerId
    )
      throw new PublicationIngressRejected("scope");
    return "open";
  }
  seal(owner: PublicationOwnership, fact: AcceptedExecutionSealFact): Promise<AcceptedFactReceipt> {
    if (this.#closed || !this.options.current(owner))
      return Promise.reject(new PublicationIngressRejected("not_owner"));
    const key = JSON.stringify([fact.scope.tenantId, fact.factId]);
    const prior = this.#pending.get(key);
    if (prior) {
      // Join the old incarnation through writer disposal. Only a failed old
      // owner needs a fresh attempt, and seal() rechecks the replacement owner.
      return prior.owner === owner
        ? prior.promise
        : prior.promise.catch(() => this.seal(owner, fact));
    }
    const promise = this.#seal(owner, fact).finally(() => {
      if (this.#pending.get(key)?.promise === promise) this.#pending.delete(key);
    });
    this.#pending.set(key, { owner, promise });
    return promise;
  }
  async #seal(
    owner: PublicationOwnership,
    fact: AcceptedExecutionSealFact,
  ): Promise<AcceptedFactReceipt> {
    const receipt = { factId: fact.factId, durable: true as const };
    if ((await this.#state(fact)) === "closed") return receipt;
    if (this.#closed || !this.options.current(owner))
      throw new PublicationIngressRejected("not_owner");
    await this.options.beforeSeal(fact);
    if (this.#closed || !this.options.current(owner))
      throw new PublicationIngressRejected("not_owner");
    if ((await this.#state(fact)) === "closed") return receipt;
    return this.options.publish(owner, fact);
  }
  async close(): Promise<void> {
    this.#closed = true;
    await Promise.allSettled([...this.#pending.values()].map(({ promise }) => promise));
  }
}
