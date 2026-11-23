import {
  resolveDurableDisplay,
  type PiCloudEvent,
  type NativeInputConfirmation,
} from "@pi-cloud/protocol";
import { sql, type Kysely, type Transaction } from "kysely";
import type { Database } from "@pi-cloud/database";
import {
  DurablePostgresProjection,
  collectInputConfirmations,
  nativeCommitFingerprint,
  type StoredDurableBatch,
} from "@pi-cloud/pi-session-postgres/durable";
import type { AcceptedDurableCommitFact } from "./accepted-fact.ts";
import { projectionOffsetWrite, type FactPosition } from "./accepted-fact-recovery.ts";
import { projectionTransaction, type ProjectionBudget } from "./projection-transaction.ts";
import { captureNativePresentation, mayCaptureNativePresentation } from "./frozen-history.ts";

function cloudMetadata(fact: AcceptedDurableCommitFact) {
  const controlTools = fact.tools.filter((tool) => tool.target === "control");
  const remoteTools = fact.tools.filter((tool) => tool.target !== "control" && tool.replyRecovery);
  // Retained records must reproduce the byte identity of an already projected commit.
  const historicalProof =
    "modelEvidence" in fact && Array.isArray(fact.modelEvidence) && fact.modelEvidence.length
      ? fact.modelEvidence
      : undefined;
  return controlTools.length || remoteTools.length || historicalProof
    ? {
        ...(controlTools.length ? { controlTools } : {}),
        ...(remoteTools.length ? { remoteTools } : {}),
        ...(historicalProof ? { modelEvidence: historicalProof } : {}),
      }
    : undefined;
}

type Scope = AcceptedDurableCommitFact["scope"];
type DisplayView = {
  session_id: string;
  turn_id: string;
  output_display_seq: string;
  output_display_native_seq?: string;
};
type LiveAdmission = {
  hot: boolean;
  scope: Scope;
  topic: string;
  partition: number;
  offset: bigint;
  displays: Map<string, DisplayView>;
};

function sameWriter(a: Scope, b: Scope): boolean {
  return (
    a.tenantId === b.tenantId &&
    a.piSessionId === b.piSessionId &&
    a.writerId === b.writerId &&
    a.fencingToken === b.fencingToken &&
    a.runId === b.runId
  );
}

async function lockNativeOwner(tx: Transaction<Database>, scope: Scope, position: FactPosition) {
  // Materialized dependencies take the Run, writer and physical Session
  // locks in that order. Native reads use a subsequent READ COMMITTED snapshot.
  const {
    rows: [run],
  } = await sql<{
    id: string;
    session_id: string;
    turn_id: string;
    lease_id: string | null;
    output_seal_offset: string | null;
    output_first_topic: string | null;
    output_first_partition: number | null;
    output_first_offset: string | null;
    output_display_seq: string;
    output_display_native_seq?: string;
    mailbox_position: string;
    pruned_at: Date | null;
    writer_seal_offset: string | null;
    writer_fencing_token: string;
    writer_pi_session_id: string;
  }>`with locked_run as materialized (
    select id,session_id,turn_id,lease_id,output_seal_offset::text,output_first_topic,output_first_partition,output_first_offset::text,output_display_seq::text,mailbox_position::text,pruned_at${position.previouslyProjected ? sql`,output_display_native_seq::text` : sql``}
    from runs where tenant_id=${scope.tenantId}::uuid and id=${scope.runId}::uuid
    for no key update
  ), owner as materialized (
    select r.*,l.writer_seal_offset,l.fencing_token as writer_fencing_token,l.pi_session_id as writer_pi_session_id from locked_run r join session_leases l
    on l.tenant_id=${scope.tenantId}::uuid and l.lease_id=${scope.writerId}::uuid
    for no key update of l
  ) select o.* from owner o join pi_sessions p on p.tenant_id=${scope.tenantId}::uuid
    and p.id=o.writer_pi_session_id and p.id=${scope.piSessionId}
    for update of p`.execute(tx);
  if (
    !run ||
    Number(run.writer_fencing_token) !== scope.fencingToken ||
    run.writer_pi_session_id !== scope.piSessionId ||
    (run.writer_seal_offset !== null && position.offset >= BigInt(run.writer_seal_offset))
  )
    return;
  if (
    run.lease_id !== scope.writerId ||
    run.session_id !== scope.sessionId ||
    run.turn_id !== scope.turnId ||
    (run.output_seal_offset !== null && position.offset >= BigInt(run.output_seal_offset))
  )
    return;
  if (
    run.output_first_offset !== null &&
    (run.output_first_topic !== position.topic || run.output_first_partition !== position.partition)
  )
    throw new Error("Native Run publication moved between Kafka streams");
  return run;
}

function displayEvents(
  fact: AcceptedDurableCommitFact,
  displays: ReadonlyMap<string, DisplayView>,
  rows: ReadonlyMap<number, { seq: string; body: string }>,
  through: number | undefined,
): PiCloudEvent[] {
  const events: PiCloudEvent[] = [];
  for (const display of fact.display) {
    const previous = Number(displays.get(display.scope.sessionId)!.output_display_seq);
    // Historical live documents may have been superseded by a newer PG
    // checkpoint. Never resolve text already represented by native Entries.
    for (const description of display.events) {
      if (description.event.seq <= previous) continue;
      events.push(
        resolveDurableDisplay(fact.commit, description, (id) => {
          const row = rows.get(id);
          if (!row || through !== fact.commit.seq || Number(row.seq) !== fact.commit.seq)
            throw new Error("Native display is missing its current document projection");
          return JSON.parse(row.body).content.value;
        }),
      );
    }
  }
  return events;
}

export class PostgresDurableCommitProjector {
  readonly #views = new Map<string, DurablePostgresProjection>();
  readonly #admissions = new WeakMap<DurablePostgresProjection, LiveAdmission>();
  constructor(
    readonly database: Kysely<Database>,
    readonly budget?: ProjectionBudget,
  ) {}
  #key(scope: AcceptedDurableCommitFact["scope"]): string {
    // All Conversations/anchors of one physical writer share this view. Old
    // closure replay must never evict the next lease incarnation's live suffix.
    return JSON.stringify([scope.tenantId, scope.piSessionId, scope.writerId]);
  }
  #view(scope: AcceptedDurableCommitFact["scope"]): DurablePostgresProjection {
    const key = this.#key(scope);
    let view = this.#views.get(key);
    if (!view) {
      view = new DurablePostgresProjection(
        this.database,
        { tenantId: scope.tenantId, sessionId: scope.piSessionId },
        undefined,
        "references",
      );
      this.#views.set(key, view);
    }
    return view;
  }
  reset(): void {
    this.#views.clear();
  }
  statistics() {
    const live = [...this.#views.values()].map((view) => view.liveState.statistics());
    return {
      views: this.#views.size,
      deferredReceipts: live.reduce((n, state) => n + state.deferredReceipts, 0),
      pendingDocuments: live.reduce((n, state) => n + state.documents, 0),
      estimatedBytes: live.reduce((n, state) => n + state.estimatedBytes, 0),
    };
  }
  releaseSettled(scope: AcceptedDurableCommitFact["scope"]): void {
    const key = this.#key(scope);
    if (this.#views.get(key)?.liveState.rows.size === 0) this.#views.delete(key);
  }
  /** Caller has confirmed closure/checkpoint in its transaction. Invoke only
   * after COMMIT, including when reconciling a previously lost commit return. */
  closedWriterCleanup(
    scope: AcceptedDurableCommitFact["scope"],
    current: () => boolean,
  ): () => void {
    const key = this.#key(scope);
    return this.#viewCleanup(key, this.#views.get(key), current);
  }
  #viewCleanup(
    key: string,
    view: DurablePostgresProjection | undefined,
    current: () => boolean,
  ): () => void {
    return () => {
      if (current() && view !== undefined && this.#views.get(key) === view) this.#views.delete(key);
    };
  }
  /** The assignment-time partition checkpoint is a hint. A complete root view
   * also needs this exact Session receipt, immutable bytes, display coverage
   * and no outstanding document needing this sequence. This query performs no row locks/DML. */
  async #coveredHistory(fact: AcceptedDurableCommitFact, position: FactPosition) {
    const scope = fact.scope;
    if (
      !fact.display.every(
        (d) =>
          sameWriter(d.scope, scope) &&
          d.scope.sessionId === scope.sessionId &&
          d.scope.turnId === scope.turnId,
      )
    )
      return false;
    const through = fact.display.reduce(
      (n, d) =>
        d.events.reduce(
          (high, e) => Math.max(high, e.event.seq),
          Math.max(n, d.coveredThrough ?? 0),
        ),
      0,
    );
    const {
      rows: [proof],
    } = await sql<{ fingerprint: string }>`select c.fingerprint
      from pi_durable_commits c join runs r on r.tenant_id=c.tenant_id and r.id=${scope.runId}::uuid
      join session_leases l on l.tenant_id=r.tenant_id and l.lease_id=r.lease_id
      where c.tenant_id=${scope.tenantId}::uuid and c.session_id=${scope.piSessionId} and c.seq=${fact.commit.seq}
        and r.session_id=${scope.sessionId}::uuid and r.turn_id=${scope.turnId}::uuid
        and r.lease_id=${scope.writerId}::uuid and l.fencing_token=${scope.fencingToken}
        and l.pi_session_id=c.session_id and r.output_first_topic=${position.topic}
        and r.output_first_partition=${position.partition} and r.output_first_offset<=${position.offset.toString()}::bigint
        and (r.output_seal_offset is null or r.output_seal_offset>${position.offset.toString()}::bigint)
        and (l.writer_seal_offset is null or l.writer_seal_offset>${position.offset.toString()}::bigint)
        and r.output_display_seq>=${through}::bigint
        and r.output_display_native_seq>=${fact.display.some((d) => d.coveredThrough !== undefined) ? fact.commit.seq : 0}::bigint
        and not exists(select 1 from pi_durable_current d where d.tenant_id=c.tenant_id
          and d.session_id=c.session_id and d.replay_offset is not null and d.replay_seq<=c.seq)`.execute(
      this.database,
    );
    if (!proof) return false;
    if (
      proof.fingerprint !==
      nativeCommitFingerprint(
        fact.commit as unknown as StoredDurableBatch,
        fact.origins,
        cloudMetadata(fact),
      )
    )
      throw new Error("Native commit position changed bytes");
    return true;
  }
  async project(
    fact: AcceptedDurableCommitFact,
    position: FactPosition,
    current: () => boolean = () => true,
  ): Promise<{
    events: PiCloudEvent[];
    covers: { sessionId: string; through: number }[];
    inputConfirmations?: NativeInputConfirmation[];
    admitted: boolean;
  }> {
    const events: PiCloudEvent[] = [],
      covers: { sessionId: string; through: number }[] = [];
    const inputConfirmations: NativeInputConfirmation[] = [];
    let admitted = false;
    if (!current()) return { events, covers, admitted };
    const existing = this.#views.get(this.#key(fact.scope));
    const grant = existing && this.#admissions.get(existing);
    if (
      existing &&
      grant?.hot &&
      sameWriter(grant.scope, fact.scope) &&
      grant.scope.sessionId === fact.scope.sessionId &&
      grant.scope.turnId === fact.scope.turnId &&
      grant.topic === position.topic &&
      grant.partition === position.partition &&
      position.offset >= grant.offset &&
      !fact.origins.length &&
      !fact.tools.length &&
      !fact.cancellations.length &&
      !fact.inputQueueSessions?.length &&
      fact.display.every(
        (d) =>
          d.coveredThrough === undefined &&
          sameWriter(d.scope, fact.scope) &&
          grant.displays.get(d.scope.sessionId)?.turn_id === d.scope.turnId &&
          d.events.every((e) => e.event.type === "assistant.text.delta"),
      )
    ) {
      const update = existing.liveState.prepareProgress(
        fact.commit as unknown as StoredDurableBatch,
      );
      if (update) {
        if (!update.duplicate) {
          if (position.offset === grant.offset)
            throw new Error("Native Kafka position changed bytes");
          events.push(...displayEvents(fact, grant.displays, update.changed, fact.commit.seq));
          update.apply();
        }
        grant.offset = position.offset;
        return { events, covers, admitted: true };
      }
    }
    if (
      position.previouslyProjected &&
      !existing?.liveState.rows.size &&
      (await this.#coveredHistory(fact, position))
    ) {
      if (!current()) return { events, covers, admitted: false };
      // Future unrepresented writes must bootstrap against the actual PG head.
      if (existing) this.releaseSettled(fact.scope);
      for (const d of fact.display)
        if (d.coveredThrough !== undefined)
          covers.push({ sessionId: d.scope.sessionId, through: d.coveredThrough });
      return { events, covers, admitted: true };
    }
    // An unsuccessful or uncertain boundary must be reconciled under the PG locks
    // before trusting this admission for fresh text again. Keep its replay proofs.
    if (grant) grant.hot = false;
    const prepared = await projectionTransaction(
      this.database,
      async (tx) => {
        const run = await lockNativeOwner(tx, fact.scope, position);
        if (!run) return;
        if (!current()) throw new Error("Native projection owner changed before state loading");
        const view = this.#view(fact.scope),
          state = view.liveState.fork();
        const childViews = fact.display
          .filter((d) => d.scope.sessionId !== run.session_id)
          .map((d) => d.scope.sessionId);
        const displayViews = childViews.length
          ? await sql<{
              session_id: string;
              turn_id: string;
              id: string;
              output_display_seq: string;
              output_display_native_seq?: string;
            }>`select session_id,turn_id,id,output_display_seq::text${position.previouslyProjected ? sql`,output_display_native_seq::text` : sql``} from conversation_turns
          where tenant_id=${fact.scope.tenantId}::uuid and id=${fact.scope.runId}::uuid
            and session_id=any(${childViews}::uuid[])`.execute(tx)
          : { rows: [] };
        // The locked root Run already supplies this exact identity and coverage.
        const displays = new Map([run, ...displayViews.rows].map((r) => [r.session_id, r]));
        for (const display of fact.display) {
          const target = displays.get(display.scope.sessionId);
          if (
            !target ||
            target.turn_id !== display.scope.turnId ||
            !sameWriter(display.scope, fact.scope)
          )
            throw new Error("Native display has no admitted Conversation view");
        }
        const batch = fact.commit as unknown as StoredDurableBatch;
        const nativeConfirmations: ReturnType<typeof collectInputConfirmations> = [];
        const capture = mayCaptureNativePresentation(batch.writes);
        const covered = fact.display.filter((d) => d.coveredThrough !== undefined);
        const finalNativeWrite =
          run.output_first_offset !== null &&
          !covered.length &&
          !capture &&
          !fact.inputQueueSessions?.length;
        const { appended, sharedEntries, finished } = await view.applyLockedIn(
          tx,
          batch,
          fact.origins,
          { state, position },
          cloudMetadata(fact),
          fact.inputQueueSessions?.length
            ? (rows, previous) =>
                nativeConfirmations.push(...collectInputConfirmations(view.scope, rows, previous))
            : undefined,
          finalNativeWrite ? (ready) => projectionOffsetWrite(position, ready) : undefined,
        );
        admitted = true;
        if (nativeConfirmations.length) {
          const conversations = await tx
            .selectFrom("sessions")
            .select(["id", "pi_conversation_id"])
            .where("tenant_id", "=", fact.scope.tenantId)
            .where("pi_session_id", "=", fact.scope.piSessionId)
            .where("pi_conversation_id", "in", [
              ...new Set(nativeConfirmations.map((c) => String(c.conversationId))),
            ])
            .execute();
          const targets = new Map(conversations.map((c) => [Number(c.pi_conversation_id), c.id]));
          for (const { conversationId, ...confirmation } of nativeConfirmations) {
            const sessionId = targets.get(conversationId);
            if (!sessionId || confirmation.runId !== fact.scope.runId)
              throw new Error("Input confirmation has no admitted Conversation view");
            inputConfirmations.push({ ...confirmation, sessionId });
          }
        }
        if (appended && capture)
          await captureNativePresentation(
            tx,
            { tenantId: fact.scope.tenantId, sessionId: fact.scope.piSessionId },
            fact.commit.seq,
            undefined,
            {
              sessionId: run.session_id,
              turnId: run.turn_id,
              mailboxPosition: run.mailbox_position,
              pruned: run.pruned_at !== null,
              sharedEntries,
            },
          );
        events.push(...displayEvents(fact, displays, state.rows, state.through));
        for (const display of covered)
          covers.push({ sessionId: display.scope.sessionId, through: display.coveredThrough! });
        const root = covered.find((d) => d.scope.sessionId === run.session_id);
        const children = covered.filter((d) => d.scope.sessionId !== run.session_id);
        const changes = [];
        if (run.output_first_offset === null || root) {
          const columns = [];
          if (run.output_first_offset === null)
            columns.push(sql`output_first_topic=${position.topic},
            output_first_partition=${position.partition},output_first_offset=${position.offset.toString()}::bigint`);
          if (root)
            columns.push(sql`output_display_seq=greatest(output_display_seq,${root.coveredThrough}::bigint),
            output_display_native_seq=greatest(output_display_native_seq,${fact.commit.seq}::bigint)`);
          changes.push(sql`root as (update runs set ${sql.join(columns)} where tenant_id=${fact.scope.tenantId}::uuid
            and id=${fact.scope.runId}::uuid
            ${run.output_first_offset !== null && root ? sql`and (output_display_seq<${root.coveredThrough}::bigint or output_display_native_seq<${fact.commit.seq}::bigint)` : sql``})`);
        }
        if (children.length) {
          changes.push(sql`children as (update subagent_conversations child set
            output_display_seq=greatest(child.output_display_seq,c.through),
            output_display_native_seq=greatest(child.output_display_native_seq,${fact.commit.seq}::bigint)
            from (values ${sql.join(children.map((d) => sql`(${d.scope.sessionId}::uuid,${d.scope.turnId}::uuid,${d.coveredThrough}::bigint)`))}) c(session_id,turn_id,through)
            where child.tenant_id=${fact.scope.tenantId}::uuid and child.root_run_id=${fact.scope.runId}::uuid
            and child.child_session_id=c.session_id and child.child_turn_id=c.turn_id and
            (child.output_display_seq<c.through or child.output_display_native_seq<${fact.commit.seq}::bigint))`);
        }
        const alreadyRecorded =
          position.previouslyProjected &&
          !appended &&
          run.output_first_offset !== null &&
          covered.every((d) => {
            const v = displays.get(d.scope.sessionId)!;
            return (
              Number(v.output_display_seq) >= d.coveredThrough! &&
              Number(v.output_display_native_seq) >= fact.commit.seq
            );
          });
        if (!finished && !alreadyRecorded)
          await sql`${changes.length ? sql`with ${sql.join(changes)}` : sql``} ${projectionOffsetWrite(position)}`.execute(
            tx,
          );
        if (!current()) throw new Error("Native projection owner changed before commit");
        for (const d of covered) {
          const v = displays.get(d.scope.sessionId)!;
          displays.set(v.session_id, {
            ...v,
            output_display_seq: String(Math.max(Number(v.output_display_seq), d.coveredThrough!)),
          });
        }
        return { view, state, displays };
      },
      this.budget,
    );
    if (prepared && current() && this.#views.get(this.#key(fact.scope)) === prepared.view) {
      prepared.view.liveState.adopt(prepared.state);
      if (prepared.state.rows.size) {
        this.#admissions.set(prepared.view, {
          hot: true,
          scope: fact.scope,
          topic: position.topic,
          partition: position.partition,
          offset: position.offset,
          displays: prepared.displays,
        });
      } else this.#admissions.delete(prepared.view);
    }
    return {
      events,
      covers,
      admitted,
      ...(inputConfirmations.length ? { inputConfirmations } : {}),
    };
  }
  /** A separate event advancing durable display coverage can refer to an
   * in-flight sequence. Materialize its evidence before the existing coverage check. */
  async prepareCoverage(
    scope: Scope,
    sequence: number,
    position: FactPosition,
    current: () => boolean,
  ) {
    const view = this.#views.get(this.#key(scope));
    const grant = view && this.#admissions.get(view);
    if (!view?.liveState.receipts.has(sequence) || !grant || !sameWriter(grant.scope, scope))
      return;
    const state = await projectionTransaction(
      this.database,
      async (tx) => {
        if (!current() || !(await lockNativeOwner(tx, grant.scope, position))) return;
        const draft = view.liveState.fork();
        await view.materializeLiveReceiptsIn(tx, draft);
        if (!current()) throw new Error("Native projection owner changed before coverage");
        return draft;
      },
      this.budget,
    );
    if (state && current() && this.#views.get(this.#key(scope)) === view)
      view.liveState.adopt(state);
  }
  async replay(
    fact: AcceptedDurableCommitFact,
    position: FactPosition,
    current: () => boolean = () => true,
  ): Promise<void> {
    if (!current()) return;
    const prepared = await projectionTransaction(
      this.database,
      async (tx) => {
        const proof = await sql<{
          writer_seal_offset: string | null;
          output_seal_offset: string | null;
          lease_id: string | null;
          fencing_token: string;
        }>`
        select l.writer_seal_offset,r.output_seal_offset,r.lease_id,l.fencing_token
        from session_leases l left join runs r on r.id=${fact.scope.runId}::uuid and r.tenant_id=l.tenant_id
        where l.tenant_id=${fact.scope.tenantId}::uuid and l.lease_id=${fact.scope.writerId}::uuid
          and l.pi_session_id=${fact.scope.piSessionId}`.execute(tx);
        const accepted = proof.rows[0];
        if (
          !accepted ||
          Number(accepted.fencing_token) !== fact.scope.fencingToken ||
          (accepted.writer_seal_offset !== null &&
            position.offset >= BigInt(accepted.writer_seal_offset)) ||
          (accepted.lease_id === fact.scope.writerId &&
            accepted.output_seal_offset !== null &&
            position.offset >= BigInt(accepted.output_seal_offset))
        )
          return;
        if (!current()) throw new Error("Native replay owner changed before state loading");
        const view = this.#view(fact.scope),
          state = view.liveState.fork();
        await view.replayLiveIn(
          tx,
          fact.commit as unknown as StoredDurableBatch,
          fact.origins,
          { state, position },
          cloudMetadata(fact),
        );
        if (!current()) throw new Error("Native replay owner changed before completion");
        return { view, state };
      },
      this.budget,
    );
    if (prepared && current() && this.#views.get(this.#key(fact.scope)) === prepared.view) {
      prepared.view.liveState.adopt(prepared.state);
      this.releaseSettled(fact.scope);
    }
  }
  async checkpointLiveIn(
    tx: Transaction<Database>,
    scope: AcceptedDurableCommitFact["scope"],
    current: () => boolean,
    expectedHead?: number,
  ): Promise<() => void> {
    if (!current()) throw new Error("Native checkpoint owner changed before state loading");
    const view = this.#view(scope),
      state = view.liveState.fork();
    await view.checkpointLiveIn(tx, state, expectedHead);
    if (!current()) throw new Error("Native checkpoint owner changed before commit");
    return this.#viewCleanup(this.#key(scope), view, current);
  }
}
