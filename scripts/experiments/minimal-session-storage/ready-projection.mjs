import { projectWorld } from "./conversation-world.mjs";
import { projectChildOutcomes } from "./child-outcomes.mjs";
import { foldRootPresentation } from "./root-presentation-frame.mjs";
import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import {
  installPrefixSql,
  applyPrefixSql,
  applyMovingSql,
  applyBulkUsageSql,
  projectionStarts,
} from "./ready-projection-sql.mjs";
import {
  foldLive,
  foldToolReferences,
  checkpoint,
  plainAnswer,
  replayableMetadata,
  useSynchronousPolicy,
  movingStarts,
  assertFamilySettled,
} from "./moving-checkpoint.mjs";

import { canGroupUsage, coCommittedUsage } from "./usage-study.mjs";

export const isPrefixStudy = (name) =>
  ["prefix-smoke", "prefix-study", "prefix-confirm"].includes(name) || isMovingStudy(name);
export const isMovingStudy = (name) =>
  [
    "moving-smoke",
    "moving-study",
    "moving-confirm",
    "moving-finish",
    "workspace-fanout",
    "workspace-rate",
    "workspace-rate-confirm",
    "workspace-rate-input",
    "workspace-continuation",
    "workspace-guest-continuation",
    "workspace-guest-mix",
    "message-rate",
    "message-rate-smoke",
    "message-rate-confirm",
    "fork-rate",
    "fork-rate-confirm",
    "fork-rate-smoke",
    "fork-rate-index",
    "usage-rate-bulk",
    "usage-rate-bulk-confirm",
    "usage-rate-bulk-smoke",
    "usage-rate-smoke",
    "usage-rate-confirm",
    "usage-rate",
  ].includes(name);
export function recoveryConnectionError(error) {
  return (
    /^(08|57P0[123]$|ECONNRESET$|EPIPE$|ECONNREFUSED$|40001$|PZ003$)/.test(error?.code ?? "") ||
    /connection (?:terminated|ended)|timeout exceeded when trying to connect/i.test(
      error?.message ?? "",
    )
  );
}

// The same native fold prepares a private overlay. Entries, Inbox values and
// previous native batches are immutable; the scalar Run state is copied.
class Draft {
  constructor(base, templates, witness, restoring) {
    this.changes = new Map();
    this.after = [];
    this.frames = [];
    this.barrier = false;
    this.views = {
      get: (id) => {
        const v = this.changes.has(id) ? this.changes.get(id) : base.get(id);
        return v ? { ...v } : undefined;
      },
      set: (id, v) => this.changes.set(id, v),
      delete: (id) => this.changes.set(id, null),
    };
    this.base = base;
    this.templates = templates;
    this.witness = witness;
    this.restoring = restoring;
  }
  begin(record) {
    this.frame = { offset: String(record.offset), ops: [] };
    this.frames.push(this.frame);
  }
  acquire(f) {
    return {
      release() {},
      query: async (sql, args) => {
        const { message, inbox, finish, plainMessage, plainInbox } = this.templates;
        if (
          sql === "begin" ||
          sql === "commit" ||
          sql === "rollback" ||
          sql.startsWith("select id from runs where tenant_id=")
        )
          return { rowCount: 1, rows: [] };
        let kind,
          expected = 1,
          opening = false,
          canonical = sql;
        if (sql === message || sql === plainMessage) {
          kind = "message";
          canonical = message;
          if (sql === plainMessage) args = [...args, f.scope.writerId];
          expected = JSON.parse(args[4]).length;
        } else if (sql === inbox || sql === plainInbox) {
          kind = "inbox";
          canonical = inbox;
          if (sql === plainInbox) args = [...args, f.scope.runId, f.scope.writerId];
          opening = f.commit.writes.some(
            (w) =>
              w.type === "document.create" &&
              w.record.kind === "pi.inbox" &&
              w.record.id === args[3],
          );
          if (!opening) this.barrier = true;
        } else if (sql === finish) {
          kind = "finish";
          this.barrier = true;
        } else throw new Error("Write shape outside ready-prefix experiment");
        this.witness?.(f, canonical, args);
        this.frame.ops.push({ kind, args, count: expected });
        return { rowCount: expected, rows: [{ accepted: true, writes: expected }] };
      },
    };
  }
  adopt(notify) {
    for (const [id, v] of this.changes) {
      if (v) this.base.set(id, v);
      else this.base.delete(id);
    }
    if (notify) for (const publish of this.after) publish();
  }
}

export class ReadyProjection {
  constructor(db, topic, sessions, templates) {
    Object.assign(this, { db, topic, sessions, templates });
    this.next = new Map();
    this.effects = new Map();
    this.generation = 0;
    this.width = 0;
  }
  async install() {
    await installPrefixSql(this.db, this.templates);
  }
  revoke() {
    this.generation++;
    this.sessions.clear();
    this.effects.clear();
    this.next.clear();
  }
  canDeliver({ partition, offset }) {
    return this.effects.get(partition)?.has(offset) === true;
  }
  configure(width, policy = "fixed") {
    assert([0, 1, 16].includes(width));
    assert(
      ["fixed", "moving-sync", "selective", "replay-finish", "recorded-history"].includes(policy),
    );
    this.policy = policy;
    this.width = width;
    this.stats = {
      commits: 0,
      usageUpserts: 0,
      usageStatements: 0,
      multiUsageStatements: 0,
      recordsInTransactions: 0,
      logicalWrites: 0,
      transactionsByWrites: {},
      covered: 0,
      obsolete: 0,
      poolMs: 0,
      holdMs: 0,
      zeroWritePrefixes: 0,
      maxRecords: 0,
      syncTransactions: 0,
      asyncTransactions: 0,
      checkpointAdvances: 0,
      firstPins: 0,
      pinOnlyWrites: 0,
    };
  }
  // Only independent benchmark trials reset the fixture. Assignment recovers
  // the existing head; a fresh process must never overwrite it.
  async reset(ends, width, policy) {
    this.revoke();
    this.configure(width, policy);
    await this.db.query(
      `insert into accepted_fact_projection_offsets select $1,p,n from unnest($2::int[],$3::bigint[]) as v(p,n)
      on conflict(topic,partition) do update set next_offset=excluded.next_offset`,
      [this.topic, [...ends.keys()], [...ends.values()].map(String)],
    );
    this.next = new Map(ends);
  }
  async recover(bounds) {
    const generation = this.generation;
    const starts = await (this.policy === "fixed" ? projectionStarts : movingStarts)(
      this.db,
      this.topic,
      bounds,
    );
    assert.equal(generation, this.generation, "Assignment changed during bootstrap");
    for (const [p, next, start, states] of starts) {
      if (start instanceof Error) continue;
      this.next.set(p, next);
      for (const [id, s] of states ?? []) this.sessions.set(id, s);
    }
    return new Map(starts.map(([p, , start]) => [p, start]));
  }
  async apply(ready, current, process, witness, connect = () => this.db.connect()) {
    if (!this.width) return 1;
    return this.#apply(ready.slice(0, this.width), current, process, witness, connect, false);
  }
  async applyToolIntents(ready, current, process) {
    assert(
      this.width && this.policy === "recorded-history" && ready.length > 0 && ready.length <= 128,
    );
    return this.#apply(ready, current, process, undefined, () => this.db.connect(), true);
  }
  async #apply(candidates, current, process, witness, connect, intentPrefix) {
    const generation = this.generation;
    const owned = () => current() && this.generation === generation;
    const partition = candidates[0].partition,
      expected = this.next.get(partition);
    assert(expected !== undefined && owned(), "Missing projection assignment");
    const covered = candidates[0].offset < expected;
    if (intentPrefix) assert(!covered && candidates[0].offset === expected);
    const draft = new Draft(this.sessions, this.templates, covered ? undefined : witness, covered);
    const effects = new Set();
    this.effects.delete(partition);
    // Only a rewind needs a cold Run lookup. Never reconstruct a superseded
    // owner's in-memory state or publish its external intent.
    const owners = covered ? await this.activeRuns(partition, candidates) : undefined;
    const kept = [];
    for (const r of candidates) {
      if (r.offset < expected !== covered) break;
      assert.equal(r.partition, partition);
      if (intentPrefix)
        assert(
          r.topic === this.topic && r.offset === expected + BigInt(kept.length),
          "Noncontiguous PG tool prefix",
        );
      draft.begin(r);
      kept.push(r);
      const f = r.fact;
      const hasEffect = !!(f.tools?.length || f.cancellations?.length);
      const previous = draft.views.get(f.scope.runId);
      const owner = owners?.get(f.scope.runId);
      if (
        covered &&
        (!owner ||
          owner.tenant_id !== f.scope.tenantId ||
          owner.session_id !== f.scope.piSessionId ||
          owner.lease_id !== f.scope.writerId ||
          r.offset < BigInt(owner.projection_first))
      )
        continue;
      if (covered && previous && f.kind === "durable_commit" && f.commit.seq <= previous.seq) {
        if (f.commit.seq === previous.seq) assert.equal(f.factId, previous.lastFact);
        if (
          hasEffect &&
          (!f.tools?.length || owner.state === "running") &&
          f.commit.seq === previous.seq &&
          previous.lastOffset === r.offset
        ) {
          effects.add(r.offset);
          break;
        }
        continue;
      }
      if (
        this.policy !== "fixed" &&
        f.kind === "work_finished" &&
        previous?.writer === f.scope.writerId
      )
        assertFamilySettled(previous);
      await process(r, draft);
      const view = draft.views.get(f.scope.runId);
      const opened = !previous && view;
      if (opened && !covered) {
        // Remember even a memory-only first fact before another Run can advance
        // this partition's checkpoint beyond it.
        const args = [f.scope.tenantId, f.scope.piSessionId, f.scope.runId, f.scope.writerId];
        if (!draft.frame.ops.length) draft.frame.ops.push({ kind: "anchor", args, count: 1 });
        draft.frame.anchor = args;
      }
      if (this.policy !== "fixed") {
        draft.frame.strict = useSynchronousPolicy(f) || draft.barrier;
        if (
          (this.policy === "replay-finish" || this.policy === "recorded-history") &&
          f.kind === "work_finished" &&
          f.terminal.type === "turn.completed" &&
          draft.frame.ops.length === 1 &&
          draft.frame.ops[0].kind === "finish" &&
          !draft.frame.rejected
        ) {
          draft.frame.strict = false;
          draft.frame.history = true;
        }
        if (view && !draft.frame.rejected && f.kind === "durable_commit") {
          if (this.persistUsage !== undefined)
            coCommittedUsage(view, f, draft.frame, this.persistUsage && !covered);
          projectWorld(view, f, draft.frame);
          if (r.rootPresentation) {
            foldRootPresentation(view, f, draft.frame);
          }
          if (r.rootPresentation || this.delegatedRuns)
            projectChildOutcomes(view, f, draft.frame, r.nativeDispatches, this.delegatedRuns);
          view.liveDocs = foldLive(previous?.liveDocs, f.commit.writes);
          if (covered && owner?.projection_pin != null)
            view.pinOffset = BigInt(owner.projection_pin);
          const registered = foldToolReferences(
            view,
            previous,
            draft.frame,
            f,
            r.offset,
            r.workspaceBatch,
            r.rootPresentation,
          );
          if (f.tools?.length && !registered)
            throw new Error(
              "Moving projection requires a retained native reference for every dispatched Tool",
            );
          if (registered)
            draft.frame.strict =
              useSynchronousPolicy(f, true) ||
              draft.barrier ||
              draft.frame.ops.some((op) => op.kind !== "tool");
          if (draft.frame.ops.length && !hasEffect && !covered) {
            const state = checkpoint(view, r);
            if (state) {
              draft.frame.snapshot = { run: f.scope.runId, writer: f.scope.writerId, state };
              delete view.pinOffset;
            }
          }
          if (!covered && !draft.frame.snapshot && previous?.pinOffset === undefined) {
            const args = [f.scope.tenantId, f.scope.piSessionId, f.scope.runId, f.scope.writerId];
            draft.frame.pin = args;
            view.pinOffset = r.offset;
            if (!draft.frame.ops.length) draft.frame.ops.push({ kind: "pin", args, count: 1 });
          }
          draft.views.set(f.scope.runId, view);
          draft.frame.history =
            plainAnswer(draft.frame, f) ||
            (this.policy === "recorded-history" &&
              !draft.frame.strict &&
              !draft.frame.anchor &&
              draft.frame.ops.length > 0 &&
              draft.frame.ops.every((op) => op.kind === "message") &&
              f.commit.writes.every(
                (w) =>
                  (this.persistUsage !== undefined &&
                    w.type === "document.change" &&
                    Object.hasOwn(view.usageDocs ?? {}, w.id)) ||
                  (w.type === "entry" &&
                    w.value.model?.length === 1 &&
                    (w.value.model[0].role === "toolResult" ||
                      (w.value.model[0].role === "assistant" &&
                        w.value.model[0].stopReason === "toolUse" &&
                        w.value.model[0].content.some((b) => b.type === "toolCall")))),
              ) &&
              !hasEffect) ||
            replayableMetadata(draft.frame, f) ||
            (registered && !draft.frame.strict && !draft.frame.anchor);
        }
      }
      if (r.rootPresentation && !covered && !draft.frame.rejected && draft.frame.ops.length) {
        let rootThrough = opened ? 0 : undefined;
        for (const d of f.display) {
          const through = Math.max(
            d.coveredThrough ?? 0,
            ...d.events
              .filter(
                ({ event }) => event.type === "tool.started" || event.type === "tool.completed",
              )
              .map(({ event }) => event.seq),
          );
          if (d.scope.sessionId === f.scope.sessionId)
            rootThrough = Math.max(rootThrough ?? 0, through);
          else if (through > 0)
            (draft.frame.childDisplays ??= []).push({
              tenant: f.scope.tenantId,
              run: f.scope.runId,
              session: d.scope.sessionId,
              turn: d.scope.turnId,
              through,
              seq: f.commit.seq,
            });
        }
        if (rootThrough !== undefined)
          draft.frame.rootDisplay = {
            tenant: f.scope.tenantId,
            session: f.scope.piSessionId,
            run: f.scope.runId,
            writer: f.scope.writerId,
            through: rootThrough,
            seq: f.commit.seq,
          };
      }
      if (intentPrefix) {
        assert(
          r.workspaceBatch &&
            f.tools?.length === 1 &&
            !f.cancellations?.length &&
            !opened &&
            !draft.frame.rejected &&
            draft.frame.ops.length === 1 &&
            draft.frame.ops[0].kind === "tool" &&
            (f.display?.length ? draft.frame.strict : !draft.frame.strict && draft.frame.history) &&
            !draft.barrier,
          "Invalid warm execution projection",
        );
        draft.frame.intentPrefix = true;
      }
      if (hasEffect && !draft.frame.rejected) {
        assert(
          draft.frame.ops.some((op) => op.kind !== "anchor" && op.kind !== "pin"),
          "Tool requires stable native intent",
        );
        if (!covered || !f.tools?.length || owner.state === "running") effects.add(r.offset);
      }
      if (
        draft.barrier ||
        f.kind !== "durable_commit" ||
        (f.tools?.length && !intentPrefix) ||
        f.cancellations?.length ||
        f.commit.writes.some(
          (w) =>
            w.type === "submission" && w.value.requestId && w.value.requestId !== f.scope.runId,
        )
      )
        break;
    }
    const operations = draft.frames.filter((f) => f.ops.length);
    let outcome = covered ? "covered" : "memory";
    if (!covered && operations.length) {
      const next = BigInt(operations.at(-1).offset) + 1n;
      const started = performance.now(),
        c = await connect(),
        taken = performance.now();
      try {
        if (!owned()) return kept.length;
        const relaxed =
          (this.policy === "selective" ||
            this.policy === "replay-finish" ||
            this.policy === "recorded-history") &&
          !draft.frames.some((f) => f.strict) &&
          operations.every((f) => f.history);
        const moving = this.policy !== "fixed";
        const usageCount = draft.frames.reduce((n, f) => n + (f.usage?.length ?? 0), 0);
        const grouped = this.bulkUsage && moving && canGroupUsage(draft.frames, usageCount);
        const statement = grouped ? applyBulkUsageSql : moving ? applyMovingSql : applyPrefixSql;
        const {
          rows: [row],
        } = await c.query(statement, [
          this.topic,
          partition,
          String(expected),
          String(next),
          JSON.stringify(draft.frames),
          ...(moving
            ? [relaxed, this.policy === "replay-finish" || this.policy === "recorded-history"]
            : []),
        ]);
        outcome = row.outcome;
        if (outcome === "recorded_without_dispatch") {
          // Tool intent terminates the ready prefix. Preserve the ordered writes,
          // while withholding the permission to start the physical operation.
          effects.delete(kept.at(-1).offset);
          outcome = "applied";
        }
        if (outcome === "applied" && moving) {
          assert.equal(row.mode, relaxed ? "off" : "on", "PG transaction policy mismatch");
          this.stats[row.mode === "off" ? "asyncTransactions" : "syncTransactions"]++;
          this.stats.usageUpserts += usageCount;
          this.stats.usageStatements += grouped ? 1 : usageCount;
          if (grouped) this.stats.multiUsageStatements++;
          this.stats.checkpointAdvances += draft.frames.filter((f) => f.snapshot).length;
          this.stats.firstPins += draft.frames.filter((f) => f.pin).length;
        }
      } finally {
        c.release();
        this.stats.poolMs += taken - started;
        this.stats.holdMs += performance.now() - taken;
      }
      if (!owned()) return kept.length;
      if (outcome === "covered") {
        if (this.policy !== "fixed") {
          const error = new Error("Reload the committed projection checkpoint");
          error.code = "PZ003";
          throw error;
        }
        // The first commit survived a lost client reply. Re-enter via the
        // durable head to validate restored state and suppress notifications.
        const {
          rows: [head],
        } = await this.db.query(
          "select next_offset from accepted_fact_projection_offsets where topic=$1 and partition=$2",
          [this.topic, partition],
        );
        if (!owned()) return kept.length;
        this.next.set(partition, BigInt(head.next_offset));
        return this.apply(kept, current, process, witness, connect);
      }
      this.next.set(partition, next);
      this.stats.commits++;
      const n = operations.reduce((a, f) => a + f.ops.length, 0);
      const pins = operations.reduce(
        (n, f) => n + f.ops.filter((op) => op.kind === "pin").length,
        0,
      );
      this.stats.pinOnlyWrites += pins;
      this.stats.logicalWrites += n - pins;
      this.stats.recordsInTransactions += kept.length;
      this.stats.transactionsByWrites[n] = (this.stats.transactionsByWrites[n] ?? 0) + 1;
      this.stats.maxRecords = Math.max(this.stats.maxRecords, kept.length);
    } else if (!covered) this.stats.zeroWritePrefixes++;
    if (!owned()) return kept.length;
    if (outcome === "obsolete") {
      this.stats.obsolete++;
      return kept.length;
    }
    if (outcome === "covered") this.stats.covered += kept.length;
    this.effects.set(partition, effects);
    draft.adopt(!covered);
    return kept.length;
  }
  async activeRuns(partition, records) {
    const ids = [...new Set(records.map((r) => r.fact.scope.runId))];
    const { rows } = await this.db.query(
      `select id,tenant_id,session_id,lease_id,state,projection_first,projection_pin from runs
      where state in ('running','cancel_requested') and projection_partition=$1 and projection_first is not null and id=any($2::uuid[])`,
      [partition, ids],
    );
    return new Map(rows.map((row) => [row.id, row]));
  }
}
