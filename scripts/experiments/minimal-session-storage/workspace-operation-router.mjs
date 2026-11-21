import { isHistoryOnlyCommit } from "../../../packages/runtime-core/src/accepted-fact.ts";
// The admitted partition owner selects the resource. An operation retains its
// Session offset; only execution resolves that position against durable bindings.
import assert from "node:assert/strict";
import { operationalLog } from "@pi-cloud/observability";
import { parseToolSandboxOperationRequest } from "@pi-cloud/protocol";
import { WorkspaceRouteSpans } from "./workspace-route-spans.mjs";
import { operationsFor } from "./workspace-operations.mjs";
import { plainExecution } from "./tool-grant-prefix.mjs";
import { projectRecord } from "./project-record.mjs";

export class WorkspaceOperationRouter {
  constructor({ pool, view, env, select, resolve }) {
    Object.assign(this, { pool, view, env, select, resolve });
    this.spans = new WorkspaceRouteSpans(pool);
  }
  async original(scope, op, position) {
    const {
      rows: [r],
    } = await this.pool.query(
      `select projection_tools->$5 as waiter, cancel_positions->$4 as cancelled
      from runs where tenant_id=$1 and session_id=$2 and id=$3`,
      [scope.tenantId, scope.piSessionId, scope.runId, op.operationId, String(op.taskId)],
    );
    const locator = r?.waiter ?? r?.cancelled;
    if (
      !locator ||
      locator.operationId !== op.operationId ||
      locator.taskId !== op.taskId ||
      (op.conversationId !== undefined && locator.conversationId !== op.conversationId)
    )
      return;
    if (op.reply) assert.deepEqual(locator.reply, op.reply, "Original reply reference changed");
    const span = await this.spans.at(scope, locator.conversationId, {
      ...position,
      offset: BigInt(locator.offset),
    });
    if (
      !span ||
      (op.scope && (span.sessionId !== op.scope.sessionId || span.turnId !== op.scope.turnId))
    )
      return;
    return {
      ...span,
      taskId: locator.taskId,
      conversationId: locator.conversationId,
      offset: locator.offset,
    };
  }
  // Input receipts, commands, controls and terminal records keep their original
  // per-record order. A stored tool proposal/result alone has no dispatch effect.
  canBatch(f) {
    return isHistoryOnlyCommit(f) && !operationsFor(f).length;
  }
  async prepareHistory(records, current) {
    let n = 0;
    while (n < records.length && n < this.view.width && this.canBatch(records[n].fact)) n++;
    if (!n) return 0;
    return this.view.apply(records.slice(0, n), current, (r, d) => projectRecord(r, d, this.env));
  }
  async #routeOperation(record, current, warmOnly = false) {
    const f = record.fact,
      ops = operationsFor(f);
    const covered = record.offset < this.view.next.get(record.partition);
    const tools = [],
      cancels = [],
      commands = [];
    for (const op of ops) {
      if (op.action === "cancel") {
        const original = await this.original(f.scope, op, record);
        cancels.push({ op, original });
        continue;
      }
      assert.equal(op.action, "execute");
      let route;
      if (covered) route = await this.original(f.scope, op, record);
      else {
        let span = await this.spans.at(op.scope, op.conversationId, record);
        if (!span || span.writerId !== op.scope.writerId) {
          if (warmOnly) return;
          span = await this.spans.activate(
            op.scope,
            op.conversationId,
            record,
            await this.select(op.scope, op.conversationId),
            current,
          );
        }
        assert(span.sessionId === op.scope.sessionId && span.turnId === op.scope.turnId);
        route = {
          ...span,
          taskId: op.taskId,
          conversationId: op.conversationId,
          offset: String(record.offset),
        };
      }
      const request = {
        operation: "tool.execute",
        type: "tool_sandbox.operation",
        toolBrokerProtocolVersion: 1,
        toolName: op.reply.toolName,
        operationId: op.operationId,
        timeoutMs: op.timeoutMs,
        maximumOutputBytes: op.maximumOutputBytes,
      };
      tools.push({
        target: "cube",
        scope: op.scope,
        taskId: op.taskId,
        request,
        replyTopic: op.reply.topic,
        replyRecovery: op.reply,
        occurredAt: f.occurredAt,
      });
      if (!route) continue;
      assert.equal(route.offset, String(record.offset));

      commands.push({
        op,
        route,
        command: {
          scope: op.scope,
          toolCallId: "task:" + op.taskId,
          request: parseToolSandboxOperationRequest({
            ...request,
            activationId: route.bindingId,
            toolCallId: op.reply.toolCallId,
            args: op.args,
          }),
        },
      });
    }
    const candidate = {
      ...record,
      fact: { ...f, tools, cancellations: cancels.map(({ op }) => op) },
      workspaceBatch: true,
    };
    return { candidate, commands, cancels };
  }
  async projectOperation(record, current, mark) {
    const { candidate, commands, cancels } = await this.#routeOperation(record, current);
    mark?.("routeReady");
    await this.view.apply([candidate], current, (r, d) => projectRecord(r, d, this.env));
    mark?.("projected");
    if (!current() || !this.view.canDeliver(record)) return;
    return { commands, cancels };
  }
  canGroupOperation(record) {
    return plainExecution(record.fact, this.view.sessions.get(record.fact.scope?.runId)?.liveDocs);
  }
  async projectOperations(records, current, mark) {
    const first = records[0],
      routed = [],
      runs = new Set(),
      bindings = new Set();
    for (const r of records) {
      if (!current()) return [];
      const f = r.fact;
      const previous = this.view.sessions.get(f.scope.runId);
      if (
        !this.canGroupOperation(r) ||
        r.topic !== first.topic ||
        r.partition !== first.partition ||
        r.offset !== this.view.next.get(r.partition) + BigInt(routed.length) ||
        runs.has(f.scope.runId) ||
        previous?.writer !== f.scope.writerId ||
        previous.seq + 1 !== f.commit.seq
      )
        break;
      const plan = await this.#routeOperation(r, current, true);
      if (!plan) break;
      assert(plan.commands.length === 1 && !plan.cancels.length);
      const binding = plan.commands[0].route.bindingId;
      if (bindings.has(binding)) break;
      bindings.add(binding);
      runs.add(f.scope.runId);
      routed.push(plan);
    }
    if (!routed.length) {
      if (!current()) return [];
      const one = await this.projectOperation(first, current, mark);
      return one ? [one] : [];
    }
    mark?.("routeReady");
    const candidates = routed.map((p) => p.candidate);
    const n = await this.view.applyToolIntents(candidates, current, (r, d) =>
      projectRecord(r, d, this.env),
    );
    mark?.("projected");
    if (!current()) return [];
    assert.equal(n, routed.length);
    if (n === 1 && !this.view.canDeliver(first)) return [];
    assert(
      candidates.every((r) => this.view.canDeliver(r)),
      "Incomplete tool prefix admission",
    );
    return routed;
  }
  async consume(record, current, grant, mark) {
    const prepared = await this.projectOperation(record, current, mark);
    if (!prepared) return;
    const { commands, cancels } = prepared;
    for (const { op, original } of cancels) {
      if (!original || !current()) continue;
      // The provider bounds its cancellation RPC. A transport error leaves
      // physical termination unknown; Pi's committed abort can still settle.
      try {
        await this.resolve(original.bindingId)?.cancel(record, {
          operationId: op.operationId,
          activationId: original.bindingId,
          originalWriterId: original.writerId,
        });
      } catch {
        operationalLog({
          service: "pma-routing-study",
          level: "warn",
          event: "tool.cancellation.unconfirmed",
          attributes: { operationId: op.operationId },
        });
      }
    }
    if (commands.length && current() && (await grant()) && current()) {
      mark?.("granted");
      for (const { op, route, command } of commands) {
        if (!current()) return;
        const endpoint = this.resolve(route.bindingId);
        assert(endpoint, "Original execution target unavailable");
        await endpoint.submit(record, command, op.reply);
      }
    }
  }
}
