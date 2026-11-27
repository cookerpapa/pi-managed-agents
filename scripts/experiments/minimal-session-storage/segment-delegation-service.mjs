import { createHash } from "node:crypto";
import { sql } from "kysely";
import { readSealedDelegationIntent } from "@pi-cloud/pi-session-postgres/durable";
import { historyScope } from "./open-segment-native.mjs";
import { ChildNameConflict } from "./fork-storage.mjs";
import Fastify from "fastify";
import { installNamedSubagentEndpoint } from "../../../packages/control-plane/src/named-subagent-endpoint.ts";
import { SubagentAdmissionRefused } from "../../../packages/control-plane/src/named-subagent-store.ts";
import { NamedSubagentExecutor } from "../../../packages/control-plane/src/named-subagent-executor.ts";

export async function serveSegmentDelegations(
  { store, inputs, topic },
  { connectionString, publish, token, host = "127.0.0.1", port = 0 },
) {
  const dispatcher = new NamedSubagentExecutor({
    db: store.db,
    connectionString,
    publish,
    independentSessions: true,
  });
  const api = Fastify({ logger: false, bodyLimit: 1024 * 1024 });
  installNamedSubagentEndpoint(
    api,
    token,
    () => true,
    async (o) => {
      if (o.tool === "send_message" && "args" in o.input) {
        await inputs.sendAgent(o);
        dispatcher.wake();
        return;
      }
      if (o.tool !== "subagent")
        throw new SubagentAdmissionRefused("Expected independent delegation");
      const {
        rows: [caller],
      } =
        await sql`select s.flow_id,r.model_snapshot from sessions s join turns r on r.tenant_id=s.tenant_id and r.session_id=s.flow_id
        where s.tenant_id=${o.tenant}::uuid and s.id=${o.session}::uuid and r.id=${o.root}::uuid and r.lease_id=${o.writer}::uuid
        and r.fencing_token=${o.epoch} and r.state='running' and r.output_seal_id is null`.execute(
          store.db,
        );
      if (!caller) throw new SubagentAdmissionRefused("Caller writer closed");
      const s = {
        tenantId: o.tenant,
        sessionId: o.session,
        piSessionId: o.session,
        runId: o.root,
        turnId: o.root,
        writerId: o.writer,
        fencingToken: o.epoch,
      };
      let intent;
      if ("recover" in o.input) {
        intent = await readSealedDelegationIntent(
          store.db,
          historyScope(s, caller.flow_id),
          topic,
          o.lane,
          o.task,
        );
        if (
          intent.operationId !== o.input.recover ||
          intent.sequence !== o.input.seq ||
          intent.contextAnchor !== o.anchor
        )
          throw new SubagentAdmissionRefused("Changed original delegation");
      } else
        intent = {
          toolName: o.tool,
          operationId: o.operation,
          taskId: o.task,
          conversationId: o.lane,
          nativeCallId: o.input.call,
          arguments: o.input.args,
          sequence: o.input.seq,
          contextAnchor: o.anchor,
          contextThrough: o.through,
          requestEntry: o.requestEntry,
          assistantEntry: o.assistantEntry,
        };
      try {
        await store.delegateFromIntent(
          o.tenant,
          o.session,
          { run: o.root, writer: o.writer },
          intent,
          caller.model_snapshot.modelId,
          { operationId: o.operation, topic: o.destination },
        );
      } catch (error) {
        if (!(error instanceof ChildNameConflict)) throw error;
        // A deterministic name conflict is also a durable Tool Result. A stale
        // caller or a failed PG transaction leaves its original bounded wait.
        const hash = createHash("sha256").update(JSON.stringify(o)).digest("hex");
        const { rows } =
          await sql`insert into subagent_deliveries(operation_id,root_id,writer,caller_task,caller_lane,operation_hash,destination,call_id,tool,kind,value,due_at)
          select ${o.operation}::uuid,r.id,${o.writer}::uuid,${o.task},${o.lane},${hash},${o.destination},${intent.nativeCallId},'subagent','fixed',
            jsonb_build_object('isError',true,'text',${"名字已被占用。已有子 Agent 的 UUID 是 " + error.childId + "。请换一个名字或联系已有的子 Agent。"}::text),clock_timestamp()
          from (select id from turns where id=${o.root}::uuid and tenant_id=${o.tenant}::uuid and session_id=${caller.flow_id}::uuid and state='running'
            and output_seal_id is null and lease_id=${o.writer}::uuid and fencing_token=${o.epoch} for share) r
          on conflict(operation_id) do update set operation_hash=excluded.operation_hash where subagent_deliveries.operation_hash=excluded.operation_hash returning operation_id`.execute(
            store.db,
          );
        if (!rows.length) throw new SubagentAdmissionRefused("Stale or conflicting delivery");
      }
      dispatcher.wake();
    },
  );
  api.get("/health/ready", async () => ({ ready: true }));
  const url = await api.listen({ host, port });
  dispatcher.start();
  return {
    url,
    async close() {
      await api.close();
      await dispatcher.close();
    },
  };
}
