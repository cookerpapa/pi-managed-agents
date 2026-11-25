import { randomUUID } from "node:crypto";
import { sql, type Kysely } from "kysely";
import { PostgresNotificationWake, type Database } from "@pi-cloud/database";
import {
  createExecutionReference,
  serializeSubagentResult,
  type SubagentHostRequest,
} from "@pi-cloud/protocol";
import type { ToolReply } from "@pi-cloud/event-log";
import {
  HttpSupervisorManagementClient,
  HttpSupervisorManagementError,
} from "./http-supervisor-management.ts";
import type { ChildRow } from "./named-subagent-store.ts";

type Delivery = {
  operation_id: string;
  root_id: string;
  writer: string;
  caller_lane: string;
  call_id: string;
  tool: string;
  destination: string;
  child_id: string | null;
  kind: string;
  value: Record<string, unknown>;
  claim: string;
};
type Handle = { job: "place" | "reply"; id: string; token: string };
type Route = { lease_id: string; fencing_token: string; management_base_url: string };
const text = (body: string) => ({ content: [{ type: "text" as const, text: body }] });

/** Durable claims expire after process loss. Native placement and each mailbox delivery
 * have immutable identities, including a publication retried after an uncertain ACK. */
export class NamedSubagentExecutor {
  readonly #stop = new AbortController();
  readonly #wake: PostgresNotificationWake;
  readonly #active = new Set<Promise<void>>();
  #loop?: Promise<void>;
  constructor(
    readonly options: {
      db: Kysely<Database>;
      connectionString: string;

      publish: (topic: string, reply: ToolReply) => Promise<void>;
    } & (
      { independentSessions: true; token?: never } | { independentSessions?: false; token: string }
    ),
  ) {
    this.#wake = new PostgresNotificationWake(options.connectionString, "pma_named_children");
  }
  start() {
    this.#loop ??= this.#run();
  }
  wake() {
    this.#wake.notify();
  }
  async close() {
    this.#stop.abort();
    this.#wake.notify();
    await this.#loop;
    await Promise.allSettled(this.#active);
    await this.#wake.close();
  }
  async #run() {
    while (!this.#stop.signal.aborted) {
      const observed = this.#wake.generation;
      this.#wake.refresh();
      let delay = 1000;
      try {
        if (this.#active.size < 16) {
          const jobs = await this.#take(Math.min(8, 16 - this.#active.size));
          for (const job of jobs) {
            const p = (job.job === "place" ? this.#place(job) : this.#deliver(job))
              .catch(async (e) => {
                console.warn(
                  "subagent.executor.retry",
                  job.job,
                  e instanceof HttpSupervisorManagementError ? e.code : "unconfirmed",
                );
                await this.#release(job, 1500).catch(() => {});
              })
              .finally(() => {
                this.#active.delete(p);
                this.#wake.notify();
              });
            this.#active.add(p);
          }
          if (jobs.length) continue;
          const next = (
            await sql<{ ms: string | null }>`select extract(epoch from (least(
            (select due_at from subagent_deliveries where not sent and due_at is not null order by due_at limit 1),
            ${
              this.options.independentSessions
                ? sql`null::timestamptz`
                : sql`(select c.placement_due from runs c join runs r on r.id=c.root_run_id and r.state='running'
             where c.state='preparing' and c.placement_due is not null order by c.placement_due limit 1)`
            }
          ) - clock_timestamp()))*1000 as ms`.execute(this.options.db)
          ).rows[0]?.ms;
          delay = next == null ? 30000 : Math.max(25, Math.min(30000, Math.ceil(Number(next))));
        }
      } catch {
        console.warn("subagent.executor.scan_unavailable");
      }
      await this.#wake.wait(observed, delay, this.#stop.signal);
    }
  }
  async #take(max: number): Promise<Handle[]> {
    const db = this.options.db,
      token = randomUUID();
    // Reserve capacity for callbacks while slow Worker endpoints are unavailable.
    const { rows: a } = this.options.independentSessions
      ? { rows: [] }
      : await sql<{
          id: string;
        }>`with pick as(select c.id from runs c join runs r on r.id=c.root_run_id and r.state='running'
      where c.state='preparing' and c.placement_due<=now() order by c.placement_due,c.id limit ${Math.ceil(max / 2)} for update of c skip locked)
      update runs c set placement_claim=${token}::uuid,placement_due=now()+interval '30 seconds' from pick where c.id=pick.id returning c.id`.execute(
          db,
        );
    const { rows: b } = await sql<{
      id: string;
    }>`with pick as(select operation_id from subagent_deliveries where not sent and due_at<=now()
      order by due_at,operation_id limit ${max - a.length} for update skip locked)
      update subagent_deliveries d set claim=${token}::uuid,due_at=now()+interval '45 seconds' from pick where d.operation_id=pick.operation_id returning d.operation_id as id`.execute(
      db,
    );
    return [
      ...a.map((r) => ({ job: "place" as const, id: r.id, token })),
      ...b.map((r) => ({ job: "reply" as const, id: r.id, token })),
    ];
  }
  async #release(j: Handle, ms: number) {
    if (j.job === "place")
      await sql`update runs set placement_due=now()+${ms}*interval '1 millisecond',placement_claim=null where id=${j.id}::uuid and placement_claim=${j.token}::uuid`.execute(
        this.options.db,
      );
    else
      await sql`update subagent_deliveries set due_at=now()+${ms}*interval '1 millisecond',claim=null where operation_id=${j.id}::uuid and claim=${j.token}::uuid`.execute(
        this.options.db,
      );
  }
  async #route(root: string) {
    return (
      await sql<Route>`select r.lease_id,r.fencing_token,h.management_base_url from runs r join workers w on w.id=r.worker_id
      join supervisor_hosts h on h.supervisor_id=w.supervisor_id where r.id=${root}::uuid and r.state='running' and r.lease_id is not null`.execute(
        this.options.db,
      )
    ).rows[0];
  }
  #rpc(route: Route, request: SubagentHostRequest) {
    if (this.options.independentSessions)
      throw Error("Independent children use their own Session worker");
    return new HttpSupervisorManagementClient({
      baseUrl: route.management_base_url,
      managementToken: this.options.token,
      allowInsecureHttp: true,
    }).subagent(request);
  }
  async #place(j: Handle) {
    const child = (
      await sql<ChildRow>`select * from runs where id=${j.id}::uuid and state='preparing' and placement_claim=${j.token}::uuid`.execute(
        this.options.db,
      )
    ).rows[0];
    if (!child) return;
    const r = await this.#route(child.root_run_id);
    if (!r) {
      await this.#release(j, 2000);
      return;
    }
    const g = child.workspace_grant,
      grant = g?.tools ?? [];
    try {
      const lane = await this.#rpc(r, {
        action: "start_conversation",
        executionReference: createExecutionReference(
          r.lease_id,
          child.root_run_id,
          Number(r.fencing_token),
        ),
        prompt: child.initial_input,
        view: {
          id: child.id,
          agentName: child.agent_name,
          sessionId: child.agent_id,
          turnId: child.id,
          parentConversationId: Number(child.placement_lane),
          ownerTaskId: Number(child.source_task_id),
          contextAnchor: child.context_at === null ? null : Number(child.context_at),
          source: {
            taskId: Number(child.source_task_id),
            sequence: Number(child.placement_sequence),
          },
          workingDirectory: g?.cwd ?? "/workspace",
          computeSessionId: g?.computeSessionId ?? null,
          toolCapabilities: grant,
          agentSystemPrompt:
            "Execute your delegated task. Use contact_supervisor when you need a decision.",
        },
      });
      if (typeof lane !== "number" || !Number.isSafeInteger(lane) || lane <= 1)
        throw Error("Invalid native placement receipt");
      await sql`update runs set placement_due=null,placement_claim=null where id=${j.id}::uuid and placement_claim=${j.token}::uuid`.execute(
        this.options.db,
      );
    } catch (e) {
      if (!(e instanceof HttpSupervisorManagementError) || e.retryable) throw e;
      // A definite refusal from this still-current owner can settle a preparation.
      await sql`update runs c set state='failed',settled_at=now(),placement_due=null,placement_claim=null from runs root
        where c.id=${j.id}::uuid and c.placement_claim=${j.token}::uuid and c.state='preparing' and root.id=c.root_run_id and root.lease_id=${r.lease_id}::uuid`.execute(
        this.options.db,
      );
    }
  }
  async #deliver(j: Handle) {
    const running = (state: string) =>
      state === "preparing" ||
      state === "running" ||
      (this.options.independentSessions &&
        ["queued", "settling", "cancel_requested"].includes(state));
    const db = this.options.db;
    const d = (
      await sql<
        Delivery & { owner: string | null }
      >`select d.*,r.lease_id as owner from subagent_deliveries d join runs r on r.id=d.root_id
      where d.operation_id=${j.id}::uuid and d.claim=${j.token}::uuid and not d.sent`.execute(db)
    ).rows[0];
    if (!d) return;
    if (d.owner !== d.writer) {
      await sql`delete from subagent_deliveries where operation_id=${j.id}::uuid and claim=${j.token}::uuid`.execute(
        db,
      );
      return;
    }
    const child = d.child_id
      ? (
          await sql<
            Pick<
              ChildRow,
              | "id"
              | "state"
              | "tenant_id"
              | "session_id"
              | "root_run_id"
              | "agent_id"
              | "native_lane"
              | "native_answer_id"
              | "agent_name"
            >
          >`${
            this.options.independentSessions
              ? sql`select c.id,c.state,c.tenant_id,c.session_id,c.id as root_run_id,${String(d.value.executionId)}::uuid as agent_id,
              1::bigint as native_lane,(select m.entry_id from message_entries m where m.tenant_id=c.tenant_id and m.pi_session_id=c.session_id
              and m.run_id=c.id and m.conversation_id=1 and m.role='assistant' and m.content::json #>> '{model,0,stopReason}' in ('stop','length') order by m.entry_id desc limit 1) as native_answer_id,${String(d.value.name)}::text as agent_name
              from runs c where c.id=${d.child_id}::uuid`
              : sql`select c.id,c.state,c.tenant_id,c.session_id,c.root_run_id,c.agent_id,c.native_lane,c.native_answer_id,a.agent_name
          from runs c join runs a on a.id=c.agent_id where c.id=${d.child_id}::uuid`
          }`.execute(db)
        ).rows[0]
      : undefined;
    let value = d.value,
      kind = d.kind;
    if (kind === "question") {
      if (typeof value.reply !== "string") {
        const left = Date.parse(String(value.expiresAt)) - Date.now();
        if (child && running(child.state) && left > 0) {
          await this.#release(j, left);
          return;
        }
        value = {
          ...value,
          text: left <= 0 ? "等待主管答复超时。" : "子 Agent 已结束。",
          isError: true,
        };
      } else value = { ...value, text: value.reply };
      kind = "answered";
    } else if (kind === "watch" || kind === "state") {
      if (!child) throw Error("Missing named child");
      const status = {
        name: child.agent_name,
        executionId: child.agent_id,
        runId: child.id,
        state: child.state,
      };
      if (!running(child.state) && kind === "state" && d.tool === "subagent_control") {
        kind = "fixed";
        value = { text: JSON.stringify(status) };
      } else if (!running(child.state)) {
        kind = "finished";
        value = { ...status, answerId: child.native_answer_id };
      } else {
        const question = (
          await sql<{
            operation_id: string;
            value: Record<string, unknown>;
          }>`select operation_id,value from subagent_deliveries
          where child_id=${child.id}::uuid and kind='question' and not sent and not(value?'reply') and (value->>'expiresAt')::timestamptz>now() and writer=${d.writer}::uuid
          order by created_at,operation_id limit 1`.execute(db)
        ).rows[0];
        if (!question && kind === "watch") {
          // A terminal projection or new supervisor question clears the claim
          // and schedules delivery. The token prevents parking over that wake.
          await sql`update subagent_deliveries set due_at=null,claim=null where operation_id=${j.id}::uuid and claim=${j.token}::uuid`.execute(
            db,
          );
          return;
        }
        kind = "fixed";
        value = {
          text: JSON.stringify(
            question
              ? {
                  ...status,
                  state: "blocked",
                  supervisorRequest: { ...question.value, requestId: question.operation_id },
                  replyTo: question.operation_id,
                }
              : status,
          ),
        };
      }
    } else if (kind === "send" || kind === "cancel") {
      const r = await this.#route(d.root_id);
      if (!r || r.lease_id !== d.writer) {
        await this.#release(j, 1000);
        return;
      }
      const reference = createExecutionReference(r.lease_id, d.root_id, Number(r.fencing_token));
      try {
        if (child && !running(child.state)) value = { text: "子 Agent 已结束。" };
        else if (kind === "cancel" && child) {
          await this.#rpc(r, {
            action: "stop_conversation",
            executionReference: reference,
            requestId: child.id,
          });
          value = { text: "已请求停止子 Agent。" };
        } else {
          const lane = Number(child?.native_lane ?? value.lane);
          if (!(lane > 0)) {
            await this.#release(j, 1000);
            return;
          }
          const delivered = await this.#rpc(r, {
            action: "input",
            executionReference: reference,
            runId: d.root_id,
            conversationId: lane,
            requestId: d.operation_id,
            message: String(value.message),
            delivery: value.delivery as "steer" | "notify" | "follow_up",
          });
          value = { text: JSON.stringify(delivered ?? {}) };
        }
      } catch (error) {
        if (!(error instanceof HttpSupervisorManagementError) || error.retryable) throw error;
        value = { text: error.message, isError: true };
      }
      kind = "fixed";
    }
    // Freeze the observed response before Kafka publication. Terminal outputs
    // are referenced by immutable native entry ID instead of copied per waiter.
    if (kind !== d.kind) {
      const saved =
        await sql`with durable as materialized (select set_config('synchronous_commit','on',true)) update subagent_deliveries set kind=${kind},value=${JSON.stringify(value)}::jsonb from durable
        where operation_id=${j.id}::uuid and claim=${j.token}::uuid`.execute(db);
      if (!Number(saved.numAffectedRows)) return;
    }
    let message = String(value.text ?? ""),
      failed = Boolean(value.isError);
    if (kind === "finished" && child) {
      const { answerId } = value;
      const status = {
        name: value.name,
        executionId: value.executionId,
        runId: value.runId,
        state: value.state,
      };
      const entry = answerId
        ? (
            await sql<{
              content: string;
            }>`select content from message_entries where tenant_id=${child.tenant_id}::uuid
        and pi_session_id=${child.session_id}::uuid and run_id=${child.root_run_id}::uuid and conversation_id=${child.native_lane} and entry_id=${String(answerId)}::bigint`.execute(
              db,
            )
          ).rows[0]
        : undefined;
      if (status.state === "completed" && !entry) throw Error("Missing original child answer");
      const answer = entry
        ? JSON.parse(entry.content).model?.find((m: { role: string }) => m.role === "assistant")
        : undefined;
      const output = answer?.content
        ?.filter((p: { type: string }) => p.type === "text")
        .map((p: { text: string }) => p.text)
        .join("");
      try {
        message = serializeSubagentResult({
          ...status,
          ...(output === undefined ? {} : { output }),
        });
        failed = status.state !== "completed";
      } catch {
        message = "subagent_result_too_large";
        failed = true;
      }
    }
    try {
      serializeSubagentResult(message);
    } catch {
      message = "subagent_result_too_large";
      failed = true;
    }
    await this.options.publish(d.destination, {
      operationId: d.operation_id,
      sequence: 1,
      kind: "tool_result",
      result: {
        toolCallId: d.call_id,
        toolName: d.tool,
        isError: failed,
        result: text(message),
      },
    });
    await sql`update subagent_deliveries set sent=true,due_at=null,claim=null where operation_id=${j.id}::uuid and claim=${j.token}::uuid`.execute(
      db,
    );
  }
}
