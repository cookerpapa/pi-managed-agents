import {
  messagingPrompt,
  cancellationPrompt,
  guestBoundaryPrompt,
} from "./lib/subagent-interaction-prompts.mjs";
import assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { PiCloudApi } from "../packages/web-ui/src/api.ts";
import { newIdempotencyKey } from "@pi-cloud/session-client";
import { execIn } from "./lib/production-kubernetes.mjs";

if (process.env.PI_CLOUD_LIVE_SUBAGENT_CHECK !== "1")
  throw new Error("Enable real Luna/Cube Subagent acceptance explicitly");
const dir = "deploy/production/runtime";
const port = (await readFile(`${dir}/.env`, "utf8")).match(/^PI_CLOUD_HTTP_PORT=(\d+)$/m)?.[1];
const api = new PiCloudApi(
  (url, init) => fetch(new URL(String(url), `http://127.0.0.1:${port}`), init),
  (await readFile(`${dir}/secrets/api-token`, "utf8")).trim(),
);
const query = (sql) =>
  JSON.parse(
    execIn("postgres", ["psql", "-U", "pi_cloud", "-d", "pi_cloud", "-At", "-c", sql], {
      stdio: "pipe",
    }),
  );
const terminal = (run) => ["completed", "failed", "cancelled", "timed_out"].includes(run.state);
async function until(fn, name, ms = 300000) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    const value = await fn();
    if (value) return value;
    await delay(250);
  }
  throw new Error(`${name} timed out`);
}
const report = {
  checkedAt: new Date().toISOString(),
  model: "gpt-6-luna",
  accepted: false,
  scenarios: [],
};
const sessions = [],
  runs = [];
let project, failure;
try {
  project = await api.createProject(`Durable interaction ${Date.now()}`);
  report.resources = { projectId: project.projectId, workspaceId: project.workspaceId };
  const marker = `MESSAGE_${randomUUID().slice(0, 8)}`;
  const scenarios = [
    {
      name: "supervisor",
      prompt: [
        "Delegate exactly ONE fresh child with workspace:null using subagent.",
        "Its exact task is: Call contact_supervisor with reason need_decision and ask for the approved colour; wait for the reply and answer with that colour. Do not delegate.",
        "When it returns blocked, call subagent_supervisor action:reply with the returned replyTo ID and message APPROVED-TEAL.",
        "Wait for that same child's actual result; do not launch another child. Finally reply SUPERVISOR-TEAL-OK.",
      ].join(" "),
      marker: "SUPERVISOR-TEAL-OK",
      count: 1,
    },
    {
      name: "messaging",
      prompt: messagingPrompt(marker),
      marker,
      count: 1,
    },
    {
      name: "parallel",
      prompt:
        'Call subagent twice using context:"fresh" and workspace:null. Ask one child to reply LEFT-DURABLE-OK and the other RIGHT-DURABLE-OK. Wait for both results and reply PARALLEL-DURABLE-OK.',
      marker: "PARALLEL-DURABLE-OK",
      count: 2,
    },
  ];
  scenarios.push({
    name: "cancellation",
    count: 1,
    childState: "cancelled",
    marker: "SUBAGENT-CANCEL-OK",
    prompt: cancellationPrompt,
  });
  scenarios.push({
    name: "guest-boundary",
    count: 0,
    marker: "GUEST-BOUNDARY-OK",
    prompt: guestBoundaryPrompt,
  });
  const selected = scenarios.filter(
    (scenario) =>
      !process.env.PI_CLOUD_LIVE_SUBAGENT_SCENARIO ||
      scenario.name === process.env.PI_CLOUD_LIVE_SUBAGENT_SCENARIO,
  );
  assert(selected.length, "Unknown interaction scenario");
  for (const scenario of selected) {
    const session = await api.createSession(
      project.projectId,
      project.workspaceId,
      scenario.name,
      "elastic",
      "starter",
      "/workspace",
      { provider: "openai-codex", modelId: report.model, thinkingLevel: "low", fastMode: false },
    );
    sessions.push(session);
    const accepted = await api.acceptTurn(
      session.sessionId,
      scenario.prompt,
      newIdempotencyKey("durable-interaction"),
      "low",
    );
    runs.push({ session, ...accepted });
    console.log(JSON.stringify({ scenario: scenario.name, runId: accepted.runId }));
    const run = await until(async () => {
      const run = await api.getRun(accepted.runId);
      return terminal(run) && run;
    }, scenario.name);
    assert.equal(run.state, "completed", JSON.stringify(run.failure));
    const turn = (await api.getConversation(session.sessionId)).turns.find(
      (t) => t.runId === accepted.runId,
    );
    const text = turn.transcript.items
      .filter((item) => item.kind === "text")
      .map((item) => item.text)
      .join("");
    const children = query(
      `select coalesce(json_agg(json_build_object('id',e.id,'state',e.state,'taskState',e.state,'failure',e.failure_code,'sameOwner',not exists(select 1 from runs cr where cr.session_id=e.child_session_id),'samePhysical',s.pi_session_id=ps.pi_session_id,'conversation',s.pi_conversation_id)),'[]') from subagent_executions e join runs p on p.id=e.root_run_id join sessions s on s.id=e.child_session_id join sessions ps on ps.id=p.session_id where e.root_run_id='${accepted.runId}'`,
    );
    const errors = turn.transcript.items
      .filter((item) => item.kind === "tool" && item.status !== "completed")
      .map((item) => ({
        name: item.toolName,
        status: item.status,
        output: JSON.stringify(item.output).slice(0, 1500),
      }));
    assert.equal(children.length, scenario.count, JSON.stringify({ children, errors }));
    assert(
      children.every(
        (c) =>
          c.taskState === (scenario.childState ?? "completed") &&
          c.sameOwner &&
          c.samePhysical &&
          Number(c.conversation) > 1,
      ),
      JSON.stringify({ children, errors }),
    );
    assert(text.includes(scenario.marker), JSON.stringify({ text, errors }));
    if (scenario.name === "supervisor") {
      const answered = query(
        `select count(*)::int from subagent_supervisor_requests q join subagent_executions e on e.id=q.execution_id where e.root_run_id='${accepted.runId}' and q.reply_message='APPROVED-TEAL'`,
      );
      assert.equal(answered, 1);
    }
    if (scenario.name === "guest-boundary") {
      const tool = turn.transcript.items.find(
        (item) => item.kind === "tool" && item.toolName === "bash",
      );
      const value = JSON.parse(tool.output.content.find((part) => part.type === "text").text);
      assert.equal(value.cwd, "/workspace");
      assert.equal(value.uid, 1000);
      assert.deepEqual(value.leakedKeys, []);
    }
    report.scenarios.push({
      name: scenario.name,
      runId: accepted.runId,
      descendants: children.length,
      sameOwner: true,
      markerVerified: true,
    });
    console.log(JSON.stringify(report.scenarios.at(-1)));
  }
  report.accepted = true;
} catch (error) {
  failure = error;
  report.failure = error.message;
} finally {
  const errors = [];
  for (const run of runs)
    try {
      if (!terminal(await api.getRun(run.runId))) {
        await api.cancelTurn(run.session.sessionId, run.turnId, newIdempotencyKey("cleanup"));
        await until(async () => terminal(await api.getRun(run.runId)), "cleanup");
      }
    } catch (e) {
      errors.push(e);
    }
  for (const session of sessions)
    try {
      await api.deleteConversation(session.sessionId);
    } catch (e) {
      errors.push(e);
    }
  if (project)
    try {
      await api.deleteWorkspace(project.workspaceId, newIdempotencyKey("cleanup"));
    } catch (e) {
      errors.push(e);
    }
  report.cleanupCompleted = errors.length === 0;
  await writeFile(
    process.argv[2] ?? "/tmp/pi-cloud-durable-interactions.json",
    JSON.stringify(report, null, 2) + "\n",
    { mode: 0o600 },
  );
  if (failure || errors.length)
    throw new AggregateError(
      [failure, ...errors].filter(Boolean),
      "Durable interaction acceptance failed",
    );
}
