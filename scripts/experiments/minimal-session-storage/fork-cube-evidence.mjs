// Compare the first model requests when the same current physical allocation
// follows two different, physically verified Conversation observations.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { BACKGROUND_CONTEXT as ctx } from "@earendil-works/chord/context";
import { Harness, createRegistry, defineExtension } from "@earendil-works/pi-durable";
import { getCurrentSystemMessage } from "@earendil-works/pi-ai";
import { createModels } from "@earendil-works/pi-ai/models";
import { fauxProvider, fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { PI_SANDBOX_RESET_MESSAGE } from "../../../packages/sandbox-supervisor/src/pi-sandbox-continuity.ts";
import { continuationPrompt, environmentSection } from "./continuation-environment.mjs";
import { installContinuationEnvironments, selectEnvironment } from "./continuation-cases.mjs";
import { selectedRunEnvironment } from "./admission-retention.mjs";
import { installRetentionFixture, seedElasticWorkspace } from "./retention-fixture.mjs";
import { withForkWorkspace } from "./fork-cube-workspace.mjs";

async function sample(conversation, provider) {
  let request,
    calls = 0;
  provider.setResponses([
    (context) => {
      calls++;
      assert(
        context.messages.some(
          (m) =>
            m.role === "assistant" &&
            m.content.some((b) => b.text === "The workspace service is running."),
        ),
      );
      const admission = getCurrentSystemMessage(context.messages)?.sections?.[environmentSection];
      const resetCount =
        Number(admission?.includes("previously used by this conversation was replaced") ?? false) +
        context.messages.filter(
          (m) => m.role === "system" && m.content === PI_SANDBOX_RESET_MESSAGE,
        ).length;
      request = {
        resetCount,
        admission,
      };
      return fauxAssistantMessage("Checked.");
    },
  ]);
  const submission = await conversation.submit(
    { type: "input", requestId: randomUUID(), content: "Is the workspace service still running?" },
    ctx,
  );
  await conversation.waitForIdle(ctx);
  assert.equal((await submission.wait(ctx)).status, "done");
  assert.equal(calls, 1);
  return request;
}

async function coldRequest({ memory, pool, dest }, target) {
  const { tenantId, workspaceId, first, current } = target;
  // Replay only confirmed physical lifecycle facts into this private resource fixture.
  await installRetentionFixture(pool);
  await seedElasticWorkspace(pool, tenantId, workspaceId, "released");
  await seedElasticWorkspace(pool, tenantId, workspaceId, "retained");
  await pool.query(
    "update workspace_runtimes set workspace_runtime_id=case when state='released' then $1::uuid else $2::uuid end",
    [first, current],
  );
  await installContinuationEnvironments(pool);
  await selectEnvironment(pool, tenantId, dest, {
    workspace: workspaceId,
    cwd: "/home/user",
    executionMode: "elastic",
  });
  const c = await pool.connect();
  let admitted;
  try {
    await c.query("begin");
    const key = randomUUID();
    admitted = await selectedRunEnvironment(c, tenantId, dest, key);
    assert.equal(admitted.elastic.state, "retained");
    await c.query(
      `insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,environment_snapshot)
      values($1,$2,$3,1,$1,'check','fork-study','queued',$4)`,
      [key, tenantId, dest, admitted],
    );
    await c.query("commit");
  } catch (e) {
    await c.query("rollback");
    throw e;
  } finally {
    c.release();
  }
  const {
    rows: [row],
  } = await pool.query(
    "select environment_snapshot from runs where session_id=$1 and tenant_id=$2",
    [dest, tenantId],
  );
  const provider = fauxProvider({
    api: "fork-study-api",
    provider: "fork-study",
    tokensPerSecond: 0,
  });
  const models = createModels(),
    registry = createRegistry();
  models.setProvider(provider.provider);
  registry.install(
    defineExtension({
      name: "fork-admission",
      sections: [
        {
          key: environmentSection,
          tag: false,
          render: () => continuationPrompt(undefined, row.environment_snapshot),
        },
      ],
    }),
  );
  const cold = await Harness.open(
    memory,
    { models, registry, settings: { retry: { maxRetries: 0 } } },
    ctx,
  );
  try {
    const root = await cold.root(ctx);
    const request = await sample(root, provider);
    assert.match(request.admission, /retained the existing elastic sandbox/);
    return request;
  } finally {
    await cold.close(ctx);
  }
}

export async function cubeForkEvidence(base, admin, scope, nativeHistory, pgHistory) {
  const evidence = await withForkWorkspace(async (fixture) => {
    const { tenantId, workspaceId, first } = fixture;
    scope = { ...scope, tenantId };
    let current;
    const changed = await nativeHistory("read", first, {
      beforeFork: async () => (current = await fixture.replace()),
      sample,
    });
    const target = { tenantId, workspaceId, first, current };
    const ids = Array.from({ length: changed.batches.length + 1 }, () => randomUUID());
    const candidateChanged = await pgHistory(base, admin, scope, changed, ids, (fork) =>
      coldRequest(fork, target),
    );
    assert.equal(await fixture.startControl(), current);
    const unchanged = await nativeHistory("read", current, {
      beforeFork: async () => {
        await fixture.confirmControl();
        return current;
      },
      sample,
    });
    assert.equal(changed.batches.length, unchanged.batches.length);
    assert.equal(changed.anchor, unchanged.anchor);
    const candidateUnchanged = await pgHistory(base, admin, scope, unchanged, ids, (fork) =>
      coldRequest(fork, target),
    );
    await fixture.confirmControl();
    assert.deepEqual(candidateChanged.page, candidateUnchanged.page);
    const { conversation_worlds: changedWorld, ...changedSource } = candidateChanged.retained;
    const { conversation_worlds: unchangedWorld, ...unchangedSource } = candidateUnchanged.retained;
    assert.deepEqual(changedSource, unchangedSource);
    assert.equal(changedWorld.length, 1);
    assert.equal(unchangedWorld.length, 1);
    assert.equal(changedWorld[0].state.sandbox.continuityId, first);
    assert.equal(unchangedWorld[0].state.sandbox.continuityId, current);
    assert.equal(changed.firstRequest.resetCount, 1);
    assert.equal(unchanged.firstRequest.resetCount, 0);
    assert.equal(candidateChanged.firstRequest.resetCount, 1);
    assert.equal(candidateUnchanged.firstRequest.resetCount, 0);
    for (const req of [candidateChanged.firstRequest, candidateUnchanged.firstRequest]) {
      assert(!req.admission.includes(first) && !req.admission.includes(current));
    }
    const cases = [
      ["original process still serving", unchanged, candidateUnchanged],
      ["original process gone", changed, candidateChanged],
    ].map(([scenario, native, candidate]) => ({
      scenario,
      nativeResets: native.firstRequest.resetCount,
      candidateResets: candidate.firstRequest.resetCount,
    }));
    return {
      cases,
      identicalSourceExceptObservation: true,
      identicalVisibleEntries: true,
      bothAdmissionsRetained: true,
      previousVmConfirmedAbsent: true,
      distinctBoots: true,
      sameVolumeMarker: true,
    };
  });
  console.log(JSON.stringify({ ...evidence, cleanupVerified: true }, null, 2));
  assert(evidence.cases.every((c) => c.nativeResets === c.candidateResets));
}
