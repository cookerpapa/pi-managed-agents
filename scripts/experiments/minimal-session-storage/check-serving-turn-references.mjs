// Exercised after the timed load against the same running API, Worker and Projector.
import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { createExecutionReference } from "@pi-cloud/protocol";
import { ExecutionTargetService } from "../../../packages/control-plane/src/execution-target-service.ts";
import { SessionScheduleService } from "../../../packages/control-plane/src/session-schedule-service.ts";

export async function checkServingTurnReferences({
  api,
  apiUrl,
  identity,
  project,
  pool,
  db,
  fake,
  until,
}) {
  const t = identity.tenantId;
  const { sessionId: s } = await api.createSession(
    project.projectId,
    project.workspaceId,
    "Turn references",
    "elastic",
  );
  const query = (sql, args) => pool.query(sql, args).then((r) => r.rows);
  assert.deepEqual(
    await query(
      "select conrelid::regclass from pg_constraint where contype='f' and confrelid='public.runs'::regclass",
    ),
    [],
  );
  const [recoveryIndex] =
    await query(`select i.indisvalid, pg_get_indexdef(i.indexrelid,1,true) as key,
    i.indnatts::int as columns, pg_get_expr(i.indpred,i.indrelid) as predicate,
    to_regclass('pma_segments.active_projection_start') as old
    from pg_index i where i.indrelid='pma_segments.turns'::regclass
    and i.indexrelid=to_regclass('pma_segments.active_projection_partition')`);
  assert(recoveryIndex?.indisvalid && recoveryIndex.old === null);
  assert.equal(recoveryIndex.key, "projection_partition");
  assert.equal(recoveryIndex.columns, 1);
  assert(!recoveryIndex.predicate.includes("projection_first"));
  const foreignKeys =
    await query(`select r.relname,c.confdeltype as deletion,pg_get_constraintdef(c.oid) as definition
    from pg_constraint c join pg_class r on r.oid=c.conrelid and r.relnamespace='public'::regnamespace
    where c.confrelid='pma_segments.turns'::regclass order by r.relname`);
  assert.deepEqual(
    foreignKeys.map((k) => [k.relname, k.deletion]),
    [
      ["environment_validations", "c"],
      ["execution_target_bindings", "a"],
      ["run_skill_catalogs", "c"],
      ["session_schedules", "n"],
      ["source_control_issue_jobs", "c"],
    ],
  );
  assert(
    foreignKeys.every((k) => k.definition.includes("REFERENCES pma_segments.turns(tenant_id, id)")),
  );
  assert(
    foreignKeys
      .find((k) => k.relname === "session_schedules")
      .definition.endsWith("SET NULL (last_run_id)"),
  );
  // A public Session survives a prune; the Turn and its binding live on the new Flow.
  let keep, anchor;
  for (const text of ["ref_anchor", "ref_tail"]) {
    const answer = await api.acceptTurn(s, fake.prompt(text), "turn:" + randomUUID());
    const result = await until(
      "sealed prune candidate",
      async () =>
        (
          await query(
            `select m.entry_id from pma_segments.turns t join pma_segments.messages m on m.run_id=t.id
      where t.id=$1 and t.state='completed' and m.role='assistant' order by m.commit_seq desc limit 1`,
            [answer.turnId],
          )
        )[0],
    );
    if (!keep) {
      keep = answer.turnId;
      anchor = String(result.entry_id);
    }
  }
  await api.pruneConversation(s, keep, anchor, "prune:" + randomUUID());
  const [logical] = await query(
    "select flow_id from pma_segments.sessions where tenant_id=$1 and id=$2",
    [t, s],
  );
  assert.notEqual(logical.flow_id, s);
  const device = randomUUID(),
    bootId = randomUUID(),
    published = [];
  const targets = new ExecutionTargetService({
    database: db,
    independentTurns: true,
    transport: {
      inspect: async () => ({
        targetId: device,
        bootId,
        kind: "android",
        capabilities: ["screen"],
      }),
      revoke: async () => {},
    },
    publish: async (ids) => published.push(...ids),
  });
  await query(
    "insert into public.execution_targets(id,tenant_id,kind,name,endpoint,capabilities) values($1,$2,'android','fixture','http://fixture.invalid',$3::jsonb)",
    [device, t, JSON.stringify(["screen"])],
  );
  const revision = (await targets.selection(t, s)).revision;
  const selected = await targets.bindSession(t, s, device, revision);
  const skill = randomUUID(),
    version = randomUUID();
  await query(
    "insert into public.skill_packages(id,tenant_id,owner_user_id,name) values($1,$2,$3,'serving-ref-test')",
    [skill, t, identity.ownerUserId],
  );
  await query(
    `insert into public.skill_versions(id,tenant_id,package_id,digest,object_key,byte_length,label,description,compatibility,requirements,source,published,runtime_metadata)
    values($1,$2,$3,repeat('d',64),'fixture',1,'1','check snapshot','', $4::jsonb,'{}',true,$5::jsonb)`,
    [
      version,
      t,
      skill,
      { tools: [], software: [], runtime: "none" },
      { disableModelInvocation: true, requiresWorkspaceFiles: false },
    ],
  );
  await query(
    "insert into public.skill_installations(tenant_id,package_id,version_id,scope_kind,scope_id,enabled) values($1,$2,$3,'project',$4,true)",
    [t, skill, version, project.projectId],
  );
  const schedules = new SessionScheduleService({ database: db });
  const id = randomUUID();
  await schedules.create({ tenantId: t, sessionId: s }, id, {
    name: "Check once",
    prompt: fake.prompt("scheduled_refs", { firstByteMs: 4000 }),
    timing: { kind: "after", seconds: 1 },
  });
  const turn = await until(
    "scheduled input received an independent lease",
    async () =>
      (
        await query(
          "select r.id from public.session_schedules p join pma_segments.turns r on r.id=p.last_run_id and r.tenant_id=p.tenant_id where p.id=$1 and r.state='running'",
          [id],
        )
      )[0]?.id,
  );
  const [lease] = await query("select lease_id,fencing_token from pma_segments.turns where id=$1", [
    turn,
  ]);
  const execution = createExecutionReference(lease.lease_id, turn, Number(lease.fencing_token));
  const binding = await targets.prepare(execution, selected.target);
  assert.equal(await targets.prepare(execution, selected.target), binding);
  assert(published.includes(binding));
  assert.equal((await targets.read(binding)).session_id, s);
  await assert.rejects(targets.bindSession(t, s, null, selected.revision));
  await query(
    "update public.skill_installations set enabled=false where tenant_id=$1 and package_id=$2",
    [t, skill],
  );
  assert.deepEqual(
    (
      await query(
        "select versions from public.run_skill_catalogs where tenant_id=$1 and run_id=$2",
        [t, turn],
      )
    )[0].versions,
    [version],
  );
  const [mail] = await query(
    "select id,starts_turn from pma_segments.user_or_agent_messages where turn_id=$1",
    [turn],
  );
  assert(mail.starts_turn);
  const response = await fetch(new URL("/v1/conversations/" + s + "/schedules/" + id, apiUrl), {
    headers: { Authorization: "Bearer " + identity.credential.token },
  });
  assert.equal(response.status, 200);
  assert.equal((await response.json()).lastRun.turnId, turn);
  assert.equal(
    (
      await query(
        "select count(*)::int as n from public.run_skill_catalogs where tenant_id=$1 and run_id=$2",
        [t, turn],
      )
    )[0].n,
    1,
  );
  const upload = randomUUID(),
    object = randomUUID(),
    digest = createHash("sha256").update("fixture").digest("hex");
  await query(
    "insert into public.session_attachment_objects(tenant_id,id,state,sha256,byte_length,mime_type,write_completed_at) values($1,$2,'ready',$3,7,'text/plain',now())",
    [t, object, digest],
  );
  await query(
    `insert into public.session_uploads(id,tenant_id,session_id,pi_session_id,workspace_id,actor_user_id,working_directory,
 name,path,mime_type,byte_length,sha256,state,claim_token,lease_expires_at,expires_at,attachment_id,source_reference)
 select $1,p.tenant_id,p.id,p.pi_session_id,p.workspace_id,p.created_by_user_id,p.working_directory,'note.txt',p.working_directory||'/note.txt','text/plain',7,$3,
 'ready',$1,now(),now()+interval '1 day',$4,$5::jsonb from public.sessions p where p.id=$2`,
    [
      upload,
      s,
      digest,
      object,
      { id: object, sha256: digest, byteLength: 7, mimeType: "text/plain" },
    ],
  );
  const key = "turn:" + randomUUID(),
    prompt = fake.prompt("attached_refs");
  const submit = () => api.acceptTurn(s, prompt, key, undefined, [upload], "follow_up");
  const accepted = await submit();
  assert.equal(accepted.kind, "input");
  assert.deepEqual(
    await query(
      "select input_id from public.session_upload_pins where tenant_id=$1 and session_id=$2 and upload_id=$3",
      [t, s, upload],
    ),
    [{ input_id: accepted.inputId }],
  );
  assert.equal((await submit()).inputId, accepted.inputId);
  await assert.rejects(pool.query("delete from public.session_uploads where id=$1", [upload]), {
    code: "23503",
  });
  const second = await api.acceptTurn(
    s,
    prompt,
    "turn:" + randomUUID(),
    undefined,
    [upload],
    "follow_up",
  );
  const queue = await api.getInputQueue(s);
  await api.mutateQueuedInput(
    s,
    second.inputId,
    { action: "cancel", revision: queue.revision },
    "cancel:" + randomUUID(),
  );
  assert.deepEqual(
    (await api.getReturnedInputs(s, undefined, { requestId: second.inputId })).items[0].uploads.map(
      (f) => f.id,
    ),
    [upload],
  );
  assert.deepEqual(
    await query("select input_id from public.session_upload_pins where input_id=$1", [
      second.inputId,
    ]),
    [{ input_id: second.inputId }],
  );
  await api.acknowledgeReturnedInputs(s, second.inputId);
  assert.deepEqual(
    await query("select input_id from public.session_upload_pins where input_id=$1", [
      second.inputId,
    ]),
    [],
  );
  assert.deepEqual(
    await query("select input_id from public.session_upload_pins where input_id=$1", [
      accepted.inputId,
    ]),
    [{ input_id: accepted.inputId }],
  );
  await until(
    "attachment transferred from mail to retained history",
    async () =>
      (
        await query(
          `select from pma_segments.user_or_agent_messages m
  join pma_segments.messages e on e.tenant_id=m.tenant_id and e.run_id=m.turn_id and e.source_input_id=m.id::text
  join public.session_uploads u on u.id=$1 and u.accepted_at is not null
  join pma_segments.flow_input_assets a on a.run_id=m.turn_id and a.request_key=m.id::text and a.attachment_id=$3
  where m.id=$2 and m.state='placed' and not exists(select from public.session_upload_pins p where p.upload_id=u.id)`,
          [upload, accepted.inputId, object],
        )
      ).length,
  );
  await until(
    "original execution binding closed",
    async () => (await targets.read(binding))?.closed,
  );
  await assert.rejects(targets.prepare(execution, selected.target));
  await targets.cleanup();
  assert.equal(await targets.read(binding), undefined);
  assert.deepEqual(await query("select id from public.runs limit 1"), []);
  console.log(
    "Scheduled Turn, scoped Skill selection, mailbox attachment adoption and legacy writer exclusion verified.",
  );
}

export async function checkScheduledSteer({
  api,
  project,
  pool,
  db,
  fake,
  until,
  identity,
  tool,
  verifyStopScope,
  pauseApi,
  resumeApi,
}) {
  const { sessionId } = await api.createSession(
    project.projectId,
    project.workspaceId,
    "Scheduled mail and steer",
    "elastic",
  );
  const initial = await api.acceptTurn(
    sessionId,
    fake.prompt("schedule_race", {
      kind: "mcp",
      toolName: tool,
      rounds: 1,
      firstByteMs: 4200,
    }),
    "turn:" + randomUUID(),
  );
  const turn = await until(
    "initial Turn is executing",
    async () =>
      (
        await pool.query(
          "select t.id from pma_segments.sessions s join pma_segments.turns t on t.session_id=s.flow_id where s.id=$1 and t.state='running'",
          [sessionId],
        )
      ).rows[0]?.id,
  );
  if (initial.kind === "run") assert.equal(initial.turnId, turn);
  const schedules = new SessionScheduleService({ database: db });
  const schedule = randomUUID();
  await schedules.create({ tenantId: identity.tenantId, sessionId }, schedule, {
    name: "During active Turn",
    prompt: fake.prompt("schedule_later"),
    timing: { kind: "after", seconds: 1 },
  });
  await until(
    "schedule fired while the first Turn was running",
    async () =>
      (
        await pool.query(
          "select 1 from public.session_schedules p join pma_segments.turns t on t.id=$2 where p.id=$1 and p.next_fire_at is null and t.state='running'",
          [schedule, turn],
        )
      ).rowCount,
  );
  if (verifyStopScope) {
    await assert.rejects(
      api.cancelTurn(sessionId, turn, "cancel:" + randomUUID(), { scheduleId: schedule }),
      (error) => error.status === 409,
    );
  }
  const input = await api.acceptTurn(
    sessionId,
    fake.prompt("steer_original"),
    "turn:" + randomUUID(),
    undefined,
    [],
    "steer",
  );
  assert.equal(input.kind, "input");
  await until(
    "Pi adopted the steer",
    async () =>
      (
        await pool.query(
          "select 1 from pma_segments.user_or_agent_messages where session_id=$1 and id=$2 and state='placed'",
          [sessionId, input.inputId],
        )
      ).rowCount,
    30000,
  );
  const placed = await pool.query(
    "select run_id from pma_segments.messages where tenant_id=$1 and source_input_id=$2",
    [identity.tenantId, input.inputId],
  );
  assert.deepEqual(
    placed.rows,
    [{ run_id: turn }],
    "The running Pi must adopt the steer in the original Turn",
  );
  console.log(
    "Scheduled occurrence followed by human steer: canonical input belongs to original Turn.",
  );
  if (verifyStopScope) {
    const ownedSession = (
      await api.createSession(
        project.projectId,
        project.workspaceId,
        "Stop scheduled opener",
        "elastic",
      )
    ).sessionId;
    const ownedSchedule = randomUUID(),
      scope = { tenantId: identity.tenantId, sessionId: ownedSession };
    const plan = await schedules.create(scope, ownedSchedule, {
      name: "Only this opener",
      prompt: fake.prompt("stop_schedule", { firstByteMs: 30000 }),
      timing: { kind: "after", seconds: 1 },
    });
    pauseApi();
    try {
      await until(
        "schedule fires with the public API suspended",
        async () =>
          (
            await pool.query(
              "select 1 from public.session_schedules where id=$1 and next_fire_at is null",
              [ownedSchedule],
            )
          ).rowCount,
        15000,
      );
    } finally {
      resumeApi();
    }
    const running = await until(
      "scheduled opener running",
      async () => {
        const value = await api.getSchedule(ownedSession, ownedSchedule);
        return value.lastRun?.state === "running" && value.lastRun;
      },
      30000,
    );
    const deleted = await api.deleteSchedule(ownedSession, ownedSchedule, plan.revision);
    assert.equal(deleted.activeRun?.turnId, running.turnId);
    const key = "cancel:" + randomUUID();
    const receipt = await api.cancelTurn(ownedSession, running.turnId, key, {
      scheduleId: ownedSchedule,
    });
    assert.equal(receipt.turnId, running.turnId);
    assert(
      (await api.cancelTurn(ownedSession, running.turnId, key, { scheduleId: ownedSchedule }))
        .replayed,
    );
    await until(
      "post-deletion scoped Stop settled",
      async () =>
        (
          await pool.query(
            "select from pma_segments.turns where id=$1 and state='cancelled' and output_sealed_at is not null",
            [running.turnId],
          )
        ).rowCount,
      30000,
    );
    console.log(
      "Schedule Stop rejected an unrelated Turn and cancelled its own opener after deletion.",
    );
  }
}
