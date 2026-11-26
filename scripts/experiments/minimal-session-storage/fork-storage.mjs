import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { Kysely, PostgresDialect, sql } from "kysely";
import { prepareNamedSubagentArguments, subagentName } from "@pi-cloud/protocol";
import { TOOL_REPLY_TOPIC_PATTERN } from "@pi-cloud/event-log";
import { initializeRoots } from "./session-defaults.mjs";
import { childWorkspaceGrant } from "@pi-cloud/subagent-control/delegated-workspace-grant";
import { restoreForkPrefix } from "./session-prefix.mjs";
import { projectionTemplates } from "./projection-statements.mjs";

export async function installForkStudy(pool, token) {
  assert.match(token, /^[0-9a-f]{16}$/);
  const {
    rows: [db],
  } = await pool.query(
    "select current_database() as db,current_user as role,current_setting('synchronous_commit') as sync,current_setting('fsync') as fsync",
  );
  assert.deepEqual(db, { db: "pma_sessbench_" + token, role: "bench", sync: "on", fsync: "on" });
  await installForkTables(pool);
}

export async function installForkTables(pool) {
  for (const file of ["flow-segments.sql", "fork-layout.sql", "flow-transcript-points.sql"])
    await pool.query(await readFile(new URL(file, import.meta.url), "utf8"));
  const paging = await readFile(new URL("flow-page-turns.sql", import.meta.url), "utf8");
  for (const order of ["asc", "desc"]) await pool.query(paging.replaceAll("PAGE_ORDER", order));
}

// Installed only in a matched database with a real object inventory.
export async function installForkAttachmentChecks(db) {
  await db.query(await readFile(new URL("fork-input-assets.sql", import.meta.url), "utf8"));
  await db.query(`create function validate_message_attachments() returns trigger language plpgsql set search_path from current as $$
    declare expected int; matched int;
    begin
      select count(*) into expected from jsonb_object_keys(new.attachment_refs);
      select count(*) into matched from (select o.id from jsonb_each(new.attachment_refs) d
        join public.session_attachment_objects o on o.tenant_id=new.tenant_id and o.id=d.key::uuid
        where o.state='ready' and o.sha256=d.value->>'sha256' and o.byte_length=(d.value->>'byteLength')::bigint
          and o.mime_type=d.value->>'mimeType' order by o.id for share of o) q;
      if matched<>expected then raise exception 'Unretained or mismatched attachment'; end if;
      return new;
    end $$;
    create trigger validate_message_attachments before insert on messages for each row
      when(new.attachment_refs is not null) execute function validate_message_attachments()`);
}

// Switch only on drained, empty study tables. Kafka payloads and all calls to
// project_ready (including the finish operation) are identical across layouts.
export async function setForkLayout(pool, enabled, cover = true) {
  const c = await pool.connect();
  try {
    await c.query("begin");
    await c.query(
      "truncate flow_input_assets,conversation_worlds,messages,inboxes,runs,fork_requests,sessions,flow_segments,fork_gc,flows",
    );
    await c.query(
      "alter table messages drop constraint if exists messages_run_id_fkey, drop constraint if exists retained_message_stream",
    );
    await c.query("alter table runs drop constraint if exists admitting_session");
    await c.query("drop index if exists fork_message_coordinates");
    await c.query(
      enabled
        ? `alter table messages add constraint retained_message_stream foreign key(tenant_id,pi_session_id) references flows(tenant_id,flow_id);
      alter table runs add constraint admitting_session foreign key(tenant_id,session_id) references flows(tenant_id,flow_id)`
        : "alter table messages add constraint messages_run_id_fkey foreign key(run_id) references runs(id)",
    );
    await c.query("drop trigger if exists new_flow_run on runs");
    if (enabled)
      await c.query(
        "create trigger new_flow_run before insert on runs for each row execute function guard_new_flow_run()",
      );
    if (enabled && cover)
      await c.query(
        "create index fork_message_coordinates on messages(tenant_id,pi_session_id,conversation_id,entry_id) include(commit_seq,ordinal,role,run_id)",
      );
    const body = projectionTemplates.finishAndRelease;
    await c.query(
      `create or replace function prefix_finish(uuid,uuid,uuid,uuid,text) returns setof uuid language sql volatile as $fn$ ${body} $fn$`,
    );
    await c.query("commit");
  } catch (e) {
    await c.query("rollback");
    throw e;
  } finally {
    c.release();
  }
}

export class ChildNameConflict extends Error {
  constructor(id) {
    super("Existing child has different creation arguments");
    this.childId = id;
  }
}
export class ForkStorage {
  constructor(pool) {
    this.db = new Kysely({ dialect: new PostgresDialect({ pool }) });
    this.pool = pool;
  }
  async register(tenant, ids, templateFlow = null, settings) {
    await this.db.transaction().execute(async (tx) => {
      await this.lock(tx, tenant);
      await sql`insert into flows select ${tenant}::uuid,x from unnest(${ids}::uuid[]) x`.execute(
        tx,
      );
      await sql`insert into sessions(tenant_id,id,flow_id,family_id,agent_name)
        select ${tenant}::uuid,x,x,x,'root' from unnest(${ids}::uuid[]) x`.execute(tx);
      if (this.inputs) await initializeRoots(tx, tenant, ids, settings);
      if (templateFlow) {
        await sql`update flows f set native_id_floor=b.native_id_floor,agent_base_stream=b.agent_base_stream,
          agent_base_seq=b.agent_base_seq,agent_base_ordinal=b.agent_base_ordinal from flows b where f.tenant_id=${tenant}::uuid and
          f.flow_id=any(${ids}::uuid[]) and b.tenant_id=f.tenant_id and b.flow_id=${templateFlow}::uuid`.execute(
          tx,
        );
        await sql`insert into flow_segments
          select g.tenant_id,x,g.segment_no,g.source_flow,g.conversation_id,g.first_entry,g.last_entry,g.through_seq,g.through_position,g.tail_coverage
          from flow_segments g cross join unnest(${ids}::uuid[]) x
          where g.tenant_id=${tenant}::uuid and g.destination_flow=${templateFlow}::uuid`.execute(
          tx,
        );
      }
    });
  }
  async session(tx, t, id) {
    return (
      await sql`select * from sessions where tenant_id=${t}::uuid and id=${id}::uuid for share`.execute(
        tx,
      )
    ).rows[0];
  }
  async freeze(tx, t, s, dest, point, requestCut = false) {
    const kind = requestCut ? "request" : "entry";
    const {
      rows: [row],
    } =
      await sql`select pin_message_fork(${t}::uuid,${s.flow_id}::uuid,${dest}::uuid,${point.run ?? null}::uuid,
      ${point.lane}::bigint,${point.entry}::bigint,${kind},${point.through ?? null}::bigint) as pinned`.execute(
        tx,
      );
    assert(
      row.pinned,
      requestCut ? "Missing visible request cutoff" : "Missing completed assistant anchor",
    );
  }
  async copyWorld(tx, t, from, lane, to) {
    await sql`insert into conversation_worlds(tenant_id,pi_session_id,conversation_id,state)
      select tenant_id,${to}::uuid,1,state from conversation_worlds
      where tenant_id=${t}::uuid and pi_session_id=${from}::uuid and conversation_id=${lane}`.execute(
      tx,
    );
  }
  async rootWorld(t, session) {
    const { rows } =
      await sql`select w.state from sessions s join conversation_worlds w on w.tenant_id=s.tenant_id and w.pi_session_id=s.flow_id and w.conversation_id=1 where s.tenant_id=${t}::uuid and s.id=${session}::uuid`.execute(
        this.db,
      );
    return rows[0]?.state;
  }
  async lock(tx, t) {
    await sql`select set_config('synchronous_commit','on',true), pg_advisory_xact_lock_shared(hashtextextended(${t},17))`.execute(
      tx,
    );
  }
  async fork(t, source, point, key = randomUUID()) {
    return this.createBranch(t, source, point, key, null);
  }
  async delegate(t, source, point, key, name, task, model, caller, workspace, returnRoute) {
    const normalized = subagentName(name);
    assert(
      normalized !== "root" && task.trim() && model && caller?.run && caller?.writer,
      "Invalid named delegation",
    );
    const requested =
      workspace === undefined
        ? undefined
        : workspace === null
          ? null
          : {
              compute: workspace.compute ?? "shared",
              ...(workspace.cwd ? { cwd: workspace.cwd } : {}),
              ...(workspace.tools ? { tools: workspace.tools.toSorted() } : {}),
            };
    return this.createBranch(t, source, point, key, {
      name: normalized,
      task,
      model,
      caller,
      workspace: requested,
      returnRoute,
    });
  }
  // Input has already passed the native call/checkpoint matching in
  // nativeDelegationIntent. Only the candidate storage consumes this adapter.
  async delegateFromIntent(t, source, caller, intent, model, delivery) {
    if (
      delivery &&
      (!TOOL_REPLY_TOPIC_PATTERN.test(delivery.topic) ||
        !delivery.operationId ||
        !Number.isSafeInteger(intent.taskId))
    )
      throw Error("Invalid subagent completion route");
    assert(intent.toolName === "subagent" && intent.nativeCallId && intent.operationId);
    const args = prepareNamedSubagentArguments(intent.arguments);
    const branch = args.context === "branch";
    if (branch)
      assert(
        Number.isSafeInteger(intent.contextAnchor) &&
          intent.contextAnchor > 0 &&
          Number.isSafeInteger(intent.contextThrough) &&
          intent.contextThrough > 0 &&
          intent.contextThrough <= intent.sequence,
        "Missing original model request position",
      );
    return this.delegate(
      t,
      source,
      branch
        ? {
            lane: intent.conversationId,
            entry: intent.contextAnchor,
            through: intent.contextThrough,
          }
        : null,
      delivery?.operationId ?? intent.operationId,
      args.name,
      args.task,
      model,
      caller,
      args.workspace,
      delivery
        ? {
            topic: delivery.topic,
            task: intent.taskId,
            call: intent.nativeCallId,
            lane: intent.conversationId,
            source: intent.operationId,
          }
        : undefined,
    );
  }
  async createBranch(t, source, point, key, child) {
    return this.db.transaction().execute(async (tx) => {
      const target = await this.registerBranch(tx, t, source, point, key, child);
      const route = child?.returnRoute;
      if (route) {
        const {
          rows: [receipt],
        } =
          await sql`select id from runs where tenant_id=${t}::uuid and id=${child.caller.run}::uuid and session_id=(select flow_id from sessions where tenant_id=${t}::uuid and id=${source}::uuid)
          and lease_id=${child.caller.writer}::uuid and state='running' and output_seal_id is null for share`.execute(
            tx,
          );
        assert(receipt, "Calling Turn is no longer active");
        const fingerprint = createHash("sha256")
          .update(
            JSON.stringify([
              t,
              source,
              child.caller.run,
              child.caller.writer,
              target,
              route.source,
              route.topic,
              route.call,
              route.task,
              route.lane,
            ]),
          )
          .digest("hex");
        const ack =
          await sql`insert into subagent_deliveries(operation_id,root_id,writer,caller_task,caller_lane,operation_hash,destination,call_id,tool,child_id,kind,value,due_at)
          values(${key}::uuid,${child.caller.run}::uuid,${child.caller.writer}::uuid,${route.task},${route.lane},${fingerprint},${route.topic},${route.call},'subagent',null,'fixed',
            jsonb_build_object('text',jsonb_build_object('name',${child.name}::text,'sessionId',${target}::uuid,'status','created')::text),clock_timestamp())
          on conflict(operation_id) do update set operation_hash=excluded.operation_hash where subagent_deliveries.operation_hash=excluded.operation_hash returning operation_id`.execute(
            tx,
          );
        assert.equal(ack.rows.length, 1, "Original child Turn or delivery identity changed");
        await sql`select pg_notify('pma_named_children','')`.execute(tx);
      }
      return target;
    });
  }
  async registerBranch(tx, t, source, point, key, child) {
    await this.lock(tx, t);
    await sql`select pg_advisory_xact_lock(hashtextextended(${t + source + key},23))`.execute(tx);
    const fingerprint = createHash("sha256")
      .update(
        JSON.stringify([
          point?.run,
          point?.lane,
          point?.entry,
          point?.through,
          child?.name,
          child?.task,
          child?.model,
          child?.caller.run,
          child?.workspace === undefined ? "inherit" : child.workspace,
        ]),
      )
      .digest("hex");
    const {
      rows: [old],
    } = await sql`select target_id, fingerprint=${fingerprint} as matches
        from fork_requests where tenant_id=${t}::uuid and source_id=${source}::uuid and request_key=${key}`.execute(
      tx,
    );
    if (old) {
      assert(old.matches, "Fork request changed");
      assert(await this.session(tx, t, old.target_id), "Fork destination was deleted");
      return old.target_id;
    }
    const parent = await this.session(tx, t, source);
    assert(parent, "Unknown Fork source");
    let creation = null;
    let inheritedGrant = null,
      childModel;
    if (child) {
      const caller =
        await sql`select id,workspace_grant${this.inputs ? sql`,model_snapshot` : sql``} from ${this.inputs ? sql`turns` : sql`runs`} where tenant_id=${t}::uuid and id=${child.caller.run}::uuid and session_id=${parent.flow_id}::uuid
          and lease_id=${child.caller.writer}::uuid and state='running' and output_seal_id is null for share`.execute(
          tx,
        );
      assert(caller.rows.length, "Calling Turn is no longer active");
      inheritedGrant = caller.rows[0].workspace_grant;
      childModel = caller.rows[0].model_snapshot;
      creation = createHash("sha256")
        .update(
          JSON.stringify([
            child.name,
            child.task,
            child.model,
            point ? "branch" : "fresh",
            child.workspace === undefined ? "inherit" : child.workspace,
          ]),
        )
        .digest("hex");
      await sql`select pg_advisory_xact_lock(hashtextextended(${t + source + child.name},37))`.execute(
        tx,
      );
      const {
        rows: [named],
      } =
        await sql`select id,creation_hash from sessions where tenant_id=${t}::uuid and parent_session_id=${source}::uuid and agent_name=${child.name}`.execute(
          tx,
        );
      if (named) {
        if (named.creation_hash !== creation) throw new ChildNameConflict(named.id);
        await sql`insert into fork_requests values(${t}::uuid,${source}::uuid,${key},${named.id}::uuid,${fingerprint})`.execute(
          tx,
        );
        return named.id;
      }
    }
    const target = randomUUID();
    const grant = child ? childWorkspaceGrant(inheritedGrant, child.workspace, target) : null;
    if (point) await this.freeze(tx, t, parent, target, point, !!child);
    else {
      assert(child, "Missing Fork anchor");
      await sql`insert into flows(tenant_id,flow_id) values(${t}::uuid,${target}::uuid)`.execute(
        tx,
      );
    }
    if (
      !child ||
      (grant &&
        grant.computeSessionId === inheritedGrant?.computeSessionId &&
        grant.cwd === inheritedGrant.cwd)
    )
      await this.copyWorld(tx, t, parent.flow_id, point?.lane ?? 1, target);
    await sql`insert into sessions(tenant_id,id,flow_id,family_id,parent_session_id,agent_name,creation_hash)
        values(${t}::uuid,${target}::uuid,${target}::uuid,${child ? parent.family_id : target}::uuid,
          ${child ? source : null}::uuid,${child?.name ?? "root"},${creation})`.execute(tx);
    if (this.inputs) {
      const logical =
        child && grant
          ? {
              ...grant,
              executionMode:
                grant.compute === "ephemeral"
                  ? "elastic"
                  : (inheritedGrant.executionMode ?? "elastic"),
              developmentEnvironmentId:
                grant.compute === "ephemeral"
                  ? null
                  : (inheritedGrant.developmentEnvironmentId ?? null),
            }
          : null;
      const workspace = child ? logical : parent.desired_workspace;
      const model = child ? childModel : parent.desired_model;
      await sql`update sessions set desired_model=${JSON.stringify(model)}::jsonb,desired_workspace=${workspace && JSON.stringify(workspace)}::jsonb,
        owner_user_id=${parent.owner_user_id}::uuid where tenant_id=${t}::uuid and id=${target}::uuid`.execute(
        tx,
      );
    }
    if (child) {
      const initialTurn = randomUUID();
      if (this.inputs) {
        await sql`insert into turns(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,state)
          values(${initialTurn}::uuid,${t}::uuid,${target}::uuid,1,${key}::uuid,${child.task},'queued')`.execute(
          tx,
        );
      } else
        await sql`insert into runs(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,model_ref,state,workspace_grant)
          values(${initialTurn}::uuid,${t}::uuid,${target}::uuid,1,${key}::uuid,${child.task},${child.model},'queued',${grant ? JSON.stringify(grant) : null}::jsonb)`.execute(
          tx,
        );
      if (this.inputs)
        await this.inputs.admitIn(tx, {
          tenant: t,
          session: target,
          id: key,
          sender: source,
          source: "agent",
          mode: point ? "steer" : "input",
          text: child.task,
          initialTurn,
        });
    }
    await sql`insert into fork_requests values(${t}::uuid,${source}::uuid,${key},${target}::uuid,${fingerprint})`.execute(
      tx,
    );
    return target;
  }
  async agentConfiguration(t, id) {
    const {
      rows: [r],
    } = await sql`select a.content from sessions s left join lateral
      fork_agent_at(s.tenant_id,s.flow_id,1,9223372036854775807) a on true
      where s.tenant_id=${t}::uuid and s.id=${id}::uuid`.execute(this.db);
    assert(r, "Unknown Fork session");
    return r.content === null ? null : JSON.parse(r.content);
  }
  // Keyset pagination traverses only the segments needed for this page.
  async readPage(tx, t, s, limit, cursor, floor = 1) {
    assert(Number.isSafeInteger(limit) && limit > 0, "Invalid page size");
    if (cursor) assert(cursor.flow === s.flow_id, "History cursor is obsolete");
    const local = 2147483647;
    const rows = [];
    let segment = cursor?.segment ?? local;
    while (segment > 0 && rows.length < limit) {
      const range =
        segment === local
          ? {
              source_flow: s.flow_id,
              conversation_id: 1,
              first_entry: 1,
              last_entry: Number.MAX_SAFE_INTEGER,
              through_seq: Number.MAX_SAFE_INTEGER,
              segment_no: local,
            }
          : (
              await sql`select * from flow_segments where tenant_id=${t}::uuid and destination_flow=${s.flow_id}::uuid
             and segment_no<=${segment} order by segment_no desc limit 1`.execute(tx)
            ).rows[0];
      if (!range) break;
      const before = cursor?.segment === range.segment_no ? cursor.entry : null;
      const page = await sql`select entry_id,commit_seq,ordinal,run_id,content from message_entries
        where tenant_id=${t}::uuid and pi_session_id=${range.source_flow}::uuid and conversation_id=${range.conversation_id}
        and entry_id between ${Math.max(floor, Number(range.first_entry))} and ${range.last_entry}
        and commit_seq<=${range.through_seq} ${before == null ? sql`` : sql`and entry_id<${before}`}
        order by entry_id desc limit ${limit - rows.length}`.execute(tx);
      for (const r of page.rows)
        rows.push({
          ...r,
          cursor: { flow: s.flow_id, segment: range.segment_no, entry: r.entry_id },
        });
      if (segment === local) segment = local - 1;
      else segment = range.segment_no - 1;
    }
    return rows;
  }
  async pageWithCursor(t, id, limit = 30, cursor = null) {
    return this.db
      .transaction()
      .setIsolationLevel("repeatable read")
      .execute(async (tx) => {
        const s = await this.session(tx, t, id);
        assert(s, "Unknown Fork session");
        return this.readPage(tx, t, s, limit, cursor);
      });
  }
  async page(t, id, limit = 30) {
    const rows = await this.pageWithCursor(t, id, limit);
    return rows.map(({ cursor, ...row }) => row);
  }
  async coldRecords(tx, t, s, inheritedOnly = false) {
    const records = [];
    let cursor = inheritedOnly ? { flow: s.flow_id, segment: 2147483646 } : null,
      floor = 1,
      head = false;
    for (;;) {
      const rows = await this.readPage(tx, t, s, 64, cursor, floor);
      for (const row of rows) {
        const entry = JSON.parse(row.content);
        if (entry.id < floor) continue;
        if (!head && entry.head !== undefined) {
          floor = entry.head;
          head = true;
        }
        const { byTaskId, ...value } = entry;
        records.push({ entry: { ...value, conversationId: 1 }, seq: Number(row.commit_seq) });
      }
      if (rows.length < 64 || Number(rows.at(-1).entry_id) <= floor) break;
      cursor = rows.at(-1).cursor;
    }
    return records.reverse();
  }
  async contextPrefix(t, id, inheritedOnly = false) {
    return this.db
      .transaction()
      .setIsolationLevel("repeatable read")
      .execute(async (tx) => {
        const s = await this.session(tx, t, id);
        assert(s, "Unknown Fork session");
        return this.coldRecords(tx, t, s, inheritedOnly);
      });
  }
  async hydrate(t, id, store) {
    return this.db
      .transaction()
      .setIsolationLevel("repeatable read")
      .execute(async (tx) => {
        const s = await this.session(tx, t, id);
        assert(s, "Unknown Fork session");
        const records = await this.coldRecords(tx, t, s);
        if (!records.length) return 0;
        const {
          rows: [base],
        } = await sql`select f.native_id_floor,a.content,w.state from flows f
        left join lateral fork_agent_at(f.tenant_id,f.flow_id,1,9223372036854775807) a on true
        left join conversation_worlds w on w.tenant_id=f.tenant_id and w.pi_session_id=f.flow_id and w.conversation_id=1
        where f.tenant_id=${t}::uuid and f.flow_id=${s.flow_id}::uuid`.execute(tx);
        restoreForkPrefix(
          store,
          records,
          base.content == null ? null : JSON.parse(base.content),
          base.state,
          Number(base.native_id_floor),
        );
        return records.length;
      });
  }
  async retire(t, id, point) {
    const fresh = point ? randomUUID() : null;
    await this.db.transaction().execute(async (tx) => {
      await sql`set local synchronous_commit=on`.execute(tx);
      await sql`select * from detach_message_window(${t}::uuid,${id}::uuid,${fresh}::uuid,${point?.run ?? null}::uuid,
        ${point?.lane ?? 1}::bigint,${point?.entry ?? null}::bigint,'entry')`.execute(tx);
    });
    return fresh ?? undefined;
  }
  async collect(t) {
    for (;;) {
      const {
        rows: [progress],
      } = await sql`select collect_flow_batch(${t}::uuid) as processed`.execute(this.db);
      if (!progress.processed) return;
    }
  }
}
