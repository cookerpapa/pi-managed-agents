import { SegmentMailReader } from "./segment-mail-reader.mjs";
import { createHash, randomUUID } from "node:crypto";
import { sql } from "kysely";
import { SessionDefaults } from "./session-defaults.mjs";
import { assertUserInputBudget, parseUserUploads } from "@pi-cloud/protocol";

const fingerprint = (value) => createHash("sha256").update(JSON.stringify(value)).digest("hex");
export class InputConflict extends Error {}
const terminal = new Set(["placed", "withdrawn"]);

export class SegmentInputs extends SegmentMailReader {
  constructor(db) {
    super(db);
    this.settings = new SessionDefaults(db);
  }
  async admit(input) {
    return this.db.transaction().execute((tx) => this.admitIn(tx, input));
  }
  async admitIn(
    tx,
    {
      tenant,
      session,
      id,
      sender,
      source,
      mode,
      text,
      uploads = [],
      requestKey = id,
      strict = false,
      initialTurn,
    },
  ) {
    if (!["user", "agent"].includes(source) || !["input", "steer", "follow_up"].includes(mode))
      throw new InputConflict("Invalid input kind");
    uploads = parseUserUploads(uploads);
    assertUserInputBudget(text, uploads);
    if (uploads.some((file) => file.sessionId !== session))
      throw new InputConflict("Attachment belongs to a different Session");
    if (!text.trim() && !uploads.length) throw new InputConflict("Empty input");
    await sql`set local synchronous_commit=on`.execute(tx);
    const {
      rows: [s],
    } =
      await sql`select flow_id,agent_mail_paused from sessions where tenant_id=${tenant}::uuid and id=${session}::uuid for no key update`.execute(
        tx,
      );
    if (!s) throw new InputConflict("Unknown Session");
    // The API accepts immutable, ordered ticket IDs. JSONB may reorder descriptor keys on retry.
    const hash = fingerprint([sender, source, mode, text, uploads.map((file) => file.id), strict]);
    const {
      rows: [old],
    } =
      await sql`select * from user_or_agent_messages where tenant_id=${tenant}::uuid and session_id=${session}::uuid and id=${id}::uuid`.execute(
        tx,
      );
    if (old) {
      if (old.fingerprint !== hash) throw new InputConflict("Input identity changed");
      return { ...old, replayed: true };
    }
    // Only the active writer can take a steer. A later queued Turn may
    // already exist but cannot receive input on that writer's tool boundary.
    const {
      rows: [current],
    } = await sql`select id,state from turns
      where tenant_id=${tenant}::uuid and session_id=${s.flow_id}::uuid
        and state not in ('completed','failed','timed_out','cancelled')
      order by (state='cancel_requested'),mailbox_position limit 1`.execute(tx);
    const busy = current !== undefined;
    // A new message after an explicit Stop waits behind that closing Turn.
    const held = source === "agent" && s.agent_mail_paused && !initialTurn;
    const open = busy && current.state !== "cancel_requested" && !s.agent_mail_paused;
    if (strict && busy && !s.agent_mail_paused && !initialTurn)
      throw new InputConflict("Session is busy");
    if (open && mode === "input" && !initialTurn) throw new InputConflict("A Turn is already open");
    if (
      source === "user" &&
      (
        await sql`select 1 from user_or_agent_messages where tenant_id=${tenant}::uuid and session_id=${session}::uuid and sender_id=${sender}::uuid and source='user' and state in ('pending','sending') offset 63 limit 1`.execute(
          tx,
        )
      ).rows.length
    )
      throw new InputConflict("Too many pending user messages");
    const turn = held ? null : (initialTurn ?? (open ? current.id : randomUUID()));
    if (!held && !initialTurn && !open) {
      await sql`insert into turns(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,input_uploads,state)
        select ${turn}::uuid,${tenant}::uuid,${s.flow_id}::uuid,coalesce(max(mailbox_position),0)+1,
          ${id}::uuid,${text},${JSON.stringify(uploads)}::jsonb,'queued'
        from turns where tenant_id=${tenant}::uuid and session_id=${s.flow_id}::uuid`.execute(tx);
      if (source === "user" && s.agent_mail_paused) {
        await sql`update sessions set agent_mail_paused=false where tenant_id=${tenant}::uuid and id=${session}::uuid`.execute(
          tx,
        );
        await sql`update user_or_agent_messages set turn_id=${turn}::uuid,state='pending'
          where tenant_id=${tenant}::uuid and session_id=${session}::uuid and state='held'`.execute(
          tx,
        );
      }
    }
    const {
      rows: [row],
    } =
      await sql`insert into user_or_agent_messages(tenant_id,session_id,id,sender_id,source,mode,body,uploads,fingerprint,turn_id,starts_turn,request_key,state)
      values(${tenant}::uuid,${session}::uuid,${id}::uuid,${sender}::uuid,${source},${mode},${text},${JSON.stringify(uploads)}::jsonb,${hash},${turn}::uuid,${!held && (Boolean(initialTurn) || !open)},${requestKey},${held ? "held" : "pending"}) returning *`.execute(
        tx,
      );
    return row;
  }
  async stop({ tenant, session }, turn, key, gracePeriodMs, scheduleId) {
    return this.db.transaction().execute(async (tx) => {
      await sql`set local synchronous_commit=on`.execute(tx);
      const {
        rows: [window],
      } =
        await sql`select flow_id from sessions where tenant_id=${tenant}::uuid and id=${session}::uuid for no key update`.execute(
          tx,
        );
      if (!window) throw new InputConflict("Unknown Session");
      const {
        rows: [old],
      } =
        await sql`select id,stop_request from turns where tenant_id=${tenant}::uuid and session_id=${window.flow_id}::uuid and stop_request is not null and stop_request->>'key'=${key}`.execute(
          tx,
        );
      if (old) {
        if (
          old.id !== turn ||
          old.stop_request.gracePeriodMs !== gracePeriodMs ||
          old.stop_request.scheduleId !== scheduleId
        )
          throw new InputConflict("Stop identity changed");
        return { ...old.stop_request.receipt, replayed: true };
      }
      const {
        rows: [target],
      } =
        await sql`select state,started_at,stop_request,output_seal_id,scheduled_origin from turns where tenant_id=${tenant}::uuid and session_id=${window.flow_id}::uuid and id=${turn}::uuid for no key update`.execute(
          tx,
        );
      if (scheduleId && target?.scheduled_origin?.scheduleId !== scheduleId)
        throw new InputConflict("This schedule did not open the Turn");
      if (
        !target ||
        target.stop_request ||
        !["queued", "running", "settling"].includes(target.state)
      )
        throw new InputConflict("Turn is not available for Stop");
      const receipt = {
        controlRequestId: randomUUID(),
        targetRunId: turn,
        turnId: turn,
        sessionId: session,
        state: "pending",
        acceptedAt: new Date().toISOString(),
        replayed: false,
      };
      // A sealed writer cannot act on a new control. Its replacement claims
      // the existing Turn with the stop signal already set.
      const recovering =
        (target.state === "queued" && target.started_at !== null) || target.output_seal_id !== null;
      const state = recovering
        ? target.state
        : target.state === "queued"
          ? "cancelled"
          : "cancel_requested";
      await sql`update turns set stop_request=${JSON.stringify({ key, gracePeriodMs, scheduleId, receipt })}::jsonb,state=${state},stop_on_resume=stop_on_resume or ${recovering},
        settled_at=case when ${state}='cancelled' then clock_timestamp() else settled_at end where id=${turn}::uuid`.execute(
        tx,
      );
      if (recovering)
        await sql`update sessions set agent_mail_paused=true where tenant_id=${tenant}::uuid and id=${session}::uuid`.execute(
          tx,
        );
      return receipt;
    });
  }
  async sendAgent(o) {
    return this.db.transaction().execute(async (tx) => {
      await sql`set local synchronous_commit=on`.execute(tx);
      const {
        rows: [source],
      } =
        await sql`select s.* from sessions s join turns r on r.tenant_id=s.tenant_id and r.session_id=s.flow_id
        where s.tenant_id=${o.tenant}::uuid and s.id=${o.session}::uuid and r.id=${o.root}::uuid and r.state='running' and r.lease_id=${o.writer}::uuid
        and r.fencing_token=${o.epoch} and r.output_seal_id is null`.execute(tx);
      if (!source) throw new InputConflict("Sender Turn closed");
      const hash = fingerprint(o);
      await sql`select pg_advisory_xact_lock(hashtextextended(${o.operation}, 74))`.execute(tx);
      const {
        rows: [prior],
      } =
        await sql`select operation_hash from subagent_deliveries where operation_id=${o.operation}::uuid`.execute(
          tx,
        );
      if (prior) {
        if (prior.operation_hash !== hash) throw new InputConflict("Message operation changed");
        return;
      }
      const { to, priority, text } = o.input.args;
      if (
        typeof to !== "string" ||
        !["urgent", "normal"].includes(priority) ||
        typeof text !== "string"
      )
        throw new InputConflict("Invalid agent message");
      const {
        rows: [target],
      } =
        await sql`select id from sessions where tenant_id=${o.tenant}::uuid and family_id=${source.family_id}::uuid and
        (case when ${to}='parent' then id=${source.parent_session_id}::uuid when ${to} in ('main','root') then id=${source.family_id}::uuid
        else id::text=${to} or (parent_session_id=${o.session}::uuid and agent_name=${to}) end) limit 1`.execute(
          tx,
        );
      if (target)
        await this.admitIn(tx, {
          tenant: o.tenant,
          session: target.id,
          sender: o.session,
          source: "agent",
          mode: priority === "urgent" ? "steer" : "follow_up",
          id: o.operation,
          text,
        });
      const current =
        await sql`select id from turns where id=${o.root}::uuid and tenant_id=${o.tenant}::uuid and state='running' and output_seal_id is null and lease_id=${o.writer}::uuid and fencing_token=${o.epoch} for share`.execute(
          tx,
        );
      if (!current.rows.length) throw new InputConflict("Sender Turn closed");
      const ack =
        await sql`insert into subagent_deliveries(operation_id,root_id,writer,caller_task,caller_lane,operation_hash,destination,call_id,tool,kind,value,due_at)
        values(${o.operation}::uuid,${o.root}::uuid,${o.writer}::uuid,${o.task},${o.lane},${hash},${o.destination},${o.input.call},'send_message','fixed',
        ${JSON.stringify(target ? { text: "已投递。" } : { text: "目标 Agent 不存在。", isError: true })}::jsonb,clock_timestamp())
        on conflict(operation_id) do update set operation_hash=excluded.operation_hash where subagent_deliveries.operation_hash=excluded.operation_hash returning operation_id`.execute(
          tx,
        );
      if (!ack.rows.length) throw new InputConflict("Message operation changed");
      await sql`select pg_notify('pma_named_children','')`.execute(tx);
    });
  }
  async version(tx, tenant, session, sender) {
    return String(
      (
        await sql`select version from session_input_queue_versions where tenant_id=${tenant}::uuid and session_id=${session}::uuid and sender_id=${sender}::uuid`.execute(
          tx,
        )
      ).rows[0]?.version ?? 0,
    );
  }
  async queue(tenant, session, sender, tx = this.db, after = 0, limit = 64) {
    return (
      await sql`select m.id,m.mode,m.body,m.uploads,m.state,m.withdrawing,m.position,m.turn_id,m.accepted_at,m.starts_turn,
     case when m.starts_turn and r.state='queued' then r.mailbox_position end as initial_position,
     (m.steer_seen_epoch=r.fencing_token and r.state='running' and r.output_seal_id is null and l.released_at is null and l.writer_failed_at is null and l.valid_until>statement_timestamp()) as owner_notified
     from user_or_agent_messages m left join turns r on r.id=m.turn_id left join session_leases l on l.lease_id=r.lease_id where m.tenant_id=${tenant}::uuid and m.session_id=${session}::uuid and m.sender_id=${sender}::uuid and m.source='user'
     and m.state in ('pending','sending') and m.position>${after} order by m.position limit ${limit}`.execute(
        tx,
      )
    ).rows;
  }
  async withdraw(tenant, session, sender, id) {
    return this.db.transaction().execute(async (tx) => {
      await sql`set local synchronous_commit=on`.execute(tx);
      // Take the same Session lock as idle admission and finalization.
      await sql`select id from sessions where tenant_id=${tenant}::uuid and id=${session}::uuid for no key update`.execute(
        tx,
      );
      await sql`select r.id from turns r join user_or_agent_messages m on m.turn_id=r.id where m.tenant_id=${tenant}::uuid and m.session_id=${session}::uuid and m.sender_id=${sender}::uuid and m.source='user' and m.id=${id}::uuid and r.state='queued' and r.started_at is null for no key update of r`.execute(
        tx,
      );
      const {
        rows: [m],
      } =
        await sql`select * from user_or_agent_messages where tenant_id=${tenant}::uuid and session_id=${session}::uuid and source='user' and sender_id=${sender}::uuid and id=${id}::uuid for update`.execute(
          tx,
        );
      if (!m) throw new InputConflict("Unknown user input");
      if (terminal.has(m.state)) return m;
      const {
        rows: [changed],
      } =
        await sql`update user_or_agent_messages set return_requested=true,state=case when state='pending' then 'withdrawn' else state end,
        withdrawing=state<>'pending' where tenant_id=${tenant}::uuid and session_id=${session}::uuid and id=${id}::uuid returning *`.execute(
          tx,
        );
      if (!changed.withdrawing && changed.turn_id !== null) {
        if (m.starts_turn) {
          await sql`with first_pending as (
              select id from user_or_agent_messages where turn_id=${m.turn_id}::uuid and state='pending' order by position limit 1
            ), promoted as (
              update user_or_agent_messages u set starts_turn=true from first_pending p
              where u.turn_id=${m.turn_id}::uuid and u.id=p.id returning u.body,u.uploads
            ) update turns set initial_input=coalesce((select body from promoted),''),
                input_uploads=coalesce((select uploads from promoted),'[]'::jsonb)
              where id=${m.turn_id}::uuid and state='queued' and started_at is null`.execute(tx);
        }
        await sql`update turns set state=case when state='running' then 'cancel_requested' else 'cancelled' end,
          settled_at=case when state='queued' then clock_timestamp() else settled_at end where id=${m.turn_id}::uuid and state in ('queued','running') and exists(select from user_or_agent_messages where turn_id=${m.turn_id}::uuid and starts_turn) and
          not exists(select from user_or_agent_messages where turn_id=${m.turn_id}::uuid and state<>'withdrawn')`.execute(
          tx,
        );
      }
      return changed;
    });
  }
  async change({ tenant, session, sender, id, operation, action, other = null, version }) {
    if (!["up", "down", "steer"].includes(action)) throw new InputConflict("Invalid queue action");
    return this.db.transaction().execute(async (tx) => {
      await sql`set local synchronous_commit=on`.execute(tx);
      await sql`select id from sessions where tenant_id=${tenant}::uuid and id=${session}::uuid for no key update`.execute(
        tx,
      );
      const {
        rows: [old],
      } =
        await sql`select c.* from session_input_changes c join user_or_agent_messages m on (m.tenant_id,m.session_id,m.id)=(c.tenant_id,c.session_id,c.message_id)
        where c.tenant_id=${tenant}::uuid and c.session_id=${session}::uuid and c.id=${operation}::uuid and m.sender_id=${sender}::uuid and m.source='user'`.execute(
          tx,
        );
      if (old) {
        if (old.message_id !== id || old.other_id !== other || old.action !== action)
          throw new InputConflict("Change identity reused");
        return "applied";
      }
      if (version !== undefined && (await this.version(tx, tenant, session, sender)) !== version)
        throw new InputConflict("Queue changed");
      const { rows } =
        await sql`select id,state,turn_id,withdrawing,position from user_or_agent_messages where tenant_id=${tenant}::uuid and session_id=${session}::uuid and sender_id=${sender}::uuid and source='user' and state in ('pending','sending') order by position for update`.execute(
          tx,
        );
      const i = rows.findIndex((m) => m.id === id),
        neighbor = rows[i + (action === "up" ? -1 : 1)];
      if (
        i < 0 ||
        rows[i].withdrawing ||
        (action === "steer"
          ? other !== null
          : !neighbor ||
            neighbor.id !== other ||
            neighbor.withdrawing ||
            neighbor.turn_id !== rows[i].turn_id)
      )
        throw new InputConflict("Queue changed");
      if (rows[i].state !== "pending" || (other && neighbor.state !== "pending"))
        throw new InputConflict("Input is entering the conversation");
      await sql`insert into session_input_changes(tenant_id,session_id,id,message_id,other_id,action)
        values(${tenant}::uuid,${session}::uuid,${operation}::uuid,${id}::uuid,${other}::uuid,${action})`.execute(
        tx,
      );
      if (action === "steer") {
        await sql`update user_or_agent_messages set mode='steer' where tenant_id=${tenant}::uuid and session_id=${session}::uuid and id=${id}::uuid`.execute(
          tx,
        );
      } else {
        await sql`update user_or_agent_messages set position=case when id=${id}::uuid then ${neighbor.position}::bigint else ${rows[i].position}::bigint end
          where tenant_id=${tenant}::uuid and session_id=${session}::uuid and id in (${id}::uuid,${other}::uuid)`.execute(
          tx,
        );
      }
      return "applied";
    });
  }
  // Empty boundaries do one indexed read. A positive read is rechecked under
  // the same Session lock used by reordering, withdrawal and writer closure.
}
