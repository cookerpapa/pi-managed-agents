import { createHash } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import {
  ControlPlaneApiValidationError,
  parseUuidPathParameter,
  parseAcceptTurnRequest,
  parseQueuedInputAction,
  INPUT_QUEUE_PAGE_SIZE,
} from "@pi-cloud/protocol";
import { sql } from "kysely";
import { InputConflict } from "./segment-inputs.mjs";
const hash = (x) => createHash("sha256").update(JSON.stringify(x)).digest("hex");
function identity(key, actor, session) {
  if (typeof key !== "string" || !key.length || key.length > 256)
    throw new InputConflict("Missing message identity");
  const h = hash([actor, session, key]);
  return [
    h.slice(0, 8),
    h.slice(8, 12),
    "4" + h.slice(13, 16),
    "8" + h.slice(17, 20),
    h.slice(20, 32),
  ].join("-");
}
// Mount with the Session API's authenticated identity and Session-scoped upload resolver.
export function installSegmentInputApi(app, inputs, authorize) {
  const base = "/v1/sessions/:session";
  const route = (method, path, handle) =>
    app.route({
      method,
      url: base + path,
      handler: async (req, res) => {
        try {
          const session = parseUuidPathParameter(req.params.session, "sessionId");
          if (req.params.id !== undefined) parseUuidPathParameter(req.params.id, "inputId");
          const a = await authorize(req, session);
          if (!a) return res.code(403).send();
          return await handle(req, { ...a, session }, res);
        } catch (e) {
          if (e instanceof ControlPlaneApiValidationError)
            return res.code(400).send({ message: e.message });
          if (e instanceof InputConflict) return res.code(409).send({ message: e.message });
          throw e;
        }
      },
    });
  const status = async (a, id) =>
    (
      await sql`select m.*,r.mailbox_position from user_or_agent_messages m join runs r on r.id=m.turn_id
    where m.id=${id}::uuid and m.tenant_id=${a.tenant}::uuid and m.session_id=${a.session}::uuid and m.sender_id=${a.sender}::uuid and m.source='user'`.execute(
        inputs.db,
      )
    ).rows[0];
  const returns = async (a, requested, tx = inputs.db) =>
    (
      await sql`select id,body,uploads,position,request_key,return_ack from user_or_agent_messages where tenant_id=${a.tenant}::uuid and session_id=${a.session}::uuid and sender_id=${a.sender}::uuid and source='user' and state='withdrawn' and return_requested and (not return_ack or ${requested ?? null}::uuid is not null) and (${requested ?? null}::uuid is null or id=${requested ?? null}::uuid) order by position limit 1`.execute(
        tx,
      )
    ).rows[0];
  route("GET", "/returned-inputs", async (req, a) => {
    const m = await returns(a, req.query.requestId);
    return {
      sessionId: a.session,
      requestId: m?.id ?? null,
      afterPosition: Number(req.query.afterPosition ?? 0),
      pending: false,
      hasMore: false,
      items:
        m && !m.return_ack && Number(m.position) > Number(req.query.afterPosition ?? 0)
          ? [
              {
                inputId: m.id,
                idempotencyKey: m.request_key,
                text: m.body,
                uploads: m.uploads,
                mailboxPosition: Number(m.position),
              },
            ]
          : [],
    };
  });
  route("POST", "/returned-inputs/acknowledgements", async (req, a) => {
    await sql`update user_or_agent_messages set return_ack=true where tenant_id=${a.tenant}::uuid and session_id=${a.session}::uuid and sender_id=${a.sender}::uuid and source='user' and state='withdrawn' and return_requested and id=${req.body.requestId}::uuid`.execute(
      inputs.db,
    );
    return {};
  });
  async function snapshot(a, after = 0, expected) {
    if (!Number.isSafeInteger(after) || after < 0) throw new InputConflict("Invalid page cursor");
    return inputs.db
      .transaction()
      .setIsolationLevel("repeatable read")
      .execute(async (tx) => {
        const version = await inputs.version(tx, a.tenant, a.session, a.sender);
        const rows = await inputs.queue(
          a.tenant,
          a.session,
          a.sender,
          tx,
          after,
          INPUT_QUEUE_PAGE_SIZE + 1,
        );
        const revision = hash(version) + "." + hash(a.session);
        if (expected && expected !== revision) throw new InputConflict("Queue changed");
        const page = rows.filter((r) => Number(r.position) > after);
        return {
          sessionId: a.session,
          revision,
          activeTurnId: rows[0]?.turn_id ?? null,
          takeBackToEdit: true,
          hasReturnedInputs: Boolean(await returns(a, undefined, tx)),
          viewVersion: version,
          afterPosition: after,
          hasMore: page.length > INPUT_QUEUE_PAGE_SIZE,
          items: page.slice(0, INPUT_QUEUE_PAGE_SIZE).map((r) => ({
            inputId: r.id,
            runId: r.turn_id,
            ...(r.initial_position ? { runMailboxPosition: Number(r.initial_position) } : {}),
            text: r.body,
            uploads: r.uploads,
            mailboxPosition: Number(r.position),
            acceptedAt: r.accepted_at.toISOString(),
            status: r.withdrawing
              ? "withdrawing"
              : r.state === "sending"
                ? "sending"
                : r.mode === "steer"
                  ? "steering"
                  : "queued",
          })),
        };
      });
  }
  route("GET", "/queued-inputs", (req, a) =>
    snapshot(a, Number(req.query.afterPosition ?? 0), req.query.revision),
  );
  route("GET", "/input-receipt", async (req, a) => ({
    status: (await status(a, identity(req.query.key, a.sender, a.session)))
      ? "accepted"
      : "unconfirmed",
  }));
  route("POST", "/turns", async (req, a) => {
    const request = parseAcceptTurnRequest(req.body),
      id = identity(req.headers["idempotency-key"], a.sender, a.session);
    const uploads = request.uploadIds?.length ? await a.resolveUploads(request.uploadIds) : [];
    if (
      request.uploadIds &&
      (uploads.length !== request.uploadIds.length ||
        uploads.some((file, i) => file?.id !== request.uploadIds[i]))
    )
      throw new InputConflict("An attachment is unavailable");
    const m = await inputs.admit({
      ...a,
      source: "user",
      requestKey: req.headers["idempotency-key"],
      id,
      mode: request.whenBusy ?? "input",
      strict: !request.whenBusy,
      text: request.prompt,
      uploads,
    });
    const at = m.accepted_at.toISOString();
    if (m.starts_turn)
      return {
        kind: "run",
        sessionId: a.session,
        runId: m.turn_id,
        turnId: m.turn_id,
        mailboxPosition: Number((await status(a, id)).mailbox_position),
        state: "queued",
        acceptedAt: at,
        replayed: Boolean(m.replayed),
      };
    return {
      kind: "input",
      sessionId: a.session,
      inputId: id,
      runId: m.turn_id,
      turnId: m.turn_id,
      delivery: m.mode,
      state: "accepted",
      acceptedAt: at,
      replayed: Boolean(m.replayed),
    };
  });
  route("POST", "/queued-inputs/:id/actions", async (req, a, res) => {
    const id = req.params.id,
      action = parseQueuedInputAction(req.body),
      op = identity(req.headers["idempotency-key"], a.sender, a.session);
    if (action.action === "cancel") {
      let item = await inputs.withdraw(a.tenant, a.session, a.sender, id);
      const until = Date.now() + 6500;
      while (item?.withdrawing && Date.now() < until) {
        await delay(75);
        item = await status(a, id);
      }
      if (item?.withdrawing) return { queue: await snapshot(a), replayed: false };
      if (item?.state !== "withdrawn")
        throw new InputConflict("Input already entered the conversation");
    } else {
      if (action.action === "edit" || action.text !== undefined || action.uploadIds !== undefined)
        throw new InputConflict("Take back the message before rewriting");
      const stored = async () =>
        (
          await sql`select c.* from session_input_changes c join user_or_agent_messages m on (m.tenant_id,m.session_id,m.id)=(c.tenant_id,c.session_id,c.message_id)
        where c.id=${op}::uuid and c.tenant_id=${a.tenant}::uuid and c.session_id=${a.session}::uuid and m.sender_id=${a.sender}::uuid and m.id=${id}::uuid`.execute(
            inputs.db,
          )
        ).rows[0];
      const kind = action.action === "move" ? action.direction : "steer";
      let previous = await stored();
      if (previous && previous.action !== kind) throw new InputConflict("Change identity reused");
      if (!previous) {
        const version = await inputs.version(inputs.db, a.tenant, a.session, a.sender);
        const q = await inputs.queue(a.tenant, a.session, a.sender);
        if (hash(version) + "." + hash(a.session) !== action.revision)
          throw new InputConflict("Queue changed");
        const i = q.findIndex((r) => r.id === id);
        await inputs.change({
          ...a,
          id,
          operation: op,
          action: kind,
          version,
          other: action.action === "move" ? (q[i + (kind === "up" ? -1 : 1)]?.id ?? null) : null,
        });
      }
    }
    return { queue: await snapshot(a), replayed: false };
  });
}
