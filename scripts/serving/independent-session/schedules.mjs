import { sql } from "kysely";
import { SessionScheduleService } from "../../../packages/control-plane/src/session-schedule-service.ts";
import { sessionInputId } from "../../experiments/minimal-session-storage/segment-input-api.mjs";
import { schema } from "./config.mjs";

export function segmentSchedules(database, inputs) {
  return new SessionScheduleService({
    database,
    async admitScheduledTurn(tx, scope, text, origin, key) {
      // The listener has locked the public Session and its plan. Use the same
      // writable Flow and mutex as all other admissions, also after a prune.
      const {
        rows: [settings],
      } = await sql`select current_setting('search_path') as path`.execute(tx);
      await sql`select set_config('search_path',${schema + ",public"},true)`.execute(tx);
      await sql`set local synchronous_commit=on`.execute(tx);
      const {
        rows: [window],
      } = await sql`select flow_id,owner_user_id from sessions
        where tenant_id=${scope.tenantId}::uuid and id=${scope.sessionId}::uuid and parent_session_id is null for no key update`.execute(
        tx,
      );
      if (!window?.owner_user_id) throw Error("Scheduled Session has no admitted owner");
      const inputId = sessionInputId(key, window.owner_user_id, scope.sessionId);
      const {
        rows: [turn],
      } =
        await sql`insert into turns(id,tenant_id,session_id,mailbox_position,idempotency_key,initial_input,scheduled_origin,state)
        select ${inputId}::uuid,${scope.tenantId}::uuid,${window.flow_id}::uuid,
         coalesce((select mailbox_position from turns where tenant_id=${scope.tenantId}::uuid and session_id=${window.flow_id}::uuid
           order by mailbox_position desc limit 1),0)+1,${inputId}::uuid,${text},${JSON.stringify(origin)}::jsonb,'queued'
        returning id`.execute(tx);
      await inputs.admitIn(tx, {
        tenant: scope.tenantId,
        session: scope.sessionId,
        id: inputId,
        sender: origin.scheduleId,
        source: "user",
        mode: "input",
        initialTurn: turn.id,
        text,
        requestKey: key,
      });
      await sql`select set_config('search_path',${settings.path},true)`.execute(tx);
      return turn.id;
    },
  });
}
