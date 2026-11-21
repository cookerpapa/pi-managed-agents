// Minimal resource registry for two disposable VMs. Only the Workspace -> VM
// mapping is seeded. Execution freezes the tool root from the admitted Run.
import assert from "node:assert/strict";

export function continuationGuestRoutes(pool, send) {
  const pending = new Map(),
    descriptors = new Map();
  return {
    async select(scope) {
      assert.equal(
        scope.sessionId,
        scope.piSessionId,
        "This resource fixture admits root Conversations only",
      );
      const { rows } = await pool.query(
        `insert into study_guest_attachments(target,cwd)
        select t.target, r.environment_snapshot->>'cwd'
        from runs r join study_guest_targets t on t.workspace=r.environment_snapshot->>'workspace'
        where r.tenant_id=$1 and r.session_id=$2 and r.id=$3
        on conflict(target,cwd) do update set cwd=excluded.cwd returning binding,target,cwd`,
        [scope.tenantId, scope.piSessionId, scope.runId],
      );
      assert.equal(rows.length, 1, "Missing Guest for admitted Workspace");
      descriptors.set(rows[0].binding, rows[0]);
      return rows[0].binding;
    },
    resolve(binding) {
      return {
        async submit(record, cmd, ref) {
          const attachment =
            descriptors.get(binding) ??
            (
              await pool.query(
                "select binding,target,cwd from study_guest_attachments where binding=$1",
                [binding],
              )
            ).rows[0];
          assert(attachment && cmd.request.activationId === binding);
          const { topic, partition, offset } = record,
            op = cmd.request.operationId;
          const wait = Promise.withResolvers();
          assert(!pending.has(op));
          pending.set(op, wait);
          const timer = setTimeout(() => wait.reject(Error("Guest submission timed out")), 15000);
          try {
            send("guestCommand", {
              attachment,
              cmd,
              ref,
              position: { topic, partition, offset: String(offset) },
            });
            await wait.promise;
          } finally {
            clearTimeout(timer);
            pending.delete(op);
          }
        },
      };
    },
    accepted(m) {
      const wait = pending.get(m.op);
      if (m.ok) wait?.resolve();
      else wait?.reject(Error("Guest did not confirm acceptance"));
    },
  };
}
