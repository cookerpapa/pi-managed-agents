// Composition of the delegated-Run PG store with the serving control dispatcher.
// Placement uses the owning Worker's native conversation port.
import { readSealedDelegationIntent } from "@pi-cloud/pi-session-postgres/durable";
import { dispatchDirectDelegation } from "./delegated-runs.mjs";

export function delegationPorts({ pool, database, topic, initialReplayComplete, place }) {
  return {
    directDelegation: (command, current) => dispatchDirectDelegation(pool, command, place, current),
    directDelegations: {
      initialReplayComplete,
      source(ref) {
        return readSealedDelegationIntent(
          database,
          ref.scope,
          topic,
          ref.conversationId,
          ref.taskId,
        );
      },
    },
  };
}
