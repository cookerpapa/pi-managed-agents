# ADR-0367. Confirm steer hints on the owning Worker

The isolated #52 candidate keeps ADR-0366's PG input authority, native placement, Session claim and ordered seal. Inside a normal tool boundary the owner reads a local hint first. An empty hint skips the PG lookup. A positive hint selects by PG queue order and by the current writer's fence. Startup and native settlement still consult PG.

A pending steer gets a monotonic notice when it becomes eligible, including promotion, requeue and delivery to a successor Turn. Issue notices under the same Session lock as admission. PG NOTIFY after commit uses the existing readiness listener. The runtime routes only the notice and lease identity to the owner's management socket. On receipt the Worker marks its in-memory hint and acknowledges on that socket. The acknowledgement may cover earlier pending notices of the same Turn. The API reports a pending steer as sending until the current writer has acknowledged its notice. An acknowledgement never places an input or authorizes work.

Register the lease-local hint before bootstrapping from PG. Reconcile on socket registration and batch missed notices per Worker at renewal. The existing retention and ordered handoff protect the message even if a notice or acknowledgement is lost. An old writer's receipt cannot certify the new writer. An empty or last-item result can clear only the exact local hint observed before that selection. New arrivals during selection survive. A message committed just before a boundary may miss it while the UI says sending.

Acceptance covers no SQL for clean tool boundaries, a full Worker receiving notices, human and agent priority, promotion and withdrawal, notification and acknowledgement loss, socket reconnect and lease takeover. The production release gate remains under #48.


[Implementation and acceptance](../reports/worker-steer-hints-20261126.md). The notice uses the PG sequence's default cache of one together with the target Session lock. Ordering is significant only among notices for the same active Turn.
