# Subagent Control

This package owns PostgreSQL Subagent admission, task trees and durable
parent/child communication. Control Plane uses its stores from the Projector;
Worker composition reads task metadata through the same port.

The Worker only freezes native Lane context and appends commands. The Projector
admits children and delivers controls; it never runs model-generated JavaScript.
Pi Tool definitions and their per-Run assembly live in
[`supervisor-host/src/tools`](../supervisor-host/src/tools). This package has no
Runner dependency or model/Tool execution entry point. Direct delegation uses the same native tool request and reply transports as other control tools.

See [the Subagent contract](../../docs/SUBAGENTS.md).
