import { startToolHost } from "../../../packages/control-plane/src/tool-host-main.ts";
if (!process.env.PI_CLOUD_TOOL_HOST_VERSION_URL)
  throw Error("Specify a release-specific internal Tool host URL");
await startToolHost({ mcpTurnAdmittedByProjector: true });
