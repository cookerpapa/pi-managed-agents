import { startToolHost } from "./tool-host-main.ts";
import { paths } from "../../../scripts/serving/independent-session/config.mjs";
await startToolHost({ mcpTurnAdmittedByProjector: true, turnReadUrl: (url) => paths(url).read });
