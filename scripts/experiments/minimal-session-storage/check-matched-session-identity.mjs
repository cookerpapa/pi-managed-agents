// Install the delegated Run and Flow layouts together and exercise the former failing gate.
import { runFlowStudy } from "./run-flow-history.mjs";
await runFlowStudy(true, process.argv[2] ?? "head");
