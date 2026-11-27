import { startIndependentWorker } from "../../../scripts/serving/independent-session/worker.mjs";
import { serve } from "../../../scripts/serving/independent-session/process.mjs";
await serve(startIndependentWorker);
