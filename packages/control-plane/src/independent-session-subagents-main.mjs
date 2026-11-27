import { serve } from "../../../scripts/serving/independent-session/process.mjs";
import { startIndependentDelegations } from "../../../scripts/serving/independent-session/subagents.mjs";
await serve(startIndependentDelegations);
