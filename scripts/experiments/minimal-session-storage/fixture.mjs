import { createHash } from "node:crypto";
export const hash = (s) => createHash("sha256").update(s).digest("hex");
const padding = Array.from({ length: 65 }, (_, i) => hash("session-benchmark-" + i)).join("");
export const answer = (id, n) => (id + " answer " + n + " " + padding).slice(0, 4096);
export function extraText(id, index, bytes = 96) {
  return (id + " " + index + " " + padding.repeat(Math.ceil(bytes / 4096))).slice(0, bytes);
}

export {
  claimBatchSql,
  claimSql,
  hintedClaimSql,
  claimHintedBatchSql,
} from "./legacy-claim-sql.mjs";
