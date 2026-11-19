import { createHash } from "node:crypto";
export const hash = (s) => createHash("sha256").update(s).digest("hex");
const padding = Array.from({ length: 65 }, (_, i) => hash("session-benchmark-" + i)).join("");
export const answer = (id, n) => (id + " answer " + n + " " + padding).slice(0, 4096);
export function extraText(id, index, bytes = 96) {
  return (id + " " + index + " " + padding.repeat(Math.ceil(bytes / 4096))).slice(0, bytes);
}

const eligible = `select r.id from runs r where r.state='queued' and not exists(
 select 1 from runs p where p.tenant_id=r.tenant_id and p.session_id=r.session_id and p.mailbox_position<r.mailbox_position and p.state<>'completed')`;
const fifo = "order by r.created_at,r.id for update of r skip locked";
const take = `update runs r set state='running',lease_id=gen_random_uuid(),started_at=clock_timestamp()
 from candidate c where r.id=c.id returning r.*`;
export const claimBatchSql = `with candidate as materialized (
 ${eligible} ${fifo} limit $1) ${take}`;
export const claimSql = claimBatchSql.replace("limit $1", "limit 1");

// A hint can promote one eligible row. Remaining slots consider the original FIFO index.
export function hintedClaimSql(extraEligibility = "") {
  return `with preferred as materialized (
 ${eligible} and r.tenant_id=$2 and r.id=$3 ${extraEligibility} for update of r skip locked
), ordinary as materialized (
 ${eligible} and not exists (select 1 from preferred p where p.id=r.id)
 ${fifo} limit ($1::integer - (select count(*) from preferred))
), candidate as (select id from preferred union all select id from ordinary) ${take}`;
}
export const claimHintedBatchSql = hintedClaimSql();
