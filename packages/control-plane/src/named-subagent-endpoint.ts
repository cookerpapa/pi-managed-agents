import { createHash, timingSafeEqual } from "node:crypto";
import type { FastifyInstance } from "fastify";
import {
  namedExecutorPath,
  parseSubagentOffer,
  type SubagentOffer,
} from "./named-subagent-client.ts";
import { SubagentAdmissionRefused } from "./named-subagent-store.ts";

/** A 202 acknowledges durable registration. Worker placement and the parent's
 * eventual Tool Result are independent of the lifetime of this HTTP request. */
export function installNamedSubagentEndpoint(
  server: FastifyInstance,
  token: string,
  ready: () => boolean,
  accept: (offer: SubagentOffer) => Promise<void>,
) {
  if (token.length < 32) throw Error("Invalid management credential");
  const digest = (s: string) => createHash("sha256").update(s).digest();
  const auth = digest("Bearer " + token);
  server.post(namedExecutorPath, async (req, res) => {
    if (!ready()) return res.code(503).send();
    if (!timingSafeEqual(auth, digest(req.headers.authorization ?? "")))
      return res.code(401).send();
    let offer: SubagentOffer;
    try {
      offer = parseSubagentOffer(req.body);
    } catch {
      return res.code(400).send();
    }
    try {
      await accept(offer);
      return res.code(202).send();
    } catch (error) {
      console.warn("subagent.executor.admission_unconfirmed");
      return res.code(error instanceof SubagentAdmissionRefused ? 409 : 503).send();
    }
  });
}
