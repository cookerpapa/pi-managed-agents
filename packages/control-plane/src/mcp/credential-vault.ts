import { createCipheriv, createDecipheriv, randomBytes } from "node:crypto";

/** The existing AES-GCM credential pattern, with its own key and purpose. */
export class McpCredentialVault {
  readonly #key: Buffer;
  constructor(key: string) {
    this.#key = Buffer.from(key, "base64url");
    if (this.#key.length !== 32 || this.#key.toString("base64url") !== key)
      throw new Error("MCP credential key must be 32 bytes encoded as base64url");
  }
  #aad(tenantId: string, connectionId: string, revision: string): Buffer {
    return Buffer.from(
      JSON.stringify(["pi-cloud.mcp-credential.v1", tenantId, connectionId, revision]),
    );
  }
  seal(tenantId: string, connectionId: string, revision: string, token: string): string {
    const nonce = randomBytes(12);
    const cipher = createCipheriv("aes-256-gcm", this.#key, nonce);
    cipher.setAAD(this.#aad(tenantId, connectionId, revision));
    const body = Buffer.concat([cipher.update(token, "utf8"), cipher.final()]);
    return "pmcp1_" + Buffer.concat([nonce, cipher.getAuthTag(), body]).toString("base64url");
  }
  open(tenantId: string, connectionId: string, revision: string, capsule: string): string {
    try {
      if (!/^pmcp1_[A-Za-z0-9_-]+$/.test(capsule) || capsule.length > 16384) throw Error();
      const bytes = Buffer.from(capsule.slice(6), "base64url");
      if (bytes.length < 29) throw Error();
      const cipher = createDecipheriv("aes-256-gcm", this.#key, bytes.subarray(0, 12));
      cipher.setAAD(this.#aad(tenantId, connectionId, revision));
      cipher.setAuthTag(bytes.subarray(12, 28));
      return Buffer.concat([cipher.update(bytes.subarray(28)), cipher.final()]).toString("utf8");
    } catch {
      throw new Error("MCP credential could not be authenticated");
    }
  }
}
