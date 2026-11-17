import { nativeInputConfirmationKey, type NativeInputConfirmation } from "@pi-cloud/protocol";

export const INPUT_CONFIRMATIONS_PATH = "/internal/v1/session-input-confirmations";
export const INPUT_CONFIRMATION_WAIT_MS = 30_000;
export interface InputConfirmationWait {
  result: Promise<boolean>;
  close(): void;
}
export interface InputConfirmationPort {
  open(expected: NativeInputConfirmation): Promise<InputConfirmationWait>;
}

/** The response headers acknowledge waiter registration, before Worker delivery. */
export class HttpInputConfirmations implements InputConfirmationPort {
  constructor(
    readonly options: { baseUrl: string; token: string; fetch?: typeof fetch; waitMs?: number },
  ) {}

  async open(expected: NativeInputConfirmation): Promise<InputConfirmationWait> {
    const abort = new AbortController();
    const timer = setTimeout(
      () => abort.abort(),
      this.options.waitMs ?? INPUT_CONFIRMATION_WAIT_MS,
    );
    timer.unref();
    const close = () => {
      clearTimeout(timer);
      abort.abort();
    };
    let url = new URL(INPUT_CONFIRMATIONS_PATH, this.options.baseUrl).toString();
    try {
      for (let redirect = 0; redirect < 4; redirect++) {
        const response = await (this.options.fetch ?? fetch)(url, {
          method: "POST",
          redirect: "manual",
          headers: {
            authorization: `Bearer ${this.options.token}`,
            "content-type": "application/json",
          },
          body: JSON.stringify(expected),
          signal: abort.signal,
        });
        if (response.status === 307) {
          await response.body?.cancel();
          const location = response.headers.get("location");
          if (!location) throw new Error("Input confirmation owner is unavailable");
          url = new URL(location, url).toString();
          continue;
        }
        if (!response.ok || !response.body) {
          await response.body?.cancel();
          throw new Error("Input confirmation subscription failed");
        }
        const result = (async () => {
          const reader = response.body!.getReader();
          const decoder = new TextDecoder();
          let pending = "";
          try {
            for (;;) {
              const chunk = await reader.read();
              if (chunk.done) return false;
              pending += decoder.decode(chunk.value, { stream: true });
              if (pending.length > 8192)
                throw new Error("Input confirmation frame exceeded budget");
              let end: number;
              while ((end = pending.indexOf("\n\n")) >= 0) {
                const frame = pending.slice(0, end);
                pending = pending.slice(end + 2);
                if (!frame.startsWith("event: applied\n")) continue;
                const data = frame.split("\n").find((line) => line.startsWith("data: "));
                if (!data) throw new Error("Input confirmation is missing");
                return (
                  nativeInputConfirmationKey(JSON.parse(data.slice(6))) ===
                  nativeInputConfirmationKey(expected)
                );
              }
            }
          } catch {
            return false;
          } finally {
            await reader.cancel().catch(() => {});
            close();
          }
        })();
        return { result, close };
      }
      throw new Error("Input confirmation ownership is changing");
    } catch (error) {
      close();
      throw error;
    }
  }
}
