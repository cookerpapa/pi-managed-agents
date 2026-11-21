// Passive Undici observations for an isolated operator submission. Never retain
// bodies, headers, URLs or Guest credentials. Concurrent submissions have separate traces.
import { AsyncLocalStorage } from "node:async_hooks";
import { channel } from "node:diagnostics_channel";
import { wall } from "./pg-path-replay.mjs";

export function observeGuestHttp() {
  const scope = new AsyncLocalStorage(),
    requests = new WeakMap();
  const subscriptions = {
    "undici:request:create": ({ request }) => {
      const trace = scope.getStore();
      if (!trace) return;
      const stage =
        request.path === "/process.Process/Start"
          ? "start"
          : request.path === "/process.Process/SendInput"
            ? "input"
            : undefined;
      if (!stage) return;
      const event = { created: wall() };
      trace[stage] = event;
      requests.set(request, event);
    },
    "undici:request:headers": ({ request, response }) => {
      const event = requests.get(request);
      if (event) {
        event.headers = wall();
        event.status = response.statusCode;
      }
    },
    "undici:request:error": ({ request, error }) => {
      const event = requests.get(request);
      if (event) {
        event.errorAt = wall();
        event.error = /^[A-Z_0-9]{1,45}$/.test(error?.code) ? error.code : "transport";
      }
    },
  };
  for (const [name, listener] of Object.entries(subscriptions)) channel(name).subscribe(listener);
  return {
    submit: (trace, call) => scope.run(trace, call),
    close() {
      for (const [name, listener] of Object.entries(subscriptions))
        channel(name).unsubscribe(listener);
      scope.disable();
    },
  };
}
