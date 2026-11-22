import type { FastifyRequest } from "fastify";

export async function abortableHistoryRead<T>(
  request: FastifyRequest,
  read: (signal: AbortSignal) => Promise<T>,
): Promise<T> {
  const abort = new AbortController();
  const closed = () => abort.abort(new Error("History reader disconnected"));
  request.raw.once("aborted", closed);
  request.raw.socket.once("close", closed);
  try {
    return await read(abort.signal);
  } finally {
    request.raw.off("aborted", closed);
    request.raw.socket.off("close", closed);
  }
}
