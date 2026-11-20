import { useCallback, useEffect, useRef, useState } from "react";
import type {
  ConversationStatusResource,
  ConversationUsageResource,
  ConversationCompactResource,
} from "@pi-cloud/protocol";
import type { PiCloudApi } from "./api.ts";
import { compactionPending, type ReportCommand } from "./composer-commands.ts";

/** Status follows stable context changes while open. Lifetime receipt sums are read on demand. */
export function useConversationCommands(api: PiCloudApi, sessionId: string | undefined) {
  const current = useRef(sessionId);
  current.current = sessionId;
  const [panel, setPanel] = useState<ReportCommand | null>(null);
  const opened = useRef<{ sessionId: string; kind: ReportCommand } | null>(null);
  const [report, setReport] = useState<
    ConversationStatusResource | ConversationUsageResource | null
  >(null);
  const [error, setError] = useState(false),
    [loading, setLoading] = useState(false);
  const [compact, setCompact] = useState<ConversationCompactResource | null>(null),
    compactRef = useRef(compact);
  compactRef.current = compact;
  const refreshTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const refreshFlags = useRef({ compact: false, status: false });
  const revision = useRef(0),
    reportRequest = useRef<AbortController | null>(null),
    compactRequest = useRef<AbortController | null>(null);
  const refreshReport = useCallback(async () => {
    const request = opened.current;
    if (!sessionId || !request || request.sessionId !== sessionId) return false;
    reportRequest.current?.abort();
    const abort = new AbortController();
    reportRequest.current = abort;
    setLoading(true);
    setError(false);
    try {
      const value =
        request.kind === "status"
          ? await api.getConversationStatus(sessionId, abort.signal)
          : await api.getConversationUsage(sessionId, abort.signal);
      if (abort.signal.aborted || current.current !== sessionId || opened.current !== request)
        return false;
      setReport(value);
      return true;
    } catch {
      if (!abort.signal.aborted && current.current === sessionId) setError(true);
      return false;
    } finally {
      if (!abort.signal.aborted && current.current === sessionId) setLoading(false);
    }
  }, [api, sessionId]);
  const refreshCompact = useCallback(async () => {
    if (!sessionId) return;
    compactRequest.current?.abort();
    const abort = new AbortController();
    compactRequest.current = abort;
    const version = revision.current;
    try {
      const value = await api.getCompaction(sessionId, undefined, abort.signal);
      if (abort.signal.aborted || current.current !== sessionId || version !== revision.current)
        return;
      setCompact((old) => {
        if (
          old &&
          compactionPending(old.state) &&
          (!value || value.nativeSequence < old.nativeSequence)
        )
          return old;
        return value;
      });
    } catch {
      /* SSE reconnect and the active-command reconciliation retry this read. */
    }
  }, [api, sessionId]);
  useEffect(() => {
    revision.current++;
    opened.current = null;
    setPanel(null);
    setReport(null);
    setError(false);
    setLoading(false);
    setCompact(null);
    void refreshCompact();
    return () => {
      reportRequest.current?.abort();
      compactRequest.current?.abort();
      clearTimeout(refreshTimer.current);
      refreshTimer.current = undefined;
      refreshFlags.current = { compact: false, status: false };
    };
  }, [refreshCompact]);
  useEffect(() => {
    if (!compact || !compactionPending(compact.state)) return;
    const timer = setInterval(() => void refreshCompact(), 10000);
    return () => clearInterval(timer);
  }, [compact?.commandId, compact?.state, refreshCompact]);
  const notify = useCallback(
    (type: string) => {
      if (
        ![
          "snapshot",
          "model.sampling.started",
          "model.sampling.completed",
          "context.compaction.started",
          "context.compaction.completed",
          "turn.completed",
          "turn.failed",
          "turn.cancelled",
        ].includes(type)
      )
        return;
      if (
        type === "snapshot" ||
        type.startsWith("context.compaction") ||
        (compactRef.current && compactionPending(compactRef.current.state))
      )
        refreshFlags.current.compact = true;
      if (
        opened.current?.kind === "status" &&
        (type !== "model.sampling.started" || compactRef.current?.state === "awaiting_placement")
      )
        refreshFlags.current.status = true;
      if (refreshTimer.current === undefined)
        refreshTimer.current = setTimeout(() => {
          const flags = refreshFlags.current;
          refreshFlags.current = { compact: false, status: false };
          refreshTimer.current = undefined;
          if (flags.compact) void refreshCompact();
          if (flags.status && opened.current?.kind === "status") void refreshReport();
        }, 100);
    },
    [refreshCompact, refreshReport],
  );
  return {
    panel: opened.current?.sessionId === sessionId ? panel : null,
    report: report?.sessionId === sessionId ? report : null,
    error,
    loading,
    compact: compact?.sessionId === sessionId ? compact : null,
    notify,
    refreshReport,
    closePanel() {
      opened.current = null;
      setPanel(null);
      reportRequest.current?.abort();
    },
    openPanel(kind: ReportCommand) {
      if (!sessionId) return Promise.resolve(false);
      opened.current = { sessionId, kind };
      setPanel(kind);
      setReport(null);
      return refreshReport();
    },
    acceptedCompact(value: ConversationCompactResource) {
      if (current.current !== value.sessionId) return;
      revision.current++;
      compactRequest.current?.abort();
      setCompact(value);
    },
  };
}
