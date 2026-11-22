import { useEffect, useState } from "react";
import type {
  DelegatedAgentDirectory,
  DelegatedAgentHistory,
  ConversationTranscriptItemResource,
} from "@pi-cloud/protocol";
import { PiCloudApi } from "./api.ts";
import { useI18n } from "./i18n.tsx";
import { Markdown } from "./Markdown.tsx";
import { ToolActivity } from "./ToolActivity.tsx";

export function delegatedHistoryUrl(session: string, agent?: string) {
  const params = new URLSearchParams({ session });
  params.set("agents", agent ?? "");
  return "/?" + params.toString();
}

function HistoryItem({ item, id }: { item: ConversationTranscriptItemResource; id: string }) {
  const { t } = useI18n();
  if (item.kind === "tool") return <ToolActivity item={{ ...item, key: id }} />;
  if (item.kind === "text") return <Markdown>{item.text}</Markdown>;
  if (item.kind === "user") return <p className="product-user-bubble">{item.text}</p>;
  if (item.kind === "compaction") return <p>{t("agents.compacted")}</p>;
  if (item.kind === "hosted_search") return <p>{item.action?.type ?? "web_search"}</p>;
  return null;
}
const api = new PiCloudApi();
export function DelegatedAgentHistoryPage({
  session,
  agent,
}: {
  session: string;
  agent: string | null;
}) {
  const { t } = useI18n();
  const [agents, setAgents] = useState<DelegatedAgentDirectory["agents"]>([]),
    [entries, setEntries] = useState<DelegatedAgentHistory["entries"]>([]);
  const [revision, retry] = useState(0);
  const [cursor, setCursor] = useState<string>(),
    [request, setRequest] = useState<string>(),
    [busy, setBusy] = useState(true),
    [error, setError] = useState("");
  useEffect(() => {
    const abort = new AbortController();
    setBusy(true);
    setError("");
    (async () => {
      if (agent) {
        const page = await api.delegatedAgentHistory(session, agent, request, abort.signal);
        if (abort.signal.aborted) return;
        setEntries((old) => [...page.entries, ...(request ? old : [])]);
        setCursor(page.before);
      } else {
        const page = await api.listDelegatedAgents(session, request, abort.signal);
        if (abort.signal.aborted) return;
        setAgents((old) => [...(request ? old : []), ...page.agents]);
        setCursor(page.nextCursor);
      }
    })()
      .catch((e: unknown) => {
        if (!abort.signal.aborted) setError(e instanceof Error ? e.message : String(e));
      })
      .finally(() => {
        if (!abort.signal.aborted) setBusy(false);
      });
    return () => abort.abort();
  }, [session, agent, request, revision]);
  return (
    <main className="product-agent-history">
      <header>
        <a
          href={
            agent
              ? delegatedHistoryUrl(session)
              : "/?" + new URLSearchParams({ session }).toString()
          }
        >
          {t(agent ? "agents.directory" : "agents.root")}
        </a>
        <h1>{t(agent ? "agents.history" : "agents.directory")}</h1>
      </header>
      {error ? (
        <p role="alert">
          {error}{" "}
          <button type="button" onClick={() => retry((v) => v + 1)}>
            {t("common.retry")}
          </button>
        </p>
      ) : null}
      {busy ? <p role="status">{t("common.loading")}</p> : null}
      {agent && cursor ? (
        <button type="button" disabled={busy} onClick={() => setRequest(cursor)}>
          {t("agents.older")}
        </button>
      ) : null}
      {agent ? (
        entries.map((e) => (
          <section key={e.entryId} data-agent-run={e.runId}>
            {e.items.map((item, i) => (
              <HistoryItem key={i} item={item} id={e.entryId + "-" + i} />
            ))}
            {e.calls?.map((call, i) => (
              <details key={i}>
                <summary>{call.name}</summary>
                <pre>{JSON.stringify(call.input, null, 2)}</pre>
              </details>
            ))}
          </section>
        ))
      ) : (
        <ul>
          {agents.map((a) => (
            <li key={a.agentId}>
              <a href={delegatedHistoryUrl(session, a.agentId)}>{a.task}</a>
            </li>
          ))}
        </ul>
      )}
      {!agent && cursor ? (
        <button type="button" disabled={busy} onClick={() => setRequest(cursor)}>
          {t("chat.loadMoreConversations")}
        </button>
      ) : null}
      {!busy && !error && !(agent ? entries.length : agents.length) ? (
        <p>{t(agent ? "agents.noHistory" : "agents.empty")}</p>
      ) : null}
    </main>
  );
}
