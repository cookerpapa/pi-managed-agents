import { useEffect, useRef, useState } from "react";
import type {
  McpConnectionChange,
  McpConnectionResource,
  McpConnectionTool,
} from "@pi-cloud/protocol";
import type { PiCloudApi } from "./api.ts";
import { useI18n } from "./i18n.tsx";
import { errorMessage } from "./ui-errors.ts";

type Draft = {
  id: string;
  original?: McpConnectionResource;
  name: string;
  endpoint: string;
  visibility: "personal" | "organization";
  enabled: boolean;
  authentication: "none" | "bearer";
  token: string;
  allowedTools: string[] | null;
  tools: McpConnectionTool[];
};
export function McpConnections({
  api,
  owner,
  onClose,
}: {
  api: PiCloudApi;
  owner: boolean;
  onClose(): void;
}) {
  const { language, t } = useI18n(),
    say = (cn: string, en: string) => (language === "zh-CN" ? cn : en);
  const [rows, setRows] = useState<McpConnectionResource[]>([]),
    [pages, setPages] = useState<(string | undefined)[]>([undefined]),
    [next, setNext] = useState<string | null>(null);
  const [revision, setRevision] = useState(0),
    [error, setError] = useState<string>(),
    [loading, setLoading] = useState(true);
  const [draft, setDraft] = useState<Draft>(),
    [busy, setBusy] = useState(false),
    [remove, setRemove] = useState<string>();
  const mounted = useRef(true),
    inFlight = useRef(false);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);
  useEffect(() => {
    const abort = new AbortController();
    setLoading(true);
    void api
      .listMcpConnections(pages.at(-1), abort.signal)
      .then((page) => {
        if (!abort.signal.aborted) {
          setRows(page.connections);
          setNext(page.next);
        }
      })
      .catch((cause) => {
        if (!abort.signal.aborted) setError(errorMessage(cause, t));
      })
      .finally(() => {
        if (!abort.signal.aborted) setLoading(false);
      });
    return () => abort.abort();
  }, [api, pages, revision, t]);
  useEffect(() => {
    if (!rows.some((row) => row.status === "pending")) return;
    const timer = setTimeout(() => setRevision((v) => v + 1), 2000);
    return () => clearTimeout(timer);
  }, [rows, revision]);
  async function action(work: () => Promise<unknown>) {
    if (inFlight.current) return;
    inFlight.current = true;
    setBusy(true);
    setError(undefined);
    try {
      await work();
    } catch (cause) {
      if (mounted.current) setError(errorMessage(cause, t));
    } finally {
      inFlight.current = false;
      if (mounted.current) {
        setBusy(false);
        setRevision((v) => v + 1);
      }
    }
  }
  const change = (patch: Partial<Draft>) => setDraft((old) => (old ? { ...old, ...patch } : old));
  function input(value: Draft): McpConnectionChange {
    return {
      expectedRevision: value.original?.revision ?? null,
      name: value.name,
      endpoint: value.endpoint,
      visibility: value.visibility,
      enabled: value.enabled,
      allowedTools: value.allowedTools,
      ...(value.authentication === "none"
        ? { credential: { kind: "none" as const } }
        : value.token
          ? { credential: { kind: "bearer" as const, token: value.token } }
          : {}),
    };
  }
  const status = (row: McpConnectionResource) =>
    ({
      pending: say("连接中", "Connecting"),
      ready: say("可用", "Ready"),
      error: say("连接失败", "Connection failed"),
      disabled: say("已停用", "Disabled"),
    })[row.status];
  const detail = (row: McpConnectionResource) => {
    if (row.error === "mcp_authentication_failed")
      return say("认证失败，请检查令牌。", "Authentication failed. Check the token.");
    if (row.error?.includes("schema"))
      return say(
        "服务返回的工具参数格式暂不支持。",
        "The service returned an unsupported tool schema.",
      );
    if (row.error === "mcp_tool_catalog_too_large")
      return say(
        "工具定义超过目录传输大小。",
        "Tool definitions exceed the catalog transport size.",
      );
    return row.error
      ? say("请检查服务地址和连接设置。", "Check the endpoint and connection settings.")
      : null;
  };
  return (
    <main className="skill-market mcp-connections">
      <header className="skill-market-header">
        <button type="button" className="skill-back" onClick={onClose}>
          {say("返回会话", "Back to chat")}
        </button>
        <h1>{say("MCP 工具", "MCP tools")}</h1>
        {owner && (
          <button
            type="button"
            disabled={busy}
            onClick={() => {
              setError(undefined);
              setDraft({
                id: crypto.randomUUID(),
                name: "",
                endpoint: "",
                visibility: "personal",
                enabled: true,
                authentication: "none",
                token: "",
                allowedTools: null,
                tools: [],
              });
            }}
          >
            {say("添加服务", "Add service")}
          </button>
        )}
      </header>
      {error && (
        <p role="alert" className="mcp-error">
          {error}
        </p>
      )}
      {draft && (
        <form
          className="mcp-editor"
          onSubmit={(event) => {
            event.preventDefault();
            void action(async () => {
              if (
                draft.authentication === "bearer" &&
                !draft.token &&
                draft.original?.authentication !== "bearer"
              )
                throw Error(say("请填写令牌。", "Enter a token."));
              if (
                draft.authentication === "bearer" &&
                !draft.token &&
                draft.original &&
                draft.endpoint !== draft.original.endpoint
              )
                throw Error(
                  say(
                    "修改地址时请重新填写令牌，或选择无认证。",
                    "Replace the token or select no authentication when changing the endpoint.",
                  ),
                );
              await api.saveMcpConnection(draft.id, input(draft));
              if (mounted.current) setDraft(undefined);
            });
          }}
        >
          <h2>
            {draft.original
              ? say("编辑连接", "Edit connection")
              : say("添加 HTTP MCP 服务", "Add HTTP MCP service")}
          </h2>
          <label>
            {say("名称", "Name")}
            <input
              required
              maxLength={128}
              value={draft.name}
              onChange={(e) => change({ name: e.target.value })}
            />
          </label>
          <label>
            {say("服务地址", "Endpoint")}
            <input
              required
              type="url"
              maxLength={2048}
              placeholder="https://example.com/mcp"
              value={draft.endpoint}
              onChange={(e) => change({ endpoint: e.target.value })}
            />
          </label>
          <div className="mcp-field-pair">
            <label>
              {say("可见范围", "Visibility")}
              <select
                value={draft.visibility}
                onChange={(e) => change({ visibility: e.target.value as Draft["visibility"] })}
              >
                <option value="personal">{say("仅我", "Only me")}</option>
                <option value="organization">{say("组织成员", "Organization")}</option>
              </select>
            </label>
            <label>
              {say("认证", "Authentication")}
              <select
                value={draft.authentication}
                onChange={(e) =>
                  change({ authentication: e.target.value as Draft["authentication"], token: "" })
                }
              >
                <option value="none">{say("无", "None")}</option>
                <option value="bearer">Bearer token</option>
              </select>
            </label>
          </div>
          {draft.visibility === "organization" && (
            <p className="mcp-hint">
              {say(
                "组织成员将共用此连接的工具授权。",
                "Organization members share this connection's tool access.",
              )}
            </p>
          )}
          {draft.authentication === "bearer" && (
            <label>
              {say("令牌", "Token")}
              <input
                type="password"
                autoComplete="new-password"
                required={draft.original?.authentication !== "bearer"}
                maxLength={4096}
                placeholder={
                  draft.original?.authentication === "bearer"
                    ? say("留空保持现有令牌", "Leave blank to keep the existing token")
                    : ""
                }
                value={draft.token}
                onChange={(e) => change({ token: e.target.value })}
              />
            </label>
          )}
          <label className="mcp-checkbox">
            <input
              type="checkbox"
              checked={draft.enabled}
              onChange={(e) => change({ enabled: e.target.checked })}
            />
            {say("启用连接", "Enable connection")}
          </label>
          <label className="mcp-checkbox">
            <input
              type="checkbox"
              checked={draft.allowedTools === null}
              onChange={(e) =>
                change({ allowedTools: e.target.checked ? null : draft.tools.map((t) => t.name) })
              }
            />
            {say("允许全部工具（含后续新增）", "Allow all tools, including new tools")}
          </label>
          {draft.allowedTools !== null && (
            <fieldset>
              <legend>{say("允许的工具", "Allowed tools")}</legend>
              {draft.tools.length ? (
                draft.tools.map((tool) => (
                  <label className="mcp-checkbox" key={tool.name}>
                    <input
                      type="checkbox"
                      checked={draft.allowedTools!.includes(tool.name)}
                      onChange={(e) =>
                        change({
                          allowedTools: e.target.checked
                            ? [...draft.allowedTools!, tool.name]
                            : draft.allowedTools!.filter((n) => n !== tool.name),
                        })
                      }
                    />
                    {tool.name}
                  </label>
                ))
              ) : (
                <p className="mcp-hint">
                  {say(
                    "保存并发现工具后，可在这里选择。",
                    "Save and discover tools before selecting them here.",
                  )}
                </p>
              )}
            </fieldset>
          )}
          <div className="mcp-actions">
            <button type="submit" disabled={busy}>
              {busy ? say("保存中…", "Saving…") : say("保存", "Save")}
            </button>
            <button type="button" disabled={busy} onClick={() => setDraft(undefined)}>
              {say("取消", "Cancel")}
            </button>
          </div>
        </form>
      )}
      <section className="mcp-list" aria-busy={loading}>
        {!rows.length && !loading && <p>{say("暂无 MCP 服务", "No MCP services yet")}</p>}
        {rows.map((row) => (
          <article key={row.id} className="mcp-row">
            <div>
              <h2>{row.name}</h2>
              <p className="mcp-endpoint">{row.endpoint}</p>
              <p className="mcp-hint">
                {status(row)} · {row.toolCount} {say("个工具", "tools")} ·{" "}
                {row.visibility === "personal"
                  ? say("个人", "Personal")
                  : say("组织", "Organization")}
              </p>
              {detail(row) && <p className="mcp-error">{detail(row)}</p>}
            </div>
            {row.canEdit && (
              <div className="mcp-actions">
                <button
                  type="button"
                  disabled={busy}
                  onClick={() =>
                    void action(async () => {
                      const d = await api.getMcpConnection(row.id);
                      if (mounted.current)
                        setDraft({
                          id: row.id,
                          original: d.connection,
                          name: d.connection.name,
                          endpoint: d.connection.endpoint,
                          visibility: d.connection.visibility,
                          enabled: d.connection.enabled,
                          authentication: d.connection.authentication,
                          token: "",
                          allowedTools: d.connection.allowedTools,
                          tools: d.tools,
                        });
                    })
                  }
                >
                  {say("编辑", "Edit")}
                </button>
                <button
                  type="button"
                  disabled={busy || !row.enabled}
                  onClick={() => void action(() => api.refreshMcpConnection(row.id, row.revision))}
                >
                  {say("刷新工具", "Refresh tools")}
                </button>
                <button
                  type="button"
                  disabled={busy}
                  onClick={() =>
                    void action(() =>
                      api.saveMcpConnection(row.id, {
                        expectedRevision: row.revision,
                        name: row.name,
                        endpoint: row.endpoint,
                        visibility: row.visibility,
                        enabled: !row.enabled,
                        allowedTools: row.allowedTools,
                      }),
                    )
                  }
                >
                  {row.enabled ? say("停用", "Disable") : say("启用", "Enable")}
                </button>
                <button
                  type="button"
                  disabled={busy}
                  onClick={() => {
                    if (remove === row.id)
                      void action(async () => {
                        await api.removeMcpConnection(row.id, row.revision);
                        setRemove(undefined);
                      });
                    else setRemove(row.id);
                  }}
                >
                  {remove === row.id ? say("确认移除", "Confirm removal") : say("移除", "Remove")}
                </button>
                {remove === row.id && (
                  <button type="button" onClick={() => setRemove(undefined)}>
                    {say("取消", "Cancel")}
                  </button>
                )}
              </div>
            )}
          </article>
        ))}
      </section>
      <div className="mcp-actions">
        <button
          type="button"
          disabled={pages.length === 1 || loading}
          onClick={() => setPages((p) => p.slice(0, -1))}
        >
          {say("上一页", "Previous")}
        </button>
        <button
          type="button"
          disabled={!next || loading}
          onClick={() => setPages((p) => [...p, next!])}
        >
          {say("下一页", "Next")}
        </button>
        <button type="button" disabled={loading} onClick={() => setRevision((v) => v + 1)}>
          {say("刷新列表", "Refresh list")}
        </button>
      </div>
    </main>
  );
}
