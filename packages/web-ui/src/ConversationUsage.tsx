import { useState } from "react";
import type {
  ConversationStatusResource,
  ConversationUsageResource,
  ConversationCompactResource,
} from "@pi-cloud/protocol";
import { useI18n } from "./i18n.tsx";

const labels = {
  queued: "compact.queued",
  summarizing: "compact.summarizing",
  awaiting_placement: "compact.awaiting",
  applied: "compact.applied",
  no_op: "compact.noOp",
  stale: "compact.stale",
  cancelled: "compact.cancelled",
  failed: "compact.failed",
} as const;
export function CompactStatus({
  state,
  instructions,
}: {
  state: ConversationCompactResource["state"];
  instructions?: string;
}) {
  const { t } = useI18n();
  return (
    <div className={`product-compact-status ${state}`} role="status">
      <code title={instructions}>/compact{instructions ? ` ${instructions}` : ""}</code>
      <span>{t(labels[state])}</span>
    </div>
  );
}
export function ConversationUsage({
  kind,
  value,
  loading,
  error,
  onRefresh,
  onClose,
}: {
  kind: "status" | "usage";
  value: ConversationUsageResource | ConversationStatusResource | null;
  loading: boolean;
  error: boolean;
  onRefresh: () => void;
  onClose: () => void;
}) {
  const { t, language } = useI18n();
  const [children, setChildren] = useState(false);
  const count = (value: number | null) =>
    value === null ? "—" : new Intl.NumberFormat(language).format(value);
  const usage = kind === "usage" && value && "tokens" in value ? value : null;
  const status = kind === "status" && value && "context" in value ? value : null;
  const tokens = children ? usage?.includingChildren : usage?.tokens;
  const missing =
    (usage?.unconfirmedRequests ?? 0) +
    (children ? (usage?.children.reduce((n, c) => n + c.unconfirmedRequests, 0) ?? 0) : 0);
  return (
    <section
      className="product-usage-panel"
      role="dialog"
      aria-label={t(kind === "status" ? "status.title" : "usage.title")}
    >
      <header>
        <strong>/{kind}</strong>
        {status && <span>{status.model.modelId}</span>}
        <button
          type="button"
          onClick={onRefresh}
          disabled={loading}
          aria-label={t("usage.refresh")}
        >
          ↻
        </button>
        <button type="button" onClick={onClose} aria-label={t("usage.close")}>
          ×
        </button>
      </header>
      {error ? <p role="alert">{t("usage.failed")}</p> : null}
      {loading && !value ? <p role="status">{t("usage.loading")}</p> : null}
      {usage && tokens ? (
        <>
          <dl className="product-usage-grid">
            <div>
              <dt>{t("usage.input")}</dt>
              <dd>{count(tokens.input)}</dd>
            </div>
            <div>
              <dt>{t("usage.output")}</dt>
              <dd>{count(tokens.output)}</dd>
            </div>
            <div>
              <dt>{t("usage.cacheRead")}</dt>
              <dd>{count(tokens.cacheRead)}</dd>
            </div>
            {tokens.cacheWrite > 0 ? (
              <div>
                <dt>{t("usage.cacheWrite")}</dt>
                <dd>{count(tokens.cacheWrite)}</dd>
              </div>
            ) : null}
            <div>
              <dt>{t("usage.total")}</dt>
              <dd>{count(tokens.total)}</dd>
            </div>
          </dl>
          {usage.children.length ? (
            <label className="product-usage-child-toggle">
              <input
                type="checkbox"
                checked={children}
                onChange={(e) => setChildren(e.target.checked)}
              />
              {t("usage.includeChildren")}
            </label>
          ) : null}
          <details>
            <summary>{t("usage.breakdown")}</summary>
            {usage.models.map((model) => (
              <div className="product-usage-breakdown" key={`${model.provider}/${model.modelId}`}>
                <span>{model.modelId}</span>
                <span>{count(model.tokens.total)}</span>
              </div>
            ))}
            {usage.children.map((child) => (
              <div className="product-usage-breakdown" key={child.sessionId}>
                <span>{child.title}</span>
                <span>{count(child.tokens.total)}</span>
              </div>
            ))}
          </details>
          <small>{t("usage.recorded")}</small>
          {missing > 0 && (
            <p>
              {t("usage.unconfirmed")} {count(missing)}
            </p>
          )}
        </>
      ) : null}
      {status && (
        <div className="product-usage-context">
          <span>{t("usage.context")}</span>
          <strong>
            ≈ {count(status.context.estimatedTokens)} / {count(status.context.window)}
          </strong>
          {status.context.window && status.context.estimatedTokens !== null ? (
            <progress
              max={status.context.window}
              value={Math.min(status.context.estimatedTokens, status.context.window)}
              aria-label={t("usage.context")}
            />
          ) : null}
          <small>
            {t("usage.threshold")} {count(status.context.compactThreshold)}
          </small>
        </div>
      )}
    </section>
  );
}
