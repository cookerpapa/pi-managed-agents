import { useEffect, useMemo, useRef, useState, type RefObject } from "react";
import type {
  ConversationTreeBranchResource,
  ConversationTreeResource,
  ConversationTreeView,
  NavigationChildren,
} from "@pi-cloud/protocol";
import { selectActiveTurn } from "./active-turn-selection.ts";
import { useI18n, type Translate } from "./i18n.tsx";
import { useResizablePanel } from "./use-resizable-panel.ts";

const PROGRAMMATIC_SCROLL_GUARD_MS = 1_000;

function compact(value: string, maximum = 76, emptyMessage = "（空消息）"): string {
  const text = value.replace(/\s+/gu, " ").trim();
  return text.length > maximum ? `${text.slice(0, maximum)}…` : text || emptyMessage;
}

function turnElements(scroller: HTMLElement): ReadonlyMap<string, HTMLElement> {
  const elements = new Map<string, HTMLElement>();
  for (const element of scroller.querySelectorAll<HTMLElement>("[data-conversation-turn-id]")) {
    const turnId = element.dataset.conversationTurnId;
    if (turnId !== undefined) elements.set(turnId, element);
  }
  return elements;
}

function entryElements(scroller: HTMLElement): ReadonlyMap<string, HTMLElement> {
  const elements = new Map<string, HTMLElement>();
  for (const element of scroller.querySelectorAll<HTMLElement>("[data-conversation-entry-id]")) {
    const entryId = element.dataset.conversationEntryId;
    if (entryId !== undefined) elements.set(entryId, element);
  }
  return elements;
}

export function selectConversationNavigationTarget<T>(
  entries: ReadonlyMap<string, T>,
  _turns: ReadonlyMap<string, T>,
  target: { turnId: string; entryId: string },
): T | undefined {
  return entries.get(target.entryId);
}

function branchChildren(tree: ConversationTreeResource) {
  const children = new Map<string, ConversationTreeBranchResource[]>();
  for (const branch of tree.branches) {
    if (branch.parentSessionId === null) continue;
    const parent = tree.branches.find(
      (candidate) => candidate.sessionId === branch.parentSessionId,
    );
    // A branch can fork at an assistant answer. Attach it under the last
    // visible input up to that answer in the same Turn.
    const sourceEntry = branch.forkedFromEntryId;
    const exact = parent?.entries.find((entry) => entry.entryId === sourceEntry);
    const anchor =
      exact?.role === "user"
        ? exact.entryId
        : parent?.entries
            .filter(
              (entry) =>
                entry.role === "user" &&
                entry.turnId === branch.forkedFromTurnId &&
                sourceEntry !== null &&
                Number(entry.entryId) <= Number(sourceEntry),
            )
            .at(-1)?.entryId;
    const key = `${branch.parentSessionId}:${anchor ?? "__root__"}`;
    const siblings = children.get(key) ?? [];
    siblings.push(branch);
    children.set(key, siblings);
  }
  return children;
}

function contextLabel(
  mode: NonNullable<ConversationTreeBranchResource["contextMode"]>,
  t: Translate,
): string {
  return mode === "branch" ? t("chat.context.inherited") : t("chat.context.fresh");
}

function sandboxLabel(
  mode: NonNullable<ConversationTreeBranchResource["sandboxMode"]>,
  t: Translate,
): string {
  if (mode === "shared") return t("chat.sandbox.shared");
  if (mode === "ephemeral") return t("chat.sandbox.ephemeral");
  return t("chat.sandbox.none");
}

type LoadChildren = (
  sessionId: string,
  entryId: string,
  after: string,
) => Promise<NavigationChildren>;

function ChildLinks({
  group,
  session,
  entry,
  open,
  load,
}: {
  group: NavigationChildren;
  session: string;
  entry: string;
  open: (id: string) => void;
  load: LoadChildren;
}) {
  const { t } = useI18n();
  const [extra, setExtra] = useState<NavigationChildren | null>(null);
  const [busy, setBusy] = useState(false);
  const [failed, setFailed] = useState(false);
  const items = [...group.items, ...(extra?.items ?? [])];
  const next = extra ? extra.next : group.next;
  const more = async () => {
    if (!next || busy) return;
    setBusy(true);
    setFailed(false);
    try {
      const page = await load(session, entry, next);
      setExtra({
        items: [...(extra?.items ?? []), ...page.items],
        ...(page.next ? { next: page.next } : {}),
      });
    } catch {
      setFailed(true);
    } finally {
      setBusy(false);
    }
  };
  return (
    <div className="product-tree-child-links">
      {items.map((item) => (
        <button
          className="product-tree-entry"
          type="button"
          key={item.sessionId}
          onClick={() => open(item.sessionId)}
        >
          <span aria-hidden="true">↳</span>
          <span>{item.name}</span>
        </button>
      ))}
      {next && (
        <button
          className="product-tree-page"
          type="button"
          disabled={busy}
          onClick={() => void more()}
        >
          {failed ? t("tree.retryChildren") : t("tree.moreChildren")}
        </button>
      )}
    </div>
  );
}

function TreeBranch({
  branch,
  children,
  depth,
  activeEntryKey,
  navigate,
  page,
  loadChildren,
  inputPath,
}: {
  loadChildren?: LoadChildren | undefined;
  inputPath?: boolean | undefined;
  page?: ((sessionId: string, before?: string) => void) | undefined;
  branch: ConversationTreeBranchResource;
  children: ReadonlyMap<string, readonly ConversationTreeBranchResource[]>;
  depth: number;
  activeEntryKey: string | null;
  navigate: (sessionId: string, target?: { turnId: string; entryId: string }) => void;
}) {
  const { t } = useI18n();
  const unanchoredChildren = children.get(`${branch.sessionId}:__root__`) ?? [];
  return (
    <div className="product-tree-branch" style={{ "--tree-depth": depth } as React.CSSProperties}>
      {inputPath || (branch.parentSessionId === null && branch.kind === "conversation") ? null : (
        <button
          className={`product-tree-branch-label ${branch.kind}${branch.current ? " current" : ""}`}
          onClick={() => navigate(branch.sessionId)}
          title={branch.kind === "subagent" ? t("chat.subagentTitle") : branch.title}
          type="button"
        >
          <span>{branch.kind === "subagent" ? "◈" : "↳"}</span>
          <span>
            <strong>{branch.kind === "subagent" ? t("chat.subagentTitle") : branch.title}</strong>
            {branch.kind === "subagent" &&
            branch.contextMode !== undefined &&
            branch.sandboxMode !== undefined ? (
              <small>
                {contextLabel(branch.contextMode, t)} · {sandboxLabel(branch.sandboxMode, t)} ·{" "}
                {branch.delegatedState}
              </small>
            ) : null}
          </span>
        </button>
      )}
      {branch.earlierEntryId && page ? (
        <button
          className="product-tree-page"
          onClick={() => page(branch.sessionId, branch.earlierEntryId)}
        >
          {t("tree.earlier")}
        </button>
      ) : null}
      {branch.hasLaterEntries && page ? (
        <button className="product-tree-page" onClick={() => page(branch.sessionId)}>
          {t("tree.recent")}
        </button>
      ) : null}
      {unanchoredChildren.map((child) => (
        <TreeBranch
          activeEntryKey={activeEntryKey}
          branch={child}
          children={children}
          depth={depth + 1}
          key={child.sessionId}
          navigate={navigate}
          page={page}
        />
      ))}
      {branch.entries
        .filter((entry) => entry.role === "user")
        .map((entry) => {
          const nested = children.get(`${branch.sessionId}:${entry.entryId}`) ?? [];
          return (
            <div className="product-tree-entry-wrap" key={`${branch.sessionId}:${entry.entryId}`}>
              <button
                aria-current={
                  activeEntryKey === entry.turnId + "/" + entry.entryId ? "true" : undefined
                }
                className={`product-tree-entry product-tree-${entry.role}${
                  activeEntryKey === entry.turnId + "/" + entry.entryId ? " active" : ""
                }`}
                onClick={() =>
                  navigate(branch.sessionId, { turnId: entry.turnId, entryId: entry.entryId })
                }
                title={compact(entry.text, 240, t("tree.emptyMessage"))}
                type="button"
              >
                <span className="product-tree-connector" aria-hidden="true">
                  {entry.source === "agent" ? "A" : "U"}
                </span>
                <span>{compact(entry.text, 76, t("tree.emptyMessage"))}</span>
              </button>
              {entry.children && loadChildren && (
                <ChildLinks
                  key={
                    branch.sessionId +
                    ":" +
                    entry.entryId +
                    ":" +
                    entry.children.items.map((c) => c.sessionId).join(",")
                  }
                  group={entry.children}
                  session={branch.sessionId}
                  entry={entry.entryId}
                  open={navigate}
                  load={loadChildren}
                />
              )}
              {nested.map((child) => (
                <TreeBranch
                  activeEntryKey={activeEntryKey}
                  branch={child}
                  children={children}
                  depth={depth + 1}
                  key={child.sessionId}
                  navigate={navigate}
                  page={page}
                />
              ))}
            </div>
          );
        })}
    </div>
  );
}

export function ConversationTreeNavigator({
  tree,
  view,
  loading,
  scrollerRef,
  onViewChange,
  onDeleteChild,
  loadChildren,
  onNavigate,
  onPage,
  onJump,
}: {
  tree: ConversationTreeResource | null;
  view: ConversationTreeView;
  loading: boolean;
  scrollerRef: RefObject<HTMLElement | null>;
  loadChildren?: LoadChildren | undefined;
  onDeleteChild?: ((sessionId: string, title: string) => void) | undefined;
  onViewChange: (view: ConversationTreeView) => void;
  onPage?: ((sessionId: string, before?: string) => void) | undefined;
  onNavigate: (sessionId: string, target?: { turnId: string; entryId: string }) => void;
  onJump?: () => void;
}) {
  const { t } = useI18n();
  const panel = useResizablePanel({
    storageKey: "pi-cloud:conversation-tree",
    initialWidth: 300,
    minimumWidth: 220,
    maximumWidth: 520,
  });
  const [activeEntryKey, setActiveEntryKey] = useState<string | null>(null);
  const jumpTargetRef = useRef<string | null>(null);
  const jumpReleaseTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const currentEntries = useMemo(() => {
    if (tree === null) return [];
    const byId = new Map(tree.branches.map((branch) => [branch.sessionId, branch] as const));
    const lineage = new Set<string>();
    let cursor: string | null = tree.currentSessionId;
    while (cursor !== null) {
      lineage.add(cursor);
      cursor = byId.get(cursor)?.parentSessionId ?? null;
    }
    return tree.branches
      .filter((branch) => lineage.has(branch.sessionId))
      .flatMap((branch) => branch.entries.filter((entry) => entry.role === "user"));
  }, [tree]);
  const identity = currentEntries.map((e) => e.turnId + "/" + e.entryId).join("\u0000");

  useEffect(() => {
    const scroller = scrollerRef.current;
    if (scroller === null || currentEntries.length === 0) return;
    const updateActive = (): void => {
      if (jumpTargetRef.current !== null) return;
      const bounds = scroller.getBoundingClientRect();
      const elements = entryElements(scroller);
      setActiveEntryKey(
        selectActiveTurn({
          anchors: currentEntries.flatMap(({ turnId, entryId }) => {
            const element = elements.get(entryId);
            return element?.dataset.conversationTurnId === turnId
              ? [{ turnId: turnId + "/" + entryId, top: element.getBoundingClientRect().top }]
              : [];
          }),
          clientHeight: scroller.clientHeight,
          scrollHeight: scroller.scrollHeight,
          scrollTop: scroller.scrollTop,
          scrollerTop: bounds.top,
        }),
      );
    };
    scroller.addEventListener("scroll", updateActive, { passive: true });
    updateActive();
    return () => scroller.removeEventListener("scroll", updateActive);
  }, [identity, scrollerRef]);

  useEffect(
    () => () => {
      if (jumpReleaseTimerRef.current !== null) clearTimeout(jumpReleaseTimerRef.current);
    },
    [],
  );

  const navigate = (
    sessionId: string,
    targetIdentity?: { turnId: string; entryId: string },
  ): void => {
    if (targetIdentity === undefined) {
      onNavigate(sessionId);
      return;
    }
    onJump?.();
    const scroller = scrollerRef.current;
    const target =
      scroller === null || sessionId !== tree?.currentSessionId
        ? undefined
        : selectConversationNavigationTarget(
            entryElements(scroller),
            turnElements(scroller),
            targetIdentity,
          );
    if (target === undefined) {
      onNavigate(sessionId, targetIdentity);
      return;
    }
    jumpTargetRef.current = targetIdentity.entryId;
    setActiveEntryKey(targetIdentity.turnId + "/" + targetIdentity.entryId);
    target.scrollIntoView({ behavior: "smooth", block: "start" });
    if (jumpReleaseTimerRef.current !== null) clearTimeout(jumpReleaseTimerRef.current);
    jumpReleaseTimerRef.current = setTimeout(() => {
      jumpTargetRef.current = null;
      jumpReleaseTimerRef.current = null;
    }, PROGRAMMATIC_SCROLL_GUARD_MS);
  };

  const root =
    tree?.branches.find(
      (branch) =>
        branch.parentSessionId === null ||
        !tree.branches.some((parent) => parent.sessionId === branch.parentSessionId),
    ) ?? null;
  const children = tree === null ? new Map() : branchChildren(tree);
  return (
    <aside
      className={`product-tree-panel product-resizable-panel${panel.collapsed ? " collapsed" : ""}`}
      style={{ width: panel.collapsed ? 42 : panel.width }}
    >
      <button
        aria-label={panel.collapsed ? t("tree.expand") : t("tree.collapse")}
        className="product-panel-collapse"
        onClick={panel.toggle}
        title={panel.collapsed ? t("tree.expand") : t("tree.collapse")}
        type="button"
      >
        {panel.collapsed ? "›" : "‹"}
      </button>
      {panel.collapsed ? <span className="product-collapsed-label">{t("tree.label")}</span> : null}
      <div className="product-panel-content">
        <header className="product-tree-header">
          <div>
            <strong>{t("tree.title")}</strong>
            {!tree?.inputPath && (
              <span>
                {tree === null
                  ? t("tree.selectConversation")
                  : loading
                    ? t("tree.syncing")
                    : t("tree.branches", { count: tree.branches.length })}
              </span>
            )}
          </div>
          {tree?.inputPath ? (
            root?.parentSessionId && (
              <div className="product-tree-child-actions">
                <button
                  type="button"
                  className="product-tree-page"
                  onClick={() =>
                    navigate(
                      root.parentSessionId!,
                      root.forkedFromTurnId && root.forkedFromEntryId
                        ? { turnId: root.forkedFromTurnId, entryId: root.forkedFromEntryId }
                        : undefined,
                    )
                  }
                >
                  {t("tree.backToParent")}
                </button>
                {onDeleteChild && (
                  <button
                    type="button"
                    className="product-delete-conversation"
                    title={t("chat.deleteConversation", { title: root.title })}
                    aria-label={t("chat.deleteConversation", { title: root.title })}
                    onClick={() => onDeleteChild(root.sessionId, root.title)}
                  >
                    ×
                  </button>
                )}
              </div>
            )
          ) : (
            <div className="product-tree-view-switch" role="group" aria-label={t("tree.view")}>
              <button
                className={view === "focus" ? "active" : ""}
                onClick={() => onViewChange("focus")}
                type="button"
              >
                {t("tree.focus")}
              </button>
              <button
                className={view === "full" ? "active" : ""}
                onClick={() => onViewChange("full")}
                type="button"
              >
                {t("tree.full")}
              </button>
            </div>
          )}
        </header>
        <nav aria-busy={loading} className="product-tree-nav" aria-label={t("tree.sessionTree")}>
          {loading && root === null ? (
            <div className="product-tree-empty">{t("tree.loading")}</div>
          ) : root === null ? (
            <div className="product-tree-empty" aria-hidden="true" />
          ) : (
            <TreeBranch
              activeEntryKey={activeEntryKey}
              branch={root}
              children={children}
              depth={0}
              navigate={navigate}
              inputPath={tree?.inputPath}
              loadChildren={loadChildren}
              page={onPage}
            />
          )}
        </nav>
      </div>
      {panel.collapsed ? null : (
        <div
          aria-label={t("tree.resize")}
          className="product-panel-resizer"
          onKeyDown={(event) => {
            if (event.key === "ArrowLeft") panel.setWidth(panel.width - 12);
            if (event.key === "ArrowRight") panel.setWidth(panel.width + 12);
          }}
          onPointerDown={panel.beginResize}
          role="separator"
          tabIndex={0}
        />
      )}
    </aside>
  );
}
