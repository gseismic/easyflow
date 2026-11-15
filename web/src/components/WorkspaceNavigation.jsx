import { useCallback, useEffect, useId, useLayoutEffect, useRef, useState } from "react";
import { usePolling } from "../api";
import { emptyFilters, listHref, workflowHref } from "../listRoute";
import {
  navigationBatchSize, navigationGroupKey, navigationGroupLabel, navigationGroups,
  navigationProjectHref, navigationWorkflowsUrl, navigationWorkflowStatus,
  readNavigationState, saveNavigationState,
} from "../navigation";
import SearchBox from "./SearchBox";
import useFacets from "./useFacets";
import usePendingRefresh from "./usePendingRefresh";

function storageForNavigation() {
  try { return window.sessionStorage; } catch { return null; }
}

function WorkflowLink({ workflow, selected, onNavigate }) {
  const status = navigationWorkflowStatus(workflow);
  return <a className={`navigation-workflow${selected ? " selected" : ""}`}
    href={workflowHref(workflow.dag_id)} aria-current={selected ? "page" : undefined}
    title={`${workflow.dag_id} · ${status.label}`} onClick={onNavigate}>
    <span className="navigation-workflow-name">{workflow.dag_id}</span>
    <span className={`navigation-workflow-status navigation-state-${status.state}`}>{status.label}</span>
  </a>;
}

function ProjectBranch({ group, expanded, onToggle, query, selectedId, metadata, onLocateWorkflow, filters, view, route, onNavigate, complete }) {
  const [page, setPage] = useState(0);
  const [cursors, setCursors] = useState([null]);
  const disclosure = useRef(null);
  const groupId = useId();
  const workflows = usePolling(expanded ? navigationWorkflowsUrl(group, query, cursors[page]) : null);
  usePendingRefresh(workflows, workflows.data?.loading);
  const groupKey = navigationGroupKey(group);
  // 详情已确认新归属时，隐藏旧分组缓存中的同一入口，等待分组轮询更新。
  const rows = (workflows.data?.dags || []).filter((item) => item.dag_id !== selectedId ||
    !metadata || metadata.unavailable || navigationGroupKey(metadata) === groupKey);
  const selectedWorkflow = rows.find((item) => item.dag_id === selectedId);
  useLayoutEffect(() => {
    if (selectedWorkflow) onLocateWorkflow(selectedWorkflow);
  }, [selectedWorkflow, onLocateWorkflow]);
  const containsCurrent = metadata && !metadata.unavailable && navigationGroupKey(metadata) === groupKey;
  const pinned = containsCurrent && !rows.some((item) => item.dag_id === selectedId);
  const currentProject = route.name === "dags" && filters.projectKind === group.project_kind &&
    (group.project_kind !== "project" || filters.project === group.project);
  const label = navigationGroupLabel(group);
  return <li className={`navigation-project${containsCurrent ? " contains-current" : ""}`}>
    <div className="navigation-project-heading">
      <button ref={disclosure} className="navigation-disclosure" aria-label={`${expanded ? "收起" : "展开"}项目 ${label}`}
        aria-expanded={expanded} aria-controls={expanded ? groupId : undefined}
        onClick={() => onToggle(!expanded)} onKeyDown={(event) => {
          if (event.key === "ArrowRight" || event.key === "ArrowLeft") {
            event.preventDefault(); onToggle(event.key === "ArrowRight");
          }
        }}><span aria-hidden="true">{expanded ? "⌄" : "›"}</span></button>
      <a className={`navigation-project-link${currentProject ? " selected" : ""}`}
        href={navigationProjectHref(group, filters, view)} onClick={onNavigate}
        aria-current={currentProject ? "page" : undefined} title={label}>
        <span>{label}</span><small>{complete ? query ? `${group.matched}/${group.total}` : group.total : "…"}</small>
      </a>
    </div>
    {expanded && <div id={groupId} className="navigation-branch" onKeyDown={(event) => {
      if (event.key === "ArrowLeft") { event.preventDefault(); onToggle(false); disclosure.current?.focus(); }
    }}>
      {pinned && <div className="navigation-pinned"><small>当前工作流</small>
        <WorkflowLink workflow={metadata} selected onNavigate={onNavigate} /></div>}
      {workflows.error && <div className="navigation-message error" role="alert">
        {workflows.data ? "更新失败，以下可能是旧记录。" : "工作流加载失败。"}
        <button className="link-button" onClick={workflows.refresh}>重试项目</button>
      </div>}
      {!workflows.data && !workflows.error ? <p className="navigation-message" role="status">正在加载工作流…</p> :
        workflows.data && !rows.length ? <p className="navigation-message" role="status">{workflows.data.loading ? "正在发现工作流…" : "本批暂无匹配工作流。"}</p> :
          <ul aria-label={`${label} 的工作流`}>{rows.map((item) => <li key={item.dag_id}>
            <WorkflowLink workflow={item} selected={selectedId === item.dag_id} onNavigate={onNavigate} />
          </li>)}</ul>}
      {workflows.data && (page > 0 || workflows.data.next_cursor) && <div className="navigation-pagination">
        <span>第 {page + 1} 批 · 每批 {navigationBatchSize} 个</span>
        <button className="link-button" disabled={!page || Boolean(workflows.error)} onClick={() => setPage((value) => value - 1)}>上一批</button>
        <button className="link-button" disabled={!workflows.data.next_cursor || Boolean(workflows.error)} onClick={() => {
          setCursors((current) => [...current.slice(0, page + 1), workflows.data.next_cursor]);
          setPage((value) => value + 1);
        }}>下一批</button>
      </div>}
    </div>}
  </li>;
}

export default function WorkspaceNavigation({ username, route, filters, view, historyHref, metadata, open, onClose, dialogRef }) {
  const storageKey = `easyflow:navigation:${username}`;
  const [saved] = useState(() => readNavigationState(storageForNavigation(), storageKey));
  const [query, setQuery] = useState(saved.query);
  const [draft, setDraft] = useState(saved.query);
  const [expanded, setExpanded] = useState(() => new Set(saved.expanded));
  const [mobile, setMobile] = useState(() => window.matchMedia("(max-width: 1000px)").matches);
  const [locatedWorkflow, setLocatedWorkflow] = useState(null);
  const locateWorkflow = useCallback((workflow) => setLocatedWorkflow(workflow), []);
  const scroll = useRef(null);
  const content = useRef(null);
  const lastScroll = useRef(saved.scrollTop);
  const restored = useRef(saved.scrollTop === 0);
  const state = useRef(null);
  state.current = { query, expanded };
  const persist = useCallback(() => saveNavigationState(storageForNavigation(), storageKey, {
    ...state.current, scrollTop: lastScroll.current,
  }), [storageKey]);
  const selectedId = route.dagId || "";
  // 左树已有记录可在详情返回前定位；详情确认的归属优先。
  const currentWorkflow = metadata && !metadata.unavailable ? metadata :
    locatedWorkflow?.dag_id === selectedId ? locatedWorkflow : metadata;
  const selectedKey = currentWorkflow && !currentWorkflow.unavailable ? navigationGroupKey(currentWorkflow) : "";
  const facetParams = new URLSearchParams();
  if (query) facetParams.set("q", query);
  const facets = useFacets(`/api/workflows/facets?${facetParams}`);
  const groups = navigationGroups(facets.data, query);

  useEffect(() => {
    const media = window.matchMedia("(max-width: 1000px)");
    const change = () => { setMobile(media.matches); onClose(); };
    media.addEventListener("change", change);
    return () => media.removeEventListener("change", change);
  }, [onClose]);
  useLayoutEffect(() => {
    const node = dialogRef.current;
    if (!mobile) { if (node.open) node.close(); node.show(); }
    else if (open && !node.open) node.showModal();
    else if (!open && node.open) node.close();
    if (node.open && scroll.current) scroll.current.scrollTop = lastScroll.current;
  }, [mobile, open, dialogRef]);
  useEffect(() => {
    if (selectedId && selectedKey) setExpanded((current) => current.has(selectedKey) ? current : new Set([...current, selectedKey]));
  }, [selectedId, selectedKey]);
  useEffect(persist, [query, expanded, persist]);
  useEffect(() => {
    const restore = () => {
      if (restored.current || !scroll.current) return;
      scroll.current.scrollTop = saved.scrollTop;
      if (Math.abs(scroll.current.scrollTop - saved.scrollTop) < 1) restored.current = true;
    };
    const observer = new ResizeObserver(restore);
    observer.observe(content.current);
    restore();
    return () => { observer.disconnect(); persist(); };
  }, [saved.scrollTop, persist]);
  const toggle = (key, value) => setExpanded((current) => {
    const next = new Set(current); if (value) next.add(key); else next.delete(key); return next;
  });
  const rememberPosition = () => {
    restored.current = true;
    lastScroll.current = scroll.current?.scrollTop ?? lastScroll.current;
  };
  const hasSelectedGroup = currentWorkflow && !currentWorkflow.unavailable && groups.some((group) => navigationGroupKey(group) === selectedKey);
  return <dialog ref={dialogRef} id="workspace-navigation" className="workspace-navigation" aria-label="工作流导航"
    onCancel={(event) => { event.preventDefault(); onClose(); }}
    onClick={(event) => {
      if (!mobile || event.target !== dialogRef.current) return;
      const box = dialogRef.current.getBoundingClientRect();
      if (event.clientX < box.left || event.clientX > box.right || event.clientY < box.top || event.clientY > box.bottom) onClose();
    }}>
    <div className="navigation-heading"><strong>工作台</strong>
      <button className="navigation-close" aria-label="关闭工作流导航" onClick={onClose}>×</button></div>
    <nav className="navigation-global" aria-label="主导航">
      <a href={listHref("dags", emptyFilters(), { query: "", state: "all" })} onClick={onClose}
        aria-current={route.name === "dags" && filters.projectKind === "all" ? "page" : undefined}>全部工作流</a>
      <a href={historyHref} onClick={onClose}
        aria-current={route.name.startsWith("history") && !route.fromWorkflow ? "page" : undefined}>运行历史</a>
    </nav>
    <div className="navigation-search" role="search" aria-label="导航搜索" title="仅筛选左侧导航，不改变右侧列表">
      <SearchBox label="搜索导航工作流" placeholder="筛选左侧工作流或项目" query={query} value={draft}
        onDraftChange={setDraft} onSearch={(value) => { setQuery(value); setDraft(value); }} resetVersion={0} />
    </div>
    <div className="navigation-scroll" ref={scroll} onScroll={(event) => {
      if (restored.current && dialogRef.current?.open) { lastScroll.current = event.currentTarget.scrollTop; persist(); }
    }} onWheel={rememberPosition} onPointerDown={rememberPosition} onKeyDown={rememberPosition}>
      <div ref={content} className="navigation-tree-content">
        {selectedId && currentWorkflow && !hasSelectedGroup && <div className="navigation-pinned"><small>当前工作流</small>
          <a className="navigation-workflow selected" href={workflowHref(selectedId)} title={selectedId} onClick={onClose}>
            <span className="navigation-workflow-name">{selectedId}</span>
          </a>{metadata?.unavailable && <p className="navigation-message">当前定义不可用，历史记录仍可查看。</p>}</div>}
        {facets.error && <div className="navigation-message error" role="alert">导航更新失败{facets.data ? "，当前显示旧记录" : ""}。
          <button className="link-button" onClick={facets.refresh}>重试导航</button></div>}
        {!facets.data && !facets.error ? <p className="navigation-message" role="status">正在加载项目…</p> : <>
          {facets.data && !facets.data.complete && <p className="navigation-message" role="status">部分工作流仍在解析或不可用，数量尚未完整。</p>}
          {draft.trim() !== query && <p className="navigation-message" role="status">正在筛选导航…</p>}
          <ul className="navigation-projects" aria-label="项目与工作流">{groups.map((group) => {
            const key = navigationGroupKey(group);
            return <ProjectBranch key={`${key}:${query}`} group={group} expanded={expanded.has(key)}
              onToggle={(value) => toggle(key, value)} query={query} selectedId={selectedId} metadata={currentWorkflow} onLocateWorkflow={locateWorkflow}
              filters={filters} view={view} route={route} onNavigate={onClose} complete={Boolean(facets.data?.complete && !facets.error)} />;
          })}</ul>
          {facets.data && !groups.length && <p className="navigation-message" role="status">{facets.data.pending ? "正在发现项目…" :
            query ? "没有匹配的导航工作流，可清空上方搜索。" : "暂无工作流；将 DAG 文件放入配置目录后会显示在这里。"}</p>}
        </>}
      </div>
    </div>
    <nav className="navigation-settings" aria-label="工作台设置">
      <a href="#/settings" aria-current={route.name === "settings" ? "page" : undefined} onClick={onClose}><span aria-hidden="true">⚙</span>设置</a>
    </nav>
  </dialog>;
}
