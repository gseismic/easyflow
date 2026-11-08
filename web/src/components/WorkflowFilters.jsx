import { useEffect, useRef, useState } from "react";
import { formatTime } from "./Status";

const projectValue = (kind, name = "") => JSON.stringify([kind, name]);

export function appendWorkflowFilters(params, filters, includeProject = true) {
  if (includeProject && filters.projectKind !== "all") {
    params.set("project_kind", filters.projectKind);
    if (filters.projectKind === "project") params.set("project", filters.project);
  }
  filters.tags.forEach((tag) => params.append("tag", tag));
  return params;
}

export function selectedFacetCount(facets, filters) {
  if (!facets?.complete) return null;
  const groups = facets.groups || [];
  if (filters.projectKind === "all") {
    return groups.reduce((total, group) => total + group.matched, 0);
  }
  return groups.find((group) =>
    group.project_kind === filters.projectKind &&
    (filters.projectKind !== "project" || group.project === filters.project),
  )?.matched || 0;
}

export default function WorkflowFilters({
  catalog,
  facets,
  filters,
  onChange,
  onReset,
  hasOtherFilters,
  pendingSearch,
  unit,
  showUnknown = false,
}) {
  const [tagQuery, setTagQuery] = useState("");
  const tagFilterRef = useRef(null);

  useEffect(() => {
    const details = tagFilterRef.current;
    const closeOnOutside = (event) => {
      if (details.open && !details.contains(event.target)) details.open = false;
    };
    const closeOnEscape = (event) => {
      if (event.key === "Escape" && details.open && details.contains(event.target)) {
        event.preventDefault();
        details.open = false;
        details.querySelector("summary")?.focus();
      }
    };
    document.addEventListener("pointerdown", closeOnOutside);
    document.addEventListener("keydown", closeOnEscape);
    return () => {
      document.removeEventListener("pointerdown", closeOnOutside);
      document.removeEventListener("keydown", closeOnEscape);
    };
  }, []);
  const groups = facets.data?.groups || [];
  const groupsByProject = new Map(groups.map((group) => [
    projectValue(group.project_kind, group.project || ""), group,
  ]));
  const totals = groups.reduce((result, group) => ({
    total: result.total + group.total,
    matched: result.matched + group.matched,
  }), { total: 0, matched: 0 });
  const projects = [...new Set(
    facets.data?.complete && !facets.error
      ? groups.filter((group) => group.project_kind === "project").map((group) => group.project)
      : catalog.data?.projects || [],
  )].sort((left, right) => left.localeCompare(right));
  const availableTags = [...new Set([...(catalog.data?.tags || []), ...filters.tags])];
  const visibleTags = availableTags.filter((tag) =>
    tag.toLocaleLowerCase().includes(tagQuery.trim().toLocaleLowerCase()),
  );
  const selectedValue = projectValue(filters.projectKind, filters.project || "");
  const selectedMissing = filters.projectKind === "project" && !projects.includes(filters.project);
  const changed = filters.projectKind !== "all" || filters.tags.length > 0 || hasOtherFilters;
  const hasMatchFilter = filters.tags.length > 0 || hasOtherFilters;
  const catalogIncomplete = Boolean(catalog.data?.loading || catalog.data?.issues?.length);
  const ready = Boolean(facets.data?.complete && !facets.error && !catalog.error && !catalogIncomplete && !pendingSearch);
  const countFor = (kind, project = "") => {
    if (!ready) return "…";
    const { total, matched } = kind === "all" ? totals :
      groupsByProject.get(projectValue(kind, project)) || { total: 0, matched: 0 };
    return hasMatchFilter ? `${matched} / ${total}` : String(total);
  };
  const labelFor = (label, kind, project = "") => `${label} · ${countFor(kind, project)} ${unit}`;
  const projectCount = ready ? groups.filter((group) => group.project_kind === "project").length : "…";
  const calculatedAt = facets.data?.calculated_at;
  const calculatedDate = new Date(calculatedAt || "");
  const shortTime = Number.isNaN(calculatedDate.valueOf()) ? "—" :
    calculatedDate.toLocaleTimeString("zh-CN", { hour12: false });
  const statusText = facets.error || catalog.error
    ? "统计不可用"
    : ready
      ? `统计更新于 ${shortTime}`
      : facets.data && !facets.data.complete && !facets.data.pending && !catalog.data?.loading
        ? "统计不完整"
        : "正在统计";
  const selectVisibleTags = () => onChange({
    ...filters,
    tags: [...new Set([...filters.tags, ...visibleTags])],
  });

  return (
    <div className="workflow-filters">
      <label className="project-filter">
        <span>项目（{projectCount}）</span>
        <select
          aria-label="筛选项目"
          value={selectedValue}
          onFocus={facets.refresh}
          onChange={(event) => {
            const [projectKind, project] = JSON.parse(event.target.value);
            onChange({ ...filters, projectKind, project });
          }}
        >
          <option value={projectValue("all")}>{labelFor("全部项目", "all")}</option>
          <option value={projectValue("unassigned")}>{labelFor("未分组", "unassigned")}</option>
          <option value={projectValue("conflict")}>{labelFor("项目异常", "conflict")}</option>
          {(showUnknown || filters.projectKind === "unknown") && (
            <option value={projectValue("unknown")}>{labelFor("项目未知", "unknown")}</option>
          )}
          {selectedMissing && (
            <option value={selectedValue}>{labelFor(filters.project, "project", filters.project)}</option>
          )}
          {projects.map((project) => (
            <option value={projectValue("project", project)} key={project}>
              {labelFor(project, "project", project)}
            </option>
          ))}
        </select>
      </label>
      <details className="tag-filter" ref={tagFilterRef}>
        <summary>标签{filters.tags.length ? ` (${filters.tags.length})` : ""}</summary>
        <div className="tag-filter-menu">
          <input
            aria-label="搜索标签"
            placeholder="搜索标签"
            value={tagQuery}
            onChange={(event) => setTagQuery(event.target.value)}
          />
          <div className="tag-actions">
            <button className="link-button" disabled={!visibleTags.length} onClick={selectVisibleTags}>
              全选当前列表
            </button>
            <button className="link-button" disabled={!filters.tags.length} onClick={() => onChange({ ...filters, tags: [] })}>
              全部不选
            </button>
          </div>
          <div className="tag-options">
            {visibleTags.length ? visibleTags.map((tag) => (
              <label key={tag}>
                <input
                  type="checkbox"
                  checked={filters.tags.includes(tag)}
                  onChange={(event) => onChange({
                    ...filters,
                    tags: event.target.checked
                      ? [...filters.tags, tag]
                      : filters.tags.filter((item) => item !== tag),
                  })}
                />
                <span title={tag}>{tag}</span>
              </label>
            )) : <p>没有匹配的标签。</p>}
          </div>
          <p className="tag-help">匹配任一选中标签；全部不选时不限标签，包括无标签工作流。</p>
        </div>
      </details>
      {changed && <button className="link-button" onClick={onReset}>重置全部</button>}
      <span className="facet-note" role="status" title={`${hasMatchFilter ? "项目数量为当前匹配数 / 总数" : "项目数量为总数"}${ready ? `；统计时间 ${formatTime(calculatedAt)}` : ""}`}>
        {statusText}{ready && hasMatchFilter ? " · 匹配 / 总数" : ""}
      </span>
    </div>
  );
}
