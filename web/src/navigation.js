import { listHref } from "./listRoute.js";

export const navigationBatchSize = 50;

export function navigationGroupKey(group) {
  return JSON.stringify([group.project_kind, group.project || ""]);
}

export function navigationGroupLabel(group) {
  return group.project_kind === "project" ? group.project :
    { unassigned: "未分组", conflict: "项目异常", unknown: "项目未知" }[group.project_kind] || "项目未知";
}

export function navigationGroups(data, query = "") {
  return (data?.groups || []).filter((group) => query ? group.matched > 0 : group.total > 0)
    .sort((left, right) => {
      const order = { project: 0, unassigned: 1, conflict: 2, unknown: 3 };
      return (order[left.project_kind] ?? 3) - (order[right.project_kind] ?? 3) ||
        navigationGroupLabel(left).localeCompare(navigationGroupLabel(right), "zh-CN");
    });
}

export function navigationWorkflowsUrl(group, query, cursor = null) {
  const params = new URLSearchParams({ limit: String(navigationBatchSize), project_kind: group.project_kind });
  if (group.project_kind === "project") params.set("project", group.project);
  if (query) params.set("q", query);
  if (cursor) params.set("cursor", cursor);
  return `/api/workflows?${params}`;
}

export function navigationProjectHref(group, filters, view) {
  return listHref("dags", { ...filters, projectKind: group.project_kind, project: group.project || "" }, view);
}

export function navigationWorkflowStatus(workflow) {
  const state = workflow.running_count > 0 ? "running" : workflow.latest_run?.state || "none";
  const label = { running: "运行中", queued: "排队", success: "成功", failed: "失败", none: "未运行" }[state] || "未知";
  return { state, label: `${workflow.paused ? "暂停 · " : ""}${label}` };
}

export function readNavigationState(storage, key) {
  const fallback = { query: "", expanded: [], scrollTop: 0 };
  try {
    const value = JSON.parse(storage?.getItem(key) || "null");
    if (!value || value.version !== 1) return fallback;
    return {
      query: typeof value.query === "string" ? value.query.slice(0, 1000) : "",
      expanded: Array.isArray(value.expanded) ? [...new Set(value.expanded.filter((item) => {
        if (typeof item !== "string" || item.length > 32768) return false;
        try {
          const [kind, project] = JSON.parse(item);
          return ["project", "unassigned", "conflict", "unknown"].includes(kind) && typeof project === "string";
        } catch { return false; }
      }))] : [],
      scrollTop: Number.isFinite(value.scrollTop) ? Math.max(0, Math.min(10000000, value.scrollTop)) : 0,
    };
  } catch { return fallback; }
}

export function saveNavigationState(storage, key, value) {
  try {
    storage?.setItem(key, JSON.stringify({ version: 1, ...value, expanded: [...value.expanded] }));
  } catch { /* 禁用存储或超出配额时，仍保留当前页面中的导航状态。 */ }
}
