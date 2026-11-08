const projectKinds = new Set(["all", "project", "unassigned", "conflict", "unknown"]);
const workflowStates = new Set(["all", "running", "failed", "paused"]);
const historyStates = new Set(["", "running", "queued", "success", "failed"]);

export const emptyFilters = () => ({ projectKind: "all", project: "", tags: [] });

function hashParams(hash) {
  return new URLSearchParams(hash.split("?", 2)[1] || "");
}

export function readListState(hash, page) {
  const params = hashParams(hash);
  let projectKind = params.get("project_kind") || "all";
  let project = params.get("project") || "";
  if (!projectKinds.has(projectKind) || (projectKind === "project" && !project)) {
    projectKind = "all";
  }
  if (projectKind !== "project") project = "";
  const states = page === "history" ? historyStates : workflowStates;
  const defaultState = page === "history" ? "" : "all";
  const state = params.get("state") || defaultState;
  return {
    filters: {
      projectKind,
      project,
      tags: [...new Set(params.getAll("tag").filter(Boolean))],
    },
    query: (params.get("q") || "").slice(0, 1000).trim(),
    state: states.has(state) ? state : defaultState,
  };
}

export function listHref(page, filters, view, dagId = "") {
  const params = new URLSearchParams();
  if (page === "history" && dagId) params.set("dag_id", dagId);
  const query = (view.draftComposing ? view.query : (view.draft ?? view.query)).trim();
  if (query) params.set("q", query);
  if (filters.projectKind !== "all") {
    params.set("project_kind", filters.projectKind);
    if (filters.projectKind === "project") params.set("project", filters.project);
  }
  filters.tags.forEach((tag) => params.append("tag", tag));
  if (view.state && view.state !== "all") params.set("state", view.state);
  const encoded = params.toString();
  return `${page === "history" ? "#/history" : "#/"}${encoded ? `?${encoded}` : ""}`;
}

export function sameFilters(left, right) {
  return left.projectKind === right.projectKind && left.project === right.project &&
    left.tags.length === right.tags.length && left.tags.every((tag, index) => tag === right.tags[index]);
}

export function workflowHref(dagId, tab = "run", view = {}) {
  const params = new URLSearchParams();
  if (tab !== "run") params.set("tab", tab);
  if (tab === "history") {
    const query = (view.draftComposing ? view.query : (view.draft ?? view.query) || "").trim();
    if (query) params.set("q", query);
    if (view.state) params.set("state", view.state);
  }
  const encoded = params.toString();
  return `#/dags/${encodeURIComponent(dagId)}${encoded ? `?${encoded}` : ""}`;
}

export function workflowHistoryRunHref(dagId, runId, view) {
  const params = new URLSearchParams({ dag_id: dagId, run_id: runId, from_workflow: "1" });
  const query = (view.draftComposing ? view.query : (view.draft ?? view.query) || "").trim();
  if (query) params.set("from_q", query);
  if (view.state) params.set("from_state", view.state);
  return `#/history/run?${params}`;
}
