function dagUrl(dagId, action) {
  return `/api/workflows/${action}?${new URLSearchParams({ dag_id: dagId })}`;
}

export const dagDetailUrl = (dagId) => dagUrl(dagId, "detail");
export const pauseDagUrl = (dagId) => dagUrl(dagId, "pause");
export const triggerDagUrl = (dagId) => dagUrl(dagId, "trigger");
export const backfillPreviewUrl = (dagId) => dagUrl(dagId, "backfill-preview");

export function dagSourceUrl(dagId, runId = "") {
  const params = new URLSearchParams({ dag_id: dagId });
  if (runId) params.set("run_id", runId);
  return `/api/workflows/source?${params}`;
}
