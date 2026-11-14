export function runDetailUrl(dagId, runId) {
  return `/api/history/run?${new URLSearchParams({ dag_id: dagId, run_id: runId })}`;
}

export function taskLogUrl(dagId, runId, taskId, attempt = "") {
  const params = new URLSearchParams({ dag_id: dagId, run_id: runId, task_id: taskId });
  if (attempt) params.set("attempt", attempt);
  return `/api/history/task-log?${params}`;
}

export function clearRunUrl(dagId, runId, preview = false) {
  const params = new URLSearchParams({ dag_id: dagId, run_id: runId });
  return `/api/runs/${preview ? "clear-preview" : "clear"}?${params}`;
}
