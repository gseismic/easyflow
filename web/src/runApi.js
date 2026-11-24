export function runDetailUrl(dagId, runId) {
  return `/api/history/run?${new URLSearchParams({ dag_id: dagId, run_id: runId })}`;
}

export function runDefinitionStatus(run, dag) {
  const comparable = run?.definition_hash && dag?.definition_hash;
  if (run?.definition_matches === false || (comparable && run.definition_hash !== dag.definition_hash)) {
    return "different";
  }
  if (run?.definition_pending) return "pending";
  return comparable && run.definition_matches === true ? "matching" : "unknown";
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
