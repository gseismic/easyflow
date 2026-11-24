import { useEffect, useState } from "react";
import { part, usePolling } from "../api";
import { dagDetailUrl } from "../dagApi";
import RunMetadata from "../components/RunMetadata";
import Status from "../components/Status";
import TaskDetails from "../components/TaskDetails";
import TaskLog from "../components/TaskLog";
import { runDetailUrl, taskLogUrl } from "../runApi";

export default function HistoryRun({ dagId, runId, requestedTaskId, requestedAttempt, backHref, backLabel = "返回运行历史", onWorkflowMetadata }) {
  const [taskId, setTaskId] = useState("");
  const [attempt, setAttempt] = useState("");
  const run = usePolling(runDetailUrl(dagId, runId), 3000);
  const dag = usePolling(dagDetailUrl(dagId));
  useEffect(() => {
    if (dag.data && !dag.error) onWorkflowMetadata?.(dag.data);
    else if (dag.error) onWorkflowMetadata?.({ dag_id: dagId, unavailable: true });
  }, [dag.data, dag.error, dagId, onWorkflowMetadata]);
  const logs = usePolling(
    taskId ? taskLogUrl(dagId, runId, taskId, attempt) : null,
    3000,
  );
  const taskIds = Object.keys(run.data?.tasks || {}).sort();
  useEffect(() => {
    if (!taskId && taskIds.length) {
      const target = taskIds.includes(requestedTaskId) ? requestedTaskId :
        taskIds.find((id) => run.data.tasks[id] === "failed") || taskIds[0];
      setTaskId(target);
      setAttempt(target === requestedTaskId ? requestedAttempt || "" : "");
    }
  }, [taskId, run.data, requestedTaskId, requestedAttempt]);
  const differentDefinition = Boolean(dag.data?.definition_hash && run.data?.definition_hash &&
    dag.data.definition_hash !== run.data.definition_hash);
  const matchesDefinition = run.data?.definition_matches === true && !dag.error &&
    Boolean(run.data.definition_hash) && run.data.definition_hash === dag.data?.definition_hash;
  const selectedTask =
    (matchesDefinition && dag.data.tasks?.find((item) => item.task_id === taskId)) ||
    (taskId ? { task_id: taskId } : null);
  return (
    <main className="page detail-page">
      <div className="detail-heading compact-heading">
        <div className="detail-identity-row">
          <a className="back-link detail-back" href={backHref} aria-label={backLabel} title={backLabel}>←</a>
          <div className="detail-identity">
          <h1 className="detail-title">{dagId}</h1>
          <div className="history-run-heading">
            <span className="mono" title={runId}>
              {runId}
            </span>
            {run.data && !run.error && <Status value={run.data.state} />}
          </div>
          </div>
        </div>
        {dag.data && !dag.error && (
          <a className="button subtle" href={`#/dags/${part(dagId)}`}>
            查看工作流
          </a>
        )}
      </div>
      {run.error && (
        <div role="alert" className="notice error">
          运行信息加载失败：{run.error}
        </div>
      )}
      {!run.data && !run.error && (
        <div className="empty">正在加载运行详情…</div>
      )}
      {run.data && !run.error && (
        <>
          <section className="card run-overview">
            <div className="section-heading">
              <div>
                <h2>运行信息</h2>
                <p>时间显示为浏览器本地时区</p>
              </div>
            </div>
            <RunMetadata run={run.data} />
          </section>
          {!matchesDefinition && (
            <div className="notice warning">
              {run.data.definition_pending
                ? "当前 DAG 定义仍在解析，匹配状态稍后会自动更新。"
                : dag.error && run.data.definition_matches === true
                  ? "当前工作流信息加载失败；下面显示已保存的任务状态和日志。"
                  : run.data.definition_matches === false || differentDefinition
                    ? "工作流定义已变化；历史上下游无法确认，下面显示已保存的任务状态和日志。"
                    : "无法确认历史工作流定义；下面显示已保存的任务状态和日志。"}
            </div>
          )}
          <section className="card history-tasks">
            <div className="section-heading">
              <div>
                <h2>运行任务</h2>
                <p>选择任务，查看执行详情和日志。</p>
              </div>
              <span className="count">{taskIds.length} 个</span>
            </div>
            {!taskIds.length ? (
              <div className="empty compact">这次运行没有已保存的任务记录。</div>
            ) : (
              <div className="history-task-list">
                {taskIds.map((id) => (
                  <button
                    key={id}
                    className={
                      taskId === id ? "history-task selected" : "history-task"
                    }
                    aria-pressed={taskId === id}
                    onClick={() => {
                      setTaskId(id);
                      setAttempt("");
                    }}
                  >
                    <span title={id}>{id}</span>
                    <Status value={run.data.tasks[id]} />
                  </button>
                ))}
              </div>
            )}
          </section>
          {taskId === requestedTaskId && attempt && attempt === requestedAttempt && (
            <p className="task-hint">已定位到 {taskId} 的第 {attempt} 次尝试日志；任务状态与时间显示最近一次尝试的结果。</p>
          )}
          <div className="detail-grid">
            <TaskDetails
              task={selectedTask}
              detail={run.data.task_details?.[taskId]}
              hasRun
            />
            <TaskLog
              logs={logs}
              taskId={taskId}
              runId={runId}
              attempt={attempt}
              setAttempt={setAttempt}
            />
          </div>
        </>
      )}
    </main>
  );
}
