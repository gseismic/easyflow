import { useEffect, useState } from "react";
import { part, usePolling } from "../api";
import RunMetadata from "../components/RunMetadata";
import Status from "../components/Status";
import TaskDetails from "../components/TaskDetails";
import TaskLog from "../components/TaskLog";

export default function HistoryRun({ dagId, runId }) {
  const [taskId, setTaskId] = useState("");
  const [attempt, setAttempt] = useState("");
  const identity = new URLSearchParams({ dag_id: dagId, run_id: runId });
  const run = usePolling(`/api/history/run?${identity}`, 3000);
  const dag = usePolling(`/api/dags/${part(dagId)}`);
  const logParams = new URLSearchParams(identity);
  if (taskId) logParams.set("task_id", taskId);
  if (attempt) logParams.set("attempt", attempt);
  const logs = usePolling(
    taskId ? `/api/history/task-log?${logParams}` : null,
    3000,
  );
  const taskIds = Object.keys(run.data?.tasks || {}).sort();
  useEffect(() => {
    if (!taskId && taskIds.length) setTaskId(taskIds[0]);
  }, [taskId, run.data]);
  const matchingTasks =
    run.data?.definition_matches === true && !dag.error ? dag.data?.tasks : null;
  const selectedTask =
    matchingTasks?.find((item) => item.task_id === taskId) ||
    (taskId ? { task_id: taskId } : null);
  return (
    <main className="page detail-page">
      <a className="back-link" href="#/history">
        ← 返回运行历史
      </a>
      <div className="detail-heading">
        <div>
          <div className="eyebrow">
            RUN <span>／ 运行详情</span>
          </div>
          <h1 className="detail-title">{dagId}</h1>
          <div className="history-run-heading">
            <span className="mono" title={runId}>
              {runId}
            </span>
            {run.data && !run.error && <Status value={run.data.state} />}
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
          {(run.data.definition_matches !== true || dag.error) && (
            <div className="notice warning">
              {dag.error && run.data.definition_matches === true
                ? "当前工作流信息加载失败；下面显示已保存的任务状态和日志。"
                : run.data.definition_matches === false
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
