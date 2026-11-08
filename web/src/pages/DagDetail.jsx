import { useEffect, useState } from "react";
import { part, post, usePolling } from "../api";
import BackfillDialog from "../components/BackfillDialog";
import ClearDialog from "../components/ClearDialog";
import Graph from "../components/Graph";
import RunMetadata, { runTypeLabel } from "../components/RunMetadata";
import ScheduleDetails from "../components/ScheduleDetails";
import SourceCode from "../components/SourceCode";
import Status, { formatTime } from "../components/Status";
import TaskDetails from "../components/TaskDetails";
import TaskLog from "../components/TaskLog";
import TriggerDialog from "../components/TriggerDialog";

const runStates = {
  queued: "排队中",
  running: "运行中",
  success: "成功",
  failed: "失败",
};

export default function DagDetail({ dagId, refreshOverview, isAdmin, onViewAllRuns }) {
  const base = `/api/dags/${part(dagId)}`;
  const detail = usePolling(base);
  const [runId, setRunId] = useState("");
  const [taskId, setTaskId] = useState("");
  const [attempt, setAttempt] = useState("");
  const [tab, setTab] = useState("run");
  const [dialog, setDialog] = useState("");
  const [notice, setNotice] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const dag = detail.data;
  useEffect(() => {
    if (!runId && dag?.runs?.length) {
      setRunId(
        (dag.runs.find((item) => item.state === "running") || dag.runs[0])
          .run_id,
      );
    }
  }, [dag, runId]);
  useEffect(() => {
    if (!taskId && dag?.tasks?.length) setTaskId(dag.tasks[0].task_id);
  }, [dag, taskId]);
  const runBase = runId ? `${base}/runs/${part(runId)}` : null;
  const run = usePolling(runBase, 3000);
  const logPath =
    runBase && taskId
      ? `${runBase}/tasks/${part(taskId)}/logs${attempt ? `?attempt=${part(attempt)}` : ""}`
      : null;
  const logs = usePolling(logPath, 3000);
  const refresh = () => {
    detail.refresh();
    run.refresh();
    logs.refresh();
    refreshOverview();
  };
  const togglePause = async () => {
    setBusy(true);
    setError("");
    try {
      const result = await post(`${base}/pause`, { paused: !dag.paused });
      setNotice(
        result.paused
          ? "已暂停：正在运行的任务会完成，其余任务等待恢复。"
          : "已恢复调度。",
      );
      refresh();
    } catch (reason) {
      setError(reason.message);
    } finally {
      setBusy(false);
    }
  };
  const triggered = (result) => {
    setRunId(result.run_id);
    setDialog("");
    setTab("run");
    setNotice(
      result.paused
        ? "已创建运行；DAG 恢复后开始执行。"
        : result.scheduler_running
          ? "运行已创建，等待调度器执行。"
          : "运行已创建；调度器启动后开始执行。",
    );
    refresh();
  };
  const cleared = (result) => {
    setDialog("");
    setNotice(`已清理 ${result.to_clear.length} 个任务，等待调度器执行。`);
    refresh();
  };
  if (!dag)
    return (
      <main className="page">
        <a className="back-link" href="#/">
          ← 所有工作流
        </a>
        <div className="empty">
          {detail.error ? `加载失败：${detail.error}` : "正在加载工作流…"}
        </div>
      </main>
    );
  const selectedTask =
    dag.tasks.find((item) => item.task_id === taskId) ||
    (taskId ? { task_id: taskId } : null);
  const selectedDetail = run.error
    ? undefined
    : run.data?.task_details?.[taskId];
  const extraTasks = Object.keys(run.data?.tasks || {}).filter(
    (id) => !dag.tasks.some((task) => task.task_id === id),
  );
  const states = runId
    ? run.error
      ? undefined
      : run.data?.tasks
    : Object.fromEntries(dag.tasks.map((task) => [task.task_id, null]));
  return (
    <main className="page detail-page">
      <a className="back-link" href="#/">
        ← 所有工作流
      </a>
      <div className="detail-heading">
        <div>
          <div className="eyebrow">
            WORKFLOW <span>／ 工作流详情</span>
          </div>
          <h1 className="detail-title">{dag.dag_id}</h1>
          <div className="meta">
            <span>{dag.paused ? "● 已暂停" : "● 已启用"}</span>
            <span>
              调度：<b className="mono">{dag.schedule || "手动触发"}</b>
            </span>
            <span>{dag.task_count} 个任务</span>
          </div>
        </div>
        {isAdmin && (
          <div className="actions">
            <button
              className="button subtle"
              disabled={busy}
              onClick={togglePause}
            >
              {dag.paused ? "恢复调度" : "暂停调度"}
            </button>
            <button
              className="button primary"
              onClick={() => setDialog("trigger")}
            >
              ＋ 触发运行
            </button>
          </div>
        )}
      </div>
      {detail.error && (
        <div role="alert" className="notice error">
          工作流信息更新失败：{detail.error}
        </div>
      )}
      {!detail.error && !dag.scheduler_running && (
        <div className="notice warning">
          调度器离线，运行会在调度器启动后继续。
        </div>
      )}
      {notice && (
        <div role="status" className="notice success">
          {notice}
          <button className="link-button" onClick={() => setNotice("")}>
            关闭
          </button>
        </div>
      )}
      {error && (
        <div role="alert" className="notice error">
          {error}
        </div>
      )}
      <nav className="detail-tabs" aria-label="工作流视图">
        {[
          ["run", "运行与任务"],
          ["schedule", "调度详情"],
          ...(isAdmin ? [["source", "源码"]] : []),
        ].map(([value, label]) => (
          <button
            key={value}
            className={tab === value ? "detail-tab active" : "detail-tab"}
            aria-current={tab === value ? "page" : undefined}
            onClick={() => setTab(value)}
          >
            {label}
          </button>
        ))}
      </nav>
      {tab === "schedule" && <ScheduleDetails dag={dag} />}
      {tab === "source" && isAdmin && <SourceCode base={base} runId={runId} />}
      {tab === "run" && (
        <>
          <section className="card run-overview">
            <div className="section-heading">
              <div>
                <h2>运行记录</h2>
                <p>最近 100 次 · 时间显示为浏览器本地时区</p>
              </div>
              <div className="section-actions">
                <a className="link-button" href={`#/history?dag_id=${part(dagId)}`} onClick={onViewAllRuns}>
                  查看全部运行
                </a>
                {isAdmin && (
                  <button className="link-button" onClick={() => setDialog("backfill")}>
                    回补预览
                  </button>
                )}
              </div>
            </div>
            {!dag.runs.length && !runId ? (
              <div className="empty compact">
                尚无运行。
                {isAdmin
                  ? "可通过“触发运行”开始。"
                  : "管理员触发后可在这里查看。"}
              </div>
            ) : (
              <div className="run-selector">
                <label>
                  选择运行{" "}
                  <select
                    aria-label="选择运行"
                    value={runId}
                    onChange={(event) => {
                      setRunId(event.target.value);
                      setAttempt("");
                    }}
                  >
                    {!dag.runs.some((item) => item.run_id === runId) && (
                      <option value={runId}>正在加载新运行…</option>
                    )}
                    {dag.runs.map((item) => (
                      <option key={item.run_id} value={item.run_id}>
                        {formatTime(item.start_date || item.queued_at)} ·{" "}
                        {runTypeLabel(item.run_type)} ·{" "}
                        {runStates[item.state] || item.state}
                      </option>
                    ))}
                  </select>
                </label>
                {run.data && !run.error && <Status value={run.data.state} />}
              </div>
            )}
            {run.error && (
              <div className="notice error" role="alert">
                运行信息更新失败，状态未知：{run.error}
              </div>
            )}
            {run.data && !run.error && <RunMetadata run={run.data} />}
          </section>
          {run.data && !run.error && run.data.definition_matches !== true && (
            <div className="notice warning">
              {run.data.definition_matches === false
                ? "此运行的定义与当前 DAG 不同。依赖图展示当前定义，历史任务请参考下方列表。"
                : "无法确认此运行与当前 DAG 定义是否一致；依赖图展示当前定义。"}
            </div>
          )}
          <section className="card graph-card">
            <div className="section-heading">
              <div>
                <h2>任务依赖</h2>
                <p>点击任务，查看上下游、执行状态和日志。</p>
              </div>
              <span className="graph-hint">当前 DAG 定义</span>
            </div>
            <Graph
              tasks={dag.tasks}
              states={states}
              selected={taskId}
              onSelect={(id) => {
                setTaskId(id);
                setAttempt("");
              }}
            />
            {extraTasks.length > 0 && (
              <div className="historical-tasks">
                <strong>仅在历史运行中存在</strong>
                {extraTasks.map((id) => (
                  <button
                    key={id}
                    className="link-button"
                    onClick={() => {
                      setTaskId(id);
                      setAttempt("");
                    }}
                  >
                    {id} <Status value={run.data.tasks[id]} />
                  </button>
                ))}
              </div>
            )}
          </section>
          <div className="detail-grid">
            <TaskDetails
              task={selectedTask}
              detail={selectedDetail}
              hasRun={Boolean(runId)}
            />
            <TaskLog
              logs={logs}
              taskId={taskId}
              runId={runId}
              attempt={attempt}
              setAttempt={setAttempt}
              isAdmin={isAdmin}
              onClear={() => setDialog("clear")}
            />
          </div>
        </>
      )}
      {isAdmin && dialog === "trigger" && (
        <TriggerDialog
          base={base}
          paused={dag.paused}
          onClose={() => setDialog("")}
          onDone={triggered}
        />
      )}
      {isAdmin && dialog === "clear" && (
        <ClearDialog
          runBase={runBase}
          taskId={taskId}
          onClose={() => setDialog("")}
          onDone={cleared}
        />
      )}
      {isAdmin && dialog === "backfill" && (
        <BackfillDialog base={base} onClose={() => setDialog("")} />
      )}
    </main>
  );
}
