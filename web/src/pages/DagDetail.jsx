import { useEffect, useState } from "react";
import { post, usePolling } from "../api";
import { dagDetailUrl, pauseDagUrl } from "../dagApi";
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
import WorkflowUptime from "../components/WorkflowUptime";
import WorkflowHeader from "../components/WorkflowHeader";
import { workflowHref } from "../listRoute";
import { runDetailUrl, taskLogUrl } from "../runApi";
import WorkflowRunHistory from "./WorkflowRunHistory";

const runStates = {
  queued: "排队中",
  running: "运行中",
  success: "成功",
  failed: "失败",
};

export default function DagDetail({ dagId, refreshOverview, isAdmin, tab: requestedTab, historyView, setHistoryView, backHref, onWorkflowMetadata }) {
  const detail = usePolling(dagDetailUrl(dagId));
  const [runId, setRunId] = useState("");
  const [taskId, setTaskId] = useState("");
  const [attempt, setAttempt] = useState("");
  const tab = requestedTab === "source" && !isAdmin ? "run" : requestedTab;
  const [dialog, setDialog] = useState(null);
  const routeHash = window.location.hash;
  const activeDialog = dialog?.routeHash === routeHash ? dialog : null;
  const openDialog = (kind) => setDialog({ kind, routeHash });
  const closeDialog = () => setDialog((current) => current === activeDialog ? null : current);
  useEffect(() => {
    setDialog((current) => current?.routeHash === routeHash ? current : null);
  }, [routeHash]);
  const [notice, setNotice] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [refreshVersion, setRefreshVersion] = useState(0);
  const dag = detail.data;
  useEffect(() => {
    if (dag && !detail.error) onWorkflowMetadata?.(dag);
    else if (detail.error) onWorkflowMetadata?.({ dag_id: dagId, unavailable: true });
  }, [dag, detail.error, dagId, onWorkflowMetadata]);
  useEffect(() => {
    if (!runId && dag?.runs?.length) {
      setRunId(
        (dag.runs.find((item) => item.state === "running") || dag.runs[0])
          .run_id,
      );
    }
  }, [dag, runId]);
  const runPath = ["run", "tasks", "logs"].includes(tab) && runId ? runDetailUrl(dagId, runId) : null;
  const run = usePolling(runPath, 3000);
  const hasTaskRecord = Boolean(run.data && Object.hasOwn(run.data.tasks || {}, taskId));
  const logPath =
    tab === "logs" && runPath && hasTaskRecord
      ? taskLogUrl(dagId, runId, taskId, attempt)
      : null;
  const logs = usePolling(logPath, 3000);
  useEffect(() => {
    if (!["run", "tasks", "logs"].includes(tab) || (runId && !run.data && taskId)) return;
    const ids = [...new Set([...(dag?.tasks || []).map((task) => task.task_id), ...Object.keys(run.data?.tasks || {})])];
    if (ids.length && !ids.includes(taskId)) { setTaskId(ids[0]); setAttempt(""); }
  }, [dag?.tasks, run.data, taskId, runId, tab]);
  const refresh = () => {
    detail.refresh();
    run.refresh();
    logs.refresh();
    refreshOverview();
    setRefreshVersion((value) => value + 1);
  };
  const togglePause = async () => {
    setBusy(true);
    setError("");
    try {
      const result = await post(pauseDagUrl(dagId), { paused: !dag.paused });
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
    if (window.location.hash !== activeDialog.routeHash) {
      refresh();
      return;
    }
    setRunId(result.run_id);
    setAttempt("");
    closeDialog();
    window.location.hash = workflowHref(dagId);
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
    closeDialog();
    setNotice(`已清理 ${result.to_clear.length} 个任务，等待调度器执行。`);
    refresh();
  };
  if (!dag)
    return (
      <main className="page detail-page" aria-busy={!detail.error}>
        <WorkflowHeader dagId={dagId} tab={tab} isAdmin={isAdmin} historyView={historyView} backHref={backHref} />
        <section className="card detail-load-state">
          {detail.error ? <div className="notice error" role="alert">
            加载失败：{detail.error}{" "}<button className="link-button" onClick={detail.refresh}>重试加载工作流</button>
          </div> : <>
            <p role="status">正在加载工作流…</p>
            <div className="detail-load-placeholder" aria-hidden="true"><div /><div /><div /></div>
          </>}
        </section>
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
      <WorkflowHeader dagId={dagId} dag={dag} tab={tab} isAdmin={isAdmin} historyView={historyView} backHref={backHref}
        schedulerOffline={!detail.error && !dag.scheduler_running}
        actions={<>
            <button className="button subtle" onClick={refresh}>↻ 刷新</button>
            {isAdmin && <>
            <button
              className="button subtle"
              disabled={busy}
              onClick={togglePause}
            >
              {dag.paused ? "恢复调度" : "暂停调度"}
            </button>
            <button
              className="button subtle"
              onClick={() => openDialog("trigger")}
            >
              ＋ 触发运行
            </button>
            </>}
        </>}>
      {detail.error && (
        <div role="alert" className="notice error">
          工作流信息更新失败：{detail.error}
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
      </WorkflowHeader>
      {(tab === "run" || tab === "history") && <WorkflowUptime dagId={dagId} refreshVersion={refreshVersion} />}
      {tab === "history" && (
        <WorkflowRunHistory dagId={dagId} view={historyView} setView={setHistoryView} />
      )}
      {tab === "schedule" && <ScheduleDetails dag={dag} />}
      {tab === "source" && isAdmin && <SourceCode dagId={dagId} runId={runId} />}
      {["run", "tasks", "logs"].includes(tab) && (
        <>
          <section className="card run-overview">
            <div className="section-heading">
              <div>
                <h2>运行记录</h2>
                <p>最近 100 次 · 时间显示为浏览器本地时区</p>
              </div>
              <div className="section-actions">
                <a className="link-button" href={workflowHref(dagId, "history", historyView)}>
                  查看全部运行
                </a>
                {isAdmin && (
                  <button className="link-button" onClick={() => openDialog("backfill")}>
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
            {tab === "run" && run.data && !run.error && <RunMetadata run={run.data} />}
          </section>
          {tab === "tasks" && <>
          {run.data && !run.error && run.data.definition_matches !== true && (
            <div className="notice warning">
              {run.data.definition_pending
                ? "当前 DAG 定义仍在解析，匹配状态稍后会自动更新。"
                : run.data.definition_matches === false
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
          <div className="workspace-task-details">
            <TaskDetails
              task={selectedTask}
              detail={selectedDetail}
              hasRun={Boolean(runId)}
            />
            <div className="task-selection-actions"><a className="button subtle" href={workflowHref(dagId, "logs")}>查看所选任务日志 →</a></div>
          </div>
          </>}
          {tab === "logs" && <>
            <section className="card task-selector-card" aria-label="选择日志任务">
              <label>任务
                <select aria-label="选择日志任务" value={taskId} onChange={(event) => { setTaskId(event.target.value); setAttempt(""); }}>
                  {dag.tasks.length || extraTasks.length ? <>
                    {dag.tasks.map((task) => <option key={task.task_id} value={task.task_id}>{task.task_id}</option>)}
                    {extraTasks.map((id) => <option key={id} value={id}>{id}（仅历史记录）</option>)}
                  </> : <option value="">暂无任务</option>}
                </select>
              </label>
              <Status value={selectedDetail?.state} />
            </section>
            <TaskLog
              logs={logs}
              unavailableMessage={run.error && !run.data ? "运行信息加载失败，暂时无法确定该次任务日志，请刷新重试。" :
                run.data && taskId && !hasTaskRecord ? "该次运行没有保存这个任务的记录，请选择本次运行中存在的任务。" : ""}
              taskId={taskId}
              runId={runId}
              attempt={attempt}
              setAttempt={setAttempt}
              isAdmin={isAdmin}
              onClear={() => openDialog("clear")}
            />
          </>}
        </>
      )}
      {isAdmin && activeDialog?.kind === "trigger" && (
        <TriggerDialog
          dagId={dagId}
          paused={dag.paused}
          onClose={closeDialog}
          onDone={triggered}
        />
      )}
      {isAdmin && activeDialog?.kind === "clear" && (
        <ClearDialog
          dagId={dagId}
          runId={runId}
          taskId={taskId}
          onClose={closeDialog}
          onDone={cleared}
        />
      )}
      {isAdmin && activeDialog?.kind === "backfill" && (
        <BackfillDialog dagId={dagId} onClose={closeDialog} />
      )}
    </main>
  );
}
