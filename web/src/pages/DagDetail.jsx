import { useEffect, useState } from "react";
import { part, post, usePolling } from "../api";
import BackfillDialog from "../components/BackfillDialog";
import ClearDialog from "../components/ClearDialog";
import Graph from "../components/Graph";
import Status, { formatTime } from "../components/Status";
import TriggerDialog from "../components/TriggerDialog";

export default function DagDetail({ dagId, refreshOverview }) {
  const base = `/api/dags/${part(dagId)}`;
  const detail = usePolling(base);
  const [runId, setRunId] = useState("");
  const [taskId, setTaskId] = useState("");
  const [attempt, setAttempt] = useState("");
  const [dialog, setDialog] = useState("");
  const [notice, setNotice] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const dag = detail.data;
  useEffect(() => {
    if (!runId && dag?.runs?.length) setRunId(dag.runs[0].run_id);
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
      </div>
      {!dag.scheduler_running && (
        <div className="notice warning">
          调度器未连接。运行会在调度器启动后继续。
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
      <section className="card graph-card">
        <div className="section-heading">
          <div>
            <h2>任务依赖</h2>
            <p>选择任务查看运行状态和日志。</p>
          </div>
          <span className="graph-hint">当前 DAG 定义</span>
        </div>
        <Graph
          tasks={dag.tasks}
          states={run.data?.tasks}
          selected={taskId}
          onSelect={(id) => {
            setTaskId(id);
            setAttempt("");
          }}
        />
      </section>
      <div className="detail-grid">
        <section className="card runs-card">
          <div className="section-heading">
            <div>
              <h2>运行记录</h2>
              <p>最近 100 次，时间为本地时区。</p>
            </div>
            <button
              className="link-button"
              onClick={() => setDialog("backfill")}
            >
              回补预览
            </button>
          </div>
          {!dag.runs.length ? (
            <div className="empty compact">尚无运行。点击“触发运行”开始。</div>
          ) : (
            <div className="runs-list">
              {dag.runs.map((item) => (
                <button
                  key={item.run_id}
                  className={`run-row ${runId === item.run_id ? "selected" : ""}`}
                  onClick={() => {
                    setRunId(item.run_id);
                    setAttempt("");
                  }}
                >
                  <span>
                    <strong>{formatTime(item.logical_date)}</strong>
                    <small title={item.run_id}>
                      {item.run_type} · {item.run_id}
                    </small>
                  </span>
                  <Status value={item.state} />
                </button>
              ))}
            </div>
          )}
        </section>
        <section className="card log-card">
          <div className="section-heading">
            <div>
              <h2>任务日志</h2>
              <p>{taskId || "选择依赖图中的任务"}</p>
            </div>
            {runId && taskId && (
              <button
                className="button subtle small"
                onClick={() => setDialog("clear")}
              >
                预览重跑
              </button>
            )}
          </div>
          {!runId ? (
            <div className="empty compact">选择一次运行后查看任务日志。</div>
          ) : !taskId ? (
            <div className="empty compact">选择任务后查看日志。</div>
          ) : (
            <>
              <div className="log-toolbar">
                <span>
                  任务状态 <Status value={run.data?.tasks?.[taskId]} />
                </span>
                <label>
                  尝试{" "}
                  <select
                    value={attempt}
                    onChange={(event) => setAttempt(event.target.value)}
                  >
                    <option value="">最新</option>
                    {logs.data?.attempts?.map((item) => (
                      <option value={item.try_number} key={item.try_number}>
                        第 {item.try_number} 次
                      </option>
                    ))}
                  </select>
                </label>
              </div>
              {logs.error ? (
                <div className="notice error">日志加载失败：{logs.error}</div>
              ) : (
                <pre className="log-output">
                  {logs.data?.content == null
                    ? "日志尚未生成。"
                    : `${logs.data.truncated ? "[仅显示最后 1 MB]\n" : ""}${logs.data.content}`}
                </pre>
              )}
            </>
          )}
        </section>
      </div>
      {dialog === "trigger" && (
        <TriggerDialog
          base={base}
          paused={dag.paused}
          onClose={() => setDialog("")}
          onDone={triggered}
        />
      )}
      {dialog === "clear" && (
        <ClearDialog
          runBase={runBase}
          taskId={taskId}
          onClose={() => setDialog("")}
          onDone={cleared}
        />
      )}
      {dialog === "backfill" && (
        <BackfillDialog base={base} onClose={() => setDialog("")} />
      )}
    </main>
  );
}
