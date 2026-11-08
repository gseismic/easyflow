import Status, { formatTime } from "../components/Status";
import { part } from "../api";

export default function DagList({ overview }) {
  const dags = overview.data?.dags || [];
  const failed = dags.filter(
    (dag) => dag.latest_run?.state === "failed",
  ).length;
  const active = dags.filter((dag) => !dag.paused).length;
  return (
    <main className="page">
      <div className="eyebrow">
        WORKFLOW OVERVIEW <span>／ 工作流</span>
      </div>
      <div className="page-title-row">
        <div>
          <h1>你的工作流</h1>
          <p className="subtitle">查看调度状态，快速找到需要关注的运行。</p>
        </div>
        <button className="button subtle" onClick={overview.refresh}>
          ↻ <span>刷新</span>
        </button>
      </div>
      {overview.data && !overview.data.scheduler_running && (
        <div className="notice warning">
          调度器未连接。触发的运行会等待调度器启动后执行。
        </div>
      )}
      {overview.error && (
        <div role="alert" className="notice error">
          加载失败：{overview.error}{" "}
          <button className="link-button" onClick={overview.refresh}>
            重试
          </button>
        </div>
      )}
      {overview.data?.issues?.length > 0 && (
        <div role="alert" className="notice error">
          <strong>部分 DAG 无法加载</strong>
          {overview.data.issues.map((issue) => (
            <div key={issue}>{issue}</div>
          ))}
        </div>
      )}
      <section className="summary-strip" aria-label="工作流概览">
        <div>
          <span>工作流</span>
          <strong>{overview.data ? dags.length : "—"}</strong>
        </div>
        <div>
          <span>调度已启用</span>
          <strong>{overview.data ? active : "—"}</strong>
        </div>
        <div>
          <span>最近运行失败</span>
          <strong className={failed ? "danger-text" : ""}>
            {overview.data ? failed : "—"}
          </strong>
        </div>
      </section>
      <section className="card list-card">
        <div className="section-heading">
          <div>
            <h2>全部工作流</h2>
            <p>按名称选择工作流，查看依赖和运行记录。</p>
          </div>
          <span className="count">{dags.length} 个</span>
        </div>
        {!overview.data && !overview.error ? (
          <div className="empty">正在加载工作流…</div>
        ) : dags.length === 0 && overview.data?.loading ? (
          <div className="empty">正在解析工作流…</div>
        ) : dags.length === 0 ? (
          <div className="empty">
            <span className="empty-symbol">◇</span>
            <strong>还没有工作流</strong>
            <span>
              将 DAG Python 文件放入启动时指定的目录，刷新后即可看到。
            </span>
          </div>
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>工作流</th>
                  <th>调度</th>
                  <th>最近运行</th>
                  <th>状态</th>
                  <th aria-label="查看详情" />
                </tr>
              </thead>
              <tbody>
                {dags.map((dag) => (
                  <tr key={dag.dag_id}>
                    <td>
                      <a
                        className="dag-link"
                        href={`#/dags/${part(dag.dag_id)}`}
                      >
                        <span className="dag-icon">↗</span>
                        <span>
                          <strong>{dag.dag_id}</strong>
                          <small>
                            {dag.task_count} 个任务 ·{" "}
                            {dag.paused ? "已暂停" : "已启用"}
                          </small>
                        </span>
                      </a>
                    </td>
                    <td className="mono">{dag.schedule || "手动触发"}</td>
                    <td>
                      {dag.latest_run ? (
                        formatTime(dag.latest_run.logical_date)
                      ) : (
                        <span className="muted">尚无运行</span>
                      )}
                    </td>
                    <td>
                      <Status value={dag.latest_run?.state} />
                    </td>
                    <td>
                      <a
                        className="row-arrow"
                        href={`#/dags/${part(dag.dag_id)}`}
                        aria-label={`查看 ${dag.dag_id}`}
                      >
                        →
                      </a>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
      <footer className="footer">
        EASYFLOW <span>轻量、清晰的工作流管理</span>
      </footer>
    </main>
  );
}
