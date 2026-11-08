import { useState } from "react";
import Status, { formatTime } from "../components/Status";
import { part } from "../api";

const filters = [
  ["all", "全部"],
  ["running", "运行中"],
  ["failed", "失败"],
  ["paused", "已暂停"],
];
const nextLabels = {
  manual: "手动触发",
  paused: "已暂停",
  completed: "无后续计划",
  unknown: "未知",
};

export default function DagList({ overview }) {
  const [query, setQuery] = useState("");
  const [filter, setFilter] = useState("all");
  const dags = overview.data?.dags || [];
  const failed = dags.filter(
    (dag) => dag.latest_run?.state === "failed",
  ).length;
  const running = dags.filter((dag) => dag.running_count > 0).length;
  const visible = dags.filter((dag) => {
    if (!dag.dag_id.toLowerCase().includes(query.trim().toLowerCase()))
      return false;
    if (filter === "paused") return dag.paused;
    if (filter === "running") return dag.running_count > 0;
    return filter === "all" || dag.latest_run?.state === filter;
  });
  return (
    <main className="page">
      <div className="eyebrow">
        WORKFLOW OVERVIEW <span>／ 工作流</span>
      </div>
      <div className="page-title-row">
        <div>
          <h1>你的工作流</h1>
          <p className="subtitle">从整体状态，到每一步的执行细节。</p>
        </div>
        <button className="button subtle" onClick={overview.refresh}>
          ↻ <span>刷新</span>
        </button>
      </div>
      {overview.error && (
        <div role="alert" className="notice error">
          连接失败，展示的可能是旧数据：{overview.error}{" "}
          <button className="link-button" onClick={overview.refresh}>
            重试
          </button>
        </div>
      )}
      {!overview.error && overview.data && !overview.data.scheduler_running && (
        <div className="notice warning">
          调度器离线，已创建的运行会等待调度器启动。
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
          <span>运行中</span>
          <strong>{overview.data ? running : "—"}</strong>
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
            <p>选择工作流，查看运行和任务详情。</p>
          </div>
          <span className="count">{visible.length} 个</span>
        </div>
        <div className="list-tools">
          <label className="search-box">
            <span aria-hidden="true">⌕</span>
            <input
              aria-label="搜索工作流"
              placeholder="搜索工作流"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
            />
          </label>
          <div className="filter-group" aria-label="筛选工作流">
            {filters.map(([value, label]) => (
              <button
                key={value}
                className={filter === value ? "filter active" : "filter"}
                aria-pressed={filter === value}
                onClick={() => setFilter(value)}
              >
                {label}
              </button>
            ))}
          </div>
        </div>
        {!overview.data && !overview.error ? (
          <div className="empty">正在加载工作流…</div>
        ) : !overview.data ? (
          <div className="empty">无法加载工作流，请检查服务连接。</div>
        ) : dags.length === 0 && overview.data.loading ? (
          <div className="empty">正在解析工作流…</div>
        ) : dags.length === 0 ? (
          <div className="empty">
            <span className="empty-symbol">◇</span>
            <strong>还没有工作流</strong>
            <span>
              将 DAG Python 文件放入启动时指定的目录，刷新后即可看到。
            </span>
          </div>
        ) : !visible.length ? (
          <div className="empty compact">
            没有匹配的工作流，请调整搜索或筛选条件。
          </div>
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>工作流</th>
                  <th>调度</th>
                  <th>下次计划</th>
                  <th>最近开始 / 入队</th>
                  <th>状态</th>
                  <th aria-label="查看详情" />
                </tr>
              </thead>
              <tbody>
                {visible.map((dag) => (
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
                      {["planned", "due"].includes(
                        dag.next_schedule?.status,
                      ) ? (
                        <>
                          {formatTime(dag.next_schedule.at)}
                          {dag.next_schedule.status === "due" && (
                            <small className="table-subtitle">待调度</small>
                          )}
                        </>
                      ) : (
                        nextLabels[dag.next_schedule?.status] || "未知"
                      )}
                    </td>
                    <td>
                      {dag.latest_run ? (
                        <>
                          <span>
                            {formatTime(
                              dag.latest_run.start_date ||
                                dag.latest_run.queued_at,
                            )}
                          </span>
                          <small className="table-subtitle">
                            {dag.latest_run.start_date
                              ? "开始时间"
                              : "入队时间"}
                          </small>
                        </>
                      ) : (
                        <span className="muted">尚无运行</span>
                      )}
                    </td>
                    <td>
                      <Status value={dag.latest_run?.state} />
                      {dag.running_count > 0 && (
                        <small className="table-subtitle">
                          {dag.running_count} 次运行中
                        </small>
                      )}
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
