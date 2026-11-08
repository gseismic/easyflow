import { useState } from "react";
import { usePolling } from "../api";
import { runTypeLabel } from "../components/RunMetadata";
import Status, { formatDuration, formatTime } from "../components/Status";

const filters = [
  ["", "全部"],
  ["running", "运行中"],
  ["queued", "排队中"],
  ["success", "成功"],
  ["failed", "失败"],
];

export default function HistoryList({ view, setView }) {
  const [search, setSearch] = useState(view.query);
  const params = new URLSearchParams({ limit: "25" });
  if (view.query) params.set("dag", view.query);
  if (view.state) params.set("state", view.state);
  if (view.cursors[view.page]) params.set("cursor", view.cursors[view.page]);
  const history = usePolling(`/api/history/runs?${params}`, 5000);
  const rows = history.data?.runs || [];
  const setFilter = (patch) =>
    setView((current) => ({ ...current, ...patch, cursors: [null], page: 0 }));
  const nextPage = () => {
    if (!history.data?.next_cursor) return;
    setView((current) => ({
      ...current,
      cursors: [
        ...current.cursors.slice(0, current.page + 1),
        history.data.next_cursor,
      ],
      page: current.page + 1,
    }));
  };
  return (
    <main className="page">
      <div className="eyebrow">
        RUN HISTORY <span>／ 运行历史</span>
      </div>
      <div className="page-title-row">
        <div>
          <h1>运行历史</h1>
          <p className="subtitle">
            所有工作流的执行记录，按入队时间从近到远排列。
          </p>
        </div>
        <button className="button subtle" onClick={history.refresh}>
          ↻ <span>刷新</span>
        </button>
      </div>
      {history.error && (
        <div className="notice error" role="alert">
          运行历史加载失败：{history.error}{" "}
          <button className="link-button" onClick={history.refresh}>
            重试
          </button>
        </div>
      )}
      <section className="card history-card">
        <div className="section-heading">
          <div>
            <h2>全部运行</h2>
            <p>时间显示为浏览器本地时区 · 每页 25 条</p>
          </div>
        </div>
        <div className="list-tools">
          <form
            className="history-search"
            onSubmit={(event) => {
              event.preventDefault();
              setFilter({ query: search.trim() });
            }}
          >
            <label className="search-box">
              <span aria-hidden="true">⌕</span>
              <input
                aria-label="搜索运行的工作流"
                placeholder="搜索工作流"
                maxLength={1000}
                value={search}
                onChange={(event) => setSearch(event.target.value)}
              />
            </label>
            <button className="button subtle small" type="submit">
              搜索
            </button>
          </form>
          <div className="filter-group" aria-label="筛选运行状态">
            {filters.map(([value, label]) => (
              <button
                key={value}
                className={view.state === value ? "filter active" : "filter"}
                aria-pressed={view.state === value}
                onClick={() => setFilter({ state: value })}
              >
                {label}
              </button>
            ))}
          </div>
        </div>
        {!history.data && !history.error ? (
          <div className="empty">正在加载运行历史…</div>
        ) : !history.data ? (
          <div className="empty">无法加载运行历史，请重试。</div>
        ) : !rows.length ? (
          <div className="empty compact">
            <span className="empty-symbol">◇</span>
            <strong>
              {view.query || view.state ? "没有匹配的运行" : "还没有运行记录"}
            </strong>
            <span>
              {view.query || view.state
                ? "请调整工作流名称或状态筛选。"
                : "工作流开始执行后，记录会显示在这里。"}
            </span>
          </div>
        ) : (
          <div className="table-wrap">
            <table className="history-table">
              <thead>
                <tr>
                  <th>工作流</th>
                  <th>状态</th>
                  <th>触发方式</th>
                  <th>入队时间</th>
                  <th>执行耗时</th>
                  <th aria-label="查看详情" />
                </tr>
              </thead>
              <tbody>
                {rows.map((run) => {
                  const query = new URLSearchParams({
                    dag_id: run.dag_id,
                    run_id: run.run_id,
                  });
                  return (
                    <tr key={JSON.stringify([run.dag_id, run.run_id])}>
                      <td>
                        <a
                          className="history-run-link"
                          href={`#/history/run?${query}`}
                          title={run.dag_id}
                        >
                          {run.dag_id}
                        </a>
                        <small
                          className="table-subtitle history-run-id"
                          title={run.run_id}
                        >
                          {run.run_id}
                        </small>
                      </td>
                      <td>
                        <Status value={run.state} />
                      </td>
                      <td>{runTypeLabel(run.run_type)}</td>
                      <td>{formatTime(run.queued_at)}</td>
                      <td>
                        {formatDuration(
                          run.start_date,
                          run.end_date ||
                            (run.state === "running"
                              ? new Date().toISOString()
                              : null),
                        )}
                      </td>
                      <td>
                        <a
                          className="row-arrow"
                          href={`#/history/run?${query}`}
                          aria-label={`查看 ${run.dag_id} 的运行详情`}
                        >
                          →
                        </a>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
        {history.data && (
          <div className="history-pagination">
            <span>{rows.length} 条记录</span>
            <div>
              <button
                className="button subtle small"
                disabled={view.page === 0}
                onClick={() =>
                  setView((current) => ({ ...current, page: current.page - 1 }))
                }
              >
                上一页
              </button>
              <span>第 {view.page + 1} 页</span>
              <button
                className="button subtle small"
                disabled={!history.data.next_cursor || Boolean(history.error)}
                onClick={nextPage}
              >
                下一页
              </button>
            </div>
          </div>
        )}
      </section>
    </main>
  );
}
