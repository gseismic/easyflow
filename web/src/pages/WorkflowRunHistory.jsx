import { usePolling } from "../api";
import { runTypeLabel } from "../components/RunMetadata";
import SearchBox from "../components/SearchBox";
import SearchContext from "../components/SearchContext";
import Status, { formatDuration, formatTime } from "../components/Status";
import usePendingRefresh from "../components/usePendingRefresh";
import { workflowHistoryRunHref } from "../listRoute";

const states = [
  ["", "全部"],
  ["running", "运行中"],
  ["queued", "排队中"],
  ["success", "成功"],
  ["failed", "失败"],
];

export default function WorkflowRunHistory({ dagId, view, setView }) {
  const params = new URLSearchParams({ dag_id: dagId, limit: "25" });
  if (view.query) params.set("q", view.query);
  if (view.state) params.set("state", view.state);
  if (view.cursors[view.page]) params.set("cursor", view.cursors[view.page]);
  const history = usePolling(`/api/history/runs?${params}`, 5000);
  usePendingRefresh(history, history.data?.metadata_loading);
  const rows = history.data?.runs || [];
  const setFilter = (patch) =>
    setView((current) => ({ ...current, ...patch, cursors: [null], page: 0 }));
  const nextPage = () => {
    if (!history.data?.next_cursor) return;
    setView((current) => ({
      ...current,
      cursors: [...current.cursors.slice(0, current.page + 1), history.data.next_cursor],
      page: current.page + 1,
    }));
  };

  return (
    <section className="card history-card workflow-history" aria-label="本工作流的运行历史">
      <div className="section-heading">
        <div>
          <h2>本工作流的运行历史</h2>
          <p>按入队时间从近到远排列 · 本地时间 · 每页 25 条</p>
        </div>
        <button className="button subtle" onClick={history.refresh}>↻ 刷新</button>
      </div>
      <div className="discovery-tools">
        <div className="search-section" role="search" aria-label="搜索本工作流运行">
          <span className="discovery-label">搜索</span>
          <div className="search-content">
            <SearchBox
              label="搜索本工作流运行"
              placeholder="搜索运行 ID 或任务 ID"
              describedBy="workflow-history-search-help"
              query={view.query}
              value={view.draft}
              resetVersion={view.resetVersion}
              onDraftChange={(draft, draftComposing) => setView((current) => ({ ...current, draft, draftComposing }))}
              onSearch={(query) => {
                if (query !== view.query) setFilter({ query });
              }}
            />
            <p id="workflow-history-search-help" className="search-help">
              任务 ID 来自该次运行；也可搜索当前工作流 ID、描述、项目和标签。多个关键词需全部匹配。
            </p>
          </div>
        </div>
        <div className="filter-section">
          <span className="discovery-label">状态</span>
          <div className="filter-group" role="group" aria-label="过滤本工作流运行状态">
            {states.map(([value, label]) => (
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
      </div>
      {history.error && (
        <div className="notice error workflow-history-notice" role="alert">
          运行历史加载失败：{history.error}{" "}
          <button className="link-button" onClick={history.refresh}>重试</button>
        </div>
      )}
      {view.query && history.data?.metadata_loading && (
        <div className="discovery-loading" role="status">
          正在发现工作流，按当前描述、项目和标签搜索的结果可能尚不完整。
        </div>
      )}
      {view.query && history.data?.metadata_error && (
        <div className="notice warning workflow-history-notice" role="status">
          当前工作流信息不可用，按描述、项目和标签搜索的结果可能不完整。
        </div>
      )}
      {!history.data && !history.error ? (
        <div className="empty">正在加载运行历史…</div>
      ) : !history.data ? (
        <div className="empty">无法加载运行历史，请重试。</div>
      ) : !rows.length ? (
        <div className="empty compact">
          <span className="empty-symbol">◇</span>
          <strong>{view.query || view.state ? "没有匹配的运行" : "本工作流还没有运行记录"}</strong>
          <span>{view.query || view.state ? "请清除搜索或调整状态。" : "工作流开始执行后，记录会显示在这里。"}</span>
        </div>
      ) : (
        <div className="table-wrap">
          <table className="history-table workflow-history-table">
            <thead>
              <tr>
                <th>运行 ID</th>
                <th>状态</th>
                <th>触发方式</th>
                <th>入队时间</th>
                <th>执行耗时</th>
                <th aria-label="查看详情" />
              </tr>
            </thead>
            <tbody>
              {rows.map((run) => {
                const href = workflowHistoryRunHref(dagId, run.run_id, view);
                const descriptionMatches = view.query.trim().toLocaleLowerCase().split(/\s+/).some(
                  (term) => term && (run.description || "").toLocaleLowerCase().includes(term),
                );
                return (
                  <tr key={run.run_id}>
                    <td>
                      <a className="history-run-link" href={href} title={run.run_id}>{run.run_id}</a>
                      {view.query && (
                        <SearchContext
                          item={{ ...run, description: descriptionMatches ? run.description : "" }}
                          query={view.query}
                          history
                        />
                      )}
                    </td>
                    <td><Status value={run.state} /></td>
                    <td>{runTypeLabel(run.run_type)}</td>
                    <td>{formatTime(run.queued_at)}</td>
                    <td>{formatDuration(run.start_date, run.end_date || (run.state === "running" ? new Date().toISOString() : null))}</td>
                    <td><a className="row-arrow" href={href} aria-label={`查看运行 ${run.run_id} 的详情`}>→</a></td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
      {history.data && (
        <div className="history-pagination">
          <span>本页 {rows.length} 条</span>
          <div>
            <button className="button subtle small" disabled={view.page === 0} onClick={() => setView((current) => ({ ...current, page: current.page - 1 }))}>上一页</button>
            <span>第 {view.page + 1} 页</span>
            <button className="button subtle small" disabled={!history.data.next_cursor || Boolean(history.error)} onClick={nextPage}>下一页</button>
          </div>
        </div>
      )}
    </section>
  );
}
