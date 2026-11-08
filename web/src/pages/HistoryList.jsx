import { usePolling } from "../api";
import { runTypeLabel } from "../components/RunMetadata";
import SearchBox from "../components/SearchBox";
import Status, { formatDuration, formatTime } from "../components/Status";
import useFacets from "../components/useFacets";
import WorkflowFilters, { appendWorkflowFilters, selectedFacetCount } from "../components/WorkflowFilters";
import WorkflowLabels from "../components/WorkflowLabels";

const filters = [
  ["", "全部"],
  ["running", "运行中"],
  ["queued", "排队中"],
  ["success", "成功"],
  ["failed", "失败"],
];

export default function HistoryList({ view, setView, filters: selection, onFiltersChange, onReset, catalog, dagId }) {
  const params = appendWorkflowFilters(new URLSearchParams({ limit: "25" }), selection);
  if (dagId) params.set("dag_id", dagId);
  if (view.query) params.set("q", view.query);
  if (view.state) params.set("state", view.state);
  if (view.cursors[view.page]) params.set("cursor", view.cursors[view.page]);
  const history = usePolling(`/api/history/runs?${params}`, 5000);
  const facetParams = appendWorkflowFilters(new URLSearchParams(), selection, false);
  if (dagId) facetParams.set("dag_id", dagId);
  if (view.query) facetParams.set("q", view.query);
  if (view.state) facetParams.set("state", view.state);
  const facets = useFacets(`/api/history/runs/facets?${facetParams}`);
  const pendingSearch = view.draft.trim() !== view.query;
  const facetReady = Boolean(facets.data?.complete && !facets.error && !catalog.error &&
    !catalog.data?.loading && !catalog.data?.issues?.length && !pendingSearch);
  const matchingTotal = facetReady ? selectedFacetCount(facets.data, selection) : null;
  const totalLabel = facets.error || catalog.error ? "统计不可用" :
    matchingTotal === null ? "统计中" : `匹配 ${matchingTotal} 次运行`;
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
        <button className="button subtle" onClick={() => { history.refresh(); facets.refresh(); catalog.refresh(); }}>
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
            <p>按工作流当前项目归类 · 时间为浏览器本地时区 · 每页 25 条</p>
          </div>
          <span className="count">{totalLabel}</span>
        </div>
        <div className="discovery-tools">
          <WorkflowFilters
            catalog={catalog}
            facets={facets}
            filters={selection}
            onChange={onFiltersChange}
            onReset={onReset}
            hasOtherFilters={Boolean(view.query || view.draft || view.state || dagId)}
            pendingSearch={pendingSearch}
            unit="次运行"
            showUnknown
          />
          <SearchBox
            label="搜索运行历史"
            placeholder="搜索工作流、项目、标签或运行 ID"
            query={view.query}
            value={view.draft}
            resetVersion={view.resetVersion}
            onDraftChange={(draft) => setView((current) => ({ ...current, draft }))}
            onSearch={(query) => {
              if (query !== view.query) setFilter({ query });
            }}
          />
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
        {dagId && (
          <div className="history-scope">
            <span>工作流：<strong>{dagId}</strong></span>
            <a className="link-button" href="#/history">查看所有工作流</a>
          </div>
        )}
        {(catalog.data?.loading || history.data?.metadata_loading) && (
          <div className="discovery-loading" role="status">
            正在发现工作流，部分历史记录的当前项目暂不可确定。
          </div>
        )}
        {(catalog.error || catalog.data?.issues?.length > 0 || history.data?.metadata_error) && (
          <div className="notice warning" role="status">
            部分工作流信息不可用；无法确定项目的历史记录显示为“项目未知”。
          </div>
        )}
        {!history.data && !history.error ? (
          <div className="empty">正在加载运行历史…</div>
        ) : !history.data ? (
          <div className="empty">无法加载运行历史，请重试。</div>
        ) : !rows.length ? (
          <div className="empty compact">
            <span className="empty-symbol">◇</span>
            <strong>
              {view.query || view.state || dagId || selection.projectKind !== "all" || selection.tags.length ? "没有匹配的运行" : "还没有运行记录"}
            </strong>
            <span>
              {view.query || view.state || dagId || selection.projectKind !== "all" || selection.tags.length
                ? "请调整搜索关键词、项目、标签或状态筛选。"
                : "工作流开始执行后，记录会显示在这里。"}
            </span>
          </div>
        ) : (
          <div className="table-wrap">
            <table className="history-table">
              <thead>
                <tr>
                  <th>工作流 / 当前项目 / 标签</th>
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
                  if (dagId) query.set("from_dag", dagId);
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
                        <WorkflowLabels item={run} filters={selection} onChange={onFiltersChange} />
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
