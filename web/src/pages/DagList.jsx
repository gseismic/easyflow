import { part, usePolling } from "../api";
import SearchBox from "../components/SearchBox";
import Status, { formatTime } from "../components/Status";
import useFacets from "../components/useFacets";
import WorkflowFilters, { appendWorkflowFilters, selectedFacetCount } from "../components/WorkflowFilters";
import WorkflowLabels from "../components/WorkflowLabels";

const states = [
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

export default function DagList({ overview, view, setView, filters, onFiltersChange, onReset }) {
  const params = appendWorkflowFilters(new URLSearchParams({ limit: "25" }), filters);
  if (view.query) params.set("q", view.query);
  if (view.state !== "all") params.set("state", view.state);
  if (view.cursors[view.page]) params.set("cursor", view.cursors[view.page]);
  const workflows = usePolling(`/api/workflows?${params}`, 5000);
  const facetParams = appendWorkflowFilters(new URLSearchParams(), filters, false);
  if (view.query) facetParams.set("q", view.query);
  if (view.state !== "all") facetParams.set("state", view.state);
  const facets = useFacets(`/api/workflows/facets?${facetParams}`);
  const pendingSearch = view.draft.trim() !== view.query;
  const facetReady = Boolean(facets.data?.complete && !facets.error && !overview.error &&
    !overview.data?.loading && !overview.data?.issues?.length && !pendingSearch);
  const matchingTotal = facetReady ? selectedFacetCount(facets.data, filters) : null;
  const totalLabel = facets.error || overview.error ? "统计不可用" :
    matchingTotal === null ? "统计中" : `匹配 ${matchingTotal} 个`;
  const dags = workflows.data?.dags || [];
  const summary = workflows.data?.summary;
  const changeView = (patch) =>
    setView((current) => ({ ...current, ...patch, cursors: [null], page: 0 }));
  const refresh = () => {
    overview.refresh();
    workflows.refresh();
    facets.refresh();
  };
  const nextPage = () => {
    if (!workflows.data?.next_cursor) return;
    setView((current) => ({
      ...current,
      cursors: [...current.cursors.slice(0, current.page + 1), workflows.data.next_cursor],
      page: current.page + 1,
    }));
  };
  return (
    <main className="page">
      <div className="eyebrow">WORKFLOW OVERVIEW <span>／ 工作流</span></div>
      <div className="page-title-row">
        <div>
          <h1>你的工作流</h1>
          <p className="subtitle">从整体状态，到每一步的执行细节。</p>
        </div>
        <button className="button subtle" onClick={refresh}>↻ <span>刷新</span></button>
      </div>
      {(overview.error || workflows.error) && (
        <div role="alert" className="notice error">
          更新失败，展示的可能是旧数据：{workflows.error || overview.error}{" "}
          <button className="link-button" onClick={refresh}>重试</button>
        </div>
      )}
      {!overview.error && overview.data && !overview.data.scheduler_running && (
        <div className="notice warning">调度器离线，已创建的运行会等待调度器启动。</div>
      )}
      {overview.data?.issues?.length > 0 && (
        <div role="alert" className="notice error">
          <strong>部分 DAG 无法加载</strong>
          {overview.data.issues.map((issue) => <div key={issue}>{issue}</div>)}
        </div>
      )}
      <section className="summary-strip" aria-label="当前搜索范围的工作流概览">
        <div><span>工作流</span><strong>{summary ? summary.total : "—"}</strong></div>
        <div><span>运行中</span><strong>{summary ? summary.running : "—"}</strong></div>
        <div><span>最近运行失败</span><strong className={summary?.failed ? "danger-text" : ""}>{summary ? summary.failed : "—"}</strong></div>
      </section>
      <section className="card list-card">
        <div className="section-heading">
          <div>
            <h2>全部工作流</h2>
            <p>选择工作流，查看运行和任务详情；统计不受状态筛选影响。</p>
          </div>
          <span className="count">{totalLabel}</span>
        </div>
        <div className="discovery-tools">
          <WorkflowFilters
            catalog={overview}
            facets={facets}
            filters={filters}
            onChange={onFiltersChange}
            onReset={onReset}
            hasOtherFilters={Boolean(view.query || view.draft || view.state !== "all")}
            pendingSearch={pendingSearch}
            unit="个工作流"
          />
          <SearchBox
            label="搜索工作流"
            query={view.query}
            value={view.draft}
            resetVersion={view.resetVersion}
            onDraftChange={(draft) => setView((current) => ({ ...current, draft }))}
            onSearch={(query) => {
              if (query !== view.query) changeView({ query });
            }}
          />
          <div className="filter-group" aria-label="筛选工作流状态">
            {states.map(([value, label]) => (
              <button
                key={value}
                className={view.state === value ? "filter active" : "filter"}
                aria-pressed={view.state === value}
                onClick={() => changeView({ state: value })}
              >{label}</button>
            ))}
          </div>
        </div>
        {(overview.data?.loading || workflows.data?.loading) && (
          <div className="discovery-loading" role="status">正在发现工作流，项目和结果可能尚未完整。</div>
        )}
        {!workflows.data && !workflows.error ? (
          <div className="empty">正在加载工作流…</div>
        ) : !workflows.data ? (
          <div className="empty">无法加载工作流，请检查服务连接。</div>
        ) : !dags.length && workflows.data.loading ? (
          <div className="empty">正在解析工作流…</div>
        ) : !dags.length ? (
          <div className="empty compact">
            <span className="empty-symbol">◇</span>
            <strong>没有匹配的工作流</strong>
            <span>请调整搜索或筛选条件；首次使用时可将 DAG 文件放入指定目录。</span>
          </div>
        ) : (
          <div className="table-wrap">
            <table>
              <thead><tr>
                <th>工作流 / 项目 / 标签</th><th>调度</th><th>下次计划</th><th>最近开始 / 入队</th><th>状态</th><th aria-label="查看详情" />
              </tr></thead>
              <tbody>
                {dags.map((dag) => (
                  <tr key={dag.dag_id}>
                    <td>
                      <a className="dag-link" href={`#/dags/${part(dag.dag_id)}`}>
                        <span className="dag-icon">↗</span>
                        <span><strong title={dag.dag_id}>{dag.dag_id}</strong><small>{dag.task_count} 个任务 · {dag.paused ? "已暂停" : "已启用"}</small></span>
                      </a>
                      <WorkflowLabels item={dag} filters={filters} onChange={onFiltersChange} />
                    </td>
                    <td className="mono">{dag.schedule || "手动触发"}</td>
                    <td>
                      {["planned", "due"].includes(dag.next_schedule?.status) ? (
                        <>
                          {formatTime(dag.next_schedule.at)}
                          {dag.next_schedule.status === "due" && <small className="table-subtitle">待调度</small>}
                        </>
                      ) : nextLabels[dag.next_schedule?.status] || "未知"}
                    </td>
                    <td>
                      {dag.latest_run ? (
                        <>
                          <span>{formatTime(dag.latest_run.start_date || dag.latest_run.queued_at)}</span>
                          <small className="table-subtitle">{dag.latest_run.start_date ? "开始时间" : "入队时间"}</small>
                        </>
                      ) : <span className="muted">尚无运行</span>}
                    </td>
                    <td>
                      <Status value={dag.latest_run?.state} />
                      {dag.running_count > 0 && <small className="table-subtitle">{dag.running_count} 次运行中</small>}
                    </td>
                    <td><a className="row-arrow" href={`#/dags/${part(dag.dag_id)}`} aria-label={`查看 ${dag.dag_id}`}>→</a></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        {workflows.data && (
          <div className="history-pagination">
            <span>{dags.length} 条记录</span>
            <div>
              <button className="button subtle small" disabled={view.page === 0} onClick={() => setView((current) => ({ ...current, page: current.page - 1 }))}>上一页</button>
              <span>第 {view.page + 1} 页</span>
              <button className="button subtle small" disabled={!workflows.data.next_cursor || Boolean(workflows.error)} onClick={nextPage}>下一页</button>
            </div>
          </div>
        )}
      </section>
      <footer className="footer">EASYFLOW <span>轻量、清晰的工作流管理</span></footer>
    </main>
  );
}
