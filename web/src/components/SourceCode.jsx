import { part, usePolling } from "../api";

export default function SourceCode({ base, runId }) {
  const source = usePolling(
    `${base}/source${runId ? `?run_id=${part(runId)}` : ""}`,
    30000,
  );
  return (
    <section className="card source-card">
      <div className="section-heading">
        <div>
          <h2>当前源码</h2>
          <p>{source.data?.path || "DAG 文件"} · 只读</p>
        </div>
        <button className="link-button" onClick={source.refresh}>
          刷新
        </button>
      </div>
      {source.error ? (
        <div className="notice error" role="alert">
          源码加载失败：{source.error}
        </div>
      ) : !source.data ? (
        <div className="empty compact">正在加载源码…</div>
      ) : (
        <>
          <div className="source-notice">
            {source.data.parsed_matches === false
              ? "文件在解析后已变化，请刷新工作流。"
              : source.data.run_matches === false
                ? "当前定义与所选历史运行不同；当前文件不是历史快照。"
                : source.data.run_matches === true
                  ? "当前定义指纹与所选运行记录相符；这里展示当前文件。"
                  : runId
                    ? "这里展示当前文件；所选运行的历史源码不可用。"
                    : "这里展示当前文件；历史源码快照未保存。"}
          </div>
          <pre className="source-output">
            <code>{source.data.content}</code>
          </pre>
        </>
      )}
    </section>
  );
}
