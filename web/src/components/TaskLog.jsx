export default function TaskLog({
  logs,
  taskId,
  runId,
  attempt,
  setAttempt,
  isAdmin,
  onClear,
}) {
  return (
    <section className="card log-card">
      <div className="section-heading">
        <div>
          <h2>任务日志</h2>
          <p>{taskId || "选择任务查看日志"}</p>
        </div>
        {isAdmin && runId && taskId && onClear && (
          <button className="button subtle small" onClick={onClear}>
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
            <span>运行日志</span>
            <label>
              执行次数{" "}
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
            <div className="notice error" role="alert">
              日志加载失败：{logs.error}
            </div>
          ) : (
            <pre className="log-output">
              {!logs.data
                ? "正在加载日志…"
                : logs.data.content == null
                  ? "日志尚未生成或已不可用。"
                  : `${logs.data.truncated ? "[仅显示最后 1 MB]\n" : ""}${logs.data.content}`}
            </pre>
          )}
        </>
      )}
    </section>
  );
}
