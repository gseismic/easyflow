import { useEffect, useRef, useState } from "react";
import { usePolling } from "../api";
import { taskLogUrl } from "../runApi";
import { failureKey, failureRunHref, uptimeFailuresUrl, uptimeLogPreview } from "../uptime";
import Status, { formatDuration, formatTime } from "./Status";

export default function UptimeFailures({ dagId, slot, id, onClose }) {
  const [offset, setOffset] = useState(0);
  const [selectedKey, setSelectedKey] = useState(null);
  const failures = usePolling(uptimeFailuresUrl(dagId, slot, offset));
  const items = failures.data?.items || [];
  const selected = items.find((item) => failureKey(item) === selectedKey) || items[0];
  const logs = usePolling(selected?.task_id != null ?
    taskLogUrl(dagId, selected.run_id, selected.task_id, selected.try_number) : null);
  const preview = logs.data ? uptimeLogPreview(logs.data) : null;
  const logOutput = useRef(null);
  useEffect(() => {
    if (logOutput.current) logOutput.current.scrollTop = logOutput.current.scrollHeight;
  }, [preview?.content]);
  const changePage = (value) => { setOffset(value); setSelectedKey(null); };
  const refresh = () => { setOffset(0); setSelectedKey(null); failures.refresh(); logs.refresh(); };
  return (
    <section className="uptime-failures" id={id} aria-label={`${dagId} 的时段失败详情`}>
      <div className="uptime-failure-heading">
        <div>
          <h3>失败详情</h3>
          {failures.data && <p>{failures.data.failed_run_count} 次运行发生错误 · {failures.data.total} 条失败记录（含重试）</p>}
        </div>
        <div className="uptime-failure-actions">
          <button className="button subtle small" onClick={refresh}>刷新失败详情</button>
          <button className="button subtle small" onClick={onClose}>收起失败详情</button>
        </div>
      </div>
      {failures.error && <div className="notice error" role="alert">
        失败记录{failures.data ? "更新失败，当前显示旧数据" : "加载失败"}：{failures.error}{" "}
        <button className="link-button" onClick={failures.refresh}>重试</button>
      </div>}
      {!failures.data ? (!failures.error && <p role="status">正在加载该时段的失败记录…</p>) : !items.length ? (
        <p role="status">该时段暂无失败记录，记录可能已变化。请刷新，或通过上方入口查看运行历史。</p>
      ) : (
        <div className="uptime-failure-grid">
          <div className="uptime-failure-list">
            <div role="group" aria-label="选择失败记录">
              {items.map((item) => (
                <button key={failureKey(item)} className="uptime-failure-item"
                  aria-pressed={failureKey(item) === failureKey(selected)}
                  onClick={() => setSelectedKey(failureKey(item))}>
                  <strong>{item.task_id ?? "运行失败 · 无任务记录"}</strong>
                  <span>{item.task_id != null ? `第 ${item.try_number} 次尝试 · ` : ""}{formatTime(item.ended_at)}</span>
                  <small title={item.run_id}>{item.run_id}</small>
                </button>
              ))}
            </div>
            <div className="uptime-failure-pagination">
              <span>{offset + 1}–{offset + items.length} / {failures.data.total}</span>
              <button className="button subtle small" disabled={!offset || Boolean(failures.error)} onClick={() => changePage(Math.max(0, offset - 25))}>上一页</button>
              <button className="button subtle small" disabled={failures.data.next_offset == null || Boolean(failures.error)} onClick={() => changePage(failures.data.next_offset)}>下一页</button>
            </div>
          </div>
          <div className="uptime-failure-detail">
            <div className="uptime-failure-title">
              <h4>{selected.task_id ?? "运行级失败"}{selected.task_id != null && <span>第 {selected.try_number} 次尝试</span>}</h4>
              <span>运行现状：<Status value={selected.run_state} /></span>
            </div>
            <div className="uptime-failure-summary">
              <strong>{selected.error_code || "错误摘要"}</strong>
              <p>{selected.error_message || (selected.task_id != null ?
                "该次尝试未保存错误摘要，请查看对应日志。" :
                "该时段保存了运行失败状态，但没有对应的失败任务尝试。请查看运行详情。")}</p>
              {selected.error_message_truncated && <small>摘要仅显示前 2000 个字符。</small>}
            </div>
            <dl className="uptime-failure-times">
              <div><dt>开始</dt><dd>{formatTime(selected.started_at)}</dd></div>
              <div><dt>结束</dt><dd>{formatTime(selected.ended_at)}</dd></div>
              <div><dt>耗时</dt><dd>{formatDuration(selected.started_at, selected.ended_at)}</dd></div>
            </dl>
            {selected.task_id != null ? <>
              <p className="uptime-failure-log-label">第 {selected.try_number} 次尝试的日志预览</p>
              {logs.error && <div className="notice error" role="alert">
                日志{logs.data ? "更新失败，当前显示旧日志" : "加载失败"}：{logs.error}{" "}
                <button className="link-button" onClick={logs.refresh}>重试日志</button>
              </div>}
              {!logs.data ? (!logs.error && <p role="status">正在加载该次尝试的日志…</p>) : <>
                {preview?.truncated && <p className="uptime-failure-log-label">仅预览日志末尾，查看完整日志可查看更多。</p>}
                <pre ref={logOutput} className="log-output uptime-failure-log" tabIndex={0} aria-label="失败尝试日志预览">
                  {preview == null ? "该次日志尚未生成或已不可用。" : preview.content || "该次日志为空。"}
                </pre>
              </>}
            </> : <p className="muted">没有可供读取日志的任务尝试；运行详情中可查看已保存的状态。</p>}
            <div className="uptime-failure-links">
              {selected.task_id != null && <a className="button primary small" href={failureRunHref(dagId, selected)}>查看完整日志 →</a>}
              <a className="link-button" href={failureRunHref(dagId, { run_id: selected.run_id })}>查看运行 →</a>
            </div>
          </div>
        </div>
      )}
    </section>
  );
}
