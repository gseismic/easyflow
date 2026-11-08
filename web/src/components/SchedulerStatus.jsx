import { formatTime } from "./Status";

export default function SchedulerStatus({ overview }) {
  const status = overview.data?.scheduler;
  const unknown = Boolean(overview.error) || !status;
  const text = unknown
    ? "状态未知"
    : status.running
      ? "调度器在线"
      : "调度器离线";
  const title = unknown
    ? "无法确认调度状态，请检查连接并刷新。"
    : status.last_heartbeat_at
      ? `最后心跳：${formatTime(status.last_heartbeat_at)}；租约到期：${formatTime(status.lease_expires_at)}`
      : "未发现有效的调度器租约。";
  return (
    <span
      className={`scheduler-badge ${!unknown && status.running ? "online" : ""}`}
      title={title}
    >
      <span className="status-dot" />
      {text}
      {!unknown && status.last_heartbeat_at && (
        <small>· 最近心跳 {formatTime(status.last_heartbeat_at)}</small>
      )}
    </span>
  );
}
