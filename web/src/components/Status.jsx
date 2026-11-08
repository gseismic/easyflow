const labels = {
  success: "成功",
  failed: "失败",
  running: "运行中",
  queued: "排队中",
  scheduled: "待运行",
  upstream_failed: "上游失败",
  skipped: "已跳过",
  up_for_retry: "等待重试",
  null: "未运行",
};

export default function Status({ value }) {
  const state = value || "null";
  return (
    <span className={`status status-${state}`}>
      <span className="status-dot" />
      {labels[state] || state}
    </span>
  );
}

export function formatTime(value) {
  if (!value) return "—";
  const time = new Date(value);
  return Number.isNaN(time.valueOf())
    ? value
    : new Intl.DateTimeFormat("zh-CN", {
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: false,
      }).format(time);
}
