import { formatDuration, formatTime } from "./Status";

export function runTypeLabel(value) {
  return (
    { manual: "手动触发", scheduled: "定时触发", backfill: "历史回补" }[value] ||
    value
  );
}

export default function RunMetadata({ run }) {
  const values = [
    ["触发方式", runTypeLabel(run.run_type)],
    ["入队时间", formatTime(run.queued_at)],
    ["开始时间", formatTime(run.start_date)],
    ["结束时间", formatTime(run.end_date)],
    [
      "执行耗时",
      formatDuration(
        run.start_date,
        run.end_date ||
          (run.state === "running" ? new Date().toISOString() : null),
      ),
    ],
    ["逻辑时间", formatTime(run.logical_date)],
    [
      "数据区间",
      `${formatTime(run.data_interval_start)} — ${formatTime(run.data_interval_end)}`,
    ],
  ];
  return (
    <dl className="run-metadata">
      {values.map(([label, value]) => (
        <div key={label}>
          <dt>{label}</dt>
          <dd>{value}</dd>
        </div>
      ))}
    </dl>
  );
}
