import { formatTime } from "./Status";

const labels = {
  manual: "手动触发",
  paused: "已暂停",
  completed: "无后续计划",
  unknown: "未知",
};

export default function ScheduleDetails({ dag }) {
  const next = dag.next_schedule;
  const entries = [
    ["调度规则", dag.schedule || "手动触发"],
    ["时区", dag.timezone || "未知"],
    ["开始边界", formatTime(dag.start_date)],
    ["结束边界", formatTime(dag.end_date)],
    ["自动补跑", dag.catchup ? "开启" : "关闭"],
    ["最大并发运行", dag.max_active_runs],
    ["最大并发任务", dag.max_active_tasks ?? "使用实例限制"],
    [
      "下次计划",
      ["planned", "due"].includes(next?.status)
        ? `${formatTime(next.at)}${next.status === "due" ? "（待调度）" : ""}`
        : labels[next?.status] || "未知",
    ],
  ];
  return (
    <section className="card info-card">
      <div className="section-heading">
        <div>
          <h2>调度详情</h2>
          <p>下次计划为候选时间，实际执行受调度器和并发限制影响。</p>
        </div>
      </div>
      <dl className="info-grid">
        {entries.map(([name, value]) => (
          <div key={name}>
            <dt>{name}</dt>
            <dd>{value}</dd>
          </div>
        ))}
      </dl>
    </section>
  );
}
