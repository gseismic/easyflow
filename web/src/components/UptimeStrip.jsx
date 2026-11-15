import { useState } from "react";
import { workflowHref } from "../listRoute";
import { slotDescription, slotTime, uptimeRunHref } from "../uptime";

export default function UptimeStrip({ dagId, slots, error }) {
  const [selectedStart, setSelectedStart] = useState(null);
  const [focused, setFocused] = useState(0);
  if (!slots) return <div className="uptime-placeholder" role="status">{error ? "运行时段不可用，请重试。" : "正在加载运行时段…"}</div>;
  const selected = slots.find((slot) => slot.start_at === selectedStart);
  const activityCount = slots.filter((slot) => slot.run_count > 0).length;
  const failedCount = slots.filter((slot) => slot.failed_run_count > 0).length;
  const historyHref = workflowHref(dagId, "history");
  const onKeyDown = (event, index) => {
    const target = event.key === "Home" ? 0 : event.key === "End" ? slots.length - 1 :
      event.key === "ArrowLeft" ? Math.max(0, index - 1) :
        event.key === "ArrowRight" ? Math.min(slots.length - 1, index + 1) : null;
    if (target !== null) {
      event.preventDefault();
      event.currentTarget.parentElement.children[target].focus();
    } else if (event.key === "Escape") setSelectedStart(null);
  };
  return (
    <div className="uptime-chart">
      <div className="uptime-scroll" role="group" aria-label={`${dagId} 的半小时运行时段`}>
        <div className="uptime-track" style={{ "--slot-count": slots.length }}>
          <div className="uptime-slots">
            {slots.map((slot, index) => (
              <button key={slot.start_at}
                className={`uptime-slot uptime-${slot.state}`}
                aria-label={slotDescription(slot)} title={slotDescription(slot)}
                aria-pressed={slot.start_at === selectedStart}
                tabIndex={index === focused ? 0 : -1}
                onFocus={() => { setFocused(index); setSelectedStart(slot.start_at); }}
                onClick={() => setSelectedStart(slot.start_at)}
                onKeyDown={(event) => onKeyDown(event, index)}>
                <span aria-hidden="true">{slot.state === "failed" ? "!" : slot.state === "running" ? "·" : ""}</span>
              </button>
            ))}
          </div>
          <div className="uptime-axis" aria-hidden="true">
            {slots.map((slot, index) => index % 12 === 0 && (
              <span key={slot.start_at} style={{ left: `${index / slots.length * 100}%` }}>{slotTime(slot.start_at)}</span>
            ))}
            <span className="uptime-axis-end">{slotTime(slots[slots.length - 1].end_at)} (+1日)</span>
          </div>
        </div>
      </div>
      <div className="uptime-caption" aria-live="polite">
        <span>{selected ? `${slotDescription(selected)}${selected.sample_task_id ? ` · 出错任务：${selected.sample_task_id}（第 ${selected.sample_try_number} 次尝试）` : ""}` :
          activityCount || failedCount ? `${activityCount} 个时段有执行 · ${failedCount} 个时段发生错误 · 点选格子查看` :
            "该日尚无执行记录；工作流执行后会显示在这里。"}</span>
        {selected?.sample_run_id ? (
          <a className="link-button" href={uptimeRunHref(dagId, selected)}>
            {selected.failed_run_count ? "查看出错运行" : "查看运行详情"} →
          </a>
        ) : <a className="link-button" href={historyHref}>查看运行历史 →</a>}
      </div>
    </div>
  );
}
