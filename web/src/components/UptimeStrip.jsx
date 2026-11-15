import { useId, useRef, useState } from "react";
import { workflowHref } from "../listRoute";
import { createSlotDescriber, dailyUptime, localDay, uptimeRunHref, uptimeSlotKey, uptimeTicks } from "../uptime";
import UptimeFailures from "./UptimeFailures";
import HorizontalScroll from "./HorizontalScroll";

export default function UptimeStrip({ dagId, slots, error, period = "day", onOpenDay, renderDay }) {
  const [selection, setSelection] = useState(null);
  const [focused, setFocused] = useState(null);
  const panelId = useId();
  const buttons = useRef([]);
  if (!slots) return <div className="uptime-placeholder" role="status">{error ? "运行时段不可用，请重试。" : "正在加载运行时段…"}</div>;
  const currentSelection = slots.find(slot => uptimeSlotKey(slot, period) === selection?.key);
  const selected = period === "recent" && currentSelection ? selection.slot : currentSelection;
  const focusIndex = Math.max(0, slots.findIndex(slot => uptimeSlotKey(slot, period) === focused));
  const select = slot => setSelection({ key: uptimeSlotKey(slot, period), slot: { ...slot } });
  const activityCount = slots.filter((slot) => slot.run_count > 0).length;
  const failedCount = slots.filter((slot) => slot.failed_run_count > 0).length;
  const historyHref = workflowHref(dagId, "history");
  const daily = dailyUptime(period);
  const describe = createSlotDescriber(period);
  const closeSelection = () => {
    buttons.current[slots.findIndex(slot => uptimeSlotKey(slot, period) === selection?.key)]?.focus();
    setSelection(null);
  };
  const onKeyDown = (event, index) => {
    const target = event.key === "Home" ? 0 : event.key === "End" ? slots.length - 1 :
      event.key === "ArrowLeft" ? Math.max(0, index - 1) :
        event.key === "ArrowRight" ? Math.min(slots.length - 1, index + 1) : null;
    if (target !== null) {
      event.preventDefault();
      event.currentTarget.parentElement.children[target].focus();
    }
  };
  return (
    <div className="uptime-chart" onKeyDown={(event) => {
      if (event.key === "Escape" && selected && !event.defaultPrevented) {
        event.preventDefault();
        closeSelection();
      }
    }}>
      <HorizontalScroll className="uptime-scroll" label={`${dagId} 的${daily ? "每日" : "半小时"}运行时段`}
        hint={`左右滑动或使用方向键查看${daily ? "完整日期" : period === "recent" ? "完整时段" : "全天时段"}`}>
        <div className={`uptime-track${daily ? " uptime-daily-track" : period === "recent" ? " uptime-recent-track" : ""}`} style={{ "--slot-count": slots.length }}>
          <div className="uptime-slots">
            {slots.map((slot, index) => {
              const key = uptimeSlotKey(slot, period);
              const description = describe(slot);
              const isSelected = key === selection?.key;
              const expandable = daily ? slot.state !== "future" && Boolean(renderDay) : slot.failed_run_count > 0;
              return <button key={key}
                ref={(node) => { buttons.current[index] = node; }}
                className={`uptime-slot uptime-${slot.state}${daily ? " uptime-daily-slot" : ""}`}
                aria-label={description} title={description}
                aria-pressed={isSelected}
                aria-expanded={expandable ? isSelected : undefined}
                aria-controls={expandable && isSelected ? daily ? `${panelId}-day` : panelId : undefined}
                tabIndex={index === focusIndex ? 0 : -1}
                onFocus={() => { setFocused(uptimeSlotKey(slot, period)); select(slot); }}
                onClick={() => select(slot)}
                onKeyDown={(event) => onKeyDown(event, index)}>
                {daily && <span className="uptime-date-number" aria-hidden="true">{new Date(slot.start_at).getDate()}</span>}
                <span aria-hidden="true">{slot.state === "failed" ? "!" : slot.state === "running" ? "·" : ""}</span>
              </button>;
            })}
          </div>
          {daily ? <div className="uptime-day-axis" aria-hidden="true">
            {slots.map((slot) => <span key={slot.start_at}>{new Intl.DateTimeFormat("zh-CN", { weekday: "short" }).format(new Date(slot.start_at)).replace("周", period === "month" ? "" : "周")}</span>)}
          </div> : <div className="uptime-axis" aria-hidden="true">
            {uptimeTicks(slots, period).map(tick => <span key={tick.key} className={tick.position === 100 ? "uptime-axis-end" : undefined}
              style={tick.position === 100 ? undefined : { left: `${tick.position}%` }}>{tick.label}</span>)}
          </div>}
        </div>
      </HorizontalScroll>
      <div className="uptime-caption" aria-live="polite">
        <span>{selected ? describe(selected) :
          activityCount || failedCount ? `${activityCount} ${daily ? "天" : "个时段"}有执行 · ${failedCount} ${daily ? "天" : "个时段"}发生错误 · 点选格子查看` :
            "所选时段暂无执行记录；工作流执行后会显示在这里。"}</span>
        {daily && selected && selected.state !== "future" && onOpenDay && (
          <button className="link-button" onClick={() => onOpenDay(localDay(new Date(selected.start_at)))}>查看当天 →</button>
        )}
        {selected?.sample_run_id ? (
          <a className="link-button" href={uptimeRunHref(dagId, selected)}>
            {selected.failed_run_count ? "查看出错运行" : "查看运行详情"} →
          </a>
        ) : <a className="link-button" href={historyHref}>查看运行历史 →</a>}
      </div>
      {daily && selected && selected.state !== "future" && renderDay?.(localDay(new Date(selected.start_at)), closeSelection, `${panelId}-day`)}
      {!daily && selected?.failed_run_count > 0 && <UptimeFailures key={selected.start_at}
        dagId={dagId} slot={selected} id={panelId} onClose={closeSelection} />}
    </div>
  );
}
