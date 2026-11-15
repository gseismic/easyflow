import { useId } from "react";
import { localDay, periodBounds, periodRange, shiftPeriod, uptimePeriods, uptimeStates } from "../uptime";

export default function UptimeControls({ day, onChange, period = "day", onPeriodChange }) {
  const id = useId();
  const today = localDay();
  const current = periodBounds(today, period);
  const bounds = periodBounds(day, period);
  const isCurrent = bounds?.start.getTime() === current?.start.getTime();
  const nextDay = shiftPeriod(day, period, 1);
  const canNext = periodBounds(nextDay, period)?.start <= current?.start;
  const labels = uptimePeriods[period];
  return (
    <div className="uptime-controls">
      <div className="uptime-date">
        <label htmlFor={id}>运行时段</label>
        <div className="uptime-period" role="group" aria-label="uptime 查看范围">
          {Object.entries(uptimePeriods).map(([value, item]) => (
            <button key={value} aria-label={`${item.label}视图`} aria-pressed={period === value}
              onClick={() => onPeriodChange?.(value)}>{item.label}</button>
          ))}
        </div>
        <button className="button subtle small" aria-label={`前一${labels.unit}`} onClick={() => onChange(shiftPeriod(day, period, -1))}>←</button>
        <input id={id} aria-label="运行时段日期" type="date" value={day} max={today}
          onChange={(event) => {
            if (event.target.value && event.target.value <= today) onChange(event.target.value);
          }} />
        <button className="button subtle small" aria-label={`后一${labels.unit}`} disabled={!canNext} onClick={() => onChange(nextDay)}>→</button>
        <button className="button subtle small" disabled={isCurrent} onClick={() => onChange(today)}>{labels.current}</button>
        {period !== "day" && <span className="uptime-range" aria-label="显示日期范围">{periodRange(day, period)}</span>}
      </div>
      <div className="uptime-legend" aria-label="运行时段图例">
        {["active", "running", "waiting", "failed", "idle", "future"].map((state) => (
          <span key={state}><i className={`uptime-swatch uptime-${state}`} aria-hidden="true" />{uptimeStates[state]}</span>
        ))}
      </div>
      <p className="uptime-help">{period === "day" ? "每格 30 分钟" : "每格 1 天"} · 本地时间 · 红色保留曾发生的错误，重试成功后仍可回看；无执行不代表故障。</p>
    </div>
  );
}
