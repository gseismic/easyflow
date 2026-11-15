import { useEffect, useId, useState } from "react";
import { dailyUptime, localDay, periodBounds, periodRange, shiftPeriod, uptimePeriods, uptimeRangeLabel, uptimeStates, validRecentHours } from "../uptime";

export default function UptimeControls({ day, onChange, period = "day", onPeriodChange, heading = false, hours = 24, onHoursChange, range, warning }) {
  const id = useId();
  const [draft, setDraft] = useState(String(hours));
  useEffect(() => setDraft(String(hours)), [hours]);
  const recent = period === "recent";
  const valid = validRecentHours(draft);
  const today = localDay();
  const current = periodBounds(today, period);
  const bounds = periodBounds(day, period);
  const isCurrent = bounds?.start.getTime() === current?.start.getTime();
  const nextDay = shiftPeriod(day, period, 1);
  const canNext = periodBounds(nextDay, period)?.start <= current?.start;
  const labels = uptimePeriods[period];
  return (
    <div className="uptime-controls">
      <div className="uptime-toolbar">
      <div className="uptime-date">
        {heading ? <h2 className="uptime-controls-heading">运行时段</h2> : <span className="uptime-controls-heading">运行时段</span>}
        <div className="uptime-period" role="group" aria-label="uptime 查看范围">
          {[["recent", { label: "最近" }], ...Object.entries(uptimePeriods)].map(([value, item]) => (
            <button key={value} aria-label={`${item.label}视图`} aria-pressed={period === value}
              onClick={() => onPeriodChange?.(value)}>{item.label}</button>
          ))}
        </div>
        {recent ? <form className="uptime-recent-form" aria-label="最近小时范围" noValidate onSubmit={event => {
          event.preventDefault();
          if (valid) onHoursChange?.(Number(draft));
        }}>
          <label htmlFor={id}>最近</label><input id={id} type="number" min="1" max="168" step="1" value={draft}
            aria-label="最近小时数" aria-invalid={!valid} aria-describedby={`${id}-help`} onChange={event => setDraft(event.target.value)} />
          <span>小时</span><button className="button subtle small" disabled={!valid}>应用</button>
          <small id={`${id}-help`} className={!valid ? "form-error" : "muted"}>{valid ? "1–168 小时" : "请输入 1–168 的整数"}</small>
        </form> : <div className="uptime-calendar">
        <div className="uptime-date-navigation">
        <button className="button subtle small" aria-label={`前一${labels.unit}`} onClick={() => onChange(shiftPeriod(day, period, -1))}>←</button>
        <input id={id} aria-label="运行时段日期" type="date" value={day} max={today}
          onChange={(event) => {
            if (event.target.value && event.target.value <= today) onChange(event.target.value);
          }} />
        <button className="button subtle small" aria-label={`后一${labels.unit}`} disabled={!canNext} onClick={() => onChange(nextDay)}>→</button>
        </div>
        <button className="button subtle small" disabled={isCurrent} onClick={() => onChange(today)}>{labels.current}</button>
        {period !== "day" && <span className="uptime-range" aria-label="显示日期范围">{periodRange(day, period)}</span>}
        </div>}
        {recent && range?.start_at && range?.end_at && <span className="uptime-range" aria-label="显示时间范围">{uptimeRangeLabel(range.start_at, range.end_at)}</span>}
      </div>
      <span className="uptime-resolution">{dailyUptime(period) ? "1 天 / 格" : "30 分钟 / 格"} · 本地时间</span>
      <details className="uptime-help">
        <summary>说明</summary>
        <p>红色保留曾发生的错误，重试成功后仍可回看；无执行不代表故障。</p>
        {recent ? <p>最近 {hours} 小时随时间滚动，首尾格子按实际范围裁剪；选中格子的查看范围会固定，便于持续阅读日志。</p> : dailyUptime(period) && <p>点选某一天，在下方展开当天的半小时图；未来日期尚不能展开。</p>}
      </details>
      </div>
      {recent && warning && <p className="uptime-storage-warning" role="status">{warning}</p>}
      <div className="uptime-legend" aria-label="运行时段图例">
        {["active", "running", "waiting", "failed", "idle", "future"].map((state) => (
          <span key={state}><i className={`uptime-swatch uptime-${state}`} aria-hidden="true" />{uptimeStates[state]}</span>
        ))}
      </div>
    </div>
  );
}
