import { useId } from "react";
import { localDay, shiftDay, uptimeStates } from "../uptime";

export default function UptimeControls({ day, onChange }) {
  const id = useId();
  const today = localDay();
  return (
    <div className="uptime-controls">
      <div className="uptime-date">
        <label htmlFor={id}>运行时段</label>
        <button className="button subtle small" aria-label="前一天" onClick={() => onChange(shiftDay(day, -1))}>←</button>
        <input id={id} aria-label="运行时段日期" type="date" value={day} max={today}
          onChange={(event) => {
            if (event.target.value && event.target.value <= today) onChange(event.target.value);
          }} />
        <button className="button subtle small" aria-label="后一天" disabled={day >= today} onClick={() => onChange(shiftDay(day, 1))}>→</button>
        <button className="button subtle small" disabled={day === today} onClick={() => onChange(today)}>今天</button>
      </div>
      <div className="uptime-legend" aria-label="运行时段图例">
        {["active", "running", "waiting", "failed", "idle", "future"].map((state) => (
          <span key={state}><i className={`uptime-swatch uptime-${state}`} aria-hidden="true" />{uptimeStates[state]}</span>
        ))}
      </div>
      <p className="uptime-help">每格 30 分钟 · 本地时间 · 红色保留曾发生的错误，重试成功后仍可回看；无执行不代表故障。</p>
    </div>
  );
}
