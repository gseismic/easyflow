import { useState } from "react";
import { localDay } from "../uptime";
import UptimeControls from "./UptimeControls";
import UptimeStrip from "./UptimeStrip";
import useUptime from "./useUptime";

export default function WorkflowUptime({ dagId }) {
  const [day, setDay] = useState(() => localDay());
  const [period, setPeriod] = useState("day");
  const uptime = useUptime([dagId], day, period);
  const openDay = (value) => { setDay(value); setPeriod("day"); };
  return (
    <section className="card uptime-card" aria-label="工作流运行时段">
      <div className="section-heading">
        <div><h2>运行时段</h2><p>按日、周、月查看实际执行情况与错误。</p></div>
        <button className="button subtle" onClick={uptime.refresh}>↻ 刷新</button>
      </div>
      <UptimeControls day={day} onChange={setDay} period={period} onPeriodChange={setPeriod} />
      {uptime.error && <div className="notice error" role="alert">运行时段更新失败{uptime.data ? "，当前显示旧数据" : ""}：{uptime.error}{" "}<button className="link-button" onClick={uptime.refresh}>重试</button></div>}
      <div className="uptime-detail-chart"><UptimeStrip key={`${period}:${day}`} dagId={dagId} slots={uptime.data?.workflows?.[dagId]} error={uptime.error} period={period} onOpenDay={openDay} /></div>
    </section>
  );
}
