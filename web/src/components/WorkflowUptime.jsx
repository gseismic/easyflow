import { useEffect, useRef, useState } from "react";
import { localDay } from "../uptime";
import UptimeControls from "./UptimeControls";
import UptimeStrip from "./UptimeStrip";
import useUptime from "./useUptime";

export default function WorkflowUptime({ dagId, refreshVersion = 0 }) {
  const [day, setDay] = useState(() => localDay());
  const [period, setPeriod] = useState("day");
  const uptime = useUptime([dagId], day, period);
  const previousRefresh = useRef(refreshVersion);
  useEffect(() => {
    if (previousRefresh.current !== refreshVersion) {
      previousRefresh.current = refreshVersion;
      uptime.refresh();
    }
  }, [refreshVersion, uptime.refresh]);
  const openDay = (value) => { setDay(value); setPeriod("day"); };
  return (
    <section className="card uptime-card" aria-label="工作流运行时段">
      <UptimeControls heading day={day} onChange={setDay} period={period} onPeriodChange={setPeriod} />
      {uptime.error && <div className="notice error" role="alert">运行时段更新失败{uptime.data ? "，当前显示旧数据" : ""}：{uptime.error}{" "}<button className="link-button" onClick={uptime.refresh}>重试</button></div>}
      <div className="uptime-detail-chart"><UptimeStrip key={`${period}:${day}`} dagId={dagId} slots={uptime.data?.workflows?.[dagId]} error={uptime.error} period={period} onOpenDay={openDay} /></div>
    </section>
  );
}
