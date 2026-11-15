import { useEffect, useRef } from "react";
import UptimeStrip from "./UptimeStrip";
import useUptime from "./useUptime";

export default function UptimeDayDetail({ dagId, day, onClose, id, refreshVersion = 0 }) {
  const uptime = useUptime([dagId], day, "day");
  const previousRefresh = useRef(refreshVersion);
  useEffect(() => {
    if (previousRefresh.current !== refreshVersion) {
      previousRefresh.current = refreshVersion;
      uptime.refresh();
    }
  }, [refreshVersion, uptime.refresh]);
  return <section className="uptime-day-detail" id={id} aria-label={`${day} 的当天运行时段`}>
    <div className="uptime-day-heading"><h3>{day} · 当天运行时段</h3><button className="link-button" onClick={onClose}>收起当天</button></div>
    {uptime.error && <div className="notice error" role="alert">当天运行时段更新失败{uptime.data ? "，当前显示旧数据" : ""}：{uptime.error} <button className="link-button" onClick={uptime.refresh}>重试当天</button></div>}
    <UptimeStrip key={day} dagId={dagId} slots={uptime.data?.workflows?.[dagId]} error={uptime.error} />
  </section>;
}
