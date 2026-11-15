import { useState } from "react";
import { localDay, readRecentHours, saveRecentHours } from "../uptime";

function storage() { try { return window.localStorage; } catch { return null; } }

export default function useUptimeView() {
  const [day, setDay] = useState(() => localDay());
  const [period, setPeriod] = useState("recent");
  const [hours, setHours] = useState(() => readRecentHours(storage()));
  const [warning, setWarning] = useState("");
  const changeHours = (value) => {
    setHours(value);
    setWarning(saveRecentHours(storage(), value) ? "" : "小时数已在本页应用，浏览器未能保存；刷新后可能丢失。");
  };
  return { day, setDay, period, setPeriod, hours, changeHours, warning,
    openDay: value => { setDay(value); setPeriod("day"); } };
}
