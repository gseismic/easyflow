import { workflowHistoryRunHref } from "./listRoute.js";

export const uptimePeriods = {
  day: { label: "日", unit: "天", current: "今天" },
  week: { label: "周", unit: "周", current: "本周" },
  month: { label: "月", unit: "月", current: "本月" },
};

export const recentHoursKey = "easyflow:uptime-hours:v1";
export function validRecentHours(value) {
  return /^(?:[1-9]\d{0,2})$/.test(String(value)) && Number(value) <= 168;
}
export function readRecentHours(storage) {
  try {
    const value = storage?.getItem(recentHoursKey);
    return validRecentHours(value) ? Number(value) : 24;
  } catch { return 24; }
}
export function saveRecentHours(storage, value) {
  if (!validRecentHours(value)) return false;
  try { storage.setItem(recentHoursKey, String(value)); return true; } catch { return false; }
}
export const dailyUptime = period => period === "week" || period === "month";
export function uptimeSlotKey(slot, period) {
  return period === "recent" ? String(Math.floor(Date.parse(slot.start_at) / 1800000)) : slot.start_at;
}

export const uptimeStates = {
  active: "有执行",
  running: "执行中",
  waiting: "当前等待",
  failed: "发生错误",
  idle: "无执行",
  future: "尚未到达",
};

export function localDay(date = new Date()) {
  const pad = (value) => String(value).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
}

function localDate(year, month, day) {
  const date = new Date(0);
  date.setFullYear(year, month, day);
  date.setHours(0, 0, 0, 0);
  return date;
}

export function dayBounds(day) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) return null;
  const start = new Date(`${day}T00:00:00`);
  if (!Number.isFinite(start.getTime()) || localDay(start) !== day) return null;
  const end = localDate(start.getFullYear(), start.getMonth(), start.getDate() + 1);
  return { start, end };
}

export function shiftDay(day, offset) {
  const bounds = dayBounds(day);
  if (!bounds) return day;
  bounds.start.setDate(bounds.start.getDate() + offset);
  return localDay(bounds.start);
}

export function periodBounds(day, period = "day") {
  const bounds = dayBounds(day);
  if (!bounds || !uptimePeriods[period]) return null;
  if (period === "day") return bounds;
  const date = bounds.start;
  const start = localDate(date.getFullYear(), date.getMonth(),
    period === "week" ? date.getDate() - (date.getDay() + 6) % 7 : 1);
  const end = localDate(start.getFullYear(), start.getMonth() + (period === "month" ? 1 : 0),
    period === "week" ? start.getDate() + 7 : 1);
  return { start, end };
}

export function shiftPeriod(day, period, offset) {
  if (period !== "month") return shiftDay(day, offset * (period === "week" ? 7 : 1));
  const bounds = dayBounds(day);
  if (!bounds) return day;
  const wantedDay = bounds.start.getDate();
  bounds.start.setDate(1);
  bounds.start.setMonth(bounds.start.getMonth() + offset);
  const lastDay = new Date(bounds.start.getFullYear(), bounds.start.getMonth() + 1, 0).getDate();
  bounds.start.setDate(Math.min(wantedDay, lastDay));
  return localDay(bounds.start);
}

export function periodRange(day, period) {
  const bounds = periodBounds(day, period);
  if (!bounds) return "";
  const last = new Date(bounds.end);
  last.setDate(last.getDate() - 1);
  return period === "month" ? localDay(bounds.start).slice(0, 7) :
    `${localDay(bounds.start)} — ${localDay(last)}`;
}

export function uptimeUrl(dagIds, day, period = "day", hours = 24) {
  const ids = [...new Set(dagIds)].sort();
  if (period === "recent") {
    if (!ids.length || !validRecentHours(hours)) return null;
    const params = new URLSearchParams({ lookback_hours: String(hours) });
    ids.forEach(id => params.append("dag_id", id));
    return `/api/workflows/uptime?${params}`;
  }
  const bounds = periodBounds(day, period);
  if (!bounds || !ids.length) return null;
  const params = new URLSearchParams({
    start_at: bounds.start.toISOString(),
    end_at: bounds.end.toISOString(),
  });
  if (period !== "day") {
    params.set("granularity", "day");
    params.set("timezone", new Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC");
  }
  ids.forEach((id) => params.append("dag_id", id));
  return `/api/workflows/uptime?${params}`;
}

export function slotTime(value) {
  return new Intl.DateTimeFormat("zh-CN", {
    hour: "2-digit", minute: "2-digit", hour12: false,
  }).format(new Date(value));
}

export function uptimeRangeLabel(start, end) {
  const format = value => new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", hour12: false,
  }).format(new Date(value));
  return `${format(start)} — ${format(end)}`;
}

export function uptimeTicks(slots, period) {
  if (!slots.length) return [];
  const format = new Intl.DateTimeFormat("zh-CN", {
    ...(period === "recent" ? { month: "2-digit", day: "2-digit" } : {}),
    hour: "2-digit", minute: "2-digit", hour12: false,
  });
  const label = value => format.format(new Date(value));
  const indices = slots.length < 8 ? [0] : [...new Set([0, 0.25, 0.5, 0.75].map(fraction => Math.floor(slots.length * fraction)))];
  const end = slots.at(-1).end_at;
  const suffix = period !== "recent" && localDay(new Date(end)) !== localDay(new Date(slots[0].start_at)) ? ` (${localDay(new Date(end)).slice(5)})` : "";
  return [...indices.map(index => ({ key: slots[index].start_at, position: index / slots.length * 100, label: label(slots[index].start_at) })),
    { key: "end", position: 100, label: label(end) + suffix }];
}

export function createSlotDescriber(period = "day") {
  const daily = dailyUptime(period);
  // 每条图共用格式器，避免长窗口的每个格子反复构造 Intl 实例。
  const format = new Intl.DateTimeFormat("zh-CN", daily
    ? { year: "numeric", month: "2-digit", day: "2-digit", weekday: "short" }
    : { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", hour12: false });
  const local = value => format.format(new Date(value));
  return slot => {
    const range = daily ? local(slot.start_at) : `${local(slot.start_at)} — ${local(slot.end_at)}`;
    const parts = [range, uptimeStates[slot.state] || "未知"];
    if (slot.run_count) parts.push(`${slot.run_count} 次运行有执行`);
    if (slot.failed_run_count) parts.push(`${slot.failed_run_count} 次运行发生错误（含重试）`);
    if (slot.waiting_run_count) parts.push(`${slot.waiting_run_count} 次运行正在等待调度、依赖或重试`);
    return parts.join(" · ");
  };
}
export function slotDescription(slot, period = "day") {
  return createSlotDescriber(period)(slot);
}
export function uptimeRunHref(dagId, slot) {
  const href = workflowHistoryRunHref(dagId, slot.sample_run_id, {});
  const [path, query] = href.split("?");
  const params = new URLSearchParams(query);
  if (slot.sample_task_id) params.set("task_id", slot.sample_task_id);
  if (slot.sample_try_number) params.set("attempt", String(slot.sample_try_number));
  return `${path}?${params}`;
}

export function uptimeFailuresUrl(dagId, slot, offset = 0) {
  if (!slot) return null;
  const params = new URLSearchParams({
    dag_id: dagId, start_at: slot.start_at, end_at: slot.end_at,
    limit: "25", offset: String(offset),
  });
  return `/api/workflows/uptime/failures?${params}`;
}

export function failureKey(item) {
  return JSON.stringify([item.run_id, item.task_id, item.try_number,
    item.task_id == null ? item.ended_at : null]);
}

export function failureRunHref(dagId, item) {
  return uptimeRunHref(dagId, {
    sample_run_id: item.run_id,
    sample_task_id: item.task_id,
    sample_try_number: item.try_number,
  });
}

export function uptimeLogPreview(log) {
  if (log.content == null) return null;
  const lines = log.content.split("\n");
  const content = lines.slice(-12).join("\n").slice(-8000);
  return { content, truncated: Boolean(log.truncated || content.length < log.content.length) };
}
