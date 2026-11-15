import { workflowHistoryRunHref } from "./listRoute.js";

export const uptimePeriods = {
  day: { label: "日", unit: "天", current: "今天" },
  week: { label: "周", unit: "周", current: "本周" },
  month: { label: "月", unit: "月", current: "本月" },
};

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

export function dayBounds(day) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) return null;
  const start = new Date(`${day}T00:00:00`);
  if (!Number.isFinite(start.getTime()) || localDay(start) !== day) return null;
  const end = new Date(start);
  end.setDate(end.getDate() + 1);
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
  const start = new Date(bounds.start);
  if (period === "week") start.setDate(start.getDate() - (start.getDay() + 6) % 7);
  else start.setDate(1);
  const end = new Date(start);
  if (period === "week") end.setDate(end.getDate() + 7);
  else end.setMonth(end.getMonth() + 1);
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

export function uptimeUrl(dagIds, day, period = "day") {
  const bounds = periodBounds(day, period);
  const ids = [...new Set(dagIds)].sort();
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

export function slotDescription(slot, period = "day") {
  const local = (value) => new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", hour12: false,
  }).format(new Date(value));
  const range = period === "day" ? `${local(slot.start_at)} — ${local(slot.end_at)}` :
    new Intl.DateTimeFormat("zh-CN", { year: "numeric", month: "2-digit", day: "2-digit", weekday: "short" }).format(new Date(slot.start_at));
  const parts = [range, uptimeStates[slot.state] || "未知"];
  if (slot.run_count) parts.push(`${slot.run_count} 次运行有执行`);
  if (slot.failed_run_count) parts.push(`${slot.failed_run_count} 次运行发生错误（含重试）`);
  if (slot.waiting_run_count) parts.push(`${slot.waiting_run_count} 次运行正在等待调度、依赖或重试`);
  return parts.join(" · ");
}
export function uptimeRunHref(dagId, slot) {
  const href = workflowHistoryRunHref(dagId, slot.sample_run_id, {});
  const [path, query] = href.split("?");
  const params = new URLSearchParams(query);
  if (slot.sample_task_id) params.set("task_id", slot.sample_task_id);
  if (slot.sample_try_number) params.set("attempt", String(slot.sample_try_number));
  return `${path}?${params}`;
}
