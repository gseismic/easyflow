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

export function uptimeUrl(dagIds, day) {
  const bounds = dayBounds(day);
  const ids = [...new Set(dagIds)].sort();
  if (!bounds || !ids.length) return null;
  const params = new URLSearchParams({
    start_at: bounds.start.toISOString(),
    end_at: bounds.end.toISOString(),
  });
  ids.forEach((id) => params.append("dag_id", id));
  return `/api/workflows/uptime?${params}`;
}

export function slotTime(value) {
  return new Intl.DateTimeFormat("zh-CN", {
    hour: "2-digit", minute: "2-digit", hour12: false,
  }).format(new Date(value));
}

export function slotDescription(slot) {
  const local = (value) => new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", hour12: false,
  }).format(new Date(value));
  const parts = [`${local(slot.start_at)} — ${local(slot.end_at)}`, uptimeStates[slot.state] || "未知"];
  if (slot.run_count) parts.push(`${slot.run_count} 次运行有执行`);
  if (slot.failed_run_count) parts.push(`${slot.failed_run_count} 次运行发生错误（含重试）`);
  if (slot.waiting_run_count) parts.push(`${slot.waiting_run_count} 次运行正在等待调度、依赖或重试`);
  return parts.join(" · ");
}
import { workflowHistoryRunHref } from "./listRoute.js";


export function uptimeRunHref(dagId, slot) {
  const href = workflowHistoryRunHref(dagId, slot.sample_run_id, {});
  const [path, query] = href.split("?");
  const params = new URLSearchParams(query);
  if (slot.sample_task_id) params.set("task_id", slot.sample_task_id);
  if (slot.sample_try_number) params.set("attempt", String(slot.sample_try_number));
  return `${path}?${params}`;
}
