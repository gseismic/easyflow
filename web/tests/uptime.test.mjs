import assert from "node:assert/strict";
import test from "node:test";
import { dailyUptime, dayBounds, failureKey, failureRunHref, localDay, periodBounds, periodRange, readRecentHours, recentHoursKey, saveRecentHours, shiftDay, shiftPeriod, slotDescription, uptimeFailuresUrl, uptimeLogPreview, uptimeRunHref, uptimeSlotKey, uptimeTicks, uptimeUrl, validRecentHours } from "../src/uptime.js";

test("最近小时查询不依赖日历或浏览器时间，排序并保留特殊 ID", () => {
  const url = new URL(uptimeUrl(["b", "a/中文?+", "b"], "", "recent", 72), "http://localhost");
  assert.deepEqual(url.searchParams.getAll("dag_id"), ["a/中文?+", "b"]);
  assert.equal(url.searchParams.get("lookback_hours"), "72");
  assert.equal(url.searchParams.has("start_at"), false);
  assert.equal(url.searchParams.has("end_at"), false);
  assert.equal(uptimeUrl([], "", "recent"), null);
  assert.equal(uptimeUrl(["a"], "", "recent", 169), null);
});

test("小时数校验与保存降级不覆盖有效偏好", () => {
  let value = null;
  const storage = { getItem: key => key === recentHoursKey ? value : null, setItem: (key, next) => { if (key === recentHoursKey) value = next; } };
  assert.equal(readRecentHours(storage), 24);
  for (const hours of [1, 24, 168]) {
    assert.equal(validRecentHours(hours), true);
    assert.equal(saveRecentHours(storage, hours), true);
    assert.equal(readRecentHours(storage), hours);
  }
  for (const invalid of [0, -1, 169, 1.5, "", "bad", "1e2", null]) {
    assert.equal(validRecentHours(invalid), false);
    assert.equal(saveRecentHours(storage, invalid), false);
    assert.equal(readRecentHours(storage), 168);
  }
  value = "bad json";
  assert.equal(readRecentHours(storage), 24);
  assert.equal(readRecentHours({ getItem() { throw new Error("denied"); } }), 24);
  assert.equal(saveRecentHours(null, 12), false);
});

test("滚动窗口保留半小时桶标识，最近说明仍为时间段", () => {
  const first = { start_at: "2026-11-15T16:01:00Z", end_at: "2026-11-15T16:30:00Z", state: "failed" };
  assert.equal(uptimeSlotKey(first, "recent"), uptimeSlotKey({ ...first, start_at: "2026-11-15T16:02:00Z" }, "recent"));
  assert.notEqual(uptimeSlotKey(first, "day"), uptimeSlotKey({ ...first, start_at: "2026-11-15T16:02:00Z" }, "day"));
  assert.equal(dailyUptime("recent"), false);
  assert.equal(dailyUptime("week"), true);
  assert.equal(dailyUptime("month"), true);
  assert.match(slotDescription(first, "recent"), /\d{2}:\d{2} — .*?\d{2}:\d{2}/);
});

test("短滚动窗口坐标不错误标记次日，跨午夜标出日期", () => {
  const original = process.env.TZ;
  process.env.TZ = "Asia/Shanghai";
  try {
    const slots = [{ start_at: "2026-11-15T15:12:00Z", end_at: "2026-11-15T15:30:00Z" }, { start_at: "2026-11-15T15:30:00Z", end_at: "2026-11-15T16:00:00Z" }, { start_at: "2026-11-15T16:00:00Z", end_at: "2026-11-15T16:12:00Z" }];
    const ticks = uptimeTicks(slots, "recent");
    assert.equal(ticks.length, 2);
    assert.match(ticks[0].label, /11\/15.*23:12/);
    assert.match(ticks[1].label, /11\/16.*00:12/);
    assert.doesNotMatch(ticks[1].label, /\+1/);
    assert.equal(uptimeTicks([], "day").length, 0);
    assert.match(uptimeTicks(slots, "day").at(-1).label, /11-16/);
  } finally {
    if (original === undefined) delete process.env.TZ;
    else process.env.TZ = original;
  }
});

test("本地日期转为 UTC 范围，跨月切换保留特殊工作流 ID", () => {
  const original = process.env.TZ;
  process.env.TZ = "Asia/Shanghai";
  try {
    const url = new URL(uptimeUrl(["b", "a/中文?+", "b"], "2026-01-02"), "http://localhost");
    assert.deepEqual(url.searchParams.getAll("dag_id"), ["a/中文?+", "b"]);
    assert.equal(url.searchParams.get("start_at"), "2026-01-01T16:00:00.000Z");
    assert.equal(url.searchParams.get("end_at"), "2026-01-02T16:00:00.000Z");
    assert.equal(shiftDay("2026-03-01", -1), "2026-02-28");
    assert.equal(shiftDay("2026-12-31", 1), "2027-01-01");
    assert.equal(localDay(new Date("2026-01-01T23:59:00Z")), "2026-01-02");
    assert.equal(uptimeUrl([], "2026-01-02"), null);
    assert.equal(dayBounds("2026-02-30"), null);
    assert.equal(dayBounds(""), null);
  } finally {
    if (original === undefined) delete process.env.TZ;
    else process.env.TZ = original;
  }
});

test("夏令时的本地日保留 23 或 25 小时，每格仍为半小时", () => {
  const original = process.env.TZ;
  process.env.TZ = "America/New_York";
  try {
    for (const [day, hours] of [["2026-03-08", 23], ["2026-11-01", 25]]) {
      const { start, end } = dayBounds(day);
      assert.equal((end - start) / 3600000, hours);
      assert.equal(shiftDay(day, 1), day === "2026-03-08" ? "2026-03-09" : "2026-11-02");
    }
  } finally {
    if (original === undefined) delete process.env.TZ;
    else process.env.TZ = original;
  }
});

test("午夜跳变后的日周月边界独立构造，不继承归一化的 01:00", () => {
  const original = process.env.TZ;
  process.env.TZ = "America/Santiago";
  try {
    const day = dayBounds("2026-09-06");
    assert.equal(day.start.getHours(), 1);
    assert.equal(day.end.getHours(), 0);
    assert.equal(localDay(day.end), "2026-09-07");
    assert.equal((day.end - day.start) / 1800000, 46);
    for (const [period, first, last] of [
      ["week", "2026-08-31", "2026-09-07"],
      ["month", "2026-09-01", "2026-10-01"],
    ]) {
      const bounds = periodBounds("2026-09-06", period);
      assert.equal(localDay(bounds.start), first);
      assert.equal(localDay(bounds.end), last);
      assert.equal(bounds.start.getHours(), 0);
      assert.equal(bounds.end.getHours(), 0);
      const url = new URL(uptimeUrl(["demo"], "2026-09-06", period), "http://localhost");
      assert.equal(url.searchParams.get("start_at"), bounds.start.toISOString());
      assert.equal(url.searchParams.get("end_at"), bounds.end.toISOString());
    }
    assert.equal(shiftPeriod("2026-09-06", "month", 1), "2026-10-06");
  } finally {
    if (original === undefined) delete process.env.TZ;
    else process.env.TZ = original;
  }
});

test("月初自身发生午夜跳变时，月末仍使用下月自己的午夜", () => {
  const original = process.env.TZ;
  process.env.TZ = "America/Asuncion";
  try {
    const bounds = periodBounds("2017-10-01", "month");
    assert.equal(bounds.start.getHours(), 1);
    assert.equal(bounds.end.getHours(), 0);
    assert.equal((bounds.end - bounds.start) / 3600000, 31 * 24 - 1);
  } finally {
    if (original === undefined) delete process.env.TZ;
    else process.env.TZ = original;
  }
});

test("格子说明包含时间、错误运行数和等待语义", () => {
  const result = slotDescription({
    start_at: "2026-01-02T00:00:00Z", end_at: "2026-01-02T00:30:00Z", state: "failed",
    run_count: 3, failed_run_count: 2, waiting_run_count: 1,
  });
  assert.match(result, /发生错误/);
  assert.match(result, /3 次运行有执行/);
  assert.match(result, /2 次运行发生错误（含重试）/);
  assert.match(result, /1 次运行正在等待调度、依赖或重试/);
});

test("红格链接保留具体失败任务与尝试，重试成功后仍可查看原日志", () => {
  const slot = { sample_run_id: "run/失败 +%?", sample_task_id: "task/发布 +%?", sample_try_number: 2 };
  const params = new URLSearchParams(uptimeRunHref("flow/中文", slot).split("?")[1]);
  assert.equal(params.get("dag_id"), "flow/中文");
  assert.equal(params.get("run_id"), slot.sample_run_id);
  assert.equal(params.get("task_id"), slot.sample_task_id);
  assert.equal(params.get("attempt"), "2");
});

test("失败时段查询与日志链接保留原边界、特殊 ID 和具体尝试", () => {
  const slot = { start_at: "2026-11-01T00:00:00-04:00", end_at: "2026-11-02T00:00:00-05:00" };
  const url = new URL(uptimeFailuresUrl("flow/中文?+%", slot, 25), "http://localhost");
  assert.equal(url.searchParams.get("dag_id"), "flow/中文?+%");
  assert.equal(url.searchParams.get("start_at"), slot.start_at);
  assert.equal(url.searchParams.get("end_at"), slot.end_at);
  assert.equal(url.searchParams.get("offset"), "25");
  const item = { run_id: "run/失败?+%", task_id: "task/发布?+%", try_number: 2 };
  const params = new URLSearchParams(failureRunHref("flow/中文?+%", item).split("?")[1]);
  assert.equal(params.get("run_id"), item.run_id);
  assert.equal(params.get("task_id"), item.task_id);
  assert.equal(params.get("attempt"), "2");
  const fallback = new URLSearchParams(failureRunHref("flow/中文?+%", { run_id: item.run_id }).split("?")[1]);
  assert.equal(fallback.has("task_id"), false);
  assert.equal(fallback.has("attempt"), false);
  assert.notEqual(failureKey(item), failureKey({ ...item, try_number: 1 }));
  assert.notEqual(failureKey({ run_id: "a:b", task_id: "c", try_number: 1 }), failureKey({ run_id: "a", task_id: "b:c", try_number: 1 }));
  const runError = { run_id: item.run_id, task_id: null, ended_at: "2026-11-16T23:10:00Z" };
  assert.notEqual(failureKey(runError), failureKey({ ...runError, ended_at: "2026-11-16T23:15:00Z" }));
});

test("日志预览保留错误末尾，限制行数和长度并区分空日志与不可用日志", () => {
  assert.equal(uptimeLogPreview({ content: null }), null);
  assert.deepEqual(uptimeLogPreview({ content: "" }), { content: "", truncated: false });
  const lines = Array.from({ length: 20 }, (_, i) => `行 ${i}`).join("\n");
  const preview = uptimeLogPreview({ content: lines });
  assert.equal(preview.content.split("\n").length, 12);
  assert.equal(preview.content.endsWith("行 19"), true);
  assert.equal(preview.truncated, true);
  assert.equal(uptimeLogPreview({ content: "x".repeat(9000) }).content.length, 8000);
  assert.equal(uptimeLogPreview({ content: "short", truncated: true }).truncated, true);
});

test("周从周一开始，月份包含完整日期，默认仍查询半小时明细", () => {
  const original = process.env.TZ;
  process.env.TZ = "Asia/Shanghai";
  try {
    const week = periodBounds("2026-11-15", "week");
    assert.equal(localDay(week.start), "2026-11-09");
    assert.equal(localDay(week.end), "2026-11-16");
    assert.equal(periodRange("2026-11-15", "week"), "2026-11-09 — 2026-11-15");
    for (const [month, days] of [["2026-02", 28], ["2024-02", 29], ["2026-04", 30], ["2026-01", 31]]) {
      const bounds = periodBounds(`${month}-15`, "month");
      assert.equal((bounds.end - bounds.start) / 86400000, days);
      assert.equal(periodRange(`${month}-15`, "month"), month);
    }
    const url = new URL(uptimeUrl(["demo"], "2026-11-15", "week"), "http://localhost");
    assert.equal(url.searchParams.get("granularity"), "day");
    assert.equal(url.searchParams.get("timezone"), "Asia/Shanghai");
    assert.equal(url.searchParams.get("start_at"), "2026-11-08T16:00:00.000Z");
    assert.equal(url.searchParams.get("end_at"), "2026-11-15T16:00:00.000Z");
    assert.equal(new URL(uptimeUrl(["demo"], "2026-11-15"), "http://localhost").searchParams.has("granularity"), false);
    assert.equal(periodBounds("2026-11-15", "invalid"), null);
  } finally {
    if (original === undefined) delete process.env.TZ;
    else process.env.TZ = original;
  }
});

test("前后周期处理跨年、短月份和周偏移", () => {
  assert.equal(shiftPeriod("2026-01-31", "month", 1), "2026-02-28");
  assert.equal(shiftPeriod("2024-01-31", "month", 1), "2024-02-29");
  assert.equal(shiftPeriod("2026-03-31", "month", -1), "2026-02-28");
  assert.equal(shiftPeriod("2026-12-31", "month", 1), "2027-01-31");
  assert.equal(shiftPeriod("2026-01-01", "week", -1), "2025-12-25");
  assert.equal(shiftPeriod("2026-03-01", "day", -1), "2026-02-28");
});

test("跨夏令时的周按本地午夜取界，日汇总说明没有伪造半小时时段", () => {
  const original = process.env.TZ;
  process.env.TZ = "America/New_York";
  try {
    const bounds = periodBounds("2026-03-08", "week");
    assert.equal((bounds.end - bounds.start) / 3600000, 167);
    const description = slotDescription({ start_at: "2026-03-08T05:00:00Z", end_at: "2026-03-09T04:00:00Z", state: "failed", run_count: 2, failed_run_count: 1 }, "week");
    assert.match(description, /2026/);
    assert.match(description, /2 次运行有执行/);
    assert.doesNotMatch(description, /00:00/);
  } finally {
    if (original === undefined) delete process.env.TZ;
    else process.env.TZ = original;
  }
});
