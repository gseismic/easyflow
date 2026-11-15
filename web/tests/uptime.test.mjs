import assert from "node:assert/strict";
import test from "node:test";
import { dayBounds, failureKey, failureRunHref, localDay, periodBounds, periodRange, shiftDay, shiftPeriod, slotDescription, uptimeFailuresUrl, uptimeLogPreview, uptimeRunHref, uptimeUrl } from "../src/uptime.js";

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
