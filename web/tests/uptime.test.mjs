import assert from "node:assert/strict";
import test from "node:test";
import { dayBounds, localDay, shiftDay, slotDescription, uptimeRunHref, uptimeUrl } from "../src/uptime.js";

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
