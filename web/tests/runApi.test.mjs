import assert from "node:assert/strict";
import test from "node:test";
import { clearRunUrl, runDefinitionStatus, runDetailUrl, taskLogUrl } from "../src/runApi.js";

test("运行接口通过查询参数保留特殊 ID，路径不含 ID", () => {
  const dag = "demo";
  const run = "custom/run +%?#中文";
  const task = "group/task +%?#中文";
  for (const [value, path] of [
    [runDetailUrl(dag, run), "/api/history/run"],
    [taskLogUrl(dag, run, task, "2"), "/api/history/task-log"],
    [clearRunUrl(dag, run, true), "/api/runs/clear-preview"],
    [clearRunUrl(dag, run), "/api/runs/clear"],
  ]) {
    const url = new URL(value, "http://localhost");
    assert.equal(decodeURIComponent(url.pathname), path);
    assert.equal(url.searchParams.get("dag_id"), dag);
    assert.equal(url.searchParams.get("run_id"), run);
    if (path.endsWith("task-log")) {
      assert.equal(url.searchParams.get("task_id"), task);
      assert.equal(url.searchParams.get("attempt"), "2");
    }
  }
  assert.equal(new URL(taskLogUrl(dag, run, task), "http://localhost").searchParams.has("attempt"), false);
});

test("任务元数据须经最新的两个响应共同确认", () => {
  const run = { definition_hash: "original", definition_matches: true };
  assert.equal(runDefinitionStatus(run, { definition_hash: "new" }), "different");
  assert.equal(runDefinitionStatus(run, { definition_hash: "original" }), "matching");
  assert.equal(runDefinitionStatus(run, null), "unknown");
  assert.equal(runDefinitionStatus({ ...run, definition_matches: false }, run), "different");
  assert.equal(runDefinitionStatus({ definition_pending: true }, run), "pending");
});
