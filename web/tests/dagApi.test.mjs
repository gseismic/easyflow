import assert from "node:assert/strict";
import test from "node:test";
import { backfillPreviewUrl, dagDetailUrl, dagSourceUrl, pauseDagUrl, triggerDagUrl } from "../src/dagApi.js";

test("工作流 API 的点号与保留字 ID 经浏览器 URL 解析后不改变路径", () => {
  for (const dagId of [".", "..", "detail", "source", "demo-a.b_1"]) {
    for (const [value, action] of [
      [dagDetailUrl(dagId), "detail"],
      [dagSourceUrl(dagId), "source"],
      [pauseDagUrl(dagId), "pause"],
      [triggerDagUrl(dagId), "trigger"],
      [backfillPreviewUrl(dagId), "backfill-preview"],
    ]) {
      const url = new URL(value, "http://localhost");
      assert.equal(url.pathname, `/api/workflows/${action}`);
      assert.equal(url.searchParams.get("dag_id"), dagId);
      assert.equal(url.searchParams.has("run_id"), false);
    }
    const runId = "custom/run +%?#中文";
    const url = new URL(dagSourceUrl(dagId, runId), "http://localhost");
    assert.equal(url.pathname, "/api/workflows/source");
    assert.equal(url.searchParams.get("dag_id"), dagId);
    assert.equal(url.searchParams.get("run_id"), runId);
  }
});
