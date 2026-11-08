import test from "node:test";
import assert from "node:assert/strict";
import { emptyFilters, listHref, readListState, sameFilters, workflowHref, workflowHistoryRunHref } from "../src/listRoute.js";

test("列表条件在链接中安全往返，特殊字符和标签顺序保留", () => {
  const filters = { projectKind: "project", project: "量化 & 数据", tags: ["日报+周报", "a/b"] };
  const view = { query: "价格 + 成交", state: "failed" };
  const href = listHref("history", filters, view, "dag/一");
  const parsed = readListState(href, "history");
  assert.deepEqual(parsed.filters, filters);
  assert.equal(parsed.query, view.query);
  assert.equal(parsed.state, "failed");
  assert.equal(new URLSearchParams(href.split("?")[1]).get("dag_id"), "dag/一");
  assert.equal(sameFilters(parsed.filters, filters), true);
});

test("非法过滤值回退到默认值，标签去重且搜索长度受限", () => {
  const href = `#/?project_kind=project&state=missing&tag=a&tag=a&tag=&q=${"x".repeat(1100)}`;
  const parsed = readListState(href, "dags");
  assert.deepEqual(parsed.filters, { ...emptyFilters(), tags: ["a"] });
  assert.equal(parsed.state, "all");
  assert.equal(parsed.query.length, 1000);
  assert.equal(listHref("dags", emptyFilters(), { query: "", state: "all" }), "#/");
});

test("地址保存未提交的输入，中文组合输入期间保持已确认关键词", () => {
  const view = { query: "old", draft: "new", draftComposing: false, state: "all" };
  assert.equal(listHref("dags", emptyFilters(), view), "#/?q=new");
  assert.equal(
    listHref("dags", emptyFilters(), { ...view, draftComposing: true }),
    "#/?q=old",
  );
});

test("工作流历史和运行详情链接保留特殊字符与独立条件", () => {
  const view = { query: "旧词", draft: "任务 / A+B", draftComposing: false, state: "failed" };
  const href = workflowHref("work/量化", "history", view);
  assert.equal(href.startsWith("#/dags/work%2F%E9%87%8F%E5%8C%96?"), true);
  const parsed = readListState(href, "history");
  assert.equal(parsed.query, "任务 / A+B");
  assert.equal(parsed.state, "failed");
  const runParams = new URLSearchParams(workflowHistoryRunHref("work/量化", "run?一&二", view).split("?")[1]);
  assert.equal(runParams.get("dag_id"), "work/量化");
  assert.equal(runParams.get("run_id"), "run?一&二");
  assert.equal(runParams.get("from_workflow"), "1");
  assert.equal(runParams.get("from_q"), "任务 / A+B");
  assert.equal(runParams.get("from_state"), "failed");
  assert.equal(new URLSearchParams(workflowHref("x", "history", { ...view, draftComposing: true }).split("?")[1]).get("q"), "旧词");
  assert.equal(workflowHref("x"), "#/dags/x");
});
