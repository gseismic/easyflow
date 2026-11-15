import test from "node:test";
import assert from "node:assert/strict";
import {
  navigationGroupKey, navigationGroupLabel, navigationGroups, navigationProjectHref,
  navigationWorkflowsUrl, navigationWorkflowStatus, readNavigationState, saveNavigationState,
} from "../src/navigation.js";
import { readListState } from "../src/listRoute.js";

test("导航按项目分组，普通标签不产生项目，搜索仅保留有匹配的分组", () => {
  const data = { groups: [
    { project_kind: "conflict", project: null, total: 1, matched: 0 },
    { project_kind: "project", project: "quant", total: 100, matched: 2 },
    { project_kind: "unassigned", project: null, total: 20, matched: 0 },
    { project_kind: "project", project: "analytics", total: 2, matched: 1 },
  ] };
  assert.deepEqual(navigationGroups(data).map(navigationGroupLabel), ["analytics", "quant", "未分组", "项目异常"]);
  assert.deepEqual(navigationGroups(data, "价格").map(navigationGroupLabel), ["analytics", "quant"]);
  assert.equal(data.groups[0].project_kind, "conflict");
  assert.notEqual(navigationGroupKey({ project_kind: "project", project: "unassigned" }), navigationGroupKey({ project_kind: "unassigned" }));
});

test("项目查询覆盖独立批次，完整保留项目、关键词和 cursor 特殊字符", () => {
  const group = { project_kind: "project", project: "A / 中文 +%?" };
  const url = new URL(navigationWorkflowsUrl(group, "price + 中文", "flow / 100?+%"), "http://localhost");
  assert.equal(url.searchParams.get("limit"), "50");
  assert.equal(url.searchParams.get("project"), group.project);
  assert.equal(url.searchParams.get("q"), "price + 中文");
  assert.equal(url.searchParams.get("cursor"), "flow / 100?+%");
  assert.equal(url.searchParams.has("state"), false);
  assert.equal(url.searchParams.has("tag"), false);
});

test("点击项目保留右侧关键词、状态和标签，项目范围替换准确", () => {
  const href = navigationProjectHref({ project_kind: "project", project: "quant & 中文" },
    { projectKind: "project", project: "old", tags: ["日+周"] },
    { query: "old", draft: "任务 / B", state: "failed" });
  const parsed = readListState(href, "dags");
  assert.deepEqual(parsed.filters, { projectKind: "project", project: "quant & 中文", tags: ["日+周"] });
  assert.equal(parsed.query, "任务 / B");
  assert.equal(parsed.state, "failed");
});

test("状态保留实际执行优先级，同时说明暂停而不覆盖失败", () => {
  assert.deepEqual(navigationWorkflowStatus({ running_count: 1, latest_run: { state: "failed" }, paused: true }), { state: "running", label: "暂停 · 运行中" });
  assert.deepEqual(navigationWorkflowStatus({ latest_run: { state: "failed" }, paused: true }), { state: "failed", label: "暂停 · 失败" });
  assert.equal(navigationWorkflowStatus({}).label, "未运行");
});

test("导航状态按账户键保存，过滤损坏数据并限制输入规模", () => {
  const values = new Map();
  const storage = { getItem: key => values.get(key), setItem: (key, value) => values.set(key, value) };
  const group = navigationGroupKey({ project_kind: "project", project: "A/B +%?" });
  saveNavigationState(storage, "reader", { query: "中文", expanded: new Set([group]), scrollTop: 420 });
  assert.deepEqual(readNavigationState(storage, "reader"), { query: "中文", expanded: [group], scrollTop: 420 });
  assert.deepEqual(readNavigationState(storage, "admin"), { query: "", expanded: [], scrollTop: 0 });
  storage.setItem("reader", JSON.stringify({ version: 1, query: "x".repeat(1200), expanded: [group, group, "bad json", 1, "{}"], scrollTop: -4 }));
  const parsed = readNavigationState(storage, "reader");
  assert.equal(parsed.query.length, 1000);
  assert.deepEqual(parsed.expanded, [group]);
  assert.equal(parsed.scrollTop, 0);
  const many = Array.from({ length: 240 }, (_, index) => navigationGroupKey({ project_kind: "project", project: `project-${index}` }));
  saveNavigationState(storage, "many-projects", { query: "", expanded: many, scrollTop: 420 });
  assert.deepEqual(readNavigationState(storage, "many-projects").expanded, many);
  storage.setItem("reader", "broken");
  assert.deepEqual(readNavigationState(storage, "reader"), { query: "", expanded: [], scrollTop: 0 });
});

test("存储不可用或配额耗尽不阻断导航", () => {
  const storage = { getItem() { throw new Error("disabled"); }, setItem() { throw new Error("quota"); } };
  assert.deepEqual(readNavigationState(storage, "reader"), { query: "", expanded: [], scrollTop: 0 });
  assert.doesNotThrow(() => saveNavigationState(storage, "reader", { query: "", expanded: [], scrollTop: 0 }));
});
