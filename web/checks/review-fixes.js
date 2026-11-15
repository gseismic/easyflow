// 在已登录的本机隔离演示环境运行：playwright-cli -s=<会话> run-code --filename=web/checks/review-fixes.js
// 使用 daily_sync 夹具；所有修改请求在浏览器层拦截，不会清理或重跑真实任务。
async (page) => {
  const origin = page.url().split("/").slice(0, 3).join("/");
  if (!/^http:\/\/127\.0\.0\.1:\d+$/.test(origin)) throw new Error("仅允许本机隔离演示环境");
  const check = (condition, message) => { if (!condition) throw new Error(message); };
  const failures = [];
  const onError = error => failures.push(error.message);
  page.on("pageerror", onError);
  const previewBodies = [];
  const clearBodies = [];
  let previewArrived;
  let finishPreview;
  let ownStates = false;
  const special = ["__proto__", "constructor", "toString"];
  const routeHandler = async route => {
    const request = route.request();
    const path = request.url().split("?")[0].slice(origin.length);
    if (path === "/api/runs/clear-preview") {
      const body = request.postDataJSON();
      previewBodies.push(body);
      await new Promise(resolve => {
        finishPreview = async (status = 200) => {
          await route.fulfill({ status, json: status === 200 ? {
            to_clear: body.task_ids || ["extract", "validate", "publish"], blocked: [],
          } : { detail: "预览测试错误" } });
          resolve();
        };
        previewArrived(body);
      });
      return;
    }
    if (path === "/api/runs/clear") {
      const body = request.postDataJSON();
      clearBodies.push(body);
      await route.fulfill({ json: { to_clear: body.task_ids } });
      return;
    }
    if (request.method() !== "GET") {
      await route.fulfill({ status: 400, json: { detail: "浏览器验收禁止修改演示数据" } });
      return;
    }
    if (path === "/api/dags/daily_sync") {
      const response = await route.fetch();
      const dag = await response.json();
      dag.tasks.push(...special.map(task_id => ({ ...dag.tasks[0], task_id, upstream: [], downstream: [] })));
      await route.fulfill({ response, json: dag });
      return;
    }
    if (path === "/api/history/run" && request.url().includes("dag_id=daily_sync") && ownStates) {
      const response = await route.fetch();
      const run = await response.json();
      run.tasks = Object.fromEntries([...Object.entries(run.tasks), ...special.map(id => [id, "success"])]);
      await route.fulfill({ response, json: run });
      return;
    }
    await route.continue();
  };
  await page.route("**/api/**", routeHandler);
  try {
    await page.setViewportSize({ width: 1440, height: 1000 });
    await page.goto(`${origin}/#/dags/daily_sync?tab=tasks`);
    for (const id of special) {
      const node = page.locator(".graph-node").filter({ has: page.locator(".node-title", { hasText: id }) });
      await node.getByText("未知", { exact: true }).waitFor();
      await node.click();
      await page.getByRole("heading", { name: id, exact: true }).waitFor();
    }
    ownStates = true;
    await page.reload();
    for (const id of special) {
      const node = page.locator(".graph-node").filter({ has: page.locator(".node-title", { hasText: id }) });
      await node.getByText("成功", { exact: true }).waitFor();
    }
    await page.goto(`${origin}/#/dags/daily_sync?tab=logs`);
    await page.getByRole("button", { name: "预览重跑", exact: true }).click();
    const dialog = page.getByRole("dialog", { name: "预览重跑" });
    const scope = dialog.getByRole("combobox");
    await scope.selectOption("all");
    let seen = new Promise(resolve => { previewArrived = resolve; });
    await dialog.getByRole("button", { name: "查看影响" }).click();
    check((await seen).task_ids === null, "全部任务预览请求体错误");
    check(await scope.isDisabled(), "请求期间仍可更改重跑范围");
    await finishPreview();
    await dialog.getByText("将清理 3 个任务", { exact: true }).waitFor();
    check(await scope.isEnabled(), "预览结束后范围未解锁");
    await scope.selectOption("task");
    check(await dialog.getByRole("button", { name: "清理并等待重跑" }).count() === 0, "切换范围未丢弃原预览");
    seen = new Promise(resolve => { previewArrived = resolve; });
    await dialog.getByRole("button", { name: "查看影响" }).click();
    const selected = (await seen).task_ids;
    check(selected.length === 1, "当前任务预览包含多个任务");
    check(await scope.isDisabled(), "单任务预览期间范围未锁定");
    await finishPreview();
    await dialog.getByText("将清理 1 个任务", { exact: true }).waitFor();
    await dialog.getByRole("button", { name: "清理并等待重跑" }).click();
    await dialog.waitFor({ state: "hidden" });
    check(JSON.stringify(clearBodies) === JSON.stringify([{ task_ids: selected }]), "确认提交使用了过期范围");
    await page.getByRole("button", { name: "预览重跑", exact: true }).click();
    seen = new Promise(resolve => { previewArrived = resolve; });
    await dialog.getByRole("button", { name: "查看影响" }).click();
    await seen;
    await finishPreview(500);
    await dialog.getByRole("alert").getByText("预览测试错误", { exact: true }).waitFor();
    check(await scope.isEnabled(), "预览失败后范围未解锁");
    await dialog.getByRole("button", { name: "取消", exact: true }).click();

    await page.setViewportSize({ width: 390, height: 844 });
    await page.goto(`${origin}/#/dags/daily_sync?tab=tasks`);
    await page.getByText("左右滑动或使用方向键查看完整依赖图", { exact: true }).waitFor();
    const graph = page.getByRole("group", { name: "任务依赖图", exact: true });
    await graph.focus();
    await graph.press("ArrowRight");
    await page.waitForFunction(() => document.querySelector(".graph-scroll").scrollLeft > 0);
    await page.goto(`${origin}/#/`);
    await page.getByText("左右滑动或使用方向键查看全天时段", { exact: true }).first().waitFor();
    const firstSlot = page.locator(".uptime-slots").first().locator("button").first();
    await firstSlot.focus();
    await firstSlot.press("End");
    check(await page.locator(".uptime-slots").first().locator("button").last().evaluate(el => el === document.activeElement), "键盘未能到达当天最后一格");
    check(await page.locator(".uptime-scroll").first().evaluate(el => el.scrollLeft > 0), "键盘选择未滚动到完整时间轴");
    check(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), "窄屏文档整体水平溢出");
    await page.setViewportSize({ width: 1440, height: 1000 });
    await page.waitForFunction(() => [...document.querySelectorAll(".uptime-chart .scroll-hint")].length === 0);
    check(failures.length === 0, `浏览器渲染错误：${failures.join("；")}`);
    return { specialTaskNames: special, previewBodies, clearBodies, mobileScrolling: "通过", desktopHintHidden: true, pageErrors: failures };
  } finally {
    await page.unroute("**/api/**", routeHandler);
    page.off("pageerror", onError);
  }
}
