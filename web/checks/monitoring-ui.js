// 在含 daily_sync 演示记录、已登录的本机页面运行：
// playwright-cli -s=<会话> --raw run-code --filename=web/checks/monitoring-ui.js
// 创建独立上下文；全部写请求被拦截，不触发、暂停、清理或回补实际运行。
async original => {
  const origin = original.url().split('/').slice(0, 3).join('/');
  if (!/^http:\/\/127\.0\.0\.1:\d+$/.test(origin)) throw new Error('仅用于本机隔离演示');
  const check = (value, message) => { if (!value) throw new Error(message); };
  const context = await original.context().browser().newContext({ storageState: await original.context().storageState(), viewport: { width: 1440, height: 900 }, timezoneId: 'Asia/Shanghai' });
  const page = await context.newPage();
  page.setDefaultTimeout(10000);
  const errors = [];
  const writes = [];
  const uptimeRequests = [];
  const result = {};
  const longId = '超长工作流-' + '价格同步_long_name_'.repeat(24);
  let longDefinition;
  let longError = true;
  let viewer = false;
  page.on('pageerror', error => errors.push(error.message));
  await page.route('**/api/**', async route => {
    const request = route.request();
    const path = request.url().split('?')[0].slice(origin.length);
    if (request.method() !== 'GET') { writes.push(path); return route.fulfill({ status: 405, json: { detail: '界面检查禁止写操作' } }); }
    if (path === '/api/workflows/uptime') {
      uptimeRequests.push(request.url());
      if (request.url().includes(encodeURIComponent(longId))) {
        return route.fulfill({ json: { workflows: { [longId]: Array.from({ length: 48 }, (_, i) => ({
          start_at: new Date(1794672000000 + i * 1800000).toISOString(), end_at: new Date(1794673800000 + i * 1800000).toISOString(),
          state: 'idle', run_count: 0, failed_run_count: 0,
        })) } } });
      }
    }
    if (path === '/api/auth/session' && viewer) {
      const response = await route.fetch();
      return route.fulfill({ response, json: { ...(await response.json()), role: 'viewer' } });
    }
    if (path === '/api/workflows/detail' && request.url().includes(`dag_id=${encodeURIComponent(longId)}`)) return route.fulfill(longError ? { status: 503, json: { detail: '模拟加载失败' } } : { json: longDefinition });
    return route.continue();
  });
  const main = page.locator('.workspace-main');
  const nav = page.locator('#workspace-navigation');
  const tabs = main.getByRole('navigation', { name: '工作流视图' });
  const tab = name => tabs.getByRole('link', { name, exact: true });
  const ready = async () => {
    await main.getByRole('heading', { name: 'daily_sync', exact: true }).waitFor();
    await main.locator('.detail-load-state').waitFor({ state: 'detached' });
  };
  const noOverflow = async label => check(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), `${label} 页面横向溢出`);
  try {
    await page.goto(`${origin}/#/dags/daily_sync`);
    await ready();
    await main.locator('.run-metadata').waitFor();
    await main.locator('.uptime-slots button').first().waitFor();
    await page.waitForFunction(() => document.querySelector('.navigation-project-link small')?.textContent === '4');
    const positions = await page.evaluate(() => {
      const top = selector => Math.round(document.querySelector(selector).getBoundingClientRect().top);
      return { chart: top('.uptime-slots'), runSelector: top('.run-selector'), metadata: top('.run-metadata'), firstWorkflow: top('.navigation-branch .navigation-workflow') };
    });
    check(positions.chart <= 450 && positions.runSelector <= 640 && positions.metadata < 850 && positions.firstWorkflow <= 290, `首屏标准未满足：${JSON.stringify(positions)}`);
    check(await main.locator('.uptime-legend > span').count() === 6, '运行图例未完整保留');
    check(await main.locator('.uptime-period button').evaluateAll(nodes => nodes.every(node => { const box = node.getBoundingClientRect(); return box.width >= 44 && box.height >= 44; })), '周期按钮的操作区域不足 44px');
    check(await main.locator('.workflow-header').getByRole('button', { name: '↻ 刷新', exact: true }).count() === 1, '详情缺少统一刷新入口');
    check(await main.locator('.uptime-card').getByRole('button', { name: '↻ 刷新', exact: true }).count() === 0, '保留了重复刷新入口');
    await main.getByText('新运行将等待调度器启动', { exact: true }).waitFor();
    await page.screenshot({ path: '/tmp/easyflow-monitoring-desktop.png' });
    result.desktop = positions;

    // 两次刷新各自须在 1.2 秒内请求时间图，排除 5 秒轮询恰好经过的假阳性。
    for (let i = 0; i < 2; i++) {
      const response = page.waitForResponse(response => response.url().includes('/api/workflows/uptime?') && response.ok(), { timeout: 1200 });
      await main.locator('.workflow-header').getByRole('button', { name: '↻ 刷新', exact: true }).click();
      await response;
    }
    result.manualRefresh = true;
    const help = main.locator('.uptime-help');
    check(!(await help.evaluate(node => node.open)), '长说明默认展开');
    await help.locator('summary').focus();
    await page.keyboard.press('Enter');
    await help.getByText('红色保留曾发生的错误，重试成功后仍可回看；无执行不代表故障。', { exact: true }).waitFor();
    await page.keyboard.press('Space');
    check(!(await help.evaluate(node => node.open)), '键盘无法关闭说明');
    await main.getByRole('button', { name: '周视图', exact: true }).click();
    await page.waitForFunction(() => document.querySelectorAll('.uptime-slots button').length === 7);
    await main.getByRole('button', { name: '月视图', exact: true }).click();
    await page.waitForFunction(() => document.querySelectorAll('.uptime-slots button').length >= 28);
    await main.getByRole('button', { name: '日视图', exact: true }).click();
    await page.waitForFunction(() => document.querySelectorAll('.uptime-slots button').length === 48);
    const selectedDay = await main.getByLabel('运行时段日期', { exact: true }).inputValue();
    await main.getByRole('button', { name: '前一天', exact: true }).click();
    await page.waitForFunction(day => document.querySelector('input[aria-label="运行时段日期"]').value !== day, selectedDay);
    await main.getByRole('button', { name: '今天', exact: true }).click();
    await page.waitForFunction(day => document.querySelector('input[aria-label="运行时段日期"]').value === day, selectedDay);
    result.rangeAndExplanation = true;

    const definition = await (await page.request.get(`${origin}/api/dags/daily_sync`)).json();
    const fixtureDay = await page.evaluate(iso => {
      const date = new Date(iso);
      return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
    }, definition.runs[0].start_date || definition.runs[0].queued_at);
    await main.getByRole('button', { name: '日视图', exact: true }).click();
    await main.getByLabel('运行时段日期', { exact: true }).fill(fixtureDay);
    await main.locator('button.uptime-failed').first().click();
    await main.locator('.uptime-failure-log').waitFor();
    const failureLink = main.getByRole('link', { name: '查看完整日志 →', exact: true });
    const failureHref = await failureLink.getAttribute('href');
    check(failureHref.includes('attempt=1') && failureHref.includes('task_id='), '红格丢失原始失败尝试');
    await failureLink.click();
    await main.locator('.log-output').getByText(/RuntimeError/).waitFor();
    check(await main.locator('.log-toolbar select').inputValue() === '1', '日志没有定位失败尝试');
    const historyBack = main.getByRole('link', { name: '返回工作流运行历史', exact: true });
    check((await historyBack.getAttribute('href')).includes('tab=history'), '历史运行返回位置错误');
    await historyBack.click();
    await main.getByRole('region', { name: '本工作流的运行历史', exact: true }).waitFor();
    await tab('概览').click();
    await ready();
    await main.getByRole('button', { name: '日视图', exact: true }).click();
    await main.getByLabel('运行时段日期', { exact: true }).fill(fixtureDay);
    await main.locator('button.uptime-failed').first().click();
    await main.getByRole('link', { name: '查看完整日志 →', exact: true }).click();
    await main.locator('.log-toolbar select').waitFor();
    await main.getByRole('link', { name: '查看工作流', exact: true }).click();
    await ready();
    await tab('任务').click();
    await main.locator('.graph-node').first().waitFor();
    await main.getByRole('combobox', { name: '选择运行', exact: true }).selectOption('browser-failed');
    await main.locator('.graph-node').filter({ hasText: 'publish' }).click();
    await main.getByRole('link', { name: '查看所选任务日志 →', exact: true }).click();
    check(await main.getByRole('combobox', { name: '选择日志任务', exact: true }).inputValue() === 'publish', '任务选择未传给日志');
    check(await main.getByRole('combobox', { name: '选择运行', exact: true }).inputValue() === 'browser-failed', '切换标签丢失运行');
    await tab('运行历史').click();
    await main.getByRole('region', { name: '本工作流的运行历史', exact: true }).waitFor();
    await tab('调度详情').click();
    await main.getByRole('heading', { name: '调度详情', exact: true }).waitFor();
    await tab('源码').click();
    await main.locator('.source-output').waitFor();
    await tab('概览').click();
    await main.locator('.uptime-slots button').first().waitFor();
    await tab('任务').click();
    const requestsBeforeReturn = uptimeRequests.length;
    await tab('概览').click();
    await main.locator('.uptime-slots button').first().waitFor();
    await page.waitForTimeout(150);
    check(uptimeRequests.length === requestsBeforeReturn + 1, '图表重新挂载重复刷新或未刷新');
    result.logsAndTabs = true;

    const widths = [];
    for (const width of [320, 390, 768, 1000, 1440]) {
      await page.setViewportSize({ width, height: 900 });
      await page.waitForFunction(() => document.querySelector('#workspace-navigation').open === (innerWidth > 1000));
      await page.evaluate(() => window.scrollTo(0, 0));
      await noOverflow(`${width}px 详情`);
      if (width > 1000) check(await main.evaluate(node => node.getBoundingClientRect().left >= 272), '桌面主区进入导航列');
      widths.push(width);
    }
    await page.setViewportSize({ width: 390, height: 844 });
    await page.waitForFunction(() => !document.querySelector('#workspace-navigation').open);
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.screenshot({ path: '/tmp/easyflow-monitoring-mobile.png' });
    await page.setViewportSize({ width: 320, height: 844 });
    const sourceTab = tab('源码');
    await sourceTab.focus();
    check(await sourceTab.evaluate(node => { const rect = node.getBoundingClientRect(); return rect.left >= 0 && rect.right <= innerWidth; }), '键盘不能到达末尾标签');
    await page.setViewportSize({ width: 390, height: 844 });
    const opener = page.getByRole('button', { name: '打开工作流导航', exact: true });
    await opener.click();
    check(await nav.evaluate(node => node.matches(':modal')), '窄屏抽屉缺少模态约束');
    await page.keyboard.press('Escape');
    await page.waitForFunction(() => !document.querySelector('#workspace-navigation').open);
    check(await opener.evaluate(node => node === document.activeElement), '抽屉关闭未恢复焦点');
    result.breakpoints = widths;

    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto(`${origin}/#/?q=daily&project_kind=unassigned`);
    await main.getByRole('heading', { name: '你的工作流', exact: true }).waitFor();
    await main.getByRole('textbox', { name: '搜索工作流', exact: true }).waitFor();
    await main.locator('a[href="#/dags/daily_sync"]').first().click();
    await ready();
    await main.getByRole('link', { name: '返回工作流列表', exact: true }).click();
    await main.getByRole('heading', { name: '你的工作流', exact: true }).waitFor();
    check(await main.getByRole('textbox', { name: '搜索工作流', exact: true }).inputValue() === 'daily', '返回列表丢失搜索');
    check(page.url().includes('project_kind=unassigned'), '返回列表丢失项目过滤');
    await noOverflow('工作流列表');
    await nav.getByRole('navigation', { name: '主导航' }).getByRole('link', { name: '运行历史', exact: true }).click();
    await main.getByRole('heading', { name: '运行历史', exact: true }).waitFor();
    await main.getByRole('textbox', { name: '搜索运行历史', exact: true }).waitFor();
    await noOverflow('全局历史');
    result.listsAndReturn = true;

    longDefinition = { ...definition, dag_id: longId, runs: [], tasks: [], task_count: 0, paused: true };
    await page.evaluate(id => { location.hash = '#/dags/' + encodeURIComponent(id); }, longId);
    await main.getByRole('button', { name: '重试加载工作流', exact: true }).waitFor();
    check(await main.getByRole('button', { name: /触发运行|暂停调度/ }).count() === 0, '首次错误保留了旧管理操作');
    longError = false;
    await main.getByRole('button', { name: '重试加载工作流', exact: true }).click();
    await main.getByText('尚无运行。', { exact: false }).waitFor();
    for (const width of [320, 390, 1440]) {
      await page.setViewportSize({ width, height: 900 });
      await page.waitForFunction(() => document.querySelector('#workspace-navigation').open === (innerWidth > 1000));
      await noOverflow(`${width}px 长名称`);
    }
    viewer = true;
    await page.goto(`${origin}/#/dags/daily_sync?tab=source`);
    await page.reload();
    await ready();
    check(await main.getByRole('link', { name: '源码', exact: true }).count() === 0, '只读界面显示源码');
    check(await main.getByRole('button', { name: /触发运行|暂停调度|恢复调度/ }).count() === 0, '只读界面显示管理操作');
    result.errorsLongNamesAndViewer = true;
    check(errors.length === 0, `浏览器异常：${errors.join('; ')}`);
    check(writes.length === 0, `检查发出了写操作：${writes.join(', ')}`);
    result.uptimeRequests = uptimeRequests.length;
    return result;
  } catch (error) {
    throw new Error(`已完成 ${JSON.stringify(result)}；${error.message}`);
  } finally { await context.close(); }
}
