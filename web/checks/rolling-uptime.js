// 已登录本机隔离演示页面，含 daily_sync 的真实失败及重试记录：
// playwright-cli -s=<会话> --raw run-code --filename=web/checks/rolling-uptime.js
// 独立上下文；目标范围/日图/失败日志使用真实服务，仅错误与慢响应场景注入故障。
async original => {
  const origin = original.url().split('/').slice(0, 3).join('/');
  if (!/^http:\/\/127\.0\.0\.1:\d+$/.test(origin)) throw new Error('仅用于本机隔离演示');
  const check = (value, message) => { if (!value) throw new Error(message); };
  const storageKey = 'easyflow:uptime-hours:v1';
  const state = await original.context().storageState();
  for (const item of state.origins || []) item.localStorage = item.localStorage.filter(value => value.name !== storageKey && value.name !== 'easyflow:appearance:v1');
  const context = await original.context().browser().newContext({ storageState: state, viewport: { width: 1440, height: 1000 }, timezoneId: 'Asia/Shanghai' });
  const extraContexts = [];
  const page = await context.newPage();
  page.setDefaultTimeout(10000);
  const errors = [];
  const writes = [];
  const result = {};
  const reads = [];
  let failDay = false;
  let delayDay = false;
  page.on('pageerror', error => errors.push(error.message));
  await context.route('**/api/**', async route => {
    const request = route.request();
    if (request.method() !== 'GET') { writes.push(request.url()); return route.fulfill({ status: 405, json: { detail: '检查禁止业务写入' } }); }
    if (request.url().includes('/api/workflows/uptime?')) {
      reads.push(request.url());
      const daily = !request.url().includes('lookback_hours=') && !request.url().includes('granularity=day');
      if (daily && delayDay) await page.waitForTimeout(700);
      if (daily && failDay) return route.fulfill({ status: 503, json: { detail: '模拟当天暂不可用' } });
    }
    return route.continue();
  });
  const root = page.locator('.uptime-detail-chart > .uptime-chart');
  const slots = () => root.locator('.uptime-slots').first().locator('button');
  const period = name => page.getByRole('button', { name: `${name}视图`, exact: true });
  const hours = page.getByRole('spinbutton', { name: '最近小时数', exact: true });
  const apply = page.getByRole('button', { name: '应用', exact: true });
  const dayRegion = () => page.locator('.uptime-day-detail');
  const setHours = async value => {
    await hours.fill(String(value));
    const response = page.waitForResponse(response => response.url().includes(`lookback_hours=${value}`) && response.ok());
    await apply.click();
    await response;
    await slots().first().waitFor();
  };
  const noOverflow = async label => check(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), `${label} 横向溢出`);
  try {
    await page.goto(`${origin}/#/dags/daily_sync`);
    await slots().first().waitFor();
    check(await period('最近').getAttribute('aria-pressed') === 'true', '未默认最近模式');
    check(await hours.inputValue() === '24', '默认不是 24 小时');
    const response = await page.request.get(`${origin}/api/workflows/uptime?dag_id=daily_sync&lookback_hours=24`);
    check(response.ok(), '真实最近 API 不可用');
    const data = await response.json();
    check(Date.parse(data.end_at) - Date.parse(data.start_at) === 86400000, '最近范围不是完整 24 小时');
    check(data.end_at === data.as_of && data.workflows.daily_sync.some(slot => slot.failed_run_count), '凌晨窗口丢失前一晚失败');
    check(await root.locator('.uptime-date-number').count() === 0, '最近格子被当成每日汇总');
    await root.locator('button.uptime-failed').first().click();
    await root.locator('.uptime-failure-log').waitFor();
    const originalHref = await root.getByRole('link', { name: '查看完整日志 →', exact: true }).getAttribute('href');
    check(originalHref.includes('attempt=1'), '滚动图丢失原始失败尝试');
    await root.locator('.uptime-failure-log').getByText(/RuntimeError/).waitFor();
    await root.locator('.uptime-failures').evaluate(node => { window.rollingPanelBefore = node; });
    await page.waitForTimeout(5500);
    check(await root.locator('.uptime-failures').evaluate(node => window.rollingPanelBefore === node), '轮询重建失败面板');
    check(await root.getByRole('link', { name: '查看完整日志 →', exact: true }).getAttribute('href') === originalHref, '轮询改变选中范围/日志入口');
    const newer = await (await page.request.get(`${origin}/api/workflows/uptime?dag_id=daily_sync&lookback_hours=24`)).json();
    check(Date.parse(newer.as_of) > Date.parse(data.as_of), '真实窗口未随时间推进');
    result.actualMidnightAndPolling = { slots: data.workflows.daily_sync.length, originalAttempt: 1, retainedPanel: true };

    for (const invalid of ['', '0', '169', '1.5']) {
      await hours.fill(invalid);
      check(await apply.isDisabled(), `非法小时数可应用：${invalid}`);
    }
    await setHours(12);
    await page.reload();
    await slots().first().waitFor();
    check(await hours.inputValue() === '12', '小时数刷新后丢失');
    await setHours(168);
    await page.waitForFunction(() => document.querySelectorAll('.uptime-detail-chart > .uptime-chart > .uptime-scroll .uptime-slot').length >= 336);
    await noOverflow('最大最近窗口');
    await setHours(24);
    result.hoursAndPersistence = true;

    const definition = await (await page.request.get(`${origin}/api/dags/daily_sync`)).json();
    const fixtureDay = await page.evaluate(iso => {
      const date = new Date(iso);
      return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
    }, definition.runs[0].start_date || definition.runs[0].queued_at);
    const dayOfMonth = Number(fixtureDay.slice(-2));
    const weekday = await page.evaluate(day => (new Date(`${day}T12:00:00`).getDay() + 6) % 7, fixtureDay);
    await period('周').click();
    await page.getByLabel('运行时段日期', { exact: true }).fill(fixtureDay);
    await page.waitForFunction(() => document.querySelectorAll('.uptime-detail-chart > .uptime-chart > .uptime-scroll .uptime-slot').length === 7);
    await slots().nth(weekday).click();
    await dayRegion().locator('.uptime-slots button').first().waitFor();
    check(await period('周').getAttribute('aria-pressed') === 'true', '展开日图切走了周视图');
    check(await dayRegion().getAttribute('aria-label') === `${fixtureDay} 的当天运行时段`, '展开日期错误');
    check(await slots().count() === 7 && await dayRegion().locator('.uptime-slots button').count() === 48, '周/日层级数量错误');
    check(await dayRegion().locator('.uptime-slot').first().evaluate(node => node.getBoundingClientRect().height === 20), '展开日图格子未缩小');
    const refreshedDay = page.waitForResponse(response => response.url().includes('/api/workflows/uptime?') && response.url().includes('start_at=') && !response.url().includes('granularity=day') && response.ok(), { timeout: 1200 });
    await page.locator('.workflow-header').getByRole('button', { name: '↻ 刷新', exact: true }).click();
    await refreshedDay;
    await dayRegion().locator('button.uptime-failed').first().click();
    await dayRegion().locator('.uptime-failure-log').getByText(/RuntimeError/).waitFor();
    check((await dayRegion().getByRole('link', { name: '查看完整日志 →', exact: true }).getAttribute('href')).includes('attempt=1'), '展开日图原始日志错误');
    await page.keyboard.press('Escape');
    check(await dayRegion().count() === 1 && await dayRegion().locator('.uptime-failures').count() === 0, '内层 Escape 同时关闭了日图');
    await page.keyboard.press('Escape');
    check(await dayRegion().count() === 0, '外层 Escape 未收起日图');
    result.weekInlineAndOriginalLog = true;

    await period('月').click();
    await page.waitForFunction(() => document.querySelectorAll('.uptime-detail-chart > .uptime-chart > .uptime-scroll .uptime-slot').length >= 28);
    await slots().nth(dayOfMonth - 1).click();
    await dayRegion().locator('.uptime-slots button').first().waitFor();
    check(await period('月').getAttribute('aria-pressed') === 'true', '月展开切走父视图');
    await page.screenshot({ path: '/tmp/easyflow-month-inline-day.png' });
    await slots().nth(Math.max(0, dayOfMonth - 2)).click();
    await dayRegion().getByRole('heading', { name: new RegExp(`${fixtureDay.slice(0, -2)}${String(Math.max(1, dayOfMonth - 1)).padStart(2, '0')}`) }).waitFor();
    await dayRegion().getByRole('button', { name: '收起当天', exact: true }).click();
    check(await dayRegion().count() === 0, '收起后日图仍驻留');
    const future = root.locator('button.uptime-daily-slot.uptime-future').first();
    if (await future.count()) {
      const before = reads.length;
      await future.click();
      check(await dayRegion().count() === 0 && reads.length === before, '未来日触发展开请求');
    }
    failDay = true;
    await slots().nth(dayOfMonth - 1).click();
    await dayRegion().getByRole('alert').getByText(/模拟当天暂不可用/).waitFor();
    failDay = false;
    await dayRegion().getByRole('button', { name: '重试当天', exact: true }).click();
    await dayRegion().locator('.uptime-slots button').first().waitFor();
    delayDay = true;
    await slots().nth(Math.max(0, dayOfMonth - 2)).click();
    await slots().nth(dayOfMonth - 1).click();
    await dayRegion().getByRole('heading', { name: new RegExp(fixtureDay) }).waitFor();
    await page.waitForTimeout(850);
    check(await dayRegion().getAttribute('aria-label') === `${fixtureDay} 的当天运行时段`, '慢响应覆盖新选择');
    delayDay = false;
    result.monthSwitchFutureRetryAndRace = true;

    for (const width of [320, 390, 768, 1440]) {
      await page.setViewportSize({ width, height: 900 });
      await noOverflow(`${width}px 周月展开`);
    }
    await page.setViewportSize({ width: 390, height: 844 });
    await page.screenshot({ path: '/tmp/easyflow-month-inline-mobile.png' });
    await page.setViewportSize({ width: 1440, height: 1000 });
    await page.goto(`${origin}/#/settings`);
    await page.getByRole('radio', { name: '黑夜', exact: true }).check();
    await page.goto(`${origin}/#/dags/daily_sync`);
    await period('月').click();
    await page.getByLabel('运行时段日期', { exact: true }).fill(fixtureDay);
    await slots().nth(dayOfMonth - 1).click();
    await dayRegion().locator('.uptime-slots button').first().waitFor();
    check(await page.evaluate(() => document.documentElement.dataset.theme) === 'dark', '日图影响黑夜主题');
    await page.screenshot({ path: '/tmp/easyflow-month-inline-dark.png' });
    result.breakpointsAndDark = true;

    await page.goto(`${origin}/#/?q=daily_sync`);
    await page.locator('.workflow-uptime-row .uptime-slot').first().waitFor();
    check(await period('最近').getAttribute('aria-pressed') === 'true' && await hours.inputValue() === '24', '列表未共用最近小时配置');
    await period('月').click();
    await page.getByLabel('运行时段日期', { exact: true }).fill(fixtureDay);
    await page.locator('.workflow-uptime-row .uptime-slots').first().locator('button').nth(dayOfMonth - 1).click();
    await dayRegion().locator('.uptime-slots button').first().waitFor();
    await page.setViewportSize({ width: 390, height: 844 });
    await noOverflow('列表窄屏日图');
    result.listInline = true;
    const restrictedContext = await original.context().browser().newContext({ storageState: state, viewport: { width: 390, height: 844 } });
    extraContexts.push(restrictedContext);
    await restrictedContext.addInitScript(() => {
      const originalSet = Storage.prototype.setItem;
      Storage.prototype.setItem = function(key, value) {
        if (key === 'easyflow:uptime-hours:v1') throw new DOMException('blocked', 'QuotaExceededError');
        return originalSet.call(this, key, value);
      };
    });
    const restricted = await restrictedContext.newPage();
    restricted.on('pageerror', error => errors.push(error.message));
    await restricted.goto(`${origin}/#/dags/daily_sync`);
    await restricted.getByRole('spinbutton', { name: '最近小时数', exact: true }).fill('12');
    await restricted.getByRole('button', { name: '应用', exact: true }).click();
    await restricted.getByText('小时数已在本页应用，浏览器未能保存；刷新后可能丢失。', { exact: true }).waitFor();
    await restricted.locator('.uptime-slots button').first().waitFor();
    await restricted.reload();
    await restricted.getByRole('spinbutton', { name: '最近小时数', exact: true }).waitFor();
    check(await restricted.getByRole('spinbutton', { name: '最近小时数', exact: true }).inputValue() === '24', '禁用存储未按默认值恢复');
    result.storageFallback = true;
    const session = await (await page.request.get(`${origin}/api/auth/session`)).json();
    if (session.role === 'viewer') check(await page.getByRole('button', { name: /触发运行|暂停调度|恢复调度/ }).count() === 0, '只读权限回归');
    check(errors.length === 0 && writes.length === 0, `异常或业务写请求：${errors.join('; ')}`);
    result.role = session.role;
    result.pageErrors = errors;
    return result;
  } catch (error) { throw new Error(`已完成 ${JSON.stringify(result)}；${error.message}`); }
  finally { for (const extra of extraContexts) await extra.close(); await context.close(); }
}
