// 已登录本机演示页面：playwright-cli -s=<会话> --raw run-code --filename=web/checks/appearance.js
// 使用独立上下文、真实设置界面和 localStorage；业务写请求一律拦截。
async original => {
  const origin = original.url().split('/').slice(0, 3).join('/');
  if (!/^http:\/\/127\.0\.0\.1:\d+$/.test(origin)) throw new Error('仅用于本机隔离演示');
  const check = (value, message) => { if (!value) throw new Error(message); };
  const key = 'easyflow:appearance:v1';
  const state = await original.context().storageState();
  for (const item of state.origins || []) item.localStorage = item.localStorage.filter(value => value.name !== key);
  const contexts = [];
  const errors = [];
  const writes = [];
  const result = {};
  const create = async options => {
    const context = await original.context().browser().newContext({ storageState: state, viewport: { width: 1440, height: 1000 }, timezoneId: 'Asia/Shanghai', ...options });
    contexts.push(context);
    context.on('page', page => { page.setDefaultTimeout(10000); page.on('pageerror', error => errors.push(error.message)); });
    await context.route('**/api/**', async route => {
      if (route.request().method() !== 'GET') { writes.push(route.request().url()); return route.fulfill({ status: 405, json: { detail: '主题检查不允许业务写操作' } }); }
      return route.continue();
    });
    return context;
  };
  const theme = (page, value) => page.waitForFunction(value => document.documentElement.dataset.theme === value, value);
  const openSettings = async page => {
    await page.goto(`${origin}/#/settings`);
    await page.getByRole('heading', { name: '设置', exact: true }).waitFor();
  };
  const mode = async (page, name, effective) => { await page.getByRole('radio', { name, exact: true }).check(); if (effective) await theme(page, effective); };
  const times = async (page, day, night) => {
    await page.getByLabel('白天开始', { exact: true }).fill(day);
    await page.getByLabel('黑夜开始', { exact: true }).fill(night);
    await page.getByRole('button', { name: '保存自动时间', exact: true }).click();
    await page.getByText('自动切换时间已保存。', { exact: true }).waitFor();
  };
  const contrast = async (page, selector, paint = 'color') => {
    const ratio = await page.locator(selector).first().evaluate((node, paint) => {
      const rgb = value => (value.match(/[\d.]+/g) || []).slice(0, 3).map(Number);
      const luminance = values => values.map(value => { const part = value / 255; return part <= 0.04045 ? part / 12.92 : ((part + 0.055) / 1.055) ** 2.4; }).reduce((sum, value, i) => sum + value * [0.2126, 0.7152, 0.0722][i], 0);
      let background = node;
      while (background.parentElement && ['rgba(0, 0, 0, 0)', 'transparent'].includes(getComputedStyle(background).backgroundColor)) background = background.parentElement;
      const style = getComputedStyle(node);
      const painted = paint === 'boxShadow' ? style.boxShadow.match(/rgba?\([^)]+\)/)?.[0] : style.color;
      if (!painted) throw new Error('选中标记缺少颜色');
      const foreground = luminance(rgb(painted));
      const back = luminance(rgb(getComputedStyle(background).backgroundColor));
      return (Math.max(foreground, back) + 0.05) / (Math.min(foreground, back) + 0.05);
    }, paint);
    check(ratio >= 4.5, `${selector} 正文对比度不足：${ratio}`);
    return Math.round(ratio * 100) / 100;
  };
  try {
    const context = await create();
    const page = await context.newPage();
    await page.goto(`${origin}/#/dags/daily_sync`);
    await page.locator('.uptime-slots button').first().waitFor();
    await page.getByRole('navigation', { name: '工作台设置', exact: true }).getByRole('link', { name: '设置', exact: true }).click();
    await page.getByRole('heading', { name: '设置', exact: true }).waitFor();
    await theme(page, 'light');
    await mode(page, '黑夜', 'dark');
    await page.reload();
    await page.getByRole('radio', { name: '黑夜', exact: true }).waitFor();
    check(await page.getByRole('radio', { name: '黑夜', exact: true }).isChecked(), '刷新丢失黑夜模式');
    await theme(page, 'dark');
    check(await page.getByLabel('白天开始', { exact: true }).isDisabled(), '固定主题仍可误改自动时间');
    const ratios = { darkHelp: await contrast(page, '.settings-help'), darkOption: await contrast(page, '.theme-option:has(input:checked) > span') };
    await page.screenshot({ path: '/tmp/easyflow-settings-dark.png' });
    const second = await context.newPage();
    await openSettings(second);
    await theme(second, 'dark');
    await mode(page, '白天', 'light');
    await theme(second, 'light');
    check(await second.getByRole('radio', { name: '白天', exact: true }).isChecked(), '跨标签页未同步设置控件');
    ratios.lightHelp = await contrast(second, '.settings-help');
    await mode(second, '黑夜', 'dark');
    await theme(page, 'dark');
    await second.close();
    result.modesPersistenceAndTabs = { ratios };

    const early = await context.newPage();
    let release;
    const gate = new Promise(resolve => { release = resolve; });
    await early.route('**/assets/*.js', async route => { await gate; return route.continue(); });
    try {
      await early.goto(`${origin}/#/settings`, { waitUntil: 'commit' });
      await theme(early, 'dark');
      await early.waitForFunction(() => getComputedStyle(document.documentElement).colorScheme === 'dark');
      check(await early.evaluate(() => document.querySelector('meta[name="theme-color"]').content === '#101913'), '首屏浏览器配色未同步');
      const html = await (await early.request.get(`${origin}/`)).text();
      check(html.indexOf('<meta charset=') >= 0 && html.indexOf('<meta charset=') < 1024, '主题脚本把字符集声明推迟到首个 1024 字节之后');
      check(await early.evaluate(() => !document.querySelector('.workspace-shell')), '应用脚本未被阻断，首屏检查无效');
      check(await early.evaluate(() => getComputedStyle(document.documentElement).backgroundColor === 'rgb(16, 25, 19)'), '应用启动前仍为白色背景');
      release();
      await early.getByRole('heading', { name: '设置', exact: true }).waitFor();
      result.beforeApplicationPaint = true;
    } finally { release(); await early.close(); }

    const timedContext = await create();
    const timed = await timedContext.newPage();
    await timed.clock.install({ time: new Date('2026-11-17T06:59:50+08:00') });
    await openSettings(timed);
    await mode(timed, '自动', 'dark');
    await timed.clock.fastForward(10000);
    await theme(timed, 'light');
    await times(timed, '08:00', '20:30');
    await theme(timed, 'dark');
    await timed.reload();
    await timed.getByRole('radio', { name: '自动', exact: true }).waitFor();
    check(await timed.getByLabel('白天开始', { exact: true }).inputValue() === '08:00', '自动时间未保存');
    await timed.getByLabel('白天开始', { exact: true }).fill('');
    await timed.getByText('请填写有效的白天和黑夜开始时间。', { exact: true }).waitFor();
    check(await timed.getByRole('button', { name: '保存自动时间', exact: true }).isDisabled(), '空时间可以保存');
    await timed.getByLabel('白天开始', { exact: true }).fill('08:00');
    await timed.getByLabel('黑夜开始', { exact: true }).fill('08:00');
    await timed.getByText('白天和黑夜的开始时间不能相同。', { exact: true }).waitFor();
    check(await timed.getByRole('button', { name: '保存自动时间', exact: true }).isDisabled(), '相等时间可以保存');
    check(await timed.evaluate(key => JSON.parse(localStorage.getItem(key)).nightStart, key) === '20:30', '非法草稿覆盖了有效设置');
    await timed.getByLabel('黑夜开始', { exact: true }).fill('20:30');
    await timed.clock.fastForward(60 * 60 * 1000);
    await theme(timed, 'light');
    await timed.clock.fastForward((12 * 60 + 30) * 60 * 1000);
    await theme(timed, 'dark');
    await times(timed, '22:00', '06:00');
    await timed.clock.fastForward(90 * 60 * 1000);
    await theme(timed, 'light');
    await timed.clock.fastForward(8 * 60 * 60 * 1000);
    await theme(timed, 'dark');
    await timed.clock.setSystemTime(new Date('2026-11-18T22:30:00+08:00'));
    await timed.evaluate(() => window.dispatchEvent(new Event('focus')));
    await theme(timed, 'light');
    await timed.clock.setSystemTime(new Date('2026-11-19T07:30:00+08:00'));
    await timed.evaluate(() => document.dispatchEvent(new Event('visibilitychange')));
    await theme(timed, 'dark');
    await mode(timed, '白天', 'light');
    await timed.clock.fastForward(60 * 60 * 1000);
    await theme(timed, 'light');
    result.automaticBoundariesAndResume = true;

    const restrictedContext = await create();
    await restrictedContext.addInitScript(() => {
      window.appearanceWriteDenied = true;
      const originalSet = Storage.prototype.setItem;
      Storage.prototype.setItem = function(key, value) {
        if (key === 'easyflow:appearance:v1' && window.appearanceWriteDenied) throw new DOMException('blocked', 'QuotaExceededError');
        return originalSet.call(this, key, value);
      };
    });
    const restricted = await restrictedContext.newPage();
    await openSettings(restricted);
    await mode(restricted, '黑夜', 'dark');
    await restricted.getByText('外观已在当前页面应用，但浏览器不允许保存；刷新后可能丢失此次更改。', { exact: true }).waitFor();
    check(await restricted.getByText('主题模式已保存。', { exact: true }).count() === 0, '保存失败却提示成功');
    await restricted.reload();
    await restricted.getByRole('heading', { name: '设置', exact: true }).waitFor();
    await theme(restricted, 'light');
    await mode(restricted, '黑夜', 'dark');
    await restricted.evaluate(() => { window.appearanceWriteDenied = false; });
    await restricted.getByRole('button', { name: '重试保存', exact: true }).click();
    await restricted.getByText('外观设置已保存。', { exact: true }).waitFor();
    await restricted.reload();
    await theme(restricted, 'dark');
    const unavailableContext = await create();
    await unavailableContext.addInitScript(() => { Object.defineProperty(window, 'localStorage', { configurable: true, get() { throw new DOMException('blocked', 'SecurityError'); } }); });
    const unavailable = await unavailableContext.newPage();
    await openSettings(unavailable);
    await theme(unavailable, 'light');
    await mode(unavailable, '黑夜', 'dark');
    await unavailable.getByRole('button', { name: '重试保存', exact: true }).waitFor();
    await page.evaluate(key => localStorage.setItem(key, 'bad json'), key);
    await page.reload();
    await page.getByRole('heading', { name: '设置', exact: true }).waitFor();
    await theme(page, 'light');
    await mode(page, '黑夜', 'dark');
    result.storageFallbackAndRecovery = true;

    for (const width of [320, 390, 1440]) {
      await page.setViewportSize({ width, height: 900 });
      await page.waitForFunction(() => document.querySelector('#workspace-navigation').open === (innerWidth > 1000));
      check(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), `${width}px 设置页横向溢出`);
    }
    await page.setViewportSize({ width: 390, height: 844 });
    await page.goto(`${origin}/#/dags/daily_sync`);
    await page.getByRole('heading', { name: 'daily_sync', exact: true }).waitFor();
    const opener = page.getByRole('button', { name: '打开工作流导航', exact: true });
    await opener.click();
    await page.getByRole('navigation', { name: '工作台设置', exact: true }).getByRole('link', { name: '设置', exact: true }).click();
    await page.getByRole('heading', { name: '设置', exact: true }).waitFor();
    await page.waitForFunction(() => !document.querySelector('#workspace-navigation').open);
    check(await opener.evaluate(node => node === document.activeElement), '抽屉设置导航未恢复焦点');
    await page.screenshot({ path: '/tmp/easyflow-settings-mobile.png' });
    result.mobileSettings = true;

    await page.setViewportSize({ width: 1440, height: 1000 });
    await page.goto(`${origin}/#/dags/daily_sync`);
    await page.locator('.uptime-slots button').first().waitFor();
    const tabs = page.getByRole('navigation', { name: '工作流视图' });
    await tabs.getByRole('link', { name: '任务', exact: true }).click();
    await page.locator('.graph-node').first().waitFor();
    await contrast(page, '.node-title');
    await tabs.getByRole('link', { name: '日志', exact: true }).click();
    await page.locator('.log-output').waitFor();
    await contrast(page, '.log-output');
    await tabs.getByRole('link', { name: '源码', exact: true }).click();
    await page.locator('.source-output').waitFor();
    await contrast(page, '.source-output');
    await page.getByRole('button', { name: '＋ 触发运行', exact: true }).click();
    const dialog = page.getByRole('dialog', { name: '触发运行', exact: true });
    await dialog.waitFor();
    check(await dialog.evaluate(node => getComputedStyle(node).backgroundColor === 'rgb(23, 36, 28)'), '管理弹窗仍为白色');
    await dialog.getByRole('button', { name: '关闭', exact: true }).click();
    await page.goto(`${origin}/#/`);
    await page.getByRole('heading', { name: '你的工作流', exact: true }).waitFor();
    await theme(page, 'dark');
    await page.locator('.workflow-table .project-label').first().click();
    await contrast(page, '.project-label[aria-pressed="true"]', 'boxShadow');
    await page.goto(`${origin}/#/history`);
    await page.getByRole('heading', { name: '运行历史', exact: true }).waitFor();
    await theme(page, 'dark');
    await page.goto(`${origin}/#/history/run?dag_id=daily_sync&run_id=browser-failed`);
    await page.locator('.history-task.selected').waitFor();
    await contrast(page, '.history-task.selected', 'boxShadow');
    const loginState = await context.storageState();
    loginState.cookies = [];
    const loginContext = await create({ storageState: loginState });
    const login = await loginContext.newPage();
    await login.goto(`${origin}/`);
    await login.getByRole('heading', { name: '欢迎回来', exact: true }).waitFor();
    await theme(login, 'dark');
    check(await login.locator('.login-card').evaluate(node => getComputedStyle(node).backgroundColor === 'rgb(23, 36, 28)'), '登录卡片未适配深色');
    result.darkPageCoverage = true;
    check(errors.length === 0, `浏览器异常：${errors.join('; ')}`);
    check(writes.length === 0, `出现业务写操作：${writes.join(', ')}`);
    return result;
  } catch (error) {
    throw new Error(`已完成 ${JSON.stringify(result)}；${error.message}`);
  } finally { for (const context of contexts) await context.close(); }
}
