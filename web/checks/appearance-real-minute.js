// 真实时钟示例，不使用 page.clock 或模拟设置结果。
// 连续运行同一命令两次：第一次创建场景，返回分钟边界后再次运行以验证并清理。
// playwright-cli -s=<会话> --raw run-code --filename=web/checks/appearance-real-minute.js
async original => {
  const origin = original.url().split('/').slice(0, 3).join('/');
  if (!/^http:\/\/127\.0\.0\.1:\d+$/.test(origin)) throw new Error('仅用于本机隔离演示');
  const browser = original.context().browser();
  let sample;
  for (const context of browser.contexts()) {
    for (const page of context.pages()) {
      if (page.url().startsWith(origin) && await page.evaluate(() => window.name === 'easyflow-real-theme-minute').catch(() => false)) sample = page;
    }
  }
  if (sample) {
    const snapshot = await sample.evaluate(() => ({ ...window.appearanceRealExample, now: Date.now() }));
    if (snapshot.boundary - snapshot.now > 20000) return { phase: '等待真实分钟边界', boundary: new Date(snapshot.boundary).toISOString(), remainingMs: snapshot.boundary - snapshot.now };
    try {
      await sample.waitForFunction(() => window.appearanceRealExample.changes.some(item => item.theme === 'dark'), null, { timeout: 25000 });
      const record = await sample.evaluate(() => window.appearanceRealExample);
      const switched = record.changes.find(item => item.theme === 'dark');
      if (!switched || switched.at < record.boundary || switched.at - record.boundary > 3000) throw new Error(`真实分钟切换未按时发生：${JSON.stringify(record)}`);
      await sample.screenshot({ path: '/tmp/easyflow-theme-real-after.png' });
      return { phase: '真实示例通过', dayStart: record.dayStart, nightStart: record.nightStart, initialTheme: record.initialTheme, finalTheme: switched.theme, nativeClock: record.nativeClock, transitionDelayMs: switched.at - record.boundary };
    } finally { await sample.context().close(); }
  }
  const state = await original.context().storageState();
  for (const item of state.origins || []) item.localStorage = item.localStorage.filter(value => value.name !== 'easyflow:appearance:v1');
  const context = await browser.newContext({ storageState: state, viewport: { width: 1440, height: 900 }, timezoneId: 'Asia/Shanghai' });
  const page = await context.newPage();
  try {
    await context.route('**/api/**', route => route.request().method() === 'GET' ? route.continue() : route.fulfill({ status: 405, json: { detail: '示例禁止业务写操作' } }));
    await page.goto(`${origin}/#/settings`);
    await page.getByRole('radio', { name: '自动', exact: true }).check();
    let remaining = await page.evaluate(() => 60000 - Date.now() % 60000);
    if (remaining < 8000) await page.waitForTimeout(remaining + 1000);
    const timing = await page.evaluate(() => {
      const now = new Date();
      const boundary = Math.floor(now.getTime() / 60000) * 60000 + 60000;
      const format = date => `${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}`;
      return { dayStart: format(now), nightStart: format(new Date(boundary)), boundary, nativeClock: Date.toString().includes('[native code]') };
    });
    if (!timing.nativeClock) throw new Error('真实示例不接受模拟时钟');
    await page.getByLabel('白天开始', { exact: true }).fill(timing.dayStart);
    await page.getByLabel('黑夜开始', { exact: true }).fill(timing.nightStart);
    await page.getByRole('button', { name: '保存自动时间', exact: true }).click();
    await page.waitForFunction(() => document.documentElement.dataset.theme === 'light');
    await page.evaluate(timing => {
      window.name = 'easyflow-real-theme-minute';
      window.appearanceRealExample = { ...timing, initialTheme: document.documentElement.dataset.theme, changes: [] };
      new MutationObserver(() => window.appearanceRealExample.changes.push({ at: Date.now(), theme: document.documentElement.dataset.theme }))
        .observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
    }, timing);
    await page.bringToFront();
    await page.screenshot({ path: '/tmp/easyflow-theme-real-before.png' });
    return { phase: '已创建真实示例', dayStart: timing.dayStart, nightStart: timing.nightStart, boundary: new Date(timing.boundary).toISOString(), nativeClock: timing.nativeClock };
  } catch (error) { await context.close(); throw error; }
}
