// 在已打开本地 easyflow 页面的 playwright-cli 会话中执行：
// playwright-cli -s=<会话名> run-code --filename=web/checks/navigation-stability.js
// 独立上下文拦截全部 API，只使用模拟记录，不登录或修改真实业务状态。
async original => {
  const check = (condition, message) => { if (!condition) throw new Error(message); };
  const context = await original.context().browser().newContext({ viewport: { width: 1440, height: 900 }, timezoneId: 'Asia/Shanghai' });
  const page = await context.newPage();
  page.setDefaultTimeout(8000);
  const base = original.url().match(/^https?:\/\/[^/]+/)[0];
  const errors = [];
  const requests = new Map();
  const delays = new Map();
  const failures = new Set();
  const overrides = new Map();
  const mutations = [];
  let role = 'admin';
  page.on('pageerror', error => errors.push(error.message));
  const row = (dag_id, project = 'A') => ({ dag_id, project_kind: 'project', project, paused: false, running_count: 0, latest_run: null });
  const rows = [...Array.from({ length: 121 }, (_, index) => row(`flow-${String(index).padStart(3, '0')}`)), row('flow-b', 'B')];
  const groups = ['A', 'B'].map(project => ({ project_kind: 'project', project, total: rows.filter(item => item.project === project).length }));
  const reply = (route, data, status = 200) => route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(data) });
  await page.route('**/api/**', async route => {
    const request = route.request();
    const address = request.url();
    const entries = (address.split('?')[1] || '').split('&').filter(Boolean).map(entry => entry.split('=').map(value => decodeURIComponent(value.replace(/\+/g, ' '))));
    const params = { get: key => entries.find(entry => entry[0] === key)?.[1] || null, getAll: key => entries.filter(entry => entry[0] === key).map(entry => entry[1]) };
    const path = address.slice(base.length).split('?')[0];
    if (request.method() !== 'GET') { mutations.push(path); return reply(route, { detail: '回归不应执行写操作' }, 405); }
    if (path === '/api/auth/session') return reply(route, { username: 'navigation-check', role, csrf_token: 'fixture' });
    if (path === '/api/catalog') return reply(route, { loading: false, scheduler_running: false, scheduler: { running: false }, issues: [] });
    const query = params.get('q') || '';
    const matches = item => `${item.dag_id} ${item.project}`.includes(query);
    if (path === '/api/workflows/facets') return reply(route, {
      complete: true, pending: false,
      groups: groups.map(group => ({ ...group, matched: rows.filter(item => item.project === group.project && matches(item)).length })),
    });
    if (path === '/api/workflows') {
      const selected = rows.filter(item => item.project === params.get('project') && matches(item));
      const cursor = params.get('cursor');
      const start = cursor ? selected.findIndex(item => item.dag_id === cursor) + 1 : 0;
      const batch = selected.slice(start, start + 50);
      return reply(route, { dags: batch, loading: false, next_cursor: start + 50 < selected.length ? batch.at(-1).dag_id : null });
    }
    if (path === '/api/workflows/uptime') {
      const start = new Date(params.get('start_at'));
      const validStart = Number.isNaN(start.getTime()) ? new Date('2026-11-15T00:00:00+08:00') : start;
      const slots = Array.from({ length: 48 }, (_, index) => ({
        start_at: new Date(validStart.getTime() + index * 1800000).toISOString(),
        end_at: new Date(validStart.getTime() + (index + 1) * 1800000).toISOString(),
        state: 'idle', run_count: 0, failed_run_count: 0,
      }));
      return reply(route, { workflows: Object.fromEntries(params.getAll('dag_id').map(id => [id, slots])) });
    }
    if (path === '/api/history/run') return reply(route, {
      dag_id: 'removed-flow', run_id: 'saved-run', state: 'success', tasks: { saved_task: 'success' },
      task_details: {}, definition_matches: false,
    });
    if (path === '/api/history/task-log') return reply(route, { content: '历史任务日志仍可读取', attempts: [{ try_number: 1 }] });
    if (path === '/api/workflows/detail') {
      const id = params.get('dag_id');
      requests.set(id, (requests.get(id) || 0) + 1);
      const failed = failures.has(id);
      const definition = overrides.get(id) || rows.find(item => item.dag_id === id);
      if (delays.has(id)) await page.waitForTimeout(delays.get(id));
      return failed || !definition ? reply(route, { detail: '模拟定义不可用' }, 404) : reply(route, {
        ...definition, task_count: 0, tasks: [], runs: [], schedule: null, scheduler_running: false,
      });
    }
    return reply(route, { detail: `未预期的接口：${path}` }, 404);
  });
  const nav = page.locator('#workspace-navigation');
  const main = page.locator('.workspace-main');
  const leaf = id => nav.locator(`.navigation-workflow[href="#/dags/${id}"]`);
  const loaded = async id => {
    await main.getByRole('heading', { name: id, exact: true }).waitFor();
    await main.locator('.detail-load-state').waitFor({ state: 'detached' });
    await main.getByRole('heading', { name: '运行记录', exact: true }).waitFor();
  };
  const unique = async id => check(await leaf(id).count() === 1, `${id} 入口重复或丢失`);
  const result = {};
  try {
    await page.goto(`${base}/#/dags/flow-000`);
    await loaded('flow-000');
    await leaf('flow-001').waitFor();
    delays.set('flow-001', 700);
    await page.evaluate(() => {
      window.navigationBefore = document.querySelector('#workspace-navigation');
      window.navigationSamples = [];
      const node = document.querySelector('.navigation-workflow[href="#/dags/flow-001"]');
      const initialY = node.getBoundingClientRect().y;
      const initialScroll = document.querySelector('.navigation-scroll').scrollTop;
      window.sampleNavigation = true;
      const sample = () => {
        const links = document.querySelectorAll('.navigation-workflow[href="#/dags/flow-001"]');
        window.navigationSamples.push({ count: links.length, shift: Math.abs(node.getBoundingClientRect().y - initialY), scroll: Math.abs(document.querySelector('.navigation-scroll').scrollTop - initialScroll) });
        if (window.sampleNavigation) requestAnimationFrame(sample);
      };
      requestAnimationFrame(sample);
    });
    await leaf('flow-001').click();
    await main.locator('[aria-busy="true"]').waitFor();
    check(await main.getByRole('heading', { name: 'flow-001', exact: true }).isVisible(), '加载未显示目标名称');
    check(await main.getByRole('navigation', { name: '工作流视图' }).isVisible(), '加载时标签消失');
    check(await main.getByRole('button', { name: /触发运行|暂停调度/ }).count() === 0, '加载暴露管理操作');
    check(await main.evaluate(node => node.getBoundingClientRect().height >= innerHeight - 64), '加载时工作区高度塌缩');
    await loaded('flow-001');
    const samples = await page.evaluate(() => { window.sampleNavigation = false; return window.navigationSamples; });
    check(samples.length > 5 && samples.every(item => item.count === 1 && item.shift < 1 && item.scroll < 1), '慢速切换发生重复、位移或滚动跳动');
    check(await page.evaluate(() => window.navigationBefore === document.querySelector('#workspace-navigation')), '导航 DOM 被重建');
    const before = requests.get('flow-001');
    await leaf('flow-001').click();
    await page.waitForTimeout(100);
    check(requests.get('flow-001') === before, '重点击当前入口重新加载了详情');
    result.slowAndRepeatedClick = { samples: samples.length, maxShift: Math.max(...samples.map(item => item.shift)) };

    delays.set('flow-002', 900);
    delays.set('flow-003', 180);
    await leaf('flow-002').click();
    await main.locator('[aria-busy="true"]').waitFor();
    await leaf('flow-003').click();
    await loaded('flow-003');
    await page.waitForTimeout(950);
    await unique('flow-003');
    check(await main.getByRole('heading', { name: 'flow-003', exact: true }).isVisible(), '慢响应覆盖了后续选择');
    check(await leaf('flow-003').getAttribute('aria-current') === 'page', '快速切换高亮错误');
    result.rapidSwitch = true;

    failures.add('flow-004');
    await leaf('flow-004').click();
    await main.getByRole('button', { name: '重试加载工作流', exact: true }).waitFor();
    await unique('flow-004');
    check(await main.getByRole('button', { name: /触发运行|暂停调度/ }).count() === 0, '首次失败暴露旧管理操作');
    failures.delete('flow-004');
    await main.getByRole('button', { name: '重试加载工作流', exact: true }).click();
    await loaded('flow-004');
    result.errorAndRetry = true;

    const search = nav.getByRole('textbox', { name: '搜索导航工作流', exact: true });
    await search.fill('flow-b');
    await page.waitForFunction(() => document.querySelectorAll('.navigation-project').length === 1);
    await nav.getByRole('button', { name: '展开项目 B', exact: true }).click();
    await leaf('flow-b').waitFor();
    await unique('flow-004');
    check(await leaf('flow-004').locator('..').getAttribute('class') === 'navigation-pinned', '搜索外的当前入口未保留');
    delays.set('flow-b', 300);
    await leaf('flow-b').click();
    await main.locator('[aria-busy="true"]').waitFor();
    await unique('flow-b');
    await loaded('flow-b');
    await nav.getByRole('button', { name: '清空搜索', exact: true }).click();
    await leaf('flow-049').waitFor();
    await leaf('flow-049').click();
    await loaded('flow-049');
    const groupA = nav.locator('.navigation-project').filter({ has: page.getByTitle('A', { exact: true }) });
    await groupA.getByRole('button', { name: '下一批', exact: true }).click();
    await leaf('flow-050').waitFor();
    await unique('flow-049');
    check(await groupA.locator('.navigation-pinned').count() === 1, '分页未保留当前入口');
    await groupA.getByRole('button', { name: '上一批', exact: true }).click();
    await leaf('flow-000').waitFor();
    await unique('flow-049');
    check(await groupA.locator('.navigation-pinned').count() === 0, '返回当前批次未移除置顶项');
    result.searchAndPagination = true;

    overrides.set('flow-010', row('flow-010', 'B'));
    await leaf('flow-010').click();
    await loaded('flow-010');
    await unique('flow-010');
    check(await leaf('flow-010').locator('xpath=ancestor::li[contains(@class,"navigation-project")]').locator('.navigation-project-link > span').innerText() === 'B', '详情归属未优先于导航缓存');
    result.projectCorrection = true;

    await page.evaluate(() => { location.hash = '#/history/run?dag_id=removed-flow&run_id=saved-run'; });
    await main.getByText('历史任务日志仍可读取', { exact: true }).waitFor();
    await unique('removed-flow');
    await nav.getByText('当前定义不可用，历史记录仍可查看。', { exact: true }).waitFor();
    result.removedDefinitionHistory = true;

    role = 'viewer';
    await page.goto(`${base}/#/dags/flow-001`);
    await page.reload();
    await main.locator('[aria-busy="true"]').waitFor();
    check(await main.getByRole('link', { name: '源码', exact: true }).count() === 0, '只读加载态暴露源码入口');
    await loaded('flow-001');
    check(await main.getByRole('link', { name: '源码', exact: true }).count() === 0, '只读详情暴露源码入口');
    await page.setViewportSize({ width: 390, height: 844 });
    const opener = page.getByRole('button', { name: '打开工作流导航', exact: true });
    await opener.click();
    check(await nav.evaluate(node => node.matches(':modal')), '手机导航不是模态抽屉');
    await leaf('flow-b').click();
    await page.waitForFunction(() => !document.querySelector('#workspace-navigation').open);
    await loaded('flow-b');
    check(await opener.evaluate(node => node === document.activeElement), '抽屉导航后未恢复焦点');
    check(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), '窄屏出现页面横向溢出');
    await opener.click();
    await page.keyboard.press('Escape');
    await page.waitForFunction(() => !document.querySelector('#workspace-navigation').open);
    check(await nav.locator('.navigation-workflow[aria-current="page"]').count() === 1, '窄屏高亮重复');
    check(errors.length === 0, `浏览器异常：${errors.join('; ')}`);
    check(mutations.length === 0, `出现写操作：${mutations.join(', ')}`);
    result.viewerAndMobile = true;
    return result;
  } catch (error) {
    throw new Error(`已完成：${JSON.stringify(result)}；${error.message}`);
  } finally {
    await context.close();
  }
}
