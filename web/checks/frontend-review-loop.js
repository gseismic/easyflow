// 在任意本机 easyflow 页运行：playwright-cli -s=<会话> run-code --filename=web/checks/frontend-review-loop.js
// 独立浏览器上下文拦截全部 API；写操作只修改模拟记录，不接触业务库或调度器。
async original => {
  const origin = original.url().split('/').slice(0, 3).join('/');
  if (!/^http:\/\/127\.0\.0\.1:\d+$/.test(origin)) throw new Error('仅允许本机隔离环境');
  const check = (value, message) => { if (!value) throw new Error(message); };
  const context = await original.context().browser().newContext({ viewport: { width: 1440, height: 1000 }, timezoneId: 'Asia/Shanghai' });
  const page = await context.newPage();
  page.setDefaultTimeout(8000);
  const errors = [];
  const unexpected = [];
  const calls = [];
  const pendingTriggers = [];
  let previewResponse = { to_clear: ['example'], blocked: [] };
  let previewStatus = 200;
  let clearStatus = 200;
  let delayPreview = false;
  let finishPreview;
  let previewArrived;
  let delayClear = false;
  let finishClear;
  let clearArrived;
  let backfillStatus = 200;
  let backfillEligible = true;
  let role = 'admin';
  let rolledUptime = null;
  const ids = ['.', '..', 'detail', 'daily_sync'];
  const runId = 'custom/run +%?#中文';
  const historicalHash = 'historical-definition';
  const firstSlot = {
    start_at: '2026-11-16T23:03:00Z', end_at: '2026-11-16T23:30:00Z',
    state: 'failed', run_count: 1, failed_run_count: 1, sample_run_id: runId,
  };
  const nextSlot = {
    start_at: firstSlot.end_at, end_at: '2026-11-17T00:00:00Z',
    state: 'idle', run_count: 0, failed_run_count: 0,
  };
  const definitions = Object.fromEntries(ids.map(dag_id => [dag_id, {
    dag_id, definition_hash: historicalHash, task_count: 1,
    tasks: [{ task_id: 'example', upstream: [], downstream: [], operator_type: 'EmptyOperator' }],
    runs: [{ run_id: runId, state: 'success', run_type: 'manual', queued_at: '2026-11-16T23:00:00Z' }],
    schedule: '@daily', paused: false, scheduler_running: false, project_kind: 'project', project: 'review',
    timezone: 'UTC', tags: [], latest_run: { state: 'success' }, running_count: 0,
  }]));
  page.on('pageerror', error => errors.push(error.message));
  const reply = (route, data, status = 200) => route.fulfill({ status, json: data });
  await page.route('**/api/**', async route => {
    const request = route.request();
    const address = request.url();
    const path = address.split('?')[0].slice(origin.length);
    const entries = (address.split('?')[1] || '').split('&').filter(Boolean).map(entry => entry.split('=').map(value => decodeURIComponent(value.replace(/\+/g, ' '))));
    const get = key => entries.find(entry => entry[0] === key)?.[1];
    const dagId = get('dag_id');
    const definition = definitions[dagId];
    const body = request.method() === 'POST' ? request.postDataJSON() : null;
    calls.push({ path, dagId, runId: get('run_id'), start: get('start_at'), end: get('end_at'), body });
    if (path === '/api/auth/session') return reply(route, { username: 'review-check', role, csrf_token: 'fixture' });
    if (path === '/api/catalog') return reply(route, { loading: false, scheduler: { running: false }, issues: [] });
    if (path === '/api/workflows/facets') return reply(route, {
      complete: true, pending: false, groups: [{ project_kind: 'project', project: 'review', total: ids.length, matched: ids.length }],
    });
    if (path === '/api/workflows') return reply(route, { dags: Object.values(definitions), loading: false });
    if (path === '/api/workflows/uptime') {
      if (rolledUptime !== null && dagId === 'daily_sync' && get('lookback_hours')) {
        return reply(route, { workflows: { daily_sync: rolledUptime ? [nextSlot] : [firstSlot, nextSlot] } });
      }
      const start = get('start_at') ? new Date(get('start_at')).getTime() : Date.now() - 86400000;
      const slots = Array.from({ length: 48 }, (_, i) => ({ start_at: new Date(start + i * 1800000).toISOString(), end_at: new Date(start + (i + 1) * 1800000).toISOString(), state: 'idle', run_count: 0, failed_run_count: 0 }));
      return reply(route, { workflows: Object.fromEntries(ids.map(id => [id, slots])) });
    }
    if (path === '/api/workflows/uptime/failures' && dagId === 'daily_sync') {
      const items = [1, 2].map(try_number => ({
        run_id: runId, task_id: 'example', try_number, ended_at: '2026-11-16T23:15:00Z',
        error_message: `失败 ${try_number}`, run_state: 'success',
      }));
      return reply(route, { items, total: items.length, failed_run_count: 1, next_offset: null });
    }
    if (path === '/api/workflows/detail' && definition) return reply(route, definition);
    if (path === '/api/workflows/source' && definition) return reply(route, { content: `DAG(${JSON.stringify(dagId)})`, parsed_matches: true, run_matches: true, path: 'fixture.py' });
    if (path === '/api/workflows/pause' && definition) {
      definition.paused = body.paused;
      return reply(route, { paused: body.paused });
    }
    if (path === '/api/workflows/trigger' && definition) return new Promise(resolve => {
      pendingTriggers.push(async (status = 200, createdId = 'created-run') => {
        if (status === 200 && createdId !== 'created-run') {
          definition.runs.unshift({ ...definition.runs[0], run_id: createdId, state: 'queued' });
        }
        await reply(route, status === 200 ? { run_id: createdId, paused: definition.paused, scheduler_running: false } : { detail: '触发测试错误' }, status);
        resolve();
      });
    });
    if (path === '/api/workflows/backfill-preview' && definition) {
      const data = backfillStatus === 200
        ? [{ logical_date: '2026-07-01T00:00:00Z', eligible: backfillEligible }]
        : { detail: '回补预览测试错误' };
      return reply(route, data, backfillStatus);
    }
    if (path === '/api/history/run') return reply(route, {
      dag_id: dagId, run_id: get('run_id'), state: 'success', tasks: { example: 'success' },
      task_details: { example: { state: 'success', try_number: 1 } },
      definition_hash: historicalHash, definition_matches: true,
    });
    if (path === '/api/history/task-log') return reply(route, { content: '原始任务日志', attempts: [{ try_number: 1 }, { try_number: 2 }] });
    if (path === '/api/history/runs') return reply(route, { runs: [{ dag_id: dagId || '.', run_id: runId, state: 'success', run_type: 'manual', project_kind: 'project', project: 'review' }], issues: [], loading: false });
    if (path === '/api/history/runs/facets') return reply(route, { complete: true, pending: false, groups: [] });
    if (path === '/api/runs/clear-preview') {
      if (delayPreview) await new Promise(resolve => { finishPreview = resolve; previewArrived(); });
      return reply(route, previewStatus === 200 ? previewResponse : { detail: '预览测试错误' }, previewStatus);
    }
    if (path === '/api/runs/clear') {
      if (delayClear) await new Promise(resolve => { finishClear = resolve; clearArrived(); });
      return reply(route, clearStatus === 200 ? { to_clear: body.task_ids } : { detail: '任务状态已变化或仍在运行，请重新预览。' }, clearStatus);
    }
    unexpected.push(`${request.method()} ${address}`);
    return reply(route, { detail: `未预期接口：${path}` }, 404);
  });
  const main = page.locator('.workspace-main');
  const tabs = main.getByRole('navigation', { name: '工作流视图' });
  const tab = name => tabs.getByRole('link', { name, exact: true });
  const go = async (id, view = '') => {
    await page.evaluate(hash => { location.hash = hash; }, `#/dags/${encodeURIComponent(id)}${view ? `?tab=${view}` : ''}`);
    await main.getByRole('heading', { name: id, exact: true }).waitFor();
    await main.locator('.detail-load-state').waitFor({ state: 'detached' });
  };
  const trigger = page.getByRole('dialog', { name: '触发运行', exact: true });
  const openTrigger = async () => {
    await main.getByRole('button', { name: '＋ 触发运行', exact: true }).click();
    await trigger.waitFor();
  };
  const submitTrigger = async () => {
    const before = pendingTriggers.length;
    await trigger.getByRole('button', { name: '创建运行', exact: true }).click();
    await trigger.getByText('关闭窗口不会取消已提交的运行，可在运行记录中查看结果。', { exact: true }).waitFor();
    check(pendingTriggers.length === before + 1, '触发未到达模拟服务');
    return pendingTriggers.at(-1);
  };
  const clearDialog = page.getByRole('dialog', { name: '预览重跑', exact: true });
  const confirm = () => clearDialog.getByRole('button', { name: '清理并等待重跑', exact: true });
  const inspect = async (refresh = false) => {
    await clearDialog.getByRole('button', { name: refresh ? '重新预览' : '查看影响', exact: true }).click();
  };
  const result = {};
  try {
    await page.goto(`${origin}/#/dags/.`);
    for (const id of ids) {
      await go(id);
      await main.getByRole('combobox', { name: '选择运行', exact: true }).waitFor();
      await tab('任务').click();
      await main.locator('.graph-node').getByText('example', { exact: true }).waitFor();
      await tab('日志').click();
      await main.getByText('原始任务日志', { exact: true }).waitFor();
      await main.getByLabel('执行次数', { exact: false }).selectOption('2');
      await tab('运行历史').click();
      await main.getByRole('region', { name: '本工作流的运行历史', exact: true }).waitFor();
      await tab('调度详情').click();
      await main.getByRole('heading', { name: '调度详情', exact: true }).waitFor();
      await tab('源码').click();
      await main.getByText(`DAG(${JSON.stringify(id)})`, { exact: true }).waitFor();
      check(calls.some(call => call.path === '/api/workflows/source' && call.dagId === id && call.runId === runId), '源码丢失 DAG 或特殊 run_id');
      await main.getByRole('button', { name: '暂停调度', exact: true }).click();
      await main.getByRole('button', { name: '恢复调度', exact: true }).waitFor();
      await tab('概览').click();
      await main.getByRole('button', { name: '回补预览', exact: true }).click();
      const backfill = page.getByRole('dialog', { name: '回补日期预览', exact: true });
      await backfill.getByLabel('开始日期', { exact: true }).fill('2026-07-01');
      await backfill.getByLabel('结束日期', { exact: true }).fill('2026-07-01');
      await backfill.getByRole('button', { name: '预览日期', exact: true }).click();
      await backfill.getByText('共 1 个区间，当前可创建 1 个', { exact: true }).waitFor();
      await backfill.locator('.dialog-actions').getByRole('button', { name: '关闭', exact: true }).click();
      await openTrigger();
      const finish = await submitTrigger();
      await finish();
      await trigger.waitFor({ state: 'hidden' });
      check(await main.getByRole('combobox', { name: '选择运行', exact: true }).inputValue() === 'created-run', '正常触发未选中新运行');
      await page.evaluate(hash => { location.hash = hash; }, `#/history/run?dag_id=${encodeURIComponent(id)}&run_id=${encodeURIComponent(runId)}`);
      await main.getByText('原始任务日志', { exact: true }).waitFor();
      await main.getByRole('link', { name: '查看工作流', exact: true }).waitFor();
    }
    result.addressingAndSixTabs = ids;

    for (const closeKind of ['button', 'escape', 'cross', 'unmount']) {
      await go('daily_sync');
      await openTrigger();
      const finish = await submitTrigger();
      if (closeKind === 'button') await trigger.getByRole('button', { name: '关闭窗口', exact: true }).click();
      if (closeKind === 'escape') await page.keyboard.press('Escape');
      if (closeKind === 'cross') await trigger.getByRole('button', { name: '关闭', exact: true }).click();
      await page.evaluate(() => { location.hash = '#/settings'; });
      await main.getByRole('heading', { name: '设置', exact: true }).waitFor();
      await main.getByRole('radio', { name: '自动', exact: true }).check();
      await main.getByLabel('白天开始', { exact: true }).fill('06:13');
      await finish(closeKind === 'cross' ? 500 : 200);
      await page.waitForTimeout(120);
      check(page.url().endsWith('#/settings'), `${closeKind} 的旧响应改变导航`);
      check(await main.getByLabel('白天开始', { exact: true }).inputValue() === '06:13', '旧响应丢失未保存输入');
    }
    await go('daily_sync');
    await openTrigger();
    const finishOld = await submitTrigger();
    await trigger.getByRole('button', { name: '关闭窗口', exact: true }).click();
    await openTrigger();
    await trigger.getByRole('textbox').fill('{"new":true}');
    await finishOld();
    await page.waitForTimeout(120);
    check(await trigger.getByRole('textbox').inputValue() === '{"new":true}', '旧响应关闭或覆盖了新窗口');
    await trigger.getByRole('button', { name: '创建运行', exact: true }).click();
    await trigger.getByText('关闭窗口不会取消已提交的运行，可在运行记录中查看结果。', { exact: true }).waitFor();
    await pendingTriggers.at(-1)(500);
    await trigger.getByRole('alert').getByText('触发测试错误', { exact: true }).waitFor();
    check(await trigger.getByRole('button', { name: '创建运行', exact: true }).isEnabled(), '有效请求失败后不能重试');
    await trigger.getByRole('button', { name: '取消', exact: true }).click();
    result.expiredTriggerCallbacks = ['关闭窗口', 'Esc', '关闭按钮', '卸载', '重开', '有效失败'];

    await go('daily_sync', 'logs');
    await main.getByRole('heading', { name: '任务日志', exact: true }).waitFor();
    await tab('概览').click();
    await openTrigger();
    const finishAfterBack = await submitTrigger();
    await page.goBack();
    await page.waitForURL('**/#/dags/daily_sync?tab=logs');
    await trigger.waitFor({ state: 'hidden' });
    await finishAfterBack(200, 'run-after-tab-navigation');
    await main.locator('select[aria-label="选择运行"] option[value="run-after-tab-navigation"]')
      .waitFor({ state: 'attached' });
    check(page.url().endsWith('?tab=logs'), '迟到的触发回调覆盖日志标签导航');
    await main.getByRole('heading', { name: '任务日志', exact: true }).waitFor();
    result.triggerAfterTabNavigation = '日志标签保留，新运行出现在运行记录';

    await go('daily_sync', 'logs');
    await main.getByRole('button', { name: '预览重跑', exact: true }).click();
    previewResponse = { to_clear: [], blocked: [{ task_id: 'example', reason: '任务仍在运行' }] };
    await inspect();
    await clearDialog.getByText('example：任务仍在运行', { exact: true }).waitFor();
    check(await confirm().isDisabled(), '阻塞预览允许确认');
    previewResponse = { to_clear: ['example'], blocked: [] };
    await inspect(true);
    await page.waitForFunction(() => ![...document.querySelectorAll('dialog button')].find(node => node.textContent.trim() === '清理并等待重跑')?.disabled);
    clearStatus = 409;
    await confirm().click();
    await clearDialog.getByRole('alert').getByText('任务状态已变化或仍在运行，请重新预览。', { exact: true }).waitFor();
    check(await confirm().count() === 0, '409 后仍能提交旧快照');
    await inspect();
    await confirm().waitFor();
    previewStatus = 500;
    await inspect(true);
    await clearDialog.getByRole('alert').getByText('预览测试错误', { exact: true }).waitFor();
    check(await confirm().count() === 0, '刷新失败后仍能提交旧快照');
    previewStatus = 200;
    delayPreview = true;
    const seen = new Promise(resolve => { previewArrived = resolve; });
    await inspect();
    await seen;
    check(await clearDialog.getByRole('combobox').isDisabled(), '预览期间范围未锁定');
    finishPreview();
    await confirm().waitFor();
    delayPreview = false;
    await clearDialog.getByRole('combobox').selectOption('all');
    check(await confirm().count() === 0, '切换范围仍保留旧快照');
    previewResponse = { to_clear: ['example', 'historical-task'], blocked: [] };
    await inspect();
    await clearDialog.getByText('将清理 2 个任务', { exact: true }).waitFor();
    clearStatus = 200;
    await confirm().click();
    await clearDialog.waitFor({ state: 'hidden' });
    const clearCalls = calls.filter(call => call.path === '/api/runs/clear');
    check(JSON.stringify(clearCalls.map(call => call.body.task_ids)) === JSON.stringify([['example'], ['example', 'historical-task']]), '确认未使用新预览任务范围');
    result.previewRecovery = ['blocked 刷新', '409 失效', '刷新失败失效', '范围锁定', '新快照确认'];

    for (const kind of ['取消', 'Escape']) {
      await main.getByRole('button', { name: '预览重跑', exact: true }).click();
      previewResponse = { to_clear: ['example'], blocked: [] };
      await inspect();
      await confirm().waitFor();
      delayClear = true;
      const accepted = new Promise(resolve => { clearArrived = resolve; });
      await confirm().click();
      await accepted;
      if (kind === '取消') await clearDialog.getByRole('button', { name: '取消', exact: true }).click();
      else await page.keyboard.press('Escape');
      await clearDialog.waitFor({ state: 'hidden' });
      await openTrigger();
      const draft = JSON.stringify({ close: kind });
      await trigger.getByRole('textbox').fill(draft);
      finishClear();
      const notice = main.locator('.notice.success').filter({ hasText: '已清理 1 个任务，等待调度器执行。' });
      await notice.waitFor();
      check(await trigger.getByRole('textbox').inputValue() === draft, '旧清理回调关闭或覆盖了新触发弹窗');
      await trigger.getByRole('button', { name: '取消', exact: true }).click();
      await notice.getByRole('button', { name: '关闭', exact: true }).click();
      delayClear = false;
    }
    result.clearAfterDialogClose = ['取消', 'Escape'];

    await main.getByRole('button', { name: '回补预览', exact: true }).click();
    const backfill = page.getByRole('dialog', { name: '回补日期预览', exact: true });
    await backfill.getByLabel('开始日期', { exact: true }).fill('2026-07-01');
    await backfill.getByLabel('结束日期', { exact: true }).fill('2026-07-01');
    const submitBackfill = backfill.getByRole('button', { name: '预览日期', exact: true });
    await submitBackfill.click();
    await backfill.getByText('共 1 个区间，当前可创建 1 个', { exact: true }).waitFor();
    backfillStatus = 503;
    await submitBackfill.click();
    await backfill.getByRole('alert').getByText('回补预览测试错误', { exact: true }).waitFor();
    check(await backfill.locator('.preview-box').count() === 0, '回补重新预览失败仍展示旧数量');
    backfillStatus = 200;
    backfillEligible = false;
    await submitBackfill.click();
    await backfill.getByText('共 1 个区间，当前可创建 0 个', { exact: true }).waitFor();
    await page.keyboard.press('Escape');
    await backfill.waitFor({ state: 'hidden' });
    result.backfillPreviewRetry = ['成功', '失败清空', '重试展示新数量'];

    for (const width of [320, 390, 768, 1440]) {
      await page.setViewportSize({ width, height: 900 });
      await page.waitForFunction(() => document.querySelector('#workspace-navigation').open === (innerWidth > 1000));
      await main.getByRole('button', { name: '预览重跑', exact: true }).click();
      await inspect();
      await confirm().waitFor();
      check(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), `${width}px 文档溢出`);
      check(await clearDialog.getByRole('button', { name: '重新预览', exact: true }).isVisible(), '窄屏刷新预览不可达');
      check(await clearDialog.evaluate(node => {
        const dialog = node.getBoundingClientRect();
        return [...node.querySelectorAll('.dialog-actions button')].every(button => {
          const box = button.getBoundingClientRect();
          return box.left >= Math.max(0, dialog.left) && box.right <= Math.min(innerWidth, dialog.right);
        });
      }), `${width}px 操作按钮超出弹窗或视口`);
      await page.keyboard.press('Escape');
    }

    const definition = definitions.daily_sync;
    definition.tasks[0].upstream = ['原定义上游'];
    await page.goto(`${origin}/#/history/run?dag_id=daily_sync&run_id=${encodeURIComponent(runId)}`);
    await main.locator('.task-info').getByText('原定义上游', { exact: true }).waitFor();
    definition.definition_hash = 'changed-definition';
    definition.tasks[0].upstream = ['新定义上游'];
    await main.getByText(/工作流定义已变化/).waitFor();
    check(await main.locator('.task-info').getByText(/原定义上游|新定义上游/).count() === 0, '异步轮询混用了历史状态与新定义');
    await main.getByText('原始任务日志', { exact: true }).waitFor();
    result.historyVersions = '定义轮询先到时隐藏未核验的依赖，保留日志';

    rolledUptime = false;
    await go('daily_sync');
    const chart = main.locator('.uptime-detail-chart');
    const redSlot = chart.locator('button.uptime-failed');
    await redSlot.click();
    const panel = chart.locator('.uptime-failures');
    await panel.locator('.uptime-failure-item').nth(1).click();
    const fullLog = panel.getByRole('link', { name: '查看完整日志 →', exact: true });
    check((await fullLog.getAttribute('href')).includes('attempt=2'), '没有选中第二次失败尝试');
    await panel.evaluate(node => { window.reviewFailurePanel = node; });
    rolledUptime = true;
    await redSlot.waitFor({ state: 'detached' });
    check(await panel.evaluate(node => window.reviewFailurePanel === node), '红格移出窗口后卸载了失败面板');
    check((await fullLog.getAttribute('href')).includes('attempt=2'), '红格移出窗口后丢失已选尝试');
    check(calls.filter(call => call.path === '/api/workflows/uptime/failures').every(call =>
      call.start === firstSlot.start_at && call.end === firstSlot.end_at), '轮询改变了打开时的失败时段');
    await panel.getByRole('button', { name: '收起失败详情', exact: true }).click();
    await panel.waitFor({ state: 'detached' });
    result.failureOutsideWindow = '保留原时段和第二次尝试直到收起';
    rolledUptime = null;

    role = 'viewer';
    await page.goto(`${origin}/#/dags/..`);
    await page.reload();
    await main.getByRole('heading', { name: '..', exact: true }).waitFor();
    await main.getByRole('combobox', { name: '选择运行', exact: true }).waitFor();
    check(await main.getByRole('link', { name: '源码', exact: true }).count() === 0, 'viewer 暴露源码');
    check(await main.getByRole('button', { name: /触发运行|暂停调度|恢复调度/ }).count() === 0, 'viewer 暴露管理操作');
    check(errors.length === 0, `浏览器异常：${errors.join('; ')}`);
    check(unexpected.length === 0, `未覆盖接口：${unexpected.join('; ')}`);
    result.viewportAndViewer = [320, 390, 768, 1440];
    result.pageErrors = errors.length;
    return result;
  } catch (error) {
    throw new Error(`已完成 ${JSON.stringify(result)}；${error.message}；未预期接口 ${unexpected.join('; ')}`);
  } finally {
    finishPreview?.();
    finishClear?.();
    await context.close();
  }
}
