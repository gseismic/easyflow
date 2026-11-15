# 失败详情原位展开结果

- 创建时间：2026-11-15 17:10 Asia/Shanghai。
- 执行模型：Codex（基于 GPT-6；环境未提供可核实的完整模型 ID，标记 `model-unknown`）。
- 完成范围：`WORKTREE`，基准提交 `f5ff6f901f51c541ad147cd90e701c3d222b8e15`。相关文件为 uptime 聚合/路由、UptimeStrip、新增 UptimeFailures、uptime 辅助函数、样式、后端/前端测试、静态入口/新构建资源、README、本轮计划/设计/结果及 TODO。编写时尚未提交。
- 计划：`docs/plans/PLAN-20261115-008-model-unknown-inline-failures.md`。
- 设计：`docs/design/easyflow-20261115-inline-failures.md`。
- 状态：实施、自查与必要验证完成。

## 完成内容

列表和工作流详情共享的红格下方已增加失败面板，日、周、月均可原位展开。面板提供分页失败记录、任务和尝试选择、原始错误类型/摘要、该次开始结束与耗时、单独标注的运行当前状态、对应失败尝试日志预览，以及同一次尝试的日志页面入口。支持收起、Esc 和焦点返回；原有基础搜索筛选继续常驻。

关键实现依据：

- `src/easyflow/web/uptime.py:211`：按实际失败时间读取，使用左闭右开区间、微秒精度、未来过滤、有界分页和摘要；重试成功不覆盖历史错误。
- `src/easyflow/web/uptime.py:254`：没有同格失败任务记录时返回运行级失败，避免重复计算运行兜底；运行数按 run_id 去重。
- `src/easyflow/web/routers/catalog.py:85`：新增只读 `GET /api/workflows/uptime/failures`，沿用登录和 viewer 读取权限，限制每页 1–100 条及非负 64 位 offset。
- `web/src/components/UptimeFailures.jsx:7`：独立处理失败列表、选中尝试、日志、错误重试及分页；日志请求明确指定 try_number。
- `web/src/components/UptimeFailures.jsx:17`：新日志滚至末尾，使最终错误可见；没有新内容时不打断用户阅读。
- `web/src/components/UptimeStrip.jsx:18`：关闭时返回原格子焦点；共享面板在红格下按选中时段实例化。
- `web/src/uptime.js:116`：查询、记录标识和具体尝试链接保留特殊 ID；日志预览最多末尾 12 行、8000 字符。

无任务、未保存摘要、日志缺失、空日志、请求失败保留旧数据、极长名称及快速切换分别提供明确状态，不从旧记录推测未保存的错误或时间。错误摘要显示上限为 2000 字符，日志详情页继续沿用既有末尾 1 MB 上限。

没有增加依赖、修改数据库或调度语义。构建保留全部旧的已跟踪资源，入口使用 `index-CEj1MzMh.js` 与 `index-D_yrgjbj.css`。

## 验证结果

- 全量 `uv run pytest -q`：262 项通过，169.89 秒；有既有 Starlette/httpx 弃用提示。补充 offset 上限后，相关 `tests/test_web_uptime.py` 再次 17 项通过。
- `pnpm --dir web test`：14 项通过；`pnpm --dir web build` 成功。
- 三个修改 Python 文件的默认 Ruff、全仓 `--select E4,E7,E9,F`、`git diff --check` 通过。
- 默认全量 Ruff 的既有基线问题仍见 `KI-20261108-2311-001`；本轮未清理该基线，也未将核心规则检查描述为默认全量检查通过。
- 真实隔离演示库：验证日周月、列表与详情、完整日志链接锁定第 1 次失败尝试、收起/Enter/Esc/方向键、查看当天，以及 390px 无横向溢出。新浏览器上下文没有控制台错误或运行时异常，最终页面无 alert。
- 网络夹具：验证 27 条记录分页、末页刷新返回首页、重试后成功状态、具体尝试参数及特殊 ID、失败请求与重试、无任务记录、日志缺失/空、切换期间旧日志不显示、延迟旧响应不覆盖新选择、超长内容窄屏、空记录和更新失败的旧数据提示。模拟 503 仅用于验证异常状态。
- 自查核对 API 与 uptime 的失败运行数一致，计数和当前页共享读快照；范围外源码和原有未提交资料没有覆盖或回滚。

验证脚本在仓库外：`/tmp/easyflow-inline-failures-browser-check.js`、`/tmp/easyflow-inline-failures-edge-check.js`。真实截图为 `/tmp/easyflow-inline-failures-panel.png`、`/tmp/easyflow-inline-failures-mobile.png`。这些是本机演示验收资料，不是业务执行证据或新图片设计稿。

## 演示服务

`http://127.0.0.1:8791` 已恢复并加载新构建，继续使用交接文档中的隔离演示状态库、DAG 和日志目录。启动参数为 `--no-scheduler`，保留调度器离线状态，未触发任务；已有 `easyflow-uptime` 浏览器会话和账户继续可用。没有改动默认业务状态库或提交演示凭据。

## 计划偏差与保留事项

无实质范围偏差。为防止极大 offset 超过 SQLite 整数范围，补充了 64 位上限及参数验证；刷新回到第一页，避免末页数据变化后无记录可选。日志预览滚至末尾，便于直接看到最终异常。

其他候选方向只记录为待评估，不自动承诺开发；索引为 `docs/TODOS.md`：

- `TD-20261115-1708-001`：周期统计，建议优先级高。
- `TD-20261115-1708-002`：任务与最近运行矩阵，建议优先级中。
- `TD-20261115-1708-003`：排队与执行耗时对比，建议优先级中。

分页请求之间不冻结数据库，自动更新可能改变当前页记录；刷新读取当前状态。未保存的旧错误/日志仍不能恢复，界面已说明。没有发现本轮结束后需要新增 KNOWN-ISSUES 的已确认缺陷。

本轮提交仅包含授权范围，保留原有编号 004 文档/索引、Review 和 A1 图片等未提交资料；不推送已有范围外提交。
