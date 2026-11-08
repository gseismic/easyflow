import Status, { formatDuration, formatTime } from "./Status";

const triggerRules = {
  all_success: "上游全部成功",
  all_done: "上游全部结束",
  one_success: "至少一个上游成功",
  one_failed: "至少一个上游失败",
  none_failed_min_one_success: "上游没有失败且至少一个成功",
  always: "不等待上游",
};

export default function TaskDetails({ task, detail, hasRun }) {
  if (!task)
    return (
      <section className="card task-info">
        <div className="empty compact">选择任务查看详情。</div>
      </section>
    );
  const heartbeat = detail?.last_heartbeat_at;
  const expired =
    detail?.lease_expires_at &&
    new Date(detail.lease_expires_at).getTime() <= Date.now();
  const heartbeatText = !heartbeat
    ? "暂无心跳"
    : `${expired && detail?.state === "running" ? "可能超时 · " : ""}${formatTime(heartbeat)}`;
  return (
    <section className="card task-info">
      <div className="section-heading">
        <div>
          <div className="eyebrow">
            TASK <span>／ 任务详情</span>
          </div>
          <h2 className="task-heading">{task.task_id}</h2>
        </div>
        <Status value={hasRun ? detail?.state : null} />
      </div>
      <dl className="info-grid task-grid">
        <div>
          <dt>任务类型</dt>
          <dd>{task.operator_type || "未知"}</dd>
        </div>
        <div>
          <dt>触发规则</dt>
          <dd>
            {triggerRules[task.trigger_rule] || task.trigger_rule || "未知"}
          </dd>
        </div>
        <div>
          <dt>上游</dt>
          <dd>
            {task.upstream?.length
              ? task.upstream.join("、")
              : task.upstream
                ? "无"
                : "未知"}
          </dd>
        </div>
        <div>
          <dt>下游</dt>
          <dd>
            {task.downstream?.length
              ? task.downstream.join("、")
              : task.downstream
                ? "无"
                : "未知"}
          </dd>
        </div>
        <div>
          <dt>入队时间</dt>
          <dd>{formatTime(detail?.queued_at)}</dd>
        </div>
        <div>
          <dt>开始时间</dt>
          <dd>{formatTime(detail?.started_at)}</dd>
        </div>
        <div>
          <dt>结束时间</dt>
          <dd>{formatTime(detail?.ended_at)}</dd>
        </div>
        <div>
          <dt>执行耗时</dt>
          <dd>
            {formatDuration(
              detail?.started_at,
              detail?.ended_at ||
                (detail?.state === "running" ? new Date().toISOString() : null),
            )}
          </dd>
        </div>
        <div>
          <dt>执行次数</dt>
          <dd>{detail?.try_number ?? "—"}</dd>
        </div>
        <div>
          <dt>最多重试</dt>
          <dd>{task.operator_type ? (task.retries ?? "0") : "未知"}</dd>
        </div>
        <div>
          <dt>重试间隔</dt>
          <dd>
            {!task.operator_type
              ? "未知"
              : task.retries
                ? task.retry_delay || "默认"
                : "—"}
          </dd>
        </div>
        <div>
          <dt>依赖上次运行</dt>
          <dd>
            {task.depends_on_past == null
              ? "未知"
              : task.depends_on_past
                ? "是"
                : "否"}
          </dd>
        </div>
        <div>
          <dt>下次重试</dt>
          <dd>{formatTime(detail?.next_retry_at)}</dd>
        </div>
        <div>
          <dt>最近心跳</dt>
          <dd>{heartbeatText}</dd>
        </div>
      </dl>
      {detail?.state === "running" && (
        <p className="task-hint">
          心跳表示最近一次报告，不能保证任务内部持续推进。
        </p>
      )}
    </section>
  );
}
