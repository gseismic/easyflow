import { workflowHref } from "../listRoute";
import SchedulerImpact from "./SchedulerImpact";

export default function WorkflowHeader({ dagId, dag, tab, isAdmin, historyView, backHref, schedulerOffline, actions, children }) {
  return <header className="workflow-header">
    <div className="detail-heading compact-heading">
      <div className="detail-identity-row">
        <a className="back-link detail-back" href={backHref} aria-label="返回工作流列表" title="返回工作流列表">←</a>
        <div className="detail-identity">
          <h1 className="detail-title">{dagId}</h1>
          <div className="meta">
            {dag ? <>
              <span>{dag.paused ? "● 已暂停" : "● 已启用"}</span>
              <span>调度：<b className="mono">{dag.schedule || "手动触发"}</b></span>
              <span>{dag.task_count} 个任务</span>
              <SchedulerImpact offline={schedulerOffline} />
            </> : <span>工作流信息待确认</span>}
          </div>
        </div>
      </div>
      {actions && <div className="actions">{actions}</div>}
    </div>
    {children}
    <nav className="detail-tabs" aria-label="工作流视图">
      {[
        ["run", "概览"], ["tasks", "任务"], ["history", "运行历史"],
        ["logs", "日志"], ["schedule", "调度详情"], ...(isAdmin ? [["source", "源码"]] : []),
      ].map(([value, label]) => <a key={value}
        className={tab === value ? "detail-tab active" : "detail-tab"}
        aria-current={tab === value ? "page" : undefined}
        href={workflowHref(dagId, value, historyView)}>{label}</a>)}
    </nav>
  </header>;
}
