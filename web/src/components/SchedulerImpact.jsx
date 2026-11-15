export default function SchedulerImpact({ offline }) {
  return offline ? <span className="scheduler-impact" role="status">新运行将等待调度器启动</span> : null;
}
